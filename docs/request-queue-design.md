# 请求队列设计

> 文档定位：说明 `gamesrv` 同步请求队列的目标、边界、背压、超时、配置、监控和演进方向。它是 `runtime-flows.md` 中命令分发链路的详细设计。

## 快速摘要

| 主题 | 设计 |
| --- | --- |
| 放置位置 | `GameCommandService.Dispatch` 入口 |
| 队列类型 | 单进程全局容量 + `RoleID` 独立 lane |
| 消费模型 | 同一 `RoleID` FIFO 串行，不同 `RoleID` 可并行 |
| 队列满 | 返回 `CommandResponse(code=429)` |
| 请求超时 | 返回 `CommandResponse(code=504)` |
| 边界 | 不持久化、不跨进程、不替代 Kafka |

## 1. 目标

请求队列用于保护 `gamesrv` 的同步玩家请求链路，解决高峰流量、慢请求和瞬时抖动导致的服务卡死问题。

它的目标是：

- 削峰：把瞬时涌入的 `Dispatch` 请求先放入有界队列。
- 背压：队列满时快速返回失败，不让请求无限堆积。
- 隔离：按 `RoleID` 分类请求，保证同一玩家串行处理，不同玩家相互独立。
- 超时：限制单个请求从入队到处理完成的最大耗时。
- 可观测：为后续暴露队列长度、耗时、超时、拒绝等指标留出明确边界。

请求队列不是可靠消息队列，不替代 Kafka。它不保证跨进程持久化、不做失败重试、不做广播，只用于单个 `gamesrv` 进程内的同步请求保护。

## 2. 放置位置

当前队列放在 `gamesrv.GameCommandService.Dispatch` 入口：

```text
gatesrv
    -> GameCommandService.Dispatch
    -> CommandRequestQueue global capacity
    -> RoleID lane
    -> worker pool
    -> CommandRegistry.Dispatch
    -> command handler
```

选择这个位置的原因：

- `gatesrv` 仍保持轻量，只负责连接、鉴权、路由和转发。
- `gamesrv` 最清楚本机业务处理能力，适合按实例控制并发和排队。
- 队列保护的是业务执行入口，而不是登录、路由、Kafka 消费等其它链路。
- 不改变 `command.proto` 的请求和回包结构，客户端协议保持稳定。

## 3. 核心模型

当前实现位于 `app/gamesrv/request_queue.go`。

```text
CommandRequestQueue:
    dispatcher: CommandDispatcher
    capacity: bounded global capacity
    lanes: map[RoleID]role lane
    workerSlots: max concurrent handlers
    request_timeout: per request deadline

role lane:
    key: RoleID
    jobs: bounded channel
    order: FIFO

commandJob:
    context
    CommandRequest
    result channel
```

请求处理步骤：

```text
1. Dispatch 收到 CommandRequest。
2. 为请求创建带超时的 context。
3. 先尝试占用全局容量。
4. 根据 CommandRequest.role_id 找到玩家 lane。
5. 尝试写入该 RoleID 的有界 lane。
6. lane 按 FIFO 顺序取出请求。
7. worker 调用 CommandRegistry.Dispatch。
8. handler 返回后组装 CommandResponse。
9. Dispatch 把响应返回给上游 gatesrv。
```

如果 `role_id` 为空，队列会退回使用 `uid` 作为 lane key，避免所有无角色请求落到同一个默认 lane。

### 3.1 RoleID lane

`RoleID lane` 是请求队列的隔离单元。每个玩家拥有独立 lane：

```text
RoleID=1001:
    req#1 -> req#2 -> req#3

RoleID=1002:
    req#4 -> req#5

RoleID=1003:
    req#6
```

调度规则：

| 规则 | 说明 |
| --- | --- |
| lane key | 优先使用 `CommandRequest.role_id`，缺失时退回 `uid` |
| lane 内顺序 | 同一 lane 内按 FIFO 串行处理 |
| lane 间关系 | 不同 lane 互不等待，可并行消费 |
| worker 限制 | 所有 lane 共享 `request_queue_workers` 并发上限 |
| 容量限制 | 全局容量 + 单 lane 容量两层限制 |

为什么按 `RoleID` 而不是只按 `UID`：

- 业务写状态通常以角色为主体，例如邮件读取、领取、删除。
- 同账号多角色时，角色之间可以天然隔离。
- `RoleID` 更贴近玩家业务状态归属，适合做串行一致性边界。

### 3.2 lane 生命周期

当前实现采用按需创建：

```text
第一次收到某个 RoleID 请求
    -> 创建 lane
    -> lane goroutine 开始监听自己的 jobs channel
```

当前版本暂不做 lane 空闲回收。这样实现简单，适合先保证行为正确。后续如果在线玩家规模很大，需要补充：

- lane 最后活跃时间。
- 空闲 lane 定期回收。
- lane 数量上限和监控。
- 回收时确保 lane 内没有未处理请求。

### 3.3 调度示例

假设 `request_queue_workers=2`：

```text
t0:
    RoleID=1 reqA 进入 lane-1，开始执行
    RoleID=1 reqB 进入 lane-1，等待 reqA
    RoleID=2 reqC 进入 lane-2，开始执行

t1:
    reqC 执行完成
    RoleID=3 reqD 进入 lane-3，可以开始执行
    reqB 仍等待 reqA，因为同一 RoleID 必须串行

t2:
    reqA 执行完成
    reqB 才开始执行
```

这个模型保证：

- 单个玩家的请求不会并发修改同一份玩家状态。
- 某个玩家的慢请求只阻塞自己的后续请求。
- 其它玩家只受 worker 总并发影响，不会进入同一个 FIFO 队列等待。

## 4. 背压策略

队列必须是有界队列，不能使用无限队列。当前有两层容量控制：

| 层级 | 作用 |
| --- | --- |
| 全局容量 | 限制单个 `gamesrv` 上所有排队和处理中的请求总量 |
| RoleID lane 容量 | 限制单个玩家最多允许排队的请求数量 |

当前策略：

```text
全局容量未满，且 RoleID lane 未满:
    请求进入该玩家 lane，等待 worker 消费。

全局容量已满，或 RoleID lane 已满:
    不阻塞写队列。
    立即返回 CommandResponse(code=429, message="command queue full")。
```

这样可以避免高峰期请求全部堆在内存中，也避免 gRPC handler 被长时间阻塞。

`429` 表示当前 `gamesrv` 繁忙。上游可以按业务策略处理：

- `gatesrv` 可直接把繁忙响应返回客户端。
- 客户端可做短暂退避后重试。
- 对非关键请求可直接提示稍后再试。
- 对强一致写请求不应盲目自动重试，必须结合幂等设计。

### 4.1 容量判定顺序

入队时按以下顺序判断：

```text
1. 请求 context 是否已取消。
2. 队列是否正在关闭。
3. 全局容量是否还有空位。
4. RoleID lane 是否还有空位。
5. 成功进入 lane。
```

返回语义：

| 场景 | 返回 |
| --- | --- |
| context 已取消 | 返回 context error |
| 队列关闭 | 返回 `ErrCommandQueueClosed` |
| 全局容量满 | `CommandResponse(code=429)` |
| 单个 RoleID lane 满 | `CommandResponse(code=429)` |
| 已入队但等待超时 | `CommandResponse(code=504)` |

全局容量先占位，再尝试写入 RoleID lane。如果写入 lane 失败，会释放全局容量，避免容量泄漏。

### 4.2 全局容量和单玩家容量

两层容量解决的问题不同：

| 容量 | 保护对象 | 示例 |
| --- | --- | --- |
| `request_queue_capacity` | 保护整个 `gamesrv` 实例 | 大量玩家同时请求时，限制实例内存占用 |
| `request_role_queue_capacity` | 保护其它玩家不被单玩家挤占 | 某个玩家网络重试风暴，只占用自己的 lane 上限 |

建议 `request_role_queue_capacity` 明显小于 `request_queue_capacity`。例如：

```text
request_queue_capacity: 1024
request_role_queue_capacity: 32
```

这样单个玩家最多占用 32 个排队请求，不会把 1024 个全局容量全部占满。

## 5. 超时策略

请求超时由 `game.request_timeout` 控制，默认 `3s`。

计时范围包括：

```text
排队等待时间 + handler 执行时间
```

当前策略：

```text
未超时:
    正常返回业务响应。

超时:
    返回 CommandResponse(code=504, message="command request timeout")。
```

超时不是强制杀死 Go goroutine。请求队列会取消传给 handler 的 `context`，但底层能否真正停止取决于 handler 是否正确使用 `context`。

handler 编写要求：

- MySQL 查询必须使用带 context 的方法。
- Redis 操作必须使用请求 context。
- 下游 gRPC 调用必须传递请求 context。
- 长循环或批处理必须定期检查 `ctx.Done()`。
- 超时后不要继续写玩家状态，除非业务明确支持幂等和补偿。

### 5.1 超时发生位置

`request_timeout` 从 `Dispatch` 入口开始计时，覆盖三段时间：

| 阶段 | 说明 | 超时结果 |
| --- | --- | --- |
| 全局容量等待 | 当前实现不阻塞等待容量，容量满直接 429 | 不进入超时 |
| lane 排队等待 | 已进入 RoleID lane，等待同玩家前序请求完成 | 返回 504 |
| worker/handler 执行 | 拿到 worker 后执行业务 handler | 返回 504 |

同一 RoleID 的前序请求过慢时，后续请求可能在 lane 内等待到超时。这是预期行为：它避免同一玩家后续请求越过前序请求直接执行。

### 5.2 超时和幂等

`504` 表示请求在队列层已超时，但不能保证底层业务完全没执行。

写请求必须考虑以下情况：

```text
客户端发送领取请求 seq=10
    -> gamesrv 开始处理
    -> 队列层 3s 超时，返回 504
    -> handler 中某个下游操作如果没有响应 context，可能稍后仍写入成功
    -> 客户端重试 seq=10
```

因此写路径需要幂等键。推荐优先级：

| 幂等键 | 适用场景 |
| --- | --- |
| 业务流水号 | 充值、奖励领取、道具发放 |
| `role_id + command_id + seq` | 客户端请求序号可靠时 |
| 业务唯一约束 | 邮件领取状态、任务领取状态 |

## 6. 关闭流程

服务退出时，`cmd/gamesrv` 会调用 `CommandRequestQueue.Close()`。

关闭语义：

```text
1. 关闭 done 信号。
2. Dispatch 不再接受新请求。
3. worker 收到 done 后退出。
4. Close 等待 worker pool 退出。
```

关闭过程中，新请求返回 `ErrCommandQueueClosed`。后续可以在 gRPC 层把它映射为更明确的服务不可用响应。

RoleID lane 在关闭时会收到统一的 `done` 信号并退出。已经进入业务 handler 的请求依赖 context 和服务优雅退出窗口完成收尾。

## 7. 配置项

YAML 配置：

```yaml
game:
  request_queue_workers: 4
  request_queue_capacity: 1024
  request_role_queue_capacity: 32
  request_timeout: 3s
```

配置含义：

| 配置项 | 默认值 | 作用 | 调整风险 |
| --- | --- | --- | --- |
| `request_queue_workers` | `4` | 控制本机同时执行 handler 的最大并发 | 过高会放大 MySQL、Redis 和下游 RPC 压力 |
| `request_queue_capacity` | `1024` | 控制单实例所有 RoleID 的总请求容量 | 过大容易把延迟问题隐藏成内存堆积 |
| `request_role_queue_capacity` | `32` | 控制单个 RoleID 的最大排队请求数 | 过大时单个玩家可能占用过多全局容量 |
| `request_timeout` | `3s` | 控制单请求总耗时，包含排队和执行 | 过短会误杀慢请求，过长会拖慢失败反馈 |

调优建议：

- `request_queue_workers` 初始可按 CPU 核数或业务压测结果设置。
- CPU 密集型业务 worker 不宜远高于 CPU 核数。
- IO 密集型业务可以适当增加 worker，但必须关注 MySQL、Redis、下游 RPC 压力。
- `request_queue_capacity` 不宜过大，过大的队列会把延迟问题隐藏成内存堆积。
- `request_role_queue_capacity` 应明显小于全局容量，避免单个玩家的重试风暴影响其它玩家。
- `request_timeout` 应小于客户端等待超时，并和 `gatesrv` 转发超时保持一致。

### 7.1 参数选择示例

小规模开发环境：

```yaml
game:
  request_queue_workers: 4
  request_queue_capacity: 1024
  request_role_queue_capacity: 32
  request_timeout: 3s
```

IO 较多、MySQL/Redis 能力充足时：

```yaml
game:
  request_queue_workers: 16
  request_queue_capacity: 4096
  request_role_queue_capacity: 32
  request_timeout: 3s
```

强写入、需要保护数据库时：

```yaml
game:
  request_queue_workers: 4
  request_queue_capacity: 1024
  request_role_queue_capacity: 16
  request_timeout: 2s
```

调参时优先观察：

- worker 是否长期打满。
- lane 等待时间是否明显增加。
- 429 是否主要来自全局容量还是单 RoleID lane。
- 504 是否来自排队等待还是 handler 执行慢。

## 8. 返回码约定

当前请求队列使用 `CommandResponse.code` 表达队列层结果：

```text
0:
    成功。

429:
    队列已满，请求未进入业务处理。

504:
    请求超时，可能发生在排队阶段，也可能发生在 handler 执行阶段。

其它业务码:
    由具体 command handler 定义。
```

注意：`504` 不一定表示业务完全没有执行。如果 handler 没有正确响应 context，底层操作可能仍继续运行。因此写路径必须设计幂等键，例如 `uid + command_id + seq` 或业务流水号。

上游处理建议：

| 返回码 | gatesrv 行为 | 客户端行为 |
| --- | --- | --- |
| `0` | 原样返回 | 正常处理 |
| `429` | 原样返回，不清路由 | 可提示繁忙或退避重试 |
| `504` | 原样返回，不清路由 | 可提示超时；写请求重试必须依赖幂等 |
| gRPC 调用失败 | 清理 UID 路由 | 客户端可重试，gate 重新选服 |

## 9. 监控指标

后续应补充以下指标：

```text
command_queue_depth:
    当前排队数量。

command_queue_capacity:
    队列容量。

command_role_lane_depth:
    单个 RoleID lane 的排队数量。

command_role_lane_count:
    当前已创建的 RoleID lane 数量。

command_queue_rejected_total:
    队列满返回 429 的次数。

command_queue_rejected_total{reason="global_full"}:
    全局容量满导致拒绝。

command_queue_rejected_total{reason="role_lane_full"}:
    单个 RoleID lane 满导致拒绝。

command_queue_timeout_total:
    请求超时返回 504 的次数。

command_queue_wait_duration:
    入队到 worker 开始处理的等待耗时。

command_handler_duration:
    handler 实际执行耗时。

command_inflight:
    正在处理的请求数量。
```

告警建议：

- 队列深度持续高于 80%。
- 单个 RoleID lane 深度持续接近上限。
- `429` 在短时间内持续增长。
- `504` 持续增长。
- handler P95/P99 接近 `request_timeout`。

定位建议：

| 现象 | 可能原因 | 优先排查 |
| --- | --- | --- |
| 全局队列深度高 | worker 不足或整体业务慢 | worker 利用率、MySQL/Redis 延迟 |
| 单个 lane 深度高 | 某个玩家请求过多或前序请求慢 | 该 RoleID 的 command 分布和 handler 耗时 |
| 429 增长 | 全局容量或 lane 容量不足 | 拒绝原因标签 |
| 504 增长 | 排队等待过久或 handler 慢 | wait duration 与 handler duration |

## 10. 后续演进

当前版本是最小可用的进程内请求队列。后续可以按业务压力继续增强：

- 按 `CommandID` 在 RoleID lane 内做优先级，避免慢查询影响关键写请求。
- 增加 RoleID lane 空闲回收，避免长时间运行后 lane map 持续增长。
- 增加命令优先级，例如心跳、查询、写请求分级处理。
- 在 `gatesrv` 增加转发超时和重试策略。
- 增加幂等表或请求流水，处理超时后的重试和去重。
- 暴露 Prometheus 指标和 pprof，辅助压测调参。

是否需要升级到 Kafka、Redis Stream 或其它 MQ，取决于请求是否允许异步化。如果客户端需要同步回包，进程内请求队列更直接；如果请求可以异步完成并允许状态查询，才适合引入可靠消息队列。

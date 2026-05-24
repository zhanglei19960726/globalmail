# 请求队列设计

> 文档定位：说明 `gamesrv` 同步请求队列的目标、边界、背压、超时、配置、监控和演进方向。它是 `runtime-flows.md` 中命令分发链路的详细设计。

## 快速摘要

| 主题 | 设计 |
| --- | --- |
| 放置位置 | `GameCommandService.Dispatch` 入口 |
| 队列类型 | 单进程有界 channel |
| 消费模型 | 固定 worker pool |
| 队列满 | 返回 `CommandResponse(code=429)` |
| 请求超时 | 返回 `CommandResponse(code=504)` |
| 边界 | 不持久化、不跨进程、不替代 Kafka |

## 1. 目标

请求队列用于保护 `gamesrv` 的同步玩家请求链路，解决高峰流量、慢请求和瞬时抖动导致的服务卡死问题。

它的目标是：

- 削峰：把瞬时涌入的 `Dispatch` 请求先放入有界队列。
- 背压：队列满时快速返回失败，不让请求无限堆积。
- 隔离：用固定 worker pool 控制业务 handler 的并发量。
- 超时：限制单个请求从入队到处理完成的最大耗时。
- 可观测：为后续暴露队列长度、耗时、超时、拒绝等指标留出明确边界。

请求队列不是可靠消息队列，不替代 Kafka。它不保证跨进程持久化、不做失败重试、不做广播，只用于单个 `gamesrv` 进程内的同步请求保护。

## 2. 放置位置

当前队列放在 `gamesrv.GameCommandService.Dispatch` 入口：

```text
gatesrv
    -> GameCommandService.Dispatch
    -> CommandRequestQueue
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
    jobs: bounded channel
    worker pool: fixed goroutines
    request_timeout: per request deadline

commandJob:
    context
    CommandRequest
    result channel
```

请求处理步骤：

```text
1. Dispatch 收到 CommandRequest。
2. 为请求创建带超时的 context。
3. 尝试写入有界 jobs channel。
4. worker 从 jobs channel 取出请求。
5. worker 调用 CommandRegistry.Dispatch。
6. handler 返回后组装 CommandResponse。
7. Dispatch 把响应返回给上游 gatesrv。
```

## 4. 背压策略

队列必须是有界队列，不能使用无限队列。

当前策略：

```text
队列未满:
    请求入队，等待 worker 消费。

队列已满:
    不阻塞写队列。
    立即返回 CommandResponse(code=429, message="command queue full")。
```

这样可以避免高峰期请求全部堆在内存中，也避免 gRPC handler 被长时间阻塞。

`429` 表示当前 `gamesrv` 繁忙。上游可以按业务策略处理：

- `gatesrv` 可直接把繁忙响应返回客户端。
- 客户端可做短暂退避后重试。
- 对非关键请求可直接提示稍后再试。
- 对强一致写请求不应盲目自动重试，必须结合幂等设计。

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

## 7. 配置项

YAML 配置：

```yaml
game:
  request_queue_workers: 4
  request_queue_capacity: 1024
  request_timeout: 3s
```

配置含义：

| 配置项 | 默认值 | 作用 | 调整风险 |
| --- | --- | --- | --- |
| `request_queue_workers` | `4` | 控制本机业务并发 | 过高会放大 MySQL、Redis 和下游 RPC 压力 |
| `request_queue_capacity` | `1024` | 控制最多允许多少请求排队 | 过大容易把延迟问题隐藏成内存堆积 |
| `request_timeout` | `3s` | 控制单请求总耗时，包含排队和执行 | 过短会误杀慢请求，过长会拖慢失败反馈 |

调优建议：

- `request_queue_workers` 初始可按 CPU 核数或业务压测结果设置。
- CPU 密集型业务 worker 不宜远高于 CPU 核数。
- IO 密集型业务可以适当增加 worker，但必须关注 MySQL、Redis、下游 RPC 压力。
- `request_queue_capacity` 不宜过大，过大的队列会把延迟问题隐藏成内存堆积。
- `request_timeout` 应小于客户端等待超时，并和 `gatesrv` 转发超时保持一致。

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

## 9. 监控指标

后续应补充以下指标：

```text
command_queue_depth:
    当前排队数量。

command_queue_capacity:
    队列容量。

command_queue_rejected_total:
    队列满返回 429 的次数。

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
- `429` 在短时间内持续增长。
- `504` 持续增长。
- handler P95/P99 接近 `request_timeout`。

## 10. 后续演进

当前版本是最小可用的进程内请求队列。后续可以按业务压力继续增强：

- 按 `CommandID` 拆分队列，避免慢命令阻塞快命令。
- 按 UID 做串行队列，保证同一玩家写请求顺序执行。
- 增加命令优先级，例如心跳、查询、写请求分级处理。
- 在 `gatesrv` 增加转发超时和重试策略。
- 增加幂等表或请求流水，处理超时后的重试和去重。
- 暴露 Prometheus 指标和 pprof，辅助压测调参。

是否需要升级到 Kafka、Redis Stream 或其它 MQ，取决于请求是否允许异步化。如果客户端需要同步回包，进程内请求队列更直接；如果请求可以异步完成并允许状态查询，才适合引入可靠消息队列。

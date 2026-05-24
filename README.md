# GlobalMail Server

GlobalMail 是一个 Go 微服务项目，用于沉淀游戏服务器的登录接入、长连接网关、玩家业务、全局邮件、缓存、服务发现、事件通知和请求队列设计。

当前重点是全局邮件业务和服务器框架骨架：

| 模块 | 说明 |
| --- | --- |
| `accsrv` | 对外 HTTP 登录入口，请求和响应使用 protobuf message，对内 gRPC 转发到 `gamesrv` |
| `gatesrv` | WebSocket 长连接网关，负责 token 校验、连接池、心跳、UID 路由和命令转发 |
| `gamesrv` | 玩家业务服务，负责账号/玩家创建、全局邮件、命令注册、请求队列和业务执行 |
| `mgrsrv` | 管理入口骨架，用于后续 GM/运营后台能力 |
| `outboxrelay` | MySQL Outbox 到 Kafka 的事件投递进程 |

## 架构摘要

| 主题 | 当前方案 |
| --- | --- |
| 协议 | protobuf 定义请求、回包、命令字和 gRPC service；`accsrv` 对外登录使用 HTTP 承载 protobuf message |
| 路由 | `gatesrv` 按 UID 使用一致性哈希选择 `gamesrv`，路由结果写 Redis |
| 存储 | MySQL 作为权威存储，Redis 作为共享缓存和运行态索引，本地缓存承接热点读 |
| 服务发现 | etcd 负责实例注册、lease 续租、ready/draining/offline 状态和发现 |
| 事件通知 | MySQL Outbox + Kafka 广播全局邮件等业务事件 |
| 请求保护 | `gamesrv` 按 `RoleID` 建立独立请求 lane，同玩家串行、不同玩家并行，并提供 `429` 背压和 `504` 超时 |

请求队列的核心行为：

```text
GameCommandService.Dispatch
  -> 全局容量检查
  -> 按 RoleID 进入玩家独立 lane
  -> worker pool 消费
  -> CommandRegistry.Dispatch
```

同一 `RoleID` 的请求按 FIFO 串行处理，不同 `RoleID` 的请求可并行处理。这样可以避免单个玩家的慢请求或重试风暴影响其它玩家。

## 目录结构

```text
cmd/        服务进程启动入口
api/rpc/    protobuf 协议定义和生成代码
app/        各服务应用层编排
config/     YAML 配置加载、默认值和校验
domain/     领域模型、接口和核心业务规则
data/       MySQL、Redis、本地缓存等数据访问实现
infra/      Kafka、etcd 等基础设施适配
docs/       架构、流程、部署、需求和请求队列设计文档
```

## 文档入口

建议先阅读 [文档导航](docs/README.md)。

| 文档 | 内容 |
| --- | --- |
| [服务器架构方案](docs/server-architecture.md) | 服务分层、职责边界、总体架构图 |
| [运行时流程设计](docs/runtime-flows.md) | 登录、连接、路由、命令分发、故障恢复 |
| [需求设计方案](docs/requirements-design.md) | 全局邮件业务、MySQL/Redis/本地缓存/Kafka 设计 |
| [请求队列设计](docs/request-queue-design.md) | `gamesrv` 请求队列、背压、超时、监控和演进 |
| [部署方案](docs/deployment-plan.md) | 部署拓扑、服务发现、扩缩容、健康检查、容灾 |
| [代码目录结构](docs/code-structure.md) | 代码分层、模块职责、依赖方向 |

## 配置

服务统一从 YAML 文件读取配置，不从环境变量读取第三方组件配置。示例配置见：

```text
config/examples/globalmail.yaml
```

核心配置块：

```yaml
service:
  name: gamesrv
  instance_id: gamesrv-local-1

mysql:
  dsn: "user:password@tcp(127.0.0.1:3306)/globalmail?parseTime=true&charset=utf8mb4&loc=Local"

redis:
  addrs:
    - "127.0.0.1:6379"

kafka:
  brokers:
    - "127.0.0.1:9092"

etcd:
  endpoints:
    - "127.0.0.1:2379"

game:
  request_queue_workers: 4
  request_queue_capacity: 1024
  request_role_queue_capacity: 32
  request_timeout: 3s
```

## 本地验证

```bash
go test ./...
```

如修改 `.proto` 文件，需要重新生成对应 Go 代码后再测试。


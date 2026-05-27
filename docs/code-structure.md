# 代码目录结构

> 文档定位：说明当前代码目录如何映射到架构分层，以及新增模块应该放在哪一层。架构原则见 `server-architecture.md`，运行时链路见 `runtime-flows.md`。

## 快速摘要

| 目录 | 职责 |
| --- | --- |
| `cmd/` | 服务进程启动入口和依赖组装 |
| `api/rpc/` | protobuf message、gRPC service、CommandID |
| `app/` | 各服务应用层编排和协议适配 |
| `domain/` | 领域模型、接口和核心业务规则 |
| `data/` | MySQL、Redis、本地缓存等数据访问实现 |
| `infra/` | Kafka、etcd、服务治理和运维基础设施 |
| `config/` | YAML 配置结构、默认值和校验 |

## 1. 设计目标

代码不再使用外层 `internal` 目录。整体按职责分成五类顶层目录：

```text
cmd/       服务进程启动入口
app/       各服务的应用层编排
config/    第三方组件和服务配置管理
domain/    领域模型、接口和核心业务规则
data/      数据库、缓存、本地缓存、数据访问实现
infra/     Kafka、etcd、LB、metrics 等基础设施适配
```

其中 `data/` 统一放 DB 和缓存相关实现，避免 MySQL、Redis、本地缓存分散在不同外层目录。

## 2. 当前目录

```text
globalmail/
├── docs/
│   ├── README.md
│   ├── server-architecture.md
│   ├── runtime-flows.md
│   ├── deployment-plan.md
│   ├── requirements-design.md
│   ├── request-queue-design.md
│   └── code-structure.md
│
├── api/
│   └── rpc/
│       ├── login.proto            # HTTP 登录消息和 PlayerService 登录协议定义
│       ├── gate.proto             # GatewayService 路由查询和清理协议定义
│       ├── command.proto          # PlayerCommandService 和 CommandID 命令字定义
│       ├── mail.proto             # PlayerMailService 全局邮件读取协议定义
│       ├── *.pb.go                # protoc-gen-go 生成的请求/回包结构
│       └── *_grpc.pb.go           # protoc-gen-go-grpc 生成的 gRPC service
│
├── domain/
│   └── globalmail/                # 全局邮件领域层
│       ├── types.go               # 领域模型和值对象
│       ├── ports.go               # MySQL/Redis/Kafka/outbox 抽象接口
│       ├── publisher.go           # 发布全局邮件应用服务
│       ├── outbox.go              # Outbox Relay 应用逻辑
│       ├── cache.go               # playersrv 本地缓存、L2 回源和条件过滤
│       └── *_test.go              # 领域层单元测试
│
├── data/
│   ├── README.md                  # data 层说明
│   ├── mysql/                     # MySQL + GORM 实现
│   │   ├── models.go              # GORM Model 定义
│   │   ├── mapper.go              # GORM Model 和领域模型转换
│   │   └── repository.go          # Repository / OutboxRepository 实现
│   ├── redis/                     # Redis 二级缓存、路由缓存、幂等缓存和重建锁
│   │   ├── client.go              # Redis client 初始化
│   │   ├── repository.go          # CacheRepository 实现
│   │   └── routing.go             # DBLoginToken、DBGateConn、DBSrvRouter 实现
│   ├── localcache/                # playersrv 本地缓存说明
│   └── sqlschema/                 # 建表 SQL 常量
│
├── infra/
│   ├── kafka/                     # Kafka 事件总线适配
│   │   ├── producer.go            # EventPublisher 实现
│   │   └── consumer.go            # GlobalMailChanged 消费者，已实现
│   ├── etcd/                      # etcd 服务注册和发现适配
│       ├── client.go              # etcd client 初始化
│       ├── registry.go            # 服务注册、状态更新和摘除
│       └── discovery.go           # 服务发现和 watch
│   └── metrics/                   # expvar 轻量指标适配
│
├── config/
│   ├── config.go                  # 第三方组件配置结构和 YAML 加载
│   └── config_test.go             # 配置加载测试
│   └── examples/
│       └── globalmail.yaml        # YAML 配置示例
│
├── app/
│   └── bootstrap/                 # 配置路径和退出信号等服务启动公共工具，已实现
│
├── go.mod
└── README.md
```

## 3. 分层职责

### 3.0 `api/rpc/`

`api/rpc/` 是客户端和服务端、服务和服务之间的协议契约层。请求、回包、命令字和 gRPC service 都先在 `.proto` 中定义，再通过 `protoc-gen-go` 和 `protoc-gen-go-grpc` 生成 Go 代码；`accountsrv` 对外 HTTP 登录也复用这里生成的 protobuf message。

当前职责：

```text
login.proto:
    LoginRequest/LoginResponse
    PlayerService/Login

gate.proto:
    GatewayService/ResolveRoute
    GatewayService/ClearRoute
    GatewayService/Heartbeat

command.proto:
    CommandID
    PlayerCommandService/Dispatch
    PlayerCommandService/ListCommands

mail.proto:
    PlayerMailService/ListGlobalMails
    PlayerMailService/MarkGlobalMailRead
    PlayerMailService/ClaimGlobalMail
    PlayerMailService/DeleteGlobalMail
```

使用原则：

- 不在业务代码中手写命令字常量，新增命令先扩展 `CommandID`。
- 不在 `app/*` 中自定义重复的请求/回包结构，优先使用 protobuf 生成结构。
- `gatewaysrv` 只解析 `CommandID` 和透传 protobuf `Any` 载荷，不解析具体业务字段。
- `playersrv` 通过 `CommandRegistry` 把 `CommandID` 映射到具体 handler。

### 3.1 `domain/`

领域层只表达业务规则和应用流程。

负责：

- 定义 `GlobalMail`、`UserGlobalMailState`、`OutboxEvent` 等领域模型。
- 定义 `MailRepository`、`CacheRepository`、`EventPublisher`、`OutboxRepository` 接口。
- 实现全局邮件发布流程：业务数据 + outbox + Redis version。
- 实现 `OutboxRelay`：从 outbox 读取事件并发布到 Kafka 抽象接口。
- 实现 `playersrv` 本地缓存快照、版本刷新、L2 回源合并和条件过滤。

不负责：

- 不直接 import GORM。
- 不直接 import Redis/Kafka/etcd SDK。
- 不关心数据库连接、topic 配置、服务注册等部署细节。

### 3.2 `data/`

`data/` 统一承载数据库和缓存相关实现。

当前职责：

- `data/mysql`：使用 GORM 实现 MySQL repository。
- `data/sqlschema`：保存建表 SQL 常量。
- `data/redis`：实现 Redis 二级缓存、路由缓存、命令幂等缓存和缓存重建锁。
- `data/localcache`：记录本地缓存实现边界；当前实现仍在 `domain/globalmail`。

`data/mysql` 负责：

- 定义 GORM Model。
- 执行 `AutoMigrate`。
- 在同一个 MySQL 事务里写 `GlobalMail`、`GlobalMailCondition`、`GlobalMailOutboxEvent`。
- 查询已发布全局邮件并转换为领域模型。
- 保存玩家全局邮件状态。
- 保存奖励账本和 `playersrv` 背包发放明细。
- 扫描 pending outbox，并标记 published 或重试。

`data/redis` 负责：

- `GlobalMailVersion`
- `GlobalMail:{globalMailId}`
- `GlobalMailActiveIndex`
- `GlobalMailByServer:{serverID}`
- `MailUserProfile:{RoleID}`
- `CommandIdempotency:{uid:command_id:seq}`
- `GlobalMailRebuildLock:{version}`
- `DBLoginToken`
- `DBGateConn`
- `DBSrvRouter`

### 3.3 `infra/`

`infra/` 放非 DB/缓存类基础设施适配。

当前职责：

- `infra/kafka`：Kafka producer、consumer、consumer group 管理。
- `infra/etcd`：服务注册、lease 续租、服务发现和 watch。
- `infra/metrics`：基于 expvar 的轻量指标适配。

规划职责：

- `infra/lb`：LB 摘除和 drain 操作。
- 将 `infra/metrics` 对接到 Prometheus、OpenTelemetry 或统一告警后端。

Kafka、etcd 不放在 `data/`，因为它们不是业务数据存取层，而是事件总线和服务治理基础设施。

### 3.4 `app/`

`app/` 是各服务的应用层编排，负责把协议层、领域层、数据层和基础设施适配层组装成服务能力。

当前重点：

```text
app/accountsrv:
    实现 HTTP POST /login，使用 protobuf LoginRequest/LoginResponse。
    登录时调用 playersrv PlayerService.Login 获取或创建用户，再写 DBLoginToken。

app/gatewaysrv:
    实现 GatewayService 路由查询和清理。
    ConnectionManager 管理 ConnectionPool、Heartbeat、DBGateConn 续期和过期连接关闭。
    ConnectionPool 按 UID 和 ConnID 建索引，替换、删除、过期扫描时负责关闭真实连接。
    CommandForwarder 解析 CommandRequest，按 UID Resolve 到 playersrv，再调用 PlayerCommandService.Dispatch。

app/playersrv:
    实现 PlayerService.Login。
    实现 PlayerCommandService 和 CommandRegistry。
    CommandRequestQueue 在 Dispatch 入口按 RoleID 分组，提供玩家独立 lane、worker pool 和队列满背压。
    同一 RoleID 的 lane 内 FIFO 串行，不同 RoleID 的 lane 可并行消费。
    注册 CommandID -> handler。
    实现 PlayerMailService 和全局邮件命令 handler。
```

### 3.5 `config/`

`config/` 是统一配置模块，负责从 YAML 文件读取服务自身和第三方组件配置。

当前支持：

```text
Service:
    service.name
    service.instance_id
    service.env
    service.listen_addr
    service.public_addr

MySQL:
    mysql.dsn
    mysql.max_open_conns
    mysql.max_idle_conns
    mysql.conn_max_lifetime
    mysql.auto_migrate

Redis:
    redis.addrs
    redis.username
    redis.password
    redis.db
    redis.key_prefix
    redis.dial_timeout
    redis.read_timeout
    redis.write_timeout

Kafka:
    kafka.brokers
    kafka.topic_prefix
    kafka.consumer_group

etcd:
    etcd.endpoints
    etcd.username
    etcd.password
    etcd.dial_timeout
    etcd.lease_ttl
    etcd.keepalive_interval
    etcd.service_key_prefix

Gateway:
    gate.route_ttl
    gate.virtual_nodes
    gate.session_ttl
    gate.gateway_conn_renew_interval

Player:
    game.request_queue_workers
    game.request_queue_capacity
    game.request_role_queue_capacity
    game.request_timeout
```

使用原则：

- `cmd/*/main.go` 只从 `config.LoadFile(path)` 获取配置。
- 配置文件统一使用 YAML 格式，例如 `config/examples/globalmail.yaml`。
- 运行环境通过启动参数指定配置文件路径，不从环境变量读取第三方组件配置。
- `app/*` 接收已经组装好的依赖，不直接读取环境变量。
- `domain/*` 不依赖 `config`，避免业务规则和部署环境耦合。
- 第三方组件连接初始化由 `data/*` 或 `infra/*` 使用配置完成。

## 4. 服务目录划分

服务入口按进程划分，每个服务拥有独立启动入口，但复用同一套领域层、数据层和基础设施适配层。

推荐目录：

```text
cmd/
├── accountsrv/
│   └── main.go                    # 登录鉴权服务启动入口，已实现 token 骨架
├── gatewaysrv/
│   └── main.go                    # 长连接网关服务启动入口，已实现路由骨架
├── playersrv/
│   └── main.go                    # 玩家业务服务启动入口，已实现事件消费骨架
├── adminsrv/
│   └── main.go                    # GM/管理后台服务启动入口，已实现骨架
├── mailrelaysrv/
│   └── main.go                    # Outbox Relay 事件投递进程，已实现
└── recovery-controller/
    └── main.go                    # 自动化容灾控制器

app/
├── bootstrap/
│   └── config.go                  # YAML 配置路径和退出信号公共工具，已实现
│
├── accountsrv/
│   ├── server.go                  # HTTP 登录接口和 protobuf 编解码，已实现 Login
│   └── service.go                 # 登录业务编排，转发 playersrv 后写 DBLoginToken
│
├── gatewaysrv/
│   ├── server.go                  # gRPC GatewayService 路由查询/清理适配层，已实现
│   ├── connection.go              # ConnectionPool、心跳、DBGateConn 续期和过期连接关闭，已实现
│   ├── command_forwarder.go       # 解析 CommandRequest 后转发到目标 playersrv Dispatch，已实现
│   ├── session.go                 # SessPool 路由缓存，已实现
│   ├── router.go                  # UID 到 playersrv 的一致性哈希路由，已实现
│   ├── route_service.go           # session route、Redis route、etcd list 串联，已实现
│   ├── etcd_provider.go           # etcd ready playersrv 转路由节点，已实现
│   └── handler.go                 # 客户端协议处理
│
├── playersrv/
│   ├── server.go                  # 游戏业务 RPC 服务组装
│   ├── account_service.go         # PlayerService.Login，负责用户获取或创建
│   ├── command_service.go         # PlayerCommandService 通用命令分发和注册表，已实现
│   ├── request_queue.go           # Dispatch 按 RoleID 分组请求队列、worker pool 和队列满背压，已实现
│   ├── mail_commands.go           # 邮件命令字注册和 Any 载荷适配，已实现
│   ├── mail_handler.go            # 邮件相关协议处理
│   ├── mail_service.go            # 全局邮件读取、状态合并、读/领/删编排，已实现
│   ├── mail_rpc.go                # PlayerMailService gRPC 读取和状态接口适配层，已实现
│   └── event_consumer.go          # Kafka 事件消费并刷新本地缓存，已实现
│
├── adminsrv/
│   ├── server.go                  # 管理后台服务组装
│   ├── mail_admin.go              # GM 创建/审核/发布全局邮件，当前合并在 server.go
│   └── auth.go                    # 管理后台鉴权
│
├── mailrelaysrv/
│   ├── worker.go                  # 扫描 MySQL outbox 并投递 Kafka，已实现
│   └── scheduler.go               # 批量、重试、退避调度
│
└── recovery/
    ├── controller.go              # 故障检测和自动摘除
    ├── watcher.go                 # etcd/metrics/Redis 信号监听
    └── action.go                  # 清路由、摘 LB、告警动作
```

### 4.1 `accountsrv`

登录鉴权服务，负责把外部登录凭证转换成服务端可信身份。

依赖：

```text
data/mysql      # 账号数据，后续接入
data/redis      # 写 DBLoginToken
infra/etcd      # 服务注册
```

不依赖：

```text
playersrv 本地缓存
Kafka 全局邮件事件消费
gatewaysrv SessPool
```

### 4.2 `gatewaysrv`

长连接网关服务，负责 WebSocket 连接、UID 绑定、连接位置记录和路由转发。

依赖：

```text
data/redis      # DBLoginToken、DBGateConn、DBSrvRouter
infra/etcd      # watch playersrv 健康实例列表
app/gatewaysrv     # SessPool、RouteNode、协议转发
```

不依赖：

```text
GORM Model
全局邮件业务规则
GM 发布流程
```

### 4.3 `playersrv`

玩家业务服务，负责处理玩家业务逻辑。全局邮件的高频读取、条件过滤、领取幂等都在这里编排。

依赖：

```text
domain/globalmail   # 全局邮件领域能力
data/mysql          # 玩家状态、领取状态
data/redis          # 二级缓存和版本号
data/localcache     # 本地缓存边界说明
infra/kafka         # 消费 GlobalMailChanged
infra/etcd          # 服务注册
infra/metrics       # 缓存刷新和 outbox 轻量指标
```

### 4.4 `adminsrv`

GM/管理后台服务，负责创建、审核、发布、下线全局邮件。

依赖：

```text
domain/globalmail   # PublisherService
data/mysql          # GORM Repository，写 GlobalMail + Outbox
data/redis          # 更新 GlobalMailVersion 和缓存
infra/etcd          # 服务注册
```

`adminsrv` 不直接逐台调用 `playersrv`，而是通过 outbox + Kafka 通知。

### 4.5 `mailrelaysrv`

事件投递进程，负责把 MySQL outbox 中的 pending 事件投递到 Kafka。

依赖：

```text
domain/globalmail   # OutboxRelay
data/mysql          # OutboxRepository
infra/kafka         # EventPublisher
```

### 4.6 `recovery-controller`

自动化容灾控制器，负责故障摘除、路由清理和告警联动。

依赖：

```text
infra/etcd          # 实例状态和 lease 变化
data/redis          # DBSrvRouter、DBGateConn 清理或检查
infra/lb            # LB 摘除动作，后续接入
infra/metrics       # 指标采集适配，后续接统一告警
```

## 5. 后续目录规划

```text
globalmail/
├── cmd/
├── app/
├── config/
├── domain/
├── data/
│   ├── mysql/
│   ├── redis/
│   ├── localcache/
│   └── sqlschema/
├── infra/
│   ├── kafka/
│   ├── etcd/
│   ├── lb/
│   └── metrics/
└── config/
```

## 6. 依赖方向

依赖只能从外层指向内层：

```text
cmd/*
  -> config
  -> app/*
  -> data/*
  -> infra/*
  -> domain/*

data/mysql
  -> domain/globalmail

data/redis
  -> domain/globalmail

infra/kafka
  -> domain/globalmail

domain/globalmail
  -> Go 标准库
```

禁止：

```text
domain/globalmail -> gorm
domain/globalmail -> kafka client
domain/globalmail -> redis client
domain/globalmail -> etcd client
```

这样可以保证领域层长期稳定，DB 和缓存实现集中在 `data/`，基础设施治理能力集中在 `infra/`。

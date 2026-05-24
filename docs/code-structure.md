# 代码目录结构

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
│   ├── server-architecture.md
│   ├── runtime-flows.md
│   ├── deployment-plan.md
│   ├── requirements-design.md
│   └── code-structure.md
│
├── api/
│   └── rpc/
│       ├── login.proto            # AccService/GameService 登录协议定义
│       ├── gate.proto             # GateService 路由查询和清理协议定义
│       ├── command.proto          # GameCommandService 和 CommandID 命令字定义
│       ├── mail.proto             # MailService 全局邮件读取协议定义
│       ├── *.pb.go                # protoc-gen-go 生成的请求/回包结构
│       └── *_grpc.pb.go           # protoc-gen-go-grpc 生成的 gRPC service
│
├── domain/
│   └── globalmail/                # 全局邮件领域层
│       ├── types.go               # 领域模型和值对象
│       ├── ports.go               # MySQL/Redis/Kafka/outbox 抽象接口
│       ├── publisher.go           # 发布全局邮件应用服务
│       ├── outbox.go              # Outbox Relay 应用逻辑
│       ├── cache.go               # gamesrv 本地缓存骨架和条件过滤
│       └── *_test.go              # 领域层单元测试
│
├── data/
│   ├── README.md                  # data 层说明
│   ├── mysql/                     # MySQL + GORM 实现
│   │   ├── models.go              # GORM Model 定义
│   │   ├── mapper.go              # GORM Model 和领域模型转换
│   │   └── repository.go          # Repository / OutboxRepository 实现
│   ├── redis/                     # Redis 二级缓存实现
│   │   ├── client.go              # Redis client 初始化
│   │   ├── repository.go          # CacheRepository 实现
│   │   └── routing.go             # DBLoginToken、DBGateConn、DBSrvRouter 实现
│   ├── localcache/                # gamesrv 本地缓存实现，后续下沉
│   └── sqlschema/                 # 建表 SQL 常量
│
├── infra/
│   ├── kafka/                     # Kafka 事件总线适配
│   │   ├── producer.go            # EventPublisher 实现
│   │   └── consumer.go            # GlobalMailChanged 消费者，已实现
│   └── etcd/                      # etcd 服务注册和发现适配
│       ├── client.go              # etcd client 初始化
│       ├── registry.go            # 服务注册、状态更新和摘除
│       └── discovery.go           # 服务发现和 watch
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

`api/rpc/` 是客户端和服务端、服务和服务之间的协议契约层。所有 gRPC service、请求、回包和命令字都先在 `.proto` 中定义，再通过 `protoc-gen-go` 和 `protoc-gen-go-grpc` 生成 Go 代码。

当前职责：

```text
login.proto:
    AccService/Login
    GameService/Login

gate.proto:
    GateService/ResolveRoute
    GateService/ClearRoute

command.proto:
    CommandID
    GameCommandService/Dispatch
    GameCommandService/ListCommands

mail.proto:
    MailService/ListGlobalMails
    MailService/MarkGlobalMailRead
    MailService/ClaimGlobalMail
    MailService/DeleteGlobalMail
```

使用原则：

- 不在业务代码中手写命令字常量，新增命令先扩展 `CommandID`。
- 不在 `app/*` 中自定义重复的请求/回包结构，优先使用 protobuf 生成结构。
- `gatesrv` 只解析 `CommandID` 和透传 protobuf `Any` 载荷，不解析具体业务字段。
- `gamesrv` 通过 `CommandRegistry` 把 `CommandID` 映射到具体 handler。

### 3.1 `domain/`

领域层只表达业务规则和应用流程。

负责：

- 定义 `GlobalMail`、`UserGlobalMailState`、`OutboxEvent` 等领域模型。
- 定义 `MailRepository`、`CacheRepository`、`EventPublisher`、`OutboxRepository` 接口。
- 实现全局邮件发布流程：业务数据 + outbox + Redis version。
- 实现 `OutboxRelay`：从 outbox 读取事件并发布到 Kafka 抽象接口。
- 实现 `gamesrv` 本地缓存快照、版本刷新和条件过滤骨架。

不负责：

- 不直接 import GORM。
- 不直接 import Redis/Kafka/etcd SDK。
- 不关心数据库连接、topic 配置、服务注册等部署细节。

### 3.2 `data/`

`data/` 统一承载数据库和缓存相关实现。

当前职责：

- `data/mysql`：使用 GORM 实现 MySQL repository。
- `data/sqlschema`：保存建表 SQL 常量。
- `data/redis`：实现 Redis 二级缓存。
- `data/localcache`：后续承载 `gamesrv` 本地缓存实现。

`data/mysql` 负责：

- 定义 GORM Model。
- 执行 `AutoMigrate`。
- 在同一个 MySQL 事务里写 `GlobalMail`、`GlobalMailCondition`、`GlobalMailOutboxEvent`。
- 查询已发布全局邮件并转换为领域模型。
- 保存玩家全局邮件状态。
- 扫描 pending outbox，并标记 published 或重试。

`data/redis` 负责：

- `GlobalMailVersion`
- `GlobalMail:{globalMailId}`
- `GlobalMailIndex`
- `GlobalMailActiveIndex`
- `GlobalMailByServer:{serverID}`
- `MailUserProfile:{RoleID}`
- 缓存重建锁和 TTL 抖动
- `DBLoginToken`
- `DBGateConn`
- `DBSrvRouter`

### 3.3 `infra/`

`infra/` 放非 DB/缓存类基础设施适配。

当前职责：

- `infra/kafka`：Kafka producer、consumer、consumer group 管理。
- `infra/etcd`：服务注册、lease 续租、服务发现和 watch。

规划职责：

- `infra/lb`：LB 摘除和 drain 操作。
- `infra/metrics`：指标采集和告警事件。

Kafka、etcd 不放在 `data/`，因为它们不是业务数据存取层，而是事件总线和服务治理基础设施。

### 3.4 `app/`

`app/` 是各服务的应用层编排，负责把协议层、领域层、数据层和基础设施适配层组装成服务能力。

当前重点：

```text
app/accsrv:
    实现 AccService.Login。
    登录时调用 gamesrv GameService.Login 获取或创建用户，再写 DBLoginToken。

app/gatesrv:
    实现 GateService 路由查询和清理。
    CommandForwarder 解析 CommandRequest，按 UID Resolve 到 gamesrv，再调用 GameCommandService.Dispatch。

app/gamesrv:
    实现 GameService.Login。
    实现 GameCommandService 和 CommandRegistry。
    注册 CommandID -> handler。
    实现 MailService 和全局邮件命令 handler。
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
├── accsrv/
│   └── main.go                    # 登录鉴权服务启动入口，已实现 token 骨架
├── gatesrv/
│   └── main.go                    # 长连接网关服务启动入口，已实现路由骨架
├── gamesrv/
│   └── main.go                    # 玩家业务服务启动入口，已实现事件消费骨架
├── mgrsrv/
│   └── main.go                    # GM/管理后台服务启动入口，已实现骨架
├── outboxrelay/
│   └── main.go                    # Outbox Relay 事件投递进程，已实现
└── recovery-controller/
    └── main.go                    # 自动化容灾控制器

app/
├── bootstrap/
│   └── config.go                  # YAML 配置路径和退出信号公共工具，已实现
│
├── accsrv/
│   ├── server.go                  # gRPC AccService 组装，已实现 Login
│   ├── handler.go                 # 登录接口处理
│   └── service.go                 # 登录业务编排，转发 gamesrv 后写 DBLoginToken
│
├── gatesrv/
│   ├── server.go                  # gRPC GateService 路由查询/清理适配层，已实现
│   ├── command_forwarder.go       # 解析 CommandRequest 后转发到目标 gamesrv Dispatch，已实现
│   ├── session.go                 # SessPool 路由缓存，已实现
│   ├── router.go                  # UID 到 gamesrv 的一致性哈希路由，已实现
│   ├── route_service.go           # session route、Redis route、etcd list 串联，已实现
│   ├── etcd_provider.go           # etcd ready gamesrv 转路由节点，已实现
│   └── handler.go                 # 客户端协议处理
│
├── gamesrv/
│   ├── server.go                  # 游戏业务 RPC 服务组装
│   ├── account_service.go         # GameService.Login，负责用户获取或创建
│   ├── command_service.go         # GameCommandService 通用命令分发和注册表，已实现
│   ├── mail_commands.go           # 邮件命令字注册和 Any 载荷适配，已实现
│   ├── mail_handler.go            # 邮件相关协议处理
│   ├── mail_service.go            # 全局邮件读取、状态合并、读/领/删编排，已实现
│   ├── mail_rpc.go                # MailService gRPC 读取和状态接口适配层，已实现
│   └── event_consumer.go          # Kafka 事件消费并刷新本地缓存，已实现
│
├── mgrsrv/
│   ├── server.go                  # 管理后台服务组装
│   ├── mail_admin.go              # GM 创建/审核/发布全局邮件，当前合并在 server.go
│   └── auth.go                    # 管理后台鉴权
│
├── outboxrelay/
│   ├── worker.go                  # 扫描 MySQL outbox 并投递 Kafka，已实现
│   └── scheduler.go               # 批量、重试、退避调度
│
└── recovery/
    ├── controller.go              # 故障检测和自动摘除
    ├── watcher.go                 # etcd/metrics/Redis 信号监听
    └── action.go                  # 清路由、摘 LB、告警动作
```

### 4.1 `accsrv`

登录鉴权服务，负责把外部登录凭证转换成服务端可信身份。

依赖：

```text
data/mysql      # 账号数据，后续接入
data/redis      # 写 DBLoginToken
infra/etcd      # 服务注册
```

不依赖：

```text
gamesrv 本地缓存
Kafka 全局邮件事件消费
gatesrv SessPool
```

### 4.2 `gatesrv`

长连接网关服务，负责 WebSocket 连接、UID 绑定、连接位置记录和路由转发。

依赖：

```text
data/redis      # DBLoginToken、DBGateConn、DBSrvRouter
infra/etcd      # watch gamesrv 健康实例列表
app/gatesrv     # SessPool、RouteNode、协议转发
```

不依赖：

```text
GORM Model
全局邮件业务规则
GM 发布流程
```

### 4.3 `gamesrv`

玩家业务服务，负责处理玩家业务逻辑。全局邮件的高频读取、条件过滤、领取幂等都在这里编排。

依赖：

```text
domain/globalmail   # 全局邮件领域能力
data/mysql          # 玩家状态、领取状态
data/redis          # 二级缓存和版本号
data/localcache     # 本地缓存实现，后续下沉
infra/kafka         # 消费 GlobalMailChanged
infra/etcd          # 服务注册
```

### 4.4 `mgrsrv`

GM/管理后台服务，负责创建、审核、发布、下线全局邮件。

依赖：

```text
domain/globalmail   # PublisherService
data/mysql          # GORM Repository，写 GlobalMail + Outbox
data/redis          # 更新 GlobalMailVersion 和缓存
infra/etcd          # 服务注册
```

`mgrsrv` 不直接逐台调用 `gamesrv`，而是通过 outbox + Kafka 通知。

### 4.5 `outboxrelay`

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
infra/metrics       # 指标和告警，后续接入
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

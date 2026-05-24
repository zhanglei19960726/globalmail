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
│   ├── redis/                     # Redis 二级缓存实现，后续接入
│   ├── localcache/                # gamesrv 本地缓存实现，后续下沉
│   └── sqlschema/                 # 建表 SQL 常量
│
├── config/
│   ├── config.go                  # 第三方组件配置结构和环境变量加载
│   └── config_test.go             # 配置加载测试
│
├── go.mod
└── README.md
```

## 3. 分层职责

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
- `data/redis`：后续实现 Redis 二级缓存。
- `data/localcache`：后续承载 `gamesrv` 本地缓存实现。

`data/mysql` 负责：

- 定义 GORM Model。
- 执行 `AutoMigrate`。
- 在同一个 MySQL 事务里写 `GlobalMail`、`GlobalMailCondition`、`GlobalMailOutboxEvent`。
- 查询已发布全局邮件并转换为领域模型。
- 保存玩家全局邮件状态。
- 扫描 pending outbox，并标记 published 或重试。

`data/redis` 后续负责：

- `GlobalMailVersion`
- `GlobalMail:{globalMailId}`
- `GlobalMailIndex`
- `GlobalMailActiveIndex`
- `GlobalMailByServer:{serverID}`
- `MailUserProfile:{RoleID}`
- 缓存重建锁和 TTL 抖动

### 3.3 `infra/`

`infra/` 放非 DB/缓存类基础设施适配。

规划职责：

- `infra/kafka`：Kafka producer、consumer、consumer group 管理。
- `infra/etcd`：服务注册、lease 续租、服务发现和 watch。
- `infra/lb`：LB 摘除和 drain 操作。
- `infra/metrics`：指标采集和告警事件。

Kafka、etcd 不放在 `data/`，因为它们不是业务数据存取层，而是事件总线和服务治理基础设施。

### 3.4 `config/`

`config/` 是统一配置模块，负责管理服务自身和第三方组件配置。

当前支持：

```text
Service:
    SERVICE_NAME
    SERVICE_INSTANCE_ID
    SERVICE_ENV
    SERVICE_LISTEN_ADDR
    SERVICE_PUBLIC_ADDR

MySQL:
    MYSQL_DSN
    MYSQL_MAX_OPEN_CONNS
    MYSQL_MAX_IDLE_CONNS
    MYSQL_CONN_MAX_LIFETIME
    MYSQL_AUTO_MIGRATE

Redis:
    REDIS_ADDRS
    REDIS_USERNAME
    REDIS_PASSWORD
    REDIS_DB
    REDIS_KEY_PREFIX
    REDIS_DIAL_TIMEOUT
    REDIS_READ_TIMEOUT
    REDIS_WRITE_TIMEOUT

Kafka:
    KAFKA_BROKERS
    KAFKA_TOPIC_PREFIX
    KAFKA_CONSUMER_GROUP

etcd:
    ETCD_ENDPOINTS
    ETCD_USERNAME
    ETCD_PASSWORD
    ETCD_DIAL_TIMEOUT
    ETCD_LEASE_TTL
    ETCD_KEEPALIVE_INTERVAL
    ETCD_SERVICE_KEY_PREFIX
```

使用原则：

- `cmd/*/main.go` 只从 `config.LoadFromEnv()` 获取配置。
- `app/*` 接收已经组装好的依赖，不直接读取环境变量。
- `domain/*` 不依赖 `config`，避免业务规则和部署环境耦合。
- 第三方组件连接初始化由 `data/*` 或 `infra/*` 使用配置完成。

## 4. 服务目录划分

服务入口按进程划分，每个服务拥有独立启动入口，但复用同一套领域层、数据层和基础设施适配层。

推荐目录：

```text
cmd/
├── accsrv/
│   └── main.go                    # 登录鉴权服务启动入口
├── gatesrv/
│   └── main.go                    # 长连接网关服务启动入口
├── gamesrv/
│   └── main.go                    # 玩家业务服务启动入口
├── mgrsrv/
│   └── main.go                    # GM/管理后台服务启动入口
├── outboxrelay/
│   └── main.go                    # Outbox Relay 事件投递进程
└── recovery-controller/
    └── main.go                    # 自动化容灾控制器

app/
├── accsrv/
│   ├── server.go                  # HTTP/RPC 服务组装
│   ├── handler.go                 # 登录接口处理
│   └── service.go                 # 登录业务编排
│
├── gatesrv/
│   ├── server.go                  # WebSocket 服务组装
│   ├── session.go                 # SessPool 和连接管理
│   ├── router.go                  # UID 到 gamesrv 路由
│   └── handler.go                 # 客户端协议处理
│
├── gamesrv/
│   ├── server.go                  # 游戏业务 RPC 服务组装
│   ├── mail_handler.go            # 邮件相关协议处理
│   ├── mail_service.go            # 全局邮件读取/领取编排
│   └── event_consumer.go          # Kafka 事件消费并刷新本地缓存
│
├── mgrsrv/
│   ├── server.go                  # 管理后台服务组装
│   ├── mail_admin.go              # GM 创建/审核/发布全局邮件
│   └── auth.go                    # 管理后台鉴权
│
├── outboxrelay/
│   ├── worker.go                  # 扫描 MySQL outbox 并投递 Kafka
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

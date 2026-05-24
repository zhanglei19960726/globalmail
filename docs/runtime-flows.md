# 运行时流程设计

## 1. 目标

本文描述服务器运行时的关键流程，包括登录连接、多 gate 连接管理、`gatesrv` 到 `gamesrv` 的路由，以及 gate/game 故障恢复。

架构分层和拆分原因见 `server-architecture.md`，etcd 注册发现和部署运维见 `deployment-plan.md`。

## 2. 登录和连接流程

```mermaid
flowchart TD
    clientLogin[客户端HTTP POST /login_Protobuf LoginRequest] --> auth[第三方登录校验]
    auth --> forward[accsrv gRPC调用gamesrv.GameService/Login]
    forward --> account[gamesrv获取或创建账号和玩家]
    account --> identity[返回UID_RoleID_ServerID]
    identity --> token[accsrv创建LoginToken]
    token --> loginToken[写DBLoginToken到Redis]
    loginToken --> sessionKey[返回SessionKey给客户端]

    sessionKey --> ws[客户端连接gate_ws_token]
    ws --> gateAuth[gatesrv查Redis_DBLoginToken]
    gateAuth --> uid[取出UID_RoleID_ServerID]
    uid --> gateConn[写DBGateConn到Redis]
    gateConn --> sess[本机SessPool绑定UID和Client]
    sess --> packet[后续业务包]
    packet --> overwrite[gatesrv用session中的UID覆盖pktHeadUID]
    overwrite --> forward[转发gamesrv或mgrsrv]
```

关键点：

- `accsrv` 登录成功后创建 `DBLoginToken`，客户端拿到的是 `SessionKey`。
- 用户注册、玩家初始化和用户资料归属 `gamesrv`；`accsrv` 对外通过 HTTP 接收登录请求，对内通过 gRPC 转发登录请求到 `gamesrv`。
- 登录请求和响应由 `api/rpc/login.proto` 的 `LoginRequest/LoginResponse` 定义；HTTP 层可使用 protojson 或 `application/x-protobuf` 二进制 protobuf。
- 客户端连接 `/gate/ws?token=xxx`。
- `gatesrv` 用 token 查 Redis 中的 `DBLoginToken`，拿到 `UID`、`RoleID`、`ServerID`。
- 后续客户端业务包中的 UID 不可信，gate 会用 session 中的 UID 覆盖包头。

## 3. 多 gatesrv 连接管理

玩家连接在哪台 gate，由 Redis 中的 `DBGateConn` 记录：

```text
DBGateConn:{UID}
- UID
- GatewayAddr
- ClientIP
- ConnID
- ConnTime
- LastActiveTime
- DeviceID
- ExpireAt
```

`DBGateConn` 的写入时机：

- 玩家通过 token 连接 `gatesrv` 并鉴权成功。
- 同一 UID 重连到新 `gatesrv` 时覆盖旧记录。
- 连接保持期间按心跳或续期任务刷新 TTL。

单台 `gatesrv` 的内存连接池负责快速找到和管理本机连接：

```text
ConnectionPool:
    key   : UID
    index : ConnID -> UID
    value : ClientConnection / Session / LastHeartbeatAt / ExpireAt / route cache
```

如果其他服务需要给玩家推送消息，先查 `DBGateConn` 获取玩家当前 gate 地址，再由目标 gate 从本机 `SessPool` 找连接下发。

连接可用性由三层保证：

```text
客户端:
    定时发送 HeartbeatRequest。
    超时未收到 HeartbeatResponse 时主动重连 LB。

gatesrv:
    ConnectionPool 保存真实连接对象和 UID、RoleID、ServerID、ConnID、LastHeartbeatAt、ExpireAt。
    每次心跳刷新本机 LastHeartbeatAt 和 ExpireAt。
    后台扫描过期 session，关闭真实连接并清理本机状态。

Redis:
    DBGateConn:{UID} 记录玩家当前 gate 位置。
    连接建立时写入。
    心跳不每次写 Redis，而是按 gate_conn_renew_interval 节流续期。
    Redis TTL 兜底清理异常断开的连接位置。
```

心跳流程：

```mermaid
sequenceDiagram
    participant C as Client
    participant G as gatesrv
    participant S as ConnectionPool
    participant R as Redis

    C->>G: GateService.Heartbeat(uid, conn_id, seq)
    G->>S: 校验 UID + ConnID
    S-->>G: session
    G->>S: 更新 LastHeartbeatAt 和 ExpireAt
    alt 达到续期间隔
        G->>R: Set DBGateConn:{UID} with TTL
    end
    G-->>C: HeartbeatResponse(seq, expire_at)
```

推荐默认值：

```text
heartbeat_interval: 10s
session_ttl: 90s
gate_conn_renew_interval: 30s
```

## 4. gatesrv 到 gamesrv 的路由

gate 转发玩家业务包时，会尽量把同一个 UID 固定到同一台 gamesrv：

```mermaid
flowchart TD
    req[玩家业务包到gate] --> memoryRoute{session内存有gamesrv地址}
    memoryRoute -->|有| sendOld[SendPktToAddr发到原gamesrv]
    sendOld --> refresh[定期刷新DBSrvRouter]
    memoryRoute -->|没有或失效| routeNode[RouteNode按UID一致性哈希选择gamesrv]
    routeNode --> sendNew[发送到新gamesrv]
    sendNew --> saveMemory[写session内存route]
    saveMemory --> saveDB[写DBSrvRouter到Redis]
```

`RouteNode` 不直接写死 `gamesrv` 地址，而是读取由 etcd watch 维护的本地健康实例列表，再按 UID 做一致性哈希选择目标节点。

一致性哈希规则：

```text
1. gatesrv watch etcd，维护 ready gamesrv 实例列表。
2. 每个 gamesrv 按 instance_id 生成多个虚拟节点。
3. UID 计算 hash 后在哈希环上顺时针找到第一个虚拟节点。
4. 命中的虚拟节点对应的真实 gamesrv 即为目标节点。
5. gamesrv 扩缩容时，只迁移落在相邻区间内的一部分 UID。
```

路由优先级：

```text
session route
    -> Redis DBSrvRouter:{UID}
    -> ConsistentHash(UID, healthyGamesrvList)
```

当前代码实现：

```text
cmd/gatesrv/main.go:
    组装 Redis route store、etcd gamesrv provider、RouteService，注册 gatesrv 实例，并启动 gRPC GateService。

app/gatesrv/server.go:
    实现 GateService.ResolveRoute 和 GateService.ClearRoute，后续 WebSocket 协议层复用 RouteService。

app/gatesrv/session.go:
    维护 UID -> gamesrv 的本机 session route cache。

app/gatesrv/router.go:
    使用一致性哈希按 UID 选择 gamesrv。

app/gatesrv/route_service.go:
    串联 session route、Redis DBSrvRouter、etcd ready gamesrv 列表和一致性哈希。

data/redis/routing.go:
    实现 DBLoginToken、DBGateConn、DBSrvRouter 的 Redis 读写、TTL 和删除。
```

## 5. 客户端命令转发流程

客户端业务包进入 `gatesrv` 后，`gatesrv` 不直接处理业务命令，只做命令解析、身份覆盖、路由和转发。

```mermaid
sequenceDiagram
    participant C as Client
    participant G as gatesrv
    participant R as RouteService
    participant GS as gamesrv
    participant CR as CommandRegistry
    participant H as Handler

    C->>G: CommandRequest(CommandID, seq, payload)
    G->>G: 使用 session 覆盖 UID/RoleID/ServerID
    G->>R: Resolve(UID)
    R-->>G: gamesrv grpc_addr
    G->>GS: GameCommandService.Dispatch(CommandRequest)
    GS->>CR: handlers[CommandID]
    CR->>H: handler(ctx, request)
    H-->>CR: protobuf Any response
    CR-->>GS: CommandResponse
    GS-->>G: CommandResponse
    G-->>C: 回包
```

职责边界：

```text
gatesrv:
    解析 CommandID
    覆盖可信身份
    RouteService.Resolve(uid)
    转发到目标 gamesrv Dispatch
    转发失败时 Clear(uid)

gamesrv:
    CommandRegistry 注册 CommandID -> handler
    Dispatch 根据 CommandID 查找 handler
    handler 解析 protobuf Any 载荷
    调用具体业务服务
```

失败处理：

```text
1. gatesrv 调用目标 gamesrv 失败。
2. gatesrv 调用 RouteService.Clear(uid)。
3. 清理本机 session route。
4. 删除 Redis DBSrvRouter:{UID}。
5. 后续请求重新 Resolve(uid)，选择健康 gamesrv。
```

## 6. gamesrv 全局邮件刷新流程

Kafka 消费链路：

```text
Kafka rh.global-mail-events
    -> infra/kafka.GlobalMailConsumer
    -> cmd/gamesrv/main.go 组装消费者和 MailService
    -> app/gamesrv.EventConsumer
    -> MailService.ForceRefreshCache
    -> domain/globalmail.LocalCache
```

邮件读取链路：

```text
客户端或 gatesrv gRPC MailService.ListGlobalMails
    -> api/rpc/mail.proto
    -> app/gamesrv.MailRPCServer
    -> MailService.ListGlobalMails
    -> LocalCache.VisibleMails
    -> MailRepository.GetUserStates
    -> 合并 unread/read/claimed/deleted 状态
    -> 返回 protobuf GlobalMailItem 列表
```

邮件状态写入链路：

```text
MailService.MarkGlobalMailRead
MailService.ClaimGlobalMail
MailService.DeleteGlobalMail
    -> api/rpc/mail.proto
    -> app/gamesrv.MailRPCServer
    -> MailService 重新校验可见性
    -> MailRepository.GetUserStates
    -> MailRepository.SaveUserState
    -> MySQL UserGlobalMailState
```

领取接口当前完成状态幂等合并，奖励实际发放后续接入独立奖励服务或背包服务时再放到 `ClaimGlobalMail` 的强校验流程内。

命令字分发链路：

```text
客户端业务包
    -> gatesrv 解析出 api/rpc/command.proto 中的 CommandID
    -> app/gatesrv.CommandForwarder
    -> RouteService.Resolve(uid) 找到目标 gamesrv
    -> 调用目标 gamesrv 的 GameCommandService.Dispatch
    -> GameCommandService.Dispatch
    -> CommandRequestQueue 入队
    -> worker pool 消费
    -> app/gamesrv.CommandRegistry
    -> 按 CommandID 查找已注册 handler
    -> app/gamesrv.mail_commands.go
    -> MailRPCServer 对应接口
```

如果 `gatesrv` 转发到目标 `gamesrv` 失败，会调用 `RouteService.Clear(uid)` 清理本机 session route 和 Redis `DBSrvRouter:{UID}`，后续请求重新按 UID 选服。

`gamesrv` 请求队列规则：

```text
正常:
    Dispatch 请求进入有界队列。
    worker 从队列取请求并调用 CommandRegistry。
    gRPC 请求等待 worker 返回 CommandResponse。

超时:
    game.request_timeout 从请求进入队列开始计时。
    包含排队等待和 handler 执行时间。
    超时后返回 CommandResponse(code=504, message="command request timeout")。
    handler 必须使用 context 调用数据库、Redis、下游 RPC，才能真正停止底层工作。

队列满:
    不再入队，立即返回 CommandResponse(code=429, message="command queue full")。

请求取消:
    客户端或上游 context 取消时，排队等待会尽快返回 context 错误。
```

默认参数：

```text
game.request_queue_workers: 4
game.request_queue_capacity: 1024
game.request_timeout: 3s
```

当前已注册命令：

```text
COMMAND_ID_MAIL_LIST_GLOBAL = 1001
COMMAND_ID_MAIL_MARK_READ   = 1002
COMMAND_ID_MAIL_CLAIM       = 1003
COMMAND_ID_MAIL_DELETE      = 1004
```

## 7. accsrv 登录 Token 流程

当前代码骨架：

```text
客户端 HTTP POST /login
    -> api/rpc/login.proto LoginRequest/LoginResponse
    -> app/accsrv.Server.Handler
    -> app/accsrv.Service.Login
    -> gRPC GameService.Login
    -> app/gamesrv.AccountService.GetOrCreateUser
    -> accsrv 创建 LoginToken
    -> data/redis.SetLoginToken
    -> Redis DBLoginToken:{token}
```

`DBLoginToken` 保存 `uid`、`role_id`、`server_id` 和过期时间，后续 `gatesrv` 建立长连接时用于校验和绑定连接。`accsrv` 不直接创建用户，只负责鉴权入口、转发 `gamesrv`、生成短期登录 token。

`DBSrvRouter` 用于跨进程共享玩家后端路由：

```text
DBSrvRouter:{UID}
- UID
- List:
  - SrvName
  - FrpcAddr
  - GrpcAddr
```

内存 route 和 `DBSrvRouter` 的区别：

```text
session内存route:
    位置: 当前gatesrv进程内
    生命周期: 当前连接期间
    优点: 快
    缺点: gate挂了就丢，其他进程看不到

DBSrvRouter:
    位置: Redis
    生命周期: TTL，gate定期刷新
    优点: 跨gate和跨服务共享
    缺点: 比内存慢，需要清理和续期
```

路由变化策略：

- 已有玩家继续走原 `gamesrv`，不因新增节点强制迁移。
- 新玩家或重建路由的玩家可以分配到新 `gamesrv`。
- 原 `gamesrv` 下线或发送失败时，gate 清理内存 route 和 `DBSrvRouter` 后重新 `RouteNode`。

## 8. gatesrv 容灾流程

```mermaid
flowchart TD
    oldGate[玩家连接gatesrv1] --> crash[gatesrv1宕机]
    crash --> disconnect[客户端连接断开]
    disconnect --> reconnect[客户端重连]
    reconnect --> newGate[LB分配到gatesrv2]
    newGate --> auth[查DBLoginToken恢复UID]
    auth --> bind[重新绑定session]
    bind --> overwrite[覆盖DBGateConn]
```

gate 容灾依赖：

- `DBLoginToken` 恢复身份。
- `DBGateConn` 覆盖玩家当前 gate 位置。
- TTL 清理旧连接位置。
- 客户端重连机制。

自动化动作：

```text
1. liveness 或 LB 健康检查发现 gatesrv 异常。
2. LB 自动摘除异常 gatesrv，不再分配新连接。
3. etcd lease 过期后删除该 gatesrv 实例 key。
4. recovery-controller 记录故障事件并触发告警。
5. 客户端心跳超时后自动重连 LB。
6. 新 gatesrv 查 DBLoginToken 恢复身份，并覆盖写 DBGateConn。
7. 旧 DBGateConn 不需要强制同步清理，依赖 TTL 收敛。
```

## 9. gamesrv 容灾流程

```mermaid
flowchart TD
    route[gate缓存UID到gamesrv1] --> gameCrash[gamesrv1宕机]
    gameCrash --> nextReq[玩家下次请求]
    nextReq --> checkNode[CheckNodeTCPAddr或SendPktToAddr失败]
    checkNode --> clear[清理内存route和DBSrvRouter]
    clear --> reroute[重新RouteNode]
    reroute --> game2[路由到gamesrv2]
```

game 容灾依赖：

- 节点健康检查。
- 发送失败后重新路由。
- `DBSrvRouter` TTL。
- 业务层幂等，处理“请求已执行但回包丢失”的情况。

自动化动作：

```text
1. gamesrv liveness/readiness 失败，或 etcd lease 过期。
2. etcd 删除实例 key，或者状态更新为 offline。
3. gatesrv 通过 etcd watch 更新本地健康 gamesrv 列表。
4. gate 发送请求失败时，清理 session 内存 route。
5. gate 删除或覆盖 Redis 中的 DBSrvRouter。
6. gate 重新 RouteNode 选择健康 gamesrv。
7. recovery-controller 可按失败计数批量标记旧路由失效，并触发告警。
```

## 10. recovery-controller 自动化流程

`recovery-controller` 是旁路控制器，不处理玩家请求，只监听状态并执行自动化治理动作。

```mermaid
flowchart TD
    watch[监听etcd_metrics_Redis] --> detect{发现异常实例或高失败率}
    detect -->|gamesrv异常| markGame[标记gamesrv_draining_or_offline]
    markGame --> removeGame[从etcd健康列表摘除]
    removeGame --> clearRoute[清理或标记DBSrvRouter失效]
    clearRoute --> notifyGate[通知gatesrv刷新服务列表]
    notifyGate --> alertGame[告警并记录事件]

    detect -->|gatesrv异常| markGate[标记gatesrv_offline]
    markGate --> removeGate[从LB和etcd摘除]
    removeGate --> waitReconnect[等待客户端自动重连]
    waitReconnect --> alertGate[告警并记录事件]
```

自动化边界：

- controller 不直接迁移 WebSocket 连接，`gatesrv` 故障依赖客户端重连。
- controller 不绕过业务幂等，`gamesrv` 故障后的重试仍要由业务层防重复执行。
- controller 不替代 Redis/MySQL 强一致写入，关键写路径失败时应返回失败或进入明确的降级流程。

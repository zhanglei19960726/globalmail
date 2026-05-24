# 文档导航

这组文档按“先看架构、再看流程、最后看落地细节”的顺序组织，避免单个文档过长，也方便后续按主题维护。

## 推荐阅读顺序

| 顺序 | 文档 | 适合阅读的内容 |
| --- | --- | --- |
| 1 | `server-architecture.md` | 服务分层、职责边界、总体架构图 |
| 2 | `runtime-flows.md` | 登录、连接、路由、命令分发、故障恢复等运行时链路 |
| 3 | `requirements-design.md` | 全局邮件业务需求、MySQL/Redis/本地缓存/Kafka 设计 |
| 4 | `request-queue-design.md` | `gamesrv` 请求队列、背压、超时和监控设计 |
| 5 | `deployment-plan.md` | 部署拓扑、服务发现、扩缩容、健康检查、容灾 |
| 6 | `code-structure.md` | 代码目录、分层职责、依赖方向 |

## 文档边界

| 主题 | 主文档 | 关联文档 |
| --- | --- | --- |
| 服务拆分 | `server-architecture.md` | `code-structure.md` |
| 登录和连接 | `runtime-flows.md` | `server-architecture.md` |
| UID 路由 | `runtime-flows.md` | `deployment-plan.md` |
| 请求队列 | `request-queue-design.md` | `runtime-flows.md` |
| 全局邮件 | `requirements-design.md` | `runtime-flows.md` |
| 部署运维 | `deployment-plan.md` | `server-architecture.md` |

## 维护原则

- 架构原则写在 `server-architecture.md`，不要在流程文档里重复展开。
- 运行时步骤写在 `runtime-flows.md`，部署细节写在 `deployment-plan.md`。
- 业务表结构和缓存结构写在 `requirements-design.md`。
- 请求队列的返回码、超时、背压和监控统一维护在 `request-queue-design.md`。
- 代码目录变化同步更新 `code-structure.md`。

# RH Server Design Notes

这个仓库用于沉淀 RH 服务器设计说明。

当前文档：

- [服务器架构方案](docs/server-architecture.md)：总体架构图、服务分层作用、拆分原因、职责边界、数据归属和 Kafka 事件层。
- [运行时流程设计](docs/runtime-flows.md)：登录连接、多 gate 管理、gate 到 game 路由、自动化容灾恢复流程。
- [部署方案](docs/deployment-plan.md)：部署拓扑、etcd 服务注册、Kafka 事件总线、LB、健康检查、扩缩容、自动化容灾和上线流程。
- [需求设计方案](docs/requirements-design.md)：全局邮件需求、MySQL + Redis + 本地缓存、Kafka 通知、条件过滤和领取幂等。
- [代码目录结构](docs/code-structure.md)：Go 代码分层、GORM 适配层、后续 Redis/Kafka/etcd 目录规划。


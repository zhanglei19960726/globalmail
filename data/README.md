# Data Layer

`data` 目录统一放数据库、缓存、数据访问和缓存重建相关实现。

当前目录：

```text
data/
├── mysql/        # MySQL + GORM repository
├── sqlschema/    # 建表 SQL 常量
├── redis/        # Redis 二级缓存、路由缓存、幂等缓存和重建锁
└── localcache/   # playersrv 本地缓存说明；当前实现仍在 domain/globalmail
```

领域层只依赖接口，`data` 层负责连接具体存储和缓存中间件。MySQL repository 同时保存奖励账本和 `playersrv` 背包发放明细，用同一个发放流水避免重复发奖。

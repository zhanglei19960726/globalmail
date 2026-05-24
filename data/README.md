# Data Layer

`data` 目录统一放数据库、缓存、数据访问和缓存重建相关实现。

当前目录：

```text
data/
├── mysql/        # MySQL + GORM repository
├── sqlschema/    # 建表 SQL 常量
├── redis/        # Redis 二级缓存实现，后续接入
└── localcache/   # gamesrv 本地缓存适配，后续从领域骨架中下沉
```

领域层只依赖接口，`data` 层负责连接具体存储和缓存中间件。

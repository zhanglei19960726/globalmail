# Local Cache

该目录用于承载 `gamesrv` 进程内本地缓存实现。

当前本地缓存实现仍在 `domain/globalmail` 中，并已接入真实 `gamesrv`：

- `LocalCache`
- 缓存快照原子替换
- Kafka 事件触发刷新
- 定时版本兜底刷新
- 条件预编译和区服索引
- Redis L2 优先读取，缺失时通过 singleflight 和 Redis rebuild lock 控制回源

后续如果需要进一步分层，可以把 `domain/globalmail.LocalCache` 的基础设施实现下沉到该目录。

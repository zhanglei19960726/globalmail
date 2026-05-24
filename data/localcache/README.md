# Local Cache

该目录用于承载 `gamesrv` 进程内本地缓存实现。

当前本地缓存骨架仍在 `domain/globalmail` 中，后续接入真实 `gamesrv` 时可以下沉到这里：

- `GlobalMailLocalCache`
- 缓存快照原子替换
- Kafka 事件触发刷新
- 定时版本兜底刷新
- 条件预编译和区服索引

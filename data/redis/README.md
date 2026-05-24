# Redis Cache

该目录用于实现 `domain/globalmail.CacheRepository`。

规划职责：

- `GlobalMailVersion`
- `GlobalMail:{globalMailId}`
- `GlobalMailIndex`
- `GlobalMailActiveIndex`
- `GlobalMailByServer:{serverID}`
- `MailUserProfile:{RoleID}`
- 缓存重建锁和 TTL 抖动

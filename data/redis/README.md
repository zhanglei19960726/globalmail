# Redis Cache

该目录用于实现 `domain/globalmail.CacheRepository`。

当前职责：

- `GlobalMailVersion`
- `GlobalMail:{globalMailId}`
- `GlobalMailActiveIndex`
- `GlobalMailByServer:{serverID}`
- `MailUserProfile:{RoleID}`
- `DBLoginToken:{token}`
- `DBGateConn:{connID}`
- `DBSrvRouter:{uid}`
- `CommandIdempotency:{uid:command_id:seq}`
- `GlobalMailRebuildLock:{version}`

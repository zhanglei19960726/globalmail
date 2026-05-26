package globalmail

import (
	"context"
	"time"
)

type MailRepository interface {
	CreateGlobalMailWithOutbox(ctx context.Context, mail GlobalMail, event OutboxEvent) error
	CreateGlobalMailWithOutboxAndIdempotency(ctx context.Context, mail GlobalMail, event OutboxEvent, record PublishIdempotencyRecord) error
	GetPublishIdempotency(ctx context.Context, key string) (PublishIdempotencyRecord, bool, error)
	GetGlobalMailByID(ctx context.Context, mailID int64) (GlobalMail, bool, error)
	GetPublishedGlobalMails(ctx context.Context, now time.Time) ([]GlobalMail, error)
	GetUserStates(ctx context.Context, roleID int64, mailIDs []int64) (map[int64]UserGlobalMailState, error)
	SaveUserState(ctx context.Context, state UserGlobalMailState) error
}

type RewardRepository interface {
	GrantGlobalMailReward(ctx context.Context, grant RewardGrant) (bool, error)
}

type CacheRepository interface {
	GetGlobalMailVersion(ctx context.Context) (int64, error)
	IncrementGlobalMailVersion(ctx context.Context) (int64, error)
	AdvanceGlobalMailVersion(ctx context.Context, targetVersion int64) (int64, error)
	GetGlobalMail(ctx context.Context, mailID int64) (GlobalMail, bool, error)
	SetGlobalMail(ctx context.Context, mail GlobalMail) error
	GetActiveGlobalMailIDs(ctx context.Context, now time.Time) ([]int64, error)
	AddGlobalMailToIndexes(ctx context.Context, mail GlobalMail) error
	GetGlobalMailsByServer(ctx context.Context, serverID int) ([]int64, error)
	SetGlobalMailsByServer(ctx context.Context, serverID int, mailIDs []int64) error
	GetUserProfile(ctx context.Context, roleID int64) (UserProfile, bool, error)
}

type CacheRebuildLocker interface {
	TryAcquireGlobalMailRebuildLock(ctx context.Context, version int64, ttl time.Duration) (release func(context.Context) error, acquired bool, err error)
}

type EventPublisher interface {
	PublishGlobalMailChanged(ctx context.Context, event GlobalMailChangedEvent) error
}

type OutboxRepository interface {
	FetchPending(ctx context.Context, limit int, lockedBy string, lockedUntil time.Time) ([]OutboxEvent, error)
	MarkPublished(ctx context.Context, eventID int64, publishedAt time.Time) error
	MarkFailed(ctx context.Context, eventID int64, nextRetryAt time.Time, cause error, maxRetries int) error
}

type IDGenerator interface {
	NextID() int64
}

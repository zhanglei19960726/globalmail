package globalmail

import (
	"context"
	"time"
)

type MailRepository interface {
	CreateGlobalMailWithOutbox(ctx context.Context, mail GlobalMail, event OutboxEvent) error
	GetPublishedGlobalMails(ctx context.Context, now time.Time) ([]GlobalMail, error)
	GetUserStates(ctx context.Context, roleID int64, mailIDs []int64) (map[int64]UserGlobalMailState, error)
	SaveUserState(ctx context.Context, state UserGlobalMailState) error
}

type CacheRepository interface {
	GetGlobalMailVersion(ctx context.Context) (int64, error)
	IncrementGlobalMailVersion(ctx context.Context) (int64, error)
	GetGlobalMail(ctx context.Context, mailID int64) (GlobalMail, bool, error)
	SetGlobalMail(ctx context.Context, mail GlobalMail) error
	GetGlobalMailsByServer(ctx context.Context, serverID int) ([]int64, error)
	SetGlobalMailsByServer(ctx context.Context, serverID int, mailIDs []int64) error
	GetUserProfile(ctx context.Context, roleID int64) (UserProfile, bool, error)
}

type EventPublisher interface {
	PublishGlobalMailChanged(ctx context.Context, event GlobalMailChangedEvent) error
}

type OutboxRepository interface {
	FetchPending(ctx context.Context, limit int) ([]OutboxEvent, error)
	MarkPublished(ctx context.Context, eventID int64, publishedAt time.Time) error
	MarkFailed(ctx context.Context, eventID int64, nextRetryAt time.Time, cause error) error
}

type IDGenerator interface {
	NextID() int64
}

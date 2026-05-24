package globalmail

import (
	"context"
	"encoding/json"
	"time"
)

type OutboxRelay struct {
	outbox    OutboxRepository
	publisher EventPublisher
	now       func() time.Time
}

func NewOutboxRelay(outbox OutboxRepository, publisher EventPublisher) *OutboxRelay {
	return &OutboxRelay{
		outbox:    outbox,
		publisher: publisher,
		now:       time.Now,
	}
}

func (r *OutboxRelay) Flush(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	events, err := r.outbox.FetchPending(ctx, limit)
	if err != nil {
		return 0, err
	}

	published := 0
	for _, event := range events {
		var changed GlobalMailChangedEvent
		if err := json.Unmarshal(event.Payload, &changed); err != nil {
			_ = r.outbox.MarkFailed(ctx, event.ID, r.nextRetryTime(event.RetryCount), err)
			continue
		}
		if err := r.publisher.PublishGlobalMailChanged(ctx, changed); err != nil {
			_ = r.outbox.MarkFailed(ctx, event.ID, r.nextRetryTime(event.RetryCount), err)
			continue
		}
		if err := r.outbox.MarkPublished(ctx, event.ID, r.now().UTC()); err != nil {
			return published, err
		}
		published++
	}
	return published, nil
}

func (r *OutboxRelay) nextRetryTime(retryCount int) time.Time {
	backoff := time.Duration(retryCount+1) * time.Minute
	if backoff > 30*time.Minute {
		backoff = 30 * time.Minute
	}
	return r.now().UTC().Add(backoff)
}

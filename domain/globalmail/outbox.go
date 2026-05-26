package globalmail

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type OutboxRelay struct {
	outbox     OutboxRepository
	mails      MailRepository
	cache      CacheRepository
	publisher  EventPublisher
	metrics    ConsistencyMetrics
	now        func() time.Time
	workerID   string
	lockTTL    time.Duration
	maxRetries int
}

type OutboxRelayOption func(*OutboxRelay)

func WithOutboxRelayWorkerID(workerID string) OutboxRelayOption {
	return func(r *OutboxRelay) {
		if workerID != "" {
			r.workerID = workerID
		}
	}
}

func WithOutboxRelayLockTTL(lockTTL time.Duration) OutboxRelayOption {
	return func(r *OutboxRelay) {
		if lockTTL > 0 {
			r.lockTTL = lockTTL
		}
	}
}

func WithOutboxRelayMaxRetries(maxRetries int) OutboxRelayOption {
	return func(r *OutboxRelay) {
		if maxRetries > 0 {
			r.maxRetries = maxRetries
		}
	}
}

func WithOutboxRelayMetrics(metrics ConsistencyMetrics) OutboxRelayOption {
	return func(r *OutboxRelay) {
		r.metrics = metrics
	}
}

func NewOutboxRelay(outbox OutboxRepository, mails MailRepository, cache CacheRepository, publisher EventPublisher, opts ...OutboxRelayOption) *OutboxRelay {
	relay := &OutboxRelay{
		outbox:     outbox,
		mails:      mails,
		cache:      cache,
		publisher:  publisher,
		now:        time.Now,
		workerID:   "outbox-relay",
		lockTTL:    5 * time.Minute,
		maxRetries: 5,
	}
	for _, opt := range opts {
		opt(relay)
	}
	return relay
}

func (r *OutboxRelay) Flush(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	now := r.now().UTC()
	events, err := r.outbox.FetchPending(ctx, limit, r.workerID, now.Add(r.lockTTL))
	if err != nil {
		return 0, err
	}

	published := 0
	for _, event := range events {
		start := r.now().UTC()
		var changed GlobalMailChangedEvent
		if err := json.Unmarshal(event.Payload, &changed); err != nil {
			r.incCounter("globalmail_outbox_failures_total", map[string]string{"stage": "decode"})
			_ = r.outbox.MarkFailed(ctx, event.ID, r.nextRetryTime(event.RetryCount), err, r.maxRetries)
			continue
		}
		mail, ok, err := r.mails.GetGlobalMailByID(ctx, event.AggregateID)
		if err != nil {
			r.incCounter("globalmail_outbox_failures_total", map[string]string{"stage": "load_mail"})
			_ = r.outbox.MarkFailed(ctx, event.ID, r.nextRetryTime(event.RetryCount), err, r.maxRetries)
			continue
		}
		if !ok {
			r.incCounter("globalmail_outbox_failures_total", map[string]string{"stage": "load_mail"})
			_ = r.outbox.MarkFailed(ctx, event.ID, r.nextRetryTime(event.RetryCount), fmt.Errorf("global mail %d not found", event.AggregateID), r.maxRetries)
			continue
		}
		if err := r.cache.SetGlobalMail(ctx, mail); err != nil {
			r.incCounter("globalmail_outbox_failures_total", map[string]string{"stage": "redis_projection"})
			_ = r.outbox.MarkFailed(ctx, event.ID, r.nextRetryTime(event.RetryCount), err, r.maxRetries)
			continue
		}
		if err := r.cache.AddGlobalMailToIndexes(ctx, mail); err != nil {
			r.incCounter("globalmail_outbox_failures_total", map[string]string{"stage": "redis_index"})
			_ = r.outbox.MarkFailed(ctx, event.ID, r.nextRetryTime(event.RetryCount), err, r.maxRetries)
			continue
		}
		if _, err := r.cache.AdvanceGlobalMailVersion(ctx, event.Version); err != nil {
			r.incCounter("globalmail_outbox_failures_total", map[string]string{"stage": "version"})
			_ = r.outbox.MarkFailed(ctx, event.ID, r.nextRetryTime(event.RetryCount), err, r.maxRetries)
			continue
		}
		if err := r.publisher.PublishGlobalMailChanged(ctx, changed); err != nil {
			r.incCounter("globalmail_outbox_failures_total", map[string]string{"stage": "publish"})
			_ = r.outbox.MarkFailed(ctx, event.ID, r.nextRetryTime(event.RetryCount), err, r.maxRetries)
			continue
		}
		if err := r.outbox.MarkPublished(ctx, event.ID, r.now().UTC()); err != nil {
			r.incCounter("globalmail_outbox_failures_total", map[string]string{"stage": "mark_published"})
			return published, err
		}
		r.incCounter("globalmail_outbox_published_total", map[string]string{"event_type": event.Type})
		r.observeDuration("globalmail_outbox_event_seconds", r.now().UTC().Sub(start), map[string]string{"event_type": event.Type})
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

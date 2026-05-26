package globalmail

import (
	"context"
	"testing"
	"time"
)

func TestPublisherCreatesMailAndOutbox(t *testing.T) {
	repo := &fakeMailRepo{}
	cache := &fakeCacheRepo{}
	service := NewPublisherService(repo, cache, &fakeIDs{})

	start := time.Now().UTC()
	mail, err := service.Publish(context.Background(), PublishCommand{
		Mail: GlobalMail{
			Title:      "title",
			Content:    "content",
			Sender:     "system",
			Category:   "global",
			StartTime:  start,
			ExpireTime: start.Add(time.Hour),
		},
	})
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	if mail.ID == 0 {
		t.Fatal("expected generated mail id")
	}
	if len(repo.mails) != 1 {
		t.Fatalf("expected 1 mail, got %d", len(repo.mails))
	}
	if len(repo.events) != 1 {
		t.Fatalf("expected 1 outbox event, got %d", len(repo.events))
	}
	if cache.version != 0 {
		t.Fatalf("expected publisher not to advance cache version, got %d", cache.version)
	}
}

func TestPublisherReplaysIdempotentPublish(t *testing.T) {
	repo := &fakeMailRepo{}
	cache := &fakeCacheRepo{}
	service := NewPublisherService(repo, cache, &fakeIDs{})

	start := time.Now().UTC()
	cmd := PublishCommand{
		IdempotencyKey: "publish-1",
		Mail: GlobalMail{
			Title:      "title",
			Content:    "content",
			Sender:     "system",
			Category:   "global",
			StartTime:  start,
			ExpireTime: start.Add(time.Hour),
		},
	}
	first, err := service.Publish(context.Background(), cmd)
	if err != nil {
		t.Fatalf("first publish failed: %v", err)
	}
	second, err := service.Publish(context.Background(), cmd)
	if err != nil {
		t.Fatalf("second publish failed: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("expected replayed mail id %d, got %d", first.ID, second.ID)
	}
	if len(repo.mails) != 1 {
		t.Fatalf("expected one persisted mail, got %d", len(repo.mails))
	}
}

func TestOutboxRelayPublishesPendingEvents(t *testing.T) {
	payload := []byte(`{"event_id":1,"global_mail_id":10,"version":2,"action":"publish","timestamp":"2026-05-24T00:00:00Z"}`)
	mails := &fakeMailRepo{
		mails: []GlobalMail{{
			ID:         10,
			Title:      "title",
			Content:    "content",
			Sender:     "system",
			Category:   "global",
			StartTime:  time.Now().Add(-time.Hour),
			ExpireTime: time.Now().Add(time.Hour),
			Status:     MailStatusPublished,
			Version:    2,
		}},
	}
	outbox := &fakeOutboxRepo{
		pending: []OutboxEvent{{ID: 1, AggregateID: 10, Version: 2, Payload: payload}},
	}
	cache := &fakeCacheRepo{}
	publisher := &fakePublisher{}
	relay := NewOutboxRelay(outbox, mails, cache, publisher)

	count, err := relay.Flush(context.Background(), 100)
	if err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 published event, got %d", count)
	}
	if len(publisher.events) != 1 {
		t.Fatalf("expected publisher called once, got %d", len(publisher.events))
	}
	if len(outbox.sent) != 1 || outbox.sent[0] != 1 {
		t.Fatalf("expected event marked published, got %#v", outbox.sent)
	}
	if cache.version != 2 {
		t.Fatalf("expected cache version advanced to 2, got %d", cache.version)
	}
	if _, ok := cache.mails[10]; !ok {
		t.Fatal("expected relay to write mail projection")
	}
	if len(cache.activeIDs) != 1 || cache.activeIDs[0] != 10 {
		t.Fatalf("expected relay to update active index, got %#v", cache.activeIDs)
	}
}

func TestOutboxRelayMarksMissingMailFailedWithMaxRetries(t *testing.T) {
	payload := []byte(`{"event_id":1,"global_mail_id":10,"version":2,"action":"publish","timestamp":"2026-05-24T00:00:00Z"}`)
	outbox := &fakeOutboxRepo{
		pending: []OutboxEvent{{ID: 1, AggregateID: 10, Version: 2, Payload: payload}},
	}
	relay := NewOutboxRelay(
		outbox,
		&fakeMailRepo{},
		&fakeCacheRepo{},
		&fakePublisher{},
		WithOutboxRelayMaxRetries(3),
	)

	count, err := relay.Flush(context.Background(), 100)
	if err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected no published events, got %d", count)
	}
	if len(outbox.failed) != 1 || outbox.failed[0] != 1 {
		t.Fatalf("expected failed event 1, got %#v", outbox.failed)
	}
	if len(outbox.failRetries) != 1 || outbox.failRetries[0] != 3 {
		t.Fatalf("expected max retries 3, got %#v", outbox.failRetries)
	}
	if len(outbox.failReasons) != 1 || outbox.failReasons[0] == "" {
		t.Fatalf("expected failure reason, got %#v", outbox.failReasons)
	}
}

func TestOutboxRelayClaimsEventsWithWorkerID(t *testing.T) {
	payload := []byte(`{"event_id":1,"global_mail_id":10,"version":2,"action":"publish","timestamp":"2026-05-24T00:00:00Z"}`)
	outbox := &fakeOutboxRepo{
		pending: []OutboxEvent{{ID: 1, AggregateID: 10, Version: 2, Payload: payload}},
	}
	relay := NewOutboxRelay(
		outbox,
		&fakeMailRepo{},
		&fakeCacheRepo{},
		&fakePublisher{},
		WithOutboxRelayWorkerID("relay-a"),
	)

	_, _ = relay.Flush(context.Background(), 100)
	if outbox.lockedBy != "relay-a" {
		t.Fatalf("expected worker id relay-a, got %q", outbox.lockedBy)
	}
	if outbox.lockedUntil.IsZero() {
		t.Fatal("expected lock expiration to be set")
	}
}

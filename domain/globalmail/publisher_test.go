package globalmail

import (
	"context"
	"testing"
	"time"
)

func TestPublisherCreatesMailOutboxAndCacheVersion(t *testing.T) {
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
	if cache.version != 1 {
		t.Fatalf("expected cache version 1, got %d", cache.version)
	}
	if _, ok := cache.mails[mail.ID]; !ok {
		t.Fatal("expected mail cached")
	}
}

func TestOutboxRelayPublishesPendingEvents(t *testing.T) {
	payload := []byte(`{"event_id":1,"global_mail_id":10,"version":2,"action":"publish","timestamp":"2026-05-24T00:00:00Z"}`)
	outbox := &fakeOutboxRepo{
		pending: []OutboxEvent{{ID: 1, Payload: payload}},
	}
	publisher := &fakePublisher{}
	relay := NewOutboxRelay(outbox, publisher)

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
}

package playersrv

import (
	"context"
	"testing"

	"globalmail/domain/globalmail"
)

type fakeGlobalMailReader struct {
	events []globalmail.GlobalMailChangedEvent
}

func (r *fakeGlobalMailReader) ReadGlobalMailChanged(ctx context.Context) (globalmail.GlobalMailChangedEvent, error) {
	if len(r.events) == 0 {
		return globalmail.GlobalMailChangedEvent{}, context.Canceled
	}
	event := r.events[0]
	r.events = r.events[1:]
	return event, nil
}

type fakeCacheRefresher struct {
	version int64
	calls   int
}

func (r *fakeCacheRefresher) CacheVersion() int64 {
	return r.version
}

func (r *fakeCacheRefresher) ForceRefreshCache(context.Context) error {
	r.calls++
	return nil
}

func TestEventConsumerHandleRefreshesCache(t *testing.T) {
	refresher := &fakeCacheRefresher{}
	consumer := NewEventConsumer(&fakeGlobalMailReader{}, refresher)

	err := consumer.Handle(context.Background(), globalmail.GlobalMailChangedEvent{GlobalMailID: 1, Version: 2})
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}
	if refresher.calls != 1 {
		t.Fatalf("expected one refresh, got %d", refresher.calls)
	}
}

func TestEventConsumerRunConsumesUntilContextCancel(t *testing.T) {
	refresher := &fakeCacheRefresher{}
	reader := &fakeGlobalMailReader{
		events: []globalmail.GlobalMailChangedEvent{
			{GlobalMailID: 1, Version: 1},
			{GlobalMailID: 2, Version: 2},
		},
	}
	consumer := NewEventConsumer(reader, refresher)

	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if refresher.calls != 2 {
		t.Fatalf("expected two refreshes, got %d", refresher.calls)
	}
}

func TestEventConsumerIgnoresStaleVersion(t *testing.T) {
	refresher := &fakeCacheRefresher{version: 3}
	consumer := NewEventConsumer(&fakeGlobalMailReader{}, refresher)

	err := consumer.Handle(context.Background(), globalmail.GlobalMailChangedEvent{GlobalMailID: 1, Version: 2})
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}
	if refresher.calls != 0 {
		t.Fatalf("expected stale event ignored, got %d refreshes", refresher.calls)
	}
}

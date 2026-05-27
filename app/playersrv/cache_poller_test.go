package playersrv

import (
	"context"
	"testing"
	"time"
)

type fakePollRefresher struct {
	calls int
}

func (f *fakePollRefresher) RefreshCache(context.Context) error {
	f.calls++
	return nil
}

func TestRunGlobalMailCachePollerRefreshesUntilCancel(t *testing.T) {
	refresher := &fakePollRefresher{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := RunGlobalMailCachePoller(ctx, refresher, time.Millisecond, nil)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if refresher.calls == 0 {
		t.Fatal("expected poller to refresh at least once")
	}
}

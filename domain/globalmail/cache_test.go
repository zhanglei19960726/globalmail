package globalmail

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestLocalCacheFiltersVisibleMails(t *testing.T) {
	now := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	serverValue, _ := json.Marshal([]int{1, 2})
	vipValue, _ := json.Marshal(5)

	repo := &fakeMailRepo{
		mails: []GlobalMail{
			{
				ID:         100,
				Title:      "visible",
				Content:    "content",
				Status:     MailStatusPublished,
				StartTime:  now.Add(-time.Hour),
				ExpireTime: now.Add(time.Hour),
				Conditions: []Condition{
					{Type: "server", Operator: "in", Value: serverValue},
					{Type: "vip", Operator: "gte", Value: vipValue},
				},
			},
			{
				ID:         101,
				Title:      "expired",
				Content:    "content",
				Status:     MailStatusPublished,
				StartTime:  now.Add(-2 * time.Hour),
				ExpireTime: now.Add(-time.Hour),
			},
		},
	}
	cacheRepo := &fakeCacheRepo{version: 1}
	local := NewLocalCache(repo, cacheRepo)
	local.now = func() time.Time { return now }

	if err := local.RefreshIfStale(context.Background()); err != nil {
		t.Fatalf("refresh failed: %v", err)
	}
	visible, err := local.VisibleMails(context.Background(), UserProfile{
		RoleID:   1,
		ServerID: 1,
		VIPLevel: 6,
	})
	if err != nil {
		t.Fatalf("visible mails failed: %v", err)
	}
	if len(visible) != 1 {
		t.Fatalf("expected 1 visible mail, got %d", len(visible))
	}
	if visible[0].ID != 100 {
		t.Fatalf("expected mail 100 visible, got %d", visible[0].ID)
	}
}

func TestLocalCacheRefreshUsesSharedCache(t *testing.T) {
	now := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	repo := &fakeMailRepo{}
	cacheRepo := &fakeCacheRepo{
		version:   1,
		activeIDs: []int64{100},
		mails: map[int64]GlobalMail{
			100: {
				ID:         100,
				Title:      "cached",
				Content:    "content",
				Status:     MailStatusPublished,
				StartTime:  now.Add(-time.Hour),
				ExpireTime: now.Add(time.Hour),
			},
		},
	}
	local := NewLocalCache(repo, cacheRepo)
	local.now = func() time.Time { return now }

	if err := local.RefreshIfStale(context.Background()); err != nil {
		t.Fatalf("refresh failed: %v", err)
	}
	if repo.getPublishedCalls != 0 {
		t.Fatalf("expected shared cache refresh without MySQL, got %d MySQL calls", repo.getPublishedCalls)
	}
	visible, err := local.VisibleMails(context.Background(), UserProfile{RoleID: 1, ServerID: 1})
	if err != nil {
		t.Fatalf("visible mails failed: %v", err)
	}
	if len(visible) != 1 || visible[0].ID != 100 {
		t.Fatalf("expected cached mail 100 visible, got %+v", visible)
	}
}

func TestLocalCacheRefreshRebuildsSharedCacheOnMiss(t *testing.T) {
	now := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	repo := &fakeMailRepo{
		mails: []GlobalMail{{
			ID:         100,
			Title:      "mysql",
			Content:    "content",
			Status:     MailStatusPublished,
			StartTime:  now.Add(-time.Hour),
			ExpireTime: now.Add(time.Hour),
		}},
	}
	cacheRepo := &fakeCacheRepo{version: 1, activeIDs: []int64{100}}
	local := NewLocalCache(repo, cacheRepo)
	local.now = func() time.Time { return now }

	if err := local.RefreshIfStale(context.Background()); err != nil {
		t.Fatalf("refresh failed: %v", err)
	}
	if repo.getPublishedCalls != 1 {
		t.Fatalf("expected MySQL fallback, got %d calls", repo.getPublishedCalls)
	}
	if _, ok := cacheRepo.mails[100]; !ok {
		t.Fatal("expected MySQL fallback to refill shared cache")
	}
}

func TestLocalCacheRebuildSharedCacheUsesSingleflight(t *testing.T) {
	now := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	repo := &fakeMailRepo{
		getPublishedDelay: 50 * time.Millisecond,
		mails: []GlobalMail{{
			ID:         100,
			Title:      "mysql",
			Content:    "content",
			Status:     MailStatusPublished,
			StartTime:  now.Add(-time.Hour),
			ExpireTime: now.Add(time.Hour),
		}},
	}
	cacheRepo := &fakeCacheRepo{version: 100}
	caches := make([]*LocalCache, 5)
	for i := range caches {
		caches[i] = NewLocalCache(repo, cacheRepo)
		caches[i].now = func() time.Time { return now }
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(caches))
	for _, local := range caches {
		wg.Add(1)
		go func(local *LocalCache) {
			defer wg.Done()
			errs <- local.RefreshIfStale(context.Background())
		}(local)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("refresh failed: %v", err)
		}
	}
	if repo.getPublishedCalls != 1 {
		t.Fatalf("expected one MySQL rebuild, got %d", repo.getPublishedCalls)
	}
}

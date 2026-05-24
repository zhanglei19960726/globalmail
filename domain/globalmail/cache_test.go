package globalmail

import (
	"context"
	"encoding/json"
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

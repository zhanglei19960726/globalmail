package gamesrv

import (
	"context"
	"testing"
	"time"

	"globalmail/domain/globalmail"
)

type fakeMailRepository struct {
	mails  []globalmail.GlobalMail
	states map[int64]globalmail.UserGlobalMailState
}

func (f *fakeMailRepository) CreateGlobalMailWithOutbox(context.Context, globalmail.GlobalMail, globalmail.OutboxEvent) error {
	return nil
}

func (f *fakeMailRepository) GetPublishedGlobalMails(context.Context, time.Time) ([]globalmail.GlobalMail, error) {
	return append([]globalmail.GlobalMail(nil), f.mails...), nil
}

func (f *fakeMailRepository) GetUserStates(_ context.Context, _ int64, mailIDs []int64) (map[int64]globalmail.UserGlobalMailState, error) {
	out := map[int64]globalmail.UserGlobalMailState{}
	for _, mailID := range mailIDs {
		if state, ok := f.states[mailID]; ok {
			out[mailID] = state
		}
	}
	return out, nil
}

func (f *fakeMailRepository) SaveUserState(context.Context, globalmail.UserGlobalMailState) error {
	return nil
}

type fakeCacheRepository struct {
	version int64
}

func (f *fakeCacheRepository) GetGlobalMailVersion(context.Context) (int64, error) {
	return f.version, nil
}

func (f *fakeCacheRepository) IncrementGlobalMailVersion(context.Context) (int64, error) {
	f.version++
	return f.version, nil
}

func (f *fakeCacheRepository) GetGlobalMail(context.Context, int64) (globalmail.GlobalMail, bool, error) {
	return globalmail.GlobalMail{}, false, nil
}

func (f *fakeCacheRepository) SetGlobalMail(context.Context, globalmail.GlobalMail) error {
	return nil
}

func (f *fakeCacheRepository) GetGlobalMailsByServer(context.Context, int) ([]int64, error) {
	return nil, nil
}

func (f *fakeCacheRepository) SetGlobalMailsByServer(context.Context, int, []int64) error {
	return nil
}

func (f *fakeCacheRepository) GetUserProfile(context.Context, int64) (globalmail.UserProfile, bool, error) {
	return globalmail.UserProfile{}, false, nil
}

func TestMailServiceListGlobalMailsMergesStates(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeMailRepository{
		mails: []globalmail.GlobalMail{
			{
				ID:         1,
				Status:     globalmail.MailStatusPublished,
				StartTime:  now.Add(-time.Hour),
				ExpireTime: now.Add(time.Hour),
			},
			{
				ID:         2,
				Status:     globalmail.MailStatusPublished,
				StartTime:  now.Add(-time.Hour),
				ExpireTime: now.Add(time.Hour),
			},
		},
		states: map[int64]globalmail.UserGlobalMailState{
			1: {
				RoleID:       10001,
				GlobalMailID: 1,
				Status:       globalmail.UserMailStatusRead,
			},
		},
	}
	cacheRepo := &fakeCacheRepository{version: 1}
	localCache := globalmail.NewLocalCache(repo, cacheRepo)
	localCache.ForceRefresh(context.Background())

	service := NewMailService(repo, localCache)
	items, err := service.ListGlobalMails(context.Background(), globalmail.UserProfile{
		RoleID:   10001,
		ServerID: 1,
	})
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 visible mails, got %d", len(items))
	}
	if items[0].Mail.ID != 2 || items[0].Status != globalmail.UserMailStatusUnread {
		t.Fatalf("expected newest unread mail first, got id=%d status=%s", items[0].Mail.ID, items[0].Status)
	}
	if items[1].Mail.ID != 1 || items[1].Status != globalmail.UserMailStatusRead {
		t.Fatalf("expected read state merged for mail 1, got id=%d status=%s", items[1].Mail.ID, items[1].Status)
	}
}

func TestMailServiceListGlobalMailsSkipsDeletedState(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeMailRepository{
		mails: []globalmail.GlobalMail{
			{
				ID:         1,
				Status:     globalmail.MailStatusPublished,
				StartTime:  now.Add(-time.Hour),
				ExpireTime: now.Add(time.Hour),
			},
		},
		states: map[int64]globalmail.UserGlobalMailState{
			1: {
				RoleID:       10001,
				GlobalMailID: 1,
				Status:       globalmail.UserMailStatusDeleted,
			},
		},
	}
	cacheRepo := &fakeCacheRepository{version: 1}
	localCache := globalmail.NewLocalCache(repo, cacheRepo)
	localCache.ForceRefresh(context.Background())

	service := NewMailService(repo, localCache)
	items, err := service.ListGlobalMails(context.Background(), globalmail.UserProfile{
		RoleID:   10001,
		ServerID: 1,
	})
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected deleted mail filtered, got %d", len(items))
	}
}

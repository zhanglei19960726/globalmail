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

func (f *fakeMailRepository) CreateGlobalMailWithOutboxAndIdempotency(context.Context, globalmail.GlobalMail, globalmail.OutboxEvent, globalmail.PublishIdempotencyRecord) error {
	return nil
}

func (f *fakeMailRepository) GetPublishIdempotency(context.Context, string) (globalmail.PublishIdempotencyRecord, bool, error) {
	return globalmail.PublishIdempotencyRecord{}, false, nil
}

func (f *fakeMailRepository) GetGlobalMailByID(_ context.Context, mailID int64) (globalmail.GlobalMail, bool, error) {
	for _, mail := range f.mails {
		if mail.ID == mailID {
			return mail, true, nil
		}
	}
	return globalmail.GlobalMail{}, false, nil
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

func (f *fakeMailRepository) SaveUserState(_ context.Context, state globalmail.UserGlobalMailState) error {
	if f.states == nil {
		f.states = map[int64]globalmail.UserGlobalMailState{}
	}
	f.states[state.GlobalMailID] = state
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

func (f *fakeCacheRepository) AdvanceGlobalMailVersion(_ context.Context, targetVersion int64) (int64, error) {
	if f.version < targetVersion {
		f.version = targetVersion
	}
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

func TestMailServiceClaimGlobalMailIsIdempotent(t *testing.T) {
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
				RoleID:             10001,
				ServerID:           1,
				GlobalMailID:       1,
				Status:             globalmail.UserMailStatusClaimed,
				ClaimedLootIndexes: []int{0},
				Version:            1,
			},
		},
	}
	cacheRepo := &fakeCacheRepository{version: 1}
	localCache := globalmail.NewLocalCache(repo, cacheRepo)
	localCache.ForceRefresh(context.Background())
	service := NewMailService(repo, localCache)

	state, err := service.ClaimGlobalMail(context.Background(), globalmail.UserProfile{
		RoleID:   10001,
		ServerID: 1,
	}, 1, []int{0, 2})
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if state.Status != globalmail.UserMailStatusClaimed {
		t.Fatalf("unexpected status: %s", state.Status)
	}
	if len(state.ClaimedLootIndexes) != 2 || state.ClaimedLootIndexes[0] != 0 || state.ClaimedLootIndexes[1] != 2 {
		t.Fatalf("unexpected claimed loot indexes: %+v", state.ClaimedLootIndexes)
	}
}

func TestMailServiceDeleteGlobalMailWritesDeletedState(t *testing.T) {
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
		states: map[int64]globalmail.UserGlobalMailState{},
	}
	cacheRepo := &fakeCacheRepository{version: 1}
	localCache := globalmail.NewLocalCache(repo, cacheRepo)
	localCache.ForceRefresh(context.Background())
	service := NewMailService(repo, localCache)

	state, err := service.DeleteGlobalMail(context.Background(), globalmail.UserProfile{
		RoleID:   10001,
		ServerID: 1,
	}, 1)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if state.Status != globalmail.UserMailStatusDeleted || state.DeleteTime == nil {
		t.Fatalf("expected deleted state with delete time, got %+v", state)
	}
}

func TestMailServiceDeletedStateIsNotOverwritten(t *testing.T) {
	now := time.Now().UTC()
	deleteTime := now.Add(-time.Minute)
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
				ServerID:     1,
				GlobalMailID: 1,
				Status:       globalmail.UserMailStatusDeleted,
				DeleteTime:   &deleteTime,
				Version:      1,
			},
		},
	}
	cacheRepo := &fakeCacheRepository{version: 1}
	localCache := globalmail.NewLocalCache(repo, cacheRepo)
	localCache.ForceRefresh(context.Background())
	service := NewMailService(repo, localCache)

	state, err := service.MarkGlobalMailRead(context.Background(), globalmail.UserProfile{
		RoleID:   10001,
		ServerID: 1,
	}, 1)
	if err != nil {
		t.Fatalf("mark read failed: %v", err)
	}
	if state.Status != globalmail.UserMailStatusDeleted {
		t.Fatalf("expected deleted state preserved, got %s", state.Status)
	}

	state, err = service.ClaimGlobalMail(context.Background(), globalmail.UserProfile{
		RoleID:   10001,
		ServerID: 1,
	}, 1, []int{0})
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if state.Status != globalmail.UserMailStatusDeleted {
		t.Fatalf("expected deleted state preserved after claim, got %s", state.Status)
	}
}

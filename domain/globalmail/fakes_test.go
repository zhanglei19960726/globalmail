package globalmail

import (
	"context"
	"sync"
	"time"
)

type fakeIDs struct {
	mu   sync.Mutex
	next int64
}

func (f *fakeIDs) NextID() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	return f.next
}

type fakeMailRepo struct {
	mu                sync.Mutex
	mails             []GlobalMail
	events            []OutboxEvent
	idempotency       map[string]PublishIdempotencyRecord
	states            map[int64]UserGlobalMailState
	getPublishedCalls int
	getPublishedDelay time.Duration
}

func (f *fakeMailRepo) CreateGlobalMailWithOutbox(_ context.Context, mail GlobalMail, event OutboxEvent) error {
	f.mails = append(f.mails, mail)
	f.events = append(f.events, event)
	return nil
}

func (f *fakeMailRepo) CreateGlobalMailWithOutboxAndIdempotency(_ context.Context, mail GlobalMail, event OutboxEvent, record PublishIdempotencyRecord) error {
	f.mails = append(f.mails, mail)
	f.events = append(f.events, event)
	if f.idempotency == nil {
		f.idempotency = map[string]PublishIdempotencyRecord{}
	}
	f.idempotency[record.Key] = record
	return nil
}

func (f *fakeMailRepo) GetPublishIdempotency(_ context.Context, key string) (PublishIdempotencyRecord, bool, error) {
	record, ok := f.idempotency[key]
	return record, ok, nil
}

func (f *fakeMailRepo) GetGlobalMailByID(_ context.Context, mailID int64) (GlobalMail, bool, error) {
	for _, mail := range f.mails {
		if mail.ID == mailID {
			return mail, true, nil
		}
	}
	return GlobalMail{}, false, nil
}

func (f *fakeMailRepo) GetPublishedGlobalMails(_ context.Context, now time.Time) ([]GlobalMail, error) {
	f.mu.Lock()
	f.getPublishedCalls++
	delay := f.getPublishedDelay
	f.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var mails []GlobalMail
	for _, mail := range f.mails {
		if mail.Status == MailStatusPublished && !now.Before(mail.StartTime) && now.Before(mail.ExpireTime) {
			mails = append(mails, mail)
		}
	}
	return mails, nil
}

func (f *fakeMailRepo) GetUserStates(_ context.Context, _ int64, mailIDs []int64) (map[int64]UserGlobalMailState, error) {
	result := make(map[int64]UserGlobalMailState, len(mailIDs))
	for _, id := range mailIDs {
		if state, ok := f.states[id]; ok {
			result[id] = state
		}
	}
	return result, nil
}

func (f *fakeMailRepo) SaveUserState(_ context.Context, state UserGlobalMailState) error {
	if f.states == nil {
		f.states = map[int64]UserGlobalMailState{}
	}
	f.states[state.GlobalMailID] = state
	return nil
}

type fakeCacheRepo struct {
	mu        sync.Mutex
	version   int64
	mails     map[int64]GlobalMail
	activeIDs []int64
}

func (f *fakeCacheRepo) GetGlobalMailVersion(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.version, nil
}

func (f *fakeCacheRepo) IncrementGlobalMailVersion(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.version++
	return f.version, nil
}

func (f *fakeCacheRepo) AdvanceGlobalMailVersion(_ context.Context, targetVersion int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.version < targetVersion {
		f.version = targetVersion
	}
	return f.version, nil
}

func (f *fakeCacheRepo) GetGlobalMail(_ context.Context, mailID int64) (GlobalMail, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	mail, ok := f.mails[mailID]
	return mail, ok, nil
}

func (f *fakeCacheRepo) SetGlobalMail(_ context.Context, mail GlobalMail) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mails == nil {
		f.mails = map[int64]GlobalMail{}
	}
	f.mails[mail.ID] = mail
	return nil
}

func (f *fakeCacheRepo) GetActiveGlobalMailIDs(context.Context, time.Time) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.activeIDs...), nil
}

func (f *fakeCacheRepo) AddGlobalMailToIndexes(_ context.Context, mail GlobalMail) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range f.activeIDs {
		if id == mail.ID {
			return nil
		}
	}
	f.activeIDs = append(f.activeIDs, mail.ID)
	return nil
}

func (f *fakeCacheRepo) GetGlobalMailsByServer(context.Context, int) ([]int64, error) {
	return nil, nil
}

func (f *fakeCacheRepo) SetGlobalMailsByServer(context.Context, int, []int64) error {
	return nil
}

func (f *fakeCacheRepo) GetUserProfile(context.Context, int64) (UserProfile, bool, error) {
	return UserProfile{}, false, nil
}

type fakeOutboxRepo struct {
	pending     []OutboxEvent
	sent        []int64
	failed      []int64
	failRetries []int
	failReasons []string
	lockedBy    string
	lockedUntil time.Time
}

func (f *fakeOutboxRepo) FetchPending(_ context.Context, _ int, lockedBy string, lockedUntil time.Time) ([]OutboxEvent, error) {
	f.lockedBy = lockedBy
	f.lockedUntil = lockedUntil
	events := make([]OutboxEvent, 0, len(f.pending))
	for _, event := range f.pending {
		event.Status = OutboxStatusProcessing
		event.LockedBy = lockedBy
		event.LockedUntil = &lockedUntil
		events = append(events, event)
	}
	return events, nil
}

func (f *fakeOutboxRepo) MarkPublished(_ context.Context, eventID int64, _ time.Time) error {
	f.sent = append(f.sent, eventID)
	return nil
}

func (f *fakeOutboxRepo) MarkFailed(_ context.Context, eventID int64, _ time.Time, cause error, maxRetries int) error {
	f.failed = append(f.failed, eventID)
	f.failRetries = append(f.failRetries, maxRetries)
	if cause != nil {
		f.failReasons = append(f.failReasons, cause.Error())
	}
	return nil
}

type fakePublisher struct {
	events []GlobalMailChangedEvent
}

func (f *fakePublisher) PublishGlobalMailChanged(_ context.Context, event GlobalMailChangedEvent) error {
	f.events = append(f.events, event)
	return nil
}

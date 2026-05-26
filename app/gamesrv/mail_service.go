package gamesrv

import (
	"context"
	"errors"
	"sort"
	"time"

	"globalmail/domain/globalmail"
)

var ErrMailNotVisible = errors.New("global mail is not visible to user")

type MailItem struct {
	Mail               globalmail.GlobalMail           `json:"mail"`
	Status             globalmail.UserMailStatus       `json:"status"`
	ClaimedLootIndexes []int                           `json:"claimed_loot_indexes,omitempty"`
	State              *globalmail.UserGlobalMailState `json:"state,omitempty"`
}

type MailService struct {
	repo  globalmail.MailRepository
	cache *globalmail.LocalCache
	now   func() time.Time
}

func NewMailService(repo globalmail.MailRepository, cache *globalmail.LocalCache) *MailService {
	return &MailService{
		repo:  repo,
		cache: cache,
		now:   time.Now,
	}
}

func (s *MailService) RefreshCache(ctx context.Context) error {
	return s.cache.RefreshIfStale(ctx)
}

func (s *MailService) CacheVersion() int64 {
	return s.cache.Snapshot().Version
}

func (s *MailService) ForceRefreshCache(ctx context.Context) error {
	return s.cache.ForceRefresh(ctx)
}

func (s *MailService) ListGlobalMails(ctx context.Context, profile globalmail.UserProfile) ([]MailItem, error) {
	if err := s.cache.RefreshIfStale(ctx); err != nil {
		return nil, err
	}
	mails, err := s.cache.VisibleMails(ctx, profile)
	if err != nil {
		return nil, err
	}

	mailIDs := make([]int64, 0, len(mails))
	for _, mail := range mails {
		mailIDs = append(mailIDs, mail.ID)
	}
	states, err := s.repo.GetUserStates(ctx, profile.RoleID, mailIDs)
	if err != nil {
		return nil, err
	}

	items := make([]MailItem, 0, len(mails))
	for _, mail := range mails {
		state, ok := states[mail.ID]
		if ok && state.Status == globalmail.UserMailStatusDeleted {
			continue
		}
		item := MailItem{
			Mail:   mail,
			Status: globalmail.UserMailStatusUnread,
		}
		if ok {
			item.Status = state.Status
			item.ClaimedLootIndexes = append([]int(nil), state.ClaimedLootIndexes...)
			item.State = &state
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Mail.ID > items[j].Mail.ID
	})
	return items, nil
}

func (s *MailService) MarkGlobalMailRead(ctx context.Context, profile globalmail.UserProfile, mailID int64) (globalmail.UserGlobalMailState, error) {
	return s.updateGlobalMailState(ctx, profile, mailID, func(state globalmail.UserGlobalMailState, now time.Time) globalmail.UserGlobalMailState {
		if state.Status == globalmail.UserMailStatusDeleted {
			return state
		}
		if state.Status == "" || state.Status == globalmail.UserMailStatusUnread {
			state.Status = globalmail.UserMailStatusRead
		}
		state.UpdateTime = now
		return state
	})
}

func (s *MailService) ClaimGlobalMail(ctx context.Context, profile globalmail.UserProfile, mailID int64, lootIndexes []int) (globalmail.UserGlobalMailState, error) {
	return s.updateGlobalMailState(ctx, profile, mailID, func(state globalmail.UserGlobalMailState, now time.Time) globalmail.UserGlobalMailState {
		if state.Status == globalmail.UserMailStatusDeleted {
			return state
		}
		state.Status = globalmail.UserMailStatusClaimed
		state.ClaimedLootIndexes = mergeLootIndexes(state.ClaimedLootIndexes, lootIndexes)
		if state.ClaimTime == nil {
			claimTime := now
			state.ClaimTime = &claimTime
		}
		state.UpdateTime = now
		return state
	})
}

func (s *MailService) DeleteGlobalMail(ctx context.Context, profile globalmail.UserProfile, mailID int64) (globalmail.UserGlobalMailState, error) {
	return s.updateGlobalMailState(ctx, profile, mailID, func(state globalmail.UserGlobalMailState, now time.Time) globalmail.UserGlobalMailState {
		state.Status = globalmail.UserMailStatusDeleted
		if state.DeleteTime == nil {
			deleteTime := now
			state.DeleteTime = &deleteTime
		}
		state.UpdateTime = now
		return state
	})
}

func (s *MailService) updateGlobalMailState(ctx context.Context, profile globalmail.UserProfile, mailID int64, apply func(globalmail.UserGlobalMailState, time.Time) globalmail.UserGlobalMailState) (globalmail.UserGlobalMailState, error) {
	if err := s.cache.RefreshIfStale(ctx); err != nil {
		return globalmail.UserGlobalMailState{}, err
	}
	if ok, err := s.isMailVisible(ctx, profile, mailID); err != nil {
		return globalmail.UserGlobalMailState{}, err
	} else if !ok {
		return globalmail.UserGlobalMailState{}, ErrMailNotVisible
	}

	states, err := s.repo.GetUserStates(ctx, profile.RoleID, []int64{mailID})
	if err != nil {
		return globalmail.UserGlobalMailState{}, err
	}
	now := s.now().UTC()
	state, ok := states[mailID]
	if !ok {
		state = globalmail.UserGlobalMailState{
			RoleID:       profile.RoleID,
			ServerID:     profile.ServerID,
			GlobalMailID: mailID,
			Status:       globalmail.UserMailStatusUnread,
			CreateTime:   now,
		}
	}
	state.Version++
	state = apply(state, now)
	if state.UpdateTime.IsZero() {
		state.UpdateTime = now
	}
	if err := s.repo.SaveUserState(ctx, state); err != nil {
		return globalmail.UserGlobalMailState{}, err
	}
	return state, nil
}

func (s *MailService) isMailVisible(ctx context.Context, profile globalmail.UserProfile, mailID int64) (bool, error) {
	mails, err := s.cache.VisibleMails(ctx, profile)
	if err != nil {
		return false, err
	}
	for _, mail := range mails {
		if mail.ID == mailID {
			return true, nil
		}
	}
	return false, nil
}

func mergeLootIndexes(existing, next []int) []int {
	seen := make(map[int]struct{}, len(existing)+len(next))
	merged := make([]int, 0, len(existing)+len(next))
	for _, idx := range append(existing, next...) {
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		merged = append(merged, idx)
	}
	sort.Ints(merged)
	return merged
}

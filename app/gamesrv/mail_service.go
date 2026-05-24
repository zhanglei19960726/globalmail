package gamesrv

import (
	"context"
	"sort"

	"globalmail/domain/globalmail"
)

type MailItem struct {
	Mail               globalmail.GlobalMail           `json:"mail"`
	Status             globalmail.UserMailStatus       `json:"status"`
	ClaimedLootIndexes []int                           `json:"claimed_loot_indexes,omitempty"`
	State              *globalmail.UserGlobalMailState `json:"state,omitempty"`
}

type MailService struct {
	repo  globalmail.MailRepository
	cache *globalmail.LocalCache
}

func NewMailService(repo globalmail.MailRepository, cache *globalmail.LocalCache) *MailService {
	return &MailService{repo: repo, cache: cache}
}

func (s *MailService) RefreshCache(ctx context.Context) error {
	return s.cache.RefreshIfStale(ctx)
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

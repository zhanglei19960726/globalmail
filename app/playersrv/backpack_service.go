package playersrv

import (
	"context"
	"fmt"
	"time"

	"globalmail/domain/globalmail"
)

type BackpackRewardService struct {
	ledger   globalmail.RewardLedgerRepository
	backpack globalmail.BackpackRepository
	now      func() time.Time
}

func NewBackpackRewardService(ledger globalmail.RewardLedgerRepository, backpack globalmail.BackpackRepository) *BackpackRewardService {
	return &BackpackRewardService{
		ledger:   ledger,
		backpack: backpack,
		now:      time.Now,
	}
}

func (s *BackpackRewardService) GrantGlobalMailReward(ctx context.Context, grant globalmail.RewardGrant) (bool, error) {
	if s.ledger == nil || s.backpack == nil {
		return false, fmt.Errorf("backpack reward service requires ledger and backpack repositories")
	}
	now := s.now().UTC()
	if grant.CreateTime.IsZero() {
		grant.CreateTime = now
	}
	grant.UpdateTime = now
	grant.Status = "pending"

	reserved, err := s.ledger.ReserveGlobalMailReward(ctx, grant)
	if err != nil || !reserved {
		return reserved, err
	}

	externalRewardID, _, err := s.backpack.GrantBackpackReward(ctx, globalmail.BackpackGrant{
		RoleID:     grant.RoleID,
		ServerID:   grant.ServerID,
		GrantKey:   grant.GrantKey,
		LootIndex:  grant.LootIndex,
		Loot:       grant.Loot,
		CreateTime: now,
	})
	if err != nil {
		_ = s.ledger.MarkGlobalMailRewardFailed(ctx, grant.GrantKey, err, now)
		return false, err
	}
	if err := s.ledger.MarkGlobalMailRewardSucceeded(ctx, grant.GrantKey, externalRewardID, now); err != nil {
		return false, err
	}
	return true, nil
}

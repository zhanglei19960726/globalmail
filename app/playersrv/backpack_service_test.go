package playersrv

import (
	"context"
	"errors"
	"testing"
	"time"

	"globalmail/domain/globalmail"
)

func TestBackpackRewardServiceGrantsAndMarksLedgerSucceeded(t *testing.T) {
	ledger := &fakeRewardLedger{reserved: map[string]globalmail.RewardGrant{}}
	backpack := &fakeBackpackRepo{grants: map[string]globalmail.BackpackGrant{}}
	service := NewBackpackRewardService(ledger, backpack)
	service.now = func() time.Time { return time.Date(2026, 5, 27, 1, 2, 3, 0, time.UTC) }

	granted, err := service.GrantGlobalMailReward(context.Background(), globalmail.RewardGrant{
		RoleID:       10001,
		ServerID:     1,
		GlobalMailID: 10,
		LootIndex:    2,
		Loot:         []byte(`{"item_id":100,"count":1}`),
		GrantKey:     "10001:10:2",
	})
	if err != nil {
		t.Fatalf("grant reward failed: %v", err)
	}
	if !granted {
		t.Fatal("expected first reward grant")
	}
	if ledger.succeeded["10001:10:2"] != "playersrv-backpack:10001:10:2" {
		t.Fatalf("expected ledger success with external id, got %+v", ledger.succeeded)
	}
	if len(backpack.grants) != 1 {
		t.Fatalf("expected one backpack grant, got %d", len(backpack.grants))
	}
}

func TestBackpackRewardServiceSkipsDuplicateLedger(t *testing.T) {
	ledger := &fakeRewardLedger{reserved: map[string]globalmail.RewardGrant{
		"10001:10:2": {GrantKey: "10001:10:2"},
	}}
	backpack := &fakeBackpackRepo{grants: map[string]globalmail.BackpackGrant{}}
	service := NewBackpackRewardService(ledger, backpack)

	granted, err := service.GrantGlobalMailReward(context.Background(), globalmail.RewardGrant{
		RoleID:       10001,
		ServerID:     1,
		GlobalMailID: 10,
		LootIndex:    2,
		GrantKey:     "10001:10:2",
	})
	if err != nil {
		t.Fatalf("duplicate grant failed: %v", err)
	}
	if granted {
		t.Fatal("expected duplicate ledger row to skip backpack grant")
	}
	if len(backpack.grants) != 0 {
		t.Fatalf("expected no backpack grant, got %d", len(backpack.grants))
	}
}

func TestBackpackRewardServiceMarksLedgerFailed(t *testing.T) {
	ledger := &fakeRewardLedger{reserved: map[string]globalmail.RewardGrant{}}
	backpack := &fakeBackpackRepo{grants: map[string]globalmail.BackpackGrant{}, err: errors.New("bag full")}
	service := NewBackpackRewardService(ledger, backpack)

	_, err := service.GrantGlobalMailReward(context.Background(), globalmail.RewardGrant{
		RoleID:       10001,
		ServerID:     1,
		GlobalMailID: 10,
		LootIndex:    2,
		GrantKey:     "10001:10:2",
	})
	if err == nil {
		t.Fatal("expected backpack grant error")
	}
	if ledger.failed["10001:10:2"] == "" {
		t.Fatalf("expected failed ledger reason, got %+v", ledger.failed)
	}
}

type fakeRewardLedger struct {
	reserved  map[string]globalmail.RewardGrant
	succeeded map[string]string
	failed    map[string]string
}

func (f *fakeRewardLedger) ReserveGlobalMailReward(_ context.Context, grant globalmail.RewardGrant) (bool, error) {
	if _, ok := f.reserved[grant.GrantKey]; ok {
		return false, nil
	}
	if f.reserved == nil {
		f.reserved = map[string]globalmail.RewardGrant{}
	}
	f.reserved[grant.GrantKey] = grant
	return true, nil
}

func (f *fakeRewardLedger) MarkGlobalMailRewardSucceeded(_ context.Context, grantKey string, externalRewardID string, _ time.Time) error {
	if f.succeeded == nil {
		f.succeeded = map[string]string{}
	}
	f.succeeded[grantKey] = externalRewardID
	return nil
}

func (f *fakeRewardLedger) MarkGlobalMailRewardFailed(_ context.Context, grantKey string, cause error, _ time.Time) error {
	if f.failed == nil {
		f.failed = map[string]string{}
	}
	f.failed[grantKey] = cause.Error()
	return nil
}

type fakeBackpackRepo struct {
	grants map[string]globalmail.BackpackGrant
	err    error
}

func (f *fakeBackpackRepo) GrantBackpackReward(_ context.Context, grant globalmail.BackpackGrant) (string, bool, error) {
	if f.err != nil {
		return "", false, f.err
	}
	if _, ok := f.grants[grant.GrantKey]; ok {
		return "playersrv-backpack:" + grant.GrantKey, false, nil
	}
	f.grants[grant.GrantKey] = grant
	return "playersrv-backpack:" + grant.GrantKey, true, nil
}

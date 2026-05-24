package globalmail

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type LocalCache struct {
	repo  MailRepository
	cache CacheRepository
	value atomic.Value // *CacheSnapshot
	mu    sync.Mutex
	now   func() time.Time
}

type CacheSnapshot struct {
	Version            int64
	MaxGlobalMailID    int64
	MailsByID          map[int64]GlobalMail
	ActiveMailIDs      []int64
	MailIDsByServer    map[int][]int64
	CompiledConditions map[int64][]CompiledCondition
	LastRefreshTime    time.Time
}

type CompiledCondition struct {
	Type     string
	Operator string
	Value    json.RawMessage
}

func NewLocalCache(repo MailRepository, cache CacheRepository) *LocalCache {
	c := &LocalCache{
		repo:  repo,
		cache: cache,
		now:   time.Now,
	}
	c.value.Store(&CacheSnapshot{
		MailsByID:          map[int64]GlobalMail{},
		MailIDsByServer:    map[int][]int64{},
		CompiledConditions: map[int64][]CompiledCondition{},
	})
	return c
}

func (c *LocalCache) Snapshot() *CacheSnapshot {
	return c.value.Load().(*CacheSnapshot)
}

func (c *LocalCache) RefreshIfStale(ctx context.Context) error {
	remoteVersion, err := c.cache.GetGlobalMailVersion(ctx)
	if err != nil {
		return err
	}
	if c.Snapshot().Version >= remoteVersion {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Snapshot().Version >= remoteVersion {
		return nil
	}
	return c.refresh(ctx, remoteVersion)
}

func (c *LocalCache) ForceRefresh(ctx context.Context) error {
	remoteVersion, err := c.cache.GetGlobalMailVersion(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.refresh(ctx, remoteVersion)
}

func (c *LocalCache) refresh(ctx context.Context, version int64) error {
	now := c.now().UTC()
	mails, err := c.repo.GetPublishedGlobalMails(ctx, now)
	if err != nil {
		return err
	}

	next := &CacheSnapshot{
		Version:            version,
		MailsByID:          make(map[int64]GlobalMail, len(mails)),
		MailIDsByServer:    make(map[int][]int64),
		CompiledConditions: make(map[int64][]CompiledCondition, len(mails)),
		LastRefreshTime:    now,
	}
	for _, mail := range mails {
		next.MailsByID[mail.ID] = mail
		next.ActiveMailIDs = append(next.ActiveMailIDs, mail.ID)
		if mail.ID > next.MaxGlobalMailID {
			next.MaxGlobalMailID = mail.ID
		}
		next.CompiledConditions[mail.ID] = compileConditions(mail.Conditions)
		for _, serverID := range serverIDsFromConditions(mail.Conditions) {
			next.MailIDsByServer[serverID] = append(next.MailIDsByServer[serverID], mail.ID)
		}
	}
	c.value.Store(next)
	return nil
}

func (c *LocalCache) VisibleMails(ctx context.Context, profile UserProfile) ([]GlobalMail, error) {
	snapshot := c.Snapshot()
	now := c.now().UTC()

	candidateIDs := snapshot.MailIDsByServer[profile.ServerID]
	if len(candidateIDs) == 0 {
		candidateIDs = snapshot.ActiveMailIDs
	}

	visible := make([]GlobalMail, 0, len(candidateIDs))
	for _, mailID := range candidateIDs {
		mail, ok := snapshot.MailsByID[mailID]
		if !ok || mail.Status != MailStatusPublished {
			continue
		}
		if now.Before(mail.StartTime) || !now.Before(mail.ExpireTime) {
			continue
		}
		if !conditionsMatch(snapshot.CompiledConditions[mailID], profile) {
			continue
		}
		visible = append(visible, mail)
	}
	return visible, nil
}

func compileConditions(conditions []Condition) []CompiledCondition {
	compiled := make([]CompiledCondition, 0, len(conditions))
	for _, condition := range conditions {
		compiled = append(compiled, CompiledCondition{
			Type:     condition.Type,
			Operator: condition.Operator,
			Value:    condition.Value,
		})
	}
	return compiled
}

func serverIDsFromConditions(conditions []Condition) []int {
	var serverIDs []int
	for _, condition := range conditions {
		if condition.Type != "server" && condition.Type != "server_id" {
			continue
		}
		var ids []int
		if err := json.Unmarshal(condition.Value, &ids); err == nil {
			serverIDs = append(serverIDs, ids...)
			continue
		}
		var id int
		if err := json.Unmarshal(condition.Value, &id); err == nil {
			serverIDs = append(serverIDs, id)
		}
	}
	return serverIDs
}

func conditionsMatch(conditions []CompiledCondition, profile UserProfile) bool {
	for _, condition := range conditions {
		switch condition.Type {
		case "server", "server_id":
			if !matchInt(condition, profile.ServerID) {
				return false
			}
		case "vip", "vip_level":
			if !matchInt(condition, profile.VIPLevel) {
				return false
			}
		case "recharge", "total_recharge":
			if !matchInt64(condition, profile.TotalRecharge) {
				return false
			}
		case "corps_level":
			if !matchInt(condition, profile.CorpsLevel) {
				return false
			}
		case "country":
			if !matchString(condition, profile.Country) {
				return false
			}
		}
	}
	return true
}

func matchInt(condition CompiledCondition, actual int) bool {
	var expected int
	if err := json.Unmarshal(condition.Value, &expected); err == nil {
		return compareInt64(int64(actual), int64(expected), condition.Operator)
	}
	var list []int
	if err := json.Unmarshal(condition.Value, &list); err == nil {
		for _, item := range list {
			if item == actual {
				return true
			}
		}
		return false
	}
	return false
}

func matchInt64(condition CompiledCondition, actual int64) bool {
	var expected int64
	if err := json.Unmarshal(condition.Value, &expected); err == nil {
		return compareInt64(actual, expected, condition.Operator)
	}
	var encoded string
	if err := json.Unmarshal(condition.Value, &encoded); err == nil {
		parsed, err := strconv.ParseInt(encoded, 10, 64)
		return err == nil && compareInt64(actual, parsed, condition.Operator)
	}
	return false
}

func matchString(condition CompiledCondition, actual string) bool {
	var expected string
	if err := json.Unmarshal(condition.Value, &expected); err == nil {
		return actual == expected
	}
	var list []string
	if err := json.Unmarshal(condition.Value, &list); err == nil {
		for _, item := range list {
			if item == actual {
				return true
			}
		}
		return false
	}
	return false
}

func compareInt64(actual, expected int64, operator string) bool {
	switch operator {
	case "", "eq":
		return actual == expected
	case "ne":
		return actual != expected
	case "gt":
		return actual > expected
	case "gte":
		return actual >= expected
	case "lt":
		return actual < expected
	case "lte":
		return actual <= expected
	default:
		return false
	}
}

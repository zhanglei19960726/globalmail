package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"globalmail/domain/globalmail"

	goredis "github.com/redis/go-redis/v9"
)

type Repository struct {
	client goredis.UniversalClient
	prefix string
}

func NewRepository(client goredis.UniversalClient, keyPrefix string) *Repository {
	return &Repository{
		client: client,
		prefix: keyPrefix,
	}
}

func (r *Repository) GetGlobalMailVersion(ctx context.Context) (int64, error) {
	version, err := r.client.Get(ctx, r.key("GlobalMailVersion")).Int64()
	if err == goredis.Nil {
		return 0, nil
	}
	return version, err
}

func (r *Repository) IncrementGlobalMailVersion(ctx context.Context) (int64, error) {
	return r.client.Incr(ctx, r.key("GlobalMailVersion")).Result()
}

func (r *Repository) AdvanceGlobalMailVersion(ctx context.Context, targetVersion int64) (int64, error) {
	key := r.key("GlobalMailVersion")
	script := `
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
local target = tonumber(ARGV[1])
if current < target then
  redis.call("SET", KEYS[1], target)
  return target
end
return current
`
	version, err := r.client.Eval(ctx, script, []string{key}, targetVersion).Int64()
	if err != nil {
		return 0, err
	}
	return version, nil
}

func (r *Repository) GetGlobalMail(ctx context.Context, mailID int64) (globalmail.GlobalMail, bool, error) {
	payload, err := r.client.Get(ctx, r.globalMailKey(mailID)).Bytes()
	if err == goredis.Nil {
		return globalmail.GlobalMail{}, false, nil
	}
	if err != nil {
		return globalmail.GlobalMail{}, false, err
	}
	var mail globalmail.GlobalMail
	if err := json.Unmarshal(payload, &mail); err != nil {
		return globalmail.GlobalMail{}, false, err
	}
	return mail, true, nil
}

func (r *Repository) SetGlobalMail(ctx context.Context, mail globalmail.GlobalMail) error {
	payload, err := json.Marshal(mail)
	if err != nil {
		return err
	}
	ttl := time.Until(mail.ExpireTime)
	if ttl <= 0 {
		ttl = time.Hour
	}
	ttl += 10 * time.Minute
	return r.client.Set(ctx, r.globalMailKey(mail.ID), payload, ttl).Err()
}

func (r *Repository) GetGlobalMailsByServer(ctx context.Context, serverID int) ([]int64, error) {
	members, err := r.client.SMembers(ctx, r.globalMailByServerKey(serverID)).Result()
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(members))
	for _, member := range members {
		var id int64
		if _, err := fmt.Sscan(member, &id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (r *Repository) SetGlobalMailsByServer(ctx context.Context, serverID int, mailIDs []int64) error {
	key := r.globalMailByServerKey(serverID)
	pipe := r.client.TxPipeline()
	pipe.Del(ctx, key)
	if len(mailIDs) > 0 {
		values := make([]interface{}, 0, len(mailIDs))
		for _, id := range mailIDs {
			values = append(values, id)
		}
		pipe.SAdd(ctx, key, values...)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (r *Repository) GetUserProfile(ctx context.Context, roleID int64) (globalmail.UserProfile, bool, error) {
	payload, err := r.client.Get(ctx, r.userProfileKey(roleID)).Bytes()
	if err == goredis.Nil {
		return globalmail.UserProfile{}, false, nil
	}
	if err != nil {
		return globalmail.UserProfile{}, false, err
	}
	var profile globalmail.UserProfile
	if err := json.Unmarshal(payload, &profile); err != nil {
		return globalmail.UserProfile{}, false, err
	}
	return profile, true, nil
}

func (r *Repository) SetUserProfile(ctx context.Context, profile globalmail.UserProfile, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	payload, err := json.Marshal(profile)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, r.userProfileKey(profile.RoleID), payload, ttl).Err()
}

func (r *Repository) key(name string) string {
	return r.prefix + name
}

func (r *Repository) globalMailKey(mailID int64) string {
	return fmt.Sprintf("%sGlobalMail:%d", r.prefix, mailID)
}

func (r *Repository) globalMailByServerKey(serverID int) string {
	return fmt.Sprintf("%sGlobalMailByServer:%d", r.prefix, serverID)
}

func (r *Repository) userProfileKey(roleID int64) string {
	return fmt.Sprintf("%sMailUserProfile:%d", r.prefix, roleID)
}

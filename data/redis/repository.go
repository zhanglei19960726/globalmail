package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"globalmail/api/rpc"
	"globalmail/domain/globalmail"

	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
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

func (r *Repository) GetActiveGlobalMailIDs(ctx context.Context, now time.Time) ([]int64, error) {
	members, err := r.client.ZRangeByScore(ctx, r.key("GlobalMailActiveIndex"), &goredis.ZRangeBy{
		Min: fmt.Sprintf("%d", now.Unix()),
		Max: "+inf",
	}).Result()
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

func (r *Repository) AddGlobalMailToIndexes(ctx context.Context, mail globalmail.GlobalMail) error {
	pipe := r.client.TxPipeline()
	member := fmt.Sprintf("%d", mail.ID)
	pipe.ZAdd(ctx, r.key("GlobalMailActiveIndex"), goredis.Z{
		Score:  float64(mail.ExpireTime.Unix()),
		Member: member,
	})
	for _, serverID := range serverIDsFromConditions(mail.Conditions) {
		pipe.SAdd(ctx, r.globalMailByServerKey(serverID), member)
	}
	_, err := pipe.Exec(ctx)
	return err
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

func (r *Repository) TryAcquireGlobalMailRebuildLock(ctx context.Context, version int64, ttl time.Duration) (func(context.Context) error, bool, error) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	key := r.globalMailRebuildLockKey(version)
	token := strconv.FormatInt(time.Now().UnixNano(), 10)
	acquired, err := r.client.SetNX(ctx, key, token, ttl).Result()
	if err != nil || !acquired {
		return func(context.Context) error { return nil }, acquired, err
	}
	release := func(ctx context.Context) error {
		script := `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`
		return r.client.Eval(ctx, script, []string{key}, token).Err()
	}
	return release, true, nil
}

func (r *Repository) GetCommandIdempotency(ctx context.Context, key string) (string, *rpc.CommandResponse, bool, error) {
	values, err := r.client.HMGet(ctx, r.commandIdempotencyKey(key), "hash", "response").Result()
	if err != nil {
		return "", nil, false, err
	}
	if len(values) != 2 || values[0] == nil || values[1] == nil {
		return "", nil, false, nil
	}
	hash, ok := values[0].(string)
	if !ok {
		return "", nil, false, fmt.Errorf("invalid command idempotency hash type %T", values[0])
	}
	var payload []byte
	switch value := values[1].(type) {
	case string:
		payload = []byte(value)
	case []byte:
		payload = value
	default:
		return "", nil, false, fmt.Errorf("invalid command idempotency response type %T", values[1])
	}
	var response rpc.CommandResponse
	if err := proto.Unmarshal(payload, &response); err != nil {
		return "", nil, false, err
	}
	return hash, &response, true, nil
}

func (r *Repository) SaveCommandIdempotency(ctx context.Context, key string, requestHash string, response *rpc.CommandResponse, ttl time.Duration) error {
	payload, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	redisKey := r.commandIdempotencyKey(key)
	pipe := r.client.TxPipeline()
	pipe.HSet(ctx, redisKey, "hash", requestHash, "response", payload)
	pipe.Expire(ctx, redisKey, ttl)
	_, err = pipe.Exec(ctx)
	return err
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

func (r *Repository) globalMailRebuildLockKey(version int64) string {
	return fmt.Sprintf("%sGlobalMailRebuildLock:%d", r.prefix, version)
}

func (r *Repository) commandIdempotencyKey(key string) string {
	return fmt.Sprintf("%sCommandIdempotency:%s", r.prefix, key)
}

func serverIDsFromConditions(conditions []globalmail.Condition) []int {
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

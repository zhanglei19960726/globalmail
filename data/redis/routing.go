package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"globalmail/domain/routing"

	goredis "github.com/redis/go-redis/v9"
)

func (r *Repository) GetLoginToken(ctx context.Context, token string) (routing.LoginToken, bool, error) {
	payload, err := r.client.Get(ctx, r.loginTokenKey(token)).Bytes()
	if err == goredis.Nil {
		return routing.LoginToken{}, false, nil
	}
	if err != nil {
		return routing.LoginToken{}, false, err
	}
	var value routing.LoginToken
	if err := json.Unmarshal(payload, &value); err != nil {
		return routing.LoginToken{}, false, err
	}
	return value, true, nil
}

func (r *Repository) SetLoginToken(ctx context.Context, token routing.LoginToken, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = time.Until(token.ExpireAt)
	}
	if ttl <= 0 {
		ttl = time.Minute
	}
	payload, err := json.Marshal(token)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, r.loginTokenKey(token.Token), payload, ttl).Err()
}

func (r *Repository) DeleteLoginToken(ctx context.Context, token string) error {
	return r.client.Del(ctx, r.loginTokenKey(token)).Err()
}

func (r *Repository) GetGateConn(ctx context.Context, uid int64) (routing.GateConn, bool, error) {
	payload, err := r.client.Get(ctx, r.gateConnKey(uid)).Bytes()
	if err == goredis.Nil {
		return routing.GateConn{}, false, nil
	}
	if err != nil {
		return routing.GateConn{}, false, err
	}
	var value routing.GateConn
	if err := json.Unmarshal(payload, &value); err != nil {
		return routing.GateConn{}, false, err
	}
	return value, true, nil
}

func (r *Repository) SetGateConn(ctx context.Context, conn routing.GateConn, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = time.Until(conn.ExpireAt)
	}
	if ttl <= 0 {
		ttl = time.Minute
	}
	payload, err := json.Marshal(conn)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, r.gateConnKey(conn.UID), payload, ttl).Err()
}

func (r *Repository) DeleteGateConn(ctx context.Context, uid int64) error {
	return r.client.Del(ctx, r.gateConnKey(uid)).Err()
}

func (r *Repository) GetSrvRouter(ctx context.Context, uid int64) (routing.SrvRouter, bool, error) {
	payload, err := r.client.Get(ctx, r.srvRouterKey(uid)).Bytes()
	if err == goredis.Nil {
		return routing.SrvRouter{}, false, nil
	}
	if err != nil {
		return routing.SrvRouter{}, false, err
	}
	var value routing.SrvRouter
	if err := json.Unmarshal(payload, &value); err != nil {
		return routing.SrvRouter{}, false, err
	}
	return value, true, nil
}

func (r *Repository) SetSrvRouter(ctx context.Context, route routing.SrvRouter, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = time.Until(route.ExpireAt)
	}
	if ttl <= 0 {
		ttl = time.Minute
	}
	payload, err := json.Marshal(route)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, r.srvRouterKey(route.UID), payload, ttl).Err()
}

func (r *Repository) DeleteSrvRouter(ctx context.Context, uid int64) error {
	return r.client.Del(ctx, r.srvRouterKey(uid)).Err()
}

func (r *Repository) loginTokenKey(token string) string {
	return fmt.Sprintf("%sDBLoginToken:%s", r.prefix, token)
}

func (r *Repository) gateConnKey(uid int64) string {
	return fmt.Sprintf("%sDBGateConn:%d", r.prefix, uid)
}

func (r *Repository) srvRouterKey(uid int64) string {
	return fmt.Sprintf("%sDBSrvRouter:%d", r.prefix, uid)
}

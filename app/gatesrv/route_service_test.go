package gatesrv

import (
	"context"
	"testing"
	"time"

	"globalmail/domain/routing"
)

type fakeRouteStore struct {
	routes  map[int64]routing.SrvRouter
	writes  int
	deletes int
}

func (f *fakeRouteStore) GetSrvRouter(_ context.Context, uid int64) (routing.SrvRouter, bool, error) {
	route, ok := f.routes[uid]
	return route, ok, nil
}

func (f *fakeRouteStore) SetSrvRouter(_ context.Context, route routing.SrvRouter, _ time.Duration) error {
	if f.routes == nil {
		f.routes = map[int64]routing.SrvRouter{}
	}
	f.routes[route.UID] = route
	f.writes++
	return nil
}

func (f *fakeRouteStore) DeleteSrvRouter(_ context.Context, uid int64) error {
	delete(f.routes, uid)
	f.deletes++
	return nil
}

type fakeGameServerProvider struct {
	instances []GameServerInstance
	calls     int
}

func (f *fakeGameServerProvider) ListReadyGameServers(context.Context) ([]GameServerInstance, error) {
	f.calls++
	return f.instances, nil
}

func TestRouteServiceUsesRedisRouteBeforeHashing(t *testing.T) {
	now := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	store := &fakeRouteStore{routes: map[int64]routing.SrvRouter{
		10001: {
			UID:        10001,
			InstanceID: "game-from-redis",
			GrpcAddr:   "10.0.0.9:9001",
			FrpcAddr:   "10.0.0.9:9101",
			ExpireAt:   now.Add(time.Minute),
		},
	}}
	provider := &fakeGameServerProvider{
		instances: []GameServerInstance{{InstanceID: "game-1"}},
	}
	service := NewRouteService(store, provider, time.Minute, 100)
	service.now = func() time.Time { return now }

	route, err := service.Resolve(context.Background(), 10001)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if route.InstanceID != "game-from-redis" {
		t.Fatalf("expected redis route, got %s", route.InstanceID)
	}
	if provider.calls != 0 {
		t.Fatalf("provider should not be called on redis hit, calls=%d", provider.calls)
	}
}

func TestRouteServiceHashesAndStoresWhenRedisMisses(t *testing.T) {
	now := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	store := &fakeRouteStore{routes: map[int64]routing.SrvRouter{}}
	provider := &fakeGameServerProvider{
		instances: []GameServerInstance{
			{InstanceID: "game-1", GrpcAddr: "10.0.0.1:9001"},
			{InstanceID: "game-2", GrpcAddr: "10.0.0.2:9001"},
		},
	}
	service := NewRouteService(store, provider, time.Minute, 100)
	service.now = func() time.Time { return now }

	route, err := service.Resolve(context.Background(), 10001)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if route.InstanceID == "" {
		t.Fatal("expected selected route")
	}
	if store.writes != 1 {
		t.Fatalf("expected redis route write, got %d", store.writes)
	}
	if provider.calls != 1 {
		t.Fatalf("expected provider called once, got %d", provider.calls)
	}

	second, err := service.Resolve(context.Background(), 10001)
	if err != nil {
		t.Fatalf("second resolve failed: %v", err)
	}
	if second.InstanceID != route.InstanceID {
		t.Fatalf("expected session route hit %s, got %s", route.InstanceID, second.InstanceID)
	}
	if provider.calls != 1 {
		t.Fatalf("provider should not be called on session hit, got %d", provider.calls)
	}
}

func TestRouteServiceClearRemovesSessionAndRedisRoute(t *testing.T) {
	store := &fakeRouteStore{routes: map[int64]routing.SrvRouter{}}
	provider := &fakeGameServerProvider{instances: []GameServerInstance{{InstanceID: "game-1"}}}
	service := NewRouteService(store, provider, time.Minute, 100)
	service.session.Set(10001, GameServerInstance{InstanceID: "game-1"})

	if err := service.Clear(context.Background(), 10001); err != nil {
		t.Fatalf("clear failed: %v", err)
	}
	if _, ok := service.session.Get(10001); ok {
		t.Fatal("expected session route deleted")
	}
	if store.deletes != 1 {
		t.Fatalf("expected redis delete, got %d", store.deletes)
	}
}

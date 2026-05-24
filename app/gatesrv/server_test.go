package gatesrv

import (
	"context"
	"testing"

	"globalmail/api/rpc"
)

type fakeUIDRouter struct {
	route   GameServerInstance
	cleared int64
}

func (r fakeUIDRouter) Resolve(context.Context, int64) (GameServerInstance, error) {
	return r.route, nil
}

func (r *fakeUIDRouter) Clear(_ context.Context, uid int64) error {
	r.cleared = uid
	return nil
}

func TestServerResolveRouteReturnsGameServer(t *testing.T) {
	server := NewServer(&fakeUIDRouter{route: GameServerInstance{
		InstanceID: "game-1",
		GrpcAddr:   "127.0.0.1:9001",
	}})

	resp, err := server.ResolveRoute(context.Background(), &rpc.RouteRequest{Uid: 10001})
	if err != nil {
		t.Fatalf("resolve route failed: %v", err)
	}
	if resp.GetInstanceId() != "game-1" {
		t.Fatalf("expected game-1, got %s", resp.GetInstanceId())
	}
}

func TestServerClearRouteDelegatesToRouter(t *testing.T) {
	router := &fakeUIDRouter{}
	server := NewServer(router)

	resp, err := server.ClearRoute(context.Background(), &rpc.RouteRequest{Uid: 10001})
	if err != nil {
		t.Fatalf("clear route failed: %v", err)
	}
	if !resp.GetOk() || router.cleared != 10001 {
		t.Fatalf("expected clear ok for uid 10001, got ok=%v uid=%d", resp.GetOk(), router.cleared)
	}
}

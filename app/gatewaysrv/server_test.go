package gatewaysrv

import (
	"context"
	"testing"
	"time"

	"globalmail/api/rpc"
)

type fakeUIDRouter struct {
	route   PlayerServerInstance
	cleared int64
}

func (r fakeUIDRouter) Resolve(context.Context, int64) (PlayerServerInstance, error) {
	return r.route, nil
}

func (r *fakeUIDRouter) Clear(_ context.Context, uid int64) error {
	r.cleared = uid
	return nil
}

func TestServerResolveRouteReturnsPlayerServer(t *testing.T) {
	server := NewServer(&fakeUIDRouter{route: PlayerServerInstance{
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

func TestServerHeartbeatRefreshesConnection(t *testing.T) {
	store := &fakeGateConnStore{}
	manager := NewConnectionManager(store, "gate-1", time.Minute, 30*time.Second)
	if _, err := manager.Bind(context.Background(), BindSessionRequest{
		UID:    10001,
		ConnID: "conn-1",
	}); err != nil {
		t.Fatalf("bind failed: %v", err)
	}
	server := NewServerWithConnections(&fakeUIDRouter{}, manager)

	resp, err := server.Heartbeat(context.Background(), &rpc.HeartbeatRequest{
		Uid:    10001,
		ConnId: "conn-1",
		Seq:    9,
	})
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	if !resp.GetOk() || resp.GetSeq() != 9 || resp.GetExpireAt() == "" {
		t.Fatalf("unexpected heartbeat response: %+v", resp)
	}
}

func TestServerHeartbeatRequiresConnectionManager(t *testing.T) {
	server := NewServer(&fakeUIDRouter{})
	_, err := server.Heartbeat(context.Background(), &rpc.HeartbeatRequest{Uid: 10001})
	if err != ErrConnectionManagerRequired {
		t.Fatalf("expected ErrConnectionManagerRequired, got %v", err)
	}
}

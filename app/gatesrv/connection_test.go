package gatesrv

import (
	"context"
	"testing"
	"time"

	"globalmail/domain/routing"
)

type fakeGateConnStore struct {
	setCount    int
	deleteCount int
	lastConn    routing.GateConn
	lastTTL     time.Duration
}

type fakeClientConnection struct {
	closeCount int
}

func (c *fakeClientConnection) Close() error {
	c.closeCount++
	return nil
}

func (s *fakeGateConnStore) SetGateConn(_ context.Context, conn routing.GateConn, ttl time.Duration) error {
	s.setCount++
	s.lastConn = conn
	s.lastTTL = ttl
	return nil
}

func (s *fakeGateConnStore) DeleteGateConn(context.Context, int64) error {
	s.deleteCount++
	return nil
}

func TestConnectionManagerBindAndHeartbeatRenewal(t *testing.T) {
	now := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	store := &fakeGateConnStore{}
	manager := NewConnectionManager(store, "gate-1", 90*time.Second, 30*time.Second)
	manager.now = func() time.Time { return now }

	session, err := manager.Bind(context.Background(), BindSessionRequest{
		UID:      10001,
		RoleID:   20001,
		ServerID: 1,
		ConnID:   "conn-1",
	})
	if err != nil {
		t.Fatalf("bind failed: %v", err)
	}
	if session.GatewayAddr != "gate-1" || store.setCount != 1 {
		t.Fatalf("unexpected bind session=%+v setCount=%d", session, store.setCount)
	}

	manager.now = func() time.Time { return now.Add(10 * time.Second) }
	if _, err := manager.Heartbeat(context.Background(), 10001, "conn-1"); err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	if store.setCount != 1 {
		t.Fatalf("heartbeat before renew interval should not write redis, setCount=%d", store.setCount)
	}

	manager.now = func() time.Time { return now.Add(31 * time.Second) }
	renewed, err := manager.Heartbeat(context.Background(), 10001, "conn-1")
	if err != nil {
		t.Fatalf("renew heartbeat failed: %v", err)
	}
	if store.setCount != 2 {
		t.Fatalf("heartbeat after renew interval should write redis, setCount=%d", store.setCount)
	}
	if !renewed.LastHeartbeatAt.Equal(now.Add(31 * time.Second)) {
		t.Fatalf("unexpected heartbeat time: %s", renewed.LastHeartbeatAt)
	}
}

func TestConnectionManagerRejectsMismatchedConnID(t *testing.T) {
	manager := NewConnectionManager(&fakeGateConnStore{}, "gate-1", time.Minute, 30*time.Second)
	if _, err := manager.Bind(context.Background(), BindSessionRequest{UID: 10001, ConnID: "conn-1"}); err != nil {
		t.Fatalf("bind failed: %v", err)
	}
	if _, err := manager.Heartbeat(context.Background(), 10001, "conn-2"); err != ErrSessionMismatch {
		t.Fatalf("expected ErrSessionMismatch, got %v", err)
	}
}

func TestConnectionManagerSweepExpiredRemovesLocalSessions(t *testing.T) {
	now := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	manager := NewConnectionManager(&fakeGateConnStore{}, "gate-1", time.Minute, 30*time.Second)
	manager.now = func() time.Time { return now }
	if _, err := manager.Bind(context.Background(), BindSessionRequest{UID: 10001, ConnID: "conn-1"}); err != nil {
		t.Fatalf("bind failed: %v", err)
	}

	manager.now = func() time.Time { return now.Add(2 * time.Minute) }
	expired := manager.SweepExpired()
	if len(expired) != 1 || expired[0].UID != 10001 {
		t.Fatalf("expected expired session, got %+v", expired)
	}
	if _, ok := manager.pool.Get(10001); ok {
		t.Fatal("expected session removed from pool")
	}
}

func TestConnectionPoolIndexesByConnID(t *testing.T) {
	pool := NewConnectionPool(4)
	conn := &fakeClientConnection{}
	pool.Set(Session{UID: 10001, ConnID: "conn-1", Conn: conn})

	session, ok := pool.GetByConnID("conn-1")
	if !ok {
		t.Fatal("expected session by conn id")
	}
	if session.UID != 10001 {
		t.Fatalf("unexpected session uid: %d", session.UID)
	}
}

func TestConnectionPoolClosesReplacedConnection(t *testing.T) {
	pool := NewConnectionPool(4)
	oldConn := &fakeClientConnection{}
	newConn := &fakeClientConnection{}

	pool.Set(Session{UID: 10001, ConnID: "conn-1", Conn: oldConn})
	pool.Set(Session{UID: 10001, ConnID: "conn-2", Conn: newConn})

	if oldConn.closeCount != 1 {
		t.Fatalf("expected old connection closed once, got %d", oldConn.closeCount)
	}
	if _, ok := pool.GetByConnID("conn-1"); ok {
		t.Fatal("expected old conn index removed")
	}
	if _, ok := pool.GetByConnID("conn-2"); !ok {
		t.Fatal("expected new conn index")
	}
}

func TestConnectionPoolSweepExpiredClosesConnection(t *testing.T) {
	now := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	pool := NewConnectionPool(4)
	conn := &fakeClientConnection{}
	pool.Set(Session{
		UID:      10001,
		ConnID:   "conn-1",
		Conn:     conn,
		ExpireAt: now.Add(-time.Second),
	})

	expired := pool.SweepExpired(now)
	if len(expired) != 1 {
		t.Fatalf("expected one expired session, got %d", len(expired))
	}
	if conn.closeCount != 1 {
		t.Fatalf("expected expired connection closed, got %d", conn.closeCount)
	}
}

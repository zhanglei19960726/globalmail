package gatesrv

import (
	"context"
	"errors"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	"globalmail/domain/routing"
)

var (
	ErrSessionNotFound           = errors.New("session not found")
	ErrSessionMismatch           = errors.New("session connection mismatch")
	ErrConnectionNotFound        = errors.New("connection not found")
	ErrConnectionManagerRequired = errors.New("connection manager is required")
)

type GateConnStore interface {
	SetGateConn(ctx context.Context, conn routing.GateConn, ttl time.Duration) error
	DeleteGateConn(ctx context.Context, uid int64) error
}

type ClientConnection interface {
	Close() error
}

type Session struct {
	UID             int64
	RoleID          int64
	ServerID        int
	ConnID          string
	GatewayAddr     string
	ClientIP        string
	DeviceID        string
	ConnectedAt     time.Time
	LastHeartbeatAt time.Time
	LastRenewAt     time.Time
	ExpireAt        time.Time
	Conn            ClientConnection
}

type BindSessionRequest struct {
	UID         int64
	RoleID      int64
	ServerID    int
	ConnID      string
	GatewayAddr string
	ClientIP    string
	DeviceID    string
	Conn        ClientConnection
}

type ConnectionPool struct {
	shards []sessionShard
}

type sessionShard struct {
	mu        sync.RWMutex
	sessions  map[int64]Session
	connIndex map[string]int64
}

func NewConnectionPool(shardCount int) *ConnectionPool {
	if shardCount <= 0 {
		shardCount = 32
	}
	pool := &ConnectionPool{shards: make([]sessionShard, shardCount)}
	for i := range pool.shards {
		pool.shards[i].sessions = make(map[int64]Session)
		pool.shards[i].connIndex = make(map[string]int64)
	}
	return pool
}

func (p *ConnectionPool) Get(uid int64) (Session, bool) {
	shard := p.shard(uid)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	session, ok := shard.sessions[uid]
	return session, ok
}

func (p *ConnectionPool) GetByConnID(connID string) (Session, bool) {
	if connID == "" {
		return Session{}, false
	}
	for i := range p.shards {
		shard := &p.shards[i]
		shard.mu.RLock()
		uid, ok := shard.connIndex[connID]
		if ok {
			session, sessionOK := shard.sessions[uid]
			shard.mu.RUnlock()
			return session, sessionOK
		}
		shard.mu.RUnlock()
	}
	return Session{}, false
}

func (p *ConnectionPool) Set(session Session) {
	shard := p.shard(session.UID)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if old, ok := shard.sessions[session.UID]; ok {
		if old.ConnID != "" && old.ConnID != session.ConnID {
			delete(shard.connIndex, old.ConnID)
		}
		if old.Conn != nil && old.Conn != session.Conn {
			_ = old.Conn.Close()
		}
	}
	shard.sessions[session.UID] = session
	if session.ConnID != "" {
		shard.connIndex[session.ConnID] = session.UID
	}
}

func (p *ConnectionPool) Delete(uid int64) {
	shard := p.shard(uid)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if old, ok := shard.sessions[uid]; ok {
		if old.ConnID != "" {
			delete(shard.connIndex, old.ConnID)
		}
		if old.Conn != nil {
			_ = old.Conn.Close()
		}
	}
	delete(shard.sessions, uid)
}

func (p *ConnectionPool) SweepExpired(now time.Time) []Session {
	var expired []Session
	for i := range p.shards {
		shard := &p.shards[i]
		shard.mu.Lock()
		for uid, session := range shard.sessions {
			if now.After(session.ExpireAt) {
				expired = append(expired, session)
				if session.ConnID != "" {
					delete(shard.connIndex, session.ConnID)
				}
				if session.Conn != nil {
					_ = session.Conn.Close()
				}
				delete(shard.sessions, uid)
			}
		}
		shard.mu.Unlock()
	}
	return expired
}

func (p *ConnectionPool) CloseAll() {
	for i := range p.shards {
		shard := &p.shards[i]
		shard.mu.Lock()
		for uid, session := range shard.sessions {
			if session.Conn != nil {
				_ = session.Conn.Close()
			}
			delete(shard.sessions, uid)
		}
		for connID := range shard.connIndex {
			delete(shard.connIndex, connID)
		}
		shard.mu.Unlock()
	}
}

func (p *ConnectionPool) shard(uid int64) *sessionShard {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(strconv.FormatInt(uid, 10)))
	return &p.shards[int(hasher.Sum32())%len(p.shards)]
}

type ConnectionManager struct {
	pool          *ConnectionPool
	store         GateConnStore
	gatewayAddr   string
	sessionTTL    time.Duration
	renewInterval time.Duration
	now           func() time.Time
}

func NewConnectionManager(store GateConnStore, gatewayAddr string, sessionTTL, renewInterval time.Duration) *ConnectionManager {
	if sessionTTL <= 0 {
		sessionTTL = 90 * time.Second
	}
	if renewInterval <= 0 {
		renewInterval = 30 * time.Second
	}
	return &ConnectionManager{
		pool:          NewConnectionPool(32),
		store:         store,
		gatewayAddr:   gatewayAddr,
		sessionTTL:    sessionTTL,
		renewInterval: renewInterval,
		now:           time.Now,
	}
}

func (m *ConnectionManager) Bind(ctx context.Context, req BindSessionRequest) (Session, error) {
	now := m.now().UTC()
	if req.GatewayAddr == "" {
		req.GatewayAddr = m.gatewayAddr
	}
	session := Session{
		UID:             req.UID,
		RoleID:          req.RoleID,
		ServerID:        req.ServerID,
		ConnID:          req.ConnID,
		GatewayAddr:     req.GatewayAddr,
		ClientIP:        req.ClientIP,
		DeviceID:        req.DeviceID,
		ConnectedAt:     now,
		LastHeartbeatAt: now,
		LastRenewAt:     now,
		ExpireAt:        now.Add(m.sessionTTL),
		Conn:            req.Conn,
	}
	if err := m.store.SetGateConn(ctx, m.toGateConn(session), m.sessionTTL); err != nil {
		return Session{}, err
	}
	m.pool.Set(session)
	return session, nil
}

func (m *ConnectionManager) Heartbeat(ctx context.Context, uid int64, connID string) (Session, error) {
	session, ok := m.pool.Get(uid)
	if !ok {
		return Session{}, ErrSessionNotFound
	}
	if connID != "" && session.ConnID != "" && connID != session.ConnID {
		return Session{}, ErrSessionMismatch
	}

	now := m.now().UTC()
	session.LastHeartbeatAt = now
	session.ExpireAt = now.Add(m.sessionTTL)
	if now.Sub(session.LastRenewAt) >= m.renewInterval {
		session.LastRenewAt = now
		if err := m.store.SetGateConn(ctx, m.toGateConn(session), m.sessionTTL); err != nil {
			return Session{}, err
		}
	}
	m.pool.Set(session)
	return session, nil
}

func (m *ConnectionManager) Disconnect(ctx context.Context, uid int64) error {
	m.pool.Delete(uid)
	return m.store.DeleteGateConn(ctx, uid)
}

func (m *ConnectionManager) Get(uid int64) (Session, bool) {
	return m.pool.Get(uid)
}

func (m *ConnectionManager) GetByConnID(connID string) (Session, bool) {
	return m.pool.GetByConnID(connID)
}

func (m *ConnectionManager) SweepExpired() []Session {
	return m.pool.SweepExpired(m.now().UTC())
}

func (m *ConnectionManager) CloseAll() {
	m.pool.CloseAll()
}

func (m *ConnectionManager) toGateConn(session Session) routing.GateConn {
	return routing.GateConn{
		UID:            session.UID,
		GatewayAddr:    session.GatewayAddr,
		ClientIP:       session.ClientIP,
		ConnID:         session.ConnID,
		ConnTime:       session.ConnectedAt,
		LastActiveTime: session.LastHeartbeatAt,
		DeviceID:       session.DeviceID,
		ExpireAt:       session.ExpireAt,
	}
}

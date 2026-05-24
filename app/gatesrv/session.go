package gatesrv

import "sync"

type SessionRouteCache struct {
	mu     sync.RWMutex
	routes map[int64]GameServerInstance
}

func NewSessionRouteCache() *SessionRouteCache {
	return &SessionRouteCache{routes: make(map[int64]GameServerInstance)}
}

func (c *SessionRouteCache) Get(uid int64) (GameServerInstance, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	route, ok := c.routes[uid]
	return route, ok
}

func (c *SessionRouteCache) Set(uid int64, route GameServerInstance) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.routes[uid] = route
}

func (c *SessionRouteCache) Delete(uid int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.routes, uid)
}

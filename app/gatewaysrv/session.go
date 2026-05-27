package gatewaysrv

import "sync"

type SessionRouteCache struct {
	mu     sync.RWMutex
	routes map[int64]PlayerServerInstance
}

func NewSessionRouteCache() *SessionRouteCache {
	return &SessionRouteCache{routes: make(map[int64]PlayerServerInstance)}
}

func (c *SessionRouteCache) Get(uid int64) (PlayerServerInstance, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	route, ok := c.routes[uid]
	return route, ok
}

func (c *SessionRouteCache) Set(uid int64, route PlayerServerInstance) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.routes[uid] = route
}

func (c *SessionRouteCache) Delete(uid int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.routes, uid)
}

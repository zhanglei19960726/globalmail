package globalmail

import "sync"

type singleflightCall struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

type singleflightGroup struct {
	mu sync.Mutex
	m  map[string]*singleflightCall
}

var globalMailCacheRebuilds singleflightGroup

func (g *singleflightGroup) Do(key string, fn func() (interface{}, error)) (interface{}, error, bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*singleflightCall)
	}
	if call, ok := g.m[key]; ok {
		g.mu.Unlock()
		call.wg.Wait()
		return call.val, call.err, true
	}
	call := &singleflightCall{}
	call.wg.Add(1)
	g.m[key] = call
	g.mu.Unlock()

	call.val, call.err = fn()
	call.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	return call.val, call.err, false
}

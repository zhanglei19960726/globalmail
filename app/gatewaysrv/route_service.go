package gatewaysrv

import (
	"context"
	"time"

	"globalmail/domain/routing"
)

type SrvRouterStore interface {
	GetSrvRouter(ctx context.Context, uid int64) (routing.SrvRouter, bool, error)
	SetSrvRouter(ctx context.Context, route routing.SrvRouter, ttl time.Duration) error
	DeleteSrvRouter(ctx context.Context, uid int64) error
}

type PlayerServerProvider interface {
	ListReadyPlayerServers(ctx context.Context) ([]PlayerServerInstance, error)
}

type RouteService struct {
	session      *SessionRouteCache
	store        SrvRouterStore
	provider     PlayerServerProvider
	router       *ConsistentHashRouter
	routeTTL     time.Duration
	virtualNodes int
	now          func() time.Time
}

func NewRouteService(store SrvRouterStore, provider PlayerServerProvider, routeTTL time.Duration, virtualNodes int) *RouteService {
	if routeTTL <= 0 {
		routeTTL = time.Minute
	}
	return &RouteService{
		session:      NewSessionRouteCache(),
		store:        store,
		provider:     provider,
		router:       NewConsistentHashRouter(nil, virtualNodes),
		routeTTL:     routeTTL,
		virtualNodes: virtualNodes,
		now:          time.Now,
	}
}

func (s *RouteService) Resolve(ctx context.Context, uid int64) (PlayerServerInstance, error) {
	if route, ok := s.session.Get(uid); ok {
		return route, nil
	}

	if stored, ok, err := s.store.GetSrvRouter(ctx, uid); err != nil {
		return PlayerServerInstance{}, err
	} else if ok && stored.ExpireAt.After(s.now()) {
		route := PlayerServerInstance{
			InstanceID: stored.InstanceID,
			GrpcAddr:   stored.GrpcAddr,
			FrpcAddr:   stored.FrpcAddr,
			Weight:     100,
		}
		s.session.Set(uid, route)
		return route, nil
	}

	instances, err := s.provider.ListReadyPlayerServers(ctx)
	if err != nil {
		return PlayerServerInstance{}, err
	}
	s.router.Update(instances)
	route, err := s.router.RouteUID(uid)
	if err != nil {
		return PlayerServerInstance{}, err
	}

	expireAt := s.now().Add(s.routeTTL)
	if err := s.store.SetSrvRouter(ctx, routing.SrvRouter{
		UID:        uid,
		SrvName:    "playersrv",
		InstanceID: route.InstanceID,
		FrpcAddr:   route.FrpcAddr,
		GrpcAddr:   route.GrpcAddr,
		Version:    1,
		ExpireAt:   expireAt,
	}, s.routeTTL); err != nil {
		return PlayerServerInstance{}, err
	}
	s.session.Set(uid, route)
	return route, nil
}

func (s *RouteService) Clear(ctx context.Context, uid int64) error {
	s.session.Delete(uid)
	return s.store.DeleteSrvRouter(ctx, uid)
}

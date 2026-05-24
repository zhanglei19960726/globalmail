package gatesrv

import (
	"context"

	"globalmail/api/rpc"
)

type UIDRouter interface {
	Resolve(ctx context.Context, uid int64) (GameServerInstance, error)
	Clear(ctx context.Context, uid int64) error
}

type Server struct {
	rpc.UnimplementedGateServiceServer
	router UIDRouter
}

func NewServer(router UIDRouter) *Server {
	return &Server{router: router}
}

func (s *Server) ResolveRoute(ctx context.Context, req *rpc.RouteRequest) (*rpc.RouteResponse, error) {
	route, err := s.router.Resolve(ctx, req.GetUid())
	if err != nil {
		return nil, err
	}
	return &rpc.RouteResponse{
		InstanceId: route.InstanceID,
		GrpcAddr:   route.GrpcAddr,
		FrpcAddr:   route.FrpcAddr,
		Weight:     int32(route.Weight),
	}, nil
}

func (s *Server) ClearRoute(ctx context.Context, req *rpc.RouteRequest) (*rpc.ClearRouteResponse, error) {
	if err := s.router.Clear(ctx, req.GetUid()); err != nil {
		return nil, err
	}
	return &rpc.ClearRouteResponse{Ok: true}, nil
}

package gatesrv

import (
	"context"
	"time"

	"globalmail/api/rpc"
)

type UIDRouter interface {
	Resolve(ctx context.Context, uid int64) (GameServerInstance, error)
	Clear(ctx context.Context, uid int64) error
}

type Server struct {
	rpc.UnimplementedGateServiceServer
	router      UIDRouter
	connections *ConnectionManager
}

func NewServer(router UIDRouter) *Server {
	return &Server{router: router}
}

func NewServerWithConnections(router UIDRouter, connections *ConnectionManager) *Server {
	return &Server{router: router, connections: connections}
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

func (s *Server) Heartbeat(ctx context.Context, req *rpc.HeartbeatRequest) (*rpc.HeartbeatResponse, error) {
	if s.connections == nil {
		return nil, ErrConnectionManagerRequired
	}
	session, err := s.connections.Heartbeat(ctx, req.GetUid(), req.GetConnId())
	if err != nil {
		return nil, err
	}
	return &rpc.HeartbeatResponse{
		Ok:              true,
		Seq:             req.GetSeq(),
		ServerTimestamp: timeNowUnixMilli(),
		ExpireAt:        formatGateTime(session.ExpireAt),
	}, nil
}

func timeNowUnixMilli() int64 {
	return time.Now().UTC().UnixMilli()
}

func formatGateTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

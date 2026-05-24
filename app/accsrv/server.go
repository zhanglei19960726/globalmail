package accsrv

import (
	"context"
	"time"

	"globalmail/api/rpc"
)

type Server struct {
	rpc.UnimplementedAccServiceServer
	service *Service
}

func NewServer(service *Service) *Server {
	return &Server{service: service}
}

func (s *Server) Login(ctx context.Context, req *rpc.LoginRequest) (*rpc.LoginResponse, error) {
	token, err := s.service.Login(ctx, req.GetUid(), int(req.GetServerId()))
	if err != nil {
		return nil, err
	}
	return &rpc.LoginResponse{
		Token:    token.Token,
		Uid:      token.UID,
		RoleId:   token.RoleID,
		ServerId: int32(token.ServerID),
		ExpireAt: token.ExpireAt.Format(time.RFC3339Nano),
	}, nil
}

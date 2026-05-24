package gatesrv

import (
	"context"
	"sync"

	"globalmail/api/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type CommandRouter interface {
	Resolve(ctx context.Context, uid int64) (GameServerInstance, error)
	Clear(ctx context.Context, uid int64) error
}

type GameCommandClient interface {
	Dispatch(ctx context.Context, req *rpc.CommandRequest, opts ...grpc.CallOption) (*rpc.CommandResponse, error)
}

type GameCommandClientFactory interface {
	ClientFor(ctx context.Context, instance GameServerInstance) (GameCommandClient, error)
}

type CommandForwarder struct {
	router  CommandRouter
	clients GameCommandClientFactory
}

func NewCommandForwarder(router CommandRouter, clients GameCommandClientFactory) *CommandForwarder {
	return &CommandForwarder{router: router, clients: clients}
}

func (f *CommandForwarder) Forward(ctx context.Context, req *rpc.CommandRequest) (*rpc.CommandResponse, error) {
	route, err := f.router.Resolve(ctx, req.GetUid())
	if err != nil {
		return nil, err
	}
	client, err := f.clients.ClientFor(ctx, route)
	if err != nil {
		_ = f.router.Clear(ctx, req.GetUid())
		return nil, err
	}
	resp, err := client.Dispatch(ctx, req)
	if err != nil {
		_ = f.router.Clear(ctx, req.GetUid())
		return nil, err
	}
	return resp, nil
}

type GRPCGameCommandClientFactory struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewGRPCGameCommandClientFactory() *GRPCGameCommandClientFactory {
	return &GRPCGameCommandClientFactory{conns: make(map[string]*grpc.ClientConn)}
}

func (f *GRPCGameCommandClientFactory) ClientFor(ctx context.Context, instance GameServerInstance) (GameCommandClient, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if conn, ok := f.conns[instance.GrpcAddr]; ok {
		return rpc.NewGameCommandServiceClient(conn), nil
	}
	conn, err := grpc.DialContext(ctx, instance.GrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	f.conns[instance.GrpcAddr] = conn
	return rpc.NewGameCommandServiceClient(conn), nil
}

func (f *GRPCGameCommandClientFactory) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var firstErr error
	for addr, conn := range f.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(f.conns, addr)
	}
	return firstErr
}

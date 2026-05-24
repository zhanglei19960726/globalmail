package gatesrv

import (
	"context"
	"errors"
	"testing"

	"globalmail/api/rpc"

	"google.golang.org/grpc"
)

type fakeCommandRouter struct {
	route   GameServerInstance
	cleared int64
}

func (r *fakeCommandRouter) Resolve(context.Context, int64) (GameServerInstance, error) {
	return r.route, nil
}

func (r *fakeCommandRouter) Clear(_ context.Context, uid int64) error {
	r.cleared = uid
	return nil
}

type fakeGameCommandClientFactory struct {
	client GameCommandClient
	err    error
}

func (f fakeGameCommandClientFactory) ClientFor(context.Context, GameServerInstance) (GameCommandClient, error) {
	return f.client, f.err
}

type fakeGameCommandClient struct {
	resp *rpc.CommandResponse
	err  error
}

func (c fakeGameCommandClient) Dispatch(context.Context, *rpc.CommandRequest, ...grpc.CallOption) (*rpc.CommandResponse, error) {
	return c.resp, c.err
}

func TestCommandForwarderForwardsToResolvedGameServer(t *testing.T) {
	router := &fakeCommandRouter{route: GameServerInstance{InstanceID: "game-1", GrpcAddr: "127.0.0.1:9001"}}
	forwarder := NewCommandForwarder(router, fakeGameCommandClientFactory{
		client: fakeGameCommandClient{resp: &rpc.CommandResponse{
			CommandId: rpc.CommandID_COMMAND_ID_MAIL_LIST_GLOBAL,
			Seq:       99,
			Code:      0,
		}},
	})

	resp, err := forwarder.Forward(context.Background(), &rpc.CommandRequest{
		CommandId: rpc.CommandID_COMMAND_ID_MAIL_LIST_GLOBAL,
		Uid:       10001,
		Seq:       99,
	})
	if err != nil {
		t.Fatalf("forward failed: %v", err)
	}
	if resp.GetCode() != 0 || resp.GetSeq() != 99 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if router.cleared != 0 {
		t.Fatalf("route should not be cleared on success, got %d", router.cleared)
	}
}

func TestCommandForwarderClearsRouteOnDispatchFailure(t *testing.T) {
	router := &fakeCommandRouter{route: GameServerInstance{InstanceID: "game-1", GrpcAddr: "127.0.0.1:9001"}}
	forwarder := NewCommandForwarder(router, fakeGameCommandClientFactory{
		client: fakeGameCommandClient{err: errors.New("dispatch failed")},
	})

	_, err := forwarder.Forward(context.Background(), &rpc.CommandRequest{
		CommandId: rpc.CommandID_COMMAND_ID_MAIL_LIST_GLOBAL,
		Uid:       10001,
	})
	if err == nil {
		t.Fatal("expected forward error")
	}
	if router.cleared != 10001 {
		t.Fatalf("expected route cleared for uid 10001, got %d", router.cleared)
	}
}

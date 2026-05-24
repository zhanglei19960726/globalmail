package accsrv

import (
	"context"
	"testing"
	"time"

	"globalmail/api/rpc"
)

func TestServerLoginReturnsToken(t *testing.T) {
	store := &fakeLoginTokenStore{}
	server := NewServer(NewService(store, fakeGameLoginClient{resp: &rpc.LoginResponse{
		Uid:      10001,
		RoleId:   20001,
		ServerId: 1,
	}}, time.Minute))

	resp, err := server.Login(context.Background(), &rpc.LoginRequest{Uid: 10001, ServerId: 1})
	if err != nil {
		t.Fatalf("login failed: %v", err)
	}

	if resp.GetToken() == "" {
		t.Fatal("expected token response")
	}
}

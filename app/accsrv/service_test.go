package accsrv

import (
	"context"
	"testing"
	"time"

	"globalmail/api/rpc"
	"globalmail/domain/routing"

	"google.golang.org/grpc"
)

type fakeLoginTokenStore struct {
	token routing.LoginToken
	ttl   time.Duration
}

func (s *fakeLoginTokenStore) SetLoginToken(_ context.Context, token routing.LoginToken, ttl time.Duration) error {
	s.token = token
	s.ttl = ttl
	return nil
}

type fakeGameLoginClient struct {
	resp *rpc.LoginResponse
}

func (c fakeGameLoginClient) Login(context.Context, *rpc.LoginRequest, ...grpc.CallOption) (*rpc.LoginResponse, error) {
	return c.resp, nil
}

func TestServiceLoginWritesLoginToken(t *testing.T) {
	now := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	store := &fakeLoginTokenStore{}
	service := NewService(store, fakeGameLoginClient{resp: &rpc.LoginResponse{
		Uid:      10001,
		RoleId:   20001,
		ServerId: 1,
	}}, 2*time.Minute)
	service.now = func() time.Time { return now }

	token, err := service.Login(context.Background(), 10001, 1)
	if err != nil {
		t.Fatalf("login failed: %v", err)
	}
	if token.Token == "" {
		t.Fatal("expected generated token")
	}
	if token.UID != 10001 || token.RoleID != 20001 || token.ServerID != 1 {
		t.Fatalf("unexpected token payload: %+v", token)
	}
	if !token.ExpireAt.Equal(now.Add(2 * time.Minute)) {
		t.Fatalf("unexpected expire time: %s", token.ExpireAt)
	}
	if store.ttl != 2*time.Minute {
		t.Fatalf("unexpected ttl: %s", store.ttl)
	}
	if store.token.Token != token.Token {
		t.Fatal("expected token persisted to store")
	}
}

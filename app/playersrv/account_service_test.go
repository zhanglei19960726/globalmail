package playersrv

import (
	"context"
	"testing"

	"globalmail/api/rpc"
)

func TestAccountServiceLoginCreatesUser(t *testing.T) {
	service := NewAccountService(NewMemoryUserRepository())

	resp, err := service.Login(context.Background(), &rpc.LoginRequest{Uid: 10001, ServerId: 1})
	if err != nil {
		t.Fatalf("login failed: %v", err)
	}
	if resp.GetUid() != 10001 || resp.GetRoleId() == 0 || resp.GetServerId() != 1 {
		t.Fatalf("unexpected identity: %+v", resp)
	}
	if !resp.IsNewUser {
		t.Fatal("expected new user")
	}
}

func TestAccountServiceLoginReturnsExistingUser(t *testing.T) {
	service := NewAccountService(NewMemoryUserRepository())
	first, err := service.Login(context.Background(), &rpc.LoginRequest{Uid: 10001, ServerId: 1})
	if err != nil {
		t.Fatalf("first login failed: %v", err)
	}
	second, err := service.Login(context.Background(), &rpc.LoginRequest{Uid: 10001, ServerId: 1})
	if err != nil {
		t.Fatalf("second login failed: %v", err)
	}
	if first.GetRoleId() != second.GetRoleId() {
		t.Fatalf("expected same role id, got %d and %d", first.GetRoleId(), second.GetRoleId())
	}
	if second.IsNewUser {
		t.Fatal("expected existing user")
	}
}

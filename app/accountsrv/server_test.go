package accountsrv

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"globalmail/api/rpc"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestServerLoginReturnsToken(t *testing.T) {
	store := &fakeLoginTokenStore{}
	server := NewServer(NewService(store, fakeGameLoginClient{resp: &rpc.LoginResponse{
		Uid:       10001,
		RoleId:    20001,
		ServerId:  1,
		IsNewUser: true,
	}}, time.Minute))

	resp, err := server.Login(context.Background(), &rpc.LoginRequest{Uid: 10001, ServerId: 1})
	if err != nil {
		t.Fatalf("login failed: %v", err)
	}

	if resp.GetToken() == "" {
		t.Fatal("expected token response")
	}
	if !resp.GetIsNewUser() {
		t.Fatal("expected is_new_user from playersrv response")
	}
}

func TestServerHTTPLoginUsesProtoJSON(t *testing.T) {
	server := NewServer(NewService(&fakeLoginTokenStore{}, fakeGameLoginClient{resp: &rpc.LoginResponse{
		Uid:      10001,
		RoleId:   20001,
		ServerId: 1,
	}}, time.Minute))
	body, err := protojson.Marshal(&rpc.LoginRequest{Uid: 10001, ServerId: 1})
	if err != nil {
		t.Fatalf("marshal login request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}
	var resp rpc.LoginResponse
	if err := protojson.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal login response: %v", err)
	}
	if resp.GetToken() == "" || resp.GetUid() != 10001 {
		t.Fatalf("unexpected login response: %+v", &resp)
	}
}

func TestServerHTTPLoginUsesBinaryProto(t *testing.T) {
	server := NewServer(NewService(&fakeLoginTokenStore{}, fakeGameLoginClient{resp: &rpc.LoginResponse{
		Uid:      10001,
		RoleId:   20001,
		ServerId: 1,
	}}, time.Minute))
	body, err := proto.Marshal(&rpc.LoginRequest{Uid: 10001, ServerId: 1})
	if err != nil {
		t.Fatalf("marshal login request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}
	var resp rpc.LoginResponse
	if err := proto.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal login response: %v", err)
	}
	if resp.GetToken() == "" || resp.GetUid() != 10001 {
		t.Fatalf("unexpected login response: %+v", &resp)
	}
}

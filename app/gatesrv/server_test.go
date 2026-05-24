package gatesrv

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeUIDRouter struct {
	route GameServerInstance
}

func (r fakeUIDRouter) Resolve(context.Context, int64) (GameServerInstance, error) {
	return r.route, nil
}

func TestServerRouteReturnsGameServer(t *testing.T) {
	server := NewServer(fakeUIDRouter{route: GameServerInstance{
		InstanceID: "game-1",
		GrpcAddr:   "127.0.0.1:9001",
	}})
	req := httptest.NewRequest(http.MethodGet, "/route?uid=10001", nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "game-1") {
		t.Fatalf("expected route response, got %s", rec.Body.String())
	}
}

func TestServerRouteRejectsInvalidUID(t *testing.T) {
	server := NewServer(fakeUIDRouter{})
	req := httptest.NewRequest(http.MethodGet, "/route?uid=bad", nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

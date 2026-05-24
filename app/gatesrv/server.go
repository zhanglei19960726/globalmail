package gatesrv

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
)

type UIDRouter interface {
	Resolve(ctx context.Context, uid int64) (GameServerInstance, error)
}

type Server struct {
	router UIDRouter
}

func NewServer(router UIDRouter) *Server {
	return &Server{router: router}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/route", s.handleRoute)
	return mux
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleRoute(w http.ResponseWriter, r *http.Request) {
	uid, err := strconv.ParseInt(r.URL.Query().Get("uid"), 10, 64)
	if err != nil || uid <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid uid"})
		return
	}
	route, err := s.router.Resolve(r.Context(), uid)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, route)
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

package accsrv

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"globalmail/api/rpc"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	service *Service
}

func NewServer(service *Service) *Server {
	return &Server{service: service}
}

func (s *Server) Login(ctx context.Context, req *rpc.LoginRequest) (*rpc.LoginResponse, error) {
	result, err := s.service.Login(ctx, req.GetUid(), int(req.GetServerId()))
	if err != nil {
		return nil, err
	}
	token := result.Token
	return &rpc.LoginResponse{
		Token:     token.Token,
		Uid:       token.UID,
		RoleId:    token.RoleID,
		ServerId:  int32(token.ServerID),
		IsNewUser: result.IsNewUser,
		ExpireAt:  token.ExpireAt.Format(time.RFC3339Nano),
	}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/login", s.handleLogin)
	return mux
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	req := &rpc.LoginRequest{}
	if err := readProtoHTTP(r, req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := s.Login(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := writeProtoHTTP(w, r, resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func readProtoHTTP(r *http.Request, msg proto.Message) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	if isBinaryProto(r.Header.Get("Content-Type")) {
		return proto.Unmarshal(body, msg)
	}
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(body, msg)
}

func writeProtoHTTP(w http.ResponseWriter, r *http.Request, msg proto.Message) error {
	if acceptsBinaryProto(r.Header.Get("Accept")) {
		out, err := proto.Marshal(msg)
		if err != nil {
			return err
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, err = w.Write(out)
		return err
	}
	out, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(msg)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(out)
	return err
}

func isBinaryProto(contentType string) bool {
	contentType = strings.ToLower(contentType)
	return strings.Contains(contentType, "application/x-protobuf") ||
		strings.Contains(contentType, "application/protobuf")
}

func acceptsBinaryProto(accept string) bool {
	return isBinaryProto(accept)
}

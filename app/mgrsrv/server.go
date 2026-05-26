package mgrsrv

import (
	"encoding/json"
	"net/http"
	"time"

	"globalmail/domain/globalmail"
)

type Server struct {
	publisher *globalmail.PublisherService
	mux       *http.ServeMux
}

func NewServer(publisher *globalmail.PublisherService) *Server {
	s := &Server{
		publisher: publisher,
		mux:       http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("/admin/global-mails", s.handlePublishGlobalMail)
}

type publishGlobalMailRequest struct {
	IdempotencyKey string                 `json:"idempotency_key"`
	OperatorID     int64                  `json:"opr_id"`
	Title          string                 `json:"title"`
	Content        string                 `json:"content"`
	Sender         string                 `json:"sender"`
	Category       string                 `json:"mail_category"`
	StartTime      time.Time              `json:"start_time"`
	ExpireTime     time.Time              `json:"expire_time"`
	Conditions     []globalmail.Condition `json:"conditions"`
}

func (s *Server) handlePublishGlobalMail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req publishGlobalMailRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	idempotencyKey := req.IdempotencyKey
	if idempotencyKey == "" {
		idempotencyKey = r.Header.Get("Idempotency-Key")
	}

	mail, err := s.publisher.Publish(r.Context(), globalmail.PublishCommand{
		Mail: globalmail.GlobalMail{
			OperatorID: req.OperatorID,
			Title:      req.Title,
			Content:    req.Content,
			Sender:     req.Sender,
			Category:   req.Category,
			StartTime:  req.StartTime,
			ExpireTime: req.ExpireTime,
		},
		Conditions:     req.Conditions,
		Action:         "publish",
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(mail)
}

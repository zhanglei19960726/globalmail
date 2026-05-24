package accsrv

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"globalmail/api/rpc"
	"globalmail/domain/routing"

	"google.golang.org/grpc"
)

type LoginTokenStore interface {
	SetLoginToken(ctx context.Context, token routing.LoginToken, ttl time.Duration) error
}

type GameLoginClient interface {
	Login(ctx context.Context, in *rpc.LoginRequest, opts ...grpc.CallOption) (*rpc.LoginResponse, error)
}

type Service struct {
	store LoginTokenStore
	games GameLoginClient
	ttl   time.Duration
	now   func() time.Time
}

func NewService(store LoginTokenStore, games GameLoginClient, ttl time.Duration) *Service {
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	return &Service{
		store: store,
		games: games,
		ttl:   ttl,
		now:   time.Now,
	}
}

func (s *Service) Login(ctx context.Context, uid int64, serverID int) (routing.LoginToken, error) {
	login, err := s.games.Login(ctx, &rpc.LoginRequest{
		Uid:      uid,
		ServerId: int32(serverID),
	})
	if err != nil {
		return routing.LoginToken{}, err
	}
	token, err := randomToken()
	if err != nil {
		return routing.LoginToken{}, err
	}
	value := routing.LoginToken{
		Token:    token,
		UID:      login.GetUid(),
		RoleID:   login.GetRoleId(),
		ServerID: int(login.GetServerId()),
		ExpireAt: s.now().UTC().Add(s.ttl),
	}
	if err := s.store.SetLoginToken(ctx, value, s.ttl); err != nil {
		return routing.LoginToken{}, err
	}
	return value, nil
}

func randomToken() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

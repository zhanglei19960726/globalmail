package playersrv

import (
	"context"
	"errors"
	"sync"

	"globalmail/api/rpc"
)

var ErrInvalidLoginRequest = errors.New("invalid login request")

type UserIdentity struct {
	UID      int64
	RoleID   int64
	ServerID int
}

type UserRepository interface {
	GetOrCreateUser(ctx context.Context, uid int64, serverID int) (UserIdentity, bool, error)
}

type AccountService struct {
	rpc.UnimplementedPlayerServiceServer
	users UserRepository
}

func NewAccountService(users UserRepository) *AccountService {
	return &AccountService{users: users}
}

func (s *AccountService) Login(ctx context.Context, req *rpc.LoginRequest) (*rpc.LoginResponse, error) {
	if req.GetUid() <= 0 || req.GetServerId() <= 0 {
		return nil, ErrInvalidLoginRequest
	}
	identity, isNew, err := s.users.GetOrCreateUser(ctx, req.GetUid(), int(req.GetServerId()))
	if err != nil {
		return nil, err
	}
	return &rpc.LoginResponse{
		Uid:       identity.UID,
		RoleId:    identity.RoleID,
		ServerId:  int32(identity.ServerID),
		IsNewUser: isNew,
	}, nil
}

type MemoryUserRepository struct {
	mu     sync.Mutex
	nextID int64
	users  map[int64]UserIdentity
}

func NewMemoryUserRepository() *MemoryUserRepository {
	return &MemoryUserRepository{
		nextID: 100000,
		users:  make(map[int64]UserIdentity),
	}
}

func (r *MemoryUserRepository) GetOrCreateUser(_ context.Context, uid int64, serverID int) (UserIdentity, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if user, ok := r.users[uid]; ok {
		return user, false, nil
	}
	r.nextID++
	user := UserIdentity{
		UID:      uid,
		RoleID:   r.nextID,
		ServerID: serverID,
	}
	r.users[uid] = user
	return user, true, nil
}

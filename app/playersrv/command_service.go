package playersrv

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"globalmail/api/rpc"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var ErrCommandAlreadyRegistered = errors.New("command already registered")

type CommandHandler func(ctx context.Context, req *rpc.CommandRequest) (*anypb.Any, error)

var ErrCommandIdempotencyConflict = errors.New("command idempotency key reused with different payload")

type CommandRegistration struct {
	Definition *rpc.CommandDefinition
	Handler    CommandHandler
}

type CommandRegistry struct {
	handlers map[rpc.CommandID]CommandRegistration
}

func NewCommandRegistry() *CommandRegistry {
	return &CommandRegistry{handlers: make(map[rpc.CommandID]CommandRegistration)}
}

func (r *CommandRegistry) Register(definition *rpc.CommandDefinition, handler CommandHandler) error {
	if definition == nil || definition.GetCommandId() == rpc.CommandID_COMMAND_ID_UNSPECIFIED {
		return errors.New("command definition is required")
	}
	if handler == nil {
		return errors.New("command handler is required")
	}
	if _, ok := r.handlers[definition.GetCommandId()]; ok {
		return ErrCommandAlreadyRegistered
	}
	r.handlers[definition.GetCommandId()] = CommandRegistration{
		Definition: definition,
		Handler:    handler,
	}
	return nil
}

func (r *CommandRegistry) Dispatch(ctx context.Context, req *rpc.CommandRequest) (*rpc.CommandResponse, error) {
	registration, ok := r.handlers[req.GetCommandId()]
	if !ok {
		return &rpc.CommandResponse{
			CommandId: req.GetCommandId(),
			Seq:       req.GetSeq(),
			Code:      404,
			Message:   "unknown command",
		}, nil
	}
	payload, err := registration.Handler(ctx, req)
	if err != nil {
		return nil, err
	}
	return &rpc.CommandResponse{
		CommandId: req.GetCommandId(),
		Seq:       req.GetSeq(),
		Code:      0,
		Message:   "ok",
		Payload:   payload,
	}, nil
}

func (r *CommandRegistry) Definitions() []*rpc.CommandDefinition {
	definitions := make([]*rpc.CommandDefinition, 0, len(r.handlers))
	for _, registration := range r.handlers {
		definitions = append(definitions, registration.Definition)
	}
	sort.Slice(definitions, func(i, j int) bool {
		return definitions[i].GetCommandId() < definitions[j].GetCommandId()
	})
	return definitions
}

type IdempotentCommandDispatcher struct {
	next    CommandDispatcher
	store   CommandIdempotencyStore
	ttl     time.Duration
	records map[string]commandIdempotencyRecord
	mu      sync.Mutex
}

type CommandIdempotencyStore interface {
	GetCommandIdempotency(ctx context.Context, key string) (requestHash string, response *rpc.CommandResponse, ok bool, err error)
	SaveCommandIdempotency(ctx context.Context, key string, requestHash string, response *rpc.CommandResponse, ttl time.Duration) error
}

type commandIdempotencyRecord struct {
	hash     string
	response *rpc.CommandResponse
}

func NewIdempotentCommandDispatcher(next CommandDispatcher) *IdempotentCommandDispatcher {
	return &IdempotentCommandDispatcher{
		next:    next,
		records: make(map[string]commandIdempotencyRecord),
	}
}

func NewIdempotentCommandDispatcherWithStore(next CommandDispatcher, store CommandIdempotencyStore, ttl time.Duration) *IdempotentCommandDispatcher {
	dispatcher := NewIdempotentCommandDispatcher(next)
	dispatcher.store = store
	dispatcher.ttl = ttl
	return dispatcher
}

func (d *IdempotentCommandDispatcher) Dispatch(ctx context.Context, req *rpc.CommandRequest) (*rpc.CommandResponse, error) {
	if req.GetUid() == 0 || req.GetSeq() == 0 {
		return d.next.Dispatch(ctx, req)
	}
	key := commandIdempotencyKey(req)
	hash, err := commandRequestHash(req)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	if record, ok := d.records[key]; ok {
		d.mu.Unlock()
		if record.hash != hash {
			return commandIdempotencyConflictResponse(req), nil
		}
		return proto.Clone(record.response).(*rpc.CommandResponse), nil
	}
	d.mu.Unlock()

	if d.store != nil {
		storedHash, storedResp, ok, err := d.store.GetCommandIdempotency(ctx, key)
		if err != nil {
			return nil, err
		}
		if ok {
			if storedHash != hash {
				return commandIdempotencyConflictResponse(req), nil
			}
			d.remember(key, hash, storedResp)
			return proto.Clone(storedResp).(*rpc.CommandResponse), nil
		}
	}

	resp, err := d.next.Dispatch(ctx, req)
	if err != nil {
		return nil, err
	}
	d.remember(key, hash, resp)
	if d.store != nil {
		if err := d.store.SaveCommandIdempotency(ctx, key, hash, resp, d.ttl); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (d *IdempotentCommandDispatcher) remember(key string, hash string, resp *rpc.CommandResponse) {
	d.mu.Lock()
	d.records[key] = commandIdempotencyRecord{
		hash:     hash,
		response: proto.Clone(resp).(*rpc.CommandResponse),
	}
	d.mu.Unlock()
}

func commandIdempotencyKey(req *rpc.CommandRequest) string {
	return fmt.Sprintf("%d:%d:%d", req.GetUid(), req.GetCommandId(), req.GetSeq())
}

func commandRequestHash(req *rpc.CommandRequest) (string, error) {
	payload, err := proto.Marshal(req.GetPayload())
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("%x", sum[:]), nil
}

func commandIdempotencyConflictResponse(req *rpc.CommandRequest) *rpc.CommandResponse {
	return &rpc.CommandResponse{
		CommandId: req.GetCommandId(),
		Seq:       req.GetSeq(),
		Code:      409,
		Message:   ErrCommandIdempotencyConflict.Error(),
	}
}

type CommandRPCServer struct {
	rpc.UnimplementedPlayerCommandServiceServer
	registry   *CommandRegistry
	dispatcher CommandDispatcher
}

func NewCommandRPCServer(registry *CommandRegistry) *CommandRPCServer {
	return NewCommandRPCServerWithDispatcher(registry, registry)
}

func NewCommandRPCServerWithDispatcher(registry *CommandRegistry, dispatcher CommandDispatcher) *CommandRPCServer {
	if dispatcher == nil {
		dispatcher = registry
	}
	return &CommandRPCServer{registry: registry, dispatcher: dispatcher}
}

func (s *CommandRPCServer) Dispatch(ctx context.Context, req *rpc.CommandRequest) (*rpc.CommandResponse, error) {
	return s.dispatcher.Dispatch(ctx, req)
}

func (s *CommandRPCServer) ListCommands(context.Context, *rpc.ListCommandsRequest) (*rpc.ListCommandsResponse, error) {
	return &rpc.ListCommandsResponse{Commands: s.registry.Definitions()}, nil
}

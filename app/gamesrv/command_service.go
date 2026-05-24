package gamesrv

import (
	"context"
	"errors"
	"sort"

	"globalmail/api/rpc"

	"google.golang.org/protobuf/types/known/anypb"
)

var ErrCommandAlreadyRegistered = errors.New("command already registered")

type CommandHandler func(ctx context.Context, req *rpc.CommandRequest) (*anypb.Any, error)

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

type CommandRPCServer struct {
	rpc.UnimplementedGameCommandServiceServer
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

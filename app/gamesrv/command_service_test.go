package gamesrv

import (
	"context"
	"testing"
	"time"

	"globalmail/api/rpc"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestCommandRegistryDispatchUnknownCommand(t *testing.T) {
	registry := NewCommandRegistry()

	resp, err := registry.Dispatch(context.Background(), &rpc.CommandRequest{
		CommandId: rpc.CommandID_COMMAND_ID_MAIL_LIST_GLOBAL,
		Seq:       10,
	})
	if err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}
	if resp.GetCode() != 404 || resp.GetSeq() != 10 {
		t.Fatalf("expected unknown command response, got %+v", resp)
	}
}

func TestCommandRegistryListCommandsSorted(t *testing.T) {
	registry := NewCommandRegistry()
	if err := registry.Register(commandDefinition(rpc.CommandID_COMMAND_ID_MAIL_DELETE, "mail.delete", "req", "resp"), func(context.Context, *rpc.CommandRequest) (*anypb.Any, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("register delete failed: %v", err)
	}
	if err := registry.Register(commandDefinition(rpc.CommandID_COMMAND_ID_MAIL_LIST_GLOBAL, "mail.list_global", "req", "resp"), func(context.Context, *rpc.CommandRequest) (*anypb.Any, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("register list failed: %v", err)
	}

	definitions := registry.Definitions()
	if len(definitions) != 2 {
		t.Fatalf("expected two definitions, got %d", len(definitions))
	}
	if definitions[0].GetCommandId() != rpc.CommandID_COMMAND_ID_MAIL_LIST_GLOBAL {
		t.Fatalf("expected sorted command definitions, got %+v", definitions)
	}
}

func TestIdempotentCommandDispatcherReplaysResponse(t *testing.T) {
	registry := NewCommandRegistry()
	calls := 0
	if err := registry.Register(commandDefinition(rpc.CommandID_COMMAND_ID_MAIL_CLAIM, "mail.claim", "req", "resp"), func(context.Context, *rpc.CommandRequest) (*anypb.Any, error) {
		calls++
		return anypb.New(wrapperspb.String("claimed"))
	}); err != nil {
		t.Fatalf("register claim failed: %v", err)
	}
	dispatcher := NewIdempotentCommandDispatcher(registry)
	payload, _ := anypb.New(wrapperspb.String("same"))
	req := &rpc.CommandRequest{
		CommandId: rpc.CommandID_COMMAND_ID_MAIL_CLAIM,
		Uid:       10001,
		Seq:       7,
		Payload:   payload,
	}

	first, err := dispatcher.Dispatch(context.Background(), req)
	if err != nil {
		t.Fatalf("first dispatch failed: %v", err)
	}
	second, err := dispatcher.Dispatch(context.Background(), req)
	if err != nil {
		t.Fatalf("second dispatch failed: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected handler to be called once, got %d", calls)
	}
	if first.GetCode() != 0 || second.GetCode() != 0 || second.GetPayload() == nil {
		t.Fatalf("unexpected responses: first=%+v second=%+v", first, second)
	}
}

func TestIdempotentCommandDispatcherRejectsConflictingPayload(t *testing.T) {
	registry := NewCommandRegistry()
	if err := registry.Register(commandDefinition(rpc.CommandID_COMMAND_ID_MAIL_CLAIM, "mail.claim", "req", "resp"), func(context.Context, *rpc.CommandRequest) (*anypb.Any, error) {
		return anypb.New(wrapperspb.String("claimed"))
	}); err != nil {
		t.Fatalf("register claim failed: %v", err)
	}
	dispatcher := NewIdempotentCommandDispatcher(registry)
	firstPayload, _ := anypb.New(wrapperspb.String("one"))
	secondPayload, _ := anypb.New(wrapperspb.String("two"))
	req := &rpc.CommandRequest{
		CommandId: rpc.CommandID_COMMAND_ID_MAIL_CLAIM,
		Uid:       10001,
		Seq:       7,
		Payload:   firstPayload,
	}
	if _, err := dispatcher.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("first dispatch failed: %v", err)
	}
	req.Payload = secondPayload
	resp, err := dispatcher.Dispatch(context.Background(), req)
	if err != nil {
		t.Fatalf("second dispatch failed: %v", err)
	}
	if resp.GetCode() != 409 {
		t.Fatalf("expected conflict response, got %+v", resp)
	}
}

func TestIdempotentCommandDispatcherReplaysStoredResponse(t *testing.T) {
	store := &fakeCommandIdempotencyStore{records: map[string]commandIdempotencyRecord{}}
	registry := NewCommandRegistry()
	calls := 0
	if err := registry.Register(commandDefinition(rpc.CommandID_COMMAND_ID_MAIL_CLAIM, "mail.claim", "req", "resp"), func(context.Context, *rpc.CommandRequest) (*anypb.Any, error) {
		calls++
		return anypb.New(wrapperspb.String("claimed"))
	}); err != nil {
		t.Fatalf("register claim failed: %v", err)
	}
	payload, _ := anypb.New(wrapperspb.String("same"))
	req := &rpc.CommandRequest{
		CommandId: rpc.CommandID_COMMAND_ID_MAIL_CLAIM,
		Uid:       10001,
		Seq:       7,
		Payload:   payload,
	}
	firstDispatcher := NewIdempotentCommandDispatcherWithStore(registry, store, time.Minute)
	if _, err := firstDispatcher.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("first dispatch failed: %v", err)
	}
	secondDispatcher := NewIdempotentCommandDispatcherWithStore(registry, store, time.Minute)
	resp, err := secondDispatcher.Dispatch(context.Background(), req)
	if err != nil {
		t.Fatalf("second dispatch failed: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected persisted replay without handler call, got %d calls", calls)
	}
	if resp.GetCode() != 0 || resp.GetPayload() == nil {
		t.Fatalf("unexpected replay response: %+v", resp)
	}
}

type fakeCommandIdempotencyStore struct {
	records map[string]commandIdempotencyRecord
}

func (f *fakeCommandIdempotencyStore) GetCommandIdempotency(_ context.Context, key string) (string, *rpc.CommandResponse, bool, error) {
	record, ok := f.records[key]
	return record.hash, record.response, ok, nil
}

func (f *fakeCommandIdempotencyStore) SaveCommandIdempotency(_ context.Context, key string, requestHash string, response *rpc.CommandResponse, _ time.Duration) error {
	f.records[key] = commandIdempotencyRecord{
		hash:     requestHash,
		response: response,
	}
	return nil
}

package gamesrv

import (
	"context"
	"testing"

	"globalmail/api/rpc"

	"google.golang.org/protobuf/types/known/anypb"
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

package playersrv

import (
	"context"

	"globalmail/api/rpc"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func RegisterMailCommandHandlers(registry *CommandRegistry, mail *MailRPCServer) error {
	registrations := []struct {
		definition *rpc.CommandDefinition
		handler    CommandHandler
	}{
		{
			definition: commandDefinition(
				rpc.CommandID_COMMAND_ID_MAIL_LIST_GLOBAL,
				"mail.list_global",
				"globalmail.rpc.ListGlobalMailsRequest",
				"globalmail.rpc.ListGlobalMailsResponse",
			),
			handler: func(ctx context.Context, req *rpc.CommandRequest) (*anypb.Any, error) {
				var payload rpc.ListGlobalMailsRequest
				if err := unmarshalCommandPayload(req, &payload); err != nil {
					return nil, err
				}
				ensureCommandProfile(req, &payload)
				resp, err := mail.ListGlobalMails(ctx, &payload)
				if err != nil {
					return nil, err
				}
				return anypb.New(resp)
			},
		},
		{
			definition: commandDefinition(
				rpc.CommandID_COMMAND_ID_MAIL_MARK_READ,
				"mail.mark_read",
				"globalmail.rpc.GlobalMailStateRequest",
				"globalmail.rpc.GlobalMailStateResponse",
			),
			handler: func(ctx context.Context, req *rpc.CommandRequest) (*anypb.Any, error) {
				var payload rpc.GlobalMailStateRequest
				if err := unmarshalCommandPayload(req, &payload); err != nil {
					return nil, err
				}
				ensureCommandProfile(req, &payload)
				resp, err := mail.MarkGlobalMailRead(ctx, &payload)
				if err != nil {
					return nil, err
				}
				return anypb.New(resp)
			},
		},
		{
			definition: commandDefinition(
				rpc.CommandID_COMMAND_ID_MAIL_CLAIM,
				"mail.claim",
				"globalmail.rpc.ClaimGlobalMailRequest",
				"globalmail.rpc.GlobalMailStateResponse",
			),
			handler: func(ctx context.Context, req *rpc.CommandRequest) (*anypb.Any, error) {
				var payload rpc.ClaimGlobalMailRequest
				if err := unmarshalCommandPayload(req, &payload); err != nil {
					return nil, err
				}
				ensureCommandProfile(req, &payload)
				resp, err := mail.ClaimGlobalMail(ctx, &payload)
				if err != nil {
					return nil, err
				}
				return anypb.New(resp)
			},
		},
		{
			definition: commandDefinition(
				rpc.CommandID_COMMAND_ID_MAIL_DELETE,
				"mail.delete",
				"globalmail.rpc.GlobalMailStateRequest",
				"globalmail.rpc.GlobalMailStateResponse",
			),
			handler: func(ctx context.Context, req *rpc.CommandRequest) (*anypb.Any, error) {
				var payload rpc.GlobalMailStateRequest
				if err := unmarshalCommandPayload(req, &payload); err != nil {
					return nil, err
				}
				ensureCommandProfile(req, &payload)
				resp, err := mail.DeleteGlobalMail(ctx, &payload)
				if err != nil {
					return nil, err
				}
				return anypb.New(resp)
			},
		},
	}
	for _, registration := range registrations {
		if err := registry.Register(registration.definition, registration.handler); err != nil {
			return err
		}
	}
	return nil
}

func commandDefinition(commandID rpc.CommandID, name, requestType, responseType string) *rpc.CommandDefinition {
	return &rpc.CommandDefinition{
		CommandId:    commandID,
		Name:         name,
		RequestType:  requestType,
		ResponseType: responseType,
	}
}

func unmarshalCommandPayload(req *rpc.CommandRequest, payload proto.Message) error {
	if req.GetPayload() == nil {
		return nil
	}
	return req.GetPayload().UnmarshalTo(payload)
}

func ensureCommandProfile(req *rpc.CommandRequest, payload interface{}) {
	profile := &rpc.UserProfile{
		Uid:      req.GetUid(),
		RoleId:   req.GetRoleId(),
		ServerId: req.GetServerId(),
	}
	switch value := payload.(type) {
	case *rpc.ListGlobalMailsRequest:
		if value.Profile == nil {
			value.Profile = profile
			return
		}
		applyCommandIdentity(value.Profile, profile)
	case *rpc.GlobalMailStateRequest:
		if value.Profile == nil {
			value.Profile = profile
			return
		}
		applyCommandIdentity(value.Profile, profile)
	case *rpc.ClaimGlobalMailRequest:
		if value.Profile == nil {
			value.Profile = profile
			return
		}
		applyCommandIdentity(value.Profile, profile)
	}
}

func applyCommandIdentity(target, source *rpc.UserProfile) {
	target.Uid = source.GetUid()
	target.RoleId = source.GetRoleId()
	target.ServerId = source.GetServerId()
}

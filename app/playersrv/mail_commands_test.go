package playersrv

import (
	"context"
	"testing"
	"time"

	"globalmail/api/rpc"
	"globalmail/domain/globalmail"

	"google.golang.org/protobuf/types/known/anypb"
)

func TestMailCommandDispatchListGlobalMails(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeMailRepository{
		mails: []globalmail.GlobalMail{
			{
				ID:         10,
				Title:      "mail",
				Status:     globalmail.MailStatusPublished,
				StartTime:  now.Add(-time.Hour),
				ExpireTime: now.Add(time.Hour),
			},
		},
		states: map[int64]globalmail.UserGlobalMailState{},
	}
	cacheRepo := &fakeCacheRepository{version: 1}
	localCache := globalmail.NewLocalCache(repo, cacheRepo)
	if err := localCache.ForceRefresh(context.Background()); err != nil {
		t.Fatalf("refresh cache failed: %v", err)
	}
	mailRPC := NewMailRPCServer(NewPlayerMailService(repo, localCache))
	registry := NewCommandRegistry()
	if err := RegisterMailCommandHandlers(registry, mailRPC); err != nil {
		t.Fatalf("register mail commands failed: %v", err)
	}

	payload, err := anypb.New(&rpc.ListGlobalMailsRequest{})
	if err != nil {
		t.Fatalf("pack payload failed: %v", err)
	}
	resp, err := registry.Dispatch(context.Background(), &rpc.CommandRequest{
		CommandId: rpc.CommandID_COMMAND_ID_MAIL_LIST_GLOBAL,
		Uid:       10001,
		RoleId:    10001,
		ServerId:  1,
		Seq:       99,
		Payload:   payload,
	})
	if err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}
	if resp.GetCode() != 0 || resp.GetSeq() != 99 {
		t.Fatalf("unexpected command response: %+v", resp)
	}
	var listResp rpc.ListGlobalMailsResponse
	if err := resp.GetPayload().UnmarshalTo(&listResp); err != nil {
		t.Fatalf("unpack response failed: %v", err)
	}
	if len(listResp.GetMails()) != 1 || listResp.GetMails()[0].GetGlobalMailId() != 10 {
		t.Fatalf("unexpected list response: %+v", listResp.GetMails())
	}
}

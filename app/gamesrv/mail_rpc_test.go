package gamesrv

import (
	"context"
	"testing"
	"time"

	"globalmail/api/rpc"
	"globalmail/domain/globalmail"
)

func TestMailRPCServerListGlobalMails(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeMailRepository{
		mails: []globalmail.GlobalMail{
			{
				ID:         10,
				Title:      "title",
				Content:    "content",
				Sender:     "system",
				Status:     globalmail.MailStatusPublished,
				StartTime:  now.Add(-time.Hour),
				ExpireTime: now.Add(time.Hour),
				Conditions: []globalmail.Condition{
					{Type: "server_id", Value: []byte(`1`)},
				},
			},
		},
		states: map[int64]globalmail.UserGlobalMailState{
			10: {
				RoleID:             10001,
				GlobalMailID:       10,
				Status:             globalmail.UserMailStatusClaimed,
				ClaimedLootIndexes: []int{0, 2},
			},
		},
	}
	cacheRepo := &fakeCacheRepository{version: 1}
	localCache := globalmail.NewLocalCache(repo, cacheRepo)
	if err := localCache.ForceRefresh(context.Background()); err != nil {
		t.Fatalf("refresh cache failed: %v", err)
	}
	server := NewMailRPCServer(NewMailService(repo, localCache))

	resp, err := server.ListGlobalMails(context.Background(), &rpc.ListGlobalMailsRequest{
		Profile: &rpc.UserProfile{
			Uid:      10001,
			RoleId:   10001,
			ServerId: 1,
		},
	})
	if err != nil {
		t.Fatalf("list global mails failed: %v", err)
	}
	if len(resp.GetMails()) != 1 {
		t.Fatalf("expected 1 mail, got %d", len(resp.GetMails()))
	}
	mail := resp.GetMails()[0]
	if mail.GetGlobalMailId() != 10 || mail.GetUserStatus() != string(globalmail.UserMailStatusClaimed) {
		t.Fatalf("unexpected mail response: %+v", mail)
	}
	if len(mail.GetClaimedLootIndexes()) != 2 {
		t.Fatalf("expected claimed loot indexes, got %+v", mail.GetClaimedLootIndexes())
	}
}

func TestMailRPCServerClaimGlobalMail(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeMailRepository{
		mails: []globalmail.GlobalMail{
			{
				ID:         10,
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
	server := NewMailRPCServer(NewMailService(repo, localCache))

	resp, err := server.ClaimGlobalMail(context.Background(), &rpc.ClaimGlobalMailRequest{
		Profile: &rpc.UserProfile{
			Uid:      10001,
			RoleId:   10001,
			ServerId: 1,
		},
		GlobalMailId: 10,
		LootIndexes:  []int32{1},
	})
	if err != nil {
		t.Fatalf("claim global mail failed: %v", err)
	}
	if resp.GetStatus() != string(globalmail.UserMailStatusClaimed) {
		t.Fatalf("unexpected status: %s", resp.GetStatus())
	}
	if len(resp.GetClaimedLootIndexes()) != 1 || resp.GetClaimedLootIndexes()[0] != 1 {
		t.Fatalf("unexpected claimed loot indexes: %+v", resp.GetClaimedLootIndexes())
	}
}

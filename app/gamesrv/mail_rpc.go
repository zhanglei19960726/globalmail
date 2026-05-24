package gamesrv

import (
	"context"
	"time"

	"globalmail/api/rpc"
	"globalmail/domain/globalmail"
)

type MailRPCServer struct {
	rpc.UnimplementedMailServiceServer
	service *MailService
}

func NewMailRPCServer(service *MailService) *MailRPCServer {
	return &MailRPCServer{service: service}
}

func (s *MailRPCServer) ListGlobalMails(ctx context.Context, req *rpc.ListGlobalMailsRequest) (*rpc.ListGlobalMailsResponse, error) {
	items, err := s.service.ListGlobalMails(ctx, toDomainProfile(req.GetProfile()))
	if err != nil {
		return nil, err
	}
	resp := &rpc.ListGlobalMailsResponse{
		Mails: make([]*rpc.GlobalMailItem, 0, len(items)),
	}
	for _, item := range items {
		resp.Mails = append(resp.Mails, toRPCMailItem(item))
	}
	return resp, nil
}

func toDomainProfile(profile *rpc.UserProfile) globalmail.UserProfile {
	if profile == nil {
		return globalmail.UserProfile{}
	}
	return globalmail.UserProfile{
		UID:           profile.GetUid(),
		RoleID:        profile.GetRoleId(),
		ServerID:      int(profile.GetServerId()),
		VIPLevel:      int(profile.GetVipLevel()),
		TotalRecharge: profile.GetTotalRecharge(),
		CorpsLevel:    int(profile.GetCorpsLevel()),
		OpenDays:      int(profile.GetOpenDays()),
		PackageType:   profile.GetPackageType(),
		Country:       profile.GetCountry(),
	}
}

func toRPCMailItem(item MailItem) *rpc.GlobalMailItem {
	mail := item.Mail
	claimed := make([]int32, 0, len(item.ClaimedLootIndexes))
	for _, idx := range item.ClaimedLootIndexes {
		claimed = append(claimed, int32(idx))
	}
	return &rpc.GlobalMailItem{
		GlobalMailId:       mail.ID,
		Title:              mail.Title,
		Content:            mail.Content,
		MessageMap:         []byte(mail.MessageMap),
		Loots:              []byte(mail.Loots),
		Sender:             mail.Sender,
		Category:           mail.Category,
		TemplateId:         mail.TemplateID,
		Params:             []byte(mail.Params),
		ExternalUrl:        mail.ExternalURL,
		StartTime:          formatTime(mail.StartTime),
		ExpireTime:         formatTime(mail.ExpireTime),
		Status:             string(mail.Status),
		Version:            mail.Version,
		UserStatus:         string(item.Status),
		ClaimedLootIndexes: claimed,
	}
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

package globalmail

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

const EventTypeGlobalMailChanged = "GlobalMailChanged"

type PublisherService struct {
	repo  MailRepository
	cache CacheRepository
	ids   IDGenerator
	now   func() time.Time
}

func NewPublisherService(repo MailRepository, cache CacheRepository, ids IDGenerator) *PublisherService {
	return &PublisherService{
		repo:  repo,
		cache: cache,
		ids:   ids,
		now:   time.Now,
	}
}

type PublishCommand struct {
	Mail       GlobalMail
	Conditions []Condition
	Action     string
}

func (s *PublisherService) Publish(ctx context.Context, cmd PublishCommand) (GlobalMail, error) {
	if err := validateMail(cmd.Mail); err != nil {
		return GlobalMail{}, err
	}
	if cmd.Action == "" {
		cmd.Action = "publish"
	}

	now := s.now().UTC()
	mail := cmd.Mail
	if mail.ID == 0 {
		mail.ID = s.ids.NextID()
	}
	mail.Status = MailStatusPublished
	mail.Version = 1
	mail.Conditions = cmd.Conditions
	mail.CreateTime = now
	mail.UpdateTime = now

	eventID := s.ids.NextID()
	changed := GlobalMailChangedEvent{
		EventID:      eventID,
		GlobalMailID: mail.ID,
		Version:      mail.Version,
		Action:       cmd.Action,
		Timestamp:    now,
	}
	payload, err := json.Marshal(changed)
	if err != nil {
		return GlobalMail{}, err
	}

	outbox := OutboxEvent{
		ID:          eventID,
		Type:        EventTypeGlobalMailChanged,
		AggregateID: mail.ID,
		Version:     mail.Version,
		Payload:     payload,
		Status:      OutboxStatusPending,
		CreateTime:  now,
		UpdateTime:  now,
	}

	if err := s.repo.CreateGlobalMailWithOutbox(ctx, mail, outbox); err != nil {
		return GlobalMail{}, err
	}
	if err := s.cache.SetGlobalMail(ctx, mail); err != nil {
		return GlobalMail{}, err
	}
	if _, err := s.cache.IncrementGlobalMailVersion(ctx); err != nil {
		return GlobalMail{}, err
	}
	return mail, nil
}

func validateMail(mail GlobalMail) error {
	if mail.Title == "" {
		return errors.New("global mail title is required")
	}
	if mail.Content == "" {
		return errors.New("global mail content is required")
	}
	if mail.StartTime.IsZero() || mail.ExpireTime.IsZero() {
		return errors.New("global mail start and expire time are required")
	}
	if !mail.ExpireTime.After(mail.StartTime) {
		return errors.New("global mail expire time must be after start time")
	}
	return nil
}

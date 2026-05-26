package globalmail

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const EventTypeGlobalMailChanged = "GlobalMailChanged"

var ErrIdempotencyConflict = errors.New("idempotency key reused with different request")

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
	Mail           GlobalMail
	Conditions     []Condition
	Action         string
	IdempotencyKey string
}

func (s *PublisherService) Publish(ctx context.Context, cmd PublishCommand) (GlobalMail, error) {
	if err := validateMail(cmd.Mail); err != nil {
		return GlobalMail{}, err
	}
	if cmd.Action == "" {
		cmd.Action = "publish"
	}
	requestHash, err := hashPublishCommand(cmd)
	if err != nil {
		return GlobalMail{}, err
	}
	if cmd.IdempotencyKey != "" {
		record, ok, err := s.repo.GetPublishIdempotency(ctx, cmd.IdempotencyKey)
		if err != nil {
			return GlobalMail{}, err
		}
		if ok {
			if record.RequestHash != requestHash {
				return GlobalMail{}, ErrIdempotencyConflict
			}
			var mail GlobalMail
			if err := json.Unmarshal(record.ResponseSnapshot, &mail); err != nil {
				return GlobalMail{}, err
			}
			return mail, nil
		}
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

	if cmd.IdempotencyKey == "" {
		if err := s.repo.CreateGlobalMailWithOutbox(ctx, mail, outbox); err != nil {
			return GlobalMail{}, err
		}
		return mail, nil
	}

	responseSnapshot, err := json.Marshal(mail)
	if err != nil {
		return GlobalMail{}, err
	}
	record := PublishIdempotencyRecord{
		Key:              cmd.IdempotencyKey,
		Action:           cmd.Action,
		RequestHash:      requestHash,
		GlobalMailID:     mail.ID,
		TargetVersion:    mail.Version,
		Status:           IdempotencyStatusSucceeded,
		ResponseSnapshot: responseSnapshot,
		CreateTime:       now,
		UpdateTime:       now,
	}
	if err := s.repo.CreateGlobalMailWithOutboxAndIdempotency(ctx, mail, outbox, record); err != nil {
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

func hashPublishCommand(cmd PublishCommand) (string, error) {
	payload := struct {
		Mail       GlobalMail  `json:"mail"`
		Conditions []Condition `json:"conditions,omitempty"`
		Action     string      `json:"action"`
	}{
		Mail:       cmd.Mail,
		Conditions: cmd.Conditions,
		Action:     cmd.Action,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return fmt.Sprintf("%x", sum[:]), nil
}

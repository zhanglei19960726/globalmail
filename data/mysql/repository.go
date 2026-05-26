package mysql

import (
	"context"
	"time"

	"globalmail/domain/globalmail"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Repository struct {
	db *gorm.DB
}

func NewRepository(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&GlobalMailModel{},
		&GlobalMailConditionModel{},
		&UserPersonalMailModel{},
		&UserGlobalMailStateModel{},
		&UserMailCursorModel{},
		&GlobalMailOutboxEventModel{},
		&GlobalMailIdempotencyModel{},
	)
}

func (r *Repository) CreateGlobalMailWithOutbox(ctx context.Context, mail globalmail.GlobalMail, event globalmail.OutboxEvent) error {
	return r.createGlobalMailWithOutbox(ctx, mail, event, nil)
}

func (r *Repository) CreateGlobalMailWithOutboxAndIdempotency(ctx context.Context, mail globalmail.GlobalMail, event globalmail.OutboxEvent, record globalmail.PublishIdempotencyRecord) error {
	model := toIdempotencyModel(record)
	return r.createGlobalMailWithOutbox(ctx, mail, event, &model)
}

func (r *Repository) createGlobalMailWithOutbox(ctx context.Context, mail globalmail.GlobalMail, event globalmail.OutboxEvent, record *GlobalMailIdempotencyModel) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		mailModel := toGlobalMailModel(mail)
		if err := tx.Create(&mailModel).Error; err != nil {
			return err
		}
		for _, condition := range mail.Conditions {
			condition.MailID = mail.ID
			model := toConditionModel(condition)
			model.CreateTime = mail.CreateTime
			model.UpdateTime = mail.UpdateTime
			if err := tx.Create(&model).Error; err != nil {
				return err
			}
		}
		outbox := toOutboxModel(event)
		if err := tx.Create(&outbox).Error; err != nil {
			return err
		}
		if record != nil {
			if err := tx.Create(record).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *Repository) GetPublishIdempotency(ctx context.Context, key string) (globalmail.PublishIdempotencyRecord, bool, error) {
	var model GlobalMailIdempotencyModel
	err := r.db.WithContext(ctx).
		Where("idempotency_key = ?", key).
		First(&model).Error
	if err == gorm.ErrRecordNotFound {
		return globalmail.PublishIdempotencyRecord{}, false, nil
	}
	if err != nil {
		return globalmail.PublishIdempotencyRecord{}, false, err
	}
	return toIdempotencyRecord(model), true, nil
}

func (r *Repository) GetGlobalMailByID(ctx context.Context, mailID int64) (globalmail.GlobalMail, bool, error) {
	var model GlobalMailModel
	err := r.db.WithContext(ctx).
		Where("global_mail_id = ?", mailID).
		First(&model).Error
	if err == gorm.ErrRecordNotFound {
		return globalmail.GlobalMail{}, false, nil
	}
	if err != nil {
		return globalmail.GlobalMail{}, false, err
	}
	var conditions []GlobalMailConditionModel
	if err := r.db.WithContext(ctx).
		Where("global_mail_id = ?", model.GlobalMailID).
		Find(&conditions).Error; err != nil {
		return globalmail.GlobalMail{}, false, err
	}
	return toGlobalMail(model, conditions), true, nil
}

func (r *Repository) GetPublishedGlobalMails(ctx context.Context, now time.Time) ([]globalmail.GlobalMail, error) {
	var models []GlobalMailModel
	if err := r.db.WithContext(ctx).
		Where("status = ? AND start_time <= ? AND expire_time > ?", globalmail.MailStatusPublished, now, now).
		Find(&models).Error; err != nil {
		return nil, err
	}

	mails := make([]globalmail.GlobalMail, 0, len(models))
	for _, model := range models {
		var conditions []GlobalMailConditionModel
		if err := r.db.WithContext(ctx).
			Where("global_mail_id = ?", model.GlobalMailID).
			Find(&conditions).Error; err != nil {
			return nil, err
		}
		mails = append(mails, toGlobalMail(model, conditions))
	}
	return mails, nil
}

func (r *Repository) GetUserStates(ctx context.Context, roleID int64, mailIDs []int64) (map[int64]globalmail.UserGlobalMailState, error) {
	if len(mailIDs) == 0 {
		return map[int64]globalmail.UserGlobalMailState{}, nil
	}
	var models []UserGlobalMailStateModel
	if err := r.db.WithContext(ctx).
		Where("role_id = ? AND global_mail_id IN ?", roleID, mailIDs).
		Find(&models).Error; err != nil {
		return nil, err
	}
	states := make(map[int64]globalmail.UserGlobalMailState, len(models))
	for _, model := range models {
		states[model.GlobalMailID] = toState(model)
	}
	return states, nil
}

func (r *Repository) SaveUserState(ctx context.Context, state globalmail.UserGlobalMailState) error {
	model := toStateModel(state)
	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "role_id"},
				{Name: "global_mail_id"},
			},
			DoUpdates: clause.AssignmentColumns([]string{
				"server_id",
				"status",
				"claimed_loot_indexes",
				"claim_time",
				"delete_time",
				"version",
				"update_time",
			}),
		}).
		Create(&model).Error
}

func (r *Repository) FetchPending(ctx context.Context, limit int) ([]globalmail.OutboxEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	now := time.Now().UTC()
	var models []GlobalMailOutboxEventModel
	if err := r.db.WithContext(ctx).
		Where("status = ? AND (next_retry_time IS NULL OR next_retry_time <= ?)", globalmail.OutboxStatusPending, now).
		Order("event_id ASC").
		Limit(limit).
		Find(&models).Error; err != nil {
		return nil, err
	}
	events := make([]globalmail.OutboxEvent, 0, len(models))
	for _, model := range models {
		events = append(events, toOutboxEvent(model))
	}
	return events, nil
}

func (r *Repository) MarkPublished(ctx context.Context, eventID int64, publishedAt time.Time) error {
	return r.db.WithContext(ctx).
		Model(&GlobalMailOutboxEventModel{}).
		Where("event_id = ?", eventID).
		Updates(map[string]interface{}{
			"status":         string(globalmail.OutboxStatusPublished),
			"published_time": publishedAt,
			"update_time":    publishedAt,
		}).Error
}

func (r *Repository) MarkFailed(ctx context.Context, eventID int64, nextRetryAt time.Time, _ error) error {
	now := time.Now().UTC()
	return r.db.WithContext(ctx).
		Model(&GlobalMailOutboxEventModel{}).
		Where("event_id = ?", eventID).
		Updates(map[string]interface{}{
			"status":          string(globalmail.OutboxStatusPending),
			"retry_count":     gorm.Expr("retry_count + 1"),
			"next_retry_time": nextRetryAt,
			"update_time":     now,
		}).Error
}

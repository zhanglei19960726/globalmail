package mysql

import (
	"context"
	"strings"
	"time"

	"globalmail/domain/globalmail"
	"gorm.io/datatypes"
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
		&UserGlobalMailRewardLedgerModel{},
		&UserBackpackRewardModel{},
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
	preserveDeleted := func(column string, value interface{}) clause.Expr {
		return clause.Expr{
			SQL:  "CASE WHEN status = ? THEN " + column + " ELSE ? END",
			Vars: []interface{}{string(globalmail.UserMailStatusDeleted), value},
		}
	}
	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "role_id"},
				{Name: "global_mail_id"},
			},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"server_id":            preserveDeleted("server_id", model.ServerID),
				"status":               preserveDeleted("status", model.Status),
				"claimed_loot_indexes": preserveDeleted("claimed_loot_indexes", model.ClaimedLootIndexes),
				"claim_time":           preserveDeleted("claim_time", model.ClaimTime),
				"delete_time":          preserveDeleted("delete_time", model.DeleteTime),
				"version":              preserveDeleted("version", model.Version),
				"update_time":          preserveDeleted("update_time", model.UpdateTime),
			}),
		}).
		Create(&model).Error
}

func (r *Repository) GrantGlobalMailReward(ctx context.Context, grant globalmail.RewardGrant) (bool, error) {
	if grant.Status == "" {
		grant.Status = "succeeded"
	}
	return r.ReserveGlobalMailReward(ctx, grant)
}

func (r *Repository) ReserveGlobalMailReward(ctx context.Context, grant globalmail.RewardGrant) (bool, error) {
	now := time.Now().UTC()
	if grant.CreateTime.IsZero() {
		grant.CreateTime = now
	}
	if grant.UpdateTime.IsZero() {
		grant.UpdateTime = grant.CreateTime
	}
	model := UserGlobalMailRewardLedgerModel{
		GrantKey:         grant.GrantKey,
		RoleID:           grant.RoleID,
		ServerID:         grant.ServerID,
		GlobalMailID:     grant.GlobalMailID,
		LootIndex:        grant.LootIndex,
		Status:           grant.Status,
		ExternalRewardID: grant.ExternalRewardID,
		FailureReason:    grant.FailureReason,
		CreateTime:       grant.CreateTime,
		UpdateTime:       grant.UpdateTime,
	}
	if model.Status == "" {
		model.Status = "succeeded"
	}
	result := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&model)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func (r *Repository) MarkGlobalMailRewardSucceeded(ctx context.Context, grantKey string, externalRewardID string, updatedAt time.Time) error {
	return r.db.WithContext(ctx).
		Model(&UserGlobalMailRewardLedgerModel{}).
		Where("grant_key = ?", grantKey).
		Updates(map[string]interface{}{
			"status":             "succeeded",
			"external_reward_id": externalRewardID,
			"failure_reason":     "",
			"update_time":        updatedAt,
		}).Error
}

func (r *Repository) MarkGlobalMailRewardFailed(ctx context.Context, grantKey string, cause error, updatedAt time.Time) error {
	reason := ""
	if cause != nil {
		reason = cause.Error()
	}
	return r.db.WithContext(ctx).
		Model(&UserGlobalMailRewardLedgerModel{}).
		Where("grant_key = ?", grantKey).
		Updates(map[string]interface{}{
			"status":         "failed",
			"failure_reason": reason,
			"update_time":    updatedAt,
		}).Error
}

func (r *Repository) GrantBackpackReward(ctx context.Context, grant globalmail.BackpackGrant) (string, bool, error) {
	externalRewardID := "playersrv-backpack:" + grant.GrantKey
	model := UserBackpackRewardModel{
		GrantKey:   grant.GrantKey,
		RoleID:     grant.RoleID,
		ServerID:   grant.ServerID,
		LootIndex:  grant.LootIndex,
		Loot:       datatypes.JSON(grant.Loot),
		CreateTime: grant.CreateTime,
	}
	result := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&model)
	if result.Error != nil {
		return "", false, result.Error
	}
	return externalRewardID, result.RowsAffected > 0, nil
}

func (r *Repository) FetchPending(ctx context.Context, limit int, lockedBy string, lockedUntil time.Time) ([]globalmail.OutboxEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	now := time.Now().UTC()
	if lockedBy == "" {
		lockedBy = "outbox-relay"
	}

	var events []globalmail.OutboxEvent
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var models []GlobalMailOutboxEventModel
		if err := tx.
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where(
				"(status = ? AND (next_retry_time IS NULL OR next_retry_time <= ?)) OR (status = ? AND locked_until <= ?)",
				globalmail.OutboxStatusPending,
				now,
				globalmail.OutboxStatusProcessing,
				now,
			).
			Order("event_id ASC").
			Limit(limit).
			Find(&models).Error; err != nil {
			return err
		}
		if len(models) == 0 {
			return nil
		}

		ids := make([]int64, 0, len(models))
		events = make([]globalmail.OutboxEvent, 0, len(models))
		for _, model := range models {
			ids = append(ids, model.EventID)
			event := toOutboxEvent(model)
			event.Status = globalmail.OutboxStatusProcessing
			event.LockedBy = lockedBy
			event.LockedUntil = &lockedUntil
			events = append(events, event)
		}

		return tx.Model(&GlobalMailOutboxEventModel{}).
			Where("event_id IN ?", ids).
			Updates(map[string]interface{}{
				"status":          string(globalmail.OutboxStatusProcessing),
				"locked_by":       lockedBy,
				"locked_until":    lockedUntil,
				"next_retry_time": nil,
				"update_time":     now,
			}).Error
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

func (r *Repository) MarkPublished(ctx context.Context, eventID int64, publishedAt time.Time) error {
	return r.db.WithContext(ctx).
		Model(&GlobalMailOutboxEventModel{}).
		Where("event_id = ?", eventID).
		Updates(map[string]interface{}{
			"status":         string(globalmail.OutboxStatusPublished),
			"locked_by":      "",
			"locked_until":   nil,
			"failure_reason": "",
			"published_time": publishedAt,
			"update_time":    publishedAt,
		}).Error
}

func (r *Repository) MarkFailed(ctx context.Context, eventID int64, nextRetryAt time.Time, cause error, maxRetries int) error {
	now := time.Now().UTC()
	status := string(globalmail.OutboxStatusPending)
	if maxRetries <= 0 {
		maxRetries = 5
	}
	var model GlobalMailOutboxEventModel
	if err := r.db.WithContext(ctx).
		Select("retry_count").
		Where("event_id = ?", eventID).
		First(&model).Error; err != nil {
		return err
	}
	if model.RetryCount+1 >= maxRetries {
		status = string(globalmail.OutboxStatusFailed)
	}
	reason := ""
	if cause != nil {
		reason = cause.Error()
	}
	if len(reason) > 1024 {
		reason = reason[:1024]
	}
	reason = strings.TrimSpace(reason)

	updates := map[string]interface{}{
		"status":         status,
		"retry_count":    gorm.Expr("retry_count + 1"),
		"locked_by":      "",
		"locked_until":   nil,
		"failure_reason": reason,
		"update_time":    now,
	}
	if status == string(globalmail.OutboxStatusPending) {
		updates["next_retry_time"] = nextRetryAt
	} else {
		updates["next_retry_time"] = nil
	}
	return r.db.WithContext(ctx).
		Model(&GlobalMailOutboxEventModel{}).
		Where("event_id = ?", eventID).
		Updates(updates).Error
}

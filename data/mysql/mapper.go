package mysql

import (
	"encoding/json"

	"globalmail/domain/globalmail"
	"gorm.io/datatypes"
)

func toGlobalMailModel(mail globalmail.GlobalMail) GlobalMailModel {
	return GlobalMailModel{
		GlobalMailID: mail.ID,
		OperatorID:   mail.OperatorID,
		Title:        mail.Title,
		Content:      mail.Content,
		MessageMap:   datatypes.JSON(mail.MessageMap),
		Loots:        datatypes.JSON(mail.Loots),
		Sender:       mail.Sender,
		Category:     mail.Category,
		TemplateID:   mail.TemplateID,
		Params:       datatypes.JSON(mail.Params),
		ExternalURL:  mail.ExternalURL,
		StartTime:    mail.StartTime,
		ExpireTime:   mail.ExpireTime,
		Status:       string(mail.Status),
		Version:      mail.Version,
		CreateTime:   mail.CreateTime,
		UpdateTime:   mail.UpdateTime,
	}
}

func toGlobalMail(model GlobalMailModel, conditions []GlobalMailConditionModel) globalmail.GlobalMail {
	mail := globalmail.GlobalMail{
		ID:          model.GlobalMailID,
		OperatorID:  model.OperatorID,
		Title:       model.Title,
		Content:     model.Content,
		MessageMap:  json.RawMessage(model.MessageMap),
		Loots:       json.RawMessage(model.Loots),
		Sender:      model.Sender,
		Category:    model.Category,
		TemplateID:  model.TemplateID,
		Params:      json.RawMessage(model.Params),
		ExternalURL: model.ExternalURL,
		StartTime:   model.StartTime,
		ExpireTime:  model.ExpireTime,
		Status:      globalmail.MailStatus(model.Status),
		Version:     model.Version,
		CreateTime:  model.CreateTime,
		UpdateTime:  model.UpdateTime,
	}
	for _, condition := range conditions {
		mail.Conditions = append(mail.Conditions, globalmail.Condition{
			ID:       condition.ConditionID,
			MailID:   condition.GlobalMailID,
			Type:     condition.ConditionType,
			Operator: condition.Operator,
			Value:    json.RawMessage(condition.ConditionValue),
		})
	}
	return mail
}

func toConditionModel(condition globalmail.Condition) GlobalMailConditionModel {
	return GlobalMailConditionModel{
		ConditionID:    condition.ID,
		GlobalMailID:   condition.MailID,
		ConditionType:  condition.Type,
		Operator:       condition.Operator,
		ConditionValue: datatypes.JSON(condition.Value),
	}
}

func toOutboxModel(event globalmail.OutboxEvent) GlobalMailOutboxEventModel {
	return GlobalMailOutboxEventModel{
		EventID:       event.ID,
		EventType:     event.Type,
		AggregateID:   event.AggregateID,
		Version:       event.Version,
		Payload:       datatypes.JSON(event.Payload),
		Status:        string(event.Status),
		RetryCount:    event.RetryCount,
		NextRetryTime: event.NextRetryTime,
		LockedBy:      event.LockedBy,
		LockedUntil:   event.LockedUntil,
		FailureReason: event.FailureReason,
		PublishedTime: event.PublishedTime,
		CreateTime:    event.CreateTime,
		UpdateTime:    event.UpdateTime,
	}
}

func toOutboxEvent(model GlobalMailOutboxEventModel) globalmail.OutboxEvent {
	return globalmail.OutboxEvent{
		ID:            model.EventID,
		Type:          model.EventType,
		AggregateID:   model.AggregateID,
		Version:       model.Version,
		Payload:       json.RawMessage(model.Payload),
		Status:        globalmail.OutboxStatus(model.Status),
		RetryCount:    model.RetryCount,
		NextRetryTime: model.NextRetryTime,
		LockedBy:      model.LockedBy,
		LockedUntil:   model.LockedUntil,
		FailureReason: model.FailureReason,
		PublishedTime: model.PublishedTime,
		CreateTime:    model.CreateTime,
		UpdateTime:    model.UpdateTime,
	}
}

func toIdempotencyModel(record globalmail.PublishIdempotencyRecord) GlobalMailIdempotencyModel {
	return GlobalMailIdempotencyModel{
		IdempotencyKey:   record.Key,
		Action:           record.Action,
		RequestHash:      record.RequestHash,
		GlobalMailID:     record.GlobalMailID,
		TargetVersion:    record.TargetVersion,
		Status:           string(record.Status),
		ResponseSnapshot: datatypes.JSON(record.ResponseSnapshot),
		CreateTime:       record.CreateTime,
		UpdateTime:       record.UpdateTime,
	}
}

func toIdempotencyRecord(model GlobalMailIdempotencyModel) globalmail.PublishIdempotencyRecord {
	return globalmail.PublishIdempotencyRecord{
		Key:              model.IdempotencyKey,
		Action:           model.Action,
		RequestHash:      model.RequestHash,
		GlobalMailID:     model.GlobalMailID,
		TargetVersion:    model.TargetVersion,
		Status:           globalmail.IdempotencyStatus(model.Status),
		ResponseSnapshot: json.RawMessage(model.ResponseSnapshot),
		CreateTime:       model.CreateTime,
		UpdateTime:       model.UpdateTime,
	}
}

func toStateModel(state globalmail.UserGlobalMailState) UserGlobalMailStateModel {
	payload, _ := json.Marshal(state.ClaimedLootIndexes)
	return UserGlobalMailStateModel{
		RoleID:             state.RoleID,
		ServerID:           state.ServerID,
		GlobalMailID:       state.GlobalMailID,
		Status:             string(state.Status),
		ClaimedLootIndexes: datatypes.JSON(payload),
		ClaimTime:          state.ClaimTime,
		DeleteTime:         state.DeleteTime,
		Version:            state.Version,
		CreateTime:         state.CreateTime,
		UpdateTime:         state.UpdateTime,
	}
}

func toState(model UserGlobalMailStateModel) globalmail.UserGlobalMailState {
	var claimed []int
	_ = json.Unmarshal(model.ClaimedLootIndexes, &claimed)
	return globalmail.UserGlobalMailState{
		RoleID:             model.RoleID,
		ServerID:           model.ServerID,
		GlobalMailID:       model.GlobalMailID,
		Status:             globalmail.UserMailStatus(model.Status),
		ClaimedLootIndexes: claimed,
		ClaimTime:          model.ClaimTime,
		DeleteTime:         model.DeleteTime,
		Version:            model.Version,
		CreateTime:         model.CreateTime,
		UpdateTime:         model.UpdateTime,
	}
}

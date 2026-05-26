package globalmail

import (
	"encoding/json"
	"time"
)

type MailStatus string

const (
	MailStatusDraft     MailStatus = "draft"
	MailStatusAuditing  MailStatus = "auditing"
	MailStatusPublished MailStatus = "published"
	MailStatusOffline   MailStatus = "offline"
	MailStatusDeleted   MailStatus = "deleted"
)

type UserMailStatus string

const (
	UserMailStatusUnread  UserMailStatus = "unread"
	UserMailStatusRead    UserMailStatus = "read"
	UserMailStatusClaimed UserMailStatus = "claimed"
	UserMailStatusDeleted UserMailStatus = "deleted"
)

type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "pending"
	OutboxStatusPublished OutboxStatus = "published"
	OutboxStatusFailed    OutboxStatus = "failed"
)

type IdempotencyStatus string

const (
	IdempotencyStatusSucceeded IdempotencyStatus = "succeeded"
)

type GlobalMail struct {
	ID          int64           `json:"global_mail_id"`
	OperatorID  int64           `json:"opr_id"`
	Title       string          `json:"title"`
	Content     string          `json:"content"`
	MessageMap  json.RawMessage `json:"msg_map,omitempty"`
	Loots       json.RawMessage `json:"loots,omitempty"`
	Sender      string          `json:"sender"`
	Category    string          `json:"mail_category"`
	TemplateID  int64           `json:"mail_template_id"`
	Params      json.RawMessage `json:"param_list,omitempty"`
	ExternalURL string          `json:"external_url,omitempty"`
	StartTime   time.Time       `json:"start_time"`
	ExpireTime  time.Time       `json:"expire_time"`
	Status      MailStatus      `json:"status"`
	Version     int64           `json:"version"`
	Conditions  []Condition     `json:"conditions,omitempty"`
	CreateTime  time.Time       `json:"create_time"`
	UpdateTime  time.Time       `json:"update_time"`
}

type Condition struct {
	ID       int64           `json:"condition_id"`
	MailID   int64           `json:"global_mail_id"`
	Type     string          `json:"condition_type"`
	Operator string          `json:"operator"`
	Value    json.RawMessage `json:"condition_value"`
}

type UserProfile struct {
	RoleID        int64     `json:"role_id"`
	UID           int64     `json:"uid"`
	ServerID      int       `json:"server_id"`
	RegisterTime  time.Time `json:"register_time"`
	VIPLevel      int       `json:"vip_level"`
	TotalRecharge int64     `json:"total_recharge"`
	CorpsLevel    int       `json:"corps_level"`
	OpenDays      int       `json:"open_days"`
	PackageType   string    `json:"package_type"`
	Country       string    `json:"country"`
}

type UserGlobalMailState struct {
	RoleID             int64          `json:"role_id"`
	ServerID           int            `json:"server_id"`
	GlobalMailID       int64          `json:"global_mail_id"`
	Status             UserMailStatus `json:"status"`
	ClaimedLootIndexes []int          `json:"claimed_loot_indexes,omitempty"`
	ClaimTime          *time.Time     `json:"claim_time,omitempty"`
	DeleteTime         *time.Time     `json:"delete_time,omitempty"`
	Version            int            `json:"version"`
	CreateTime         time.Time      `json:"create_time"`
	UpdateTime         time.Time      `json:"update_time"`
}

type PersonalMail struct {
	ID         int64           `json:"mail_id"`
	RoleID     int64           `json:"role_id"`
	ServerID   int             `json:"server_id"`
	Title      string          `json:"title"`
	Content    string          `json:"content"`
	TemplateID int64           `json:"template_id"`
	Params     json.RawMessage `json:"param_list,omitempty"`
	Loots      json.RawMessage `json:"loots,omitempty"`
	Status     UserMailStatus  `json:"status"`
	CreateTime time.Time       `json:"create_time"`
	ExpireTime time.Time       `json:"expire_time"`
	UpdateTime time.Time       `json:"update_time"`
}

type OutboxEvent struct {
	ID            int64           `json:"event_id"`
	Type          string          `json:"event_type"`
	AggregateID   int64           `json:"aggregate_id"`
	Version       int64           `json:"version"`
	Payload       json.RawMessage `json:"payload"`
	Status        OutboxStatus    `json:"status"`
	RetryCount    int             `json:"retry_count"`
	NextRetryTime *time.Time      `json:"next_retry_time,omitempty"`
	PublishedTime *time.Time      `json:"published_time,omitempty"`
	CreateTime    time.Time       `json:"create_time"`
	UpdateTime    time.Time       `json:"update_time"`
}

type GlobalMailChangedEvent struct {
	EventID      int64     `json:"event_id"`
	GlobalMailID int64     `json:"global_mail_id"`
	Version      int64     `json:"version"`
	Action       string    `json:"action"`
	Timestamp    time.Time `json:"timestamp"`
}

type PublishIdempotencyRecord struct {
	Key              string            `json:"idempotency_key"`
	Action           string            `json:"action"`
	RequestHash      string            `json:"request_hash"`
	GlobalMailID     int64             `json:"global_mail_id"`
	TargetVersion    int64             `json:"target_version"`
	Status           IdempotencyStatus `json:"status"`
	ResponseSnapshot json.RawMessage   `json:"response_snapshot"`
	CreateTime       time.Time         `json:"create_time"`
	UpdateTime       time.Time         `json:"update_time"`
}

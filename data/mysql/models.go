package mysql

import (
	"time"

	"gorm.io/datatypes"
)

type GlobalMailModel struct {
	GlobalMailID int64          `gorm:"column:global_mail_id;primaryKey"`
	OperatorID   int64          `gorm:"column:opr_id;not null"`
	Title        string         `gorm:"column:title;size:256;not null"`
	Content      string         `gorm:"column:content;type:text;not null"`
	MessageMap   datatypes.JSON `gorm:"column:msg_map;type:json"`
	Loots        datatypes.JSON `gorm:"column:loots;type:json"`
	Sender       string         `gorm:"column:sender;size:64;not null"`
	Category     string         `gorm:"column:mail_category;size:32;not null"`
	TemplateID   int64          `gorm:"column:mail_template_id;not null;default:0"`
	Params       datatypes.JSON `gorm:"column:param_list;type:json"`
	ExternalURL  string         `gorm:"column:external_url;size:512"`
	StartTime    time.Time      `gorm:"column:start_time;not null;index:idx_status_time,priority:2"`
	ExpireTime   time.Time      `gorm:"column:expire_time;not null;index:idx_status_time,priority:3"`
	Status       string         `gorm:"column:status;size:32;not null;index:idx_status_time,priority:1"`
	Version      int64          `gorm:"column:version;not null;default:1"`
	CreateTime   time.Time      `gorm:"column:create_time;not null"`
	UpdateTime   time.Time      `gorm:"column:update_time;not null;index:idx_update_time"`
}

func (GlobalMailModel) TableName() string {
	return "global_mail"
}

type GlobalMailConditionModel struct {
	ConditionID    int64          `gorm:"column:condition_id;primaryKey"`
	GlobalMailID   int64          `gorm:"column:global_mail_id;not null;index:idx_global_mail_id"`
	ConditionType  string         `gorm:"column:condition_type;size:64;not null;index:idx_condition_type"`
	Operator       string         `gorm:"column:operator;size:16;not null"`
	ConditionValue datatypes.JSON `gorm:"column:condition_value;type:json;not null"`
	CreateTime     time.Time      `gorm:"column:create_time;not null"`
	UpdateTime     time.Time      `gorm:"column:update_time;not null"`
}

func (GlobalMailConditionModel) TableName() string {
	return "global_mail_condition"
}

type UserGlobalMailStateModel struct {
	RoleID             int64          `gorm:"column:role_id;primaryKey;autoIncrement:false;index:idx_role_status,priority:1"`
	ServerID           int            `gorm:"column:server_id;not null"`
	GlobalMailID       int64          `gorm:"column:global_mail_id;primaryKey;autoIncrement:false;index:idx_global_mail_id"`
	Status             string         `gorm:"column:status;size:32;not null;index:idx_role_status,priority:2"`
	ClaimedLootIndexes datatypes.JSON `gorm:"column:claimed_loot_indexes;type:json"`
	ClaimTime          *time.Time     `gorm:"column:claim_time"`
	DeleteTime         *time.Time     `gorm:"column:delete_time"`
	Version            int            `gorm:"column:version;not null;default:1"`
	CreateTime         time.Time      `gorm:"column:create_time;not null"`
	UpdateTime         time.Time      `gorm:"column:update_time;not null"`
}

func (UserGlobalMailStateModel) TableName() string {
	return "user_global_mail_state"
}

type UserGlobalMailRewardLedgerModel struct {
	GrantKey     string    `gorm:"column:grant_key;size:160;primaryKey"`
	RoleID       int64     `gorm:"column:role_id;not null;index:idx_role_mail,priority:1"`
	ServerID     int       `gorm:"column:server_id;not null"`
	GlobalMailID int64     `gorm:"column:global_mail_id;not null;index:idx_role_mail,priority:2"`
	LootIndex    int       `gorm:"column:loot_index;not null"`
	CreateTime   time.Time `gorm:"column:create_time;not null"`
}

func (UserGlobalMailRewardLedgerModel) TableName() string {
	return "user_global_mail_reward_ledger"
}

type UserPersonalMailModel struct {
	MailID     int64          `gorm:"column:mail_id;primaryKey"`
	RoleID     int64          `gorm:"column:role_id;not null;index:idx_role_status_time,priority:1;index:idx_role_create_time,priority:1"`
	ServerID   int            `gorm:"column:server_id;not null"`
	Title      string         `gorm:"column:title;size:256;not null"`
	Content    string         `gorm:"column:content;type:text;not null"`
	TemplateID int64          `gorm:"column:template_id;not null;default:0"`
	Params     datatypes.JSON `gorm:"column:param_list;type:json"`
	Loots      datatypes.JSON `gorm:"column:loots;type:json"`
	Status     string         `gorm:"column:status;size:32;not null;index:idx_role_status_time,priority:2"`
	CreateTime time.Time      `gorm:"column:create_time;not null;index:idx_role_create_time,priority:2"`
	ExpireTime time.Time      `gorm:"column:expire_time;not null;index:idx_role_status_time,priority:3"`
	UpdateTime time.Time      `gorm:"column:update_time;not null"`
}

func (UserPersonalMailModel) TableName() string {
	return "user_personal_mail"
}

type UserMailCursorModel struct {
	RoleID              int64      `gorm:"column:role_id;primaryKey"`
	ServerID            int        `gorm:"column:server_id;not null;index:idx_server_update_time,priority:1"`
	MaxSeenGlobalMailID int64      `gorm:"column:max_seen_global_mail_id;not null;default:0"`
	LastPullTime        *time.Time `gorm:"column:last_pull_time"`
	UpdateTime          time.Time  `gorm:"column:update_time;not null;index:idx_server_update_time,priority:2"`
}

func (UserMailCursorModel) TableName() string {
	return "user_mail_cursor"
}

type GlobalMailOutboxEventModel struct {
	EventID       int64          `gorm:"column:event_id;primaryKey"`
	EventType     string         `gorm:"column:event_type;size:64;not null"`
	AggregateID   int64          `gorm:"column:aggregate_id;not null;index:idx_aggregate_version,priority:1"`
	Version       int64          `gorm:"column:version;not null;index:idx_aggregate_version,priority:2"`
	Payload       datatypes.JSON `gorm:"column:payload;type:json;not null"`
	Status        string         `gorm:"column:status;size:32;not null;index:idx_status_retry,priority:1"`
	RetryCount    int            `gorm:"column:retry_count;not null;default:0"`
	NextRetryTime *time.Time     `gorm:"column:next_retry_time;index:idx_status_retry,priority:2"`
	LockedBy      string         `gorm:"column:locked_by;size:128"`
	LockedUntil   *time.Time     `gorm:"column:locked_until;index:idx_status_retry,priority:3"`
	FailureReason string         `gorm:"column:failure_reason;size:1024"`
	PublishedTime *time.Time     `gorm:"column:published_time"`
	CreateTime    time.Time      `gorm:"column:create_time;not null"`
	UpdateTime    time.Time      `gorm:"column:update_time;not null"`
}

func (GlobalMailOutboxEventModel) TableName() string {
	return "global_mail_outbox_event"
}

type GlobalMailIdempotencyModel struct {
	IdempotencyKey   string         `gorm:"column:idempotency_key;size:128;primaryKey"`
	Action           string         `gorm:"column:action;size:32;not null"`
	RequestHash      string         `gorm:"column:request_hash;size:64;not null"`
	GlobalMailID     int64          `gorm:"column:global_mail_id;not null;index:idx_global_mail_id"`
	TargetVersion    int64          `gorm:"column:target_version;not null"`
	Status           string         `gorm:"column:status;size:32;not null"`
	ResponseSnapshot datatypes.JSON `gorm:"column:response_snapshot;type:json;not null"`
	CreateTime       time.Time      `gorm:"column:create_time;not null"`
	UpdateTime       time.Time      `gorm:"column:update_time;not null"`
}

func (GlobalMailIdempotencyModel) TableName() string {
	return "global_mail_idempotency"
}

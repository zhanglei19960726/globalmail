package sqlschema

const GlobalMailSchema = `
CREATE TABLE IF NOT EXISTS global_mail (
    global_mail_id      BIGINT       NOT NULL COMMENT '全局邮件ID',
    opr_id              BIGINT       NOT NULL COMMENT '操作人或发布单号',
    title               VARCHAR(256) NOT NULL COMMENT '标题',
    content             TEXT         NOT NULL COMMENT '正文',
    msg_map             JSON         NULL     COMMENT '多语言内容',
    loots               JSON         NULL     COMMENT '奖励内容',
    sender              VARCHAR(64)  NOT NULL COMMENT '发件人',
    mail_category       VARCHAR(32)  NOT NULL COMMENT '邮件分类',
    mail_template_id    BIGINT       NOT NULL DEFAULT 0 COMMENT '模板ID',
    param_list          JSON         NULL     COMMENT '模板参数',
    external_url        VARCHAR(512) NULL     COMMENT '外链',
    start_time          DATETIME     NOT NULL COMMENT '生效时间',
    expire_time         DATETIME     NOT NULL COMMENT '过期时间',
    status              VARCHAR(32)  NOT NULL COMMENT 'draft/auditing/published/offline/deleted',
    version             BIGINT       NOT NULL DEFAULT 1 COMMENT '邮件版本',
    create_time         DATETIME     NOT NULL,
    update_time         DATETIME     NOT NULL,
    PRIMARY KEY (global_mail_id),
    KEY idx_status_time (status, start_time, expire_time),
    KEY idx_update_time (update_time)
) COMMENT='全局邮件主表';

CREATE TABLE IF NOT EXISTS global_mail_condition (
    condition_id       BIGINT      NOT NULL COMMENT '条件ID',
    global_mail_id     BIGINT      NOT NULL COMMENT '全局邮件ID',
    condition_type     VARCHAR(64) NOT NULL COMMENT 'server/register_time/channel/country/vip/recharge',
    operator           VARCHAR(16) NOT NULL COMMENT 'eq/ne/gt/gte/lt/lte/in/range',
    condition_value    JSON        NOT NULL COMMENT '条件值',
    create_time        DATETIME    NOT NULL,
    update_time        DATETIME    NOT NULL,
    PRIMARY KEY (condition_id),
    KEY idx_global_mail_id (global_mail_id),
    KEY idx_condition_type (condition_type)
) COMMENT='全局邮件条件表';

CREATE TABLE IF NOT EXISTS user_personal_mail (
    mail_id          BIGINT       NOT NULL COMMENT '个人邮件ID',
    role_id          BIGINT       NOT NULL COMMENT '角色ID',
    server_id        INT          NOT NULL COMMENT '区服ID',
    title            VARCHAR(256) NOT NULL COMMENT '标题',
    content          TEXT         NOT NULL COMMENT '正文',
    template_id      BIGINT       NOT NULL DEFAULT 0 COMMENT '模板ID',
    param_list       JSON         NULL     COMMENT '模板参数',
    loots            JSON         NULL     COMMENT '奖励内容',
    status           VARCHAR(32)  NOT NULL COMMENT 'unread/read/claimed/deleted',
    create_time      DATETIME     NOT NULL,
    expire_time      DATETIME     NOT NULL,
    update_time      DATETIME     NOT NULL,
    PRIMARY KEY (mail_id),
    KEY idx_role_status_time (role_id, status, expire_time),
    KEY idx_role_create_time (role_id, create_time)
) COMMENT='玩家个人邮件表';

CREATE TABLE IF NOT EXISTS user_global_mail_state (
    role_id                BIGINT      NOT NULL COMMENT '角色ID',
    server_id              INT         NOT NULL COMMENT '区服ID',
    global_mail_id         BIGINT      NOT NULL COMMENT '全局邮件ID',
    status                 VARCHAR(32) NOT NULL COMMENT 'unread/read/claimed/deleted',
    claimed_loot_indexes   JSON        NULL     COMMENT '已领取奖励下标',
    claim_time             DATETIME    NULL,
    delete_time            DATETIME    NULL,
    version                INT         NOT NULL DEFAULT 1 COMMENT '状态版本',
    create_time            DATETIME    NOT NULL,
    update_time            DATETIME    NOT NULL,
    PRIMARY KEY (role_id, global_mail_id),
    KEY idx_role_status (role_id, status),
    KEY idx_global_mail_id (global_mail_id)
) COMMENT='玩家全局邮件状态表';

CREATE TABLE IF NOT EXISTS user_global_mail_reward_ledger (
    grant_key      VARCHAR(160) NOT NULL COMMENT '发奖幂等流水',
    role_id        BIGINT       NOT NULL COMMENT '角色ID',
    server_id      INT          NOT NULL COMMENT '区服ID',
    global_mail_id BIGINT       NOT NULL COMMENT '全局邮件ID',
    loot_index     INT          NOT NULL COMMENT '奖励下标',
    status         VARCHAR(32)  NOT NULL COMMENT '发奖状态',
    external_reward_id VARCHAR(128) NOT NULL DEFAULT '' COMMENT '外部奖励服务流水',
    failure_reason VARCHAR(1024) NOT NULL DEFAULT '' COMMENT '失败原因',
    create_time    DATETIME     NOT NULL,
    update_time    DATETIME     NOT NULL,
    PRIMARY KEY (grant_key),
    KEY idx_role_mail (role_id, global_mail_id),
    KEY idx_status (status)
) COMMENT='全局邮件奖励发放幂等流水表';

CREATE TABLE IF NOT EXISTS user_backpack_reward (
    grant_key   VARCHAR(160) NOT NULL COMMENT '背包发放幂等流水',
    role_id     BIGINT       NOT NULL COMMENT '角色ID',
    server_id   INT          NOT NULL COMMENT '区服ID',
    loot_index  INT          NOT NULL COMMENT '奖励下标',
    loot        JSON         NULL     COMMENT '奖励内容快照',
    create_time DATETIME     NOT NULL,
    PRIMARY KEY (grant_key),
    KEY idx_role_create_time (role_id, create_time)
) COMMENT='playersrv 背包奖励发放表';

CREATE TABLE IF NOT EXISTS user_mail_cursor (
    role_id                    BIGINT   NOT NULL COMMENT '角色ID',
    server_id                  INT      NOT NULL COMMENT '区服ID',
    max_seen_global_mail_id    BIGINT   NOT NULL DEFAULT 0 COMMENT '已看过的最大全局邮件ID',
    last_pull_time             DATETIME NULL COMMENT '最近拉取邮件时间',
    update_time                DATETIME NOT NULL,
    PRIMARY KEY (role_id),
    KEY idx_server_update_time (server_id, update_time)
) COMMENT='玩家邮件游标表';

CREATE TABLE IF NOT EXISTS global_mail_outbox_event (
    event_id           BIGINT      NOT NULL COMMENT '事件ID',
    event_type         VARCHAR(64) NOT NULL COMMENT 'GlobalMailChanged',
    aggregate_id       BIGINT      NOT NULL COMMENT 'global_mail_id',
    version            BIGINT      NOT NULL COMMENT '全局邮件版本',
    payload            JSON        NOT NULL COMMENT '事件小payload',
    status             VARCHAR(32) NOT NULL COMMENT 'pending/processing/published/failed',
    retry_count        INT         NOT NULL DEFAULT 0,
    next_retry_time    DATETIME    NULL,
    locked_by          VARCHAR(128) NULL,
    locked_until       DATETIME    NULL,
    failure_reason     VARCHAR(1024) NULL,
    published_time     DATETIME    NULL,
    create_time        DATETIME    NOT NULL,
    update_time        DATETIME    NOT NULL,
    PRIMARY KEY (event_id),
    KEY idx_status_retry (status, next_retry_time, locked_until),
    KEY idx_aggregate_version (aggregate_id, version)
) COMMENT='全局邮件事件Outbox表';

CREATE TABLE IF NOT EXISTS global_mail_idempotency (
    idempotency_key   VARCHAR(128) NOT NULL COMMENT '幂等键',
    action            VARCHAR(32)  NOT NULL COMMENT 'publish/update/offline/delete',
    request_hash      VARCHAR(64)  NOT NULL COMMENT '请求内容hash',
    global_mail_id    BIGINT       NOT NULL COMMENT '全局邮件ID',
    target_version    BIGINT       NOT NULL COMMENT '目标版本',
    status            VARCHAR(32)  NOT NULL COMMENT 'succeeded/failed',
    response_snapshot JSON         NOT NULL COMMENT '历史响应快照',
    create_time       DATETIME     NOT NULL,
    update_time       DATETIME     NOT NULL,
    PRIMARY KEY (idempotency_key),
    KEY idx_global_mail_id (global_mail_id)
) COMMENT='全局邮件发布幂等表';
`

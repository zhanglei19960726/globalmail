package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfigIsValid(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should be valid: %v", err)
	}
	if cfg.Redis.KeyPrefix != "rh:" {
		t.Fatalf("unexpected redis key prefix: %s", cfg.Redis.KeyPrefix)
	}
	if cfg.Etcd.LeaseTTL.Duration != 10*time.Second {
		t.Fatalf("unexpected etcd lease ttl: %s", cfg.Etcd.LeaseTTL.Duration)
	}
	if cfg.Gateway.RouteTTL.Duration != 5*time.Minute {
		t.Fatalf("unexpected gateway route ttl: %s", cfg.Gateway.RouteTTL.Duration)
	}
	if cfg.Gateway.SessionTTL.Duration != 90*time.Second {
		t.Fatalf("unexpected gateway session ttl: %s", cfg.Gateway.SessionTTL.Duration)
	}
	if cfg.Account.LoginTokenTTL.Duration != 2*time.Minute {
		t.Fatalf("unexpected account login token ttl: %s", cfg.Account.LoginTokenTTL.Duration)
	}
	if cfg.Account.PlayerServiceAddr == "" {
		t.Fatal("expected default player service addr")
	}
	if cfg.Player.RequestQueueWorkers != 4 || cfg.Player.RequestQueueCapacity != 1024 {
		t.Fatalf("unexpected player request queue config: %+v", cfg.Player)
	}
	if cfg.Player.RequestRoleQueueCapacity != 32 {
		t.Fatalf("unexpected role queue capacity: %d", cfg.Player.RequestRoleQueueCapacity)
	}
	if cfg.Player.RequestTimeout.Duration != 3*time.Second {
		t.Fatalf("unexpected player request timeout: %s", cfg.Player.RequestTimeout.Duration)
	}
	if cfg.Player.CommandIdempotencyTTL.Duration != 10*time.Minute {
		t.Fatalf("unexpected command idempotency ttl: %s", cfg.Player.CommandIdempotencyTTL.Duration)
	}
	if cfg.Player.GlobalMailPollInterval.Duration != 30*time.Second {
		t.Fatalf("unexpected global mail poll interval: %s", cfg.Player.GlobalMailPollInterval.Duration)
	}
	if cfg.Player.GlobalMailRebuildLockTTL.Duration != 30*time.Second || cfg.Player.GlobalMailRebuildWait.Duration != 100*time.Millisecond {
		t.Fatalf("unexpected global mail rebuild config: %+v", cfg.Player)
	}
	if cfg.MailRelay.LockTTL.Duration != 5*time.Minute || cfg.MailRelay.MaxRetries != 5 {
		t.Fatalf("unexpected mail relay config: %+v", cfg.MailRelay)
	}
}

func TestLoadBytesOverridesDefaults(t *testing.T) {
	cfg, err := LoadBytes([]byte(`
service:
  name: playersrv
  instance_id: playersrv-1
mysql:
  dsn: user:pass@tcp(localhost:3306)/rh
  max_open_conns: 100
  auto_migrate: true
redis:
  addrs:
    - redis-1:6379
    - redis-2:6379
kafka:
  brokers:
    - kafka-1:9092
    - kafka-2:9092
etcd:
  endpoints:
    - etcd-1:2379
    - etcd-2:2379
  lease_ttl: 15s
gateway:
  route_ttl: 10m
  virtual_nodes: 200
  session_ttl: 120s
  gateway_conn_renew_interval: 40s
account:
  login_token_ttl: 3m
  player_service_addr: "playersrv:9001"
player:
  request_queue_workers: 8
  request_queue_capacity: 2048
  request_role_queue_capacity: 64
  request_timeout: 5s
  command_idempotency_ttl: 20m
  global_mail_poll_interval: 45s
  global_mail_rebuild_lock_ttl: 20s
  global_mail_rebuild_wait_interval: 200ms
mail_relay:
  flush_interval: 2s
  fetch_limit: 50
  lock_ttl: 30s
  max_retries: 3
`))
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Service.Name != "playersrv" {
		t.Fatalf("unexpected service name: %s", cfg.Service.Name)
	}
	if cfg.MySQL.DSN == "" {
		t.Fatal("expected mysql dsn from env")
	}
	if cfg.MySQL.MaxOpenConns != 100 {
		t.Fatalf("unexpected max open conns: %d", cfg.MySQL.MaxOpenConns)
	}
	if !cfg.MySQL.AutoMigrate {
		t.Fatal("expected auto migrate enabled")
	}
	if len(cfg.Redis.Addrs) != 2 {
		t.Fatalf("expected two redis addrs, got %d", len(cfg.Redis.Addrs))
	}
	if len(cfg.Kafka.Brokers) != 2 {
		t.Fatalf("expected two kafka brokers, got %d", len(cfg.Kafka.Brokers))
	}
	if cfg.Etcd.LeaseTTL.Duration != 15*time.Second {
		t.Fatalf("unexpected etcd lease ttl: %s", cfg.Etcd.LeaseTTL.Duration)
	}
	if cfg.Redis.KeyPrefix != "rh:" {
		t.Fatalf("expected default redis key prefix, got %s", cfg.Redis.KeyPrefix)
	}
	if cfg.Gateway.RouteTTL.Duration != 10*time.Minute {
		t.Fatalf("unexpected gateway route ttl: %s", cfg.Gateway.RouteTTL.Duration)
	}
	if cfg.Gateway.VirtualNodes != 200 {
		t.Fatalf("unexpected gateway virtual nodes: %d", cfg.Gateway.VirtualNodes)
	}
	if cfg.Gateway.SessionTTL.Duration != 120*time.Second {
		t.Fatalf("unexpected gateway session ttl: %s", cfg.Gateway.SessionTTL.Duration)
	}
	if cfg.Gateway.GatewayConnRenewInterval.Duration != 40*time.Second {
		t.Fatalf("unexpected gateway conn renew interval: %s", cfg.Gateway.GatewayConnRenewInterval.Duration)
	}
	if cfg.Account.LoginTokenTTL.Duration != 3*time.Minute {
		t.Fatalf("unexpected account login token ttl: %s", cfg.Account.LoginTokenTTL.Duration)
	}
	if cfg.Account.PlayerServiceAddr != "playersrv:9001" {
		t.Fatalf("unexpected player service addr: %s", cfg.Account.PlayerServiceAddr)
	}
	if cfg.Player.RequestQueueWorkers != 8 {
		t.Fatalf("unexpected request queue workers: %d", cfg.Player.RequestQueueWorkers)
	}
	if cfg.Player.RequestQueueCapacity != 2048 {
		t.Fatalf("unexpected request queue capacity: %d", cfg.Player.RequestQueueCapacity)
	}
	if cfg.Player.RequestRoleQueueCapacity != 64 {
		t.Fatalf("unexpected role queue capacity: %d", cfg.Player.RequestRoleQueueCapacity)
	}
	if cfg.Player.RequestTimeout.Duration != 5*time.Second {
		t.Fatalf("unexpected request timeout: %s", cfg.Player.RequestTimeout.Duration)
	}
	if cfg.Player.CommandIdempotencyTTL.Duration != 20*time.Minute {
		t.Fatalf("unexpected command idempotency ttl: %s", cfg.Player.CommandIdempotencyTTL.Duration)
	}
	if cfg.Player.GlobalMailPollInterval.Duration != 45*time.Second {
		t.Fatalf("unexpected global mail poll interval: %s", cfg.Player.GlobalMailPollInterval.Duration)
	}
	if cfg.Player.GlobalMailRebuildLockTTL.Duration != 20*time.Second || cfg.Player.GlobalMailRebuildWait.Duration != 200*time.Millisecond {
		t.Fatalf("unexpected global mail rebuild config: %+v", cfg.Player)
	}
	if cfg.MailRelay.FlushInterval.Duration != 2*time.Second || cfg.MailRelay.FetchLimit != 50 || cfg.MailRelay.LockTTL.Duration != 30*time.Second || cfg.MailRelay.MaxRetries != 3 {
		t.Fatalf("unexpected mail relay config: %+v", cfg.MailRelay)
	}
}

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(`
service:
  name: adminsrv
  instance_id: adminsrv-1
`), 0600); err != nil {
		t.Fatalf("write config failed: %v", err)
	}

	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("load config file failed: %v", err)
	}
	if cfg.Service.Name != "adminsrv" {
		t.Fatalf("unexpected service name: %s", cfg.Service.Name)
	}
}

func TestValidateRejectsMissingServiceName(t *testing.T) {
	cfg := Default()
	cfg.Service.Name = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

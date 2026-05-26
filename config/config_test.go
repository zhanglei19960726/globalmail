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
	if cfg.Gate.RouteTTL.Duration != 5*time.Minute {
		t.Fatalf("unexpected gate route ttl: %s", cfg.Gate.RouteTTL.Duration)
	}
	if cfg.Gate.SessionTTL.Duration != 90*time.Second {
		t.Fatalf("unexpected gate session ttl: %s", cfg.Gate.SessionTTL.Duration)
	}
	if cfg.Acc.LoginTokenTTL.Duration != 2*time.Minute {
		t.Fatalf("unexpected acc login token ttl: %s", cfg.Acc.LoginTokenTTL.Duration)
	}
	if cfg.Acc.GameServiceAddr == "" {
		t.Fatal("expected default game service addr")
	}
	if cfg.Game.RequestQueueWorkers != 4 || cfg.Game.RequestQueueCapacity != 1024 {
		t.Fatalf("unexpected game request queue config: %+v", cfg.Game)
	}
	if cfg.Game.RequestRoleQueueCapacity != 32 {
		t.Fatalf("unexpected role queue capacity: %d", cfg.Game.RequestRoleQueueCapacity)
	}
	if cfg.Game.RequestTimeout.Duration != 3*time.Second {
		t.Fatalf("unexpected game request timeout: %s", cfg.Game.RequestTimeout.Duration)
	}
	if cfg.Game.GlobalMailPollInterval.Duration != 30*time.Second {
		t.Fatalf("unexpected global mail poll interval: %s", cfg.Game.GlobalMailPollInterval.Duration)
	}
	if cfg.Outbox.LockTTL.Duration != 5*time.Minute || cfg.Outbox.MaxRetries != 5 {
		t.Fatalf("unexpected outbox config: %+v", cfg.Outbox)
	}
}

func TestLoadBytesOverridesDefaults(t *testing.T) {
	cfg, err := LoadBytes([]byte(`
service:
  name: gamesrv
  instance_id: gamesrv-1
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
gate:
  route_ttl: 10m
  virtual_nodes: 200
  session_ttl: 120s
  gate_conn_renew_interval: 40s
acc:
  login_token_ttl: 3m
  game_service_addr: "gamesrv:9001"
game:
  request_queue_workers: 8
  request_queue_capacity: 2048
  request_role_queue_capacity: 64
  request_timeout: 5s
  global_mail_poll_interval: 45s
outbox:
  flush_interval: 2s
  fetch_limit: 50
  lock_ttl: 30s
  max_retries: 3
`))
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Service.Name != "gamesrv" {
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
	if cfg.Gate.RouteTTL.Duration != 10*time.Minute {
		t.Fatalf("unexpected gate route ttl: %s", cfg.Gate.RouteTTL.Duration)
	}
	if cfg.Gate.VirtualNodes != 200 {
		t.Fatalf("unexpected gate virtual nodes: %d", cfg.Gate.VirtualNodes)
	}
	if cfg.Gate.SessionTTL.Duration != 120*time.Second {
		t.Fatalf("unexpected gate session ttl: %s", cfg.Gate.SessionTTL.Duration)
	}
	if cfg.Gate.GateConnRenewInterval.Duration != 40*time.Second {
		t.Fatalf("unexpected gate conn renew interval: %s", cfg.Gate.GateConnRenewInterval.Duration)
	}
	if cfg.Acc.LoginTokenTTL.Duration != 3*time.Minute {
		t.Fatalf("unexpected acc login token ttl: %s", cfg.Acc.LoginTokenTTL.Duration)
	}
	if cfg.Acc.GameServiceAddr != "gamesrv:9001" {
		t.Fatalf("unexpected game service addr: %s", cfg.Acc.GameServiceAddr)
	}
	if cfg.Game.RequestQueueWorkers != 8 {
		t.Fatalf("unexpected request queue workers: %d", cfg.Game.RequestQueueWorkers)
	}
	if cfg.Game.RequestQueueCapacity != 2048 {
		t.Fatalf("unexpected request queue capacity: %d", cfg.Game.RequestQueueCapacity)
	}
	if cfg.Game.RequestRoleQueueCapacity != 64 {
		t.Fatalf("unexpected role queue capacity: %d", cfg.Game.RequestRoleQueueCapacity)
	}
	if cfg.Game.RequestTimeout.Duration != 5*time.Second {
		t.Fatalf("unexpected request timeout: %s", cfg.Game.RequestTimeout.Duration)
	}
	if cfg.Game.GlobalMailPollInterval.Duration != 45*time.Second {
		t.Fatalf("unexpected global mail poll interval: %s", cfg.Game.GlobalMailPollInterval.Duration)
	}
	if cfg.Outbox.FlushInterval.Duration != 2*time.Second || cfg.Outbox.FetchLimit != 50 || cfg.Outbox.LockTTL.Duration != 30*time.Second || cfg.Outbox.MaxRetries != 3 {
		t.Fatalf("unexpected outbox config: %+v", cfg.Outbox)
	}
}

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(`
service:
  name: mgrsrv
  instance_id: mgrsrv-1
`), 0600); err != nil {
		t.Fatalf("write config failed: %v", err)
	}

	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("load config file failed: %v", err)
	}
	if cfg.Service.Name != "mgrsrv" {
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

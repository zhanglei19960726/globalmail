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

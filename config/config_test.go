package config

import (
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
	if cfg.Etcd.LeaseTTL != 10*time.Second {
		t.Fatalf("unexpected etcd lease ttl: %s", cfg.Etcd.LeaseTTL)
	}
}

func TestLoadFromEnvOverridesDefaults(t *testing.T) {
	t.Setenv("SERVICE_NAME", "gamesrv")
	t.Setenv("SERVICE_INSTANCE_ID", "gamesrv-1")
	t.Setenv("MYSQL_DSN", "user:pass@tcp(localhost:3306)/rh")
	t.Setenv("MYSQL_MAX_OPEN_CONNS", "100")
	t.Setenv("MYSQL_AUTO_MIGRATE", "true")
	t.Setenv("REDIS_ADDRS", "redis-1:6379,redis-2:6379")
	t.Setenv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092")
	t.Setenv("ETCD_ENDPOINTS", "etcd-1:2379,etcd-2:2379")
	t.Setenv("ETCD_LEASE_TTL", "15s")

	cfg, err := LoadFromEnv()
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
	if cfg.Etcd.LeaseTTL != 15*time.Second {
		t.Fatalf("unexpected etcd lease ttl: %s", cfg.Etcd.LeaseTTL)
	}
}

func TestValidateRejectsMissingServiceName(t *testing.T) {
	cfg := Default()
	cfg.Service.Name = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

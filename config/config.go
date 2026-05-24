package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Service ServiceConfig
	MySQL   MySQLConfig
	Redis   RedisConfig
	Kafka   KafkaConfig
	Etcd    EtcdConfig
}

type ServiceConfig struct {
	Name       string
	InstanceID string
	Env        string
	ListenAddr string
	PublicAddr string
}

type MySQLConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	AutoMigrate     bool
}

type RedisConfig struct {
	Addrs        []string
	Username     string
	Password     string
	DB           int
	KeyPrefix    string
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type KafkaConfig struct {
	Brokers       []string
	TopicPrefix   string
	ConsumerGroup string
}

type EtcdConfig struct {
	Endpoints         []string
	Username          string
	Password          string
	DialTimeout       time.Duration
	LeaseTTL          time.Duration
	KeepAliveInterval time.Duration
	ServiceKeyPrefix  string
}

func Default() Config {
	return Config{
		Service: ServiceConfig{
			Name:       "globalmail",
			InstanceID: "local",
			Env:        "dev",
			ListenAddr: ":8080",
			PublicAddr: "127.0.0.1:8080",
		},
		MySQL: MySQLConfig{
			MaxOpenConns:    50,
			MaxIdleConns:    10,
			ConnMaxLifetime: time.Hour,
		},
		Redis: RedisConfig{
			Addrs:        []string{"127.0.0.1:6379"},
			KeyPrefix:    "rh:",
			DialTimeout:  3 * time.Second,
			ReadTimeout:  2 * time.Second,
			WriteTimeout: 2 * time.Second,
		},
		Kafka: KafkaConfig{
			Brokers:     []string{"127.0.0.1:9092"},
			TopicPrefix: "rh",
		},
		Etcd: EtcdConfig{
			Endpoints:         []string{"127.0.0.1:2379"},
			DialTimeout:       3 * time.Second,
			LeaseTTL:          10 * time.Second,
			KeepAliveInterval: 3 * time.Second,
			ServiceKeyPrefix:  "/rh/services",
		},
	}
}

func LoadFromEnv() (Config, error) {
	cfg := Default()

	cfg.Service.Name = envString("SERVICE_NAME", cfg.Service.Name)
	cfg.Service.InstanceID = envString("SERVICE_INSTANCE_ID", cfg.Service.InstanceID)
	cfg.Service.Env = envString("SERVICE_ENV", cfg.Service.Env)
	cfg.Service.ListenAddr = envString("SERVICE_LISTEN_ADDR", cfg.Service.ListenAddr)
	cfg.Service.PublicAddr = envString("SERVICE_PUBLIC_ADDR", cfg.Service.PublicAddr)

	cfg.MySQL.DSN = envString("MYSQL_DSN", cfg.MySQL.DSN)
	cfg.MySQL.MaxOpenConns = envInt("MYSQL_MAX_OPEN_CONNS", cfg.MySQL.MaxOpenConns)
	cfg.MySQL.MaxIdleConns = envInt("MYSQL_MAX_IDLE_CONNS", cfg.MySQL.MaxIdleConns)
	cfg.MySQL.ConnMaxLifetime = envDuration("MYSQL_CONN_MAX_LIFETIME", cfg.MySQL.ConnMaxLifetime)
	cfg.MySQL.AutoMigrate = envBool("MYSQL_AUTO_MIGRATE", cfg.MySQL.AutoMigrate)

	cfg.Redis.Addrs = envCSV("REDIS_ADDRS", cfg.Redis.Addrs)
	cfg.Redis.Username = envString("REDIS_USERNAME", cfg.Redis.Username)
	cfg.Redis.Password = envString("REDIS_PASSWORD", cfg.Redis.Password)
	cfg.Redis.DB = envInt("REDIS_DB", cfg.Redis.DB)
	cfg.Redis.KeyPrefix = envString("REDIS_KEY_PREFIX", cfg.Redis.KeyPrefix)
	cfg.Redis.DialTimeout = envDuration("REDIS_DIAL_TIMEOUT", cfg.Redis.DialTimeout)
	cfg.Redis.ReadTimeout = envDuration("REDIS_READ_TIMEOUT", cfg.Redis.ReadTimeout)
	cfg.Redis.WriteTimeout = envDuration("REDIS_WRITE_TIMEOUT", cfg.Redis.WriteTimeout)

	cfg.Kafka.Brokers = envCSV("KAFKA_BROKERS", cfg.Kafka.Brokers)
	cfg.Kafka.TopicPrefix = envString("KAFKA_TOPIC_PREFIX", cfg.Kafka.TopicPrefix)
	cfg.Kafka.ConsumerGroup = envString("KAFKA_CONSUMER_GROUP", cfg.Kafka.ConsumerGroup)

	cfg.Etcd.Endpoints = envCSV("ETCD_ENDPOINTS", cfg.Etcd.Endpoints)
	cfg.Etcd.Username = envString("ETCD_USERNAME", cfg.Etcd.Username)
	cfg.Etcd.Password = envString("ETCD_PASSWORD", cfg.Etcd.Password)
	cfg.Etcd.DialTimeout = envDuration("ETCD_DIAL_TIMEOUT", cfg.Etcd.DialTimeout)
	cfg.Etcd.LeaseTTL = envDuration("ETCD_LEASE_TTL", cfg.Etcd.LeaseTTL)
	cfg.Etcd.KeepAliveInterval = envDuration("ETCD_KEEPALIVE_INTERVAL", cfg.Etcd.KeepAliveInterval)
	cfg.Etcd.ServiceKeyPrefix = envString("ETCD_SERVICE_KEY_PREFIX", cfg.Etcd.ServiceKeyPrefix)

	return cfg, cfg.Validate()
}

func (c Config) Validate() error {
	if c.Service.Name == "" {
		return errors.New("service name is required")
	}
	if c.Service.InstanceID == "" {
		return errors.New("service instance id is required")
	}
	if c.MySQL.MaxOpenConns < 0 || c.MySQL.MaxIdleConns < 0 {
		return errors.New("mysql connection limits must be non-negative")
	}
	if len(c.Redis.Addrs) == 0 {
		return errors.New("redis addrs are required")
	}
	if len(c.Kafka.Brokers) == 0 {
		return errors.New("kafka brokers are required")
	}
	if len(c.Etcd.Endpoints) == 0 {
		return errors.New("etcd endpoints are required")
	}
	if c.Etcd.LeaseTTL <= 0 {
		return errors.New("etcd lease ttl must be positive")
	}
	if c.Etcd.KeepAliveInterval <= 0 {
		return errors.New("etcd keepalive interval must be positive")
	}
	return nil
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envCSV(key string, fallback []string) []string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

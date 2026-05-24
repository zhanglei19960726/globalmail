package config

import (
	"errors"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		parsed, err := time.ParseDuration(value.Value)
		if err == nil {
			d.Duration = parsed
			return nil
		}
	}
	var seconds int64
	if err := value.Decode(&seconds); err != nil {
		return err
	}
	d.Duration = time.Duration(seconds) * time.Second
	return nil
}

type Config struct {
	Service ServiceConfig `yaml:"service"`
	MySQL   MySQLConfig   `yaml:"mysql"`
	Redis   RedisConfig   `yaml:"redis"`
	Kafka   KafkaConfig   `yaml:"kafka"`
	Etcd    EtcdConfig    `yaml:"etcd"`
}

type ServiceConfig struct {
	Name       string `yaml:"name"`
	InstanceID string `yaml:"instance_id"`
	Env        string `yaml:"env"`
	ListenAddr string `yaml:"listen_addr"`
	PublicAddr string `yaml:"public_addr"`
}

type MySQLConfig struct {
	DSN             string   `yaml:"dsn"`
	MaxOpenConns    int      `yaml:"max_open_conns"`
	MaxIdleConns    int      `yaml:"max_idle_conns"`
	ConnMaxLifetime Duration `yaml:"conn_max_lifetime"`
	AutoMigrate     bool     `yaml:"auto_migrate"`
}

type RedisConfig struct {
	Addrs        []string `yaml:"addrs"`
	Username     string   `yaml:"username"`
	Password     string   `yaml:"password"`
	DB           int      `yaml:"db"`
	KeyPrefix    string   `yaml:"key_prefix"`
	DialTimeout  Duration `yaml:"dial_timeout"`
	ReadTimeout  Duration `yaml:"read_timeout"`
	WriteTimeout Duration `yaml:"write_timeout"`
}

type KafkaConfig struct {
	Brokers       []string `yaml:"brokers"`
	TopicPrefix   string   `yaml:"topic_prefix"`
	ConsumerGroup string   `yaml:"consumer_group"`
}

type EtcdConfig struct {
	Endpoints         []string `yaml:"endpoints"`
	Username          string   `yaml:"username"`
	Password          string   `yaml:"password"`
	DialTimeout       Duration `yaml:"dial_timeout"`
	LeaseTTL          Duration `yaml:"lease_ttl"`
	KeepAliveInterval Duration `yaml:"keepalive_interval"`
	ServiceKeyPrefix  string   `yaml:"service_key_prefix"`
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
			ConnMaxLifetime: Duration{Duration: time.Hour},
		},
		Redis: RedisConfig{
			Addrs:        []string{"127.0.0.1:6379"},
			KeyPrefix:    "rh:",
			DialTimeout:  Duration{Duration: 3 * time.Second},
			ReadTimeout:  Duration{Duration: 2 * time.Second},
			WriteTimeout: Duration{Duration: 2 * time.Second},
		},
		Kafka: KafkaConfig{
			Brokers:     []string{"127.0.0.1:9092"},
			TopicPrefix: "rh",
		},
		Etcd: EtcdConfig{
			Endpoints:         []string{"127.0.0.1:2379"},
			DialTimeout:       Duration{Duration: 3 * time.Second},
			LeaseTTL:          Duration{Duration: 10 * time.Second},
			KeepAliveInterval: Duration{Duration: 3 * time.Second},
			ServiceKeyPrefix:  "/rh/services",
		},
	}
}

func LoadFile(path string) (Config, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	return LoadBytes(payload)
}

func LoadBytes(payload []byte) (Config, error) {
	cfg := Default()
	if err := yaml.Unmarshal(payload, &cfg); err != nil {
		return Config{}, err
	}
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
	if c.Etcd.LeaseTTL.Duration <= 0 {
		return errors.New("etcd lease ttl must be positive")
	}
	if c.Etcd.KeepAliveInterval.Duration <= 0 {
		return errors.New("etcd keepalive interval must be positive")
	}
	return nil
}

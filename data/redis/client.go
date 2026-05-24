package redis

import (
	"globalmail/config"

	goredis "github.com/redis/go-redis/v9"
)

func NewClient(cfg config.RedisConfig) goredis.UniversalClient {
	return goredis.NewUniversalClient(&goredis.UniversalOptions{
		Addrs:        cfg.Addrs,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})
}

package main

import (
	"context"
	"flag"
	"log"

	"globalmail/app/bootstrap"
	"globalmail/app/outboxrelay"
	"globalmail/data/mysql"
	redisdata "globalmail/data/redis"
	"globalmail/domain/globalmail"
	"globalmail/infra/kafka"
)

func main() {
	configPath := bootstrap.ConfigPathFlag("")
	interval := flag.Duration("interval", 0, "outbox flush interval")
	limit := flag.Int("limit", 0, "max events per flush")
	flag.Parse()

	cfg, err := bootstrap.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	db, err := mysql.Open(cfg.MySQL)
	if err != nil {
		log.Fatalf("open mysql: %v", err)
	}
	if cfg.MySQL.AutoMigrate {
		if err := mysql.AutoMigrate(db); err != nil {
			log.Fatalf("auto migrate: %v", err)
		}
	}

	repo := mysql.NewRepository(db)
	redisClient := redisdata.NewClient(cfg.Redis)
	defer redisClient.Close()
	cacheRepo := redisdata.NewRepository(redisClient, cfg.Redis.KeyPrefix)
	publisher := kafka.NewGlobalMailPublisher(cfg.Kafka)
	defer publisher.Close()

	relay := globalmail.NewOutboxRelay(
		repo,
		repo,
		cacheRepo,
		publisher,
		globalmail.WithOutboxRelayWorkerID(cfg.Service.InstanceID),
		globalmail.WithOutboxRelayLockTTL(cfg.Outbox.LockTTL.Duration),
		globalmail.WithOutboxRelayMaxRetries(cfg.Outbox.MaxRetries),
	)
	flushInterval := cfg.Outbox.FlushInterval.Duration
	if *interval > 0 {
		flushInterval = *interval
	}
	fetchLimit := cfg.Outbox.FetchLimit
	if *limit > 0 {
		fetchLimit = *limit
	}
	worker := outboxrelay.NewWorker(relay, fetchLimit, flushInterval, log.Default())

	ctx, stop := bootstrap.SignalContext(context.Background())
	defer stop()

	if err := worker.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("run outbox relay: %v", err)
	}
}

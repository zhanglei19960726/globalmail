package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"globalmail/app/outboxrelay"
	"globalmail/config"
	"globalmail/data/mysql"
	"globalmail/domain/globalmail"
	"globalmail/infra/kafka"
)

func main() {
	configPath := flag.String("config", "config/examples/globalmail.yaml", "path to YAML config file")
	interval := flag.Duration("interval", time.Second, "outbox flush interval")
	limit := flag.Int("limit", 100, "max events per flush")
	flag.Parse()

	cfg, err := config.LoadFile(*configPath)
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
	publisher := kafka.NewGlobalMailPublisher(cfg.Kafka)
	defer publisher.Close()

	relay := globalmail.NewOutboxRelay(repo, publisher)
	worker := outboxrelay.NewWorker(relay, *limit, *interval, log.Default())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := worker.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("run outbox relay: %v", err)
	}
}

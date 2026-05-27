package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"globalmail/app/adminsrv"
	"globalmail/app/bootstrap"
	"globalmail/data/mysql"
	redisdata "globalmail/data/redis"
	"globalmail/domain/globalmail"
)

type timeIDGenerator struct{}

func (timeIDGenerator) NextID() int64 {
	return time.Now().UnixNano()
}

func main() {
	configPath := bootstrap.ConfigPathFlag("")
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

	mysqlRepo := mysql.NewRepository(db)
	redisClient := redisdata.NewClient(cfg.Redis)
	defer redisClient.Close()
	cacheRepo := redisdata.NewRepository(redisClient, cfg.Redis.KeyPrefix)

	publisher := globalmail.NewPublisherService(mysqlRepo, cacheRepo, timeIDGenerator{})
	server := adminsrv.NewServer(publisher)

	log.Printf("adminsrv listening on %s", cfg.Service.ListenAddr)
	if err := http.ListenAndServe(cfg.Service.ListenAddr, server.Handler()); err != nil {
		log.Fatalf("listen: %v", err)
	}
}

package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	"globalmail/api/rpc"
	"globalmail/app/accsrv"
	"globalmail/app/bootstrap"
	redisdata "globalmail/data/redis"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	configPath := bootstrap.ConfigPathFlag("")
	flag.Parse()

	cfg, err := bootstrap.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, stop := bootstrap.SignalContext(context.Background())
	defer stop()

	redisClient := redisdata.NewClient(cfg.Redis)
	defer redisClient.Close()

	gameConn, err := grpc.DialContext(ctx, cfg.Acc.GameServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial gamesrv: %v", err)
	}
	defer gameConn.Close()

	repo := redisdata.NewRepository(redisClient, cfg.Redis.KeyPrefix)
	service := accsrv.NewService(repo, rpc.NewGameServiceClient(gameConn), cfg.Acc.LoginTokenTTL.Duration)
	server := &http.Server{
		Addr:    cfg.Service.ListenAddr,
		Handler: accsrv.NewServer(service).Handler(),
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown accsrv: %v", err)
		}
	}()

	log.Printf("accsrv %s listening on %s", cfg.Service.InstanceID, cfg.Service.ListenAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve accsrv: %v", err)
	}
}

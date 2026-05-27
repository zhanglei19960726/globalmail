package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	"globalmail/api/rpc"
	"globalmail/app/accountsrv"
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

	gameConn, err := grpc.DialContext(ctx, cfg.Account.PlayerServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial playersrv: %v", err)
	}
	defer gameConn.Close()

	repo := redisdata.NewRepository(redisClient, cfg.Redis.KeyPrefix)
	service := accountsrv.NewService(repo, rpc.NewPlayerServiceClient(gameConn), cfg.Account.LoginTokenTTL.Duration)
	server := &http.Server{
		Addr:    cfg.Service.ListenAddr,
		Handler: accountsrv.NewServer(service).Handler(),
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown accountsrv: %v", err)
		}
	}()

	log.Printf("accountsrv %s listening on %s", cfg.Service.InstanceID, cfg.Service.ListenAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve accountsrv: %v", err)
	}
}

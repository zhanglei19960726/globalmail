package main

import (
	"context"
	"flag"
	"log"
	"net"

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
	server := grpc.NewServer()
	rpc.RegisterAccServiceServer(server, accsrv.NewServer(service))

	listener, err := net.Listen("tcp", cfg.Service.ListenAddr)
	if err != nil {
		log.Fatalf("listen accsrv: %v", err)
	}

	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()

	log.Printf("accsrv %s listening on %s", cfg.Service.InstanceID, cfg.Service.ListenAddr)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("serve accsrv: %v", err)
	}
}

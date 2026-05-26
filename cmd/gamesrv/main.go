package main

import (
	"context"
	"flag"
	"log"
	"net"

	"globalmail/api/rpc"
	"globalmail/app/bootstrap"
	"globalmail/app/gamesrv"
	"globalmail/data/mysql"
	redisdata "globalmail/data/redis"
	"globalmail/domain/globalmail"
	infraetcd "globalmail/infra/etcd"
	"globalmail/infra/kafka"

	"google.golang.org/grpc"
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

	db, err := mysql.Open(cfg.MySQL)
	if err != nil {
		log.Fatalf("open mysql: %v", err)
	}
	if cfg.MySQL.AutoMigrate {
		if err := mysql.AutoMigrate(db); err != nil {
			log.Fatalf("auto migrate: %v", err)
		}
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("get sql db: %v", err)
	}
	defer sqlDB.Close()

	redisClient := redisdata.NewClient(cfg.Redis)
	defer redisClient.Close()

	mysqlRepo := mysql.NewRepository(db)
	cacheRepo := redisdata.NewRepository(redisClient, cfg.Redis.KeyPrefix)
	localCache := globalmail.NewLocalCache(mysqlRepo, cacheRepo)
	localCache.SetRebuildLockOptions(cfg.Game.GlobalMailRebuildLockTTL.Duration, cfg.Game.GlobalMailRebuildWait.Duration)
	mailService := gamesrv.NewMailService(mysqlRepo, localCache)

	if err := mailService.ForceRefreshCache(ctx); err != nil {
		log.Printf("initial global mail cache refresh failed: %v", err)
	}

	listener, err := net.Listen("tcp", cfg.Service.ListenAddr)
	if err != nil {
		log.Fatalf("listen gamesrv: %v", err)
	}
	grpcServer := grpc.NewServer()
	rpc.RegisterGameServiceServer(grpcServer, gamesrv.NewAccountService(gamesrv.NewMemoryUserRepository()))
	mailRPCServer := gamesrv.NewMailRPCServer(mailService)
	rpc.RegisterMailServiceServer(grpcServer, mailRPCServer)
	commandRegistry := gamesrv.NewCommandRegistry()
	if err := gamesrv.RegisterMailCommandHandlers(commandRegistry, mailRPCServer); err != nil {
		log.Fatalf("register mail commands: %v", err)
	}
	idempotentDispatcher := gamesrv.NewIdempotentCommandDispatcherWithStore(commandRegistry, cacheRepo, cfg.Game.CommandIdempotencyTTL.Duration)
	commandQueue := gamesrv.NewCommandRequestQueue(idempotentDispatcher, gamesrv.CommandQueueOptions{
		Workers:           cfg.Game.RequestQueueWorkers,
		Capacity:          cfg.Game.RequestQueueCapacity,
		RoleQueueCapacity: cfg.Game.RequestRoleQueueCapacity,
		RequestTimeout:    cfg.Game.RequestTimeout.Duration,
	})
	defer commandQueue.Close()
	rpc.RegisterGameCommandServiceServer(grpcServer, gamesrv.NewCommandRPCServerWithDispatcher(commandRegistry, commandQueue))

	etcdClient, err := infraetcd.NewClient(cfg.Etcd)
	if err != nil {
		log.Fatalf("open etcd: %v", err)
	}
	defer etcdClient.Close()
	registry := infraetcd.NewRegistry(etcdClient, cfg.Etcd)
	keepAlive, err := registry.Register(ctx, infraetcd.Instance{
		ServiceName: cfg.Service.Name,
		InstanceID:  cfg.Service.InstanceID,
		PublicAddr:  cfg.Service.PublicAddr,
		GrpcAddr:    cfg.Service.PublicAddr,
		Status:      "ready",
		Weight:      100,
	})
	if err != nil {
		log.Fatalf("register gamesrv: %v", err)
	}
	defer registry.Unregister(context.Background(), cfg.Service.Name, cfg.Service.InstanceID)
	go func() {
		for range keepAlive {
		}
	}()

	consumer := kafka.NewGlobalMailConsumer(cfg.Kafka, cfg.Kafka.ConsumerGroup)
	defer consumer.Close()

	eventConsumer := gamesrv.NewEventConsumer(consumer, mailService)
	errCh := make(chan error, 3)
	go func() {
		log.Printf("gamesrv %s grpc listening on %s", cfg.Service.InstanceID, cfg.Service.ListenAddr)
		errCh <- grpcServer.Serve(listener)
	}()
	go func() {
		log.Printf("gamesrv %s consuming global mail events", cfg.Service.InstanceID)
		errCh <- eventConsumer.Run(ctx)
	}()
	go func() {
		log.Printf("gamesrv %s polling global mail version", cfg.Service.InstanceID)
		errCh <- gamesrv.RunGlobalMailCachePoller(ctx, mailService, cfg.Game.GlobalMailPollInterval.Duration, log.Default())
	}()
	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	if err := <-errCh; err != nil && err != context.Canceled {
		log.Fatalf("run gamesrv: %v", err)
	}
}

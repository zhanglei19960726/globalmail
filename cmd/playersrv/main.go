package main

import (
	"context"
	"flag"
	"log"
	"net"

	"globalmail/api/rpc"
	"globalmail/app/bootstrap"
	"globalmail/app/playersrv"
	"globalmail/data/mysql"
	redisdata "globalmail/data/redis"
	"globalmail/domain/globalmail"
	infraetcd "globalmail/infra/etcd"
	"globalmail/infra/kafka"
	"globalmail/infra/metrics"

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
	localCache.SetRebuildLockOptions(cfg.Player.GlobalMailRebuildLockTTL.Duration, cfg.Player.GlobalMailRebuildWait.Duration)
	localCache.SetMetrics(metrics.NewExpvarMetrics("playersrv_globalmail"))
	playerMailService := playersrv.NewPlayerMailService(mysqlRepo, localCache)
	playerMailService.SetRewardRepository(playersrv.NewBackpackRewardService(mysqlRepo, mysqlRepo))

	if err := playerMailService.ForceRefreshCache(ctx); err != nil {
		log.Printf("initial global mail cache refresh failed: %v", err)
	}

	listener, err := net.Listen("tcp", cfg.Service.ListenAddr)
	if err != nil {
		log.Fatalf("listen playersrv: %v", err)
	}
	grpcServer := grpc.NewServer()
	rpc.RegisterPlayerServiceServer(grpcServer, playersrv.NewAccountService(playersrv.NewMemoryUserRepository()))
	mailRPCServer := playersrv.NewMailRPCServer(playerMailService)
	rpc.RegisterPlayerMailServiceServer(grpcServer, mailRPCServer)
	commandRegistry := playersrv.NewCommandRegistry()
	if err := playersrv.RegisterMailCommandHandlers(commandRegistry, mailRPCServer); err != nil {
		log.Fatalf("register mail commands: %v", err)
	}
	idempotentDispatcher := playersrv.NewIdempotentCommandDispatcherWithStore(commandRegistry, cacheRepo, cfg.Player.CommandIdempotencyTTL.Duration)
	commandQueue := playersrv.NewCommandRequestQueue(idempotentDispatcher, playersrv.CommandQueueOptions{
		Workers:           cfg.Player.RequestQueueWorkers,
		Capacity:          cfg.Player.RequestQueueCapacity,
		RoleQueueCapacity: cfg.Player.RequestRoleQueueCapacity,
		RequestTimeout:    cfg.Player.RequestTimeout.Duration,
	})
	defer commandQueue.Close()
	rpc.RegisterPlayerCommandServiceServer(grpcServer, playersrv.NewCommandRPCServerWithDispatcher(commandRegistry, commandQueue))

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
		log.Fatalf("register playersrv: %v", err)
	}
	defer registry.Unregister(context.Background(), cfg.Service.Name, cfg.Service.InstanceID)
	go func() {
		for range keepAlive {
		}
	}()

	consumer := kafka.NewGlobalMailConsumer(cfg.Kafka, cfg.Kafka.ConsumerGroup)
	defer consumer.Close()

	eventConsumer := playersrv.NewEventConsumer(consumer, playerMailService)
	errCh := make(chan error, 3)
	go func() {
		log.Printf("playersrv %s grpc listening on %s", cfg.Service.InstanceID, cfg.Service.ListenAddr)
		errCh <- grpcServer.Serve(listener)
	}()
	go func() {
		log.Printf("playersrv %s consuming global mail events", cfg.Service.InstanceID)
		errCh <- eventConsumer.Run(ctx)
	}()
	go func() {
		log.Printf("playersrv %s polling global mail version", cfg.Service.InstanceID)
		errCh <- playersrv.RunGlobalMailCachePoller(ctx, playerMailService, cfg.Player.GlobalMailPollInterval.Duration, log.Default())
	}()
	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	if err := <-errCh; err != nil && err != context.Canceled {
		log.Fatalf("run playersrv: %v", err)
	}
}

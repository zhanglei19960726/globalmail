package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"

	"globalmail/app/bootstrap"
	"globalmail/app/gatesrv"
	redisdata "globalmail/data/redis"
	infraetcd "globalmail/infra/etcd"
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
	routeStore := redisdata.NewRepository(redisClient, cfg.Redis.KeyPrefix)

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
		Status:      "ready",
		Weight:      100,
	})
	if err != nil {
		log.Fatalf("register gatesrv: %v", err)
	}
	defer registry.Unregister(context.Background(), cfg.Service.Name, cfg.Service.InstanceID)
	go func() {
		for range keepAlive {
		}
	}()

	provider := gatesrv.NewEtcdGameServerProvider(registry)
	routeService := gatesrv.NewRouteService(routeStore, provider, cfg.Gate.RouteTTL.Duration, cfg.Gate.VirtualNodes)
	server := &http.Server{
		Addr:    cfg.Service.ListenAddr,
		Handler: gatesrv.NewServer(routeService).Handler(),
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Etcd.KeepAliveInterval.Duration)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown gatesrv: %v", err)
		}
	}()

	log.Printf("gatesrv %s listening on %s", cfg.Service.InstanceID, cfg.Service.ListenAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen gatesrv: %v", err)
	}
}

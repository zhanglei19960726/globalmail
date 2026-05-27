package main

import (
	"context"
	"flag"
	"log"
	"net"

	"globalmail/api/rpc"
	"globalmail/app/bootstrap"
	"globalmail/app/gatewaysrv"
	redisdata "globalmail/data/redis"
	infraetcd "globalmail/infra/etcd"

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
		log.Fatalf("register gatewaysrv: %v", err)
	}
	defer registry.Unregister(context.Background(), cfg.Service.Name, cfg.Service.InstanceID)
	go func() {
		for range keepAlive {
		}
	}()

	provider := gatewaysrv.NewEtcdPlayerServerProvider(registry)
	routeService := gatewaysrv.NewRouteService(routeStore, provider, cfg.Gateway.RouteTTL.Duration, cfg.Gateway.VirtualNodes)
	connections := gatewaysrv.NewConnectionManager(routeStore, cfg.Service.PublicAddr, cfg.Gateway.SessionTTL.Duration, cfg.Gateway.GatewayConnRenewInterval.Duration)
	defer connections.CloseAll()
	server := grpc.NewServer()
	rpc.RegisterGatewayServiceServer(server, gatewaysrv.NewServerWithConnections(routeService, connections))
	listener, err := net.Listen("tcp", cfg.Service.ListenAddr)
	if err != nil {
		log.Fatalf("listen gatewaysrv: %v", err)
	}

	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()

	log.Printf("gatewaysrv %s grpc listening on %s", cfg.Service.InstanceID, cfg.Service.ListenAddr)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("serve gatewaysrv: %v", err)
	}
}

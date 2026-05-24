package gatesrv

import (
	"context"

	infraetcd "globalmail/infra/etcd"
)

type EtcdGameServerProvider struct {
	registry *infraetcd.Registry
}

func NewEtcdGameServerProvider(registry *infraetcd.Registry) *EtcdGameServerProvider {
	return &EtcdGameServerProvider{registry: registry}
}

func (p *EtcdGameServerProvider) ListReadyGameServers(ctx context.Context) ([]GameServerInstance, error) {
	instances, err := p.registry.ListReady(ctx, "gamesrv")
	if err != nil {
		return nil, err
	}
	out := make([]GameServerInstance, 0, len(instances))
	for _, instance := range instances {
		out = append(out, GameServerInstance{
			InstanceID: instance.InstanceID,
			GrpcAddr:   instance.GrpcAddr,
			FrpcAddr:   instance.FrpcAddr,
			Weight:     instance.Weight,
		})
	}
	return out, nil
}

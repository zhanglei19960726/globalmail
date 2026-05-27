package gatewaysrv

import (
	"context"

	infraetcd "globalmail/infra/etcd"
)

type EtcdPlayerServerProvider struct {
	registry *infraetcd.Registry
}

func NewEtcdPlayerServerProvider(registry *infraetcd.Registry) *EtcdPlayerServerProvider {
	return &EtcdPlayerServerProvider{registry: registry}
}

func (p *EtcdPlayerServerProvider) ListReadyPlayerServers(ctx context.Context) ([]PlayerServerInstance, error) {
	instances, err := p.registry.ListReady(ctx, "playersrv")
	if err != nil {
		return nil, err
	}
	out := make([]PlayerServerInstance, 0, len(instances))
	for _, instance := range instances {
		out = append(out, PlayerServerInstance{
			InstanceID: instance.InstanceID,
			GrpcAddr:   instance.GrpcAddr,
			FrpcAddr:   instance.FrpcAddr,
			Weight:     instance.Weight,
		})
	}
	return out, nil
}

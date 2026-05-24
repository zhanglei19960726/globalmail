package etcd

import (
	"context"
	"encoding/json"
	"path"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func (r *Registry) ListReady(ctx context.Context, serviceName string) ([]Instance, error) {
	resp, err := r.client.Get(ctx, r.servicePrefix(serviceName), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	instances := make([]Instance, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var instance Instance
		if err := json.Unmarshal(kv.Value, &instance); err != nil {
			return nil, err
		}
		if instance.Status == "ready" {
			instances = append(instances, instance)
		}
	}
	return instances, nil
}

func (r *Registry) Watch(ctx context.Context, serviceName string) clientv3.WatchChan {
	return r.client.Watch(ctx, r.servicePrefix(serviceName), clientv3.WithPrefix(), clientv3.WithPrevKV())
}

func (r *Registry) servicePrefix(serviceName string) string {
	return path.Join(r.cfg.ServiceKeyPrefix, serviceName) + "/"
}

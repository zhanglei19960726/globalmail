package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"time"

	"globalmail/config"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Instance struct {
	ServiceName string    `json:"service_name"`
	InstanceID  string    `json:"instance_id"`
	PublicAddr  string    `json:"public_addr"`
	GrpcAddr    string    `json:"grpc_addr,omitempty"`
	FrpcAddr    string    `json:"frpc_addr,omitempty"`
	Status      string    `json:"status"`
	Weight      int       `json:"weight"`
	Zone        string    `json:"zone,omitempty"`
	Version     string    `json:"version,omitempty"`
	UpdateTime  time.Time `json:"update_time"`
}

type Registry struct {
	client *clientv3.Client
	cfg    config.EtcdConfig
}

func NewRegistry(client *clientv3.Client, cfg config.EtcdConfig) *Registry {
	return &Registry{client: client, cfg: cfg}
}

func (r *Registry) Register(ctx context.Context, instance Instance) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	if instance.ServiceName == "" {
		instance.ServiceName = "unknown"
	}
	if instance.Status == "" {
		instance.Status = "ready"
	}
	if instance.Weight == 0 {
		instance.Weight = 100
	}
	instance.UpdateTime = time.Now().UTC()

	lease, err := r.client.Grant(ctx, int64(r.cfg.LeaseTTL.Seconds()))
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(instance)
	if err != nil {
		return nil, err
	}
	if _, err := r.client.Put(ctx, r.instanceKey(instance.ServiceName, instance.InstanceID), string(payload), clientv3.WithLease(lease.ID)); err != nil {
		return nil, err
	}
	return r.client.KeepAlive(ctx, lease.ID)
}

func (r *Registry) UpdateStatus(ctx context.Context, serviceName, instanceID, status string) error {
	key := r.instanceKey(serviceName, instanceID)
	resp, err := r.client.Get(ctx, key)
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return fmt.Errorf("service instance not found: %s/%s", serviceName, instanceID)
	}
	var instance Instance
	if err := json.Unmarshal(resp.Kvs[0].Value, &instance); err != nil {
		return err
	}
	instance.Status = status
	instance.UpdateTime = time.Now().UTC()
	payload, err := json.Marshal(instance)
	if err != nil {
		return err
	}
	_, err = r.client.Put(ctx, key, string(payload))
	return err
}

func (r *Registry) Unregister(ctx context.Context, serviceName, instanceID string) error {
	_, err := r.client.Delete(ctx, r.instanceKey(serviceName, instanceID))
	return err
}

func (r *Registry) instanceKey(serviceName, instanceID string) string {
	return path.Join(r.cfg.ServiceKeyPrefix, serviceName, instanceID)
}

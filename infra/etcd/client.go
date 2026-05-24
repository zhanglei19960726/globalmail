package etcd

import (
	"globalmail/config"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func NewClient(cfg config.EtcdConfig) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		Username:    cfg.Username,
		Password:    cfg.Password,
		DialTimeout: cfg.DialTimeout.Duration,
	})
}

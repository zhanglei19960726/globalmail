package routing

import "time"

type LoginToken struct {
	Token    string    `json:"token"`
	UID      int64     `json:"uid"`
	RoleID   int64     `json:"role_id"`
	ServerID int       `json:"server_id"`
	ExpireAt time.Time `json:"expire_at"`
}

type GateConn struct {
	UID            int64     `json:"uid"`
	GatewayAddr    string    `json:"gateway_addr"`
	ClientIP       string    `json:"client_ip"`
	ConnID         string    `json:"conn_id"`
	ConnTime       time.Time `json:"conn_time"`
	LastActiveTime time.Time `json:"last_active_time"`
	DeviceID       string    `json:"device_id"`
	ExpireAt       time.Time `json:"expire_at"`
}

type SrvRouter struct {
	UID        int64     `json:"uid"`
	SrvName    string    `json:"srv_name"`
	InstanceID string    `json:"instance_id"`
	FrpcAddr   string    `json:"frpc_addr"`
	GrpcAddr   string    `json:"grpc_addr"`
	Version    int64     `json:"version"`
	ExpireAt   time.Time `json:"expire_at"`
}

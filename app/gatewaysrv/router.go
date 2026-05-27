package gatewaysrv

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
)

var ErrNoPlayerServer = errors.New("no available playersrv instance")

type PlayerServerInstance struct {
	InstanceID string
	GrpcAddr   string
	FrpcAddr   string
	Weight     int
}

type ConsistentHashRouter struct {
	virtualNodes int
	ring         []uint32
	nodes        map[uint32]PlayerServerInstance
}

func NewConsistentHashRouter(instances []PlayerServerInstance, virtualNodes int) *ConsistentHashRouter {
	if virtualNodes <= 0 {
		virtualNodes = 100
	}
	router := &ConsistentHashRouter{
		virtualNodes: virtualNodes,
		nodes:        make(map[uint32]PlayerServerInstance),
	}
	router.Update(instances)
	return router
}

func (r *ConsistentHashRouter) Update(instances []PlayerServerInstance) {
	r.ring = r.ring[:0]
	r.nodes = make(map[uint32]PlayerServerInstance)

	for _, instance := range instances {
		if instance.InstanceID == "" {
			continue
		}
		weight := instance.Weight
		if weight <= 0 {
			weight = 100
		}
		replicas := r.virtualNodes * weight / 100
		if replicas <= 0 {
			replicas = 1
		}
		for i := 0; i < replicas; i++ {
			key := fmt.Sprintf("%s#%d", instance.InstanceID, i)
			hash := crc32.ChecksumIEEE([]byte(key))
			r.ring = append(r.ring, hash)
			r.nodes[hash] = instance
		}
	}
	sort.Slice(r.ring, func(i, j int) bool {
		return r.ring[i] < r.ring[j]
	})
}

func (r *ConsistentHashRouter) RouteUID(uid int64) (PlayerServerInstance, error) {
	if len(r.ring) == 0 {
		return PlayerServerInstance{}, ErrNoPlayerServer
	}
	hash := crc32.ChecksumIEEE([]byte(strconv.FormatInt(uid, 10)))
	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i] >= hash
	})
	if idx == len(r.ring) {
		idx = 0
	}
	return r.nodes[r.ring[idx]], nil
}

package gatesrv

import (
	"errors"
	"testing"
)

func TestConsistentHashRouterRoutesSameUIDToSameInstance(t *testing.T) {
	router := NewConsistentHashRouter([]GameServerInstance{
		{InstanceID: "game-1", GrpcAddr: "10.0.0.1:9001"},
		{InstanceID: "game-2", GrpcAddr: "10.0.0.2:9001"},
		{InstanceID: "game-3", GrpcAddr: "10.0.0.3:9001"},
	}, 100)

	first, err := router.RouteUID(10001)
	if err != nil {
		t.Fatalf("route uid failed: %v", err)
	}
	for i := 0; i < 10; i++ {
		next, err := router.RouteUID(10001)
		if err != nil {
			t.Fatalf("route uid failed: %v", err)
		}
		if next.InstanceID != first.InstanceID {
			t.Fatalf("route should be stable, first=%s next=%s", first.InstanceID, next.InstanceID)
		}
	}
}

func TestConsistentHashRouterReturnsErrorWithoutInstances(t *testing.T) {
	router := NewConsistentHashRouter(nil, 100)
	_, err := router.RouteUID(10001)
	if !errors.Is(err, ErrNoGameServer) {
		t.Fatalf("expected ErrNoGameServer, got %v", err)
	}
}

func TestConsistentHashRouterMinimizesMovementWhenAddingNode(t *testing.T) {
	before := NewConsistentHashRouter([]GameServerInstance{
		{InstanceID: "game-1"},
		{InstanceID: "game-2"},
		{InstanceID: "game-3"},
	}, 200)
	after := NewConsistentHashRouter([]GameServerInstance{
		{InstanceID: "game-1"},
		{InstanceID: "game-2"},
		{InstanceID: "game-3"},
		{InstanceID: "game-4"},
	}, 200)

	moved := 0
	total := 1000
	for uid := int64(1); uid <= int64(total); uid++ {
		oldNode, err := before.RouteUID(uid)
		if err != nil {
			t.Fatalf("route before failed: %v", err)
		}
		newNode, err := after.RouteUID(uid)
		if err != nil {
			t.Fatalf("route after failed: %v", err)
		}
		if oldNode.InstanceID != newNode.InstanceID {
			moved++
		}
	}

	if moved >= total/2 {
		t.Fatalf("too many keys moved after adding one node: moved=%d total=%d", moved, total)
	}
}

package gamesrv

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"globalmail/api/rpc"
)

type commandDispatcherFunc func(context.Context, *rpc.CommandRequest) (*rpc.CommandResponse, error)

func (f commandDispatcherFunc) Dispatch(ctx context.Context, req *rpc.CommandRequest) (*rpc.CommandResponse, error) {
	return f(ctx, req)
}

func TestCommandRequestQueueDispatchesWithWorker(t *testing.T) {
	queue := NewCommandRequestQueue(commandDispatcherFunc(func(_ context.Context, req *rpc.CommandRequest) (*rpc.CommandResponse, error) {
		return &rpc.CommandResponse{CommandId: req.GetCommandId(), Seq: req.GetSeq(), Code: 0}, nil
	}), CommandQueueOptions{Workers: 1, Capacity: 1, RoleQueueCapacity: 1})
	defer queue.Close()

	resp, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{
		CommandId: rpc.CommandID_COMMAND_ID_MAIL_LIST_GLOBAL,
		RoleId:    20001,
		Seq:       100,
	})
	if err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}
	if resp.GetCode() != 0 || resp.GetSeq() != 100 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestCommandRequestQueueReturnsFullResponse(t *testing.T) {
	started := make(chan struct{})
	var startedOnce sync.Once
	release := make(chan struct{})
	queue := NewCommandRequestQueue(commandDispatcherFunc(func(ctx context.Context, req *rpc.CommandRequest) (*rpc.CommandResponse, error) {
		startedOnce.Do(func() { close(started) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return &rpc.CommandResponse{CommandId: req.GetCommandId(), Seq: req.GetSeq()}, nil
		}
	}), CommandQueueOptions{Workers: 1, Capacity: 2, RoleQueueCapacity: 1})
	defer queue.Close()

	firstDone := make(chan error, 1)
	go func() {
		_, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{RoleId: 20001, Seq: 1})
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected first request to start")
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{RoleId: 20001, Seq: 2})
		secondDone <- err
	}()
	waitForRoleQueueDepth(t, queue, 20001, 1)

	resp, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{
		CommandId: rpc.CommandID_COMMAND_ID_MAIL_DELETE,
		RoleId:    20001,
		Seq:       3,
	})
	if err != nil {
		t.Fatalf("dispatch should return queue full response, got error: %v", err)
	}
	if resp.GetCode() != 429 || resp.GetSeq() != 3 {
		t.Fatalf("expected queue full response, got %+v", resp)
	}

	close(release)

	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first request failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected first request to finish")
	}
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second request failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected second request to finish")
	}
}

func TestCommandRequestQueueRejectsAfterClose(t *testing.T) {
	queue := NewCommandRequestQueue(commandDispatcherFunc(func(context.Context, *rpc.CommandRequest) (*rpc.CommandResponse, error) {
		return nil, nil
	}), CommandQueueOptions{Workers: 1, Capacity: 1, RoleQueueCapacity: 1})
	queue.Close()

	_, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{})
	if !errors.Is(err, ErrCommandQueueClosed) {
		t.Fatalf("expected ErrCommandQueueClosed, got %v", err)
	}
}

func TestCommandRequestQueueReturnsTimeoutResponse(t *testing.T) {
	queue := NewCommandRequestQueue(commandDispatcherFunc(func(ctx context.Context, req *rpc.CommandRequest) (*rpc.CommandResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}), CommandQueueOptions{Workers: 1, Capacity: 1, RoleQueueCapacity: 1, RequestTimeout: 10 * time.Millisecond})
	defer queue.Close()

	resp, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{
		CommandId: rpc.CommandID_COMMAND_ID_MAIL_CLAIM,
		RoleId:    20001,
		Seq:       9,
	})
	if err != nil {
		t.Fatalf("dispatch should return timeout response, got error: %v", err)
	}
	if resp.GetCode() != 504 || resp.GetSeq() != 9 {
		t.Fatalf("expected timeout response, got %+v", resp)
	}
}

func TestCommandRequestQueueReleasesWorkerAfterTimeout(t *testing.T) {
	block := make(chan struct{})
	queue := NewCommandRequestQueue(commandDispatcherFunc(func(context.Context, *rpc.CommandRequest) (*rpc.CommandResponse, error) {
		<-block
		return &rpc.CommandResponse{Code: 0}, nil
	}), CommandQueueOptions{Workers: 1, Capacity: 1, RoleQueueCapacity: 1, RequestTimeout: 10 * time.Millisecond})
	defer queue.Close()
	defer close(block)

	resp, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{RoleId: 20001, Seq: 1})
	if err != nil {
		t.Fatalf("first dispatch failed: %v", err)
	}
	if resp.GetCode() != 504 {
		t.Fatalf("expected first dispatch timeout, got %+v", resp)
	}

	resp, err = queue.Dispatch(context.Background(), &rpc.CommandRequest{RoleId: 20002, Seq: 2})
	if err != nil {
		t.Fatalf("second dispatch failed: %v", err)
	}
	if resp.GetCode() != 504 {
		t.Fatalf("expected second dispatch timeout, got %+v", resp)
	}
}

func TestCommandRequestQueueSerializesSameRole(t *testing.T) {
	started := make(chan int64, 2)
	releaseFirst := make(chan struct{})
	queue := NewCommandRequestQueue(commandDispatcherFunc(func(ctx context.Context, req *rpc.CommandRequest) (*rpc.CommandResponse, error) {
		started <- req.GetSeq()
		if req.GetSeq() == 1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-releaseFirst:
			}
		}
		return &rpc.CommandResponse{Seq: req.GetSeq()}, nil
	}), CommandQueueOptions{Workers: 2, Capacity: 4, RoleQueueCapacity: 4})
	defer queue.Close()

	firstDone := make(chan error, 1)
	go func() {
		_, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{RoleId: 20001, Seq: 1})
		firstDone <- err
	}()
	if got := <-started; got != 1 {
		t.Fatalf("expected first request to start, got seq %d", got)
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{RoleId: 20001, Seq: 2})
		secondDone <- err
	}()
	select {
	case got := <-started:
		t.Fatalf("same role request should wait, got seq %d", got)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first request failed: %v", err)
	}
	if got := <-started; got != 2 {
		t.Fatalf("expected second request after first finished, got seq %d", got)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second request failed: %v", err)
	}
}

func TestCommandRequestQueueRunsDifferentRolesIndependently(t *testing.T) {
	roleOneStarted := make(chan struct{})
	releaseRoleOne := make(chan struct{})
	queue := NewCommandRequestQueue(commandDispatcherFunc(func(ctx context.Context, req *rpc.CommandRequest) (*rpc.CommandResponse, error) {
		if req.GetRoleId() == 20001 {
			close(roleOneStarted)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-releaseRoleOne:
			}
		}
		return &rpc.CommandResponse{Seq: req.GetSeq()}, nil
	}), CommandQueueOptions{Workers: 2, Capacity: 4, RoleQueueCapacity: 2})
	defer queue.Close()

	firstDone := make(chan error, 1)
	go func() {
		_, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{RoleId: 20001, Seq: 1})
		firstDone <- err
	}()
	select {
	case <-roleOneStarted:
	case <-time.After(time.Second):
		t.Fatal("expected role one request to start")
	}

	resp, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{RoleId: 20002, Seq: 2})
	if err != nil {
		t.Fatalf("different role dispatch failed: %v", err)
	}
	if resp.GetSeq() != 2 {
		t.Fatalf("unexpected different role response: %+v", resp)
	}

	close(releaseRoleOne)
	if err := <-firstDone; err != nil {
		t.Fatalf("role one request failed: %v", err)
	}
}

func waitForRoleQueueDepth(t *testing.T, queue *CommandRequestQueue, roleID int64, depth int) {
	t.Helper()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("expected role queue depth %d", depth)
		case <-ticker.C:
			queue.mu.Lock()
			lane := queue.lanes[roleID]
			got := 0
			if lane != nil {
				got = len(lane.jobs)
			}
			queue.mu.Unlock()
			if got == depth {
				return
			}
		}
	}
}

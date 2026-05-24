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
	}), CommandQueueOptions{Workers: 1, Capacity: 1})
	defer queue.Close()

	resp, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{
		CommandId: rpc.CommandID_COMMAND_ID_MAIL_LIST_GLOBAL,
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
	}), CommandQueueOptions{Workers: 1, Capacity: 1})
	defer queue.Close()

	firstDone := make(chan error, 1)
	go func() {
		_, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{Seq: 1})
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected first request to start")
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{Seq: 2})
		secondDone <- err
	}()
	waitForQueueDepth(t, queue, 1)

	resp, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{
		CommandId: rpc.CommandID_COMMAND_ID_MAIL_DELETE,
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
	}), CommandQueueOptions{Workers: 1, Capacity: 1})
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
	}), CommandQueueOptions{Workers: 1, Capacity: 1, RequestTimeout: 10 * time.Millisecond})
	defer queue.Close()

	resp, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{
		CommandId: rpc.CommandID_COMMAND_ID_MAIL_CLAIM,
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
	}), CommandQueueOptions{Workers: 1, Capacity: 1, RequestTimeout: 10 * time.Millisecond})
	defer queue.Close()
	defer close(block)

	resp, err := queue.Dispatch(context.Background(), &rpc.CommandRequest{Seq: 1})
	if err != nil {
		t.Fatalf("first dispatch failed: %v", err)
	}
	if resp.GetCode() != 504 {
		t.Fatalf("expected first dispatch timeout, got %+v", resp)
	}

	resp, err = queue.Dispatch(context.Background(), &rpc.CommandRequest{Seq: 2})
	if err != nil {
		t.Fatalf("second dispatch failed: %v", err)
	}
	if resp.GetCode() != 504 {
		t.Fatalf("expected second dispatch timeout, got %+v", resp)
	}
}

func waitForQueueDepth(t *testing.T, queue *CommandRequestQueue, depth int) {
	t.Helper()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("expected queue depth %d, got %d", depth, len(queue.jobs))
		case <-ticker.C:
			if len(queue.jobs) == depth {
				return
			}
		}
	}
}

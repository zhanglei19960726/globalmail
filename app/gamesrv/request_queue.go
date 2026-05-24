package gamesrv

import (
	"context"
	"errors"
	"sync"
	"time"

	"globalmail/api/rpc"
)

var ErrCommandQueueClosed = errors.New("command queue closed")

type CommandDispatcher interface {
	Dispatch(ctx context.Context, req *rpc.CommandRequest) (*rpc.CommandResponse, error)
}

type CommandQueueOptions struct {
	Workers        int
	Capacity       int
	RequestTimeout time.Duration
}

type CommandRequestQueue struct {
	dispatcher     CommandDispatcher
	jobs           chan commandJob
	done           chan struct{}
	requestTimeout time.Duration
	closeOnce      sync.Once
	wg             sync.WaitGroup
}

type commandJob struct {
	ctx    context.Context
	req    *rpc.CommandRequest
	result chan commandResult
}

type commandResult struct {
	resp *rpc.CommandResponse
	err  error
}

func NewCommandRequestQueue(dispatcher CommandDispatcher, opts CommandQueueOptions) *CommandRequestQueue {
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.Capacity <= 0 {
		opts.Capacity = 1024
	}
	queue := &CommandRequestQueue{
		dispatcher:     dispatcher,
		jobs:           make(chan commandJob, opts.Capacity),
		done:           make(chan struct{}),
		requestTimeout: opts.RequestTimeout,
	}
	for i := 0; i < opts.Workers; i++ {
		queue.wg.Add(1)
		go queue.worker()
	}
	return queue
}

func (q *CommandRequestQueue) Dispatch(ctx context.Context, req *rpc.CommandRequest) (*rpc.CommandResponse, error) {
	dispatchCtx := ctx
	cancel := func() {}
	if q.requestTimeout > 0 {
		dispatchCtx, cancel = context.WithTimeout(ctx, q.requestTimeout)
	}
	defer cancel()

	result := make(chan commandResult, 1)
	job := commandJob{ctx: dispatchCtx, req: req, result: result}

	select {
	case <-dispatchCtx.Done():
		return q.contextDoneResponse(ctx, dispatchCtx, req)
	case <-q.done:
		return nil, ErrCommandQueueClosed
	case q.jobs <- job:
	default:
		return commandQueueFullResponse(req), nil
	}

	select {
	case <-dispatchCtx.Done():
		return q.contextDoneResponse(ctx, dispatchCtx, req)
	case <-q.done:
		return nil, ErrCommandQueueClosed
	case res := <-result:
		if errors.Is(res.err, context.DeadlineExceeded) {
			return commandTimeoutResponse(req), nil
		}
		return res.resp, res.err
	}
}

func (q *CommandRequestQueue) Close() {
	q.closeOnce.Do(func() {
		close(q.done)
	})
	q.wg.Wait()
}

func (q *CommandRequestQueue) worker() {
	defer q.wg.Done()
	for {
		select {
		case <-q.done:
			return
		case job := <-q.jobs:
			q.handle(job)
		}
	}
}

func (q *CommandRequestQueue) handle(job commandJob) {
	select {
	case <-job.ctx.Done():
		job.result <- commandResult{err: job.ctx.Err()}
		return
	default:
	}
	result := make(chan commandResult, 1)
	go func() {
		resp, err := q.dispatcher.Dispatch(job.ctx, job.req)
		result <- commandResult{resp: resp, err: err}
	}()
	select {
	case <-job.ctx.Done():
		job.result <- commandResult{err: job.ctx.Err()}
	case res := <-result:
		job.result <- res
	}
}

func commandQueueFullResponse(req *rpc.CommandRequest) *rpc.CommandResponse {
	return &rpc.CommandResponse{
		CommandId: req.GetCommandId(),
		Seq:       req.GetSeq(),
		Code:      429,
		Message:   "command queue full",
	}
}

func commandTimeoutResponse(req *rpc.CommandRequest) *rpc.CommandResponse {
	return &rpc.CommandResponse{
		CommandId: req.GetCommandId(),
		Seq:       req.GetSeq(),
		Code:      504,
		Message:   "command request timeout",
	}
}

func (q *CommandRequestQueue) contextDoneResponse(parent, dispatch context.Context, req *rpc.CommandRequest) (*rpc.CommandResponse, error) {
	if errors.Is(dispatch.Err(), context.DeadlineExceeded) && parent.Err() == nil {
		return commandTimeoutResponse(req), nil
	}
	return nil, dispatch.Err()
}

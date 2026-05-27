package playersrv

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
	Workers           int
	Capacity          int
	RoleQueueCapacity int
	RequestTimeout    time.Duration
}

type CommandRequestQueue struct {
	dispatcher        CommandDispatcher
	capacity          chan struct{}
	workerSlots       chan struct{}
	done              chan struct{}
	requestTimeout    time.Duration
	roleQueueCapacity int
	lanes             map[int64]*commandRoleLane
	mu                sync.Mutex
	closeOnce         sync.Once
	wg                sync.WaitGroup
}

type commandJob struct {
	ctx         context.Context
	req         *rpc.CommandRequest
	result      chan commandResult
	releaseOnce *sync.Once
	release     func()
}

type commandResult struct {
	resp *rpc.CommandResponse
	err  error
}

type commandRoleLane struct {
	roleID int64
	jobs   chan commandJob
	queue  *CommandRequestQueue
}

func NewCommandRequestQueue(dispatcher CommandDispatcher, opts CommandQueueOptions) *CommandRequestQueue {
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.Capacity <= 0 {
		opts.Capacity = 1024
	}
	if opts.RoleQueueCapacity <= 0 {
		opts.RoleQueueCapacity = 32
	}
	queue := &CommandRequestQueue{
		dispatcher:        dispatcher,
		capacity:          make(chan struct{}, opts.Capacity),
		workerSlots:       make(chan struct{}, opts.Workers),
		done:              make(chan struct{}),
		requestTimeout:    opts.RequestTimeout,
		roleQueueCapacity: opts.RoleQueueCapacity,
		lanes:             make(map[int64]*commandRoleLane),
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
	job := commandJob{
		ctx:         dispatchCtx,
		req:         req,
		result:      result,
		releaseOnce: &sync.Once{},
		release: func() {
			<-q.capacity
		},
	}

	select {
	case <-dispatchCtx.Done():
		return q.contextDoneResponse(ctx, dispatchCtx, req)
	case <-q.done:
		return nil, ErrCommandQueueClosed
	case q.capacity <- struct{}{}:
	default:
		return commandQueueFullResponse(req), nil
	}

	lane := q.laneFor(commandRoleID(req))
	select {
	case <-dispatchCtx.Done():
		job.releaseCapacity()
		return q.contextDoneResponse(ctx, dispatchCtx, req)
	case <-q.done:
		job.releaseCapacity()
		return nil, ErrCommandQueueClosed
	case lane.jobs <- job:
	default:
		job.releaseCapacity()
		return commandQueueFullResponse(req), nil
	}

	select {
	case <-dispatchCtx.Done():
		job.releaseCapacity()
		return q.contextDoneResponse(ctx, dispatchCtx, req)
	case <-q.done:
		job.releaseCapacity()
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

func (q *CommandRequestQueue) laneFor(roleID int64) *commandRoleLane {
	q.mu.Lock()
	defer q.mu.Unlock()
	if lane, ok := q.lanes[roleID]; ok {
		return lane
	}
	lane := &commandRoleLane{
		roleID: roleID,
		jobs:   make(chan commandJob, q.roleQueueCapacity),
		queue:  q,
	}
	q.lanes[roleID] = lane
	q.wg.Add(1)
	go lane.run()
	return lane
}

func (l *commandRoleLane) run() {
	defer l.queue.wg.Done()
	for {
		select {
		case <-l.queue.done:
			return
		case job := <-l.jobs:
			l.queue.handle(job)
		}
	}
}

func (q *CommandRequestQueue) acquireWorker(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.done:
		return ErrCommandQueueClosed
	case q.workerSlots <- struct{}{}:
		return nil
	}
}

func (q *CommandRequestQueue) releaseWorker() {
	<-q.workerSlots
}

func (job *commandJob) releaseCapacity() {
	job.releaseOnce.Do(job.release)
}

func commandRoleID(req *rpc.CommandRequest) int64 {
	if req.GetRoleId() != 0 {
		return req.GetRoleId()
	}
	return req.GetUid()
}

func (q *CommandRequestQueue) handle(job commandJob) {
	defer job.releaseCapacity()
	select {
	case <-job.ctx.Done():
		job.result <- commandResult{err: job.ctx.Err()}
		return
	default:
	}
	if err := q.acquireWorker(job.ctx); err != nil {
		job.result <- commandResult{err: err}
		return
	}
	var releaseWorker sync.Once
	release := func() {
		releaseWorker.Do(q.releaseWorker)
	}

	result := make(chan commandResult, 1)
	go func() {
		defer release()
		resp, err := q.dispatcher.Dispatch(job.ctx, job.req)
		result <- commandResult{resp: resp, err: err}
	}()
	select {
	case <-job.ctx.Done():
		release()
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

package pool

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/tychoish/amboy"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/recovery"
)

type abortablePool struct {
	size     int
	started  bool
	wg       sync.WaitGroup
	mu       sync.RWMutex
	canceler context.CancelFunc
	queue    amboy.Queue
	log      grip.Journaler
	jobs     map[string]context.CancelFunc
}

// NewAbortablePool produces a simple implementation of a worker pool
// that provides access to cancel running jobs. The cancellation
// functions work by creating context cancelation function and then
// canceling the contexts passed to the jobs specifically.
func NewAbortablePool(opts *WorkerOptions) amboy.AbortableRunner {
	opts.setDefaults()
	return &abortablePool{
		queue: opts.Queue,
		size:  opts.NumWorkers,
		log:   opts.Logger,
		jobs:  map[string]context.CancelFunc{},
	}
}

func (p *abortablePool) Started() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.started
}

func (p *abortablePool) SetQueue(q amboy.Queue) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return errors.New("cannot set queue after the pool has started")
	}

	p.queue = q

	return nil
}

func (p *abortablePool) Close(ctx context.Context) {
	var (
		wg  *sync.WaitGroup
		log grip.Journaler
	)

	func() {
		p.mu.Lock()
		defer p.mu.Unlock()

		if p.canceler != nil {
			p.canceler()
			p.canceler = nil
			p.started = false
		}

		for id, closer := range p.jobs {
			if ctx.Err() != nil {
				return
			}

			closer()
			delete(p.jobs, id)
		}
		wg = &p.wg
		log = p.log
	}()

	wait := make(chan struct{})
	go func() {
		defer recovery.SendStackTraceAndContinue(log, "waiting for close")
		defer close(wait)
		p.mu.Lock()
		defer p.mu.Unlock()
		wg.Wait()
	}()

	select {
	case <-ctx.Done():
	case <-wait:
	}
}

func (p *abortablePool) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return nil
	}

	if p.queue == nil {
		return errors.New("runner must have an embedded queue")
	}

	workerCtx, cancel := context.WithCancel(ctx)
	p.canceler = cancel
	p.started = true

	for w := 1; w <= p.size; w++ {
		go p.worker(workerCtx)
		p.log.Debugf("started worker %d of %d waiting for jobs", w, p.size)
	}

	return nil
}

func (p *abortablePool) worker(bctx context.Context) {
	var (
		err    error
		job    amboy.Job
		ctx    context.Context
		cancel context.CancelFunc
	)

	p.mu.Lock()
	p.wg.Add(1)
	p.mu.Unlock()

	defer p.wg.Done()
	defer func() {
		// if we hit a panic we want to add an error to the job;
		err = recovery.SendMessageWithPanicError(recover(), nil, p.log, "worker process encountered error")
		if err != nil {
			if job != nil {
				job.AddError(err)
				_ = p.queue.Complete(bctx, job)
			}

			// start a replacement worker
			go p.worker(bctx)
		}

		if cancel != nil {
			cancel()
		}
	}()

	timer := time.NewTimer(baseJobInterval)
	for {
		select {
		case <-bctx.Done():
			return
		case <-timer.C:
			job, err := p.queue.Next(bctx)
			if err != nil {
				timer.Reset(jitterNilJobWait())
				continue
			}

			ctx, cancel = context.WithCancel(bctx)
			p.runJob(ctx, job)
			cancel()
			timer.Reset(baseJobInterval)
		}
	}
}

func (p *abortablePool) addCanceler(id string, cancel context.CancelFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.jobs[id] = cancel
}

func (p *abortablePool) runJob(ctx context.Context, job amboy.Job) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)

	p.addCanceler(job.ID(), cancel)

	defer func() {
		if ctx.Err() == nil {
			p.mu.Lock()
			defer p.mu.Unlock()
			delete(p.jobs, job.ID())
		}
	}()

	executeJob(ctx, p.log, "abortable", job, p.queue)
}

func (p *abortablePool) IsRunning(id string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.jobs[id]

	return ok
}

func (p *abortablePool) RunningJobs() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	out := []string{}

	for id := range p.jobs {
		out = append(out, id)
	}

	return out
}

func (p *abortablePool) Abort(ctx context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	cancel, ok := p.jobs[id]
	if !ok {
		return errors.Errorf("job '%s' is not defined", id)
	}
	cancel()
	delete(p.jobs, id)

	job, err := p.queue.Get(ctx, id)
	if err != nil {
		return errors.Wrapf(err, "could not find '%s' in the queue", id)
	}

	stat := job.Status()
	stat.Canceled = false
	stat.Completed = true
	job.SetStatus(stat)

	return errors.WithStack(p.queue.Complete(ctx, job))
}

func (p *abortablePool) AbortAll(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for id, cancel := range p.jobs {
		if ctx.Err() != nil {
			break
		}
		cancel()
		delete(p.jobs, id)
		job, err := p.queue.Get(ctx, id)
		if err != nil {
			continue
		}
		stat := job.Status()
		stat.Canceled = false
		stat.Completed = true
		job.SetStatus(stat)

		_ = p.queue.Complete(ctx, job)
	}
}

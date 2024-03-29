/*
Rate Limiting Pools

Amboy includes two rate limiting pools, to control the flow of tasks
processed by the queue. The "simple" implementation sleeps for a
configurable interval in-between each task, while the averaged tool,
uses an exponential weighted average and a targeted number of tasks to
complete over an interval to achieve a reasonable flow of tasks
through the runner.
*/
package pool

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/tychoish/amboy"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/recovery"
)

// NewSimpleRateLimitedWorkers returns a worker pool that sleeps for
// the specified interval after completing each task. After that
// interval, the runner will run the next available task as soon as its ready.
//
// The constructor returns an error if the size (number of workers) is
// less than 1 or the interval is less than a millisecond.
func NewSimpleRateLimitedWorkers(sleepInterval time.Duration, opts *WorkerOptions) (amboy.Runner, error) {
	errs := []string{}

	if opts.NumWorkers < 1 {
		errs = append(errs, "cannot specify a pool size less than 1")
	}

	if sleepInterval < time.Millisecond {
		errs = append(errs, "cannot specify a sleep interval less than a millisecond.")
	}

	if opts.Queue == nil {
		errs = append(errs, "cannot specify a nil queue")
	}

	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "; "))
	}

	opts.setDefaults()
	p := &simpleRateLimited{
		size:     opts.NumWorkers,
		interval: sleepInterval,
		queue:    opts.Queue,
		log:      opts.Logger,
	}

	return p, nil
}

type simpleRateLimited struct {
	size     int
	log      grip.Logger
	interval time.Duration
	queue    amboy.Queue
	canceler context.CancelFunc
	wg       sync.WaitGroup
	mu       sync.RWMutex
}

func (p *simpleRateLimited) Started() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.canceler != nil
}

func (p *simpleRateLimited) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.canceler != nil {
		return nil
	}
	if p.queue == nil {
		return errors.New("runner must have an embedded queue")
	}

	ctx, p.canceler = context.WithCancel(ctx)

	// start some threads
	for w := 1; w <= p.size; w++ {
		go p.worker(ctx)
		grip.Debugf("started rate limited worker %d of %d ", w, p.size)
	}
	return nil
}

func (p *simpleRateLimited) worker(bctx context.Context) {
	var (
		err    error
		ctx    context.Context
		cancel context.CancelFunc
		job    amboy.Job
	)

	p.mu.Lock()
	p.wg.Add(1)
	p.mu.Unlock()

	defer p.wg.Done()

	defer func() {
		err = recovery.SendMessageWithPanicError(recover(), nil, p.log, "worker process encountered error")
		if err != nil {
			if job != nil {
				job.AddError(err)
				_ = p.queue.Complete(bctx, job)
			}
			if bctx.Err() == nil && p.canceler != nil {
				// start a replacement worker.
				go p.worker(bctx)
			}
		}
		if cancel != nil {
			cancel()
		}
	}()

	timer := time.NewTimer(0)
	defer timer.Stop()
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
			executeJob(ctx, p.log, "rate-limited-simple", job, p.queue)

			cancel()
			timer.Reset(p.interval)
		}
	}
}

func (p *simpleRateLimited) SetQueue(q amboy.Queue) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.canceler != nil {
		return errors.New("cannot change queue on active runner")
	}

	p.queue = q
	return nil
}

func (p *simpleRateLimited) Close(ctx context.Context) {
	var (
		wg  *sync.WaitGroup
		log grip.Logger
	)

	func() {
		p.mu.Lock()
		defer p.mu.Unlock()

		if p.canceler != nil {
			p.canceler()
			p.canceler = nil
		}
		wg = &p.wg
		log = p.log
	}()

	// because of the timer+2 contexts in the worker
	// implementation, we can end up returning earlier and because
	// pools are restartable, end up calling wait more than once,
	// which doesn't affect behavior but does cause this to panic in
	// tests
	defer func() { _ = recover() }()

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

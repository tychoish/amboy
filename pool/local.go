/*
Local Workers Pool

The LocalWorkers is a simple worker pool implementation that spawns a
collection of (n) workers and dispatches jobs to worker threads, that
consume work items from the Queue's Next() method.
*/
package pool

import (
	"context"
	"errors"
	"sync"

	"github.com/cdr/amboy"
	"github.com/cdr/grip"
	"github.com/cdr/grip/logging"
	"github.com/cdr/grip/message"
	"github.com/cdr/grip/recovery"
)

// WorkerOptions describes the arguments passed to the constructors of
// worker pools. Queue must not be nil.
type WorkerOptions struct {
	Logger     grip.Journaler
	Queue      amboy.Queue
	NumWorkers int
}

func (opts *WorkerOptions) setDefaults() {
	if opts.NumWorkers < 1 {
		opts.NumWorkers = 1
	}

	if opts.Logger == nil {
		opts.Logger = logging.MakeGrip(grip.GetSender())
	}

	opts.Logger.CriticalWhen(opts.Queue == nil, "cannot construct a pool without a queue")
}

// NewLocalWorkers is a constructor for pool of worker processes that
// execute jobs from a queue locally, and takes arguments for
// the number of worker processes and a amboy.Queue object.
func NewLocalWorkers(opts *WorkerOptions) amboy.Runner {
	opts.setDefaults()

	return &localWorkers{
		queue: opts.Queue,
		log:   opts.Logger,
		size:  opts.NumWorkers,
	}
}

// localWorkers is a very minimal implementation of a worker pool, and
// supports a configurable number of workers to process Job tasks.
type localWorkers struct {
	size     int
	started  bool
	wg       sync.WaitGroup
	canceler context.CancelFunc
	queue    amboy.Queue
	log      grip.Journaler
	mu       sync.RWMutex
}

// SetQueue allows callers to inject alternate amboy.Queue objects into
// constructed Runner objects. Returns an error if the Runner has
// started.
func (r *localWorkers) SetQueue(q amboy.Queue) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return errors.New("cannot add new queue after starting a runner")
	}

	r.queue = q
	return nil
}

// Started returns true when the Runner has begun executing tasks. For
// localWorkers this means that workers are running.
func (r *localWorkers) Started() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.started
}

// Start initializes all worker process, and returns an error if the
// Runner does not have a queue.
func (r *localWorkers) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		return nil
	}

	if r.queue == nil {
		return errors.New("runner must have an embedded queue")
	}

	workerCtx, cancel := context.WithCancel(ctx)
	r.canceler = cancel

	for w := 1; w <= r.size; w++ {
		go worker(workerCtx, r.log, "local", r.queue, &r.wg)

		r.log.Debug(message.Fields{
			"worker_id": w,
			"total":     r.size,
		})
	}
	r.started = true

	r.log.Debug(message.Fields{
		"message": "running workers",
		"total":   r.size,
	})

	return nil
}

// Close terminates all worker processes as soon as possible.
func (r *localWorkers) Close(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.canceler != nil {
		r.canceler()
		r.canceler = nil
		r.started = false
	}

	wait := make(chan struct{})
	go func() {
		defer recovery.SendStackTraceAndContinue(r.log, "waiting for close")
		defer close(wait)
		r.wg.Wait()
	}()

	select {
	case <-ctx.Done():
	case <-wait:
	}
}

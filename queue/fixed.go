package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/pool"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/message"
	"github.com/tychoish/grip/recovery"
)

type FixedSizeQueueOptions struct {
	Workers  int
	Capacity int
	Logger   grip.Logger
}

func (opts *FixedSizeQueueOptions) setDefaults() {
	if opts.Logger.Sender() == nil {
		opts.Logger = grip.NewLogger(grip.Sender())
	}
}

// LocalLimitedSize implements the amboy.Queue interface, and unlike
// other implementations, the size of the queue is limited for both
// incoming tasks and completed tasks. This makes it possible to use
// these queues in situations as parts of services and in
// longer-running contexts.
//
// Specify a capacity when constructing the queue; the queue will
// store no more than 2x the number specified, and no more the
// specified capacity of completed jobs.
type limitedSizeLocal struct {
	channel      chan amboy.Job
	toDelete     chan string
	capacity     int
	storage      map[string]amboy.Job
	scopes       ScopeManager
	dispatcher   Dispatcher
	lifetimeCtx  context.Context
	log          grip.Logger
	deletedCount int
	staleCount   int
	id           string
	runner       amboy.Runner
	mu           sync.RWMutex
}

// NewLocalLimitedSize constructs a LocalLimitedSize queue instance
// with the specified number of workers and capacity.
func NewLocalLimitedSize(opts *FixedSizeQueueOptions) amboy.Queue {
	opts.setDefaults()
	q := &limitedSizeLocal{
		capacity: opts.Capacity,
		storage:  make(map[string]amboy.Job),
		scopes:   NewLocalScopeManager(),
		id:       fmt.Sprintf("queue.local.unordered.fixed.%s", uuid.New().String()),
	}
	q.log = opts.Logger
	q.dispatcher = NewDispatcher(q, q.log)
	q.runner = pool.NewLocalWorkers(&pool.WorkerOptions{Logger: q.log, NumWorkers: opts.Workers, Queue: q})
	return q
}

func (q *limitedSizeLocal) ID() string {
	return q.id
}

// Put adds a job to the queue, returning an error if the queue isn't
// opened, a task of that name exists has been completed (and is
// stored in the results storage,) or is pending, and finally if the
// queue is at capacity.
func (q *limitedSizeLocal) Put(ctx context.Context, j amboy.Job) error {
	if !q.Info().Started {
		return fmt.Errorf("queue not open. could not add %s", j.ID())
	}

	j.UpdateTimeInfo(amboy.JobTimeInfo{
		Created: time.Now(),
	})

	if err := j.TimeInfo().Validate(); err != nil {
		return fmt.Errorf("invalid job timeinfo: %w", err)
	}

	name := j.ID()

	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.storage[name]; ok {
		return amboy.NewDuplicateJobErrorf("cannot dispatch '%s', already complete", name)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("queue full, cannot add %q: %w", name, ctx.Err())
	case q.channel <- j:
		q.storage[name] = j
		return nil
	}
}

func (q *limitedSizeLocal) Save(ctx context.Context, j amboy.Job) error {
	if !q.Info().Started {
		return fmt.Errorf("queue not open. could not add %s", j.ID())
	}

	name := j.ID()
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.storage[name]; !ok {
		return fmt.Errorf("cannot save '%s', which is not tracked", name)
	}

	q.storage[name] = j
	return nil
}

// Get returns a job, by name. This will include all tasks currently
// stored in the queue.
func (q *limitedSizeLocal) Get(ctx context.Context, name string) (amboy.Job, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	j, ok := q.storage[name]
	if !ok {
		return nil, amboy.NewJobNotDefinedError(q, name)
	}
	return j, nil
}

// Next returns the next pending job, and is used by amboy.Runner
// implementations to fetch work. This operation blocks until a job is
// available or the context is canceled.
func (q *limitedSizeLocal) Next(ctx context.Context) (amboy.Job, error) {
	misses := 0
	for {
		if misses > q.capacity {
			return nil, errors.New("not pending job")
		}

		select {
		case job := <-q.channel:
			if _, err := q.Get(ctx, job.ID()); err != nil {
				// if the job's been deleted just continue
				continue
			}

			ti := job.TimeInfo()
			if ti.IsStale() {
				q.mu.Lock()
				delete(q.storage, job.ID())
				q.staleCount++
				q.mu.Unlock()

				q.log.Notice(message.Fields{
					"state":    "stale",
					"job":      job.ID(),
					"job_type": job.Type().Name,
				})
				misses++
				continue
			}

			if !ti.IsDispatchable() {
				go q.requeue(job)
				misses++
				continue
			}

			if err := q.dispatcher.Dispatch(ctx, job); err != nil {
				go q.requeue(job)
				misses++
				continue
			}

			if err := q.scopes.Acquire(job.ID(), job.Scopes()); err != nil {
				q.dispatcher.Release(ctx, job)
				go q.requeue(job)
				misses++
				continue
			}

			return job, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (q *limitedSizeLocal) requeue(job amboy.Job) {
	defer recovery.LogStackTraceAndContinue("re-queue waiting job", job.ID())
	select {
	case <-q.lifetimeCtx.Done():
	case q.channel <- job:
	}
}

func (q *limitedSizeLocal) Info() amboy.QueueInfo {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return amboy.QueueInfo{
		Started:     q.channel != nil,
		LockTimeout: amboy.LockTimeout,
	}
}

// Jobs is a generator of all completed tasks in the queue.
func (q *limitedSizeLocal) Jobs(ctx context.Context) <-chan amboy.Job {
	q.mu.RLock()
	defer q.mu.RUnlock()

	out := make(chan amboy.Job, len(q.storage))
	for name := range q.storage {
		out <- q.storage[name]
	}
	close(out)

	return out
}

// Runner returns the Queue's embedded amboy.Runner instance.
func (q *limitedSizeLocal) Runner() amboy.Runner {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.runner
}

// SetRunner allows callers to, if the queue has not started, inject a
// different runner implementation.
func (q *limitedSizeLocal) SetRunner(r amboy.Runner) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.channel != nil {
		return errors.New("cannot set runner on started queue")
	}

	q.runner = r

	return nil
}

// Stats returns information about the current state of jobs in the
// queue, and the amount of work completed.
func (q *limitedSizeLocal) Stats(ctx context.Context) amboy.QueueStats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	s := amboy.QueueStats{
		Total:     len(q.storage) + q.staleCount + q.deletedCount,
		Completed: len(q.toDelete) + q.deletedCount,
		Pending:   len(q.channel),
	}
	s.Running = s.Total - s.Completed - s.Pending - q.staleCount
	return s
}

// Complete marks a job complete in the queue.
func (q *limitedSizeLocal) Complete(ctx context.Context, j amboy.Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if stat := j.Status(); stat.Canceled {
		q.dispatcher.Release(ctx, j)
		return nil
	}

	if err := q.dispatcher.Complete(ctx, j); err != nil {
		return err
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	// save it
	status := j.Status()
	status.InProgress = false
	status.ModificationTime = time.Now()
	status.ModificationCount += 1
	j.SetStatus(status)
	q.storage[j.ID()] = j

	if len(q.toDelete) == q.capacity-1 {
		id := <-q.toDelete
		if _, ok := q.storage[id]; ok {
			delete(q.storage, id)
			q.deletedCount++
		}
	}

	q.log.Alert(message.WrapError(
		q.scopes.Release(j.ID(), j.Scopes()),
		message.Fields{
			"id":     j.ID(),
			"scopes": j.Scopes(),
			"queue":  q.ID(),
			"op":     "releasing scope lock during completion",
		}))

	q.toDelete <- j.ID()
	return nil
}

// Start starts the runner and initializes the pending task
// storage. Only produces an error if the underlying runner fails to
// start.
func (q *limitedSizeLocal) Start(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.channel != nil {
		return errors.New("cannot start a running queue")
	}

	q.lifetimeCtx = ctx
	q.toDelete = make(chan string, q.capacity)
	q.channel = make(chan amboy.Job, q.capacity)

	err := q.runner.Start(ctx)
	if err != nil {
		return err
	}

	q.log.Debug("queue job server running")

	return nil
}

func (q *limitedSizeLocal) Close(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.runner != nil || q.runner.Started() {
		q.runner.Close(ctx)
	}

	return q.dispatcher.Close(ctx)
}

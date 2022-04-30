/*
Local Shuffled Queue

The shuffled queue is functionally similar to the LocalUnordered Queue
(which is, in fact, a FIFO queue as a result of its implementation);
however, the shuffled queue dispatches tasks randomized, using the
properties of Go's map type, which is not dependent on insertion
order.

Additionally this implementation does not using locking, which may
improve performance for some workloads. Intentionally, the
implementation retains pointers to all completed tasks, and does not
cap the number of pending tasks.
*/
package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/pool"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/message"
	"github.com/tychoish/grip/recovery"
)

// LocalShuffled provides a queue implementation that shuffles the
// order of jobs, relative the insertion order. Unlike
// some of the other local queue implementations that predate LocalShuffled
// (e.g. LocalUnordered,) there are no mutexes uses in the implementation.
type shuffledLocal struct {
	operations chan func(map[string]amboy.Job, map[string]amboy.Job, map[string]amboy.Job, *fixedStorage)
	capacity   int
	id         string
	starter    *sync.Once
	scopes     ScopeManager
	dispatcher Dispatcher
	log        grip.Logger
	runner     amboy.Runner
}

// NewShuffledLocal provides a queue implementation that shuffles the
// order of jobs, relative the insertion order.
func NewShuffledLocal(opts *FixedSizeQueueOptions) amboy.Queue {
	opts.setDefaults()
	q := &shuffledLocal{
		scopes:   NewLocalScopeManager(),
		capacity: opts.Capacity,
		id:       fmt.Sprintf("queue.local.unordered.shuffled.%s", uuid.New().String()),
		log:      opts.Logger,
		starter:  &sync.Once{},
	}
	q.dispatcher = NewDispatcher(q, q.log)
	q.runner = pool.NewLocalWorkers(&pool.WorkerOptions{Logger: q.log, NumWorkers: opts.Workers, Queue: q})
	return q
}

func (q *shuffledLocal) ID() string { return q.id }

// Start takes a context object and starts the embedded Runner instance
// and the queue's own background dispatching thread. Returns an error
// if there is no embedded runner, but is safe to call multiple times.
func (q *shuffledLocal) Start(ctx context.Context) error {
	if q.runner == nil {
		return errors.New("cannot start queue without a runner")
	}

	q.starter.Do(func() {
		q.operations = make(chan func(map[string]amboy.Job, map[string]amboy.Job, map[string]amboy.Job, *fixedStorage))
		go q.reactor(ctx)
		q.log.Error(q.runner.Start(ctx))
		q.log.Info("started shuffled job storage rector")
	})

	return nil
}

// reactor is the background dispatching process.
func (q *shuffledLocal) reactor(ctx context.Context) {
	defer recovery.LogStackTraceAndExit("shuffled amboy queue reactor")

	pending := make(map[string]amboy.Job)
	completed := make(map[string]amboy.Job)
	dispatched := make(map[string]amboy.Job)
	toDelete := newFixedStorage(q.capacity)

	for {
		select {
		case op := <-q.operations:
			op(pending, completed, dispatched, toDelete)
		case <-ctx.Done():
			q.log.Info("shuffled storage reactor closing")
			return
		}
	}
}

// Put adds a job to the queue, and returns errors if the queue hasn't
// started or if a job with the same ID value already exists.
func (q *shuffledLocal) Put(ctx context.Context, j amboy.Job) error {
	id := j.ID()

	if !q.Info().Started {
		return errors.Errorf("cannot put job %s; queue not started", id)
	}

	j.UpdateTimeInfo(amboy.JobTimeInfo{
		Created: time.Now(),
	})

	if err := j.TimeInfo().Validate(); err != nil {
		return errors.Wrap(err, "invalid job timeinfo")
	}

	ret := make(chan error)
	op := func(
		pending map[string]amboy.Job,
		completed map[string]amboy.Job,
		dispatched map[string]amboy.Job,
		toDelete *fixedStorage,
	) {
		defer close(ret)
		_, isPending := pending[id]
		_, isCompleted := completed[id]
		_, isDispatched := dispatched[id]

		if isPending || isCompleted || isDispatched {
			ret <- amboy.NewDuplicateJobErrorf("job '%s' already exists", id)
			return
		}

		pending[id] = j
	}

	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case q.operations <- op:
		return <-ret
	}
}

func (q *shuffledLocal) Save(ctx context.Context, j amboy.Job) error {
	id := j.ID()

	if !q.Info().Started {
		return errors.Errorf("cannot save job %s; queue not started", id)
	}

	ret := make(chan error)
	op := func(
		pending map[string]amboy.Job,
		completed map[string]amboy.Job,
		dispatched map[string]amboy.Job,
		toDelete *fixedStorage,
	) {
		defer close(ret)
		if _, ok := pending[id]; ok {
			pending[id] = j
			return
		}

		if _, ok := completed[id]; ok {
			completed[id] = j
			return
		}

		if _, ok := dispatched[id]; ok {
			dispatched[id] = j
			return
		}

		ret <- errors.Errorf("job '%s' does not exist", id)
	}

	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case q.operations <- op:
		return <-ret
	}
}

// Get returns a job based on the specified ID. Considers all pending,
// completed, and in progress jobs.
func (q *shuffledLocal) Get(ctx context.Context, name string) (amboy.Job, error) {
	if !q.Info().Started {
		return nil, errors.New("queue is not started")
	}

	ret := make(chan amboy.Job)
	op := func(
		pending map[string]amboy.Job,
		completed map[string]amboy.Job,
		dispatched map[string]amboy.Job,
		toDelete *fixedStorage,
	) {
		defer close(ret)

		if job, ok := pending[name]; ok {
			ret <- job
			return
		}

		if job, ok := completed[name]; ok {
			ret <- job
			return
		}

		if job, ok := dispatched[name]; ok {
			ret <- job
			return
		}
	}

	select {
	case <-ctx.Done():
		return nil, errors.WithStack(ctx.Err())
	case q.operations <- op:
		job, ok := <-ret
		if !ok {
			return nil, amboy.NewJobNotDefinedError(q, name)
		}
		return job, nil
	}
}

// Jobs returns all completed jobs processed by the queue.
func (q *shuffledLocal) Jobs(ctx context.Context) <-chan amboy.Job {
	output := make(chan amboy.Job)

	if !q.Info().Started {
		close(output)
		return output
	}

	q.operations <- func(
		pending map[string]amboy.Job,
		completed map[string]amboy.Job,
		dispatched map[string]amboy.Job,
		toDelete *fixedStorage,
	) {

		defer close(output)

		for _, job := range completed {
			select {
			case <-ctx.Done():
				return
			case output <- job:
				continue
			}
		}
		for _, job := range pending {
			select {
			case <-ctx.Done():
				return
			case output <- job:
				continue
			}
		}
		for _, job := range dispatched {
			select {
			case <-ctx.Done():
				return
			case output <- job:
				continue
			}
		}
	}

	return output
}

// Stats returns a standard report on the number of pending, running,
// and completed jobs processed by the queue.
func (q *shuffledLocal) Stats(ctx context.Context) amboy.QueueStats {
	if !q.Info().Started {
		return amboy.QueueStats{}
	}

	ret := make(chan amboy.QueueStats)
	q.operations <- func(
		pending map[string]amboy.Job,
		completed map[string]amboy.Job,
		dispatched map[string]amboy.Job,
		toDelete *fixedStorage,
	) {
		stat := amboy.QueueStats{
			Running:   len(dispatched),
			Pending:   len(pending),
			Completed: len(completed),
		}

		stat.Total = stat.Running + stat.Pending + stat.Completed

		ret <- stat
		close(ret)
	}

	select {
	case <-ctx.Done():
		return amboy.QueueStats{}
	case out := <-ret:
		return out
	}
}

func (q *shuffledLocal) Info() amboy.QueueInfo {
	return amboy.QueueInfo{
		Started:     q.operations != nil,
		LockTimeout: amboy.LockTimeout,
	}
}

// Next returns a new pending job, and is used by the Runner interface
// to fetch new jobs. This method returns a nil job object is there are
// no pending jobs.
func (q *shuffledLocal) Next(ctx context.Context) (amboy.Job, error) {
	ret := make(chan amboy.Job)

	op := func(
		pending map[string]amboy.Job,
		completed map[string]amboy.Job,
		dispatched map[string]amboy.Job,
		toDelete *fixedStorage,
	) {
		defer close(ret)

		for id, j := range pending {
			if j.TimeInfo().IsStale() {
				delete(pending, j.ID())
				continue
			}

			if err := q.scopes.Acquire(j.ID(), j.Scopes()); err != nil {
				continue
			}

			select {
			case <-ctx.Done():
				return
			case ret <- j:
				dispatched[id] = j
				delete(pending, id)
				return
			}
		}
	}

	select {
	case <-ctx.Done():
		return nil, errors.WithStack(ctx.Err())
	case q.operations <- op:
		j := <-ret
		if j == nil {
			return nil, errors.New("could not dispatch job")
		}
		if err := q.dispatcher.Dispatch(ctx, j); err != nil {
			_ = q.Put(ctx, j)
			return nil, errors.WithStack(err)
		}

		return j, nil
	}
}

// Complete marks a job as complete in the internal representation. If
// the context is canceled after calling Complete but before it
// executes, no change occurs.
func (q *shuffledLocal) Complete(ctx context.Context, j amboy.Job) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}

	var op func(map[string]amboy.Job, map[string]amboy.Job, map[string]amboy.Job, *fixedStorage)
	if stat := j.Status(); stat.Canceled {
		q.dispatcher.Release(ctx, j)

		op = func(
			pending map[string]amboy.Job,
			completed map[string]amboy.Job,
			dispatched map[string]amboy.Job,
			toDelete *fixedStorage,
		) {
			id := j.ID()
			delete(dispatched, id)
			pending[id] = j
		}

	} else if err := q.dispatcher.Complete(ctx, j); err != nil {
		return errors.WithStack(err)
	} else {
		op = func(
			pending map[string]amboy.Job,
			completed map[string]amboy.Job,
			dispatched map[string]amboy.Job,
			toDelete *fixedStorage,
		) {
			id := j.ID()

			completed[id] = j
			delete(dispatched, id)
			toDelete.Push(id)

			if num := toDelete.Oversize(); num > 0 {
				for i := 0; i < num; i++ {
					delete(completed, toDelete.Pop())
				}
			}

			q.log.Warning(message.WrapError(
				q.scopes.Release(j.ID(), j.Scopes()),
				message.Fields{
					"id":     j.ID(),
					"scopes": j.Scopes(),
					"queue":  q.ID(),
					"op":     "releasing scope lock during completion",
				}))
		}

	}

	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case q.operations <- op:
		return nil
	}
}

func (q *shuffledLocal) Delete(ctx context.Context, id string) error {
	res := make(chan error)

	op := func(
		pending map[string]amboy.Job,
		completed map[string]amboy.Job,
		dispatched map[string]amboy.Job,
		toDelete *fixedStorage,
	) {
		defer close(res)

		if _, ok := dispatched[id]; ok {
			res <- errors.New("cannot delete dispatched job")
			return
		}

		if num := toDelete.Delete(id); num != 0 {
			return
		}

		if _, ok := pending[id]; ok {
			delete(pending, id)
			return
		}

		if _, ok := completed[id]; ok {
			delete(pending, id)
			return
		}
	}

	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case q.operations <- op:
		return <-res
	}
}

// SetRunner modifies the embedded amboy.Runner instance, and return an
// error if the current runner has started.
func (q *shuffledLocal) SetRunner(r amboy.Runner) error {
	if q.runner != nil && q.runner.Started() {
		return errors.New("cannot set a runner, current runner is running")
	}

	q.runner = r
	return r.SetQueue(q)
}

// Runner returns the embedded runner.
func (q *shuffledLocal) Runner() amboy.Runner {
	return q.runner
}

func (q *shuffledLocal) Close(ctx context.Context) error {
	if q.runner != nil || q.runner.Started() {
		q.runner.Close(ctx)
	}

	return q.dispatcher.Close(ctx)
}

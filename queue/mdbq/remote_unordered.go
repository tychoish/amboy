package mdbq

import (
	"context"

	"github.com/cdr/amboy"
	"github.com/cdr/amboy/pool"
	"github.com/cdr/amboy/queue"
	"github.com/cdr/grip"
	"github.com/pkg/errors"
)

// RemoteUnordered are queues that use a Driver as backend for job
// storage and processing and do not impose any additional ordering
// beyond what's provided by the driver.
type remoteUnordered struct {
	*remoteBase
}

// newRemoteUnordered returns a queue that has been initialized with a
// local worker pool Runner instance of the specified size.
func newRemoteUnordered(size int, logger grip.Journaler) remoteQueue {
	q := &remoteUnordered{
		remoteBase: newRemoteBase(),
	}
	q.log = logger
	q.dispatcher = queue.NewDispatcher(q, q.log)
	q.log.Error(q.SetRunner(pool.NewLocalWorkers(&pool.WorkerOptions{NumWorkers: size, Queue: q, Logger: logger})))
	q.log.Infof("creating new remote job queue with %d workers", size)

	return q
}

// Next returns a Job from the queue. Returns a nil Job object if the
// context is canceled. The operation is blocking until an
// undispatched, unlocked job is available. This operation takes a job
// lock.
func (q *remoteUnordered) Next(ctx context.Context) (amboy.Job, error) {
	count := 0
	for {
		count++
		select {
		case <-ctx.Done():
			return nil, errors.WithStack(ctx.Err())
		case job := <-q.channel:
			if job == nil {
				return nil, errors.New("no dispatchable job")
			}
			return job, nil
		}
	}
}

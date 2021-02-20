package queue

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/tychoish/amboy"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/message"
	"github.com/tychoish/grip/recovery"
)

// Dispatcher provides a common mechanism shared between queue
// implementations to handle job locking to prevent multiple workers
// from running the same job.
type Dispatcher interface {
	Dispatch(context.Context, amboy.Job) error
	Release(context.Context, amboy.Job)
	Complete(context.Context, amboy.Job) error
	Close(context.Context) error
}

type dispatcherImpl struct {
	queue amboy.Queue
	mutex sync.Mutex
	cache map[string]dispatcherInfo
	log   grip.Journaler
}

// NewDispatcher constructs a default dispatching implementation.
func NewDispatcher(q amboy.Queue, log grip.Journaler) Dispatcher {
	return &dispatcherImpl{
		queue: q,
		cache: map[string]dispatcherInfo{},
		log:   log,
	}
}

type dispatcherInfo struct {
	job        amboy.Job
	jobContext context.Context
	jobCancel  context.CancelFunc
	stopPing   context.CancelFunc
}

func (d *dispatcherImpl) Dispatch(ctx context.Context, job amboy.Job) error {
	if job == nil {
		return errors.New("cannot dispatch nil job")
	}

	if !amboy.IsDispatchable(job.Status(), d.queue.Info().LockTimeout) {
		return errors.New("cannot dispatch in progress or completed job")
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	ti := amboy.JobTimeInfo{
		Start: time.Now(),
	}
	job.UpdateTimeInfo(ti)

	if stat := job.Status(); stat.Canceled {
		stat.Canceled = false
		job.SetStatus(stat)
	}

	if err := job.Lock(d.queue.ID(), d.queue.Info().LockTimeout); err != nil {
		return errors.Wrap(err, "problem locking job")
	}

	if err := d.queue.Save(ctx, job); err != nil {
		return errors.Wrap(err, "problem saving job state")
	}

	info := dispatcherInfo{
		job: job,
	}
	maxTime := job.TimeInfo().MaxTime
	if maxTime > 0 {
		info.jobContext, info.jobCancel = context.WithTimeout(ctx, maxTime)
	} else {
		info.jobContext, info.jobCancel = context.WithCancel(ctx)
	}

	var pingerCtx context.Context
	pingerCtx, info.stopPing = context.WithCancel(ctx)

	go func() {
		defer recovery.LogStackTraceAndContinue("background lock ping", job.ID())
		iters := 0
		ticker := time.NewTicker(d.queue.Info().LockTimeout / 4)
		defer ticker.Stop()
		for {
			select {
			case <-pingerCtx.Done():
				stat := job.Status()

				// if we get here, then the job is
				// being canceled, and we should try
				// and save that.
				if stat.InProgress && !stat.Completed {
					stat.Canceled = true
					stat.InProgress = false
					job.SetStatus(stat)
					// best effort to save the job
					_ = d.queue.Save(ctx, job)
				}

				return
			case <-ticker.C:
				if err := job.Lock(d.queue.ID(), d.queue.Info().LockTimeout); err != nil {
					job.AddError(errors.Wrapf(err, "problem pinging job lock on cycle #%d", iters))
					info.jobCancel()
					return
				}
				if err := d.queue.Save(ctx, job); err != nil {
					job.AddError(errors.Wrapf(err, "problem saving job for lock ping on cycle #%d", iters))
					info.jobCancel()
					return
				}
				d.log.Debug(message.Fields{
					"queue_id":  d.queue.ID(),
					"job_id":    job.ID(),
					"ping_iter": iters,
					"stat":      job.Status(),
				})
			}
			iters++
		}
	}()
	d.cache[job.ID()] = info

	return nil
}

func (d *dispatcherImpl) Release(ctx context.Context, job amboy.Job) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if info, ok := d.cache[job.ID()]; ok {
		info.stopPing()
		info.jobCancel()
		delete(d.cache, job.ID())
	}
}

func (d *dispatcherImpl) Complete(ctx context.Context, job amboy.Job) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	info, ok := d.cache[job.ID()]
	if !ok {
		return nil
	}
	delete(d.cache, job.ID())

	ti := job.TimeInfo()
	ti.End = time.Now()
	job.UpdateTimeInfo(ti)

	if info.jobContext.Err() != nil && job.Error() == nil {
		job.AddError(errors.New("job was aborted during execution"))
	}

	if stat := job.Status(); stat.Canceled && stat.Completed {
		stat.Completed = false
		job.SetStatus(stat)
	}

	info.stopPing()
	info.jobCancel()
	return nil
}

func (d *dispatcherImpl) Close(ctx context.Context) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for name, info := range d.cache {
		info.stopPing()
		info.jobCancel()
		delete(d.cache, name)
	}
	return nil
}

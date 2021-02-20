package pool

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/tychoish/amboy"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/message"
	"github.com/tychoish/grip/recovery"
)

const (
	nilJobWaitIntervalMax = time.Second
	baseJobInterval       = time.Millisecond
)

func jitterNilJobWait() time.Duration {
	return time.Duration(rand.Int63n(int64(nilJobWaitIntervalMax)))

}

func executeJob(ctx context.Context, logger grip.Journaler, id string, job amboy.Job, q amboy.Queue) {
	job.Run(ctx)

	if stat := job.Status(); stat.Canceled {
		stat.InProgress = false
		job.SetStatus(stat)
	}

	cerr := q.Complete(ctx, job)

	ti := job.TimeInfo()
	r := message.Fields{
		"job":           job.ID(),
		"job_type":      job.Type().Name,
		"duration_secs": ti.Duration().Seconds(),
		"dispatch_secs": ti.Start.Sub(ti.Created).Seconds(),
		"pending_secs":  ti.End.Sub(ti.Created).Seconds(),
		"queue_type":    fmt.Sprintf("%T", q),
		"stat":          job.Status(),
		"pool":          id,
		"max_time_secs": ti.MaxTime.Seconds(),
	}
	err := job.Error()
	if err != nil {
		r["error"] = err.Error()
	}
	if cerr != nil {
		r["complete_error"] = cerr.Error()
	}

	if err != nil || cerr != nil {
		logger.Error(r)
	} else {
		logger.Debug(r)
	}
}

func worker(bctx context.Context, logger grip.Journaler, id string, q amboy.Queue, wg *sync.WaitGroup) {
	var (
		err    error
		job    amboy.Job
		cancel context.CancelFunc
		ctx    context.Context
	)

	wg.Add(1)
	defer wg.Done()
	defer func() {
		// if we hit a panic we want to add an error to the job;
		err = recovery.SendMessageWithPanicError(recover(), nil, logger, "worker process encountered error")
		if err != nil {
			if job != nil {
				job.AddError(err)
				// we ignore this next error because
				// it's during panic recovery and it
				// doesn't make sense to add
				// queue-level errors to a local copy
				// of a job that we're about to throw
				// out.
				//
				// There's some debate, if
				// we should save or complete this
				// job. I think it's more correct to
				// save them, but realistically, if a
				// job panics once, we shouldn't try
				// and re-execute, which Save would
				// permit and complete wouldn't.
				_ = q.Complete(bctx, job)
			}
			// start a replacement worker.
			if bctx.Err() == nil {
				go worker(bctx, logger, id, q, wg)
			}
		}

		if cancel != nil {
			cancel()
		}
	}()

	timer := time.NewTimer(baseJobInterval)
	defer timer.Stop()
	for {
		select {
		case <-bctx.Done():
			return
		case <-timer.C:
			job, err := q.Next(bctx)
			if err != nil {
				timer.Reset(jitterNilJobWait())
				continue
			}

			ctx, cancel = context.WithCancel(bctx)
			executeJob(ctx, logger, id, job, q)
			cancel()
			timer.Reset(baseJobInterval)
		}
	}
}

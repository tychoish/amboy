package amboy

import (
	"context"
	"time"

	"github.com/tychoish/fun/erc"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/message"
)

// ResolveErrors takes a queue object and iterates over the results
// and returns a single aggregated error for the queue's job. The
// completeness of this operation depends on the implementation of a
// the queue implementation's Results() method.
func ResolveErrors(ctx context.Context, q Queue) error {
	catcher := &erc.Collector{}
	ExtractErrors(ctx, catcher, q)
	return catcher.Resolve()
}

// ExtractErrors adds any errors in the completed jobs of the queue to
// the specified catcher.
func ExtractErrors(ctx context.Context, catcher *erc.Collector, q Queue) {
	for result := range q.Jobs(ctx) {
		if !result.Status().Completed {
			continue
		}
		if err := ctx.Err(); err != nil {
			catcher.Add(err)
			break
		}

		catcher.Add(result.Error())
	}
}

// RetrieveErrors adds any errors in the completed jobs of the queue to
// the specified error channel.
func RetrieveErrors(ctx context.Context, errs chan<- error, q Queue) {
	for result := range q.Jobs(ctx) {
		if !result.Status().Completed {
			continue
		}
		if err := ctx.Err(); err != nil {
			return
		}
		if err := result.Error(); err != nil {
			select {
			case <-ctx.Done():
				return
			case errs <- err:
				continue
			}
		}
	}
}

// PopulateQueue adds jobs from a channel to a queue and returns an
// error with the aggregated results of these operations.
func PopulateQueue(ctx context.Context, q Queue, jobs <-chan Job) error {
	catcher := &erc.Collector{}

	for j := range jobs {
		if err := ctx.Err(); err != nil {
			catcher.Add(err)
			break
		}

		catcher.Add(q.Put(ctx, j))
	}

	return catcher.Resolve()
}

// QueueReport holds the ids of all tasks in a queue by state.
type QueueReport struct {
	Completed  []string `json:"completed"`
	InProgress []string `json:"in_progress"`
	Pending    []string `json:"pending"`
}

// Report returns a QueueReport status for the state of a queue.
func Report(ctx context.Context, q Queue, limit int) QueueReport {
	var out QueueReport

	if limit == 0 {
		return out
	}

	var count int
	for j := range q.Jobs(ctx) {
		stat := j.Status()
		switch {
		case stat.Completed:
			out.Completed = append(out.Completed, stat.ID)
		case stat.InProgress:
			out.InProgress = append(out.InProgress, stat.ID)
		default:
			out.Pending = append(out.Pending, stat.ID)
		}

		count++
		if limit > 0 && count >= limit {
			break
		}

	}

	return out
}

// RunJob executes a single job directly, without a queue, with
// similar semantics as it would execute in a queue: MaxTime is
// respected, and it uses similar logging as is present in the queue,
// with errors propogated functionally.
func RunJob(ctx context.Context, job Job) error {
	var cancel context.CancelFunc
	ti := job.TimeInfo()
	ti.Start = time.Now()
	job.UpdateTimeInfo(ti)
	if ti.MaxTime > 0 {
		ctx, cancel = context.WithTimeout(ctx, ti.MaxTime)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	job.Run(ctx)
	ti.End = time.Now()
	job.UpdateTimeInfo(ti)
	msg := message.Fields{
		"job":           job.ID(),
		"job_type":      job.Type().Name,
		"duration_secs": ti.Duration().Seconds(),
	}
	err := job.Error()
	if err != nil {
		grip.Error(message.WrapError(err, msg))
	} else {
		grip.Debug(msg)
	}

	return err
}

package management

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tychoish/amboy"
	"github.com/tychoish/fun/erc"
	"github.com/tychoish/grip"
)

type queueManager struct {
	queue amboy.Queue
}

// NewQueueManager returns a queue manager that provides the supported
// Management interface by calling the output of amboy.Queue.Jobs()
// over jobs directly. Use this to manage in-memory queue
// implementations more generically.
//
// The management algorithms may impact performance of jobs, as queues may
// require some locking to their Jobs function. Additionally, the speed of
// these operations will necessarily degrade with the number of jobs. Do pass
// contexts with timeouts to in these cases.
func NewQueueManager(q amboy.Queue) Manager {
	return &queueManager{
		queue: q,
	}
}

func (m *queueManager) JobStatus(ctx context.Context, f StatusFilter) (*JobStatusReport, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	counters := map[string]int{}
	switch f {
	case InProgress:
		for job := range m.queue.Jobs(ctx) {
			if job.Status().InProgress {
				counters[job.Type().Name]++
			}
		}
	case Pending:
		for job := range m.queue.Jobs(ctx) {
			stat := job.Status()
			if !stat.Completed && !stat.InProgress {
				counters[job.Type().Name]++
			}
		}
	case Stale:
		for job := range m.queue.Jobs(ctx) {
			stat := job.Status()
			if !stat.Completed && stat.InProgress && time.Since(stat.ModificationTime) > m.queue.Info().LockTimeout {
				counters[job.Type().Name]++
			}
		}
	case Completed:
		for job := range m.queue.Jobs(ctx) {
			stat := job.Status()
			if stat.InProgress {
				counters[job.Type().Name]++
			}
		}
	case All:
		for job := range m.queue.Jobs(ctx) {
			counters[job.Type().Name]++
		}
	}

	out := JobStatusReport{}

	for jt, num := range counters {
		out.Stats = append(out.Stats, JobCounters{
			ID:    jt,
			Count: num,
		})
	}

	out.Filter = string(f)

	return &out, nil
}

func (m *queueManager) RecentTiming(ctx context.Context, window time.Duration, f RuntimeFilter) (*JobRuntimeReport, error) {
	var err error

	if err = f.Validate(); err != nil {
		return nil, err
	}

	if window <= time.Second {
		return nil, errors.New("must specify windows greater than one second")
	}

	counters := map[string][]time.Duration{}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	switch f {
	case Duration:
		for job := range m.queue.Jobs(ctx) {
			stat := job.Status()
			ti := job.TimeInfo()
			if stat.Completed && time.Since(ti.End) < window {
				jt := job.Type().Name
				counters[jt] = append(counters[jt], ti.End.Sub(ti.Start))
			}
		}
	case Latency:
		for job := range m.queue.Jobs(ctx) {
			stat := job.Status()
			ti := job.TimeInfo()
			if (stat.Completed || stat.InProgress) && time.Since(ti.Created) < window {
				jt := job.Type().Name
				counters[jt] = append(counters[jt], ti.Start.Sub(ti.Created))
			}
		}
	case Running:
		for job := range m.queue.Jobs(ctx) {
			stat := job.Status()
			if !stat.Completed && stat.InProgress {
				ti := job.TimeInfo()
				jt := job.Type().Name
				counters[jt] = append(counters[jt], time.Since(ti.Start))
			}
		}
	default:
		return nil, errors.New("invalid job runtime filter")
	}

	runtimes := []JobRuntimes{}

	for k, v := range counters {
		var total time.Duration
		for _, i := range v {
			total += i
		}

		runtimes = append(runtimes, JobRuntimes{
			ID:       k,
			Duration: total / time.Duration(len(v)),
		})
	}

	return &JobRuntimeReport{
		Filter: string(f),
		Period: window,
		Stats:  runtimes,
	}, nil
}

func (m *queueManager) JobIDsByState(ctx context.Context, jobType string, f StatusFilter) (*JobReportIDs, error) {
	var err error
	if err = f.Validate(); err != nil {
		return nil, err
	}

	// it might be the case that we shold use something with
	// set-ish properties if queues return the same job more than
	// once, and it poses a problem.
	var ids []string

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	switch f {
	case InProgress:
		for job := range m.queue.Jobs(ctx) {
			stat := job.Status()
			if job.Type().Name != jobType {
				continue
			}
			if !stat.Completed && stat.InProgress {
				ids = append(ids, job.ID())
			}
		}
	case Pending:
		for job := range m.queue.Jobs(ctx) {
			if job.Type().Name != jobType {
				continue
			}
			stat := job.Status()
			if !stat.Completed && !stat.InProgress {
				ids = append(ids, stat.ID)
			}
		}
	case Stale:
		for job := range m.queue.Jobs(ctx) {
			if jobType != job.Type().Name {
				continue
			}
			stat := job.Status()
			if !stat.Completed && stat.InProgress && time.Since(stat.ModificationTime) > m.queue.Info().LockTimeout {
				ids = append(ids, stat.ID)
			}
		}
	case Completed:
		for job := range m.queue.Jobs(ctx) {
			if job.Type().Name != jobType {
				continue
			}
			stat := job.Status()
			if stat.Completed {
				ids = append(ids, stat.ID)
			}
		}
	default:
		return nil, errors.New("invalid job status filter")
	}

	return &JobReportIDs{
		Filter: string(f),
		Type:   jobType,
		IDs:    ids,
	}, nil
}

func (m *queueManager) RecentErrors(ctx context.Context, window time.Duration, f ErrorFilter) (*JobErrorsReport, error) {
	var err error
	if err = f.Validate(); err != nil {
		return nil, err
	}
	if window <= time.Second {
		return nil, errors.New("must specify windows greater than one second")
	}

	collector := map[string]JobErrorsForType{}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	switch f {
	case UniqueErrors:
		for job := range m.queue.Jobs(ctx) {
			stat := job.Status()
			if stat.Completed && stat.ErrorCount > 0 {
				ti := job.TimeInfo()
				if time.Since(ti.End) > window {
					continue
				}
				jt := job.Type().Name

				val := collector[jt]

				val.Count++
				val.Total += stat.ErrorCount
				val.Errors = append(val.Errors, stat.Errors...)
				collector[jt] = val
			}
		}
		for k, v := range collector {
			errs := map[string]struct{}{}

			for _, e := range v.Errors {
				errs[e] = struct{}{}
			}

			v.Errors = []string{}
			for e := range errs {
				v.Errors = append(v.Errors, e)
			}

			collector[k] = v
		}
	case AllErrors:
		for job := range m.queue.Jobs(ctx) {
			stat := job.Status()
			if stat.Completed && stat.ErrorCount > 0 {
				ti := job.TimeInfo()
				if time.Since(ti.End) > window {
					continue
				}
				jt := job.Type().Name

				val := collector[jt]

				val.Count++
				val.Total += stat.ErrorCount
				val.Errors = append(val.Errors, stat.Errors...)
				collector[jt] = val
			}
		}
	case StatsOnly:
		for job := range m.queue.Jobs(ctx) {
			stat := job.Status()
			if stat.Completed && stat.ErrorCount > 0 {
				ti := job.TimeInfo()
				if time.Since(ti.End) > window {
					continue
				}
				jt := job.Type().Name

				val := collector[jt]

				val.Count++
				val.Total += stat.ErrorCount
				collector[jt] = val
			}
		}
	default:
		return nil, errors.New("operation is not supported")
	}

	var reports []JobErrorsForType

	for k, v := range collector {
		v.ID = k
		v.Average = float64(v.Total / v.Count)

		reports = append(reports, v)
	}

	return &JobErrorsReport{
		Period:         window,
		FilteredByType: false,
		Data:           reports,
	}, nil
}

func (m *queueManager) RecentJobErrors(ctx context.Context, jobType string, window time.Duration, f ErrorFilter) (*JobErrorsReport, error) {
	var err error
	if err = f.Validate(); err != nil {
		return nil, err
	}
	if window <= time.Second {
		return nil, errors.New("must specify windows greater than one second")
	}

	collector := map[string]JobErrorsForType{}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	switch f {
	case UniqueErrors:
		for job := range m.queue.Jobs(ctx) {
			stat := job.Status()
			if stat.Completed && stat.ErrorCount > 0 {
				if job.Type().Name != jobType {
					continue
				}

				ti := job.TimeInfo()
				if time.Since(ti.End) > window {
					continue
				}

				val := collector[jobType]

				val.Count++
				val.Total += stat.ErrorCount
				val.Errors = append(val.Errors, stat.Errors...)
				collector[jobType] = val
			}
		}
		for k, v := range collector {
			errs := map[string]struct{}{}

			for _, e := range v.Errors {
				errs[e] = struct{}{}
			}

			v.Errors = []string{}
			for e := range errs {
				v.Errors = append(v.Errors, e)
			}

			collector[k] = v
		}
	case AllErrors:
		for job := range m.queue.Jobs(ctx) {
			stat := job.Status()
			if stat.Completed && stat.ErrorCount > 0 {
				if job.Type().Name != jobType {
					continue
				}

				ti := job.TimeInfo()
				if time.Since(ti.End) > window {
					continue
				}

				val := collector[jobType]

				val.Count++
				val.Total += stat.ErrorCount
				val.Errors = append(val.Errors, stat.Errors...)
				collector[jobType] = val
			}
		}
	case StatsOnly:
		for job := range m.queue.Jobs(ctx) {
			stat := job.Status()
			if stat.Completed && stat.ErrorCount > 0 {
				if job.Type().Name != jobType {
					continue
				}

				ti := job.TimeInfo()
				if time.Since(ti.End) > window {
					continue
				}

				val := collector[jobType]

				val.Count++
				val.Total += stat.ErrorCount
				collector[jobType] = val
			}
		}
	default:
		return nil, errors.New("operation is not supported")
	}

	var reports []JobErrorsForType

	for k, v := range collector {
		v.ID = k
		v.Average = float64(v.Total / v.Count)

		reports = append(reports, v)
	}

	return &JobErrorsReport{
		Period:         window,
		FilteredByType: true,
		Data:           reports,
	}, nil
}

func (m *queueManager) CompleteJob(ctx context.Context, name string) error {
	j, err := m.queue.Get(ctx, name)
	if err != nil {
		return fmt.Errorf("cannot recover job with name '%s': %w", name, err)
	}

	status := j.Status()
	status.ModificationCount += 2
	status.Completed = true
	j.SetStatus(status)

	return m.queue.Complete(ctx, j)
}

func (m *queueManager) CompleteJobsByType(ctx context.Context, f StatusFilter, jobType string) error {
	if err := f.Validate(); err != nil {
		return err
	}

	if f == Completed {
		return errors.New("invalid specification of completed job type")
	}

	catcher := &erc.Collector{}
	for job := range m.queue.Jobs(ctx) {
		if job.Type().Name != jobType {
			continue
		}

		stat := job.Status()
		switch f {
		case Stale:
			if !stat.InProgress || time.Since(stat.ModificationTime) < m.queue.Info().LockTimeout {
				continue
			}
		case InProgress:
			if !stat.InProgress {
				continue
			}
		case All, Pending:
			// pass: (because there's no fallthrough
			// everything else should be in progress)
			if stat.Completed {
				continue
			}
		default:
			// futureproofing...
			continue
		}

		status := job.Status()
		status.ModificationCount += 2
		status.Completed = true
		job.SetStatus(status)
		catcher.Push(m.queue.Complete(ctx, job))
	}

	return catcher.Resolve()
}

func (m *queueManager) CompleteJobs(ctx context.Context, f StatusFilter) error {
	if err := f.Validate(); err != nil {
		return err
	}
	if f == Completed {
		return errors.New("invalid specification of completed job type")
	}

	var err error
	catcher := &erc.Collector{}
	for job := range m.queue.Jobs(ctx) {
		stat := job.Status()

		if stat.Completed {
			continue
		}

		switch f {
		case Stale:
			if stat.InProgress && time.Since(stat.ModificationTime) > m.queue.Info().LockTimeout {
				continue
			}
		case InProgress:
			if !stat.InProgress {
				continue
			}
		case Pending:
			if stat.InProgress {
				continue
			}
		case All:
			// pass
		default:
			// futureproofing...
			continue
		}

		job, err = m.queue.Get(ctx, job.ID())
		if err != nil {
			catcher.Push(fmt.Errorf("could not retrieve job %q: %w", job.ID(), err))
			continue
		}
		status := job.Status()
		status.ModificationCount += 2
		status.Completed = true
		job.SetStatus(status)
		catcher.Push(m.queue.Complete(ctx, job))
	}

	return catcher.Resolve()
}

func (m queueManager) PruneJobs(ctx context.Context, ts time.Time, limit int, f StatusFilter) (int, error) {
	if err := f.Validate(); err != nil {
		return 0, err
	}

	grip.WarningWhen(f == InProgress || f == All, "deleting in progress has undefined implications")

	if ts.After(time.Now()) {
		return 0, errors.New("cannot prune jobs from the future")
	}

	dq, ok := m.queue.(amboy.DeletableJobQueue)
	if !ok {
		return 0, errors.New("queue does not support deletion")
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	catcher := &erc.Collector{}
	count := 0

	for job := range dq.Jobs(ctx) {
		stat := job.Status()
		ti := job.TimeInfo()
		id := job.ID()

		switch f {
		case Completed:
			if stat.Completed && ti.End.Before(ts) {
				count++
				if err := dq.Delete(ctx, id); err != nil {
					catcher.Push(fmt.Errorf("problem deleting stale job %q: %w", id, err))
				}
			}
		case Stale:
			if stat.InProgress && time.Since(stat.ModificationTime) > m.queue.Info().LockTimeout && ti.Start.Before(ts) {
				count++
				if err := dq.Delete(ctx, id); err != nil {
					catcher.Push(fmt.Errorf("problem deleting stale job %q: %w", id, err))
				}
			}
		case InProgress:
			if stat.InProgress && ti.Start.Before(ts) {
				count++
				if err := dq.Delete(ctx, id); err != nil {
					catcher.Push(fmt.Errorf("problem deleting in progress job %q: %w", id, err))
				}
			}
		case Pending:
			if !stat.Completed && !stat.InProgress && ti.Created.Before(ts) {
				count++
				if err := dq.Delete(ctx, id); err != nil {
					catcher.Push(fmt.Errorf("problem deleting pending job %q: %w", id, err))
				}
			}
		case All:
			if !stat.Completed && !stat.InProgress && ti.Created.Before(ts) {
				count++
				if err := dq.Delete(ctx, id); err != nil {
					catcher.Push(fmt.Errorf("problem deleting pending job %q: %w", id, err))
				}
			}
		}
		if limit > 0 && limit >= count {
			break
		}
	}

	return count, catcher.Resolve()
}

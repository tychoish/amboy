package pgq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/pool"
	"github.com/tychoish/amboy/queue"
	"github.com/tychoish/amboy/registry"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/message"
	"github.com/tychoish/grip/recovery"
)

type sqlQueue struct {
	db         *sqlx.DB
	id         string
	started    bool
	opts       Options
	mutex      sync.RWMutex
	runner     amboy.Runner
	dispatcher queue.Dispatcher
	log        grip.Logger
}

// Options describe the
type Options struct {
	SchemaName      string
	Name            string
	GroupName       string
	UseGroups       bool
	Priority        bool
	CheckWaitUntil  bool
	CheckDispatchBy bool
	SkipBootstrap   bool
	PoolSize        int

	// When true, ordered forces the queue to respect dependencies
	// of jobs. This can slow dispatching considerably,
	// particularly if jobs define cyclic or incomplete dependency chains.
	Ordered bool

	// Number of times the Complete operation should
	// retry. Previously defaulted to 10, and settings of 1 or 2
	// are reasonable.
	CompleteRetries int

	// LockTimeout overrides the default job lock timeout if set.
	WaitInterval time.Duration
	LockTimeout  time.Duration
	Logger       grip.Logger
}

// Validate ensures that all options are reasonable, and will override
// and set default options where possible.
func (opts *Options) Validate() error {
	if opts.LockTimeout < 0 {
		return errors.New("cannot have negative lock timeout")
	}

	if opts.CompleteRetries < 0 {
		return errors.New("cannot have negative complete retries")
	}

	if opts.LockTimeout == 0 {
		opts.LockTimeout = amboy.LockTimeout
	}

	if opts.PoolSize == 0 {
		opts.PoolSize = runtime.NumCPU()
	}

	if opts.WaitInterval == 0 {
		opts.WaitInterval = 100 * time.Millisecond
	}

	if opts.SchemaName == "" {
		opts.SchemaName = "amboy"
	}

	if opts.Logger.Sender() == nil {
		opts.Logger = grip.NewLogger(grip.Sender())
	}

	return nil
}

// PrepareDatabase creates the schema, tables, and indexes in the
// given schema to support operational separation of database/queue
// configuration and construction queue objects.
func PrepareDatabase(ctx context.Context, db *sqlx.DB, schemaName string) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", schemaName)); err != nil {
		return fmt.Errorf("creating schema '%s': %w", schemaName, err)
	}

	if _, err := db.ExecContext(ctx, strings.ReplaceAll(bootstrapDB, "{schemaName}", schemaName)); err != nil {
		return fmt.Errorf("bootstrapping tables and indexes schema '%s': %w", schemaName, err)
	}

	return nil
}

// CleanDatabase removes all database tables related to the "jobs"
// table in the specified schema.
func CleanDatabase(ctx context.Context, db *sqlx.DB, schemaName string) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.jobs CASCADE;", schemaName)); err != nil {
		return fmt.Errorf("dropping job tables in '%s': %w", schemaName, err)
	}

	return nil
}

// NewQueue produces a new SQL-database backed queue. Broadly similar
// to the MongoDB implementation, this queue is available only in
// "unordered" variant (e.g. dependencies are not considered in
// dispatching order,) but can respect settings including: scopes,
// priority, WaitUntil, DispatchBy.
//
// All job implementations *must* be JSON serializable, and the queue
// implementation assumes that the dependency Manager (and its edges)
// are immutable after the job is Put into the queue. Similarly, jobs
// must treat the Error array in the amboy.JobStatuseInfo as
// append-only.
//
// Be aware, that in the current release this implementation will log
// warnings if some job metadata is above maxint32, (e.g. priority,
// error count), and error if more critical values are above this
// threshold (e.g. mod Count and version). Also because MaxTime is
// stored internally as an int32 of milliseconds (maximum ~500 hours),
// rather than go's native 64bit integer of nanoseconds, attempting to
// set longer maxtimes results in an error.
func NewQueue(ctx context.Context, db *sqlx.DB, opts Options) (amboy.Queue, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	q := &sqlQueue{
		opts: opts,
		db:   db,
		id:   fmt.Sprintf("%s.%s", opts.Name, uuid.New().String()),
		log:  opts.Logger,
	}

	if err := q.SetRunner(pool.NewLocalWorkers(&pool.WorkerOptions{
		Logger:     q.log,
		NumWorkers: opts.PoolSize,
		Queue:      q,
	})); err != nil {
		return nil, err
	}

	if !opts.SkipBootstrap {
		if err := PrepareDatabase(context.TODO(), db, opts.SchemaName); err != nil {
			return nil, err
		}
	}

	q.dispatcher = queue.NewDispatcher(q, q.log)

	if opts.Ordered {
		return orderedQueue{sqlQueue: q}, nil
	}

	return q, nil
}

func (q *sqlQueue) ID() string {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return q.id
}

func (q *sqlQueue) Start(ctx context.Context) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.runner == nil {
		return errors.New("cannot start queue with an uninitialized runner")
	}

	if err := q.runner.Start(ctx); err != nil {
		return fmt.Errorf("problem starting runner in remote queue: %w", err)
	}

	q.started = true

	return nil
}
func isPgDuplicateError(err error) bool {
	if err == nil {
		return false
	}

	pgerr := &pq.Error{}
	if errors.As(err, pgerr) && pgerr.Code == "23505" {
		return true
	}

	return false
}

func (q *sqlQueue) processQueryString(query string) string {
	return strings.ReplaceAll(query, "{schemaName}", q.opts.SchemaName)
}

func (q *sqlQueue) prepareForDB(j *registry.JobInterchange) error {
	truncMaxTime := time.Duration(j.TimeInfo.MaxTime.Milliseconds())
	if truncMaxTime > time.Duration(math.MaxInt32) {
		return fmt.Errorf("maxTime must be less than %s, [%s]", time.Duration(math.MaxInt32), j.TimeInfo.MaxTime)
	}
	j.TimeInfo.MaxTime = truncMaxTime

	if j.Status.ModificationCount > math.MaxInt32 {
		return fmt.Errorf("modification count exceeded maxint32 [%d]", j.Status.ModificationCount)
	}
	if j.Version > math.MaxInt32 {
		return fmt.Errorf("version exceeded maxint32 [%d]", j.Version)
	}

	if j.Priority > math.MaxInt32 {
		j.Priority = math.MaxInt32
		q.log.Warning(message.Fields{
			"job_id":   j.Name,
			"priority": j.Priority,
			"mesage":   "priority is over maxint32",
			"queue_id": q.ID(),
		})
	}

	if j.Status.ErrorCount > math.MaxInt32 {
		j.Status.ErrorCount = math.MaxInt32
		q.log.Warning(message.Fields{
			"job_id":      j.Name,
			"error_count": j.Status.ErrorCount,
			"num_errors":  len(j.Status.Errors),
			"mesage":      "priority is over maxint32",
			"queue_id":    q.ID(),
		})
	}

	return nil
}

func (q *sqlQueue) prepareFromDB(j *registry.JobInterchange) {
	j.TimeInfo.MaxTime = j.TimeInfo.MaxTime * time.Millisecond
}

func (q *sqlQueue) Put(ctx context.Context, j amboy.Job) error {
	ti := j.TimeInfo()
	if ti.Created.IsZero() {
		ti.Created = time.Now()
		j.UpdateTimeInfo(ti)
	}

	if err := j.TimeInfo().Validate(); err != nil {
		return fmt.Errorf("invalid job timeinfo: %w", err)
	}

	payload, err := registry.MakeJobInterchange(j, json.Marshal)
	if err != nil {
		return fmt.Errorf("problem converting job to interchange format: %w", err)
	}
	if err = q.prepareForDB(payload); err != nil {
		return fmt.Errorf("problem preparing job for database: %w", err)
	}
	q.processJobForGroup(payload)

	tx, err := q.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("problem starting transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	_, err = tx.NamedExecContext(ctx, q.processQueryString(insertJob),
		struct {
			*registry.JobInterchange                       // nolint: govet
			Errors                          pq.StringArray `db:"errors"` // nolint: govet
			amboy.JobStatusInfo                            // nolint: govet
			amboy.JobTimeInfo                              // nolint: govet
			*registry.DependencyInterchange                // nolint: govet
			DependencyEdges                 pq.StringArray `db:"dep_edges"` // nolint: govet
		}{
			JobInterchange:        payload,
			Errors:                pq.StringArray(append([]string{}, payload.Status.Errors...)),
			JobStatusInfo:         payload.Status,
			JobTimeInfo:           payload.TimeInfo,
			DependencyInterchange: payload.Dependency,
			DependencyEdges:       pq.StringArray(append([]string{}, payload.Dependency.Edges...)),
		})
	if err != nil {
		if isPgDuplicateError(err) {
			return amboy.NewDuplicateJobErrorf("job '%s' already exists", j.ID())
		}

		return fmt.Errorf("insert job: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit txn: %w", err)
	}

	return nil
}

func (q *sqlQueue) Get(ctx context.Context, id string) (amboy.Job, error) {
	tx, err := q.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("problem starting transaction to resolve job %s from %s: %w", id, q.id, err)
	}
	defer tx.Rollback() //nolint:errcheck

	return q.getJobTx(ctx, tx, id)
}

func (q *sqlQueue) getJobTx(ctx context.Context, tx *sqlx.Tx, id string) (amboy.Job, error) {
	// this triggers govet because all of these structs have
	// duplicate job id and type fields, which is expected (and
	// needed,) to support the join.
	payload := struct {
		registry.JobInterchange                       // nolint: govet
		amboy.JobStatusInfo                           // nolint: govet
		Errors                         pq.StringArray `db:"errors"` // nolint: govet
		amboy.JobTimeInfo                             // nolint: govet
		registry.DependencyInterchange                // nolint: govet
		DependencyEdges                pq.StringArray `db:"dep_edges"` // nolint: govet
	}{}

	id = q.getIDFromName(id)
	if err := tx.GetContext(ctx, &payload, q.processQueryString(getJobByID), id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, amboy.NewJobNotDefinedError(q, id)
		}
		return nil, fmt.Errorf("problem getting job %q: %w", id, err)
	}

	payload.JobInterchange.Name = id
	payload.JobInterchange.Status = payload.JobStatusInfo
	payload.JobInterchange.Dependency = &payload.DependencyInterchange
	payload.JobInterchange.TimeInfo = payload.JobTimeInfo
	payload.JobInterchange.Status.Errors = payload.Errors
	payload.DependencyInterchange.Edges = payload.DependencyEdges

	q.processNameForUsers(&payload.JobInterchange)
	q.prepareFromDB(&payload.JobInterchange)

	job, err := payload.JobInterchange.Resolve(json.Unmarshal)
	if err != nil {
		if registry.IsVersionResolutionError(err) {
			q.log.Warning(message.WrapError(q.tryDelete(ctx, id), message.Fields{
				"queue_id":  q.id,
				"job_id":    id,
				"operation": "delete stale version",
				"info":      err,
			}))
		}

		return nil, fmt.Errorf("failed to resolve job: %w", err)
	}

	return job, nil
}

func (q *sqlQueue) processNameForUsers(j *registry.JobInterchange) {
	if q.opts.UseGroups {
		j.Name = j.Name[len(q.opts.GroupName)+1:]
	}

	j.Status.ID = j.Name
	j.Dependency.ID = j.Name
	j.TimeInfo.ID = j.Name
}

func (q *sqlQueue) processJobForGroup(j *registry.JobInterchange) {
	if q.opts.UseGroups {
		j.Name = fmt.Sprintf("%s.%s", q.opts.GroupName, j.Name)
		j.Group = q.opts.GroupName
	}

	j.Status.ID = j.Name
	j.Dependency.ID = j.Name
	j.TimeInfo.ID = j.Name
}

func (q *sqlQueue) getIDFromName(name string) string {
	if q.opts.UseGroups {
		return fmt.Sprintf("%s.%s", q.opts.GroupName, name)
	}

	return name
}

func (q *sqlQueue) Save(ctx context.Context, j amboy.Job) error {
	stat := j.Status()
	stat.ErrorCount = len(stat.Errors)
	stat.ModificationTime = time.Now()

	if stat.Completed && stat.InProgress {
		// this is a weird state, likely cauesd by a job that
		// aborts (the MarkComplete defer runs,) and the
		// thread that's pinging the lock saves it before
		// Complete() runs. It's harmless, but looks weird in
		// the database, so we complete it and log.
		q.log.Warning(message.Fields{
			"message":    "encountered inconsistency in job state, correcting",
			"cause":      []string{"completed and running", "aborted job", "locking interference"},
			"queue":      q.ID(),
			"job_id":     j.ID(),
			"dur_secs":   j.TimeInfo().Duration().Seconds(),
			"has_errors": j.Error() != nil,
		})
		stat.InProgress = false
	}

	j.SetStatus(stat)

	job, err := registry.MakeJobInterchange(j, json.Marshal)
	if err != nil {
		return fmt.Errorf("problem converting job to interchange format: %w", err)
	}

	job.Scopes = j.Scopes()

	return q.doUpdate(ctx, job)
}

func (q *sqlQueue) Complete(ctx context.Context, j amboy.Job) error {
	if err := q.dispatcher.Complete(ctx, j); err != nil {
		return err
	}

	stat := j.Status()
	stat.ErrorCount = len(stat.Errors)
	stat.ModificationTime = time.Now()
	stat.ModificationCount++
	stat.InProgress = false

	j.SetStatus(stat)
	j.UpdateTimeInfo(amboy.JobTimeInfo{
		End: time.Now(),
	})

	job, err := registry.MakeJobInterchange(j, json.Marshal)
	if err != nil {
		return err
	}
	job.Scopes = nil

	const retryInterval = time.Second
	timer := time.NewTimer(0)
	defer timer.Stop()

	startAt := time.Now()
	id := j.ID()
	count := 0

RETRY:
	for {
		count++
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			if err := q.doUpdate(ctx, job); err != nil {
				if time.Since(startAt) > time.Minute+q.opts.LockTimeout {
					q.log.Warning(message.WrapError(err, message.Fields{
						"job_id":      id,
						"service":     "amboy.queue.pgq",
						"job_type":    j.Type().Name,
						"retry_count": count,
						"queue_id":    q.id,
						"message":     "job took too long to mark complete",
					}))
					return fmt.Errorf("encountered timeout marking %q complete : %w", id, err)
				} else if count > q.opts.CompleteRetries {
					q.log.Warning(message.WrapError(err, message.Fields{
						"job_id":      id,
						"service":     "amboy.queue.pgq",
						"job_type":    j.Type().Name,
						"retry_count": count,
						"queue_id":    q.id,
						"message":     "after 10 retries, aborting marking job complete",
					}))
					return fmt.Errorf("failed to mark %q complete 10 times: %w", id, err)
				} else if isPgDuplicateError(err) {
					q.log.Warning(message.WrapError(err, message.Fields{
						"job_id":      id,
						"service":     "amboy.queue.pgq",
						"job_type":    j.Type().Name,
						"retry_count": count,
						"queue_id":    q.id,
						"message":     "attempting to mark job complete without lock",
					}))
					return fmt.Errorf("problem marking job %q complete without the lock", id)
				} else if errors.Is(err, errLockNotHeld) {
					q.log.Warning(message.WrapError(err, message.Fields{
						"job_id":      id,
						"service":     "amboy.queue.pgq",
						"job_type":    j.Type().Name,
						"retry_count": count,
						"queue_id":    q.id,
						"message":     "cannot complete a job without lock",
					}))
					return fmt.Errorf("problem marking job %q complete without the lock", id)
				} else {
					timer.Reset(retryInterval)
					continue RETRY
				}
			}
			return nil
		}
	}
}

var errLockNotHeld = errors.New("lock not held")

func (q *sqlQueue) doUpdate(ctx context.Context, job *registry.JobInterchange) error {
	q.processJobForGroup(job)
	defer func() { q.processNameForUsers(job) }()
	if err := q.prepareForDB(job); err != nil {
		return fmt.Errorf("fatal error with job: %w", err)
	}

	tx, err := q.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("problem starting transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	var count int
	job.Status.ID = job.Name
	job.Status.Owner = q.id

	err = tx.GetContext(ctx, &count, q.processQueryString(checkCanUpdate),
		job.Name,
		job.Status.Owner,
		job.Status.ModificationCount,
		time.Now().Add(-q.opts.LockTimeout),
	)
	if err != nil {
		return fmt.Errorf("problem reading count for lock query for %s: %w", job.Name, err)
	}

	if count == 0 {
		return fmt.Errorf("job='%s' num=%d: %w", job.Name, count, errLockNotHeld)
	}

	if _, err = tx.ExecContext(ctx, q.processQueryString(removeJobScopes), job.Name); err != nil {
		return fmt.Errorf("problem clearing scopes: %w", err)
	}

	for _, s := range job.Scopes {
		_, err = tx.NamedExecContext(ctx, fmt.Sprintln(
			fmt.Sprintf("INSERT INTO %s.job_scopes (id, scope)", q.opts.SchemaName),
			"VALUES (:id, :scope)"),
			struct {
				ID    string `db:"id"`
				Scope string `db:"scope"`
			}{ID: job.Name, Scope: s})
		if err != nil {
			return fmt.Errorf("problem inserting scope %s: %w", s, err)
		}
	}

	if _, err = tx.NamedExecContext(ctx, q.processQueryString(updateJob), struct {
		*registry.JobInterchange                       // nolint: govet
		Errors                          pq.StringArray `db:"errors"` // nolint: govet
		amboy.JobStatusInfo                            // nolint: govet
		amboy.JobTimeInfo                              // nolint: govet
		*registry.DependencyInterchange                // nolint: govet
		DependencyEdges                 pq.StringArray `db:"dep_edges"` // nolint: govet
	}{
		JobInterchange:        job,
		Errors:                pq.StringArray(append([]string{}, job.Status.Errors...)),
		JobStatusInfo:         job.Status,
		JobTimeInfo:           job.TimeInfo,
		DependencyInterchange: job.Dependency,
		DependencyEdges:       pq.StringArray(append([]string{}, job.Dependency.Edges...)),
	}); err != nil {
		return fmt.Errorf("problem updating job data: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("committing job save transaction: %w", err)
	}

	return nil
}

func (q *sqlQueue) Jobs(ctx context.Context) <-chan amboy.Job {
	output := make(chan amboy.Job)
	go func() {
		defer close(output)
		defer recovery.LogStackTraceAndContinue("jobs iterator", "amboy.queue.pgq", q.ID())

		rows, err := q.db.QueryContext(ctx, q.processQueryString(getAllJobIDs))
		if err != nil {
			q.log.Error(message.WrapError(err, message.Fields{
				"queue":    q.id,
				"service":  "amboy.queue.pg",
				"is_group": q.opts.UseGroups,
				"group":    q.opts.GroupName,
				"message":  "problem finding job ids for iterator",
				"op":       "jobs iterator",
			}))
			return
		}
		defer rows.Close()

	CURSOR:
		for rows.Next() {
			var id string

			if err := rows.Scan(&id); err != nil {
				q.log.Debug(message.WrapError(err, message.Fields{
					"queue":    q.id,
					"service":  "amboy.queue.pg",
					"is_group": q.opts.UseGroups,
					"group":    q.opts.GroupName,
					"message":  "problem reading job result from row",
					"op":       "jobs iterator",
				}))

				continue CURSOR
			}

			if q.opts.UseGroups {
				id = id[len(q.opts.GroupName)+1:]
			}

			job, err := q.Get(ctx, id)
			if err != nil {
				q.log.Debug(message.WrapError(err, message.Fields{
					"queue":    q.id,
					"service":  "amboy.queue.pg",
					"is_group": q.opts.UseGroups,
					"group":    q.opts.GroupName,
					"message":  "problem resolving job",
					"op":       "jobs iterator",
				}))
				continue CURSOR
			}

			select {
			case output <- job:
				continue CURSOR
			case <-ctx.Done():
				break CURSOR
			}

		}
		q.log.Debug(message.WrapError(rows.Close(), message.Fields{
			"queue":    q.id,
			"service":  "amboy.queue.pg",
			"is_group": q.opts.UseGroups,
			"group":    q.opts.GroupName,
			"op":       "jobs iterator",
			"message":  "problem closing cursor",
		}))
	}()
	return output
}

func (q *sqlQueue) getNextQuery() string {
	var query string
	if !q.opts.CheckWaitUntil && !q.opts.CheckDispatchBy {
		query = q.processQueryString(getNextJobsBasic)
	} else {
		timing := []string{}

		if q.opts.CheckWaitUntil {
			timing = append(timing, "  AND wait_until <= :now")
		}
		if q.opts.CheckDispatchBy {
			timing = append(timing, "  AND (dispatch_by > :now OR dispatch_by = :zero_time)")
		}

		query = fmt.Sprintln(q.processQueryString(getNextJobsTimingTemplate), strings.Join(timing, ""))
	}

	if q.opts.Priority {
		query = fmt.Sprintln(query, "ORDER BY priority DESC")
	}

	return query
}

func (q *sqlQueue) Next(ctx context.Context) (amboy.Job, error) {
	return q.getNext(ctx, func(j amboy.Job) (amboy.Job, bool) { return j, true })
}

func (q *sqlQueue) getNext(ctx context.Context, check func(j amboy.Job) (amboy.Job, bool)) (amboy.Job, error) {
	var (
		misses         int64
		dispatchSkips  int64
		dispatchMisses int64
		checkSkips     int64
		job            amboy.Job
	)

	startAt := time.Now()
	defer func() {
		q.log.DebugWhen(time.Since(startAt) > time.Second,
			message.Fields{
				"duration_secs": time.Since(startAt).Seconds(),
				"service":       "amboy.queue.pgq",
				"operation":     "next job",
				"attempts":      dispatchMisses,
				"skips":         dispatchSkips,
				"check_passes":  checkSkips,
				"misses":        misses,
				"dispatched":    job != nil,
				"message":       "slow job dispatching operation",
				"id":            q.id,
				"is_group":      q.opts.UseGroups,
				"group":         q.opts.GroupName,
			})
	}()

	query := q.getNextQuery()
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			misses++
			rows, err := q.db.NamedQueryContext(ctx, query, struct {
				GroupName   string    `db:"group_name"`
				LockExpires time.Time `db:"lock_expires"`
				Now         time.Time `db:"now"`
				ZeroTime    time.Time `db:"zero_time"`
			}{
				GroupName:   q.opts.GroupName,
				Now:         time.Now(),
				LockExpires: time.Now().Add(-q.opts.LockTimeout),
			})
			if err != nil {
				q.log.Warning(message.WrapError(err, message.Fields{
					"id":            q.id,
					"service":       "amboy.queue.pgq",
					"operation":     "retrieving next job",
					"message":       "problem generating query",
					"is_group":      q.opts.UseGroups,
					"group":         q.opts.GroupName,
					"duration_secs": time.Since(startAt).Seconds(),
				}))
				return nil, fmt.Errorf("generating next job query: %w", err)
			}
			defer rows.Close()
		CURSOR:
			for rows.Next() {

				if err = ctx.Err(); err != nil {
					return nil, err
				}
				var id string
				if err = rows.Scan(&id); err != nil {
					q.log.Debug(message.WrapError(err, message.Fields{
						"id":        q.id,
						"service":   "amboy.queue.pgq",
						"operation": "retrieving next job",
						"message":   "problem reading results",
						"is_group":  q.opts.UseGroups,
						"group":     q.opts.GroupName,
					}))

					continue CURSOR
				}

				if q.opts.UseGroups {
					id = id[len(q.opts.GroupName)+1:]
				}

				job, err = q.Get(ctx, id)
				if err != nil {
					continue CURSOR
				}

				if job.TimeInfo().IsStale() {
					q.log.Notice(message.WrapError(q.tryDelete(ctx, job.ID()), message.Fields{
						"id":        q.id,
						"service":   "amboy.queue.pgq",
						"operation": "removing stale job",
						"is_group":  q.opts.UseGroups,
						"group":     q.opts.GroupName,
						"message":   "failed to remove stale job",
					}))
					job = nil
					continue CURSOR
				}

				if q.scopesInUse(ctx, job.Scopes()) {
					dispatchSkips++
					job = nil
					continue CURSOR
				}
				if !amboy.IsDispatchable(job.Status(), q.opts.LockTimeout) {
					dispatchSkips++
					job = nil
					continue CURSOR
				}

				if j, shouldDispatch := check(job); shouldDispatch {
					job = j
				} else {
					checkSkips++
					job = nil
					continue CURSOR
				}

				if err = q.dispatcher.Dispatch(ctx, job); err != nil {
					q.log.DebugWhen(amboy.IsDispatchable(job.Status(), q.opts.LockTimeout),
						message.WrapError(err, message.Fields{
							"id":            q.id,
							"service":       "amboy.queue.pgq",
							"operation":     "dispatch job",
							"job_id":        job.ID(),
							"job_type":      job.Type().Name,
							"scopes":        job.Scopes(),
							"stat":          job.Status(),
							"is_group":      q.opts.UseGroups,
							"group":         q.opts.GroupName,
							"duration_secs": time.Since(startAt).Seconds(),
						}),
					)
					job = nil
					continue CURSOR
				}
				return job, nil
			}
		}
		timer.Reset(time.Duration(misses * rand.Int63n(int64(q.opts.WaitInterval))))
	}
}

func (q *sqlQueue) scopesInUse(ctx context.Context, scopes []string) bool {
	if len(scopes) == 0 {
		return false
	}

	query, args, err := sqlx.In(fmt.Sprintf("SELECT COUNT(*) FROM %s.job_scopes WHERE scope IN (?);", q.opts.SchemaName), convertStringsToInterfaces(scopes))
	if err != nil {
		return false
	}

	var numScopesInUse int
	if err = q.db.GetContext(ctx, &numScopesInUse, q.db.Rebind(query), args...); err != nil {
		return false
	}

	if numScopesInUse > 0 {
		return true
	}

	return false
}

func convertStringsToInterfaces(in []string) []interface{} {
	out := make([]interface{}, len(in))
	for idx := range in {
		out[idx] = in[idx]
	}
	return out
}

func (q *sqlQueue) Stats(ctx context.Context) amboy.QueueStats {
	stats := amboy.QueueStats{}

	q.log.Warning(message.WrapError(
		q.db.GetContext(ctx, &stats.Total, q.processQueryString(countTotalJobs), q.opts.GroupName),
		message.Fields{
			"queue":    q.id,
			"service":  "amboy.queue.pg",
			"is_group": q.opts.UseGroups,
			"group":    q.opts.GroupName,
			"message":  "problem getting total jobs",
		}))
	q.log.Warning(message.WrapError(
		q.db.GetContext(ctx, &stats.Pending, q.processQueryString(countPendingJobs), q.opts.GroupName),
		message.Fields{
			"queue":    q.id,
			"service":  "amboy.queue.pg",
			"is_group": q.opts.UseGroups,
			"group":    q.opts.GroupName,
			"message":  "problem getting pending jobs",
		}))
	q.log.Warning(message.WrapError(
		q.db.GetContext(ctx, &stats.Running, q.processQueryString(countInProgJobs), q.opts.GroupName),
		message.Fields{
			"queue":    q.id,
			"service":  "amboy.queue.pg",
			"is_group": q.opts.UseGroups,
			"group":    q.opts.GroupName,
			"message":  "problem getting running jobs",
		}))

	stats.Completed = stats.Total - stats.Pending
	return stats
}

func (q *sqlQueue) Info() amboy.QueueInfo {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return amboy.QueueInfo{
		Started:     q.started,
		LockTimeout: q.opts.LockTimeout,
	}
}

func (q *sqlQueue) Runner() amboy.Runner {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return q.runner
}

func (q *sqlQueue) SetRunner(r amboy.Runner) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.runner != nil && q.runner.Started() {
		return errors.New("cannot change runners after starting")
	}

	q.runner = r
	return nil
}

func (q *sqlQueue) Delete(ctx context.Context, id string) error {
	num, err := q.deleteMaybe(ctx, id)
	if err != nil {
		return err
	}

	if num == 0 {
		return fmt.Errorf("did not delete job %s", id)
	}

	return nil
}

func (q *sqlQueue) deleteMaybe(ctx context.Context, id string) (int, error) {
	res, err := q.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s.jobs WHERE id = $1", q.opts.SchemaName), id)
	if err != nil {
		return 0, err
	}

	num, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(num), nil
}

func (q *sqlQueue) tryDelete(ctx context.Context, id string) error {
	_, err := q.deleteMaybe(ctx, id)
	return err
}

func (q *sqlQueue) Close(ctx context.Context) error {
	if q.runner != nil || q.runner.Started() {
		q.runner.Close(ctx)
	}

	return q.dispatcher.Close(ctx)
}

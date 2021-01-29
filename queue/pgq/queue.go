package pgq

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/cdr/amboy"
	"github.com/cdr/amboy/pool"
	"github.com/cdr/amboy/queue"
	"github.com/cdr/amboy/registry"
	"github.com/cdr/grip"
	"github.com/cdr/grip/message"
	"github.com/cdr/grip/recovery"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

type sqlQueue struct {
	db         *sqlx.DB
	id         string
	started    bool
	opts       Options
	mutex      sync.RWMutex
	runner     amboy.Runner
	dispatcher queue.Dispatcher
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
	// LockTimeout overrides the default job lock timeout if set.
	WaitInterval time.Duration
	LockTimeout  time.Duration
}

// Validate ensures that all options are reasonable, and will override
// and set default options where possible.
func (opts *Options) Validate() error {
	if opts.LockTimeout < 0 {
		return errors.New("cannot have negative lock timeout")
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

	return nil
}

// PrepareDatabase creates the schema, tables, and indexes in the
// given schema to support operational separation of database/queue
// configuration and construction queue objects.
func PrepareDatabase(ctx context.Context, db *sqlx.DB, schemaName string) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", schemaName)); err != nil {
		return errors.Wrapf(err, "creating schema '%s'", schemaName)
	}

	if _, err := db.ExecContext(ctx, strings.ReplaceAll(bootstrapDB, "{schemaName}", schemaName)); err != nil {
		return errors.Wrapf(err, "bootstrapping tables and indexes schema '%s'", schemaName)
	}

	return nil
}

// CleanDatabase removes all database tables related to the "jobs"
// table in the specified schema.
func CleanDatabase(ctx context.Context, db *sqlx.DB, schemaName string) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.jobs CASCADE;", schemaName)); err != nil {
		return errors.Wrapf(err, "dropping job tables in '%s'", schemaName)
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
		return nil, errors.WithStack(err)
	}

	q := &sqlQueue{
		opts: opts,
		db:   db,
		id:   fmt.Sprintf("%s.%s", opts.Name, uuid.New().String()),
	}

	if err := q.SetRunner(pool.NewLocalWorkers(opts.PoolSize, q)); err != nil {
		return nil, errors.WithStack(err)
	}

	if !opts.SkipBootstrap {
		if err := PrepareDatabase(context.TODO(), db, opts.SchemaName); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	q.dispatcher = queue.NewDispatcher(q)

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
		return errors.Wrap(err, "problem starting runner in remote queue")
	}
	q.started = true

	return nil
}
func isPgDuplicateError(err error) bool {
	if err == nil {
		return false
	}

	if pgerr, ok := err.(*pq.Error); ok && pgerr.Code == "23505" {
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
		return errors.Errorf("maxTime must be less than %s, [%s]", time.Duration(math.MaxInt32), j.TimeInfo.MaxTime)
	}
	j.TimeInfo.MaxTime = truncMaxTime

	if j.Status.ModificationCount > math.MaxInt32 {
		return errors.Errorf("modification count exceeded maxint32 [%d]", j.Status.ModificationCount)
	}
	if j.Version > math.MaxInt32 {
		return errors.Errorf("version exceeded maxint32 [%d]", j.Version)
	}

	if j.Priority > math.MaxInt32 {
		j.Priority = math.MaxInt32
		grip.Warning(message.Fields{
			"job_id":   j.Name,
			"priority": j.Priority,
			"mesage":   "priority is over maxint32",
			"queue_id": q.ID(),
		})
	}

	if j.Status.ErrorCount > math.MaxInt32 {
		j.Status.ErrorCount = math.MaxInt32
		grip.Warning(message.Fields{
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
		return errors.Wrap(err, "invalid job timeinfo")
	}

	payload, err := registry.MakeJobInterchange(j, json.Marshal)
	if err != nil {
		return errors.Wrap(err, "problem converting job to interchange format")
	}
	if err = q.prepareForDB(payload); err != nil {
		return errors.Wrap(err, "problem preparing job for database")
	}
	q.processJobForGroup(payload)

	tx, err := q.db.BeginTxx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "problem starting transaction")
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

		return errors.Wrap(err, "insert job")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "commit txn")
	}

	return nil
}

func (q *sqlQueue) Get(ctx context.Context, id string) (amboy.Job, error) {
	tx, err := q.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, errors.Wrapf(err, "problem starting transaction to resolve job %s from %s", id, q.id)
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
		if errors.Cause(err) == sql.ErrNoRows {
			return nil, amboy.NewJobNotDefinedError(q, id)
		}
		return nil, errors.Wrapf(err, "problem getting job %q", id)
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
			grip.Warning(message.WrapError(q.tryDelete(ctx, id), message.Fields{
				"queue_id":  q.id,
				"job_id":    id,
				"operation": "delete stale version",
				"info":      err,
			}))
		}

		return nil, errors.Wrap(err, "failed to resolve job")
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
	j.SetStatus(stat)

	job, err := registry.MakeJobInterchange(j, json.Marshal)
	if err != nil {
		return errors.Wrap(err, "problem converting job to interchange format")
	}

	job.Scopes = j.Scopes()

	return errors.WithStack(q.doUpdate(ctx, job))
}

func (q *sqlQueue) Complete(ctx context.Context, j amboy.Job) {
	q.dispatcher.Complete(ctx, j)

	stat := j.Status()
	stat.ErrorCount = len(stat.Errors)
	stat.ModificationTime = time.Now()
	stat.ModificationCount++
	stat.Completed = true
	stat.InProgress = false
	j.SetStatus(stat)
	j.UpdateTimeInfo(amboy.JobTimeInfo{
		End: time.Now(),
	})

	job, err := registry.MakeJobInterchange(j, json.Marshal)
	if err != nil {
		return
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
			return
		case <-timer.C:
			if err := q.doUpdate(ctx, job); err != nil {
				if time.Since(startAt) > time.Minute+q.opts.LockTimeout {
					grip.Warning(message.WrapError(err, message.Fields{
						"job_id":      id,
						"service":     "amboy.queue.pgq",
						"job_type":    j.Type().Name,
						"retry_count": count,
						"queue_id":    q.id,
						"message":     "job took too long to mark complete",
					}))
				} else if count > 10 {
					grip.Warning(message.WrapError(err, message.Fields{
						"job_id":      id,
						"service":     "amboy.queue.pgq",
						"job_type":    j.Type().Name,
						"retry_count": count,
						"queue_id":    q.id,
						"message":     "after 10 retries, aborting marking job complete",
					}))
				} else if isPgDuplicateError(err) {
					grip.Warning(message.WrapError(err, message.Fields{
						"job_id":      id,
						"service":     "amboy.queue.pgq",
						"job_type":    j.Type().Name,
						"retry_count": count,
						"queue_id":    q.id,
						"message":     "attempting to mark job complete without lock",
					}))
				} else {
					timer.Reset(retryInterval)
					continue RETRY
				}
				j.AddError(err)
				return
			}
			return

		}
	}
}

func (q *sqlQueue) doUpdate(ctx context.Context, job *registry.JobInterchange) error {
	q.processJobForGroup(job)
	defer func() { q.processNameForUsers(job) }()
	if err := q.prepareForDB(job); err != nil {
		return errors.Wrap(err, "fatal error with job")
	}

	tx, err := q.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return errors.Wrap(err, "problem starting transaction")
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
		return errors.Wrapf(err, "problem reading count for lock query for %s", job.Name)
	}

	if count == 0 {
		return errors.Errorf("do not have lock for job='%s' num=%d", job.Name, count)
	}

	if _, err = tx.ExecContext(ctx, q.processQueryString(removeJobScopes), job.Name); err != nil {
		return errors.Wrap(err, "problem clearing scopes")
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
			return errors.Wrapf(err, "problem inserting scope %s", s)
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
		return errors.Wrap(err, "problem updating job data")
	}

	if err = tx.Commit(); err != nil {
		return errors.Wrap(err, "committing job save transaction")
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
			grip.Error(message.WrapError(err, message.Fields{
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

		for rows.Next() {
			var id string

			if err := rows.Scan(&id); err != nil {
				grip.Debug(message.WrapError(err, message.Fields{
					"queue":    q.id,
					"service":  "amboy.queue.pg",
					"is_group": q.opts.UseGroups,
					"group":    q.opts.GroupName,
					"message":  "problem reading job result from row",
					"op":       "jobs iterator",
				}))

				continue
			}

			if q.opts.UseGroups {
				id = id[len(q.opts.GroupName)+1:]
			}

			job, err := q.Get(ctx, id)
			if err != nil {
				grip.Debug(message.WrapError(err, message.Fields{
					"queue":    q.id,
					"service":  "amboy.queue.pg",
					"is_group": q.opts.UseGroups,
					"group":    q.opts.GroupName,
					"message":  "problem resolving job",
					"op":       "jobs iterator",
				}))
				continue
			}

			output <- job
		}
		grip.Debug(message.WrapError(rows.Close(), message.Fields{
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
	var (
		misses         int64
		dispatchSkips  int64
		dispatchMisses int64
		job            amboy.Job
	)

	startAt := time.Now()
	defer func() {
		grip.DebugWhen(time.Since(startAt) > time.Second,
			message.Fields{
				"duration_secs": time.Since(startAt).Seconds(),
				"service":       "amboy.queue.pgq",
				"operation":     "next job",
				"attempts":      dispatchMisses,
				"skips":         dispatchSkips,
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
			return nil, errors.WithStack(ctx.Err())
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
				grip.Warning(message.WrapError(err, message.Fields{
					"id":            q.id,
					"service":       "amboy.queue.pgq",
					"operation":     "retrieving next job",
					"message":       "problem generating query",
					"is_group":      q.opts.UseGroups,
					"group":         q.opts.GroupName,
					"duration_secs": time.Since(startAt).Seconds(),
				}))
				return nil, errors.Wrap(err, "generating next job query")
			}
			defer rows.Close()
		CURSOR:
			for rows.Next() {

				if err = ctx.Err(); err != nil {
					return nil, errors.WithStack(err)
				}
				var id string
				if err = rows.Scan(&id); err != nil {
					grip.Debug(message.WrapError(err, message.Fields{
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
					grip.Notice(message.WrapError(q.tryDelete(ctx, job.ID()), message.Fields{
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

				if err = q.dispatcher.Dispatch(ctx, job); err != nil {
					grip.DebugWhen(amboy.IsDispatchable(job.Status(), q.opts.LockTimeout),
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

	grip.Warning(message.WrapError(
		q.db.GetContext(ctx, &stats.Total, q.processQueryString(countTotalJobs), q.opts.GroupName),
		message.Fields{
			"queue":    q.id,
			"service":  "amboy.queue.pg",
			"is_group": q.opts.UseGroups,
			"group":    q.opts.GroupName,
			"message":  "problem getting total jobs",
		}))
	grip.Warning(message.WrapError(
		q.db.GetContext(ctx, &stats.Pending, q.processQueryString(countPendingJobs), q.opts.GroupName),
		message.Fields{
			"queue":    q.id,
			"service":  "amboy.queue.pg",
			"is_group": q.opts.UseGroups,
			"group":    q.opts.GroupName,
			"message":  "problem getting pending jobs",
		}))
	grip.Warning(message.WrapError(
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
		return errors.WithStack(err)
	}

	if num == 0 {
		return errors.Errorf("did not delete job %s", id)
	}

	return nil
}

func (q *sqlQueue) deleteMaybe(ctx context.Context, id string) (int, error) {
	res, err := q.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s.jobs WHERE id = $1", q.opts.SchemaName), id)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	num, err := res.RowsAffected()
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return int(num), nil
}

func (q *sqlQueue) tryDelete(ctx context.Context, id string) error {
	_, err := q.deleteMaybe(ctx, id)
	return errors.WithStack(err)
}

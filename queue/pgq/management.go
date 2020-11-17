package pgq

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/deciduosity/amboy/management"
	"github.com/deciduosity/grip"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type sqlManager struct {
	opts ManagerOptions
	db   *sqlx.DB
}

// ManagerOptions control the behavior of the Manager implementation,
// particularly with regards to group handling. Also contains a nested
// queue options for additional relevant settings.
type ManagerOptions struct {
	SingleGroup bool
	ByGroups    bool
	Options     Options
}

func (o *ManagerOptions) Validate() error {
	catcher := grip.NewBasicCatcher()
	catcher.NewWhen(o.SingleGroup && o.ByGroups, "cannot specify conflicting group options")
	catcher.Wrap(o.Options.Validate(), "invalid database options")
	return catcher.Resolve()
}

// NewManager constructs a manager instance that interacts with the
// job data.
func NewManager(db *sqlx.DB, opts ManagerOptions) management.Manager {

	return &sqlManager{db: db, opts: opts}
}

func (m *sqlManager) processQueryString(query string) string {
	return strings.ReplaceAll(query, "{schemaName}", m.opts.Options.SchemaName)
}

func (m *sqlManager) JobStatus(ctx context.Context, filter management.StatusFilter) (*management.JobStatusReport, error) {
	if err := filter.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	var where []string
	group := "type"
	query := m.processQueryString(groupJobStatusTemplate)
	if m.opts.SingleGroup {
		where = append(where, "queue_group = :queue_group")
		group = "type, queue_group"
		query = strings.Replace(query, "{{project_group}}", ",\n   queue_group", 1)
	} else if m.opts.ByGroups {
		query = strings.Replace(query, "{{project_group}}", ",\n   queue_group", 1)
		group = "type, queue_group"
		where = append(where, "queue_group != ''")
	} else {
		query = strings.Replace(query, "{{project_group}}", "", 1)
	}

	switch filter {
	case management.Completed:
		where = append(where,
			"completed = true",
			"in_progress = false")
	case management.InProgress:
		where = append(where,
			"completed = false",
			"in_progress = true")
	case management.Pending:
		where = append(where,
			"completed = false",
			"in_progress = false")
	case management.Stale:
		where = append(where,
			"in_progress = true",
			"completed = false",
			"mod_ts > :lock_timeout")
	case management.All:
		// pass (all jobs)
	default:
		return nil, errors.New("invalid job status filter")
	}

	if len(where) > 0 {
		query = fmt.Sprintln(query, "\nWHERE", "\n  ", strings.Join(where, "\n   AND "))
	}
	query = fmt.Sprintln(query, "GROUP BY", group)

	stmt, err := m.db.PrepareNamedContext(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "problem preparing query")
	}

	args := struct {
		LockTimeout time.Time `db:"lock_timeout"`
		Group       string    `db:"queue_group"`
	}{
		LockTimeout: time.Now().Add(-m.opts.Options.LockTimeout),
		Group:       m.opts.Options.GroupName,
	}
	out := []management.JobCounters{}
	if err := stmt.SelectContext(ctx, &out, args); err != nil {
		return nil, errors.Wrap(err, "problem finding job output")
	}

	return &management.JobStatusReport{
		Filter: string(filter),
		Stats:  out,
	}, nil
}

func (m *sqlManager) JobIDsByState(ctx context.Context, jobType string, filter management.StatusFilter) (*management.JobReportIDs, error) {
	if err := filter.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	var where []string
	if m.opts.SingleGroup {
		where = append(where, "queue_group = :queue_group")
	} else if m.opts.ByGroups {
		where = append(where, "queue_group != ''")
	}

	switch filter {
	case management.Completed:
		where = append(where,
			"completed = true",
			"in_progress = false")
	case management.InProgress:
		where = append(where,
			"completed = false",
			"in_progress = true")
	case management.Stale:
		where = append(where,
			"in_progress = true",
			"mod_ts > :lock_timeout")
	case management.Pending:
		where = append(where,
			"in_progress = false",
			"completed = false")
	default:
		return nil, errors.New("invalid job status filter")
	}

	query := fmt.Sprintln(m.processQueryString(findJobIDsByStateTemplate), "  AND ", strings.Join(where, "\n   AND "))
	stmt, err := m.db.PrepareNamedContext(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "problem preparing query")
	}

	args := struct {
		LockTimeout time.Time `db:"lock_timeout"`
		Group       string    `db:"queue_group"`
		JobType     string    `db:"job_type"`
	}{
		LockTimeout: time.Now().Add(-m.opts.Options.LockTimeout),
		Group:       m.opts.Options.GroupName,
		JobType:     jobType,
	}

	var out []string
	if err := stmt.SelectContext(ctx, &out, args); err != nil {
		return nil, errors.Wrap(err, "problem finding job output")
	}
	return &management.JobReportIDs{
		Filter: string(filter),
		Type:   jobType,
		IDs:    out,
	}, nil
}

func (m *sqlManager) RecentTiming(ctx context.Context, window time.Duration, filter management.RuntimeFilter) (*management.JobRuntimeReport, error) {
	if err := filter.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	if window <= time.Second {
		return nil, errors.New("must specify windows greater than one second")
	}

	var where []string
	var group string

	query := m.processQueryString(recentTimingTemplate)

	if m.opts.SingleGroup {
		where = append(where, "queue_group = :queue_group")
		group = "type, queue_group"
		query = strings.Replace(query, "{{project_group}}", "queue_group,", 1)
	} else if m.opts.ByGroups {
		group = "type, queue_group"
		query = strings.Replace(query, "{{project_group}}", "queue_group,", 1)
		where = append(where, "queue_group != ''")
	} else {
		group = "type"
		query = strings.Replace(query, "{{project_group}}", "", 1)
	}

	switch filter {
	case management.Duration:
		where = append(where,
			"completed = true",
			"ended > :window")
		query = strings.Replace(query, "{{from_time}}", "time_info.ended", 1)
		query = strings.Replace(query, "{{to_time}}", "time_info.started", 1)
	case management.Latency:
		where = append(where,
			"completed = false",
			"created > :window")
		query = strings.Replace(query, "{{from_time}}", "now()", 1)
		query = strings.Replace(query, "{{to_time}}", "time_info.created", 1)
	case management.Running:
		where = append(where,
			"completed = false",
			"in_progress = true")
		query = strings.Replace(query, "{{from_time}}", "now()", 1)
		query = strings.Replace(query, "{{to_time}}", "time_info.created", 1)
	default:
		return nil, errors.New("invalid job status filter")
	}

	query = fmt.Sprintln(query, " ", strings.Join(where, "\n   AND "),
		"\nGROUP BY", group)

	stmt, err := m.db.PrepareNamedContext(ctx, query)
	if err != nil {
		fmt.Println(query)
		return nil, errors.Wrap(err, "problem preparing query")
	}

	now := time.Now()
	args := struct {
		GroupName string    `db:"queue_group"`
		Window    time.Time `db:"window"`
	}{
		GroupName: m.opts.Options.GroupName,
		Window:    now.Add(-window),
	}

	out := []management.JobRuntimes{}
	if err := stmt.SelectContext(ctx, &out, args); err != nil {
		return nil, errors.Wrap(err, "problem finding job output")
	}

	return &management.JobRuntimeReport{
		Filter: string(filter),
		Period: window,
		Stats:  out,
	}, nil
}

func (m *sqlManager) RecentErrors(ctx context.Context, window time.Duration, filter management.ErrorFilter) (*management.JobErrorsReport, error) {
	return m.doRecentErrors(ctx, "", window, filter)
}

func (m *sqlManager) RecentJobErrors(ctx context.Context, jobType string, window time.Duration, filter management.ErrorFilter) (*management.JobErrorsReport, error) {
	return m.doRecentErrors(ctx, jobType, window, filter)
}

func (m *sqlManager) doRecentErrors(ctx context.Context, jobType string, window time.Duration, filter management.ErrorFilter) (*management.JobErrorsReport, error) {
	if err := filter.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	if window <= time.Second {
		return nil, errors.New("must specify windows greater than one second")
	}

	var clauses []string

	if m.opts.ByGroups {
		clauses = append(clauses, "  AND queue_group != ''")
	}

	if jobType != "" {
		clauses = append(clauses, "  AND type = :job_type")
	}

	query := m.processQueryString(recentJobErrorsTemplate)
	if m.opts.SingleGroup {
		clauses = append(clauses, "  AND queue_group = :queue_group")
		clauses = append(clauses, "GROUP BY type, queue_group")
		query = strings.Replace(query, "{{queue_group}}", "\n   jobs.queue_group,", 1)
	} else if m.opts.ByGroups {
		clauses = append(clauses, "GROUP BY type, queue_group")
		query = strings.Replace(query, "{{queue_group}}", "\n   jobs.queue_group,", 1)
	} else {
		clauses = append(clauses, "GROUP BY jobs.type")
		query = strings.Replace(query, "{{queue_group}}", "", 1)
	}

	query = fmt.Sprintln(query, strings.Join(clauses, "\n"))

	switch filter {
	case management.UniqueErrors:
		query = strings.Replace(query, "{{agg_errors}}", ",\n   array_agg(DISTINCT job_errors.error) as errors", 1)
	case management.AllErrors:
		query = strings.Replace(query, "{{agg_errors}}", ",\n   array_agg(job_errors.error) as errors", 1)
	case management.StatsOnly:
		query = strings.Replace(query, "{{agg_errors}}", "", 1)
	default:
		return nil, errors.New("operation is not supported")
	}

	args := struct {
		GroupName string    `db:"queue_group"`
		Window    time.Time `db:"window"`
		JobType   string    `db:"job_type"`
	}{
		GroupName: m.opts.Options.GroupName,
		Window:    time.Now().Add(-window),
		JobType:   jobType,
	}

	stmt, err := m.db.PrepareNamedContext(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "problem preparing query")
	}

	var out []management.JobErrorsForType
	if err := stmt.SelectContext(ctx, &out, args); err != nil {
		return nil, errors.Wrap(err, "problem finding job output")
	}

	return &management.JobErrorsReport{
		Period:         window,
		FilteredByType: jobType != "",
		Data:           out,
	}, nil
}

func (m *sqlManager) CompleteJob(ctx context.Context, id string) error {
	if m.opts.Options.GroupName != "" {
		id = fmt.Sprintf("%s.%s", m.opts.Options.GroupName, id)
	}

	tx, err := m.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return errors.Wrap(err, "problem starting transaction")
	}
	defer tx.Rollback()

	if _, err = tx.ExecContext(ctx, m.processQueryString(completeSinglePendingJob), id); err != nil {
		return errors.Wrap(err, "problem clearing scopes")
	}

	if _, err = tx.ExecContext(ctx, m.processQueryString(removeJobScopes), id); err != nil {
		return errors.Wrap(err, "problem clearing scopes")
	}

	if err = tx.Commit(); err != nil {
		return errors.Wrap(err, "problem committing")
	}

	return nil
}

func (m *sqlManager) CompleteJobs(ctx context.Context, filter management.StatusFilter) error {
	return errors.WithStack(m.doCompleteJobs(ctx, filter, ""))
}

func (m *sqlManager) CompleteJobsByType(ctx context.Context, filter management.StatusFilter, jobType string) error {
	return errors.WithStack(m.doCompleteJobs(ctx, filter, jobType))
}

func (m *sqlManager) doCompleteJobs(ctx context.Context, filter management.StatusFilter, typeName string) error {
	if err := filter.Validate(); err != nil {
		return errors.WithStack(err)
	}

	var clauses []string

	if m.opts.Options.GroupName != "" {
		clauses = append(clauses, "queue_group = :group_name")
	}

	if m.opts.ByGroups {
		clauses = append(clauses, "queue_group != ''")
	}

	if typeName != "" {
		clauses = append(clauses, "type = :type_name")
	}

	args := struct {
		GroupName   string    `db:"group_name"`
		TypeName    string    `db:"type_name"`
		LockTimeout time.Time `db:"lock_timeout"`
	}{
		TypeName:    typeName,
		GroupName:   m.opts.Options.GroupName,
		LockTimeout: time.Now().Add(-m.opts.Options.LockTimeout),
	}

	switch filter {
	case management.Completed:
		return errors.New("cannot mark completed jobs complete")
	case management.InProgress:
		clauses = append(clauses,
			"completed = false",
			"in_progress = true")
	case management.Stale:
		clauses = append(clauses,
			"in_progress = true",
			"mod_ts > :lock_timeout")
	case management.Pending:
		clauses = append(clauses,
			"completed = false")
	case management.All:
		clauses = append(clauses, "in_progress = false")
	default:
		return errors.New("invalid job status filter")
	}

	var toDelete []string
	tx, err := m.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return errors.Wrap(err, "problem starting transaction")
	}
	defer tx.Rollback()

	query := fmt.Sprintln(m.processQueryString(findJobsToCompleteTemplate), " ", strings.Join(clauses, "\n   AND "))

	stmt, err := tx.PrepareNamedContext(ctx, query)
	if err != nil {
		return errors.Wrap(err, "problem preparing query")
	}
	if err := stmt.SelectContext(ctx, &toDelete, args); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return errors.Wrap(err, "problem finding job output")
	}

	toDeleteArgs := convertStringsToInterfaces(toDelete)

	query, inargs, err := sqlx.In(m.processQueryString(completeManyPendingJobs), toDeleteArgs)
	if err != nil {
		return errors.Wrap(err, "problem building delete jobs query")
	}

	if _, err = tx.ExecContext(ctx, m.db.Rebind(query), inargs...); err != nil {
		return errors.Wrap(err, "problem clearing jobs")
	}

	query, inargs, err = sqlx.In(m.processQueryString(removeManyJobScopes), toDeleteArgs)
	if err != nil {
		return errors.Wrap(err, "problem building delete jobs scopes")
	}

	if _, err = tx.ExecContext(ctx, m.db.Rebind(query), inargs...); err != nil {
		return errors.Wrap(err, "problem clearing job s copes")
	}

	if err = tx.Commit(); err != nil {
		return errors.Wrap(err, "problem committing")
	}

	return nil
}

func (m *sqlManager) PruneJobs(ctx context.Context, ts time.Time, limit int, f management.StatusFilter) (int, error) {
	if err := f.Validate(); err != nil {
		return 0, errors.WithStack(err)
	}

	where := []string{}
	if m.opts.SingleGroup {
		where = append(where, "queue_group = :queue_group")
	}

	if m.opts.ByGroups {
		where = append(where, "queue_group != ''")
	}

	switch f {
	case management.Completed:
		where = append(where,
			"completed = true",
			"in_progress = false",
			"ended < :ts")
	case management.InProgress:
		where = append(where,
			"completed = false",
			"in_progress = true",
			"started < :ts")
	case management.Pending:
		where = append(where,
			"completed = false",
			"in_progress = false",
			"created < :ts")
	case management.Stale:
		where = append(where,
			"in_progress = true",
			"completed = false",
			"mod_ts > :lock_timeout",
			"started < :ts")
	case management.All:
		where = append(where,
			"created < :ts")
	default:
		return 0, errors.New("invalid job status filter")
	}

	query := m.processQueryString(pruneJobsQueryTemplate)

	query = strings.Replace(query, "{{match}}",
		strings.Join(where, "\n      AND "), 1)

	if limit > 0 {
		query = strings.Replace(query, "{{limit}}", fmt.Sprint("\n   LIMIT ", limit), 1)
	} else {
		query = strings.Replace(query, "{{limit}}", "", 1)
	}

	stmt, err := m.db.PrepareNamedContext(ctx, query)
	if err != nil {
		return 0, errors.Wrap(err, "problem preparing query")
	}

	args := struct {
		LockTimeout time.Time `db:"lock_timeout"`
		Timestamp   time.Time `db:"ts"`
		Group       string    `db:"queue_group"`
	}{
		LockTimeout: time.Now().Add(-m.opts.Options.LockTimeout),
		Timestamp:   ts,
		Group:       m.opts.Options.GroupName,
	}

	res, err := stmt.ExecContext(ctx, args)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	num, err := res.RowsAffected()
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return int(num), nil
}

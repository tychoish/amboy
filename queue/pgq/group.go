package pgq

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/deciduosity/amboy"
	"github.com/deciduosity/amboy/queue"
	"github.com/deciduosity/grip"
	"github.com/deciduosity/grip/level"
	"github.com/deciduosity/grip/message"
	"github.com/deciduosity/grip/recovery"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type sqlGroup struct {
	db        *sqlx.DB
	opts      GroupOptions
	queueOpts Options
	cache     queue.GroupCache
	canceler  context.CancelFunc
}

// GroupOptions controls the behavior of the amboy.QueueGroup
// implementation.
type GroupOptions struct {
	// Abortable controls if the queue will use an abortable pool
	// imlementation. The DefaultWorkers options sets the default number
	// of workers new queues will have if the WorkerPoolSize
	// function is not set.
	Abortable      bool
	DefaultWorkers int

	// WorkerPoolSize determines how many works will be allocated
	// to each queue, based on the queue ID passed to it.
	WorkerPoolSize func(string) int

	// PruneFrequency is how often Prune runs by default.
	PruneFrequency time.Duration

	// BackgroundCreateFrequency is how often the background queue
	// creation runs, in the case that queues may be created in
	// the background without
	BackgroundCreateFrequency time.Duration

	// The BackgroundOperation values control how often and at
	// what level to log at.  These default to 5 successive errors
	// and the level of "Warning" if unset.
	BackgroundOperationErrorCountThreshold int
	BackgroundOperationErrorLogLevel       level.Priority

	// TTL is how old the oldest task in the queue must be for the collection to be pruned.
	TTL time.Duration
}

func (opts *GroupOptions) validate() error {
	catcher := grip.NewBasicCatcher()
	catcher.NewWhen(opts.TTL < 0, "ttl must be greater than or equal to 0")
	catcher.NewWhen(opts.TTL > 0 && opts.TTL < time.Second, "ttl cannot be less than 1 second, unless it is 0")
	catcher.NewWhen(opts.PruneFrequency < 0, "prune frequency must be greater than or equal to 0")
	catcher.NewWhen(opts.PruneFrequency > 0 && opts.TTL < time.Second, "prune frequency cannot be less than 1 second, unless it is 0")
	catcher.NewWhen((opts.TTL == 0 && opts.PruneFrequency != 0) || (opts.TTL != 0 && opts.PruneFrequency == 0),
		"ttl and prune frequency must both be 0 or both be not 0")
	catcher.NewWhen(opts.DefaultWorkers == 0 && opts.WorkerPoolSize == nil,
		"must specify either a default worker pool size or a WorkerPoolSize function")

	if opts.BackgroundOperationErrorLogLevel == 0 {
		opts.BackgroundOperationErrorLogLevel = level.Warning
	}

	if opts.BackgroundOperationErrorCountThreshold < 0 {
		opts.BackgroundOperationErrorCountThreshold = 1
	} else if opts.BackgroundOperationErrorCountThreshold == 0 {
		opts.BackgroundOperationErrorCountThreshold = 5
	}

	return catcher.Resolve()
}

// NewGroup constructs a new SQL backed group queue and starts the
// background queue creation and pruning so that queues started on one
// process eventually end up on other processes, and that empty queues
// eventually release resources.
//
// Group queues reduce the overall cost of creating a new queues, and
// make it possible to isolate workloads from each other.
//
// The SQL backed queue is implemented such that single queues and
// group queues can coexist in the same database and underlying
// tables though there are some (minor and pathological) cases where
// this may behave unexpectedly. (e.g. non group jobs with prefixes
// could theoretically block similar group queue job, in a way that
// could never happen with multiple group queues.)
func NewGroup(ctx context.Context, db *sqlx.DB, opts Options, gopts GroupOptions) (amboy.QueueGroup, error) {
	if err := gopts.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid queue group options")
	}

	if err := opts.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid queue options")
	}

	if _, err := db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", opts.SchemaName)); err != nil {
		return nil, errors.WithStack(err)
	}

	opts.UseGroups = true

	group := &sqlGroup{
		queueOpts: opts,
		db:        db,
		opts:      gopts,
		cache:     queue.NewGroupCache(gopts.TTL),
	}

	if _, err := db.Exec(group.processQueryString(bootstrapDB)); err != nil {
		return nil, errors.WithStack(err)
	}

	ctx, group.canceler = context.WithCancel(ctx)

	if gopts.PruneFrequency > 0 && gopts.TTL > 0 {
		go func() {
			defer recovery.LogStackTraceAndContinue("panic in remote queue group ticker")
			ticker := time.NewTicker(gopts.PruneFrequency)
			defer ticker.Stop()
			errCount := 0
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					err := group.Prune(ctx)
					if err != nil {
						errCount++
					} else {
						errCount = 0
					}

					grip.LogWhen(errCount > gopts.BackgroundOperationErrorCountThreshold,
						gopts.BackgroundOperationErrorLogLevel,
						message.WrapError(err, "problem pruning remote queues"))
				}
			}
		}()
	}

	if gopts.BackgroundCreateFrequency > 0 {
		go func() {
			defer recovery.LogStackTraceAndContinue("panic in remote queue group ticker")
			ticker := time.NewTicker(gopts.BackgroundCreateFrequency)
			errCount := 0
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					err := group.startQueues(ctx)
					if err != nil {
						errCount++
					} else {
						errCount = 0
					}

					grip.LogWhen(errCount > gopts.BackgroundOperationErrorCountThreshold,
						gopts.BackgroundOperationErrorLogLevel,
						message.WrapError(err, "problem starting external queues"))
				}
			}
		}()
	}

	if err := group.startQueues(ctx); err != nil {
		return nil, errors.WithStack(err)
	}

	return group, nil
}

func (g *sqlGroup) startQueues(ctx context.Context) error {
	queues, err := g.getQueues(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	catcher := grip.NewBasicCatcher()
	for _, id := range queues {
		_, err := g.Get(ctx, id)
		catcher.Add(err)
	}

	return catcher.Resolve()
}

func (g *sqlGroup) processQueryString(query string) string {
	return strings.ReplaceAll(query, "{schemaName}", g.queueOpts.SchemaName)
}

func (g *sqlGroup) getQueues(ctx context.Context) ([]string, error) {
	var groups []string

	if err := g.db.SelectContext(ctx, &groups, g.processQueryString(getActiveGroups), time.Now().Add(-g.opts.TTL)); err != nil {
		if err == sql.ErrNoRows {
			return []string{}, nil
		}
		return nil, errors.WithStack(err)
	}

	return groups, nil
}

func (g *sqlGroup) Get(ctx context.Context, id string) (amboy.Queue, error) {
	q := g.cache.Get(id)
	if q != nil {
		return q, nil
	}
	if id == "" {
		return nil, errors.New("must specify a group id")
	}

	opts := g.queueOpts
	var err error
	opts.GroupName = id

	q, err = NewQueue(g.db, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "problem creating new queue for '%s'", id)
	}

	if err = g.cache.Set(id, q, g.opts.TTL); err != nil {
		// safe to throw away the partially constructed here,
		// because another won and we haven't started the
		// workers.
		if q := g.cache.Get(id); q != nil {
			return q, nil
		}

		return nil, errors.Wrap(err, "problem caching queue")
	}

	if err := q.Start(ctx); err != nil {
		return nil, errors.Wrap(err, "problem starting queue")
	}

	return q, nil
}

func (g *sqlGroup) Put(ctx context.Context, id string, q amboy.Queue) error {
	return g.cache.Set(id, q, 0)
}

func (g *sqlGroup) Close(ctx context.Context) error {
	defer g.canceler()
	return g.cache.Close(ctx)
}

func (g *sqlGroup) Prune(ctx context.Context) error { return g.cache.Prune(ctx) }
func (g *sqlGroup) Len() int                        { return g.cache.Len() }

func (g *sqlGroup) Queues(ctx context.Context) []string {
	queues, _ := g.getQueues(ctx) // nolint
	return queues
}

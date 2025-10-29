package pgq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/queue"
	"github.com/tychoish/fun/erc"
	"github.com/tychoish/fun/ers"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/message"
	"github.com/tychoish/grip/recovery"
)

type sqlGroup struct {
	db        *sqlx.DB
	opts      GroupOptions
	queueOpts Options
	log       grip.Logger
	cache     queue.GroupCache
	canceler  context.CancelFunc
	started   bool // reflects background prune/create threads active
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
	catcher := &erc.Collector{}
	catcher.If(opts.TTL < 0, ers.Error("ttl must be greater than or equal to 0"))
	catcher.If(opts.TTL > 0 && opts.TTL < time.Second, ers.Error("ttl cannot be less than 1 second, unless it is 0"))
	catcher.If(opts.PruneFrequency < 0, ers.Error("prune frequency must be greater than or equal to 0"))
	catcher.If(opts.PruneFrequency > 0 && opts.TTL < time.Second, ers.Error("prune frequency cannot be less than 1 second, unless it is 0"))
	catcher.If((opts.TTL == 0 && opts.PruneFrequency != 0) || (opts.TTL != 0 && opts.PruneFrequency == 0), ers.Error("ttl and prune frequency must both be 0 or both be not 0"))
	catcher.If(opts.DefaultWorkers == 0 && opts.WorkerPoolSize == nil, ers.Error("must specify either a default worker pool size or a WorkerPoolSize function"))

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
		return nil, fmt.Errorf("invalid queue group options: %w", err)
	}

	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid queue options: %w", err)
	}

	opts.UseGroups = true

	group := &sqlGroup{
		queueOpts: opts,
		db:        db,
		opts:      gopts,
		log:       opts.Logger,
		cache:     queue.NewGroupCache(gopts.TTL),
	}

	if !opts.SkipBootstrap {
		if err := PrepareDatabase(ctx, db, opts.SchemaName); err != nil {
			return nil, err
		}
	}

	return group, nil
}

func (g *sqlGroup) Start(ctx context.Context) error {
	if g.started {
		return g.startQueues(ctx)
	}
	ctx, g.canceler = context.WithCancel(ctx)

	if g.opts.PruneFrequency > 0 && g.opts.TTL > 0 {
		go func() {
			defer recovery.LogStackTraceAndContinue("panic in remote queue group ticker")
			ticker := time.NewTicker(g.opts.PruneFrequency)
			defer ticker.Stop()
			errCount := 0
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					err := g.Prune(ctx)
					if err != nil {
						errCount++
					} else {
						errCount = 0
					}

					g.log.LogWhen(errCount > g.opts.BackgroundOperationErrorCountThreshold,
						g.opts.BackgroundOperationErrorLogLevel,
						message.WrapError(err, "problem pruning remote queues"))
				}
			}
		}()
	}

	if g.opts.BackgroundCreateFrequency > 0 {
		go func() {
			defer recovery.LogStackTraceAndContinue("panic in remote queue group ticker")
			ticker := time.NewTicker(g.opts.BackgroundCreateFrequency)
			errCount := 0
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					err := g.startQueues(ctx)
					if err != nil {
						errCount++
					} else {
						errCount = 0
					}

					g.log.LogWhen(errCount > g.opts.BackgroundOperationErrorCountThreshold,
						g.opts.BackgroundOperationErrorLogLevel,
						message.WrapError(err, "problem starting external queues"))
				}
			}
		}()
	}

	g.started = true

	if err := g.startQueues(ctx); err != nil {
		return err
	}
	return nil
}

func (g *sqlGroup) startQueues(ctx context.Context) error {
	queues, err := g.getQueues(ctx)
	if err != nil {
		return err
	}

	catcher := &erc.Collector{}
	for _, id := range queues {
		_, err := g.Get(ctx, id)
		catcher.Push(err)
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
		return nil, err
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

	q, err = NewQueue(ctx, g.db, opts)
	if err != nil {
		return nil, fmt.Errorf("problem creating new queue for '%s': %w", id, err)
	}

	if err = g.cache.Set(id, q, g.opts.TTL); err != nil {
		// safe to throw away the partially constructed here,
		// because another won and we haven't started the
		// workers.
		if q = g.cache.Get(id); q != nil {
			return q, nil
		}

		return nil, fmt.Errorf("problem caching queue: %w", err)
	}

	if err := q.Start(ctx); err != nil {
		return nil, fmt.Errorf("problem starting queue: %w", err)
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

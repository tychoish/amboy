package mdbq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/queue"
	"github.com/tychoish/fun/erc"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/message"
	"github.com/tychoish/grip/recovery"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type remoteMongoQueueGroupSingle struct {
	canceler context.CancelFunc
	client   *mongo.Client
	opts     MongoDBQueueGroupOptions
	log      grip.Logger
	dbOpts   MongoDBOptions
	cache    queue.GroupCache
	started  bool // reflects background prune/create threads active
}

// NewMongoDBSingleQueueGroup constructs a new remote queue group. If ttl is 0, the queues will not be
// TTLed except when the client explicitly calls Prune.
func NewMongoDBSingleQueueGroup(ctx context.Context, opts MongoDBQueueGroupOptions, client *mongo.Client, mdbopts MongoDBOptions) (amboy.QueueGroup, error) {
	if err := opts.validate(); err != nil {
		return nil, fmt.Errorf("invalid queue group options: %w", err)
	}

	if mdbopts.DB == "" {
		return nil, errors.New("no database name specified")
	}

	if mdbopts.URI == "" {
		return nil, errors.New("no mongodb uri specified")
	}

	mdbopts.UseGroups = true
	mdbopts.GroupName = opts.Prefix

	g := &remoteMongoQueueGroupSingle{
		client: client,
		dbOpts: mdbopts,
		opts:   opts,
		log:    opts.Logger,
		cache:  queue.NewGroupCache(opts.TTL),
	}

	return g, nil
}

func (g *remoteMongoQueueGroupSingle) Start(ctx context.Context) error {
	if g.started {
		return g.startQueues(ctx)
	}

	if g.opts.PruneFrequency > 0 && g.opts.TTL > 0 {
		go func() {
			defer recovery.LogStackTraceAndContinue("panic in remote queue group ticker")
			ticker := time.NewTicker(g.opts.PruneFrequency)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					g.log.Error(message.WrapError(g.Prune(ctx), "problem pruning remote queue group database"))
				}
			}
		}()
	}

	if g.opts.BackgroundCreateFrequency > 0 {
		go func() {
			defer recovery.LogStackTraceAndContinue("panic in remote queue group ticker")
			ticker := time.NewTicker(g.opts.BackgroundCreateFrequency)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					g.log.Error(message.WrapError(g.startQueues(ctx), "problem starting external queues"))
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

func (g *remoteMongoQueueGroupSingle) getQueues(ctx context.Context) ([]string, error) {
	cursor, err := g.client.Database(g.dbOpts.DB).Collection(addGroupSuffix(g.opts.Prefix)).Aggregate(ctx,
		[]bson.M{
			{
				"$match": bson.M{
					"$or": []bson.M{
						{
							"status.completed": false,
						},
						{
							"status.completed": true,
							"status.mod_ts":    bson.M{"$gte": time.Now().Add(-g.opts.TTL)},
						},
					},
				},
			},
			{

				"$group": bson.M{
					"_id": nil,
					"groups": bson.M{
						"$addToSet": "$group",
					},
				},
			},
		},
	)
	if err != nil {
		return nil, err
	}

	out := struct {
		Groups []string `bson:"groups"`
	}{}

	catcher := &erc.Collector{}
	for cursor.Next(ctx) {
		if err = cursor.Decode(&out); err != nil {
			catcher.Add(err)
		} else {
			break
		}
	}
	catcher.Add(cursor.Err())
	catcher.Add(cursor.Close(ctx))

	return out.Groups, catcher.Resolve()
}

func (g *remoteMongoQueueGroupSingle) startQueues(ctx context.Context) error {
	queues, err := g.getQueues(ctx)
	if err != nil {
		return err
	}

	catcher := &erc.Collector{}
	for _, id := range queues {
		_, err := g.Get(ctx, id)
		catcher.Add(err)
	}

	return catcher.Resolve()
}

func (g *remoteMongoQueueGroupSingle) Get(ctx context.Context, id string) (amboy.Queue, error) {
	var queue remoteQueue

	switch q := g.cache.Get(id).(type) {
	case remoteQueue:
		return q, nil
	case nil:
		queue = g.opts.constructor(ctx, id)
	default:
		return q, nil
	}

	driver, err := openNewMongoGroupDriver(ctx, g.opts.Prefix, g.dbOpts, id, g.client)
	if err != nil {
		return nil, fmt.Errorf("problem opening driver for queue: %w", err)
	}

	if err = queue.SetDriver(driver); err != nil {
		return nil, fmt.Errorf("problem setting driver: %w", err)
	}

	if err = g.cache.Set(id, queue, g.opts.TTL); err != nil {
		// safe to throw away the partially constructed here,
		// because another won and we haven't started the
		// workers.
		if q := g.cache.Get(id); q != nil {
			return q, nil
		}

		return nil, fmt.Errorf("problem caching queue: %w", err)
	}

	if err := queue.Start(ctx); err != nil {
		return nil, fmt.Errorf("problem starting queue: %w", err)
	}

	return queue, nil
}

func (g *remoteMongoQueueGroupSingle) Put(ctx context.Context, name string, queue amboy.Queue) error {
	return g.cache.Set(name, queue, 0)
}

func (g *remoteMongoQueueGroupSingle) Len() int { return g.cache.Len() }

func (g *remoteMongoQueueGroupSingle) Queues(ctx context.Context) []string {
	queues, _ := g.getQueues(ctx) // nolint
	return queues
}

func (g *remoteMongoQueueGroupSingle) Prune(ctx context.Context) error {
	return g.cache.Prune(ctx)
}

func (g *remoteMongoQueueGroupSingle) Close(ctx context.Context) error {
	if g.canceler != nil {
		defer g.canceler()
	}
	return g.cache.Close(ctx)
}

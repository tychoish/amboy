package mdbq

import (
	"context"
	"fmt"

	"github.com/tychoish/amboy"
	"github.com/tychoish/fun/erc"
	"github.com/tychoish/fun/ers"
	"github.com/tychoish/grip"
	"go.mongodb.org/mongo-driver/mongo"
)

// MongoDBQueueCreationOptions describes the options passed to the remote
// queue, that store jobs in a remote persistence layer to support
// distributed systems of workers.
type MongoDBQueueCreationOptions struct {
	Size    int
	Name    string
	Ordered bool
	MDB     MongoDBOptions
	Logger  grip.Logger
	Client  *mongo.Client
}

// NewMongoDBQueue builds a new queue that persists jobs to a MongoDB
// instance. These queues allow workers running in multiple processes
// to service shared workloads in multiple processes.
func NewMongoDBQueue(ctx context.Context, opts MongoDBQueueCreationOptions) (amboy.Queue, error) {
	if opts.Logger.Sender() == nil {
		opts.Logger = grip.NewLogger(grip.Sender())
	}
	opts.MDB.logger = opts.Logger

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return opts.build(ctx)
}

// Validate ensure that the arguments defined are valid.
func (opts *MongoDBQueueCreationOptions) Validate() error {
	catcher := &erc.Collector{}

	catcher.If(opts.Name == "", ers.Error("must specify a name"))

	catcher.If(opts.Client == nil && (opts.MDB.URI == "" && opts.MDB.DB == ""), ers.Error("must specify database options"))

	return catcher.Resolve()
}

func (opts *MongoDBQueueCreationOptions) build(ctx context.Context) (amboy.Queue, error) {
	var driver remoteQueueDriver
	var err error

	if opts.Client == nil {
		if opts.MDB.UseGroups {
			driver, err = newMongoGroupDriver(opts.Name, opts.MDB, opts.MDB.GroupName)
			if err != nil {
				return nil, fmt.Errorf("problem creating group driver: %w", err)
			}
		} else {
			driver, err = newMongoDriver(opts.Name, opts.MDB)
			if err != nil {
				return nil, fmt.Errorf("problem creating driver: %w", err)
			}
		}

		err = driver.Open(ctx)
	} else {
		if opts.MDB.UseGroups {
			driver, err = openNewMongoGroupDriver(ctx, opts.Name, opts.MDB, opts.MDB.GroupName, opts.Client)
		} else {
			driver, err = openNewMongoDriver(ctx, opts.Name, opts.MDB, opts.Client)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("problem building driver: %w", err)
	}

	var q remoteQueue
	if opts.Ordered {
		q = newSimpleRemoteOrdered(opts.Size, opts.Logger)
	} else {
		q = newRemoteUnordered(opts.Size, opts.Logger)
	}

	if err = q.SetDriver(driver); err != nil {
		return nil, fmt.Errorf("problem configuring queue: %w", err)
	}

	return q, nil
}

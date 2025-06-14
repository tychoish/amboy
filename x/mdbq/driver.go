package mdbq

import (
	"context"
	"strings"
	"time"

	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/queue"
	"github.com/tychoish/amboy/registry"
	"github.com/tychoish/fun/erc"
	"github.com/tychoish/fun/ers"
	"github.com/tychoish/grip"
	"go.mongodb.org/mongo-driver/bson"
)

// remoteQueueDriver describes the interface between a queue and an out of
// process persistence layer, like a database.
type remoteQueueDriver interface {
	ID() string
	Open(context.Context) error
	Close()

	Get(context.Context, string) (amboy.Job, error)
	Put(context.Context, amboy.Job) error
	Save(context.Context, amboy.Job) error

	Jobs(context.Context) <-chan amboy.Job
	Next(context.Context) amboy.Job

	Stats(context.Context) amboy.QueueStats
	Complete(context.Context, amboy.Job) error

	LockTimeout() time.Duration

	SetDispatcher(queue.Dispatcher)
	Dispatcher() queue.Dispatcher

	Delete(context.Context, string) error
}

// MongoDBOptions is a struct passed to the NewMongo constructor to
// communicate mgoDriver specific settings about the driver's behavior
// and operation.
type MongoDBOptions struct {
	URI                      string
	DB                       string
	GroupName                string
	UseGroups                bool
	Priority                 bool
	CheckWaitUntil           bool
	CheckDispatchBy          bool
	SkipQueueIndexBuilds     bool
	SkipReportingIndexBuilds bool
	Marshler                 registry.Marshaler
	Unmarshaler              registry.Unmarshaler
	WaitInterval             time.Duration
	// TTL sets the number of seconds for a TTL index on the "info.created"
	// field. If set to zero, the TTL index will not be created and
	// and documents may live forever in the database.
	TTL time.Duration
	// LockTimeout overrides the default job lock timeout if set.
	LockTimeout time.Duration

	logger grip.Logger
}

// DefaultMongoDBOptions constructs a new options object with default
// values: connecting to a MongoDB instance on localhost, using the
// "amboy" database, and *not* using priority ordering of jobs.
func DefaultMongoDBOptions() MongoDBOptions {
	return MongoDBOptions{
		URI:                      "mongodb://localhost:27017",
		DB:                       "amboy",
		Priority:                 false,
		UseGroups:                false,
		CheckWaitUntil:           true,
		SkipQueueIndexBuilds:     false,
		SkipReportingIndexBuilds: false,
		WaitInterval:             time.Second,
		Marshler:                 bson.Marshal,
		Unmarshaler:              bson.Unmarshal,
		LockTimeout:              amboy.LockTimeout,
	}
}

// Validate validates that the required options are given and sets fields that
// are unspecified and have a default value.
func (opts *MongoDBOptions) Validate() error {
	catcher := &erc.Collector{}
	catcher.When(opts.URI == "", ers.Error("must specify connection URI"))
	catcher.When(opts.DB == "", ers.Error("must specify database"))
	catcher.When(opts.LockTimeout < 0, ers.Error("cannot have negative lock timeout"))
	catcher.When(opts.Marshler == nil, ers.Error("must specify a bson marshaler"))
	catcher.When(opts.Unmarshaler == nil, ers.Error("must specify a bson unmashlser"))

	if opts.LockTimeout == 0 {
		opts.LockTimeout = amboy.LockTimeout
	}

	if opts.logger.Sender() == nil {
		opts.logger = grip.NewLogger(grip.Sender())
	}

	return catcher.Resolve()
}

func addJobsSuffix(s string) string {
	return s + ".jobs"
}

func trimJobsSuffix(s string) string {
	return strings.TrimSuffix(s, ".jobs")
}

func addGroupSuffix(s string) string {
	return s + ".group"
}

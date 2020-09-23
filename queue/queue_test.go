package queue

import (
	"context"
	"testing"
	"time"

	"github.com/deciduosity/amboy"
	"github.com/deciduosity/amboy/queue/testutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const defaultLocalQueueCapcity = 10000

func DefaultQueueTestCases() []testutil.QueueTestCase {
	return []testutil.QueueTestCase{
		{
			Name:                    "AdaptiveOrdering",
			OrderedSupported:        true,
			OrderedStartsBefore:     true,
			WaitUntilSupported:      true,
			SingleWorker:            true,
			DispatchBeforeSupported: true,
			MinSize:                 2,
			MaxSize:                 16,
			Constructor: func(ctx context.Context, _ string, size int) (amboy.Queue, testutil.TestCloser, error) {
				return NewAdaptiveOrderedLocalQueue(size, defaultLocalQueueCapcity), func(ctx context.Context) error { return nil }, nil
			},
		},
		{
			Name:                "LocalOrdered",
			OrderedStartsBefore: false,
			OrderedSupported:    true,
			MinSize:             2,
			MaxSize:             8,
			Constructor: func(ctx context.Context, _ string, size int) (amboy.Queue, testutil.TestCloser, error) {
				return NewLocalOrdered(size), func(ctx context.Context) error { return nil }, nil
			},
		},
		{
			Name: "Priority",
			Constructor: func(ctx context.Context, _ string, size int) (amboy.Queue, testutil.TestCloser, error) {
				return NewLocalPriorityQueue(size, defaultLocalQueueCapcity), func(ctx context.Context) error { return nil }, nil
			},
		},
		{
			Name:                    "LimitedSize",
			WaitUntilSupported:      true,
			DispatchBeforeSupported: true,
			Constructor: func(ctx context.Context, _ string, size int) (amboy.Queue, testutil.TestCloser, error) {
				return NewLocalLimitedSize(size, 1024*size), func(ctx context.Context) error { return nil }, nil
			},
		},
		{
			Name:         "Shuffled",
			SingleWorker: true,
			Constructor: func(ctx context.Context, _ string, size int) (amboy.Queue, testutil.TestCloser, error) {
				return NewShuffledLocal(size, defaultLocalQueueCapcity), func(ctx context.Context) error { return nil }, nil
			},
		},
		{
			Name:    "SQSFifo",
			MaxSize: 4,
			Skip:    true,
			Constructor: func(ctx context.Context, _ string, size int) (amboy.Queue, testutil.TestCloser, error) {
				q, err := NewSQSFifoQueue(randomString(4), size)
				closer := func(ctx context.Context) error { return nil }
				return q, closer, err
			},
		},
	}
}

func MongoDBQueueTestCases(client *mongo.Client) []testutil.QueueTestCase {
	return []testutil.QueueTestCase{
		{
			Name:     "MongoUnordered",
			IsRemote: true,
			Constructor: func(ctx context.Context, name string, size int) (amboy.Queue, testutil.TestCloser, error) {
				opts := MongoDBQueueCreationOptions{
					Size:    size,
					Name:    name,
					Ordered: false,
					MDB:     DefaultMongoDBOptions(),
					Client:  client,
				}
				opts.MDB.Format = amboy.BSON2
				q, err := NewMongoDBQueue(ctx, opts)
				if err != nil {
					return nil, nil, err
				}
				rq, ok := q.(remoteQueue)
				if !ok {
					return nil, nil, errors.New("invalid queue constructed")
				}

				closer := func(ctx context.Context) error {
					d := rq.Driver()
					if d != nil {
						d.Close()
					}

					return client.Database(opts.MDB.DB).Collection(addJobsSuffix(name)).Drop(ctx)
				}

				return q, closer, nil
			},
		},
		{
			Name:     "MongoGroupUnordered",
			IsRemote: true,
			Constructor: func(ctx context.Context, name string, size int) (amboy.Queue, testutil.TestCloser, error) {
				opts := MongoDBQueueCreationOptions{
					Size:    size,
					Name:    name,
					Ordered: false,
					MDB:     DefaultMongoDBOptions(),
					Client:  client,
				}
				opts.MDB.Format = amboy.BSON2
				opts.MDB.GroupName = "group." + name
				opts.MDB.UseGroups = true
				q, err := NewMongoDBQueue(ctx, opts)
				if err != nil {
					return nil, nil, err
				}
				rq, ok := q.(remoteQueue)
				if !ok {
					return nil, nil, errors.New("invalid queue constructed")
				}

				closer := func(ctx context.Context) error {
					d := rq.Driver()
					if d != nil {
						d.Close()
					}

					return client.Database(opts.MDB.DB).Collection(addGroupSufix(name)).Drop(ctx)
				}

				return q, closer, nil
			},
		},
		{
			Name:     "MongoUnorderedMGOBSON",
			IsRemote: true,
			MaxSize:  32,
			Constructor: func(ctx context.Context, name string, size int) (amboy.Queue, testutil.TestCloser, error) {
				opts := MongoDBQueueCreationOptions{
					Size:    size,
					Name:    name,
					Ordered: false,
					MDB:     DefaultMongoDBOptions(),
					Client:  client,
				}
				opts.MDB.Format = amboy.BSON
				q, err := NewMongoDBQueue(ctx, opts)
				if err != nil {
					return nil, nil, err
				}
				rq, ok := q.(remoteQueue)
				if !ok {
					return nil, nil, errors.New("invalid queue constructed")
				}

				closer := func(ctx context.Context) error {
					d := rq.Driver()
					if d != nil {
						d.Close()
					}

					return client.Database(opts.MDB.DB).Collection(addJobsSuffix(name)).Drop(ctx)
				}

				return q, closer, nil
			},
		},
		{
			Name:     "MongoOrdered",
			IsRemote: true,
			Constructor: func(ctx context.Context, name string, size int) (amboy.Queue, testutil.TestCloser, error) {
				opts := MongoDBQueueCreationOptions{
					Size:    size,
					Name:    name,
					Ordered: true,
					MDB:     DefaultMongoDBOptions(),
					Client:  client,
				}
				opts.MDB.Format = amboy.BSON2
				q, err := NewMongoDBQueue(ctx, opts)
				if err != nil {
					return nil, nil, err
				}
				rq, ok := q.(remoteQueue)
				if !ok {
					return nil, nil, errors.New("invalid queue constructed")
				}

				closer := func(ctx context.Context) error {
					d := rq.Driver()
					if d != nil {
						d.Close()
					}

					return client.Database(opts.MDB.DB).Collection(addJobsSuffix(name)).Drop(ctx)
				}

				return q, closer, nil
			},
		},
	}

}

func TestQueueSmoke(t *testing.T) {
	bctx, bcancel := context.WithCancel(context.Background())
	defer bcancel()

	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017").SetConnectTimeout(time.Second))
	require.NoError(t, err)
	require.NoError(t, client.Connect(bctx))

	defer func() { require.NoError(t, client.Disconnect(bctx)) }()

	for test := range testutil.MergeQueueTestCases(bctx, DefaultQueueTestCases(), MongoDBQueueTestCases(client)) {
		testutil.RunSmokeTest(bctx, t, test)
	}
}

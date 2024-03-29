package mdbq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/queue/testutil"
	"github.com/tychoish/fun/erc"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func MongoDBQueueTestCases(client *mongo.Client) []testutil.QueueTestCase {
	return []testutil.QueueTestCase{
		{
			Name:                 "MongoUnordered",
			IsRemote:             true,
			DisableParallelTests: true,
			Constructor: func(ctx context.Context, name string, size int) (amboy.Queue, testutil.TestCloser, error) {
				opts := MongoDBQueueCreationOptions{
					Size:    size,
					Name:    name,
					Ordered: false,
					MDB:     DefaultMongoDBOptions(),
					Client:  client,
				}
				q, err := NewMongoDBQueue(ctx, opts)
				if err != nil {
					return nil, nil, err
				}
				rq, ok := q.(remoteQueue)
				if !ok {
					return nil, nil, errors.New("invalid queue constructed")
				}

				closer := func(ctx context.Context) error {
					catcher := &erc.Collector{}

					catcher.Add(q.Close(ctx))

					if d := rq.Driver(); d != nil {
						d.Close()
					}

					catcher.Add(client.Database(opts.MDB.DB).Collection(addJobsSuffix(name)).Drop(ctx))

					return catcher.Resolve()
				}

				return q, closer, nil
			},
		},
		{
			Name:                 "MongoGroupUnordered",
			IsRemote:             true,
			DisableParallelTests: true,
			Constructor: func(ctx context.Context, name string, size int) (amboy.Queue, testutil.TestCloser, error) {
				opts := MongoDBQueueCreationOptions{
					Size:    size,
					Name:    name,
					Ordered: false,
					MDB:     DefaultMongoDBOptions(),
					Client:  client,
				}
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
					catcher := &erc.Collector{}

					catcher.Add(q.Close(ctx))

					if d := rq.Driver(); d != nil {
						d.Close()
					}

					catcher.Add(client.Database(opts.MDB.DB).Collection(addJobsSuffix(name)).Drop(ctx))

					return catcher.Resolve()
				}

				return q, closer, nil
			},
		},
		{
			Name:                 "MongoOrdered",
			IsRemote:             true,
			DisableParallelTests: true,
			Constructor: func(ctx context.Context, name string, size int) (amboy.Queue, testutil.TestCloser, error) {
				opts := MongoDBQueueCreationOptions{
					Size:    size,
					Name:    name,
					Ordered: true,
					MDB:     DefaultMongoDBOptions(),
					Client:  client,
				}
				q, err := NewMongoDBQueue(ctx, opts)
				if err != nil {
					return nil, nil, err
				}
				rq, ok := q.(remoteQueue)
				if !ok {
					return nil, nil, errors.New("invalid queue constructed")
				}

				closer := func(ctx context.Context) error {
					catcher := &erc.Collector{}

					catcher.Add(q.Close(ctx))

					if d := rq.Driver(); d != nil {
						d.Close()
					}

					catcher.Add(client.Database(opts.MDB.DB).Collection(addJobsSuffix(name)).Drop(ctx))

					return catcher.Resolve()
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

	for test := range testutil.MergeQueueTestCases(bctx, MongoDBQueueTestCases(client)) {
		testutil.RunSmokeTest(bctx, t, test)
	}
}

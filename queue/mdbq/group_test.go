package mdbq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/queue"
	"github.com/tychoish/amboy/queue/testutil"
	"github.com/tychoish/grip"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func localConstructor(ctx context.Context) (amboy.Queue, error) {
	return queue.NewLocalLimitedSize(&queue.FixedSizeQueueOptions{Workers: 2, Capacity: 128}), nil
}

func TestQueueGroup(t *testing.T) {
	bctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const mdburl = "mongodb://localhost:27017"
	client, cerr := mongo.NewClient(options.Client().ApplyURI(mdburl).SetConnectTimeout(2 * time.Second))
	require.NoError(t, cerr)
	require.NoError(t, client.Connect(bctx))
	defer func() { require.NoError(t, client.Disconnect(bctx)) }()

	t.Run("Constructor", func(t *testing.T) {
		remoteTests := []struct {
			name       string
			db         string
			prefix     string
			uri        string
			workers    int
			workerFunc func(string) int
			valid      bool
		}{
			{
				name:       "AllFieldsSet",
				db:         "db",
				prefix:     "prefix",
				uri:        "uri",
				workerFunc: func(s string) int { return 1 },
				workers:    1,
				valid:      true,
			},
			{
				name:   "WorkersMissing",
				db:     "db",
				prefix: "prefix",
				uri:    "uri",
				valid:  false,
			},
			{
				name:       "WorkerFunctions",
				db:         "db",
				prefix:     "prefix",
				workerFunc: func(s string) int { return 1 },
				uri:        "uri",
				valid:      true,
			},
			{
				name:    "WorkerDefault",
				db:      "db",
				prefix:  "prefix",
				workers: 2,
				uri:     "uri",
				valid:   true,
			},
			{
				name:    "DBMissing",
				prefix:  "prefix",
				uri:     "uri",
				workers: 1,
				valid:   false,
			},
			{
				name:    "PrefixMissing",
				db:      "db",
				workers: 1,
				uri:     "uri",
				valid:   false,
			},
			{
				name:    "URIMissing",
				db:      "db",
				prefix:  "prefix",
				workers: 1,
				valid:   false,
			},
		}

		t.Run("Mongo", func(t *testing.T) {
			for _, remoteTest := range remoteTests {
				t.Run(remoteTest.name, func(t *testing.T) {
					ctx, cancel := context.WithCancel(bctx)
					defer cancel()
					mopts := MongoDBOptions{
						DB:           remoteTest.db,
						URI:          remoteTest.uri,
						WaitInterval: time.Millisecond,
						Marshler:     bson.Marshal,
						Unmarshaler:  bson.Unmarshal,
					}

					remoteOpts := MongoDBQueueGroupOptions{
						DefaultWorkers: remoteTest.workers,
						WorkerPoolSize: remoteTest.workerFunc,
						Prefix:         remoteTest.prefix,
						TTL:            time.Minute,
						PruneFrequency: time.Minute,
					}

					g, err := NewMongoDBQueueGroup(ctx, remoteOpts, client, mopts) // nolint
					if remoteTest.valid {
						require.NoError(t, err)
						require.NotNil(t, g)
					} else {
						require.Error(t, err)
						require.Nil(t, g)
					}
				})
			}
		})
		t.Run("MongoMerged", func(t *testing.T) {
			for _, remoteTest := range remoteTests {
				t.Run(remoteTest.name, func(t *testing.T) {
					ctx, cancel := context.WithCancel(bctx)
					defer cancel()
					mopts := MongoDBOptions{
						WaitInterval: time.Millisecond,
						DB:           remoteTest.db,
						URI:          remoteTest.uri,
						Marshler:     bson.Marshal,
						Unmarshaler:  bson.Unmarshal,
					}

					remoteOpts := MongoDBQueueGroupOptions{
						DefaultWorkers: remoteTest.workers,
						WorkerPoolSize: remoteTest.workerFunc,
						Prefix:         remoteTest.prefix,
						TTL:            time.Minute,
						PruneFrequency: time.Minute,
					}

					g, err := NewMongoDBSingleQueueGroup(ctx, remoteOpts, client, mopts) // nolint
					if remoteTest.valid {
						require.NoError(t, err)
						require.NotNil(t, g)
					} else {
						require.Error(t, err)
						require.Nil(t, g)
					}
				})
			}
		})
	})
	t.Run("PruneSmokeTest", func(t *testing.T) {
		ctx, cancel := context.WithCancel(bctx)
		defer cancel()
		mopts := MongoDBOptions{
			DB:           "amboy_group_test",
			WaitInterval: time.Millisecond,
			URI:          "mongodb://localhost:27017",
		}

		for i := 0; i < 10; i++ {
			_, err := client.Database("amboy_group_test").Collection(fmt.Sprintf("gen-%d.jobs", i)).InsertOne(ctx, bson.M{"foo": "bar"})
			require.NoError(t, err)
		}
		remoteOpts := MongoDBQueueGroupOptions{
			Prefix:         "gen",
			DefaultWorkers: 1,
			TTL:            time.Second,
			PruneFrequency: time.Second,
		}
		_, err := NewMongoDBQueueGroup(ctx, remoteOpts, client, mopts)
		require.NoError(t, err)
		time.Sleep(time.Second)
		for i := 0; i < 10; i++ {
			count, err := client.Database("amboy_group_test").Collection(fmt.Sprintf("gen-%d.jobs", i)).CountDocuments(ctx, bson.M{})
			require.NoError(t, err)
			require.Zero(t, count, fmt.Sprintf("gen-%d.jobs not dropped", i))
		}
	})
	t.Run("Integration", func(t *testing.T) {
		for _, group := range []testutil.GroupIntegrationCase{
			{
				Name:             "Mongo",
				LocalConstructor: localConstructor,
				Constructor: func(ctx context.Context, ttl time.Duration) (amboy.QueueGroup, testutil.TestCloser, error) {
					mopts := MongoDBOptions{
						WaitInterval: time.Millisecond,
						DB:           "amboy_group_test",
						URI:          "mongodb://localhost:27017",
						Marshler:     bson.Marshal,
						Unmarshaler:  bson.Unmarshal,
					}

					closer := func(cctx context.Context) error {
						return client.Database(mopts.DB).Drop(cctx)
					}

					opts := MongoDBQueueGroupOptions{
						DefaultWorkers: 1,
						Prefix:         "prefix",
						TTL:            ttl,
						PruneFrequency: ttl,
					}

					if err := client.Database(mopts.DB).Drop(ctx); err != nil {
						return nil, closer, err
					}

					if err := client.Ping(ctx, nil); err != nil {
						return nil, closer, errors.Wrap(err, "server not pingable")
					}

					qg, err := NewMongoDBQueueGroup(ctx, opts, client, mopts)
					return qg, closer, err
				},
			},
			{
				Name:             "MongoMerged",
				LocalConstructor: localConstructor,
				Constructor: func(ctx context.Context, ttl time.Duration) (amboy.QueueGroup, testutil.TestCloser, error) {
					mopts := MongoDBOptions{
						DB:           "amboy_group_test",
						URI:          "mongodb://localhost:27017",
						WaitInterval: time.Millisecond,
						Marshler:     bson.Marshal,
						Unmarshaler:  bson.Unmarshal,
					}

					closer := func(cctx context.Context) error {
						catcher := grip.NewBasicCatcher()
						catcher.Add(client.Database(mopts.DB).Drop(cctx))
						return catcher.Resolve()
					}
					if ttl == 0 {
						ttl = time.Hour
					}

					opts := MongoDBQueueGroupOptions{
						DefaultWorkers: 1,
						Prefix:         "prefix",
						TTL:            ttl,
						PruneFrequency: ttl,
					}

					if err := client.Database(mopts.DB).Drop(ctx); err != nil {
						return nil, closer, err
					}

					if err := client.Ping(ctx, nil); err != nil {
						return nil, closer, errors.Wrap(err, "server not pingable")
					}

					qg, err := NewMongoDBSingleQueueGroup(ctx, opts, client, mopts)
					return qg, closer, err
				},
			},
		} {
			testutil.RunGroupIntegrationTest(bctx, t, group)
		}
	})
}

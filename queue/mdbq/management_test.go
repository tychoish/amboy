package mdbq

import (
	"context"
	"testing"
	"time"

	"github.com/deciduosity/amboy/management"
	"github.com/deciduosity/amboy/queue/testutil"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestManagerSuiteBackedByMongoDB(t *testing.T) {
	s := new(testutil.ManagerSuite)
	name := uuid.New().String()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(opts.URI))
	require.NoError(t, err)
	s.Factory = func() management.Manager {
		manager, err := MakeDBQueueManager(ctx, DBQueueManagerOptions{
			Options: opts,
			Name:    name,
		}, client)
		require.NoError(t, err)
		return manager
	}

	s.Setup = func() {
		s.Require().NoError(client.Database(opts.DB).Drop(ctx))
		args := MongoDBQueueCreationOptions{
			Size:   2,
			Name:   name,
			MDB:    opts,
			Client: client,
		}

		remote, err := NewMongoDBQueue(ctx, args)
		require.NoError(t, err)
		s.Queue = remote
	}

	s.Cleanup = func() error {
		require.NoError(t, client.Disconnect(ctx))
		s.Queue.Runner().Close(ctx)
		return nil
	}

	suite.Run(t, s)
}

func TestManagerSuiteBackedByMongoDBSingleGroup(t *testing.T) {
	s := new(testutil.ManagerSuite)
	name := uuid.New().String()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(opts.URI))
	require.NoError(t, err)
	s.Factory = func() management.Manager {
		manager, err := MakeDBQueueManager(ctx, DBQueueManagerOptions{
			Options:     opts,
			Name:        name,
			Group:       "foo",
			SingleGroup: true,
		}, client)
		require.NoError(t, err)
		return manager
	}

	opts.UseGroups = true
	opts.GroupName = "foo"

	s.Setup = func() {
		s.Require().NoError(client.Database(opts.DB).Drop(ctx))
		args := MongoDBQueueCreationOptions{
			Size:   2,
			Name:   name,
			MDB:    opts,
			Client: client,
		}

		remote, err := NewMongoDBQueue(ctx, args)
		require.NoError(t, err)
		s.Queue = remote
	}

	s.Cleanup = func() error {
		require.NoError(t, client.Disconnect(ctx))
		s.Queue.Runner().Close(ctx)
		return nil
	}

	suite.Run(t, s)
}

func TestManagerSuiteBackedByMongoDBMultiGroup(t *testing.T) {
	s := new(testutil.ManagerSuite)
	name := uuid.New().String()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(opts.URI))
	require.NoError(t, err)
	s.Factory = func() management.Manager {
		manager, err := MakeDBQueueManager(ctx, DBQueueManagerOptions{
			Options:  opts,
			Name:     name,
			Group:    "foo",
			ByGroups: true,
		}, client)
		require.NoError(t, err)
		return manager
	}

	opts.UseGroups = true
	opts.GroupName = "foo"

	s.Setup = func() {
		s.Require().NoError(client.Database(opts.DB).Drop(ctx))
		args := MongoDBQueueCreationOptions{
			Size:   2,
			Name:   name,
			MDB:    opts,
			Client: client,
		}

		remote, err := NewMongoDBQueue(ctx, args)
		require.NoError(t, err)
		s.queue = remote
	}

	s.Cleanup = func() error {
		require.NoError(t, client.Disconnect(ctx))
		s.Queue.Runner().Close(ctx)
		return nil
	}

	suite.Run(t, s)
}

func TestMongoDBConstructors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017").SetConnectTimeout(time.Second))
	require.NoError(t, err)
	require.NoError(t, client.Connect(ctx))

	t.Run("NilSessionShouldError", func(t *testing.T) {
		opts := DefaultMongoDBOptions()
		opts.DB = "amboy_test"
		conf := DBQueueManagerOptions{Options: opts}

		db, err := MakeDBQueueManager(ctx, conf, nil)
		assert.Error(t, err)
		assert.Nil(t, db)
	})
	t.Run("UnpingableSessionError", func(t *testing.T) {
		opts := DefaultMongoDBOptions()
		opts.DB = "amboy_test"
		conf := DBQueueManagerOptions{Options: opts}

		db, err := MakeDBQueueManager(ctx, conf, client)
		assert.Error(t, err)
		assert.Nil(t, db)
	})
	t.Run("BuildNewConnector", func(t *testing.T) {
		opts := DefaultMongoDBOptions()
		opts.DB = "amboy_test"
		conf := DBQueueManagerOptions{Name: "foo", Options: opts}

		db, err := MakeDBQueueManager(ctx, conf, client)
		assert.NoError(t, err)
		assert.NotNil(t, db)

		r, ok := db.(*dbQueueManager)
		require.True(t, ok)
		require.NotNil(t, r)
		assert.NotZero(t, r.collection)
	})
	t.Run("DialWithNewConstructor", func(t *testing.T) {
		opts := DefaultMongoDBOptions()
		opts.DB = "amboy_test"
		conf := DBQueueManagerOptions{Name: "foo", Options: opts}

		r, err := NewDBQueueManager(ctx, conf)
		assert.NoError(t, err)
		assert.NotNil(t, r)
	})
	t.Run("DialWithBadURI", func(t *testing.T) {
		opts := DefaultMongoDBOptions()
		opts.DB = "amboy_test"
		opts.URI = "mongodb://lochost:26016"
		conf := DBQueueManagerOptions{Options: opts}

		r, err := NewDBQueueManager(ctx, conf)
		assert.Error(t, err)
		assert.Nil(t, r)
	})
}

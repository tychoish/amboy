package pgq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/deciduosity/amboy"
	"github.com/deciduosity/amboy/job"
	"github.com/deciduosity/amboy/queue/testutil"
	"github.com/deciduosity/grip"
	"github.com/deciduosity/grip/message"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func init() {
	job.RegisterDefaultJobs()
}

func GetTestDatabase(bctx context.Context, t *testing.T) (*sqlx.DB, func() error) {
	db, closer, err := MakeTestDatabase(bctx, uuid.New().String()[0:7])
	require.NoError(t, err)

	return db, closer
}

func MakeTestDatabase(bctx context.Context, name string) (*sqlx.DB, func() error, error) {
	ctx, cancel := context.WithCancel(bctx)
	dbName := "amboy_testing_" + name

	tdb, err := sqlx.ConnectContext(ctx, "postgres", "user=amboy database=postgres sslmode=disable")
	if err != nil {
		cancel()
		return nil, nil, err
	}
	tdb.SetMaxOpenConns(128)
	tdb.SetMaxIdleConns(8)

	_, _ = tdb.Exec("CREATE DATABASE " + dbName)

	db, err := sqlx.ConnectContext(ctx, "postgres", fmt.Sprintf("user=amboy database=%s sslmode=disable", dbName))
	if err != nil {
		cancel()
		return nil, nil, err
	}

	db.SetMaxOpenConns(128)
	db.SetMaxIdleConns(8)

	closer := func() error {
		cancel()
		catcher := grip.NewBasicCatcher()
		catcher.Wrap(db.Close(), "problem closing test database")

		_, err = tdb.Exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1;", dbName)
		catcher.Wrap(err, "problem killing connections")

		_, err = tdb.Exec("DROP DATABASE " + dbName)
		if perr, ok := err.(*pq.Error); ok && perr.Code == "3D000" {
			grip.Debug(errors.Wrap(err, "error dropping database"))
		} else {
			catcher.Wrap(err, "error dropping database")
		}

		catcher.Wrap(tdb.Close(), "problem closing connection")
		grip.Critical(message.WrapError(catcher.Resolve(), "problem cleaning up test database"))
		return nil
	}

	return db, closer, nil

}

func TestQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()
	t.Run("GetPutRoundTrip", func(t *testing.T) {
		db, close := GetTestDatabase(ctx, t)
		defer close()

		q, err := NewQueue(db, Options{})
		require.NoError(t, err)
		j := job.NewShellJob("ls", "")
		j.UpdateTimeInfo(amboy.JobTimeInfo{
			Created: time.Now().UTC().Round(time.Millisecond),
		})
		id := j.ID()
		require.NoError(t, q.Put(ctx, j))
		jrt, ok := q.Get(ctx, id)
		require.True(t, ok)
		require.Equal(t, j, jrt)
	})
}

func TestQueueSmoke(t *testing.T) {
	t.Parallel()
	bctx, bcancel := context.WithCancel(context.Background())
	defer bcancel()

	for _, test := range []testutil.QueueTestCase{
		{
			Name: "PostgreSQL/Simple",
			Constructor: func(ctx context.Context, name string, size int) (amboy.Queue, testutil.TestCloser, error) {
				db, closer, err := MakeTestDatabase(ctx, name)
				if err != nil {
					return nil, nil, err
				}
				q, err := NewQueue(db, Options{
					Name:     name,
					PoolSize: size,
				})
				if err != nil {
					return nil, nil, err
				}

				return q, func(ctx context.Context) error { q.Runner().Close(ctx); return closer() }, nil
			},
			SingleWorker:   false,
			IsRemote:       true,
			MultiSupported: true,
			MaxSize:        32,
		},
		{
			Name: "PostgreSQL/Timing",
			Constructor: func(ctx context.Context, name string, size int) (amboy.Queue, testutil.TestCloser, error) {
				db, closer, err := MakeTestDatabase(ctx, name)
				if err != nil {
					return nil, nil, err
				}
				q, err := NewQueue(db, Options{
					Name:            name,
					PoolSize:        size,
					CheckWaitUntil:  true,
					CheckDispatchBy: true,
				})
				if err != nil {
					return nil, nil, err
				}

				return q, func(ctx context.Context) error { q.Runner().Close(ctx); return closer() }, nil
			},
			SingleWorker:            false,
			IsRemote:                true,
			WaitUntilSupported:      true,
			DispatchBeforeSupported: true,
			MaxSize:                 32,
		},
		{
			Name: "PostgreSQL/Group",
			Constructor: func(ctx context.Context, name string, size int) (amboy.Queue, testutil.TestCloser, error) {
				db, closer, err := MakeTestDatabase(ctx, name)
				if err != nil {
					return nil, nil, err
				}

				q, err := NewQueue(db, Options{
					Name:      name,
					PoolSize:  size,
					UseGroups: true,
					GroupName: "kip",
				})
				if err != nil {
					return nil, nil, err
				}

				return q, func(ctx context.Context) error { q.Runner().Close(ctx); return closer() }, nil
			},
			IsRemote:       true,
			SingleWorker:   false,
			MultiSupported: true,
			MaxSize:        32,
		},
	} {
		testutil.RunSmokeTest(bctx, t, test)
	}
}

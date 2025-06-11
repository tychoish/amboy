package pgq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/job"
	"github.com/tychoish/amboy/queue/testutil"
	"github.com/tychoish/fun/erc"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/message"
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
		catcher := &erc.Collector{}
		catcher.Add(db.Close())

		_, err = tdb.Exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1;", dbName)
		catcher.Wrap(err, "problem killing connections")

		_, err = tdb.Exec("DROP DATABASE " + dbName)
		if perr, ok := err.(*pq.Error); ok && perr.Code == "3D000" {
			grip.Debug(fmt.Errorf("dropping database: %w", err))
		} else {
			catcher.Add(err)
		}

		catcher.Add(tdb.Close())
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

		q, err := NewQueue(ctx, db, Options{})
		require.NoError(t, err)
		j := job.NewShellJob("ls", "")
		j.UpdateTimeInfo(amboy.JobTimeInfo{
			Created: time.Now().UTC().Round(time.Millisecond),
		})
		id := j.ID()
		require.NoError(t, q.Put(ctx, j))
		jrt, err := q.Get(ctx, id)
		require.NoError(t, err)

		// TODO (Sean): More comprehensive equality checks
		// (See also BasicRoundTrip test in serialization tests)
		require.Equal(t, j.ID(), jrt.Status().ID)
		require.Equal(t, j.JobType, jrt.Type())
		require.True(t, j.TimeInfo().Created.Equal(jrt.TimeInfo().Created))
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
				q, err := NewQueue(ctx, db, Options{
					Name:            name,
					PoolSize:        size,
					CompleteRetries: 2,
					WaitInterval:    2 * time.Second,
				})
				if err != nil {
					return nil, nil, err
				}

				return q, func(ctx context.Context) error {
					catcher := &erc.Collector{}
					go func() {

						cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
						defer cancel()

						catcher.Add(q.Close(cctx))
						grip.Warning(CleanDatabase(cctx, db, "amboy"))
						catcher.Add(closer())

					}()
					return catcher.Resolve()
				}, nil
			},
			SingleWorker:   false,
			IsRemote:       true,
			MultiSupported: true,
			MaxSize:        16,
		},

		{
			Name: "PostgreSQL/Ordered",
			Constructor: func(ctx context.Context, name string, size int) (amboy.Queue, testutil.TestCloser, error) {
				db, closer, err := MakeTestDatabase(ctx, name)
				if err != nil {
					return nil, nil, err
				}
				q, err := NewQueue(ctx, db, Options{
					Name:            name,
					PoolSize:        size,
					CompleteRetries: 2,
					WaitInterval:    2 * time.Second,
					Ordered:         true,
				})
				if err != nil {
					return nil, nil, err
				}
				return q, func(ctx context.Context) error {
					catcher := &erc.Collector{}
					go func() {

						cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
						defer cancel()

						catcher.Add(q.Close(cctx))
						grip.Warning(CleanDatabase(cctx, db, "amboy"))
						catcher.Add(closer())

					}()
					return catcher.Resolve()
				}, nil
			},
			SingleWorker:          false,
			IsRemote:              true,
			OrderedSupported:      true,
			MultiSupported:        true,
			MinSize:               2,
			MaxSize:               16,
			SkipRateLimitedWorker: true,
		},

		{
			Name: "PostgreSQL/Timing",
			Constructor: func(ctx context.Context, name string, size int) (amboy.Queue, testutil.TestCloser, error) {
				db, closer, err := MakeTestDatabase(ctx, name)
				if err != nil {
					return nil, nil, err
				}
				q, err := NewQueue(ctx, db, Options{
					Name:            name,
					PoolSize:        size,
					CompleteRetries: 2,
					CheckWaitUntil:  true,
					CheckDispatchBy: true,
					WaitInterval:    2 * time.Second,
				})
				if err != nil {
					return nil, nil, err
				}

				return q, func(ctx context.Context) error {
					catcher := &erc.Collector{}
					go func() {

						cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
						defer cancel()

						catcher.Add(q.Close(cctx))
						grip.Warning(CleanDatabase(cctx, db, "amboy"))
						catcher.Add(closer())

					}()
					return catcher.Resolve()
				}, nil
			},
			SingleWorker:            false,
			IsRemote:                true,
			WaitUntilSupported:      true,
			DispatchBeforeSupported: true,
			MinSize:                 2,
			MaxSize:                 16,
			SkipRateLimitedWorker:   true,
		},
		{
			Name: "PostgreSQL/Group",
			Constructor: func(ctx context.Context, name string, size int) (amboy.Queue, testutil.TestCloser, error) {
				db, closer, err := MakeTestDatabase(ctx, name)
				if err != nil {
					return nil, nil, err
				}

				q, err := NewQueue(ctx, db, Options{
					Name:            name,
					PoolSize:        size,
					UseGroups:       true,
					CompleteRetries: 2,
					GroupName:       "kip",
					WaitInterval:    2 * time.Second,
				})
				if err != nil {
					return nil, nil, err
				}

				return q, func(ctx context.Context) error {
					catcher := &erc.Collector{}
					go func() {

						cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
						defer cancel()

						catcher.Add(q.Close(cctx))
						grip.Warning(CleanDatabase(cctx, db, "amboy"))
						catcher.Add(closer())

					}()
					return catcher.Resolve()
				}, nil
			},
			IsRemote:              true,
			SingleWorker:          false,
			MultiSupported:        true,
			MinSize:               2,
			MaxSize:               8,
			SkipRateLimitedWorker: true,
		},
	} {
		testutil.RunSmokeTest(bctx, t, test)
	}
}

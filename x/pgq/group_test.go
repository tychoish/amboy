package pgq

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/queue"
	"github.com/tychoish/amboy/queue/testutil"
	"github.com/tychoish/fun/erc"
	"github.com/tychoish/grip"
)

func TestGroup(t *testing.T) {
	logger := grip.NewLogger(grip.Sender())

	bctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()
	t.Run("Constructor", func(t *testing.T) {
		remoteTests := []struct {
			name       string
			prefix     string
			workers    int
			workerFunc func(string) int
			valid      bool
		}{
			{
				name:       "AllFieldsSet",
				prefix:     "prefix",
				workerFunc: func(s string) int { return 1 },
				workers:    1,
				valid:      true,
			},
			{
				name:   "WorkersMissing",
				prefix: "prefix",
				valid:  false,
			},
			{
				name:       "WorkerFunctions",
				prefix:     "prefix",
				workerFunc: func(s string) int { return 1 },
				valid:      true,
			},
			{
				name:    "WorkerDefault",
				prefix:  "prefix",
				workers: 2,
				valid:   true,
			},
		}
		for _, remoteTest := range remoteTests {
			t.Run(remoteTest.name, func(t *testing.T) {
				remoteTest := remoteTest
				t.Parallel()
				ctx, cancel := context.WithCancel(bctx)
				defer cancel()
				db, closer := GetTestDatabase(ctx, t)
				defer func() { require.NoError(t, closer()) }()

				opts := Options{
					WaitInterval:    time.Millisecond,
					Logger:          logger,
					CompleteRetries: 2,
				}

				groupOpts := GroupOptions{
					DefaultWorkers: remoteTest.workers,
					WorkerPoolSize: remoteTest.workerFunc,
					TTL:            time.Minute,
					PruneFrequency: time.Minute,
				}

				g, err := NewGroup(ctx, db, opts, groupOpts)
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
	t.Run("Integration", func(t *testing.T) {
		ctx, cancel := context.WithCancel(bctx)
		defer cancel()

		testutil.RunGroupIntegrationTest(ctx, t, testutil.GroupIntegrationCase{
			Name: "PostgreSQL",
			LocalConstructor: func(ctx context.Context) (amboy.Queue, error) {
				return queue.NewLocalLimitedSize(&queue.FixedSizeQueueOptions{
					Workers:  2,
					Capacity: 128,
					Logger:   logger,
				}), nil
			},
			Constructor: func(ctx context.Context, ttl time.Duration) (amboy.QueueGroup, testutil.TestCloser, error) {
				db, closer, err := MakeTestDatabase(ctx, "group_"+uuid.New().String()[0:4])
				if err != nil {
					return nil, nil, err
				}

				opts := Options{
					WaitInterval:    time.Millisecond,
					CompleteRetries: 2,
				}

				groupOpts := GroupOptions{
					DefaultWorkers: 4,
					TTL:            ttl,
					PruneFrequency: ttl,
				}

				g, err := NewGroup(ctx, db, opts, groupOpts)
				if err != nil {
					catcher := &erc.Collector{}
					catcher.Push(err)
					catcher.Check(closer)
					return nil, nil, catcher.Resolve()
				}

				return g, func(ctx context.Context) error { return closer() }, nil
			},
		})
	})
}

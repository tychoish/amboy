package pgq

import (
	"context"
	"testing"
	"time"

	"github.com/deciduosity/amboy"
	"github.com/deciduosity/amboy/queue"
	"github.com/deciduosity/amboy/queue/testutil"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestGroup(t *testing.T) {
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
					WaitInterval: time.Millisecond,
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
			Name:             "PostgreSQL",
			LocalConstructor: func(ctx context.Context) (amboy.Queue, error) { return queue.NewLocalLimitedSize(2, 128), nil },
			Constructor: func(ctx context.Context, ttl time.Duration) (amboy.QueueGroup, testutil.TestCloser, error) {
				db, closer, err := MakeTestDatabase(ctx, "group_"+uuid.New().String()[0:4])
				if err != nil {
					return nil, nil, err
				}

				opts := Options{
					WaitInterval: time.Millisecond,
				}

				groupOpts := GroupOptions{
					DefaultWorkers: 4,
					TTL:            ttl,
					PruneFrequency: ttl,
				}

				g, err := NewGroup(ctx, db, opts, groupOpts)
				if err != nil {
					closer()
					return nil, nil, err
				}

				return g, func(ctx context.Context) error { return closer() }, nil
			},
		})

	})

}

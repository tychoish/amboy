package queue

import (
	"context"
	"testing"
	"time"

	"github.com/cdr/amboy"
	"github.com/cdr/amboy/queue/testutil"
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
			SkipRateLimitedWorker:   true,
			Constructor: func(ctx context.Context, _ string, size int) (amboy.Queue, testutil.TestCloser, error) {
				q := NewAdaptiveOrderedLocalQueue(&FixedSizeQueueOptions{Workers: size, Capacity: defaultLocalQueueCapcity})
				return q, func(ctx context.Context) error {
					cctx, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()
					return q.Close(cctx)
				}, nil
			},
		},
		{
			Name:                "LocalOrdered",
			OrderedStartsBefore: false,
			OrderedSupported:    true,
			MinSize:             2,
			MaxSize:             8,
			Constructor: func(ctx context.Context, _ string, size int) (amboy.Queue, testutil.TestCloser, error) {
				q := NewLocalOrdered(size)
				return q, func(ctx context.Context) error {
					cctx, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()
					return q.Close(cctx)
				}, nil
			},
		},
		{
			Name:                  "Priority",
			SkipRateLimitedWorker: true,
			Constructor: func(ctx context.Context, _ string, size int) (amboy.Queue, testutil.TestCloser, error) {
				q := NewLocalPriorityQueue(&FixedSizeQueueOptions{Workers: size, Capacity: defaultLocalQueueCapcity})
				return q, func(ctx context.Context) error {
					cctx, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()
					return q.Close(cctx)
				}, nil
			},
		},
		{
			Name:                    "LimitedSize",
			WaitUntilSupported:      true,
			DispatchBeforeSupported: true,
			Constructor: func(ctx context.Context, _ string, size int) (amboy.Queue, testutil.TestCloser, error) {
				q := NewLocalLimitedSize(&FixedSizeQueueOptions{Workers: size, Capacity: 1024 * size})

				return q, func(ctx context.Context) error {
					cctx, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()
					return q.Close(cctx)
				}, nil
			},
		},
		{
			Name:                  "Shuffled",
			SingleWorker:          true,
			SkipRateLimitedWorker: true,
			Constructor: func(ctx context.Context, _ string, size int) (amboy.Queue, testutil.TestCloser, error) {
				q := NewShuffledLocal(&FixedSizeQueueOptions{Workers: size, Capacity: defaultLocalQueueCapcity})
				return q, func(ctx context.Context) error {
					cctx, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()
					return q.Close(cctx)
				}, nil
			},
		},
	}
}

func TestQueueSmoke(t *testing.T) {
	bctx, bcancel := context.WithCancel(context.Background())
	defer bcancel()

	t.Parallel()
	for test := range testutil.MergeQueueTestCases(bctx, DefaultQueueTestCases()) {
		testutil.RunSmokeTest(bctx, t, test)
	}
}

package queue

import (
	"context"
	"testing"

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

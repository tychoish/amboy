package amzsqs

import (
	"context"
	"testing"

	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/queue/testutil"
)

func TestQueueSmoke(t *testing.T) {
	bctx, bcancel := context.WithCancel(context.Background())
	defer bcancel()

	test := testutil.QueueTestCase{
		Name:    "SQSFifo",
		MaxSize: 4,
		Skip:    true,
		Constructor: func(ctx context.Context, _ string, size int) (amboy.Queue, testutil.TestCloser, error) {
			q, err := NewFifoQueue(&Options{Name: randomString(4), NumWorkers: size})
			closer := func(ctx context.Context) error { return nil }
			return q, closer, err
		},
	}

	testutil.RunSmokeTest(bctx, t, test)
}

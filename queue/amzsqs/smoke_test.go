package amzsqs

import (
	"context"
	"testing"

	"github.com/deciduosity/amboy"
	"github.com/deciduosity/amboy/queue/testutil"
)

func TestQueueSmoke(t *testing.T) {
	bctx, bcancel := context.WithCancel(context.Background())
	defer bcancel()

	test := testutil.QueueTestCase{
		Name:    "SQSFifo",
		MaxSize: 4,
		Skip:    true,
		Constructor: func(ctx context.Context, _ string, size int) (amboy.Queue, testutil.TestCloser, error) {
			q, err := NewFifoQueue(randomString(4), size)
			closer := func(ctx context.Context) error { return nil }
			return q, closer, err
		},
	}

	testutil.RunSmokeTest(bctx, t, test)
}

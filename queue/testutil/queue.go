package testutil

import (
	"context"

	"github.com/deciduosity/amboy"
)

type TestCloser func(context.Context) error

type QueueTestCase struct {
	Name                    string
	Constructor             func(context.Context, string, int) (amboy.Queue, TestCloser, error)
	MinSize                 int
	MaxSize                 int
	DisableParallelTests    bool
	SingleWorker            bool
	MultiSupported          bool
	OrderedSupported        bool
	OrderedStartsBefore     bool
	WaitUntilSupported      bool
	DispatchBeforeSupported bool
	SkipUnordered           bool
	IsRemote                bool
	Skip                    bool
}

type PoolTestCase struct {
	Name         string
	SetPool      func(amboy.Queue, int) error
	SkipRemote   bool
	SkipMulti    bool
	RateLimiting bool
	MinSize      int
	MaxSize      int
}

type SizeTestCase struct {
	Name string
	Size int
}

func MergeQueueTestCases(ctx context.Context, cases ...[]QueueTestCase) <-chan QueueTestCase {
	out := make(chan QueueTestCase)
	go func() {
		defer close(out)
		for _, group := range cases {
			for _, cs := range group {
				select {
				case <-ctx.Done():
					return
				case out <- cs:
				}
			}
		}
	}()
	return out
}

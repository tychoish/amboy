package testutil

import (
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/pool"
	"github.com/tychoish/grip"
)

func RandomID() string { return strings.Replace(uuid.New().String(), "-", "", -1) }

func DefaultSizeTestCases() []SizeTestCase {
	return []SizeTestCase{
		{Name: "One", Size: 1},
		{Name: "Two", Size: 2},
		{Name: "Four", Size: 4},
		{Name: "Eight", Size: 8},
		{Name: "Sixteen", Size: 16},
		{Name: "ThirtyTwo", Size: 32},
		{Name: "SixtyFour", Size: 64},
	}
}

func DefaultPoolTestCases() []PoolTestCase {
	return []PoolTestCase{
		{
			Name: "Default",
			SetPool: func(q amboy.Queue, size int) error {
				return q.SetRunner(pool.NewLocalWorkers(&pool.WorkerOptions{
					Logger:     grip.NewLogger(grip.Sender()),
					NumWorkers: size,
					Queue:      q,
				}))
			},
		},
		{
			Name:      "Single",
			SkipMulti: true,
			MinSize:   1,
			MaxSize:   1,
			SetPool: func(q amboy.Queue, _ int) error {
				runner := pool.NewSingle(grip.NewLogger(grip.Sender()))
				if err := runner.SetQueue(q); err != nil {
					return err
				}

				return q.SetRunner(runner)
			},
		},
		{
			Name:    "Abortable",
			MinSize: 4,
			SetPool: func(q amboy.Queue, size int) error {
				return q.SetRunner(pool.NewAbortablePool(&pool.WorkerOptions{
					Logger:     grip.NewLogger(grip.Sender()),
					NumWorkers: size,
					Queue:      q,
				}))

			},
		},
		{
			Name:         "RateLimitedSimple",
			MinSize:      4,
			MaxSize:      16,
			RateLimiting: true,
			SetPool: func(q amboy.Queue, size int) error {
				runner, err := pool.NewSimpleRateLimitedWorkers(10*time.Millisecond,
					&pool.WorkerOptions{
						Logger:     grip.NewLogger(grip.Sender()),
						NumWorkers: size,
						Queue:      q,
					})
				if err != nil {
					return nil
				}

				return q.SetRunner(runner)
			},
		},
		{
			Name:         "RateLimitedAverage",
			MinSize:      4,
			MaxSize:      16,
			RateLimiting: true,
			SkipMulti:    true,
			SkipRemote:   true,
			SetPool: func(q amboy.Queue, size int) error {
				runner, err := pool.NewMovingAverageRateLimitedWorkers(size*100, 10*time.Millisecond,
					&pool.WorkerOptions{
						Logger:     grip.NewLogger(grip.Sender()),
						NumWorkers: size,
						Queue:      q,
					})
				if err != nil {
					return nil
				}

				return q.SetRunner(runner)
			},
		},
	}
}

package pool

import (
	"context"
	"testing"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/stretchr/testify/assert"
	"github.com/tychoish/amboy"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/send"
)

func init() {
	sender := grip.Sender()
	lvl := send.LevelInfo{
		Threshold: level.Alert,
		Default:   level.Warning,
	}
	grip.Warning(sender.SetLevel(lvl))
}

type poolFactory func() amboy.Runner
type testCaseFunc func(*testing.T)

func makeTestQueue(pool amboy.Runner) amboy.Queue {
	return &QueueTester{
		pool:      pool,
		toProcess: make(chan amboy.Job),
		storage:   make(map[string]amboy.Job),
	}

}

func TestRunnerImplementations(t *testing.T) {
	logger := grip.NewLogger(grip.Sender())
	pools := map[string]func() amboy.Runner{
		"Local":  func() amboy.Runner { return &localWorkers{log: logger} },
		"Single": func() amboy.Runner { return &single{log: logger} },
		"Noop":   func() amboy.Runner { return &noopPool{} },
		"RateLimitedSimple": func() amboy.Runner {
			return &simpleRateLimited{
				size:     1,
				interval: time.Second,
				log:      logger,
			}
		},
		"RateLimitedAverage": func() amboy.Runner {
			return &ewmaRateLimiting{
				size:   1,
				period: time.Second,
				target: 5,
				ewma:   ewma.NewMovingAverage(),
				log:    logger,
			}
		},
		"Abortable": func() amboy.Runner {
			return &abortablePool{
				size: 1,
				jobs: make(map[string]context.CancelFunc),
				log:  logger,
			}
		},
	}
	cases := map[string]func(poolFactory) testCaseFunc{
		"NotStarted": func(factory poolFactory) testCaseFunc {
			return func(t *testing.T) {
				pool := factory()
				assert.False(t, pool.Started())
			}
		},
		"MutableQueue": func(factory poolFactory) testCaseFunc {
			return func(t *testing.T) {
				pool := factory()
				queue := makeTestQueue(pool)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				// it's an unconfigured runner without a queue, it should always error
				if err := pool.Start(ctx); err == nil {
					t.Error("expected error")
				}

				// this should start the queue
				if err := pool.SetQueue(queue); err != nil {
					t.Fatal(err)
				}

				// it's cool to start the runner
				if err := pool.Start(ctx); err != nil {
					t.Fatal(err)
				}

				// once the runner starts you can't add pools
				if err := pool.SetQueue(queue); err == nil {
					t.Error("expected error")
				}

				// subsequent calls to start should noop
				if err := pool.Start(ctx); err != nil {
					t.Fatal(err)
				}
			}
		},
		"CloseImpactsStateAsExpected": func(factory poolFactory) testCaseFunc {
			return func(t *testing.T) {
				pool := factory()
				queue := makeTestQueue(pool)
				assert.False(t, pool.Started())

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				assert.False(t, pool.Started())
				if err := pool.SetQueue(queue); err != nil {
					t.Fatal(err)
				}
				if err := pool.Start(ctx); err != nil {
					t.Fatal(err)
				}
				assert.True(t, pool.Started())

				assert.NotPanics(t, func() {
					pool.Close(ctx)
				})

				assert.False(t, pool.Started())
			}
		},
	}

	for poolName, factory := range pools {
		t.Run(poolName, func(t *testing.T) {
			for caseName, test := range cases {
				t.Run(poolName+caseName, test(factory))
			}
		})
	}
}

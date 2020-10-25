package testutil

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/deciduosity/amboy"
	"github.com/deciduosity/amboy/job"
	"github.com/stretchr/testify/require"
)

func RunSmokeTest(bctx context.Context, t *testing.T, test QueueTestCase) {
	if test.Skip {
		return
	}

	t.Run(test.Name, func(t *testing.T) {
		t.Run("Serialization", func(t *testing.T) {
			ctx, cancel := context.WithCancel(bctx)
			defer cancel()
			RunSerializationTest(ctx, t, test)
		})

		for _, runner := range DefaultPoolTestCases() {
			if test.IsRemote && runner.SkipRemote {
				continue
			}

			t.Run(runner.Name+"Pool", func(t *testing.T) {
				for _, size := range DefaultSizeTestCases() {
					if test.MaxSize > 0 && size.Size > test.MaxSize {
						continue
					}

					if runner.MinSize > 0 && runner.MinSize > size.Size {
						continue
					}

					if runner.MaxSize > 0 && runner.MaxSize < size.Size {
						continue
					}

					if size.Size > 8 && (runtime.GOOS == "windows" || runtime.GOOS == "darwin" || testing.Short()) {
						continue
					}

					t.Run(size.Name, func(t *testing.T) {
						if !test.SkipUnordered {
							t.Run("Unordered", func(t *testing.T) {
								UnorderedTest(bctx, t, test, runner, size)
							})
						}
						if test.OrderedSupported {
							t.Run("Ordered", func(t *testing.T) {
								OrderedTest(bctx, t, test, runner, size)
							})
						}
						if test.WaitUntilSupported {
							t.Run("WaitUntil", func(t *testing.T) {
								WaitUntilTest(bctx, t, test, runner, size)
							})
						}

						if test.DispatchBeforeSupported {
							t.Run("DispatchBefore", func(t *testing.T) {
								DispatchBeforeTest(bctx, t, test, runner, size)
							})
						}

						t.Run("OneExecution", func(t *testing.T) {
							OneExecutionTest(bctx, t, test, runner, size)
						})

						if test.SingleWorker && (!test.OrderedSupported || test.OrderedStartsBefore) && size.Size >= 4 && size.Size <= 32 {
							t.Run("ScopedLock", func(t *testing.T) {
								ScopedLockTest(bctx, t, test, runner, size)
							})
						}

						if test.IsRemote && test.MultiSupported && !runner.SkipMulti {
							t.Run("MultiExecution", func(t *testing.T) {
								MultiExecutionTest(bctx, t, test, runner, size)
							})

							if size.Size < 8 {
								t.Run("ManyQueues", func(t *testing.T) {
									ManyQueueTest(bctx, t, test, runner, size)
								})
							}
						}

						t.Run("SaveLockingCheck", func(t *testing.T) {
							if test.OrderedSupported && !test.OrderedStartsBefore {
								t.Skip("test does not support queues where queues don't accept work after dispatching")
							}
							ctx, cancel := context.WithCancel(bctx)
							defer cancel()
							name := RandomID()

							q, closer, err := test.Constructor(ctx, name, size.Size)
							require.NoError(t, err)
							defer func() { require.NoError(t, closer(ctx)) }()

							require.NoError(t, runner.SetPool(q, size.Size))
							require.NoError(t, err)
							j := amboy.Job(job.NewShellJob("sleep 300", ""))
							j.UpdateTimeInfo(amboy.JobTimeInfo{
								WaitUntil: time.Now().Add(4 * amboy.LockTimeout),
							})
							require.NoError(t, q.Start(ctx))
							require.NoError(t, q.Put(ctx, j))

							require.NoError(t, j.Lock(q.ID(), q.Info().LockTimeout))
							require.NoError(t, q.Save(ctx, j))

							if test.IsRemote {
								// this errors because you can't save if you've double-locked,
								// but only real remote drivers check locks.
								require.NoError(t, j.Lock(q.ID(), q.Info().LockTimeout))
								require.NoError(t, j.Lock(q.ID(), q.Info().LockTimeout))
								require.Error(t, q.Save(ctx, j))
							}

							for i := 0; i < 25; i++ {
								var ok bool
								j, ok = q.Get(ctx, j.ID())
								require.True(t, ok)
								require.NoError(t, j.Lock(q.ID(), q.Info().LockTimeout))
								require.NoError(t, q.Save(ctx, j))
							}

							j, ok := q.Get(ctx, j.ID())
							require.True(t, ok)

							require.NoError(t, j.Error())
							q.Complete(ctx, j)
							require.NoError(t, j.Error())
						})

					})
				}
			})
		}
	})
}

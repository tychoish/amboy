package testutil

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/deciduosity/amboy"
	"github.com/deciduosity/amboy/job"
	"github.com/deciduosity/grip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type GroupConstructor func(context.Context, time.Duration) (amboy.QueueGroup, TestCloser, error)

type QueueConstructor func(context.Context) (amboy.Queue, error)

type GroupConstructorCase struct {
	Name             string
	Valid            bool
	LocalConstructor func(context.Context) (amboy.Queue, error)
	TTL              time.Duration
	SkipRemote       bool
}

func DefaultGroupConstructorCases(localConstructor QueueConstructor) []GroupConstructorCase {
	return []GroupConstructorCase{
		{
			Name:             "NilNegativeTime",
			LocalConstructor: nil,
			Valid:            false,
			TTL:              -time.Minute,
			SkipRemote:       true,
		},
		{
			Name:             "NilZeroTime",
			LocalConstructor: nil,
			Valid:            false,
			TTL:              0,
			SkipRemote:       true,
		},
		{
			Name:             "NilPositiveTime",
			LocalConstructor: nil,
			Valid:            false,
			TTL:              time.Minute,
			SkipRemote:       true,
		},
		{
			Name:             "NegativeTime",
			LocalConstructor: localConstructor,
			Valid:            false,
			TTL:              -time.Minute,
		},
		{
			Name:             "ZeroTime",
			LocalConstructor: localConstructor,
			Valid:            true,
			TTL:              0,
		},
		{
			Name:             "PositiveTime",
			LocalConstructor: localConstructor,
			Valid:            true,
			TTL:              time.Minute,
		},
	}

}

type GroupIntegrationCase struct {
	Name             string
	Constructor      GroupConstructor
	LocalConstructor QueueConstructor
}

func RunGroupIntegrationTest(bctx context.Context, t *testing.T, group GroupIntegrationCase) {
	t.Run(group.Name, func(t *testing.T) {
		t.Run("Get", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(bctx, 20*time.Second)
			defer cancel()

			g, closer, err := group.Constructor(ctx, 0)
			require.NoError(t, err)
			defer func() { require.NoError(t, closer(ctx)) }()
			require.NotNil(t, g)
			defer g.Close(ctx)

			q1, err := g.Get(ctx, "one")
			require.NoError(t, err)
			require.NotNil(t, q1)
			require.True(t, q1.Info().Started)

			q2, err := g.Get(ctx, "two")
			require.NoError(t, err)
			require.NotNil(t, q2)
			require.True(t, q2.Info().Started)

			j1 := job.NewShellJob("true", "")
			j2 := job.NewShellJob("true", "")
			j3 := job.NewShellJob("true", "")

			// Add j1 to q1. Add j2 and j3 to q2.
			require.NoError(t, q1.Put(ctx, j1))
			require.NoError(t, q2.Put(ctx, j2))
			require.NoError(t, q2.Put(ctx, j3))

			amboy.WaitInterval(ctx, q1, 100*time.Millisecond)
			amboy.WaitInterval(ctx, q2, 100*time.Millisecond)

			resultsQ1 := []amboy.Job{}
			for result := range q1.Jobs(ctx) {
				resultsQ1 = append(resultsQ1, result)
			}
			resultsQ2 := []amboy.Job{}
			for result := range q2.Jobs(ctx) {
				resultsQ2 = append(resultsQ2, result)
			}

			require.True(t, assert.Len(t, resultsQ1, 1, "first") && assert.Len(t, resultsQ2, 2, "second"))

			// Try getting the queues again
			q1, err = g.Get(ctx, "one")
			require.NoError(t, err)
			require.NotNil(t, q1)

			q2, err = g.Get(ctx, "two")
			require.NoError(t, err)
			require.NotNil(t, q2)

			// The queues should be the same, i.e., contain the jobs we expect
			resultsQ1 = []amboy.Job{}
			for result := range q1.Jobs(ctx) {
				resultsQ1 = append(resultsQ1, result)
			}
			resultsQ2 = []amboy.Job{}
			for result := range q2.Jobs(ctx) {
				resultsQ2 = append(resultsQ2, result)
			}
			require.Len(t, resultsQ1, 1)
			require.Len(t, resultsQ2, 2)
		})
		t.Run("Put", func(t *testing.T) {
			ctx, cancel := context.WithCancel(bctx)
			defer cancel()

			g, closer, err := group.Constructor(ctx, 0)
			require.NoError(t, err)
			defer func() { require.NoError(t, closer(ctx)) }()
			require.NotNil(t, g)

			defer g.Close(ctx)

			q1, err := g.Get(ctx, "one")
			require.NoError(t, err)
			require.NotNil(t, q1)
			if !q1.Info().Started {
				require.NoError(t, q1.Start(ctx))
			}

			q2, err := group.LocalConstructor(ctx)
			require.NoError(t, err)
			require.Error(t, g.Put(ctx, "one", q2), "cannot add queue to existing index")
			if !q2.Info().Started {
				require.NoError(t, q2.Start(ctx))
			}

			q3, err := group.LocalConstructor(ctx)
			require.NoError(t, err)
			require.NoError(t, g.Put(ctx, "three", q3))
			if !q3.Info().Started {
				require.NoError(t, q3.Start(ctx))
			}

			q4, err := group.LocalConstructor(ctx)
			require.NoError(t, err)
			require.NoError(t, g.Put(ctx, "four", q4))
			if !q4.Info().Started {
				require.NoError(t, q4.Start(ctx))
			}

			j1 := job.NewShellJob("true", "")
			j2 := job.NewShellJob("true", "")
			j3 := job.NewShellJob("true", "")

			// Add j1 to q3. Add j2 and j3 to q4.
			require.NoError(t, q3.Put(ctx, j1))
			require.NoError(t, q4.Put(ctx, j2))
			require.NoError(t, q4.Put(ctx, j3))

			amboy.WaitInterval(ctx, q3, 10*time.Millisecond)
			amboy.WaitInterval(ctx, q4, 10*time.Millisecond)

			resultsQ3 := []amboy.Job{}
			for result := range q3.Jobs(ctx) {
				resultsQ3 = append(resultsQ3, result)
			}
			resultsQ4 := []amboy.Job{}
			for result := range q4.Jobs(ctx) {
				resultsQ4 = append(resultsQ4, result)
			}
			require.Len(t, resultsQ3, 1)
			require.Len(t, resultsQ4, 2)

			// Try getting the queues again
			q3, err = g.Get(ctx, "three")
			require.NoError(t, err)
			require.NotNil(t, q3)

			q4, err = g.Get(ctx, "four")
			require.NoError(t, err)
			require.NotNil(t, q4)

			// The queues should be the same, i.e., contain the jobs we expect
			resultsQ3 = []amboy.Job{}
			for result := range q3.Jobs(ctx) {
				resultsQ3 = append(resultsQ3, result)
			}
			resultsQ4 = []amboy.Job{}
			for result := range q4.Jobs(ctx) {
				resultsQ4 = append(resultsQ4, result)
			}
			require.Len(t, resultsQ3, 1)
			require.Len(t, resultsQ4, 2)
		})
		t.Run("Prune", func(t *testing.T) {
			if runtime.GOOS == "windows" && group.Name == "Mongo" {
				t.Skip("legacy implementation performs poorly on windows")
			}

			ctx, cancel := context.WithTimeout(bctx, 10*time.Second)
			defer cancel()

			g, closer, err := group.Constructor(ctx, (1*time.Second)+(500*time.Millisecond))
			require.NoError(t, err)
			defer func() { require.NoError(t, closer(ctx)) }()
			require.NotNil(t, g)
			defer g.Close(ctx)

			q1, err := g.Get(ctx, "five")
			require.NoError(t, err)
			require.NotNil(t, q1)

			q2, err := g.Get(ctx, "six")
			require.NoError(t, err)
			require.NotNil(t, q2)

			j1 := job.NewShellJob("true", "")
			j2 := job.NewShellJob("true", "")
			j3 := job.NewShellJob("true", "")

			// Add j1 to q1. Add j2 and j3 to q2.
			require.NoError(t, q1.Put(ctx, j1))
			require.NoError(t, q2.Put(ctx, j2))
			require.NoError(t, q2.Put(ctx, j3))
			require.Equal(t, 1, q1.Stats(ctx).Total)
			require.Equal(t, 2, q2.Stats(ctx).Total)

			amboy.WaitInterval(ctx, q2, 10*time.Millisecond)
			amboy.WaitInterval(ctx, q1, 10*time.Millisecond)

			require.Equal(t, 1, q1.Stats(ctx).Total)
			require.Equal(t, 2, q2.Stats(ctx).Total)

			// Queues should have completed work
			assert.True(t, q1.Stats(ctx).IsComplete())
			assert.True(t, q2.Stats(ctx).IsComplete())
			assert.Equal(t, 1, q1.Stats(ctx).Completed)
			assert.Equal(t, 2, q2.Stats(ctx).Completed)

			require.Equal(t, 2, g.Len())

			time.Sleep(3 * time.Second)
			require.NoError(t, g.Prune(ctx))

			require.Equal(t, 0, g.Len())
		})
		t.Run("PruneWithTTL", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(bctx, 40*time.Second)
			defer cancel()

			g, closer, err := group.Constructor(ctx, 3*time.Second)
			require.NoError(t, err)
			defer func() { require.NoError(t, closer(ctx)) }()
			require.NotNil(t, g)
			defer g.Close(ctx)

			q1, err := g.Get(ctx, "seven")
			require.NoError(t, err)
			require.NotNil(t, q1)

			q2, err := g.Get(ctx, "eight")
			require.NoError(t, err)
			require.NotNil(t, q2)

			j1 := job.NewShellJob("true", "")
			j2 := job.NewShellJob("true", "")
			j3 := job.NewShellJob("true", "")

			// Add j1 to q1. Add j2 and j3 to q2.
			require.NoError(t, q1.Put(ctx, j1))
			require.NoError(t, q2.Put(ctx, j2))
			require.NoError(t, q2.Put(ctx, j3))

			amboy.WaitInterval(ctx, q1, 100*time.Millisecond)
			amboy.WaitInterval(ctx, q2, 100*time.Millisecond)

			// Queues should have completed work
			assert.True(t, q1.Stats(ctx).IsComplete())
			assert.True(t, q2.Stats(ctx).IsComplete())
			assert.Equal(t, 1, q1.Stats(ctx).Completed)
			assert.Equal(t, 2, q2.Stats(ctx).Completed)

			require.Equal(t, 2, g.Len())

			// this is just a way for tests that
			// prune more quickly to avoid a long sleep.
			for i := 0; i < 30; i++ {
				time.Sleep(time.Second)

				if ctx.Err() != nil {
					grip.Info(ctx.Err())
					break
				}
				if g.Len() == 0 {
					break
				}
			}

			require.Equal(t, 0, g.Len())
		})
		t.Run("Close", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			g, closer, err := group.Constructor(ctx, 0)
			defer func() { require.NoError(t, closer(ctx)) }()
			require.NoError(t, err)
			require.NotNil(t, g)

			q1, err := g.Get(ctx, "nine")
			require.NoError(t, err)
			require.NotNil(t, q1)

			q2, err := g.Get(ctx, "ten")
			require.NoError(t, err)
			require.NotNil(t, q2)

			j1 := job.NewShellJob("true", "")
			j2 := job.NewShellJob("true", "")
			j3 := job.NewShellJob("true", "")

			// Add j1 to q1. Add j2 and j3 to q2.
			require.NoError(t, q1.Put(ctx, j1))
			require.NoError(t, q2.Put(ctx, j2))
			require.NoError(t, q2.Put(ctx, j3))

			amboy.WaitInterval(ctx, q1, 10*time.Millisecond)
			amboy.WaitInterval(ctx, q2, 10*time.Millisecond)

			require.NoError(t, g.Close(ctx))
		})
	})
}

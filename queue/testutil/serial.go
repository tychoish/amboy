package testutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/dependency"
	"github.com/tychoish/amboy/job"
	"github.com/stretchr/testify/require"
)

func RunSerializationTest(ctx context.Context, t *testing.T, test QueueTestCase) {
	if !test.OrderedStartsBefore && test.OrderedSupported {
		t.Skip("skipped ordered testing for test simplicity")
	}

	if !test.DisableParallelTests {
		t.Parallel()
	}

	t.Run("BasicRoundTrip", func(t *testing.T) {
		queue, closer, err := test.Constructor(ctx, RandomID(), 2)
		require.NoError(t, err)
		defer func() { require.NoError(t, closer(ctx)) }()
		require.NoError(t, queue.Start(ctx))

		require.NoError(t, err)
		j := job.NewShellJob("ls", "")
		j.UpdateTimeInfo(amboy.JobTimeInfo{
			Created: time.Now().UTC().Round(time.Millisecond),
		})
		id := j.ID()
		require.NoError(t, queue.Put(ctx, j))
		jrt, err := queue.Get(ctx, id)
		require.NoError(t, err)

		// TODO (Sean): More comprehensive equality checks. Pulling out
		// individual fields for now to avoid deep equality checks on time
		// structs.
		require.Equal(t, j.JobType, jrt.Type())
		require.True(t, j.TimeInfo().Created.Equal(jrt.TimeInfo().Created))
	})

	t.Run("WithMaxTime", func(t *testing.T) {
		queue, closer, err := test.Constructor(ctx, RandomID(), 2)
		require.NoError(t, err)
		defer func() { require.NoError(t, closer(ctx)) }()
		require.NoError(t, queue.Start(ctx))

		for _, dur := range []time.Duration{time.Second, time.Minute, time.Hour, 24 * time.Hour} {
			t.Run(dur.String(), func(t *testing.T) {
				require.NoError(t, err)
				j := job.NewShellJob("ls", "")
				j.UpdateTimeInfo(amboy.JobTimeInfo{
					Created: time.Now().UTC().Round(time.Millisecond),
					MaxTime: dur,
				})
				id := j.ID()
				require.NoError(t, queue.Put(ctx, j))
				jrt, err := queue.Get(ctx, id)
				require.NoError(t, err)

				// TODO (Sean): More comprehensive equality checks.
				// (See also BasicRoundTrip test)
				require.Equal(t, j.JobType, jrt.Type())
				require.True(t, j.TimeInfo().Created.Equal(jrt.TimeInfo().Created))
				require.Equal(t, j.TimeInfo().MaxTime, jrt.TimeInfo().MaxTime)
			})
		}
	})
	t.Run("WithError", func(t *testing.T) {
		queue, closer, err := test.Constructor(ctx, RandomID(), 2)
		require.NoError(t, err)
		defer func() { require.NoError(t, closer(ctx)) }()
		require.NoError(t, queue.Start(ctx))

		require.NoError(t, err)
		j := job.NewShellJob("ls", "")
		j.UpdateTimeInfo(amboy.JobTimeInfo{
			Created: time.Now().UTC().Round(time.Millisecond),
		})
		id := j.ID()
		require.NoError(t, queue.Put(ctx, j))
		j.AddError(errors.New("mock error"))
		require.NoError(t, queue.Save(ctx, j))

		jrt, err := queue.Get(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, j.Error(), jrt.Error())
		require.NotNil(t, jrt.Error())
		require.Equal(t, j.Error().Error(), jrt.Error().Error())
	})
	t.Run("WithDependencyEdges", func(t *testing.T) {
		queue, closer, err := test.Constructor(ctx, RandomID(), 2)
		require.NoError(t, err)
		defer func() { require.NoError(t, closer(ctx)) }()
		require.NoError(t, queue.Start(ctx))

		require.NoError(t, err)
		j := job.NewShellJob("ls", "")
		j.UpdateTimeInfo(amboy.JobTimeInfo{
			Created: time.Now().UTC().Round(time.Millisecond),
		})
		dep := dependency.NewCheckManager("foo")
		require.NoError(t, dep.AddEdge("foo"))
		require.NoError(t, dep.AddEdge("bar"))
		j.SetDependency(dep)

		id := j.ID()
		require.NoError(t, queue.Put(ctx, j))
		jrt, err := queue.Get(ctx, id)
		require.NoError(t, err)
		require.Equal(t, j.Dependency().Edges(), jrt.Dependency().Edges())
	})
	t.Run("WithScopes", func(t *testing.T) {
		queue, closer, err := test.Constructor(ctx, RandomID(), 2)
		require.NoError(t, err)
		defer func() { require.NoError(t, closer(ctx)) }()
		require.NoError(t, queue.Start(ctx))

		require.NoError(t, err)
		j := job.NewShellJob("ls", "")
		j.UpdateTimeInfo(amboy.JobTimeInfo{
			Created: time.Now().UTC().Round(time.Millisecond),
		})
		j.RequiredScopes = []string{"foo", "bar"}

		id := j.ID()
		require.NoError(t, queue.Put(ctx, j))

		jrt, err := queue.Get(ctx, id)
		require.NoError(t, err)
		require.Equal(t, j.Scopes(), jrt.Scopes())

		amboy.WaitInterval(ctx, queue, 10*time.Millisecond)

		jrt, err = queue.Get(ctx, id)
		require.NoError(t, err)
		require.NoError(t, jrt.Error())
	})
}

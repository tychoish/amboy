package queue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tychoish/amboy/queue/testutil"
)

func TestGroupCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue := NewLocalLimitedSize(&FixedSizeQueueOptions{Workers: 2, Capacity: 128})
	require.NotNil(t, queue)

	for _, impl := range []struct {
		name    string
		factory func() GroupCache
	}{
		{
			name:    "BaseMinute",
			factory: func() GroupCache { return NewGroupCache(time.Minute) },
		},
		{
			name:    "BaseZero",
			factory: func() GroupCache { return NewGroupCache(0) },
		},
		{
			name:    "BaseHour",
			factory: func() GroupCache { return NewGroupCache(time.Hour) },
		},
	} {
		t.Run(impl.name, func(t *testing.T) {
			for _, test := range []struct {
				name string
				test func(*testing.T, GroupCache)
			}{
				{
					name: "ValidateFixture",
					test: func(t *testing.T, cache GroupCache) {
						require.Len(t, cache.Names(), 0)
						require.Zero(t, cache.Len())
					},
				},
				{
					name: "GetNilCase",
					test: func(t *testing.T, cache GroupCache) {
						require.Nil(t, cache.Get("foo"))
					},
				},
				{
					name: "SetNilCase",
					test: func(t *testing.T, cache GroupCache) {
						if err := cache.Set("foo", nil, 0); err == nil {
							t.Fatal("expected error")
						}
					},
				},
				{
					name: "SetZero",
					test: func(t *testing.T, cache GroupCache) {
						if err := cache.Set("foo", queue, 0); err != nil {
							t.Fatal(err)
						}

						require.Len(t, cache.Names(), 1)
						require.Equal(t, 1, cache.Len())
					},
				},
				{
					name: "DoubleSet",
					test: func(t *testing.T, cache GroupCache) {
						if err := cache.Set("foo", queue, 0); err != nil {
							t.Fatal(err)
						}
						if err := cache.Set("foo", queue, 0); err == nil {
							t.Fatal("expected error")
						}
					},
				},
				{
					name: "RoundTrip",
					test: func(t *testing.T, cache GroupCache) {
						if err := cache.Set("foo", queue, 0); err != nil {
							t.Fatal(err)
						}
						require.Equal(t, queue, cache.Get("foo"))
					},
				},
				{
					name: "RemoveNonExistantQueue",
					test: func(t *testing.T, cache GroupCache) {
						if err := cache.Remove(ctx, "foo"); err != nil {
							t.Fatal(err)
						}
					},
				},
				{
					name: "RemoveSteadyQueue",
					test: func(t *testing.T, cache GroupCache) {
						if err := cache.Set("foo", queue, 0); err != nil {
							t.Fatal(err)
						}
						require.Equal(t, 1, cache.Len())
						if err := cache.Remove(ctx, "foo"); err != nil {
							t.Fatal(err)
						}
						require.Equal(t, 0, cache.Len())
					},
				},
				{
					name: "RemoveQueueWithWork",
					test: func(t *testing.T, cache GroupCache) {
						q := NewLocalLimitedSize(&FixedSizeQueueOptions{Workers: 1, Capacity: 128})
						if err := q.Start(ctx); err != nil {
							t.Fatal(err)
						}
						if err := q.Put(ctx, testutil.NewSleepJob(time.Minute)); err != nil {
							t.Fatal(err)
						}

						if err := cache.Set("foo", q, 1); err != nil {
							t.Fatal(err)
						}
						require.Equal(t, 1, cache.Len())
						if err := cache.Remove(ctx, "foo"); err == nil {
							t.Fatal("expected error")
						}
						require.Equal(t, 1, cache.Len())
					},
				},
				{
					name: "PruneNil",
					test: func(t *testing.T, cache GroupCache) {
						if err := cache.Prune(ctx); err != nil {
							t.Fatal(err)
						}
					},
				},
				{
					name: "PruneOne",
					test: func(t *testing.T, cache GroupCache) {
						if err := cache.Set("foo", queue, time.Millisecond); err != nil {
							t.Fatal(err)
						}
						time.Sleep(2 * time.Millisecond)
						if err := cache.Prune(ctx); err != nil {
							t.Fatal(err)
						}
						require.Zero(t, cache.Len())
					},
				},
				{
					name: "PruneWithCanceledContext",
					test: func(t *testing.T, cache GroupCache) {
						tctx, cancel := context.WithCancel(ctx)
						cancel()

						if err := cache.Set("foo", queue, time.Hour); err != nil {
							t.Fatal(err)
						}
						require.Equal(t, 1, cache.Len())
						if err := cache.Prune(tctx); err == nil {
							t.Fatal("expected error")
						}
						require.Equal(t, 1, cache.Len())
					},
				},
				{
					name: "PruneWithUnexpiredTTL",
					test: func(t *testing.T, cache GroupCache) {
						if err := cache.Set("foo", queue, time.Hour); err != nil {
							t.Fatal(err)
						}
						require.Equal(t, 1, cache.Len())
						if err := cache.Prune(ctx); err != nil {
							t.Fatal(err)
						}
						require.Equal(t, 1, cache.Len())
					},
				},
				{
					name: "CloseNoop",
					test: func(t *testing.T, cache GroupCache) {
						if err := cache.Close(ctx); err != nil {
							t.Fatal(err)
						}
					},
				},
				{
					name: "CloseErrorsCtxCancel",
					test: func(t *testing.T, cache GroupCache) {
						tctx, cancel := context.WithCancel(ctx)
						cancel()

						if err := cache.Set("foo", queue, time.Hour); err != nil {
							t.Fatal(err)
						}
						require.Equal(t, 1, cache.Len())
						if err := cache.Close(tctx); err == nil {
							t.Fatal("expected error")
						}
						require.Equal(t, 1, cache.Len())
					},
				},
				{
					name: "CloseEmptyErrorsCtxCancel",
					test: func(t *testing.T, cache GroupCache) {
						tctx, cancel := context.WithCancel(ctx)
						cancel()

						if err := cache.Close(tctx); err == nil {
							t.Fatal("expected error")
						}
					},
				},
				{
					name: "ClosingClearsQueue",
					test: func(t *testing.T, cache GroupCache) {
						if err := cache.Set("foo", queue, time.Hour); err != nil {
							t.Fatal(err)
						}
						require.Equal(t, 1, cache.Len())
						if err := cache.Close(ctx); err != nil {
							t.Fatal(err)
						}
						require.Equal(t, 0, cache.Len())

					},
				},
				// {
				//	name: "",
				//	test: func(t *testing.T, cache GroupCache) {
				//	},
				// },
				// {
				//	name: "",
				//	test: func(t *testing.T, cache GroupCache) {
				//	},
				// },
			} {
				t.Run(test.name, func(t *testing.T) {
					cache := impl.factory()
					require.NotNil(t, cache)

					test.test(t, cache)
				})
			}
		})
	}

}

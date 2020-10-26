package queue

import (
	"context"
	"testing"
	"time"

	"github.com/deciduosity/amboy"
	"github.com/deciduosity/amboy/queue/testutil"
	"github.com/stretchr/testify/require"
)

func localConstructor(ctx context.Context) (amboy.Queue, error) {
	return NewLocalLimitedSize(2, 128), nil
}

func TestQueueGroup(t *testing.T) {
	bctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()
	t.Run("Constructor", func(t *testing.T) {
		t.Parallel()
		for _, test := range testutil.DefaultGroupConstructorCases(localConstructor) {
			t.Run(test.Name, func(t *testing.T) {
				t.Run("Local", func(t *testing.T) {
					ctx, cancel := context.WithCancel(bctx)
					defer cancel()

					localOpts := LocalQueueGroupOptions{
						Constructor: test.LocalConstructor,
						TTL:         test.TTL,
					}
					g, err := NewLocalQueueGroup(ctx, localOpts) // nolint
					if test.Valid {
						require.NotNil(t, g)
						require.NoError(t, err)
					} else {
						require.Nil(t, g)
						require.Error(t, err)
					}
				})
			})
		}
	})
	t.Run("Integration", func(t *testing.T) {
		for _, group := range []testutil.GroupIntegrationCase{
			{
				Name:             "Local",
				LocalConstructor: localConstructor,
				Constructor: func(ctx context.Context, ttl time.Duration) (amboy.QueueGroup, testutil.TestCloser, error) {
					qg, err := NewLocalQueueGroup(ctx, LocalQueueGroupOptions{Constructor: localConstructor, TTL: ttl})
					closer := func(_ context.Context) error { return nil }
					return qg, closer, err
				},
			},
		} {
			testutil.RunGroupIntegrationTest(bctx, t, group)
		}

	})
}

package pgq

import (
	"context"
	"testing"

	"github.com/deciduosity/amboy/management"
	"github.com/deciduosity/amboy/queue/testutil"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestManager(t *testing.T) {
	t.Parallel()
	bctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("Single", func(t *testing.T) {
		s := new(testutil.ManagerSuite)

		ctx, cancel := context.WithCancel(bctx)
		defer cancel()

		var (
			db     *sqlx.DB
			closer func() error
			opts   = ManagerOptions{}
		)

		s.Factory = func() management.Manager {
			return NewManager(db, opts)
		}

		opts.Options.UseGroups = false

		s.Setup = func() {
			db, closer = GetTestDatabase(ctx, t)
			var err error
			s.Queue, err = NewQueue(db, opts.Options)
			require.NoError(t, err)
		}

		s.Cleanup = func() error {
			s.Queue.Runner().Close(ctx)
			return closer()
		}

		suite.Run(t, s)
	})
	t.Run("SingleGroup", func(t *testing.T) {
		s := new(testutil.ManagerSuite)

		ctx, cancel := context.WithCancel(bctx)
		defer cancel()

		var (
			db     *sqlx.DB
			closer func() error
			opts   = ManagerOptions{}
		)

		s.Factory = func() management.Manager {
			return NewManager(db, opts)
		}

		opts.Options.UseGroups = true
		opts.Options.GroupName = "foo"
		opts.SingleGroup = true

		s.Setup = func() {
			db, closer = GetTestDatabase(ctx, t)
			var err error
			s.Queue, err = NewQueue(db, opts.Options)
			require.NoError(t, err)
		}

		s.Cleanup = func() error {
			s.Queue.Runner().Close(ctx)
			return closer()
		}

		suite.Run(t, s)
	})
	t.Run("ByGroup", func(t *testing.T) {
		s := new(testutil.ManagerSuite)

		ctx, cancel := context.WithCancel(bctx)
		defer cancel()

		var (
			db     *sqlx.DB
			closer func() error
			opts   = ManagerOptions{}
		)

		s.Factory = func() management.Manager {
			return NewManager(db, opts)
		}

		opts.Options.UseGroups = true
		opts.Options.GroupName = "foo"
		opts.ByGroups = true

		s.Setup = func() {
			db, closer = GetTestDatabase(ctx, t)
			var err error
			s.Queue, err = NewQueue(db, opts.Options)
			require.NoError(t, err)
		}

		s.Cleanup = func() error {
			s.Queue.Runner().Close(ctx)
			return closer()
		}

		suite.Run(t, s)
	})
}

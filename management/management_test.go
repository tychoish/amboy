package management

import (
	"context"
	"testing"

	"github.com/deciduosity/amboy/job"
	"github.com/deciduosity/amboy/queue"
	"github.com/stretchr/testify/suite"
)

func init() {
	job.RegisterDefaultJobs()
}

func TestManagerSuiteBackedByQueueMethods(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := new(ManagerSuite)
	s.setup = func() {
		s.queue = queue.NewLocalLimitedSize(2, 128)
		s.Require().NoError(s.queue.Start(ctx))
	}

	s.factory = func() Manager {
		return NewQueueManager(s.queue)
	}

	s.cleanup = func() error {
		return nil
	}
	suite.Run(t, s)
}

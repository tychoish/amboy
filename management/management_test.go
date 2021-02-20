package management

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tychoish/amboy/job"
	"github.com/tychoish/amboy/queue"
)

func init() {
	job.RegisterDefaultJobs()
}

func TestManagerSuiteBackedByQueueMethods(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := new(ManagerSuite)
	s.Setup = func() {
		s.Queue = queue.NewLocalLimitedSize(&queue.FixedSizeQueueOptions{Workers: 2, Capacity: 128})
		s.Require().NoError(s.Queue.Start(ctx))
	}

	s.Factory = func() Manager {
		return NewQueueManager(s.Queue)
	}

	s.Cleanup = func() error {
		return nil
	}
	suite.Run(t, s)
}

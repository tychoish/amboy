package testutil

import (
	"context"
	"time"

	"github.com/deciduosity/amboy"
	"github.com/deciduosity/amboy/dependency"
	"github.com/deciduosity/amboy/job"
	"github.com/deciduosity/amboy/management"
	"github.com/deciduosity/amboy/registry"
	"github.com/stretchr/testify/suite"
)

func init() {
	registry.AddJobType("test", func() amboy.Job { return makeTestJob() })
}

type ManagerSuite struct {
	Queue   amboy.Queue
	Manager management.Manager
	Factory func() management.Manager
	Setup   func()
	Cleanup func() error

	ctx    context.Context
	cancel context.CancelFunc

	suite.Suite
}

func (s *ManagerSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.Setup()
	s.Manager = s.Factory()
}

func (s *ManagerSuite) TearDownTest() {
	s.cancel()
	s.NoError(s.Cleanup())
}

func (s *ManagerSuite) TestJobStatusInvalidFilter() {
	for _, f := range []string{"", "foo", "inprog"} {
		r, err := s.Manager.JobStatus(s.ctx, management.StatusFilter(f))
		s.Error(err)
		s.Nil(r)

		rr, err := s.Manager.JobIDsByState(s.ctx, "foo", management.StatusFilter(f))
		s.Error(err)
		s.Nil(rr)
	}
}

func (s *ManagerSuite) TestTimingWithInvalidFilter() {
	for _, f := range []string{"", "foo", "inprog"} {
		r, err := s.Manager.RecentTiming(s.ctx, time.Hour, management.RuntimeFilter(f))
		s.Error(err)
		s.Nil(r)
	}
}

func (s *ManagerSuite) TestErrorsWithInvalidFilter() {
	for _, f := range []string{"", "foo", "inprog"} {
		r, err := s.Manager.RecentJobErrors(s.ctx, "foo", time.Hour, management.ErrorFilter(f))
		s.Error(err)
		s.Nil(r)

		r, err = s.Manager.RecentErrors(s.ctx, time.Hour, management.ErrorFilter(f))
		s.Error(err)
		s.Nil(r)
	}
}

func (s *ManagerSuite) TestJobCounterHighLevel() {
	for _, f := range []management.StatusFilter{management.InProgress, management.Pending, management.Stale} {
		r, err := s.Manager.JobStatus(s.ctx, f)
		s.NoError(err)
		s.NotNil(r)
	}

}

func (s *ManagerSuite) TestJobCountingIDHighLevel() {
	for _, f := range []management.StatusFilter{management.InProgress, management.Pending, management.Stale, management.Completed} {
		r, err := s.Manager.JobIDsByState(s.ctx, "foo", f)
		s.NoError(err, "%s", f)
		s.NotNil(r)
	}
}

func (s *ManagerSuite) TestJobTimingMustBeLongerThanASecond() {
	for _, dur := range []time.Duration{-1, 0, time.Millisecond, -time.Hour} {
		r, err := s.Manager.RecentTiming(s.ctx, dur, management.Duration)
		s.Error(err)
		s.Nil(r)
		je, err := s.Manager.RecentJobErrors(s.ctx, "foo", dur, management.StatsOnly)
		s.Error(err)
		s.Nil(je)

		je, err = s.Manager.RecentErrors(s.ctx, dur, management.StatsOnly)
		s.Error(err)
		s.Nil(je)

	}
}

func (s *ManagerSuite) TestJobTiming() {
	for _, f := range []management.RuntimeFilter{management.Duration, management.Latency, management.Running} {
		r, err := s.Manager.RecentTiming(s.ctx, time.Minute, f)
		s.NoError(err)
		s.NotNil(r)
	}
}

func (s *ManagerSuite) TestRecentErrors() {
	for _, f := range []management.ErrorFilter{management.UniqueErrors, management.AllErrors, management.StatsOnly} {
		r, err := s.Manager.RecentErrors(s.ctx, time.Minute, f)
		s.NoError(err)
		s.NotNil(r)
	}
}

func (s *ManagerSuite) TestRecentJobErrors() {
	for _, f := range []management.ErrorFilter{management.UniqueErrors, management.AllErrors, management.StatsOnly} {
		r, err := s.Manager.RecentJobErrors(s.ctx, "shell", time.Minute, f)
		s.NoError(err)
		s.NotNil(r)
	}
}

func (s *ManagerSuite) TestCompleteJob() {
	j1 := job.NewShellJob("ls", "")
	s.Require().NoError(s.Queue.Put(s.ctx, j1))
	j2 := newTestJob("complete")
	s.Require().NoError(s.Queue.Put(s.ctx, j2))
	j3 := newTestJob("uncomplete")
	s.Require().NoError(s.Queue.Put(s.ctx, j3))

	s.Require().NoError(s.Manager.CompleteJob(s.ctx, "complete"))
	jobCount := 0
	for job := range s.Queue.Jobs(s.ctx) {
		jobStats := job.Status()

		if job.ID() == "complete" {
			s.True(jobStats.Completed)
			if jobStats.ModificationCount != 0 {
				s.Equal(3, jobStats.ModificationCount)
			}
		} else {
			s.False(jobStats.Completed)
			s.Equal(0, jobStats.ModificationCount)
		}
		jobCount++
	}
	s.Equal(3, jobCount)
}

func (s *ManagerSuite) TestCompleteJobsInvalidFilter() {
	s.Error(s.Manager.CompleteJobs(s.ctx, "invalid"))
	s.Error(s.Manager.CompleteJobs(s.ctx, management.Completed))
}

func (s *ManagerSuite) TestCompleteJobsValidFilter() {
	j1 := job.NewShellJob("ls", "")
	s.Require().NoError(s.Queue.Put(s.ctx, j1))
	j2 := newTestJob("0")
	s.Require().NoError(s.Queue.Put(s.ctx, j2))
	j3 := newTestJob("1")
	s.Require().NoError(s.Queue.Put(s.ctx, j3))

	s.Require().NoError(s.Manager.CompleteJobs(s.ctx, management.Pending))
	jobCount := 0
	for job := range s.Queue.Jobs(s.ctx) {
		jobStats := job.Status()

		s.True(jobStats.Completed, "id='%s' status='%+v'", job.ID(), jobStats)
		if jobStats.ModificationCount != 0 {
			s.Equal(3, jobStats.ModificationCount)
		}
		jobCount++
	}
	s.Equal(3, jobCount)
}

func (s *ManagerSuite) TestCompleteJobsByTypeInvalidFilter() {
	s.Error(s.Manager.CompleteJobsByType(s.ctx, "invalid", "type"))
	s.Error(s.Manager.CompleteJobsByType(s.ctx, management.Completed, "type"))
}

func (s *ManagerSuite) TestCompleteJobsByTypeValidFilter() {
	j1 := job.NewShellJob("ls", "")
	s.Require().NoError(s.Queue.Put(s.ctx, j1))
	j2 := newTestJob("0")
	s.Require().NoError(s.Queue.Put(s.ctx, j2))
	j3 := newTestJob("1")
	s.Require().NoError(s.Queue.Put(s.ctx, j3))

	s.Require().NoError(s.Manager.CompleteJobsByType(s.ctx, management.Pending, "test"))
	jobCount := 0
	for job := range s.Queue.Jobs(s.ctx) {
		jobStats := job.Status()
		if job.ID() == "0" || job.ID() == "1" {
			s.True(jobStats.Completed)
			if jobStats.ModificationCount != 0 {
				s.Equal(3, jobStats.ModificationCount)
			}
		} else {
			s.False(jobStats.Completed)
			s.Equal(0, jobStats.ModificationCount)
		}
		jobCount++
	}
	s.Equal(3, jobCount)
}

type testJob struct {
	job.Base
}

func newTestJob(id string) *testJob {
	j := makeTestJob()
	j.Base.TaskID = id
	j.SetDependency(dependency.NewAlways())

	return j
}

func makeTestJob() *testJob {
	j := &testJob{
		Base: job.Base{
			JobType: amboy.JobType{Name: "test"},
		},
	}
	j.SetDependency(dependency.NewAlways())
	return j
}

func (j *testJob) Run(ctx context.Context) {
	time.Sleep(time.Minute)
	return
}

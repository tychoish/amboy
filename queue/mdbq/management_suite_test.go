package mdbq

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

// NOTE: this file is duplicated from
// amboy/management/management_suite_test

func init() {
	registry.AddJobType("test", func() amboy.Job { return makeTestJob() })
}

type managerSuite struct {
	queue   amboy.Queue
	manager management.Manager
	ctx     context.Context
	cancel  context.CancelFunc

	factory func() management.Manager
	setup   func()
	cleanup func() error
	suite.Suite
}

func (s *managerSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.setup()
	s.manager = s.factory()
}

func (s *managerSuite) TearDownTest() {
	s.cancel()
}

func (s *managerSuite) TearDownSuite() {
	s.NoError(s.cleanup())
}

func (s *managerSuite) TestJobStatusInvalidFilter() {
	for _, f := range []string{"", "foo", "inprog"} {
		r, err := s.manager.JobStatus(s.ctx, management.StatusFilter(f))
		s.Error(err)
		s.Nil(r)

		rr, err := s.manager.JobIDsByState(s.ctx, "foo", management.StatusFilter(f))
		s.Error(err)
		s.Nil(rr)
	}
}

func (s *managerSuite) TestTimingWithInvalidFilter() {
	for _, f := range []string{"", "foo", "inprog"} {
		r, err := s.manager.RecentTiming(s.ctx, time.Hour, management.RuntimeFilter(f))
		s.Error(err)
		s.Nil(r)
	}
}

func (s *managerSuite) TestErrorsWithInvalidFilter() {
	for _, f := range []string{"", "foo", "inprog"} {
		r, err := s.manager.RecentJobErrors(s.ctx, "foo", time.Hour, management.ErrorFilter(f))
		s.Error(err)
		s.Nil(r)

		r, err = s.manager.RecentErrors(s.ctx, time.Hour, management.ErrorFilter(f))
		s.Error(err)
		s.Nil(r)
	}
}

func (s *managerSuite) TestJobCounterHighLevel() {
	for _, f := range []management.StatusFilter{management.InProgress, management.Pending, management.Stale} {
		r, err := s.manager.JobStatus(s.ctx, f)
		s.NoError(err)
		s.NotNil(r)
	}

}

func (s *managerSuite) TestJobCountingIDHighLevel() {
	for _, f := range []management.StatusFilter{management.InProgress, management.Pending, management.Stale, management.Completed} {
		r, err := s.manager.JobIDsByState(s.ctx, "foo", f)
		s.NoError(err, "%s", f)
		s.NotNil(r)
	}
}

func (s *managerSuite) TestJobTimingMustBeLongerThanASecond() {
	for _, dur := range []time.Duration{-1, 0, time.Millisecond, -time.Hour} {
		r, err := s.manager.RecentTiming(s.ctx, dur, management.Duration)
		s.Error(err)
		s.Nil(r)
		je, err := s.manager.RecentJobErrors(s.ctx, "foo", dur, management.StatsOnly)
		s.Error(err)
		s.Nil(je)

		je, err = s.manager.RecentErrors(s.ctx, dur, management.StatsOnly)
		s.Error(err)
		s.Nil(je)

	}
}

func (s *managerSuite) TestJobTiming() {
	for _, f := range []management.RuntimeFilter{management.Duration, management.Latency, management.Running} {
		r, err := s.manager.RecentTiming(s.ctx, time.Minute, f)
		s.NoError(err)
		s.NotNil(r)
	}
}

func (s *managerSuite) TestRecentErrors() {
	for _, f := range []management.ErrorFilter{management.UniqueErrors, management.AllErrors, management.StatsOnly} {
		r, err := s.manager.RecentErrors(s.ctx, time.Minute, f)
		s.NoError(err)
		s.NotNil(r)
	}
}

func (s *managerSuite) TestRecentJobErrors() {
	for _, f := range []management.ErrorFilter{management.UniqueErrors, management.AllErrors, management.StatsOnly} {
		r, err := s.manager.RecentJobErrors(s.ctx, "shell", time.Minute, f)
		s.NoError(err)
		s.NotNil(r)
	}
}

func (s *managerSuite) TestCompleteJob() {
	j1 := job.NewShellJob("ls", "")
	s.Require().NoError(s.queue.Put(s.ctx, j1))
	j2 := newTestJob("complete")
	s.Require().NoError(s.queue.Put(s.ctx, j2))
	j3 := newTestJob("uncomplete")
	s.Require().NoError(s.queue.Put(s.ctx, j3))

	s.Require().NoError(s.manager.CompleteJob(s.ctx, "complete"))
	jobCount := 0
	for job := range s.queue.Jobs(s.ctx) {
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

func (s *managerSuite) TestCompleteJobsInvalidFilter() {
	s.Error(s.manager.CompleteJobs(s.ctx, "invalid"))
	s.Error(s.manager.CompleteJobs(s.ctx, management.Completed))
}

func (s *managerSuite) TestCompleteJobsValidFilter() {
	j1 := job.NewShellJob("ls", "")
	s.Require().NoError(s.queue.Put(s.ctx, j1))
	j2 := newTestJob("0")
	s.Require().NoError(s.queue.Put(s.ctx, j2))
	j3 := newTestJob("1")
	s.Require().NoError(s.queue.Put(s.ctx, j3))

	s.Require().NoError(s.manager.CompleteJobs(s.ctx, management.Pending))
	jobCount := 0
	for job := range s.queue.Jobs(s.ctx) {
		jobStats := job.Status()

		s.True(jobStats.Completed, "id='%s' status='%+v'", job.ID(), jobStats)
		if jobStats.ModificationCount != 0 {
			s.Equal(3, jobStats.ModificationCount)
		}
		jobCount++
	}
	s.Equal(3, jobCount)
}

func (s *managerSuite) TestCompleteJobsByTypeInvalidFilter() {
	s.Error(s.manager.CompleteJobsByType(s.ctx, "invalid", "type"))
	s.Error(s.manager.CompleteJobsByType(s.ctx, management.Completed, "type"))
}

func (s *managerSuite) TestCompleteJobsByTypeValidFilter() {
	j1 := job.NewShellJob("ls", "")
	s.Require().NoError(s.queue.Put(s.ctx, j1))
	j2 := newTestJob("0")
	s.Require().NoError(s.queue.Put(s.ctx, j2))
	j3 := newTestJob("1")
	s.Require().NoError(s.queue.Put(s.ctx, j3))

	s.Require().NoError(s.manager.CompleteJobsByType(s.ctx, management.Pending, "test"))
	jobCount := 0
	for job := range s.queue.Jobs(s.ctx) {
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

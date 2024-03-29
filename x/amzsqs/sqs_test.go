package amzsqs

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/job"
	"github.com/tychoish/amboy/pool"
	"github.com/tychoish/amboy/queue/testutil"
	"github.com/tychoish/grip"
)

type SQSFifoQueueSuite struct {
	queue amboy.Queue
	suite.Suite
	jobID string
}

func TestSQSFifoQueueSuite(t *testing.T) {
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		t.Skip("skipping SQS tests on non-core platforms")
	}
	suite.Run(t, new(SQSFifoQueueSuite))
}

func (s *SQSFifoQueueSuite) SetupTest() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := grip.NewLogger(grip.Sender())

	var err error
	s.queue, err = NewFifoQueue(&Options{Name: randomString(4), NumWorkers: 4, Logger: logger})
	s.NoError(err)
	r := pool.NewSingle(logger)
	s.NoError(r.SetQueue(s.queue))
	s.NoError(s.queue.SetRunner(r))
	s.Equal(r, s.queue.Runner())
	s.NoError(s.queue.Start(ctx))

	stats := s.queue.Stats(ctx)
	s.Equal(0, stats.Total)
	s.Equal(0, stats.Running)
	s.Equal(0, stats.Pending)
	s.Equal(0, stats.Completed)

	j := job.NewShellJob("echo true", "")
	s.jobID = j.ID()
	s.NoError(s.queue.Put(ctx, j))
}

func (s *SQSFifoQueueSuite) TestPutMethodErrorsForDuplicateJobs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	job, err := s.queue.Get(ctx, s.jobID)
	s.NoError(err)
	s.Error(s.queue.Put(ctx, job))
}

func (s *SQSFifoQueueSuite) TestGetMethodReturnsRequestedJob() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	job, err := s.queue.Get(ctx, s.jobID)
	s.NoError(err)
	s.NotNil(job)
	s.Equal(s.jobID, job.ID())
}

func (s *SQSFifoQueueSuite) TestCannotSetRunnerWhenQueueStarted() {
	s.True(s.queue.Info().Started)

	s.Error(s.queue.SetRunner(pool.NewSingle(grip.NewLogger(grip.Sender()))))
}

func (s *SQSFifoQueueSuite) TestCompleteMethodChangesStatsAndResults() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	j := job.NewShellJob("echo true", "")
	s.NoError(s.queue.Put(ctx, j))
	s.NoError(s.queue.Complete(context.Background(), j))

	counter := 0
	results := s.queue.Jobs(context.Background())
	for job := range results {
		s.Require().NotNil(job)
		s.Equal(j.ID(), job.ID())
		counter++
	}
	stats := s.queue.Stats(ctx)
	s.Equal(1, stats.Completed)
	s.Equal(1, counter)
}

func TestSQSFifoQueueRunsJobsOnlyOnce(t *testing.T) {
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		t.Skip("skipping SQS tests on non-core platforms")
	}

	testID := testutil.RandomID()
	counter := testutil.GetCounterCache().Get(testID)
	counter.Reset()

	assert := assert.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	q, err := NewFifoQueue(&Options{Name: randomString(8), NumWorkers: 4, Logger: grip.NewLogger(grip.Sender())})
	assert.NoError(err)
	assert.NoError(q.Start(ctx))
	wg := &sync.WaitGroup{}

	const (
		inside  = 250
		outside = 2
	)

	wg.Add(outside)
	for i := 0; i < outside; i++ {
		go func(i int) {
			defer wg.Done()
			for ii := 0; ii < inside; ii++ {
				j := testutil.MakeMockJob(fmt.Sprintf("%d-%d-%d", i, ii, job.GetNumber()), testID)
				assert.NoError(q.Put(ctx, j))
			}
		}(i)
	}

	grip.Notice("waiting to add all jobs")
	wg.Wait()

	amboy.WaitInterval(ctx, q, 20*time.Second)
	stats := q.Stats(ctx)
	assert.True(stats.Total <= inside*outside)
	assert.Equal(stats.Total, stats.Pending+stats.Completed)
}

func TestMultipleSQSFifoQueueRunsJobsOnlyOnce(t *testing.T) {
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		t.Skip("skipping SQS tests on non-core platforms")
	}

	testID := testutil.RandomID()

	// case two
	counter := testutil.GetCounterCache().Get(testID)
	counter.Reset()

	assert := assert.New(t) // nolint
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)

	defer cancel()
	name := randomString(8)
	q, err := NewFifoQueue(&Options{Name: name, NumWorkers: 4, Logger: grip.NewLogger(grip.Sender())})
	assert.NoError(err)
	assert.NoError(q.Start(ctx))

	q2, err := NewFifoQueue(&Options{Name: name, NumWorkers: 4, Logger: grip.NewLogger(grip.Sender())})
	assert.NoError(err)
	assert.NoError(q2.Start(ctx))

	const (
		inside  = 250
		outside = 3
	)

	wg := &sync.WaitGroup{}
	wg.Add(outside)
	for i := 0; i < outside; i++ {
		go func(i int) {
			defer wg.Done()
			for ii := 0; ii < inside; ii++ {
				j := testutil.MakeMockJob(fmt.Sprintf("%d-%d-%d", i, ii, job.GetNumber()), testID)
				assert.NoError(q2.Put(ctx, j))
			}
		}(i)
	}
	grip.Notice("waiting to add all jobs")
	wg.Wait()

	grip.Notice("waiting to run jobs")

	amboy.WaitInterval(ctx, q, 10*time.Second)
	amboy.WaitInterval(ctx, q2, 10*time.Second)

	stats1 := q.Stats(ctx)
	stats2 := q2.Stats(ctx)
	assert.Equal(stats1.Pending, stats2.Pending)

	sum := stats1.Pending + stats2.Pending + stats1.Completed + stats2.Completed
	assert.Equal(sum, stats1.Total+stats2.Total)
}

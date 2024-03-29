package mdbq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/job"
	"github.com/tychoish/amboy/pool"
	"github.com/tychoish/amboy/queue/testutil"
	"github.com/tychoish/grip"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func init() {
	job.RegisterDefaultJobs()
}

type RemoteUnorderedSuite struct {
	queue             *remoteUnordered
	driver            remoteQueueDriver
	driverConstructor func() remoteQueueDriver
	tearDown          func()
	require           *require.Assertions
	canceler          context.CancelFunc
	suite.Suite
}

func TestRemoteUnorderedMongoSuite(t *testing.T) {
	tests := new(RemoteUnorderedSuite)
	name := "test-" + uuid.New().String()
	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017").SetConnectTimeout(time.Second))
	require.NoError(t, err)

	err = client.Connect(ctx)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, client.Disconnect(ctx))
	}()

	require.NoError(t, client.Database(opts.DB).Drop(ctx))

	tests.driverConstructor = func() remoteQueueDriver {
		d, err := openNewMongoDriver(ctx, name, opts, client)
		require.NoError(t, err)
		return d
	}

	tests.tearDown = func() {
		require.NoError(t, client.Database(opts.DB).Drop(ctx))

	}

	suite.Run(t, tests)
}

// TODO run these same tests with different drivers by cloning the
// above Test function and replacing the driverConstructor function.

func (s *RemoteUnorderedSuite) SetupSuite() {
	s.require = s.Require()
}

func (s *RemoteUnorderedSuite) SetupTest() {
	ctx, canceler := context.WithCancel(context.Background())
	s.driver = s.driverConstructor()
	s.canceler = canceler
	s.NoError(s.driver.Open(ctx))
	s.queue = newRemoteUnordered(2, grip.NewLogger(grip.Sender())).(*remoteUnordered)
}

func (s *RemoteUnorderedSuite) TearDownTest() {
	// this order is important, running teardown before canceling
	// the context to prevent closing the connection before
	// running the teardown procedure, given that some connection
	// resources may be shared in the driver.
	s.tearDown()
	s.canceler()
}

func (s *RemoteUnorderedSuite) TestDriverIsUnitializedByDefault() {
	s.Nil(s.queue.Driver())
}

func (s *RemoteUnorderedSuite) TestRemoteUnorderdImplementsQueueInterface() {
	s.Implements((*amboy.Queue)(nil), s.queue)
}

func (s *RemoteUnorderedSuite) TestJobPutIntoQueueFetchableViaGetMethod() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.SetDriver(s.driver))
	s.NotNil(s.queue.Driver())

	j := job.NewShellJob("echo foo", "")
	name := j.ID()
	s.NoError(s.queue.Put(ctx, j))
	fetchedJob, err := s.queue.Get(ctx, name)

	if s.NoError(err) {
		s.IsType(j.Dependency(), fetchedJob.Dependency())
		s.Equal(j.ID(), fetchedJob.ID())
		s.Equal(j.Type(), fetchedJob.Type())

		nj := fetchedJob.(*job.ShellJob)
		s.Equal(j.ID(), nj.ID())
		s.Equal(j.Status().Completed, nj.Status().Completed)
		s.Equal(j.Command, nj.Command, fmt.Sprintf("%+v\n%+v", j, nj))
		s.Equal(j.Output, nj.Output)
		s.Equal(j.WorkingDir, nj.WorkingDir)
		s.Equal(j.Type(), nj.Type())
	}
}

func (s *RemoteUnorderedSuite) TestGetMethodHandlesMissingJobs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Nil(s.queue.Driver())
	s.NoError(s.queue.SetDriver(s.driver))
	s.NotNil(s.queue.Driver())

	s.NoError(s.queue.Start(ctx))

	j := job.NewShellJob("echo foo", "")
	name := j.ID()

	// before putting a job in the queue, it shouldn't exist.
	fetchedJob, err := s.queue.Get(ctx, name)
	s.Error(err)
	s.Nil(fetchedJob)

	s.NoError(s.queue.Put(ctx, j))

	// wrong name also returns error case
	fetchedJob, err = s.queue.Get(ctx, name+name)
	s.Error(err)
	s.Nil(fetchedJob)
}

func (s *RemoteUnorderedSuite) TestInternalRunnerCanBeChangedBeforeStartingTheQueue() {
	s.NoError(s.queue.SetDriver(s.driver))

	originalRunner := s.queue.Runner()

	newRunner := pool.NewLocalWorkers(&pool.WorkerOptions{NumWorkers: 3, Queue: s.queue})
	s.NotEqual(originalRunner, newRunner)

	s.NoError(s.queue.SetRunner(newRunner))
	s.Exactly(newRunner, s.queue.Runner())
}

func (s *RemoteUnorderedSuite) TestInternalRunnerCannotBeChangedAfterStartingAQueue() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.SetDriver(s.driver))

	runner := s.queue.Runner()
	s.False(s.queue.Info().Started)
	s.NoError(s.queue.Start(ctx))
	s.True(s.queue.Info().Started)

	newRunner := pool.NewLocalWorkers(&pool.WorkerOptions{NumWorkers: 2, Queue: s.queue})
	s.Error(s.queue.SetRunner(newRunner))
	s.NotEqual(runner, newRunner)
}

func (s *RemoteUnorderedSuite) TestPuttingAJobIntoAQueueImpactsStats() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.SetDriver(s.driver))

	existing := s.queue.Stats(ctx)
	s.NoError(s.queue.Start(ctx))

	j := job.NewShellJob("true", "")
	s.NoError(s.queue.Put(ctx, j))

	_, err := s.queue.Get(ctx, j.ID())
	s.NoError(err)

	stats := s.queue.Stats(ctx)

	report := fmt.Sprintf("%+v", stats)
	s.Equal(existing.Total+1, stats.Total, report)
}

func (s *RemoteUnorderedSuite) TestQueueFailsToStartIfDriverIsNotSet() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Nil(s.queue.driver)
	s.Nil(s.queue.Driver())
	s.Error(s.queue.Start(ctx))

	s.NoError(s.queue.SetDriver(s.driver))

	s.NotNil(s.queue.driver)
	s.NotNil(s.queue.Driver())
	s.NoError(s.queue.Start(ctx))
}

func (s *RemoteUnorderedSuite) TestQueueFailsToStartIfRunnerIsNotSet() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NotNil(s.queue.Runner())

	s.NoError(s.queue.SetRunner(nil))

	s.Nil(s.queue.runner)
	s.Nil(s.queue.Runner())

	s.Error(s.queue.Start(ctx))
}

func (s *RemoteUnorderedSuite) TestSetDriverErrorsIfQueueHasStarted() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.SetDriver(s.driver))
	s.NoError(s.queue.Start(ctx))

	s.Error(s.queue.SetDriver(s.driver))
}

func (s *RemoteUnorderedSuite) TestStartMethodCanBeCalledMultipleTimes() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.SetDriver(s.driver))
	for i := 0; i < 200; i++ {
		s.NoError(s.queue.Start(ctx))
		s.True(s.queue.Info().Started)
	}
}

func (s *RemoteUnorderedSuite) TestNextMethodSkipsLockedJobs() {
	s.require.NoError(s.queue.SetDriver(s.driver))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	numLocked := 0
	lockedJobs := map[string]struct{}{}

	created := 0
	for i := 0; i < 30; i++ {
		cmd := fmt.Sprintf("echo 'foo: %d'", i)
		j := job.NewShellJob(cmd, "")

		if i%3 == 0 {
			numLocked++
			err := j.Lock(s.driver.ID(), amboy.LockTimeout)
			s.NoError(err)

			s.Error(j.Lock("elsewhere", amboy.LockTimeout))
			lockedJobs[j.ID()] = struct{}{}
		}

		if s.NoError(s.queue.Put(ctx, j)) {
			created++
		}
	}

	s.queue.started = true
	s.require.NoError(s.queue.Start(ctx))
	go s.queue.jobServer(ctx)

	observed := 0
	startAt := time.Now()
checkResults:
	for {
		if time.Since(startAt) >= time.Second {
			break checkResults
		}

		nctx, ncancel := context.WithTimeout(ctx, 100*time.Millisecond)
		work, err := s.queue.Next(nctx)
		ncancel()
		if work == nil {
			s.Error(err)
			continue checkResults
		}
		s.NoError(err)
		observed++

		_, ok := lockedJobs[work.ID()]
		s.False(ok, fmt.Sprintf("%s\n\tjob: %+v\n\tqueue: %+v",
			work.ID(), work.Status(), s.queue.Stats(ctx)))

		if observed == created || observed+numLocked == created {
			break checkResults
		}

	}
	s.require.NoError(ctx.Err())
	qStat := s.queue.Stats(ctx)
	s.True(qStat.Running >= numLocked)
	s.True(qStat.Total == created)
	s.True(qStat.Completed <= observed, fmt.Sprintf("%d <= %d", qStat.Completed, observed))
	s.Equal(created, observed+numLocked, "%+v", qStat)
}

func (s *RemoteUnorderedSuite) TestTimeInfoPersists() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.require.NoError(s.queue.SetDriver(s.driver))
	j := testutil.NewMockJob()
	s.Zero(j.TimeInfo())
	s.NoError(s.queue.Put(ctx, j))
	go s.queue.jobServer(ctx)
	j2, err := s.queue.Next(ctx)
	s.require.NoError(err)
	s.NotZero(j2.TimeInfo())
}

func (s *RemoteUnorderedSuite) TestInfoReturnsDefaultLockTimeout() {
	s.Equal(amboy.LockTimeout, s.queue.Info().LockTimeout)
}

func (s *RemoteUnorderedSuite) TestInfoReturnsConfigurableLockTimeout() {
	opts := DefaultMongoDBOptions()
	opts.LockTimeout = 30 * time.Minute
	d, err := newMongoDriver(s.T().Name(), opts)
	s.Require().NoError(err)
	s.Require().NoError(s.queue.SetDriver(d))
	s.Equal(opts.LockTimeout, s.queue.Info().LockTimeout)
}

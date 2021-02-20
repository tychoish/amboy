package rest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/job"
	"github.com/tychoish/amboy/registry"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/send"
)

func init() {
	grip.SetName("amboy.rest.tests")
	grip.Error(grip.SetSender(send.MakeNative()))

	lvl := grip.GetSender().Level()
	lvl.Threshold = level.Warning
	_ = grip.GetSender().SetLevel(lvl)

	job.RegisterDefaultJobs()
}

type RestServiceSuite struct {
	service *QueueService
	require *require.Assertions
	suite.Suite
}

func TestRestServiceSuite(t *testing.T) {
	suite.Run(t, new(RestServiceSuite))
}

func (s *RestServiceSuite) SetupSuite() {
	// need to import job so that we load the init functions that
	// register jobs so these tests are more meaningful.
	job.RegisterDefaultJobs()
	s.require = s.Require()
}

func (s *RestServiceSuite) SetupTest() {
	s.service = NewQueueService()
}

func (s *RestServiceSuite) TearDownTest() {
	s.service.Close()
}

func (s *RestServiceSuite) TestInitialListOfRegisteredJobs() {
	defaultJob := &QueueService{}
	s.Len(defaultJob.registeredTypes, 0)

	count := 0
	for _, jType := range s.service.registeredTypes {
		factory, err := registry.GetJobFactory(jType)
		if s.NoError(err) {
			count++
			s.Implements((*amboy.Job)(nil), factory())
		}
	}

	s.Len(s.service.registeredTypes, count)
}

func (s *RestServiceSuite) TestServiceOpenMethodInitializesResources() {
	s.Nil(s.service.closer)
	s.Nil(s.service.queue)

	ctx := context.Background()
	s.NoError(s.service.Open(ctx))

	s.NotNil(s.service.queue)
	s.NotNil(s.service.closer)
}

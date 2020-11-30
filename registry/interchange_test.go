package registry

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cdr/amboy"
	"github.com/cdr/amboy/dependency"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
)

// JobInterchangeSuite tests the JobInterchange format and
// converters. JobInterchange provides a generic method, in
// conjunction with the job type registry to let amboy be able to
// serialize and pass job data between instances. This is used both to
// pass information between different queue instances *and* by the
// JobGroup implementation.
type JobInterchangeSuite struct {
	job *JobTest
	suite.Suite
	unmarshaler Unmarshaler
	marshaler   Marshaler
}

func TestJobInterchangeSuiteJSON(t *testing.T) {
	s := new(JobInterchangeSuite)
	s.unmarshaler = json.Unmarshal
	s.marshaler = json.Marshal
	suite.Run(t, s)
}

func TestJobInterchangeSuiteLegacyBSON(t *testing.T) {
	s := new(JobInterchangeSuite)
	s.unmarshaler = bson.Unmarshal
	s.marshaler = bson.Marshal
	suite.Run(t, s)
}

func (s *JobInterchangeSuite) SetupTest() {
	s.job = NewTestJob("interchange-test")
}

func (s *JobInterchangeSuite) TestRoundTripHighLevel() {
	i, err := MakeJobInterchange(s.job, s.marshaler)
	s.NoError(err)

	outJob, err := i.Resolve(s.unmarshaler)
	s.NoError(err)

	outJob.SetScopes(nil)
	// outJob.SetDependency(s.job.Dependency())

	s.Equal(s.job, outJob)
}

func (s *JobInterchangeSuite) TestRoundTripLowLevel() {
	i, err := MakeJobInterchange(s.job, s.marshaler)
	s.NoError(err)

	j2, err := i.Resolve(s.unmarshaler)
	if s.NoError(err) {
		j2.SetScopes(nil)
		j2.SetDependency(dependency.NewAlways())
		s.Equal(s.job, j2)
	}
}

func (s *JobInterchangeSuite) TestConversionToInterchangeMaintainsMetaDataFidelity() {
	i, err := MakeJobInterchange(s.job, s.marshaler)
	if s.NoError(err) {
		s.Equal(s.job.ID(), i.Name)
		s.Equal(s.job.Type().Name, i.Type)
		s.Equal(s.job.Type().Version, i.Version)
		s.Equal(s.job.Status(), i.Status)
	}
}

func (s *JobInterchangeSuite) TestConversionFromInterchangeMaintainsFidelity() {
	i, err := MakeJobInterchange(s.job, s.marshaler)
	if !s.NoError(err) {
		return
	}

	j, err := i.Resolve(s.unmarshaler)

	if s.NoError(err) {
		s.IsType(s.job, j)

		new := j.(*JobTest)

		s.Equal(s.job.Name, new.Name)
		s.Equal(s.job.Content, new.Content)
		s.Equal(s.job.ShouldFail, new.ShouldFail)
		s.Equal(s.job.T, new.T)
	}
}

func (s *JobInterchangeSuite) TestUnregisteredTypeCannotConvertToJob() {
	s.job.T.Name = "different"

	i, err := MakeJobInterchange(s.job, s.marshaler)
	if s.NoError(err) {
		j, err := i.Resolve(s.unmarshaler)
		s.Nil(j)
		s.Error(err)
	}
}

func (s *JobInterchangeSuite) TestMismatchedVersionResultsInErrorOnConversion() {
	s.job.T.Version += 100

	i, err := MakeJobInterchange(s.job, s.marshaler)
	if s.NoError(err) {
		j, err := i.Resolve(s.unmarshaler)
		s.Nil(j)
		s.Error(err)
	}
}

func (s *JobInterchangeSuite) TestConvertToJobForUnknownJobType() {
	s.job.T.Name = "missing-job-type"
	i, err := MakeJobInterchange(s.job, s.marshaler)
	if s.NoError(err) {
		j, err := i.Resolve(s.unmarshaler)
		s.Nil(j)
		s.Error(err)
	}
}

func (s *JobInterchangeSuite) TestMismatchedDependencyCausesJobConversionToError() {
	s.job.T.Version += 100

	i, err := MakeJobInterchange(s.job, s.marshaler)
	if s.NoError(err) {
		j, err := i.Resolve(s.unmarshaler)
		s.Error(err)
		s.Nil(j)
	}
}

func (s *JobInterchangeSuite) TestTimeInfoPersists() {
	now := time.Now()
	ti := amboy.JobTimeInfo{
		Start:     now.Round(time.Millisecond),
		End:       now.Add(time.Hour).Round(time.Millisecond),
		WaitUntil: now.Add(-time.Minute).Round(time.Millisecond),
	}
	s.job.UpdateTimeInfo(ti)
	s.Equal(ti, s.job.TimingInfo)

	i, err := MakeJobInterchange(s.job, s.marshaler)
	if s.NoError(err) {
		s.Equal(i.TimeInfo, ti)

		j, err := i.Resolve(s.unmarshaler)
		s.NoError(err)
		if s.NotNil(j) {
			s.Equal(ti, j.TimeInfo())
		}
	}

}

// DependencyInterchangeSuite tests the DependencyInterchange format
// and converters. This type provides a way for Jobs and Queues to
// serialize their objects quasi-generically.
type DependencyInterchangeSuite struct {
	dep         dependency.Manager
	interchange *DependencyInterchange
	unmarshaler Unmarshaler
	marshaler   Marshaler

	suite.Suite
}

func TestDependencyInterchangeBSONSuite(t *testing.T) {
	s := new(DependencyInterchangeSuite)
	s.unmarshaler = bson.Unmarshal
	s.marshaler = bson.Marshal
	suite.Run(t, s)
}

func TestDependencyInterchangeJSONSuite(t *testing.T) {
	s := new(DependencyInterchangeSuite)
	s.unmarshaler = json.Unmarshal
	s.marshaler = json.Marshal
	suite.Run(t, s)
}

func (s *DependencyInterchangeSuite) SetupTest() {
	s.dep = dependency.NewAlways()
	s.Equal(s.dep.Type().Name, "always")
}

func (s *DependencyInterchangeSuite) TestDependencyInterchangeFormatStoresDataCorrectly() {
	i, err := makeDependencyInterchange(s.marshaler, s.dep)
	if s.NoError(err) {
		s.Equal(s.dep.Type().Name, i.Type)
		s.Equal(s.dep.Type().Version, i.Version)
	}
}

func (s *DependencyInterchangeSuite) TestConvertFromDependencyInterchangeFormatMaintainsFidelity() {
	i, err := makeDependencyInterchange(s.marshaler, s.dep)
	if s.NoError(err) {
		s.Require().IsType(i, s.interchange)

		dep, err := convertToDependency(s.unmarshaler, i)
		s.NoError(err)
		s.Require().IsType(dep, s.dep)
		old := s.dep
		new := dep
		s.Equal(old.Type(), new.Type())
		s.Equal(len(old.Edges()), len(new.Edges()))
	}

}

func (s *DependencyInterchangeSuite) TestVersionInconsistencyCauseConverstionToDependencyToError() {
	i, err := makeDependencyInterchange(s.marshaler, s.dep)
	if s.NoError(err) {
		s.Require().IsType(i, s.interchange)

		i.Version += 100

		dep, err := convertToDependency(s.unmarshaler, i)
		s.Error(err)
		s.Nil(dep)
	}
}

func (s *DependencyInterchangeSuite) TestNameInconsistencyCasuesConversionToDependencyToError() {
	i, err := makeDependencyInterchange(s.marshaler, s.dep)
	if s.NoError(err) {
		s.Require().IsType(i, s.interchange)

		i.Type = "sommetimes"

		dep, err := convertToDependency(s.unmarshaler, i)
		s.Error(err)
		s.Nil(dep)
	}
}

package registry

import (
	"github.com/deciduosity/amboy"
	"github.com/deciduosity/amboy/dependency"
	"github.com/pkg/errors"
)

// JobInterchange provides a consistent way to describe and reliably
// serialize Job objects between different queue
// instances. Interchange is also used internally as part of JobGroup
// Job type.
type JobInterchange struct {
	Name       string                 `json:"name" bson:"_id" yaml:"name" db:"id"`
	Type       string                 `json:"type" bson:"type" yaml:"type" db:"type"`
	Group      string                 `bson:"group,omitempty" json:"group,omitempty" yaml:"group,omitempty" db:"queue_group"`
	Version    int                    `json:"version" bson:"version" yaml:"version" db:"version"`
	Priority   int                    `json:"priority" bson:"priority" yaml:"priority" db:"priority"`
	Status     amboy.JobStatusInfo    `bson:"status" json:"status" yaml:"status" db:"status"`
	Scopes     []string               `bson:"scopes,omitempty" json:"scopes,omitempty" yaml:"scopes,omitempty" db:"scopes"`
	TimeInfo   amboy.JobTimeInfo      `bson:"time_info" json:"time_info,omitempty" yaml:"time_info,omitempty" db:"time_info"`
	Job        rawJob                 `json:"job" bson:"job" yaml:"job" db:"job"`
	Dependency *DependencyInterchange `json:"dependency,omitempty" bson:"dependency,omitempty" yaml:"dependency,omitempty" db:"depdendency"`
}

type Marshaler func(interface{}) ([]byte, error)

// Unmrashaler describes a standard function that takes a byte slice
// and populates an object from this serialized view, and is available
// as a type to make it more clear that queue implementations are
// responsible for mantaining their population
type Unmarshaler func([]byte, interface{}) error

// MakeJobInterchange changes a Job interface into a JobInterchange
// structure, for easier serialization.
func MakeJobInterchange(j amboy.Job, convertTo Marshaler) (*JobInterchange, error) {
	typeInfo := j.Type()

	if typeInfo.Version < 0 {
		return nil, errors.New("cannot use jobs with versions less than 0 with job interchange")
	}

	dep, err := makeDependencyInterchange(convertTo, j.Dependency())
	if err != nil {
		return nil, err
	}

	data, err := convertTo(j)
	if err != nil {
		return nil, err
	}

	output := &JobInterchange{
		Name:       j.ID(),
		Type:       typeInfo.Name,
		Version:    typeInfo.Version,
		Priority:   j.Priority(),
		Status:     j.Status(),
		TimeInfo:   j.TimeInfo(),
		Job:        data,
		Dependency: dep,
	}

	output.Status.ID = output.Name
	output.TimeInfo.ID = output.Name
	output.Dependency.ID = output.Name

	return output, nil
}

// Resolve reverses the process of ConvertToInterchange and
// converts the interchange format to a Job object using the types in
// the registry. Returns an error if the job type of the
// JobInterchange object isn't registered or the current version of
// the job produced by the registry is *not* the same as the version
// of the Job.
func (j *JobInterchange) Resolve(convertFrom Unmarshaler) (amboy.Job, error) {
	factory, err := GetJobFactory(j.Type)
	if err != nil {
		return nil, err
	}

	job := factory()

	if job.Type().Version != j.Version {
		return nil, errors.Errorf("job '%s' (version=%d) does not match the current version (%d) for the job type '%s'",
			j.Name, j.Version, job.Type().Version, j.Type)
	}

	err = convertFrom(j.Job, job)
	if err != nil {
		return nil, errors.Wrap(err, "converting job body")
	}

	if j.Dependency != nil {
		// it would be reasonable to only do this when the dependency
		// is not nil.
		dep, err := convertToDependency(convertFrom, j.Dependency)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		job.SetDependency(dep)
	}

	job.SetPriority(j.Priority)
	job.SetStatus(j.Status)
	job.UpdateTimeInfo(j.TimeInfo)

	return job, nil
}

// Raw returns the serialized version of the job.
func (j *JobInterchange) Raw() []byte { return j.Job }

////////////////////////////////////////////////////////////////////////////////////////////////////

// DependencyInterchange objects are a standard form for
// dependency.Manager objects. Amboy (should) only pass
// DependencyInterchange objects between processes, which have the
// type information in easy to access and index-able locations.
type DependencyInterchange struct {
	ID         string        `bson:"id,omitempty" json:"id,omitempty" yaml:"id,omitempty" db:"id"`
	Type       string        `json:"type" bson:"type" yaml:"type" db:"dep_type"`
	Version    int           `json:"version" bson:"version" yaml:"version" db:"dep_version"`
	Edges      []string      `bson:"edges" json:"edges" yaml:"edges" db:"edges"`
	Dependency rawDependency `json:"dependency" bson:"dependency" yaml:"dependency" db:"dependency"`
}

// MakeDependencyInterchange converts a dependency.Manager document to
// its DependencyInterchange format.
func makeDependencyInterchange(convertTo Marshaler, d dependency.Manager) (*DependencyInterchange, error) {
	typeInfo := d.Type()

	data, err := convertTo(d)
	if err != nil {
		return nil, err
	}

	output := &DependencyInterchange{
		Type:       typeInfo.Name,
		Version:    typeInfo.Version,
		Edges:      d.Edges(),
		Dependency: data,
	}

	return output, nil
}

// convertToDependency uses the registry to convert a
// DependencyInterchange object to the correct dependnecy.Manager
// type.
func convertToDependency(convertFrom Unmarshaler, d *DependencyInterchange) (dependency.Manager, error) {
	factory, err := GetDependencyFactory(d.Type)
	if err != nil {
		return nil, err
	}

	dep := factory()

	if dep.Type().Version != d.Version {
		return nil, errors.Errorf("dependency '%s' (version=%d) does not match the current version (%d) for the dependency type '%s'",
			d.Type, d.Version, dep.Type().Version, dep.Type().Name)
	}

	// this works, because we want to use all the data from the
	// interchange object, but want to use the type information
	// associated with the object that we produced with the
	// factory.
	if err := convertFrom(d.Dependency, dep); err != nil {
		return nil, errors.Wrap(err, "converting dependency")
	}

	return dep, nil
}

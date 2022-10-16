package registry

import (
	"errors"
	"fmt"

	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/dependency"
)

type resolutionError struct {
	Kind              string `bson:"kind" json:"kind" yaml:"kind"`
	Name              string `bson:"name" json:"name" yaml:"name"`
	RegisteredVersion int    `bson:"registered_version" json:"registered_version" yaml:"registered_version"`
	RecordVersion     int    `bson:"record_version" json:"record_version" yaml:"record_version"`
}

func (e *resolutionError) Error() string {
	return fmt.Sprintf("registered %s type '%s' at version %d does not match the current version %d",
		e.Kind, e.Name, e.RegisteredVersion, e.RecordVersion)
}

// NewJobResolutionError returns an error if the version in the job
// type and the version provided are mismatched. This error is
// detectable using the IsVersionResolutionError.
func NewJobResolutionError(reg amboy.JobType, version int) error {
	if reg.Version == version {
		return nil
	}

	return &resolutionError{
		Kind:              "job",
		Name:              reg.Name,
		RegisteredVersion: reg.Version,
		RecordVersion:     version,
	}
}

// NewDependencyResolutionError returns an error if the version in the
// dependency type and the version provided are mismatched. This error
// is detectable using the IsVersionResolutionError.
func NewDependencyResolutionError(reg dependency.TypeInfo, version int) error {
	if reg.Version == version {
		return nil
	}
	return &resolutionError{
		Kind:              "dependency",
		Name:              reg.Name,
		RegisteredVersion: reg.Version,
		RecordVersion:     version,
	}
}

// IsVersionResolutionError returns true if an error is a resolution
// error and false otherwise.
func IsVersionResolutionError(err error) bool {
	if err == nil {
		return false
	}

	var re *resolutionError
	if errors.As(err, &re) {
		return re.RegisteredVersion != re.RecordVersion
	}

	return false
}

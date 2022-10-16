package amboy

import (
	"context"
	"errors"
	"fmt"
)

// EnqueueUniqueJob is a generic wrapper for adding jobs to queues
// (using the Put() method), but that ignores duplicate job errors.
func EnqueueUniqueJob(ctx context.Context, queue Queue, job Job) error {
	err := queue.Put(ctx, job)

	if IsDuplicateJobError(err) {
		return nil
	}

	return err
}

type duplJobError struct {
	msg string
}

func (e *duplJobError) Error() string { return e.msg }

// NewDuplicateJobError creates a new error object to represent a
// duplicate job error, for use by queue implementations.
func NewDuplicateJobError(msg string) error { return &duplJobError{msg: msg} }

// NewDuplicateJobErrorf creates a new error object to represent a
// duplicate job error with a formatted message, for use by queue
// implementations.
func NewDuplicateJobErrorf(msg string, args ...interface{}) error {
	return NewDuplicateJobError(fmt.Sprintf(msg, args...))
}

// MakeDuplicateJobError constructs a duplicate job error from an
// existing error of any type, for use by queue implementations.
func MakeDuplicateJobError(err error) error {
	if err == nil {
		return nil
	}

	return NewDuplicateJobError(err.Error())
}

// IsDuplicateJobError tests an error object to see if it is a
// duplicate job error.
func IsDuplicateJobError(err error) bool {
	if err == nil {
		return false
	}
	var dje *duplJobError
	return errors.As(err, &dje)
}

type jobNotDefinedError struct {
	ID      string `bson:"job_id" json:"job_id" yaml:"job_id"`
	QueueID string `bson:"queue_id" json:"queue_id" yaml:"queue_id"`
}

func (e *jobNotDefinedError) Error() string {
	return fmt.Sprintf("job %q not defined in queue %q", e.ID, e.QueueID)
}

// NewJobNotDefinedError produces an error that is detectable with
// IsJobNotDefinedError, and can be produced by Get methods of
// queues.
func NewJobNotDefinedError(q Queue, id string) error {
	return MakeJobNotDefinedError(q.ID(), id)
}

// MakeJobNotDefinedError provides a less well typed constructor for a
// job-not-defined error.
func MakeJobNotDefinedError(queueID, jobID string) error {
	return &jobNotDefinedError{
		ID:      jobID,
		QueueID: queueID,
	}

}

// IsJobNotDefinedError returns true if the error is a job-not-defined
// error, and false otherwise.
func IsJobNotDefinedError(err error) bool {
	if err == nil {
		return false
	}

	var jnde *jobNotDefinedError
	return errors.As(err, &jnde)
}

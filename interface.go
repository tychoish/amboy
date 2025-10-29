package amboy

import (
	"context"
	"time"

	"github.com/tychoish/amboy/dependency"
	"github.com/tychoish/fun/erc"
	"github.com/tychoish/fun/ers"
)

// LockTimeout describes the default period of time that a queue will respect
// a stale lock from another queue before beginning work on a job.
const LockTimeout = 10 * time.Minute

// Job describes a unit of work. Implementations of Job instances are
// the content of the Queue. The amboy/job package contains several
// general purpose and example implementations. Jobs are responsible,
// primarily via their Dependency property, for determining: if they
// need to run, and what Jobs they depend on. Actual use of the
// dependency system is the responsibility of the Queue implementation.
//
// In most cases, applications only need to implement the Run()
// method, all additional functionality is provided by the job.Base type,
// which can be embedded anonymously in implementations of the Job.
type Job interface {
	// Provides a unique identifier for a job. Queues may error if
	// two jobs have different IDs.
	ID() string

	// The primary execution method for the job. Should toggle the
	// completed state for the job.
	Run(context.Context)

	// Returns a pointer to a JobType object that Queue
	// implementations can use to de-serialize tasks.
	Type() JobType

	// Provides access to the job's dependency information, and
	// allows queues to override a dependency (e.g. in a force
	// build state, or as part of serializing dependency objects
	// with jobs.)
	Dependency() dependency.Manager
	SetDependency(dependency.Manager)

	// Provides access to the JobStatusInfo object for the job,
	// which reports the current state.
	Status() JobStatusInfo
	SetStatus(JobStatusInfo)

	// TimeInfo reports the start/end time of jobs, as well as
	// providing for a "wait until" functionality that queues can
	// use to schedule jobs in the future. The update method, only
	// updates non-zero methods.
	TimeInfo() JobTimeInfo
	UpdateTimeInfo(JobTimeInfo)

	// Provides access to the job's priority value, which some
	// queues may use to order job dispatching. Most Jobs
	// implement these values by composing the
	// amboy/priority.Value type.
	Priority() int
	SetPriority(int)

	// AddError allows another actor to annotate the job with an
	// error.
	AddError(error)
	// Error returns an error object if the task was an
	// error. Typically if the job has not run, this is nil.
	Error() error

	// Lock and Unlock are responsible for handling the locking
	// behavor for the job. Lock is responsible for setting the
	// owner (its argument), incrementing the modification count
	// and marking the job in progress, and returning an error if
	// another worker has access to the job. Unlock is responsible
	// for unsetting the owner and marking the job as
	// not-in-progress, and should be a no-op if the job does not
	// belong to the owner. In general the owner should be the value
	// of queue.ID()
	Lock(owner string, lockTimeout time.Duration) error
	Unlock(owner string, lockTimeout time.Duration)

	// Scope provides the ability to provide more configurable
	// exclusion a job can provide.
	Scopes() []string
	SetScopes([]string)
}

// JobType contains information about the type of a job, which queues
// can use to serialize objects. All Job implementations must store
// and produce instances of this type that identify the type and
// implementation version.
type JobType struct {
	Name    string `json:"name" bson:"name" yaml:"name"`
	Version int    `json:"version" bson:"version" yaml:"version"`
}

// JobStatusInfo contains information about the current status of a
// job and is reported by the Status and set by the SetStatus methods
// in the Job interface.e
type JobStatusInfo struct {
	ID                string    `bson:"id,omitempty" json:"id,omitempty" yaml:"id,omitempty" db:"id"`
	Owner             string    `bson:"owner" json:"owner" yaml:"owner" db:"owner"`
	Completed         bool      `bson:"completed" json:"completed" yaml:"completed" db:"completed" `
	InProgress        bool      `bson:"in_prog" json:"in_progress" yaml:"in_progress" db:"in_progress"`
	Canceled          bool      `bson:"canceled" json:"canceled" yaml:"canceled" db:"canceled"`
	ModificationTime  time.Time `bson:"mod_ts" json:"mod_time" yaml:"mod_time" db:"updated_at"`
	ModificationCount int       `bson:"mod_count" json:"mod_count" yaml:"mod_count" db:"mod_count"`
	ErrorCount        int       `bson:"err_count" json:"err_count" yaml:"err_count" db:"err_count"`
	Errors            []string  `bson:"errors,omitempty" json:"errors,omitempty" yaml:"errors,omitempty" db:"_"`
}

// JobTimeInfo stores timing information for a job and is used by both
// the Runner and Job implementations to track how long jobs take to
// execute.
//
// Additionally, the Queue implementations __may__ use WaitUntil to
// defer the execution of a job, until WaitUntil refers to a time in
// the past.
//
// If the deadline is specified, and the queue
// implementation supports it, the queue may drop the job if the
// deadline is in the past when the job would be dispatched.
type JobTimeInfo struct {
	ID         string        `bson:"id,omitempty" json:"id,omitempty" yaml:"id,omitempty" db:"id"`
	Created    time.Time     `bson:"created,omitempty" json:"created,omitempty" yaml:"created,omitempty" db:"created_at"`
	Start      time.Time     `bson:"start,omitempty" json:"start,omitempty" yaml:"start,omitempty" db:"started_at"`
	End        time.Time     `bson:"end,omitempty" json:"end,omitempty" yaml:"end,omitempty" db:"ended_at"`
	WaitUntil  time.Time     `bson:"wait_until" json:"wait_until,omitempty" yaml:"wait_until,omitempty" db:"wait_until"`
	DispatchBy time.Time     `bson:"dispatch_by" json:"dispatch_by,omitempty" yaml:"dispatch_by,omitempty" db:"dispatch_by"`
	MaxTime    time.Duration `bson:"max_time" json:"max_time,omitempty" yaml:"max_time,omitempty" db:"max_time"`
}

// Duration is a convenience function to return a duration for a job.
func (j JobTimeInfo) Duration() time.Duration { return j.End.Sub(j.Start) }

// IsStale determines if the job is too old to be dispatched, and if
// so, queues may remove or drop the job entirely.
func (j JobTimeInfo) IsStale() bool {
	if j.DispatchBy.IsZero() {
		return false
	}

	return j.DispatchBy.Before(time.Now())
}

// IsDispatchable determines if the job should be dispatched based on
// the value of WaitUntil.
func (j JobTimeInfo) IsDispatchable() bool {
	return time.Now().After(j.WaitUntil)
}

// Validate ensures that the structure has reasonable values set.
func (j JobTimeInfo) Validate() error {
	catcher := &erc.Collector{}
	catcher.If(!j.DispatchBy.IsZero() && j.WaitUntil.After(j.DispatchBy), ers.Error("invalid for wait_until to be after dispatch_by"))
	catcher.If(j.Created.IsZero(), ers.Error("must specify non-zero created timestamp"))
	catcher.If(j.MaxTime < 0, ers.Error("must specify 0 or positive max_time"))

	return catcher.Resolve()
}

// Queue describes a very simple Job queue interface that allows users
// to define Job objects, add them to a worker queue and execute tasks
// from that queue. Queue implementations may run locally or as part
// of a distributed application, with multiple workers and submitter
// Queue instances, which can support different job dispatching and
// organization properties.
type Queue interface {
	// Used to add a job to the queue. Should only error if the
	// Queue cannot accept jobs if the job already exists in a
	// queue.
	Put(context.Context, Job) error

	// Returns a unique identifier for the instance of the queue.
	ID() string

	// Given a job id, get that job. The second return value is a
	// Boolean, which indicates if the named job had been
	// registered by a Queue.
	Get(context.Context, string) (Job, error)

	// Returns the next job in the queue. These calls are
	// blocking, but may be interrupted with a canceled
	// context. Next must return a non-nil job if error is nil.
	Next(context.Context) (Job, error)

	// Info returns information related to management of the Queue.
	Info() QueueInfo

	// Used to mark a Job complete and remove it from the pending
	// work of the queue.
	Complete(context.Context, Job) error

	// Saves the state of a current job to the underlying storage,
	// generally in support of locking and incremental
	// persistence. Should error if the job does not exist (use
	// put,) or if the queue does not have ownership of the job.
	Save(context.Context, Job) error

	// Channel that produces all job objects Job objects.
	Jobs(context.Context) <-chan Job

	// Returns an object that contains statistics about the
	// current state of the Queue.
	Stats(context.Context) QueueStats

	// Getter for the Runner implementation embedded in the Queue
	// instance.
	Runner() Runner

	// Setter for the Runner implementation embedded in the Queue
	// instance. Permits runtime substitution of interfaces, but
	// implementations are not expected to permit users to change
	// runner implementations after starting the Queue.
	SetRunner(Runner) error

	// Begins the execution of the job Queue, using the embedded
	// Runner.
	Start(context.Context) error

	// Close will terminate the runner (e.g. Runner().Close())
	// and also release any queue specific resources.
	Close(context.Context) error
}

// QueueInfo describes runtime information associated with a Queue.
type QueueInfo struct {
	Started     bool
	LockTimeout time.Duration
}

// QueueGroup describes a group of queues. Each queue is indexed by a
// string. Users can use these queues if there are many different types
// of work or if the types of work are only knowable at runtime.
type QueueGroup interface {
	// Get a queue with the given index. Most implementations will
	// create a queue if it doesn't exist in a Get operation.
	Get(context.Context, string) (Queue, error)

	// Put a queue at the given index. Because most
	// implementations create new queues in their Get operation,
	// the Put operation for groups of queues that don't rely on
	// the default queue construction in Get, and need not be used
	// directly.
	Put(context.Context, string, Queue) error

	// Start all queues with pending work, and for QueueGroup
	// implementations that have background workers to prune idle
	// queues or create queues with pending work (for queues with
	// distributed backends.)
	Start(context.Context) error

	// Prune old queues. Most queue implementations automatically
	// prune queues that are inactive based on some TTL. For most
	// implementations, users will never need to call this
	// directly.
	Prune(context.Context) error

	// Close the queues.
	Close(context.Context) error

	// Len returns the number of active queues managed in the
	// group.
	Len() int

	// Queues returns all currently registered and running queues
	Queues(context.Context) []string
}

// DeletableJobQueue describes an optional feature superset of a queue
// that allows some jobs to be deleted from the underlying storage by
// passing that job's ID.
//
// Queue implementations may decide how to handle deletion of jobs if
// they implement it at all. In most cases the deletion of an
// in-progress job will result in undefined behavior.
type DeletableJobQueue interface {
	Queue
	Delete(context.Context, string) error
}

// Runner describes a simple worker interface for executing jobs in
// the context of a Queue. Used by queue implementations to run
// tasks. Generally Queue implementations will spawn a runner as part
// of their constructor or Start() methods, but client code can inject
// alternate Runner implementations, as required.
type Runner interface {
	// Reports if the pool has started.
	Started() bool

	// Provides a method to change or set the pointer to the
	// enclosing Queue object after instance creation. Runner
	// implementations may not be able to change their Queue
	// association after starting.
	SetQueue(Queue) error

	// Prepares the runner implementation to begin doing work, if
	// any is required (e.g. starting workers.) Typically called
	// by the enclosing Queue object's Start() method.
	Start(context.Context) error

	// Termaintes all in progress work and waits for processes to
	// return.
	Close(context.Context)
}

// AbortableRunner provides a superset of the Runner interface but
// allows callers to abort jobs by ID.
//
// Implementations should be sure to abort jobs in such a way that
// they not set the job to be canceled, and have the effect of
// "completing" the job.
type AbortableRunner interface {
	Runner

	IsRunning(string) bool
	RunningJobs() []string
	Abort(context.Context, string) error
	AbortAll(context.Context)
}

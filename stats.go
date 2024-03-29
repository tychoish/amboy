package amboy

import (
	"fmt"
	"time"

	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/message"
)

// QueueStats is a simple structure that the Stats() method in the
// Queue interface returns and tracks the state of the queue, and
// provides a common format for different Queue implementations to
// report on their state.
//
// Implement's grip's message.Composer interface when passed as a
// pointer.
type QueueStats struct {
	Running   int            `bson:"running" json:"running" yaml:"running"`
	Completed int            `bson:"completed" json:"completed" yaml:"completed"`
	Pending   int            `bson:"pending" json:"pending" yaml:"pending"`
	Blocked   int            `bson:"blocked" json:"blocked" yaml:"blocked"`
	Total     int            `bson:"total" json:"total" yaml:"total"`
	Context   message.Fields `bson:"context,omitempty" json:"context,omitempty" yaml:"context,omitempty"`

	priority level.Priority
}

var _ message.Composer = &QueueStats{}

// Schema definese a version schema for the messages.
func (QueueStats) Schema() string { return "amboy.queueStats.0" }

// String prints a long form report of the queue for human consumption.
func (s QueueStats) String() string {
	return fmt.Sprintf("running='%d', completed='%d', pending='%d', blocked='%d', total='%d'",
		s.Running, s.Completed, s.Pending, s.Blocked, s.Total)
}

// IsDispatchable reports if a job acn be dispatched (e.g. its not in
// progress and not complete) or if it is in progress the lock has expired.
func IsDispatchable(stat JobStatusInfo, lockTimeout time.Duration) bool {
	if stat.InProgress && time.Since(stat.ModificationTime) > lockTimeout {
		return true
	}
	if stat.Completed {
		return false
	}
	if stat.InProgress {
		return false
	}

	return true
}

// IsComplete reutrns true when the total number of tasks are equal to
// the number completed, or if the number of completed and blocked are
// greater than or equal to total. This method is used by the Wait<>
// functions to determine when a queue has completed all actionable
// work.
func (s QueueStats) IsComplete() bool {
	if s.Total == s.Completed {
		return true
	}

	if s.Total <= s.Completed+s.Blocked {
		return true
	}

	return false
}

// Loggable is part of the grip/message.Composer interface and only
// returns true if the queue has at least one job.
func (s QueueStats) Loggable() bool { return s.Total > 0 }

// Raw  is part of the grip/message.Composer interface and simply
// returns the QueueStats object.
func (s QueueStats) Raw() interface{} { return s }

// Priority is part of the grip/message.Composer interface and returns
// the priority of the message.
func (s QueueStats) Priority() level.Priority { return s.priority }

// Structured indicates that the message type is structured and can be
// handled using structured logging methods
func (*QueueStats) Structured() bool { return true }

// SetOption is a part of the grip/message.Composer interface but is a
// noop for QueueStats.
func (*QueueStats) SetOption(opts ...message.Option) {}

// SetPriority  is part of the grip/message.Composer interface and
// allows the caller to configure the piroity of the message.
func (s *QueueStats) SetPriority(l level.Priority) { s.priority = l }

// Annotate is part of the grip/message.Composer interface and allows
// the logging infrastructure to inject content and context into log
// messages.
func (s *QueueStats) Annotate(key string, value interface{}) {
	if s.Context == nil {
		s.Context = message.Fields{}
	}

	s.Context[key] = value
}

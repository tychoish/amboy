package logger

import (
	"context"
	"errors"
	"fmt"

	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/job"
	"github.com/tychoish/grip/message"
	"github.com/tychoish/grip/send"
)

type sendMessageJob struct {
	message message.Composer
	sender  send.Sender
	job.Base
}

// NewSendMessageJob creates an amboy.Job instance that sends the
// specified message to the specified sender.
//
// This job is not compatible with remote-storage backed queues.
func NewSendMessageJob(m message.Composer, s send.Sender) amboy.Job {
	j := &sendMessageJob{
		message: m,
		sender:  s,
		Base: job.Base{
			JobType: amboy.JobType{
				Name:    "send-grip-message",
				Version: -1,
			},
		},
	}
	j.SetID(fmt.Sprintf("queued-message-%d-<m:%T>-<s:%T>", job.GetNumber(), m, s))
	return j
}

func (j *sendMessageJob) Run(_ context.Context) {
	defer j.MarkComplete()

	if j.message == nil {
		j.AddError(errors.New("message cannot be nil"))
		return
	}

	if j.sender == nil {
		j.AddError(errors.New("sender cannot be nil"))
		return
	}

	j.sender.Send(j.message)
}

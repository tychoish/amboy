package logger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tychoish/amboy"
	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/message"
	"github.com/tychoish/grip/send"
)

type MsgJobSuite struct {
	constructor func(m message.Composer, s send.Sender) amboy.Job
	suite.Suite
}

func TestSingleMsgJobSuite(t *testing.T) {
	s := new(MsgJobSuite)
	s.constructor = NewSendMessageJob
	suite.Run(t, s)
}

func TestMultiMsgJobSuite(t *testing.T) {
	s := new(MsgJobSuite)
	s.constructor = func(m message.Composer, s send.Sender) amboy.Job {
		if s == nil {
			return NewMultiSendMessageJob(m, []send.Sender{})
		}
		return NewMultiSendMessageJob(m, []send.Sender{s})
	}
	suite.Run(t, s)
}

func (s *MsgJobSuite) TestIsNotSerializable() {
	job := s.constructor(nil, nil)
	s.True(0 > job.Type().Version)
}

func (s *MsgJobSuite) TestWithNilOpts() {
	job := s.constructor(nil, nil)
	s.NoError(job.Error())
	job.Run(context.Background())
	s.Error(job.Error())
}

func (s *MsgJobSuite) TestWithNilMessage() {
	sender := send.MakePlain()
	sender.SetName("test")
	job := s.constructor(nil, sender)

	s.NoError(job.Error())
	job.Run(context.Background())
	s.Error(job.Error())
}

func (s *MsgJobSuite) TestWithNilSender() {
	job := s.constructor(message.MakeString("foo"), nil)

	s.NoError(job.Error())
	job.Run(context.Background())
	s.Error(job.Error())
}

func (s *MsgJobSuite) TestMessgeSends() {
	sender, err := send.NewInternalLogger("test", send.LevelInfo{Default: level.Debug, Threshold: level.Info})
	s.NoError(err)
	job := s.constructor(message.NewString(level.Alert, "foo"), sender)

	s.False(sender.HasMessage())
	s.NoError(job.Error())
	job.Run(context.Background())
	s.NoError(job.Error())
	s.True(sender.HasMessage())
}

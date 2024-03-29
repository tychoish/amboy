package logger

import (
	"context"
	"sync"

	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/queue"
	"github.com/tychoish/grip/message"
	"github.com/tychoish/grip/send"
)

type queueSender struct {
	mu       sync.RWMutex
	ctx      context.Context
	queue    amboy.Queue
	canceler context.CancelFunc
	send.Sender
}

func newSender(ctx context.Context, q amboy.Queue, sender send.Sender) *queueSender {
	return &queueSender{
		Sender: sender,
		queue:  q,
		ctx:    ctx,
	}
}

// MakeQueueSender wraps the sender with a queue-backed delivery
// mechanism using the specified queue instance.
//
// These senders do not ensure that logged messages are propagated to
// the underlying sender implementation in any order, and may result
// in out-of-order logging.
//
// The close method does not close the underlying sender.
//
// In the event that the sender's Put method returns an error, the
// message (and its error) will be logged directly (and synchronously)
func MakeQueueSender(ctx context.Context, q amboy.Queue, sender send.Sender) send.Sender {
	return newSender(ctx, q, sender)
}

// NewQueueBackedSender creates a new LimitedSize queue, and creates a
// sender implementation wrapping this sender. The queue is not shared.
//
// This sender returns an error if there is a problem starting the
// queue, and cancels the queue upon closing, without waiting for the
// queue to empty.
func NewQueueBackedSender(ctx context.Context, sender send.Sender, workers, capacity int) (send.Sender, error) {
	q := queue.NewLocalLimitedSize(&queue.FixedSizeQueueOptions{Workers: workers, Capacity: capacity})
	s := newSender(ctx, q, sender)

	s.ctx, s.canceler = context.WithCancel(s.ctx)
	if err := q.Start(s.ctx); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *queueSender) Send(m message.Composer) {
	if !send.ShouldLog(s, m) {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.queue.Put(s.ctx, NewSendMessageJob(m, s.Sender)); err != nil {
		s.ErrorHandler()(err, m)
	}
}

func (s *queueSender) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !amboy.Wait(ctx, s.queue) {
		return ctx.Err()
	}

	return s.Sender.Flush(ctx)
}

func (s *queueSender) Close() error {
	if s.canceler != nil {
		s.canceler()
	}

	return nil
}

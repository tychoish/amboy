package pool

import (
	"context"
	"errors"

	"github.com/tychoish/amboy"
)

type noopPool struct {
	isStarted bool
	queue     amboy.Queue
}

// NewNoop creates a runner implementation that has no workers, but
// satisfies the workers and semantics of the Runner interface to
// support queues deployments that have insert only queues.
func NewNoop() amboy.Runner { return new(noopPool) }

func (p *noopPool) Started() bool { return p.isStarted }

func (p *noopPool) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if p.queue == nil {
		return errors.New("cannot start pool without set queue")
	}

	p.isStarted = true
	return nil
}

func (p *noopPool) SetQueue(q amboy.Queue) error {
	if q == nil {
		return errors.New("cannot set a nil queue")
	}

	if p.queue != nil {
		return errors.New("cannot override existing queue")
	}

	p.queue = q
	return nil
}

func (p *noopPool) Close(ctx context.Context) {
	p.isStarted = false
}

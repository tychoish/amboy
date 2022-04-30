package pgq

import (
	"context"

	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/dependency"
	"github.com/tychoish/grip/message"
)

type orderedQueue struct {
	*sqlQueue
}

func (q orderedQueue) Next(ctx context.Context) (amboy.Job, error) {
	return q.sqlQueue.getNext(ctx, func(job amboy.Job) (amboy.Job, bool) {
		dep := job.Dependency()

		id := job.ID()
		switch dep.State() {
		case dependency.Ready:
			return job, true
		case dependency.Passed:
			err := q.Complete(ctx, job)
			msg := message.MakeFields(message.Fields{
				"job_id":  id,
				"message": "marking job with passed (noop) dependency as complete",
			})

			if err != nil {
				q.log.Error(message.WrapError(err, msg))
			} else {
				q.log.Debug(msg)
			}

			return nil, false
		case dependency.Unresolved:
			q.log.Warning(message.MakeAnnotated("detected a dependency error",
				message.Fields{
					"job":   id,
					"edges": dep.Edges(),
					"dep":   dep.Type(),
				}))
			// TODO: it would be reasonable to attempt to
			// mark this job stuck (complete? maybe
			// deleted?) in some way that will prevent it
			// from slowing down dispatching.
			return nil, false
		case dependency.Blocked:
			edges := dep.Edges()
			q.log.Debug(message.Fields{
				"job_id":    id,
				"edges":     edges,
				"dep":       dep.Type(),
				"job_type":  job.Type().Name,
				"num_edges": len(edges),
				"message":   "job is blocked",
			})

			if len(edges) == 1 {
				// this is just an optimization; to
				// attempt to short circuit queries for
				// simple 2-job dependency chains.
				dj, err := q.Get(ctx, edges[0])
				if err != nil {
					return nil, false
				}
				if q.scopesInUse(ctx, dj.Scopes()) {
					return nil, false
				}
				if !amboy.IsDispatchable(dj.Status(), q.Info().LockTimeout) {
					return nil, false
				}
				if dj.Dependency().State() != dependency.Ready {
					return nil, false
				}

				return dj, true
			}

			return nil, false
		default:
			// TODO: it would be reasonable to attempt to
			// mark this job stuck (complete? maybe
			// deleted?) in some way that will prevent it
			// from slowing down dispatching.
			q.log.Warning(message.MakeAnnotated("detected invalid dependency",
				message.Fields{
					"job_id": id,
					"edges":  dep.Edges(),
					"dep":    dep.Type(),
					"state": message.Fields{
						"value":  dep.State(),
						"valid":  dependency.IsValidState(dep.State()),
						"string": dep.State().String(),
					},
				}))
			return nil, false
		}
	})
}

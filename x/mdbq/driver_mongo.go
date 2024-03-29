package mdbq

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/queue"
	"github.com/tychoish/amboy/registry"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/message"
	"github.com/tychoish/grip/recovery"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

type mongoDriver struct {
	client     *mongo.Client
	name       string
	opts       MongoDBOptions
	instanceID string
	mu         sync.RWMutex
	canceler   context.CancelFunc
	log        grip.Logger
	dispatcher queue.Dispatcher
}

// NewMongoDriver constructs a MongoDB backed queue driver
// implementation using the go.mongodb.org/mongo-driver as the
// database interface.
func newMongoDriver(name string, opts MongoDBOptions) (remoteQueueDriver, error) {
	host, _ := os.Hostname() // nolint

	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid mongo driver options: %w", err)
	}

	return &mongoDriver{
		name:       name,
		opts:       opts,
		log:        opts.logger,
		instanceID: fmt.Sprintf("%s.%s.%s", name, host, uuid.New()),
	}, nil
}

// openNewMongoDriver constructs and opens a new MongoDB driver instance
// using the specified session. It is equivalent to calling
// NewMongoDriver() and calling driver.Open().
func openNewMongoDriver(ctx context.Context, name string, opts MongoDBOptions, client *mongo.Client) (remoteQueueDriver, error) {
	d, err := newMongoDriver(name, opts)
	if err != nil {
		return nil, fmt.Errorf("could not create driver: %w", err)
	}
	md, ok := d.(*mongoDriver)
	if !ok {
		return nil, errors.New("amboy programmer error: incorrect constructor")
	}

	if err := md.start(ctx, client); err != nil {
		return nil, fmt.Errorf("problem starting driver: %w", err)
	}

	return d, nil
}

// newMongoGroupDriver is similar to the MongoDriver, except it
// prefixes job ids with a prefix and adds the group field to the
// documents in the database which makes it possible to manage
// distinct queues with a single MongoDB collection.
func newMongoGroupDriver(name string, opts MongoDBOptions, group string) (remoteQueueDriver, error) {
	host, _ := os.Hostname() // nolint

	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid mongo driver options: %w", err)
	}
	opts.UseGroups = true
	opts.GroupName = group

	return &mongoDriver{
		name:       name,
		opts:       opts,
		instanceID: fmt.Sprintf("%s.%s.%s.%s", name, group, host, uuid.New()),
	}, nil
}

// OpenNewMongoGroupDriver constructs and opens a new MongoDB driver instance
// using the specified session. It is equivalent to calling
// NewMongoGroupDriver() and calling driver.Open().
func openNewMongoGroupDriver(ctx context.Context, name string, opts MongoDBOptions, group string, client *mongo.Client) (remoteQueueDriver, error) {
	d, err := newMongoGroupDriver(name, opts, group)
	if err != nil {
		return nil, fmt.Errorf("could not create driver: %w", err)
	}
	md, ok := d.(*mongoDriver)
	if !ok {
		return nil, errors.New("amboy programmer error: incorrect constructor")
	}

	opts.UseGroups = true
	opts.GroupName = group

	if err := md.start(ctx, client); err != nil {
		return nil, fmt.Errorf("problem starting driver: %w", err)
	}

	return d, nil
}

func (d *mongoDriver) ID() string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.instanceID
}

func (d *mongoDriver) Open(ctx context.Context) error {
	if d.canceler != nil {
		return nil
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(d.opts.URI))
	if err != nil {
		return fmt.Errorf("problem opening connection to mongodb at '%s: %w", d.opts.URI, err)
	}

	return d.start(ctx, client)
}

func (d *mongoDriver) start(ctx context.Context, client *mongo.Client) error {
	dCtx, cancel := context.WithCancel(ctx)
	d.canceler = cancel

	d.mu.Lock()
	d.client = client
	d.mu.Unlock()

	startAt := time.Now()
	go func() {
		<-dCtx.Done()
		d.log.Info(message.Fields{
			"message":  "closing session for mongodb driver",
			"id":       d.instanceID,
			"uptime":   time.Since(startAt),
			"span":     time.Since(startAt).String(),
			"service":  "amboy.queue.mdb",
			"is_group": d.opts.UseGroups,
			"group":    d.opts.GroupName,
		})
	}()

	if err := d.setupDB(ctx); err != nil {
		return fmt.Errorf("problem setting up database: %w", err)
	}

	return nil
}

func (d *mongoDriver) getCollection() *mongo.Collection {
	db := d.client.Database(d.opts.DB)
	if d.opts.UseGroups {
		return db.Collection(addGroupSuffix(d.name))
	}

	return db.Collection(addJobsSuffix(d.name))
}

func (d *mongoDriver) setupDB(ctx context.Context) error {
	indexes := []mongo.IndexModel{}
	if !d.opts.SkipQueueIndexBuilds {
		indexes = append(indexes, d.queueIndexes()...)
	}
	if !d.opts.SkipReportingIndexBuilds {
		indexes = append(indexes, d.reportingIndexes()...)
	}

	if len(indexes) > 0 {
		_, err := d.getCollection().Indexes().CreateMany(ctx, indexes)
		return fmt.Errorf("problem building indexes: %w", err)
	}

	return nil
}

func (d *mongoDriver) queueIndexes() []mongo.IndexModel {
	primary := bsonx.Doc{}

	if d.opts.UseGroups {
		primary = append(primary, bsonx.Elem{
			Key:   "group",
			Value: bsonx.Int32(1),
		})
	}

	primary = append(primary,
		bsonx.Elem{
			Key:   "status.completed",
			Value: bsonx.Int32(1),
		},
		bsonx.Elem{
			Key:   "status.in_prog",
			Value: bsonx.Int32(1),
		})

	if d.opts.Priority {
		primary = append(primary, bsonx.Elem{
			Key:   "priority",
			Value: bsonx.Int32(1),
		})
	}

	if d.opts.CheckWaitUntil {
		primary = append(primary, bsonx.Elem{
			Key:   "time_info.wait_until",
			Value: bsonx.Int32(1),
		})
	} else if d.opts.CheckDispatchBy {
		primary = append(primary, bsonx.Elem{
			Key:   "time_info.dispatch_by",
			Value: bsonx.Int32(1),
		})
	}

	indexes := []mongo.IndexModel{
		{
			Keys: primary,
		},
		{
			Keys: bsonx.Doc{
				{
					Key:   "scopes",
					Value: bsonx.Int32(1),
				},
			},
			Options: options.Index().SetSparse(true).SetUnique(true),
		},
	}

	if d.opts.TTL > 0 {
		ttl := int32(d.opts.TTL / time.Second)
		indexes = append(indexes, mongo.IndexModel{
			Keys: bsonx.Doc{
				{
					Key:   "time_info.created",
					Value: bsonx.Int32(1),
				},
			},
			Options: options.Index().SetExpireAfterSeconds(ttl),
		})
	}

	return indexes
}

func (d *mongoDriver) reportingIndexes() []mongo.IndexModel {
	indexes := []mongo.IndexModel{}
	completedInProgModTs := bsonx.Doc{}
	completedEnd := bsonx.Doc{}
	completedCreated := bsonx.Doc{}
	typeCompletedInProgModTs := bsonx.Doc{}
	typeCompletedEnd := bsonx.Doc{}

	if d.opts.UseGroups {
		completedInProgModTs = append(completedInProgModTs, bsonx.Elem{
			Key:   "group",
			Value: bsonx.Int32(1),
		})
		completedEnd = append(completedEnd, bsonx.Elem{
			Key:   "group",
			Value: bsonx.Int32(1),
		})
		completedCreated = append(completedCreated, bsonx.Elem{
			Key:   "group",
			Value: bsonx.Int32(1),
		})
		typeCompletedInProgModTs = append(typeCompletedInProgModTs, bsonx.Elem{
			Key:   "group",
			Value: bsonx.Int32(1),
		})
		typeCompletedEnd = append(typeCompletedEnd, bsonx.Elem{
			Key:   "group",
			Value: bsonx.Int32(1),
		})
	}

	completedInProgModTs = append(completedInProgModTs,
		bsonx.Elem{
			Key:   "status.completed",
			Value: bsonx.Int32(1),
		},
		bsonx.Elem{
			Key:   "status.in_prog",
			Value: bsonx.Int32(1),
		},
		bsonx.Elem{
			Key:   "status.mod_ts",
			Value: bsonx.Int32(1),
		},
	)
	indexes = append(indexes, mongo.IndexModel{Keys: completedInProgModTs})

	completedEnd = append(completedEnd,
		bsonx.Elem{
			Key:   "status.completed",
			Value: bsonx.Int32(1),
		},
		bsonx.Elem{
			Key:   "time_info.end",
			Value: bsonx.Int32(1),
		},
	)
	indexes = append(indexes, mongo.IndexModel{Keys: completedEnd})

	completedCreated = append(completedCreated,
		bsonx.Elem{
			Key:   "status.completed",
			Value: bsonx.Int32(1),
		},
		bsonx.Elem{
			Key:   "time_info.created",
			Value: bsonx.Int32(1),
		},
	)
	indexes = append(indexes, mongo.IndexModel{Keys: completedCreated})

	typeCompletedInProgModTs = append(typeCompletedInProgModTs,
		bsonx.Elem{
			Key:   "type",
			Value: bsonx.Int32(1),
		},
		bsonx.Elem{
			Key:   "status.completed",
			Value: bsonx.Int32(1),
		},
		bsonx.Elem{
			Key:   "status.in_prog",
			Value: bsonx.Int32(1),
		},
		bsonx.Elem{
			Key:   "status.mod_ts",
			Value: bsonx.Int32(1),
		},
	)
	indexes = append(indexes, mongo.IndexModel{Keys: typeCompletedInProgModTs})

	typeCompletedEnd = append(typeCompletedEnd,
		bsonx.Elem{
			Key:   "type",
			Value: bsonx.Int32(1),
		},
		bsonx.Elem{
			Key:   "status.completed",
			Value: bsonx.Int32(1),
		},
		bsonx.Elem{
			Key:   "time_info.end",
			Value: bsonx.Int32(1),
		},
	)
	indexes = append(indexes, mongo.IndexModel{Keys: typeCompletedEnd})

	return indexes
}

func (d *mongoDriver) Close() {
	if d.canceler != nil {
		d.canceler()
	}
}

func (d *mongoDriver) getIDFromName(name string) string {
	if d.opts.UseGroups {
		return fmt.Sprintf("%s.%s", d.opts.GroupName, name)
	}

	return name
}

func (d *mongoDriver) processNameForUsers(j *registry.JobInterchange) {
	if !d.opts.UseGroups {
		return
	}

	j.Name = j.Name[len(d.opts.GroupName)+1:]
}

func (d *mongoDriver) processJobForGroup(j *registry.JobInterchange) {
	if !d.opts.UseGroups {
		return
	}

	j.Group = d.opts.GroupName
	j.Name = d.getIDFromName(j.Name)
}

func (d *mongoDriver) modifyQueryForGroup(q bson.M) {
	if !d.opts.UseGroups {
		return
	}

	q["group"] = d.opts.GroupName
}

func (d *mongoDriver) Get(ctx context.Context, name string) (amboy.Job, error) {
	j := &registry.JobInterchange{}

	res := d.getCollection().FindOne(ctx, bson.M{"_id": d.getIDFromName(name)})
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, amboy.MakeJobNotDefinedError(d.name, name)
		}

		return nil, fmt.Errorf("GET problem fetching '%s': %w", name, err)
	}

	if err := res.Decode(j); err != nil {
		return nil, fmt.Errorf("GET problem decoding '%s': %w", name, err)
	}

	d.processNameForUsers(j)

	output, err := j.Resolve(d.opts.Unmarshaler)
	if err != nil {
		return nil, fmt.Errorf("GET problem converting '%s' to job object: %w", name, err)
	}

	return output, nil
}

func (d *mongoDriver) Put(ctx context.Context, j amboy.Job) error {
	job, err := registry.MakeJobInterchange(j, d.opts.Marshler)
	if err != nil {
		return fmt.Errorf("problem converting job to interchange format: %w", err)
	}

	d.processJobForGroup(job)

	if _, err = d.getCollection().InsertOne(ctx, job); err != nil {
		if isMongoDupKey(err) {
			return amboy.NewDuplicateJobErrorf("job '%s' already exists", j.ID())
		}

		return fmt.Errorf("problem saving new job %s: %w", j.ID(), err)
	}

	return nil
}

func (d *mongoDriver) getAtomicQuery(jobName string, modCount int) bson.M {
	d.mu.RLock()
	defer d.mu.RUnlock()
	owner := d.instanceID
	timeoutTs := time.Now().Add(-d.LockTimeout())

	return bson.M{
		"_id": jobName,
		"$or": []bson.M{
			// owner and modcount should match, which
			// means there's an active lock but we own it.
			//
			// The modcount is +1 in the case that we're
			// looking to update and update the modcount
			// (rather than just save, as in the Complete
			// case).
			{
				"status.owner":     owner,
				"status.mod_count": bson.M{"$in": []int{modCount, modCount - 1}},
				"status.mod_ts":    bson.M{"$gt": timeoutTs},
			},
			// modtime is older than the lock timeout,
			// regardless of what the other data is,
			{"status.mod_ts": bson.M{"$lte": timeoutTs}},
		},
	}
}

func isMongoDupKey(err error) bool {
	we := &mongo.WriteException{}
	if !errors.As(err, we) {
		return false
	}
	if we.WriteConcernError != nil {
		wce := we.WriteConcernError
		return wce.Code == 11000 || wce.Code == 11001 || wce.Code == 12582 || wce.Code == 16460 && strings.Contains(wce.Message, " E11000 ")
	}
	if we.WriteErrors != nil && len(we.WriteErrors) > 0 {
		for _, wErr := range we.WriteErrors {
			if wErr.Code == 11000 {
				return true
			}
		}
	}
	return false
}

func (d *mongoDriver) Save(ctx context.Context, j amboy.Job) error {
	job, err := d.prepareInterchange(j)
	if err != nil {
		return err
	}

	job.Scopes = j.Scopes()

	return d.doUpdate(ctx, job)
}

func (d *mongoDriver) Complete(ctx context.Context, j amboy.Job) error {
	job, err := d.prepareInterchange(j)
	if err != nil {
		return err
	}

	job.Scopes = nil

	return d.doUpdate(ctx, job)
}

func (d *mongoDriver) prepareInterchange(j amboy.Job) (*registry.JobInterchange, error) {
	stat := j.Status()
	stat.ErrorCount = len(stat.Errors)
	stat.ModificationTime = time.Now()
	j.SetStatus(stat)

	job, err := registry.MakeJobInterchange(j, d.opts.Marshler)
	if err != nil {
		return nil, fmt.Errorf("problem converting job to interchange format: %w", err)
	}
	return job, nil
}

func (d *mongoDriver) doUpdate(ctx context.Context, job *registry.JobInterchange) error {
	d.processJobForGroup(job)
	query := d.getAtomicQuery(job.Name, job.Status.ModificationCount)
	res, err := d.getCollection().ReplaceOne(ctx, query, job)
	if err != nil {
		return fmt.Errorf("problem saving document %s: %+v: %w", job.Name, res, err)
	}

	if res.MatchedCount == 0 {
		return fmt.Errorf("problem saving job [id=%s, matched=%d, modified=%d]", job.Name, res.MatchedCount, res.ModifiedCount)
	}
	return nil
}

func (d *mongoDriver) Jobs(ctx context.Context) <-chan amboy.Job {
	output := make(chan amboy.Job)
	go func() {
		defer close(output)
		defer recovery.LogStackTraceAndContinue("jobs iterator", "amboy.queue.mdbq", d.instanceID)
		q := bson.M{}
		d.modifyQueryForGroup(q)

		iter, err := d.getCollection().Find(ctx, q, options.Find().SetSort(bson.M{"status.mod_ts": -1}))
		if err != nil {
			d.log.Warning(message.WrapError(err, message.Fields{
				"id":        d.instanceID,
				"service":   "amboy.queue.mdb",
				"is_group":  d.opts.UseGroups,
				"group":     d.opts.GroupName,
				"operation": "job iterator",
				"message":   "problem with query",
			}))
			return
		}
		var job amboy.Job
		for iter.Next(ctx) {
			j := &registry.JobInterchange{}
			if err = iter.Decode(j); err != nil {
				d.log.Warning(message.WrapError(err, message.Fields{
					"id":        d.instanceID,
					"service":   "amboy.queue.mdb",
					"is_group":  d.opts.UseGroups,
					"group":     d.opts.GroupName,
					"operation": "job iterator",
					"message":   "problem reading job from cursor",
				}))

				continue
			}

			d.processNameForUsers(j)

			job, err = j.Resolve(d.opts.Unmarshaler)
			if err != nil {
				d.log.Warning(message.WrapError(err, message.Fields{
					"id":        d.instanceID,
					"service":   "amboy.queue.mdb",
					"operation": "job iterator",
					"is_group":  d.opts.UseGroups,
					"group":     d.opts.GroupName,
					"message":   "problem converting job obj",
				}))
				continue
			}

			output <- job
		}

		d.log.Error(message.WrapError(iter.Err(), message.Fields{
			"id":        d.instanceID,
			"service":   "amboy.queue.mdb",
			"is_group":  d.opts.UseGroups,
			"group":     d.opts.GroupName,
			"operation": "job iterator",
			"message":   "database interface error",
		}))
	}()
	return output
}

func (d *mongoDriver) getNextQuery() bson.M {
	d.mu.RLock()
	lockTimeout := d.LockTimeout()
	d.mu.RUnlock()
	now := time.Now()
	qd := bson.M{
		"status.completed": false,
		"$or": []bson.M{
			{
				"status.in_prog": false,
			},
			{
				"status.in_prog": true,
				"status.mod_ts":  bson.M{"$lte": now.Add(-lockTimeout)},
			},
		},
	}

	d.modifyQueryForGroup(qd)

	timeLimits := bson.M{}
	if d.opts.CheckWaitUntil {
		timeLimits["time_info.wait_until"] = bson.M{"$lte": now}
	}
	if d.opts.CheckDispatchBy {
		timeLimits["$or"] = []bson.M{
			{"time_info.dispatch_by": bson.M{"$gt": now}},
			{"time_info.dispatch_by": time.Time{}},
		}
	}
	if len(timeLimits) > 0 {
		qd = bson.M{"$and": []bson.M{qd, timeLimits}}
	}
	return qd
}

func (d *mongoDriver) Next(ctx context.Context) amboy.Job {
	var (
		qd             bson.M
		job            amboy.Job
		misses         int64
		dispatchMisses int64
		dispatchSkips  int64
	)

	startAt := time.Now()
	defer func() {
		d.log.WarningWhen(
			time.Since(startAt) > time.Second,
			message.Fields{
				"duration_secs": time.Since(startAt).Seconds(),
				"service":       "amboy.queue.mdb",
				"operation":     "next job",
				"attempts":      dispatchMisses,
				"skips":         dispatchSkips,
				"misses":        misses,
				"dispatched":    job != nil,
				"message":       "slow job dispatching operation",
				"id":            d.instanceID,
				"is_group":      d.opts.UseGroups,
				"group":         d.opts.GroupName,
			})
	}()

	opts := options.Find()
	if d.opts.Priority {
		opts.SetSort(bson.M{"priority": -1})
	}

	j := &registry.JobInterchange{}
	timer := time.NewTimer(0)
	defer timer.Stop()

RETRY:
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			misses++
			qd = d.getNextQuery()
			iter, err := d.getCollection().Find(ctx, qd, opts)
			if err != nil {
				d.log.Debug(message.WrapError(err, message.Fields{
					"id":            d.instanceID,
					"service":       "amboy.queue.mdb",
					"operation":     "retrieving next job",
					"message":       "problem generating query",
					"is_group":      d.opts.UseGroups,
					"group":         d.opts.GroupName,
					"duration_secs": time.Since(startAt).Seconds(),
				}))
				return nil
			}

		CURSOR:
			for iter.Next(ctx) {
				if err = iter.Decode(j); err != nil {
					d.log.Warning(message.WrapError(err, message.Fields{
						"id":            d.instanceID,
						"service":       "amboy.queue.mdb",
						"operation":     "converting next job",
						"message":       "problem reading document from cursor",
						"is_group":      d.opts.UseGroups,
						"group":         d.opts.GroupName,
						"duration_secs": time.Since(startAt).Seconds(),
					}))
					// try for the next thing in the iterator if we can
					continue CURSOR
				}

				job, err = j.Resolve(d.opts.Unmarshaler)
				if err != nil {
					d.log.Warning(message.WrapError(err, message.Fields{
						"id":            d.instanceID,
						"service":       "amboy.queue.mdb",
						"operation":     "converting document",
						"message":       "problem converting job from intermediate form",
						"is_group":      d.opts.UseGroups,
						"group":         d.opts.GroupName,
						"duration_secs": time.Since(startAt).Seconds(),
					}))
					// try for the next thing in the iterator if we can
					job = nil
					continue CURSOR
				}

				if job.TimeInfo().IsStale() {
					var res *mongo.DeleteResult

					res, err = d.getCollection().DeleteOne(ctx, bson.M{"_id": job.ID()})
					msg := message.Fields{
						"id":            d.instanceID,
						"service":       "amboy.queue.mdb",
						"num_deleted":   res.DeletedCount,
						"message":       "found stale job",
						"operation":     "job staleness check",
						"job":           job.ID(),
						"job_type":      job.Type().Name,
						"is_group":      d.opts.UseGroups,
						"group":         d.opts.GroupName,
						"duration_secs": time.Since(startAt).Seconds(),
					}
					d.log.Warning(message.WrapError(err, msg))
					d.log.NoticeWhen(err == nil, msg)
					job = nil
					continue CURSOR
				}

				if d.scopesInUse(ctx, job.Scopes()) {
					dispatchSkips++
					job = nil
					continue CURSOR
				}

				if !amboy.IsDispatchable(job.Status(), d.opts.LockTimeout) {
					dispatchSkips++
					job = nil
					continue CURSOR
				}

				if err = d.dispatcher.Dispatch(ctx, job); err != nil {
					dispatchMisses++
					d.log.DebugWhen(
						amboy.IsDispatchable(job.Status(), d.opts.LockTimeout),
						message.WrapError(err, message.Fields{
							"id":            d.instanceID,
							"service":       "amboy.queue.mdb",
							"operation":     "dispatch job",
							"job_id":        job.ID(),
							"job_type":      job.Type().Name,
							"scopes":        job.Scopes(),
							"stat":          job.Status(),
							"is_group":      d.opts.UseGroups,
							"group":         d.opts.GroupName,
							"dup_key":       isMongoDupKey(err),
							"duration_secs": time.Since(startAt).Seconds(),
						}),
					)
					job = nil
					continue CURSOR
				}
				break CURSOR
			}

			if err = iter.Err(); err != nil {
				d.log.Warning(message.WrapError(err, message.Fields{
					"id":            d.instanceID,
					"service":       "amboy.queue.mdb",
					"message":       "problem reported by iterator",
					"operation":     "retrieving next job",
					"is_group":      d.opts.UseGroups,
					"group":         d.opts.GroupName,
					"duration_secs": time.Since(startAt).Seconds(),
				}))
				return nil
			}

			if err = iter.Close(ctx); err != nil {
				d.log.Warning(message.WrapError(err, message.Fields{
					"id":        d.instanceID,
					"service":   "amboy.queue.mdb",
					"message":   "problem closing iterator",
					"operation": "retrieving next job",
					"is_group":  d.opts.UseGroups,
					"group":     d.opts.GroupName,
				}))
				return nil
			}

			if job != nil {
				break RETRY
			}

			timer.Reset(time.Duration(misses * rand.Int63n(int64(d.opts.WaitInterval))))
			continue RETRY
		}
	}
	return job
}

func (d *mongoDriver) scopesInUse(ctx context.Context, scopes []string) bool {
	if len(scopes) == 0 {
		return false
	}
	num, err := d.getCollection().CountDocuments(ctx, bson.M{"scopes": bson.M{"$in": scopes}})
	if err != nil {
		return false
	}

	return num > 0
}

func (d *mongoDriver) Stats(ctx context.Context) amboy.QueueStats {
	coll := d.getCollection()

	var numJobs int64
	var err error
	if d.opts.UseGroups {
		numJobs, err = coll.CountDocuments(ctx, bson.M{"group": d.opts.GroupName})
	} else {
		numJobs, err = coll.EstimatedDocumentCount(ctx)
	}

	d.log.Warning(message.WrapError(err, message.Fields{
		"id":         d.instanceID,
		"service":    "amboy.queue.mdb",
		"collection": coll.Name(),
		"operation":  "queue stats",
		"is_group":   d.opts.UseGroups,
		"group":      d.opts.GroupName,
		"message":    "problem counting all jobs",
	}))

	pendingQuery := bson.M{"status.completed": false}
	d.modifyQueryForGroup(pendingQuery)
	pending, err := coll.CountDocuments(ctx, pendingQuery)
	d.log.Warning(message.WrapError(err, message.Fields{
		"id":         d.instanceID,
		"service":    "amboy.queue.mdb",
		"collection": coll.Name(),
		"operation":  "queue stats",
		"is_group":   d.opts.UseGroups,
		"group":      d.opts.GroupName,
		"message":    "problem counting pending jobs",
	}))

	lockedQuery := bson.M{"status.completed": false, "status.in_prog": true}
	d.modifyQueryForGroup(lockedQuery)
	numLocked, err := coll.CountDocuments(ctx, lockedQuery)
	d.log.Warning(message.WrapError(err, message.Fields{
		"id":         d.instanceID,
		"service":    "amboy.queue.mdb",
		"collection": coll.Name(),
		"is_group":   d.opts.UseGroups,
		"group":      d.opts.GroupName,
		"operation":  "queue stats",
		"message":    "problem counting locked jobs",
	}))

	return amboy.QueueStats{
		Total:     int(numJobs),
		Pending:   int(pending),
		Completed: int(numJobs - pending),
		Running:   int(numLocked),
	}
}

func (d *mongoDriver) LockTimeout() time.Duration {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.opts.LockTimeout
}

func (d *mongoDriver) Dispatcher() queue.Dispatcher {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.dispatcher
}

func (d *mongoDriver) SetDispatcher(disp queue.Dispatcher) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dispatcher = disp
}

func (d *mongoDriver) Delete(ctx context.Context, id string) error {
	res, err := d.getCollection().DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return err
	}

	if res.DeletedCount == 0 {
		return fmt.Errorf("did not delete job '%s'", id)
	}

	return nil
}

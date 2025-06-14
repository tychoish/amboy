package mdbq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tychoish/amboy/management"
	"github.com/tychoish/fun/erc"
	"github.com/tychoish/fun/ers"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type dbQueueManager struct {
	client     *mongo.Client
	collection *mongo.Collection
	log        grip.Logger
	opts       DBQueueManagerOptions
}

// DBQueueManagerOptions describes the arguments to the operations to construct
// queue managers, and accommodates both group-backed queues and conventional
// queues.
type DBQueueManagerOptions struct {
	Name        string
	Group       string
	SingleGroup bool
	ByGroups    bool
	Options     MongoDBOptions
}

func (o *DBQueueManagerOptions) hasGroups() bool { return o.SingleGroup || o.ByGroups }

func (o *DBQueueManagerOptions) collName() string {
	if o.hasGroups() {
		return addGroupSuffix(o.Name)
	}

	return addJobsSuffix(o.Name)
}

// Validate checks the state of the manager configuration, preventing logically
// invalid options.
func (o *DBQueueManagerOptions) Validate() error {
	catcher := &erc.Collector{}
	catcher.When(o.SingleGroup && o.ByGroups, ers.Error("cannot specify conflicting group options"))
	catcher.When(o.Name == "", ers.Error("must specify queue name"))
	catcher.Add(o.Options.Validate())

	return catcher.Resolve()
}

// NewDBQueueManager produces a queue manager for (remote) queues that persist
// jobs in MongoDB. This implementation does not interact with the queue
// directly, and manages by interacting with the database directly.
func NewDBQueueManager(ctx context.Context, opts DBQueueManagerOptions) (management.Manager, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	client, err := mongo.NewClient(options.Client().ApplyURI(opts.Options.URI).SetConnectTimeout(time.Second))
	if err != nil {
		return nil, fmt.Errorf("problem constructing mongodb client: %w", err)
	}

	if err = client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("problem connecting to database: %w", err)
	}

	db, err := MakeDBQueueManager(ctx, opts, client)
	if err != nil {
		return nil, fmt.Errorf("problem building reporting interface: %w", err)
	}

	return db, nil
}

// MakeDBQueueManager make it possible to produce a queue manager with an
// existing database Connection. This operations runs the "ping" command and
// and will return an error if there is no session or no active server.
func MakeDBQueueManager(ctx context.Context, opts DBQueueManagerOptions, client *mongo.Client) (management.Manager, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	if client == nil {
		return nil, errors.New("cannot make a manager without a client")
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("could not establish a connection with the database: %w", err)
	}

	db := &dbQueueManager{
		opts:       opts,
		client:     client,
		log:        opts.Options.logger,
		collection: client.Database(opts.Options.DB).Collection(opts.collName()),
	}

	return db, nil
}

func (db *dbQueueManager) aggregateCounters(ctx context.Context, stages ...bson.M) ([]management.JobCounters, error) {
	cursor, err := db.collection.Aggregate(ctx, stages, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return nil, fmt.Errorf("problem running aggregation: %w", err)
	}

	catcher := &erc.Collector{}
	out := []management.JobCounters{}
	for cursor.Next(ctx) {
		val := management.JobCounters{}
		err = cursor.Decode(&val)
		if err != nil {
			catcher.Add(err)
			continue
		}
		out = append(out, val)
	}
	catcher.Add(cursor.Err())
	if !catcher.Ok() {
		return nil, catcher.Resolve()
	}

	return out, nil
}

func (db *dbQueueManager) aggregateRuntimes(ctx context.Context, stages ...bson.M) ([]management.JobRuntimes, error) {
	cursor, err := db.collection.Aggregate(ctx, stages, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return nil, fmt.Errorf("problem running aggregation: %w", err)
	}

	catcher := &erc.Collector{}
	out := []management.JobRuntimes{}
	for cursor.Next(ctx) {
		val := management.JobRuntimes{}
		err = cursor.Decode(&val)
		if err != nil {
			catcher.Add(err)
			continue
		}
		out = append(out, val)
	}
	catcher.Add(cursor.Err())
	if !catcher.Ok() {
		return nil, catcher.Resolve()
	}

	return out, nil
}

func (db *dbQueueManager) aggregateErrors(ctx context.Context, stages ...bson.M) ([]management.JobErrorsForType, error) {
	cursor, err := db.collection.Aggregate(ctx, stages, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return nil, fmt.Errorf("problem running aggregation: %w", err)
	}

	catcher := &erc.Collector{}
	out := []management.JobErrorsForType{}
	for cursor.Next(ctx) {
		val := management.JobErrorsForType{}
		err = cursor.Decode(&val)
		if err != nil {
			catcher.Add(err)
			continue
		}
		out = append(out, val)
	}
	catcher.Add(cursor.Err())
	if !catcher.Ok() {
		return nil, catcher.Resolve()
	}

	return out, nil
}

func (db *dbQueueManager) findJobs(ctx context.Context, match bson.M) ([]string, error) {
	group := bson.M{
		"_id":  nil,
		"jobs": bson.M{"$push": "$_id"},
	}

	if db.opts.ByGroups {
		group["_id"] = "$group"
	}

	stages := []bson.M{
		{"$match": match},
		{"$group": group},
	}

	out := []struct {
		Jobs []string `bson:"jobs"`
	}{}

	cursor, err := db.collection.Aggregate(ctx, stages, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return nil, fmt.Errorf("problem running query: %w", err)
	}

	if cursor.Next(ctx) {
		if err := cursor.Decode(&out); err != nil {
			return nil, fmt.Errorf("problem decoding result: %w", err)
		}
	}

	switch len(out) {
	case 0:
		return []string{}, nil
	case 1:
		return out[0].Jobs, nil
	default:
		return nil, fmt.Errorf("job results malformed with %d results", len(out))
	}
}

func (db *dbQueueManager) JobStatus(ctx context.Context, f management.StatusFilter) (*management.JobStatusReport, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}

	match := bson.M{}

	group := bson.M{
		"_id":   "$type",
		"count": bson.M{"$sum": 1},
	}

	if db.opts.SingleGroup {
		match["group"] = db.opts.Group
	} else if db.opts.ByGroups {
		group["_id"] = bson.M{"type": "$type", "group": "$group"}
	}

	switch f {
	case management.InProgress:
		match["status.in_prog"] = true
		match["status.completed"] = false
	case management.Pending:
		match["status.in_prog"] = false
		match["status.completed"] = false
	case management.Stale:
		match["status.in_prog"] = true
		match["status.mod_ts"] = bson.M{"$gt": time.Now().Add(-db.opts.Options.LockTimeout)}
		match["status.completed"] = false
	case management.Completed:
		match["status.in_prog"] = false
		match["status.completed"] = true
	case management.All:
		// pass (all jobs, completed and in progress)
	default:
		return nil, errors.New("invalid job status filter")
	}

	stages := []bson.M{
		{"$match": match},
		{"$group": group},
	}

	if db.opts.ByGroups {
		stages = append(stages, bson.M{"$project": bson.M{
			"_id":   "$_id.type",
			"count": "$count",
			"group": "$_id.group",
		}})
	}

	counters, err := db.aggregateCounters(ctx, stages...)

	if err != nil {
		return nil, err
	}

	return &management.JobStatusReport{
		Filter: string(f),
		Stats:  counters,
	}, nil
}

func (db *dbQueueManager) RecentTiming(ctx context.Context, window time.Duration, f management.RuntimeFilter) (*management.JobRuntimeReport, error) {
	if window <= time.Second {
		return nil, errors.New("must specify windows greater than one second")
	}

	if err := f.Validate(); err != nil {
		return nil, err
	}

	var match bson.M
	var group bson.M

	switch f {
	case management.Duration:
		match = bson.M{
			"status.completed": true,
			"time_info.end":    bson.M{"$gt": time.Now().Add(-window)},
		}
		group = bson.M{
			"_id": "$type",
			"duration": bson.M{"$avg": bson.M{
				"$multiply": []interface{}{bson.M{
					"$subtract": []string{"$time_info.end", "$time_info.start"}},
					1000000, // convert to nanoseconds
				},
			}}}
	case management.Latency:
		now := time.Now()
		match = bson.M{
			"$or": []bson.M{
				{"status.completed": false},
				{"status.in_prog": true},
			},
			"time_info.created": bson.M{"$gt": now.Add(-window)},
		}
		group = bson.M{
			"_id": "$type",
			"duration": bson.M{"$avg": bson.M{
				"$multiply": []interface{}{bson.M{
					"$subtract": []interface{}{"$time_info.start", "$time_info.created"}},
					1000000, // convert to nanoseconds
				}},
			}}
	case management.Running:
		now := time.Now()
		match = bson.M{
			"status.completed": false,
			"status.in_prog":   true,
		}
		group = bson.M{
			"_id": "$type",
			"duration": bson.M{"$avg": bson.M{
				"$multiply": []interface{}{bson.M{
					"$subtract": []interface{}{now, "$time_info.created"}},
					1000000, // convert to nanoseconds
				}},
			}}
	default:
		return nil, errors.New("invalid job runtime filter")
	}

	if db.opts.SingleGroup {
		match["group"] = db.opts.Group
	}

	stages := []bson.M{
		{"$match": match},
		{"$group": group},
	}

	if db.opts.ByGroups {
		stages = append(stages, bson.M{"$project": bson.M{
			"_id":      "$_id.type",
			"group":    "$_id.group",
			"duration": "$duration",
		}})
	}

	runtimes, err := db.aggregateRuntimes(ctx, stages...)
	if err != nil {
		return nil, err
	}
	return &management.JobRuntimeReport{
		Filter: string(f),
		Period: window,
		Stats:  runtimes,
	}, nil
}

func (db *dbQueueManager) JobIDsByState(ctx context.Context, jobType string, f management.StatusFilter) (*management.JobReportIDs, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}

	query := bson.M{
		"type":             jobType,
		"status.completed": false,
	}

	switch f {
	case management.InProgress:
		query["status.in_prog"] = true
	case management.Pending:
		query["status.in_prog"] = false
	case management.Stale:
		query["status.in_prog"] = true
		query["status.mod_ts"] = bson.M{"$gt": time.Now().Add(-db.opts.Options.LockTimeout)}
	case management.Completed:
		query["status.in_prog"] = false
		query["status.completed"] = true
	default:
		return nil, errors.New("invalid job status filter")
	}

	if db.opts.SingleGroup {
		query["group"] = db.opts.Group
	}

	ids, err := db.findJobs(ctx, query)
	if err != nil {
		return nil, err
	}

	return &management.JobReportIDs{
		Filter: string(f),
		Type:   jobType,
		IDs:    ids,
	}, nil
}

func (db *dbQueueManager) RecentErrors(ctx context.Context, window time.Duration, f management.ErrorFilter) (*management.JobErrorsReport, error) {
	if window <= time.Second {
		return nil, errors.New("must specify windows greater than one second")
	}

	if err := f.Validate(); err != nil {
		return nil, err

	}
	now := time.Now()

	match := bson.M{
		"status.completed": true,
		"status.err_count": bson.M{"$gt": 0},
		"time_info.end":    bson.M{"$gt": now.Add(-window)},
	}

	group := bson.M{
		"_id":     "$type",
		"count":   bson.M{"$sum": 1},
		"total":   bson.M{"$sum": "$status.err_count"},
		"average": bson.M{"$avg": "$status.err_count"},
		"errors":  bson.M{"$addToSet": "$status.errors"},
	}

	if db.opts.SingleGroup {
		match["group"] = db.opts.Group
	}
	if db.opts.ByGroups {
		group["_id"] = bson.M{"type": "$type", "group": "$group"}
	}

	stages := []bson.M{
		{"$match": match},
	}

	switch f {
	case management.UniqueErrors:
		stages = append(stages, bson.M{"$group": group})
	case management.AllErrors:
		stages = append(stages,
			bson.M{"$group": group},
			bson.M{"$unwind": "$status.errors"},
			bson.M{"$group": bson.M{
				"_id":     group["_id"],
				"count":   bson.M{"$first": "$count"},
				"total":   bson.M{"$first": "$total"},
				"average": bson.M{"$first": "$average"},
				"errors":  bson.M{"$push": "$errors"},
			}})
	case management.StatsOnly:
		delete(group, "errors")
		stages = append(stages, bson.M{"$group": group})
	default:
		return nil, errors.New("operation is not supported")
	}

	if db.opts.ByGroups {
		prj := bson.M{
			"_id":     "$_id.type",
			"group":   "$_id.group",
			"count":   "$count",
			"total":   "$total",
			"average": "$average",
		}
		if f != management.StatsOnly {
			prj["errors"] = "$errors"
		}

		stages = append(stages, bson.M{"$project": prj})
	}

	reports, err := db.aggregateErrors(ctx, stages...)
	if err != nil {
		return nil, err
	}

	return &management.JobErrorsReport{
		Period:         window,
		FilteredByType: false,
		Data:           reports,
	}, nil
}

func (db *dbQueueManager) RecentJobErrors(ctx context.Context, jobType string, window time.Duration, f management.ErrorFilter) (*management.JobErrorsReport, error) {
	if window <= time.Second {
		return nil, errors.New("must specify windows greater than one second")
	}

	if err := f.Validate(); err != nil {
		return nil, err
	}

	now := time.Now()

	match := bson.M{
		"type":             jobType,
		"status.completed": true,
		"status.err_count": bson.M{"$gt": 0},
		"time_info.end":    bson.M{"$gt": now.Add(-window)},
	}

	group := bson.M{
		"_id":     nil,
		"count":   bson.M{"$sum": 1},
		"total":   bson.M{"$sum": "$status.err_count"},
		"average": bson.M{"$avg": "$status.err_count"},
		"errors":  bson.M{"$push": "$statys.errors"},
	}

	if db.opts.SingleGroup {
		match["group"] = db.opts.Group
	}

	if db.opts.ByGroups {
		group["_id"] = bson.M{"type": "$type", "group": "$group"}
	}

	stages := []bson.M{
		{"$match": match},
	}

	switch f {
	case management.UniqueErrors:
		stages = append(stages, bson.M{"$group": group})
	case management.AllErrors:
		stages = append(stages,
			bson.M{"$group": group},
			bson.M{"$unwind": "$status.errors"},
			bson.M{"$group": bson.M{
				"_id":     group["_id"],
				"count":   bson.M{"$first": "$count"},
				"total":   bson.M{"$first": "$total"},
				"average": bson.M{"$first": "$average"},
				"errors":  bson.M{"$addToSet": "$errors"},
			}})
	case management.StatsOnly:
		delete(group, "errors")
		stages = append(stages, bson.M{"$group": group})
	default:
		return nil, errors.New("operation is not supported")

	}

	prj := bson.M{
		"_id":     "$_id.type",
		"count":   "$count",
		"total":   "$total",
		"average": "$average",
	}

	if db.opts.ByGroups {
		prj["group"] = "$_id.group"

		if f != management.StatsOnly {
			prj["errors"] = "$errors"
		}
	}

	stages = append(stages, bson.M{"$project": prj})

	reports, err := db.aggregateErrors(ctx, stages...)
	if err != nil {
		return nil, err
	}

	return &management.JobErrorsReport{
		Period:         window,
		FilteredByType: true,
		Data:           reports,
	}, nil
}

func (*dbQueueManager) getUpdateStatement() bson.M {
	return bson.M{
		"$set":   bson.M{"status.completed": true},
		"$inc":   bson.M{"status.mod_count": 3},
		"$unset": bson.M{"scopes": 1},
	}
}

func (db *dbQueueManager) completeJobs(ctx context.Context, query bson.M, f management.StatusFilter) error {
	if db.opts.Group != "" {
		query["group"] = db.opts.Group
	}

	switch f {
	case management.Completed:
		return errors.New("cannot mark completed jobs complete")
	case management.InProgress:
		query["status.completed"] = false
		query["status.in_prog"] = true
	case management.Stale:
		query["status.in_prog"] = true
		query["status.mod_ts"] = bson.M{"$gt": time.Now().Add(-db.opts.Options.LockTimeout)}
	case management.Pending:
		query["status.completed"] = false
		query["status.in_prog"] = false
	case management.All:
		query["status.in_prog"] = false
	default:
		return errors.New("invalid job status filter")
	}

	res, err := db.collection.UpdateMany(ctx, query, db.getUpdateStatement())
	db.log.Info(message.Fields{
		"op":         "mark-jobs-complete",
		"collection": db.collection.Name(),
		"filter":     f,
		"modified":   res.ModifiedCount,
	})
	return fmt.Errorf("problem marking jobs complete: %w", err)
}

func (db *dbQueueManager) CompleteJob(ctx context.Context, name string) error {
	if db.opts.Group != "" {
		name = fmt.Sprintf("%s.%s", db.opts.Group, name)
	}
	query := bson.M{
		"_id":              name,
		"status.completed": false,
	}

	if _, err := db.collection.UpdateOne(ctx, query, db.getUpdateStatement()); err != nil {
		return fmt.Errorf("problem marking job with name '%s' complete: %w", name, err)
	}
	return nil
}

func (db *dbQueueManager) CompleteJobsByType(ctx context.Context, f management.StatusFilter, jobType string) error {
	return db.completeJobs(ctx, bson.M{"type": jobType}, f)
}

func (db *dbQueueManager) CompleteJobs(ctx context.Context, f management.StatusFilter) error {
	return db.completeJobs(ctx, bson.M{}, f)
}

func (db *dbQueueManager) PruneJobs(ctx context.Context, ts time.Time, limit int, f management.StatusFilter) (int, error) {
	if err := f.Validate(); err != nil {
		return 0, err
	}

	if limit > 1 {
		return 0, errors.New("limits of greater than 1 are not supported ")
	}

	match := bson.M{}

	if db.opts.SingleGroup {
		match["group"] = db.opts.Group
	}

	switch f {
	case management.InProgress:
		match["status.in_prog"] = true
		match["status.completed"] = false
		match["time_info.start"] = bson.M{"$lt": ts}
	case management.Pending:
		match["status.in_prog"] = false
		match["status.completed"] = false
		match["time_info.created"] = bson.M{"$lt": ts}
	case management.Stale:
		match["status.in_prog"] = true
		match["status.completed"] = false
		match["status.mod_ts"] = bson.M{"$gt": time.Now().Add(-db.opts.Options.LockTimeout)}
		match["time_info.start"] = bson.M{"$lt": ts}
	case management.Completed:
		match["status.in_prog"] = false
		match["status.completed"] = true
		match["time_info.end"] = bson.M{"$lt": ts}
	case management.All:
		match["time_info.created"] = bson.M{"$lt": ts}
	default:
		return 0, errors.New("invalid job status filter")
	}

	var (
		res *mongo.DeleteResult
		err error
	)

	if limit == 1 {
		res, err = db.collection.DeleteOne(ctx, match)
	} else {
		res, err = db.collection.DeleteMany(ctx, match)
	}

	if err != nil {
		return 0, err
	}

	return int(res.DeletedCount), nil
}

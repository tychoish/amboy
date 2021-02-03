package amzsqs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/cdr/amboy"
	"github.com/cdr/amboy/pool"
	"github.com/cdr/amboy/queue"
	"github.com/cdr/amboy/registry"
	"github.com/cdr/grip"
	"github.com/cdr/grip/logging"
	"github.com/cdr/grip/message"
	"github.com/cdr/grip/recovery"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// RandomString returns a cryptographically random string.
func randomString(x int) string {
	b := make([]byte, x)
	_, _ = rand.Read(b) // nolint
	return hex.EncodeToString(b)
}

const defaultRegion string = "us-east-1"

type sqsFIFOQueue struct {
	sqsClient  *sqs.SQS
	sqsURL     string
	id         string
	started    bool
	numRunning int
	dispatcher queue.Dispatcher
	tasks      struct { // map jobID to job information
		completed map[string]bool
		all       map[string]amboy.Job
	}
	runner amboy.Runner
	log    grip.Journaler
	mutex  sync.RWMutex
}

// Options holds the creation options. If region is not specified, it
// defaults to "us-east-1" and if the journaler is not specified, the
// global journal is used.
type Options struct {
	Name       string
	Region     string
	NumWorkers int
	Logger     grip.Journaler
}

func (opts *Options) Validate() error {
	if opts.Logger == nil {
		opts.Logger = logging.MakeGrip(grip.GetSender())
	}

	if opts.Region == "" {
		opts.Region = defaultRegion
	}

	catcher := grip.NewBasicCatcher()
	catcher.NewWhen(opts.Name == "", "must specify a name")
	catcher.NewWhen(opts.NumWorkers <= 0, "must specify > 1 workers")
	return catcher.Resolve()
}

// NewFifoQueue constructs a AWS SQS backed Queue
// implementation. This queue, generally is ephemeral: tasks are
// removed from the queue, and therefore may not handle jobs across
// restarts.
func NewFifoQueue(opts *Options) (amboy.Queue, error) {
	if err := opts.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	q := &sqsFIFOQueue{
		sqsClient: sqs.New(session.Must(session.NewSession(&aws.Config{
			Region: aws.String(opts.Region),
		}))),
		id: fmt.Sprintf("queue.remote.sqs.fifo..%s", uuid.New().String()),
	}

	q.tasks.completed = make(map[string]bool)
	q.tasks.all = make(map[string]amboy.Job)
	q.runner = pool.NewLocalWorkers(&pool.WorkerOptions{NumWorkers: opts.NumWorkers, Queue: q, Logger: opts.Logger})
	q.dispatcher = queue.NewDispatcher(q, opts.Logger)
	result, err := q.sqsClient.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(fmt.Sprintf("%s.fifo", opts.Name)),
		Attributes: map[string]*string{
			"FifoQueue": aws.String("true"),
		},
	})
	if err != nil {
		return nil, errors.Wrapf(err, "creating queue: %s", opts.Name)
	}
	q.sqsURL = *result.QueueUrl
	return q, nil
}

func (q *sqsFIFOQueue) ID() string {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return q.id
}

func (q *sqsFIFOQueue) Put(ctx context.Context, j amboy.Job) error {
	name := j.ID()

	j.UpdateTimeInfo(amboy.JobTimeInfo{
		Created: time.Now(),
	})

	if err := j.TimeInfo().Validate(); err != nil {
		return errors.Wrap(err, "invalid job timeinfo")
	}

	q.mutex.Lock()
	defer q.mutex.Unlock()

	if !q.started {
		return errors.Errorf("cannot put job %s; queue not started", name)
	}

	if _, ok := q.tasks.all[name]; ok {
		return amboy.NewDuplicateJobErrorf("cannot add %s because duplicate job already exists", name)
	}

	dedupID := strings.Replace(j.ID(), " ", "", -1) //remove all spaces
	curStatus := j.Status()
	curStatus.ID = dedupID
	j.SetStatus(curStatus)
	jobItem, err := registry.MakeJobInterchange(j, json.Marshal)
	if err != nil {
		return errors.Wrap(err, "Error converting job in Put")
	}
	job, err := json.Marshal(jobItem)
	if err != nil {
		return errors.Wrap(err, "Error marshalling job to JSON in Put")
	}
	input := &sqs.SendMessageInput{
		MessageBody:            aws.String(string(job)),
		QueueUrl:               aws.String(q.sqsURL),
		MessageGroupId:         aws.String(randomString(16)),
		MessageDeduplicationId: aws.String(dedupID),
	}
	_, err = q.sqsClient.SendMessageWithContext(ctx, input)

	if err != nil {
		return errors.Wrap(err, "Error sending message in Put")
	}
	q.tasks.all[name] = j
	return nil
}

func (q *sqsFIFOQueue) Save(ctx context.Context, j amboy.Job) error {
	name := j.ID()

	q.mutex.Lock()
	defer q.mutex.Unlock()

	if !q.started {
		return errors.Errorf("cannot save job %s; queue not started", name)
	}

	if _, ok := q.tasks.all[name]; !ok {
		return errors.Errorf("cannot save '%s' because a job does not exist with that name", name)
	}

	q.tasks.all[name] = j
	return nil

}

// Returns the next job in the queue. These calls are
// blocking, but may be interrupted with a canceled context.
func (q *sqsFIFOQueue) Next(ctx context.Context) (amboy.Job, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}

	messageOutput, err := q.sqsClient.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(q.sqsURL),
	})
	if err != nil {
		return nil, errors.WithStack(err)
	} else if len(messageOutput.Messages) == 0 {
		q.log.Debugf("No messages received in Next from %s", q.sqsURL)
		return nil, errors.New("no dispatchable jobs")
	}
	message := messageOutput.Messages[0]
	_, err = q.sqsClient.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.sqsURL),
		ReceiptHandle: message.ReceiptHandle,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var jobItem *registry.JobInterchange
	err = json.Unmarshal([]byte(*message.Body), jobItem)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	job, err := jobItem.Resolve(json.Unmarshal)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if err := q.dispatcher.Dispatch(ctx, job); err != nil {
		_ = q.Put(ctx, job)
		return nil, errors.WithStack(err)
	}

	if job.TimeInfo().IsStale() {
		return nil, errors.New("cannot dispatch stale job")
	}

	q.numRunning++
	return job, nil
}

func (q *sqsFIFOQueue) Get(ctx context.Context, name string) (amboy.Job, error) {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	j, ok := q.tasks.all[name]
	if !ok {
		return nil, amboy.NewJobNotDefinedError(q, name)
	}
	return j, nil
}

func (q *sqsFIFOQueue) Info() amboy.QueueInfo {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return amboy.QueueInfo{
		Started:     q.started,
		LockTimeout: amboy.LockTimeout,
	}
}

// Used to mark a Job complete and remove it from the pending
// work of the queue.
func (q *sqsFIFOQueue) Complete(ctx context.Context, job amboy.Job) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	name := job.ID()
	if err := q.dispatcher.Complete(ctx, job); err != nil {
		return errors.WithStack(err)
	}
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if err := ctx.Err(); err != nil {
		q.log.Notice(message.Fields{
			"message":   "Did not complete job because context cancelled",
			"id":        name,
			"operation": "Complete",
		})
		return errors.WithStack(err)
	}

	q.tasks.completed[name] = true
	savedJob := q.tasks.all[name]
	if savedJob != nil {
		savedJob.SetStatus(job.Status())
		savedJob.UpdateTimeInfo(job.TimeInfo())
	}
	return nil
}

// Returns a channel that produces completed Job objects.
func (q *sqsFIFOQueue) Jobs(ctx context.Context) <-chan amboy.Job {
	results := make(chan amboy.Job)

	go func() {
		q.mutex.RLock()
		defer q.mutex.RUnlock()
		defer close(results)
		defer recovery.LogStackTraceAndContinue("cancelled context in Results")
		for name, job := range q.tasks.all {
			if _, ok := q.tasks.completed[name]; ok {
				select {
				case <-ctx.Done():
					return
				case results <- job:
					continue
				}
			}
		}
	}()
	return results
}

// Returns an object that contains statistics about the
// current state of the Queue.
func (q *sqsFIFOQueue) Stats(ctx context.Context) amboy.QueueStats {
	q.mutex.RLock()
	defer q.mutex.RUnlock()
	s := amboy.QueueStats{
		Completed: len(q.tasks.completed),
	}
	s.Running = q.numRunning - s.Completed

	output, err := q.sqsClient.GetQueueAttributesWithContext(ctx,
		&sqs.GetQueueAttributesInput{
			AttributeNames: []*string{aws.String("ApproximateNumberOfMessages"),
				aws.String("ApproximateNumberOfMessagesNotVisible")},
			QueueUrl: aws.String(q.sqsURL),
		})
	if err != nil {
		return s
	}

	numMsgs, _ := strconv.Atoi(*output.Attributes["ApproximateNumberOfMessages"])                   // nolint
	numMsgsInFlight, _ := strconv.Atoi(*output.Attributes["ApproximateNumberOfMessagesNotVisible"]) // nolint

	s.Pending = numMsgs + numMsgsInFlight
	s.Total = s.Pending + s.Completed

	return s
}

// Getter for the Runner implementation embedded in the Queue
// instance.
func (q *sqsFIFOQueue) Runner() amboy.Runner {
	return q.runner
}

func (q *sqsFIFOQueue) SetRunner(r amboy.Runner) error {
	if q.Info().Started {
		return errors.New("cannot change runners after starting")
	}

	q.runner = r
	return nil
}

// Begins the execution of the job Queue, using the embedded
// Runner.
func (q *sqsFIFOQueue) Start(ctx context.Context) error {
	if q.Info().Started {
		return nil
	}
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.started = true
	err := q.runner.Start(ctx)
	if err != nil {
		return errors.Wrap(err, "problem starting runner")
	}
	return nil
}

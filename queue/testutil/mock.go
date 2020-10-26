package testutil

import (
	"context"
	"sync"
	"time"

	"github.com/deciduosity/amboy"
	"github.com/deciduosity/amboy/dependency"
	"github.com/deciduosity/amboy/job"
	"github.com/deciduosity/amboy/registry"
	"github.com/google/uuid"
)

type counterCacheImpl struct {
	cache map[string]Counters
	mutex sync.Mutex
}

var counterCache *counterCacheImpl

func init() {
	counterCache = &counterCacheImpl{
		cache: map[string]Counters{},
	}

	registry.AddJobType("mock", NewMockJob)
	registry.AddJobType("sleep", func() amboy.Job { return newSleepJob() })
}

type CounterCache interface {
	Put(string, Counters)
	Get(string) Counters
	Reset(string)
}

func GetCounterCache() CounterCache {
	return counterCache
}

func (cc *counterCacheImpl) Get(name string) Counters {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()

	count, ok := cc.cache[name]
	if ok {
		return count
	}

	count = &mockJobRunEnv{}
	cc.cache[name] = count
	return count
}

func (cc *counterCacheImpl) Reset(name string) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()

	delete(cc.cache, name)
}

func (cc *counterCacheImpl) Put(name string, counter Counters) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()

	cc.cache[name] = counter
}

type Counters interface {
	Reset()
	Inc()
	Count() int
}

type mockJobRunEnv struct {
	runCount int
	mu       sync.Mutex
}

func (e *mockJobRunEnv) Inc() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.runCount++
}

func (e *mockJobRunEnv) Count() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.runCount
}

func (e *mockJobRunEnv) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.runCount = 0
}

//
type mockJob struct {
	Test     string `bson:"test_name" json:"test_name" yaml:"test_name"`
	job.Base `bson:"job_base" json:"job_base" yaml:"job_base"`
}

func MakeMockJob(id string, name string) amboy.Job {
	j := NewMockJob().(*mockJob)
	j.Test = name
	j.SetID(id)
	return j
}

func NewMockJob() amboy.Job {
	j := &mockJob{
		Base: job.Base{
			JobType: amboy.JobType{
				Name:    "mock",
				Version: 0,
			},
		},
	}
	j.SetDependency(dependency.NewAlways())
	return j
}

func (j *mockJob) Run(_ context.Context) {
	defer j.MarkComplete()

	GetCounterCache().Get(j.Test).Inc()
}

type sleepJob struct {
	Sleep time.Duration
	job.Base
}

func NewSleepJob(dur time.Duration) amboy.Job {
	j := newSleepJob()
	j.Sleep = dur
	return j
}

func newSleepJob() *sleepJob {
	j := &sleepJob{
		Base: job.Base{
			JobType: amboy.JobType{
				Name:    "sleep",
				Version: 0,
			},
		},
	}
	j.SetDependency(dependency.NewAlways())
	j.SetID(uuid.New().String())
	return j
}

func (j *sleepJob) Run(ctx context.Context) {
	defer j.MarkComplete()

	if j.Sleep == 0 {
		return
	}

	timer := time.NewTimer(j.Sleep)
	defer timer.Stop()

	select {
	case <-timer.C:
		return
	case <-ctx.Done():
		return
	}
}

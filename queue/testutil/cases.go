package testutil

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/deciduosity/amboy"
	"github.com/deciduosity/amboy/job"
	"github.com/deciduosity/grip"
	"github.com/deciduosity/grip/level"
	"github.com/deciduosity/grip/send"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	grip.SetName("amboy.queue.tests")
	grip.Error(grip.SetSender(send.MakeNative()))

	lvl := grip.GetSender().Level()
	lvl.Threshold = level.Error
	_ = grip.GetSender().SetLevel(lvl)

	job.RegisterDefaultJobs()
}

func UnorderedTest(bctx context.Context, t *testing.T, test QueueTestCase, runner PoolTestCase, size SizeTestCase) {
	ctx, cancel := context.WithCancel(bctx)
	defer cancel()

	q, closer, err := test.Constructor(ctx, RandomID(), size.Size)
	require.NoError(t, err)
	defer func() { require.NoError(t, closer(ctx)) }()
	require.NoError(t, runner.SetPool(q, size.Size))

	if test.OrderedSupported && !test.OrderedStartsBefore {
		// pass
	} else {
		require.NoError(t, q.Start(ctx))
	}

	testNames := []string{"test", "second", "workers", "forty-two", "true", "false", ""}
	numJobs := size.Size * len(testNames)

	wg := &sync.WaitGroup{}

	for i := 0; i < size.Size; i++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			for _, name := range testNames {
				cmd := fmt.Sprintf("echo %s.%d", name, num)
				j := job.NewShellJob(cmd, "")
				assert.NoError(t, q.Put(ctx, j),
					fmt.Sprintf("with %d workers", num))
				_, ok := q.Get(ctx, j.ID())
				assert.True(t, ok)
			}
		}(i)
	}
	wg.Wait()
	if test.OrderedSupported && !test.OrderedStartsBefore {
		require.NoError(t, q.Start(ctx))
	}

	amboy.WaitInterval(ctx, q, 100*time.Millisecond)

	assert.Equal(t, numJobs, q.Stats(ctx).Total, fmt.Sprintf("with %d workers", size.Size))

	amboy.WaitInterval(ctx, q, 100*time.Millisecond)

	grip.Infof("workers complete for %d worker smoke test", size.Size)
	assert.Equal(t, numJobs, q.Stats(ctx).Completed, fmt.Sprintf("%+v", q.Stats(ctx)))
	for result := range q.Jobs(ctx) {
		require.True(t, result.Status().Completed, fmt.Sprintf("with %d workers", size.Size))

		// assert that we had valid time info persisted
		ti := result.TimeInfo()
		assert.NotZero(t, ti.Start)
		assert.NotZero(t, ti.End)
	}

	statCounter := 0
	for job := range q.Jobs(ctx) {
		statCounter++
		assert.True(t, job.ID() != "")
	}
	assert.Equal(t, numJobs, statCounter, fmt.Sprintf("want jobStats for every job"))

	grip.Infof("completed results check for %d worker smoke test", size.Size)
}

func OrderedTest(bctx context.Context, t *testing.T, test QueueTestCase, runner PoolTestCase, size SizeTestCase) {
	ctx, cancel := context.WithCancel(bctx)
	defer cancel()

	q, closer, err := test.Constructor(ctx, RandomID(), size.Size)
	require.NoError(t, err)
	defer func() { require.NoError(t, closer(ctx)) }()
	require.NoError(t, runner.SetPool(q, size.Size))

	var lastJobName string

	testNames := []string{"amboy", "cusseta", "jasper", "sardis", "dublin"}

	numJobs := size.Size / 2 * len(testNames)

	tempDir, err := ioutil.TempDir("", strings.Join([]string{"amboy-ordered-queue-smoke-test",
		uuid.New().String()}, "-"))
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	if test.OrderedStartsBefore {
		require.NoError(t, q.Start(ctx))
	}
	for i := 0; i < size.Size/2; i++ {
		for _, name := range testNames {
			fn := filepath.Join(tempDir, fmt.Sprintf("%s.%d", name, i))
			cmd := fmt.Sprintf("echo %s", fn)
			j := job.NewShellJob(cmd, fn)
			if lastJobName != "" {
				require.NoError(t, j.Dependency().AddEdge(lastJobName))
			}
			lastJobName = j.ID()

			require.NoError(t, q.Put(ctx, j))
		}
	}

	if !test.OrderedStartsBefore {
		require.NoError(t, q.Start(ctx))
	}

	require.Equal(t, numJobs, q.Stats(ctx).Total, fmt.Sprintf("with %d workers", size.Size))
	amboy.WaitInterval(ctx, q, 50*time.Millisecond)
	require.Equal(t, numJobs, q.Stats(ctx).Completed, fmt.Sprintf("%+v", q.Stats(ctx)))
	for result := range q.Jobs(ctx) {
		require.True(t, result.Status().Completed, fmt.Sprintf("with %d workers", size.Size))
	}

	statCounter := 0
	for job := range q.Jobs(ctx) {
		statCounter++
		require.True(t, job.ID() != "")
	}
	require.Equal(t, statCounter, numJobs)
}

func WaitUntilTest(bctx context.Context, t *testing.T, test QueueTestCase, runner PoolTestCase, size SizeTestCase) {
	ctx, cancel := context.WithTimeout(bctx, 2*time.Minute)
	defer cancel()

	q, closer, err := test.Constructor(ctx, RandomID(), size.Size)
	require.NoError(t, err)
	defer func() { require.NoError(t, closer(ctx)) }()
	require.NoError(t, runner.SetPool(q, size.Size))

	require.NoError(t, q.Start(ctx))

	testNames := []string{"test", "second", "workers", "forty-two", "true", "false"}

	sz := size.Size
	if sz > 16 {
		sz = 16
	} else if sz < 2 {
		sz = 2
	}
	numJobs := sz * len(testNames)
	wg := &sync.WaitGroup{}

	for i := 0; i < sz; i++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			for _, name := range testNames {
				cmd := fmt.Sprintf("echo %s.%d.default", name, num)
				j := job.NewShellJob(cmd, "")
				ti := j.TimeInfo()
				require.Zero(t, ti.WaitUntil)
				require.NoError(t, q.Put(ctx, j), fmt.Sprintf("(a) with %d workers", num))
				_, ok := q.Get(ctx, j.ID())
				require.True(t, ok)

				cmd = fmt.Sprintf("echo %s.%d.waiter", name, num)
				j2 := job.NewShellJob(cmd, "")
				j2.UpdateTimeInfo(amboy.JobTimeInfo{
					WaitUntil: time.Now().Add(time.Hour),
				})
				ti2 := j2.TimeInfo()
				require.NotZero(t, ti2.WaitUntil)
				require.NoError(t, q.Put(ctx, j2), fmt.Sprintf("(b) with %d workers", num))
				_, ok = q.Get(ctx, j2.ID())
				require.True(t, ok)
			}
		}(i)
	}
	wg.Wait()
	// waitC for things to finish
	const (
		interval = 100 * time.Millisecond
		maxTime  = 10 * time.Second
	)
	var dur time.Duration
	timer := time.NewTimer(interval)
	defer timer.Stop()
waitLoop:
	for {
		select {
		case <-ctx.Done():
			break waitLoop
		case <-timer.C:
			dur += interval
			stat := q.Stats(ctx)
			if stat.Completed >= numJobs {
				break waitLoop
			}

			if dur >= maxTime {
				break waitLoop
			}

			timer.Reset(interval)
		}
	}

	stats := q.Stats(ctx)
	require.Equal(t, numJobs*2, stats.Total, "%+v", stats)
	assert.Equal(t, numJobs, stats.Completed)

	completed := 0
	for result := range q.Jobs(ctx) {
		status := result.Status()
		ti := result.TimeInfo()

		if status.Completed {
			completed++
			require.True(t, ti.WaitUntil.IsZero(), "val=%s id=%s", ti.WaitUntil, result.ID())
		} else {
			require.False(t, ti.WaitUntil.IsZero(), "val=%s id=%s", ti.WaitUntil, result.ID())
		}
	}

	assert.Equal(t, numJobs, completed)
}

func DispatchBeforeTest(bctx context.Context, t *testing.T, test QueueTestCase, runner PoolTestCase, size SizeTestCase) {
	ctx, cancel := context.WithTimeout(bctx, 2*time.Minute)
	defer cancel()

	q, closer, err := test.Constructor(ctx, RandomID(), size.Size)
	require.NoError(t, err)
	defer func() { require.NoError(t, closer(ctx)) }()
	require.NoError(t, runner.SetPool(q, size.Size))

	require.NoError(t, q.Start(ctx))

	for i := 0; i < 2*size.Size; i++ {
		j := job.NewShellJob("ls", "")
		ti := j.TimeInfo()

		if i%2 == 0 {
			ti.DispatchBy = time.Now().Add(time.Second)
		} else {
			ti.DispatchBy = time.Now().Add(-time.Second)
		}
		j.UpdateTimeInfo(ti)
		require.NoError(t, q.Put(ctx, j))
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

waitLoop:
	for {
		select {
		case <-ctx.Done():
			break waitLoop
		case <-ticker.C:
			stat := q.Stats(ctx)
			if stat.Completed == size.Size {
				break waitLoop
			}
		}
	}

	stats := q.Stats(ctx)
	assert.Equal(t, 2*size.Size, stats.Total)
	assert.Equal(t, size.Size, stats.Completed)
}

func OneExecutionTest(bctx context.Context, t *testing.T, test QueueTestCase, runner PoolTestCase, size SizeTestCase) {
	if test.Name == "LocalOrdered" {
		t.Skip("topological sort deadlocks")
	}
	ctx, cancel := context.WithTimeout(bctx, 2*time.Minute)
	defer cancel()

	testID := RandomID()
	q, closer, err := test.Constructor(ctx, testID, size.Size)
	require.NoError(t, err)
	require.NoError(t, runner.SetPool(q, size.Size))

	defer func() { require.NoError(t, closer(ctx)) }()

	counter := GetCounterCache().Get(testID)
	counter.Reset()

	count := 40

	if !test.OrderedSupported || test.OrderedStartsBefore {
		require.NoError(t, q.Start(ctx))
	}

	for i := 0; i < count; i++ {
		j := MakeMockJob(fmt.Sprintf("%d.%d.mock.single-exec", i, job.GetNumber()), testID)
		assert.NoError(t, q.Put(ctx, j))
	}

	if test.OrderedSupported && !test.OrderedStartsBefore {
		require.NoError(t, q.Start(ctx))
	}

	amboy.WaitInterval(ctx, q, 100*time.Millisecond)
	assert.Equal(t, count, counter.Count())
}

func MultiExecutionTest(bctx context.Context, t *testing.T, test QueueTestCase, runner PoolTestCase, size SizeTestCase) {
	ctx, cancel := context.WithTimeout(bctx, 2*time.Minute)
	defer cancel()
	name := RandomID()
	qOne, closerOne, err := test.Constructor(ctx, name, size.Size)
	require.NoError(t, err)
	defer func() { require.NoError(t, closerOne(ctx)) }()
	qTwo, closerTwo, err := test.Constructor(ctx, name, size.Size)
	defer func() { require.NoError(t, closerTwo(ctx)) }()
	require.NoError(t, err)
	require.NoError(t, runner.SetPool(qOne, size.Size))
	require.NoError(t, runner.SetPool(qTwo, size.Size))

	assert.NoError(t, qOne.Start(ctx))
	assert.NoError(t, qTwo.Start(ctx))

	num := 200
	adderProcs := 4

	wg := &sync.WaitGroup{}
	for o := 0; o < adderProcs; o++ {
		wg.Add(1)
		go func(o int) {
			defer wg.Done()
			// add a bunch of jobs: half to one queue and half to the other.
			for i := 0; i < num; i++ {
				cmd := fmt.Sprintf("echo %d.%d", o, i)
				j := job.NewShellJob(cmd, "")
				if i%2 == 0 {
					assert.NoError(t, qOne.Put(ctx, j))
				} else {
					assert.NoError(t, qTwo.Put(ctx, j))
				}

			}
		}(o)
	}
	wg.Wait()

	num = num * adderProcs

	grip.Info("added jobs to queues")

	// wait for all jobs to complete.
	amboy.WaitInterval(ctx, qOne, 100*time.Millisecond)
	amboy.WaitInterval(ctx, qTwo, 100*time.Millisecond)

	// check that both queues see all jobs
	statsOne := qOne.Stats(ctx)
	statsTwo := qTwo.Stats(ctx)

	var shouldExit bool
	if !assert.Equal(t, num, statsOne.Total, "ONE: %+v", statsOne) {
		shouldExit = true
	}
	if !assert.Equal(t, num, statsTwo.Total, "TWO: %+v", statsTwo) {
		shouldExit = true
	}
	if shouldExit {
		return
	}

	// check that all the results in the queues are are completed,
	// and unique
	firstCount := 0
	results := make(map[string]struct{})
	for result := range qOne.Jobs(ctx) {
		firstCount++
		assert.True(t, result.Status().Completed)
		results[result.ID()] = struct{}{}
	}

	secondCount := 0
	// make sure that all of the results in the second queue match
	// the results in the first queue.
	for result := range qTwo.Jobs(ctx) {
		secondCount++
		assert.True(t, result.Status().Completed)
		results[result.ID()] = struct{}{}
	}

	assert.Equal(t, firstCount, secondCount)
	assert.Equal(t, len(results), firstCount)
}

func ManyQueueTest(bctx context.Context, t *testing.T, test QueueTestCase, runner PoolTestCase, size SizeTestCase) {
	ctx, cancel := context.WithCancel(bctx)
	defer cancel()

	driverID := RandomID()
	sz := size.Size
	if sz > 8 {
		sz = 8
	} else if sz < 2 {
		sz = 2
	}

	queues := []amboy.Queue{}
	for i := 0; i < sz; i++ {
		q, closer, err := test.Constructor(ctx, driverID, size.Size)
		require.NoError(t, err)
		defer func() { cancel(); require.NoError(t, closer(ctx)) }()

		require.NoError(t, q.Start(ctx))
		queues = append(queues, q)
	}

	const (
		inside  = 15
		outside = 10
	)

	counter := GetCounterCache().Get(driverID)
	counter.Reset()

	wg := &sync.WaitGroup{}
	for i := 0; i < size.Size; i++ {
		for ii := 0; ii < outside; ii++ {
			wg.Add(1)
			go func(f, s int) {
				defer wg.Done()
				for iii := 0; iii < inside; iii++ {
					j := MakeMockJob(fmt.Sprintf("%d-%d-%d-%d", f, s, iii, job.GetNumber()), driverID)
					assert.NoError(t, queues[0].Put(ctx, j))
				}
			}(i, ii)
		}
	}

	grip.Notice("waiting to add all jobs")
	wg.Wait()

	grip.Notice("waiting to run jobs")

	for _, q := range queues {
		amboy.WaitInterval(ctx, q, 20*time.Millisecond)
	}

	assert.Equal(t, size.Size*inside*outside, counter.Count())
}

func ScopedLockTest(bctx context.Context, t *testing.T, test QueueTestCase, runner PoolTestCase, size SizeTestCase) {
	ctx, cancel := context.WithTimeout(bctx, time.Minute)
	defer cancel()
	q, closer, err := test.Constructor(ctx, RandomID(), 2*size.Size)
	require.NoError(t, err)
	defer func() { require.NoError(t, closer(ctx)) }()
	require.NoError(t, runner.SetPool(q, size.Size*3))

	require.NoError(t, q.Start(ctx))

	for i := 0; i < 2*size.Size; i++ {
		j := newSleepJob()
		if i%2 == 0 {
			j.SetScopes([]string{"a"})
			j.Sleep = time.Hour
		}
		require.NoError(t, q.Put(ctx, j))
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

waitLoop:
	for {
		select {
		case <-ctx.Done():
			break waitLoop
		case <-ticker.C:
			stat := q.Stats(ctx)
			if stat.Completed >= size.Size {
				break waitLoop
			}
		}
	}

	time.Sleep(50 * time.Millisecond)
	stats := q.Stats(ctx)
	assert.Equal(t, 2*size.Size, stats.Total)
	assert.Equal(t, size.Size, stats.Completed)
}

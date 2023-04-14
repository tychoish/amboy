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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tychoish/amboy"
	"github.com/tychoish/amboy/job"
	"github.com/tychoish/grip"
)

func init() {
	job.RegisterDefaultJobs()
}

func UnorderedTest(bctx context.Context, t *testing.T, test QueueTestCase, runner PoolTestCase, size SizeTestCase) {
	ctx, cancel := context.WithTimeout(bctx, time.Minute)
	defer cancel()

	q, closer, err := test.Constructor(ctx, RandomID(), size.Size)
	require.NoError(t, err)
	defer func() { cancel(); require.NoError(t, closer(bctx)) }()
	if err := runner.SetPool(q, size.Size); err != nil {
		t.Fatal(err)
	}

	if test.OrderedSupported && !test.OrderedStartsBefore {
		// pass
	} else {
		if err := q.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}

	testNames := []string{"test", "second", "workers", "forty-two", "true", "false", ""}

	wg := &sync.WaitGroup{}

	sz := size.Size

	if sz > 4 {
		sz = 4
	}

	numJobs := sz * len(testNames)

	for i := 0; i < sz; i++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			for _, name := range testNames {
				cmd := fmt.Sprintf("echo %s.%d", name, num)
				j := job.NewShellJob(cmd, "")
				if err := q.Put(ctx, j); err != nil {
					t.Error(err)
				}
				if _, err := q.Get(ctx, j.ID()); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}
	wg.Wait()
	if test.OrderedSupported && !test.OrderedStartsBefore {
		if err := q.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}

	amboy.WaitInterval(ctx, q, 10*time.Millisecond)

	require.NoError(t, ctx.Err(), "test hit timedout")

	assert.Equal(t, numJobs, q.Stats(ctx).Total, fmt.Sprintf("with %d workers", size.Size))

	amboy.WaitInterval(ctx, q, 10*time.Millisecond)

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
	ctx, cancel := context.WithTimeout(bctx, time.Minute)
	defer cancel()

	q, closer, err := test.Constructor(ctx, RandomID(), size.Size)
	require.NoError(t, err)
	defer func() { require.NoError(t, closer(ctx)) }()
	if err := runner.SetPool(q, size.Size); err != nil {
		t.Fatal(err)
	}

	var lastJobName string

	testNames := []string{"amboy", "cusseta", "jasper", "sardis", "dublin"}

	numJobs := size.Size / 2 * len(testNames)

	tempDir, err := ioutil.TempDir("", strings.Join([]string{"amboy-ordered-queue-smoke-test",
		uuid.New().String()}, "-"))
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	if test.OrderedStartsBefore {
		if err := q.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < size.Size/2; i++ {
		for _, name := range testNames {
			fn := filepath.Join(tempDir, fmt.Sprintf("%s.%d", name, i))
			cmd := fmt.Sprintf("echo %s", fn)
			j := job.NewShellJob(cmd, fn)
			if lastJobName != "" {
				if err := j.Dependency().AddEdge(lastJobName); err != nil {
					t.Fatal(err)
				}
			}
			lastJobName = j.ID()

			if err := q.Put(ctx, j); err != nil {
				t.Fatal(err)
			}
		}
	}

	if !test.OrderedStartsBefore {
		if err := q.Start(ctx); err != nil {
			t.Fatal(err)
		}
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
	ctx, cancel := context.WithTimeout(bctx, time.Minute)
	defer cancel()

	q, closer, err := test.Constructor(ctx, RandomID(), size.Size)
	require.NoError(t, err)
	defer func() { cancel(); require.NoError(t, closer(bctx)) }()
	if err := runner.SetPool(q, size.Size); err != nil {
		t.Fatal(err)
	}

	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	testNames := []string{"test", "second", "workers", "forty-two", "true", "false"}

	sz := size.Size
	if sz > 8 {
		sz = 8
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
				_, err := q.Get(ctx, j.ID())
				require.NoError(t, err)

				cmd = fmt.Sprintf("echo %s.%d.waiter", name, num)
				j2 := job.NewShellJob(cmd, "")
				j2.UpdateTimeInfo(amboy.JobTimeInfo{
					WaitUntil: time.Now().Add(time.Hour),
				})
				ti2 := j2.TimeInfo()
				require.NotZero(t, ti2.WaitUntil)
				require.NoError(t, q.Put(ctx, j2), fmt.Sprintf("(b) with %d workers", num))
				_, err = q.Get(ctx, j2.ID())
				require.NoError(t, err)
			}
		}(i)
	}
	wg.Wait()
	// waitC for things to finish
	const (
		interval = 10 * time.Millisecond
		maxTime  = 5 * time.Second
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
	ctx, cancel := context.WithTimeout(bctx, time.Minute)
	defer cancel()

	q, closer, err := test.Constructor(ctx, RandomID(), size.Size)
	require.NoError(t, err)
	defer func() { cancel(); require.NoError(t, closer(bctx)) }()
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
	ctx, cancel := context.WithTimeout(bctx, time.Minute)
	defer cancel()

	testID := RandomID()
	q, closer, err := test.Constructor(ctx, testID, size.Size)
	require.NoError(t, err)
	require.NoError(t, runner.SetPool(q, size.Size))

	defer func() { cancel(); require.NoError(t, closer(bctx)) }()

	counter := GetCounterCache().Get(testID)
	counter.Reset()

	count := 40

	if !test.OrderedSupported || test.OrderedStartsBefore {
		require.NoError(t, q.Start(ctx))
	}

	for i := 0; i < count; i++ {
		j := MakeMockJob(fmt.Sprintf("%d.%d.mock.single-exec", i, job.GetNumber()), testID)
		if err := q.Put(ctx, j); err != nil {
			t.Fatal(err)
		}
	}

	if test.OrderedSupported && !test.OrderedStartsBefore {
		require.NoError(t, q.Start(ctx))
	}

	amboy.WaitInterval(ctx, q, 100*time.Millisecond)
	assert.Equal(t, count, counter.Count())
}

func MultiExecutionTest(bctx context.Context, t *testing.T, test QueueTestCase, runner PoolTestCase, size SizeTestCase) {
	ctx, cancel := context.WithTimeout(bctx, 90*time.Second)
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

	if err := qOne.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := qTwo.Start(ctx); err != nil {
		t.Fatal(err)
	}

	num := 50
	adderProcs := 2

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
					if err := qOne.Put(ctx, j); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := qTwo.Put(ctx, j); err != nil {
						t.Fatal(err)
					}
				}

			}
		}(o)
	}
	wg.Wait()

	num = num * adderProcs

	grip.Info("added jobs to queues")

	// wait for all jobs to complete.
	amboy.WaitInterval(ctx, qOne, 10*time.Millisecond)
	amboy.WaitInterval(ctx, qTwo, 10*time.Millisecond)

	require.NoError(t, ctx.Err(), "test timed out")

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
	ctx, cancel := context.WithTimeout(bctx, time.Minute)
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
		defer func() { require.NoError(t, closer(bctx)) }()

		require.NoError(t, q.Start(ctx))
		queues = append(queues, q)
	}
	defer cancel()

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
					if err := queues[0].Put(ctx, j); err != nil {
						t.Fatal(err)
					}
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
	defer func() { cancel(); require.NoError(t, closer(bctx)) }()
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

	timer := time.NewTimer(0)
	const interval = 10 * time.Millisecond
	defer timer.Stop()

waitLoop:
	for {
		select {
		case <-ctx.Done():
			break waitLoop
		case <-timer.C:
			stat := q.Stats(ctx)
			if stat.Completed >= size.Size {
				break waitLoop
			}
			timer.Reset(interval)
		}
	}

	time.Sleep(25 * time.Millisecond)
	stats := q.Stats(ctx)
	assert.Equal(t, 2*size.Size, stats.Total)
	assert.Equal(t, size.Size, stats.Completed)
}

func AbortTracking(bctx context.Context, t *testing.T, test QueueTestCase, runner PoolTestCase, size SizeTestCase) {
	ctx, cancel := context.WithTimeout(bctx, time.Minute)
	defer cancel()
	testID := RandomID()

	q, closer, err := test.Constructor(ctx, testID, size.Size)
	require.NoError(t, err)
	defer func() { cancel(); require.NoError(t, closer(bctx)) }()
	require.NoError(t, runner.SetPool(q, size.Size))

	if test.OrderedSupported && !test.OrderedStartsBefore {
		// pass
	} else {
		require.NoError(t, q.Start(ctx))
	}

	counter := GetCounterCache().Get(testID)
	counter.Reset()

	for i := 0; i < 2*size.Size; i++ {
		require.NoError(t, q.Put(ctx, MakeAbortedJob(fmt.Sprintf("%d.%d.mock.abortable", i, job.GetNumber()), testID)))
	}

	if test.OrderedSupported && !test.OrderedStartsBefore {
		require.NoError(t, q.Start(ctx))
	}

	// wait for the jobs to all run at least once, none of them
	// complete, which makes this awkward.
	time.Sleep(2 * time.Second)

	func() {
		nctx, ncancel := context.WithTimeout(ctx, 4*time.Second)
		defer ncancel()
		q.Close(nctx)
		time.Sleep(500 * time.Millisecond)
	}()

	stat := q.Stats(ctx)

	require.NoError(t, ctx.Err(), "main test context timed out")
	assert.Equal(t, 0, stat.Completed, "%+v", stat)
	assert.Equal(t, 2*size.Size, stat.Total, "%+v", stat)
	assert.True(t, 2*size.Size <= counter.Count(), "%+v, %d <= %d", stat, 2*size.Size, counter.Count())
	assert.Equal(t, stat, q.Stats(ctx))

	count := 0
	for job := range q.Jobs(ctx) {
		stat := job.Status()
		if stat.InProgress {
			count++
			continue
		}
		failed := false
		if assert.True(t, stat.Canceled, "%+v", stat) {
			failed = true
		} else if assert.False(t, stat.Completed) {
			failed = true
		}

		if failed {
			break
		}
	}

	assert.True(t, count <= 2, "most jobs should no longer be in progress, but there were %d", count)
}

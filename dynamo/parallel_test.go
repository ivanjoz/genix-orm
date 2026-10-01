package dynamo

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunInParallel(t *testing.T) {
	if err := runInParallel(0, func(int) error { t.Fatal("no task must run"); return nil }); err != nil {
		t.Fatalf("zero tasks: %v", err)
	}

	// Every task runs once, and never more than writeParallelism at a time.
	const taskCount = 35
	var ranTasks [taskCount]atomic.Int32
	var runningTasks, peakRunningTasks atomic.Int32
	err := runInParallel(taskCount, func(taskIndex int) error {
		running := runningTasks.Add(1)
		for peak := peakRunningTasks.Load(); running > peak && !peakRunningTasks.CompareAndSwap(peak, running); peak = peakRunningTasks.Load() {
		}
		time.Sleep(2 * time.Millisecond)
		runningTasks.Add(-1)
		ranTasks[taskIndex].Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for taskIndex := range ranTasks {
		if ranTasks[taskIndex].Load() != 1 {
			t.Fatalf("task %d ran %d times", taskIndex, ranTasks[taskIndex].Load())
		}
	}
	if peak := peakRunningTasks.Load(); peak > writeParallelism || peak < 2 {
		t.Fatalf("peak parallelism %d, want 2..%d", peak, writeParallelism)
	}

	// The errors of every failed task come back joined.
	firstErr, secondErr := errors.New("first"), errors.New("second")
	err = runInParallel(3, func(taskIndex int) error {
		return []error{firstErr, nil, secondErr}[taskIndex]
	})
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("joined error %v must hold both task errors", err)
	}
}

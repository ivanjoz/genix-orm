// Package utils holds what genix-orm's modules share and no database owns: the file store
// (filestore.go), Safe85 (safe85.go), SUnixTime (sunixtime.go) and bounded parallel calls
// (parallel.go). The dynamo backend and the dataframe module import it; scylla can too.
package utils

import (
	"errors"
	"sync"
)

// maxConcurrentTasks bounds the concurrent calls of one step (the conditional puts of
// PutManyIfVersion, the GroupBy counter updates, the ReprocessBlobs rewrites). It matches
// the AWS SDK's default of 10 idle connections per host, so a parallel step reuses warm connections
// instead of opening new TLS ones.
const maxConcurrentTasks = 10

// RunParallel runs task(0)..task(count-1), at most maxConcurrentTasks at a time, and returns their errors
// joined. The calls of one step are independent, so their order does not matter.
func RunParallel(count int, task func(index int) error) error {
	return RunParallelLimit(maxConcurrentTasks, count, task)
}

// RunParallelLimit is RunParallel with at most limit tasks at a time.
func RunParallelLimit(limit int, count int, task func(index int) error) error {
	if count == 1 {
		return task(0)
	}
	taskErrors := make([]error, count)
	parallelSlots := make(chan struct{}, limit)
	var pendingTasks sync.WaitGroup
	for index := 0; index < count; index++ {
		pendingTasks.Add(1)
		parallelSlots <- struct{}{}
		go func(taskIndex int) {
			defer func() { <-parallelSlots; pendingTasks.Done() }()
			taskErrors[taskIndex] = task(taskIndex)
		}(index)
	}
	pendingTasks.Wait()
	return errors.Join(taskErrors...)
}

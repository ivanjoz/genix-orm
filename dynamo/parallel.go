package dynamo

import (
	"errors"
	"sync"
)

// writeParallelism bounds the concurrent DynamoDB calls of one write step (the conditional puts of
// PutManyIfVersion, the GroupBy counter updates). It matches the AWS SDK's default of 10 idle
// connections per host, so a parallel step reuses warm connections instead of opening new TLS ones.
const writeParallelism = 10

// runInParallel runs task(0)..task(count-1), at most writeParallelism at a time, and returns their
// errors joined. The calls of one step are independent, so their order does not matter.
func runInParallel(count int, task func(index int) error) error {
	if count == 1 {
		return task(0)
	}
	taskErrors := make([]error, count)
	parallelSlots := make(chan struct{}, writeParallelism)
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

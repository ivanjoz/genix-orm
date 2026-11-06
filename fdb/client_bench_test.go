package fdb

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
)

// Step 0 microbenchmark: the socket hop (client -> fdbgate -> FDB). fdbgate/binding_bench_test.go
// runs the identical workloads with the Apple binding in-process, to isolate what the hop costs.
// Run: GENIX_FDB_BENCH=1 FDBGATE_SOCKET=... go test -run xxx -bench . -benchtime 3s

const (
	benchValueBytes = 100
	benchRangeRows  = 1000
	benchCommitRows = 1000
)

func skipUnlessBench(b *testing.B) {
	if os.Getenv("GENIX_FDB_BENCH") != "1" {
		b.Skip("set GENIX_FDB_BENCH=1")
	}
}

// runConcurrent splits b.N operations across workers goroutines.
func runConcurrent(b *testing.B, workers int, operation func(workerIndex, iteration int)) {
	b.ResetTimer()
	var waitGroup sync.WaitGroup
	for workerIndex := range workers {
		waitGroup.Go(func() {
			for iteration := workerIndex; iteration < b.N; iteration += workers {
				operation(workerIndex, iteration)
			}
		})
	}
	waitGroup.Wait()
}

func seedBenchRows(b *testing.B, client *Client) {
	value := bytes.Repeat([]byte{7}, benchValueBytes)
	mutations := []Mutation{{Kind: MutationSet, Key: []byte("bench/get"), Value: value}}
	for rowIndex := range benchRangeRows {
		mutations = append(mutations, Mutation{Kind: MutationSet, Key: []byte(fmt.Sprintf("bench/range/%05d", rowIndex)), Value: value})
	}
	if _, commitError := client.Commit(context.Background(), nil, mutations); commitError != nil {
		b.Fatal(commitError)
	}
}

func BenchmarkSocketHop(b *testing.B) {
	skipUnlessBench(b)
	client := dialForTest(b)
	seedBenchRows(b, client)
	ctx := context.Background()
	value := bytes.Repeat([]byte{7}, benchValueBytes)

	for _, workers := range []int{1, 64} {
		b.Run(fmt.Sprintf("get1/c%d", workers), func(b *testing.B) {
			keys := [][]byte{[]byte("bench/get")}
			runConcurrent(b, workers, func(_, _ int) {
				if _, _, getError := client.GetMany(ctx, keys, false); getError != nil {
					b.Error(getError)
				}
			})
		})
		b.Run(fmt.Sprintf("range1k/c%d", workers), func(b *testing.B) {
			ranges := []Range{{Begin: []byte("bench/range/"), End: []byte("bench/range0"), Limit: benchRangeRows}}
			runConcurrent(b, workers, func(_, _ int) {
				results, rangeError := client.GetRanges(ctx, ranges)
				if rangeError != nil {
					b.Error(rangeError)
				} else if results[0].Keys.Len() != benchRangeRows {
					b.Errorf("read %d rows", results[0].Keys.Len())
				}
			})
		})
		b.Run(fmt.Sprintf("commit1k/c%d", workers), func(b *testing.B) {
			// One key space per worker so concurrent commits never share keys.
			mutationsByWorker := make([][]Mutation, workers)
			for workerIndex := range workers {
				for rowIndex := range benchCommitRows {
					mutationsByWorker[workerIndex] = append(mutationsByWorker[workerIndex],
						Mutation{Kind: MutationSet, Key: []byte(fmt.Sprintf("bench/commit/%02d/%04d", workerIndex, rowIndex)), Value: value})
				}
			}
			runConcurrent(b, workers, func(workerIndex, _ int) {
				if _, commitError := client.Commit(ctx, nil, mutationsByWorker[workerIndex]); commitError != nil {
					b.Error(commitError)
				}
			})
		})
	}
}

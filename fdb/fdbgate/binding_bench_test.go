package main

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// The same workloads as ../client_bench_test.go, through the Apple binding in-process: the baseline
// the socket hop is compared against. It needs libfdb_c and the cluster file, so it runs inside the
// container: podman exec genix-bench-fdb env GENIX_FDB_BENCH=1 /usr/bin/fdbgate.test -test.bench .

const (
	benchValueBytes = 100
	benchRangeRows  = 1000
	benchCommitRows = 1000
)

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

func BenchmarkInProcessBinding(b *testing.B) {
	if os.Getenv("GENIX_FDB_BENCH") != "1" {
		b.Skip("set GENIX_FDB_BENCH=1")
	}
	fdb.MustAPIVersion(800)
	database, openError := fdb.OpenDatabase(envOrDefault("FDB_CLUSTER_FILE", "/var/fdb/fdb.cluster"))
	if openError != nil {
		b.Fatal(openError)
	}
	value := bytes.Repeat([]byte{7}, benchValueBytes)
	if _, seedError := database.Transact(func(transaction fdb.Transaction) (any, error) {
		transaction.Set(fdb.Key("bench/get"), value)
		for rowIndex := range benchRangeRows {
			transaction.Set(fdb.Key(fmt.Sprintf("bench/range/%05d", rowIndex)), value)
		}
		return nil, nil
	}); seedError != nil {
		b.Fatal(seedError)
	}

	for _, workers := range []int{1, 64} {
		b.Run(fmt.Sprintf("get1/c%d", workers), func(b *testing.B) {
			runConcurrent(b, workers, func(_, _ int) {
				if _, getError := database.ReadTransact(func(readTransaction fdb.ReadTransaction) (any, error) {
					return readTransaction.Get(fdb.Key("bench/get")).Get()
				}); getError != nil {
					b.Error(getError)
				}
			})
		})
		b.Run(fmt.Sprintf("range1k/c%d", workers), func(b *testing.B) {
			keyRange := fdb.KeyRange{Begin: fdb.Key("bench/range/"), End: fdb.Key("bench/range0")}
			runConcurrent(b, workers, func(_, _ int) {
				rows, rangeError := database.ReadTransact(func(readTransaction fdb.ReadTransaction) (any, error) {
					return readTransaction.GetRange(keyRange, fdb.RangeOptions{Limit: benchRangeRows, Mode: fdb.StreamingModeWantAll}).GetSliceWithError()
				})
				if rangeError != nil {
					b.Error(rangeError)
				} else if len(rows.([]fdb.KeyValue)) != benchRangeRows {
					b.Errorf("read %d rows", len(rows.([]fdb.KeyValue)))
				}
			})
		})
		b.Run(fmt.Sprintf("commit1k/c%d", workers), func(b *testing.B) {
			runConcurrent(b, workers, func(workerIndex, _ int) {
				if _, commitError := database.Transact(func(transaction fdb.Transaction) (any, error) {
					for rowIndex := range benchCommitRows {
						transaction.Set(fdb.Key(fmt.Sprintf("bench/commit/%02d/%04d", workerIndex, rowIndex)), value)
					}
					return nil, nil
				}); commitError != nil {
					b.Error(commitError)
				}
			})
		})
	}
}

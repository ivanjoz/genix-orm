package dataframe

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"unsafe"

	"github.com/ivanjoz/genix-orm/utils"
)

// recordScanningBackend is memoryBackend as a PartitionRecordScanner: it hands a rebuild the stored
// records, with a History only for the ones written after the rebuild's target.
type recordScanningBackend struct {
	*memoryBackend
	target int64
}

func (backend *recordScanningBackend) ScanPartitionRecords(partition int64, onPage func([]ScannedRecord) error) error {
	histories := partitionHistories(backend.sim.historiesAt(backend.readAt, -1), partition)
	stored := map[string]simWrite{}
	for _, write := range backend.sim.storedAt(backend.readAt) {
		stored[fmt.Sprint(write.record.EmployeeID)] = write
	}
	var page []ScannedRecord
	for i, history := range histories {
		write := stored[history.RecordKey]
		scanned := ScannedRecord{Record: unsafe.Pointer(&write.record)}
		if write.updated > backend.target {
			scanned.History = &histories[i]
		}
		page = append(page, scanned)
		// Pages of 7, so a rebuild sees several.
		if len(page) == 7 {
			if err := onPage(page); err != nil {
				return err
			}
			page = nil
		}
	}
	return onPage(page)
}

// TestRebuildFromRecordsMatchesHistories: a rebuild from a PartitionRecordScanner writes the same
// files as one from Histories, every frame and partition, records written after the target included.
func TestRebuildFromRecordsMatchesHistories(t *testing.T) {
	table := compileTestTable(t)
	sim := simulateWrites(t, table, rand.New(rand.NewPCG(11, 12)), 30000)
	now := simStart + 20000
	for partition := range int64(simPartitions) {
		historiesBackend, historiesStore := newMemoryBackend(sim), utils.NewMemoryFileStore()
		historiesBackend.readAt = now
		recordsBackend := &recordScanningBackend{memoryBackend: newMemoryBackend(sim), target: now - TargetLag}
		recordsBackend.readAt = now
		recordsStore := utils.NewMemoryFileStore()
		if err := Rebuild(table, historiesBackend, historiesStore, partition, "", timeOfSUnix(now)); err != nil {
			t.Fatal(err)
		}
		if err := Rebuild(table, recordsBackend, recordsStore, partition, "", timeOfSUnix(now)); err != nil {
			t.Fatal(err)
		}
		keys, _ := historiesStore.ListFiles("")
		recordKeys, _ := recordsStore.ListFiles("")
		slices.Sort(keys)
		slices.Sort(recordKeys)
		if !slices.Equal(keys, recordKeys) || len(keys) < 3 {
			t.Fatalf("partition %d: files %v, want %v", partition, recordKeys, keys)
		}
		for _, key := range keys {
			want, _, _ := historiesStore.GetFile(key)
			got, _, _ := recordsStore.GetFile(key)
			if !slices.Equal(got, want) {
				t.Fatalf("partition %d file %s differs", partition, key)
			}
		}
	}
}

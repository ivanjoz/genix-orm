package benchmarks

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/ivanjoz/genix-orm/dataframe"
	"github.com/ivanjoz/genix-orm/utils"
)

// pageSize is how many records a scan hands over at once (gocql's default page).
const pageSize = 5000

// memoryBackend answers a case's records, its state and its lock from memory. With computesHistories
// it turns each page of records into lines and Histories as a database backend does (scylla's
// historyOf); otherwise it hands over the case's precomputed Histories, so only the engine is timed.
type memoryBackend struct {
	c                 *Case
	computesHistories bool
	state             dataframe.State
	holderID          string
}

func newMemoryBackend(c *Case, computesHistories bool) *memoryBackend {
	return &memoryBackend{c: c, computesHistories: computesHistories}
}

func (backend *memoryBackend) ReadState(frame *dataframe.Frame, partition int64) (dataframe.State, error) {
	return backend.state, nil
}

func (backend *memoryBackend) TakeLock(frame *dataframe.Frame, partition int64, holderID string, now, lockedUntil time.Time, wait time.Duration) (dataframe.State, bool, error) {
	backend.holderID = holderID
	return backend.state, true, nil
}

func (backend *memoryBackend) ReleaseLock(frame *dataframe.Frame, partition int64, holderID string) {
	backend.holderID = ""
}

func (backend *memoryBackend) CommitSnapshot(ctx context.Context, frame *dataframe.Frame, partition int64, holderID string, state dataframe.State, snapshot int64) error {
	if holderID != backend.holderID || state != backend.state {
		return errors.New("lock or state moved")
	}
	backend.state.Snapshot = snapshot
	return nil
}

func (backend *memoryBackend) CommitGeneration(ctx context.Context, frame *dataframe.Frame, partition int64, holderID string, generation string, snapshot int64) error {
	backend.state = dataframe.State{Generation: generation, Shape: frame.Shape, Snapshot: snapshot}
	return nil
}

func (backend *memoryBackend) PartitionHistoriesWrittenAfter(partition int64, moment int64) ([]dataframe.History, error) {
	var indexes []int
	for i, lastWrite := range backend.c.LastWrites {
		if lastWrite > moment {
			indexes = append(indexes, i)
		}
	}
	return backend.historiesOf(indexes)
}

func (backend *memoryBackend) ScanPartitionHistories(partition int64, onPage func(histories []dataframe.History) error) error {
	for start := 0; start < len(backend.c.Records); start += pageSize {
		indexes := make([]int, 0, pageSize)
		for i := start; i < min(start+pageSize, len(backend.c.Records)); i++ {
			indexes = append(indexes, i)
		}
		histories, err := backend.historiesOf(indexes)
		if err != nil {
			return err
		}
		if err := onPage(histories); err != nil {
			return err
		}
	}
	return nil
}

func (backend *memoryBackend) historiesOf(indexes []int) ([]dataframe.History, error) {
	c := backend.c
	if !backend.computesHistories {
		histories := c.Histories()
		page := make([]dataframe.History, len(indexes))
		for p, i := range indexes {
			page[p] = histories[i]
		}
		return page, nil
	}
	page := make([]dataframe.History, 0, len(indexes))
	for _, i := range indexes {
		lines, err := c.Table.LinesOf(c.Records[i])
		if err != nil {
			return nil, err
		}
		history, err := c.Table.NewHistoryOfChanges(partition, c.Keys[i], lines, c.Changes[i])
		if err != nil {
			return nil, err
		}
		page = append(page, history)
	}
	return page, nil
}

// recordBackend is memoryBackend as a dataframe.PartitionRecordScanner, as scylla is: the records
// themselves, a History only for the ones with logged changes.
type recordBackend struct{ *memoryBackend }

func (backend recordBackend) ScanPartitionRecords(partition int64, onPage func(records []dataframe.ScannedRecord) error) error {
	c := backend.c
	for start := 0; start < len(c.Records); start += pageSize {
		page := make([]dataframe.ScannedRecord, 0, pageSize)
		for i := start; i < min(start+pageSize, len(c.Records)); i++ {
			scanned := dataframe.ScannedRecord{Record: c.Records[i]}
			if len(c.Changes[i]) > 0 {
				histories, err := backend.historiesOf([]int{i})
				if err != nil {
					return err
				}
				scanned.History = &histories[0]
			}
			page = append(page, scanned)
		}
		if err := onPage(page); err != nil {
			return err
		}
	}
	return nil
}

// buildMode is how a build reads the records: precomputed Histories (the engine alone), Histories
// computed page by page (dynamo), or the records themselves (scylla).
type buildMode int

const (
	fromPrecomputedHistories buildMode = iota
	fromHistories
	fromRecords
)

// build rebuilds the case's partition at t0 into a new store.
func (c *Case) build(mode buildMode) (*memoryBackend, *utils.MemoryFileStore, error) {
	backend, store := newMemoryBackend(c, mode != fromPrecomputedHistories), utils.NewMemoryFileStore()
	var scanning dataframe.Backend = backend
	if mode == fromRecords {
		scanning = recordBackend{backend}
	}
	err := dataframe.Rebuild(c.Table, scanning, store, partition, "", t0)
	return backend, store, err
}

// cloneStore copies every file of a store into a new one.
func cloneStore(store *utils.MemoryFileStore) *utils.MemoryFileStore {
	clone := utils.NewMemoryFileStore()
	keys, _ := store.ListFiles("")
	for _, key := range keys {
		content, _, _ := store.GetFile(key)
		if _, err := clone.PutFileIfMatch(context.Background(), key, content, ""); err != nil {
			panic(err)
		}
	}
	return clone
}

// frameFiles lists the store's frame files (not the versions files), sorted.
func frameFiles(store *utils.MemoryFileStore) []string {
	keys, _ := store.ListFiles(dataframe.Folder)
	keys = slices.DeleteFunc(keys, func(key string) bool { return len(key) > 9 && key[len(key)-9:] == ".versions" })
	slices.Sort(keys)
	return keys
}

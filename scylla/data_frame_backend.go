package scylla

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/ivanjoz/genix-orm/dataframe"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// scylla's dataframe.Backend. The state and the lock are dataframe.FileState's: a file
// per frame in the store, the lock in AcquireFrameLock. The records come as Histories:
// a record's tracked values now, read from the table, and its changes from the log.
//
// The order of the reads is what keeps a History exact: the log is read AFTER the
// values. A write landing between the two is then in both, and its change undoes the
// new value back to the old one for any moment before it. Read the other way round, a
// value would hold a write the log did not, and the History would put it too early.
// ─────────────────────────────────────────────────────────────────────────────

// frameScanPageSize is how many records a rebuild reads before it reads the log for them.
const frameScanPageSize = 5000

// frameRebuildLogMargin: a rebuild sums the records at its target (dataframe.TargetLag before the
// call started) and so needs every change logged after it; the log is read from 5 minutes earlier.
const frameRebuildLogMargin = 150

type frameBackend struct {
	*dataframe.FileState
	scyllaTable *ScyllaTable
	// startedAt is the call's now: Materialize and Rebuild take it as theirs, and the rebuild scans
	// read the log from before the target it gives them.
	startedAt time.Time
}

func newFrameBackend(scyllaTable *ScyllaTable) (*frameBackend, error) {
	if cache.FileStore() == nil {
		return nil, fmt.Errorf("table %q has DataFrames and no file store holds their files: call db.SetFileStore at boot", scyllaTable.Name)
	}
	if AcquireFrameLock == nil {
		return nil, fmt.Errorf("table %q has DataFrames and no lock takes them: install scylla.AcquireFrameLock at boot", scyllaTable.Name)
	}
	return &frameBackend{
		FileState:   &dataframe.FileState{Table: scyllaTable.dataFrames.frames, Store: cache.FileStore(), AcquireLock: AcquireFrameLock},
		scyllaTable: scyllaTable,
		startedAt:   db.Now(),
	}, nil
}

func (backend *frameBackend) HistoriesWrittenAfter(moment int64) ([]dataframe.History, error) {
	return backend.loggedHistories(0, true, moment)
}

func (backend *frameBackend) PartitionHistoriesWrittenAfter(partition int64, moment int64) ([]dataframe.History, error) {
	return backend.loggedHistories(partition, false, moment)
}

// loggedHistories is the History of every record with a change logged after moment. The log is read
// once to know the records, then again after their values. A record only the second read logs changed
// after the first read began, past any target a run has: it is left to the next run.
func (backend *frameBackend) loggedHistories(partition int64, isEveryPartition bool, moment int64) ([]dataframe.History, error) {
	loggedFirst, err := readDataFrameLog(*backend.scyllaTable, partition, isEveryPartition, moment, db.Now())
	if err != nil || len(loggedFirst) == 0 {
		return nil, err
	}
	idsOfPartition := map[int64][]int64{}
	for recordKey := range loggedFirst {
		idsOfPartition[recordKey[0]] = append(idsOfPartition[recordKey[0]], recordKey[1])
	}
	stored, err := readStoredRows(*backend.scyllaTable, idsOfPartition, backend.scyllaTable.dataFrames.trackedColumns)
	if err != nil {
		return nil, err
	}
	logged, err := readDataFrameLog(*backend.scyllaTable, partition, isEveryPartition, moment, db.Now())
	if err != nil {
		return nil, err
	}
	var histories []dataframe.History
	for recordKey := range loggedFirst {
		history, isStored, err := backend.historyOf(recordKey, stored, logged[recordKey])
		if err != nil {
			return nil, err
		}
		if isStored {
			histories = append(histories, history)
		}
	}
	return histories, nil
}

// historyOf builds a record's History from its stored row and its changes, newest first; false for a
// record the table does not hold (its write failed after its log row).
func (backend *frameBackend) historyOf(recordKey [2]int64, stored storedRows, changes []dataframe.HistoryChange) (dataframe.History, bool, error) {
	storedRow, isStored := stored[recordKey]
	if !isStored {
		return dataframe.History{}, false, nil
	}
	info := backend.scyllaTable.dataFrames
	values, err := info.trackedValuesOfRow(storedRow)
	if err != nil {
		return dataframe.History{}, false, fmt.Errorf("table %q: %w", backend.scyllaTable.Name, err)
	}
	return info.frames.NewHistoryOfChanges(recordKey[0], strconv.FormatInt(recordKey[1], 10), values, changes), true, nil
}

func (backend *frameBackend) ScanHistories(onPage func(histories []dataframe.History) error) error {
	return backend.scanHistories(0, true, onPage)
}

func (backend *frameBackend) ScanPartitionHistories(partition int64, onPage func(histories []dataframe.History) error) error {
	return backend.scanHistories(partition, false, onPage)
}

// scanHistories reads every record of the table or of one partition, a page at a time, and the log
// after each page: the changes since a little before the rebuild's target.
func (backend *frameBackend) scanHistories(partition int64, isEveryPartition bool, onPage func(histories []dataframe.History) error) error {
	scyllaTable := backend.scyllaTable
	partitionColumn, keyColumn := scyllaTable.GetPartKey(), scyllaTable.Keys[0]
	columnNames := []string{partitionColumn.GetName(), keyColumn.GetName()}
	for _, column := range scyllaTable.dataFrames.trackedColumns {
		if !slices.Contains(columnNames, column.GetName()) {
			columnNames = append(columnNames, column.GetName())
		}
	}
	queryString := fmt.Sprintf("SELECT %v FROM %v.%v", strings.Join(columnNames, ", "), resolveKeyspace(*scyllaTable), scyllaTable.Name)
	var queryValues []any
	if !isEveryPartition {
		queryString += fmt.Sprintf(" WHERE %v = ?", partitionColumn.GetName())
		queryValues = append(queryValues, coerceManagedIntegerValue(partitionColumn, partition))
	}
	logSince := utils.SUnixTimeOf(backend.startedAt) - dataframe.TargetLag - frameRebuildLogMargin

	flushPage := func(page storedRows) error {
		logged, err := readDataFrameLog(*scyllaTable, partition, isEveryPartition, logSince, db.Now())
		if err != nil {
			return err
		}
		histories := make([]dataframe.History, 0, len(page))
		for recordKey := range page {
			history, _, err := backend.historyOf(recordKey, page, logged[recordKey])
			if err != nil {
				return err
			}
			histories = append(histories, history)
		}
		return onPage(histories)
	}

	iterator := getScyllaConnection().Query(queryString, queryValues...).Consistency(gocql.LocalQuorum).PageSize(frameScanPageSize).Iter()
	page := storedRows{}
	for {
		row := map[string]any{}
		if !iterator.MapScan(row) {
			break
		}
		page[[2]int64{convertToInt64(row[columnNames[0]]), convertToInt64(row[columnNames[1]])}] = row
		if len(page) == frameScanPageSize {
			if err := flushPage(page); err != nil {
				return errors.Join(err, iterator.Close())
			}
			page = storedRows{}
		}
	}
	if err := iterator.Close(); err != nil {
		return fmt.Errorf("table %q: reading the records for a DataFrame rebuild: %w", scyllaTable.Name, err)
	}
	if len(page) == 0 {
		return nil
	}
	return flushPage(page)
}

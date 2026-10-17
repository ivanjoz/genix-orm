package scylla

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/gocql/gocql"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
)

// Delta logs: the files, their buckets, the cursor rules and the Metadata live in db/cache
// (delta_log.go). This file is scylla's side.
//
//   - Write: before the CQL, the stored version, status and pinned values of the written keys are
//     read (one "key IN" select per partition), and each record is stamped with the next Version.
//     After the CQL, in the marker phase, every written record appends (log ID, Version) to the log
//     of each delta index: a deleted entry when its status is 0, and one in the folder it left
//     when a pinned value changed.
//   - Read: Delta() reads the log past the client's cursor and selects the logged IDs; a first sync
//     or a replace lists the index's view instead. Both end with the Metadata records.
//
// Two concurrent writers of one record can store the same Version. The client only skips a log
// entry whose version equals the one it holds, so the cost is one write not re-applied until the
// record's next change.

// deltaIndexInfo is one compiled TypeDelta index.
type deltaIndexInfo struct {
	// tag names the index's folder in the log: its Keys' column names dash-joined, "all" when keyless.
	tag string
	// pinned are the Keys before the last: each value has its own log, and Delta() needs an equality
	// on each.
	pinned []IColInfo
	// syncFilter is the last Key, which a first sync filters on; nil on a keyless index.
	syncFilter IColInfo
	// is64BitScale: the log ID (the table's key) is an int64.
	is64BitScale bool
}

// deltaLogFolder is the index's log folder for one partition and its pinned values, ending in "/".
func deltaLogFolder(scyllaTable ScyllaTable, partitionValue int64, deltaIndex deltaIndexInfo, pinnedValues []int64) string {
	folder := cache.DeltaLogFolder + cacheBasePK(scyllaTable, partitionValue) + "/" + deltaIndex.tag + "/"
	for _, pinnedValue := range pinnedValues {
		folder += cache.PinnedValueText(pinnedValue) + "/"
	}
	return folder
}

/* Write */

// deltaWritePhase is one half of a write call: its records and the columns it writes. A column it
// does not write keeps its stored value, which is what the log must see.
type deltaWritePhase struct {
	records        recordSlice
	writtenColumns []IColInfo
}

// deltaWrittenRecord is what one written record appends to the logs.
type deltaWrittenRecord struct {
	partition int64
	logID     int64
	version   int16
	isDeleted bool
	// writtenValues and storedValues hold the status and pinned columns by name, as written and as
	// stored before the write; storedValues is nil for a record that was not stored.
	writtenValues map[string]int64
	storedValues  map[string]int64
}

// deltaTrackedColumns are the columns whose stored value the log needs: the status and every
// pinned column.
func deltaTrackedColumns(scyllaTable ScyllaTable) []IColInfo {
	trackedColumns := []IColInfo{}
	if statusColumn := scyllaTable.ColumnsMap["status"]; statusColumn != nil && !statusColumn.IsNil() {
		trackedColumns = append(trackedColumns, statusColumn)
	}
	for _, deltaIndex := range scyllaTable.deltaIndexes {
		for _, pinnedColumn := range deltaIndex.pinned {
			if !slices.ContainsFunc(trackedColumns, func(column IColInfo) bool { return column.GetName() == pinnedColumn.GetName() }) {
				trackedColumns = append(trackedColumns, pinnedColumn)
			}
		}
	}
	return trackedColumns
}

// stampDeltaVersions reads the stored state of every written record, stamps each with its next
// Version, and returns what the records will append to the logs once the rows are written. The
// keys must be final (after handlePreInsert).
func stampDeltaVersions(scyllaTable ScyllaTable, phases []deltaWritePhase) ([]deltaWrittenRecord, error) {
	if len(scyllaTable.deltaIndexes) == 0 {
		return nil, nil
	}
	partitionColumn, keyColumn := scyllaTable.GetPartKey(), scyllaTable.Keys[0]
	idsOfPartition := map[int64][]int64{}
	for _, phase := range phases {
		for i := range phase.records.len() {
			recordPointer := phase.records.at(i)
			partition := convertToInt64(partitionColumn.GetRawValue(recordPointer))
			idsOfPartition[partition] = append(idsOfPartition[partition], convertToInt64(keyColumn.GetRawValue(recordPointer)))
		}
	}
	trackedColumns := deltaTrackedColumns(scyllaTable)
	storedRows, err := readStoredDeltaRows(scyllaTable, idsOfPartition, trackedColumns)
	if err != nil {
		return nil, err
	}

	var writtenRecords []deltaWrittenRecord
	for _, phase := range phases {
		isWritten := map[string]bool{}
		for _, column := range phase.writtenColumns {
			isWritten[column.GetName()] = true
		}
		for i := range phase.records.len() {
			recordPointer := phase.records.at(i)
			written := deltaWrittenRecord{
				partition:     convertToInt64(partitionColumn.GetRawValue(recordPointer)),
				logID:         convertToInt64(keyColumn.GetRawValue(recordPointer)),
				writtenValues: map[string]int64{},
			}
			storedRow, isStored := storedRows[[2]int64{written.partition, written.logID}]
			written.version = int16(cache.NextVersion(storedRow[managedVersionColumnName]))
			scyllaTable.VersionCol.SetValue(recordPointer, coerceManagedIntegerValue(scyllaTable.VersionCol, int64(written.version)))
			if isStored {
				written.storedValues = storedRow
			}
			for _, column := range trackedColumns {
				if isWritten[column.GetName()] || !isStored {
					written.writtenValues[column.GetName()] = convertToInt64(column.GetRawValue(recordPointer))
				} else {
					written.writtenValues[column.GetName()] = storedRow[column.GetName()]
				}
			}
			// Status 0 leaves every view: physical deletes are not part of the app's flow, so the
			// deleted entry is the soft delete's.
			statusValue, hasStatus := written.writtenValues["status"]
			written.isDeleted = hasStatus && statusValue == 0
			writtenRecords = append(writtenRecords, written)
		}
	}
	return writtenRecords, nil
}

// readStoredDeltaRows reads the version and the tracked columns of the given keys, by partition and
// key, at LocalQuorum so a write right after another sees its version.
func readStoredDeltaRows(scyllaTable ScyllaTable, idsOfPartition map[int64][]int64, trackedColumns []IColInfo) (map[[2]int64]map[string]int64, error) {
	columnNames := []string{scyllaTable.Keys[0].GetName(), managedVersionColumnName}
	for _, column := range trackedColumns {
		columnNames = append(columnNames, column.GetName())
	}
	for partition := range idsOfPartition {
		slices.Sort(idsOfPartition[partition])
		idsOfPartition[partition] = slices.Compact(idsOfPartition[partition])
	}

	storedRows := map[[2]int64]map[string]int64{}
	err := forEachIDsBatch(scyllaTable, idsOfPartition, strings.Join(columnNames, ", "),
		func(queryString string, queryValues []any, partition int64) error {
			iterator := getScyllaConnection().Query(queryString, queryValues...).Consistency(gocql.LocalQuorum).Iter()
			for {
				row := map[string]any{}
				if !iterator.MapScan(row) {
					break
				}
				storedRow := map[string]int64{}
				for _, columnName := range columnNames[1:] {
					if storedValue := row[columnName]; storedValue != nil { // a null column reads as 0
						storedRow[columnName] = convertToInt64(storedValue)
					}
				}
				storedRows[[2]int64{partition, convertToInt64(row[columnNames[0]])}] = storedRow
			}
			return iterator.Close()
		})
	if err != nil {
		return nil, fmt.Errorf("table %q: reading the stored versions: %w", scyllaTable.Name, err)
	}
	return storedRows, nil
}

// appendDeltaLogs appends the landed writes to the log of every delta index. It runs after the rows
// are written: an entry published first would send a client to read the old row.
func appendDeltaLogs(scyllaTable ScyllaTable, writtenRecords []deltaWrittenRecord) error {
	if len(writtenRecords) == 0 {
		return nil
	}
	if cache.FileStore() == nil {
		return fmt.Errorf(`table %q has a TypeDelta index and no file store holds its log: call db.SetFileStore at boot`, scyllaTable.Name)
	}
	appends := cache.NewDeltaLogAppends(db.Now())
	for _, written := range writtenRecords {
		for _, deltaIndex := range scyllaTable.deltaIndexes {
			entry := cache.DeltaLogEntry{LogID: uint64(written.logID), Version: cache.WireVersion(written.version), IsDeleted: written.isDeleted}
			writtenFolder := deltaLogFolder(scyllaTable, written.partition, deltaIndex, pinnedValuesOf(deltaIndex, written.writtenValues))
			appends.Add(writtenFolder, deltaIndex.is64BitScale, entry)
			if written.storedValues == nil {
				continue
			}
			// A record whose pinned value changed left its previous folder.
			if leftFolder := deltaLogFolder(scyllaTable, written.partition, deltaIndex, pinnedValuesOf(deltaIndex, written.storedValues)); leftFolder != writtenFolder {
				entry.IsDeleted = true
				appends.Add(leftFolder, deltaIndex.is64BitScale, entry)
			}
		}
	}
	if err := appends.Write(); err != nil {
		return fmt.Errorf("table %q: %w", scyllaTable.Name, err)
	}
	return nil
}

func pinnedValuesOf(deltaIndex deltaIndexInfo, valuesByColumn map[string]int64) []int64 {
	pinnedValues := make([]int64, len(deltaIndex.pinned))
	for i, pinnedColumn := range deltaIndex.pinned {
		pinnedValues[i] = valuesByColumn[pinnedColumn.GetName()]
	}
	return pinnedValues
}

/* Read */

// selectDeltaIndex picks the TypeDelta index whose pinned Keys all have an equality, the one pinning
// the most when several fit. needsSyncFilter skips a keyless index.
func selectDeltaIndex(scyllaTable ScyllaTable, equalityValues map[string]any, needsSyncFilter bool) (deltaIndexInfo, error) {
	selectedPosition, selectedPinnedCount, isAmbiguous := -1, -1, false
	for position, deltaIndex := range scyllaTable.deltaIndexes {
		if needsSyncFilter && deltaIndex.syncFilter == nil {
			continue
		}
		isEveryKeyPinned := !slices.ContainsFunc(deltaIndex.pinned, func(column IColInfo) bool {
			_, isPinned := equalityValues[column.GetName()]
			return !isPinned
		})
		if !isEveryKeyPinned {
			continue
		}
		switch {
		case len(deltaIndex.pinned) > selectedPinnedCount:
			selectedPosition, selectedPinnedCount, isAmbiguous = position, len(deltaIndex.pinned), false
		case len(deltaIndex.pinned) == selectedPinnedCount:
			isAmbiguous = true
		}
	}
	if selectedPosition < 0 {
		return deltaIndexInfo{}, fmt.Errorf(`table %q: Delta() found no TypeDelta index whose pinned Keys all have an equality (and a sync filter column when values are given)`, scyllaTable.Name)
	}
	if isAmbiguous {
		return deltaIndexInfo{}, fmt.Errorf(`table %q: Delta() fits several TypeDelta indexes equally: pin one more Keys column, or drop an index`, scyllaTable.Name)
	}
	return scyllaTable.deltaIndexes[selectedPosition], nil
}

// execDelta is Exec for a Delta() read (db/cache's delta_log.go describes the protocol). Its reads
// run at LocalQuorum: they follow a log the writes appended after their rows landed.
func execDelta[E any](records *[]E, tableInfo *TableInfo, scyllaTable ScyllaTable, selectStartTime time.Time) error {
	if cache.FileStore() == nil {
		return fmt.Errorf(`table %q: Delta() reads a delta log and no file store holds it: call db.SetFileStore at boot`, scyllaTable.Name)
	}
	deltaRead := tableInfo.Delta
	equalityValues := map[string]any{}
	for _, statement := range tableInfo.Statements {
		if statement.Operator == "=" {
			equalityValues[statement.Col] = statement.Value
		}
	}
	partitionValue, isPartitionPinned := equalityValues[scyllaTable.GetPartKey().GetName()]
	if !isPartitionPinned {
		return fmt.Errorf(`table %q: Delta() needs an equality on the partition column: each partition has its own log`, scyllaTable.Name)
	}
	isFirstSync := len(deltaRead.Cursor) == 0
	deltaIndex, err := selectDeltaIndex(scyllaTable, equalityValues, isFirstSync && len(deltaRead.SyncFilterValues) > 0)
	if err != nil {
		return err
	}
	pinnedValues := make([]int64, len(deltaIndex.pinned))
	for i, pinnedColumn := range deltaIndex.pinned {
		pinnedValues[i] = convertToInt64(equalityValues[pinnedColumn.GetName()])
	}

	// The files go first: a write landing after them comes again on the next sync.
	folder := deltaLogFolder(scyllaTable, convertToInt64(partitionValue), deltaIndex, pinnedValues)
	logRead, err := cache.ReadDeltaLog(folder, deltaRead.Cursor, db.Now())
	if err != nil {
		return fmt.Errorf("table %q: %w", scyllaTable.Name, err)
	}

	scyllaTable.readConsistency = gocql.LocalQuorum
	if logRead.IsReplace {
		// The view, as the caller's predicates select it; a first sync also filters the sync filter.
		listingInfo := *tableInfo
		listingInfo.Delta = nil
		listingInfo.Statements = slices.Clone(tableInfo.Statements)
		if isFirstSync && len(deltaRead.SyncFilterValues) > 0 {
			listingInfo.Statements = append(listingInfo.Statements, db.ColumnStatement{
				Col: deltaIndex.syncFilter.GetName(), Operator: "IN", Values: deltaRead.SyncFilterValues,
			})
		}
		err = selectExec(records, &listingInfo, scyllaTable, nil, selectStartTime)
	} else {
		err = appendLoggedRecords(records, scyllaTable, convertToInt64(partitionValue), logRead, deltaIndex.is64BitScale)
	}
	if err != nil {
		return err
	}

	for _, metadataFile := range logRead.MetadataFiles(deltaIndex.is64BitScale, nil) {
		metadataRecord, err := deltaMetadataRecord[E](scyllaTable, metadataFile)
		if err != nil {
			return err
		}
		*records = append(*records, metadataRecord)
	}
	return nil
}

// appendLoggedRecords reads the records the log entries past the cursor name, the last entry per log
// ID winning, and appends those not deleted.
func appendLoggedRecords[E any](records *[]E, scyllaTable ScyllaTable, partitionValue int64, logRead *cache.DeltaLogRead, is64BitScale bool) error {
	entries, err := logRead.LoggedEntries(is64BitScale)
	if err != nil {
		return fmt.Errorf("table %q: %w", scyllaTable.Name, err)
	}
	var ids []int64
	for _, entry := range entries {
		if !entry.IsDeleted {
			ids = append(ids, int64(entry.LogID))
		}
	}
	if len(ids) == 0 {
		return nil
	}
	slices.Sort(ids)
	loggedRecords, err := readRecordsByIDs[E](scyllaTable, map[int64][]int64{partitionValue: ids})
	*records = append(*records, loggedRecords...)
	return err
}

// deltaMetadataRecord is the Metadata record of one log file: −bucket as its key, the Metadata text
// in its `Metadata *string json:"mdt,omitempty"` field.
func deltaMetadataRecord[E any](scyllaTable ScyllaTable, metadataFile cache.DeltaMetadataFile) (E, error) {
	var record E
	metadataField := reflect.ValueOf(&record).Elem().FieldByName("Metadata")
	if !metadataField.IsValid() || metadataField.Type() != reflect.TypeFor[*string]() {
		return record, fmt.Errorf(`table %q: Delta() returns Metadata records and the record has no field "Metadata *string" (json "mdt,omitempty")`, scyllaTable.Name)
	}
	metadataText := metadataFile.Text
	metadataField.Set(reflect.ValueOf(&metadataText))
	keyColumn := scyllaTable.Keys[0]
	keyColumn.SetValue(unsafe.Pointer(&record), coerceManagedIntegerValue(keyColumn, -metadataFile.Bucket))
	return record, nil
}

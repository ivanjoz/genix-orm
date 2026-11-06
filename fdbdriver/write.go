package fdbdriver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/fdb"
)

// The write algorithm (docs/FOUNDATIONDB_PLAN.md §8). Records go in chunks; a chunk is one FDB
// transaction, so a record, its index entries and its delta moves land together or not at all:
//
//  1. prepare (shared with every driver, db/write_prepare.go): SelfParse, autoincrement and key
//     packing (inserts), the managed columns, the columns written and the partial-update refusals;
//  2. pre-read the stored rows of the chunk (one GetMany);
//  3. build each new row: the stored row overlaid with the columns the write writes (a column it
//     leaves out keeps its stored value, as a CQL upsert does); stamp the managed version;
//  4. diff the TypeDelta entries: clear the stored one, set the new one, and write a move when the
//     pinned prefix changed;
//  5. commit with one condition per record (row absent, or same rowVersion), and redo steps 2–4 for
//     the chunk when another writer got there first.
//
// Not done yet (parity work, plan §10): entries of the other index kinds, cache-group freshness
// marks, the DataFrame log, by-IDs slot raises and the text-search sync.

const (
	writeChunkRecords    = 500
	writeConflictRetries = 5
)

// writePhase is one half of a write call: its records and which columns it writes.
type writePhase struct {
	records  db.RecordSlice
	isInsert bool
	// usesIncludeList marks an Update(columns...) phase: declaredColumns are the columns it writes.
	// Otherwise declaredColumns are the ones it leaves out.
	usesIncludeList bool
	declaredColumns []db.Coln
	// writtenColumns are the stored columns the write sets; every other one keeps its stored value.
	// Settled by resolveWrittenColumns once the records are prepared.
	writtenColumns []db.IColInfo
	// writesEveryColumn: the phase replaces whole rows, so the stored row is not merged.
	writesEveryColumn bool
}

func insertPhase(records db.RecordSlice, columnsToExclude []db.Coln) writePhase {
	return writePhase{records: records, isInsert: true, declaredColumns: columnsToExclude}
}

func updateIncludePhase(records db.RecordSlice, columnsToInclude []db.Coln) writePhase {
	return writePhase{records: records, usesIncludeList: true, declaredColumns: columnsToInclude}
}

func updateExcludePhase(records db.RecordSlice, columnsToExclude []db.Coln) writePhase {
	return writePhase{records: records, declaredColumns: columnsToExclude}
}

// resolveWrittenColumns settles which stored columns a phase writes, from the column set every driver
// resolves (db.ResolveUpdateColumns, with its partial-update refusals). The derived columns in it are
// not stored here (the driver writes index entries instead), and a computed column is its From
// columns.
func (table *FdbTable) resolveWrittenColumns(phase *writePhase) {
	var resolvedColumns []db.IColInfo
	switch {
	case phase.isInsert:
		resolvedColumns = table.AffectedColumnsForInsert(phase.declaredColumns)
	case phase.usesIncludeList:
		if len(phase.declaredColumns) == 0 {
			panic("No se incluyeron columnas a actualizar.")
		}
		resolvedColumns = table.ResolveUpdateColumns(phase.records, phase.declaredColumns, nil)
	default:
		resolvedColumns = table.ResolveUpdateColumns(phase.records, nil, phase.declaredColumns)
	}

	writtenNames := map[string]bool{}
	for _, resolvedColumn := range resolvedColumns {
		for _, storedColumn := range table.StoredColumnsOf(resolvedColumn) {
			if !storedColumn.GetInfo().IsVirtual && !writtenNames[storedColumn.GetName()] {
				writtenNames[storedColumn.GetName()] = true
				phase.writtenColumns = append(phase.writtenColumns, storedColumn)
			}
		}
	}
	for _, keyColumn := range table.keyAndPartitionColumns() {
		writtenNames[keyColumn.GetName()] = true
	}
	phase.writesEveryColumn = !slices.ContainsFunc(table.storedColumns, func(stored storedColumn) bool { return !writtenNames[stored.column.Name] })
}

func writeRecords(table *FdbTable, phases []writePhase) error {
	gate, err := gateClient()
	if err != nil {
		return err
	}
	for phaseIndex := range phases {
		phase := &phases[phaseIndex]
		if phase.records.Len() == 0 {
			continue
		}
		db.RunSelfParse(phase.records)
		if phase.isInsert {
			err := table.AssignAutoincrementKeys(phase.records, func(counterName string, count int) (int64, error) {
				return reserveCounter(table.Namespace, counterName, count)
			})
			if err != nil {
				return err
			}
		}
		stamps := table.StampManagedColumns(phase.records, phase.isInsert, int64(db.ManagedUnixTime()))
		table.resolveWrittenColumns(phase)
		for chunkStart := 0; chunkStart < phase.records.Len(); chunkStart += writeChunkRecords {
			chunkEnd := min(chunkStart+writeChunkRecords, phase.records.Len())
			if err := table.writeChunk(gate, *phase, stamps, chunkStart, chunkEnd); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeChunk writes records [chunkStart, chunkEnd) of a phase as one transaction, retrying when a
// concurrent writer changed one of the rows between the pre-read and the commit.
func (table *FdbTable) writeChunk(gate *fdb.Client, phase writePhase, stamps []db.ManagedStamp, chunkStart, chunkEnd int) error {
	recordKeys := make([][]byte, 0, chunkEnd-chunkStart)
	partitions := make([]int64, 0, chunkEnd-chunkStart)
	for index := chunkStart; index < chunkEnd; index++ {
		recordPointer := phase.records.At(index)
		partition := table.GetPartValue(recordPointer)
		partitions = append(partitions, partition)
		recordKeys = append(recordKeys, append(table.partitionPrefix(partition), table.recordSuffix(recordPointer)...))
	}

	for attempt := 0; ; attempt++ {
		ctx, cancel := requestContext()
		storedRows, isStored, err := gate.GetMany(ctx, recordKeys, false)
		cancel()
		if err != nil {
			return fmt.Errorf("table %q: reading the stored rows: %w", table.Name, err)
		}
		conditions := make([]fdb.Condition, 0, len(recordKeys))
		mutations := make([]fdb.Mutation, 0, len(recordKeys)*(1+2*len(table.deltaIndexes)))
		for chunkIndex, recordKey := range recordKeys {
			recordIndex := chunkStart + chunkIndex
			var storedRow []byte
			if isStored[chunkIndex] {
				storedRow = storedRows[chunkIndex]
				conditions = append(conditions, fdb.Condition{Kind: fdb.ConditionValuePrefixEquals, Key: recordKey, Value: storedRow[:8]})
			} else {
				conditions = append(conditions, fdb.Condition{Kind: fdb.ConditionKeyAbsent, Key: recordKey})
			}
			recordMutations, err := table.buildRecordMutations(phase, recordIndex, partitions[chunkIndex], recordKey, storedRow, stamps[recordIndex])
			if err != nil {
				return err
			}
			mutations = append(mutations, recordMutations...)
		}

		ctx, cancel = requestContext()
		_, err = gate.Commit(ctx, conditions, mutations)
		cancel()
		var conflict *fdb.ErrConditionFailed
		if err == nil {
			return nil
		}
		if !errors.As(err, &conflict) || attempt+1 >= writeConflictRetries {
			return fmt.Errorf("table %q: committing %d record(s): %w", table.Name, len(recordKeys), err)
		}
	}
}

// buildRecordMutations builds the row of one record and the delta entries and moves that go with it.
func (table *FdbTable) buildRecordMutations(phase writePhase, recordIndex int, partition int64, recordKey, storedRow []byte, stamp db.ManagedStamp) ([]fdb.Mutation, error) {
	recordPointer := phase.records.At(recordIndex)
	rowVersion := rowVersionOf(storedRow) + 1

	// The stored record, decoded only when something needs it: a partial write merges into it, and the
	// delta entries and version derive from it.
	var storedRecord unsafe.Pointer
	if storedRow != nil && (!phase.writesEveryColumn || len(table.deltaIndexes) > 0 || table.hasManagedWithoutField()) {
		storedRecord = reflect.New(phase.records.RecordType).UnsafePointer()
		if err := table.decodeRow(storedRow, storedRecord, nil); err != nil {
			return nil, fmt.Errorf("table %q: decoding a stored row: %w", table.Name, err)
		}
		if !phase.isInsert && table.CreatedCol != nil {
			// created is never rewritten by an update, field or not.
			stamp.Created = storedManagedInteger(table, storedRow, managedCreatedColumnName)
		}
	}

	// The row to write: the record itself when it writes every column, else the stored record with the
	// written columns copied over (a new record starts from zero, keys included by the copy).
	rowRecord := recordPointer
	if !phase.writesEveryColumn {
		rowRecord = reflect.New(phase.records.RecordType).UnsafePointer()
		if storedRecord != nil {
			copyRecord(phase.records.RecordType, rowRecord, storedRecord)
		}
		for _, column := range append(slices.Clone(phase.writtenColumns), table.keyAndPartitionColumns()...) {
			copyColumnValue(column, rowRecord, recordPointer)
		}
	}

	var mutations []fdb.Mutation
	if len(table.deltaIndexes) > 0 {
		storedVersion := int64(0)
		if storedRecord != nil {
			storedVersion = db.ConvertToInt64(table.VersionCol.GetRawValue(storedRecord))
		}
		version := cache.NextVersion(storedVersion)
		table.VersionCol.SetValue(recordPointer, version)
		table.VersionCol.SetValue(rowRecord, version)
	}

	row, err := table.encodeRow(make([]byte, 0, 256), rowVersion, rowRecord, stamp)
	if err != nil {
		return nil, err
	}
	if len(row) > maxRowBytes {
		return nil, fmt.Errorf("table %q: a row of %d bytes passes FoundationDB's %d-byte value limit (chunked rows are not implemented yet)", table.Name, len(row), maxRowBytes)
	}
	if len(table.deltaIndexes) > 0 {
		mutations = table.appendDeltaMutations(mutations, partition, rowRecord, storedRecord, row)
	}
	return append(mutations, fdb.Mutation{Kind: fdb.MutationSet, Key: recordKey, Value: row}), nil
}

// appendDeltaMutations moves each TypeDelta entry from the stored record's packed key to the new
// one's, and records a move when the pinned prefix changed (delta_view.go stampDeltaWrites).
//
// An entry holds a copy of the row, as a Scylla delta view does, so a sync is one range read.
// Entries that only pointed to the record cost one point read per record in the daemon: 4.5x slower
// on a 10k first sync (docs/FOUNDATIONDB_PLAN.md §14). The copy costs the row's bytes once more per
// TypeDelta index on every write, which the entry's key change already rewrites anyway.
func (table *FdbTable) appendDeltaMutations(mutations []fdb.Mutation, partition int64, rowRecord, storedRecord unsafe.Pointer, row []byte) []fdb.Mutation {
	recordSuffix := table.recordSuffix(rowRecord)
	pkBytes := recordSuffix[1:]
	updated := db.ConvertToInt64(table.UpdatedCol.GetRawValue(rowRecord))
	for _, deltaIndex := range table.deltaIndexes {
		pinnedValues := pinnedValuesOf(deltaIndex, rowRecord)
		newEntry := table.deltaEntryKey(partition, deltaIndex, deltaIndex.PackedKey(table.Name, pinnedValues, updated), pkBytes)
		if storedRecord != nil {
			storedPinned := pinnedValuesOf(deltaIndex, storedRecord)
			storedUpdated := db.ConvertToInt64(table.UpdatedCol.GetRawValue(storedRecord))
			storedEntry := table.deltaEntryKey(partition, deltaIndex, deltaIndex.PackedKey(table.Name, storedPinned, storedUpdated), pkBytes)
			mutations = append(mutations, fdb.Mutation{Kind: fdb.MutationClear, Key: storedEntry})
			if !slices.Equal(storedPinned, pinnedValues) {
				moveKey := appendOrderedInt64(table.deltaMovePrefix(partition, deltaIndex.id, deltaIndex.PackedKey(table.Name, storedPinned, 0)), updated)
				mutations = append(mutations, fdb.Mutation{Kind: fdb.MutationSet, Key: append(moveKey, pkBytes...)})
			}
		}
		mutations = append(mutations, fdb.Mutation{Kind: fdb.MutationSet, Key: newEntry, Value: row})
	}
	return mutations
}

func (table *FdbTable) deltaEntryKey(partition int64, deltaIndex fdbDeltaIndex, packed uint64, pkBytes []byte) []byte {
	key := binary.BigEndian.AppendUint64(table.deltaEntryPrefix(partition, deltaIndex.id), packed)
	return append(key, pkBytes...)
}

func pinnedValuesOf(deltaIndex fdbDeltaIndex, recordPointer unsafe.Pointer) []int64 {
	pinnedValues := make([]int64, len(deltaIndex.Pinned))
	for index, pinnedColumn := range deltaIndex.Pinned {
		pinnedValues[index] = db.ConvertToInt64(pinnedColumn.GetRawValue(recordPointer))
	}
	return pinnedValues
}

func (table *FdbTable) keyAndPartitionColumns() []db.IColInfo {
	columns := slices.Clone(table.Keys)
	if table.PartKey != nil {
		columns = append(columns, table.PartKey)
	}
	return columns
}

func (table *FdbTable) hasManagedWithoutField() bool {
	return table.CreatedCol != nil && table.CreatedCol.(*db.ColumnInfo).Field == nil
}

// storedManagedInteger reads a managed column with no record field straight from a row.
func storedManagedInteger(table *FdbTable, row []byte, columnName string) int64 {
	position := slices.IndexFunc(table.storedColumns, func(stored storedColumn) bool { return stored.column.Name == columnName })
	if position < 0 {
		return 0
	}
	wantedID := table.storedColumns[position].id
	offset := rowHeaderBytes
	for range int(binary.BigEndian.Uint16(row[9:11])) {
		columnID := binary.BigEndian.Uint16(row[offset:])
		valueLength, lengthBytes := binary.Uvarint(row[offset+2:])
		valueBytes := row[offset+2+lengthBytes : offset+2+lengthBytes+int(valueLength)]
		offset += 2 + lengthBytes + int(valueLength)
		if columnID == wantedID {
			value, _ := binary.Varint(valueBytes)
			return value
		}
	}
	return 0
}

func copyRecord(recordType reflect.Type, destination, source unsafe.Pointer) {
	reflect.NewAt(recordType, destination).Elem().Set(reflect.NewAt(recordType, source).Elem())
}

// copyColumnValue copies one column's field from source to destination; a column with no field (a
// managed one) has nothing to copy.
func copyColumnValue(column db.IColInfo, destination, source unsafe.Pointer) {
	columnInfo, isColumnInfo := column.(*db.ColumnInfo)
	if !isColumnInfo || columnInfo.Field == nil {
		return
	}
	fieldType := columnInfo.Field.Type
	reflect.NewAt(fieldType, columnInfo.Field.Pointer(destination)).Elem().Set(reflect.NewAt(fieldType, columnInfo.Field.Pointer(source)).Elem())
}

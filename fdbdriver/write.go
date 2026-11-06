package fdbdriver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
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
//  1. prepare: SelfParse, autoincrement and key packing (inserts), the managed columns;
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

// recordSlice is a typed slice seen through unsafe pointers, so the write path is not stenciled per
// record type.
type recordSlice struct {
	base       unsafe.Pointer
	count      int
	size       uintptr
	recordType reflect.Type
}

func makeRecordSlice[RecordT any](records *[]RecordT) recordSlice {
	slice := recordSlice{size: unsafe.Sizeof(*new(RecordT)), recordType: reflect.TypeFor[RecordT]()}
	if records != nil && len(*records) > 0 {
		slice.base, slice.count = unsafe.Pointer(&(*records)[0]), len(*records)
	}
	return slice
}

func (records recordSlice) at(index int) unsafe.Pointer {
	return unsafe.Add(records.base, uintptr(index)*records.size)
}

// writePhase is one half of a write call: its records and which columns it writes.
type writePhase struct {
	records  recordSlice
	isInsert bool
	// writtenColumns are the stored columns the write sets; every other one keeps its stored value.
	// nil until resolveWrittenColumns, which then takes every stored column but excludedNames.
	writtenColumns []db.IColInfo
	excludedNames  map[string]bool
}

func insertPhase[RecordT any](records *[]RecordT, columnsToExclude []db.Coln) writePhase {
	return writePhase{records: makeRecordSlice(records), isInsert: true, writtenColumns: nil}.withExcluded(columnsToExclude)
}

// withExcluded is resolved against the table later (resolveWrittenColumns): keep the names here.
func (phase writePhase) withExcluded(columnsToExclude []db.Coln) writePhase {
	phase.excludedNames = map[string]bool{}
	for _, column := range columnsToExclude {
		phase.excludedNames[column.GetName()] = true
	}
	return phase
}

func updateIncludePhase[RecordT any](table *FdbTable, records *[]RecordT, columnsToInclude []db.Coln) writePhase {
	phase := writePhase{records: makeRecordSlice(records)}
	if len(columnsToInclude) == 0 {
		return phase.withExcluded(nil)
	}
	for _, declared := range columnsToInclude {
		column := table.ColumnsMap[declared.GetName()]
		if column == nil {
			panic(fmt.Sprintf(`Table "%v": update column "%v" was not found`, table.Name, declared.GetName()))
		}
		if slices.Contains(table.KeysIdx, column.GetInfo().Idx) {
			panic(fmt.Sprintf(`Table "%v": column "%v" is part of the primary key and cannot be updated`, table.Name, column.GetName()))
		}
		phase.writtenColumns = append(phase.writtenColumns, table.StoredColumnsOf(column)...)
	}
	return phase
}

func updateExcludePhase[RecordT any](table *FdbTable, records *[]RecordT, columnsToExclude []db.Coln) writePhase {
	return writePhase{records: makeRecordSlice(records)}.withExcluded(columnsToExclude)
}

func splitByIsInsert[RecordT any](records *[]RecordT, isInsert func(record *RecordT) bool) (toInsert, toUpdate []RecordT) {
	for index := range *records {
		if isInsert(&(*records)[index]) {
			toInsert = append(toInsert, (*records)[index])
		} else {
			toUpdate = append(toUpdate, (*records)[index])
		}
	}
	return toInsert, toUpdate
}

// resolveWrittenColumns settles which stored columns a phase writes. The managed updated and version
// are always written; keys and the partition always are (they name the row).
func (table *FdbTable) resolveWrittenColumns(phase *writePhase) {
	if phase.writtenColumns == nil {
		for _, stored := range table.storedColumns {
			if !phase.excludedNames[stored.column.Name] {
				phase.writtenColumns = append(phase.writtenColumns, stored.column)
			}
		}
	}
	for _, managed := range []db.IColInfo{table.UpdatedCol, table.VersionCol} {
		if managed != nil && !slices.ContainsFunc(phase.writtenColumns, func(column db.IColInfo) bool { return column.GetName() == managed.GetName() }) {
			phase.writtenColumns = append(phase.writtenColumns, managed)
		}
	}
}

// writesEveryColumn reports a write that replaces the whole row, so the stored row is not merged.
func (table *FdbTable) writesEveryColumn(phase writePhase) bool {
	return len(phase.writtenColumns) >= len(table.storedColumns)
}

func writeRecords(table *FdbTable, phases []writePhase) error {
	gate, err := gateClient()
	if err != nil {
		return err
	}
	for phaseIndex := range phases {
		phase := &phases[phaseIndex]
		if phase.records.count == 0 {
			continue
		}
		table.resolveWrittenColumns(phase)
		runSelfParse(phase.records)
		if phase.isInsert {
			if err := table.assignAutoincrementKeys(phase.records); err != nil {
				return err
			}
		}
		writeTime := int64(managedUnixTime())
		stamps := table.stampManagedColumns(phase.records, phase.isInsert, writeTime)
		for chunkStart := 0; chunkStart < phase.records.count; chunkStart += writeChunkRecords {
			chunkEnd := min(chunkStart+writeChunkRecords, phase.records.count)
			if err := table.writeChunk(gate, *phase, stamps, chunkStart, chunkEnd); err != nil {
				return err
			}
		}
	}
	return nil
}

func runSelfParse(records recordSlice) {
	for index := range records.count {
		if parser, hasSelfParse := reflect.NewAt(records.recordType, records.at(index)).Interface().(interface{ SelfParse() }); hasSelfParse {
			parser.SelfParse()
		} else {
			return
		}
	}
}

// managedUnixTime is SUnixTime on the ORM's effective clock (db.Now honours a historical override).
func managedUnixTime() int32 {
	return int32((db.Now().Unix() - 1e9) / 2)
}

// stampManagedColumns sets created (on insert, when the caller left it zero) and updated on every
// record, and returns the stamps the row stores for managed columns with no record field.
func (table *FdbTable) stampManagedColumns(records recordSlice, isInsert bool, writeTime int64) []managedStamp {
	stamps := make([]managedStamp, records.count)
	for index := range records.count {
		recordPointer := records.at(index)
		if isInsert && table.CreatedCol != nil {
			createdValue := writeTime
			if current := table.CreatedCol.GetRawValue(recordPointer); current != nil && db.ConvertToInt64(current) > 0 {
				createdValue = db.ConvertToInt64(current)
			}
			table.CreatedCol.SetValue(recordPointer, createdValue)
			stamps[index].created = createdValue
		}
		if table.UpdatedCol != nil {
			table.UpdatedCol.SetValue(recordPointer, writeTime)
			stamps[index].updated = writeTime
		}
	}
	return stamps
}

// assignAutoincrementKeys reserves IDs for the records whose autoincrement column is ≤ 0, one counter
// per (partition, AutoincrementPart) as the Scylla driver names them, then packs KeyIntPacking keys.
func (table *FdbTable) assignAutoincrementKeys(records recordSlice) error {
	if table.AutoincrementCol == nil && len(table.KeyIntPacking) == 0 {
		return nil
	}
	type counterGroup struct {
		name    string
		members []int
	}
	groups := map[string]*counterGroup{}
	needsID := make([]bool, records.count)
	for index := range records.count {
		recordPointer := records.at(index)
		if table.AutoincrementCol == nil {
			continue
		}
		if rawValue := table.AutoincrementCol.GetRawValue(recordPointer); rawValue != nil && db.ConvertToInt64(rawValue) > 0 {
			continue
		}
		needsID[index] = true
		partitionValue := int64(0)
		if table.PartKey != nil {
			partitionValue = db.ConvertToInt64(table.PartKey.GetRawValue(recordPointer))
		}
		autoincrementPart := int64(0)
		if table.AutoincrementPart != nil {
			autoincrementPart = db.ConvertToInt64(table.AutoincrementPart.GetRawValue(recordPointer))
		}
		counterName := fmt.Sprintf("x%v_%v_%v", int32(partitionValue), table.Name, autoincrementPart)
		if groups[counterName] == nil {
			groups[counterName] = &counterGroup{name: counterName}
		}
		groups[counterName].members = append(groups[counterName].members, index)
	}

	generatedIDs := make([]int64, records.count)
	for _, group := range groups {
		firstID, err := reserveCounter(table.Namespace, group.name, len(group.members))
		if err != nil {
			return fmt.Errorf("table %q: reserving %d IDs on %s: %w", table.Name, len(group.members), group.name, err)
		}
		autoincrementColumn := table.AutoincrementCol.(*db.ColumnInfo)
		for offset, index := range group.members {
			generatedID := firstID + int64(offset)
			if autoincrementColumn.AutoincrementFirstID > 1 {
				generatedID += autoincrementColumn.AutoincrementFirstID - 1
			}
			if autoincrementColumn.AutoincrementRandBits > 0 {
				generatedID = generatedID<<autoincrementColumn.AutoincrementRandBits | rand.Int64N(int64(1)<<autoincrementColumn.AutoincrementRandBits)
			}
			generatedIDs[index] = generatedID
			if len(table.KeyIntPacking) == 0 {
				table.AutoincrementCol.SetValue(records.at(index), generatedID)
			}
		}
	}

	if len(table.KeyIntPacking) > 0 {
		for index := range records.count {
			recordPointer := records.at(index)
			componentValues := make([]int64, len(table.KeyIntPacking))
			for slotIndex, column := range table.KeyIntPacking {
				if column == table.AutoincrementCol && needsID[index] {
					componentValues[slotIndex] = generatedIDs[index]
				} else if column.GetInfo().IsVirtual {
					componentValues[slotIndex] = 0
				} else {
					componentValues[slotIndex] = db.ConvertToInt64(column.GetRawValue(recordPointer))
				}
			}
			packedKey := db.PackRowValues(table.Name, table.Keys[0].GetName(), table.KeyIntPacking, componentValues, table.KeyIntPackingSlotBits)
			table.Keys[0].SetValue(recordPointer, int64(packedKey))
		}
	}
	return nil
}

// writeChunk writes records [chunkStart, chunkEnd) of a phase as one transaction, retrying when a
// concurrent writer changed one of the rows between the pre-read and the commit.
func (table *FdbTable) writeChunk(gate *fdb.Client, phase writePhase, stamps []managedStamp, chunkStart, chunkEnd int) error {
	recordKeys := make([][]byte, 0, chunkEnd-chunkStart)
	partitions := make([]int64, 0, chunkEnd-chunkStart)
	for index := chunkStart; index < chunkEnd; index++ {
		recordPointer := phase.records.at(index)
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
func (table *FdbTable) buildRecordMutations(phase writePhase, recordIndex int, partition int64, recordKey, storedRow []byte, stamp managedStamp) ([]fdb.Mutation, error) {
	recordPointer := phase.records.at(recordIndex)
	rowVersion := rowVersionOf(storedRow) + 1

	// The stored record, decoded only when something needs it: a partial write merges into it, and the
	// delta entries and version derive from it.
	var storedRecord unsafe.Pointer
	if storedRow != nil && (!table.writesEveryColumn(phase) || len(table.deltaIndexes) > 0 || table.hasManagedWithoutField()) {
		storedRecord = reflect.New(phase.records.recordType).UnsafePointer()
		if err := table.decodeRow(storedRow, storedRecord, nil); err != nil {
			return nil, fmt.Errorf("table %q: decoding a stored row: %w", table.Name, err)
		}
		if !phase.isInsert && table.CreatedCol != nil {
			// created is never rewritten by an update, field or not.
			stamp.created = storedManagedInteger(table, storedRow, managedCreatedColumnName)
		}
	}

	// The row to write: the record itself when it writes every column, else the stored record with the
	// written columns copied over (a new record starts from zero, keys included by the copy).
	rowRecord := recordPointer
	if !table.writesEveryColumn(phase) {
		rowRecord = reflect.New(phase.records.recordType).UnsafePointer()
		if storedRecord != nil {
			copyRecord(phase.records.recordType, rowRecord, storedRecord)
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

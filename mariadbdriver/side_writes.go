package mariadbdriver

import (
	"cmp"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
)

// What a write chunk writes beside its rows, from the stored records it locks first (plan §9.2,
// §9.4, §9.5): the TypeDelta version, packed columns and moves (Scylla: delta_view.go
// stampDeltaWrites), and the cache-group marks (Scylla: index_groups.go collectIndexGroupMarks).
// Everything commits with the rows, so a reader never sees a mark or a move before the row.

// chunkSideWrites are a chunk's delta and group writes.
type chunkSideWrites struct {
	// packedDeltaValues is each delta packed column's value per record of the chunk, in order.
	packedDeltaValues map[string][]uint64
	// moves are __delta_moves rows: (partition, index tag, old prefix, updated, id).
	moves [][]any
	marks []groupMark
	// groupElementWrites are the raw-values rows of the slices' cache groups, by groupElementTables.
	groupElementWrites []groupElementWrite
}

// groupMark is one __index_updated row: a cache group a write touched, under its packed value.
type groupMark struct {
	partition  int64
	indexID    int16
	indexValue uint64
}

// needsStoredRecords tells whether a phase must read (and lock) its stored records before writing.
// The version never needs them: the upsert raises it in SQL (versionAssignment). They are read when
//   - the table has cache groups: a record marks the groups it leaves, known only from the stored row;
//   - the table has DataFrames: their aggregates subtract the lines the stored record gave;
//   - the phase writes a pinned column of a TypeDelta index: a changed prefix writes a delta move.
//
// A phase writing no pinned column keeps each packed delta column's stored prefix in SQL
// (packedDeltaAssignment). An insert of new keys (minted, or promised by db.Insert) has nothing stored.
func (table *MariaTable) needsStoredRecords(phase writePhase) bool {
	if phase.hasOnlyNewKeys {
		return false
	}
	if len(table.IndexGroups) > 0 || table.dataFrames != nil {
		return true
	}
	for _, deltaIndex := range table.DeltaIndexes {
		if phase.writesAnyOf(deltaIndex.Pinned) {
			return true
		}
	}
	return false
}

// storedReadColumns are the stored columns a write reads before writing: every persisted one when
// the table has DataFrames (a ToFrames method may read any), otherwise the pinned columns and the
// group sources the side writes derive from.
func (table *MariaTable) storedReadColumns() []db.IColInfo {
	if table.dataFrames != nil {
		return table.dataFrames.storedColumns
	}
	readColumns := []db.IColInfo{}
	for _, deltaIndex := range table.DeltaIndexes {
		readColumns = append(readColumns, deltaIndex.Pinned...)
	}
	return append(readColumns, table.indexGroupStoredColumns()...)
}

// prepareSideWrites derives a chunk's side writes from its records and, when the phase read them,
// its stored records (storedByKey; empty otherwise). A column the write leaves out keeps its stored
// value in the packed delta key and in the groups the record ends in; a stored record also marks the
// groups it leaves.
func (table *MariaTable) prepareSideWrites(phase writePhase, chunkStart, chunkEnd int, storedByKey map[string]unsafe.Pointer) chunkSideWrites {
	isWritten := map[string]bool{}
	for _, stored := range phase.writtenColumns {
		// By ORM column too: an expanded slice's element columns all stand for it.
		isWritten[stored.name], isWritten[stored.column.GetName()] = true, true
	}
	readColumns := table.storedReadColumns()
	sideWrites := chunkSideWrites{packedDeltaValues: map[string][]uint64{}}
	// A table with no delta index and no cache group has no side write.
	if len(table.DeltaIndexes) == 0 && len(table.IndexGroups) == 0 && len(table.groupElementTables) == 0 {
		return sideWrites
	}
	isMarked := map[groupMark]bool{}
	// A record written twice in the chunk keeps the rows of its last write.
	elementRowsByKey := make([]map[string][][]any, len(table.groupElementTables))
	elementKeyOrder := make([][]string, len(table.groupElementTables))
	clearedByKey := make([]map[string][]any, len(table.groupElementTables))
	for tableIndex := range table.groupElementTables {
		elementRowsByKey[tableIndex], clearedByKey[tableIndex] = map[string][][]any{}, map[string][]any{}
	}
	for recordIndex := chunkStart; recordIndex < chunkEnd; recordIndex++ {
		recordPointer := phase.records.At(recordIndex)
		storedRecord, isStored := storedByKey[table.primaryKeyToken(recordPointer)]
		partition := table.GetPartValue(recordPointer)

		// finalRecord is the record as stored once the write lands, as far as the side writes read it.
		finalRecord := recordPointer
		if isStored {
			finalRecord = reflect.New(phase.records.RecordType).UnsafePointer()
			phase.records.CopyRecord(finalRecord, recordPointer)
			for _, column := range readColumns {
				if !isWritten[column.GetName()] {
					copyColumnValue(column, finalRecord, storedRecord)
				}
			}
		}

		if len(table.DeltaIndexes) > 0 {
			updated := db.ConvertToInt64(table.UpdatedCol.GetRawValue(recordPointer))
			for _, deltaIndex := range table.DeltaIndexes {
				finalPinned := pinnedValuesOf(deltaIndex, finalRecord)
				columnName := deltaIndex.PackedColumn.GetName()
				sideWrites.packedDeltaValues[columnName] = append(sideWrites.packedDeltaValues[columnName], deltaIndex.PackedKey(table.Name, finalPinned, updated))
				if storedPinned := pinnedValuesOf(deltaIndex, storedRecord); isStored && !slices.Equal(storedPinned, finalPinned) {
					sideWrites.moves = append(sideWrites.moves, []any{
						partition, deltaIndex.Tag, deltaIndex.PackedKey(table.Name, storedPinned, 0), updated, table.Keys[0].GetRawValue(recordPointer),
					})
				}
			}
		}

		// The raw-values rows are rewritten when the phase writes a column they derive from.
		for tableIndex := range table.groupElementTables {
			elementTable := &table.groupElementTables[tableIndex]
			if !phase.hasOnlyNewKeys && !phase.writesAnyOf(elementTable.sourceColumns) {
				continue
			}
			keyToken := table.primaryKeyToken(recordPointer)
			if _, isSeen := elementRowsByKey[tableIndex][keyToken]; !isSeen {
				elementKeyOrder[tableIndex] = append(elementKeyOrder[tableIndex], keyToken)
			}
			elementRowsByKey[tableIndex][keyToken] = elementTable.rowsOf(finalRecord)
			if isStored {
				recordKey := make([]any, len(elementTable.recordKeyColumns))
				for position, keyColumn := range elementTable.recordKeyColumns {
					recordKey[position] = keyColumn.GetRawValue(recordPointer)
				}
				clearedByKey[tableIndex][keyToken] = recordKey
			}
		}

		// A record marks the groups it ends in and, when stored, the ones it was in.
		markedRecords := []unsafe.Pointer{finalRecord}
		if isStored {
			markedRecords = append(markedRecords, storedRecord)
		}
		for _, markedRecord := range markedRecords {
			for _, indexGroup := range table.IndexGroups {
				// A no-index group carries its base group's mark: it writes none of its own.
				if indexGroup.BaseGroup != nil {
					continue
				}
				for _, groupValues := range db.ComputeIndexGroupValueCombinations(markedRecord, indexGroup.SourceColumns) {
					mark := groupMark{partition: partition, indexID: indexGroup.IndexID, indexValue: indexGroup.PackedValue(table.Name, groupValues)}
					if !isMarked[mark] {
						isMarked[mark] = true
						sideWrites.marks = append(sideWrites.marks, mark)
					}
				}
			}
		}
	}
	for tableIndex := range table.groupElementTables {
		elementWrite := groupElementWrite{}
		for _, keyToken := range elementKeyOrder[tableIndex] {
			if recordKey := clearedByKey[tableIndex][keyToken]; recordKey != nil {
				elementWrite.clearedKeys = append(elementWrite.clearedKeys, recordKey)
			}
			elementWrite.rows = append(elementWrite.rows, elementRowsByKey[tableIndex][keyToken]...)
		}
		sideWrites.groupElementWrites = append(sideWrites.groupElementWrites, elementWrite)
	}
	return sideWrites
}

// versionAssignment raises the stored version in the upsert itself (cache.NextVersion in SQL): a new
// row takes the VALUES' 1, an existing one its version + 1, back to 1 past cache.MaxVersion. On a
// duplicate key, a column named alone is the existing row's value.
func versionAssignment(versionColumn string) string {
	quoted := quoteName(versionColumn)
	return fmt.Sprintf("%s = IF(%s >= %d, 1, %s + 1)", quoted, quoted, cache.MaxVersion, quoted)
}

// packedDeltaAssignment keeps the stored pinned prefix of a packed delta column and takes only the
// new updated slot (its low db.DeltaUpdatedSlotBits): the write carries no pinned column, so the
// stored prefix is the right one without reading it.
func packedDeltaAssignment(packedColumn string) string {
	quoted := quoteName(packedColumn)
	updatedMask := uint64(1)<<db.DeltaUpdatedSlotBits - 1
	return fmt.Sprintf("%s = (%s & %d) | (VALUES(%s) & %d)", quoted, quoted, ^updatedMask, quoted, updatedMask)
}

func pinnedValuesOf(deltaIndex db.DeltaIndexInfo, recordPointer unsafe.Pointer) []int64 {
	if recordPointer == nil {
		return nil
	}
	pinnedValues := make([]int64, len(deltaIndex.Pinned))
	for position, pinnedColumn := range deltaIndex.Pinned {
		pinnedValues[position] = db.ConvertToInt64(pinnedColumn.GetRawValue(recordPointer))
	}
	return pinnedValues
}

// indexGroupStoredColumns are the stored columns the index groups hash: a computed source
// contributes its From columns.
func (table *MariaTable) indexGroupStoredColumns() []db.IColInfo {
	var storedColumns []db.IColInfo
	for _, indexGroup := range table.IndexGroups {
		if indexGroup.BaseGroup != nil {
			continue
		}
		for _, sourceColumn := range indexGroup.SourceColumns {
			for _, storedColumn := range table.StoredColumnsOf(sourceColumn.Column) {
				if !slices.ContainsFunc(storedColumns, func(column db.IColInfo) bool { return column.GetName() == storedColumn.GetName() }) {
					storedColumns = append(storedColumns, storedColumn)
				}
			}
		}
	}
	return storedColumns
}

// lockStoredRecords reads the primary key and readColumns of the stored records of [chunkStart,
// chunkEnd), decoded, and locks those rows until the transaction ends. Records are keyed by
// primaryKeyToken. A phase whose every key is new (hasOnlyNewKeys) has nothing stored:
// it skips the read, one round trip per insert of new records.
func (table *MariaTable) lockStoredRecords(transaction *writeTransaction, phase writePhase, chunkStart, chunkEnd int, readColumns []db.IColInfo) (map[string]unsafe.Pointer, error) {
	if phase.hasOnlyNewKeys {
		return map[string]unsafe.Pointer{}, nil
	}
	records := phase.records
	keyNames := make([]string, len(table.primaryKeyColumns))
	for position, keyColumn := range table.primaryKeyColumns {
		keyNames[position] = keyColumn.GetName()
	}
	selectedColumns := []storedColumn{}
	for _, stored := range table.storedColumns {
		isRead := slices.Contains(keyNames, stored.name) || slices.ContainsFunc(readColumns, func(column db.IColInfo) bool { return column.GetName() == stored.column.GetName() })
		if isRead && stored.hasField() {
			selectedColumns = append(selectedColumns, stored)
		}
	}
	selectedNames := make([]string, len(selectedColumns))
	for position, stored := range selectedColumns {
		selectedNames[position] = stored.name
	}
	keyPlaceholders := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(keyNames)), ", ") + ")"
	args := make([]any, 0, (chunkEnd-chunkStart)*len(keyNames))
	for recordIndex := chunkStart; recordIndex < chunkEnd; recordIndex++ {
		for _, keyColumn := range table.primaryKeyColumns {
			args = append(args, keyColumn.GetRawValue(records.At(recordIndex)))
		}
	}
	statement := fmt.Sprintf("SELECT %s FROM %s %s WHERE (%s) IN (%s) FOR UPDATE", quotedNames(selectedNames), table.qualifiedName(), primaryKeyLookup,
		quotedNames(keyNames), strings.TrimSuffix(strings.Repeat(keyPlaceholders+", ", chunkEnd-chunkStart), ", "))
	var storedRecords []unsafe.Pointer
	sink := recordSink{
		recordType:   records.RecordType,
		newRecord:    records.NewRecord,
		appendRecord: func(record unsafe.Pointer) { storedRecords = append(storedRecords, record) },
	}
	if err := table.queryIntoWith(transaction, transaction.ctx, statement, args, selectedColumns, sink); err != nil {
		return nil, fmt.Errorf("table %q: reading the stored rows: %w", table.Name, err)
	}
	storedByKey := make(map[string]unsafe.Pointer, len(storedRecords))
	for _, storedRecord := range storedRecords {
		storedByKey[table.primaryKeyToken(storedRecord)] = storedRecord
	}
	return storedByKey, nil
}

// rememberLocked keeps the rows LockByKeys read under lock for a later write of them in the same
// transaction (storedRecordsForChunk).
func (transaction *writeTransaction) rememberLocked(table *MariaTable, storedByKey map[string]unsafe.Pointer) {
	if transaction.lockedRecords == nil {
		transaction.lockedRecords = map[string]map[string]unsafe.Pointer{}
	}
	lockedByKey := transaction.lockedRecords[table.Name]
	if lockedByKey == nil {
		lockedByKey = map[string]unsafe.Pointer{}
		transaction.lockedRecords[table.Name] = lockedByKey
	}
	for keyToken, storedRecord := range storedByKey {
		lockedByKey[keyToken] = storedRecord
	}
}

// storedRecordsForChunk is lockStoredRecords for a chunk, served from the rows LockByKeys already
// locked in this transaction when it holds every one of them: those rows are read whole and cannot
// have changed, being locked. The chunk's rows then leave the map, since this write changes them.
func (table *MariaTable) storedRecordsForChunk(transaction *writeTransaction, phase writePhase, chunkStart, chunkEnd int) (map[string]unsafe.Pointer, error) {
	lockedByKey := transaction.lockedRecords[table.Name]
	if phase.hasOnlyNewKeys || len(lockedByKey) == 0 {
		return table.lockStoredRecords(transaction, phase, chunkStart, chunkEnd, table.storedReadColumns())
	}
	storedByKey := make(map[string]unsafe.Pointer, chunkEnd-chunkStart)
	for recordIndex := chunkStart; recordIndex < chunkEnd; recordIndex++ {
		keyToken := table.primaryKeyToken(phase.records.At(recordIndex))
		storedRecord, isLocked := lockedByKey[keyToken]
		if !isLocked {
			return table.lockStoredRecords(transaction, phase, chunkStart, chunkEnd, table.storedReadColumns())
		}
		storedByKey[keyToken] = storedRecord
	}
	for keyToken := range storedByKey {
		delete(lockedByKey, keyToken)
	}
	return storedByKey, nil
}

// writeDeltaMoves writes the moves of a chunk in its transaction.
func (table *MariaTable) writeDeltaMoves(transaction *writeTransaction, moves [][]any) error {
	if len(moves) == 0 {
		return nil
	}
	args := make([]any, 0, len(moves)*5)
	for _, move := range moves {
		args = append(args, move...)
	}
	statement := fmt.Sprintf("INSERT INTO %s (`partition_id`, `index_tag`, `pinned_prefix`, `updated`, `id`) VALUES %s ON DUPLICATE KEY UPDATE `updated` = `updated`",
		deltaMovesTableSpec(table.Namespace, table.Name, "").qualifiedName(), strings.TrimSuffix(strings.Repeat("(?, ?, ?, ?, ?), ", len(moves)), ", "))
	if err := transaction.queue(statement, args...); err != nil {
		return fmt.Errorf("table %q: writing %d delta move(s): %w", table.Name, len(moves), err)
	}
	return nil
}

// writeGroupMarks defers a chunk's marks to the end of its transaction (queueGroupMarks): a mark is
// stamped right before the COMMIT, so it lands within the settle window of its stamp however long
// the transaction ran before it.
func (table *MariaTable) writeGroupMarks(transaction *writeTransaction, marks []groupMark) error {
	if len(marks) == 0 {
		return nil
	}
	markTable := indexUpdatedTableSpec(table.Namespace, table.Name).qualifiedName()
	if transaction.deferredMarks == nil {
		transaction.deferredMarks = map[string][]groupMark{}
	}
	transaction.deferredMarks[markTable] = append(transaction.deferredMarks[markTable], marks...)
	return nil
}

// maxMarksPerStatement keeps one mark upsert a bounded statement.
const maxMarksPerStatement = 4000

// queueGroupMarks stamps every deferred mark of the transaction with one clock reading and queues
// their upserts, the last statements before the COMMIT. Tables and marks go in key order, so two
// transactions lock shared mark rows in the same order and never deadlock on them.
func (transaction *writeTransaction) queueGroupMarks() error {
	lastWrite := db.ManagedUnixTime()
	for _, markTable := range slices.Sorted(maps.Keys(transaction.deferredMarks)) {
		marks := transaction.deferredMarks[markTable]
		slices.SortFunc(marks, func(left, right groupMark) int {
			return cmp.Or(cmp.Compare(left.partition, right.partition), cmp.Compare(left.indexID, right.indexID), cmp.Compare(left.indexValue, right.indexValue))
		})
		marks = slices.Compact(marks)
		for chunkStart := 0; chunkStart < len(marks); chunkStart += maxMarksPerStatement {
			chunk := marks[chunkStart:min(chunkStart+maxMarksPerStatement, len(marks))]
			args := make([]any, 0, len(chunk)*4)
			for _, mark := range chunk {
				args = append(args, mark.partition, mark.indexID, mark.indexValue, lastWrite)
			}
			statement := fmt.Sprintf("INSERT INTO %s (`partition_id`, `index_id`, `index_value`, `last_write`) VALUES %s ON DUPLICATE KEY UPDATE `last_write` = VALUES(`last_write`)",
				markTable, strings.TrimSuffix(strings.Repeat("(?, ?, ?, ?), ", len(chunk)), ", "))
			if err := transaction.queue(statement, args...); err != nil {
				return fmt.Errorf("%s: writing %d cache-group mark(s): %w", markTable, len(chunk), err)
			}
		}
	}
	return nil
}

// copyColumnValue copies one column's field from source to destination; a column with no field has
// nothing to copy.
func copyColumnValue(column db.IColInfo, destination, source unsafe.Pointer) {
	columnInfo, isColumnInfo := column.(*db.ColumnInfo)
	if !isColumnInfo || columnInfo.Field == nil {
		return
	}
	fieldType := columnInfo.Field.Type
	reflect.NewAt(fieldType, columnInfo.Field.Pointer(destination)).Elem().Set(reflect.NewAt(fieldType, columnInfo.Field.Pointer(source)).Elem())
}

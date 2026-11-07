package obdriver

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
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
}

// groupMark is one __index_updated row: a cache group a write touched.
type groupMark struct {
	partition int64
	indexID   int16
	indexHash int32
}

// prepareSideWrites locks the chunk's stored records and derives its side writes. Each record gets
// its next version; a column the write leaves out keeps its stored value in the packed delta key and
// in the groups the record ends in; a stored record also marks the groups it leaves.
func (table *ObTable) prepareSideWrites(transaction *sql.Tx, ctx context.Context, phase writePhase, chunkStart, chunkEnd int) (chunkSideWrites, error) {
	isWritten := map[string]bool{}
	for _, stored := range phase.writtenColumns {
		isWritten[stored.name] = true
	}
	// The stored columns the side writes derive from: version and pinned columns, group sources.
	readColumns := []db.IColInfo{}
	if table.VersionCol != nil {
		readColumns = append(readColumns, table.VersionCol)
	}
	for _, deltaIndex := range table.DeltaIndexes {
		readColumns = append(readColumns, deltaIndex.Pinned...)
	}
	groupColumns := table.indexGroupStoredColumns()
	readColumns = append(readColumns, groupColumns...)
	storedByKey, err := table.lockStoredRecords(transaction, ctx, phase.records, chunkStart, chunkEnd, readColumns)
	if err != nil {
		return chunkSideWrites{}, err
	}

	sideWrites := chunkSideWrites{packedDeltaValues: map[string][]uint64{}}
	isMarked := map[groupMark]bool{}
	for recordIndex := chunkStart; recordIndex < chunkEnd; recordIndex++ {
		recordPointer := phase.records.At(recordIndex)
		storedRecord, isStored := storedByKey[table.primaryKeyToken(recordPointer)]
		partition := db.ConvertToInt64(table.GetPartKey().GetRawValue(recordPointer))

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
			storedVersion := int64(0)
			if isStored {
				storedVersion = db.ConvertToInt64(table.VersionCol.GetRawValue(storedRecord))
			}
			version := db.CoerceManagedInteger(table.VersionCol, cache.NextVersion(storedVersion))
			table.VersionCol.SetValue(recordPointer, version)
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
				for _, hashValue := range db.ComputeIndexGroupHashes(markedRecord, indexGroup.SourceColumns) {
					mark := groupMark{partition: partition, indexID: indexGroup.IndexID, indexHash: hashValue}
					if !isMarked[mark] {
						isMarked[mark] = true
						sideWrites.marks = append(sideWrites.marks, mark)
					}
				}
			}
		}
	}
	return sideWrites, nil
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
func (table *ObTable) indexGroupStoredColumns() []db.IColInfo {
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
// primaryKeyToken.
func (table *ObTable) lockStoredRecords(transaction *sql.Tx, ctx context.Context, records db.RecordSlice, chunkStart, chunkEnd int, readColumns []db.IColInfo) (map[string]unsafe.Pointer, error) {
	keyNames := make([]string, len(table.primaryKeyColumns))
	for position, keyColumn := range table.primaryKeyColumns {
		keyNames[position] = keyColumn.GetName()
	}
	selectedColumns := []storedColumn{}
	for _, stored := range table.storedColumns {
		isRead := slices.Contains(keyNames, stored.name) || slices.ContainsFunc(readColumns, func(column db.IColInfo) bool { return column.GetName() == stored.name })
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
	statement := fmt.Sprintf("SELECT %s FROM %s WHERE (%s) IN (%s) FOR UPDATE", quotedNames(selectedNames), table.qualifiedName(),
		quotedNames(keyNames), strings.TrimSuffix(strings.Repeat(keyPlaceholders+", ", chunkEnd-chunkStart), ", "))
	var storedRecords []unsafe.Pointer
	sink := recordSink{
		recordType:   records.RecordType,
		newRecord:    records.NewRecord,
		appendRecord: func(record unsafe.Pointer) { storedRecords = append(storedRecords, record) },
	}
	if err := table.queryIntoWith(transaction, ctx, statement, args, selectedColumns, sink); err != nil {
		return nil, fmt.Errorf("table %q: reading the stored rows: %w", table.Name, err)
	}
	storedByKey := make(map[string]unsafe.Pointer, len(storedRecords))
	for _, storedRecord := range storedRecords {
		storedByKey[table.primaryKeyToken(storedRecord)] = storedRecord
	}
	return storedByKey, nil
}

// writeDeltaMoves writes the moves of a chunk in its transaction.
func (table *ObTable) writeDeltaMoves(transaction *sql.Tx, ctx context.Context, moves [][]any) error {
	if len(moves) == 0 {
		return nil
	}
	args := make([]any, 0, len(moves)*5)
	for _, move := range moves {
		args = append(args, move...)
	}
	statement := fmt.Sprintf("INSERT INTO %s (`partition_id`, `index_tag`, `pinned_prefix`, `updated`, `id`) VALUES %s ON DUPLICATE KEY UPDATE `updated` = `updated`",
		deltaMovesTableSpec(table.Namespace, table.Name, "", 0).qualifiedName(), strings.TrimSuffix(strings.Repeat("(?, ?, ?, ?, ?), ", len(moves)), ", "))
	if _, err := transaction.ExecContext(ctx, statement, args...); err != nil {
		return fmt.Errorf("table %q: writing %d delta move(s): %w", table.Name, len(moves), err)
	}
	return nil
}

// writeGroupMarks stamps each touched group's last_write with the write's time, in the chunk's
// transaction. The marks are written in key order, so two writes of one company lock the shared mark
// rows in the same order and never deadlock on them.
func (table *ObTable) writeGroupMarks(transaction *sql.Tx, ctx context.Context, marks []groupMark) error {
	if len(marks) == 0 {
		return nil
	}
	slices.SortFunc(marks, func(left, right groupMark) int {
		return cmp.Or(cmp.Compare(left.partition, right.partition), cmp.Compare(left.indexID, right.indexID), cmp.Compare(left.indexHash, right.indexHash))
	})
	lastWrite := db.ManagedUnixTime()
	args := make([]any, 0, len(marks)*4)
	for _, mark := range marks {
		args = append(args, mark.partition, mark.indexID, mark.indexHash, lastWrite)
	}
	statement := fmt.Sprintf("INSERT INTO %s (`partition_id`, `index_id`, `index_hash`, `last_write`) VALUES %s ON DUPLICATE KEY UPDATE `last_write` = VALUES(`last_write`)",
		indexUpdatedTableSpec(table.Namespace, table.Name, 0).qualifiedName(), strings.TrimSuffix(strings.Repeat("(?, ?, ?, ?), ", len(marks)), ", "))
	if _, err := transaction.ExecContext(ctx, statement, args...); err != nil {
		return fmt.Errorf("table %q: writing %d cache-group mark(s): %w", table.Name, len(marks), err)
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

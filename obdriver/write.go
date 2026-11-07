package obdriver

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

// The write algorithm (docs/OCEANBASE_PLAN.md §9). Records go in chunks; a chunk is one transaction:
//
//  1. prepare (shared, db/write_prepare.go): SelfParse, autoincrement IDs reserved before the
//     transaction (a retried chunk burns none), key packing, managed stamps, the columns written and
//     the partial-update refusals;
//  2. a table with TypeDelta indexes or cache groups pre-reads and locks the chunk's stored records
//     (SELECT … FOR UPDATE, side_writes.go): the version goes up from the stored one, the packed delta
//     column takes the final pinned values (a partial write keeps the stored ones), a changed pinned
//     prefix writes a row to __delta_moves, and every cache group a record leaves or enters is marked;
//  3. the rows: INSERT … ON DUPLICATE KEY UPDATE over the written columns, so a column the write
//     leaves out keeps its stored value and a missing row is created (Scylla's upsert);
//  4. the by-IDs slots, the delta moves and the group marks, in the same transaction;
//  5. commit. Every index is synchronous, so the commit is visible through all of them at once.
//
//  6. a table with DataFrames locks the chunk's whole stored records and logs, in the same
//     transaction, every record whose frame lines the write changes (data_frame.go).
//
// After every chunk commits, a text-search table syncs its index (text_search.go).

const (
	writeChunkRecords = 1000
	// maxStatementPlaceholders is the protocol's limit on one statement's placeholders.
	maxStatementPlaceholders = 65_535
	// maxStatementBytes keeps a statement at half the tenant's 16 MB max_allowed_packet.
	maxStatementBytes = 8 << 20
)

// writePhase is one half of a write call: its records and which columns it writes.
type writePhase struct {
	records  db.RecordSlice
	isInsert bool
	// usesIncludeList marks an Update(columns...) phase: declaredColumns are the columns it writes.
	// Otherwise declaredColumns are the ones it leaves out.
	usesIncludeList bool
	declaredColumns []db.Coln
	// writtenColumns are the stored columns the statement sets, primary key first; settled by
	// resolveWrittenColumns once the records are prepared.
	writtenColumns []storedColumn
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

// resolveWrittenColumns settles the stored columns a phase writes. An insert writes every column but
// the excluded ones, and every generated column. An update writes the set every driver resolves
// (db.ResolveUpdateColumns, with its refusals), a computed column standing for its From columns.
func (table *ObTable) resolveWrittenColumns(phase *writePhase) {
	writtenNames := map[string]bool{}
	for _, keyColumn := range table.primaryKeyColumns {
		writtenNames[keyColumn.GetName()] = true
	}
	var resolvedColumns []db.IColInfo
	switch {
	case phase.isInsert:
		resolvedColumns = table.InsertColumns(phase.declaredColumns)
		for _, stored := range table.storedColumns {
			if !stored.hasField() && stored.column != table.CreatedCol && stored.column != table.UpdatedCol {
				writtenNames[stored.name] = true
			}
		}
	case phase.usesIncludeList:
		if len(phase.declaredColumns) == 0 {
			panic("No se incluyeron columnas a actualizar.")
		}
		resolvedColumns = table.ResolveUpdateColumns(phase.records, phase.declaredColumns, nil)
	default:
		resolvedColumns = table.ResolveUpdateColumns(phase.records, nil, phase.declaredColumns)
	}
	for _, resolvedColumn := range resolvedColumns {
		for _, storedColumn := range table.StoredColumnsOf(resolvedColumn) {
			writtenNames[storedColumn.GetName()] = true
		}
	}
	phase.writtenColumns = nil
	for _, keyColumn := range table.primaryKeyColumns {
		phase.writtenColumns = append(phase.writtenColumns, table.storedColumns[slices.IndexFunc(table.storedColumns, func(stored storedColumn) bool { return stored.name == keyColumn.GetName() })])
	}
	for _, stored := range table.storedColumns {
		if writtenNames[stored.name] && !slices.ContainsFunc(table.primaryKeyColumns, func(keyColumn db.IColInfo) bool { return keyColumn.GetName() == stored.name }) {
			phase.writtenColumns = append(phase.writtenColumns, stored)
		}
	}
}

func writeRecords(table *ObTable, phases []writePhase) error {
	pool, err := sharedConnection()
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
				return reserveTableKeys(table.Namespace, counterName, count)
			})
			if err != nil {
				return err
			}
		}
		stamps := table.StampManagedColumns(phase.records, phase.isInsert, int64(db.ManagedUnixTime()))
		table.resolveWrittenColumns(phase)
		chunkRecords := min(writeChunkRecords, maxStatementPlaceholders/len(phase.writtenColumns))
		for chunkStart := 0; chunkStart < phase.records.Len(); chunkStart += chunkRecords {
			chunkEnd := min(chunkStart+chunkRecords, phase.records.Len())
			err := runTransaction(pool, func(transaction *sql.Tx, ctx context.Context) error {
				return table.writeChunk(transaction, ctx, *phase, stamps, chunkStart, chunkEnd)
			})
			if err != nil {
				return fmt.Errorf("obdriver: writing %d record(s) to %q: %w", chunkEnd-chunkStart, table.Name, err)
			}
		}
	}
	if table.TextSearchIndex == nil {
		return nil
	}
	// Every phase goes in one sync, so each partition gets a single index write.
	textSearchWrites := make([]textSearchWrite, len(phases))
	for phaseIndex, phase := range phases {
		textSearchWrites[phaseIndex].records = phase.records
		for _, stored := range phase.writtenColumns {
			textSearchWrites[phaseIndex].columns = append(textSearchWrites[phaseIndex].columns, stored.column)
		}
	}
	return table.syncTextSearchAfterWrite(textSearchWrites)
}

// writeChunk writes records [chunkStart, chunkEnd) of a phase inside one transaction.
func (table *ObTable) writeChunk(transaction *sql.Tx, ctx context.Context, phase writePhase, stamps []db.ManagedStamp, chunkStart, chunkEnd int) error {
	sideWrites := chunkSideWrites{}
	if len(table.DeltaIndexes) > 0 || len(table.IndexGroups) > 0 {
		var err error
		if sideWrites, err = table.prepareSideWrites(transaction, ctx, phase, chunkStart, chunkEnd); err != nil {
			return err
		}
	}
	packedDeltaValues := sideWrites.packedDeltaValues
	// The log compares against the records as stored before this write: it reads them first.
	if table.dataFrames != nil {
		storedByKey, err := table.lockStoredRecords(transaction, ctx, phase.records, chunkStart, chunkEnd, table.dataFrames.storedColumns)
		if err != nil {
			return err
		}
		if err := table.writeDataFrameLog(transaction, ctx, phase, chunkStart, chunkEnd, storedByKey); err != nil {
			return err
		}
	}

	columnNames := make([]string, len(phase.writtenColumns))
	updateAssignments := []string{}
	for position, stored := range phase.writtenColumns {
		columnNames[position] = stored.name
		if position >= len(table.primaryKeyColumns) {
			updateAssignments = append(updateAssignments, fmt.Sprintf("%s = VALUES(%s)", quoteName(stored.name), quoteName(stored.name)))
		}
	}
	if len(updateAssignments) == 0 {
		// A write of keys only: an existing row stays as it is.
		updateAssignments = append(updateAssignments, fmt.Sprintf("%s = %s", quoteName(columnNames[0]), quoteName(columnNames[0])))
	}
	statementHead := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table.qualifiedName(), quotedNames(columnNames))
	statementTail := " ON DUPLICATE KEY UPDATE " + strings.Join(updateAssignments, ", ")
	rowPlaceholders := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(columnNames)), ", ") + ")"

	// Rows accumulate into one statement until it would pass maxStatementBytes.
	statementArgs, statementRows, statementBytes := []any{}, 0, 0
	flush := func() error {
		if statementRows == 0 {
			return nil
		}
		statement := statementHead + strings.TrimSuffix(strings.Repeat(rowPlaceholders+", ", statementRows), ", ") + statementTail
		_, err := transaction.ExecContext(ctx, statement, statementArgs...)
		statementArgs, statementRows, statementBytes = []any{}, 0, 0
		return err
	}
	for recordIndex := chunkStart; recordIndex < chunkEnd; recordIndex++ {
		recordPointer := phase.records.At(recordIndex)
		rowBytes := 0
		for _, stored := range phase.writtenColumns {
			var value any
			if packedValues, isDeltaColumn := packedDeltaValues[stored.name]; isDeltaColumn {
				value = packedValues[recordIndex-chunkStart]
			} else {
				var err error
				if value, err = bindStoredValue(table, stored, recordPointer, stamps[recordIndex]); err != nil {
					return fmt.Errorf("table %q: %w", table.Name, err)
				}
			}
			statementArgs = append(statementArgs, value)
			rowBytes += argumentBytes(value)
		}
		statementRows++
		statementBytes += rowBytes
		if statementBytes >= maxStatementBytes {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if table.CacheByIDs {
		if err := table.raiseWrittenSlots(transaction, ctx, phase.records, chunkStart, chunkEnd); err != nil {
			return err
		}
	}
	if err := table.writeDeltaMoves(transaction, ctx, sideWrites.moves); err != nil {
		return err
	}
	return table.writeGroupMarks(transaction, ctx, sideWrites.marks)
}

// argumentBytes approximates an argument's size on the wire, for the statement byte bound.
func argumentBytes(value any) int {
	switch typed := value.(type) {
	case string:
		return len(typed)
	case []byte:
		return len(typed)
	}
	return 8
}

// primaryKeyToken identifies a record by its primary key, as keyToken does for a scanned row.
func (table *ObTable) primaryKeyToken(recordPointer unsafe.Pointer) string {
	keyValues := make([]any, len(table.primaryKeyColumns))
	for position, keyColumn := range table.primaryKeyColumns {
		keyValues[position] = keyColumn.GetRawValue(recordPointer)
	}
	return keyToken(keyValues)
}

func keyToken(keyValues []any) string {
	parts := make([]string, len(keyValues))
	for position, value := range keyValues {
		switch typed := value.(type) {
		case []byte:
			parts[position] = string(typed)
		case int64:
			parts[position] = strconv.FormatInt(typed, 10)
		default:
			parts[position] = fmt.Sprint(typed)
		}
	}
	return strings.Join(parts, "\x00")
}

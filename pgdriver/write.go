package pgdriver

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

// The write algorithm (obdriver's, docs/OCEANBASE_PLAN.md §9). Records go in chunks; a chunk is one
// transaction:
//
//  1. prepare (shared, db/write_prepare.go): SelfParse, autoincrement IDs reserved before the
//     transaction (a retried chunk burns none), key packing, managed stamps, the columns written and
//     the partial-update refusals;
//  2. a table with TypeDelta indexes or cache groups pre-reads and locks the chunk's stored records
//     (SELECT … FOR UPDATE, side_writes.go): the version goes up from the stored one, the packed delta
//     column takes the final pinned values (a partial write keeps the stored ones), a changed pinned
//     prefix writes a row to __delta_moves, and every cache group a record leaves or enters is marked;
//  3. the rows: INSERT … ON CONFLICT DO UPDATE over the written columns, so a column the write
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
	// maxStatementBytes bounds one statement's arguments.
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
	// assertsNewKeys marks an Insert (Exec.InsertNew): the caller promises its keys are not stored.
	assertsNewKeys bool
	// hasOnlyNewKeys marks an insert with nothing stored under its keys: every key was minted from the
	// sequence, or the caller promised them new (assertsNewKeys). Nothing is read before writing, and
	// the rows go in as a plain INSERT.
	hasOnlyNewKeys bool
}

func insertPhase(records db.RecordSlice, columnsToExclude []db.Coln) writePhase {
	return writePhase{records: records, isInsert: true, declaredColumns: columnsToExclude}
}

// insertNewPhase is an insert whose keys the caller promises are new.
func insertNewPhase(records db.RecordSlice, columnsToExclude []db.Coln) writePhase {
	return writePhase{records: records, isInsert: true, assertsNewKeys: true, declaredColumns: columnsToExclude}
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
func (table *PgTable) resolveWrittenColumns(phase *writePhase) {
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

func writeRecords(table *PgTable, phases []writePhase) error {
	if err := writePhases(table, phases, runTransaction); err != nil {
		return err
	}
	return table.syncPhasesTextSearch(phases)
}

// writeRecordsIn writes phases inside an open transaction (db.RunTransaction). Its text-search sync
// must wait for the commit: it is returned, to run then.
func writeRecordsIn(transaction *writeTransaction, table *PgTable, phases []writePhase) (func() error, error) {
	inTransaction := func(body func(transaction *writeTransaction) error) error { return body(transaction) }
	if err := writePhases(table, phases, inTransaction); err != nil {
		return nil, err
	}
	return func() error { return table.syncPhasesTextSearch(phases) }, nil
}

// writePhases writes each phase chunk by chunk, every chunk through runChunk: its own transaction
// (runTransaction), or the one the caller opened (writeRecordsIn).
func writePhases(table *PgTable, phases []writePhase, runChunk func(body func(transaction *writeTransaction) error) error) error {
	for phaseIndex := range phases {
		phase := &phases[phaseIndex]
		if phase.records.Len() == 0 {
			continue
		}
		db.RunSelfParse(phase.records)
		if phase.isInsert {
			// Read before the keys are assigned: a record that comes with its key may already be stored.
			phase.hasOnlyNewKeys = phase.assertsNewKeys || table.AutoincrementCol != nil
			for recordIndex := 0; !phase.assertsNewKeys && phase.hasOnlyNewKeys && recordIndex < phase.records.Len(); recordIndex++ {
				if rawValue := table.AutoincrementCol.GetRawValue(phase.records.At(recordIndex)); rawValue != nil && db.ConvertToInt64(rawValue) > 0 {
					phase.hasOnlyNewKeys = false
				}
			}
			err := table.AssignAutoincrementKeys(phase.records, func(counterName string, count int) (int64, error) {
				return reserveTableKeys(table.Namespace, counterName, count)
			})
			if err != nil {
				return err
			}
		}
		stamps := table.StampManagedColumns(phase.records, phase.isInsert, int64(db.ManagedUnixTime()))
		table.resolveWrittenColumns(phase)
		// The same contract as MariaDB, whose upsert would resolve a unique-index clash by updating
		// the other row (db.TypeUniqueIndex).
		if !phase.hasOnlyNewKeys {
			if err := table.RefuseUniqueIndexOverwrite(phase.writtenColumnNames()); err != nil {
				return err
			}
		}
		chunkRecords := min(writeChunkRecords, maxStatementPlaceholders/len(phase.writtenColumns))
		for chunkStart := 0; chunkStart < phase.records.Len(); chunkStart += chunkRecords {
			chunkEnd := min(chunkStart+chunkRecords, phase.records.Len())
			err := runChunk(func(transaction *writeTransaction) error {
				return table.writeChunk(transaction, *phase, stamps, chunkStart, chunkEnd)
			})
			if err != nil {
				return fmt.Errorf("pgdriver: writing %d record(s) to %q: %w", chunkEnd-chunkStart, table.Name, err)
			}
		}
	}
	return nil
}

// syncPhasesTextSearch syncs the text-search index with what the phases wrote, once committed.
func (table *PgTable) syncPhasesTextSearch(phases []writePhase) error {
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

func (phase writePhase) writtenColumnNames() map[string]bool {
	names := make(map[string]bool, len(phase.writtenColumns))
	for _, stored := range phase.writtenColumns {
		names[stored.name] = true
	}
	return names
}

// writesAnyOf tells whether the phase writes one of columns.
func (phase writePhase) writesAnyOf(columns []db.IColInfo) bool {
	return slices.ContainsFunc(columns, func(column db.IColInfo) bool {
		return slices.ContainsFunc(phase.writtenColumns, func(stored storedColumn) bool { return stored.name == column.GetName() })
	})
}

// writeChunk writes records [chunkStart, chunkEnd) of a phase inside one transaction. The stored
// records are read (and locked) first only when a side write needs them (needsStoredRecords).
func (table *PgTable) writeChunk(transaction *writeTransaction, phase writePhase, stamps []db.ManagedStamp, chunkStart, chunkEnd int) error {
	storedByKey := map[string]unsafe.Pointer{}
	if table.needsStoredRecords(phase) {
		var err error
		if storedByKey, err = table.storedRecordsForChunk(transaction, phase, chunkStart, chunkEnd); err != nil {
			return err
		}
	}
	sideWrites := table.prepareSideWrites(phase, chunkStart, chunkEnd, storedByKey)
	packedDeltaValues := sideWrites.packedDeltaValues
	// The aggregate rows move by what the records give now minus what they gave as stored.
	if table.dataFrames != nil {
		if err := table.writeFrameAggregates(transaction, phase, chunkStart, chunkEnd, storedByKey); err != nil {
			return err
		}
	}

	// The packed delta columns whose pinned columns the phase leaves out: an existing row keeps its
	// stored prefix (packedDeltaAssignment).
	keepsStoredPrefix := map[string]bool{}
	for _, deltaIndex := range table.DeltaIndexes {
		keepsStoredPrefix[deltaIndex.PackedColumn.GetName()] = !phase.writesAnyOf(deltaIndex.Pinned)
	}
	columnNames := make([]string, len(phase.writtenColumns))
	updateAssignments := []string{}
	for position, stored := range phase.writtenColumns {
		columnNames[position] = stored.name
		switch {
		case position < len(table.primaryKeyColumns):
		case table.VersionCol != nil && stored.name == table.VersionCol.GetName():
			updateAssignments = append(updateAssignments, versionAssignment(stored.name))
		case keepsStoredPrefix[stored.name]:
			updateAssignments = append(updateAssignments, packedDeltaAssignment(stored.name))
		default:
			updateAssignments = append(updateAssignments, fmt.Sprintf("%s = EXCLUDED.%s", quoteName(stored.name), quoteName(stored.name)))
		}
	}
	// A write of keys only leaves an existing row as it is. An insert of new keys is a plain INSERT:
	// a key that turns out to be stored fails the write instead of overwriting the row.
	statementTail := " ON CONFLICT DO NOTHING"
	switch {
	case phase.hasOnlyNewKeys:
		statementTail = ""
	case len(updateAssignments) > 0:
		statementTail = fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s", quotedNames(table.mainTable.primaryKey), strings.Join(updateAssignments, ", "))
	}
	statementHead := fmt.Sprintf(`INSERT INTO %s AS "stored" (%s) VALUES `, table.qualifiedName(), quotedNames(columnNames))
	rowPlaceholders := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(columnNames)), ", ") + ")"

	// Rows accumulate into one statement until it would pass maxStatementBytes. An upsert may not
	// touch one row twice, so a record written twice in a chunk keeps only its last write.
	lastIndexByKey := map[string]int{}
	for recordIndex := chunkStart; recordIndex < chunkEnd; recordIndex++ {
		lastIndexByKey[table.primaryKeyToken(phase.records.At(recordIndex))] = recordIndex
	}
	statementArgs, statementRows, statementBytes := []any{}, 0, 0
	flush := func() error {
		if statementRows == 0 {
			return nil
		}
		statement := statementHead + strings.TrimSuffix(strings.Repeat(rowPlaceholders+", ", statementRows), ", ") + statementTail
		err := transaction.queue(statement, statementArgs...)
		statementArgs, statementRows, statementBytes = []any{}, 0, 0
		return err
	}
	for recordIndex := chunkStart; recordIndex < chunkEnd; recordIndex++ {
		recordPointer := phase.records.At(recordIndex)
		if lastIndexByKey[table.primaryKeyToken(recordPointer)] != recordIndex {
			continue
		}
		rowBytes := 0
		for _, stored := range phase.writtenColumns {
			var value any
			if packedValues, isDeltaColumn := packedDeltaValues[stored.name]; isDeltaColumn {
				value = packedValues[recordIndex-chunkStart]
			} else if table.VersionCol != nil && stored.name == table.VersionCol.GetName() {
				// A new row's version; an existing row's is raised by versionAssignment.
				value = int64(1)
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
		if err := table.raiseWrittenSlots(transaction, phase.records, chunkStart, chunkEnd); err != nil {
			return err
		}
	}
	if err := table.writeDeltaMoves(transaction, sideWrites.moves); err != nil {
		return err
	}
	return table.writeGroupMarks(transaction, sideWrites.marks)
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
func (table *PgTable) primaryKeyToken(recordPointer unsafe.Pointer) string {
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

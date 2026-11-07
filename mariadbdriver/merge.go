package mariadbdriver

import (
	"errors"
	"fmt"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

// mergeRecords is Scylla's Merge (scylla/merge.go): it reads the stored version of every record by
// primary key, sends a stored record through recordNeedsUpdate and a new one through onInsert, then
// writes the inserts and the accepted updates (all columns but columnsToExcludeUpdate). What the
// insert stamped (generated keys, managed columns) is copied back to the caller's records.
//
// The stored records are read before the write transaction, as on Scylla: the callbacks run once and
// a concurrent writer of the same record between the read and the write wins nothing back.
func mergeRecords(table *MariaTable, records db.RecordSlice, columnsToExcludeUpdate []db.Coln,
	recordNeedsUpdate func(previous, current unsafe.Pointer) bool, onInsert func(record unsafe.Pointer),
) error {
	if len(table.Keys) != 1 {
		return errors.New("mariadbdriver: merge requires exactly one key column in schema")
	}
	storedByKey, err := table.readStoredRecords(records)
	if err != nil {
		return fmt.Errorf("mariadbdriver: merge of %q: reading the stored records: %w", table.Name, err)
	}

	recordsToInsert, recordsToUpdate := records.NewRecordList(), records.NewRecordList()
	insertRecordIndexes := []int{}
	for recordIndex := range records.Len() {
		currentRecord := records.At(recordIndex)
		if storedRecord, isStored := storedByKey[table.primaryKeyToken(currentRecord)]; isStored {
			if recordNeedsUpdate(storedRecord, currentRecord) {
				records.AppendCopy(recordsToUpdate, currentRecord)
			}
			continue
		}
		onInsert(currentRecord)
		records.AppendCopy(recordsToInsert, currentRecord)
		insertRecordIndexes = append(insertRecordIndexes, recordIndex)
	}

	insertRecords := db.MakeReflectRecordSlice(recordsToInsert)
	err = writeRecords(table, []writePhase{
		insertPhase(insertRecords, nil),
		updateExcludePhase(db.MakeReflectRecordSlice(recordsToUpdate), columnsToExcludeUpdate),
	})
	if err != nil {
		return err
	}
	for insertIndex, recordIndex := range insertRecordIndexes {
		records.CopyRecord(records.At(recordIndex), insertRecords.At(insertIndex))
	}
	return nil
}

// readStoredRecords reads, by primary key, the stored records of the given ones, keyed by
// primaryKeyToken. A record whose key is not positive yet is an insert and is not looked up.
func (table *MariaTable) readStoredRecords(records db.RecordSlice) (map[string]unsafe.Pointer, error) {
	storedRecords := records.NewRecordList()
	keyNames := make([]string, len(table.primaryKeyColumns))
	for position, keyColumn := range table.primaryKeyColumns {
		keyNames[position] = keyColumn.GetName()
	}
	keyPlaceholders := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(keyNames)), ", ") + ")"
	selectedColumns := table.projectedColumns(&db.TableInfo{})
	selectedNames := make([]string, len(selectedColumns))
	for position, stored := range selectedColumns {
		selectedNames[position] = stored.name
	}

	lookedUpKeys := 0
	args := []any{}
	flush := func() error {
		if lookedUpKeys == 0 {
			return nil
		}
		statement := fmt.Sprintf("SELECT %s FROM %s %s WHERE (%s) IN (%s)", quotedNames(selectedNames), table.qualifiedName(), primaryKeyLookup,
			quotedNames(keyNames), strings.TrimSuffix(strings.Repeat(keyPlaceholders+", ", lookedUpKeys), ", "))
		err := table.queryInto(statement, args, selectedColumns, makeReflectRecordSink(storedRecords))
		lookedUpKeys, args = 0, []any{}
		return err
	}
	for recordIndex := range records.Len() {
		recordPointer := records.At(recordIndex)
		if keyValue := table.Keys[0].GetRawValue(recordPointer); keyValue == nil || db.ConvertToInt64(keyValue) <= 0 && table.Keys[0].GetType().FieldType != "string" {
			continue
		}
		for _, keyColumn := range table.primaryKeyColumns {
			args = append(args, keyColumn.GetRawValue(recordPointer))
		}
		if lookedUpKeys++; lookedUpKeys == writeChunkRecords {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}

	// The view is taken once the reads are done: no later append moves the records.
	stored := db.MakeReflectRecordSlice(storedRecords)
	storedByKey := make(map[string]unsafe.Pointer, stored.Len())
	for storedIndex := range stored.Len() {
		storedByKey[table.primaryKeyToken(stored.At(storedIndex))] = stored.At(storedIndex)
	}
	return storedByKey, nil
}

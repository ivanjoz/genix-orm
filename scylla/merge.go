package scylla

import (
	"fmt"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

type bulkLookupRecord struct {
	recordKey string
	keyToken  string
}

type bulkLookupGroup struct {
	partitionValue   any
	keyValues        []any
	seenKeyValues    map[string]bool
	recordsToResolve []bulkLookupRecord
}

func isNonPositiveNumericValue(value any) bool {
	switch typedValue := value.(type) {
	case int:
		return typedValue <= 0
	case int8:
		return typedValue <= 0
	case int16:
		return typedValue <= 0
	case int32:
		return typedValue <= 0
	case int64:
		return typedValue <= 0
	}
	if plainValue, isNamed := db.NormalizeNamedNumeric(value); isNamed {
		return isNonPositiveNumericValue(plainValue)
	}
	return false
}

// preloadExistingRecordsBySingleKey reads the stored version of every record, by primary key, and
// returns them by record key. The stored records live in a reflect-built []T of their own.
func preloadExistingRecordsBySingleKey(records recordSlice, scyllaTable ScyllaTable) (map[string]unsafe.Pointer, error) {
	existingByRecordKey := map[string]unsafe.Pointer{}

	partitionColumn := scyllaTable.GetPartKey()
	keyColumn := scyllaTable.GetKeys()[0]
	groupsByPartition := map[string]*bulkLookupGroup{}

	for recordIndex := range records.len() {
		recordPointer := records.at(recordIndex)
		recordKey := makePrimaryKeyRecordKey(recordPointer, scyllaTable)
		keyValue := keyColumn.GetRawValue(recordPointer)

		// Temporary numeric keys are always inserts, so skip DB lookups.
		if isNonPositiveNumericValue(keyValue) {
			continue
		}

		partitionValue := any(nil)
		partitionToken := "__no_partition__"
		if partitionColumn != nil {
			partitionValue = partitionColumn.GetRawValue(recordPointer)
			partitionToken = partitionColumn.GetValueString(recordPointer)
		}
		group, groupExists := groupsByPartition[partitionToken]
		if !groupExists {
			group = &bulkLookupGroup{
				partitionValue:   partitionValue,
				keyValues:        []any{},
				seenKeyValues:    map[string]bool{},
				recordsToResolve: []bulkLookupRecord{},
			}
			groupsByPartition[partitionToken] = group
		}

		keyToken := keyColumn.GetValueString(recordPointer)
		if !group.seenKeyValues[keyToken] {
			group.keyValues = append(group.keyValues, keyValue)
			group.seenKeyValues[keyToken] = true
		}
		group.recordsToResolve = append(group.recordsToResolve, bulkLookupRecord{
			recordKey: recordKey,
			keyToken:  keyToken,
		})
	}

	for _, group := range groupsByPartition {
		if len(group.keyValues) == 0 {
			continue
		}

		statements := []ColumnStatement{
			{Col: keyColumn.GetName(), Operator: "IN", Values: group.keyValues},
		}
		if partitionColumn != nil {
			// Keep tenant isolation by constraining the optional partition key when present.
			statements = append(statements, ColumnStatement{
				Col: partitionColumn.GetName(), Operator: "=", Value: group.partitionValue,
			})
		}

		queryResult := records.newRecordList()
		queryInfo := &TableInfo{Statements: statements}
		if err := selectExec(makeReflectRecordSink(queryResult), queryInfo, scyllaTable, nil, time.Now()); err != nil {
			return nil, err
		}

		// The view is taken once the read is done: no later append can move the records.
		foundRecords := makeReflectRecordSlice(queryResult)
		foundByKeyToken := map[string]unsafe.Pointer{}
		for resultIndex := range foundRecords.len() {
			foundPointer := foundRecords.at(resultIndex)
			foundKeyToken := keyColumn.GetValueString(foundPointer)
			if _, alreadyMapped := foundByKeyToken[foundKeyToken]; !alreadyMapped {
				foundByKeyToken[foundKeyToken] = foundPointer
			}
		}

		for _, lookupRecord := range group.recordsToResolve {
			if foundRecord, wasFound := foundByKeyToken[lookupRecord.keyToken]; wasFound {
				existingByRecordKey[lookupRecord.recordKey] = foundRecord
			}
		}
	}

	return existingByRecordKey, nil
}

// textSearchRecordChanged reports whether a record's indexed text or status differs between the
// previously-stored record and the incoming one. When neither changed, the index write for that
// record is redundant and is skipped.
func textSearchRecordChanged(info *textSearchIndexInfo, previousPointer, currentPointer unsafe.Pointer) bool {
	for _, column := range info.columns {
		if !column.FieldsEqual(previousPointer, currentPointer) {
			return true
		}
	}
	return info.statusColumn != nil && !info.statusColumn.FieldsEqual(previousPointer, currentPointer)
}

func Merge[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T,
	columnsToExcludeUpdate []Coln,
	onUpdateHandler func(prev, current *T) bool, /* need update? */
	onInsertHandler func(e *T),
) error {
	if onUpdateHandler == nil || onInsertHandler == nil {
		panic("Merge requires non-nil onUpdateHandler and onInsertHandler")
	}

	if records == nil {
		return Err("merge received nil records pointer")
	}

	if len(*records) == 0 {
		return nil
	}

	// Only the handlers need T; the merge itself runs on records by pointer.
	return mergeRecords(makeRecordSlice(records), MakeScyllaTable[T, E](), columnsToExcludeUpdate,
		func(previous, current unsafe.Pointer) bool { return onUpdateHandler((*T)(previous), (*T)(current)) },
		func(record unsafe.Pointer) { onInsertHandler((*T)(record)) })
}

func mergeRecords(
	records recordSlice, scyllaTable ScyllaTable, columnsToExcludeUpdate []Coln,
	recordNeedsUpdate func(previous, current unsafe.Pointer) bool, onInsert func(record unsafe.Pointer),
) error {
	if len(scyllaTable.GetKeys()) != 1 {
		return Err(`merge requires exactly one key column in schema`)
	}

	recordsToInsert := records.newRecordList()
	recordsToUpdate := records.newRecordList()
	insertRecordIndexes := []int{}
	skipTextSearchRecordsIDs := []int64{}
	textSearchInfo := scyllaTable.textSearchIndex
	existingByRecordKey, err := preloadExistingRecordsBySingleKey(records, scyllaTable)
	if err != nil {
		return Err("merge bulk lookup failed:", err)
	}

	for recordIndex := range records.len() {
		currentRecordPointer := records.at(recordIndex)
		recordKey := makePrimaryKeyRecordKey(currentRecordPointer, scyllaTable)

		if previousRecordPointer, wasFound := existingByRecordKey[recordKey]; wasFound {
			if recordNeedsUpdate(previousRecordPointer, currentRecordPointer) {
				records.appendCopy(recordsToUpdate, currentRecordPointer)
				// We already hold the previous record, so decide here whether the searchable
				// content actually changed and let the write path skip unchanged re-indexes.
				if textSearchInfo != nil && !textSearchRecordChanged(textSearchInfo, previousRecordPointer, currentRecordPointer) {
					recordID := convertToInt64(scyllaTable.Keys[0].GetRawValue(currentRecordPointer))
					skipTextSearchRecordsIDs = append(skipTextSearchRecordsIDs, recordID)
				}
			}
			continue
		}

		onInsert(currentRecordPointer)
		records.appendCopy(recordsToInsert, currentRecordPointer)
		insertRecordIndexes = append(insertRecordIndexes, recordIndex)
	}

	insertRecords := makeReflectRecordSlice(recordsToInsert)
	updateRecords := makeReflectRecordSlice(recordsToUpdate)
	if insertRecords.len() == 0 && updateRecords.len() == 0 {
		return nil
	}

	// Combine inserts and updates into a single batch so the merge issues one Scylla round-trip,
	// one managed-counter prefetch, one cache-version bump, and one text-search sync.
	fmt.Println("Merge | records to Insert:", insertRecords.len(), "Update:", updateRecords.len(), "| Text skip:", len(skipTextSearchRecordsIDs))

	if err := executeInsertUpdateBatchErased(erasedInsertUpdateParams{
		insertRecords:            insertRecords,
		updateRecords:            updateRecords,
		compiledTable:            scyllaTable,
		columnsToUpdate:          columnsToExcludeUpdate,
		skipTextSearchRecordsIDs: skipTextSearchRecordsIDs,
	}); err != nil {
		return err
	}

	// Insert may mutate records (e.g. autoincrement IDs), so propagate persisted values back.
	for insertIndex, originalRecordIndex := range insertRecordIndexes {
		records.copyRecord(records.at(originalRecordIndex), insertRecords.at(insertIndex))
	}

	return nil
}

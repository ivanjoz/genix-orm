package scylla

import (
	"fmt"
	"reflect"
	"slices"
	"unsafe"

	"github.com/gocql/gocql"
	"github.com/ivanjoz/genix-orm/db"
)

type indexUpdatedRow struct {
	partitionID int32
	indexID     int16
	indexHash   int32
	// lastWrite (SUnixTime) is when the group's latest write landed: the label clients compare.
	lastWrite int64
}

// indexGroupSettleTicks (16 s) is how long a group stays unsettled after its last write. An
// unsettled group is always sent whole and labelled 0, which no stored last_write matches, so a
// client that read it while writes of the same 2 s tick, or a write still landing, were in flight
// refetches it once it settles.
const indexGroupSettleTicks = 8

func appendIndexUpdatedRowsForRecord(
	recordPointer unsafe.Pointer,
	scyllaTable *ScyllaTable,
	partitionValue int32,
	lastWriteValue int64,
	rowsByPartitionAndHash map[string]indexUpdatedRow,
	rowsToPersist *[]indexUpdatedRow,
) {
	for _, indexGroup := range scyllaTable.IndexGroups {
		hashValues := db.ComputeIndexGroupHashes(recordPointer, indexGroup.SourceColumns)
		for _, hashValue := range hashValues {
			dedupKey := fmt.Sprintf("%d|%d|%d", partitionValue, indexGroup.IndexID, hashValue)
			currentRow, exists := rowsByPartitionAndHash[dedupKey]
			if exists && currentRow.lastWrite >= lastWriteValue {
				continue
			}

			nextRow := indexUpdatedRow{
				partitionID: partitionValue,
				indexID:     indexGroup.IndexID,
				indexHash:   hashValue,
				lastWrite:   lastWriteValue,
			}
			rowsByPartitionAndHash[dedupKey] = nextRow
			if exists {
				for rowIndex := range *rowsToPersist {
					row := &(*rowsToPersist)[rowIndex]
					if row.partitionID == partitionValue && row.indexID == indexGroup.IndexID && row.indexHash == hashValue {
						*row = nextRow
						break
					}
				}
				continue
			}

			*rowsToPersist = append(*rowsToPersist, nextRow)
		}
	}
}

func getIndexUpdatedTableCreateScript(keyspace string, tableInfo *indexUpdatedTableInfo) string {
	// partition_id stays int because table partitions in this codebase are standardized as int32.
	return fmt.Sprintf(`CREATE TABLE %v.%v (
		partition_id int,
		index_id smallint,
		index_hash int,
		last_write int,
		PRIMARY KEY ((partition_id), index_id, index_hash)
	)
	%v;`,
		keyspace,
		tableInfo.name,
		makeStatementWith,
	)
}

var persistIndexUpdatedRows = persistIndexUpdatedRowsBatch

func persistIndexUpdatedRowsBatch(keyspace string, tableName string, rows []indexUpdatedRow) error {
	if len(rows) == 0 {
		return nil
	}

	session := getScyllaConnection()
	batch := session.NewBatch(gocql.UnloggedBatch)
	insertQuery := fmt.Sprintf(`INSERT INTO %v.%v (partition_id, index_id, index_hash, last_write) VALUES (?, ?, ?, ?)`,
		keyspace, tableName)

	for _, row := range rows {
		batch.Query(insertQuery, row.partitionID, row.indexID, row.indexHash, int32(row.lastWrite))
	}

	return session.ExecuteBatch(batch)
}

// materializeGroupHashIndex gives a group's hash column its CQL. A scalar hash is indexed locally,
// one partition's index answering a grouped read; Scylla has no local index on a collection
// ("not implemented yet"), so the hashes of a slice key are a set<int> with a global index and are
// bound as a collection literal. Reads are planned by db.PlanSourceSteps.
func materializeGroupHashIndex(dbTable *ScyllaTable, view *viewInfo) {
	indexGroup := view.IndexGroup
	usesCollectionValues := indexGroup.UsesCollectionValues
	virtualColumn := view.Column.(*columnInfo)
	virtualColumnName := virtualColumn.GetName()

	if usesCollectionValues {
		describedHashes := virtualColumn.GetValueFn
		virtualColumn.GetValueFn = func(ptr unsafe.Pointer) any {
			return makeSignedIntCollectionLiteral(virtualColumn.DBType, describedHashes(ptr).([]int32))
		}
	}

	view.getCreateScript = func() string {
		if usesCollectionValues {
			return fmt.Sprintf(`CREATE INDEX %v ON %v (VALUES(%v))`, view.Name, dbTable.GetFullName(), virtualColumnName)
		}
		return fmt.Sprintf(`CREATE INDEX %v ON %v ((%v),%v)`, view.Name, dbTable.GetFullName(), dbTable.GetPartKey().GetName(), virtualColumnName)
	}
}

// indexGroupStoredColumns are the stored columns the index groups hash: a computed source
// contributes its From columns.
func indexGroupStoredColumns(scyllaTable *ScyllaTable) []IColInfo {
	var storedColumns []IColInfo
	for _, indexGroup := range scyllaTable.IndexGroups {
		for _, sourceColumn := range indexGroup.SourceColumns {
			for _, storedColumn := range scyllaTable.StoredColumnsOf(sourceColumn.Column) {
				if !slices.ContainsFunc(storedColumns, func(column IColInfo) bool { return column.GetName() == storedColumn.GetName() }) {
					storedColumns = append(storedColumns, storedColumn)
				}
			}
		}
	}
	return storedColumns
}

// collectIndexGroupMarks lists the groups a write touches, read before it lands (stored holds the
// rows as they were). Each record marks the groups it ends in — a group column the write leaves out
// keeps its stored value, so a partial update marks the record's real group, not the zero value's —
// and, when it was stored, the groups it was in: a record that leaves a group changes it too.
// lastWrite is left 0: markIndexGroupsBeforeWrite stamps it.
func collectIndexGroupMarks(scyllaTable *ScyllaTable, phases []writePhase, stored storedRows) []indexUpdatedRow {
	if len(scyllaTable.IndexGroups) == 0 || scyllaTable.indexUpdatedTable == nil {
		return nil
	}
	groupColumns := indexGroupStoredColumns(scyllaTable)
	partitionColumn, keyColumn := scyllaTable.GetPartKey(), scyllaTable.Keys[0]

	rowsByPartitionAndHash := map[string]indexUpdatedRow{}
	var marks []indexUpdatedRow
	for _, phase := range phases {
		isWritten := map[string]bool{}
		for _, column := range phase.writtenColumns {
			isWritten[column.GetName()] = true
		}
		for i := range phase.records.len() {
			recordPointer := phase.records.at(i)
			partition := db.ConvertToInt64(partitionColumn.GetRawValue(recordPointer))
			storedRow, isStored := stored[[2]int64{partition, db.ConvertToInt64(keyColumn.GetRawValue(recordPointer))}]

			finalRecord := reflect.New(phase.records.recordType).UnsafePointer()
			phase.records.copyRecord(finalRecord, recordPointer)
			if isStored {
				leftRecord := reflect.New(phase.records.recordType).UnsafePointer()
				for _, column := range groupColumns {
					storedValue, hasValue := storedRow[column.GetName()]
					if !hasValue {
						continue
					}
					column.SetValue(leftRecord, storedValue)
					if !isWritten[column.GetName()] {
						column.SetValue(finalRecord, storedValue)
					}
				}
				appendIndexUpdatedRowsForRecord(leftRecord, scyllaTable, int32(partition), 0, rowsByPartitionAndHash, &marks)
			}
			appendIndexUpdatedRowsForRecord(finalRecord, scyllaTable, int32(partition), 0, rowsByPartitionAndHash, &marks)
		}
	}
	return marks
}

// markIndexGroupsBeforeWrite publishes the marks before the rows are written, with last_write = now.
// Marking first fails safe: if this fails the write stops with nothing written, and if the write dies
// after it the cost is one needless refetch. A group stays unsettled for indexGroupSettleTicks (16 s)
// from its mark, which covers the data landing after it. A data phase slower than that can be cached
// without its rows until the group's next write: the accepted gap (ORM_INTERNALS.md, Group freshness).
func markIndexGroupsBeforeWrite(scyllaTable *ScyllaTable, marks []indexUpdatedRow) error {
	if len(marks) == 0 {
		return nil
	}
	rowsToPersist := slices.Clone(marks)
	markerTime := int64(getManagedUnixTime())
	for rowIndex := range rowsToPersist {
		rowsToPersist[rowIndex].lastWrite = markerTime
	}

	// Persisted synchronously. These rows are the freshness marker the frontend group cache
	// reads (cc-gh / cc-lw), so losing one silently strands every client on stale data. A
	// fire-and-forget goroutine cannot guarantee delivery: AWS Lambda freezes the execution
	// environment the moment the handler returns, suspending the write until the environment is
	// reused — or discarding it outright when the environment is recycled. The
	// cost is one round trip before the rows.
	return persistIndexUpdatedRows(scyllaTable.Namespace, scyllaTable.indexUpdatedTable.name, rowsToPersist)
}

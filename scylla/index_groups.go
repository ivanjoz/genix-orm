package scylla

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
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

func registerIndexGroup(dbTable *ScyllaTable, idxCount *int8, indexCfg Index) {
	sourceColumnNames := make([]string, 0, len(indexCfg.Keys))
	for _, key := range indexCfg.Keys {
		sourceColumnNames = append(sourceColumnNames, key.GetName())
	}
	indexID := dbTable.AllocateIndexGroupID(sourceColumnNames)
	if len(indexCfg.Cols) > 0 || indexCfg.UseHash {
		panic(fmt.Sprintf(`Table "%v": a cache group on %v takes Keys only (no Cols, no UseHash)`, dbTable.Name, sourceColumnNames))
	}

	if indexCfg.Type == TypeCacheGroupFromKey {
		registerInheritFromKeyIndexGroup(dbTable, indexCfg, indexID, sourceColumnNames)
		return
	}

	if len(indexCfg.Keys) == 1 {
		singleKeyIndex := Index{Type: TypeLocalIndex, Keys: indexCfg.Keys}
		registerSchemaLocalIndex(dbTable, idxCount, singleKeyIndex)
		dbTable.IndexGroups = append(dbTable.IndexGroups, db.IndexGroupInfo{
			Name:    indexCfg.Keys[0].GetName(),
			IndexID: indexID,
			SourceColumns: []db.IndexGroupSourceColumn{{
				Column: dbTable.ColumnsMap[indexCfg.Keys[0].GetName()],
			}},
			SnapshotInS3: indexCfg.Type == TypeCacheGroupS3,
		})
		if dbTable.indexUpdatedTable == nil {
			dbTable.indexUpdatedTable = &indexUpdatedTableInfo{name: dbTable.Name + "__index_updated"}
		}
		return
	}

	sourceColumns := make([]db.IndexGroupSourceColumn, 0, len(indexCfg.Keys))
	usesCollectionValues := false
	for _, key := range indexCfg.Keys {
		baseColumn := dbTable.ColumnsMap[key.GetName()]
		if baseColumn == nil || baseColumn.IsNil() {
			panic(fmt.Sprintf(`Table "%v": IndexGroup column "%v" was not found`, dbTable.Name, key.GetName()))
		}
		sourceColumns = append(sourceColumns, db.IndexGroupSourceColumn{Column: baseColumn})
		if baseColumn.GetType().IsSlice {
			usesCollectionValues = true
		}
	}

	virtualColumnName := db.MakeIndexGroupVirtualColumnName(sourceColumnNames, usesCollectionValues)
	if _, exists := dbTable.ColumnsMap[virtualColumnName]; exists {
		panic(fmt.Sprintf(`Table "%v": generated IndexGroup column already exists: %v`, dbTable.Name, virtualColumnName))
	}

	virtualColumn := &columnInfo{
		ColInfo: colInfo{
			Name:      virtualColumnName,
			FieldName: virtualColumnName,
			IsVirtual: true,
			Idx:       dbTable.MaxColIdx,
		},
		ColType: db.GetColTypeByName("int32"),
	}
	if usesCollectionValues {
		virtualColumn.ColType = colType{
			Type:      13,
			FieldType: "[]int32",
			DBType:    "set<int>",
			IsSlice:   true,
		}
	}

	sourceColumnsLocal := slices.Clone(sourceColumns)
	virtualColumn.GetRawValueFn = func(ptr unsafe.Pointer) any {
		hashes := db.ComputeIndexGroupHashes(ptr, sourceColumnsLocal)
		if usesCollectionValues {
			return hashes
		}
		if len(hashes) == 0 {
			return int32(0)
		}
		return hashes[0]
	}
	virtualColumn.GetStatementValueFn = func(ptr unsafe.Pointer) any {
		hashes := db.ComputeIndexGroupHashes(ptr, sourceColumnsLocal)
		if usesCollectionValues {
			return hashes
		}
		if len(hashes) == 0 {
			return int32(0)
		}
		return hashes[0]
	}
	virtualColumn.GetValueFn = func(ptr unsafe.Pointer) any {
		hashes := db.ComputeIndexGroupHashes(ptr, sourceColumnsLocal)
		if usesCollectionValues {
			return makeSignedIntCollectionLiteral(virtualColumn.DBType, hashes)
		}
		if len(hashes) == 0 {
			return int32(0)
		}
		return hashes[0]
	}

	dbTable.MaxColIdx++
	dbTable.ColumnsMap[virtualColumn.GetName()] = virtualColumn

	// Every grouped read pins the partition, so the hash is indexed locally: one partition's index
	// answers it. Scylla has no local index on a collection ("not implemented yet"), so a set<int>
	// hash (a slice Key) keeps a global index. The suffix names the kind, so a change recreates it.
	indexName := fmt.Sprintf(`%v__%v_index_1`, dbTable.Name, virtualColumnName)
	if usesCollectionValues {
		indexName = fmt.Sprintf(`%v__%v_index_0`, dbTable.Name, virtualColumnName)
	}
	index := &viewInfo{
		IndexSource: &db.IndexSource{
			Type:               3,
			Name:               indexName,
			Idx:                *idxCount,
			Column:             virtualColumn,
			Columns:            slices.Clone(sourceColumnNames),
			RequiresPostFilter: false,
		},
	}
	index.getCreateScript = func() string {
		if usesCollectionValues {
			return fmt.Sprintf(`CREATE INDEX %v ON %v (VALUES(%v))`, index.Name, dbTable.GetFullName(), virtualColumnName)
		}
		return fmt.Sprintf(`CREATE INDEX %v ON %v ((%v),%v)`, index.Name, dbTable.GetFullName(), dbTable.GetPartKey().GetName(), virtualColumnName)
	}

	virtualColumnNameLocal := virtualColumnName
	usesCollectionLocal := usesCollectionValues
	index.getStatementPrepared = func(statements ...ColumnStatement) []boundWhereClause {
		// Build all hash input combinations using resolveIndexGroupQueryValues, which handles
		// =, CONTAINS, IN, and BETWEEN (BETWEEN is expanded into one value per step in range).
		valuesGroups := [][]int64{{}}
		for _, sourceColumn := range sourceColumnsLocal {
			st, err := findIndexGroupStatement(sourceColumn.Column.GetName(), statements)
			if err != nil {
				return nil
			}
			columnValues, err := resolveIndexGroupQueryValues(st)
			if err != nil || len(columnValues) == 0 {
				return nil
			}
			nextGroups := make([][]int64, 0, len(valuesGroups)*len(columnValues))
			for _, vg := range valuesGroups {
				for _, v := range columnValues {
					nextGroups = append(nextGroups, append(append([]int64{}, vg...), v))
				}
			}
			valuesGroups = nextGroups
		}

		// Emit one WHERE clause per unique hash value.
		// Collection columns (set<int>) require CONTAINS; scalar columns use =.
		seen := map[int32]struct{}{}
		clauses := make([]boundWhereClause, 0, len(valuesGroups))
		for _, vg := range valuesGroups {
			hashValue := db.HashInt64(vg...)
			if _, exists := seen[hashValue]; exists {
				continue
			}
			seen[hashValue] = struct{}{}
			clause := fmt.Sprintf("%v = ?", virtualColumnNameLocal)
			if usesCollectionLocal {
				clause = fmt.Sprintf("%v CONTAINS ?", virtualColumnNameLocal)
			}
			clauses = append(clauses, boundWhereClause{
				Clause: clause,
				Values: []any{hashValue},
			})
		}
		return clauses
	}
	*idxCount = *idxCount + 1
	dbTable.indexes[index.Name] = index
	dbTable.IndexGroups = append(dbTable.IndexGroups, db.IndexGroupInfo{
		Name:                 strings.Join(sourceColumnNames, "_"),
		IndexID:              indexID,
		SourceColumns:        sourceColumns,
		VirtualColumn:        virtualColumn,
		UsesCollectionValues: usesCollectionValues,
		SnapshotInS3:         indexCfg.Type == TypeCacheGroupS3,
	})

	if dbTable.indexUpdatedTable == nil {
		dbTable.indexUpdatedTable = &indexUpdatedTableInfo{name: dbTable.Name + "__index_updated"}
	}
}

func registerInheritFromKeyIndexGroup(dbTable *ScyllaTable, indexCfg Index, indexID int16, sourceColumnNames []string) {
	if len(dbTable.KeyIntPacking) == 0 {
		panic(fmt.Sprintf(`Table "%v": TypeCacheGroupFromKey requires KeyIntPacking on the table`, dbTable.Name))
	}
	if len(indexCfg.Keys) > len(dbTable.KeyIntPacking) {
		panic(fmt.Sprintf(`Table "%v": TypeCacheGroupFromKey index has %d keys but KeyIntPacking only declares %d columns`,
			dbTable.Name, len(indexCfg.Keys), len(dbTable.KeyIntPacking)))
	}
	for columnIndex, key := range indexCfg.Keys {
		packedColumnName := dbTable.KeyIntPacking[columnIndex].GetName()
		if key.GetName() != packedColumnName {
			panic(fmt.Sprintf(`Table "%v": TypeCacheGroupFromKey keys must be a prefix of KeyIntPacking. Position %d expected "%v" but got "%v"`,
				dbTable.Name, columnIndex, packedColumnName, key.GetName()))
		}
		baseColumn := dbTable.ColumnsMap[key.GetName()]
		if baseColumn == nil || baseColumn.IsNil() {
			panic(fmt.Sprintf(`Table "%v": TypeCacheGroupFromKey column "%v" was not found`, dbTable.Name, key.GetName()))
		}
		if baseColumn.GetType().IsSlice {
			panic(fmt.Sprintf(`Table "%v": TypeCacheGroupFromKey does not support slice column "%v"`, dbTable.Name, key.GetName()))
		}
	}

	sourceColumns := make([]db.IndexGroupSourceColumn, 0, len(indexCfg.Keys))
	for _, key := range indexCfg.Keys {
		sourceColumns = append(sourceColumns, db.IndexGroupSourceColumn{
			Column: dbTable.ColumnsMap[key.GetName()],
		})
	}

	dbTable.IndexGroups = append(dbTable.IndexGroups, db.IndexGroupInfo{
		Name:           strings.Join(sourceColumnNames, "_"),
		IndexID:        indexID,
		SourceColumns:  sourceColumns,
		InheritFromKey: true,
	})

	if dbTable.indexUpdatedTable == nil {
		dbTable.indexUpdatedTable = &indexUpdatedTableInfo{name: dbTable.Name + "__index_updated"}
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

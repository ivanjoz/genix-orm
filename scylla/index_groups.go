package scylla

import (
	"fmt"
	"slices"
	"strings"
	"unsafe"

	"github.com/gocql/gocql"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/viant/xunsafe"
)

type indexUpdatedRow struct {
	partitionID   int32
	indexID       int16
	indexHash     int32
	updateCounter int64
}

func makeIndexGroupCacheKey(indexID int16, indexHash int32) int64 {
	return (int64(indexID) << 32) | int64(uint32(indexHash))
}

func allocateIndexGroupID(scyllaTable *ScyllaTable, sourceColumnNames []string) int16 {
	logicalName := strings.Join(sourceColumnNames, "_")
	if scyllaTable.indexGroupIDs == nil {
		scyllaTable.indexGroupIDs = map[int16]string{}
	}

	candidateID := int16(BasicHashInt(logicalName))
	if candidateID == 0 {
		candidateID = 1
	}

	for {
		if existingName, exists := scyllaTable.indexGroupIDs[candidateID]; !exists {
			scyllaTable.indexGroupIDs[candidateID] = logicalName
			return candidateID
		} else if existingName == logicalName {
			return candidateID
		}
		candidateID++
		if candidateID == 0 {
			candidateID = 1
		}
	}
}

func appendIndexUpdatedRowsForRecord(
	recordPointer unsafe.Pointer,
	scyllaTable *ScyllaTable,
	partitionValue int32,
	updateCounterValue int64,
	rowsByPartitionAndHash map[string]indexUpdatedRow,
	rowsToPersist *[]indexUpdatedRow,
) {
	for _, indexGroup := range scyllaTable.indexGroups {
		hashValues := computeIndexGroupHashes(recordPointer, indexGroup.sourceColumns)
		for _, hashValue := range hashValues {
			dedupKey := fmt.Sprintf("%d|%d|%d", partitionValue, indexGroup.indexID, hashValue)
			currentRow, exists := rowsByPartitionAndHash[dedupKey]
			if exists && currentRow.updateCounter >= updateCounterValue {
				continue
			}

			nextRow := indexUpdatedRow{
				partitionID:   partitionValue,
				indexID:       indexGroup.indexID,
				indexHash:     hashValue,
				updateCounter: updateCounterValue,
			}
			rowsByPartitionAndHash[dedupKey] = nextRow
			if exists {
				for rowIndex := range *rowsToPersist {
					row := &(*rowsToPersist)[rowIndex]
					if row.partitionID == partitionValue && row.indexID == indexGroup.indexID && row.indexHash == hashValue {
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
		update_counter int,
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
	insertQuery := fmt.Sprintf(`INSERT INTO %v.%v (partition_id, index_id, index_hash, update_counter) VALUES (?, ?, ?, ?)`,
		keyspace, tableName)

	for _, row := range rows {
		batch.Query(insertQuery, row.partitionID, row.indexID, row.indexHash, row.updateCounter)
	}

	return session.ExecuteBatch(batch)
}

func makeIndexGroupVirtualColumnName(sourceColumnNames []string, usesCollection bool) string {
	prefix := "zz_ig_"
	if usesCollection {
		prefix = "zz_igs_"
	}
	return prefix + strings.Join(sourceColumnNames, "_")
}

func computeIndexGroupHashes(ptr unsafe.Pointer, sourceColumns []indexGroupSourceColumn) []int32 {
	combinations := [][]int64{{}}

	for _, sourceColumn := range sourceColumns {
		columnValues := flattenCompositeInt64Values(sourceColumn.column.GetRawValue(ptr))
		if len(columnValues) == 0 {
			return nil
		}

		nextCombinations := make([][]int64, 0, len(combinations)*len(columnValues))
		for _, combination := range combinations {
			for _, value := range columnValues {
				combinationExpanded := append(append([]int64{}, combination...), value)
				nextCombinations = append(nextCombinations, combinationExpanded)
			}
		}
		combinations = nextCombinations
	}

	uniqueHashes := map[int32]struct{}{}
	for _, combination := range combinations {
		uniqueHashes[HashInt64(combination...)] = struct{}{}
	}

	hashes := make([]int32, 0, len(uniqueHashes))
	for hashValue := range uniqueHashes {
		hashes = append(hashes, hashValue)
	}
	slices.Sort(hashes)
	return hashes
}

func buildIndexGroupHashValues(sourceColumns []indexGroupSourceColumn, statements []ColumnStatement) []int32 {
	valuesGroups := [][]int64{{}}

	for _, sourceColumn := range sourceColumns {
		var statementFound *ColumnStatement
		for statementIndex := range statements {
			if statements[statementIndex].Col == sourceColumn.column.GetName() {
				statementFound = &statements[statementIndex]
				break
			}
		}
		if statementFound == nil {
			return nil
		}

		rawValues := []any{statementFound.Value}
		if len(statementFound.Values) > 0 {
			rawValues = statementFound.Values
		}

		columnValues := make([]int64, 0, len(rawValues))
		for _, rawValue := range rawValues {
			columnValues = append(columnValues, flattenCompositeInt64Values(rawValue)...)
		}
		if len(columnValues) == 0 {
			return nil
		}

		nextGroups := make([][]int64, 0, len(valuesGroups)*len(columnValues))
		for _, valuesGroup := range valuesGroups {
			for _, value := range columnValues {
				nextGroups = append(nextGroups, append(append([]int64{}, valuesGroup...), value))
			}
		}
		valuesGroups = nextGroups
	}

	hashValues := make([]int32, 0, len(valuesGroups))
	for _, valuesGroup := range valuesGroups {
		hashValues = append(hashValues, HashInt64(valuesGroup...))
	}
	return hashValues
}

func registerIndexGroup(dbTable *ScyllaTable, idxCount *int8, indexCfg Index) {
	sourceColumnNames := make([]string, 0, len(indexCfg.Keys))
	for _, key := range indexCfg.Keys {
		sourceColumnNames = append(sourceColumnNames, key.GetName())
	}
	indexID := allocateIndexGroupID(dbTable, sourceColumnNames)

	if indexCfg.Type == TypeInheritFromKey {
		registerInheritFromKeyIndexGroup(dbTable, indexCfg, indexID, sourceColumnNames)
		return
	}

	if len(indexCfg.Keys) == 1 {
		singleKeyIndex := Index{Type: TypeLocalIndex, Keys: indexCfg.Keys}
		registerSchemaLocalIndex(dbTable, idxCount, singleKeyIndex)
		dbTable.indexGroups = append(dbTable.indexGroups, indexGroupInfo{
			name:    indexCfg.Keys[0].GetName(),
			indexID: indexID,
			sourceColumns: []indexGroupSourceColumn{{
				column: dbTable.ColumnsMap[indexCfg.Keys[0].GetName()],
			}},
		})
		if dbTable.indexUpdatedTable == nil {
			dbTable.indexUpdatedTable = &indexUpdatedTableInfo{name: dbTable.Name + "__index_updated"}
		}
		return
	}

	sourceColumns := make([]indexGroupSourceColumn, 0, len(indexCfg.Keys))
	usesCollectionValues := false
	for _, key := range indexCfg.Keys {
		baseColumn := dbTable.ColumnsMap[key.GetName()]
		if baseColumn == nil || baseColumn.IsNil() {
			panic(fmt.Sprintf(`Table "%v": IndexGroup column "%v" was not found`, dbTable.Name, key.GetName()))
		}
		sourceColumns = append(sourceColumns, indexGroupSourceColumn{column: baseColumn})
		if baseColumn.GetType().IsSlice {
			usesCollectionValues = true
		}
	}

	virtualColumnName := makeIndexGroupVirtualColumnName(sourceColumnNames, usesCollectionValues)
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
		hashes := computeIndexGroupHashes(ptr, sourceColumnsLocal)
		if usesCollectionValues {
			return hashes
		}
		if len(hashes) == 0 {
			return int32(0)
		}
		return hashes[0]
	}
	virtualColumn.GetStatementValueFn = func(ptr unsafe.Pointer) any {
		hashes := computeIndexGroupHashes(ptr, sourceColumnsLocal)
		if usesCollectionValues {
			return hashes
		}
		if len(hashes) == 0 {
			return int32(0)
		}
		return hashes[0]
	}
	virtualColumn.GetValueFn = func(ptr unsafe.Pointer) any {
		hashes := computeIndexGroupHashes(ptr, sourceColumnsLocal)
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

	index := &viewInfo{
		Type:               3,
		name:               fmt.Sprintf(`%v__%v_index_0`, dbTable.Name, virtualColumnName),
		idx:                *idxCount,
		column:             virtualColumn,
		columns:            slices.Clone(sourceColumnNames),
		RequiresPostFilter: false,
	}
	index.getCreateScript = func() string {
		if usesCollectionValues {
			return fmt.Sprintf(`CREATE INDEX %v ON %v (VALUES(%v))`, index.name, dbTable.GetFullName(), virtualColumnName)
		}
		return fmt.Sprintf(`CREATE INDEX %v ON %v (%v)`, index.name, dbTable.GetFullName(), virtualColumnName)
	}

	virtualColumnNameLocal := virtualColumnName
	usesCollectionLocal := usesCollectionValues
	index.getStatementPrepared = func(statements ...ColumnStatement) []boundWhereClause {
		// Build all hash input combinations using resolveIndexGroupQueryValues, which handles
		// =, CONTAINS, IN, and BETWEEN (BETWEEN is expanded into one value per step in range).
		valuesGroups := [][]int64{{}}
		for _, sourceColumn := range sourceColumnsLocal {
			st, err := findIndexGroupStatement(sourceColumn.column.GetName(), statements)
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
			hashValue := HashInt64(vg...)
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
	dbTable.indexes[index.name] = index
	dbTable.indexGroups = append(dbTable.indexGroups, indexGroupInfo{
		name:                 strings.Join(sourceColumnNames, "_"),
		indexID:              indexID,
		sourceColumns:        sourceColumns,
		virtualColumn:        virtualColumn,
		usesCollectionValues: usesCollectionValues,
	})

	if dbTable.indexUpdatedTable == nil {
		dbTable.indexUpdatedTable = &indexUpdatedTableInfo{name: dbTable.Name + "__index_updated"}
	}
}

func registerInheritFromKeyIndexGroup(dbTable *ScyllaTable, indexCfg Index, indexID int16, sourceColumnNames []string) {
	if len(dbTable.keyIntPacking) == 0 {
		panic(fmt.Sprintf(`Table "%v": TypeInheritFromKey requires KeyIntPacking on the table`, dbTable.Name))
	}
	if len(indexCfg.Keys) > len(dbTable.keyIntPacking) {
		panic(fmt.Sprintf(`Table "%v": TypeInheritFromKey index has %d keys but KeyIntPacking only declares %d columns`,
			dbTable.Name, len(indexCfg.Keys), len(dbTable.keyIntPacking)))
	}
	for columnIndex, key := range indexCfg.Keys {
		packedColumnName := dbTable.keyIntPacking[columnIndex].GetName()
		if key.GetName() != packedColumnName {
			panic(fmt.Sprintf(`Table "%v": TypeInheritFromKey keys must be a prefix of KeyIntPacking. Position %d expected "%v" but got "%v"`,
				dbTable.Name, columnIndex, packedColumnName, key.GetName()))
		}
		baseColumn := dbTable.ColumnsMap[key.GetName()]
		if baseColumn == nil || baseColumn.IsNil() {
			panic(fmt.Sprintf(`Table "%v": TypeInheritFromKey column "%v" was not found`, dbTable.Name, key.GetName()))
		}
		if baseColumn.GetType().IsSlice {
			panic(fmt.Sprintf(`Table "%v": TypeInheritFromKey does not support slice column "%v"`, dbTable.Name, key.GetName()))
		}
	}

	sourceColumns := make([]indexGroupSourceColumn, 0, len(indexCfg.Keys))
	for _, key := range indexCfg.Keys {
		sourceColumns = append(sourceColumns, indexGroupSourceColumn{
			column: dbTable.ColumnsMap[key.GetName()],
		})
	}

	dbTable.indexGroups = append(dbTable.indexGroups, indexGroupInfo{
		name:           strings.Join(sourceColumnNames, "_"),
		indexID:        indexID,
		sourceColumns:  sourceColumns,
		inheritFromKey: true,
	})

	if dbTable.indexUpdatedTable == nil {
		dbTable.indexUpdatedTable = &indexUpdatedTableInfo{name: dbTable.Name + "__index_updated"}
	}
}

func syncIndexGroupsAfterWrite[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T, scyllaTable *ScyllaTable, managedValues managedWriteValues,
) error {
	if len(*records) == 0 || len(scyllaTable.indexGroups) == 0 || scyllaTable.indexUpdatedTable == nil {
		return nil
	}
	if len(managedValues.updatedVersionValues) == 0 {
		return nil
	}

	rowsByPartitionAndHash := map[string]indexUpdatedRow{}
	rowsToPersist := []indexUpdatedRow{}
	for recordIndex := range *records {
		recordPointer := xunsafe.AsPointer(&(*records)[recordIndex])
		partitionValue := int32(scyllaTable.GetPartValue(recordPointer))
		updateCounterValue := convertToInt64(managedValues.updatedVersionValues[recordIndex])
		appendIndexUpdatedRowsForRecord(recordPointer, scyllaTable, partitionValue, updateCounterValue, rowsByPartitionAndHash, &rowsToPersist)
	}

	if len(rowsToPersist) == 0 {
		return nil
	}

	// Persisted synchronously. These counters are the freshness marker the frontend delta cache
	// reads (cc-gh / cc-upc), so losing one silently strands every client on stale data. A
	// fire-and-forget goroutine cannot guarantee delivery: AWS Lambda freezes the execution
	// environment the moment the handler returns, suspending the write until the environment is
	// reused — or discarding it outright when the environment is recycled. The caller recovers the
	// lost concurrency by running this alongside the other post-write syncs.
	return persistIndexUpdatedRows(scyllaTable.Namespace, scyllaTable.indexUpdatedTable.name, rowsToPersist)
}

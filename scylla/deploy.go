package scylla

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/gocql/gocql"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/scylla/text_search"
)

type ScyllaController[T db.RecordWithExecutor[E, T, Exec[E, T]], E TableSchemaInterface[E]] struct {
	TableName string
	Table     ScyllaTable
	Schema    TableSchema
}

// record is a reflect.Value rather than a T so the recalc body can stay non-generic. Keeping the
// Value (not just the pointer) is what holds the allocation alive for the GC.
type virtualColumnsRecalcUpdate struct {
	record                reflect.Value
	changedVirtualColumns []IColInfo
}

func (e *ScyllaController[T, E]) GetTable() db.Table {
	return e.Table
}

func (e *ScyllaController[T, E]) GetTableName() string {
	return e.Table.Name
}

func (e *ScyllaController[T, E]) GetRecords(partValue, limit int32, lastKey any) []any {

	records := []T{}
	query := any(Query(&records)).(TableQueryInterface[E])

	pk := e.Table.GetPartKey()
	if partValue > 0 && pk != nil && !pk.IsNil() {
		query.SetWhere(pk.GetName(), "=", partValue)
	}

	// Add lastKey filter if provided (for pagination)
	if lastKey != nil && len(e.Table.Keys) > 0 {
		query.SetWhere(e.Table.Keys[0].GetName(), ">=", lastKey)
	}

	// Execute the query
	fmt.Println("Obteniendo registros de::", e.Table.Name)
	if err := query.Exec(); err != nil {
		fmt.Println("Error al consultar", e.Table.Name, ":", err)
		return nil
	}
	fmt.Println("reqgistros obtenidos (1)::", len(records))

	recordsAny := []any{}
	for _, e := range records {
		recordsAny = append(recordsAny, any(e))
	}

	return recordsAny
}

func (e *ScyllaController[T, E]) GetRecordsCSV(partValue int32) (db.CSVResult, error) {
	scyllaTable := &e.Table
	return exportToCSV(scyllaTable, partValue)
}

func (e *ScyllaController[T, E]) ReloadRecords(partValue int32) error {
	records := []T{}
	query := any(Query(&records)).(TableQueryInterface[E])

	pk := e.Table.GetPartKey()
	if partValue > 0 && pk != nil && !pk.IsNil() {
		query.SetWhere(pk.GetName(), "=", partValue)
	}

	if err := query.Exec(); err != nil {
		fmt.Println("Error al consultar", e.Table.Name, ":", err)
		return err
	}

	if len(records) > 0 {
		if err := Insert(&records); err != nil {
			return Err("Error al re-insertar registros:", err)
		}
	}

	return nil
}

// RecalcVirtualColumns is a shim over a non-generic body; T survives only as a reflect.Type used
// to allocate one record per scanned row. See BINARY_SIZE_PLAN.md §6.
func (e *ScyllaController[T, E]) RecalcVirtualColumns(partValue int32) error {
	return recalcVirtualColumnsForTable(
		getOrCompileScyllaTable(db.InitStructTable[E, T](new(E))), reflect.TypeFor[T](), partValue)
}

func recalcVirtualColumnsForTable(scyllaTable ScyllaTable, recordType reflect.Type, partValue int32) error {
	virtualColumns := []IColInfo{}
	selectedColumns := make([]IColInfo, 0, len(scyllaTable.Columns))
	for _, column := range scyllaTable.Columns {
		if column == nil || column.IsNil() {
			continue
		}
		if !column.GetInfo().IsVirtual && column.GetInfo().Field == nil {
			// Managed write-only columns are not readable from schemas that don't expose them.
			continue
		}
		selectedColumns = append(selectedColumns, column)
		// Recalc only persists generated ORM columns; physical source fields stay untouched.
		if column.GetInfo().IsVirtual {
			virtualColumns = append(virtualColumns, column)
		}
	}
	if len(virtualColumns) == 0 {
		return nil
	}

	// Keep query projection order stable so scanned values map back to ORM columns by position.
	slices.SortFunc(selectedColumns, func(leftColumn, rightColumn IColInfo) int {
		return int(leftColumn.GetInfo().Idx - rightColumn.GetInfo().Idx)
	})

	selectColumnNames := make([]string, 0, len(selectedColumns))
	for _, selectedColumn := range selectedColumns {
		selectColumnNames = append(selectColumnNames, selectedColumn.GetName())
	}

	queryStr := fmt.Sprintf("SELECT %v FROM %v", strings.Join(selectColumnNames, ", "), scyllaTable.GetFullName())
	partitionColumn := scyllaTable.GetPartKey()
	if partValue > 0 && partitionColumn != nil && !partitionColumn.IsNil() {
		queryStr = fmt.Sprintf("%v WHERE %v = %v", queryStr, partitionColumn.GetName(), partValue)
	}

	fmt.Printf("RecalcVirtualColumns | table=%s | query=%s\n", scyllaTable.Name, queryStr)
	queryIterator := getScyllaConnection().Query(queryStr).Iter()
	rowData, err := queryIterator.RowData()
	if err != nil {
		return Err("RecalcVirtualColumns RowData failed for table", scyllaTable.Name, ":", err)
	}

	updatesToApply := []virtualColumnsRecalcUpdate{}
	rowsScanned := 0
	updatedRowsByVirtualColumn := map[string]int{}

	scanner := queryIterator.Scanner()
	for scanner.Next() {
		if err := scanner.Scan(rowData.Values...); err != nil {
			return Err("RecalcVirtualColumns scan failed for table", scyllaTable.Name, ":", err)
		}
		rowsScanned++

		record := reflect.New(recordType).Elem()
		recordPointer := record.Addr().UnsafePointer()
		persistedVirtualValueByName := map[string]string{}

		for valueIndex, selectedColumn := range selectedColumns {
			rawValue := dereferenceScyllaValue(rowData.Values[valueIndex])
			if selectedColumn.GetInfo().IsVirtual {
				persistedVirtualValueByName[selectedColumn.GetName()] = makeVirtualValueSignature(rawValue)
				continue
			}
			// Rebuild the source record from raw DB values so virtual accessors recompute from persisted inputs.
			selectedColumn.SetValue(recordPointer, rawValue)
		}

		changedVirtualColumns := []IColInfo{}
		for _, virtualColumn := range virtualColumns {
			persistedValueSignature := persistedVirtualValueByName[virtualColumn.GetName()]
			recalculatedValueSignature := makeVirtualValueSignature(virtualColumn.GetStatementValue(recordPointer))
			if persistedValueSignature != recalculatedValueSignature {
				changedVirtualColumns = append(changedVirtualColumns, virtualColumn)
				updatedRowsByVirtualColumn[virtualColumn.GetName()]++
			}
		}
		if len(changedVirtualColumns) == 0 {
			continue
		}

		updatesToApply = append(updatesToApply, virtualColumnsRecalcUpdate{
			record:                record,
			changedVirtualColumns: changedVirtualColumns,
		})
	}

	if err := queryIterator.Close(); err != nil {
		return Err("RecalcVirtualColumns query close failed for table", scyllaTable.Name, ":", err)
	}

	if len(updatesToApply) == 0 {
		fmt.Printf("RecalcVirtualColumns | table=%s | rows_scanned=%d | rows_updated=0\n", scyllaTable.Name, rowsScanned)
		return nil
	}

	whereColumns := scyllaTable.Keys
	if partitionColumn != nil && !partitionColumn.IsNil() {
		whereColumns = append([]IColInfo{partitionColumn}, whereColumns...)
	}
	whereParts := make([]string, 0, len(whereColumns))
	for _, whereColumn := range whereColumns {
		whereParts = append(whereParts, fmt.Sprintf("%v = ?", whereColumn.GetName()))
	}
	whereClause := strings.Join(whereParts, " and ")

	const maxRecalcUpdatesPerBatch = 200
	totalChunks := (len(updatesToApply) + maxRecalcUpdatesPerBatch - 1) / maxRecalcUpdatesPerBatch
	session := getScyllaConnection()
	for chunkIndex := 0; chunkIndex < totalChunks; chunkIndex++ {
		fromIndex := chunkIndex * maxRecalcUpdatesPerBatch
		toIndex := fromIndex + maxRecalcUpdatesPerBatch
		if toIndex > len(updatesToApply) {
			toIndex = len(updatesToApply)
		}
		chunk := updatesToApply[fromIndex:toIndex]

		batch := session.NewBatch(gocql.UnloggedBatch)
		for chunkRowIndex := range chunk {
			updateToApply := &chunk[chunkRowIndex]
			recordPointer := updateToApply.record.Addr().UnsafePointer()

			setParts := make([]string, 0, len(updateToApply.changedVirtualColumns))
			boundValues := make([]any, 0, len(updateToApply.changedVirtualColumns)+len(whereColumns))
			for _, column := range updateToApply.changedVirtualColumns {
				setParts = append(setParts, fmt.Sprintf("%v = ?", column.GetName()))
				boundValues = append(boundValues, getNormalizedWriteValue(column, recordPointer))
			}
			for _, whereColumn := range whereColumns {
				boundValues = append(boundValues, whereColumn.GetStatementValue(recordPointer))
			}

			stmt := fmt.Sprintf("UPDATE %v SET %v WHERE %v",
				scyllaTable.GetFullName(), strings.Join(setParts, ", "), whereClause,
			)
			batch.Query(stmt, boundValues...)
		}

		fmt.Printf("RecalcVirtualColumns | table=%s | chunk=%d/%d | rows_in_chunk=%d\n",
			scyllaTable.Name, chunkIndex+1, totalChunks, len(chunk))

		if err := session.ExecuteBatch(batch); err != nil {
			return Err("RecalcVirtualColumns update failed for table", scyllaTable.Name, "chunk", chunkIndex+1, "of", totalChunks, ":", err)
		}
	}

	virtualColumnNames := make([]string, 0, len(updatedRowsByVirtualColumn))
	for virtualColumnName := range updatedRowsByVirtualColumn {
		virtualColumnNames = append(virtualColumnNames, virtualColumnName)
	}
	slices.Sort(virtualColumnNames)
	for _, virtualColumnName := range virtualColumnNames {
		fmt.Printf("RecalcVirtualColumns | table=%s | virtual_column=%s | rows_saved=%d\n",
			scyllaTable.Name, virtualColumnName, updatedRowsByVirtualColumn[virtualColumnName])
	}

	fmt.Printf("RecalcVirtualColumns | table=%s | rows_scanned=%d | rows_updated=%d\n",
		scyllaTable.Name, rowsScanned, len(updatesToApply))
	return nil
}

// RecalcGroupIndexHashes is a shim over a non-generic body, for the same reason as
// RecalcVirtualColumns.
func (e *ScyllaController[T, E]) RecalcGroupIndexHashes(partValue int32) error {
	return recalcGroupIndexHashesForTable(
		getOrCompileScyllaTable(db.InitStructTable[E, T](new(E))), reflect.TypeFor[T](), partValue)
}

func recalcGroupIndexHashesForTable(scyllaTable ScyllaTable, recordType reflect.Type, partValue int32) error {
	if scyllaTable.indexUpdatedTable == nil || len(scyllaTable.indexGroups) == 0 {
		return nil
	}

	partitionColumn := scyllaTable.GetPartKey()
	if partitionColumn == nil || partitionColumn.IsNil() {
		return Err("RecalcGroupIndexHashes requires a partition column for table:", scyllaTable.Name)
	}
	if partValue <= 0 {
		return Err("RecalcGroupIndexHashes requires a partition value > 0 for table:", scyllaTable.Name)
	}

	selectedColumns := []IColInfo{}
	selectedColumnNamesSeen := map[string]bool{}
	appendSelectedColumn := func(column IColInfo) {
		if column == nil || column.IsNil() {
			return
		}
		if selectedColumnNamesSeen[column.GetName()] {
			return
		}
		selectedColumnNamesSeen[column.GetName()] = true
		selectedColumns = append(selectedColumns, column)
	}

	appendSelectedColumn(partitionColumn)
	if scyllaTable.UpdatedVersionCol != nil {
		appendSelectedColumn(scyllaTable.UpdatedVersionCol)
	}
	for _, indexGroup := range scyllaTable.indexGroups {
		for _, sourceColumn := range indexGroup.sourceColumns {
			appendSelectedColumn(sourceColumn.column)
		}
	}
	if len(selectedColumns) == 0 {
		return nil
	}

	slices.SortFunc(selectedColumns, func(leftColumn, rightColumn IColInfo) int {
		return int(leftColumn.GetInfo().Idx - rightColumn.GetInfo().Idx)
	})

	selectColumnNames := make([]string, 0, len(selectedColumns))
	for _, selectedColumn := range selectedColumns {
		selectColumnNames = append(selectColumnNames, selectedColumn.GetName())
	}

	deleteStatement := fmt.Sprintf("DELETE FROM %v.%v WHERE partition_id = ?",
		scyllaTable.Namespace,
		scyllaTable.indexUpdatedTable.name,
	)
	if err := QueryExec(deleteStatement, partValue); err != nil {
		return Err("RecalcGroupIndexHashes delete failed for table", scyllaTable.Name, ":", err)
	}

	queryStr := fmt.Sprintf(
		"SELECT %v FROM %v WHERE %v = %v",
		strings.Join(selectColumnNames, ", "),
		scyllaTable.GetFullName(),
		partitionColumn.GetName(),
		partValue,
	)
	fmt.Printf("RecalcGroupIndexHashes | table=%s | query=%s\n", scyllaTable.Name, queryStr)

	queryIterator := getScyllaConnection().Query(queryStr).Iter()
	rowData, err := queryIterator.RowData()
	if err != nil {
		return Err("RecalcGroupIndexHashes RowData failed for table", scyllaTable.Name, ":", err)
	}

	rowsScanned := 0
	rowsToPersist := []indexUpdatedRow{}
	rowsByPartitionAndHash := map[string]indexUpdatedRow{}
	scanner := queryIterator.Scanner()
	for scanner.Next() {
		if err := scanner.Scan(rowData.Values...); err != nil {
			return Err("RecalcGroupIndexHashes scan failed for table", scyllaTable.Name, ":", err)
		}
		rowsScanned++

		record := reflect.New(recordType).Elem()
		recordPointer := record.Addr().UnsafePointer()
		updateCounterValue := int64(0)

		for valueIndex, selectedColumn := range selectedColumns {
			rawValue := dereferenceScyllaValue(rowData.Values[valueIndex])
			if selectedColumn.GetName() == managedUpdatedVersionColumnName {
				updateCounterValue = convertToInt64(rawValue)
				continue
			}
			selectedColumn.SetValue(recordPointer, rawValue)
		}
		if updateCounterValue == 0 {
			continue
		}

		appendIndexUpdatedRowsForRecord(recordPointer, &scyllaTable, int32(partValue), updateCounterValue, rowsByPartitionAndHash, &rowsToPersist)
	}

	if err := queryIterator.Close(); err != nil {
		return Err("RecalcGroupIndexHashes query close failed for table", scyllaTable.Name, ":", err)
	}

	if len(rowsToPersist) == 0 {
		fmt.Printf("RecalcGroupIndexHashes | table=%s | partition=%d | rows_scanned=%d | rows_persisted=0\n",
			scyllaTable.Name, partValue, rowsScanned)
		return nil
	}

	if err := persistIndexUpdatedRows(scyllaTable.Namespace, scyllaTable.indexUpdatedTable.name, rowsToPersist); err != nil {
		return Err("RecalcGroupIndexHashes persist failed for table", scyllaTable.Name, ":", err)
	}

	fmt.Printf("RecalcGroupIndexHashes | table=%s | partition=%d | rows_scanned=%d | rows_persisted=%d\n",
		scyllaTable.Name, partValue, rowsScanned, len(rowsToPersist))
	return nil
}

func dereferenceScyllaValue(value any) any {
	if value == nil {
		return nil
	}

	valueRef := reflect.ValueOf(value)
	for valueRef.Kind() == reflect.Pointer {
		if valueRef.IsNil() {
			return nil
		}
		valueRef = valueRef.Elem()
	}

	return valueRef.Interface()
}

func makeVirtualValueSignature(value any) string {
	if value == nil {
		return "<nil>"
	}

	value = dereferenceScyllaValue(value)
	if value == nil {
		return "<nil>"
	}

	valueRef := reflect.ValueOf(value)
	switch valueRef.Kind() {
	case reflect.Slice, reflect.Array:
		parts := make([]string, 0, valueRef.Len())
		for valueIndex := 0; valueIndex < valueRef.Len(); valueIndex++ {
			parts = append(parts, makeVirtualValueSignature(valueRef.Index(valueIndex).Interface()))
		}
		return "[" + strings.Join(parts, ",") + "]"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprintf("%d", valueRef.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fmt.Sprintf("%d", valueRef.Uint())
	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%g", valueRef.Float())
	case reflect.Bool:
		return fmt.Sprintf("%t", valueRef.Bool())
	case reflect.String:
		return valueRef.String()
	default:
		if byteSlice, ok := value.([]byte); ok {
			return fmt.Sprintf("%v", byteSlice)
		}
		return fmt.Sprintf("%v", value)
	}
}

// ResetCounter is a shim: the body below is non-generic so it exists once instead of once per
// table type. See BINARY_SIZE_PLAN.md §6.
func (e *ScyllaController[T, E]) ResetCounter(partValue any) error {
	return resetCounterForTable(&e.Table, partValue)
}

func resetCounterForTable(controllerTable *ScyllaTable, partValue any) error {
	scyllaTable := &(*controllerTable)

	// Sequence reset only applies to partitioned tables with explicit autoincrement usage.
	partitionColumn := scyllaTable.GetPartKey()
	if partitionColumn == nil || partitionColumn.IsNil() {
		return nil
	}
	if !scyllaTable.UseSequences || scyllaTable.AutoincrementCol == nil {
		return nil
	}
	if len(scyllaTable.Keys) == 0 {
		return Err("ResetCounter requires at least one key column for table:", scyllaTable.Name)
	}
	if partValue == nil {
		return Err("ResetCounter requires a non-nil partition value for table:", scyllaTable.Name)
	}

	// Read max persisted key in the target partition to align sequence with current data.
	keyColumn := scyllaTable.Keys[0]
	// Use column metadata to validate the key once, then reuse the shared numeric converter.
	switch keyColumn.GetType().Type {
	case 2, 3, 4, 5:
	default:
		return Err("ResetCounter only supports numeric key types. table:", scyllaTable.Name, "key:", keyColumn.GetName())
	}

	maxValueQuery := fmt.Sprintf(
		"SELECT max(%v) FROM %v WHERE %v = ?",
		keyColumn.GetName(), scyllaTable.GetFullName(), partitionColumn.GetName(),
	)

	// Let gocql allocate the aggregate destination types, then normalize the first value.
	queryIterator := getScyllaConnection().Query(maxValueQuery, partValue).Iter()
	rowData, err := queryIterator.RowData()
	if err != nil {
		return Err("ResetCounter max-value query failed for table", scyllaTable.Name, ":", err)
	}

	maxKeyValue := int64(0)
	rowScanner := queryIterator.Scanner()
	if rowScanner.Next() {
		if err := rowScanner.Scan(rowData.Values...); err != nil {
			return Err("ResetCounter max-value query failed for table", scyllaTable.Name, ":", err)
		}
		if len(rowData.Values) > 0 && rowData.Values[0] != nil {
			maxKeyValue = db.ToInt64(rowData.Values[0])
		}
	}
	if err := queryIterator.Close(); err != nil {
		return Err("ResetCounter max-value query failed for table", scyllaTable.Name, ":", err)
	}

	// Counter naming must match the insert path (x{partition}_{table}_{autoincrementPart}).
	counterName := fmt.Sprintf("x%v_%v_%v", partValue, scyllaTable.Name, 0)
	currentCounterValue, err := getSequenceCurrentValue(counterName)
	if err != nil {
		return Err("ResetCounter sequence read failed for", counterName, ":", err)
	}

	// Counters are increment-only, so we apply the delta to move to the target absolute value.
	delta := maxKeyValue - currentCounterValue
	if delta == 0 {
		return nil
	}

	updateStatement := fmt.Sprintf("UPDATE %v.sequences SET current_value = current_value + ? WHERE name = ?", scyllaTable.Namespace)
	if err := getScyllaConnection().Query(updateStatement, delta, counterName).Exec(); err != nil {
		return Err("ResetCounter sequence update failed for", counterName, ":", err)
	}

	fmt.Printf("ResetCounter | table=%s | partition=%v | counter=%s | previous=%d | maxKey=%d | delta=%d\n",
		scyllaTable.Name, partValue, counterName, currentCounterValue, maxKeyValue, delta)

	return nil
}

// FlushTextSearchIndex erases this table's GenixSearch buckets for one
// partition — both status groups (s0 inactive, s1 active) — via FLUSHB.
// No-op for tables that don't declare a TextSearchColumn. Buckets are
// re-created lazily on the next indexed write, so this is the clean-slate
// step before reindexing a single partition.
func (e *ScyllaController[T, E]) FlushTextSearchIndex(partValue int32) error {
	scyllaTable := &e.Table
	if scyllaTable.textSearchIndex == nil {
		return nil
	}
	ctx := context.Background()
	for _, statusGroup := range []int8{0, 1} {
		if err := text_search.FlushBucket(ctx, scyllaTable.Name, partValue, statusGroup); err != nil {
			return Err("FlushTextSearchIndex failed for table", scyllaTable.Name, "partition", partValue, "group", statusGroup, ":", err)
		}
	}
	fmt.Printf("FlushTextSearchIndex | table=%s | partition=%d | groups=[0 1] flushed\n", scyllaTable.Name, partValue)
	return nil
}

// DeleteViewsAndIndexes is a shim over a non-generic body, for the same reason as ResetCounter.
func (e *ScyllaController[T, E]) DeleteViewsAndIndexes() error {
	return deleteViewsAndIndexesForTable(&e.Table)
}

func deleteViewsAndIndexesForTable(controllerTable *ScyllaTable) error {
	scyllaTable := &(*controllerTable)

	// Read the live catalog first so old DB artifacts are deleted even if the current schema no longer declares them.
	session := getScyllaConnection()

	liveMaterializedViewNames := []string{}
	viewNamesSeen := map[string]bool{}
	viewsQuery := fmt.Sprintf(
		"SELECT keyspace_name, view_name, base_table_name FROM system_schema.views WHERE keyspace_name = '%s'",
		scyllaTable.Namespace,
	)
	viewIterator := session.Query(viewsQuery).Iter()
	var liveView ScyllaView
	for viewIterator.Scan(&liveView.Keyspace, &liveView.Name, &liveView.BaseTable) {
		if liveView.BaseTable != scyllaTable.Name || viewNamesSeen[liveView.Name] {
			continue
		}
		viewNamesSeen[liveView.Name] = true
		liveMaterializedViewNames = append(liveMaterializedViewNames, liveView.Name)
	}
	if err := viewIterator.Close(); err != nil {
		return Err("DeleteViewsAndIndexes failed reading views catalog for table", scyllaTable.Name, ":", err)
	}

	indexQuery := fmt.Sprintf(
		"SELECT keyspace_name, table_name, index_name, kind FROM system_schema.indexes WHERE keyspace_name = '%s'",
		scyllaTable.Namespace,
	)
	indexIterator := session.Query(indexQuery).Iter()
	liveIndexesByTable := map[string][]string{}
	indexNamesSeenByTable := map[string]map[string]bool{}
	var liveIndex ScyllaIndexes
	for indexIterator.Scan(&liveIndex.Keyspace, &liveIndex.Table, &liveIndex.Name, &liveIndex.Kind) {
		if _, exists := indexNamesSeenByTable[liveIndex.Table]; !exists {
			indexNamesSeenByTable[liveIndex.Table] = map[string]bool{}
		}
		if indexNamesSeenByTable[liveIndex.Table][liveIndex.Name] {
			continue
		}
		indexNamesSeenByTable[liveIndex.Table][liveIndex.Name] = true
		liveIndexesByTable[liveIndex.Table] = append(liveIndexesByTable[liveIndex.Table], liveIndex.Name)
	}
	if err := indexIterator.Close(); err != nil {
		return Err("DeleteViewsAndIndexes failed reading indexes catalog for table", scyllaTable.Name, ":", err)
	}

	// Drop live base-table indexes directly from the catalog instead of trusting only the current ORM metadata.
	for _, indexName := range liveIndexesByTable[scyllaTable.Name] {
		dropIndexStatement := fmt.Sprintf("DROP INDEX IF EXISTS %v.%v", scyllaTable.Namespace, indexName)
		fmt.Println("DeleteViewsAndIndexes |", dropIndexStatement)
		if err := QueryExec(dropIndexStatement); err != nil {
			return Err("DeleteViewsAndIndexes failed dropping live index", indexName, "for table", scyllaTable.Name, ":", err)
		}
	}

	// Drop all live MVs that still depend on the base table so DROP TABLE is no longer blocked by stale dependencies.
	for _, viewName := range liveMaterializedViewNames {
		dropViewStatement := fmt.Sprintf("DROP MATERIALIZED VIEW IF EXISTS %v.%v", scyllaTable.Namespace, viewName)
		fmt.Println("DeleteViewsAndIndexes |", dropViewStatement)
		if err := QueryExec(dropViewStatement); err != nil {
			return Err("DeleteViewsAndIndexes failed dropping live materialized view", viewName, "for table", scyllaTable.Name, ":", err)
		}
	}

	// View-table cleanup still uses ORM metadata to know which derived tables belong to this base table.
	for _, view := range scyllaTable.views {
		if view.Type != TypeViewTable {
			continue
		}

		for _, indexName := range liveIndexesByTable[view.name] {
			dropIndexStatement := fmt.Sprintf("DROP INDEX IF EXISTS %v.%v", scyllaTable.Namespace, indexName)
			fmt.Println("DeleteViewsAndIndexes |", dropIndexStatement)
			if err := QueryExec(dropIndexStatement); err != nil {
				return Err("DeleteViewsAndIndexes failed dropping live index", indexName, "for view table", view.name, ":", err)
			}
		}

		dropViewTableStatement := fmt.Sprintf("DROP TABLE IF EXISTS %v.%v", scyllaTable.Namespace, view.name)
		fmt.Println("DeleteViewsAndIndexes |", dropViewTableStatement)
		if err := QueryExec(dropViewTableStatement); err != nil {
			return Err("DeleteViewsAndIndexes failed dropping view table", view.name, "for table", scyllaTable.Name, ":", err)
		}
	}

	return nil
}

func getSequenceCurrentValue(counterName string) (int64, error) {
	result := []Increment{}
	if err := Query(&result).Name.Equals(counterName).Exec(); err != nil {
		return 0, err
	}
	if len(result) == 0 {
		return 0, nil
	}
	return result[0].CurrentValue, nil
}

func (e *ScyllaController[T, E]) GetRecordsGob(partValue, limit int32, lastKey any) ([]byte, error) {

	gob.Register(*new(T))
	records := e.GetRecords(partValue, limit, lastKey)
	if len(records) == 0 {
		return nil, nil
	}

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(records)
	if err != nil {
		return []byte{}, err
	}

	return buffer.Bytes(), nil
}

func (e *ScyllaController[T, E]) RestoreCSVRecords(partValue int32, content *[]byte) error {
	scyllaTable := &e.Table
	records, err := CsvToRecords[T](scyllaTable, content, partValue)

	if err != nil {
		return err
	}

	pk := e.Table.GetPartKey()
	if partValue > 0 && pk != nil && !pk.IsNil() {
		statement := fmt.Sprintf(`DELETE FROM %v WHERE %v = %v`, e.Table.GetFullName(), pk.GetName(), partValue)
		if err := QueryExec(statement); err != nil {
			fmt.Println("Error en statement: ", statement)
			return Err("Error al eliminar registros:", err)
		}
	}

	// Insert new records
	fmt.Println("Registros a insertar:", len(records))

	if len(records) > 0 {
		Print(records[0])
	}

	if err := Insert(&records); err != nil {
		return Err("Error al insertar registros:", err)
	}
	return nil
}

type ScyllaColumns struct {
	Name     string
	Type     string
	Keyspace string
	Table    string
}

type ScyllaIndexes struct {
	Name     string
	Table    string
	Keyspace string
	Kind     string
}

type ScyllaView struct {
	Keyspace  string
	Name      string
	BaseTable string
}

var cacheCodePrev int32
var scyllaColumnsSaved []ScyllaColumns
var scyllaIndexesSaved []ScyllaIndexes

// getViewMissingColumns diffs a compiled view's declared columns against the live DB catalog and
// returns the ones the DB is missing. A type mismatch on an existing column is only reported: the
// same as the base table does, since changing a column type is not something deploy can decide.
func getViewMissingColumns(view *viewInfo, liveViewColumns []ScyllaColumns) []viewExpectedColumn {
	if view.getExpectedColumns == nil {
		return nil
	}

	liveColumnTypes := map[string]string{}
	for _, liveColumn := range liveViewColumns {
		liveColumnTypes[liveColumn.Name] = liveColumn.Type
	}

	missingColumns := []viewExpectedColumn{}
	for _, expectedColumn := range view.getExpectedColumns() {
		liveType, exists := liveColumnTypes[expectedColumn.name]
		if !exists {
			missingColumns = append(missingColumns, expectedColumn)
			continue
		}
		if liveType != expectedColumn.dbType {
			Logx(5, fmt.Sprintf(`La columna "%v" de la view "%v" está en la BD como "%v" pero el Struct la declara como "%v".`+"\n",
				expectedColumn.name, view.name, liveType, expectedColumn.dbType))
		}
	}
	return missingColumns
}

// repairViewMissingColumns realigns a live view with the columns its schema declares. Adding a
// column to a base table never reaches that table's derived views on its own — a materialized view
// only carries the columns named in its CREATE, and a view table is a separate physical table — so
// without this every read or write touching the new column fails on the view while the base table
// looks correct.
//
// The repair differs by view kind:
//   - A materialized view cannot be ALTERed, so it is dropped and recreated. Scylla repopulates it
//     from the base table, so nothing is lost, but the rebuild is not instant on a large table.
//   - A view table holds rows only the ORM's write path produces and nothing rebuilds them in bulk,
//     so it is ALTERed instead. Dropping it would empty the view until every base row is rewritten.
func repairViewMissingColumns(table *ScyllaTable, view *viewInfo, liveViewColumns []ScyllaColumns) {
	missingColumns := getViewMissingColumns(view, liveViewColumns)
	if len(missingColumns) == 0 {
		return
	}

	viewFullName := table.Namespace + "." + view.name
	missingColumnNames := []string{}
	for _, missingColumn := range missingColumns {
		missingColumnNames = append(missingColumnNames, missingColumn.name)
	}
	Logx(5, fmt.Sprintf(`A la view "%v" le faltan las columnas: %v. Reparando...`+"\n",
		view.name, strings.Join(missingColumnNames, ", ")))

	if view.Type == TypeViewTable {
		for _, missingColumn := range missingColumns {
			alterStatement := fmt.Sprintf(`ALTER TABLE %v ADD %v %v`, viewFullName, missingColumn.name, missingColumn.dbType)
			fmt.Println(alterStatement)
			if err := QueryExec(alterStatement); err != nil {
				panic(fmt.Sprintf(`Error agregando la columna "%v" a la view table "%v" | %v`, missingColumn.name, view.name, err))
			}
		}
		Logx(2, fmt.Sprintf(`View table columns added "%v": %v`+"\n", view.name, strings.Join(missingColumnNames, ", ")))
		return
	}

	// Build the CREATE before dropping so only the DB can fail between the two statements. If it
	// does, the view is simply absent and the next run recreates it through the not-found branch.
	createScript := view.getCreateScript()

	dropStatement := fmt.Sprintf(`DROP MATERIALIZED VIEW IF EXISTS %v`, viewFullName)
	fmt.Println(dropStatement)
	if err := QueryExec(dropStatement); err != nil {
		panic(fmt.Sprintf(`Error eliminando la view "%v" para recrearla | %v`, view.name, err))
	}

	fmt.Println(createScript)
	if err := QueryExec(createScript); err != nil {
		panic(fmt.Sprintf(`Error recreando la view "%v" en %v | %v`, view.name, table.GetFullName(), err))
	}
	Logx(2, fmt.Sprintf(`View rebuilt with the missing columns "%v"`+"\n", view.name))
}

func DeployScylla(cacheCode int32, controllers ...db.Controller) {
	// The ORM's own tables are raw CQL, so the homologation pass below can never discover them —
	// it only sees the controllers it was handed. Creating them here is what makes a standalone
	// deploy ("fn-homologate") produce a usable keyspace, not just one with the app tables in it.
	if err := EnsureInternalTables(); err != nil {
		panic("Error al crear las tablas internas del ORM: " + err.Error())
	}

	var scyllaColumns []ScyllaColumns
	var scyllaIndexes []ScyllaIndexes
	isFetched := false

	if cacheCode > 0 && cacheCode == cacheCodePrev {
		scyllaColumns = scyllaColumnsSaved
		scyllaIndexes = scyllaIndexesSaved
	} else {
		fmt.Println("Obteniendo columnas...")
		// Query system_schema.columns
		query := fmt.Sprintf("SELECT keyspace_name, table_name, column_name, type FROM system_schema.columns WHERE keyspace_name = '%s'", connParams.Keyspace)

		session := getScyllaConnection()
		iter := session.Query(query).Iter()
		var col ScyllaColumns
		for iter.Scan(&col.Keyspace, &col.Table, &col.Name, &col.Type) {
			scyllaColumns = append(scyllaColumns, col)
		}
		if err := iter.Close(); err != nil {
			panic("Error al obtener columnas:" + err.Error())
		}
		fmt.Println("Scylla columns obtenidas::", len(scyllaColumns))

		fmt.Println("Obteniendo Indices...")
		// Query system_schema.indexes
		indexQuery := fmt.Sprintf("SELECT keyspace_name, table_name, index_name, kind FROM system_schema.indexes WHERE keyspace_name = '%s'", connParams.Keyspace)

		iter = session.Query(indexQuery).Iter()
		var idx ScyllaIndexes
		for iter.Scan(&idx.Keyspace, &idx.Table, &idx.Name, &idx.Kind) {
			scyllaIndexes = append(scyllaIndexes, idx)
		}
		if err := iter.Close(); err != nil {
			panic("Error al obtener índices:" + err.Error())
		}
		fmt.Println("Índices obtenidos:", len(scyllaIndexes))
		isFetched = true
	}

	tableColumnsMap := map[string][]ScyllaColumns{}

	for _, e := range scyllaColumns {
		key := fmt.Sprintf("%v.%v", e.Keyspace, e.Table)
		tableColumnsMap[key] = append(tableColumnsMap[key], e)
	}

	if isFetched {
		tablesNames := []string{}

		for tableName, columns := range tableColumnsMap {
			tablesNames = append(tablesNames, tableName)

			s1 := strings.Split(tableName, "_")
			if s1[len(s1)-1] == "view" {
				continue
			}
			fmt.Println("✔ Table =", tableName)
			columnsNames := []string{}
			for _, c := range columns {
				columnsNames = append(columnsNames, fmt.Sprintf("%v(%v)", c.Name, c.Type))
			}
			fmt.Println("  Columns =", strings.Join(columnsNames, ", "))
		}

		fmt.Println("Tables::", tablesNames)
	}

	tableIndexesMap := map[string][]string{}
	for _, e := range scyllaIndexes {
		key := fmt.Sprintf("%v.%v", e.Keyspace, e.Table)
		tableIndexesMap[key] = append(tableIndexesMap[key], e.Name)
	}

	for _, controller := range controllers {
		// Deploy emits CQL, so it needs this driver's full metadata (views, packed
		// indexes, the index-updated table). A table compiled by another driver has
		// nothing to deploy here.
		table, isScyllaTable := controller.GetTable().(ScyllaTable)
		if !isScyllaTable {
			fmt.Printf("DeployScylla: %q was not compiled by the scylla driver, skipping\n",
				controller.GetTableName())
			continue
		}
		tableName := table.GetFullName()
		originColumns := tableColumnsMap[tableName]
		fmt.Println("Struct Type:", controller.GetTableName(), "| Columns:", len(originColumns))

		// Si no existe la tabla entonces la crea...
		if len(originColumns) == 0 {
			Logx(6, "No se encontró la tabla: "+tableName+"\n")
			Logx(2, fmt.Sprintf(`Creando tabla "%v"...`+"\n", tableName))

			columnsTypes := []string{}
			for _, e := range table.Columns {
				columnsTypes = append(columnsTypes, e.GetName()+" "+e.GetType().DBType)
			}

			keys := []string{}
			for _, key := range table.Keys {
				keys = append(keys, key.GetName())
			}

			pk := strings.Join(keys, ", ")
			partKey := table.GetPartKey()
			if partKey != nil && !partKey.IsNil() && len(partKey.GetName()) > 0 {
				pk = fmt.Sprintf("(%v), %v", partKey.GetName(), pk)
			}

			query := `
		CREATE TABLE %v (
			%v,
			PRIMARY KEY (%v)
		)
			WITH caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
			and compaction = {'class': 'SizeTieredCompactionStrategy'}
			and compression = {'compression_level': '3', 'sstable_compression': 'org.apache.cassandra.io.compress.ZstdCompressor'}
			and dclocal_read_repair_chance = 0
			and speculative_retry = '99.0PERCENTILE';
		`
			query = fmt.Sprintf(query, tableName, strings.Join(columnsTypes, ", "), pk)
			fmt.Println(query)

			err := QueryExec(query)
			if err != nil {
				panic(fmt.Sprintf(`Error creando tabla "%v" | `, tableName) + err.Error())
			}

			Logx(2, fmt.Sprintf(`Tabla creada: "%v"`+"\n", tableName))
		}

		columnsSchemaMap := map[string]IColInfo{}
		for name, col := range table.ColumnsMap {
			columnsSchemaMap[name] = col
		}
		columnsNoMapeadas := map[string]string{}

		for _, originColumn := range originColumns {
			if column, ok := columnsSchemaMap[originColumn.Name]; ok {
				if column.GetType().DBType != originColumn.Type {
					Logx(5, fmt.Sprintf(`La columna "%v" está definida con type "%v", pero en el Struct está con "%v" equivalente a "%v"`+"\n", originColumn.Name, originColumn.Type, column.GetType().FieldType, column.GetType().DBType))
				}
				delete(columnsSchemaMap, originColumn.Name)
			} else {
				columnsNoMapeadas[originColumn.Name] = originColumn.Type
			}
		}

		if len(originColumns) > 0 {
			// Revisa las columnas existentes en la BD pero no mapeadas
			for name, dbType := range columnsNoMapeadas {
				Logx(5, fmt.Sprintf(`La columna "%v" con type "%v" existe en la BD origen no está mapeada en el Struct.`+"\n", name, dbType))
			}
			// Revisa las columnas que deben crearse en BD
			for _, column := range columnsSchemaMap {
				Logx(5, fmt.Sprintf(`La columna "%v" con struct type "%v" no existe en la BD de origen.`+"\n", column.GetName(), column.GetType().FieldType))
				query := fmt.Sprintf(`ALTER TABLE %v ADD %v %v`, tableName, column.GetName(), column.GetType().DBType)
				fmt.Printf(`Ejecutando agregar columna "%v"...`+"\n", query)

				if err := QueryExec(query); err != nil {
					panic(fmt.Sprintf(`Error agregando columna "%v" | %v`, column.GetName(), err))
				}
				Logx(2, fmt.Sprintf(`Columna Agregada: "%v"`+"\n", column.GetName()))
			}
		}

		// Revisa si posee índices, en su defecto los crea
		tableIndexes := tableIndexesMap[tableName]

		if table.indexUpdatedTable != nil {
			indexUpdatedTableName := fmt.Sprintf("%v.%v", table.Namespace, table.indexUpdatedTable.name)
			if _, exists := tableColumnsMap[indexUpdatedTableName]; !exists {
				Logx(5, fmt.Sprintf(`No se encontró la tabla de index updates "%v". Creando...`+"\n", indexUpdatedTableName))

				createScript := getIndexUpdatedTableCreateScript(table.Namespace, table.indexUpdatedTable)
				fmt.Println(createScript)
				if err := QueryExec(createScript); err != nil {
					fmt.Println(err)
					panic(fmt.Sprintf(`Error creando la tabla de index updates "%v" en %v`, table.indexUpdatedTable.name, tableName))
				}

				tableColumnsMap[indexUpdatedTableName] = []ScyllaColumns{
					{Name: "partition_id", Type: "int", Keyspace: table.Namespace, Table: table.indexUpdatedTable.name},
					{Name: "index_id", Type: "smallint", Keyspace: table.Namespace, Table: table.indexUpdatedTable.name},
					{Name: "index_hash", Type: "int", Keyspace: table.Namespace, Table: table.indexUpdatedTable.name},
					{Name: "update_counter", Type: "int", Keyspace: table.Namespace, Table: table.indexUpdatedTable.name},
				}
				Logx(2, fmt.Sprintf(`Index update table created "%v"`+"\n", table.indexUpdatedTable.name))
			}
		}

		// TextSearchColumn-backed indexes used to live in Scylla as a
		// {table}_{column}_search_idx companion table. They've moved to
		// Sonic (backend/db/text_search) — collections and buckets are
		// created lazily on first write, so deploy does nothing here.

		for _, index := range table.indexes {
			if slices.Contains(tableIndexes, index.name) {
				continue
			}
			Logx(5, fmt.Sprintf(`No se encontró el índice "%v" en "%v". Creando...`+"\n", index.name, tableName))

			createScript := index.getCreateScript()
			fmt.Println(createScript)
			if err := QueryExec(createScript); err != nil {
				fmt.Println(err)
				panic(fmt.Sprintf(`Error creando el índice "%v" en %v`, index.name, tableName))
			}
			Logx(2, fmt.Sprintf(`Index created "%v"`+"\n", index.name))
		}

		// Revisa si posee views, en su defecto las crea
		for _, view := range table.views {
			name := table.Namespace + "." + view.name
			if liveViewColumns, ok := tableColumnsMap[name]; ok {
				repairViewMissingColumns(&table, view, liveViewColumns)
			} else {
				Logx(5, fmt.Sprintf(`No se encontró la view "%v" en la tabla "%v". Preparando creación...`+"\n", view.name, table.Name))

				createScript := view.getCreateScript()
				fmt.Println(createScript)
				if err := QueryExec(createScript); err != nil {
					fmt.Println(err)
					panic(fmt.Sprintf(`Error creando la view "%v" en %v`, view.name, tableName))
				}

				Logx(2, fmt.Sprintf(`View created "%v"`+"\n", view.name))
			}

			viewTableName := table.Namespace + "." + view.name
			if view.Type == 9 {
				maintenanceIndexName := getViewTableMaintenanceIndexName(view)
				viewTableIndexes := tableIndexesMap[viewTableName]
				if slices.Contains(viewTableIndexes, maintenanceIndexName) {
					continue
				}
				Logx(5, fmt.Sprintf(`No se encontró el índice "%v" en "%v". Creando...`+"\n", maintenanceIndexName, viewTableName))

				createScript := getViewTableMaintenanceIndexCreateScript(view, table)
				fmt.Println(createScript)
				if err := QueryExec(createScript); err != nil {
					fmt.Println(err)
					panic(fmt.Sprintf(`Error creando el índice "%v" en %v`, maintenanceIndexName, viewTableName))
				}

				Logx(2, fmt.Sprintf(`Index created "%v"`+"\n", maintenanceIndexName))
			}
		}
	}

	// Cache the results if cacheCode is provided
	if cacheCode > 0 {
		cacheCodePrev = cacheCode
		scyllaColumnsSaved = scyllaColumns
		scyllaIndexesSaved = scyllaIndexes
	}
}

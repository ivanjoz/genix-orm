package scylla

import (
	"errors"
	"fmt"
	"github.com/ivanjoz/genix-orm/db"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sync/errgroup"
)

func deduplicateRecordsByPrimaryKey(sink recordSink, scyllaTable ScyllaTable) {
	records := sink.heldRecords()
	if len(records) <= 1 {
		return
	}

	recordsUniqueMap := map[string]unsafe.Pointer{}
	for _, recordPointer := range records {
		recordKey := makePrimaryKeyRecordKey(recordPointer, scyllaTable)
		recordsUniqueMap[recordKey] = recordPointer
	}

	recordsDeduplicated := make([]unsafe.Pointer, 0, len(recordsUniqueMap))
	for _, recordPointer := range recordsUniqueMap {
		recordsDeduplicated = append(recordsDeduplicated, recordPointer)
	}
	sink.replaceRecords(recordsDeduplicated)
}

func buildRemainingWhereClauses(statements []ColumnStatement) []boundWhereClause {
	clauses := []boundWhereClause{}
	for _, statement := range statements {
		if len(statement.From) > 0 {
			for idx := range statement.From {
				columnName := statement.From[idx].Col
				// Keep BETWEEN inclusive to match the fluent API and post-filter behavior.
				clauses = append(clauses, boundWhereClause{
					Clause: fmt.Sprintf("%v >= ?", columnName),
					Values: []any{statement.From[idx].Value},
				})
				clauses = append(clauses, boundWhereClause{
					Clause: fmt.Sprintf("%v <= ?", columnName),
					Values: []any{statement.To[idx].Value},
				})
			}
			continue
		}

		if statement.Operator == "IN" {
			placeholders := make([]string, 0, len(statement.Values))
			queryValues := make([]any, 0, len(statement.Values))
			for _, value := range statement.Values {
				placeholders = append(placeholders, "?")
				queryValues = append(queryValues, value)
			}
			clauses = append(clauses, boundWhereClause{
				Clause: fmt.Sprintf("%v IN (%v)", statement.Col, strings.Join(placeholders, ", ")),
				Values: queryValues,
			})
			continue
		}

		clauses = append(clauses, boundWhereClause{
			Clause: fmt.Sprintf("%v %v ?", statement.Col, statement.Operator),
			Values: []any{statement.Value},
		})
	}
	return clauses
}

func makePrimaryKeyRecordKey(ptr unsafe.Pointer, scyllaTable ScyllaTable) string {
	// Dedupe key always includes partition key plus clustering keys to preserve table key semantics.
	parts := []string{}
	partKey := scyllaTable.GetPartKey()
	if partKey != nil && !partKey.IsNil() {
		parts = append(parts, fmt.Sprintf("%s=%v", partKey.GetName(), scyllaTable.GetPartValue(ptr)))
	}
	keyValues := scyllaTable.GetKeyValues(ptr)
	for keyIndex, keyColumn := range scyllaTable.Keys {
		parts = append(parts, fmt.Sprintf("%s=%v", keyColumn.GetName(), keyValues[keyIndex]))
	}
	return strings.Join(parts, "|")
}

func recordMatchesPostFilter(ptr unsafe.Pointer, statements []ColumnStatement, scyllaTable ScyllaTable) bool {
	// Final in-memory filtering guarantees exact semantics after overfetch (e.g. a one-sided packed index bound spilling into neighbouring prefixes).
	for _, statement := range statements {
		column := scyllaTable.ColumnsMap[statement.Col]
		if column == nil {
			return false
		}
		rawValue := column.GetRawValue(ptr)

		switch statement.Operator {
		case "=":
			if db.ConvertToInt64(rawValue) != db.ConvertToInt64(statement.Value) {
				return false
			}
		case ">":
			if db.ConvertToInt64(rawValue) <= db.ConvertToInt64(statement.Value) {
				return false
			}
		case ">=":
			if db.ConvertToInt64(rawValue) < db.ConvertToInt64(statement.Value) {
				return false
			}
		case "<":
			if db.ConvertToInt64(rawValue) >= db.ConvertToInt64(statement.Value) {
				return false
			}
		case "<=":
			if db.ConvertToInt64(rawValue) > db.ConvertToInt64(statement.Value) {
				return false
			}
		case "IN":
			matched := false
			rawValueInt := db.ConvertToInt64(rawValue)
			for _, value := range statement.Values {
				if rawValueInt == db.ConvertToInt64(value) {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		case "CONTAINS":
			contains := false
			statementValue := db.ConvertToInt64(statement.Value)
			for _, value := range db.FlattenInt64Values(rawValue) {
				if value == statementValue {
					contains = true
					break
				}
			}
			if !contains {
				return false
			}
		case "BETWEEN":
			if len(statement.From) == 0 || len(statement.To) == 0 {
				return false
			}
			valueI64 := db.ConvertToInt64(rawValue)
			fromI64 := db.ConvertToInt64(statement.From[0].Value)
			toI64 := db.ConvertToInt64(statement.To[0].Value)
			if valueI64 < fromI64 || valueI64 > toI64 {
				return false
			}
		}
	}
	return true
}

func formatDebugQuery(queryStr string, queryValues []any) string {
	// Keep debug output short by collapsing the projected column list before injecting values.
	minifiedQuery := minifySelectColumns(queryStr)
	if len(queryValues) == 0 {
		return minifiedQuery
	}

	var formattedQuery strings.Builder
	formattedQuery.Grow(len(minifiedQuery) + len(queryValues)*8)

	valueIndex := 0
	for _, queryChar := range minifiedQuery {
		if queryChar == '?' && valueIndex < len(queryValues) {
			formattedQuery.WriteString(formatDebugValue(queryValues[valueIndex]))
			valueIndex++
			continue
		}
		formattedQuery.WriteRune(queryChar)
	}

	if valueIndex < len(queryValues) {
		formattedQuery.WriteString(" /* extra_values=")
		formattedQuery.WriteString(formatDebugValues(queryValues[valueIndex:]))
		formattedQuery.WriteString(" */")
	}

	return formattedQuery.String()
}

func minifySelectColumns(queryStr string) string {
	// Only rewrite the top-level SELECT projection so the rest of the query stays intact.
	upperQuery := strings.ToUpper(queryStr)
	selectIndex := strings.Index(upperQuery, "SELECT ")
	fromIndex := strings.Index(upperQuery, " FROM ")
	if selectIndex < 0 || fromIndex <= selectIndex+len("SELECT ") {
		return queryStr
	}

	selectColumns := queryStr[selectIndex+len("SELECT ") : fromIndex]
	columnCount := countTopLevelCSVColumns(selectColumns)
	if columnCount == 0 {
		return queryStr
	}

	return queryStr[:selectIndex] + fmt.Sprintf("SELECT (%d)", columnCount) + queryStr[fromIndex:]
}

func countTopLevelCSVColumns(columnsText string) int {
	// Count only commas at the top level so function calls like foo(a, b) stay as one projected column.
	trimmedColumns := strings.TrimSpace(columnsText)
	if len(trimmedColumns) == 0 {
		return 0
	}

	columnCount := 1
	parenthesisDepth := 0
	for _, currentChar := range trimmedColumns {
		switch currentChar {
		case '(':
			parenthesisDepth++
		case ')':
			if parenthesisDepth > 0 {
				parenthesisDepth--
			}
		case ',':
			if parenthesisDepth == 0 {
				columnCount++
			}
		}
	}

	return columnCount
}

func formatDebugValues(queryValues []any) string {
	formattedValues := make([]string, 0, len(queryValues))
	for _, queryValue := range queryValues {
		formattedValues = append(formattedValues, formatDebugValue(queryValue))
	}
	return "[" + strings.Join(formattedValues, ", ") + "]"
}

func formatDebugValue(rawValue any) string {
	// Render bound values as valid-enough CQL literals for debugging and copy-paste inspection.
	if rawValue == nil {
		return "null"
	}

	switch value := rawValue.(type) {
	case string:
		return "'" + strings.ReplaceAll(value, "'", "''") + "'"
	case []byte:
		return fmt.Sprintf("0x%x", value)
	case fmt.Stringer:
		return "'" + strings.ReplaceAll(value.String(), "'", "''") + "'"
	case time.Time:
		return "'" + value.Format(time.RFC3339Nano) + "'"
	case bool:
		if value {
			return "true"
		}
		return "false"
	}

	valueRef := reflect.ValueOf(rawValue)
	for valueRef.Kind() == reflect.Pointer {
		if valueRef.IsNil() {
			return "null"
		}
		valueRef = valueRef.Elem()
	}

	switch valueRef.Kind() {
	case reflect.String:
		return "'" + strings.ReplaceAll(valueRef.String(), "'", "''") + "'"
	case reflect.Slice, reflect.Array:
		formattedValues := make([]string, 0, valueRef.Len())
		for idx := 0; idx < valueRef.Len(); idx++ {
			formattedValues = append(formattedValues, formatDebugValue(valueRef.Index(idx).Interface()))
		}
		return "[" + strings.Join(formattedValues, ", ") + "]"
	}

	return fmt.Sprintf("%v", valueRef.Interface())
}

func normalizeScannedValue(value any) any {
	// RowData stores pointers for scalar columns; grouped virtual decomposition works with the plain value.
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

func scanSelectQueryRows(
	queryStr string,
	queryValues []any,
	scanColumns []selectScanColumn,
	scyllaTable ScyllaTable,
	sink recordSink,
	postFilterStatements []ColumnStatement,
	scanHandler func(unsafe.Pointer) bool,
	queryNoticeTime time.Time,
) error {
	usePostFilter := len(postFilterStatements) > 0

	doScan := func() error {
		if ShouldLog() {
			// Log the executable statement with compact projection output to keep noisy SELECTs readable.
			fmt.Printf("Select Elapsed: %d | %s\n",
				time.Since(queryNoticeTime).Milliseconds(),
				formatDebugQuery(queryStr, queryValues),
			)
		}

		query := getScyllaConnection().Query(queryStr, queryValues...)
		if scyllaTable.readConsistency != 0 {
			query = query.Consistency(scyllaTable.readConsistency)
		}
		iter := query.Iter()
		rd, err := iter.RowData()
		if err != nil {
			fmt.Println("Error on RowData::", err)
			fmt.Println("Query::", formatDebugQuery(queryStr, queryValues))
			if strings.Contains(err.Error(), "use ALLOW FILTERING") {
				fmt.Println("Posible Indexes or Views::")
				for _, capability := range scyllaTable.capabilities {
					fmt.Println(capability.Signature)
				}
			}
			return err
		}

		rowsScaned := 0
		rowsFinal := 0

		// A sink that copies on append (a typed destination) lets every row decode into one
		// scratch record. A scan handler or a collecting sink may keep the pointer, so those still
		// get a fresh record per row.
		var scratchRecord unsafe.Pointer
		if sink.resetRecord != nil && scanHandler == nil {
			scratchRecord = sink.newRecord()
		}

		scanner := iter.Scanner()
		for scanner.Next() {
			rowsScaned++

			rowValues := rd.Values
			if err := scanner.Scan(rowValues...); err != nil {
				fmt.Println("Error on scan::", err)
				return err
			}

			recordPtr := scratchRecord
			if recordPtr != nil {
				sink.resetRecord(recordPtr)
			} else {
				recordPtr = sink.newRecord()
			}
			// shouldLog := ShouldLogFull()

			/*
				if shouldLog {
					fmt.Printf("\n--- Scanning record %d ---\n", atomic.LoadUint32(&LogCount)+1)
				}
			*/

			// Map each scanned DB column into the destination struct using precomputed column metadata.
			for columnIndex, scanColumn := range scanColumns {
				columnName := scanColumn.ColumnName
				column := scyllaTable.ColumnsMap[columnName]
				value := rowValues[columnIndex]
				/*
					if shouldLog {
						valueText := "nil"
						if value != nil {
							valueText = fmt.Sprintf("%v", reflect.Indirect(reflect.ValueOf(value)).Interface())
						}
						offset := uintptr(0)
						if column.GetInfo().Field != nil {
							offset = column.GetInfo().Field.Offset
						}
						fmt.Printf("Col: %-20s (Field: %-20s) | Offset: %-4d | DB Value: %s\n", columnName, column.GetInfo().FieldName, offset, valueText)
					}
				*/
				if value == nil {
					continue
				}
				if scanColumn.DecomposeView != nil && scanColumn.DecomposeView.decomposeVirtualValue != nil {
					// Packed group keys are scanned as one virtual column and decomposed back into the primitive fields.
					decomposedValues := scanColumn.DecomposeView.decomposeVirtualValue(normalizeScannedValue(value))
					for valueIndex, sourceColumn := range scanColumn.DecomposeView.PackedSourceColumns {
						if valueIndex >= len(decomposedValues) {
							break
						}
						sourceColumn.SetValue(recordPtr, decomposedValues[valueIndex])
					}
					continue
				}
				column.SetValue(recordPtr, value)
			}
			/*
				if shouldLog {
					recordJSON, _ := json.MarshalIndent(record, "", "  ")
					fmt.Printf("Resulting Struct: %s\n", string(recordJSON))
				}
			*/

			// Post-filter keeps exact semantics when indexed query planning intentionally overfetches.
			if usePostFilter && !recordMatchesPostFilter(recordPtr, postFilterStatements, scyllaTable) {
				continue
			}

			// Let callers process the decoded row and optionally avoid storing it to reduce peak memory.
			if scanHandler != nil && scanHandler(recordPtr) {
				continue
			}

			rowsFinal++
			sink.appendRecord(recordPtr)
		}

		// Per-query detail is debug output (GENIX_DB_LOG): printed unconditionally it was a stdout
		// write on every query of every request.
		if ShouldLog() {
			fmt.Printf(`Table %v | Rows Scanned %v | Final %v`+"\n", scyllaTable.Name, rowsScaned, rowsFinal)
		}

		return nil
	}

	err := doScan()
	if err != nil && strings.Contains(err.Error(), "no hosts available") {
		scyllaSession = nil
		fmt.Println("Reconectando con ScyllaDB...")
		getScyllaConnection()
		err = doScan()
	}

	return err
}

func executeBoundSelectQueries(
	sink recordSink,
	boundStatements []BoundSelectStatement,
	scanColumns []selectScanColumn,
	requiresDeduplication bool,
	scanHandler func(unsafe.Pointer) bool,
	scyllaTable ScyllaTable,
	queryNoticeTime time.Time,
) error {
	// Keep query execution shared so cached and legacy planning paths use the same scan, merge, and dedup behavior.
	if len(boundStatements) == 0 {
		return nil
	}

	// Each statement of a fan-out scans into its own list, so the goroutines never share one.
	recordsMap := map[int]*[]unsafe.Pointer{}
	for statementIndex := range boundStatements {
		recordsMap[statementIndex] = &[]unsafe.Pointer{}
	}

	eg := errgroup.Group{}
	// Cap fanout so a chunked IN query (often 100+ statements) can't flood a single coordinator and trigger read timeouts.
	eg.SetLimit(8)
	for statementIndex, boundStatement := range boundStatements {
		recordsTarget := sink.collectInto(recordsMap[statementIndex])
		if len(boundStatements) == 1 {
			recordsTarget = sink
		}

		// fmt.Println("Query::", boundStatement.QueryStr)
		boundStatement := boundStatement
		eg.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("Panic in select goroutine: %v", r)
				}
			}()

			return scanSelectQueryRows(
				boundStatement.QueryStr,
				boundStatement.QueryValues,
				scanColumns,
				scyllaTable,
				recordsTarget,
				boundStatement.PostFilterStatements,
				scanHandler,
				queryNoticeTime,
			)
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	if len(boundStatements) > 1 {
		for _, records := range recordsMap {
			for _, record := range *records {
				sink.appendRecord(record)
			}
		}
	}
	if requiresDeduplication {
		deduplicateRecordsByPrimaryKey(sink, scyllaTable)
	}

	return nil
}

// selectExec executes a query using TableSchema and TableInfo
func selectExec(
	sink recordSink,
	tableInfo *TableInfo,
	scyllaTable ScyllaTable,
	scanHandler func(unsafe.Pointer) bool,
	selectStartTime time.Time,
) error {
	if len(scyllaTable.Namespace) == 0 {
		scyllaTable.Namespace = connParams.Keyspace
	}
	if len(scyllaTable.Namespace) == 0 {
		return errors.New("no se ha especificado un keyspace")
	}
	if tableInfo.Delta != nil {
		return execDelta(sink, tableInfo, scyllaTable)
	}

	compiledSelect, err := tryGetOrCompileSelectStatement(tableInfo, scyllaTable)
	if err != nil {
		return err
	}

	boundPlan, err := compiledSelect.Compute(tableInfo, scyllaTable)
	if err != nil {
		return err
	}

	if err := executeBoundSelectQueries(
		sink,
		boundPlan.Statements,
		boundPlan.ScanColumns,
		boundPlan.RequiresDeduplication,
		scanHandler,
		scyllaTable,
		selectStartTime,
	); err != nil {
		return err
	}

	return nil
}

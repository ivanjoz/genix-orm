package pgdriver

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// The SQL report path: a read the ORM's query builder does not express (a JOIN, a GROUP BY over
// two tables), written as SQL by its caller and scanned into a row struct. It reads only; every
// write goes through the managed write path. Table names come from QualifiedTableName, never
// hard-coded, so a renamed table or another database still resolves.

// QualifiedTableName is the "schema"."table" of a bound table struct (db.TableOf), for SQL reports.
func QualifiedTableName(schemaStruct any) string {
	return getOrCompilePgTable(schemaStruct).qualifiedName()
}

// sqlRowFieldsCache maps a row struct type to its field index per lower-cased column label.
var sqlRowFieldsCache sync.Map

// sqlRowFields: a field answers to its `db` tag, else to its name, case-insensitively.
func sqlRowFields(rowType reflect.Type) map[string]int {
	if cached, isCached := sqlRowFieldsCache.Load(rowType); isCached {
		return cached.(map[string]int)
	}
	fieldByLabel := map[string]int{}
	for fieldIndex := range rowType.NumField() {
		field := rowType.Field(fieldIndex)
		if !field.IsExported() {
			continue
		}
		fieldByLabel[strings.ToLower(field.Name)] = fieldIndex
		if tag, _, _ := strings.Cut(field.Tag.Get("db"), ","); tag != "" {
			fieldByLabel[strings.ToLower(tag)] = fieldIndex
		}
	}
	sqlRowFieldsCache.Store(rowType, fieldByLabel)
	return fieldByLabel
}

// QuerySQL runs a read-only statement and scans every row into a RowT. Each result column must name
// a field of RowT (alias it with AS); a column that can be NULL needs a COALESCE.
func QuerySQL[RowT any](statement string, args ...any) ([]RowT, error) {
	pool, err := sharedConnection()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	rows, err := queryStatement(pool, ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: SQL report: %w", err)
	}
	defer rows.Close()
	columnLabels, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	rowType := reflect.TypeFor[RowT]()
	fieldByLabel := sqlRowFields(rowType)
	fieldIndexes := make([]int, len(columnLabels))
	for position, label := range columnLabels {
		fieldIndex, isField := fieldByLabel[strings.ToLower(label)]
		if !isField {
			return nil, fmt.Errorf("pgdriver: SQL report: column %q names no field of %v", label, rowType)
		}
		fieldIndexes[position] = fieldIndex
	}
	result := []RowT{}
	targets := make([]any, len(columnLabels))
	for rows.Next() {
		var row RowT
		rowValue := reflect.ValueOf(&row).Elem()
		for position, fieldIndex := range fieldIndexes {
			targets[position] = rowValue.Field(fieldIndex).Addr().Interface()
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, fmt.Errorf("pgdriver: SQL report: %w", err)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

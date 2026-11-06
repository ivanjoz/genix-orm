package db

import (
	"fmt"
	"slices"
	"unsafe"
)

// TextSearchIndexInfo is a table's compiled TextSearch declaration. Which records an index holds,
// and with what text, is the same on every driver; only where the index is stored differs.
type TextSearchIndexInfo struct {
	// Columns hold the indexed text, in order.
	Columns []IColInfo
	// StatusColumn marks a record inactive (0), which takes it out of the index; nil means every
	// record is active.
	StatusColumn IColInfo
	// Manual: writes do not touch the index, only the controller's reindex does.
	Manual bool
	// The record-ID column is always the table's single key (Keys[0]) and the partition is always
	// PartKey, so both are read from the TableCore directly rather than cached here.
}

// ConfigureTextSearchIndex validates a table's TextSearch declaration: a partition column, exactly
// one numeric key (the document ID), persisted string columns for the text and an optional int8
// status column. Schema mistakes panic when the table compiles; they are never recoverable at
// runtime.
func (core *TableCore) ConfigureTextSearchIndex(schema TableSchema) {
	textSearch := schema.TextSearch
	if textSearch == nil {
		return
	}

	fail := func(format string, args ...any) {
		panic(fmt.Sprintf(`Table "%v": TextSearch `, core.Name) + fmt.Sprintf(format, args...))
	}

	if core.PartKey == nil || core.PartKey.IsNil() {
		fail("requires a partition column")
	}
	if fieldType := core.PartKey.GetType().FieldType; fieldType != "int32" && fieldType != "int" {
		fail(`partition column "%v" must be int32. Found: %v`, core.PartKey.GetName(), fieldType)
	}
	if len(core.Keys) != 1 {
		fail("requires exactly one key column. Found: %v", len(core.Keys))
	}
	if fieldType := core.Keys[0].GetType().FieldType; !slices.Contains([]string{"int16", "int32", "int64", "int"}, fieldType) {
		fail(`key column "%v" must be numeric. Found: %v`, core.Keys[0].GetName(), fieldType)
	}
	if len(textSearch.Columns) == 0 {
		fail("declares no Columns")
	}

	info := &TextSearchIndexInfo{Manual: textSearch.Manual}

	for _, declared := range textSearch.Columns {
		column := core.ColumnsMap[declared.GetName()]
		if column == nil {
			fail(`column "%v" was not found`, declared.GetName())
		}
		if fieldType := column.GetType().FieldType; fieldType != "string" {
			fail(`column "%v" must be string. Found: %v`, column.GetName(), fieldType)
		}
		// A reindex reads the text back from the table, so it has to be stored there.
		if column.GetInfo().IsVirtual {
			fail(`column "%v" is virtual; the indexed text must be persisted`, column.GetName())
		}
		info.Columns = append(info.Columns, column)
	}

	if textSearch.Status != nil {
		column := core.ColumnsMap[textSearch.Status.GetName()]
		if column == nil {
			fail(`status column "%v" was not found`, textSearch.Status.GetName())
		}
		if fieldType := column.GetType().FieldType; fieldType != "int8" {
			fail(`status column "%v" must be int8. Found: %v`, column.GetName(), fieldType)
		}
		info.StatusColumn = column
	}

	core.TextSearchIndex = info
}

// TextOf is the indexed text of a record: its TextSearch columns joined by a space.
func (info *TextSearchIndexInfo) TextOf(recordPointer unsafe.Pointer) string {
	values := make([]string, len(info.Columns))
	for i, column := range info.Columns {
		values[i], _ = column.GetRawValue(recordPointer).(string)
	}
	return JoinSearchText(values...)
}

// ActiveOf reports whether a record belongs in the index: its status is not 0, or the table has
// no status column.
func (info *TextSearchIndexInfo) ActiveOf(recordPointer unsafe.Pointer) bool {
	return info.StatusColumn == nil || ConvertToInt64(info.StatusColumn.GetRawValue(recordPointer)) != 0
}

// WritesAll reports whether a write that set columns wrote every column the index depends on, so
// the records in memory hold their stored text and status.
func (info *TextSearchIndexInfo) WritesAll(columns []IColInfo) bool {
	written := make(map[string]bool, len(columns))
	for _, column := range columns {
		if column != nil {
			written[column.GetName()] = true
		}
	}

	for _, column := range info.Columns {
		if !written[column.GetName()] {
			return false
		}
	}
	return info.StatusColumn == nil || written[info.StatusColumn.GetName()]
}

// Touches reports whether a write that set columns can change the index at all.
func (info *TextSearchIndexInfo) Touches(columns []IColInfo) bool {
	for _, column := range columns {
		if column == nil {
			continue
		}
		if info.StatusColumn != nil && column.GetName() == info.StatusColumn.GetName() {
			return true
		}
		for _, textColumn := range info.Columns {
			if column.GetName() == textColumn.GetName() {
				return true
			}
		}
	}
	return false
}

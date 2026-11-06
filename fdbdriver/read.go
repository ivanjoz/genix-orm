package fdbdriver

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/fdb"
)

// The read algorithm for a plain Select (docs/FOUNDATIONDB_PLAN.md §9, reduced): the partition's
// records are one contiguous range, so a read with no usable index scans that range — narrowed to a
// point read when the keys are pinned, or to a key range on the first key — and applies every
// predicate exactly in memory. No index entries are read yet except TypeDelta's (delta.go), so the
// planner's index choice does not apply here; results are the same, only the cost differs.

const (
	readPageRecords = 10_000
	readPageBytes   = 4 << 20
)

// recordSink is where decoded records go, seen through unsafe pointers so the read path is not
// stenciled per record type.
type recordSink struct {
	recordType reflect.Type
	newRecord  func() unsafe.Pointer
	// appendRecord stores a record, unless the caller's scan handler consumed it.
	appendRecord func(record unsafe.Pointer)
	reserve      func(additionalRecords int)
}

func makeRecordSink[RecordT any](destination *[]RecordT, scan func(record *RecordT) bool) recordSink {
	return recordSink{
		recordType: reflect.TypeFor[RecordT](),
		newRecord:  func() unsafe.Pointer { return unsafe.Pointer(new(RecordT)) },
		appendRecord: func(record unsafe.Pointer) {
			if scan != nil && scan((*RecordT)(record)) {
				return
			}
			*destination = append(*destination, *(*RecordT)(record))
		},
		reserve: func(additionalRecords int) { *destination = slices.Grow(*destination, additionalRecords) },
	}
}

func selectRecords(table *FdbTable, tableInfo *db.TableInfo, sink recordSink) error {
	if tableInfo.Delta != nil {
		return selectDelta(table, tableInfo, sink)
	}
	if len(tableInfo.GroupByColumns) > 0 {
		return errNotImplemented("GroupBy reads")
	}
	gate, err := gateClient()
	if err != nil {
		return err
	}
	statements := slices.Clone(tableInfo.Statements)
	if tableInfo.Between.Operator != "" {
		statements = append(statements, tableInfo.Between)
	}
	for _, statement := range statements {
		if table.ColumnsMap[statement.Col] == nil {
			return fmt.Errorf("table %q: no column %q", table.Name, statement.Col)
		}
		if table.ColumnsMap[statement.Col].GetInfo().IsComputed || table.ColumnsMap[statement.Col].GetInfo().IsVirtual {
			return fmt.Errorf("table %q: filtering on the generated column %q is not implemented on FoundationDB yet", table.Name, statement.Col)
		}
	}
	partitions, err := table.resolvePartitions(statements)
	if err != nil {
		return err
	}
	filter := table.projectionFilter(tableInfo, statements, nil)
	isDescending := tableInfo.OrderBy != ""
	limit := int(tableInfo.Limit)

	// The pinned primary key turns the read into point reads.
	if keyCombinations, isPinned := table.pinnedKeyCombinations(statements); isPinned {
		var recordKeys [][]byte
		for _, partition := range partitions {
			for _, keyValues := range keyCombinations {
				recordKey := append(table.partitionPrefix(partition), keyKindRecord)
				for keyIndex, keyColumn := range table.Keys {
					recordKey = appendKeyComponent(recordKey, keyColumn, keyValues[keyIndex])
				}
				recordKeys = append(recordKeys, recordKey)
			}
		}
		ctx, cancel := requestContext()
		rows, isPresent, err := gate.GetMany(ctx, recordKeys, false)
		cancel()
		if err != nil {
			return fmt.Errorf("table %q: %w", table.Name, err)
		}
		emitted := 0
		for rowIndex, row := range rows {
			if !isPresent[rowIndex] || (limit > 0 && emitted >= limit) {
				continue
			}
			if emitted, err = table.emitRow(row, filter, statements, sink, emitted); err != nil {
				return err
			}
		}
		return nil
	}

	emitted := 0
	for _, partition := range partitions {
		recordsPrefix := append(table.partitionPrefix(partition), keyKindRecord)
		readRange := table.firstKeyRange(recordsPrefix, statements)
		readRange.Reverse = isDescending
		for {
			readRange.Limit = readPageRecords
			readRange.ByteLimit = readPageBytes
			ctx, cancel := requestContext()
			results, err := gate.GetRanges(ctx, []fdb.Range{readRange})
			cancel()
			if err != nil {
				return fmt.Errorf("table %q: %w", table.Name, err)
			}
			for _, row := range results[0].Values.Slices() {
				if limit > 0 && emitted >= limit {
					return nil
				}
				if emitted, err = table.emitRow(row, filter, statements, sink, emitted); err != nil {
					return err
				}
			}
			if !results[0].More {
				break
			}
			if isDescending {
				readRange.End = results[0].NextBegin
			} else {
				readRange.Begin = results[0].NextBegin
			}
		}
	}
	return nil
}

// emitRow decodes one row, filters it exactly and hands it to the sink.
func (table *FdbTable) emitRow(row []byte, filter columnFilter, statements []db.ColumnStatement, sink recordSink, emitted int) (int, error) {
	recordPointer := sink.newRecord()
	if err := table.decodeRow(row, recordPointer, filter); err != nil {
		return emitted, err
	}
	if !recordMatchesStatements(table, recordPointer, statements) {
		return emitted, nil
	}
	sink.appendRecord(recordPointer)
	return emitted + 1, nil
}

// resolvePartitions lists the partitions a read covers: the equality or IN on the partition column.
func (table *FdbTable) resolvePartitions(statements []db.ColumnStatement) ([]int64, error) {
	if table.PartKey == nil || table.PartKey.IsNil() {
		return []int64{0}, nil
	}
	for _, statement := range statements {
		if statement.Col != table.PartKey.GetName() {
			continue
		}
		switch statement.Operator {
		case "=":
			return []int64{db.ConvertToInt64(statement.Value)}, nil
		case "IN":
			partitions := make([]int64, 0, len(statement.Values))
			for _, value := range statement.Values {
				partitions = append(partitions, db.ConvertToInt64(value))
			}
			return partitions, nil
		}
	}
	return nil, fmt.Errorf(`table %q: a read needs an equality or IN on the partition column %q (global indexes are not implemented on FoundationDB yet)`,
		table.Name, table.PartKey.GetName())
}

// pinnedKeyCombinations returns every primary key the statements pin with = or IN, when they pin all
// key columns.
func (table *FdbTable) pinnedKeyCombinations(statements []db.ColumnStatement) ([][]any, bool) {
	combinations := [][]any{{}}
	for _, keyColumn := range table.Keys {
		var columnValues []any
		for _, statement := range statements {
			if statement.Col != keyColumn.GetName() {
				continue
			}
			if statement.Operator == "=" {
				columnValues = []any{statement.Value}
			} else if statement.Operator == "IN" {
				columnValues = statement.Values
			}
		}
		if columnValues == nil {
			return nil, false
		}
		nextCombinations := make([][]any, 0, len(combinations)*len(columnValues))
		for _, combination := range combinations {
			for _, value := range columnValues {
				nextCombinations = append(nextCombinations, append(slices.Clone(combination), value))
			}
		}
		combinations = nextCombinations
	}
	return combinations, true
}

// firstKeyRange narrows the records range with the first key column's equality or bounds.
func (table *FdbTable) firstKeyRange(recordsPrefix []byte, statements []db.ColumnStatement) fdb.Range {
	readRange := fdb.Range{Begin: recordsPrefix, End: prefixEnd(recordsPrefix)}
	firstKey := table.Keys[0]
	componentKey := func(value any) []byte {
		return appendKeyComponent(slices.Clone(recordsPrefix), firstKey, value)
	}
	for _, statement := range statements {
		if statement.Col != firstKey.GetName() {
			continue
		}
		switch statement.Operator {
		case "=":
			equalPrefix := componentKey(statement.Value)
			return fdb.Range{Begin: equalPrefix, End: prefixEnd(equalPrefix)}
		case ">=":
			readRange.Begin = maxKey(readRange.Begin, componentKey(statement.Value))
		case ">":
			readRange.Begin = maxKey(readRange.Begin, prefixEnd(componentKey(statement.Value)))
		case "<=":
			readRange.End = minKey(readRange.End, prefixEnd(componentKey(statement.Value)))
		case "<":
			readRange.End = minKey(readRange.End, componentKey(statement.Value))
		case "BETWEEN":
			readRange.Begin = maxKey(readRange.Begin, componentKey(statement.From[0].Value))
			readRange.End = minKey(readRange.End, prefixEnd(componentKey(statement.To[0].Value)))
		}
	}
	return readRange
}

func maxKey(left, right []byte) []byte {
	if string(left) >= string(right) {
		return left
	}
	return right
}

func minKey(left, right []byte) []byte {
	if string(left) <= string(right) {
		return left
	}
	return right
}

// projectionFilter is the columns a read decodes: the caller's Select/Exclude projection plus every
// column a predicate or the protocol (required) reads. nil decodes every column.
func (table *FdbTable) projectionFilter(tableInfo *db.TableInfo, statements []db.ColumnStatement, required []string) columnFilter {
	if len(tableInfo.ColumnsInclude) == 0 && len(tableInfo.ColumnsExclude) == 0 {
		return nil
	}
	isWanted := map[string]bool{}
	if len(tableInfo.ColumnsInclude) > 0 {
		for _, column := range tableInfo.ColumnsInclude {
			isWanted[column.Name] = true
		}
	} else {
		isExcluded := map[string]bool{}
		for _, column := range tableInfo.ColumnsExclude {
			isExcluded[column.Name] = true
		}
		for _, stored := range table.storedColumns {
			isWanted[stored.column.Name] = !isExcluded[stored.column.Name]
		}
	}
	for _, statement := range statements {
		isWanted[statement.Col] = true
	}
	for _, requiredName := range required {
		isWanted[requiredName] = true
	}
	filter := make(columnFilter, len(table.storedColumns))
	for position, stored := range table.storedColumns {
		filter[position] = isWanted[stored.column.Name]
	}
	return filter
}

// recordMatchesStatements applies the predicates exactly to a decoded record.
func recordMatchesStatements(table *FdbTable, recordPointer unsafe.Pointer, statements []db.ColumnStatement) bool {
	for _, statement := range statements {
		column := table.ColumnsMap[statement.Col]
		recordValue := column.GetRawValue(recordPointer)
		switch statement.Operator {
		case "=":
			if compareValues(recordValue, statement.Value) != 0 {
				return false
			}
		case "IN":
			if !slices.ContainsFunc(statement.Values, func(value any) bool { return compareValues(recordValue, value) == 0 }) {
				return false
			}
		case ">":
			if compareValues(recordValue, statement.Value) <= 0 {
				return false
			}
		case ">=":
			if compareValues(recordValue, statement.Value) < 0 {
				return false
			}
		case "<":
			if compareValues(recordValue, statement.Value) >= 0 {
				return false
			}
		case "<=":
			if compareValues(recordValue, statement.Value) > 0 {
				return false
			}
		case "BETWEEN":
			if compareValues(recordValue, statement.From[0].Value) < 0 || compareValues(recordValue, statement.To[0].Value) > 0 {
				return false
			}
		case "CONTAINS":
			if !sliceContains(recordValue, statement.Value) {
				return false
			}
		default:
			panic(fmt.Sprintf("fdbdriver: operator %q is not supported", statement.Operator))
		}
	}
	return true
}

// compareValues orders two column values: strings by text, floats by number, integers as int64.
func compareValues(left, right any) int {
	switch typedLeft := left.(type) {
	case string:
		return strings.Compare(typedLeft, fmt.Sprint(right))
	case float32, float64:
		leftNumber, rightNumber := db.ToFloat64(left), db.ToFloat64(right)
		switch {
		case leftNumber < rightNumber:
			return -1
		case leftNumber > rightNumber:
			return 1
		}
		return 0
	case bool:
		if typedLeft == (right == true) {
			return 0
		}
		return 1
	}
	leftNumber, rightNumber := db.ConvertToInt64(left), db.ConvertToInt64(right)
	switch {
	case leftNumber < rightNumber:
		return -1
	case leftNumber > rightNumber:
		return 1
	}
	return 0
}

func sliceContains(sliceValue, wanted any) bool {
	elements := reflect.ValueOf(sliceValue)
	if elements.Kind() != reflect.Slice {
		return false
	}
	for index := range elements.Len() {
		if compareValues(elements.Index(index).Interface(), wanted) == 0 {
			return true
		}
	}
	return false
}

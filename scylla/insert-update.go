package scylla

import (
	"fmt"
	"github.com/ivanjoz/genix-orm/db"
	"reflect"
	"slices"
	"strings"
	"unsafe"

	"github.com/gocql/gocql"
	"github.com/viant/xunsafe"
	"golang.org/x/sync/errgroup"
)

const maxInsertBatchRows = 500

var getWriteCounterValue = GetCounter
var getManagedUnixTime = currentManagedUnixTime

type managedWriteValues struct {
	createdValues        []any
	updatedValues        []any
	updatedVersionValues []any
}

type prefetchedManagedCounterValues struct {
	counterValueByPartition map[int64]any
	counterNameByPartition  map[int64]string
}

func currentManagedUnixTime() int32 {
	// Keep DB-managed audit timestamps aligned with the project SUnixTime convention without importing core.
	// db.Now() rather than time.Now() so a historical-clock override reaches the columns the ORM
	// writes on its own: otherwise a backdated record would carry today's created/updated.
	return int32((db.Now().Unix() - 1e9) / 2)
}

func (e managedWriteValues) slice(start int, end int) managedWriteValues {
	slicedValues := managedWriteValues{}
	if len(e.createdValues) > 0 {
		slicedValues.createdValues = e.createdValues[start:end]
	}
	if len(e.updatedValues) > 0 {
		slicedValues.updatedValues = e.updatedValues[start:end]
	}
	if len(e.updatedVersionValues) > 0 {
		slicedValues.updatedVersionValues = e.updatedVersionValues[start:end]
	}
	return slicedValues
}

func (e managedWriteValues) getValueForColumn(recordIndex int, column IColInfo, isInsert bool) (any, bool) {
	switch column.GetName() {
	case managedCreatedColumnName:
		if isInsert && recordIndex < len(e.createdValues) && e.createdValues[recordIndex] != nil {
			return e.createdValues[recordIndex], true
		}
	case managedUpdatedColumnName:
		if recordIndex < len(e.updatedValues) && e.updatedValues[recordIndex] != nil {
			return e.updatedValues[recordIndex], true
		}
	case managedUpdatedVersionColumnName:
		if recordIndex < len(e.updatedVersionValues) && e.updatedVersionValues[recordIndex] != nil {
			return e.updatedVersionValues[recordIndex], true
		}
	}
	return nil, false
}

func coerceManagedIntegerValue(column IColInfo, value int64) any {
	switch column.GetType().FieldType {
	case "int8":
		return int8(value)
	case "int16":
		return int16(value)
	case "int32":
		return int32(value)
	case "int64":
		return value
	case "int":
		return int(value)
	default:
		return int32(value)
	}
}

func resolveManagedTimestampValue(column IColInfo, ptr unsafe.Pointer, fallbackValue int64) any {
	if column == nil {
		return nil
	}

	currentValue := column.GetRawValue(ptr)
	if currentValue != nil {
		currentValueInt := convertToInt64(currentValue)
		if currentValueInt > 0 {
			return coerceManagedIntegerValue(column, currentValueInt)
		}
	}

	return coerceManagedIntegerValue(column, fallbackValue)
}

type selfParser interface {
	SelfParse()
}

func runSelfParseIfDefined(records recordSlice) {
	if records.len() == 0 {
		return
	}
	if _, hasSelfParse := records.recordPointerAt(0).(selfParser); !hasSelfParse {
		return
	}
	for i := range records.len() {
		records.recordPointerAt(i).(selfParser).SelfParse()
	}
}

func fetchManagedCounterValues(
	records recordSliceGroup, scyllaTable ScyllaTable,
) (prefetchedManagedCounterValues, error) {
	prefetchedValues := prefetchedManagedCounterValues{
		counterValueByPartition: map[int64]any{},
		counterNameByPartition:  map[int64]string{},
	}
	if records.len() == 0 || scyllaTable.UpdatedVersionCol == nil {
		return prefetchedValues, nil
	}

	partitionColumn := scyllaTable.GetPartKey()
	partitionValuesToFetch := map[int64]struct{}{}
	for recordIndex := range records.len() {
		recordPointer := records.at(recordIndex)
		partitionValue := int64(0)
		if partitionColumn != nil && !partitionColumn.IsNil() {
			partitionValue = convertToInt64(partitionColumn.GetRawValue(recordPointer))
		}
		partitionValuesToFetch[partitionValue] = struct{}{}
	}

	for partitionValue := range partitionValuesToFetch {
		counterName := fmt.Sprintf("x%v_%v_updated", partitionValue, scyllaTable.Name)
		nextCounterValue, err := getWriteCounterValue(scyllaTable.Namespace, counterName, 1)
		if err != nil {
			return prefetchedManagedCounterValues{}, fmt.Errorf("write updated_version %s: %w", counterName, err)
		}
		// A delta view packs the version into a fixed digit slot and trims overruns from the right,
		// which would silently collapse versions into buckets of ten. Refuse the write instead.
		if scyllaTable.maxDeltaVersionValue > 0 && nextCounterValue > scyllaTable.maxDeltaVersionValue {
			return prefetchedManagedCounterValues{}, fmt.Errorf(
				`table %q: updated_version %d exhausted the delta view slot (max %d) for counter %s`,
				scyllaTable.Name, nextCounterValue, scyllaTable.maxDeltaVersionValue, counterName)
		}
		prefetchedValues.counterNameByPartition[partitionValue] = counterName
		prefetchedValues.counterValueByPartition[partitionValue] = coerceManagedIntegerValue(scyllaTable.UpdatedVersionCol, nextCounterValue)
	}

	return prefetchedValues, nil
}

func applyPrefetchedManagedCounterValues(
	records recordSlice, scyllaTable ScyllaTable, managedValues *managedWriteValues, prefetchedValues prefetchedManagedCounterValues,
) {
	if scyllaTable.UpdatedVersionCol == nil {
		return
	}

	partitionColumn := scyllaTable.GetPartKey()
	for recordIndex := range records.len() {
		recordPointer := records.at(recordIndex)
		partitionValue := int64(0)
		if partitionColumn != nil && !partitionColumn.IsNil() {
			partitionValue = convertToInt64(partitionColumn.GetRawValue(recordPointer))
		}

		counterValue := prefetchedValues.counterValueByPartition[partitionValue]
		managedValues.updatedVersionValues[recordIndex] = counterValue
		scyllaTable.UpdatedVersionCol.SetValue(recordPointer, counterValue)
	}

	if DebugFull {
		for partitionValue, counterName := range prefetchedValues.counterNameByPartition {
			fmt.Printf("Write updated_version assigned: table=%s partition=%d column=%s counter=%s records=%d\n",
				scyllaTable.Name, partitionValue, scyllaTable.UpdatedVersionCol.GetName(), counterName, records.len())
		}
	}
}

func applyWriteManagedColumnsWithPrefetchedCounters(
	records recordSlice, scyllaTable ScyllaTable, isInsert bool, prefetchedValues *prefetchedManagedCounterValues,
) (managedWriteValues, error) {
	managedValues := managedWriteValues{}
	if records.len() == 0 {
		return managedValues, nil
	}

	managedValues.createdValues = make([]any, records.len())
	managedValues.updatedValues = make([]any, records.len())
	managedValues.updatedVersionValues = make([]any, records.len())
	currentWriteTime := int64(getManagedUnixTime())

	for recordIndex := range records.len() {
		recordPointer := records.at(recordIndex)

		if isInsert && scyllaTable.CreatedCol != nil {
			createdValue := resolveManagedTimestampValue(scyllaTable.CreatedCol, recordPointer, currentWriteTime)
			managedValues.createdValues[recordIndex] = createdValue
			scyllaTable.CreatedCol.SetValue(recordPointer, createdValue)
		}

		if scyllaTable.UpdatedCol != nil {
			updatedValue := resolveManagedTimestampValue(scyllaTable.UpdatedCol, recordPointer, currentWriteTime)
			managedValues.updatedValues[recordIndex] = updatedValue
			scyllaTable.UpdatedCol.SetValue(recordPointer, updatedValue)
		}
	}

	if scyllaTable.UpdatedVersionCol != nil {
		valuesToApply := prefetchedManagedCounterValues{}
		if prefetchedValues == nil {
			fetchedValues, err := fetchManagedCounterValues(recordSliceGroup{records}, scyllaTable)
			if err != nil {
				return managedWriteValues{}, err
			}
			valuesToApply = fetchedValues
		} else {
			valuesToApply = *prefetchedValues
		}
		applyPrefetchedManagedCounterValues(records, scyllaTable, &managedValues, valuesToApply)
	}

	return managedValues, nil
}

func applyWriteManagedColumns(
	records recordSlice, scyllaTable ScyllaTable, isInsert bool,
) (managedWriteValues, error) {
	return applyWriteManagedColumnsWithPrefetchedCounters(records, scyllaTable, isInsert, nil)
}

func fetchAutoincrementCounterStarts(
	records recordSlice, scyllaTable ScyllaTable,
) (map[string]int64, error) {
	counterStartByGroup := map[string]int64{}
	if scyllaTable.AutoincrementCol == nil {
		return counterStartByGroup, nil
	}

	partitionColumn := scyllaTable.GetPartKey()
	groups := map[string][]unsafe.Pointer{}

	for i := range records.len() {
		ptr := records.at(i)

		partitionValue := int32(0)
		if partitionColumn != nil {
			partitionValue = int32(convertToInt64(partitionColumn.GetRawValue(ptr)))
		}

		autoPartVal := int64(0)
		if scyllaTable.AutoincrementPart != nil {
			autoPartVal = convertToInt64(scyllaTable.AutoincrementPart.GetRawValue(ptr))
		}

		key := fmt.Sprintf("%d|%v", partitionValue, autoPartVal)
		groups[key] = append(groups[key], ptr)
	}

	for groupKey, group := range groups {
		partValues := strings.Split(groupKey, "|")
		recordsNeedingAutoincrement := 0
		for _, ptr := range group {
			rawAutoincrementValue := scyllaTable.AutoincrementCol.GetRawValue(ptr)
			if convertToInt64(rawAutoincrementValue) <= 0 {
				recordsNeedingAutoincrement++
			}
		}
		if recordsNeedingAutoincrement == 0 {
			continue
		}

		counterName := fmt.Sprintf("x%v_%v_%v", partValues[0], scyllaTable.Name, partValues[1])
		keyspace := strings.Split(scyllaTable.GetFullName(), ".")[0]
		counterStartValue, err := GetCounter(keyspace, counterName, recordsNeedingAutoincrement)
		if err != nil {
			return nil, err
		}
		// GetCounter reserves the whole batch and returns the first ID in that reserved range.
		counterStartByGroup[groupKey] = counterStartValue
	}

	return counterStartByGroup, nil
}

func handlePreInsert(
	records recordSlice, scyllaTable ScyllaTable, counterStartByGroup map[string]int64,
) error {
	partitionColumn := scyllaTable.GetPartKey()
	// Group records by composite key: partition value + autoincrementPart value
	groups := map[string][]unsafe.Pointer{}

	for i := range records.len() {
		ptr := records.at(i)

		// Get partition key value
		partitionValue := int32(0)
		if partitionColumn != nil {
			partitionValue = int32(convertToInt64(partitionColumn.GetRawValue(ptr)))
		}

		// Get autoincrement part value (0 if not defined)
		autoPartVal := int64(0)
		if scyllaTable.AutoincrementPart != nil {
			autoPartVal = convertToInt64(scyllaTable.AutoincrementPart.GetRawValue(ptr))
		}

		// Group key is concatenation of partition value + autoincrementPart value
		key := fmt.Sprintf("%d|%v", partitionValue, autoPartVal)
		groups[key] = append(groups[key], ptr)
	}

	for partKey, group := range groups {
		// Filter records that need autoincrement.
		// Rule: autoincrement applies when the configured autoincrement column is <= 0.
		recordsNeedingAutoincrement := []unsafe.Pointer{}
		recordNeedsAutoincrement := map[unsafe.Pointer]bool{}

		for _, ptr := range group {
			autoincrementColumnValue := int64(0)
			if scyllaTable.AutoincrementCol != nil {
				rawAutoincrementValue := scyllaTable.AutoincrementCol.GetRawValue(ptr)
				autoincrementColumnValue = convertToInt64(rawAutoincrementValue)
			}

			if autoincrementColumnValue <= 0 {
				recordsNeedingAutoincrement = append(recordsNeedingAutoincrement, ptr)
				recordNeedsAutoincrement[ptr] = true
			}
		}

		counterVal := counterStartByGroup[partKey]

		for _, ptr := range group {
			var currentAutoVal int64

			// Only apply autoincrement when this record was marked as needing it (<= 0 rule).
			if scyllaTable.AutoincrementCol != nil && recordNeedsAutoincrement[ptr] {
				currentAutoVal = counterVal
				counterVal++

				colInfo := scyllaTable.AutoincrementCol.(*columnInfo)
				if colInfo.AutoincrementRandDigits > 0 {
					suffix := GetRandomInt64(colInfo.AutoincrementRandDigits)
					currentAutoVal = currentAutoVal*Pow10Int64(int64(colInfo.AutoincrementRandDigits)) + suffix
				}

				// If not packing, set directly
				if len(scyllaTable.keyIntPacking) == 0 {
					scyllaTable.AutoincrementCol.SetValue(ptr, currentAutoVal)
				}
			}

			if len(scyllaTable.keyIntPacking) > 0 {
				var packedValue int64
				remainingDigits := int64(19)
				for i, col := range scyllaTable.keyIntPacking {
					if col == nil {
						continue
					}
					var val int64
					if col == scyllaTable.AutoincrementCol {
						val = currentAutoVal
					} else {
						val = convertToInt64(col.GetRawValue(ptr))
					}

					colPackingInfo := col.(*columnInfo)
					decSize := int64(colPackingInfo.DecimalDigits)
					// If it's the last one and size is 0, it takes all remaining space
					if i == len(scyllaTable.keyIntPacking)-1 && decSize == 0 {
						decSize = remainingDigits
					}

					remainingDigits -= decSize
					if remainingDigits < 0 {
						remainingDigits = 0
					}

					shift := Pow10Int64(remainingDigits)
					packedValue += val * shift
				}
				// Set into the only key
				scyllaTable.Keys[0].SetValue(ptr, packedValue)
			}
		}
	}

	return nil
}

// PreparedStatement is the (template, bound-values) pair that gocql consumes for one query.
// Returned by MakeInsertStatement and MakeUpdateStatements so tests can inspect the exact
// statement shape and arguments that the production write path produces.
type PreparedStatement struct {
	Stmt string
	Args []any
}

// MakeInsertStatement returns the prepared INSERT statement and bound values that the
// production batch path would emit for each record. Intended for tests and debugging — the
// production path builds the same shape directly into a gocql.Batch via appendInsertQueriesToBatch.
func MakeInsertStatement[T TableBaseInterface[E, T], E TableSchemaInterface[E]](records *[]T, columnsToExclude ...Coln) []PreparedStatement {
	refTable := db.InitStructTable[E, T](new(E))
	scyllaTable := getOrCompileScyllaTable(refTable)
	managedValues, err := applyWriteManagedColumns(makeRecordSlice(records), scyllaTable, true)
	if err != nil {
		panic(err)
	}

	columns := collectInsertColumns(&scyllaTable, columnsToExclude)

	columnNames := make([]string, 0, len(columns))
	placeholders := make([]string, 0, len(columns))
	for _, col := range columns {
		columnNames = append(columnNames, col.GetName())
		placeholders = append(placeholders, "?")
	}

	stmt := fmt.Sprintf(`INSERT INTO %v (%v) VALUES (%v)`,
		scyllaTable.GetFullName(), strings.Join(columnNames, ", "), strings.Join(placeholders, ", "))

	entries := make([]PreparedStatement, 0, len(*records))
	for i := range *records {
		rec := &(*records)[i]
		ptr := xunsafe.AsPointer(rec)

		args := make([]any, 0, len(columns))
		for _, col := range columns {
			var value any
			if managedValue, found := managedValues.getValueForColumn(i, col, true); found {
				value = managedValue
			} else if slices.Contains(scyllaTable.KeysIdx, col.GetInfo().Idx) {
				value = col.GetStatementValue(ptr)
			} else {
				value = getNormalizedWriteValue(col, ptr)
			}
			args = append(args, value)
		}
		entries = append(entries, PreparedStatement{Stmt: stmt, Args: args})
	}
	return entries
}

func normalizeEmptyStringWriteValue(value any) any {
	// Rationale: Scylla should persist empty strings as NULL on write operations to keep storage semantics consistent.
	switch typedValue := value.(type) {
	case string:
		if typedValue == "" {
			return nil
		}
	case *string:
		if typedValue == nil || *typedValue == "" {
			return nil
		}
	}

	return value
}

func getNormalizedWriteValue(column IColInfo, ptr unsafe.Pointer) any {
	// Rationale: prepared statements must receive typed Go values instead of serialized CQL literals.
	writeValue := column.GetStatementValue(ptr)
	if writeValue == nil {
		writeValue = column.GetRawValue(ptr)
	}
	return normalizeEmptyStringWriteValue(writeValue)
}

func collectInsertColumns(scyllaTable *ScyllaTable, columnsToExclude []Coln) []IColInfo {
	if len(columnsToExclude) == 0 {
		return scyllaTable.Columns
	}

	columnNamesToExclude := []string{}
	for _, columnToExclude := range columnsToExclude {
		columnNamesToExclude = append(columnNamesToExclude, columnToExclude.GetInfo().Name)
	}

	columnsToInsert := []IColInfo{}
	for _, column := range scyllaTable.Columns {
		mustIncludeManagedColumn := column.GetName() == managedCreatedColumnName ||
			column.GetName() == managedUpdatedColumnName ||
			column.GetName() == managedUpdatedVersionColumnName
		if mustIncludeManagedColumn || !slices.Contains(columnNamesToExclude, column.GetName()) {
			columnsToInsert = append(columnsToInsert, column)
		}
	}

	return columnsToInsert
}

func makeInsertBatch[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T, scyllaTable ScyllaTable, managedValues managedWriteValues, columnsToExclude ...Coln,
) *gocql.Batch {
	columns := collectInsertColumns(&scyllaTable, columnsToExclude)

	columnsNames := []string{}
	columnPlaceholders := []string{}
	for _, col := range columns {
		columnsNames = append(columnsNames, col.GetName())
		columnPlaceholders = append(columnPlaceholders, "?")
	}

	session := getScyllaConnection()
	batch := session.NewBatch(gocql.UnloggedBatch)

	queryStrInsert := fmt.Sprintf(`INSERT INTO %v (%v) VALUES (%v)`,
		scyllaTable.GetFullName(), strings.Join(columnsNames, ", "), strings.Join(columnPlaceholders, ", "))

	appendInsertQueriesToBatch(batch, queryStrInsert, makeRecordSlice(records), columns, scyllaTable.KeysIdx, managedValues)
	return batch
}

func appendInsertQueriesToBatch(
	batch *gocql.Batch, queryStrInsert string, records recordSlice, columns []IColInfo, keysIdx []int16, managedValues managedWriteValues,
) {
	for i := range records.len() {
		ptr := records.at(i)
		values := []any{}

		for _, col := range columns {
			var value any
			if managedValue, found := managedValues.getValueForColumn(i, col, true); found {
				value = managedValue
			} else if slices.Contains(keysIdx, col.GetInfo().Idx) {
				// Key columns must never be coerced to null — keep empty string as-is.
				value = col.GetStatementValue(ptr)
			} else {
				value = getNormalizedWriteValue(col, ptr)
			}
			values = append(values, value)
		}

		batch.Query(queryStrInsert, values...)
	}
}

func collectAllWritableColumns(scyllaTable *ScyllaTable) []IColInfo {
	affectedColumns := []IColInfo{}
	for _, column := range scyllaTable.Columns {
		if column.GetInfo().IsVirtual {
			continue
		}
		affectedColumns = append(affectedColumns, column)
	}
	return affectedColumns
}

func collectAffectedColumnsForInsert(scyllaTable *ScyllaTable, columnsToExclude []Coln) []IColInfo {
	affectedColumns := []IColInfo{}
	for _, column := range collectInsertColumns(scyllaTable, columnsToExclude) {
		if column.GetInfo().IsVirtual {
			continue
		}
		affectedColumns = append(affectedColumns, column)
	}
	return affectedColumns
}

func collectAffectedColumnsForInclude(scyllaTable *ScyllaTable, columnsToInclude []Coln) []IColInfo {
	affectedColumns := []IColInfo{}
	for _, columnToInclude := range columnsToInclude {
		column := scyllaTable.ColumnsMap[columnToInclude.GetName()]
		if column == nil || column.IsNil() || column.GetInfo().IsVirtual {
			continue
		}
		affectedColumns = append(affectedColumns, column)
	}
	if scyllaTable.UpdatedCol != nil {
		updatedAlreadyIncluded := false
		for _, affectedColumn := range affectedColumns {
			if affectedColumn.GetName() == scyllaTable.UpdatedCol.GetName() {
				updatedAlreadyIncluded = true
				break
			}
		}
		if !updatedAlreadyIncluded {
			affectedColumns = append(affectedColumns, scyllaTable.UpdatedCol)
		}
	}
	return affectedColumns
}

func collectAffectedColumnsForExclude(scyllaTable *ScyllaTable, columnsToExclude []Coln) []IColInfo {
	excludedColumnNames := map[string]bool{}
	for _, columnToExclude := range columnsToExclude {
		excludedColumnNames[columnToExclude.GetName()] = true
	}

	affectedColumns := []IColInfo{}
	for _, column := range scyllaTable.Columns {
		mustIncludeUpdated := scyllaTable.UpdatedCol != nil && column.GetName() == scyllaTable.UpdatedCol.GetName()
		if column.GetInfo().IsVirtual || (!mustIncludeUpdated && excludedColumnNames[column.GetName()]) {
			continue
		}
		affectedColumns = append(affectedColumns, column)
	}
	return affectedColumns
}

func hasUsableIndexSourceValue(rawValue any) bool {
	if rawValue == nil {
		return false
	}

	valueRef := reflect.ValueOf(rawValue)
	for valueRef.Kind() == reflect.Pointer {
		if valueRef.IsNil() {
			return false
		}
		valueRef = valueRef.Elem()
	}

	switch valueRef.Kind() {
	case reflect.String:
		return valueRef.Len() > 0
	case reflect.Slice, reflect.Array:
		return valueRef.Len() > 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return valueRef.Int() != 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return valueRef.Uint() != 0
	case reflect.Float32, reflect.Float64:
		return valueRef.Float() != 0
	case reflect.Bool:
		return valueRef.Bool()
	default:
		return true
	}
}

func syncTableBackedViews(
	records recordSlice, scyllaTable *ScyllaTable, affectedColumns []IColInfo,
) error {
	if records.len() == 0 || !scyllaTable.hasTableBackedViews {
		return nil
	}

	session := getScyllaConnection()
	partColumn := scyllaTable.GetPartKey()
	if partColumn == nil || partColumn.IsNil() {
		return nil
	}

	for _, view := range scyllaTable.views {
		if view.Type != 9 {
			continue
		}
		if len(affectedColumns) > 0 {
			shouldSyncCurrentView := false
			for _, affectedColumn := range affectedColumns {
				if view.rebuildColumnNames[affectedColumn.GetName()] {
					shouldSyncCurrentView = true
					break
				}
			}
			if !shouldSyncCurrentView {
				continue
			}
		}

		for start := 0; start < records.len(); start += maxInsertBatchRows {
			end := min(start+maxInsertBatchRows, records.len())

			recordsChunk := records.sub(start, end)
			if err := executeViewTableSyncChunk(view, recordsChunk.len(), recordsChunk.at, session, scyllaTable); err != nil {
				return err
			}
		}
	}

	return nil
}

func Insert[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T, columnsToExclude ...Coln,
) error {
	recordsForUpdate := []T{}
	return InsertUpdateBase(records, &recordsForUpdate, nil, columnsToExclude...)
}

func InsertOne[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	record T, columnsToExclude ...Coln,
) error {
	return Insert(&[]T{record}, columnsToExclude...)
}

// MakeUpdateStatements returns the prepared UPDATE statement and bound values that the
// production batch path would emit for each record. Intended for tests and debugging — the
// production path builds the same shape directly into a gocql.Batch via appendUpdateQueriesToBatch.
func MakeUpdateStatements[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T, columnsToInclude ...Coln,
) []PreparedStatement {
	scyllaTable := MakeScyllaTable[T, E]()
	recordsView := makeRecordSlice(records)
	managedValues, err := applyWriteManagedColumns(recordsView, scyllaTable, false)
	if err != nil {
		panic(err)
	}

	resolvedTable, columnsToUpdate := resolveUpdateColumnsForWrite(scyllaTable, recordsView.len(),
		recordsView.at, columnsToInclude, nil, false)
	columnsWhere := collectUpdateWhereColumns(resolvedTable)

	setParts := make([]string, 0, len(columnsToUpdate))
	for _, col := range columnsToUpdate {
		setParts = append(setParts, fmt.Sprintf("%v = ?", col.GetName()))
	}
	whereParts := make([]string, 0, len(columnsWhere))
	for _, whereCol := range columnsWhere {
		whereParts = append(whereParts, fmt.Sprintf("%v = ?", whereCol.GetName()))
	}

	stmt := fmt.Sprintf("UPDATE %v SET %v WHERE %v",
		resolvedTable.GetFullName(),
		strings.Join(setParts, ", "),
		strings.Join(whereParts, " and "),
	)

	entries := make([]PreparedStatement, 0, len(*records))
	for i := range *records {
		rec := &(*records)[i]
		ptr := xunsafe.AsPointer(rec)

		args := make([]any, 0, len(columnsToUpdate)+len(columnsWhere))
		for _, col := range columnsToUpdate {
			var value any
			if managedValue, found := managedValues.getValueForColumn(i, col, false); found {
				value = managedValue
			} else {
				value = getNormalizedWriteValue(col, ptr)
			}
			args = append(args, value)
		}
		for _, whereCol := range columnsWhere {
			args = append(args, whereCol.GetStatementValue(ptr))
		}
		entries = append(entries, PreparedStatement{Stmt: stmt, Args: args})
	}
	return entries
}

func Update[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T, columnsToInclude ...Coln,
) error {
	if len(columnsToInclude) == 0 {
		panic("No se incluyeron columnas a actualizar.")
	}

	recordsForInsert := []T{}
	return InsertUpdateBase(&recordsForInsert, records, columnsToInclude)
}

func UpdateOne[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	record T, columnsToInclude ...Coln,
) error {
	return Update(&[]T{record}, columnsToInclude...)
}

func UpdateExclude[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T, columnsToExclude ...Coln,
) error {
	if records == nil || len(*records) == 0 {
		return nil
	}
	// Route through the shared insert/update batch path so writes go out as prepared statements,
	// avoiding the raw-CQL string concatenation that previously broke on values containing single quotes.
	recordsForInsert := []T{}
	return executeInsertUpdateBatch(insertUpdateBatchParams[T, E]{
		recordsForInsert: &recordsForInsert,
		recordsForUpdate: records,
		columnsToUpdate:  columnsToExclude,
	})
}

func mergeManagedWriteValues(first managedWriteValues, second managedWriteValues) managedWriteValues {
	return managedWriteValues{
		createdValues:        append(append([]any{}, first.createdValues...), second.createdValues...),
		updatedValues:        append(append([]any{}, first.updatedValues...), second.updatedValues...),
		updatedVersionValues: append(append([]any{}, first.updatedVersionValues...), second.updatedVersionValues...),
	}
}

func InsertUpdateBase[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	recordsForInsert *[]T,
	recordsForUpdate *[]T,
	columnsToIncludeUpdate []Coln,
	columnsToExcludeInsert ...Coln,
) error {
	return executeInsertUpdateBatch(insertUpdateBatchParams[T, E]{
		recordsForInsert:        recordsForInsert,
		recordsForUpdate:        recordsForUpdate,
		useIncludeUpdateColumns: true,
		columnsToUpdate:         columnsToIncludeUpdate,
		columnsToExcludeInsert:  columnsToExcludeInsert,
	})
}

func splitInsertAndUpdateRecords[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T, isInsert func(e *T) bool,
) ([]T, []T) {
	if isInsert == nil {
		panic("Se requiere la función isInsert para clasificar los registros.")
	}

	recordsForInsert := make([]T, 0, len(*records))
	recordsForUpdate := make([]T, 0, len(*records))

	// Classify the mixed payload once so the combined batch can reuse the shared insert/update flow.
	for recordIndex := range *records {
		record := &(*records)[recordIndex]
		if isInsert(record) {
			recordsForInsert = append(recordsForInsert, *record)
		} else {
			recordsForUpdate = append(recordsForUpdate, *record)
		}
	}

	return recordsForInsert, recordsForUpdate
}

type insertUpdateBatchParams[T TableBaseInterface[E, T], E TableSchemaInterface[E]] struct {
	recordsForInsert        *[]T
	recordsForUpdate        *[]T
	useIncludeUpdateColumns bool
	columnsToUpdate         []Coln
	columnsToExcludeInsert  []Coln
	// skipTextSearchRecordsIDs lists update-record IDs whose text-search source (and status)
	// did not change, so the update-phase re-index can skip re-upserting them into GenixSearch.
	// The caller (e.g. Merge) already holds the previous record and computes this, avoiding a
	// per-record comparison inside the write path.
	skipTextSearchRecordsIDs []int64
}

// executeInsertUpdateBatch is the generic doorway to the write path. It does the three things
// that genuinely need the record and table types -- compile the table, run SelfParse through the
// record's own method set, and take the address of each slice -- and hands everything else to the
// shared body below. Splitting it this way is what stops one write engine from being compiled once
// per table; see record_slice.go.
func executeInsertUpdateBatch[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	params insertUpdateBatchParams[T, E],
) error {
	recordsForInsert := params.recordsForInsert
	recordsForUpdate := params.recordsForUpdate
	if recordsForInsert == nil {
		recordsForInsert = &[]T{}
	}
	if recordsForUpdate == nil {
		recordsForUpdate = &[]T{}
	}
	if len(*recordsForInsert) == 0 && len(*recordsForUpdate) == 0 {
		return nil
	}

	insertRecords := makeRecordSlice(recordsForInsert)
	updateRecords := makeRecordSlice(recordsForUpdate)
	// SelfParse runs before anything reads a column, and it mutates records in place without
	// reallocating, so the views taken above stay valid.
	runSelfParseIfDefined(insertRecords)
	runSelfParseIfDefined(updateRecords)

	return executeInsertUpdateBatchErased(erasedInsertUpdateParams{
		insertRecords:            insertRecords,
		updateRecords:            updateRecords,
		compiledTable:            getOrCompileScyllaTable(db.InitStructTable[E, T](new(E))),
		useIncludeUpdateColumns:  params.useIncludeUpdateColumns,
		columnsToUpdate:          params.columnsToUpdate,
		columnsToExcludeInsert:   params.columnsToExcludeInsert,
		skipTextSearchRecordsIDs: params.skipTextSearchRecordsIDs,
	})
}

// erasedInsertUpdateParams is insertUpdateBatchParams with the record type resolved away.
type erasedInsertUpdateParams struct {
	insertRecords            recordSlice
	updateRecords            recordSlice
	compiledTable            ScyllaTable
	useIncludeUpdateColumns  bool
	columnsToUpdate          []Coln
	columnsToExcludeInsert   []Coln
	skipTextSearchRecordsIDs []int64
}

func executeInsertUpdateBatchErased(params erasedInsertUpdateParams) error {
	insertRecords := params.insertRecords
	updateRecords := params.updateRecords
	scyllaTable := params.compiledTable
	useIncludeUpdateColumns := params.useIncludeUpdateColumns
	columnsToUpdate := params.columnsToUpdate
	columnsToExcludeInsert := params.columnsToExcludeInsert

	skipTextSearchIDs := map[int64]bool(nil)
	if len(params.skipTextSearchRecordsIDs) > 0 {
		skipTextSearchIDs = make(map[int64]bool, len(params.skipTextSearchRecordsIDs))
		for _, id := range params.skipTextSearchRecordsIDs {
			skipTextSearchIDs[id] = true
		}
	}

	if useIncludeUpdateColumns && updateRecords.len() > 0 && len(columnsToUpdate) == 0 {
		panic("No se incluyeron columnas a actualizar.")
	}

	// A view over both slices rather than slices.Concat: the marker syncs only walk the records,
	// so copying every one of them to do it was pure waste -- and the copy had to be rebuilt
	// after the write phase, because the first one held stale pre-mutation records.
	allWrittenRecords := recordSliceGroup{insertRecords, updateRecords}
	prefetchedManagedCounters := prefetchedManagedCounterValues{}
	autoincrementCounterStarts := map[string]int64{}
	var fetchGroup errgroup.Group

	if scyllaTable.UpdatedVersionCol != nil {
		fetchGroup.Go(func() error {
			values, err := fetchManagedCounterValues(allWrittenRecords, scyllaTable)
			if err != nil {
				return err
			}
			prefetchedManagedCounters = values
			return nil
		})
	}
	if scyllaTable.AutoincrementCol != nil && insertRecords.len() > 0 {
		fetchGroup.Go(func() error {
			values, err := fetchAutoincrementCounterStarts(insertRecords, scyllaTable)
			if err != nil {
				return err
			}
			autoincrementCounterStarts = values
			return nil
		})
	}

	if err := fetchGroup.Wait(); err != nil {
		return err
	}

	managedInsertValues, err := applyWriteManagedColumnsWithPrefetchedCounters(insertRecords, scyllaTable, true, &prefetchedManagedCounters)
	if err != nil {
		return err
	}
	managedUpdateValues, err := applyWriteManagedColumnsWithPrefetchedCounters(updateRecords, scyllaTable, false, &prefetchedManagedCounters)
	if err != nil {
		return err
	}

	if insertRecords.len() > 0 {
		if err := handlePreInsert(insertRecords, scyllaTable, autoincrementCounterStarts); err != nil {
			return err
		}
	}

	session := getScyllaConnection()

	// Writes are sent in chunks of maxInsertBatchRows statements per gocql.Batch. A single
	// batch holding tens of thousands of statements overruns ScyllaDB's batch limits and
	// triggers a server-side connection reset, so inserts and updates are each chunked here.
	if insertRecords.len() > 0 {
		insertColumns := collectInsertColumns(&scyllaTable, columnsToExcludeInsert)
		insertColumnNames := []string{}
		insertColumnPlaceholders := []string{}
		for _, insertColumn := range insertColumns {
			insertColumnNames = append(insertColumnNames, insertColumn.GetName())
			insertColumnPlaceholders = append(insertColumnPlaceholders, "?")
		}
		insertQueryStatement := fmt.Sprintf(`INSERT INTO %v (%v) VALUES (%v)`,
			scyllaTable.GetFullName(), strings.Join(insertColumnNames, ", "), strings.Join(insertColumnPlaceholders, ", "))

		for start := 0; start < insertRecords.len(); start += maxInsertBatchRows {
			end := min(start+maxInsertBatchRows, insertRecords.len())
			chunk := insertRecords.sub(start, end)
			queryBatch := session.NewBatch(gocql.UnloggedBatch)
			appendInsertQueriesToBatch(queryBatch, insertQueryStatement, chunk, insertColumns, scyllaTable.KeysIdx, managedInsertValues.slice(start, end))
			if DebugFull {
				fmt.Printf("InsertUpdate insert batch write: table=%s rows=%d statements=%d chunk=%d-%d total=%d\n",
					scyllaTable.GetFullName(), chunk.len(), len(queryBatch.Entries), start, end, insertRecords.len())
			}
			if err := session.ExecuteBatch(queryBatch); err != nil {
				fmt.Printf("Error executing insert batch: table=%s rows=%d chunk=%d-%d err=%v\n",
					scyllaTable.GetFullName(), chunk.len(), start, end, err)
				return err
			}
		}
	}

	if updateRecords.len() > 0 {
		updateColumnsToInclude := []Coln(nil)
		updateColumnsToExclude := []Coln(nil)
		if useIncludeUpdateColumns {
			updateColumnsToInclude = columnsToUpdate
		} else {
			updateColumnsToExclude = columnsToUpdate
		}

		for start := 0; start < updateRecords.len(); start += maxInsertBatchRows {
			end := min(start+maxInsertBatchRows, updateRecords.len())
			chunk := updateRecords.sub(start, end)
			queryBatch := session.NewBatch(gocql.UnloggedBatch)
			appendUpdateQueriesToBatch(queryBatch, scyllaTable, chunk, managedUpdateValues.slice(start, end), updateColumnsToInclude, updateColumnsToExclude, false)
			if DebugFull {
				fmt.Printf("InsertUpdate update batch write: table=%s rows=%d statements=%d chunk=%d-%d total=%d\n",
					scyllaTable.GetFullName(), chunk.len(), len(queryBatch.Entries), start, end, updateRecords.len())
			}
			if err := session.ExecuteBatch(queryBatch); err != nil {
				fmt.Printf("Error executing update batch: table=%s rows=%d chunk=%d-%d err=%v\n",
					scyllaTable.GetFullName(), chunk.len(), start, end, err)
				return err
			}
		}
	}

	combinedManagedValues := mergeManagedWriteValues(managedInsertValues, managedUpdateValues)

	// The update-phase column set drives both the view sync and the text-search sync, so it is
	// resolved once here instead of being recomputed inside each block.
	updateAffectedColumns := []IColInfo{}
	if updateRecords.len() > 0 {
		if useIncludeUpdateColumns {
			updateAffectedColumns = collectAffectedColumnsForInclude(&scyllaTable, columnsToUpdate)
		} else {
			updateAffectedColumns = collectAffectedColumnsForExclude(&scyllaTable, columnsToUpdate)
		}
	}

	// The post-write syncs run in two parallel phases, separated by a barrier.
	//
	// Phase 1 writes the derived DATA (view tables, search index). Phase 2 writes the FRESHNESS
	// MARKERS the delta cache reads to decide whether to refetch (__index_updated counters and
	// cache_updated_version slots). The barrier is what makes the protocol safe: publishing a
	// marker before the data it gates lets a client refetch a stale row and then record the new
	// version, permanently missing the update.
	//
	// Within a phase the syncs target distinct tables and only ever read the record slices, so
	// they are independent. Each sync KIND keeps its own insert/update phases sequential inside
	// one goroutine — the view sync is a read-modify-write cycle over a shared view table and
	// text-search upserts can land in the same bucket, so overlapping them with themselves would
	// add a hazard for no gain.
	var dataSyncGroup errgroup.Group

	if scyllaTable.hasTableBackedViews {
		dataSyncGroup.Go(func() error {
			if insertRecords.len() > 0 {
				insertAffectedColumns := collectAffectedColumnsForInsert(&scyllaTable, columnsToExcludeInsert)
				if err := syncTableBackedViews(insertRecords, &scyllaTable, insertAffectedColumns); err != nil {
					return fmt.Errorf("syncing view tables after insert phase: %w", err)
				}
			}
			if updateRecords.len() > 0 {
				if err := syncTableBackedViews(updateRecords, &scyllaTable, updateAffectedColumns); err != nil {
					return fmt.Errorf("syncing view tables after update phase: %w", err)
				}
			}
			return nil
		})
	}

	if scyllaTable.textSearchIndex != nil {
		dataSyncGroup.Go(func() error {
			if insertRecords.len() > 0 {
				if err := syncTextSearchIndexAfterWrite(insertRecords, &scyllaTable, false, nil); err != nil {
					return fmt.Errorf("syncing text search index after insert phase: %w", err)
				}
			}
			if updateRecords.len() > 0 {
				textChanged, statusChanged := textSearchAffectedColumns(&scyllaTable, updateAffectedColumns)
				if textChanged {
					if err := syncTextSearchIndexAfterWrite(updateRecords, &scyllaTable, !statusChanged, skipTextSearchIDs); err != nil {
						return fmt.Errorf("syncing text search index after update phase: %w", err)
					}
				} else if statusChanged {
					if err := syncTextSearchStatusAfterWrite(updateRecords, &scyllaTable); err != nil {
						return fmt.Errorf("syncing text search status after update phase: %w", err)
					}
				}
			}
			return nil
		})
	}

	if err := dataSyncGroup.Wait(); err != nil {
		fmt.Println("Error syncing derived data after insert-update:", err)
		return err
	}

	var markerSyncGroup errgroup.Group

	markerSyncGroup.Go(func() error {
		if err := syncIndexGroupsAfterWrite(allWrittenRecords, &scyllaTable, combinedManagedValues); err != nil {
			return fmt.Errorf("syncing index groups: %w", err)
		}
		return nil
	})

	// Combined insert/update writes bump each touched slot once for the merged mutation set.
	markerSyncGroup.Go(func() error {
		if err := updateSlotVersionsAfterWrite(allWrittenRecords, scyllaTable); err != nil {
			return fmt.Errorf("updating slot versions: %w", err)
		}
		return nil
	})

	if err := markerSyncGroup.Wait(); err != nil {
		fmt.Println("Error syncing cache markers after insert-update:", err)
		return err
	}

	return nil
}

func InsertUpdate[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	recordsForInsert *[]T,
	recordsForUpdate *[]T,
	columnsToIncludeUpdate []Coln,
	columnsToExcludeInsert ...Coln,
) error {
	return executeInsertUpdateBatch(insertUpdateBatchParams[T, E]{
		recordsForInsert:        recordsForInsert,
		recordsForUpdate:        recordsForUpdate,
		useIncludeUpdateColumns: true,
		columnsToUpdate:         columnsToIncludeUpdate,
		columnsToExcludeInsert:  columnsToExcludeInsert,
	})
}

func InsertUpdateInclude[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T,
	isInsert func(e *T) bool,
	columnsToIncludeUpdate []Coln,
	columnsToExcludeInsert ...Coln,
) error {
	recordsForInsert, recordsForUpdate := splitInsertAndUpdateRecords(records, isInsert)
	return executeInsertUpdateBatch(insertUpdateBatchParams[T, E]{
		recordsForInsert:        &recordsForInsert,
		recordsForUpdate:        &recordsForUpdate,
		useIncludeUpdateColumns: true,
		columnsToUpdate:         columnsToIncludeUpdate,
		columnsToExcludeInsert:  columnsToExcludeInsert,
	})
}

func InsertUpdateExclude[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T,
	isInsert func(e *T) bool,
	columnsToExcludeUpdate []Coln,
	columnsToExcludeInsert ...Coln,
) error {
	recordsForInsert, recordsForUpdate := splitInsertAndUpdateRecords(records, isInsert)
	return executeInsertUpdateBatch(insertUpdateBatchParams[T, E]{
		recordsForInsert:       &recordsForInsert,
		recordsForUpdate:       &recordsForUpdate,
		columnsToUpdate:        columnsToExcludeUpdate,
		columnsToExcludeInsert: columnsToExcludeInsert,
	})
}

// Non-generic on purpose: the body works on ScyllaTable and IColInfo, and reaches records only
// through unsafe.Pointer. A type parameter here stencilled all ~190 lines once per table type.
func resolveUpdateColumnsForWrite(
	scyllaTable ScyllaTable, recordCount int, recordPointerAt func(int) unsafe.Pointer,
	columnsToInclude []Coln, columnsToExclude []Coln, onlyVirtual bool,
) (ScyllaTable, []IColInfo) {
	columnsToUpdate := []IColInfo{}

	if len(columnsToInclude) > 0 {
		updatedAlreadyIncluded := scyllaTable.UpdatedCol == nil
		updatedVersionAlreadyIncluded := scyllaTable.UpdatedVersionCol == nil
		for _, col_ := range columnsToInclude {
			col := scyllaTable.ColumnsMap[col_.GetName()]
			if col == nil {
				Print(col)
				panic("No se encontró la columna (update):" + col_.GetName())
			}
			if slices.Contains(scyllaTable.KeysIdx, col.GetInfo().Idx) {
				msg := fmt.Sprintf(`Table "%v": The column "%v" can't be updated because is part of primary key.`, scyllaTable.Name, col.GetName())
				panic(msg)
			}
			columnsToUpdate = append(columnsToUpdate, col)
			if scyllaTable.UpdatedCol != nil && col.GetName() == scyllaTable.UpdatedCol.GetName() {
				updatedAlreadyIncluded = true
			}
			if scyllaTable.UpdatedVersionCol != nil && col.GetName() == scyllaTable.UpdatedVersionCol.GetName() {
				updatedVersionAlreadyIncluded = true
			}
		}
		if !updatedAlreadyIncluded {
			columnsToUpdate = append(columnsToUpdate, scyllaTable.UpdatedCol)
		}
		if !updatedVersionAlreadyIncluded {
			// The managed update counter must always persist, even when callers provide an explicit include list.
			columnsToUpdate = append(columnsToUpdate, scyllaTable.UpdatedVersionCol)
		}
	} else {
		columnsToExcludeNames := []string{}
		for _, c := range columnsToExclude {
			columnsToExcludeNames = append(columnsToExcludeNames, c.GetName())
		}
		for _, col := range scyllaTable.Columns {
			isExcluded := slices.Contains(columnsToExcludeNames, col.GetName())
			mustIncludeManagedColumn := (scyllaTable.UpdatedCol != nil && col.GetName() == scyllaTable.UpdatedCol.GetName()) ||
				(scyllaTable.UpdatedVersionCol != nil && col.GetName() == scyllaTable.UpdatedVersionCol.GetName())
			if !col.GetInfo().IsVirtual && (mustIncludeManagedColumn || !isExcluded) && !slices.Contains(scyllaTable.KeysIdx, col.GetInfo().Idx) {
				columnsToUpdate = append(columnsToUpdate, col)
			}
		}
	}

	columnsIdx := []int16{}
	for _, col := range columnsToUpdate {
		columnsIdx = append(columnsIdx, col.GetInfo().Idx)
	}
	columnsIncluded := slices.Concat(scyllaTable.KeysIdx, columnsIdx)
	pk := scyllaTable.GetPartKey()
	if pk != nil && !pk.IsNil() {
		columnsIncluded = append(columnsIncluded, pk.GetInfo().Idx)
	}

	// Revisa si hay columnas que deben actualizarse juntas para los índices calculados.
	for _, indexViews := range scyllaTable.indexViews {
		if indexViews.column.GetInfo().IsVirtual {
			if indexViews.Type == 3 {
				// Index groups own their own validation and virtual-column recomputation below.
				continue
			}
			includedCols := []int16{}
			notIncludedCols := []int16{}

			for _, colIdx := range indexViews.columnsIdx {
				if slices.Contains(columnsIncluded, colIdx) {
					includedCols = append(includedCols, colIdx)
				} else {
					notIncludedCols = append(notIncludedCols, colIdx)
				}
			}

			if len(includedCols) > 0 && len(notIncludedCols) > 0 {
				colnames := []string{}
				for _, colname := range indexViews.columns {
					if pk != nil && !pk.IsNil() && pk.GetName() == colname {
						continue
					}
					colnames = append(colnames, fmt.Sprintf(`"%v"`, colname))
				}

				includedColsNames := []string{}
				for _, idx := range notIncludedCols {
					includedColsNames = append(includedColsNames, scyllaTable.ColumnsIdxMap[idx].GetName())
				}

				msg := fmt.Sprintf(`Table "%v": A composit index/view requires the columns %v be updated together. Not Included: %v`, scyllaTable.Name, strings.Join(colnames, ", "), strings.Join(includedColsNames, ", "))
				panic(msg)
			} else if len(includedCols) > 0 {
				columnsToUpdate = append(columnsToUpdate, indexViews.column)
			}
		}
	}

	columnsToUpdateByName := map[string]IColInfo{}
	// Track already-selected columns to avoid duplicate SET clauses when adding virtual bucket columns.
	for _, col := range columnsToUpdate {
		columnsToUpdateByName[col.GetName()] = col
	}

	for _, compositeBucketIndex := range scyllaTable.compositeBucketIndexes {
		// Composite bucket hashes become inconsistent if only part of the source tuple is updated.
		includedCols := []string{}
		notIncludedCols := []string{}

		for _, sourceColumn := range compositeBucketIndex.sourceColumns {
			if slices.Contains(columnsIncluded, sourceColumn.GetInfo().Idx) {
				includedCols = append(includedCols, sourceColumn.GetName())
			} else {
				notIncludedCols = append(notIncludedCols, sourceColumn.GetName())
			}
		}

		if len(includedCols) > 0 && len(notIncludedCols) > 0 {
			panic(fmt.Sprintf(`Table "%v": CompositeBucketing index "%v" requires updating all source columns together. Included: %v | Missing: %v`,
				scyllaTable.Name,
				compositeBucketIndex.name,
				strings.Join(includedCols, ", "),
				strings.Join(notIncludedCols, ", "),
			))
		}

		if len(includedCols) > 0 {
			// Recompute all generated bucket columns whenever any source tuple is updated.
			for _, virtualColumn := range compositeBucketIndex.virtualColumnsBySize {
				if _, exists := columnsToUpdateByName[virtualColumn.GetName()]; !exists {
					columnsToUpdate = append(columnsToUpdate, virtualColumn)
					columnsToUpdateByName[virtualColumn.GetName()] = virtualColumn
				}
			}
		}
	}

	for _, indexGroup := range scyllaTable.indexGroups {
		includedSourceColumns := []string{}
		missingSourceColumns := []string{}

		for _, sourceColumn := range indexGroup.sourceColumns {
			if slices.Contains(columnsIncluded, sourceColumn.column.GetInfo().Idx) {
				includedSourceColumns = append(includedSourceColumns, sourceColumn.column.GetName())
			} else {
				missingSourceColumns = append(missingSourceColumns, sourceColumn.column.GetName())
			}
		}

		if len(includedSourceColumns) > 0 && len(missingSourceColumns) > 0 {
			for recordIndex := 0; recordIndex < recordCount; recordIndex++ {
				recordPointer := recordPointerAt(recordIndex)
				missingValuesInStruct := []string{}

				for _, sourceColumn := range indexGroup.sourceColumns {
					if slices.Contains(columnsIncluded, sourceColumn.column.GetInfo().Idx) {
						continue
					}
					if !hasUsableIndexSourceValue(sourceColumn.column.GetRawValue(recordPointer)) {
						missingValuesInStruct = append(missingValuesInStruct, sourceColumn.column.GetName())
					}
				}

				if len(missingValuesInStruct) > 0 {
					panic(fmt.Sprintf(`Table "%v": IndexGroup "%v" needs struct values for omitted source columns. Included in update: %v | Missing in struct: %v`,
						scyllaTable.Name,
						indexGroup.name,
						strings.Join(includedSourceColumns, ", "),
						strings.Join(missingValuesInStruct, ", "),
					))
				}
			}
		}

		if len(includedSourceColumns) > 0 && indexGroup.virtualColumn != nil && !indexGroup.virtualColumn.IsNil() {
			if _, exists := columnsToUpdateByName[indexGroup.virtualColumn.GetName()]; !exists {
				columnsToUpdate = append(columnsToUpdate, indexGroup.virtualColumn)
				columnsToUpdateByName[indexGroup.virtualColumn.GetName()] = indexGroup.virtualColumn
			}
		}
	}

	if onlyVirtual {
		cols := columnsToUpdate
		columnsToUpdate = nil
		for _, col := range cols {
			if col.GetInfo().IsVirtual {
				columnsToUpdate = append(columnsToUpdate, col)
			}
		}
	}

	return scyllaTable, columnsToUpdate
}

func collectUpdateWhereColumns(scyllaTable ScyllaTable) []IColInfo {
	columnsWhere := scyllaTable.Keys
	if partitionKey := scyllaTable.GetPartKey(); partitionKey != nil && !partitionKey.IsNil() {
		columnsWhere = append([]IColInfo{partitionKey}, columnsWhere...)
	}
	return columnsWhere
}

func appendUpdateQueriesToBatch(
	batch *gocql.Batch, compiledTable ScyllaTable, records recordSlice, managedValues managedWriteValues,
	columnsToInclude []Coln, columnsToExclude []Coln, onlyVirtual bool,
) {
	scyllaTable, columnsToUpdate := resolveUpdateColumnsForWrite(compiledTable, records.len(),
		records.at, columnsToInclude, columnsToExclude, onlyVirtual)
	columnsWhere := collectUpdateWhereColumns(scyllaTable)

	setStatements := []string{}
	for _, columnToUpdate := range columnsToUpdate {
		setStatements = append(setStatements, fmt.Sprintf(`%v = ?`, columnToUpdate.GetName()))
	}

	whereStatements := []string{}
	for _, whereColumn := range columnsWhere {
		whereStatements = append(whereStatements, fmt.Sprintf(`%v = ?`, whereColumn.GetName()))
	}

	queryStatement := fmt.Sprintf(
		"UPDATE %v SET %v WHERE %v",
		scyllaTable.GetFullName(), Concatx(", ", setStatements), Concatx(" and ", whereStatements),
	)

	for recordIndex := range records.len() {
		recordPointer := records.at(recordIndex)
		values := []any{}

		for _, columnToUpdate := range columnsToUpdate {
			value := any(nil)
			if managedValue, found := managedValues.getValueForColumn(recordIndex, columnToUpdate, false); found {
				value = managedValue
			} else {
				value = getNormalizedWriteValue(columnToUpdate, recordPointer)
			}
			values = append(values, value)
		}

		for _, whereColumn := range columnsWhere {
			values = append(values, whereColumn.GetStatementValue(recordPointer))
		}

		batch.Query(queryStatement, values...)
	}
}

func InsertOrUpdate[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T,
	isRecordForInsert func(e *T) bool,
	columnsToExcludeUpdate []Coln,
	columnsToExcludeInsert ...Coln,
) error {

	recordsToInsert := []T{}
	recordsToUpdate := []T{}

	for _, e := range *records {
		if isRecordForInsert(&e) {
			recordsToInsert = append(recordsToInsert, e)
		} else {
			recordsToUpdate = append(recordsToUpdate, e)
		}
	}

	if len(recordsToUpdate) > 0 {
		fmt.Println("Registros a actualizar:", len(recordsToUpdate))
		if err := UpdateExclude(&recordsToUpdate, columnsToExcludeUpdate...); err != nil {
			return err
		}
	}

	if len(recordsToInsert) > 0 {
		fmt.Println("Registros a insertar:", len(recordsToInsert))
		if err := Insert(&recordsToInsert, columnsToExcludeInsert...); err != nil {
			return err
		}
	}

	return nil
}

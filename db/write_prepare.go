package db

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"unsafe"
)

// Write preparation (docs/FOUNDATIONDB_PLAN.md §7.2): everything a write does to its records and
// its column set before any driver builds a statement or a mutation. Every driver calls these, so a
// write means the same on each: the same managed stamps, the same generated keys, the same columns
// written, and the same refusals.

// ManagedUnixTime is SUnixTime on the ORM's effective clock: Now() rather than time.Now() so a
// historical-clock override reaches the columns the ORM writes on its own.
func ManagedUnixTime() int32 {
	return int32((Now().Unix() - 1e9) / 2)
}

// ManagedStamp is what a write stamped on one record: created (inserts only; 0 when not stamped)
// and updated. A managed column with no record field is stored from here.
type ManagedStamp struct {
	Created int64
	Updated int64
}

// CoerceManagedInteger types a managed timestamp as its column's field, as a driver binds it.
func CoerceManagedInteger(column IColInfo, value int64) any {
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

// RunSelfParse runs the record type's SelfParse, when it declares one, on every record. It runs
// before anything reads a column and mutates the records in place.
func RunSelfParse(records RecordSlice) {
	type selfParser interface{ SelfParse() }
	if records.Len() == 0 {
		return
	}
	if _, hasSelfParse := records.RecordPointerAt(0).(selfParser); !hasSelfParse {
		return
	}
	for recordIndex := range records.Len() {
		records.RecordPointerAt(recordIndex).(selfParser).SelfParse()
	}
}

// StampManagedColumns stamps the audit columns: created on insert when the caller left it zero, and
// updated on every write. Both cache protocols read updated as the time of the write, so it is never
// kept from the caller.
func (core *TableCore) StampManagedColumns(records RecordSlice, isInsert bool, writeTime int64) []ManagedStamp {
	stamps := make([]ManagedStamp, records.Len())
	for recordIndex := range records.Len() {
		recordPointer := records.At(recordIndex)
		if isInsert && core.CreatedCol != nil {
			createdValue := writeTime
			if currentValue := core.CreatedCol.GetRawValue(recordPointer); currentValue != nil && ConvertToInt64(currentValue) > 0 {
				createdValue = ConvertToInt64(currentValue)
			}
			core.CreatedCol.SetValue(recordPointer, CoerceManagedInteger(core.CreatedCol, createdValue))
			stamps[recordIndex].Created = createdValue
		}
		if core.UpdatedCol != nil {
			core.UpdatedCol.SetValue(recordPointer, CoerceManagedInteger(core.UpdatedCol, writeTime))
			stamps[recordIndex].Updated = writeTime
		}
	}
	return stamps
}

// AssignAutoincrementKeys generates the keys of the records being inserted. A record whose
// autoincrement column is ≤ 0 takes the next ID of its counter, one counter per (partition,
// AutoincrementPart), reserved in one range per counter through reserveCounter. Then every
// KeyIntPacking key is packed from its slot columns, generated ID included.
func (core *TableCore) AssignAutoincrementKeys(records RecordSlice, reserveCounter func(counterName string, count int) (int64, error)) error {
	if core.AutoincrementCol == nil && len(core.KeyIntPacking) == 0 {
		return nil
	}
	recordIndexesByCounter := map[string][]int{}
	needsGeneratedID := make([]bool, records.Len())
	for recordIndex := range records.Len() {
		recordPointer := records.At(recordIndex)
		if core.AutoincrementCol == nil {
			break
		}
		// A virtual TableStruct.Autoincrement() column has no field and reads nil: it always needs a value.
		if rawValue := core.AutoincrementCol.GetRawValue(recordPointer); rawValue != nil && ConvertToInt64(rawValue) > 0 {
			continue
		}
		needsGeneratedID[recordIndex] = true
		partitionValue := int64(0)
		if partitionColumn := core.GetPartKey(); partitionColumn != nil {
			partitionValue = ConvertToInt64(partitionColumn.GetRawValue(recordPointer))
		}
		autoincrementPartValue := int64(0)
		if core.AutoincrementPart != nil {
			autoincrementPartValue = ConvertToInt64(core.AutoincrementPart.GetRawValue(recordPointer))
		}
		counterName := fmt.Sprintf("x%v_%v_%v", int32(partitionValue), core.Name, autoincrementPartValue)
		recordIndexesByCounter[counterName] = append(recordIndexesByCounter[counterName], recordIndex)
	}

	generatedIDs := make([]int64, records.Len())
	for counterName, recordIndexes := range recordIndexesByCounter {
		// The allocator reserves the whole range and returns its first ID.
		firstID, err := reserveCounter(counterName, len(recordIndexes))
		if err != nil {
			return fmt.Errorf("table %q: reserving %d IDs on %s: %w", core.Name, len(recordIndexes), counterName, err)
		}
		autoincrementColumn := core.AutoincrementCol.(*ColumnInfo)
		for offset, recordIndex := range recordIndexes {
			generatedID := firstID + int64(offset)
			if autoincrementColumn.AutoincrementFirstID > 1 {
				generatedID += autoincrementColumn.AutoincrementFirstID - 1
			}
			if autoincrementColumn.AutoincrementRandBits > 0 {
				generatedID = generatedID<<autoincrementColumn.AutoincrementRandBits | rand.Int64N(int64(1)<<autoincrementColumn.AutoincrementRandBits)
			}
			generatedIDs[recordIndex] = generatedID
			if len(core.KeyIntPacking) == 0 {
				core.AutoincrementCol.SetValue(records.At(recordIndex), generatedID)
			}
		}
	}

	if len(core.KeyIntPacking) == 0 {
		return nil
	}
	for recordIndex := range records.Len() {
		recordPointer := records.At(recordIndex)
		slotValues := make([]int64, len(core.KeyIntPacking))
		for slotIndex, slotColumn := range core.KeyIntPacking {
			switch {
			case slotColumn == core.AutoincrementCol && needsGeneratedID[recordIndex]:
				slotValues[slotIndex] = generatedIDs[recordIndex]
			case slotColumn.GetInfo().IsVirtual:
				slotValues[slotIndex] = 0
			default:
				slotValues[slotIndex] = ConvertToInt64(slotColumn.GetRawValue(recordPointer))
			}
		}
		// The 63-bit layout keeps the packed key a positive int64.
		packedKey := PackRowValues(core.Name, core.Keys[0].GetName(), core.KeyIntPacking, slotValues, core.KeyIntPackingSlotBits)
		core.Keys[0].SetValue(recordPointer, int64(packedKey))
	}
	return nil
}

// SplitInsertAndUpdateRecords copies the records into an insert list and an update list.
func SplitInsertAndUpdateRecords(records RecordSlice, isInsert func(unsafe.Pointer) bool) (insertRecords, updateRecords RecordSlice) {
	insertList, updateList := records.NewRecordList(), records.NewRecordList()
	for recordIndex := range records.Len() {
		recordPointer := records.At(recordIndex)
		if isInsert(recordPointer) {
			records.AppendCopy(insertList, recordPointer)
		} else {
			records.AppendCopy(updateList, recordPointer)
		}
	}
	return MakeReflectRecordSlice(insertList), MakeReflectRecordSlice(updateList)
}

func isManagedColumnName(columnName string) bool {
	return columnName == ColumnNameCreated || columnName == ColumnNameUpdated || columnName == ColumnNameVersion
}

// InsertColumns are the columns an insert writes: every column but the excluded ones; the managed
// created, updated and version are never excluded.
func (core *TableCore) InsertColumns(columnsToExclude []Coln) []IColInfo {
	if len(columnsToExclude) == 0 {
		return core.Columns
	}
	excludedNames := make([]string, 0, len(columnsToExclude))
	for _, columnToExclude := range columnsToExclude {
		excludedNames = append(excludedNames, columnToExclude.GetInfo().Name)
	}
	insertColumns := []IColInfo{}
	for _, column := range core.Columns {
		if isManagedColumnName(column.GetName()) || !slices.Contains(excludedNames, column.GetName()) {
			insertColumns = append(insertColumns, column)
		}
	}
	return insertColumns
}

// AffectedColumnsForInsert are the stored columns an insert writes. The post-write syncs (view
// tables, text search, the DataFrame log) read them.
func (core *TableCore) AffectedColumnsForInsert(columnsToExclude []Coln) []IColInfo {
	affectedColumns := []IColInfo{}
	for _, column := range core.InsertColumns(columnsToExclude) {
		if !column.GetInfo().IsVirtual {
			affectedColumns = append(affectedColumns, column)
		}
	}
	return affectedColumns
}

// AffectedColumnsForInclude are the stored columns an Update(columns...) writes, updated included.
func (core *TableCore) AffectedColumnsForInclude(columnsToInclude []Coln) []IColInfo {
	affectedColumns := []IColInfo{}
	for _, columnToInclude := range columnsToInclude {
		column := core.ColumnsMap[columnToInclude.GetName()]
		if column == nil || column.IsNil() || column.GetInfo().IsVirtual {
			continue
		}
		affectedColumns = append(affectedColumns, column)
	}
	if core.UpdatedCol != nil && !slices.ContainsFunc(affectedColumns, func(column IColInfo) bool { return column.GetName() == core.UpdatedCol.GetName() }) {
		affectedColumns = append(affectedColumns, core.UpdatedCol)
	}
	return affectedColumns
}

// AffectedColumnsForExclude are the stored columns an UpdateExclude(columns...) writes: every one
// but the excluded, updated included.
func (core *TableCore) AffectedColumnsForExclude(columnsToExclude []Coln) []IColInfo {
	excludedNames := map[string]bool{}
	for _, columnToExclude := range columnsToExclude {
		excludedNames[columnToExclude.GetName()] = true
	}
	affectedColumns := []IColInfo{}
	for _, column := range core.Columns {
		isUpdated := core.UpdatedCol != nil && column.GetName() == core.UpdatedCol.GetName()
		if column.GetInfo().IsVirtual || (!isUpdated && excludedNames[column.GetName()]) {
			continue
		}
		affectedColumns = append(affectedColumns, column)
	}
	return affectedColumns
}

// ResolveUpdateColumns is the column set an update writes: the included columns (or every non-key
// column but the excluded ones), the managed updated and version, and every derived column the
// written ones feed: packed / hashed index columns, composite bucket columns, cache-group hashes and
// the TypeDelta packed columns. It refuses an update that writes part of a derived column's sources,
// because a driver that recomputes the derived column from the record would compute it from values
// the update does not carry. Every driver enforces it, so a write accepted by one is accepted by all.
func (core *TableCore) ResolveUpdateColumns(records RecordSlice, columnsToInclude, columnsToExclude []Coln) []IColInfo {
	columnsToUpdate := []IColInfo{}
	if len(columnsToInclude) > 0 {
		updatedAlreadyIncluded := core.UpdatedCol == nil
		versionAlreadyIncluded := core.VersionCol == nil
		for _, declaredColumn := range columnsToInclude {
			column := core.ColumnsMap[declaredColumn.GetName()]
			if column == nil {
				panic("No se encontró la columna (update):" + declaredColumn.GetName())
			}
			if slices.Contains(core.KeysIdx, column.GetInfo().Idx) {
				panic(fmt.Sprintf(`Table "%v": The column "%v" can't be updated because is part of primary key.`, core.Name, column.GetName()))
			}
			columnsToUpdate = append(columnsToUpdate, column)
			updatedAlreadyIncluded = updatedAlreadyIncluded || column.GetName() == core.UpdatedCol.GetName()
			versionAlreadyIncluded = versionAlreadyIncluded || column.GetName() == core.VersionCol.GetName()
		}
		if !updatedAlreadyIncluded {
			columnsToUpdate = append(columnsToUpdate, core.UpdatedCol)
		}
		if !versionAlreadyIncluded {
			// The managed version always persists, even with an explicit include list.
			columnsToUpdate = append(columnsToUpdate, core.VersionCol)
		}
	} else {
		excludedNames := make([]string, 0, len(columnsToExclude))
		for _, columnToExclude := range columnsToExclude {
			excludedNames = append(excludedNames, columnToExclude.GetName())
		}
		for _, column := range core.Columns {
			isManaged := (core.UpdatedCol != nil && column.GetName() == core.UpdatedCol.GetName()) ||
				(core.VersionCol != nil && column.GetName() == core.VersionCol.GetName())
			isExcluded := slices.Contains(excludedNames, column.GetName())
			if !column.GetInfo().IsVirtual && (isManaged || !isExcluded) && !slices.Contains(core.KeysIdx, column.GetInfo().Idx) {
				columnsToUpdate = append(columnsToUpdate, column)
			}
		}
	}

	// The columns whose values the update carries: keys, partition, written columns, and the computed
	// columns derived from them.
	includedIdx := slices.Clone(core.KeysIdx)
	for _, column := range columnsToUpdate {
		includedIdx = append(includedIdx, column.GetInfo().Idx)
	}
	partitionColumn := core.GetPartKey()
	if partitionColumn != nil && !partitionColumn.IsNil() {
		includedIdx = append(includedIdx, partitionColumn.GetInfo().Idx)
	}
	includedIdx = core.IncludeComputedColumns(includedIdx)
	isIncluded := func(column IColInfo) bool { return slices.Contains(includedIdx, column.GetInfo().Idx) }

	columnsToUpdateByName := map[string]bool{}
	for _, column := range columnsToUpdate {
		columnsToUpdateByName[column.GetName()] = true
	}
	appendDerivedColumn := func(column IColInfo) {
		if !columnsToUpdateByName[column.GetName()] {
			columnsToUpdate = append(columnsToUpdate, column)
			columnsToUpdateByName[column.GetName()] = true
		}
	}

	// Packed and hashed indexes / views: their generated column is rewritten when the update writes
	// all of its sources, and refused when it writes only some.
	for _, source := range core.IndexSources {
		// Cache groups are checked below; a delta view's packed column is always written, from the
		// stored pinned values a partial update leaves out.
		if source.Column == nil || !source.Column.GetInfo().IsVirtual || source.Type == 3 || source.IsDeltaView {
			continue
		}
		missingColumnNames := []string{}
		writesSomeSource := false
		for _, sourceColumnName := range source.Columns {
			sourceColumn := core.ColumnsMap[sourceColumnName]
			if isIncluded(sourceColumn) {
				writesSomeSource = true
			} else {
				missingColumnNames = append(missingColumnNames, sourceColumnName)
			}
		}
		if writesSomeSource && len(missingColumnNames) > 0 {
			quotedColumnNames := []string{}
			for _, sourceColumnName := range source.Columns {
				if partitionColumn == nil || partitionColumn.IsNil() || partitionColumn.GetName() != sourceColumnName {
					quotedColumnNames = append(quotedColumnNames, fmt.Sprintf(`"%v"`, sourceColumnName))
				}
			}
			panic(fmt.Sprintf(`Table "%v": A composit index/view requires the columns %v be updated together. Not Included: %v`,
				core.Name, strings.Join(quotedColumnNames, ", "), strings.Join(missingColumnNames, ", ")))
		}
		if writesSomeSource {
			appendDerivedColumn(source.Column)
		}
	}

	// Composite bucket hashes become inconsistent if only part of the source tuple is updated.
	for _, compositeBucketIndex := range core.CompositeBucketIndexes {
		includedColumnNames, missingColumnNames := []string{}, []string{}
		for _, sourceColumn := range compositeBucketIndex.SourceColumns {
			if isIncluded(sourceColumn) {
				includedColumnNames = append(includedColumnNames, sourceColumn.GetName())
			} else {
				missingColumnNames = append(missingColumnNames, sourceColumn.GetName())
			}
		}
		if len(includedColumnNames) > 0 && len(missingColumnNames) > 0 {
			panic(fmt.Sprintf(`Table "%v": CompositeBucketing index "%v" requires updating all source columns together. Included: %v | Missing: %v`,
				core.Name, compositeBucketIndex.Name, strings.Join(includedColumnNames, ", "), strings.Join(missingColumnNames, ", ")))
		}
		if len(includedColumnNames) > 0 {
			for _, virtualColumn := range compositeBucketIndex.VirtualColumnsBySize {
				appendDerivedColumn(virtualColumn)
			}
		}
	}

	// A cache group hashes all of its source columns: an update writing some of them must carry the
	// others in the record, and rewrites the hash.
	for _, indexGroup := range core.IndexGroups {
		// A no-index group stores no hash, so a partial update has nothing of it to recompute.
		if indexGroup.BaseGroup != nil {
			continue
		}
		includedColumnNames, omittedColumns := []string{}, []IColInfo{}
		for _, sourceColumn := range indexGroup.SourceColumns {
			if isIncluded(sourceColumn.Column) {
				includedColumnNames = append(includedColumnNames, sourceColumn.Column.GetName())
			} else {
				omittedColumns = append(omittedColumns, sourceColumn.Column)
			}
		}
		if len(includedColumnNames) > 0 && len(omittedColumns) > 0 {
			for recordIndex := range records.Len() {
				recordPointer := records.At(recordIndex)
				missingInRecord := []string{}
				for _, omittedColumn := range omittedColumns {
					if !hasUsableIndexSourceValue(omittedColumn.GetRawValue(recordPointer)) {
						missingInRecord = append(missingInRecord, omittedColumn.GetName())
					}
				}
				if len(missingInRecord) > 0 {
					panic(fmt.Sprintf(`Table "%v": IndexGroup "%v" needs struct values for omitted source columns. Included in update: %v | Missing in struct: %v`,
						core.Name, indexGroup.Name, strings.Join(includedColumnNames, ", "), strings.Join(missingInRecord, ", ")))
				}
			}
		}
		if len(includedColumnNames) > 0 && indexGroup.VirtualColumn != nil && !indexGroup.VirtualColumn.IsNil() {
			appendDerivedColumn(indexGroup.VirtualColumn)
		}
	}

	// A TypeDelta packed column holds updated, which every update writes, so it is always rewritten,
	// from the pinned values the record keeps.
	for _, deltaIndex := range core.DeltaIndexes {
		columnsToUpdate = append(columnsToUpdate, deltaIndex.PackedColumn)
	}
	return columnsToUpdate
}

// hasUsableIndexSourceValue: a record carries a value for an omitted cache-group source column when
// it is set (non-zero, non-empty).
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
	case reflect.String, reflect.Slice, reflect.Array:
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

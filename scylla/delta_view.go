package scylla

import (
	"cmp"
	"fmt"
	"math/bits"
	"reflect"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/gocql/gocql"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"golang.org/x/sync/errgroup"
)

// ─────────────────────────────────────────────────────────────────────────────
// TypeDelta on Scylla: the delta window protocol (db/cache delta_window.go).
//
// Each TypeDelta index is a materialized view holding every stored column of the
// record, keyed so one pinned prefix is a contiguous range ordered by updated:
//
//	PRIMARY KEY ((partition), zz_dlt_<pinned>, id)   ordered DESC
//	zz_dlt_<pinned> = pinned values … | updated (31 bits), one bigint
//
// Scylla allows a single non-key base column in a view key, hence the packing (a
// keyless index packs updated alone). Every declared Key is pinned: a read names
// each value, by an equality or Delta()'s values for the last Key, or fans out one
// read per FixedValues value (resolveDeltaPrefixes). The view is also a range view
// (Type 8) of the query planner on [partition, pinned…, updated], with the same
// FixedValues fan-out, so it serves plain reads on its keys.
//
//   - Write: before the CQL, the stored version and pinned values of the written keys
//     are read (one "key IN" select per partition). Each record is stamped with the
//     next Version, and its packed column with the pinned values it ends with: a
//     partial update that leaves a pinned column out keeps the stored value. After
//     the CQL, a record whose pinned prefix changed writes one row, under its old
//     prefix, to <table>__delta_moves.
//   - Read: Delta() reads, per prefix and in parallel, the changed records straight
//     from the view, then the hashed window's (ID, version) and the moves slice of
//     the same window. The replace threshold counts across every prefix.
//
// Records of a TypeDelta table are never hard-deleted: the view would drop the row
// and no client would hear of it.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// deltaUpdatedSlotBits holds a positive int32 SUnixTime, the trailing slot of the packed key.
	deltaUpdatedSlotBits = 31
	// deltaViewGCGraceSeconds is low because every update moves the view row (its key holds
	// updated): a resurrected stale row only fails a window hash, which resends the prefix.
	deltaViewGCGraceSeconds = 86400
)

// deltaIndexInfo is one compiled TypeDelta index.
type deltaIndexInfo struct {
	// tag names the index: its Keys' column names joined by "_", "all" when keyless. It names the
	// view and the index's rows in the moves table.
	tag string
	// pinned are every declared Key, in order: each is a slot of the packed key, so one combination of
	// their values (a prefix) is one contiguous span ordered by updated. A read names the value of
	// each, or fans out over its FixedValues (resolveDeltaPrefixes).
	pinned         []IColInfo
	pinnedSlotBits []int64
	// packedColumn packs the pinned values and updated (updated alone on a keyless index): the
	// view's clustering key.
	packedColumn *columnInfo
	viewName     string
	// replaceThreshold is how many changed or moved records a sync answers before it answers a replace.
	replaceThreshold int
}

func (deltaIndex deltaIndexInfo) slotBits() []int64 {
	return append(slices.Clone(deltaIndex.pinnedSlotBits), deltaUpdatedSlotBits)
}

// packedKeyValue is the stored value of the packed column for pinned values and an updated.
func (deltaIndex deltaIndexInfo) packedKeyValue(tableName string, pinnedValues []int64, updated int64) int64 {
	sourceColumns := append(slices.Clone(deltaIndex.pinned), deltaIndex.packedColumn)
	packed := packRowValues(tableName, deltaIndex.packedColumn.GetName(), sourceColumns,
		append(slices.Clone(pinnedValues), updated), deltaIndex.slotBits())
	return storeVirtualPacked(packed, false).(int64)
}

/* Compile */

// compileSchemaDeltaView registers a TypeDelta index and its view. Every Key is pinned, the last one
// being the one Delta()'s values name; the table's single integer key is the record ID.
func compileSchemaDeltaView(dbTable *ScyllaTable, indexCfg Index) {
	if dbTable.VersionCol == nil || dbTable.VersionCol.IsNil() {
		panic(fmt.Sprintf(`Table "%v": TypeDelta requires the managed "%v" column; declare Version in the record and table structs`,
			dbTable.Name, managedVersionColumnName))
	}
	if dbTable.UpdatedCol == nil || dbTable.UpdatedCol.IsNil() {
		panic(fmt.Sprintf(`Table "%v": TypeDelta requires the managed "%v" column`, dbTable.Name, managedUpdatedColumnName))
	}
	if partitionColumn := dbTable.GetPartKey(); partitionColumn == nil || partitionColumn.IsNil() ||
		!isIntegerFieldType(partitionColumn.GetType().FieldType) {
		panic(fmt.Sprintf(`Table "%v": TypeDelta requires an integer partition column`, dbTable.Name))
	}
	if len(dbTable.Keys) != 1 || !isIntegerFieldType(dbTable.Keys[0].GetType().FieldType) {
		panic(fmt.Sprintf(`Table "%v": TypeDelta requires exactly one integer key column, the record ID. A table keyed otherwise answers its route with db.DeltaReplaceMetadata() instead`,
			dbTable.Name))
	}

	replaceThreshold := indexCfg.DeltaReplaceThreshold
	if replaceThreshold <= 0 {
		replaceThreshold = cache.DefaultDeltaReplaceThreshold
	}
	deltaIndex := deltaIndexInfo{tag: "all", replaceThreshold: replaceThreshold}

	declaredColumns := make([]IColInfo, 0, len(indexCfg.Keys))
	declaredKeyNames := make([]string, 0, len(indexCfg.Keys))
	for _, declaredKey := range indexCfg.Keys {
		column := dbTable.ColumnsMap[declaredKey.GetInfo().Name]
		if column == nil || column.IsNil() {
			panic(fmt.Sprintf(`Table "%v": TypeDelta column "%v" was not found`, dbTable.Name, declaredKey.GetInfo().Name))
		}
		if column.GetType().IsComplexType || column.GetType().IsSlice ||
			!isSupportedPackedIndexNumericFieldType(column.GetType().FieldType) {
			panic(fmt.Sprintf(`Table "%v": TypeDelta key "%v" must be a scalar integer. Found: %v`,
				dbTable.Name, column.GetName(), column.GetType().FieldType))
		}
		if column.GetName() == dbTable.Keys[0].GetName() {
			panic(fmt.Sprintf(`Table "%v": TypeDelta key "%v" is the table's key, the record ID`, dbTable.Name, column.GetName()))
		}
		declaredColumns = append(declaredColumns, column)
		declaredKeyNames = append(declaredKeyNames, column.GetName())
	}
	if len(declaredColumns) > 0 {
		deltaIndex.tag = strings.Join(declaredKeyNames, "_")
		deltaIndex.pinned = declaredColumns
	}
	deltaIndex.viewName = fmt.Sprintf("%v__dlt_%v_view", dbTable.Name, deltaIndex.tag)

	// A keyless index packs updated alone: every delta view keys on one packed column.
	deltaIndex.pinnedSlotBits = planDeltaPinnedSlots(dbTable, deltaIndex.pinned, indexCfg)
	packedColumnSuffix := managedUpdatedColumnName
	if len(deltaIndex.pinned) > 0 {
		packedColumnSuffix = deltaIndex.tag
	}
	packedColumn := &columnInfo{
		ColInfo: colInfo{IsVirtual: true, Idx: dbTable.MaxColIdx},
		ColType: colType{FieldType: "int64", DBType: "bigint"},
	}
	packedColumn.GetInfo().Name = "zz_dlt_" + packedColumnSuffix
	dbTable.MaxColIdx++
	dbTable.ColumnsMap[packedColumn.GetName()] = packedColumn
	deltaIndex.packedColumn = packedColumn

	// An insert carries every pinned value on the record, so this is exact there. An update binds
	// the value stampDeltaWrites computed from the stored pinned values instead (managedWriteValues).
	tableName, updatedColumn, compiledIndex := dbTable.Name, dbTable.UpdatedCol, deltaIndex
	packedColumn.GetValueFn = func(recordPointer unsafe.Pointer) any {
		pinnedValues := make([]int64, len(compiledIndex.pinned))
		for i, pinnedColumn := range compiledIndex.pinned {
			pinnedValues[i] = convertToInt64(pinnedColumn.GetValue(recordPointer))
		}
		return compiledIndex.packedKeyValue(tableName, pinnedValues, convertToInt64(updatedColumn.GetValue(recordPointer)))
	}

	for _, registered := range dbTable.deltaIndexes {
		if registered.viewName == deltaIndex.viewName {
			panic(fmt.Sprintf(`Table "%v": two TypeDelta indexes declare the same Keys (%v)`, dbTable.Name, deltaIndex.tag))
		}
	}
	dbTable.deltaIndexes = append(dbTable.deltaIndexes, deltaIndex)
	dbTable.views[deltaIndex.viewName] = makeDeltaViewInfo(dbTable, deltaIndex)
	if ShouldLog() {
		fmt.Printf("Delta index registered: table=%s view=%s pinnedSlotBits=%v\n", dbTable.Name, deltaIndex.viewName, deltaIndex.pinnedSlotBits)
	}
}

// planDeltaPinnedSlots sizes each pinned column's slot: an explicit Size(), else its FixedValues
// range. One pinned column with neither takes every bit the others and updated leave of the bigint.
func planDeltaPinnedSlots(dbTable *ScyllaTable, pinned []IColInfo, indexCfg Index) []int64 {
	slotBits := make([]int64, len(pinned))
	elasticIndex := -1
	for i, pinnedColumn := range pinned {
		if declaredBits := int64(indexCfg.Keys[i].GetInfo().SlotBits); declaredBits > 0 {
			slotBits[i] = declaredBits
		} else if valueRange, isDeclared := dbTable.fixedValueRanges[pinnedColumn.GetName()]; isDeclared {
			slotBits[i] = max(1, int64(bits.Len64(uint64(valueRange.maxValue))))
		} else if elasticIndex >= 0 {
			panic(fmt.Sprintf(`Table "%v": TypeDelta pinned keys "%v" and "%v" both lack a FixedValues entry and a Size; only one can take the remaining bits`,
				dbTable.Name, pinned[elasticIndex].GetName(), pinnedColumn.GetName()))
		} else {
			elasticIndex = i
		}
	}
	remainingBits := packedVirtualInt64Bits - deltaUpdatedSlotBits - sumSlotBits(slotBits, 0)
	if elasticIndex >= 0 {
		slotBits[elasticIndex] = remainingBits
		remainingBits = 0
	}
	if remainingBits < 0 || (elasticIndex >= 0 && slotBits[elasticIndex] < 1) {
		panic(fmt.Sprintf(`Table "%v": TypeDelta pinned keys take more than the %v bits a bigint leaves beside updated. Narrow a FixedValues range.`,
			dbTable.Name, packedVirtualInt64Bits-deltaUpdatedSlotBits))
	}
	return slotBits
}

// makeDeltaViewInfo registers an index's view in the query planner as a range view on
// [partition, pinned…, updated]: an equality on the pinned values is one span of the packed column,
// and an updated range narrows it.
func makeDeltaViewInfo(dbTable *ScyllaTable, deltaIndex deltaIndexInfo) *viewInfo {
	partitionColumn := dbTable.GetPartKey()
	sourceColumns := append(slices.Clone(deltaIndex.pinned), dbTable.UpdatedCol)
	sourceNames := make([]string, len(sourceColumns))
	for i, sourceColumn := range sourceColumns {
		sourceNames[i] = sourceColumn.GetName()
	}
	view := &viewInfo{
		Type:                    8,
		isDeltaView:             true,
		name:                    deltaIndex.viewName,
		column:                  deltaIndex.packedColumn,
		columns:                 append([]string{partitionColumn.GetName()}, sourceNames...),
		columnsNoPart:           sourceNames,
		packedSourceColumns:     sourceColumns,
		packedSlotBitsPerColumn: deltaIndex.slotBits(),
	}
	compilePackedRangeAccessors(view, sourceColumns, deltaIndex.slotBits(), false, partitionColumn)

	storedColumns := deltaViewStoredColumns(dbTable)
	for _, storedColumn := range storedColumns {
		view.availableColumns = append(view.availableColumns, storedColumn.GetName())
	}
	view.getExpectedColumns = func() []viewExpectedColumn {
		return makeViewExpectedColumns(append(slices.Clone(storedColumns), deltaIndex.packedColumn))
	}
	view.getCreateScript = func() string { return deltaViewCreateScript(*dbTable, deltaIndex, storedColumns) }
	return view
}

// deltaViewStoredColumns are the columns a delta view copies: every stored column of the record, in
// schema order. Delta() and the planner read whole records from the view.
func deltaViewStoredColumns(dbTable *ScyllaTable) []IColInfo {
	storedColumns := []IColInfo{}
	for _, column := range dbTable.ColumnsMap {
		if !column.GetInfo().IsVirtual && !column.GetInfo().IsComputed {
			storedColumns = append(storedColumns, column)
		}
	}
	slices.SortFunc(storedColumns, func(left, right IColInfo) int { return int(left.GetInfo().Idx - right.GetInfo().Idx) })
	return storedColumns
}

// deltaViewCreateScript is the CREATE of an index's view. The view is ordered newest first: a sync
// reads the recent end of its prefix.
func deltaViewCreateScript(scyllaTable ScyllaTable, deltaIndex deltaIndexInfo, storedColumns []IColInfo) string {
	partitionName, keyName := scyllaTable.GetPartKey().GetName(), scyllaTable.Keys[0].GetName()
	packedName := deltaIndex.packedColumn.GetName()
	selectNames := []string{packedName}
	for _, storedColumn := range storedColumns {
		selectNames = append(selectNames, storedColumn.GetName())
	}
	return fmt.Sprintf(`CREATE MATERIALIZED VIEW %v.%v AS
		SELECT %v FROM %v
		WHERE %v IS NOT NULL AND %v IS NOT NULL AND %v IS NOT NULL
		PRIMARY KEY ((%v), %v, %v)
		WITH CLUSTERING ORDER BY (%v DESC, %v ASC)
		AND gc_grace_seconds = %d;`,
		resolveKeyspace(scyllaTable), deltaIndex.viewName, strings.Join(selectNames, ", "), scyllaTable.GetFullName(),
		partitionName, packedName, keyName,
		partitionName, packedName, keyName,
		packedName, keyName, deltaViewGCGraceSeconds)
}

func deltaMovesTableName(scyllaTable ScyllaTable) string { return scyllaTable.Name + "__delta_moves" }

// hasDeltaMoves reports whether any TypeDelta index pins a column, so its records can move.
func hasDeltaMoves(scyllaTable ScyllaTable) bool {
	return slices.ContainsFunc(scyllaTable.deltaIndexes, func(deltaIndex deltaIndexInfo) bool { return len(deltaIndex.pinned) > 0 })
}

// deltaMovesCreateScript is the CREATE of the moves table: one row per record that left a pinned
// prefix, under the prefix it left (pinned_prefix is the packed column with updated 0). Its rows never
// expire: a cursor of any age reads the moves since it, so age alone never forces a replace.
func deltaMovesCreateScript(scyllaTable ScyllaTable) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %v.%v (
			partition_id %v, index_tag text, pinned_prefix bigint, updated int, id %v,
			PRIMARY KEY ((partition_id, index_tag, pinned_prefix), updated, id)
		);`,
		resolveKeyspace(scyllaTable), deltaMovesTableName(scyllaTable),
		scyllaTable.GetPartKey().GetType().DBType, scyllaTable.Keys[0].GetType().DBType)
}

/* Write */

// writePhase is one half of a write call: its records and the columns it writes. A column it does not
// write keeps its stored value, which is what the delta view and the frame log must see.
type writePhase struct {
	records        recordSlice
	writtenColumns []IColInfo
}

// deltaMove is one record that left a pinned prefix.
type deltaMove struct {
	partition    int64
	recordID     int64
	updated      int64
	indexTag     string
	pinnedPrefix int64
}

// deltaPinnedColumns are the columns whose stored value a write needs: every pinned column.
func deltaPinnedColumns(scyllaTable ScyllaTable) []IColInfo {
	pinnedColumns := []IColInfo{}
	for _, deltaIndex := range scyllaTable.deltaIndexes {
		for _, pinnedColumn := range deltaIndex.pinned {
			if !slices.ContainsFunc(pinnedColumns, func(column IColInfo) bool { return column.GetName() == pinnedColumn.GetName() }) {
				pinnedColumns = append(pinnedColumns, pinnedColumn)
			}
		}
	}
	return pinnedColumns
}

// storedRows are the stored columns of the written records, read before the write, by (partition,
// key) and column name; a record not stored is absent and a null column has no entry.
type storedRows map[[2]int64]map[string]any

// readStoredWrittenRows reads, in one select per partition, what the delta indexes, the DataFrames
// and the index groups need of the records about to be written: the version and the pinned columns,
// the whole row for the frames (a ToFrames method may read any field), and the group columns (the
// groups a record leaves are marked too). nil when the table has none of them. The keys must be
// final (after handlePreInsert).
func readStoredWrittenRows(scyllaTable ScyllaTable, phases []writePhase) (storedRows, error) {
	var columns []IColInfo
	if len(scyllaTable.deltaIndexes) > 0 {
		columns = append(columns, scyllaTable.VersionCol)
		columns = append(columns, deltaPinnedColumns(scyllaTable)...)
	}
	if scyllaTable.dataFrames != nil {
		columns = append(columns, scyllaTable.dataFrames.storedColumns...)
	}
	if len(scyllaTable.indexGroups) > 0 && scyllaTable.indexUpdatedTable != nil {
		columns = append(columns, indexGroupStoredColumns(&scyllaTable)...)
	}
	if len(columns) == 0 {
		return nil, nil
	}
	partitionColumn, keyColumn := scyllaTable.GetPartKey(), scyllaTable.Keys[0]
	idsOfPartition := map[int64][]int64{}
	for _, phase := range phases {
		for i := range phase.records.len() {
			recordPointer := phase.records.at(i)
			partition := convertToInt64(partitionColumn.GetRawValue(recordPointer))
			idsOfPartition[partition] = append(idsOfPartition[partition], convertToInt64(keyColumn.GetRawValue(recordPointer)))
		}
	}
	return readStoredRows(scyllaTable, idsOfPartition, columns)
}

// stampDeltaWrites stamps every written record with its next Version and returns, per phase, the
// packed column values its CQL binds (by column name, one per record), and the moves to write once
// the rows landed.
func stampDeltaWrites(scyllaTable ScyllaTable, phases []writePhase, stored storedRows) ([]map[string][]any, []deltaMove) {
	if len(scyllaTable.deltaIndexes) == 0 {
		return make([]map[string][]any, len(phases)), nil
	}
	partitionColumn, keyColumn := scyllaTable.GetPartKey(), scyllaTable.Keys[0]
	pinnedColumns := deltaPinnedColumns(scyllaTable)

	packedValuesByPhase := make([]map[string][]any, len(phases))
	var moves []deltaMove
	for phaseIndex, phase := range phases {
		isWritten := map[string]bool{}
		for _, column := range phase.writtenColumns {
			isWritten[column.GetName()] = true
		}
		packedValuesByPhase[phaseIndex] = map[string][]any{}
		for i := range phase.records.len() {
			recordPointer := phase.records.at(i)
			partition := convertToInt64(partitionColumn.GetRawValue(recordPointer))
			recordID := convertToInt64(keyColumn.GetRawValue(recordPointer))
			updated := convertToInt64(scyllaTable.UpdatedCol.GetValue(recordPointer))
			storedRow, isStored := stored[[2]int64{partition, recordID}]
			version := cache.NextVersion(storedIntegerOf(storedRow, managedVersionColumnName))
			scyllaTable.VersionCol.SetValue(recordPointer, coerceManagedIntegerValue(scyllaTable.VersionCol, version))

			// A pinned column the write leaves out keeps its stored value.
			finalValues := map[string]int64{}
			for _, column := range pinnedColumns {
				if isWritten[column.GetName()] || !isStored {
					finalValues[column.GetName()] = convertToInt64(column.GetRawValue(recordPointer))
				} else {
					finalValues[column.GetName()] = storedIntegerOf(storedRow, column.GetName())
				}
			}
			for _, deltaIndex := range scyllaTable.deltaIndexes {
				finalPinned := pinnedValuesOf(deltaIndex, finalValues)
				columnName := deltaIndex.packedColumn.GetName()
				packedValuesByPhase[phaseIndex][columnName] = append(packedValuesByPhase[phaseIndex][columnName],
					deltaIndex.packedKeyValue(scyllaTable.Name, finalPinned, updated))
				if !isStored {
					continue
				}
				storedPinned := make([]int64, len(deltaIndex.pinned))
				for pinnedIndex, pinnedColumn := range deltaIndex.pinned {
					storedPinned[pinnedIndex] = storedIntegerOf(storedRow, pinnedColumn.GetName())
				}
				if !slices.Equal(storedPinned, finalPinned) {
					moves = append(moves, deltaMove{
						partition: partition, recordID: recordID, updated: updated, indexTag: deltaIndex.tag,
						pinnedPrefix: deltaIndex.packedKeyValue(scyllaTable.Name, storedPinned, 0),
					})
				}
			}
		}
	}
	return packedValuesByPhase, moves
}

func pinnedValuesOf(deltaIndex deltaIndexInfo, valuesByColumn map[string]int64) []int64 {
	pinnedValues := make([]int64, len(deltaIndex.pinned))
	for i, pinnedColumn := range deltaIndex.pinned {
		pinnedValues[i] = valuesByColumn[pinnedColumn.GetName()]
	}
	return pinnedValues
}

// storedIntegerOf is an integer column of a stored row: 0 for a null column or a record not stored.
func storedIntegerOf(storedRow map[string]any, columnName string) int64 {
	if storedValue, isSet := storedRow[columnName]; isSet {
		return convertToInt64(storedValue)
	}
	return 0
}

// readStoredRows reads columns of the given keys, by partition and key, at LocalQuorum so a write
// right after another sees it.
func readStoredRows(scyllaTable ScyllaTable, idsOfPartition map[int64][]int64, columns []IColInfo) (storedRows, error) {
	columnNames := []string{scyllaTable.Keys[0].GetName()}
	for _, column := range columns {
		if !slices.Contains(columnNames, column.GetName()) {
			columnNames = append(columnNames, column.GetName())
		}
	}
	for partition := range idsOfPartition {
		slices.Sort(idsOfPartition[partition])
		idsOfPartition[partition] = slices.Compact(idsOfPartition[partition])
	}

	stored := storedRows{}
	err := forEachIDsBatch(scyllaTable, idsOfPartition, strings.Join(columnNames, ", "),
		func(queryString string, queryValues []any, partition int64) error {
			iterator := getScyllaConnection().Query(queryString, queryValues...).Consistency(gocql.LocalQuorum).Iter()
			for {
				row := map[string]any{}
				if !iterator.MapScan(row) {
					break
				}
				storedRow := map[string]any{}
				for _, columnName := range columnNames {
					if storedValue := row[columnName]; storedValue != nil {
						storedRow[columnName] = storedValue
					}
				}
				stored[[2]int64{partition, convertToInt64(row[columnNames[0]])}] = storedRow
			}
			return iterator.Close()
		})
	if err != nil {
		return nil, fmt.Errorf("table %q: reading the stored rows: %w", scyllaTable.Name, err)
	}
	return stored, nil
}

// writeDeltaMoves writes the moves of a write. It runs after the rows landed: a move published first
// would remove, from its old prefix's clients, a record whose write then failed.
func writeDeltaMoves(scyllaTable ScyllaTable, moves []deltaMove) error {
	if len(moves) == 0 {
		return nil
	}
	statement := fmt.Sprintf("INSERT INTO %v.%v (partition_id, index_tag, pinned_prefix, updated, id) VALUES (?, ?, ?, ?, ?)",
		resolveKeyspace(scyllaTable), deltaMovesTableName(scyllaTable))
	session := getScyllaConnection()
	batch := session.NewBatch(gocql.UnloggedBatch)
	for _, move := range moves {
		batch.Query(statement, coerceManagedIntegerValue(scyllaTable.GetPartKey(), move.partition), move.indexTag,
			move.pinnedPrefix, int32(move.updated), coerceManagedIntegerValue(scyllaTable.Keys[0], move.recordID))
	}
	if err := session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("table %q: writing %d delta move(s): %w", scyllaTable.Name, len(moves), err)
	}
	return nil
}

/* Read */

// resolveDeltaPrefixes lists the pinned prefixes a Delta() read covers on an index. Each pinned
// column takes its equality value, or — the last one — the values Delta() names, or else every
// value its FixedValues enumerates; their combinations are the prefixes. ok is false when a column
// has none of them, or when the enumerated fan-out passes maxFixedValueFanoutQueries.
// equalityCount is how many pinned columns the query's own equalities fixed.
func resolveDeltaPrefixes(scyllaTable ScyllaTable, deltaIndex deltaIndexInfo, equalityValues map[string]any, deltaValues []int64) (prefixes [][]int64, equalityCount int, ok bool) {
	if len(deltaValues) > 0 && len(deltaIndex.pinned) == 0 {
		return nil, 0, false // a keyless index has no column for the values to name
	}
	prefixes = [][]int64{{}}
	enumeratedQueries := 1
	for pinnedIndex, pinnedColumn := range deltaIndex.pinned {
		var columnValues []int64
		isLastPinned := pinnedIndex == len(deltaIndex.pinned)-1
		if equalityValue, hasEquality := equalityValues[pinnedColumn.GetName()]; hasEquality {
			columnValues = []int64{convertToInt64(equalityValue)}
			equalityCount++
		} else if isLastPinned && len(deltaValues) > 0 {
			columnValues = deltaValues
		} else if valueRange, isDeclared := scyllaTable.fixedValueRanges[pinnedColumn.GetName()]; isDeclared && len(valueRange.declaredValues) > 0 {
			columnValues = valueRange.declaredValues
			enumeratedQueries *= len(columnValues)
		} else {
			return nil, 0, false
		}
		combined := make([][]int64, 0, len(prefixes)*len(columnValues))
		for _, prefix := range prefixes {
			for _, columnValue := range columnValues {
				combined = append(combined, append(slices.Clone(prefix), columnValue))
			}
		}
		prefixes = combined
	}
	if enumeratedQueries > maxFixedValueFanoutQueries {
		return nil, 0, false
	}
	return prefixes, equalityCount, true
}

// selectDeltaIndex picks the TypeDelta index a Delta() read runs on: of those whose prefixes
// resolve, the one the query's equalities fix most, then the one with fewest prefixes.
func selectDeltaIndex(scyllaTable ScyllaTable, equalityValues map[string]any, deltaValues []int64) (deltaIndexInfo, [][]int64, error) {
	selectedPosition, isAmbiguous := -1, false
	var selectedPrefixes [][]int64
	selectedEqualityCount := -1
	for position, deltaIndex := range scyllaTable.deltaIndexes {
		prefixes, equalityCount, ok := resolveDeltaPrefixes(scyllaTable, deltaIndex, equalityValues, deltaValues)
		if !ok {
			continue
		}
		switch {
		case equalityCount > selectedEqualityCount,
			equalityCount == selectedEqualityCount && len(prefixes) < len(selectedPrefixes):
			selectedPosition, selectedPrefixes, selectedEqualityCount, isAmbiguous = position, prefixes, equalityCount, false
		case equalityCount == selectedEqualityCount && len(prefixes) == len(selectedPrefixes):
			isAmbiguous = true
		}
	}
	if selectedPosition < 0 {
		return deltaIndexInfo{}, nil, fmt.Errorf(`table %q: Delta() found no TypeDelta index whose Keys all resolve: each needs an equality, the Delta() values (the last one), or an enumerable FixedValues (at most %d queries)`,
			scyllaTable.Name, maxFixedValueFanoutQueries)
	}
	if isAmbiguous {
		return deltaIndexInfo{}, nil, fmt.Errorf(`table %q: Delta() fits several TypeDelta indexes equally: pin one more Keys column, or drop an index`, scyllaTable.Name)
	}
	return scyllaTable.deltaIndexes[selectedPosition], selectedPrefixes, nil
}

// deltaViewRow is the (ID, updated, version) of one row of a delta view.
type deltaViewRow struct {
	recordID int64
	updated  int64
	version  int16
}

// deltaViewUpdatedMax is the highest updated a view slot holds: an upper bound meaning "up to now".
var deltaViewUpdatedMax = int64(db.SlotMaxValue(deltaUpdatedSlotBits))

// deltaViewRange is the WHERE of a prefix's view rows with updated in [fromUpdated, toUpdated]: one
// span of the packed column, read newest first.
func deltaViewRange(scyllaTable ScyllaTable, deltaIndex deltaIndexInfo, partition int64, pinnedValues []int64, fromUpdated, toUpdated int64) (string, []any) {
	packedName := deltaIndex.packedColumn.GetName()
	return fmt.Sprintf("%v = ? AND %v >= ? AND %v <= ?", scyllaTable.GetPartKey().GetName(), packedName, packedName),
		[]any{
			coerceManagedIntegerValue(scyllaTable.GetPartKey(), partition),
			deltaIndex.packedKeyValue(scyllaTable.Name, pinnedValues, fromUpdated),
			deltaIndex.packedKeyValue(scyllaTable.Name, pinnedValues, toUpdated),
		}
}

// readDeltaViewRecords reads whole records of a prefix straight from the view into sink: updated in
// [fromUpdated, toUpdated], newest first. rowLimit > 0 stops the read there. columnNames projects
// the read; nil reads every stored column.
func readDeltaViewRecords(
	sink recordSink, scyllaTable ScyllaTable, deltaIndex deltaIndexInfo, partition int64, pinnedValues []int64,
	fromUpdated, toUpdated int64, rowLimit int, columnNames []string,
) error {
	if toUpdated < fromUpdated {
		return nil
	}
	if columnNames == nil {
		for _, column := range scyllaTable.Columns {
			if !column.GetInfo().IsVirtual {
				columnNames = append(columnNames, column.GetName())
			}
		}
	}
	whereClause, queryValues := deltaViewRange(scyllaTable, deltaIndex, partition, pinnedValues, fromUpdated, toUpdated)
	queryString := fmt.Sprintf("SELECT %v FROM %v.%v WHERE %v", strings.Join(columnNames, ", "),
		resolveKeyspace(scyllaTable), deltaIndex.viewName, whereClause)
	if rowLimit > 0 {
		queryString += fmt.Sprintf(" LIMIT %d", rowLimit)
	}
	scyllaTable.readConsistency = gocql.LocalQuorum
	return scanSelectQueryRows(queryString, queryValues, buildDefaultScanColumns(columnNames), scyllaTable, sink, nil, nil, time.Now())
}

// readDeltaViewRows reads the (ID, updated, version) of a prefix's view rows with updated in
// [fromUpdated, toUpdated], newest first: what the window hash needs, not the records.
func readDeltaViewRows(
	scyllaTable ScyllaTable, deltaIndex deltaIndexInfo, partition int64, pinnedValues []int64,
	fromUpdated, toUpdated int64,
) ([]deltaViewRow, error) {
	if toUpdated < fromUpdated {
		return nil, nil
	}
	keyName := scyllaTable.Keys[0].GetName()
	whereClause, queryValues := deltaViewRange(scyllaTable, deltaIndex, partition, pinnedValues, fromUpdated, toUpdated)
	queryString := fmt.Sprintf("SELECT %v, %v, %v FROM %v.%v WHERE %v", keyName, managedUpdatedColumnName, managedVersionColumnName,
		resolveKeyspace(scyllaTable), deltaIndex.viewName, whereClause)

	var viewRows []deltaViewRow
	iterator := getScyllaConnection().Query(queryString, queryValues...).Consistency(gocql.LocalQuorum).Iter()
	for {
		row := map[string]any{}
		if !iterator.MapScan(row) {
			break
		}
		viewRows = append(viewRows, deltaViewRow{
			recordID: convertToInt64(row[keyName]),
			updated:  convertToInt64(row[managedUpdatedColumnName]),
			version:  int16(convertToInt64(row[managedVersionColumnName])),
		})
	}
	if err := iterator.Close(); err != nil {
		return nil, fmt.Errorf("table %q: reading the delta view %v: %w", scyllaTable.Name, deltaIndex.viewName, err)
	}
	return viewRows, nil
}

// readDeltaMovedIDs reads the IDs that left a prefix with updated ≥ fromUpdated, up to rowLimit.
func readDeltaMovedIDs(scyllaTable ScyllaTable, deltaIndex deltaIndexInfo, partition int64, pinnedValues []int64, fromUpdated int64, rowLimit int) ([]int64, error) {
	queryString := fmt.Sprintf("SELECT id FROM %v.%v WHERE partition_id = ? AND index_tag = ? AND pinned_prefix = ? AND updated >= ? LIMIT %d",
		resolveKeyspace(scyllaTable), deltaMovesTableName(scyllaTable), rowLimit)
	iterator := getScyllaConnection().Query(queryString,
		coerceManagedIntegerValue(scyllaTable.GetPartKey(), partition), deltaIndex.tag,
		deltaIndex.packedKeyValue(scyllaTable.Name, pinnedValues, 0), int32(fromUpdated)).Consistency(gocql.LocalQuorum).Iter()
	var movedIDs []int64
	var movedID int64
	for iterator.Scan(&movedID) {
		movedIDs = append(movedIDs, movedID)
	}
	if err := iterator.Close(); err != nil {
		return nil, fmt.Errorf("table %q: reading the delta moves: %w", scyllaTable.Name, err)
	}
	return movedIDs, nil
}

// deltaPrefixRead is what one prefix of a Delta() read returned. Each prefix is read by its own
// goroutine into its own fields.
type deltaPrefixRead struct {
	records    []unsafe.Pointer
	movedIDs   []int64
	windowRows []deltaViewRow
}

// readDeltaPrefixes runs readPrefix for every prefix in parallel, under the same concurrency cap as
// a fanned-out select.
func readDeltaPrefixes(prefixes [][]int64, reads []deltaPrefixRead, readPrefix func(prefix []int64, read *deltaPrefixRead) error) error {
	group := errgroup.Group{}
	group.SetLimit(maxFixedValueFanoutQueries)
	for prefixIndex, prefix := range prefixes {
		group.Go(func() error { return readPrefix(prefix, &reads[prefixIndex]) })
	}
	return group.Wait()
}

// execDelta is Exec for a Delta() read (db/cache delta_window.go describes the protocol). Every
// step reads each pinned prefix on its own, in parallel: one contiguous span of the view.
func execDelta(sink recordSink, tableInfo *TableInfo, scyllaTable ScyllaTable) error {
	deltaRead := tableInfo.Delta
	equalityValues := map[string]any{}
	for _, statement := range tableInfo.Statements {
		if statement.Operator == "=" {
			equalityValues[statement.Col] = statement.Value
		}
	}
	partitionValue, isPartitionPinned := equalityValues[scyllaTable.GetPartKey().GetName()]
	if !isPartitionPinned {
		return fmt.Errorf(`table %q: Delta() needs an equality on the partition column`, scyllaTable.Name)
	}
	deltaValues := make([]int64, len(deltaRead.LastKeyValues))
	for i, deltaValue := range deltaRead.LastKeyValues {
		deltaValues[i] = convertToInt64(deltaValue)
	}
	deltaIndex, prefixes, err := selectDeltaIndex(scyllaTable, equalityValues, deltaValues)
	if err != nil {
		return err
	}
	partition := convertToInt64(partitionValue)

	// The client hashes the records it holds whose last pinned value is one the read covered.
	metadata := cache.DeltaWindowMetadata{}
	if len(deltaIndex.pinned) > 0 {
		lastPinned := deltaIndex.pinned[len(deltaIndex.pinned)-1]
		metadata.FilterField = deltaFieldJSONName(sink, lastPinned)
		for _, prefix := range prefixes {
			if lastValue := prefix[len(prefix)-1]; !slices.Contains(metadata.FilterValues, lastValue) {
				metadata.FilterValues = append(metadata.FilterValues, lastValue)
			}
		}
	}

	// V for the client's next sync, on the clock that stamps updated: taken before any read, so
	// every write stamped before V − DeltaWriteLagTicks was visible to the reads below.
	metadata.ReadTime = getManagedUnixTime()
	cursor := deltaRead.Cursor
	plan := cache.PlanDeltaSync(cursor)
	// A first sync is a replace; any other cursor, however old, is decided by how much changed.
	metadata.IsReplace = cursor.Updated == 0
	projection := deltaProjection(tableInfo, scyllaTable, deltaIndex)
	recordIDOf := func(recordPointer unsafe.Pointer) int64 {
		return convertToInt64(scyllaTable.Keys[0].GetRawValue(recordPointer))
	}
	updatedOf := func(recordPointer unsafe.Pointer) int64 {
		return convertToInt64(scyllaTable.UpdatedCol.GetRawValue(recordPointer))
	}

	// The changed records and the moves of each prefix. A sync needs at most threshold + 1 of each
	// per prefix to know it is a replace: the view lists newest first, so an old cursor costs no more
	// than a recent one.
	reads := make([]deltaPrefixRead, len(prefixes))
	decisionLimit := deltaIndex.replaceThreshold + 1
	if !metadata.IsReplace {
		err := readDeltaPrefixes(prefixes, reads, func(prefix []int64, read *deltaPrefixRead) error {
			if err := readDeltaViewRecords(sink.collectInto(&read.records), scyllaTable, deltaIndex, partition, prefix,
				int64(plan.ChangedFrom), deltaViewUpdatedMax, decisionLimit, projection); err != nil {
				return err
			}
			var err error
			if len(deltaIndex.pinned) > 0 {
				read.movedIDs, err = readDeltaMovedIDs(scyllaTable, deltaIndex, partition, prefix, int64(plan.MovesFrom), decisionLimit)
			}
			return err
		})
		if err != nil {
			return err
		}
		changedCount, movedCount := 0, 0
		for _, read := range reads {
			changedCount += len(read.records)
			movedCount += len(read.movedIDs)
		}
		metadata.IsReplace = changedCount > deltaIndex.replaceThreshold || movedCount > deltaIndex.replaceThreshold
	}

	if metadata.IsReplace {
		// Every record of the prefixes, newest first across them, so a Limit keeps the most recently
		// written ones. Each prefix comes from its snapshot when that one still holds (delta_snapshot.go).
		reads = make([]deltaPrefixRead, len(prefixes))
		err := readDeltaPrefixes(prefixes, reads, func(prefix []int64, read *deltaPrefixRead) error {
			return readDeltaPrefixReplace(sink, scyllaTable, deltaIndex, partition, prefix, tableInfo.Limit, projection,
				metadata.ReadTime, read)
		})
		if err != nil {
			return err
		}
		var replaceRecords []unsafe.Pointer
		for _, read := range reads {
			replaceRecords = append(replaceRecords, read.records...)
		}
		slices.SortStableFunc(replaceRecords, func(left, right unsafe.Pointer) int {
			return cmp.Compare(updatedOf(right), updatedOf(left))
		})
		if tableInfo.Limit > 0 && len(replaceRecords) > int(tableInfo.Limit) {
			replaceRecords = replaceRecords[:tableInfo.Limit]
		}
		// One copy into an array of the final size, not log2(n) doublings: on a 50k first sync the
		// doublings allocated more than the records themselves. +1 is the metadata record.
		if sink.reserve != nil {
			sink.reserve(len(replaceRecords) + 1)
		}
		for _, recordPointer := range replaceRecords {
			metadata.NextCursor = max(metadata.NextCursor, int32(updatedOf(recordPointer)))
			sink.appendRecord(recordPointer)
		}
	} else {
		// The unsettled slice of every prefix: its rows are not resent, only hashed.
		err := readDeltaPrefixes(prefixes, reads, func(prefix []int64, read *deltaPrefixRead) error {
			var err error
			read.windowRows, err = readDeltaViewRows(scyllaTable, deltaIndex, partition, prefix,
				int64(plan.WindowFrom), int64(plan.WindowTo)-1)
			return err
		})
		if err != nil {
			return err
		}
		metadata.WindowFrom, metadata.WindowTo = plan.WindowFrom, plan.WindowTo
		metadata.NextCursor = cursor.Updated
		if sink.reserve != nil {
			changedRecordCount := 1 // the metadata record
			for _, read := range reads {
				changedRecordCount += len(read.records)
			}
			sink.reserve(changedRecordCount)
		}
		isInView := map[int64]bool{}
		for _, read := range reads {
			for _, viewRow := range read.windowRows {
				isInView[viewRow.recordID] = true
				metadata.WindowHash += cache.DeltaRecordHash(viewRow.recordID, viewRow.version)
			}
			for _, recordPointer := range read.records {
				isInView[recordIDOf(recordPointer)] = true
				metadata.NextCursor = max(metadata.NextCursor, int32(updatedOf(recordPointer)))
				sink.appendRecord(recordPointer)
			}
		}
		// A record that moved to another prefix the read covers is in the view again, with a newer
		// updated: it is not removed.
		for _, read := range reads {
			for _, movedID := range read.movedIDs {
				if !isInView[movedID] && !slices.Contains(metadata.RemovedIDs, movedID) {
					metadata.RemovedIDs = append(metadata.RemovedIDs, movedID)
				}
			}
		}
	}

	metadataRecord, err := deltaMetadataRecord(sink, scyllaTable, cache.EncodeDeltaWindowMetadata(metadata))
	if err != nil {
		return err
	}
	sink.appendRecord(metadataRecord)
	return nil
}

// deltaProjection is the caller's Select() projection plus what the client's window hash reads: the
// key, updated, version and the pinned columns. nil (every column) without a projection.
func deltaProjection(tableInfo *TableInfo, scyllaTable ScyllaTable, deltaIndex deltaIndexInfo) []string {
	if len(tableInfo.ColumnsInclude) == 0 && len(tableInfo.ColumnsExclude) == 0 {
		return nil
	}
	columnNames, _, _ := buildSelectProjection(tableInfo, scyllaTable)
	requiredNames := []string{scyllaTable.GetPartKey().GetName(), scyllaTable.Keys[0].GetName(), managedUpdatedColumnName, managedVersionColumnName}
	for _, pinnedColumn := range deltaIndex.pinned {
		requiredNames = append(requiredNames, pinnedColumn.GetName())
	}
	for _, requiredName := range requiredNames {
		if !slices.Contains(columnNames, requiredName) {
			columnNames = append(columnNames, requiredName)
		}
	}
	return columnNames
}

// deltaFieldJSONName is the JSON name the client reads a column's field by.
func deltaFieldJSONName(sink recordSink, column IColInfo) string {
	recordType := reflect.TypeOf(sink.asRecordPointer(sink.newRecord())).Elem()
	fieldName := column.GetInfo().FieldName
	field, isFound := recordType.FieldByName(fieldName)
	if !isFound {
		return fieldName
	}
	if jsonName, _, _ := strings.Cut(field.Tag.Get("json"), ","); jsonName != "" && jsonName != "-" {
		return jsonName
	}
	return fieldName
}

// deltaMetadataRecord is the Metadata record of a Delta() read: −1 as its key, the Metadata text in
// its `Metadata *string json:"mdt,omitempty"` field.
func deltaMetadataRecord(sink recordSink, scyllaTable ScyllaTable, metadataText string) (unsafe.Pointer, error) {
	record := sink.newRecord()
	metadataField := reflect.ValueOf(sink.asRecordPointer(record)).Elem().FieldByName("Metadata")
	if !metadataField.IsValid() || metadataField.Type() != reflect.TypeFor[*string]() {
		return nil, fmt.Errorf(`table %q: Delta() returns a Metadata record and the record has no field "Metadata *string" (json "mdt,omitempty")`, scyllaTable.Name)
	}
	metadataField.Set(reflect.ValueOf(&metadataText))
	keyColumn := scyllaTable.Keys[0]
	keyColumn.SetValue(record, coerceManagedIntegerValue(keyColumn, -1))
	return record, nil
}

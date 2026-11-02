package scylla

import (
	"fmt"
	"math/bits"
	"reflect"
	"slices"
	"strings"
	"unsafe"

	"github.com/gocql/gocql"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
)

// ─────────────────────────────────────────────────────────────────────────────
// TypeDelta on Scylla: the delta window protocol (db/cache delta_window.go).
//
// Each TypeDelta index is a slim materialized view of the record's ID, updated,
// version and sync filter column, keyed so one pinned prefix is a contiguous
// range ordered by updated:
//
//	nothing pinned   PRIMARY KEY ((partition), updated, id)
//	pinned Keys      PRIMARY KEY ((partition), zz_dlt_<tag>, id)
//	                 zz_dlt_<tag> = pinned values … | updated (31 bits), one bigint
//
// Scylla allows a single non-key base column in a view key, hence the packing.
// The view is not part of the query planner: only Delta() reads it.
//
//   - Write: before the CQL, the stored version and pinned values of the written keys
//     are read (one "key IN" select per partition). Each record is stamped with the
//     next Version, and its packed column with the pinned values it ends with: a
//     partial update that leaves a pinned column out keeps the stored value. After
//     the CQL, a record whose pinned prefix changed writes one row, under its old
//     prefix, to <table>__delta_moves.
//   - Read: Delta() reads the view slice of the prefix from the hashed window on,
//     the moves slice of the same window, and the changed records by ID.
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
	// pinned are the Keys before the last: Delta() needs an equality on each.
	pinned         []IColInfo
	pinnedSlotBits []int64
	// syncFilter is the last Key; nil on a keyless index.
	syncFilter IColInfo
	// packedColumn packs the pinned values and updated; nil when nothing is pinned, and the view keys
	// on updated itself.
	packedColumn *columnInfo
	viewName     string
	// replaceThreshold is how many changed records a sync reads by ID before it answers a replace.
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

// compileSchemaDeltaView registers a TypeDelta index and its view. The last Key is the sync filter
// column, the others are pinned, and the table's single integer key is the record ID.
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
		deltaIndex.pinned = declaredColumns[:len(declaredColumns)-1]
		deltaIndex.syncFilter = declaredColumns[len(declaredColumns)-1]
	}
	deltaIndex.viewName = fmt.Sprintf("%v__dlt_%v_view", dbTable.Name, deltaIndex.tag)

	if len(deltaIndex.pinned) > 0 {
		deltaIndex.pinnedSlotBits = planDeltaPinnedSlots(dbTable, deltaIndex.pinned, indexCfg)
		packedColumn := &columnInfo{
			ColInfo: colInfo{IsVirtual: true, Idx: dbTable.MaxColIdx},
			ColType: colType{FieldType: "int64", DBType: "bigint"},
		}
		packedColumn.GetInfo().Name = "zz_dlt_" + strings.Join(declaredKeyNames[:len(declaredKeyNames)-1], "_")
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
	}

	for _, registered := range dbTable.deltaIndexes {
		if registered.viewName == deltaIndex.viewName {
			panic(fmt.Sprintf(`Table "%v": two TypeDelta indexes declare the same Keys (%v)`, dbTable.Name, deltaIndex.tag))
		}
	}
	dbTable.deltaIndexes = append(dbTable.deltaIndexes, deltaIndex)
	fmt.Printf("Delta index registered: table=%s view=%s pinnedSlotBits=%v\n", dbTable.Name, deltaIndex.viewName, deltaIndex.pinnedSlotBits)
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

// deltaViewCreateScript is the CREATE of an index's view. The view is ordered newest first: a sync
// reads the recent end of its prefix.
func deltaViewCreateScript(scyllaTable ScyllaTable, deltaIndex deltaIndexInfo) string {
	partitionName, keyName := scyllaTable.GetPartKey().GetName(), scyllaTable.Keys[0].GetName()
	orderColumnName := managedUpdatedColumnName
	selectNames := []string{partitionName, keyName, managedUpdatedColumnName, managedVersionColumnName}
	if deltaIndex.packedColumn != nil {
		orderColumnName = deltaIndex.packedColumn.GetName()
		selectNames = append(selectNames, orderColumnName)
	}
	if deltaIndex.syncFilter != nil {
		selectNames = append(selectNames, deltaIndex.syncFilter.GetName())
	}
	return fmt.Sprintf(`CREATE MATERIALIZED VIEW %v.%v AS
		SELECT %v FROM %v
		WHERE %v IS NOT NULL AND %v IS NOT NULL AND %v IS NOT NULL
		PRIMARY KEY ((%v), %v, %v)
		WITH CLUSTERING ORDER BY (%v DESC, %v ASC)
		AND gc_grace_seconds = %d;`,
		resolveKeyspace(scyllaTable), deltaIndex.viewName, strings.Join(selectNames, ", "), scyllaTable.GetFullName(),
		partitionName, orderColumnName, keyName,
		partitionName, orderColumnName, keyName,
		orderColumnName, keyName, deltaViewGCGraceSeconds)
}

func deltaMovesTableName(scyllaTable ScyllaTable) string { return scyllaTable.Name + "__delta_moves" }

// hasDeltaMoves reports whether any TypeDelta index pins a column, so its records can move.
func hasDeltaMoves(scyllaTable ScyllaTable) bool {
	return slices.ContainsFunc(scyllaTable.deltaIndexes, func(deltaIndex deltaIndexInfo) bool { return deltaIndex.packedColumn != nil })
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

// readStoredWrittenRows reads, in one select per partition, what the delta indexes and the DataFrames
// need of the records about to be written: the version and the pinned columns, and the whole row for
// the frames (a ToFrames method may read any field). nil when the table has neither. The keys must be
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
				if deltaIndex.packedColumn == nil {
					continue
				}
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

// selectDeltaIndex picks the TypeDelta index whose pinned Keys all have an equality, the one pinning
// the most when several fit. needsSyncFilter skips a keyless index.
func selectDeltaIndex(scyllaTable ScyllaTable, equalityValues map[string]any, needsSyncFilter bool) (deltaIndexInfo, error) {
	selectedPosition, selectedPinnedCount, isAmbiguous := -1, -1, false
	for position, deltaIndex := range scyllaTable.deltaIndexes {
		if needsSyncFilter && deltaIndex.syncFilter == nil {
			continue
		}
		isEveryKeyPinned := !slices.ContainsFunc(deltaIndex.pinned, func(column IColInfo) bool {
			_, isPinned := equalityValues[column.GetName()]
			return !isPinned
		})
		if !isEveryKeyPinned {
			continue
		}
		switch {
		case len(deltaIndex.pinned) > selectedPinnedCount:
			selectedPosition, selectedPinnedCount, isAmbiguous = position, len(deltaIndex.pinned), false
		case len(deltaIndex.pinned) == selectedPinnedCount:
			isAmbiguous = true
		}
	}
	if selectedPosition < 0 {
		return deltaIndexInfo{}, fmt.Errorf(`table %q: Delta() found no TypeDelta index whose pinned Keys all have an equality (and a sync filter column when values are given)`, scyllaTable.Name)
	}
	if isAmbiguous {
		return deltaIndexInfo{}, fmt.Errorf(`table %q: Delta() fits several TypeDelta indexes equally: pin one more Keys column, or drop an index`, scyllaTable.Name)
	}
	return scyllaTable.deltaIndexes[selectedPosition], nil
}

// deltaViewRow is one row of a delta view.
type deltaViewRow struct {
	recordID    int64
	updated     int64
	version     int16
	filterValue int64
}

// deltaViewUpdatedMax is the highest updated a view slot holds: an upper bound meaning "up to now".
var deltaViewUpdatedMax = int64(db.SlotMaxValue(deltaUpdatedSlotBits))

// readDeltaViewRows reads a prefix's view rows with updated in [fromUpdated, toUpdated], newest first.
// rowLimit > 0 stops the read there.
func readDeltaViewRows(
	scyllaTable ScyllaTable, deltaIndex deltaIndexInfo, partition int64, pinnedValues []int64,
	fromUpdated, toUpdated int64, rowLimit int,
) ([]deltaViewRow, error) {
	if toUpdated < fromUpdated {
		return nil, nil
	}
	keyName := scyllaTable.Keys[0].GetName()
	selectNames := []string{keyName, managedUpdatedColumnName, managedVersionColumnName}
	if deltaIndex.syncFilter != nil {
		selectNames = append(selectNames, deltaIndex.syncFilter.GetName())
	}
	queryString := fmt.Sprintf("SELECT %v FROM %v.%v WHERE %v = ?", strings.Join(selectNames, ", "),
		resolveKeyspace(scyllaTable), deltaIndex.viewName, scyllaTable.GetPartKey().GetName())
	queryValues := []any{coerceManagedIntegerValue(scyllaTable.GetPartKey(), partition)}
	if deltaIndex.packedColumn == nil {
		queryString += fmt.Sprintf(" AND %v >= ? AND %v <= ?", managedUpdatedColumnName, managedUpdatedColumnName)
		queryValues = append(queryValues, int32(fromUpdated), int32(toUpdated))
	} else {
		packedName := deltaIndex.packedColumn.GetName()
		queryString += fmt.Sprintf(" AND %v >= ? AND %v <= ?", packedName, packedName)
		queryValues = append(queryValues,
			deltaIndex.packedKeyValue(scyllaTable.Name, pinnedValues, fromUpdated),
			deltaIndex.packedKeyValue(scyllaTable.Name, pinnedValues, toUpdated))
	}
	if rowLimit > 0 {
		queryString += fmt.Sprintf(" LIMIT %d", rowLimit)
	}

	var viewRows []deltaViewRow
	iterator := getScyllaConnection().Query(queryString, queryValues...).Consistency(gocql.LocalQuorum).Iter()
	for {
		row := map[string]any{}
		if !iterator.MapScan(row) {
			break
		}
		viewRow := deltaViewRow{
			recordID: convertToInt64(row[keyName]),
			updated:  convertToInt64(row[managedUpdatedColumnName]),
			version:  int16(convertToInt64(row[managedVersionColumnName])),
		}
		if deltaIndex.syncFilter != nil {
			viewRow.filterValue = convertToInt64(row[deltaIndex.syncFilter.GetName()])
		}
		viewRows = append(viewRows, viewRow)
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

// execDelta is Exec for a Delta() read (db/cache delta_window.go describes the protocol).
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
	deltaIndex, err := selectDeltaIndex(scyllaTable, equalityValues, len(deltaRead.SyncFilterValues) > 0)
	if err != nil {
		return err
	}
	partition := convertToInt64(partitionValue)
	pinnedValues := make([]int64, len(deltaIndex.pinned))
	for i, pinnedColumn := range deltaIndex.pinned {
		pinnedValues[i] = convertToInt64(equalityValues[pinnedColumn.GetName()])
	}
	filterValues := make([]int64, len(deltaRead.SyncFilterValues))
	for i, filterValue := range deltaRead.SyncFilterValues {
		filterValues[i] = convertToInt64(filterValue)
	}
	// A row counts for a replace and for the hash when its sync filter value was asked for.
	isFilterMatch := func(viewRow deltaViewRow) bool {
		return len(filterValues) == 0 || slices.Contains(filterValues, viewRow.filterValue)
	}

	metadata := cache.DeltaWindowMetadata{FilterValues: filterValues}
	if deltaIndex.syncFilter != nil && len(filterValues) > 0 {
		metadata.FilterField = deltaFieldJSONName(sink, deltaIndex.syncFilter)
	}

	cursor := int64(deltaRead.CursorUpdated)
	windowFrom, windowTo := cursor-cache.DeltaWindowTicks, cursor-cache.DeltaWindowMarginTicks
	// A first sync is a replace; any other cursor, however old, is decided by how much changed.
	metadata.IsReplace = cursor == 0

	var viewRows []deltaViewRow
	var idsToRead []int64
	var movedIDs []int64
	// A sync needs at most threshold + 1 rows of each kind to know it is a replace: the view lists
	// newest first, so the changed rows come first and an old cursor costs no more than a recent one.
	decisionLimit := deltaIndex.replaceThreshold + 1
	if !metadata.IsReplace {
		changedRows, err := readDeltaViewRows(scyllaTable, deltaIndex, partition, pinnedValues, max(windowTo, 0), deltaViewUpdatedMax, decisionLimit)
		if err != nil {
			return err
		}
		if deltaIndex.packedColumn != nil {
			if movedIDs, err = readDeltaMovedIDs(scyllaTable, deltaIndex, partition, pinnedValues, max(windowFrom, 0), decisionLimit); err != nil {
				return err
			}
		}
		metadata.IsReplace = len(changedRows) > deltaIndex.replaceThreshold || len(movedIDs) > deltaIndex.replaceThreshold
		if !metadata.IsReplace {
			// The hashed window [windowFrom, windowTo): its rows are not resent, only hashed.
			windowRows, err := readDeltaViewRows(scyllaTable, deltaIndex, partition, pinnedValues, max(windowFrom, 0), windowTo-1, 0)
			if err != nil {
				return err
			}
			for _, changedRow := range changedRows {
				idsToRead = append(idsToRead, changedRow.recordID)
			}
			viewRows = append(changedRows, windowRows...)
		}
	}

	if metadata.IsReplace {
		if viewRows, err = readDeltaViewRows(scyllaTable, deltaIndex, partition, pinnedValues, 0, deltaViewUpdatedMax, 0); err != nil {
			return err
		}
		idsToRead = idsToRead[:0]
		// The view lists newest updated first, so a Limit keeps the most recently written records.
		for _, viewRow := range viewRows {
			if tableInfo.Limit > 0 && len(idsToRead) == int(tableInfo.Limit) {
				break
			}
			if isFilterMatch(viewRow) {
				idsToRead = append(idsToRead, viewRow.recordID)
			}
		}
	} else {
		metadata.WindowFrom, metadata.WindowTo = int32(windowFrom), int32(windowTo)
		isInView := map[int64]bool{}
		for _, viewRow := range viewRows {
			isInView[viewRow.recordID] = true
			if viewRow.updated >= windowFrom && viewRow.updated < windowTo && isFilterMatch(viewRow) {
				metadata.WindowHash += cache.DeltaRecordHash(viewRow.recordID, viewRow.version)
			}
		}
		// A record that moved back is in the view again, with a newer updated.
		for _, movedID := range movedIDs {
			if !isInView[movedID] && !slices.Contains(metadata.RemovedIDs, movedID) {
				metadata.RemovedIDs = append(metadata.RemovedIDs, movedID)
			}
		}
	}

	for _, viewRow := range viewRows {
		metadata.NextCursor = max(metadata.NextCursor, int32(viewRow.updated))
	}
	if !metadata.IsReplace {
		metadata.NextCursor = max(metadata.NextCursor, int32(cursor))
	}

	if len(idsToRead) > 0 {
		slices.Sort(idsToRead)
		if err := readRecordsByIDs(sink, scyllaTable, map[int64][]int64{partition: idsToRead}, deltaProjection(tableInfo, scyllaTable, deltaIndex)); err != nil {
			return err
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
// key, updated, version and the sync filter. nil (every column) without a projection.
func deltaProjection(tableInfo *TableInfo, scyllaTable ScyllaTable, deltaIndex deltaIndexInfo) []string {
	if len(tableInfo.ColumnsInclude) == 0 && len(tableInfo.ColumnsExclude) == 0 {
		return nil
	}
	columnNames, _, _ := buildSelectProjection(tableInfo, scyllaTable)
	requiredNames := []string{scyllaTable.GetPartKey().GetName(), scyllaTable.Keys[0].GetName(), managedUpdatedColumnName, managedVersionColumnName}
	if deltaIndex.syncFilter != nil {
		requiredNames = append(requiredNames, deltaIndex.syncFilter.GetName())
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

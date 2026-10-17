package scylla

import (
	"fmt"
	"math/bits"
	"slices"
	"strings"
)

// columnValueRange is the inclusive range a column was declared to hold via FixedValues.
type columnValueRange struct {
	minValue int64
	maxValue int64
	// declaredValues enumerates every value the column may hold. It is what lets the select planner
	// fan a query out over an unconstrained key column instead of falling back to a table scan. It
	// stays nil for a range too wide to be worth enumerating, which is exactly the case where the
	// fan-out would cost more than it saves.
	declaredValues []int64
}

// maxEnumerableFixedValues caps how wide a Min/Max declaration may be before its values stop being
// enumerated. It sits above the planner's own fan-out ceiling so a range that just misses the cut
// is still visible to any future caller that can afford a wider fan-out.
const maxEnumerableFixedValues = 32

// enumerateFixedValues lists every value a FixedValues entry pins down, preferring the explicit
// list over the Min/Max span. A span wider than maxEnumerableFixedValues yields nil: the column is
// still range-bounded for slot sizing, just not enumerable.
func enumerateFixedValues(declared FixedValues, minValue, maxValue int64) []int64 {
	if len(declared.Values) > 0 {
		return slices.Clone(declared.Values)
	}
	if maxValue-minValue+1 > maxEnumerableFixedValues {
		return nil
	}
	values := make([]int64, 0, maxValue-minValue+1)
	for value := minValue; value <= maxValue; value++ {
		values = append(values, value)
	}
	return values
}

// resolveFixedValueRanges validates the schema's FixedValues and indexes them by column name.
// Packed keys are non-negative by construction, so a negative bound is rejected here rather than
// panicking deep inside the packing arithmetic on the first write.
func resolveFixedValueRanges(dbTable *ScyllaTable, declaredFixedValues []FixedValues) map[string]columnValueRange {
	if len(declaredFixedValues) == 0 {
		return nil
	}

	valueRanges := make(map[string]columnValueRange, len(declaredFixedValues))
	for _, declared := range declaredFixedValues {
		if declared.Col == nil {
			panic(fmt.Sprintf(`Table "%v": FixedValues entry must name a Col`, dbTable.Name))
		}
		columnName := declared.Col.GetInfo().Name
		column := dbTable.ColumnsMap[columnName]
		if column == nil || column.IsNil() {
			panic(fmt.Sprintf(`Table "%v": FixedValues column "%v" was not found`, dbTable.Name, columnName))
		}
		if _, isRepeated := valueRanges[columnName]; isRepeated {
			panic(fmt.Sprintf(`Table "%v": FixedValues declares column "%v" twice`, dbTable.Name, columnName))
		}

		minValue, maxValue, isDeclared := declared.Bounds()
		if !isDeclared {
			panic(fmt.Sprintf(`Table "%v": FixedValues for "%v" must declare Values or a Max`, dbTable.Name, columnName))
		}
		if minValue < 0 {
			panic(fmt.Sprintf(`Table "%v": FixedValues for "%v" must not be negative. Got min: %v`,
				dbTable.Name, columnName, minValue))
		}
		if maxValue < minValue {
			panic(fmt.Sprintf(`Table "%v": FixedValues for "%v" has Max %v below Min %v`,
				dbTable.Name, columnName, maxValue, minValue))
		}

		valueRanges[columnName] = columnValueRange{
			minValue:       minValue,
			maxValue:       maxValue,
			declaredValues: enumerateFixedValues(declared, minValue, maxValue),
		}
	}
	return valueRanges
}

// compileSchemaDeltaView registers a TypeDelta index: its change log (delta_log.go) and the view a
// first sync lists. The last Key is the sync filter column, the others are pinned (each pinned value
// has its own log), and the table's single integer key is the log ID. The view is keyed on the
// declared Keys, packed into one column when there are several; a keyless index has no view, since
// its first sync lists the whole partition.
func compileSchemaDeltaView(dbTable *ScyllaTable, indexCfg Index) {
	if dbTable.VersionCol == nil || dbTable.VersionCol.IsNil() {
		panic(fmt.Sprintf(`Table "%v": TypeDelta requires the managed "%v" column; declare Version in the record and table structs`,
			dbTable.Name, managedVersionColumnName))
	}
	if partitionColumn := dbTable.GetPartKey(); partitionColumn == nil || partitionColumn.IsNil() ||
		!isIntegerFieldType(partitionColumn.GetType().FieldType) {
		panic(fmt.Sprintf(`Table "%v": TypeDelta requires an integer partition column: each partition has its own log`, dbTable.Name))
	}
	if len(dbTable.Keys) != 1 || !isIntegerFieldType(dbTable.Keys[0].GetType().FieldType) {
		panic(fmt.Sprintf(`Table "%v": TypeDelta requires exactly one integer key column, the record's log ID. A table keyed otherwise answers its route with db.DeltaReplaceMetadata() instead`,
			dbTable.Name))
	}

	deltaIndex := deltaIndexInfo{tag: "all", is64BitScale: dbTable.Keys[0].GetType().FieldType == "int64"}
	if len(indexCfg.Keys) == 0 {
		dbTable.deltaIndexes = append(dbTable.deltaIndexes, deltaIndex)
		fmt.Printf("Delta index registered: table=%s keys=[] (lists the partition)\n", dbTable.Name)
		return
	}

	declaredKeyNames := make([]string, 0, len(indexCfg.Keys))
	declaredKeyBits := make([]int64, 0, len(indexCfg.Keys))
	declaredColumns := make([]IColInfo, 0, len(indexCfg.Keys))
	forcesInt32 := false
	// elasticKeyIndex is the one key, if any, whose width is left for planDeltaSlots to derive.
	elasticKeyIndex := -1

	for _, declaredKey := range indexCfg.Keys {
		keyConfig := declaredKey.GetInfo()
		column := dbTable.ColumnsMap[keyConfig.Name]
		if column == nil || column.IsNil() {
			panic(fmt.Sprintf(`Table "%v": TypeDelta column "%v" was not found`, dbTable.Name, keyConfig.Name))
		}
		if column.GetType().IsComplexType || column.GetType().IsSlice ||
			!isSupportedPackedIndexNumericFieldType(column.GetType().FieldType) {
			panic(fmt.Sprintf(`Table "%v": TypeDelta key "%v" must be a scalar integer. Found: %v`,
				dbTable.Name, column.GetName(), column.GetType().FieldType))
		}
		if column.GetName() == dbTable.Keys[0].GetName() {
			panic(fmt.Sprintf(`Table "%v": TypeDelta key "%v" is the table's key, which is the log ID`, dbTable.Name, column.GetName()))
		}

		if keyConfig.UseInt32Packing {
			forcesInt32 = true
		}

		// An explicit Size() stays available as the escape hatch; otherwise the slot width comes from
		// the declared value range. A key with neither is elastic: it absorbs whatever bits the rest
		// of the layout leaves over.
		keyBits := int64(keyConfig.SlotBits)
		if keyBits <= 0 {
			if valueRange, isDeclared := dbTable.fixedValueRanges[column.GetName()]; isDeclared {
				keyBits = max(1, int64(bits.Len64(uint64(valueRange.maxValue))))
			} else {
				// Two elastic keys would have no unambiguous split of the remainder.
				if elasticKeyIndex >= 0 {
					panic(fmt.Sprintf(`Table "%v": TypeDelta keys "%v" and "%v" both lack a FixedValues entry and a Size; only one key can absorb the bit remainder`,
						dbTable.Name, declaredKeyNames[elasticKeyIndex], column.GetName()))
				}
				elasticKeyIndex = len(declaredKeyNames)
				keyBits = 0
			}
		}

		declaredKeyNames = append(declaredKeyNames, column.GetName())
		declaredKeyBits = append(declaredKeyBits, keyBits)
		declaredColumns = append(declaredColumns, column)
	}

	deltaIndex.tag = strings.Join(declaredKeyNames, "-")
	deltaIndex.pinned = declaredColumns[:len(declaredColumns)-1]
	deltaIndex.syncFilter = declaredColumns[len(declaredColumns)-1]
	dbTable.deltaIndexes = append(dbTable.deltaIndexes, deltaIndex)

	listingCfg := indexCfg
	listingCfg.Type = TypeView
	if len(declaredColumns) == 1 {
		fmt.Printf("Delta index registered: table=%s keys=[%s]\n", dbTable.Name, declaredKeyNames[0])
		compileSchemaView(dbTable, listingCfg, nil)
		return
	}

	plan := planDeltaSlots(dbTable.Name, declaredKeyNames, declaredKeyBits, forcesInt32, elasticKeyIndex)
	fmt.Printf("Delta index registered: table=%s keys=[%s] slotBits=%v int32=%v\n",
		dbTable.Name, strings.Join(declaredKeyNames, ","), plan.slotBitsPerColumn, plan.useInt32)
	compileSchemaView(dbTable, listingCfg, &plan)
}

// deltaSlotPlan is the resolved bit layout of a TypeDelta packed listing view. It is handed to
// compileSchemaView so the generic view compiler skips its own Size-based derivation.
type deltaSlotPlan struct {
	slotBitsPerColumn []int64
	useInt32          bool
}

// planDeltaSlots settles the bit layout and storage width of a delta listing view: an int32 when the
// keys fit its 32 bits, a bigint otherwise. An elastic key (elasticKeyIndex >= 0) takes every bit the
// others leave of a bigint.
func planDeltaSlots(tableName string, declaredKeyNames []string, declaredKeyBits []int64,
	forcesInt32 bool, elasticKeyIndex int) deltaSlotPlan {

	if elasticKeyIndex >= 0 {
		if forcesInt32 {
			panic(fmt.Sprintf(`Table "%v": TypeDelta cannot pack into an int32 while key "%v" has no declared range. Drop .Int32(), or give "%v" a FixedValues entry or a Size.`,
				tableName, declaredKeyNames[elasticKeyIndex], declaredKeyNames[elasticKeyIndex]))
		}
		// The elastic slot is still 0 here, so the sum covers only the other keys.
		remainingBits := packedVirtualInt64Bits - sumSlotBits(declaredKeyBits, 0)
		if remainingBits < 1 {
			panic(fmt.Sprintf(`Table "%v": TypeDelta leaves no bits for key "%v": the other keys already fill the %v an int64 packed key holds. Narrow a FixedValues range.`,
				tableName, declaredKeyNames[elasticKeyIndex], packedVirtualInt64Bits))
		}
		declaredKeyBits[elasticKeyIndex] = remainingBits
		return deltaSlotPlan{slotBitsPerColumn: slices.Clone(declaredKeyBits)}
	}

	declaredBits := sumSlotBits(declaredKeyBits, 0)
	if declaredBits <= packedVirtualInt32Bits {
		return deltaSlotPlan{slotBitsPerColumn: slices.Clone(declaredKeyBits), useInt32: true}
	}
	if forcesInt32 {
		panic(fmt.Sprintf(`Table "%v": TypeDelta cannot pack into an int32: the keys take %v bits, past the %v an int holds. Drop .Int32() or narrow a FixedValues range.`,
			tableName, declaredBits, packedVirtualInt32Bits))
	}
	if declaredBits > packedVirtualInt64Bits {
		panic(fmt.Sprintf(`Table "%v": TypeDelta keys take %v bits, past the %v an int64 packed key holds. Narrow a FixedValues range.`,
			tableName, declaredBits, packedVirtualInt64Bits))
	}
	return deltaSlotPlan{slotBitsPerColumn: slices.Clone(declaredKeyBits)}
}

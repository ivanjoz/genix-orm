package scylla

import (
	"fmt"
	"math/bits"
	"slices"
	"strings"

	"github.com/ivanjoz/genix-orm/db"
)

// deltaVersionBitsInt32 is the bit slot the implicit "updated_version" key gets when the packed
// column fits an int. 27 bits leave 5 of the int's 32 to the declared keys (32 value combinations);
// it caps the table at 2^27 (~134M) write calls per partition, after which writes fail loudly (see
// the maxDeltaVersionValue check in fetchManagedCounterValues).
const deltaVersionBitsInt32 = 27

// deltaVersionBitsInt64 is the slot used once the bit budget has already forced a bigint. The
// extra bits are paid for either way, so they are spent on sequence headroom.
const deltaVersionBitsInt64 = 34

// deltaVersionBitsElastic is the slot the version gets when one key was left undeclared and
// absorbs the bit remainder. It sits between the two fixed widths on purpose: the layout is already
// a bigint, and 30 bits for the sequence still leave the elastic key and the others 34.
const deltaVersionBitsElastic = 30

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

// deltaSlotPlan is the resolved bit layout of a TypeDelta packed column. It is handed to
// compileSchemaView so the generic view compiler skips its own Size-based derivation.
type deltaSlotPlan struct {
	slotBitsPerColumn []int64
	useInt32          bool
}

// deltaKeyColumn adapts a resolved table column back into the Coln shape a schema index declares,
// so TypeDelta can append the implicit "updated_version" key it never receives from the schema.
type deltaKeyColumn struct {
	info columnInfo
}

func (c deltaKeyColumn) GetInfo() columnInfo { return c.info }
func (c deltaKeyColumn) GetName() string     { return c.info.Name }

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

// compileSchemaDeltaView expands a TypeDelta declaration into the packed range view that backs it:
// the declared keys plus the table's "updated_version" column, with every bit slot resolved up
// front.
func compileSchemaDeltaView(dbTable *ScyllaTable, indexCfg Index) {
	versionColumn := dbTable.UpdatedVersionCol
	if versionColumn == nil || versionColumn.IsNil() {
		panic(fmt.Sprintf(`Table "%v": TypeDelta requires the managed "%v" column; declare UpdatedVersion in the record and table structs`,
			dbTable.Name, managedUpdatedVersionColumnName))
	}

	versionKeyColumn := func(slotBits int8) deltaKeyColumn {
		versionKey := deltaKeyColumn{}
		versionKey.info.Name = versionColumn.GetName()
		versionKey.info.SlotBits = slotBits
		return versionKey
	}

	// With no declared keys there is nothing to pack: the view degenerates to a plain one on
	// "updated_version" within the partition, which is all a sync that filters nothing but its
	// watermark needs. No packing also means no bit slot, so writes keep the column's full int32
	// range instead of a trimmed budget. Delta() must then be called with no filter values —
	// resolveDeltaSyncFilterColumn rejects the alternative.
	if len(indexCfg.Keys) == 0 {
		watermarkOnlyCfg := indexCfg
		watermarkOnlyCfg.Type = TypeView
		watermarkOnlyCfg.Keys = []Coln{versionKeyColumn(0)}

		fmt.Printf("Delta view registered: table=%s keys=[%s] watermarkOnly=true\n",
			dbTable.Name, versionColumn.GetName())

		compileSchemaView(dbTable, watermarkOnlyCfg, nil)
		return
	}

	declaredKeyNames := make([]string, 0, len(indexCfg.Keys))
	declaredKeyBits := make([]int64, 0, len(indexCfg.Keys))
	forcesInt32 := false
	// elasticKeyIndex is the one key, if any, whose width is left for planDeltaSlots to derive.
	elasticKeyIndex := -1

	for _, declaredKey := range indexCfg.Keys {
		keyConfig := declaredKey.GetInfo()
		if keyConfig.Name == versionColumn.GetName() {
			panic(fmt.Sprintf(`Table "%v": TypeDelta appends "%v" implicitly; remove it from Keys`,
				dbTable.Name, versionColumn.GetName()))
		}

		column := dbTable.ColumnsMap[keyConfig.Name]
		if column == nil || column.IsNil() {
			panic(fmt.Sprintf(`Table "%v": TypeDelta column "%v" was not found`, dbTable.Name, keyConfig.Name))
		}
		if column.GetType().IsComplexType || column.GetType().IsSlice ||
			!isSupportedPackedIndexNumericFieldType(column.GetType().FieldType) {
			panic(fmt.Sprintf(`Table "%v": TypeDelta key "%v" must be a scalar integer. Found: %v`,
				dbTable.Name, column.GetName(), column.GetType().FieldType))
		}

		if keyConfig.UseInt32Packing {
			forcesInt32 = true
		}

		// An explicit Size() stays available as the escape hatch; otherwise the slot width comes from
		// the declared value range, which is the whole point of TypeDelta. A key with neither is
		// elastic: it absorbs whatever bits the rest of the layout leaves over, which is how a column
		// with no natural ceiling (an autoincrement id) can still be a delta key.
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
	}

	plan := planDeltaSlots(dbTable.Name, declaredKeyNames, declaredKeyBits, forcesInt32, elasticKeyIndex)

	// The implicit key carries only a name and its bit width; the view compiler resolves the rest
	// against the base table.
	versionKey := versionKeyColumn(int8(plan.slotBitsPerColumn[len(plan.slotBitsPerColumn)-1]))

	// Writes must refuse a version wider than its slot, before the packer panics on it: the check in
	// fetchManagedCounterValues fails the write with the counter's name.
	dbTable.maxDeltaVersionValue = int64(db.SlotMaxValue(int64(versionKey.info.SlotBits)))

	deltaCfg := indexCfg
	deltaCfg.Type = TypeView
	deltaCfg.Keys = append(slices.Clone(indexCfg.Keys), versionKey)

	packedColumnTypeName := "int64"
	if plan.useInt32 {
		packedColumnTypeName = "int32"
	}
	fmt.Printf("Delta view registered: table=%s keys=[%s,%s] slotBits=%v packedType=%s\n",
		dbTable.Name, strings.Join(declaredKeyNames, ","), versionColumn.GetName(),
		plan.slotBitsPerColumn, packedColumnTypeName)

	compileSchemaView(dbTable, deltaCfg, &plan)
}

// planDeltaSlots settles the bit layout and storage width of a delta packed column. The declared
// keys keep the widths they were resolved with; only the trailing "updated_version" slot flexes,
// widening once the budget has already spilled past int32. Key order does not change whether a
// layout fits: every slot is a whole number of bits, so only their sum counts.
//
// An elastic key (elasticKeyIndex >= 0) inverts that: the version slot is pinned to
// deltaVersionBitsElastic and the elastic key takes every bit the others leave, which always
// means a bigint.
func planDeltaSlots(tableName string, declaredKeyNames []string, declaredKeyBits []int64,
	forcesInt32 bool, elasticKeyIndex int) deltaSlotPlan {

	if elasticKeyIndex >= 0 {
		if forcesInt32 {
			panic(fmt.Sprintf(`Table "%v": TypeDelta cannot pack into an int32 while key "%v" has no declared range. Drop .Int32(), or give "%v" a FixedValues entry or a Size.`,
				tableName, declaredKeyNames[elasticKeyIndex], declaredKeyNames[elasticKeyIndex]))
		}

		// The elastic slot is still 0 here, so the sum covers only the other keys.
		remainingBits := packedVirtualInt64Bits - deltaVersionBitsElastic - sumSlotBits(declaredKeyBits, 0)
		if remainingBits < 1 {
			panic(fmt.Sprintf(`Table "%v": TypeDelta leaves no bits for key "%v": the other keys and the %v-bit version slot already fill the %v an int64 packed key holds. Narrow a FixedValues range.`,
				tableName, declaredKeyNames[elasticKeyIndex], deltaVersionBitsElastic, packedVirtualInt64Bits))
		}
		declaredKeyBits[elasticKeyIndex] = remainingBits
		return deltaSlotPlan{slotBitsPerColumn: append(slices.Clone(declaredKeyBits), deltaVersionBitsElastic)}
	}

	declaredBits := sumSlotBits(declaredKeyBits, 0)
	if declaredBits+deltaVersionBitsInt32 <= packedVirtualInt32Bits {
		return deltaSlotPlan{slotBitsPerColumn: append(slices.Clone(declaredKeyBits), deltaVersionBitsInt32), useInt32: true}
	}
	if forcesInt32 {
		panic(fmt.Sprintf(`Table "%v": TypeDelta cannot pack into an int32: the keys take %v bits and the version %v, past the %v an int holds. Drop .Int32() or narrow a FixedValues range.`,
			tableName, declaredBits, deltaVersionBitsInt32, packedVirtualInt32Bits))
	}

	// int64 was unavoidable, so spend the spare bits on sequence headroom.
	if declaredBits+deltaVersionBitsInt64 > packedVirtualInt64Bits {
		panic(fmt.Sprintf(`Table "%v": TypeDelta keys take %v bits, which with the %v-bit version slot exceed the %v an int64 packed key holds. Narrow a FixedValues range.`,
			tableName, declaredBits, deltaVersionBitsInt64, packedVirtualInt64Bits))
	}
	return deltaSlotPlan{slotBitsPerColumn: append(slices.Clone(declaredKeyBits), deltaVersionBitsInt64)}
}

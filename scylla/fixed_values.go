package scylla

import (
	"fmt"
	"math/bits"
	"slices"
)

// fixedValueSlotPlan is the bit layout of a packed TypeView sized from FixedValues instead of Size().
type fixedValueSlotPlan struct {
	slotBitsPerColumn []int64
	useInt32          bool
}

// planFixedValueViewSlots sizes a multi-key TypeView from FixedValues: each key takes the bits of
// its declared maximum, and at most one key with no declared range takes what a bigint leaves. It
// packs into an int when the slots fit 32 bits. nil when the view isn't eligible: a single key, a
// Size() hint on any key (the Size() rules apply), or no declared range at all.
func planFixedValueViewSlots(dbTable *ScyllaTable, viewCfg Index) *fixedValueSlotPlan {
	if len(viewCfg.Keys) < 2 {
		return nil
	}
	slotBits := make([]int64, len(viewCfg.Keys))
	elasticIndex, forcesInt32 := -1, false
	for i, declaredKey := range viewCfg.Keys {
		keyConfig := declaredKey.GetInfo()
		if keyConfig.SlotBits > 0 {
			return nil
		}
		forcesInt32 = forcesInt32 || keyConfig.UseInt32Packing
		if valueRange, isDeclared := dbTable.fixedValueRanges[keyConfig.Name]; isDeclared {
			slotBits[i] = max(1, int64(bits.Len64(uint64(valueRange.maxValue))))
		} else if elasticIndex >= 0 {
			return nil // two keys without a range: no unambiguous split, the view stays a hash view
		} else {
			elasticIndex = i
		}
	}
	if elasticIndex >= 0 {
		if forcesInt32 {
			panic(fmt.Sprintf(`Table "%v": a FixedValues packed view cannot use .Int32() while key "%v" has no declared range`,
				dbTable.Name, viewCfg.Keys[elasticIndex].GetInfo().Name))
		}
		remainingBits := packedVirtualInt64Bits - sumSlotBits(slotBits, 0)
		if remainingBits < 1 {
			panic(fmt.Sprintf(`Table "%v": the FixedValues keys fill the %v bits of a bigint, leaving none for "%v"`,
				dbTable.Name, packedVirtualInt64Bits, viewCfg.Keys[elasticIndex].GetInfo().Name))
		}
		slotBits[elasticIndex] = remainingBits
		return &fixedValueSlotPlan{slotBitsPerColumn: slotBits}
	}

	declaredBits := sumSlotBits(slotBits, 0)
	if declaredBits <= packedVirtualInt32Bits {
		return &fixedValueSlotPlan{slotBitsPerColumn: slotBits, useInt32: true}
	}
	if forcesInt32 {
		panic(fmt.Sprintf(`Table "%v": a FixedValues packed view cannot pack into an int32: its keys take %v bits`, dbTable.Name, declaredBits))
	}
	if declaredBits > packedVirtualInt64Bits {
		panic(fmt.Sprintf(`Table "%v": a FixedValues packed view's keys take %v bits, past the %v of a bigint`,
			dbTable.Name, declaredBits, packedVirtualInt64Bits))
	}
	return &fixedValueSlotPlan{slotBitsPerColumn: slotBits}
}

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

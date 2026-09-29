package db

import "fmt"

// A packed key stores several non-negative integers in one integer column, one bit slot per
// component, most significant first. A slot is a range cap: Col.Size(n) declares that the column's
// values stay below 2^n. Nothing is ever truncated to fit, because truncation depends on each
// value's own magnitude and breaks the key's sort order when a value crosses a power of two, so a
// component that does not fit its slot panics instead.

// KeyIntPackingBits is the budget of a KeyIntPacking key. It is the record's own int64 ID, which
// travels to handlers, JSON and the frontend, so it keeps the sign bit clear and stays positive.
const KeyIntPackingBits = 63

// SlotMaxValue is the largest value a slot of slotBits bits holds. A 64-bit slot yields
// math.MaxUint64: Go defines an unsigned shift by the full width as 0.
func SlotMaxValue(slotBits int64) uint64 {
	return (uint64(1) << slotBits) - 1
}

// PackSlotValues packs one value per slot.
func PackSlotValues(componentValues []int64, slotBits []int64) uint64 {
	if len(componentValues) != len(slotBits) {
		panic(fmt.Sprintf("PackSlotValues: %v values for %v slots", len(componentValues), len(slotBits)))
	}
	return PackSlotPrefixBound(componentValues, slotBits, false)
}

// PackSlotPrefixBound packs prefixValues into the leading slots and fills every slot after them with
// zero (fillWithMax=false) or with its max value (fillWithMax=true). The two fills are the inclusive
// lower and upper bounds of the contiguous range a pinned prefix spans.
func PackSlotPrefixBound(prefixValues []int64, slotBits []int64, fillWithMax bool) uint64 {
	if len(prefixValues) > len(slotBits) {
		panic(fmt.Sprintf("PackSlotPrefixBound: %v values for %v slots", len(prefixValues), len(slotBits)))
	}

	var packed uint64
	for slotIndex, bits := range slotBits {
		slotValue := uint64(0)
		if slotIndex < len(prefixValues) {
			value := prefixValues[slotIndex]
			if value < 0 || uint64(value) > SlotMaxValue(bits) {
				panic(fmt.Sprintf("packed key slot %v: value %v does not fit its %v-bit slot (0..%v)",
					slotIndex, value, bits, SlotMaxValue(bits)))
			}
			slotValue = uint64(value)
		} else if fillWithMax {
			slotValue = SlotMaxValue(bits)
		}
		packed = packed<<bits | slotValue
	}
	return packed
}

// UnpackSlotValues is the inverse of PackSlotValues.
func UnpackSlotValues(packed uint64, slotBits []int64) []int64 {
	values := make([]int64, len(slotBits))
	for slotIndex := len(slotBits) - 1; slotIndex >= 0; slotIndex-- {
		bits := slotBits[slotIndex]
		values[slotIndex] = int64(packed & SlotMaxValue(bits))
		packed >>= bits
	}
	return values
}

// KeyIntPackingSlotBits resolves the slot layout of a KeyIntPacking declaration. Every column but
// the last must declare .Size(n); the last one (normally the Autoincrement placeholder) takes the
// bits the others leave.
func KeyIntPackingSlotBits(tableName string, keyIntPacking []Coln) []int64 {
	slotBits := make([]int64, len(keyIntPacking))
	remainingBits := int64(KeyIntPackingBits)
	for columnIndex, column := range keyIntPacking {
		bits := int64(column.GetInfo().SlotBits)
		isLast := columnIndex == len(keyIntPacking)-1
		if bits <= 0 {
			if !isLast {
				panic(fmt.Sprintf(`Table "%v": KeyIntPacking column %v must set .Size(n); only the last column takes the remaining bits`,
					tableName, columnIndex))
			}
			bits = remainingBits
		}
		remainingBits -= bits
		if remainingBits < 0 || bits <= 0 {
			panic(fmt.Sprintf(`Table "%v": KeyIntPacking slots exceed the %v-bit budget of an int64 ID`,
				tableName, KeyIntPackingBits))
		}
		slotBits[columnIndex] = bits
	}
	return slotBits
}

// PackedKeyComponent is one decoded slot of a packed key.
type PackedKeyComponent struct {
	Column string
	Value  int64
}

// DecodeKeyIntPacking splits a KeyIntPacking ID back into its components, for debugging. The
// autoincrement slot is reported twice more, as its sequence and its random suffix, because that is
// what an ID is read for.
func DecodeKeyIntPacking(schema TableSchema, packedKey int64) []PackedKeyComponent {
	if len(schema.KeyIntPacking) == 0 {
		panic(fmt.Sprintf(`Table "%v" declares no KeyIntPacking`, schema.Name))
	}
	if packedKey < 0 {
		panic(fmt.Sprintf(`Table "%v": a KeyIntPacking ID is never negative. Got: %v`, schema.Name, packedKey))
	}

	slotBits := KeyIntPackingSlotBits(schema.Name, schema.KeyIntPacking)
	slotValues := UnpackSlotValues(uint64(packedKey), slotBits)
	components := make([]PackedKeyComponent, 0, len(slotValues)+2)
	for slotIndex, column := range schema.KeyIntPacking {
		info := column.GetInfo()
		columnName := info.Name
		if columnName == "" {
			columnName = "autoincrement"
		}
		components = append(components, PackedKeyComponent{Column: columnName, Value: slotValues[slotIndex]})

		if randomBits := int64(info.AutoincrementRandBits); randomBits > 0 {
			components = append(components,
				PackedKeyComponent{Column: columnName + ".sequence", Value: slotValues[slotIndex] >> randomBits},
				PackedKeyComponent{Column: columnName + ".random", Value: slotValues[slotIndex] & int64(SlotMaxValue(randomBits))},
			)
		}
	}
	return components
}

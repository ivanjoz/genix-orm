package scylla

import (
	"fmt"
	"math"
	"slices"
	"strings"
)

// deltaVersionDigitsInt32 is the digit slot the implicit "updated_version" key gets when the packed
// column fits an int. 8 digits is what keeps a two-key delta view inside 4 bytes; it caps the table
// at 10^8 write calls per partition, after which writes fail loudly (see assertDeltaVersionFits).
const deltaVersionDigitsInt32 = 8

// deltaVersionDigitsInt64 is the slot used once the digit budget has already forced a bigint. The
// extra digits are paid for either way, so they are spent on sequence headroom.
const deltaVersionDigitsInt64 = 10

// maxPackedInt64Digits is the widest packed key an int64 column can hold without risking overflow
// on the upper bound arithmetic in getStatementPrepared.
const maxPackedInt64Digits = 18

// columnValueRange is the inclusive range a column was declared to hold via FixedValues.
type columnValueRange struct {
	minValue int64
	maxValue int64
}

// deltaSlotPlan is the resolved digit layout of a TypeDelta packed column. It is handed to
// compileSchemaView so the generic view compiler skips its own DecimalSize-based derivation.
type deltaSlotPlan struct {
	slotDigitsPerColumn []int64
	useInt32            bool
	maxPackedValue      int64
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

		valueRanges[columnName] = columnValueRange{minValue: minValue, maxValue: maxValue}
	}
	return valueRanges
}

// compileSchemaDeltaView expands a TypeDelta declaration into the packed range view that backs it:
// the declared keys plus the table's "updated_version" column, with every digit slot resolved up
// front.
func compileSchemaDeltaView(dbTable *ScyllaTable, indexCfg Index) {
	if len(indexCfg.Keys) == 0 {
		panic(fmt.Sprintf(`Table "%v": TypeDelta entries must declare at least one key column`, dbTable.Name))
	}

	versionColumn := dbTable.UpdatedVersionCol
	if versionColumn == nil || versionColumn.IsNil() {
		panic(fmt.Sprintf(`Table "%v": TypeDelta requires the managed "%v" column; remove DisableUpdatedVersion`,
			dbTable.Name, managedUpdatedVersionColumnName))
	}

	declaredKeyNames := make([]string, 0, len(indexCfg.Keys))
	declaredKeyDigits := make([]int64, 0, len(indexCfg.Keys))
	declaredKeyMaxValues := make([]int64, 0, len(indexCfg.Keys))
	forcesInt32 := false

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

		// An explicit DecimalSize() stays available as the escape hatch; otherwise the slot width
		// comes from the declared value range, which is the whole point of TypeDelta.
		keyDigits := int64(keyConfig.DecimalDigits)
		keyMaxValue := Pow10Int64(keyDigits) - 1
		if keyDigits <= 0 {
			valueRange, isDeclared := dbTable.fixedValueRanges[column.GetName()]
			if !isDeclared {
				panic(fmt.Sprintf(`Table "%v": TypeDelta key "%v" needs a FixedValues entry (or an explicit DecimalSize) so its digit slot can be sized`,
					dbTable.Name, column.GetName()))
			}
			keyMaxValue = valueRange.maxValue
			keyDigits = countBase10DigitsNonNegative(keyMaxValue)
		}

		declaredKeyNames = append(declaredKeyNames, column.GetName())
		declaredKeyDigits = append(declaredKeyDigits, keyDigits)
		declaredKeyMaxValues = append(declaredKeyMaxValues, keyMaxValue)
	}

	plan := planDeltaSlots(dbTable.Name, declaredKeyDigits, declaredKeyMaxValues, forcesInt32)

	// The implicit key carries only a name and its digit width; the view compiler resolves the rest
	// against the base table.
	versionKey := deltaKeyColumn{}
	versionKey.info.Name = versionColumn.GetName()
	versionKey.info.DecimalDigits = int8(plan.slotDigitsPerColumn[len(plan.slotDigitsPerColumn)-1])

	// Writes must refuse to pack a version wider than its slot: the packer trims from the right, so
	// an overrun would silently bucket versions in groups of ten and break the delta watermark.
	dbTable.maxDeltaVersionValue = Pow10Int64(int64(versionKey.info.DecimalDigits)) - 1

	deltaCfg := indexCfg
	deltaCfg.Type = TypeView
	deltaCfg.Keys = append(slices.Clone(indexCfg.Keys), versionKey)

	packedColumnTypeName := "int64"
	if plan.useInt32 {
		packedColumnTypeName = "int32"
	}
	fmt.Printf("Delta view registered: table=%s keys=[%s,%s] slotDigits=%v packedType=%s maxPacked=%d\n",
		dbTable.Name, strings.Join(declaredKeyNames, ","), versionColumn.GetName(),
		plan.slotDigitsPerColumn, packedColumnTypeName, plan.maxPackedValue)

	if !plan.useInt32 {
		logDeltaInt32KeyOrderHint(dbTable.Name, declaredKeyNames, declaredKeyDigits, declaredKeyMaxValues)
	}

	compileSchemaView(dbTable, deltaCfg, &plan)
}

// planDeltaSlots settles the digit layout and storage width of a delta packed column. The declared
// keys keep the widths they were resolved with; only the trailing "updated_version" slot flexes,
// widening once the budget has already spilled past int32.
func planDeltaSlots(tableName string, declaredKeyDigits, declaredKeyMaxValues []int64, forcesInt32 bool) deltaSlotPlan {
	planForVersionDigits := func(versionDigits int64) deltaSlotPlan {
		slotDigits := append(slices.Clone(declaredKeyDigits), versionDigits)
		componentMaxValues := append(slices.Clone(declaredKeyMaxValues), Pow10Int64(versionDigits)-1)

		totalDigits := int64(0)
		for _, digits := range slotDigits {
			totalDigits += digits
		}
		if totalDigits > maxPackedInt64Digits {
			panic(fmt.Sprintf(`Table "%v": TypeDelta digit budget is %v, which exceeds the %v an int64 packed key allows. Narrow a FixedValues range.`,
				tableName, totalDigits, maxPackedInt64Digits))
		}

		return deltaSlotPlan{
			slotDigitsPerColumn: slotDigits,
			maxPackedValue:      computePackedInt64ValueNonNegative(componentMaxValues, slotDigits),
		}
	}

	plan := planForVersionDigits(deltaVersionDigitsInt32)
	plan.useInt32 = plan.maxPackedValue <= math.MaxInt32
	if plan.useInt32 {
		return plan
	}

	if forcesInt32 {
		panic(fmt.Sprintf(`Table "%v": TypeDelta cannot pack into an int32: the declared ranges reach %v, past the %v limit. Drop .Int32() or narrow a FixedValues range.`,
			tableName, plan.maxPackedValue, math.MaxInt32))
	}

	// int64 was unavoidable, so spend the spare digits on sequence headroom.
	return planForVersionDigits(deltaVersionDigitsInt64)
}

// logDeltaInt32KeyOrderHint reports whether some other key order would have fit an int32, since
// only the leading slot's magnitude decides it. Reordering is left to the developer: key order is
// part of the declaration, and silently permuting it would change the view name and Delta()'s
// inferred sync-filter column.
func logDeltaInt32KeyOrderHint(tableName string, keyNames []string, keyDigits, keyMaxValues []int64) {
	if len(keyNames) < 2 {
		return
	}

	for candidateIndex := range keyNames {
		if candidateIndex == 0 {
			continue
		}
		reorderedNames := moveToFront(keyNames, candidateIndex)
		reorderedDigits := moveToFront(keyDigits, candidateIndex)
		reorderedMaxValues := moveToFront(keyMaxValues, candidateIndex)

		slotDigits := append(slices.Clone(reorderedDigits), deltaVersionDigitsInt32)
		componentMaxValues := append(slices.Clone(reorderedMaxValues), Pow10Int64(deltaVersionDigitsInt32)-1)
		if computePackedInt64ValueNonNegative(componentMaxValues, slotDigits) <= math.MaxInt32 {
			fmt.Printf("Delta view hint: table=%s packed as bigint, but Keys order [%s] would fit an int. Note this also changes the column Delta() filters on.\n",
				tableName, strings.Join(reorderedNames, ","))
			return
		}
	}
}

func moveToFront[T any](values []T, index int) []T {
	reordered := make([]T, 0, len(values))
	reordered = append(reordered, values[index])
	for i, value := range values {
		if i != index {
			reordered = append(reordered, value)
		}
	}
	return reordered
}

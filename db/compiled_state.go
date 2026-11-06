package db

// Index state that a TableSchema compiles to, independent of any storage engine. It used to live
// privately on the Scylla driver's table; every driver needs the same facts (which slots a packed
// key has, which composite buckets a column feeds, which cache groups a table keeps), because they
// decide what an index entry contains. A driver reads them from its TableCore.

import (
	"fmt"
	"hash/fnv"
	"math/bits"
	"slices"
	"strings"
	"unsafe"
)

// DeltaUpdatedSlotBits holds a positive int32 SUnixTime, the trailing slot of a delta's packed key.
const DeltaUpdatedSlotBits = 31

// CompositeBucketIndex stores the source columns and generated virtual bucket columns for one
// composite-bucket index.
type CompositeBucketIndex struct {
	Name          string
	SourceColumns []IColInfo
	BucketColumn  IColInfo
	// BucketIsWeek keeps schema-level week semantics for custom week-code arithmetic in bucketing and range planning.
	BucketIsWeek         bool
	BucketSizes          []int8
	VirtualColumnsBySize map[int8]IColInfo
}

type IndexGroupSourceColumn struct {
	Column IColInfo
}

// IndexGroupInfo is one compiled cache-group index (TypeCacheGroup, TypeCacheGroupS3,
// TypeCacheGroupFromKey).
type IndexGroupInfo struct {
	Name                 string
	IndexID              int16
	SourceColumns        []IndexGroupSourceColumn
	VirtualColumn        IColInfo
	UsesCollectionValues bool
	// InheritFromKey routes grouped fetches through KeyIntPacking primary-key ranges instead of a secondary index.
	InheritFromKey bool
	// SnapshotInS3 (TypeCacheGroupS3) keeps each settled group in the file store.
	SnapshotInS3 bool
}

// PackedIndexInfo stores schema-time metadata for TableSchema.Indexes packed local indexes.
// It is used for capability generation and for query rewriting.
type PackedIndexInfo struct {
	IndexName         string
	PackedColumnName  string
	SourceColumnNames []string
	// PartitionColumnName is non-empty when the packed index is local (partition + packed).
	// Empty means "global index on packed column only".
	PartitionColumnName string
	// SlotBitsPerColumn aligns with SourceColumnNames (same length).
	SlotBitsPerColumn []int64
	// IsInt32Packed indicates the packed column is a 32-bit integer.
	IsInt32Packed bool
}

// DeltaIndexInfo is one compiled TypeDelta index.
type DeltaIndexInfo struct {
	// Tag names the index: its Keys' column names joined by "_", "all" when keyless. It names the
	// index's rows in the moves table.
	Tag string
	// Pinned are every declared Key, in order: each is a slot of the packed key, so one combination of
	// their values (a prefix) is one contiguous span ordered by updated. A read names the value of
	// each, or fans out over its FixedValues.
	Pinned         []IColInfo
	PinnedSlotBits []int64
	// PackedColumn packs the pinned values and updated (updated alone on a keyless index): the
	// index's clustering key.
	PackedColumn *ColumnInfo
	// ViewName is the Scylla view's name; other drivers do not read it.
	ViewName string
	// ReplaceThreshold is how many changed or moved records a sync answers before it answers a replace.
	ReplaceThreshold int
}

// SlotBits is the layout of the packed key: the pinned slots, then updated.
func (deltaIndex DeltaIndexInfo) SlotBits() []int64 {
	return append(slices.Clone(deltaIndex.PinnedSlotBits), DeltaUpdatedSlotBits)
}

// PackedKey is the unsigned packed layout for pinned values and an updated. A driver whose integer
// comparison is signed re-maps it (the Scylla driver flips the top bit).
func (deltaIndex DeltaIndexInfo) PackedKey(tableName string, pinnedValues []int64, updated int64) uint64 {
	sourceColumns := append(slices.Clone(deltaIndex.Pinned), deltaIndex.PackedColumn)
	return PackRowValues(tableName, deltaIndex.PackedColumn.GetName(), sourceColumns,
		append(slices.Clone(pinnedValues), updated), deltaIndex.SlotBits())
}

// PinnedValuesOf picks the pinned columns' values, in key order, out of a name -> value map.
func (deltaIndex DeltaIndexInfo) PinnedValuesOf(valuesByColumn map[string]int64) []int64 {
	pinnedValues := make([]int64, len(deltaIndex.Pinned))
	for i, pinnedColumn := range deltaIndex.Pinned {
		pinnedValues[i] = valuesByColumn[pinnedColumn.GetName()]
	}
	return pinnedValues
}

// FixedValueSlotPlan is the bit layout of a packed TypeView sized from FixedValues instead of Size().
type FixedValueSlotPlan struct {
	SlotBitsPerColumn []int64
	UseInt32          bool
}

// PlanFixedValueViewSlots sizes a multi-key TypeView from FixedValues: each key takes the bits of
// its declared maximum, and at most one key with no declared range takes what a bigint leaves. It
// packs into an int when the slots fit 32 bits. nil when the view isn't eligible: a single key, a
// Size() hint on any key (the Size() rules apply), or no declared range at all.
func (core *TableCore) PlanFixedValueViewSlots(viewCfg Index) *FixedValueSlotPlan {
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
		if valueRange, isDeclared := core.FixedValueRanges[keyConfig.Name]; isDeclared {
			slotBits[i] = max(1, int64(bits.Len64(uint64(valueRange.MaxValue))))
		} else if elasticIndex >= 0 {
			return nil // two keys without a range: no unambiguous split, the view stays a hash view
		} else {
			elasticIndex = i
		}
	}
	if elasticIndex >= 0 {
		if forcesInt32 {
			panic(fmt.Sprintf(`Table "%v": a FixedValues packed view cannot use .Int32() while key "%v" has no declared range`,
				core.Name, viewCfg.Keys[elasticIndex].GetInfo().Name))
		}
		remainingBits := PackedVirtualInt64Bits - SumSlotBits(slotBits, 0)
		if remainingBits < 1 {
			panic(fmt.Sprintf(`Table "%v": the FixedValues keys fill the %v bits of a bigint, leaving none for "%v"`,
				core.Name, PackedVirtualInt64Bits, viewCfg.Keys[elasticIndex].GetInfo().Name))
		}
		slotBits[elasticIndex] = remainingBits
		return &FixedValueSlotPlan{SlotBitsPerColumn: slotBits}
	}

	declaredBits := SumSlotBits(slotBits, 0)
	if declaredBits <= PackedVirtualInt32Bits {
		return &FixedValueSlotPlan{SlotBitsPerColumn: slotBits, UseInt32: true}
	}
	if forcesInt32 {
		panic(fmt.Sprintf(`Table "%v": a FixedValues packed view cannot pack into an int32: its keys take %v bits`, core.Name, declaredBits))
	}
	if declaredBits > PackedVirtualInt64Bits {
		panic(fmt.Sprintf(`Table "%v": a FixedValues packed view's keys take %v bits, past the %v of a bigint`,
			core.Name, declaredBits, PackedVirtualInt64Bits))
	}
	return &FixedValueSlotPlan{SlotBitsPerColumn: slotBits}
}

// ResolveFixedValueRanges validates the schema's FixedValues and indexes them by column name.
// Packed keys are non-negative by construction, so a negative bound is rejected here rather than
// panicking deep inside the packing arithmetic on the first write.
func (core *TableCore) ResolveFixedValueRanges(declaredFixedValues []FixedValues) map[string]ColumnValueRange {
	if len(declaredFixedValues) == 0 {
		return nil
	}

	valueRanges := make(map[string]ColumnValueRange, len(declaredFixedValues))
	for _, declared := range declaredFixedValues {
		if declared.Col == nil {
			panic(fmt.Sprintf(`Table "%v": FixedValues entry must name a Col`, core.Name))
		}
		columnName := declared.Col.GetInfo().Name
		column := core.ColumnsMap[columnName]
		if column == nil || column.IsNil() {
			panic(fmt.Sprintf(`Table "%v": FixedValues column "%v" was not found`, core.Name, columnName))
		}
		if _, isRepeated := valueRanges[columnName]; isRepeated {
			panic(fmt.Sprintf(`Table "%v": FixedValues declares column "%v" twice`, core.Name, columnName))
		}

		minValue, maxValue, isDeclared := declared.Bounds()
		if !isDeclared {
			panic(fmt.Sprintf(`Table "%v": FixedValues for "%v" must declare Values or a Max`, core.Name, columnName))
		}
		if minValue < 0 {
			panic(fmt.Sprintf(`Table "%v": FixedValues for "%v" must not be negative. Got min: %v`,
				core.Name, columnName, minValue))
		}
		if maxValue < minValue {
			panic(fmt.Sprintf(`Table "%v": FixedValues for "%v" has Max %v below Min %v`,
				core.Name, columnName, maxValue, minValue))
		}

		valueRanges[columnName] = ColumnValueRange{
			MinValue:       minValue,
			MaxValue:       maxValue,
			DeclaredValues: EnumerateFixedValues(declared, minValue, maxValue),
		}
	}
	return valueRanges
}

// PlanDeltaPinnedSlots sizes the pinned slots of a TypeDelta key: a declared Size() wins, then the
// FixedValues range, and at most one elastic key takes the bits a bigint leaves beside updated.
func (core *TableCore) PlanDeltaPinnedSlots(pinned []IColInfo, indexCfg Index) []int64 {
	slotBits := make([]int64, len(pinned))
	elasticIndex := -1
	for i, pinnedColumn := range pinned {
		if declaredBits := int64(indexCfg.Keys[i].GetInfo().SlotBits); declaredBits > 0 {
			slotBits[i] = declaredBits
		} else if valueRange, isDeclared := core.FixedValueRanges[pinnedColumn.GetName()]; isDeclared {
			slotBits[i] = max(1, int64(bits.Len64(uint64(valueRange.MaxValue))))
		} else if elasticIndex >= 0 {
			panic(fmt.Sprintf(`Table "%v": TypeDelta pinned keys "%v" and "%v" both lack a FixedValues entry and a Size; only one can take the remaining bits`,
				core.Name, pinned[elasticIndex].GetName(), pinnedColumn.GetName()))
		} else {
			elasticIndex = i
		}
	}
	remainingBits := PackedVirtualInt64Bits - DeltaUpdatedSlotBits - SumSlotBits(slotBits, 0)
	if elasticIndex >= 0 {
		slotBits[elasticIndex] = remainingBits
		remainingBits = 0
	}
	if remainingBits < 0 || (elasticIndex >= 0 && slotBits[elasticIndex] < 1) {
		panic(fmt.Sprintf(`Table "%v": TypeDelta pinned keys take more than the %v bits a bigint leaves beside updated. Narrow a FixedValues range.`,
			core.Name, PackedVirtualInt64Bits-DeltaUpdatedSlotBits))
	}
	return slotBits
}

// BasicHashInt is a deterministic string hash that reserves 0 as an invalid/sentinel ID.
func BasicHashInt(s string) int32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	hashValue := int32(h.Sum32())
	if hashValue == 0 {
		return 1
	}
	return hashValue
}

// MakeIndexGroupCacheKey joins a group's index ID and hash into the key clients cache it under.
func MakeIndexGroupCacheKey(indexID int16, indexHash int32) int64 {
	return (int64(indexID) << 32) | int64(uint32(indexHash))
}

// AllocateIndexGroupID derives a stable int16 for a logical group name, probing past collisions
// with other groups of the same table.
func (core *TableCore) AllocateIndexGroupID(sourceColumnNames []string) int16 {
	logicalName := strings.Join(sourceColumnNames, "_")
	if core.IndexGroupIDs == nil {
		core.IndexGroupIDs = map[int16]string{}
	}

	candidateID := int16(BasicHashInt(logicalName))
	if candidateID == 0 {
		candidateID = 1
	}

	for {
		if existingName, exists := core.IndexGroupIDs[candidateID]; !exists {
			core.IndexGroupIDs[candidateID] = logicalName
			return candidateID
		} else if existingName == logicalName {
			return candidateID
		}
		candidateID++
		if candidateID == 0 {
			candidateID = 1
		}
	}
}

// MakeIndexGroupVirtualColumnName names the generated column holding a group's hash (or hash set).
func MakeIndexGroupVirtualColumnName(sourceColumnNames []string, usesCollection bool) string {
	prefix := "zz_ig_"
	if usesCollection {
		prefix = "zz_igs_"
	}
	return prefix + strings.Join(sourceColumnNames, "_")
}

// ComputeIndexGroupHashes hashes every combination of the record's source values, one hash per group
// the record belongs to.
func ComputeIndexGroupHashes(ptr unsafe.Pointer, sourceColumns []IndexGroupSourceColumn) []int32 {
	combinations := [][]int64{{}}

	for _, sourceColumn := range sourceColumns {
		columnValues := FlattenInt64Values(sourceColumn.Column.GetRawValue(ptr))
		if len(columnValues) == 0 {
			return nil
		}

		nextCombinations := make([][]int64, 0, len(combinations)*len(columnValues))
		for _, combination := range combinations {
			for _, value := range columnValues {
				combinationExpanded := append(append([]int64{}, combination...), value)
				nextCombinations = append(nextCombinations, combinationExpanded)
			}
		}
		combinations = nextCombinations
	}

	uniqueHashes := map[int32]struct{}{}
	for _, combination := range combinations {
		uniqueHashes[HashInt64(combination...)] = struct{}{}
	}

	hashes := make([]int32, 0, len(uniqueHashes))
	for hashValue := range uniqueHashes {
		hashes = append(hashes, hashValue)
	}
	slices.Sort(hashes)
	return hashes
}

// BuildIndexGroupHashValues hashes the values a query pins on the group's source columns; nil when a
// source column has no statement.
func BuildIndexGroupHashValues(sourceColumns []IndexGroupSourceColumn, statements []ColumnStatement) []int32 {
	valuesGroups := [][]int64{{}}

	for _, sourceColumn := range sourceColumns {
		var statementFound *ColumnStatement
		for statementIndex := range statements {
			if statements[statementIndex].Col == sourceColumn.Column.GetName() {
				statementFound = &statements[statementIndex]
				break
			}
		}
		if statementFound == nil {
			return nil
		}

		rawValues := []any{statementFound.Value}
		if len(statementFound.Values) > 0 {
			rawValues = statementFound.Values
		}

		columnValues := make([]int64, 0, len(rawValues))
		for _, rawValue := range rawValues {
			columnValues = append(columnValues, FlattenInt64Values(rawValue)...)
		}
		if len(columnValues) == 0 {
			return nil
		}

		nextGroups := make([][]int64, 0, len(valuesGroups)*len(columnValues))
		for _, valuesGroup := range valuesGroups {
			for _, value := range columnValues {
				nextGroups = append(nextGroups, append(append([]int64{}, valuesGroup...), value))
			}
		}
		valuesGroups = nextGroups
	}

	hashValues := make([]int32, 0, len(valuesGroups))
	for _, valuesGroup := range valuesGroups {
		hashValues = append(hashValues, HashInt64(valuesGroup...))
	}
	return hashValues
}

// NormalizeCompositeBucketSizes validates and orders bucket sizes once so read/write logic can rely
// on a deterministic order.
func NormalizeCompositeBucketSizes(rawSizes []int8) []int8 {
	bucketSizeSeen := map[int8]bool{}
	bucketSizes := make([]int8, 0, len(rawSizes))
	for _, bucketSize := range rawSizes {
		if bucketSize <= 0 {
			panic(fmt.Sprintf("CompositeBucketing requires bucket sizes > 0. Got: %v", bucketSize))
		}
		if bucketSizeSeen[bucketSize] {
			continue
		}
		bucketSizeSeen[bucketSize] = true
		bucketSizes = append(bucketSizes, bucketSize)
	}
	if len(bucketSizes) == 0 {
		panic("CompositeBucketing requires at least one bucket size.")
	}
	slices.Sort(bucketSizes)
	return bucketSizes
}

// IsCompositeNumericFieldType: composite bucket hashing supports integer scalars/slices only,
// matching the query planner's assumptions.
func IsCompositeNumericFieldType(fieldType string) bool {
	switch fieldType {
	case "int", "int8", "int16", "int32", "int64", "[]int", "[]int8", "[]int16", "[]int32", "[]int64":
		return true
	}
	return false
}

// CompositeBucketValues indexes bucket IDs (value/size), not raw values.
func CompositeBucketValues(rawValue any, bucketSize int8, isWeek bool) []int64 {
	flatValues := FlattenInt64Values(rawValue)
	bucketValues := make([]int64, 0, len(flatValues))
	for _, value := range flatValues {
		bucketValues = append(bucketValues, CompositeBucketID(value, bucketSize, isWeek))
	}
	return bucketValues
}

// ComputeCompositeHashSet builds the cartesian product across up to 3 numeric source columns, then
// hashes each tuple: the set of values a composite-bucket index holds for one record.
func ComputeCompositeHashSet(ptr unsafe.Pointer, sourceColumns []IColInfo, bucketColumn IColInfo, bucketSize int8, bucketIsWeek bool) []int32 {
	combinations := [][]int64{{}}

	for _, sourceColumn := range sourceColumns {
		var columnValues []int64
		if sourceColumn.GetName() == bucketColumn.GetName() {
			columnValues = CompositeBucketValues(sourceColumn.GetRawValue(ptr), bucketSize, bucketIsWeek)
		} else {
			columnValues = FlattenInt64Values(sourceColumn.GetRawValue(ptr))
		}

		if len(columnValues) == 0 {
			return []int32{}
		}

		nextCombinations := make([][]int64, 0, len(combinations)*len(columnValues))
		for _, combination := range combinations {
			for _, columnValue := range columnValues {
				combinationExpanded := append(append([]int64{}, combination...), columnValue)
				nextCombinations = append(nextCombinations, combinationExpanded)
			}
		}
		combinations = nextCombinations
	}

	uniqueHashes := map[int32]struct{}{}
	// Deduplicate hashes per record to keep generated set values compact and stable.
	for _, combination := range combinations {
		uniqueHashes[HashInt64(combination...)] = struct{}{}
	}

	hashValues := make([]int32, 0, len(uniqueHashes))
	for hashValue := range uniqueHashes {
		hashValues = append(hashValues, hashValue)
	}
	slices.Sort(hashValues)
	return hashValues
}

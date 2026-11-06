package db

import "fmt"

// DeltaReadPlan is what a Delta() read resolves to before any storage syntax: the TypeDelta index it
// runs on (IndexPosition into TableCore.DeltaIndexes), the partition, and the pinned prefixes it
// covers. Each prefix is one contiguous span of the index's packed key, ordered by updated.
type DeltaReadPlan struct {
	IndexPosition int
	Index         DeltaIndexInfo
	Partition     int64
	Prefixes      [][]int64
}

// PlanDeltaRead resolves a Delta() read: the partition must be pinned by an equality, and the index
// is the one whose pinned columns the query's equalities fix most, then the one with fewest prefixes.
func (core *TableCore) PlanDeltaRead(tableInfo *TableInfo) (*DeltaReadPlan, error) {
	equalityValues := map[string]any{}
	for _, statement := range tableInfo.Statements {
		if statement.Operator == "=" {
			equalityValues[statement.Col] = statement.Value
		}
	}
	partitionValue, isPartitionPinned := equalityValues[core.GetPartKey().GetName()]
	if !isPartitionPinned {
		return nil, fmt.Errorf(`table %q: Delta() needs an equality on the partition column`, core.Name)
	}
	deltaValues := make([]int64, len(tableInfo.Delta.LastKeyValues))
	for valueIndex, deltaValue := range tableInfo.Delta.LastKeyValues {
		deltaValues[valueIndex] = ConvertToInt64(deltaValue)
	}

	selectedPosition, isAmbiguous, selectedEqualityCount := -1, false, -1
	var selectedPrefixes [][]int64
	for position, deltaIndex := range core.DeltaIndexes {
		prefixes, equalityCount, isResolved := core.resolveDeltaPrefixes(deltaIndex, equalityValues, deltaValues)
		if !isResolved {
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
		return nil, fmt.Errorf(`table %q: Delta() found no TypeDelta index whose Keys all resolve: each needs an equality, the Delta() values (the last one), or an enumerable FixedValues (at most %d queries)`,
			core.Name, MaxFixedValueFanoutQueries)
	}
	if isAmbiguous {
		return nil, fmt.Errorf(`table %q: Delta() fits several TypeDelta indexes equally: pin one more Keys column, or drop an index`, core.Name)
	}
	return &DeltaReadPlan{
		IndexPosition: selectedPosition,
		Index:         core.DeltaIndexes[selectedPosition],
		Partition:     ConvertToInt64(partitionValue),
		Prefixes:      selectedPrefixes,
	}, nil
}

// resolveDeltaPrefixes lists the pinned prefixes a Delta() read covers on an index. Each pinned
// column takes its equality value, or (the last one) the values Delta() names, or else every value
// its FixedValues enumerates; their combinations are the prefixes. false when a column has none of
// them, or when the enumerated fan-out passes MaxFixedValueFanoutQueries. equalityCount is how many
// pinned columns the query's own equalities fixed.
func (core *TableCore) resolveDeltaPrefixes(deltaIndex DeltaIndexInfo, equalityValues map[string]any, deltaValues []int64) (prefixes [][]int64, equalityCount int, isResolved bool) {
	if len(deltaValues) > 0 && len(deltaIndex.Pinned) == 0 {
		return nil, 0, false // a keyless index has no column for the values to name
	}
	prefixes = [][]int64{{}}
	enumeratedQueries := 1
	for pinnedIndex, pinnedColumn := range deltaIndex.Pinned {
		var columnValues []int64
		isLastPinned := pinnedIndex == len(deltaIndex.Pinned)-1
		if equalityValue, hasEquality := equalityValues[pinnedColumn.GetName()]; hasEquality {
			columnValues = []int64{ConvertToInt64(equalityValue)}
			equalityCount++
		} else if isLastPinned && len(deltaValues) > 0 {
			columnValues = deltaValues
		} else if valueRange, isDeclared := core.FixedValueRanges[pinnedColumn.GetName()]; isDeclared && len(valueRange.DeclaredValues) > 0 {
			columnValues = valueRange.DeclaredValues
			enumeratedQueries *= len(columnValues)
		} else {
			return nil, 0, false
		}
		prefixes = expandValueGroups(prefixes, columnValues)
	}
	if enumeratedQueries > MaxFixedValueFanoutQueries {
		return nil, 0, false
	}
	return prefixes, equalityCount, true
}

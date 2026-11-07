package db

import (
	"fmt"
	"slices"
)

// IndexGroupSettleTicks (16 s) is how long a cache group stays unsettled after its last write: a
// write of the same 2 s tick, or one still landing, may be missing from a fetch made now, so an
// unsettled group is always fetched and labelled 0.
const IndexGroupSettleTicks = 8

// SQLIndexGroupSettleTicks (8 s) is the settle window of a SQL driver. Its marks are stamped in the
// last statements before the COMMIT (deferred group marks) and a lock wait is capped by the session's
// lock timeout, so a mark lands within seconds of its stamp, well inside the window.
const SQLIndexGroupSettleTicks = 4

// IndexGroupHash is one group a grouped read covers: its hash and the source-column values hashed.
type IndexGroupHash struct {
	HashValue        int32
	IndexGroupValues []int64
}

// IndexGroupReadPlan is what a grouped read (QueryIndexGroup) resolves to: the cache group chosen,
// the partition, and the groups it covers, in hash order.
type IndexGroupReadPlan struct {
	PartitionValue int32
	IndexGroup     IndexGroupInfo
	HashGroups     []IndexGroupHash
}

// IndexGroupFetchState is one group of a read that is fetched or answered from the client cache.
type IndexGroupFetchState struct {
	HashValue        int32
	IndexGroupValues []int64
	// LastWrite is the label the group goes out with: its stored last_write, or 0 while unsettled.
	LastWrite int32
}

// PlanIndexGroupRead resolves a grouped read: an equality on the partition, exactly one BETWEEN,
// and the cache group that covers the most predicates with the smallest hash fan-out.
func (core *TableCore) PlanIndexGroupRead(tableInfo *TableInfo) (*IndexGroupReadPlan, error) {
	switch {
	case len(tableInfo.ColumnsInclude) > 0 || len(tableInfo.ColumnsExclude) > 0:
		return nil, fmt.Errorf("QueryIndexGroup does not support Select() or Exclude() yet")
	case len(tableInfo.GroupByColumns) > 0:
		return nil, fmt.Errorf("QueryIndexGroup does not support GroupBy()")
	case tableInfo.OrderBy != "":
		return nil, fmt.Errorf("QueryIndexGroup does not support OrderDesc() yet")
	case tableInfo.Limit > 0:
		return nil, fmt.Errorf("QueryIndexGroup does not support Limit() yet")
	case tableInfo.AllowFilter:
		return nil, fmt.Errorf("QueryIndexGroup does not support AllowFilter()")
	}

	statements := tableInfo.SelectStatements()
	partitionColumn := core.GetPartKey()
	if partitionColumn == nil || partitionColumn.IsNil() {
		return nil, fmt.Errorf(`QueryIndexGroup requires a partition column on table "%v"`, core.Name)
	}
	statementsByColumn := map[string][]ColumnStatement{}
	betweenStatementsCount := 0
	for _, statement := range statements {
		statementsByColumn[statement.Col] = append(statementsByColumn[statement.Col], statement)
		if statement.Operator == "BETWEEN" {
			betweenStatementsCount++
		}
	}
	partitionStatements := statementsByColumn[partitionColumn.GetName()]
	if len(partitionStatements) == 0 || partitionStatements[0].Operator != "=" {
		return nil, fmt.Errorf(`QueryIndexGroup requires partition equality on "%v"`, partitionColumn.GetName())
	}
	if betweenStatementsCount != 1 {
		return nil, fmt.Errorf("QueryIndexGroup requires exactly one BETWEEN statement")
	}

	var bestIndexGroup *IndexGroupInfo
	bestScore := -1
	for indexGroupIndex := range core.IndexGroups {
		indexGroup := &core.IndexGroups[indexGroupIndex]
		score, isServable := scoreIndexGroupCandidate(*indexGroup, statementsByColumn)
		if !isServable || score <= bestScore {
			continue
		}
		bestScore, bestIndexGroup = score, indexGroup
	}
	if bestIndexGroup == nil {
		return nil, fmt.Errorf(`QueryIndexGroup did not find a compatible TypeCacheGroup for table "%v"`, core.Name)
	}

	hashGroups, err := buildIndexGroupHashes(*bestIndexGroup, statements)
	if err != nil {
		return nil, err
	}
	if len(hashGroups) == 0 {
		return nil, fmt.Errorf("QueryIndexGroup did not produce candidate hashes")
	}
	if ShouldLog() {
		fmt.Printf("QueryIndexGroup plan selected: table=%s group=%s candidate_hashes=%d\n",
			core.Name, bestIndexGroup.Name, len(hashGroups))
	}
	return &IndexGroupReadPlan{
		PartitionValue: int32(ConvertToInt64(partitionStatements[0].Value)),
		IndexGroup:     *bestIndexGroup,
		HashGroups:     hashGroups,
	}, nil
}

// scoreIndexGroupCandidate: a group serves the read when each of its source columns has exactly one
// statement it can enumerate and exactly one of them is the BETWEEN. The most specific group wins,
// then the one with the smallest hash fan-out.
func scoreIndexGroupCandidate(indexGroup IndexGroupInfo, statementsByColumn map[string][]ColumnStatement) (int, bool) {
	betweenColumns := 0
	fanoutSize := 1
	for _, sourceColumn := range indexGroup.SourceColumns {
		columnStatements := statementsByColumn[sourceColumn.Column.GetName()]
		if len(columnStatements) != 1 {
			return 0, false
		}
		statement := columnStatements[0]
		switch statement.Operator {
		case "=", "CONTAINS":
		case "IN":
			if len(statement.Values) == 0 {
				return 0, false
			}
			fanoutSize *= len(statement.Values)
		case "BETWEEN":
			if len(statement.From) == 0 || len(statement.To) == 0 {
				return 0, false
			}
			betweenColumns++
			fromValue, toValue := ConvertToInt64(statement.From[0].Value), ConvertToInt64(statement.To[0].Value)
			if toValue < fromValue {
				fromValue, toValue = toValue, fromValue
			}
			fanoutSize *= int(toValue-fromValue) + 1
		default:
			return 0, false
		}
	}
	if betweenColumns != 1 {
		return 0, false
	}
	return len(indexGroup.SourceColumns)*100000 - fanoutSize, true
}

// buildIndexGroupHashes lists the distinct groups of a read, in hash order. nil when a source
// column selects no value.
func buildIndexGroupHashes(indexGroup IndexGroupInfo, statements []ColumnStatement) ([]IndexGroupHash, error) {
	valueGroups := [][]int64{{}}
	for _, sourceColumn := range indexGroup.SourceColumns {
		statement, err := FindIndexGroupStatement(sourceColumn.Column.GetName(), statements)
		if err != nil {
			return nil, err
		}
		columnValues, err := ResolveIndexGroupQueryValues(statement)
		if err != nil {
			return nil, err
		}
		if len(columnValues) == 0 {
			return nil, nil
		}
		valueGroups = expandValueGroups(valueGroups, columnValues)
	}

	hashGroupsByValue := map[int32]IndexGroupHash{}
	for _, valueGroup := range valueGroups {
		hashValue := HashInt64(valueGroup...)
		if _, isListed := hashGroupsByValue[hashValue]; !isListed {
			hashGroupsByValue[hashValue] = IndexGroupHash{HashValue: hashValue, IndexGroupValues: slices.Clone(valueGroup)}
		}
	}
	hashValues := make([]int32, 0, len(hashGroupsByValue))
	for hashValue := range hashGroupsByValue {
		hashValues = append(hashValues, hashValue)
	}
	slices.Sort(hashValues)
	hashGroups := make([]IndexGroupHash, 0, len(hashValues))
	for _, hashValue := range hashValues {
		hashGroups = append(hashGroups, hashGroupsByValue[hashValue])
	}
	return hashGroups, nil
}

// HashValues lists the plan's group hashes in hash order.
func (plan *IndexGroupReadPlan) HashValues() []int32 {
	hashValues := make([]int32, 0, len(plan.HashGroups))
	for _, hashGroup := range plan.HashGroups {
		hashValues = append(hashValues, hashGroup.HashValue)
	}
	return hashValues
}

// BaseGroupHashes maps every group of a no-index group's read to the base group it is a slice of:
// a no-index group has no marks of its own, so its freshness is its base group's.
func (plan *IndexGroupReadPlan) BaseGroupHashes() (baseHashByGroupHash map[int32]int32, baseHashes []int32) {
	baseHashByGroupHash = make(map[int32]int32, len(plan.HashGroups))
	for _, hashGroup := range plan.HashGroups {
		baseHash := HashInt64(plan.IndexGroup.BaseGroupValues(hashGroup.IndexGroupValues)...)
		baseHashByGroupHash[hashGroup.HashValue] = baseHash
		if !slices.Contains(baseHashes, baseHash) {
			baseHashes = append(baseHashes, baseHash)
		}
	}
	return baseHashByGroupHash, baseHashes
}

// SplitIndexGroupFetches decides, per group the server holds, whether it is fetched or answered from
// the client's cache. An unsettled group (written less than IndexGroupSettleTicks ago) is always
// fetched and labelled 0; a settled group whose last_write the client holds is cached.
func SplitIndexGroupFetches(hashGroups []IndexGroupHash, serverFreshnessByHash, cachedIndexGroups map[int32]int32,
	nowTime int32) (fetchStates, cachedStates []IndexGroupFetchState) {
	return SplitIndexGroupFetchesSettledAfter(hashGroups, serverFreshnessByHash, cachedIndexGroups, nowTime, IndexGroupSettleTicks)
}

// SplitIndexGroupFetchesSettledAfter is SplitIndexGroupFetches with the driver's settle window.
func SplitIndexGroupFetchesSettledAfter(hashGroups []IndexGroupHash, serverFreshnessByHash, cachedIndexGroups map[int32]int32,
	nowTime, settleTicks int32) (fetchStates, cachedStates []IndexGroupFetchState) {

	fetchStates = make([]IndexGroupFetchState, 0, len(hashGroups))
	cachedStates = make([]IndexGroupFetchState, 0, len(hashGroups))
	for _, hashGroup := range hashGroups {
		lastWrite, existsOnServer := serverFreshnessByHash[hashGroup.HashValue]
		if !existsOnServer {
			continue
		}
		groupState := IndexGroupFetchState{
			HashValue: hashGroup.HashValue, IndexGroupValues: slices.Clone(hashGroup.IndexGroupValues), LastWrite: lastWrite,
		}
		if nowTime-lastWrite < settleTicks {
			groupState.LastWrite = 0
			fetchStates = append(fetchStates, groupState)
			continue
		}
		if cachedLastWrite, existsInCache := cachedIndexGroups[hashGroup.HashValue]; existsInCache && cachedLastWrite == lastWrite {
			cachedStates = append(cachedStates, groupState)
			continue
		}
		fetchStates = append(fetchStates, groupState)
	}
	return fetchStates, cachedStates
}

// GroupPostFilterStatements is the post-filter of one group's fetch. A no-index group reads its whole
// base range, so it also pins each of its Keys to the group's own value: the query's IN over a Key
// fans out into one group per value, and each group must keep only its value's records.
func GroupPostFilterStatements(indexGroup IndexGroupInfo, fetchState IndexGroupFetchState, postFilterStatements []ColumnStatement) []ColumnStatement {
	if indexGroup.BaseGroup == nil {
		return postFilterStatements
	}
	groupStatements := slices.Clone(postFilterStatements)
	for columnIndex, sourceColumn := range indexGroup.SourceColumns {
		groupStatements = append(groupStatements, ColumnStatement{
			Col: sourceColumn.Column.GetName(), Operator: "=", Value: fetchState.IndexGroupValues[columnIndex],
		})
	}
	return groupStatements
}

// IndexGroupFetchStep is the base-table read of one group, within the plan's partition:
//   - a TypeCacheGroupFromKey group (or a no-index group over one) reads its int-packed primary-key
//     block, [from, to) as the Scylla driver has always bound it;
//   - a single-column group reads its source column, which has its own secondary index;
//   - a slice group looks its hash up in the hash collection, any other group equals its hash.
func (core *TableCore) IndexGroupFetchStep(plan *IndexGroupReadPlan, fetchState IndexGroupFetchState) AccessStep {
	partitionPrefix := []KeyValues{{Column: core.GetPartKey().GetName(), Values: []any{plan.PartitionValue}}}
	withPartition := func(keyValues KeyValues) AccessStep {
		return rangeStep(KeyRange{EqualPrefix: append(partitionPrefix, keyValues)})
	}

	// A no-index group reads its base group's range; the post-filter drops its other Keys' misses.
	keyRangeGroup, keyRangeValues := plan.IndexGroup, fetchState.IndexGroupValues
	if keyRangeGroup.BaseGroup != nil {
		keyRangeGroup, keyRangeValues = *keyRangeGroup.BaseGroup, keyRangeGroup.BaseGroupValues(keyRangeValues)
	}
	if keyRangeGroup.InheritFromKey {
		keyStatements := make([]ColumnStatement, 0, len(keyRangeGroup.SourceColumns))
		for columnIndex, sourceColumn := range keyRangeGroup.SourceColumns {
			keyStatements = append(keyStatements, ColumnStatement{Col: sourceColumn.Column.GetName(), Operator: "=", Value: keyRangeValues[columnIndex]})
		}
		rewrittenStatements, canRewrite := core.BuildKeyIntPackingStatements(keyStatements)
		if !canRewrite || len(rewrittenStatements) == 0 {
			panic(fmt.Sprintf(`QueryIndexGroup: TypeCacheGroupFromKey fetch could not rewrite keys for table "%v"`, core.Name))
		}
		keyPredicate := rewrittenStatements[0]
		if keyPredicate.Operator != "BETWEEN" {
			return withPartition(KeyValues{Column: keyPredicate.Col, Values: []any{keyPredicate.Value}})
		}
		return rangeStep(KeyRange{
			EqualPrefix: partitionPrefix, BoundColumn: keyPredicate.Col,
			From: keyPredicate.From[0].Value, FromInclusive: true,
			To: keyPredicate.To[0].Value, ToInclusive: false,
		})
	}

	indexGroup := plan.IndexGroup
	switch {
	case indexGroup.VirtualColumn == nil || indexGroup.VirtualColumn.IsNil():
		return withPartition(KeyValues{Column: indexGroup.SourceColumns[0].Column.GetName(), Values: []any{fetchState.IndexGroupValues[0]}})
	case indexGroup.UsesCollectionValues:
		return AccessStep{Element: &ElementLookup{EqualPrefix: partitionPrefix, Column: indexGroup.VirtualColumn.GetName(), Value: fetchState.HashValue}}
	}
	return withPartition(KeyValues{Column: indexGroup.VirtualColumn.GetName(), Values: []any{fetchState.HashValue}})
}

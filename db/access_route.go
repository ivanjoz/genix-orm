package db

import (
	"fmt"
	"slices"
)

// AccessRouteKind names how a query reaches its rows.
type AccessRouteKind int8

const (
	// AccessRoutePrimaryKey reads the base table: a key prefix plus the rest as Filter.
	AccessRoutePrimaryKey AccessRouteKind = iota
	// AccessRouteSource reads an IndexSource whose steps PlanSourceSteps binds (a view, a view
	// table, a cache-group hash).
	AccessRouteSource
	// AccessRouteKeyConcatenated rewrites predicates on a concatenated key's columns into a range
	// on the key text.
	AccessRouteKeyConcatenated
	// AccessRouteKeyIntPacking rewrites predicates on an int-packed key's components into a range
	// on the packed key.
	AccessRouteKeyIntPacking
	// AccessRouteCompositeBucket reads the bucket-hash indexes of a composite-bucketing index.
	AccessRouteCompositeBucket
	// AccessRouteNativeIndex reads the base table through a single-column or packed secondary index
	// the database applies itself: the predicates stay on their own columns.
	AccessRouteNativeIndex
)

// AccessRoute is the shape-only half of planning: which route and source serve a query, and how
// its statements split between them. It depends only on the query shape (columns, operators,
// value counts), so a driver caches it per computeSelectShapeHash and binds values per query.
type AccessRoute struct {
	Kind   AccessRouteKind
	Source *IndexSource
	// ReadsSourceTable: the rows come from the source itself (a projected view or view table)
	// rather than from the base table.
	ReadsSourceTable bool
	// SelectedStatementIndexes are the statements the source binds, RemainingStatementIndexes the
	// ones left as Filter, PostFilterStatementIndexes the ones re-checked in memory. All index the
	// query's statements followed by FixedValueFanoutStatements.
	SelectedStatementIndexes   []int
	RemainingStatementIndexes  []int
	PostFilterStatementIndexes []int
	// FixedValueFanoutStatements are the schema-derived IN predicates that filled a gap in the
	// source's key prefix; they depend only on the schema, so they are appended at bind time.
	FixedValueFanoutStatements []ColumnStatement
	RequiresDeduplication      bool
}

// CanReadProjectionFrom tells whether a source holds every selected column, so a read can be
// answered from the source alone. A source with no AvailableColumns holds every column.
func CanReadProjectionFrom(source *IndexSource, selectedColumnNames []string) bool {
	if source == nil || len(source.AvailableColumns) == 0 {
		return true
	}
	for _, selectedColumnName := range selectedColumnNames {
		if !slices.Contains(source.AvailableColumns, selectedColumnName) {
			if ShouldLog() {
				fmt.Printf("Projected view rejected: view=%s missing_column=%s available=%v\n",
					source.Name, selectedColumnName, source.AvailableColumns)
			}
			return false
		}
	}
	return true
}

// ChooseAccessRoute picks the route for a query shape. selectedColumnNames is the projection, which
// decides whether a projected view can answer the read by itself.
func (core *TableCore) ChooseAccessRoute(statements []ColumnStatement, capabilities []QueryCapability, selectedColumnNames []string) *AccessRoute {
	if core.planCompositeBucketSteps(statements) != nil {
		return &AccessRoute{Kind: AccessRouteCompositeBucket, RequiresDeduplication: true}
	}

	route := &AccessRoute{Kind: AccessRoutePrimaryKey}
	bestCapability := MatchQueryCapability(statements, capabilities)
	bestCapability, route.FixedValueFanoutStatements = core.ApplyFixedValueFanout(statements, bestCapability, capabilities)
	statements = append(slices.Clone(statements), route.FixedValueFanoutStatements...)
	if bestCapability == nil {
		return route
	}

	if bestCapability.Source == nil {
		if bestCapability.IsKey {
			if _, canRewrite := core.BuildKeyConcatenatedStatements(statements); canRewrite {
				route.Kind = AccessRouteKeyConcatenated
			} else if _, canRewrite := core.BuildKeyIntPackingStatements(statements); canRewrite {
				route.Kind = AccessRouteKeyIntPacking
			}
		}
		return route
	}

	source := bestCapability.Source
	// A cache-group hash (Type 3) is a column of the base table; a view (Type >= 6) is read from
	// its own table, which it can only do when it holds every selected column.
	isIndexGroupSource := source.Type == 3 && HasAccessPlanner(source)
	// A unique index is an index of the base table, never a table of its own.
	isProjectedView := source.Type >= 6 && source.Kind != IndexKindUnique && CanReadProjectionFrom(source, selectedColumnNames)
	if !isIndexGroupSource && !isProjectedView {
		if source.Type == 1 || source.Type == 2 || source.Kind == IndexKindUnique {
			// The database applies its own secondary index to the base-table predicates.
			route.Kind, route.Source = AccessRouteNativeIndex, source
		}
		return route
	}

	route.Kind, route.Source = AccessRouteSource, source
	route.ReadsSourceTable = isProjectedView
	// A view table holds one row per fan-out element, so a record can come back several times.
	route.RequiresDeduplication = isProjectedView && source.Type == 9
	for statementIndex, statement := range statements {
		if slices.Contains(source.Columns, statement.Col) {
			route.SelectedStatementIndexes = append(route.SelectedStatementIndexes, statementIndex)
			continue
		}
		// A BETWEEN is served by the source when every one of its columns is.
		if len(statement.From) > 0 && !slices.ContainsFunc(statement.From, func(betweenStatement ColumnStatement) bool {
			return !slices.Contains(source.Columns, betweenStatement.Col)
		}) {
			route.SelectedStatementIndexes = append(route.SelectedStatementIndexes, statementIndex)
			continue
		}
		route.RemainingStatementIndexes = append(route.RemainingStatementIndexes, statementIndex)
	}
	if source.RequiresPostFilter || isIndexGroupSource {
		// An overfetching source (and a group hash, which can collide) is re-checked exactly.
		route.PostFilterStatementIndexes = slices.Clone(route.SelectedStatementIndexes)
		route.RequiresDeduplication = true
	}
	return route
}

// primaryKeyColumnNames is the base table key in order, partition first.
func (core *TableCore) primaryKeyColumnNames() []string {
	keyColumnNames := []string{}
	if partitionColumn := core.GetPartKey(); partitionColumn != nil && !partitionColumn.IsNil() {
		keyColumnNames = append(keyColumnNames, partitionColumn.GetName())
	}
	for _, keyColumn := range core.Keys {
		keyColumnNames = append(keyColumnNames, keyColumn.GetName())
	}
	return keyColumnNames
}

func pickStatements(statements []ColumnStatement, indexes []int) []ColumnStatement {
	picked := make([]ColumnStatement, 0, len(indexes))
	for _, statementIndex := range indexes {
		if statementIndex >= 0 && statementIndex < len(statements) {
			picked = append(picked, statements[statementIndex])
		}
	}
	return picked
}

// BindAccessPlan binds a query's statement values into the route chosen for its shape. An error
// means the route no longer serves these values (a refused source, a rewrite that stopped applying)
// or the query filters on a computed column outside its cache group.
func (core *TableCore) BindAccessPlan(route *AccessRoute, tableInfo *TableInfo) (*AccessPlan, error) {
	statements := append(tableInfo.SelectStatements(), route.FixedValueFanoutStatements...)
	plan := &AccessPlan{Source: route.Source, Limit: tableInfo.Limit, Descending: tableInfo.IsOrderDescending()}
	// unservedStatements are the ones the database is asked to filter on; a computed column can
	// only be queried through its cache group, never filtered.
	var unservedStatements []ColumnStatement

	switch route.Kind {
	case AccessRoutePrimaryKey, AccessRouteNativeIndex:
		keyColumnNames := core.primaryKeyColumnNames()
		if route.Kind == AccessRouteNativeIndex {
			keyColumnNames = route.Source.Columns
		}
		step, filter := planKeyPrefixStep(keyColumnNames, "", statements)
		plan.Steps, plan.Filter, unservedStatements = []AccessStep{step}, filter, statements

	case AccessRouteKeyConcatenated, AccessRouteKeyIntPacking:
		rewrite := core.BuildKeyConcatenatedStatements
		if route.Kind == AccessRouteKeyIntPacking {
			rewrite = core.BuildKeyIntPackingStatements
		}
		rewrittenStatements, canRewrite := rewrite(statements)
		if !canRewrite {
			return nil, fmt.Errorf("the smart key select shape no longer matches the cached route")
		}
		step, filter := planKeyPrefixStep(core.primaryKeyColumnNames(), "", rewrittenStatements)
		plan.Steps, plan.Filter, unservedStatements = []AccessStep{step}, filter, rewrittenStatements

	case AccessRouteSource:
		steps, sourceFilter, err := PlanSourceSteps(route.Source, pickStatements(statements, route.SelectedStatementIndexes))
		if err != nil {
			return nil, fmt.Errorf(`view "%v" cannot serve this query's predicates: %w`, route.Source.Name, err)
		}
		remainingStatements := pickStatements(statements, route.RemainingStatementIndexes)
		plan.Steps = steps
		plan.Filter = append(sourceFilter, remainingStatements...)
		plan.PostFilter = pickStatements(statements, route.PostFilterStatementIndexes)
		unservedStatements = remainingStatements

	case AccessRouteCompositeBucket:
		compositePlan := core.planCompositeBucketSteps(statements)
		if compositePlan == nil {
			return nil, fmt.Errorf("composite bucket select shape no longer matches the cached route")
		}
		for _, statement := range statements {
			if !compositePlan.handledColumns[statement.Col] {
				plan.Filter = append(plan.Filter, statement)
			}
		}
		plan.Steps, plan.PostFilter, unservedStatements = compositePlan.steps, compositePlan.filterStatements, plan.Filter
	}

	if err := core.RejectComputedStatements(unservedStatements); err != nil {
		return nil, err
	}
	return plan, nil
}

// compositeBucketPlan is the bound read of a composite-bucketing index: one element lookup per
// (bucket x source tuple), the columns those lookups pin, and what must be re-checked exactly.
type compositeBucketPlan struct {
	steps            []AccessStep
	handledColumns   map[string]bool
	filterStatements []ColumnStatement
}

// planCompositeBucketSteps serves a range on a composite-bucketed column: the range is covered by
// buckets of the declared sizes, and every bucket combined with each tuple of the other source
// columns' values is one hash, looked up in that bucket size's hash collection. nil when no
// composite index applies to the statements.
func (core *TableCore) planCompositeBucketSteps(statements []ColumnStatement) *compositeBucketPlan {
	partitionColumn := core.GetPartKey()
	if partitionColumn == nil || partitionColumn.IsNil() {
		return nil
	}
	statementsByColumn := map[string][]ColumnStatement{}
	for _, statement := range statements {
		statementsByColumn[statement.Col] = append(statementsByColumn[statement.Col], statement)
	}
	partitionStatements := statementsByColumn[partitionColumn.GetName()]
	if len(partitionStatements) == 0 || partitionStatements[0].Operator != "=" {
		return nil
	}

	for _, compositeIndex := range core.CompositeBucketIndexes {
		bucketColumnName := compositeIndex.BucketColumn.GetName()
		bucketStatements := statementsByColumn[bucketColumnName]
		if len(bucketStatements) == 0 {
			continue
		}
		from, to, isRange := BetweenRange(bucketStatements[0], compositeIndex.BucketIsWeek)
		if !isRange {
			continue
		}

		// Every other source column must be pinned by hashable values.
		filterStatements := []ColumnStatement{bucketStatements[0]}
		handledColumns := map[string]bool{bucketColumnName: true, partitionColumn.GetName(): true}
		valueGroups := [][]int64{{}}
		isServable := true
		for _, sourceColumn := range compositeIndex.SourceColumns {
			if sourceColumn.GetName() == bucketColumnName {
				continue
			}
			sourceStatements := statementsByColumn[sourceColumn.GetName()]
			if len(sourceStatements) == 0 {
				isServable = false
				break
			}
			values, isHashable := StatementValuesForHash(sourceStatements[0])
			if !isHashable || len(values) == 0 {
				isServable = false
				break
			}
			valueGroups = expandValueGroups(valueGroups, values)
			filterStatements = append(filterStatements, sourceStatements[0])
			handledColumns[sourceColumn.GetName()] = true
		}
		if !isServable {
			continue
		}
		bucketSelections := PlanCompositeBuckets(from, to, compositeIndex.BucketSizes)
		if len(bucketSelections) == 0 {
			continue
		}

		partitionPrefix := []KeyValues{{Column: partitionColumn.GetName(), Values: []any{partitionStatements[0].GetValue()}}}
		steps := []AccessStep{}
		for _, bucketSelection := range bucketSelections {
			virtualColumn := compositeIndex.VirtualColumnsBySize[bucketSelection.BucketSize]
			if virtualColumn == nil {
				isServable = false
				break
			}
			for _, valueGroup := range valueGroups {
				// The hash takes the source columns in declaration order, the bucket ID in the
				// bucketed column's place.
				hashValues := make([]int64, 0, len(compositeIndex.SourceColumns))
				otherValueIndex := 0
				for _, sourceColumn := range compositeIndex.SourceColumns {
					if sourceColumn.GetName() == bucketColumnName {
						hashValues = append(hashValues, bucketSelection.BucketID)
						continue
					}
					hashValues = append(hashValues, valueGroup[otherValueIndex])
					otherValueIndex++
				}
				steps = append(steps, AccessStep{Element: &ElementLookup{
					EqualPrefix: partitionPrefix, Column: virtualColumn.GetName(), Value: HashInt64(hashValues...),
				}})
			}
		}
		if !isServable || len(steps) == 0 {
			continue
		}
		if ShouldLog() {
			fmt.Printf("CompositeBucketing select plan selected: index=%s buckets=%d statements=%d rangeNorm=[%d,%d] rangeRaw=[%d,%d] isWeek=%v\n",
				compositeIndex.Name, len(bucketSelections), len(steps), from, to,
				ConvertToInt64(bucketStatements[0].From[0].Value), ConvertToInt64(bucketStatements[0].To[0].Value), compositeIndex.BucketIsWeek)
		}
		return &compositeBucketPlan{steps: steps, handledColumns: handledColumns, filterStatements: filterStatements}
	}
	return nil
}

package scylla

import (
	"fmt"
	"hash/fnv"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/ivanjoz/genix-orm/db"
)

type selectExecutionRoute int8

const (
	selectRouteAllStatements selectExecutionRoute = iota
	selectRouteViewStatements
	selectRouteKeyConcatenated
	selectRouteKeyIntPacking
	selectRouteCompositeBucket
	selectRouteNativeGroupBy
)

type selectPlanCache struct {
	mutex sync.RWMutex
	plans map[uint64]*SelectStatement
}

type boundWhereClause struct {
	Clause string
	Values []any
}

type BoundSelectStatement struct {
	QueryStr             string
	QueryValues          []any
	PostFilterStatements []ColumnStatement
}

type BoundSelectPlan struct {
	Statements            []BoundSelectStatement
	ScanColumns           []selectScanColumn
	RequiresDeduplication bool
}

// SelectStatement caches one compiled select shape so repeated queries skip capability matching and source selection.
type SelectStatement struct {
	hash uint64
	// queryTemplate already includes SELECT + FROM and expects the final WHERE/GROUP/ORDER/LIMIT suffix.
	queryTemplate string
	scanColumns   []selectScanColumn
	route         selectExecutionRoute
	sourceView    *viewInfo
	// selectedStatementIndexes tracks the predicates consumed by a source view so bind-time can reuse current values.
	selectedStatementIndexes   []int
	remainingStatementIndexes  []int
	postFilterStatementIndexes []int
	// fixedValueFanoutStatements are the schema-derived IN predicates that filled a gap in the
	// chosen source's key prefix. They depend only on the schema, so bind-time appends them after
	// the query's own statements exactly as compile-time did and every cached index stays valid.
	fixedValueFanoutStatements []ColumnStatement
	requiresDeduplication      bool
	orderBy                    string
	orderColumnName            string
	limit                      int32
	allowFilter                bool
	groupByColumns             []string
}

func newSelectPlanCache() *selectPlanCache {
	return &selectPlanCache{plans: map[uint64]*SelectStatement{}}
}

func (e *selectPlanCache) Load(hash uint64) (*SelectStatement, bool) {
	if e == nil {
		return nil, false
	}

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	plan, cacheHit := e.plans[hash]
	return plan, cacheHit
}

func (e *selectPlanCache) Store(hash uint64, plan *SelectStatement) {
	if e == nil || plan == nil {
		return
	}

	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.plans[hash] = plan
}

func collectSelectStatements(tableInfo *TableInfo) []ColumnStatement {
	// Keep statement extraction centralized so compile and bind share the exact same logical input order.
	statements := make([]ColumnStatement, 0, len(tableInfo.Statements)+1)
	statements = append(statements, tableInfo.Statements...)
	if len(tableInfo.Between.From) > 0 {
		statements = append(statements, tableInfo.Between)
	}
	return statements
}

func shouldAutoSelectColumn(column IColInfo) bool {
	if column == nil || column.IsNil() {
		return false
	}
	if column.GetInfo().IsVirtual {
		return false
	}
	// Managed write-only columns may exist in Scylla metadata without a backing struct field.
	// They should not be injected into default SELECT projections.
	return column.GetInfo().Field != nil
}

func buildSelectProjection(tableInfo *TableInfo, scyllaTable ScyllaTable) ([]string, []selectScanColumn, []string) {
	columnNames := []string{}
	scanColumns := []selectScanColumn{}
	selectExpressions := []string{}

	if len(tableInfo.ColumnsInclude) > 0 {
		for _, col := range tableInfo.ColumnsInclude {
			columnNames = append(columnNames, col.GetName())
		}
		scanColumns = buildDefaultScanColumns(columnNames)
		selectExpressions = append(selectExpressions, columnNames...)
		return columnNames, scanColumns, selectExpressions
	}

	if len(tableInfo.ColumnsExclude) > 0 {
		excludedColumns := make([]string, 0, len(tableInfo.ColumnsExclude))
		for _, col := range tableInfo.ColumnsExclude {
			excludedColumns = append(excludedColumns, col.GetName())
		}
		for _, col := range scyllaTable.Columns {
			if slices.Contains(excludedColumns, col.GetName()) || !shouldAutoSelectColumn(col) {
				continue
			}
			columnNames = append(columnNames, col.GetName())
		}
	} else {
		for _, col := range scyllaTable.Columns {
			if !shouldAutoSelectColumn(col) {
				continue
			}
			columnNames = append(columnNames, col.GetName())
		}
	}

	scanColumns = buildDefaultScanColumns(columnNames)
	selectExpressions = append(selectExpressions, columnNames...)
	return columnNames, scanColumns, selectExpressions
}

func computeSelectShapeHash(tableInfo *TableInfo, scyllaTable ScyllaTable) uint64 {
	// Hash only the query shape so the same logical select can reuse the compiled plan for different values.
	hashBuilder := fnv.New64a()

	writeText := func(value string) {
		_, _ = hashBuilder.Write([]byte(value))
		_, _ = hashBuilder.Write([]byte{0})
	}

	writeText(scyllaTable.Name)
	columnNames, _, _ := buildSelectProjection(tableInfo, scyllaTable)
	writeText("projection:resolved")
	for _, columnName := range columnNames {
		writeText(columnName)
	}

	if len(tableInfo.GroupByColumns) > 0 {
		writeText("group-by")
		for _, col := range tableInfo.GroupByColumns {
			writeText(col.GetName())
			writeText(col.AggregateFn)
		}
	}

	for _, statement := range collectSelectStatements(tableInfo) {
		writeText(statement.Col)
		writeText(db.CapabilityOpForStatement(statement))

		switch statement.Operator {
		case "IN":
			writeText(fmt.Sprintf("values:%d", len(statement.Values)))
		case "BETWEEN":
			writeText(fmt.Sprintf("between:%d", len(statement.From)))
		default:
			writeText("values:1")
		}
	}

	writeText(tableInfo.OrderBy)
	writeText(fmt.Sprintf("limit:%d", tableInfo.Limit))
	if tableInfo.AllowFilter {
		writeText("allow-filter")
	}

	return hashBuilder.Sum64()
}

func pickStatementsByIndexes(statements []ColumnStatement, indexes []int) []ColumnStatement {
	pickedStatements := make([]ColumnStatement, 0, len(indexes))
	for _, statementIndex := range indexes {
		if statementIndex < 0 || statementIndex >= len(statements) {
			continue
		}
		pickedStatements = append(pickedStatements, statements[statementIndex])
	}
	return pickedStatements
}

func makeSelectQueryTemplate(selectExpressions []string, keyspace, sourceTableName string) string {
	return fmt.Sprintf("SELECT %v FROM %v.%v %%v", strings.Join(selectExpressions, ", "), keyspace, sourceTableName)
}

func getMaxClusteringKeyRestrictionsPerQuery() int {
	// Keep the Scylla clustering-key fanout limit configurable while staying safe by default.
	// The connection params win so each project can pass its own node setting through
	// ConnParams; the environment variable stays as the fallback for tools and tests.
	if connParams.MaxClusteringKey > 0 {
		return connParams.MaxClusteringKey
	}

	rawMaxClusteringKeys := strings.TrimSpace(os.Getenv("MAX_CLUSTERING_KEY"))
	if rawMaxClusteringKeys == "" {
		return 100
	}

	maxClusteringKeys, parseError := strconv.Atoi(rawMaxClusteringKeys)
	if parseError != nil || maxClusteringKeys <= 0 {
		fmt.Printf("Invalid MAX_CLUSTERING_KEY=%q. Using default 100.\n", rawMaxClusteringKeys)
		return 100
	}
	return maxClusteringKeys
}

func buildRemainingWhereClauseBatches(remainingStatements []ColumnStatement) []boundWhereClause {
	if len(remainingStatements) == 0 {
		return []boundWhereClause{{}}
	}

	maxClusteringKeys := getMaxClusteringKeyRestrictionsPerQuery()
	statementBatches := [][]ColumnStatement{{}}
	currentCartesianProductSize := 1

	for _, statement := range remainingStatements {
		if statement.Operator != "IN" || len(statement.Values) == 0 {
			for batchIndex := range statementBatches {
				statementBatches[batchIndex] = append(statementBatches[batchIndex], statement)
			}
			continue
		}

		maxValuesPerBatch := maxClusteringKeys / currentCartesianProductSize
		if maxValuesPerBatch < 1 {
			maxValuesPerBatch = 1
		}

		valueChunks := db.ChunkValues(statement.Values, maxValuesPerBatch)
		nextStatementBatches := make([][]ColumnStatement, 0, len(statementBatches)*len(valueChunks))
		maxChunkSize := 0

		for _, valueChunk := range valueChunks {
			if len(valueChunk) > maxChunkSize {
				maxChunkSize = len(valueChunk)
			}
			for _, currentBatchStatements := range statementBatches {
				statementBatch := slices.Clone(currentBatchStatements)
				statementCopy := statement
				statementCopy.Values = slices.Clone(valueChunk)
				statementBatch = append(statementBatch, statementCopy)
				nextStatementBatches = append(nextStatementBatches, statementBatch)
			}
		}

		statementBatches = nextStatementBatches
		currentCartesianProductSize *= max(1, maxChunkSize)
	}

	whereClauseBatches := make([]boundWhereClause, 0, len(statementBatches))
	for _, statementBatch := range statementBatches {
		boundClauses := buildRemainingWhereClauses(statementBatch)
		clauseParts := make([]string, 0, len(boundClauses))
		queryValues := make([]any, 0, len(boundClauses))
		for _, boundClause := range boundClauses {
			clauseParts = append(clauseParts, boundClause.Clause)
			queryValues = append(queryValues, boundClause.Values...)
		}
		whereClauseBatches = append(whereClauseBatches, boundWhereClause{
			Clause: strings.Join(clauseParts, " AND "),
			Values: queryValues,
		})
	}

	if len(whereClauseBatches) > 1 {
		fmt.Printf("Select batching IN query: statements=%d max_clustering_key=%d batches=%d\n",
			len(remainingStatements), maxClusteringKeys, len(whereClauseBatches))
	}

	return whereClauseBatches
}

func buildBoundSelectPlan(
	queryTemplate string,
	scanColumns []selectScanColumn,
	requiresDeduplication bool,
	whereStatements []boundWhereClause,
	remainingStatements []ColumnStatement,
	postFilterStatements []ColumnStatement,
	groupByColumns []string,
	orderBy string,
	orderColumnName string,
	limit int32,
	allowFilter bool,
) *BoundSelectPlan {
	if len(whereStatements) == 0 {
		whereStatements = []boundWhereClause{{}}
	}

	remainingWhereClauseBatches := buildRemainingWhereClauseBatches(remainingStatements)
	boundStatements := make([]BoundSelectStatement, 0, max(1, len(whereStatements)*len(remainingWhereClauseBatches)))

	for _, whereStatement := range whereStatements {
		for _, remainingWhereClause := range remainingWhereClauseBatches {
			whereStatementCombined := whereStatement.Clause
			whereRemainClause := remainingWhereClause.Clause
			queryValues := append([]any{}, whereStatement.Values...)
			if remainingWhereClause.Clause != "" {
				if whereStatementCombined != "" {
					whereRemainClause = " AND " + whereRemainClause
				}
				whereStatementCombined += whereRemainClause
				queryValues = append(queryValues, remainingWhereClause.Values...)
			}
			if whereStatementCombined != "" {
				whereStatementCombined = " WHERE " + whereStatementCombined
			}
			if len(groupByColumns) > 0 {
				whereStatementCombined += " GROUP BY " + strings.Join(groupByColumns, ", ")
			}
			if orderBy != "" {
				whereStatementCombined += " " + fmt.Sprintf(orderBy, orderColumnName)
			}
			if limit > 0 {
				whereStatementCombined += fmt.Sprintf(" LIMIT %v", limit)
			}
			if allowFilter {
				whereStatementCombined += " ALLOW FILTERING"
			}

			boundStatements = append(boundStatements, BoundSelectStatement{
				QueryStr:             fmt.Sprintf(queryTemplate, whereStatementCombined),
				QueryValues:          queryValues,
				PostFilterStatements: slices.Clone(postFilterStatements),
			})
		}
	}

	return &BoundSelectPlan{
		Statements:            boundStatements,
		ScanColumns:           slices.Clone(scanColumns),
		RequiresDeduplication: requiresDeduplication,
	}
}

func buildRemainingStatementsForCompositePlan(statements []ColumnStatement, compositePlan *compositeBucketQueryPlan) []ColumnStatement {
	remainingStatements := []ColumnStatement{}
	for _, statement := range statements {
		if !compositePlan.handledColumns[statement.Col] {
			remainingStatements = append(remainingStatements, statement)
		}
	}
	return remainingStatements
}

func compileSelectStatement(tableInfo *TableInfo, scyllaTable ScyllaTable) (*SelectStatement, error) {
	statements := collectSelectStatements(tableInfo)
	selectShapeHash := computeSelectShapeHash(tableInfo, scyllaTable)

	if len(tableInfo.GroupByColumns) > 0 {
		groupByPlan, err := buildNativeGroupByPlan(tableInfo, statements, scyllaTable)
		if err != nil {
			return nil, err
		}
		if groupByPlan == nil {
			return nil, fmt.Errorf("group by select shape did not produce a native plan")
		}

		sourceTableName := scyllaTable.Name
		if groupByPlan.ViewTableName != "" {
			sourceTableName = groupByPlan.ViewTableName
		}

		orderColumnName := ""
		if groupByPlan.OrderColumn != nil && !groupByPlan.OrderColumn.IsNil() {
			orderColumnName = groupByPlan.OrderColumn.GetName()
		}

		compiledStatement := &SelectStatement{
			hash:                  selectShapeHash,
			queryTemplate:         makeSelectQueryTemplate(groupByPlan.SelectExpressions, scyllaTable.Namespace, sourceTableName),
			scanColumns:           slices.Clone(groupByPlan.ScanColumns),
			route:                 selectRouteNativeGroupBy,
			orderBy:               tableInfo.OrderBy,
			orderColumnName:       orderColumnName,
			limit:                 tableInfo.Limit,
			allowFilter:           tableInfo.AllowFilter,
			groupByColumns:        slices.Clone(groupByPlan.GroupByColumns),
			requiresDeduplication: false,
		}

		if ShouldLog() {
			fmt.Printf("Select plan compiled: table=%s hash=%d route=%d source=%s post_filter=false dedup=false\n",
				scyllaTable.Name, compiledStatement.hash, compiledStatement.route, sourceTableName)
		}
		return compiledStatement, nil
	}

	columnNames, scanColumns, selectExpressions := buildSelectProjection(tableInfo, scyllaTable)
	viewTableName := scyllaTable.Name
	orderColumnName := ""
	if len(scyllaTable.Keys) > 0 {
		orderColumnName = scyllaTable.Keys[0].GetName()
	}

	compiledStatement := &SelectStatement{
		hash:            computeSelectShapeHash(tableInfo, scyllaTable),
		scanColumns:     slices.Clone(scanColumns),
		orderBy:         tableInfo.OrderBy,
		orderColumnName: orderColumnName,
		limit:           tableInfo.Limit,
		allowFilter:     tableInfo.AllowFilter,
		route:           selectRouteAllStatements,
	}

	if compositePlan := tryBuildCompositeBucketPlan(statements, scyllaTable); compositePlan != nil {
		compiledStatement.route = selectRouteCompositeBucket
		compiledStatement.requiresDeduplication = true
		compiledStatement.queryTemplate = makeSelectQueryTemplate(selectExpressions, scyllaTable.Namespace, viewTableName)
		if ShouldLog() {
			fmt.Printf("Select plan compiled: table=%s hash=%d route=%d source=%s post_filter=true dedup=true\n",
				scyllaTable.Name, compiledStatement.hash, compiledStatement.route, viewTableName)
		}
		return compiledStatement, nil
	}

	bestCapability := db.MatchQueryCapability(statements, scyllaTable.capabilities)
	bestCapability, fanoutStatements := scyllaTable.ApplyFixedValueFanout(statements, bestCapability, scyllaTable.capabilities)
	if len(fanoutStatements) > 0 {
		// The fan-out predicates take part in planning from here on, so they must sit in the
		// statement list the cached indexes are computed against.
		statements = append(slices.Clone(statements), fanoutStatements...)
		compiledStatement.fixedValueFanoutStatements = fanoutStatements
	}

	if bestCapability != nil {
		if bestCapability.Source != nil {
			selectedView := scyllaTable.viewOfSource(bestCapability.Source)
			// Index group views (Type 3) query the main table via a virtual hash column;
			// projected views (Type >= 6) redirect to a separate view table.
			hasAccessPlanner := db.HasAccessPlanner(selectedView.IndexSource)
			isIndexGroupView := selectedView.Type == 3 && hasAccessPlanner
			isProjectedView := selectedView.Type >= 6 && canUseProjectedView(columnNames, selectedView)

			if isProjectedView {
				viewTableName = selectedView.Name
				if selectedView.Type == 9 {
					compiledStatement.requiresDeduplication = true
				}
				// The view's first clustering column drives ORDER BY in the view table, not the base ID.
				if selectedView.Column != nil && !selectedView.Column.IsNil() {
					compiledStatement.orderColumnName = selectedView.Column.GetName()
				}
			}

			if (isProjectedView && hasAccessPlanner) || isIndexGroupView {
				selectedStatementIndexes := []int{}
				remainingStatementIndexes := []int{}

				for statementIndex, statement := range statements {
					if slices.Contains(selectedView.Columns, statement.Col) {
						selectedStatementIndexes = append(selectedStatementIndexes, statementIndex)
						continue
					}
					if len(statement.From) > 0 {
						isIncluded := true
						for _, betweenStatement := range statement.From {
							if !slices.Contains(selectedView.Columns, betweenStatement.Col) {
								isIncluded = false
								break
							}
						}
						if isIncluded {
							selectedStatementIndexes = append(selectedStatementIndexes, statementIndex)
						} else {
							remainingStatementIndexes = append(remainingStatementIndexes, statementIndex)
						}
						continue
					}
					remainingStatementIndexes = append(remainingStatementIndexes, statementIndex)
				}

				compiledStatement.route = selectRouteViewStatements
				compiledStatement.sourceView = selectedView
				compiledStatement.selectedStatementIndexes = selectedStatementIndexes
				compiledStatement.remainingStatementIndexes = remainingStatementIndexes
				if selectedView.RequiresPostFilter || isIndexGroupView {
					// Index group views: always post-filter to guard against hash collisions.
					compiledStatement.postFilterStatementIndexes = slices.Clone(selectedStatementIndexes)
					compiledStatement.requiresDeduplication = true
				}
			}
		} else if bestCapability.IsKey {
			if _, canRewrite := scyllaTable.BuildKeyConcatenatedStatements(statements); canRewrite {
				compiledStatement.route = selectRouteKeyConcatenated
			} else if _, canRewrite := scyllaTable.BuildKeyIntPackingStatements(statements); canRewrite {
				compiledStatement.route = selectRouteKeyIntPacking
			}
		}
	}

	compiledStatement.queryTemplate = makeSelectQueryTemplate(selectExpressions, scyllaTable.Namespace, viewTableName)

	if ShouldLog() {
		fmt.Printf("Select plan compiled: table=%s hash=%d route=%d source=%s post_filter=%v dedup=%v\n",
			scyllaTable.Name, compiledStatement.hash, compiledStatement.route, viewTableName,
			len(compiledStatement.postFilterStatementIndexes) > 0, compiledStatement.requiresDeduplication)
	}

	return compiledStatement, nil
}

func (e *SelectStatement) Compute(tableInfo *TableInfo, scyllaTable ScyllaTable) (*BoundSelectPlan, error) {
	// Bind current values into the cached query shape without rerunning planner selection in selectExec.
	statements := collectSelectStatements(tableInfo)
	if len(e.fixedValueFanoutStatements) > 0 {
		statements = append(statements, e.fixedValueFanoutStatements...)
	}
	whereStatements := []boundWhereClause{{}}
	remainingStatements := statements
	postFilterStatements := pickStatementsByIndexes(statements, e.postFilterStatementIndexes)
	scanColumns := slices.Clone(e.scanColumns)
	queryTemplate := e.queryTemplate
	groupByColumns := slices.Clone(e.groupByColumns)
	orderColumnName := e.orderColumnName

	switch e.route {
	case selectRouteViewStatements:
		selectedStatements := pickStatementsByIndexes(statements, e.selectedStatementIndexes)
		remainingStatements = pickStatementsByIndexes(statements, e.remainingStatementIndexes)
		whereStatements = e.sourceView.prepareWhereClauses(selectedStatements...)
		if len(whereStatements) == 0 {
			// A view that cannot bind the predicates would otherwise yield zero bound statements,
			// which reads as an empty result set instead of a planning failure.
			return nil, fmt.Errorf(`view "%v" cannot serve this query's predicates`, e.sourceView.Name)
		}
	case selectRouteKeyConcatenated:
		rewrittenStatements, canRewrite := scyllaTable.BuildKeyConcatenatedStatements(statements)
		if !canRewrite {
			return nil, fmt.Errorf("key concatenated select shape no longer matches the cached route")
		}
		remainingStatements = rewrittenStatements
	case selectRouteKeyIntPacking:
		rewrittenStatements, canRewrite := scyllaTable.BuildKeyIntPackingStatements(statements)
		if !canRewrite {
			return nil, fmt.Errorf("key int packing select shape no longer matches the cached route")
		}
		remainingStatements = rewrittenStatements
	case selectRouteCompositeBucket:
		compositePlan := tryBuildCompositeBucketPlan(statements, scyllaTable)
		if compositePlan == nil {
			return nil, fmt.Errorf("composite bucket select shape no longer matches the cached route")
		}
		whereStatements = compositePlan.whereStatements
		remainingStatements = buildRemainingStatementsForCompositePlan(statements, compositePlan)
		postFilterStatements = slices.Clone(compositePlan.filterStatements)
	case selectRouteNativeGroupBy:
		groupByPlan, err := buildNativeGroupByPlan(tableInfo, statements, scyllaTable)
		if err != nil {
			return nil, err
		}
		if groupByPlan == nil {
			return nil, fmt.Errorf("group by select shape no longer matches the cached route")
		}

		sourceTableName := scyllaTable.Name
		if groupByPlan.ViewTableName != "" {
			sourceTableName = groupByPlan.ViewTableName
		}
		queryTemplate = makeSelectQueryTemplate(groupByPlan.SelectExpressions, scyllaTable.Namespace, sourceTableName)
		scanColumns = slices.Clone(groupByPlan.ScanColumns)
		groupByColumns = slices.Clone(groupByPlan.GroupByColumns)
		remainingStatements = nil
		whereStatements = slices.Clone(groupByPlan.WhereStatements)
		if groupByPlan.OrderColumn != nil && !groupByPlan.OrderColumn.IsNil() {
			orderColumnName = groupByPlan.OrderColumn.GetName()
		}
	}
	if err := scyllaTable.RejectComputedStatements(remainingStatements); err != nil {
		return nil, err
	}

	return buildBoundSelectPlan(
		queryTemplate,
		scanColumns,
		e.requiresDeduplication,
		whereStatements,
		remainingStatements,
		postFilterStatements,
		groupByColumns,
		e.orderBy,
		orderColumnName,
		e.limit,
		e.allowFilter,
	), nil
}

func tryGetOrCompileSelectStatement(tableInfo *TableInfo, scyllaTable ScyllaTable) (*SelectStatement, error) {
	selectShapeHash := computeSelectShapeHash(tableInfo, scyllaTable)
	// fmt.Printf("Select cache lookup: table=%s hash=%d\n", scyllaTable.name, selectShapeHash)

	if cachedPlan, cacheHit := scyllaTable.selectStatementCache.Load(selectShapeHash); cacheHit {
		// fmt.Printf("Select cache hit: table=%s hash=%d\n", scyllaTable.name, selectShapeHash)
		return cachedPlan, nil
	}

	// fmt.Printf("Select cache miss: table=%s hash=%d\n", scyllaTable.name, selectShapeHash)
	compiledStatement, err := compileSelectStatement(tableInfo, scyllaTable)
	if err != nil {
		return nil, err
	}

	scyllaTable.selectStatementCache.Store(selectShapeHash, compiledStatement)
	return compiledStatement, nil
}

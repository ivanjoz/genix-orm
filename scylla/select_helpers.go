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
	// accessRoute is the driver-neutral route db chose for this shape (nil for a native GROUP BY);
	// route and sourceView restate it for logs and tests.
	accessRoute     *db.AccessRoute
	route           selectExecutionRoute
	sourceView      *viewInfo
	orderBy         string
	orderColumnName string
	limit           int32
	allowFilter     bool
	groupByColumns  []string
}

// selectRouteOfAccessRoute is the CQL route label of a db access route.
func selectRouteOfAccessRoute(kind db.AccessRouteKind) selectExecutionRoute {
	switch kind {
	case db.AccessRouteSource:
		return selectRouteViewStatements
	case db.AccessRouteKeyConcatenated:
		return selectRouteKeyConcatenated
	case db.AccessRouteKeyIntPacking:
		return selectRouteKeyIntPacking
	case db.AccessRouteCompositeBucket:
		return selectRouteCompositeBucket
	}
	return selectRouteAllStatements
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

	for _, statement := range tableInfo.SelectStatements() {
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

func compileSelectStatement(tableInfo *TableInfo, scyllaTable ScyllaTable) (*SelectStatement, error) {
	statements := tableInfo.SelectStatements()
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
			hash:            selectShapeHash,
			queryTemplate:   makeSelectQueryTemplate(groupByPlan.SelectExpressions, scyllaTable.Namespace, sourceTableName),
			scanColumns:     slices.Clone(groupByPlan.ScanColumns),
			route:           selectRouteNativeGroupBy,
			orderBy:         tableInfo.OrderBy,
			orderColumnName: orderColumnName,
			limit:           tableInfo.Limit,
			allowFilter:     tableInfo.AllowFilter,
			groupByColumns:  slices.Clone(groupByPlan.GroupByColumns),
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

	// db chooses the route; only the CQL table to read and its ORDER BY column are decided here.
	accessRoute := scyllaTable.ChooseAccessRoute(statements, scyllaTable.capabilities, columnNames)
	compiledStatement := &SelectStatement{
		hash:            selectShapeHash,
		scanColumns:     slices.Clone(scanColumns),
		accessRoute:     accessRoute,
		route:           selectRouteOfAccessRoute(accessRoute.Kind),
		orderBy:         tableInfo.OrderBy,
		orderColumnName: orderColumnName,
		limit:           tableInfo.Limit,
		allowFilter:     tableInfo.AllowFilter,
	}
	if accessRoute.Kind == db.AccessRouteSource {
		compiledStatement.sourceView = scyllaTable.viewOfSource(accessRoute.Source)
	}
	if accessRoute.ReadsSourceTable {
		viewTableName = accessRoute.Source.Name
		// The view's first clustering column drives ORDER BY in the view table, not the base ID.
		if accessRoute.Source.Column != nil && !accessRoute.Source.Column.IsNil() {
			compiledStatement.orderColumnName = accessRoute.Source.Column.GetName()
		}
	}
	compiledStatement.queryTemplate = makeSelectQueryTemplate(selectExpressions, scyllaTable.Namespace, viewTableName)

	if ShouldLog() {
		fmt.Printf("Select plan compiled: table=%s hash=%d route=%d source=%s post_filter=%v dedup=%v\n",
			scyllaTable.Name, compiledStatement.hash, compiledStatement.route, viewTableName,
			len(accessRoute.PostFilterStatementIndexes) > 0, accessRoute.RequiresDeduplication)
	}
	return compiledStatement, nil
}

func (e *SelectStatement) Compute(tableInfo *TableInfo, scyllaTable ScyllaTable) (*BoundSelectPlan, error) {
	// Bind current values into the cached query shape without rerunning planner selection in selectExec.
	queryTemplate := e.queryTemplate
	scanColumns := slices.Clone(e.scanColumns)
	groupByColumns := slices.Clone(e.groupByColumns)
	orderColumnName := e.orderColumnName
	var whereStatements []boundWhereClause
	var postFilterStatements []ColumnStatement
	requiresDeduplication := false

	if e.route == selectRouteNativeGroupBy {
		// Native GROUP BY is Scylla-only and stays outside the AccessPlan.
		groupByPlan, err := buildNativeGroupByPlan(tableInfo, tableInfo.SelectStatements(), scyllaTable)
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
		whereStatements = slices.Clone(groupByPlan.WhereStatements)
		if groupByPlan.OrderColumn != nil && !groupByPlan.OrderColumn.IsNil() {
			orderColumnName = groupByPlan.OrderColumn.GetName()
		}
	} else {
		accessPlan, err := scyllaTable.BindAccessPlan(e.accessRoute, tableInfo)
		if err != nil {
			return nil, err
		}
		whereStatements = renderAccessPlan(&scyllaTable, accessPlan)
		postFilterStatements = accessPlan.PostFilter
		requiresDeduplication = e.accessRoute.RequiresDeduplication
	}

	return buildBoundSelectPlan(
		queryTemplate,
		scanColumns,
		requiresDeduplication,
		whereStatements,
		nil,
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

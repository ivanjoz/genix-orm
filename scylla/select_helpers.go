package scylla

import (
	"fmt"
	"hash/fnv"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
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
		writeText(capabilityOpForStatement(statement))

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

func chunkStatementInValues(values []any, chunkSize int) [][]any {
	if len(values) == 0 {
		return nil
	}
	if chunkSize <= 0 {
		chunkSize = 1
	}

	valueChunks := make([][]any, 0, (len(values)+chunkSize-1)/chunkSize)
	for startIndex := 0; startIndex < len(values); startIndex += chunkSize {
		endIndex := min(startIndex+chunkSize, len(values))
		valueChunks = append(valueChunks, slices.Clone(values[startIndex:endIndex]))
	}
	return valueChunks
}

// buildChunkedInWhereClauses turns one IN list into as many clauses as the node's
// clustering-key restriction ceiling requires. Views bind their predicates directly
// into a clause string, so they cannot go through buildRemainingWhereClauseBatches
// and need the split here instead.
func buildChunkedInWhereClauses(columnName string, inValues []any) []boundWhereClause {
	if len(inValues) == 0 {
		return nil
	}

	valueChunks := chunkStatementInValues(inValues, getMaxClusteringKeyRestrictionsPerQuery())
	whereClauses := make([]boundWhereClause, 0, len(valueChunks))

	for _, valueChunk := range valueChunks {
		if len(valueChunk) == 1 {
			whereClauses = append(whereClauses, boundWhereClause{
				Clause: fmt.Sprintf("%v = ?", columnName),
				Values: valueChunk,
			})
			continue
		}

		placeholders := make([]string, len(valueChunk))
		for placeholderIndex := range placeholders {
			placeholders[placeholderIndex] = "?"
		}
		whereClauses = append(whereClauses, boundWhereClause{
			Clause: fmt.Sprintf("%v IN (%v)", columnName, strings.Join(placeholders, ", ")),
			Values: valueChunk,
		})
	}

	if len(whereClauses) > 1 {
		fmt.Printf("View batching IN query: column=%s values=%d batches=%d\n",
			columnName, len(inValues), len(whereClauses))
	}

	return whereClauses
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

		valueChunks := chunkStatementInValues(statement.Values, maxValuesPerBatch)
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

func canRewriteKeyConcatenated(statements []ColumnStatement, scyllaTable ScyllaTable) bool {
	_, canRewrite := buildKeyConcatenatedStatements(statements, scyllaTable)
	return canRewrite
}

func buildKeyConcatenatedStatements(statements []ColumnStatement, scyllaTable ScyllaTable) ([]ColumnStatement, bool) {
	// Convert equality/range prefixes on smart concatenated keys into one physical PK predicate plus residual filters.
	keyCol := scyllaTable.Keys[0]
	hasKeyColQuery := false
	for _, st := range statements {
		if st.Col == keyCol.GetName() {
			hasKeyColQuery = true
			break
		}
	}
	if hasKeyColQuery || len(scyllaTable.keyConcatenated) == 0 {
		return nil, false
	}

	prefixValues := []any{}
	var rangeStatement *ColumnStatement
	handledColumns := map[string]bool{}

	for _, concatCol := range scyllaTable.keyConcatenated {
		found := false
		for statementIndex := range statements {
			statement := statements[statementIndex]
			if statement.Col != concatCol.GetName() {
				continue
			}
			if statement.Operator == "=" {
				prefixValues = append(prefixValues, statement.Value)
				handledColumns[statement.Col] = true
				found = true
				break
			}
			if slices.Contains(rangeOperators, statement.Operator) || statement.Operator == "BETWEEN" {
				rangeStatement = &statements[statementIndex]
				handledColumns[statement.Col] = true
				found = true
				break
			}
		}
		if !found || rangeStatement != nil {
			break
		}
	}

	if len(prefixValues) == 0 && rangeStatement == nil {
		return nil, false
	}

	prefixValueText := ""
	if len(prefixValues) > 0 {
		prefixValueText = MakeKeyConcat(prefixValues...)
	}

	var rewrittenStatement ColumnStatement
	if rangeStatement == nil {
		if len(prefixValues) == len(scyllaTable.keyConcatenated) {
			rewrittenStatement = ColumnStatement{Col: keyCol.GetName(), Operator: "=", Value: prefixValueText}
		} else {
			rewrittenStatement = ColumnStatement{
				Col:      keyCol.GetName(),
				Operator: "BETWEEN",
				From:     []ColumnStatement{{Col: keyCol.GetName(), Value: prefixValueText + "_"}},
				To:       []ColumnStatement{{Col: keyCol.GetName(), Value: prefixValueText + "_\uffff"}},
			}
		}
	} else {
		if rangeStatement.Operator == "BETWEEN" {
			valueFrom := MakeKeyConcat(append(prefixValues, rangeStatement.From[0].Value)...)
			valueTo := MakeKeyConcat(append(prefixValues, rangeStatement.To[0].Value)...)
			rewrittenStatement = ColumnStatement{
				Col:      keyCol.GetName(),
				Operator: "BETWEEN",
				From:     []ColumnStatement{{Col: keyCol.GetName(), Value: valueFrom}},
				To:       []ColumnStatement{{Col: keyCol.GetName(), Value: valueTo + "\uffff"}},
			}
		} else if rangeTransform, ok := smartRangeMap[rangeStatement.Operator]; ok {
			rangeValue := MakeKeyConcat(append(prefixValues, rangeStatement.Value)...)
			prefixMin, prefixMax := "", "\uffff"
			if prefixValueText != "" {
				prefixMin = prefixValueText + "_"
				prefixMax = prefixValueText + "_\uffff"
			}
			fromValue := rangeTransform.from(rangeValue, prefixMin, prefixMax)
			toValue := rangeTransform.to(rangeValue, prefixMin, prefixMax)
			rewrittenStatement = ColumnStatement{Col: keyCol.GetName(), Operator: "BETWEEN"}
			if fromValue != "" {
				rewrittenStatement.From = append(rewrittenStatement.From, ColumnStatement{Col: keyCol.GetName(), Operator: ">=", Value: fromValue})
			}
			if toValue != "" {
				rewrittenStatement.To = append(rewrittenStatement.To, ColumnStatement{Col: keyCol.GetName(), Operator: "<", Value: toValue})
			}
		}
	}

	rewrittenStatements := []ColumnStatement{rewrittenStatement}
	for _, statement := range statements {
		if !handledColumns[statement.Col] {
			rewrittenStatements = append(rewrittenStatements, statement)
		}
	}

	return rewrittenStatements, true
}

func canRewriteKeyIntPacking(statements []ColumnStatement, scyllaTable ScyllaTable) bool {
	_, canRewrite := buildKeyIntPackingStatements(statements, scyllaTable)
	return canRewrite
}

func buildKeyIntPackingStatements(statements []ColumnStatement, scyllaTable ScyllaTable) ([]ColumnStatement, bool) {
	// Convert equality/range prefixes on packed numeric keys into one physical PK predicate plus residual filters.
	keyCol := scyllaTable.Keys[0]
	hasKeyColQuery := false
	for _, st := range statements {
		if st.Col == keyCol.GetName() {
			hasKeyColQuery = true
			break
		}
	}
	if hasKeyColQuery || len(scyllaTable.keyIntPacking) == 0 {
		return nil, false
	}

	prefixValues := []any{}
	var rangeStatement *ColumnStatement
	handledColumns := map[string]bool{}

	for _, packedCol := range scyllaTable.keyIntPacking {
		colName := packedCol.GetName()
		if colName == "autoincrement_placeholder" {
			break
		}

		found := false
		for statementIndex := range statements {
			statement := statements[statementIndex]
			if statement.Col != colName {
				continue
			}
			if statement.Operator == "=" {
				prefixValues = append(prefixValues, statement.Value)
				handledColumns[statement.Col] = true
				found = true
				break
			}
			if slices.Contains(rangeOperators, statement.Operator) || statement.Operator == "BETWEEN" {
				rangeStatement = &statements[statementIndex]
				handledColumns[statement.Col] = true
				found = true
				break
			}
		}

		if !found || rangeStatement != nil {
			break
		}
	}

	if len(prefixValues) == 0 && rangeStatement == nil {
		return nil, false
	}

	makePackedRange := func(values []any, rangeStatement *ColumnStatement) (int64, int64, bool) {
		// Keep the exact packing math in one helper so compile and bind follow the same physical-key semantics.
		remainingDigits := int64(19)
		var packedValue int64

		for columnIndex, column := range scyllaTable.keyIntPacking {
			columnInfo := column.(*columnInfo)
			DecimalDigits := int64(columnInfo.DecimalDigits)
			if columnIndex == len(scyllaTable.keyIntPacking)-1 && DecimalDigits == 0 {
				DecimalDigits = remainingDigits
			}
			remainingDigits -= DecimalDigits

			if columnIndex < len(values) {
				packedValue += convertToInt64(values[columnIndex]) * Pow10Int64(remainingDigits)
				continue
			}

			if rangeStatement != nil && column.GetName() == rangeStatement.Col {
				if rangeStatement.Operator == "BETWEEN" {
					fromValue := packedValue + convertToInt64(rangeStatement.From[0].Value)*Pow10Int64(remainingDigits)
					toValue := packedValue + (convertToInt64(rangeStatement.To[0].Value)+1)*Pow10Int64(remainingDigits)
					return fromValue, toValue, false
				}

				rangeValue := convertToInt64(rangeStatement.Value)
				fromValue := packedValue + rangeValue*Pow10Int64(remainingDigits)
				return fromValue, fromValue + Pow10Int64(remainingDigits), false
			}

			fromValue := packedValue
			toValue := packedValue + Pow10Int64(remainingDigits+DecimalDigits)
			isEquality := columnIndex == len(scyllaTable.keyIntPacking)
			return fromValue, toValue, isEquality
		}

		return packedValue, packedValue, true
	}

	fromValue, toValue, isEquality := makePackedRange(prefixValues, rangeStatement)

	rewrittenStatement := ColumnStatement{
		Col:      keyCol.GetName(),
		Operator: "=",
		Value:    fromValue,
	}
	if !isEquality {
		rewrittenStatement = ColumnStatement{
			Col:      keyCol.GetName(),
			Operator: "BETWEEN",
			From:     []ColumnStatement{{Col: keyCol.GetName(), Value: fromValue}},
			To:       []ColumnStatement{{Col: keyCol.GetName(), Value: toValue}},
		}
	}

	rewrittenStatements := []ColumnStatement{rewrittenStatement}
	for _, statement := range statements {
		if !handledColumns[statement.Col] {
			rewrittenStatements = append(rewrittenStatements, statement)
		}
	}

	return rewrittenStatements, true
}

// maxFixedValueFanoutPerColumn is the widest declared value set the planner will enumerate to fill
// a gap in an index key prefix.
const maxFixedValueFanoutPerColumn = 8

// maxFixedValueFanoutQueries caps the product of the enumerated sets, which is how many parallel
// queries the fan-out costs. It matches the concurrency limit executeBoundSelectQueries runs under,
// so a fanned-out read never queues against itself.
const maxFixedValueFanoutQueries = 8

// narrowInt64ToColumnWidth returns value as the column's own Go integer type. The synthesized
// values come from the schema as int64, but they end up bound straight into CQL and, on hash views,
// fed to HashInt — which writes each width differently, so an int64 1 would not match the int8 1
// the row was written with.
func narrowInt64ToColumnWidth(value int64, column IColInfo) any {
	switch column.GetType().FieldType {
	case "int8":
		return int8(value)
	case "int16":
		return int16(value)
	case "int32":
		return int32(value)
	case "int":
		return int(value)
	}
	return value
}

// buildFixedValueFanoutStatements synthesizes an IN predicate for every unconstrained column whose
// FixedValues enumerate a small, closed set.
//
// An index whose key prefix has a gap at such a column is still reachable: enumerating the column
// is logically identical to leaving it open, and each resulting value group is one contiguous range
// over the key. Without this the planner drops to a base-table read that Scylla rejects unless the
// caller allows filtering — a query pinning only the trailing half of a delta view's keys being the
// motivating case.
func buildFixedValueFanoutStatements(statements []ColumnStatement, scyllaTable ScyllaTable) []ColumnStatement {
	if len(scyllaTable.fixedValueRanges) == 0 {
		return nil
	}

	constrainedColumns := map[string]bool{}
	for _, statement := range statements {
		constrainedColumns[statement.Col] = true
		for _, betweenStatement := range statement.From {
			constrainedColumns[betweenStatement.Col] = true
		}
	}

	fanoutStatements := []ColumnStatement{}
	plannedQueryCount := 1
	// Walking the table's columns rather than the fixedValueRanges map keeps the synthesized order
	// stable, which the statement indexes cached on SelectStatement depend on.
	for _, column := range scyllaTable.Columns {
		columnName := column.GetName()
		if constrainedColumns[columnName] {
			continue
		}
		valueRange, isDeclared := scyllaTable.fixedValueRanges[columnName]
		if !isDeclared || len(valueRange.declaredValues) == 0 ||
			len(valueRange.declaredValues) > maxFixedValueFanoutPerColumn ||
			plannedQueryCount*len(valueRange.declaredValues) > maxFixedValueFanoutQueries {
			continue
		}
		plannedQueryCount *= len(valueRange.declaredValues)

		fanoutValues := make([]any, 0, len(valueRange.declaredValues))
		for _, declaredValue := range valueRange.declaredValues {
			fanoutValues = append(fanoutValues, narrowInt64ToColumnWidth(declaredValue, column))
		}
		fanoutStatements = append(fanoutStatements, ColumnStatement{
			Col: columnName, Operator: "IN", Values: fanoutValues,
		})
	}

	return fanoutStatements
}

// countSignatureCoveredQueryColumns counts how many of the query's own predicate columns a
// capability constrains. Columns the fan-out synthesized are deliberately not counted: a signature
// that only grew by them constrains nothing the caller asked for, so it buys no selectivity while
// still costing one query per enumerated value.
func countSignatureCoveredQueryColumns(capability *QueryCapability, statements []ColumnStatement) int {
	if capability == nil {
		return 0
	}

	signatureParts := strings.Split(capability.Signature, "|")
	coveredColumns := make(map[string]bool, len(signatureParts)/2)
	for partIndex := 0; partIndex < len(signatureParts); partIndex += 2 {
		coveredColumns[signatureParts[partIndex]] = true
	}

	coveredCount := 0
	for _, statement := range statements {
		if coveredColumns[statement.Col] {
			coveredCount++
		}
	}
	return coveredCount
}

// applyFixedValueFanout retries capability matching with the synthesized predicates and keeps them
// only when they let an index bind strictly more of the query's own predicates. It returns the
// capability to compile against and the predicates that must be appended to the statement list.
func applyFixedValueFanout(statements []ColumnStatement, baseCapability *QueryCapability,
	scyllaTable ScyllaTable) (*QueryCapability, []ColumnStatement) {

	candidateStatements := buildFixedValueFanoutStatements(statements, scyllaTable)
	if len(candidateStatements) == 0 {
		return baseCapability, nil
	}

	fannedCapability := MatchQueryCapability(append(slices.Clone(statements), candidateStatements...),
		scyllaTable.capabilities)
	// A base-key capability is excluded outright: the key rewrite paths read equality and range
	// predicates only, so a synthesized IN would survive as a plain filter.
	if fannedCapability == nil || fannedCapability.Source == nil {
		return baseCapability, nil
	}
	// Coverage, not capability priority, is what decides this. Priority ranks a longer packed-key
	// prefix above a shorter one, but both are one contiguous range over the same column — reaching
	// the longer one by enumerating a gap costs several queries and returns the very same rows. The
	// fan-out only pays for itself when it rescues a predicate that would otherwise be left as a
	// filter Scylla refuses to run.
	if countSignatureCoveredQueryColumns(fannedCapability, statements) <=
		countSignatureCoveredQueryColumns(baseCapability, statements) {
		return baseCapability, nil
	}

	// A synthesized predicate the chosen source cannot bind would land in remainingStatements as
	// the very filter this is meant to avoid, so only the bindable ones are kept.
	fanoutStatements := []ColumnStatement{}
	fanoutColumnNames := []string{}
	for _, candidateStatement := range candidateStatements {
		if slices.Contains(fannedCapability.Source.columns, candidateStatement.Col) {
			fanoutStatements = append(fanoutStatements, candidateStatement)
			fanoutColumnNames = append(fanoutColumnNames, candidateStatement.Col)
		}
	}
	if len(fanoutStatements) == 0 {
		return baseCapability, nil
	}

	fanoutQueryCount := 1
	for _, fanoutStatement := range fanoutStatements {
		fanoutQueryCount *= len(fanoutStatement.Values)
	}
	baseSignature := "none"
	if baseCapability != nil {
		baseSignature = baseCapability.Signature
	}
	fmt.Printf("FixedValues fanout applied: table=%s columns=%v queries=%d signature=%s (was %s, covering %d of %d predicates)\n",
		scyllaTable.Name, fanoutColumnNames, fanoutQueryCount, fannedCapability.Signature, baseSignature,
		countSignatureCoveredQueryColumns(baseCapability, statements), len(statements))

	return fannedCapability, fanoutStatements
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

		fmt.Printf("Select plan compiled: table=%s hash=%d route=%d source=%s post_filter=false dedup=false\n",
			scyllaTable.Name, compiledStatement.hash, compiledStatement.route, sourceTableName)
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
		fmt.Printf("Select plan compiled: table=%s hash=%d route=%d source=%s post_filter=true dedup=true\n",
			scyllaTable.Name, compiledStatement.hash, compiledStatement.route, viewTableName)
		return compiledStatement, nil
	}

	bestCapability := MatchQueryCapability(statements, scyllaTable.capabilities)
	bestCapability, fanoutStatements := applyFixedValueFanout(statements, bestCapability, scyllaTable)
	if len(fanoutStatements) > 0 {
		// The fan-out predicates take part in planning from here on, so they must sit in the
		// statement list the cached indexes are computed against.
		statements = append(slices.Clone(statements), fanoutStatements...)
		compiledStatement.fixedValueFanoutStatements = fanoutStatements
	}

	if bestCapability != nil {
		if bestCapability.Source != nil {
			selectedView := bestCapability.Source
			// Index group views (Type 3) query the main table via a virtual hash column;
			// projected views (Type >= 6) redirect to a separate view table.
			isIndexGroupView := selectedView.Type == 3 && selectedView.getStatementPrepared != nil
			isProjectedView := selectedView.Type >= 6 && canUseProjectedView(columnNames, selectedView)

			if isProjectedView {
				viewTableName = selectedView.name
				if selectedView.Type == 9 {
					compiledStatement.requiresDeduplication = true
				}
				// The view's first clustering column drives ORDER BY in the view table, not the base ID.
				if selectedView.column != nil && !selectedView.column.IsNil() {
					compiledStatement.orderColumnName = selectedView.column.GetName()
				}
			}

			if (isProjectedView && selectedView.getStatementPrepared != nil) || isIndexGroupView {
				selectedStatementIndexes := []int{}
				remainingStatementIndexes := []int{}

				for statementIndex, statement := range statements {
					if slices.Contains(selectedView.columns, statement.Col) {
						selectedStatementIndexes = append(selectedStatementIndexes, statementIndex)
						continue
					}
					if len(statement.From) > 0 {
						isIncluded := true
						for _, betweenStatement := range statement.From {
							if !slices.Contains(selectedView.columns, betweenStatement.Col) {
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
			if canRewriteKeyConcatenated(statements, scyllaTable) {
				compiledStatement.route = selectRouteKeyConcatenated
			} else if canRewriteKeyIntPacking(statements, scyllaTable) {
				compiledStatement.route = selectRouteKeyIntPacking
			}
		}
	}

	compiledStatement.queryTemplate = makeSelectQueryTemplate(selectExpressions, scyllaTable.Namespace, viewTableName)

	fmt.Printf("Select plan compiled: table=%s hash=%d route=%d source=%s post_filter=%v dedup=%v\n",
		scyllaTable.Name, compiledStatement.hash, compiledStatement.route, viewTableName,
		len(compiledStatement.postFilterStatementIndexes) > 0, compiledStatement.requiresDeduplication)

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
		whereStatements = e.sourceView.getStatementPrepared(selectedStatements...)
		if len(whereStatements) == 0 {
			// A view that cannot bind the predicates would otherwise yield zero bound statements,
			// which reads as an empty result set instead of a planning failure.
			return nil, fmt.Errorf(`view "%v" cannot serve this query's predicates`, e.sourceView.name)
		}
	case selectRouteKeyConcatenated:
		rewrittenStatements, canRewrite := buildKeyConcatenatedStatements(statements, scyllaTable)
		if !canRewrite {
			return nil, fmt.Errorf("key concatenated select shape no longer matches the cached route")
		}
		remainingStatements = rewrittenStatements
	case selectRouteKeyIntPacking:
		rewrittenStatements, canRewrite := buildKeyIntPackingStatements(statements, scyllaTable)
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

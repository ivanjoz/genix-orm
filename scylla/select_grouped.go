package scylla

import (
	"fmt"
	"github.com/ivanjoz/genix-orm/db"
	"slices"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sync/errgroup"
)

type queryIndexGroupHash struct {
	hashValue        int32
	indexGroupValues []int64
}

type indexGroupSelectPlan struct {
	partitionValue int32
	indexGroup     db.IndexGroupInfo
	hashGroups     []queryIndexGroupHash
}

type indexGroupFetchState struct {
	hashValue        int32
	indexGroupValues []int64
	// lastWrite is the label the group goes out with: its stored last_write, or 0 while unsettled.
	lastWrite int32
}

// loadIndexGroupFreshnessRows is overridable in tests so grouped planning can be verified without Scylla.
var loadIndexGroupFreshnessRows = loadIndexGroupFreshnessRowsFromScylla

// execIndexGroupQuery is the typed doorway of a grouped read: the read runs on records by pointer
// (RecordGroup[unsafe.Pointer]) and only the final copy into the caller's groups names E.
func execIndexGroupQuery[T TableSchemaInterface[T], E any](schemaStruct *T, tableInfo *TableInfo) error {
	refGroups := tableInfo.RefSlice.(*[]RecordGroup[E])
	*refGroups = (*refGroups)[:0]

	erasedGroups, err := queryIndexGroups(getOrCompileScyllaTable(schemaStruct), tableInfo, makeRecordSink(&[]E{}))
	for _, erasedGroup := range erasedGroups {
		records := make([]E, len(erasedGroup.Records))
		for recordIndex, recordPointer := range erasedGroup.Records {
			records[recordIndex] = *(*E)(recordPointer)
		}
		*refGroups = append(*refGroups, RecordGroup[E]{
			IndexID:          erasedGroup.IndexID,
			GroupHash:        erasedGroup.GroupHash,
			IndexGroupValues: erasedGroup.IndexGroupValues,
			Records:          records,
			LastWrite:        erasedGroup.LastWrite,
		})
	}
	return err
}

// queryIndexGroups returns the groups of a grouped read in hash order. sink only allocates the
// records: they come back grouped, not appended.
func queryIndexGroups(scyllaTable ScyllaTable, tableInfo *TableInfo, sink recordSink) ([]RecordGroup[unsafe.Pointer], error) {
	if len(tableInfo.ColumnsInclude) > 0 || len(tableInfo.ColumnsExclude) > 0 {
		return nil, fmt.Errorf("QueryIndexGroup does not support Select() or Exclude() yet")
	}
	if len(tableInfo.GroupByColumns) > 0 {
		return nil, fmt.Errorf("QueryIndexGroup does not support GroupBy()")
	}
	if tableInfo.OrderBy != "" {
		return nil, fmt.Errorf("QueryIndexGroup does not support OrderDesc() yet")
	}
	if tableInfo.Limit > 0 {
		return nil, fmt.Errorf("QueryIndexGroup does not support Limit() yet")
	}
	if tableInfo.AllowFilter {
		return nil, fmt.Errorf("QueryIndexGroup does not support AllowFilter()")
	}

	if len(scyllaTable.Namespace) == 0 {
		scyllaTable.Namespace = connParams.Keyspace
	}
	if len(scyllaTable.Namespace) == 0 {
		return nil, fmt.Errorf("no se ha especificado un keyspace")
	}

	queryPlan, err := buildIndexGroupSelectPlan(tableInfo, scyllaTable)
	if err != nil {
		return nil, err
	}

	hashValues := extractQueryIndexGroupHashes(queryPlan.hashGroups)
	serverFreshnessByHash, err := loadPlannedGroupFreshness(scyllaTable, queryPlan, hashValues)
	if err != nil {
		return nil, err
	}

	readTime := getManagedUnixTime()
	fetchStates, cachedStates := splitIndexGroupFetches(queryPlan.hashGroups, serverFreshnessByHash, tableInfo.CachedIndexGroups, readTime)
	if len(fetchStates) == 0 && len(cachedStates) == 0 {
		if ShouldLog() {
			fmt.Printf("QueryIndexGroup skipped all groups: table=%s partition=%d hashes=%d\n",
				scyllaTable.Name, queryPlan.partitionValue, len(hashValues))
		}
		return nil, nil
	}

	fetchedGroups, err := fetchIndexGroupRecordGroups(sink, scyllaTable, queryPlan, fetchStates, tableInfo.SelectStatements(), time.Now(), readTime)
	if err != nil {
		return nil, err
	}

	groupsByHash := map[int32]RecordGroup[unsafe.Pointer]{}
	for _, cachedState := range cachedStates {
		// Metadata-only groups let the frontend read unchanged records from IndexedDB by igVal key.
		groupsByHash[cachedState.hashValue] = RecordGroup[unsafe.Pointer]{
			IndexID:          queryPlan.indexGroup.IndexID,
			GroupHash:        cachedState.hashValue,
			IndexGroupValues: append([]int64(nil), cachedState.indexGroupValues...),
			LastWrite:        cachedState.lastWrite,
		}
	}
	for _, fetchedGroup := range fetchedGroups {
		groupsByHash[fetchedGroup.GroupHash] = fetchedGroup
	}
	groups := []RecordGroup[unsafe.Pointer]{}
	for _, hashGroup := range queryPlan.hashGroups {
		groupRecord, exists := groupsByHash[hashGroup.hashValue]
		if exists {
			groups = append(groups, groupRecord)
		}
	}
	return groups, nil
}

func buildIndexGroupSelectPlan(tableInfo *TableInfo, scyllaTable ScyllaTable) (*indexGroupSelectPlan, error) {
	statements := tableInfo.SelectStatements()
	partitionColumn := scyllaTable.GetPartKey()
	if partitionColumn == nil || partitionColumn.IsNil() {
		return nil, fmt.Errorf(`QueryIndexGroup requires a partition column on table "%v"`, scyllaTable.Name)
	}

	statementByColumn := map[string][]ColumnStatement{}
	betweenStatementsCount := 0
	for _, statement := range statements {
		statementByColumn[statement.Col] = append(statementByColumn[statement.Col], statement)
		if statement.Operator == "BETWEEN" {
			betweenStatementsCount++
		}
	}

	partitionStatements := statementByColumn[partitionColumn.GetName()]
	if len(partitionStatements) == 0 || partitionStatements[0].Operator != "=" {
		return nil, fmt.Errorf(`QueryIndexGroup requires partition equality on "%v"`, partitionColumn.GetName())
	}
	if betweenStatementsCount != 1 {
		return nil, fmt.Errorf("QueryIndexGroup requires exactly one BETWEEN statement")
	}

	var bestIndexGroup *db.IndexGroupInfo
	bestScore := -1
	for indexGroupIndex := range scyllaTable.IndexGroups {
		indexGroup := &scyllaTable.IndexGroups[indexGroupIndex]
		score, isValid := scoreIndexGroupCandidate(*indexGroup, statementByColumn)
		if !isValid || score <= bestScore {
			continue
		}
		bestScore = score
		bestIndexGroup = indexGroup
	}

	if bestIndexGroup == nil {
		return nil, fmt.Errorf(`QueryIndexGroup did not find a compatible TypeCacheGroup for table "%v"`, scyllaTable.Name)
	}

	hashGroups, err := buildQueryIndexGroupHashes(*bestIndexGroup, statements)
	if err != nil {
		return nil, err
	}
	if len(hashGroups) == 0 {
		return nil, fmt.Errorf("QueryIndexGroup did not produce candidate hashes")
	}

	if ShouldLog() {
		fmt.Printf("QueryIndexGroup plan selected: table=%s group=%s candidate_hashes=%d\n",
			scyllaTable.Name, bestIndexGroup.Name, len(hashGroups))
	}

	return &indexGroupSelectPlan{
		partitionValue: convertToInt32(partitionStatements[0].Value),
		indexGroup:     *bestIndexGroup,
		hashGroups:     hashGroups,
	}, nil
}

func scoreIndexGroupCandidate(indexGroup db.IndexGroupInfo, statementByColumn map[string][]ColumnStatement) (int, bool) {
	betweenColumns := 0
	fanoutSize := 1

	for _, sourceColumn := range indexGroup.SourceColumns {
		columnStatements := statementByColumn[sourceColumn.Column.GetName()]
		if len(columnStatements) != 1 {
			return 0, false
		}

		statement := columnStatements[0]
		switch statement.Operator {
		case "=":
			fanoutSize *= 1
		case "CONTAINS":
			fanoutSize *= 1
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
			fromValue := db.ConvertToInt64(statement.From[0].Value)
			toValue := db.ConvertToInt64(statement.To[0].Value)
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

	// Prefer the most specific group and break ties by lower hash fanout.
	return len(indexGroup.SourceColumns)*100000 - fanoutSize, true
}

func buildQueryIndexGroupHashes(indexGroup db.IndexGroupInfo, statements []ColumnStatement) ([]queryIndexGroupHash, error) {
	valuesGroups := [][]int64{{}}

	for _, sourceColumn := range indexGroup.SourceColumns {
		statement, err := db.FindIndexGroupStatement(sourceColumn.Column.GetName(), statements)
		if err != nil {
			return nil, err
		}

		columnValues, err := db.ResolveIndexGroupQueryValues(statement)
		if err != nil {
			return nil, err
		}
		if len(columnValues) == 0 {
			return nil, nil
		}

		nextGroups := make([][]int64, 0, len(valuesGroups)*len(columnValues))
		for _, valuesGroup := range valuesGroups {
			for _, value := range columnValues {
				nextGroups = append(nextGroups, append(append([]int64{}, valuesGroup...), value))
			}
		}
		valuesGroups = nextGroups
	}

	hashGroupsByValue := map[int32]queryIndexGroupHash{}
	for _, valuesGroup := range valuesGroups {
		hashValue := db.HashInt64(valuesGroup...)
		if _, exists := hashGroupsByValue[hashValue]; exists {
			continue
		}
		hashGroupsByValue[hashValue] = queryIndexGroupHash{
			hashValue:        hashValue,
			indexGroupValues: append([]int64(nil), valuesGroup...),
		}
	}

	hashValues := make([]int32, 0, len(hashGroupsByValue))
	for hashValue := range hashGroupsByValue {
		hashValues = append(hashValues, hashValue)
	}
	slices.Sort(hashValues)

	hashGroups := make([]queryIndexGroupHash, 0, len(hashValues))
	for _, hashValue := range hashValues {
		hashGroups = append(hashGroups, hashGroupsByValue[hashValue])
	}
	return hashGroups, nil
}

func extractQueryIndexGroupHashes(hashGroups []queryIndexGroupHash) []int32 {
	hashValues := make([]int32, 0, len(hashGroups))
	for _, hashGroup := range hashGroups {
		hashValues = append(hashValues, hashGroup.hashValue)
	}
	return hashValues
}

// loadPlannedGroupFreshness is the last_write of each planned group, by its hash. A no-index group
// has no marks of its own: each group takes the mark of the base group it is a slice of, so a base
// group absent on the server (no records that day) leaves the group absent too.
func loadPlannedGroupFreshness(scyllaTable ScyllaTable, queryPlan *indexGroupSelectPlan, hashValues []int32) (map[int32]int32, error) {
	indexGroup := queryPlan.indexGroup
	if indexGroup.BaseGroup == nil {
		return loadIndexGroupFreshnessRows(scyllaTable, indexGroup, queryPlan.partitionValue, hashValues)
	}
	baseHashByGroupHash := make(map[int32]int32, len(queryPlan.hashGroups))
	baseHashes := []int32{}
	for _, hashGroup := range queryPlan.hashGroups {
		baseHash := db.HashInt64(indexGroup.BaseGroupValues(hashGroup.indexGroupValues)...)
		baseHashByGroupHash[hashGroup.hashValue] = baseHash
		if !slices.Contains(baseHashes, baseHash) {
			baseHashes = append(baseHashes, baseHash)
		}
	}
	baseFreshnessByHash, err := loadIndexGroupFreshnessRows(scyllaTable, *indexGroup.BaseGroup, queryPlan.partitionValue, baseHashes)
	if err != nil {
		return nil, err
	}
	freshnessByHash := make(map[int32]int32, len(baseHashByGroupHash))
	for groupHash, baseHash := range baseHashByGroupHash {
		if lastWrite, isOnServer := baseFreshnessByHash[baseHash]; isOnServer {
			freshnessByHash[groupHash] = lastWrite
		}
	}
	return freshnessByHash, nil
}

func loadIndexGroupFreshnessRowsFromScylla(
	scyllaTable ScyllaTable,
	indexGroup db.IndexGroupInfo,
	partitionValue int32,
	hashValues []int32,
) (map[int32]int32, error) {
	if len(hashValues) == 0 {
		return map[int32]int32{}, nil
	}
	if scyllaTable.indexUpdatedTable == nil {
		return nil, fmt.Errorf(`QueryIndexGroup requires "__index_updated" metadata for table "%v"`, scyllaTable.Name)
	}

	freshnessByHash := map[int32]int32{}
	maxHashesPerQuery := getMaxClusteringKeyRestrictionsPerQuery()
	if maxHashesPerQuery <= 0 {
		maxHashesPerQuery = 100
	}

	for startIndex := 0; startIndex < len(hashValues); startIndex += maxHashesPerQuery {
		endIndex := startIndex + maxHashesPerQuery
		if endIndex > len(hashValues) {
			endIndex = len(hashValues)
		}

		hashChunk := hashValues[startIndex:endIndex]
		placeholders := make([]string, 0, len(hashChunk))
		queryValues := make([]any, 0, len(hashChunk)+2)
		queryValues = append(queryValues, partitionValue)
		queryValues = append(queryValues, indexGroup.IndexID)
		for _, hashValue := range hashChunk {
			placeholders = append(placeholders, "?")
			queryValues = append(queryValues, hashValue)
		}

		queryStr := fmt.Sprintf(
			`SELECT index_hash, last_write FROM %v.%v WHERE partition_id = ? AND index_id = ? AND index_hash IN (%v)`,
			scyllaTable.Namespace,
			scyllaTable.indexUpdatedTable.name,
			strings.Join(placeholders, ", "),
		)

		if ShouldLog() {
			fmt.Printf("QueryIndexGroup freshness probe: table=%s partition=%d index_id=%d hashes=%d\n",
				scyllaTable.Name, partitionValue, indexGroup.IndexID, len(hashChunk))
		}

		iter := getScyllaConnection().Query(queryStr, queryValues...).Iter()
		var hashValue int32
		var lastWrite int32
		for iter.Scan(&hashValue, &lastWrite) {
			freshnessByHash[hashValue] = lastWrite
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
	}

	return freshnessByHash, nil
}

// splitIndexGroupFetches decides, per group the server holds, whether it is fetched or answered from
// the client's cache. A group written less than indexGroupSettleTicks ago is unsettled: a write of
// the same 2 s tick, or one still landing, may be missing from what a fetch reads now, so it is
// always fetched and labelled 0. A settled group whose last_write the client holds is cached.
func splitIndexGroupFetches(
	hashGroups []queryIndexGroupHash,
	serverFreshnessByHash map[int32]int32,
	cachedIndexGroups map[int32]int32,
	nowTime int32,
) ([]indexGroupFetchState, []indexGroupFetchState) {
	fetchStates := make([]indexGroupFetchState, 0, len(hashGroups))
	cachedStates := make([]indexGroupFetchState, 0, len(hashGroups))
	for _, hashGroup := range hashGroups {
		lastWrite, existsOnServer := serverFreshnessByHash[hashGroup.hashValue]
		if !existsOnServer {
			continue
		}
		groupState := indexGroupFetchState{
			hashValue:        hashGroup.hashValue,
			indexGroupValues: append([]int64(nil), hashGroup.indexGroupValues...),
			lastWrite:        lastWrite,
		}
		if nowTime-lastWrite < indexGroupSettleTicks {
			groupState.lastWrite = 0
			fetchStates = append(fetchStates, groupState)
			continue
		}
		if cachedLastWrite, existsInCache := cachedIndexGroups[hashGroup.hashValue]; existsInCache && cachedLastWrite == lastWrite {
			cachedStates = append(cachedStates, groupState)
			continue
		}
		fetchStates = append(fetchStates, groupState)
	}
	return fetchStates, cachedStates
}

// fetchIndexGroupRecordGroups reads the groups concurrently, through the group's S3 snapshot when
// its index keeps one (cache_group_snapshot.go). Each goroutine collects into its own pointer list,
// so the shared sink is only asked to allocate.
func fetchIndexGroupRecordGroups(
	sink recordSink,
	scyllaTable ScyllaTable,
	queryPlan *indexGroupSelectPlan,
	fetchStates []indexGroupFetchState,
	postFilterStatements []ColumnStatement,
	queryNoticeTime time.Time,
	readTime int32,
) ([]RecordGroup[unsafe.Pointer], error) {
	_, scanColumns, selectExpressions := buildSelectProjection(&TableInfo{}, scyllaTable)
	groupsByIndex := make([]RecordGroup[unsafe.Pointer], len(fetchStates))

	eg := errgroup.Group{}
	if queryPlan.indexGroup.SnapshotInS3 {
		eg.SetLimit(cacheGroupSnapshotConcurrency)
	}
	for fetchIndex, fetchState := range fetchStates {
		fetchIndex := fetchIndex
		fetchState := fetchState

		eg.Go(func() error {
			readGroupFromDatabase := func(groupPostFilter []ColumnStatement) ([]unsafe.Pointer, error) {
				queryStr, queryValues := buildIndexGroupFetchQuery(scyllaTable, queryPlan, fetchState, selectExpressions)
				var groupRecords []unsafe.Pointer
				err := scanSelectQueryRows(queryStr, queryValues, scanColumns, scyllaTable,
					sink.collectInto(&groupRecords), groupPostFilter, nil, queryNoticeTime)
				return groupRecords, err
			}
			records, err := readCacheGroupRecords(sink, scyllaTable, queryPlan, fetchState, readGroupFromDatabase,
				groupPostFilterStatements(queryPlan.indexGroup, fetchState, postFilterStatements), readTime)
			if err != nil {
				return err
			}

			groupsByIndex[fetchIndex] = RecordGroup[unsafe.Pointer]{
				IndexID:          queryPlan.indexGroup.IndexID,
				GroupHash:        fetchState.hashValue,
				IndexGroupValues: append([]int64(nil), fetchState.indexGroupValues...),
				Records:          records,
				LastWrite:        fetchState.lastWrite,
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return groupsByIndex, nil
}

// groupPostFilterStatements is the post-filter of one group's fetch. A no-index group reads its whole
// base range, so it also pins each of its Keys to the group's own value: the query's IN over a Key
// fans out into one group per value, and each group must keep only its value's records.
func groupPostFilterStatements(indexGroup db.IndexGroupInfo, fetchState indexGroupFetchState, postFilterStatements []ColumnStatement) []ColumnStatement {
	if indexGroup.BaseGroup == nil {
		return postFilterStatements
	}
	groupStatements := slices.Clone(postFilterStatements)
	for columnIndex, sourceColumn := range indexGroup.SourceColumns {
		groupStatements = append(groupStatements, ColumnStatement{
			Col: sourceColumn.Column.GetName(), Operator: "=", Value: fetchState.indexGroupValues[columnIndex],
		})
	}
	return groupStatements
}

func buildIndexGroupFetchQuery(
	scyllaTable ScyllaTable,
	queryPlan *indexGroupSelectPlan,
	fetchState indexGroupFetchState,
	selectExpressions []string,
) (string, []any) {
	whereStatements := []string{
		fmt.Sprintf("%v = ?", scyllaTable.GetPartKey().GetName()),
	}
	queryValues := []any{queryPlan.partitionValue}

	// A no-index group reads its base group's range; the post-filter drops its other Keys' misses.
	keyRangeGroup, keyRangeValues := queryPlan.indexGroup, fetchState.indexGroupValues
	if keyRangeGroup.BaseGroup != nil {
		keyRangeGroup, keyRangeValues = *keyRangeGroup.BaseGroup, keyRangeGroup.BaseGroupValues(keyRangeValues)
	}

	// TypeCacheGroupFromKey groups rewrite per-hash fetches to a packed primary-key range,
	// so no secondary index is needed and ALLOW_FILTERING is avoided.
	if keyRangeGroup.InheritFromKey {
		keyStatements := make([]ColumnStatement, 0, len(keyRangeGroup.SourceColumns))
		for columnIndex, sourceColumn := range keyRangeGroup.SourceColumns {
			keyStatements = append(keyStatements, ColumnStatement{
				Col:      sourceColumn.Column.GetName(),
				Operator: "=",
				Value:    keyRangeValues[columnIndex],
			})
		}
		rewrittenStatements, canRewrite := scyllaTable.BuildKeyIntPackingStatements(keyStatements)
		if !canRewrite || len(rewrittenStatements) == 0 {
			panic(fmt.Sprintf(`QueryIndexGroup: TypeCacheGroupFromKey fetch could not rewrite keys for table "%v"`, scyllaTable.Name))
		}
		keyPredicate := rewrittenStatements[0]
		keyColumnName := keyPredicate.Col
		if keyPredicate.Operator == "BETWEEN" {
			whereStatements = append(whereStatements, fmt.Sprintf("%v >= ? AND %v < ?", keyColumnName, keyColumnName))
			queryValues = append(queryValues, keyPredicate.From[0].Value, keyPredicate.To[0].Value)
		} else {
			whereStatements = append(whereStatements, fmt.Sprintf("%v = ?", keyColumnName))
			queryValues = append(queryValues, keyPredicate.Value)
		}
	} else if queryPlan.indexGroup.VirtualColumn == nil || queryPlan.indexGroup.VirtualColumn.IsNil() {
		// Single-column groups already have a real secondary index on the source column,
		// so grouped fetches can probe that column directly without a virtual hash column.
		sourceColumn := queryPlan.indexGroup.SourceColumns[0].Column
		whereStatements = append(whereStatements, fmt.Sprintf("%v = ?", sourceColumn.GetName()))
		queryValues = append(queryValues, fetchState.indexGroupValues[0])
	} else if queryPlan.indexGroup.UsesCollectionValues {
		whereStatements = append(whereStatements, fmt.Sprintf("%v CONTAINS ?", queryPlan.indexGroup.VirtualColumn.GetName()))
		queryValues = append(queryValues, fetchState.hashValue)
	} else {
		whereStatements = append(whereStatements, fmt.Sprintf("%v = ?", queryPlan.indexGroup.VirtualColumn.GetName()))
		queryValues = append(queryValues, fetchState.hashValue)
	}

	queryStr := fmt.Sprintf(
		"SELECT %v FROM %v.%v WHERE %v",
		strings.Join(selectExpressions, ", "),
		scyllaTable.Namespace,
		scyllaTable.Name,
		strings.Join(whereStatements, " AND "),
	)
	return queryStr, queryValues
}

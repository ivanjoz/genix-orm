package scylla

import (
	"fmt"
	"github.com/ivanjoz/genix-orm/db"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sync/errgroup"
)

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
	if len(scyllaTable.Namespace) == 0 {
		scyllaTable.Namespace = connParams.Keyspace
	}
	if len(scyllaTable.Namespace) == 0 {
		return nil, fmt.Errorf("no se ha especificado un keyspace")
	}

	queryPlan, err := scyllaTable.PlanIndexGroupRead(tableInfo)
	if err != nil {
		return nil, err
	}

	hashValues := queryPlan.HashValues()
	serverFreshnessByHash, err := loadPlannedGroupFreshness(scyllaTable, queryPlan, hashValues)
	if err != nil {
		return nil, err
	}

	readTime := getManagedUnixTime()
	fetchStates, cachedStates := db.SplitIndexGroupFetches(queryPlan.HashGroups, serverFreshnessByHash, tableInfo.CachedIndexGroups, readTime)
	if len(fetchStates) == 0 && len(cachedStates) == 0 {
		if ShouldLog() {
			fmt.Printf("QueryIndexGroup skipped all groups: table=%s partition=%d hashes=%d\n",
				scyllaTable.Name, queryPlan.PartitionValue, len(hashValues))
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
		groupsByHash[cachedState.HashValue] = RecordGroup[unsafe.Pointer]{
			IndexID:          queryPlan.IndexGroup.IndexID,
			GroupHash:        cachedState.HashValue,
			IndexGroupValues: append([]int64(nil), cachedState.IndexGroupValues...),
			LastWrite:        cachedState.LastWrite,
		}
	}
	for _, fetchedGroup := range fetchedGroups {
		groupsByHash[fetchedGroup.GroupHash] = fetchedGroup
	}
	groups := []RecordGroup[unsafe.Pointer]{}
	for _, hashGroup := range queryPlan.HashGroups {
		groupRecord, exists := groupsByHash[hashGroup.HashValue]
		if exists {
			groups = append(groups, groupRecord)
		}
	}
	return groups, nil
}

// loadPlannedGroupFreshness is the last_write of each planned group, by its hash. A no-index group
// has no marks of its own: each group takes the mark of the base group it is a slice of, so a base
// group absent on the server (no records that day) leaves the group absent too.
func loadPlannedGroupFreshness(scyllaTable ScyllaTable, queryPlan *db.IndexGroupReadPlan, hashValues []int32) (map[int32]int32, error) {
	indexGroup := queryPlan.IndexGroup
	if indexGroup.BaseGroup == nil {
		return loadIndexGroupFreshnessRows(scyllaTable, indexGroup, queryPlan.PartitionValue, hashValues)
	}
	baseHashByGroupHash, baseHashes := queryPlan.BaseGroupHashes()
	baseFreshnessByHash, err := loadIndexGroupFreshnessRows(scyllaTable, *indexGroup.BaseGroup, queryPlan.PartitionValue, baseHashes)
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

// fetchIndexGroupRecordGroups reads the groups concurrently, through the group's S3 snapshot when
// its index keeps one (cache_group_snapshot.go). Each goroutine collects into its own pointer list,
// so the shared sink is only asked to allocate.
func fetchIndexGroupRecordGroups(
	sink recordSink,
	scyllaTable ScyllaTable,
	queryPlan *db.IndexGroupReadPlan,
	fetchStates []db.IndexGroupFetchState,
	postFilterStatements []ColumnStatement,
	queryNoticeTime time.Time,
	readTime int32,
) ([]RecordGroup[unsafe.Pointer], error) {
	_, scanColumns, selectExpressions := buildSelectProjection(&TableInfo{}, scyllaTable)
	groupsByIndex := make([]RecordGroup[unsafe.Pointer], len(fetchStates))

	eg := errgroup.Group{}
	if queryPlan.IndexGroup.SnapshotInS3 {
		eg.SetLimit(cacheGroupSnapshotConcurrency)
	}
	for fetchIndex, fetchState := range fetchStates {
		eg.Go(func() error {
			readGroupFromDatabase := func(groupPostFilter []ColumnStatement) ([]unsafe.Pointer, error) {
				queryStr, queryValues := buildIndexGroupFetchQuery(scyllaTable, queryPlan, fetchState, selectExpressions)
				var groupRecords []unsafe.Pointer
				err := scanSelectQueryRows(queryStr, queryValues, scanColumns, scyllaTable,
					sink.collectInto(&groupRecords), groupPostFilter, nil, queryNoticeTime)
				return groupRecords, err
			}
			records, err := readCacheGroupRecords(sink, scyllaTable, queryPlan, fetchState, readGroupFromDatabase,
				db.GroupPostFilterStatements(queryPlan.IndexGroup, fetchState, postFilterStatements), readTime)
			if err != nil {
				return err
			}

			groupsByIndex[fetchIndex] = RecordGroup[unsafe.Pointer]{
				IndexID:          queryPlan.IndexGroup.IndexID,
				GroupHash:        fetchState.HashValue,
				IndexGroupValues: append([]int64(nil), fetchState.IndexGroupValues...),
				Records:          records,
				LastWrite:        fetchState.LastWrite,
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return groupsByIndex, nil
}

// buildIndexGroupFetchQuery renders one group's base-table read (db.IndexGroupFetchStep) as CQL.
func buildIndexGroupFetchQuery(
	scyllaTable ScyllaTable,
	queryPlan *db.IndexGroupReadPlan,
	fetchState db.IndexGroupFetchState,
	selectExpressions []string,
) (string, []any) {
	fetchStep := scyllaTable.IndexGroupFetchStep(queryPlan, fetchState)
	whereStatements, queryValues := []string{}, []any{}
	for _, whereClause := range buildRemainingWhereClauses(accessStepStatements(nil, fetchStep)) {
		whereStatements = append(whereStatements, whereClause.Clause)
		queryValues = append(queryValues, whereClause.Values...)
	}
	queryStr := fmt.Sprintf("SELECT %v FROM %v.%v WHERE %v",
		strings.Join(selectExpressions, ", "), scyllaTable.Namespace, scyllaTable.Name, strings.Join(whereStatements, " AND "))
	return queryStr, queryValues
}

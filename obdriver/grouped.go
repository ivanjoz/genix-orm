package obdriver

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
	"golang.org/x/sync/errgroup"
)

// Cache-group reads (QueryIndexGroup), as Scylla's driver runs them (scylla/select_grouped.go,
// scylla/cache_group_snapshot.go): the shared plan names the groups, __index_updated says which the
// server holds and when each was last written, the client's labels decide which are resent, and each
// resent group is one read of its index (or of its primary-key block), served from its file-store
// snapshot when the index is TypeCacheGroupS3 and the snapshot's label is the group's last_write.

// cacheGroupSnapshotConcurrency caps the parallel group reads of one request on an S3 index.
const cacheGroupSnapshotConcurrency = 16

// queryIndexGroups returns the groups of a grouped read in hash order. sink only allocates.
func (table *ObTable) queryIndexGroups(tableInfo *db.TableInfo, sink recordSink) ([]db.RecordGroup[unsafe.Pointer], error) {
	queryPlan, err := table.PlanIndexGroupRead(tableInfo)
	if err != nil {
		return nil, err
	}
	serverFreshnessByHash, err := table.loadPlannedGroupFreshness(queryPlan)
	if err != nil {
		return nil, err
	}
	readTime := db.ManagedUnixTime()
	fetchStates, cachedStates := db.SplitIndexGroupFetches(queryPlan.HashGroups, serverFreshnessByHash, tableInfo.CachedIndexGroups, readTime)
	if len(fetchStates) == 0 && len(cachedStates) == 0 {
		return nil, nil
	}
	fetchedGroups, err := table.fetchIndexGroups(sink, queryPlan, fetchStates, tableInfo.SelectStatements(), readTime)
	if err != nil {
		return nil, err
	}

	groupsByHash := map[int32]db.RecordGroup[unsafe.Pointer]{}
	for _, cachedState := range cachedStates {
		// Metadata-only groups let the client read its unchanged records from IndexedDB.
		groupsByHash[cachedState.HashValue] = db.RecordGroup[unsafe.Pointer]{
			IndexID: queryPlan.IndexGroup.IndexID, GroupHash: cachedState.HashValue,
			IndexGroupValues: slices.Clone(cachedState.IndexGroupValues), LastWrite: cachedState.LastWrite,
		}
	}
	for _, fetchedGroup := range fetchedGroups {
		groupsByHash[fetchedGroup.GroupHash] = fetchedGroup
	}
	groups := []db.RecordGroup[unsafe.Pointer]{}
	for _, hashGroup := range queryPlan.HashGroups {
		if group, exists := groupsByHash[hashGroup.HashValue]; exists {
			groups = append(groups, group)
		}
	}
	return groups, nil
}

// loadPlannedGroupFreshness is the last_write of each planned group, by its hash. A no-index group
// takes the mark of the base group it is a slice of.
func (table *ObTable) loadPlannedGroupFreshness(queryPlan *db.IndexGroupReadPlan) (map[int32]int32, error) {
	indexGroup := queryPlan.IndexGroup
	if indexGroup.BaseGroup == nil {
		return table.loadGroupFreshness(indexGroup.IndexID, queryPlan.PartitionValue, queryPlan.HashValues())
	}
	baseHashByGroupHash, baseHashes := queryPlan.BaseGroupHashes()
	baseFreshnessByHash, err := table.loadGroupFreshness(indexGroup.BaseGroup.IndexID, queryPlan.PartitionValue, baseHashes)
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

func (table *ObTable) loadGroupFreshness(indexID int16, partition int32, hashValues []int32) (map[int32]int32, error) {
	freshnessByHash := map[int32]int32{}
	if len(hashValues) == 0 {
		return freshnessByHash, nil
	}
	pool, err := sharedConnection()
	if err != nil {
		return nil, err
	}
	args := []any{partition, indexID}
	for _, hashValue := range hashValues {
		args = append(args, hashValue)
	}
	rows, err := pool.Query(fmt.Sprintf("SELECT `index_hash`, `last_write` FROM %s WHERE `partition_id` = ? AND `index_id` = ? AND `index_hash` IN (%s)",
		indexUpdatedTableSpec(table.Namespace, table.Name, 0).qualifiedName(), strings.TrimSuffix(strings.Repeat("?, ", len(hashValues)), ", ")), args...)
	if err != nil {
		return nil, fmt.Errorf("obdriver: reading the cache-group marks of %q: %w", table.Name, err)
	}
	defer rows.Close()
	for rows.Next() {
		var hashValue, lastWrite int32
		if err := rows.Scan(&hashValue, &lastWrite); err != nil {
			return nil, err
		}
		freshnessByHash[hashValue] = lastWrite
	}
	return freshnessByHash, rows.Err()
}

// fetchIndexGroups reads the groups concurrently, each into its own pointer list.
func (table *ObTable) fetchIndexGroups(
	sink recordSink, queryPlan *db.IndexGroupReadPlan, fetchStates []db.IndexGroupFetchState,
	postFilterStatements []db.ColumnStatement, readTime int32,
) ([]db.RecordGroup[unsafe.Pointer], error) {
	selectedColumns := table.projectedColumns(&db.TableInfo{})
	selectedNames := make([]string, len(selectedColumns))
	for position, stored := range selectedColumns {
		selectedNames[position] = stored.name
	}
	hint := ""
	if sourcePosition := slices.IndexFunc(table.IndexSources, func(source *db.IndexSource) bool {
		return source.IndexGroup != nil && source.IndexGroup.IndexID == queryPlan.IndexGroup.IndexID
	}); sourcePosition >= 0 {
		hint = fmt.Sprintf("/*+ INDEX(%s %s) */ ", table.Name, table.indexOfSource(table.IndexSources[sourcePosition]).name)
	}

	groups := make([]db.RecordGroup[unsafe.Pointer], len(fetchStates))
	group := errgroup.Group{}
	group.SetLimit(cacheGroupSnapshotConcurrency)
	for fetchIndex, fetchState := range fetchStates {
		group.Go(func() error {
			readGroupFromDatabase := func() ([]unsafe.Pointer, error) {
				conditions, args, err := table.renderConditions(accessStepStatements(table.IndexGroupFetchStep(queryPlan, fetchState)))
				if err != nil {
					return nil, err
				}
				var groupRecords []unsafe.Pointer
				statement := fmt.Sprintf("SELECT %s%s FROM %s WHERE %s", hint, quotedNames(selectedNames), table.qualifiedName(), strings.Join(conditions, " AND "))
				err = table.queryInto(statement, args, selectedColumns, sink.collectInto(&groupRecords))
				return groupRecords, err
			}
			records, err := table.readCacheGroupRecords(sink, queryPlan, fetchState, readGroupFromDatabase, readTime)
			if err != nil {
				return err
			}
			groupPostFilter := db.GroupPostFilterStatements(queryPlan.IndexGroup, fetchState, postFilterStatements)
			groups[fetchIndex] = db.RecordGroup[unsafe.Pointer]{
				IndexID: queryPlan.IndexGroup.IndexID, GroupHash: fetchState.HashValue,
				IndexGroupValues: slices.Clone(fetchState.IndexGroupValues), LastWrite: fetchState.LastWrite,
				Records: slices.DeleteFunc(records, func(recordPointer unsafe.Pointer) bool {
					return !table.recordMatchesStatements(recordPointer, groupPostFilter)
				}),
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return groups, nil
}

// readCacheGroupRecords reads one whole group. A settled group of a TypeCacheGroupS3 index is served
// from its file when the file's label is the group's last_write; otherwise it is read from the
// database and, when settled, the file rewritten. lastWrite 0 is an unsettled group: never served
// from a file nor stored.
func (table *ObTable) readCacheGroupRecords(
	sink recordSink, queryPlan *db.IndexGroupReadPlan, fetchState db.IndexGroupFetchState,
	readGroupFromDatabase func() ([]unsafe.Pointer, error), readTime int32,
) ([]unsafe.Pointer, error) {
	fileStore := cache.FileStore()
	if !queryPlan.IndexGroup.SnapshotInS3 || fetchState.LastWrite == 0 || fileStore == nil || !cache.RecordSnapshotsEnabled() {
		return readGroupFromDatabase()
	}
	codec := table.recordSnapshotCodecFor(sink.recordType)
	if !codec.isEligible {
		return readGroupFromDatabase()
	}
	snapshotKey := cache.CacheGroupSnapshotKey(table.Name, int64(queryPlan.PartitionValue), queryPlan.IndexGroup.IndexID, fetchState.HashValue)
	content, etag, err := fileStore.GetFile(snapshotKey)
	switch {
	case errors.Is(err, utils.ErrFileNotFound):
		etag = ""
	case err != nil:
		fmt.Println("Cache group snapshot: reading", snapshotKey, "failed, reading the database:", err)
		return readGroupFromDatabase()
	default:
		snapshot, snapshotRecords, isUsable := decodeRecordSnapshotFile(content, codec, sink.recordType, readTime, snapshotKey)
		if isUsable && snapshot.Cursor == fetchState.LastWrite {
			return snapshotRecords, nil
		}
	}
	groupRecords, err := readGroupFromDatabase()
	if err != nil {
		return nil, err
	}
	if len(groupRecords) > 0 {
		writeRecordSnapshotFile(fileStore, snapshotKey, etag, codec, sink.recordType, fetchState.LastWrite, readTime, groupRecords)
	}
	return groupRecords, nil
}

// recordMatchesStatements checks predicates on a decoded record in memory, with Scylla's post-filter
// semantics (scylla/select.go recordMatchesPostFilter): integer comparisons, CONTAINS over a slice. A
// computed column reads its value from the record's method.
func (table *ObTable) recordMatchesStatements(recordPointer unsafe.Pointer, statements []db.ColumnStatement) bool {
	for _, statement := range statements {
		column := table.ColumnsMap[statement.Col]
		if column == nil {
			return false
		}
		value := db.ConvertToInt64(column.GetRawValue(recordPointer))
		switch statement.Operator {
		case "=":
			if value != db.ConvertToInt64(statement.Value) {
				return false
			}
		case ">":
			if value <= db.ConvertToInt64(statement.Value) {
				return false
			}
		case ">=":
			if value < db.ConvertToInt64(statement.Value) {
				return false
			}
		case "<":
			if value >= db.ConvertToInt64(statement.Value) {
				return false
			}
		case "<=":
			if value > db.ConvertToInt64(statement.Value) {
				return false
			}
		case "IN":
			if !slices.ContainsFunc(statement.Values, func(candidate any) bool { return db.ConvertToInt64(candidate) == value }) {
				return false
			}
		case "CONTAINS":
			if !slices.Contains(db.FlattenInt64Values(column.GetRawValue(recordPointer)), db.ConvertToInt64(statement.Value)) {
				return false
			}
		case "BETWEEN":
			if len(statement.From) == 0 || len(statement.To) == 0 ||
				value < db.ConvertToInt64(statement.From[0].Value) || value > db.ConvertToInt64(statement.To[0].Value) {
				return false
			}
		}
	}
	return true
}

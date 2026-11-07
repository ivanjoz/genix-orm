package mariadbdriver

import (
	"database/sql"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"golang.org/x/sync/errgroup"
)

// Cache-group reads (QueryIndexGroup), as Scylla's driver runs them (scylla/select_grouped.go): the
// shared plan names the groups, __index_updated says which the server holds and when each was last
// written (keyed by the group's packed value, collision-free), the client's labels decide which are
// resent, and each resent group is one read of its index (or of its primary-key block).
//
// A settled group is served from its __group_data snapshot when the snapshot's built_write is the
// group's last_write: a write after the snapshot moves last_write (it is stamped at least the settle
// window later), so a stale snapshot is never served, and nothing on the write path touches it.

// groupReadConcurrency caps the parallel group reads of one request.
const groupReadConcurrency = 16

// queryIndexGroups returns the groups of a grouped read in hash order. sink only allocates.
func (table *MariaTable) queryIndexGroups(tableInfo *db.TableInfo, sink recordSink) ([]db.RecordGroup[unsafe.Pointer], error) {
	queryPlan, err := table.PlanIndexGroupRead(tableInfo)
	if err != nil {
		return nil, err
	}
	serverFreshnessByHash, err := table.loadPlannedGroupFreshness(queryPlan)
	if err != nil {
		return nil, err
	}
	readTime := db.ManagedUnixTime()
	fetchStates, cachedStates := db.SplitIndexGroupFetchesSettledAfter(queryPlan.HashGroups, serverFreshnessByHash, tableInfo.CachedIndexGroups,
		readTime, db.SQLIndexGroupSettleTicks)
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
func (table *MariaTable) loadPlannedGroupFreshness(queryPlan *db.IndexGroupReadPlan) (map[int32]int32, error) {
	markGroup := queryPlan.IndexGroup
	if markGroup.BaseGroup != nil {
		markGroup = *markGroup.BaseGroup
	}
	hashesByMarkValue := map[uint64][]int32{}
	for _, hashGroup := range queryPlan.HashGroups {
		markValues := hashGroup.IndexGroupValues
		if queryPlan.IndexGroup.BaseGroup != nil {
			markValues = queryPlan.IndexGroup.BaseGroupValues(markValues)
		}
		if markValue, isPackable := markGroup.LookupPackedValue(markValues); isPackable {
			hashesByMarkValue[markValue] = append(hashesByMarkValue[markValue], hashGroup.HashValue)
		}
	}
	freshnessByHash := map[int32]int32{}
	if len(hashesByMarkValue) == 0 {
		return freshnessByHash, nil
	}
	pool, err := sharedConnection()
	if err != nil {
		return nil, err
	}
	args := []any{queryPlan.PartitionValue, markGroup.IndexID}
	for markValue := range hashesByMarkValue {
		args = append(args, markValue)
	}
	rows, err := pool.Query(fmt.Sprintf("SELECT `index_value`, `last_write` FROM %s WHERE `partition_id` = ? AND `index_id` = ? AND `index_value` IN (%s)",
		indexUpdatedTableSpec(table.Namespace, table.Name).qualifiedName(), strings.TrimSuffix(strings.Repeat("?, ", len(hashesByMarkValue)), ", ")), args...)
	if err != nil {
		return nil, fmt.Errorf("mariadbdriver: reading the cache-group marks of %q: %w", table.Name, err)
	}
	defer rows.Close()
	for rows.Next() {
		var markValue uint64
		var lastWrite int32
		if err := rows.Scan(&markValue, &lastWrite); err != nil {
			return nil, err
		}
		for _, hashValue := range hashesByMarkValue[markValue] {
			freshnessByHash[hashValue] = lastWrite
		}
	}
	return freshnessByHash, rows.Err()
}

// fetchIndexGroups reads the groups concurrently, each into its own pointer list: a settled group from
// its snapshot when it is still good, any other from the database (then snapshotted when settled).
func (table *MariaTable) fetchIndexGroups(
	sink recordSink, queryPlan *db.IndexGroupReadPlan, fetchStates []db.IndexGroupFetchState,
	postFilterStatements []db.ColumnStatement, readTime int32,
) ([]db.RecordGroup[unsafe.Pointer], error) {
	selectedColumns := table.projectedColumns(&db.TableInfo{})
	selectedNames := make([]string, len(selectedColumns))
	for position, stored := range selectedColumns {
		selectedNames[position] = stored.name
	}
	codec := table.recordSnapshotCodecFor(sink.recordType)
	usesSnapshots := codec.isEligible && cache.RecordSnapshotsEnabled()
	snapshotByValue := map[uint64][]byte{}
	if usesSnapshots {
		var err error
		if snapshotByValue, err = table.loadGroupSnapshots(queryPlan, fetchStates); err != nil {
			return nil, err
		}
	}

	groups := make([]db.RecordGroup[unsafe.Pointer], len(fetchStates))
	group := errgroup.Group{}
	group.SetLimit(groupReadConcurrency)
	for fetchIndex, fetchState := range fetchStates {
		group.Go(func() error {
			// A settled group has a packed value: its values fit the slots its mark was found under.
			groupValue, _ := queryPlan.IndexGroup.LookupPackedValue(fetchState.IndexGroupValues)
			records, isServed := []unsafe.Pointer(nil), false
			if content, hasSnapshot := snapshotByValue[groupValue]; hasSnapshot {
				_, records, isServed = decodeRecordSnapshotFile(content, codec, sink.recordType, readTime, table.Name+" __group_data")
			}
			if !isServed {
				// A group over a slice reads its slice's raw-values table, a scalar group over several keys
				// its composite index; any other, its fetch step.
				var conditions []string
				var args []any
				var err error
				if condition, conditionArgs, isOverSlice := table.groupElementCondition(queryPlan, fetchState.IndexGroupValues); isOverSlice {
					conditions, args = []string{condition}, conditionArgs
				} else if condition, conditionArgs, isScalar := table.scalarGroupCondition(queryPlan, fetchState.IndexGroupValues); isScalar {
					conditions, args = []string{condition}, conditionArgs
				} else if conditions, args, err = table.renderConditions(accessStepStatements(table.IndexGroupFetchStep(queryPlan, fetchState))); err != nil {
					return err
				}
				statement := fmt.Sprintf("SELECT %s FROM %s WHERE %s", quotedNames(selectedNames), table.qualifiedName(), strings.Join(conditions, " AND "))
				if err := table.queryInto(statement, args, selectedColumns, sink.collectInto(&records)); err != nil {
					return err
				}
				// lastWrite 0 is an unsettled group: a write of its tick may still land, so it is not kept.
				if usesSnapshots && fetchState.LastWrite != 0 && len(records) > 0 {
					table.saveGroupSnapshot(queryPlan, groupValue, fetchState.LastWrite, codec, sink.recordType, readTime, records)
				}
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

// loadGroupSnapshots reads, in one statement, the snapshot of every settled group of the fetch whose
// built_write is still the group's last_write, by the group's packed value.
func (table *MariaTable) loadGroupSnapshots(queryPlan *db.IndexGroupReadPlan, fetchStates []db.IndexGroupFetchState) (map[uint64][]byte, error) {
	lastWriteByValue := map[uint64]int32{}
	for _, fetchState := range fetchStates {
		if groupValue, isPackable := queryPlan.IndexGroup.LookupPackedValue(fetchState.IndexGroupValues); isPackable && fetchState.LastWrite != 0 {
			lastWriteByValue[groupValue] = fetchState.LastWrite
		}
	}
	snapshotByValue := map[uint64][]byte{}
	if len(lastWriteByValue) == 0 {
		return snapshotByValue, nil
	}
	pool, err := sharedConnection()
	if err != nil {
		return nil, err
	}
	args := []any{queryPlan.PartitionValue, queryPlan.IndexGroup.IndexID}
	for groupValue := range lastWriteByValue {
		args = append(args, groupValue)
	}
	rows, err := pool.Query(fmt.Sprintf("SELECT `index_value`, `built_write`, `data` FROM %s WHERE `partition_id` = ? AND `index_id` = ? AND `index_value` IN (%s)",
		groupDataTableSpec(table.Namespace, table.Name).qualifiedName(), strings.TrimSuffix(strings.Repeat("?, ", len(lastWriteByValue)), ", ")), args...)
	if err != nil {
		return nil, fmt.Errorf("mariadbdriver: reading the cache-group snapshots of %q: %w", table.Name, err)
	}
	defer rows.Close()
	for rows.Next() {
		var groupValue uint64
		var builtWrite int32
		var content []byte
		if err := rows.Scan(&groupValue, &builtWrite, &content); err != nil {
			return nil, err
		}
		if lastWriteByValue[groupValue] == builtWrite {
			snapshotByValue[groupValue] = content
		}
	}
	return snapshotByValue, rows.Err()
}

// saveGroupSnapshot stores a settled group's records labelled builtWrite, its last_write when read. A
// plain upsert: should a write land meanwhile, its mark moves last_write and the label stops matching.
// A failed save only logs: the response already has its records.
func (table *MariaTable) saveGroupSnapshot(queryPlan *db.IndexGroupReadPlan, groupValue uint64, builtWrite int32,
	codec recordSnapshotCodec, recordType reflect.Type, readTime int32, records []unsafe.Pointer) {
	content, err := encodeRecordSnapshot(codec, recordType, builtWrite, readTime, records)
	if err == nil {
		var pool *sql.DB
		if pool, err = sharedConnection(); err == nil {
			_, err = pool.Exec(fmt.Sprintf("INSERT INTO %s (`partition_id`, `index_id`, `index_value`, `built_write`, `data`) VALUES (?, ?, ?, ?, ?)"+
				" ON DUPLICATE KEY UPDATE `built_write` = VALUES(`built_write`), `data` = VALUES(`data`)",
				groupDataTableSpec(table.Namespace, table.Name).qualifiedName()),
				queryPlan.PartitionValue, queryPlan.IndexGroup.IndexID, groupValue, builtWrite, content)
		}
	}
	if err != nil {
		fmt.Println("Cache group snapshot: saving a group of", table.Name, "failed:", err)
	}
}

// recordMatchesStatements checks predicates on a decoded record in memory, with Scylla's post-filter
// semantics (scylla/select.go recordMatchesPostFilter): integer comparisons, CONTAINS over a slice. A
// computed column reads its value from the record's method.
func (table *MariaTable) recordMatchesStatements(recordPointer unsafe.Pointer, statements []db.ColumnStatement) bool {
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

// scalarGroupCondition is the WHERE of a scalar cache group over several keys (index_mapping.go): its
// real columns pinned, served by its composite index. ok is false for a group read otherwise.
func (table *MariaTable) scalarGroupCondition(queryPlan *db.IndexGroupReadPlan, groupValues []int64) (condition string, args []any, ok bool) {
	indexGroup := queryPlan.IndexGroup
	if indexGroup.VirtualColumn == nil || indexGroup.VirtualColumn.IsNil() || indexGroup.UsesCollectionValues || indexGroup.BaseGroup != nil {
		return "", nil, false
	}
	conditions, args := []string{quoteName(table.GetPartKey().GetName()) + " = ?"}, []any{queryPlan.PartitionValue}
	for position, sourceColumn := range indexGroup.SourceColumns {
		conditions = append(conditions, quoteName(sourceColumn.Column.GetName())+" = ?")
		args = append(args, groupValues[position])
	}
	return strings.Join(conditions, " AND "), args, true
}

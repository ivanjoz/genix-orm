package scylla

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// The by-IDs cache lets a client ask "give me these records" and get back only the ones that
// actually moved. It works on slots, not records: every record belongs to the slot uint8(record_id),
// and cache_updated_version stores one version per (table+partition, slot).
//
// Write path: every write already reserves a per-partition sequence value for the record's
// "updated_version" column, so the slot row is a blind UPDATE with that value truncated to int16 —
// no read, no read-modify-write, one small row per touched slot.
//
// Read path: one partition read returns every slot version for a tenant's table. A requested ID
// whose client-held version still equals its slot version is not read from the table at all.
//
// The truncation to int16 is deliberate: slot versions are only ever compared for equality, so
// wrap-around aliasing (1 in 65536) is accepted in exchange for a much smaller row.
//
// On the by-IDs path only, the returned record's "upv" field is overwritten with the *slot*
// version instead of its own write version — that is the value the client must send back next time.
// Every other read path leaves "upv" as the record's own version, which is what the delta cache
// uses as a watermark.

const cacheUpdatedVersionTableName = "cache_updated_version"

// Feature is opt-in per schema and skipped for the slot table itself to avoid recursive writes.
func shouldUseSlotVersionFeature(scyllaTable ScyllaTable) bool {
	return scyllaTable.SaveUpdatedVersion && scyllaTable.Name != cacheUpdatedVersionTableName
}

// Precomputes and validates all table-level metadata needed by runtime slot-version updates.
func configureSlotVersionFields(scyllaTable *ScyllaTable) {
	if !shouldUseSlotVersionFeature(*scyllaTable) {
		return
	}

	if len(scyllaTable.Keys) != 1 {
		panic(fmt.Sprintf(`Table "%v": SaveUpdatedVersion requires exactly one key column.`, scyllaTable.Name))
	}

	keyColumn := scyllaTable.Keys[0]
	keyFieldType := keyColumn.GetType().FieldType
	if keyFieldType != "int16" && keyFieldType != "int32" && keyFieldType != "int64" {
		panic(fmt.Sprintf(`Table "%v": SaveUpdatedVersion key column "%v" must be int16/int32/int64. Found: %v`,
			scyllaTable.Name, keyColumn.GetName(), keyFieldType))
	}

	partitionColumn := scyllaTable.GetPartKey()
	if partitionColumn == nil || partitionColumn.IsNil() {
		panic(fmt.Sprintf(`Table "%v": SaveUpdatedVersion requires a partition column.`, scyllaTable.Name))
	}

	partitionFieldType := partitionColumn.GetType().FieldType
	if partitionFieldType != "int32" && partitionFieldType != "int64" {
		panic(fmt.Sprintf(`Table "%v": SaveUpdatedVersion partition column "%v" must be int32/int64. Found: %v`,
			scyllaTable.Name, partitionColumn.GetName(), partitionFieldType))
	}

	if scyllaTable.UpdatedVersionCol == nil || scyllaTable.UpdatedVersionCol.IsNil() {
		panic(fmt.Sprintf(`Table "%v": SaveUpdatedVersion requires the managed "%v" column; declare UpdatedVersion in the record and table structs.`,
			scyllaTable.Name, managedUpdatedVersionColumnName))
	}

	scyllaTable.SlotVersionPartitionCol = partitionColumn
	scyllaTable.SlotVersionKeyCol = keyColumn
}

// makeSlotVersionPartitionID packs the tenant and the table into one key: 18 bits of partition in
// the high bits, 14 bits of table ID in the low ones. The result is stored as a signed int, so its
// top bit may be set — both sides treat it as opaque.
func makeSlotVersionPartitionID(partitionID int32, tableID int16) (int32, error) {
	if partitionID < 0 || partitionID > MaxCachePartitionID {
		return 0, fmt.Errorf("partition %d is outside the %d slots the by-IDs cache key can address",
			partitionID, MaxCachePartitionID)
	}
	if tableID <= 0 || tableID > MaxTableID {
		return 0, fmt.Errorf("table ID %d is outside 1..%d", tableID, MaxTableID)
	}
	return int32(uint32(partitionID)<<14 | uint32(tableID)), nil
}

// slotOfRecordID buckets a record into one of 256 slots. Every read and write path must agree on
// this, so it exists exactly once.
func slotOfRecordID(recordID int64) uint8 {
	return uint8(recordID)
}

// toStoredSlotVersion truncates a sequence value to the width the slot row holds. Zero is reserved
// for "the client holds no version", so a stored version never takes it.
func toStoredSlotVersion(updatedVersion int64) uint16 {
	storedVersion := uint16(updatedVersion)
	if storedVersion == 0 {
		return 1
	}
	return storedVersion
}

// InitCacheUpdatedVersionTable ensures the slot-version table exists before any by-IDs read or write.
func InitCacheUpdatedVersionTable() error {
	keyspace := connParams.Keyspace
	if keyspace == "" {
		return fmt.Errorf("InitCacheUpdatedVersionTable: no keyspace configured")
	}

	// One partition per (tenant, table) holding at most 256 tiny clustering rows, so the whole
	// partition is read in one query and stays in the row cache.
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %v.%v (
			partition_table_id int, id_slot tinyint, updated_version smallint,
			PRIMARY KEY (partition_table_id, id_slot)
		)
		%v;`,
		keyspace, cacheUpdatedVersionTableName, makeStatementWith)

	return QueryExec(createTableQuery)
}

func resolveSlotVersionKeyspace(scyllaTable ScyllaTable) string {
	if scyllaTable.Namespace != "" {
		return scyllaTable.Namespace
	}
	return connParams.Keyspace
}

// loadSlotVersions reads every slot version of one (table, partition) in a single query. A missing
// row means "unknown", which never matches a client value and therefore always forces a read.
func loadSlotVersions(keyspace string, partitionTableID int32) (map[uint8]uint16, error) {
	query := fmt.Sprintf("SELECT id_slot, updated_version FROM %v.%v WHERE partition_table_id = ?",
		keyspace, cacheUpdatedVersionTableName)

	versionBySlot := map[uint8]uint16{}
	iterator := getScyllaConnection().Query(query, partitionTableID).Iter()

	var slotValue int8
	var storedVersion int16
	for iterator.Scan(&slotValue, &storedVersion) {
		versionBySlot[uint8(slotValue)] = uint16(storedVersion)
	}
	if err := iterator.Close(); err != nil {
		return nil, err
	}
	return versionBySlot, nil
}

// Write path: one blind UPDATE per touched slot, batched. Nothing is read first, and the records
// are left untouched — their "upv" already holds the value being stored here.
func updateSlotVersionsAfterWrite(records recordSliceGroup, scyllaTable ScyllaTable) error {
	if !shouldUseSlotVersionFeature(scyllaTable) || records.len() == 0 {
		return nil
	}
	if scyllaTable.UpdatedVersionCol == nil || scyllaTable.UpdatedVersionCol.IsNil() {
		return nil
	}

	type slotKey struct {
		partitionID int32
		slot        uint8
	}

	// Keep the highest version per slot so a batch spanning several writes still lands monotonically.
	versionBySlotKey := map[slotKey]int64{}
	for i := range records.len() {
		rawRecordPtr := records.at(i)

		updatedVersion := convertToInt64(scyllaTable.UpdatedVersionCol.GetRawValue(rawRecordPtr))
		if updatedVersion <= 0 {
			continue
		}
		key := slotKey{
			partitionID: convertToInt32(scyllaTable.SlotVersionPartitionCol.GetRawValue(rawRecordPtr)),
			slot:        slotOfRecordID(convertToInt64(scyllaTable.SlotVersionKeyCol.GetRawValue(rawRecordPtr))),
		}
		if currentVersion, exists := versionBySlotKey[key]; !exists || updatedVersion > currentVersion {
			versionBySlotKey[key] = updatedVersion
		}
	}
	if len(versionBySlotKey) == 0 {
		return nil
	}

	keyspace := resolveSlotVersionKeyspace(scyllaTable)
	updateQuery := fmt.Sprintf(
		"UPDATE %v.%v SET updated_version = ? WHERE partition_table_id = ? AND id_slot = ?",
		keyspace, cacheUpdatedVersionTableName)

	session := getScyllaConnection()
	batch := session.NewBatch(gocql.UnloggedBatch)
	for key, updatedVersion := range versionBySlotKey {
		partitionTableID, err := makeSlotVersionPartitionID(key.partitionID, scyllaTable.ID)
		if err != nil {
			return fmt.Errorf("table %q: %w", scyllaTable.Name, err)
		}
		batch.Query(updateQuery, int16(toStoredSlotVersion(updatedVersion)), partitionTableID, int8(key.slot))
	}

	if err := session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("table %q: saving %d slot versions: %w", scyllaTable.Name, len(versionBySlotKey), err)
	}
	if DebugFull {
		fmt.Printf("Slot versions written: table=%s slots=%d\n", scyllaTable.Name, len(versionBySlotKey))
	}
	return nil
}

/* Selecting by IDs */

type slotVersionMismatchDebugRow struct {
	ID            int64
	Slot          uint8
	ClientVersion uint16
	ServerVersion uint16
}

const queryCachedIDsMaxBatchSize = 100

func splitIDsIntoBatches(ids []int64, batchSize int) [][]int64 {
	if len(ids) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = len(ids)
	}

	idBatches := make([][]int64, 0, (len(ids)+batchSize-1)/batchSize)
	for startIndex := 0; startIndex < len(ids); startIndex += batchSize {
		endIndex := startIndex + batchSize
		if endIndex > len(ids) {
			endIndex = len(ids)
		}
		// Copy the batch to keep query input deterministic and isolated from later mutations.
		currentBatch := append([]int64(nil), ids[startIndex:endIndex]...)
		idBatches = append(idBatches, currentBatch)
	}

	return idBatches
}

// cachedIDsFetchPlan is the result of the slot-version comparison phase: which IDs actually need a
// table read, plus the already-loaded slot state so "upv" can be stamped without re-reading it.
type cachedIDsFetchPlan struct {
	idsToFetchByPartition map[int32][]int64
	versionsBySlotAndPart map[int32]map[uint8]uint16
}

func (plan cachedIDsFetchPlan) hasRecordsToFetch() bool {
	return len(plan.idsToFetchByPartition) > 0
}

// Shared validation for every by-IDs entry point: the feature must be on, the keyspace resolvable,
// and the precomputed partition/key metadata present.
func prepareCachedIDsTable(scyllaTable *ScyllaTable, callerName string) error {
	if !shouldUseSlotVersionFeature(*scyllaTable) {
		return fmt.Errorf(`Table "%v": %v requires SaveUpdatedVersion enabled`, scyllaTable.Name, callerName)
	}
	if len(scyllaTable.Namespace) == 0 {
		scyllaTable.Namespace = connParams.Keyspace
	}
	if len(scyllaTable.Namespace) == 0 {
		return fmt.Errorf("%v: no keyspace configured", callerName)
	}
	if scyllaTable.SlotVersionPartitionCol == nil || scyllaTable.SlotVersionPartitionCol.IsNil() {
		return fmt.Errorf(`Table "%v": %v slot-version partition metadata missing`, scyllaTable.Name, callerName)
	}
	if scyllaTable.SlotVersionKeyCol == nil || scyllaTable.SlotVersionKeyCol.IsNil() {
		return fmt.Errorf(`Table "%v": %v slot-version key metadata missing`, scyllaTable.Name, callerName)
	}
	return nil
}

// Phase 1: compare client versions against cache_updated_version, without touching the main table.
func planCachedIDsFetch(scyllaTable ScyllaTable, cachedIDs []IDUpdatedVersion) (cachedIDsFetchPlan, error) {
	plan := cachedIDsFetchPlan{
		idsToFetchByPartition: map[int32][]int64{},
		versionsBySlotAndPart: map[int32]map[uint8]uint16{},
	}

	// Keep client versions by partition+id so equal IDs in different partitions are isolated.
	clientVersionByPartitionAndID := map[int32]map[int64]uint16{}
	uniqueIDsByPartition := map[int32]map[int64]struct{}{}

	for _, cachedID := range cachedIDs {
		partitionID := convertToInt32(cachedID.PartitionID)
		if _, exists := uniqueIDsByPartition[partitionID]; !exists {
			uniqueIDsByPartition[partitionID] = map[int64]struct{}{}
			clientVersionByPartitionAndID[partitionID] = map[int64]uint16{}
		}
		uniqueIDsByPartition[partitionID][cachedID.ID] = struct{}{}
		clientVersionByPartitionAndID[partitionID][cachedID.ID] = cachedID.UpdatedVersion
	}

	if len(uniqueIDsByPartition) == 0 {
		fmt.Println("planCachedIDsFetch: no unique IDs after normalization")
		return plan, nil
	}

	mismatchDebugRowsByPartition := map[int32][]slotVersionMismatchDebugRow{}

	for partitionID, idsSet := range uniqueIDsByPartition {
		partitionTableID, err := makeSlotVersionPartitionID(partitionID, scyllaTable.ID)
		if err != nil {
			return plan, fmt.Errorf("table %q: %w", scyllaTable.Name, err)
		}
		versionBySlot, err := loadSlotVersions(scyllaTable.Namespace, partitionTableID)
		if err != nil {
			return plan, err
		}
		plan.versionsBySlotAndPart[partitionID] = versionBySlot

		for recordID := range idsSet {
			slot := slotOfRecordID(recordID)
			serverVersion := versionBySlot[slot]
			clientVersion := clientVersionByPartitionAndID[partitionID][recordID]
			// A zero on either side means "unknown", and unknown is never fresh.
			if serverVersion != 0 && clientVersion == serverVersion {
				continue
			}
			if DebugFull {
				mismatchDebugRowsByPartition[partitionID] = append(
					mismatchDebugRowsByPartition[partitionID],
					slotVersionMismatchDebugRow{
						ID:            recordID,
						Slot:          slot,
						ClientVersion: clientVersion,
						ServerVersion: serverVersion,
					},
				)
			}
			plan.idsToFetchByPartition[partitionID] = append(plan.idsToFetchByPartition[partitionID], recordID)
		}
		// Deterministic ordering keeps the emitted batches stable across requests.
		sort.Slice(plan.idsToFetchByPartition[partitionID], func(leftIndex, rightIndex int) bool {
			return plan.idsToFetchByPartition[partitionID][leftIndex] < plan.idsToFetchByPartition[partitionID][rightIndex]
		})
	}

	if DebugFull && len(mismatchDebugRowsByPartition) > 0 {
		fmt.Println("planCachedIDsFetch: slot version mismatches by partition:", mismatchDebugRowsByPartition)
	}

	return plan, nil
}

// Phase 2: run one batched "partition = ? AND key IN (...)" select per partition batch, capped to
// Scylla's clustering-key restriction limit. The caller decides how to scan each batch.
func forEachCachedIDsBatch(
	plan cachedIDsFetchPlan,
	scyllaTable ScyllaTable,
	projection string,
	runBatch func(queryString string, queryValues []any, partitionID int32) error,
) error {
	partitionColumnName := scyllaTable.SlotVersionPartitionCol.GetName()
	keyColumnName := scyllaTable.SlotVersionKeyCol.GetName()

	for partitionID, recordIDsToFetch := range plan.idsToFetchByPartition {
		if len(recordIDsToFetch) == 0 {
			continue
		}

		recordIDBatches := splitIDsIntoBatches(recordIDsToFetch, queryCachedIDsMaxBatchSize)
		for _, recordIDBatch := range recordIDBatches {
			queryValues := make([]any, 0, len(recordIDBatch)+1)
			valuePlaceholders := make([]string, 0, len(recordIDBatch))
			queryValues = append(queryValues, partitionID)
			for _, recordID := range recordIDBatch {
				queryValues = append(queryValues, recordID)
				valuePlaceholders = append(valuePlaceholders, "?")
			}

			queryString := fmt.Sprintf(
				"SELECT %v FROM %v.%v WHERE %v = ? AND %v IN (%v)",
				projection,
				scyllaTable.Namespace,
				scyllaTable.Name,
				partitionColumnName,
				keyColumnName,
				strings.Join(valuePlaceholders, ", "),
			)

			if err := runBatch(queryString, queryValues, partitionID); err != nil {
				return err
			}
		}
	}

	return nil
}

// stampSlotVersionsOnRecords overwrites each record's "upv" with the slot version it was validated
// against. This is the value the client sends back on its next by-IDs request; the record's own
// write version is not useful there, because a record is refetched whenever any record in its slot
// moves and would otherwise mismatch forever.
func stampSlotVersionsOnRecords(records recordSlice, scyllaTable ScyllaTable, plan cachedIDsFetchPlan) {
	for i := range records.len() {
		rawRecordPtr := records.at(i)

		partitionID := convertToInt32(scyllaTable.SlotVersionPartitionCol.GetRawValue(rawRecordPtr))
		recordID := convertToInt64(scyllaTable.SlotVersionKeyCol.GetRawValue(rawRecordPtr))
		slotVersion := plan.versionsBySlotAndPart[partitionID][slotOfRecordID(recordID)]

		scyllaTable.UpdatedVersionCol.SetValue(rawRecordPtr,
			coerceManagedIntegerValue(scyllaTable.UpdatedVersionCol, int64(slotVersion)))
	}
}

// Only return records whose slot version differs from the client-provided version.
func QueryCachedIDs[T TableBaseInterface[E, T], E TableSchemaInterface[E]](refSlice *[]T, cachedIDs []IDUpdatedVersion) error {
	if len(cachedIDs) == 0 {
		fmt.Println("QueryCachedIDs: empty request, nothing to process")
		return nil
	}

	scyllaTable := MakeScyllaTable[T, E]()
	if err := prepareCachedIDsTable(&scyllaTable, "QueryCachedIDs"); err != nil {
		return err
	}

	plan, err := planCachedIDsFetch(scyllaTable, cachedIDs)
	if err != nil {
		return err
	}
	if !plan.hasRecordsToFetch() {
		fmt.Println("QueryCachedIDs: all IDs resolved from slot versions, skipping table select")
		return nil
	}

	// The typed path returns whole records, so every non-virtual column is projected.
	columnNames := make([]string, 0, len(scyllaTable.Columns))
	for _, column := range scyllaTable.Columns {
		if column.GetInfo().IsVirtual {
			continue
		}
		columnNames = append(columnNames, column.GetName())
	}

	fetchedRecords := []T{}
	scanColumns := buildDefaultScanColumns(columnNames)

	err = forEachCachedIDsBatch(plan, scyllaTable, strings.Join(columnNames, ", "),
		func(queryString string, queryValues []any, _ int32) error {
			return scanSelectQueryRows(
				queryString, queryValues, scanColumns, scyllaTable,
				makeRecordSink(&fetchedRecords), nil, nil, time.Now(),
			)
		})
	if err != nil {
		return err
	}

	stampSlotVersionsOnRecords(makeRecordSlice(&fetchedRecords), scyllaTable, plan)

	*refSlice = append(*refSlice, fetchedRecords...)
	return nil
}

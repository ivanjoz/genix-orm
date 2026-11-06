package scylla

import (
	"errors"
	"fmt"
	"github.com/ivanjoz/genix-orm/db"
	"reflect"
	"slices"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

// Cache-group snapshots (TypeCacheGroupS3): the server keeps each settled group it read from the
// database in the file store, labelled with the group's last_write, so the next client that lacks
// the group — another browser, another user — is answered from the file with no database read.
//
//   - Only a full database read of a settled group writes a file (an unsettled group goes out with
//     label 0 and is never stored), never a served snapshot.
//   - A file is served while its label equals the probe's last_write and it is younger than
//     cache.RecordSnapshotMaxAgeTicks (2 h): the age bounds how long a group whose rows landed past
//     the settle margin (or a lost replica mutation) is served without them.
//   - The file holds the whole group; the read's post-filter runs in memory, so one file serves
//     every query shape that lands on the group.
//   - The store never fails a read: an error is a miss, a failed write is dropped.

// cacheGroupSnapshotConcurrency caps the parallel group reads of one request on an S3 index, each
// being a GET and maybe a database read and a PUT.
const cacheGroupSnapshotConcurrency = 16

// requireCacheGroupSnapshotCodec fails the compile of a table with a TypeCacheGroupS3 index whose
// record does not round-trip through colbin: a column colbin drops would come back zero from the file
// and filled from the database, so a read's result would depend on whether the cache was hit.
func requireCacheGroupSnapshotCodec(dbTable *ScyllaTable, structRefValue reflect.Value) {
	if !slices.ContainsFunc(dbTable.IndexGroups, func(indexGroup db.IndexGroupInfo) bool { return indexGroup.SnapshotInS3 }) {
		return
	}
	var recordType reflect.Type
	if recordTyped, isTyped := structRefValue.Addr().Interface().(interface{ BaseStructType() reflect.Type }); isTyped {
		recordType = recordTyped.BaseStructType()
	}
	if recordType == nil {
		panic(fmt.Sprintf(`Table "%v": TypeCacheGroupS3 needs the table's record type`, dbTable.Name))
	}
	if !recordSnapshotCodecFor(*dbTable, recordType).isEligible {
		panic(fmt.Sprintf(`Table "%v": TypeCacheGroupS3 needs every stored column in the colbin schema of %v (no cb:"-" on a stored column)`,
			dbTable.Name, recordType))
	}
}

// readCacheGroupRecords reads one group of a grouped fetch. A settled group of a TypeCacheGroupS3
// index is served from its file when the file's label is the group's last_write; otherwise it is
// read whole from the database and the file rewritten. Every other group is a plain database read.
// readTime is the server clock of this request.
func readCacheGroupRecords(
	sink recordSink, scyllaTable ScyllaTable, queryPlan *indexGroupSelectPlan, fetchState indexGroupFetchState,
	readGroupFromDatabase func(postFilterStatements []ColumnStatement) ([]unsafe.Pointer, error),
	postFilterStatements []ColumnStatement, readTime int32,
) ([]unsafe.Pointer, error) {
	fileStore := cache.FileStore()
	// lastWrite 0 is an unsettled group: it is never served from a file nor stored.
	if !queryPlan.indexGroup.SnapshotInS3 || fetchState.lastWrite == 0 || fileStore == nil || !cache.RecordSnapshotsEnabled() {
		return readGroupFromDatabase(postFilterStatements)
	}
	recordType := sinkRecordType(sink)
	codec := recordSnapshotCodecFor(scyllaTable, recordType)
	snapshotKey := cache.CacheGroupSnapshotKey(scyllaTable.Name, int64(queryPlan.partitionValue), queryPlan.indexGroup.IndexID, fetchState.hashValue)

	content, etag, err := fileStore.GetFile(snapshotKey)
	switch {
	case errors.Is(err, utils.ErrFileNotFound):
		etag = ""
	case err != nil:
		// The store didn't answer: the database does, and nothing is written over a file we could not see.
		fmt.Println("Cache group snapshot: reading", snapshotKey, "failed, reading the database:", err)
		return readGroupFromDatabase(postFilterStatements)
	default:
		snapshot, snapshotRecords, isUsable := decodeRecordSnapshotFile(content, codec, recordType, readTime, snapshotKey)
		if isUsable && snapshot.Cursor == fetchState.lastWrite {
			return filterGroupRecords(snapshotRecords, postFilterStatements, scyllaTable), nil
		}
	}

	groupRecords, err := readGroupFromDatabase(nil)
	if err != nil {
		return nil, err
	}
	// An empty group is not stored: reading it again costs next to nothing.
	if len(groupRecords) > 0 {
		writeRecordSnapshotFile(fileStore, snapshotKey, etag, codec, recordType, fetchState.lastWrite, readTime, groupRecords)
	}
	return filterGroupRecords(groupRecords, postFilterStatements, scyllaTable), nil
}

// filterGroupRecords applies a read's post-filter to a whole group in memory.
func filterGroupRecords(groupRecords []unsafe.Pointer, postFilterStatements []ColumnStatement, scyllaTable ScyllaTable) []unsafe.Pointer {
	if len(postFilterStatements) == 0 {
		return groupRecords
	}
	return slices.DeleteFunc(groupRecords, func(recordPointer unsafe.Pointer) bool {
		return !recordMatchesPostFilter(recordPointer, postFilterStatements, scyllaTable)
	})
}

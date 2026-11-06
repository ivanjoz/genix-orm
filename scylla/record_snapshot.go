package scylla

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"reflect"
	"sync"
	"unsafe"

	"github.com/ivanjoz/colbin"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

// Record snapshots (db/cache record_snapshot.go) as the driver reads and writes them: the codec of a
// record type, and the file <-> records steps both kinds share (delta_snapshot.go,
// cache_group_snapshot.go).

// recordSnapshotCodec is what a record type needs to be kept in a snapshot.
type recordSnapshotCodec struct {
	// fingerprint names the record type's colbin schema: a file written by another schema is a miss.
	fingerprint uint64
	// isEligible is false when a stored column is a field colbin drops (cb:"-"): the file would
	// serve that column zeroed, so the table never uses snapshots.
	isEligible bool
}

// recordSnapshotCodecs caches recordSnapshotCodec by record type.
var recordSnapshotCodecs sync.Map

func recordSnapshotCodecFor(scyllaTable ScyllaTable, recordType reflect.Type) recordSnapshotCodec {
	if cachedCodec, isCached := recordSnapshotCodecs.Load(recordType); isCached {
		return cachedCodec.(recordSnapshotCodec)
	}
	codec := recordSnapshotCodec{}
	schema, schemaErr := colbin.SchemaOf(reflect.New(recordType).Interface())
	fieldIDs, fieldsErr := colbin.FieldIDs(reflect.New(recordType).Interface())
	if schemaErr == nil && fieldsErr == nil {
		fingerprintHash := fnv.New64a()
		fingerprintHash.Write(schema.Bytes())
		codec.fingerprint = fingerprintHash.Sum64()
		codec.isEligible = true
		for _, column := range scyllaTable.Columns {
			columnInfo := column.GetInfo()
			// Virtual columns and managed ones the record doesn't declare (created) never reach it.
			if columnInfo.IsVirtual || columnInfo.Field == nil {
				continue
			}
			if _, isEncoded := fieldIDs[columnInfo.FieldName]; !isEncoded {
				codec.isEligible = false
				break
			}
		}
	} else {
		fmt.Println("Record snapshot: table", scyllaTable.Name, "is not colbin-encodable, snapshots disabled:", errors.Join(schemaErr, fieldsErr))
	}
	recordSnapshotCodecs.Store(recordType, codec)
	return codec
}

// sinkRecordType is the record type a sink allocates.
func sinkRecordType(sink recordSink) reflect.Type {
	return reflect.TypeOf(sink.asRecordPointer(sink.newRecord())).Elem()
}

// decodeRecordSnapshotFile opens a snapshot file for a read at readTime. isUsable is false when the
// file can't answer: unreadable, written by another schema, or older than cache.RecordSnapshotMaxAgeTicks.
// The caller still checks the snapshot's Cursor against what it needs.
func decodeRecordSnapshotFile(
	content []byte, codec recordSnapshotCodec, recordType reflect.Type, readTime int32, snapshotKey string,
) (snapshot cache.RecordSnapshot, records []unsafe.Pointer, isUsable bool) {
	snapshot, err := cache.DecodeRecordSnapshot(content)
	if err != nil || snapshot.SchemaFingerprint != codec.fingerprint || cache.IsRecordSnapshotExpired(snapshot, readTime) {
		return snapshot, nil, false
	}
	recordsPointer := reflect.New(reflect.SliceOf(recordType))
	if err := colbin.Unmarshal(snapshot.Records, recordsPointer.Interface()); err != nil {
		fmt.Println("Record snapshot: decoding", snapshotKey, "failed:", err)
		return snapshot, nil, false
	}
	return snapshot, db.MakeReflectRecordSlice(recordsPointer).Pointers(), true
}

// writeRecordSnapshotFile stores records as a snapshot labelled cursor, read at readTime. etag is the
// file the read replaced ("" = none): when another request wrote it first, this write is skipped,
// its snapshot being as good. A failed write only logs: the response already has its records.
func writeRecordSnapshotFile(
	fileStore utils.FileStore, snapshotKey, etag string, codec recordSnapshotCodec, recordType reflect.Type,
	cursor, readTime int32, records []unsafe.Pointer,
) {
	recordsPointer := reflect.New(reflect.SliceOf(recordType))
	recordList := recordsPointer.Elem()
	recordList.Set(reflect.MakeSlice(recordList.Type(), len(records), len(records)))
	for index, recordPointer := range records {
		recordList.Index(index).Set(reflect.NewAt(recordType, recordPointer).Elem())
	}
	encodedRecords, err := colbin.Marshal(recordsPointer.Interface())
	if err != nil {
		fmt.Println("Record snapshot: encoding", snapshotKey, "failed:", err)
		return
	}
	content := cache.EncodeRecordSnapshot(cache.RecordSnapshot{
		SchemaFingerprint: codec.fingerprint, Cursor: cursor, ReadTime: readTime, Records: encodedRecords,
	})
	if _, err := fileStore.PutFileIfMatch(context.Background(), snapshotKey, content, etag); err != nil &&
		!errors.Is(err, utils.ErrFilePreconditionFailed) {
		fmt.Println("Record snapshot: writing", snapshotKey, "failed:", err)
	}
}

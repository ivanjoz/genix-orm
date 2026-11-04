package cache

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ─────────────────────────────────────────────────────────────────────────────
// Record snapshots: records a full database read returned, kept in the file store
//
// Two features keep one: the delta snapshot of a TypeDelta prefix (delta_snapshot.go)
// and the cache-group snapshot of a TypeCacheGroupS3 group (CacheGroupSnapshotKey).
// Both share the file layout, the schema fingerprint and the max age. Each file
// is written only from a full database read, never from another snapshot.
// ─────────────────────────────────────────────────────────────────────────────

// RecordSnapshotMaxAgeTicks (2 h) bounds how long a snapshot is served after the read that wrote it.
// Neither kind is checked against the database again once settled, so a write that broke its lag
// bound (a delta write past DeltaWriteLagTicks, a group's rows landing past its settle margin, a lost
// replica mutation) would otherwise be missing from every first read until the next rewrite.
const RecordSnapshotMaxAgeTicks = 3600

// IsRecordSnapshotExpired: the snapshot is older than RecordSnapshotMaxAgeTicks at readTime, the
// current read's server clock, so the database answers and rewrites it.
func IsRecordSnapshotExpired(snapshot RecordSnapshot, readTime int32) bool {
	return readTime-snapshot.ReadTime > RecordSnapshotMaxAgeTicks
}

// RecordSnapshot is one decoded snapshot file.
type RecordSnapshot struct {
	// SchemaFingerprint names the colbin schema of the record type that wrote Records.
	SchemaFingerprint uint64
	// Cursor is the label the file is valid for. Delta: the highest updated among the records, the C
	// a client holding them would send. Cache group: the group's last_write.
	Cursor int32
	// ReadTime is the server clock when the read that wrote the records began (the delta's V).
	ReadTime int32
	// Records is the colbin of the []Record.
	Records []byte
}

var errRecordSnapshotTruncated = errors.New("record snapshot: truncated header")

// EncodeRecordSnapshot lays out a snapshot file:
//
//	[schema fingerprint: u64 LE][cursor: uvarint][read time: uvarint][records: colbin of []Record]
func EncodeRecordSnapshot(snapshot RecordSnapshot) []byte {
	content := binary.LittleEndian.AppendUint64(nil, snapshot.SchemaFingerprint)
	content = binary.AppendUvarint(content, uint64(snapshot.Cursor))
	content = binary.AppendUvarint(content, uint64(snapshot.ReadTime))
	return append(content, snapshot.Records...)
}

// DecodeRecordSnapshot reads a file EncodeRecordSnapshot wrote. Records aliases content.
func DecodeRecordSnapshot(content []byte) (RecordSnapshot, error) {
	if len(content) < 8 {
		return RecordSnapshot{}, errRecordSnapshotTruncated
	}
	snapshot := RecordSnapshot{SchemaFingerprint: binary.LittleEndian.Uint64(content)}
	cursor, cursorSize := binary.Uvarint(content[8:])
	if cursorSize <= 0 {
		return RecordSnapshot{}, errRecordSnapshotTruncated
	}
	readTime, readTimeSize := binary.Uvarint(content[8+cursorSize:])
	if readTimeSize <= 0 {
		return RecordSnapshot{}, errRecordSnapshotTruncated
	}
	snapshot.Cursor, snapshot.ReadTime = int32(cursor), int32(readTime)
	snapshot.Records = content[8+cursorSize+readTimeSize:]
	return snapshot, nil
}

// CacheGroupSnapshotKey is the file of one group of a TypeCacheGroupS3 index, under the store's root:
//
//	cache-group/<table>/<partition>/<index id>/<group hash>.cb
func CacheGroupSnapshotKey(tableName string, partition int64, indexID int16, groupHash int32) string {
	return fmt.Sprintf("cache-group/%s/%d/%d/%d.cb", tableName, partition, indexID, groupHash)
}

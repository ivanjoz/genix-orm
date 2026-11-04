package cache

import (
	"bytes"
	"testing"
)

func TestRecordSnapshotRoundTrip(t *testing.T) {
	snapshot := RecordSnapshot{SchemaFingerprint: 0xA1B2C3D4E5F60718, Cursor: 812345678, ReadTime: 812345680, Records: []byte{1, 2, 3, 250}}
	decoded, err := DecodeRecordSnapshot(EncodeRecordSnapshot(snapshot))
	if err != nil || decoded.SchemaFingerprint != snapshot.SchemaFingerprint || decoded.Cursor != snapshot.Cursor ||
		decoded.ReadTime != snapshot.ReadTime || !bytes.Equal(decoded.Records, snapshot.Records) {
		t.Fatalf("got %+v, err %v", decoded, err)
	}
	// No bytes, a short fingerprint, a fingerprint with no cursor, a cursor with no read time.
	for _, truncated := range [][]byte{nil, {1, 2, 3}, make([]byte, 8), make([]byte, 9)} {
		if _, err := DecodeRecordSnapshot(truncated); err == nil {
			t.Fatalf("expected %v to be rejected", truncated)
		}
	}
}

func TestIsRecordSnapshotExpired(t *testing.T) {
	snapshot := RecordSnapshot{Cursor: 1000, ReadTime: 1001}
	if IsRecordSnapshotExpired(snapshot, 1001+RecordSnapshotMaxAgeTicks) {
		t.Fatal("a snapshot exactly 2 h old is still served")
	}
	if !IsRecordSnapshotExpired(snapshot, 1002+RecordSnapshotMaxAgeTicks) {
		t.Fatal("a snapshot past 2 h must be read from the database again")
	}
}

func TestCacheGroupSnapshotKey(t *testing.T) {
	if key := CacheGroupSnapshotKey("sale_orders", 7, 2, -184467); key != "cache-group/sale_orders/7/2/-184467.cb" {
		t.Fatalf("got %q", key)
	}
}

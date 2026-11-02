package cache

import (
	"encoding/binary"
	"math/bits"
	"slices"
	"strconv"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// Delta window protocol (the Scylla driver's Delta()): no change log. A TypeDelta
// index is a slim materialized view (ID, updated, version, sync filter) keyed by
// the partition, the pinned values and updated, and a sync reads it past the
// client's cursor.
//
// The cursor C is the highest Updated (SUnixTime) the client holds. A sync:
//
//   - returns the records the view lists with updated ≥ C − DeltaWindowMarginTicks,
//     read from the table by ID, in every status;
//   - returns the hash of (ID, version) over the view rows with updated in
//     [C − DeltaWindowTicks, C − DeltaWindowMarginTicks) whose sync filter value is
//     in the call's values;
//   - returns as removed the IDs that left the pinned prefix (a pinned value
//     changed) in the window, read from the moves table.
//
// The client compares: it hashes the records it holds in the same window with a
// matching filter value, leaving out every ID the response brought or removed (a
// record edited again after C moved out of the server's window but not out of the
// client's). A mismatch means a write the client missed (one that landed late, a
// view lag, a lost move): it asks again without a cursor, which replaces its
// records with the view's.
//
// A first sync, or more than the index's threshold of changed or moved records,
// answers a replace: every record of the prefix whose filter value is in the
// call's values. The cursor's age never does: the view and the moves never expire.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// DeltaWindowMarginTicks (10 s) absorbs the writes stamped just before C that land after a sync:
	// they are always resent instead of failing the hash.
	DeltaWindowMarginTicks = 5
	// DeltaWindowTicks (15 min) is how far behind C the hash checks.
	DeltaWindowTicks = 450
	// DefaultDeltaReplaceThreshold is how many changed (or moved) records a sync answers by ID before
	// it answers a replace instead.
	DefaultDeltaReplaceThreshold = 100

	DeltaWindowFlagReplace = 2
	// DeltaWindowFlagWindow tells genix-ui this Metadata is the window protocol, not a delta log file.
	DeltaWindowFlagWindow = 8
)

// ParseDeltaWindowCursor reads the cursor genix-ui sends: the highest Updated it holds. Anything
// else, absent included, is 0: a first sync.
func ParseDeltaWindowCursor(encodedCursor string) int32 {
	cursorUpdated, err := strconv.ParseInt(encodedCursor, 10, 32)
	if err != nil || cursorUpdated < 0 {
		return 0
	}
	return int32(cursorUpdated)
}

// DeltaRecordHash is one record's term of the window hash. genix-ui computes the same value
// (testdata/delta_window_vectors.json pins both): the murmur3 32-bit finalizer over the ID's halves
// and the version.
func DeltaRecordHash(recordID int64, version int16) uint32 {
	lowHalf, highHalf := uint32(uint64(recordID)), uint32(uint64(recordID)>>32)
	mixed := lowHalf ^ bits.RotateLeft32(highHalf, 16) ^ (uint32(uint16(version)) * 0x9E3779B1)
	mixed ^= mixed >> 16
	mixed *= 0x85EBCA6B
	mixed ^= mixed >> 13
	mixed *= 0xC2B2AE35
	mixed ^= mixed >> 16
	return mixed
}

// DeltaWindowMetadata is what a window sync tells the client besides the records.
type DeltaWindowMetadata struct {
	IsReplace bool
	// NextCursor is the highest updated the view listed, the cursor the client keeps.
	NextCursor int32
	// WindowFrom and WindowTo bound the hashed window, WindowTo excluded; both 0 on a replace.
	WindowFrom int32
	WindowTo   int32
	WindowHash uint32
	// FilterField is the JSON name of the sync filter field, "" when the index has none;
	// FilterValues are the values the hash (and a replace) kept.
	FilterField  string
	FilterValues []int64
	// RemovedIDs left the pinned prefix inside the window.
	RemovedIDs []int64
}

// EncodeDeltaWindowMetadata encodes the Metadata text of a window sync, utils.EncodeSafe85 of:
//
//	[flags: u8]                      DeltaWindowFlagWindow, | DeltaWindowFlagReplace
//	[next cursor: uvarint]
//	[window from: uvarint][window to: uvarint]
//	[window hash: u32 LE]
//	[filter field length: u8][filter field: UTF-8]
//	[filter value count: uvarint][filter value: zigzag varint] × count
//	[removed ID count: uvarint][removed ID delta: uvarint] × count   ascending, the first absolute
func EncodeDeltaWindowMetadata(metadata DeltaWindowMetadata) string {
	flags := byte(DeltaWindowFlagWindow)
	if metadata.IsReplace {
		flags |= DeltaWindowFlagReplace
	}
	content := []byte{flags}
	content = binary.AppendUvarint(content, uint64(metadata.NextCursor))
	content = binary.AppendUvarint(content, uint64(metadata.WindowFrom))
	content = binary.AppendUvarint(content, uint64(metadata.WindowTo))
	content = binary.LittleEndian.AppendUint32(content, metadata.WindowHash)
	content = append(content, byte(len(metadata.FilterField)))
	content = append(content, metadata.FilterField...)
	content = binary.AppendUvarint(content, uint64(len(metadata.FilterValues)))
	for _, filterValue := range metadata.FilterValues {
		content = binary.AppendVarint(content, filterValue)
	}
	removedIDs := slices.Clone(metadata.RemovedIDs)
	slices.Sort(removedIDs)
	content = binary.AppendUvarint(content, uint64(len(removedIDs)))
	previousID := int64(0)
	for _, removedID := range removedIDs {
		content = binary.AppendUvarint(content, uint64(removedID-previousID))
		previousID = removedID
	}
	return utils.EncodeSafe85(content)
}

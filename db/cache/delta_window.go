package cache

import (
	"encoding/binary"
	"math/bits"
	"slices"
	"strconv"
	"strings"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// Delta window protocol (the Scylla driver's Delta()): no change log. A TypeDelta
// index is a materialized view of the whole record keyed by the partition, the
// index's Keys (all pinned) and updated, and a sync reads it past the client's
// cursor. A call covers one prefix per combination of key values: its last Key's
// values are Delta()'s (a status tab, say), the others come from equalities or fan
// out over FixedValues.
//
// The cursor is "C.V": C is the highest Updated (SUnixTime) the client holds, V the
// server's clock when the read that answered it began (the response's ReadTime).
// A write becomes visible at most DeltaWriteLagTicks after the Updated it is stamped
// with, so every write stamped before V − DeltaWriteLagTicks was visible to that
// read: the client may only have missed the ones in the unsettled slice
// [V − DeltaWriteLagTicks, C]. A sync:
//
//   - returns the records of the prefixes with updated > C;
//   - returns the hash of (ID, version) over the prefixes' rows in the unsettled
//     slice, instead of resending them (a reload right after a bulk write would
//     resend the whole write); an empty window once C is settled (V − lag > C);
//   - returns as removed the IDs that left a prefix (a key value changed: a move, a
//     soft delete, an annulment) since the slice began, read from the moves table;
//   - returns its own ReadTime, the next V.
//
// The client compares: it hashes the records it holds in the same window whose last
// Key holds one of the call's values, leaving out every ID the response brought or
// removed (a record edited again after C: the client still holds its old copy in the
// slice, the view no longer does). A mismatch means a write that landed late in the
// slice: the client asks again with "C.V.r", and the server resends the slice's
// records (updated ≥ V − DeltaWriteLagTicks) instead of hashing them.
//
// A first sync, or more than the index's threshold of changed or moved records,
// answers a replace: every record of the prefixes, newest first up to the read's
// Limit. The cursor's age never does: the view and the moves never expire.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// DeltaWriteLagTicks (16 s) bounds how long after its Updated stamp a write becomes visible to
	// the view. Both clocks floor to a tick, so a write stamped at tick s is visible to a read that
	// began at tick V ≥ s + DeltaWriteLagTicks + 1: the unsettled slice starts at V − DeltaWriteLagTicks.
	DeltaWriteLagTicks = 8
	// DefaultDeltaReplaceThreshold is how many changed (or moved) records a sync answers before it
	// answers a replace instead.
	DefaultDeltaReplaceThreshold = 100

	DeltaWindowFlagReplace = 2
	// DeltaWindowFlagWindow tells genix-ui this Metadata is the window protocol, not a delta log file.
	DeltaWindowFlagWindow = 8
)

// DeltaWindowCursor is what a client holds of one response key. The zero value is a first sync.
type DeltaWindowCursor struct {
	// Updated is C, the highest Updated the client holds.
	Updated int32
	// ReadTime is V, the server clock when the read that answered the client began.
	ReadTime int32
	// ResendUnsettled asks for the unsettled slice's records instead of its hash: the client's
	// hash of it mismatched.
	ResendUnsettled bool
}

// DeltaSyncPlan is what a sync that is not a replace reads for a cursor.
type DeltaSyncPlan struct {
	// ChangedFrom: the records with updated ≥ ChangedFrom are sent.
	ChangedFrom int32
	// MovesFrom: the moves with updated ≥ MovesFrom are sent as removed IDs. A late move inside
	// the slice is invisible to the hash (the client's stale copy sits outside it), so moves are
	// always read from the slice's start.
	MovesFrom int32
	// WindowFrom and WindowTo bound the hashed rows, WindowTo excluded; equal when nothing is hashed.
	WindowFrom int32
	WindowTo   int32
}

// PlanDeltaSync places the unsettled slice [V − DeltaWriteLagTicks, C] of a cursor: hashed, or
// resent when the cursor asks for it, or absent once C is settled.
func PlanDeltaSync(cursor DeltaWindowCursor) DeltaSyncPlan {
	unsettledFrom := max(cursor.ReadTime-DeltaWriteLagTicks, 0)
	plan := DeltaSyncPlan{ChangedFrom: cursor.Updated + 1, MovesFrom: min(cursor.Updated+1, unsettledFrom)}
	switch {
	case unsettledFrom > cursor.Updated:
		// Settled: every write stamped up to C was visible to the read that answered the client.
	case cursor.ResendUnsettled:
		plan.ChangedFrom = unsettledFrom
	default:
		plan.WindowFrom, plan.WindowTo = unsettledFrom, cursor.Updated+1
	}
	return plan
}

// ParseDeltaWindowCursor reads the cursor genix-ui sends: "C.V", or "C.V.r" to resend the
// unsettled slice. Anything else, absent included, is the zero cursor: a first sync.
func ParseDeltaWindowCursor(encodedCursor string) DeltaWindowCursor {
	cursorParts := strings.Split(encodedCursor, ".")
	if len(cursorParts) < 2 || len(cursorParts) > 3 || (len(cursorParts) == 3 && cursorParts[2] != "r") {
		return DeltaWindowCursor{}
	}
	cursorUpdated, updatedErr := strconv.ParseInt(cursorParts[0], 10, 32)
	readTime, readTimeErr := strconv.ParseInt(cursorParts[1], 10, 32)
	if updatedErr != nil || readTimeErr != nil || cursorUpdated <= 0 || readTime <= 0 {
		return DeltaWindowCursor{}
	}
	return DeltaWindowCursor{Updated: int32(cursorUpdated), ReadTime: int32(readTime), ResendUnsettled: len(cursorParts) == 3}
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
	// NextCursor is the highest updated the view listed, the C the client keeps.
	NextCursor int32
	// ReadTime is the server clock when this read began, the V the client keeps.
	ReadTime int32
	// WindowFrom and WindowTo bound the hashed window, WindowTo excluded; both 0 when nothing is
	// hashed (a replace, a settled cursor, a resend).
	WindowFrom int32
	WindowTo   int32
	WindowHash uint32
	// FilterField is the JSON name of the index's last Key, "" when the index has none;
	// FilterValues are the values the call read, which the client's hash must match.
	FilterField  string
	FilterValues []int64
	// RemovedIDs left the call's prefixes since the window began.
	RemovedIDs []int64
}

// EncodeDeltaWindowMetadata encodes the Metadata text of a window sync, utils.EncodeSafe85 of:
//
//	[flags: u8]                      DeltaWindowFlagWindow, | DeltaWindowFlagReplace
//	[next cursor: uvarint]
//	[read time: uvarint]
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
	content = binary.AppendUvarint(content, uint64(metadata.ReadTime))
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

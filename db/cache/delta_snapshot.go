package cache

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

// ─────────────────────────────────────────────────────────────────────────────
// Delta snapshots: the replace a server already read, kept in the file store
//
// A replace (a first sync, or a sync past the threshold) reads every record of
// its prefixes from the database whether or not anyone keeps them. The driver
// writes each prefix's records to one snapshot file, so the next replace of that
// prefix — another browser, another user — starts from the file and reads only
// what changed since it, exactly as a client holding the snapshot would sync.
//
// The rules that keep a snapshot as good as a database read:
//
//   - Only a full read of the database writes a snapshot. A snapshot merged with
//     the changes since it is served, never stored: a miss the window hash cannot
//     see (a late write older than the window) would otherwise be copied into
//     every later snapshot.
//   - Serving one runs the window protocol against it, its cursor being the C and
//     V of the read that wrote it: the changed records since C, the moves, and the
//     unsettled slice's hash, which the server compares over
//     the snapshot's records the way a client compares over its own. A mismatch, or
//     more than the threshold of changes or moves, reads the prefix from the
//     database instead (and that read writes a new snapshot).
//   - A snapshot older than RecordSnapshotMaxAgeTicks (2 h) is read from the
//     database again: past the slice, nothing checks it any more.
//   - A projected read (Select / Exclude) has its own file, named by its column
//     list: the file holds the records as that projection reads them, and the
//     changes merged into it are read with the same projection.
//   - The file names the record type's colbin schema; a struct change makes every
//     older file a miss.
//
// The file layout is the shared RecordSnapshot (record_snapshot.go): its Cursor is
// the C and its ReadTime the V of the read that wrote it.
// ─────────────────────────────────────────────────────────────────────────────

// DeltaSnapshotKey is the file of one prefix of a TypeDelta index, under the store's root:
//
//	delta-snapshot/<table>/<partition>/<index tag>/<prefix values joined by _ | all>.l<limit>[.p<projection hash>].cb
//
// The read's Limit is part of the name: a snapshot cut at one Limit cannot answer another. So is
// its projection (nil = every column): a file of projected records cannot answer a read of more
// columns, nor one of fewer without sending what that read excluded.
func DeltaSnapshotKey(tableName string, partition int64, indexTag string, prefix []int64, limit int32, projection []string) string {
	prefixName := "all"
	if len(prefix) > 0 {
		prefixValues := make([]string, len(prefix))
		for index, prefixValue := range prefix {
			prefixValues[index] = strconv.FormatInt(prefixValue, 10)
		}
		prefixName = strings.Join(prefixValues, "_")
	}
	projectionName := ""
	if projection != nil {
		projectionHash := fnv.New64a()
		projectionHash.Write([]byte(strings.Join(projection, ",")))
		projectionName = fmt.Sprintf(".p%016x", projectionHash.Sum64())
	}
	return fmt.Sprintf("%s%s/%d/%s/%s.l%d%s.cb", deltaSnapshotKeyPrefix, tableName, partition, indexTag, prefixName, limit, projectionName)
}

// DeltaHashRow is the part of a held record the window hash reads.
type DeltaHashRow struct {
	ID      int64
	Updated int32
	Version int16
}

// DeltaHeldWindowHash is the hash a holder of rows compares with the server's: the sum of
// DeltaRecordHash over the rows with updated in [windowFrom, windowTo), leaving out excludedIDs
// (the IDs the sync brought or removed). genix-ui computes the same over the records it holds; the
// server computes it over a snapshot's.
func DeltaHeldWindowHash(rows []DeltaHashRow, windowFrom, windowTo int32, excludedIDs map[int64]bool) uint32 {
	var windowHash uint32
	for _, row := range rows {
		if row.Updated >= windowFrom && row.Updated < windowTo && !excludedIDs[row.ID] {
			windowHash += DeltaRecordHash(row.ID, row.Version)
		}
	}
	return windowHash
}

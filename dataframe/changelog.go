package dataframe

import (
	"errors"
	"fmt"
	"math"
	"slices"
)

// ─────────────────────────────────────────────────────────────────────────────
// Changelog: a binary value every record of a table with frames carries (genix-orm
// dynamo: the item attribute cgl, beside its blob). It holds, newest first, the values
// the tracked columns held before each write that changed them, only the columns
// that changed:
//
//	entry header, 3 bytes
//	  16 bits  delta: Updated − the change's SUnixTime          bytes 0–1, big-endian
//	   6 bits  column count (1..63)                             byte 2, bits 7–2
//	   1 bit   reduced format                                   byte 2, bit 1
//	   1 bit   contains negatives                               byte 2, bit 0
//	column, reduced (every value of the entry in [0, 2^30))
//	   u8 column id, then 2 bits size s (bits 7–6) and 6 + 8s value bits: 6 in the
//	   first byte, then s bytes, least significant first: uint6 / uint14 / uint22 / uint30
//	column, normal
//	   u8 column id, then 3 bits size s (bits 7–5), [1 bit sign (bit 4), only when the
//	   entry contains negatives], and n + 8s magnitude bits: n = 5 (4 with the sign)
//	   in the first byte, then s bytes: up to 61 bits (60 with negatives)
//
// Every write of a stored record shifts the deltas by how much Updated moved, drops
// the entries past MaxDelta (≈ 36.4 h) and, when a tracked column changed, puts first
// an entry with delta 0 and the stored values of the changed columns. An insert
// writes none: created = Updated says the record did not exist before.
// ─────────────────────────────────────────────────────────────────────────────

// MaxDelta: an entry is kept at most this many SUnixTime ticks (2 s) behind Updated.
const MaxDelta = math.MaxUint16

const changelogReducedLimit = 1 << 30

var errChangelogCorrupt = errors.New("dataframe: changelog is truncated or corrupt")

// changelogValue is the value a column held before a change.
type changelogValue struct {
	columnID uint8
	value    int64
}

// changelogEntry is one change: delta ticks before Updated, the columns it changed with their values before it.
type changelogEntry struct {
	delta  int64
	values []changelogValue
}

// NextChangelog is the changelog of a record written over a stored one: the stored entries shifted by
// how much Updated moved and cut at MaxDelta, after a new entry with the stored values of the columns
// that changed. The values are tracked-values slices (Table.Tracked order). nil when nothing is left:
// the backend then stores no changelog.
func (table *Table) NextChangelog(storedChangelog []byte, storedUpdated, writtenUpdated int64, storedValues, writtenValues []int64) ([]byte, error) {
	entries, err := decodeChangelog(storedChangelog)
	if err != nil {
		return nil, err
	}
	// A write stamped before the stored Updated (a lagging clock) shifts nothing.
	shift := max(writtenUpdated-storedUpdated, 0)
	var nextEntries []changelogEntry
	var changed []changelogValue
	for i, column := range table.Tracked {
		if storedValues[i] != writtenValues[i] {
			changed = append(changed, changelogValue{columnID: uint8(column.ColumnID), value: storedValues[i]})
		}
	}
	if len(changed) > 0 {
		nextEntries = append(nextEntries, changelogEntry{delta: 0, values: changed})
	}
	for _, entry := range entries {
		if entry.delta += shift; entry.delta <= MaxDelta {
			nextEntries = append(nextEntries, entry)
		}
	}
	return encodeChangelog(nextEntries), nil
}

func encodeChangelog(entries []changelogEntry) []byte {
	var out []byte
	for _, entry := range entries {
		hasNegatives, isReduced := false, true
		for _, column := range entry.values {
			hasNegatives = hasNegatives || column.value < 0
			isReduced = isReduced && column.value >= 0 && column.value < changelogReducedLimit
		}
		flags := byte(len(entry.values)) << 2
		if isReduced {
			flags |= 2
		}
		if hasNegatives {
			flags |= 1
		}
		out = append(out, byte(entry.delta>>8), byte(entry.delta), flags)
		for _, column := range entry.values {
			out = append(out, column.columnID)
			out = appendChangelogValue(out, column.value, isReduced, hasNegatives)
		}
	}
	return out
}

// changelogValueBits is the layout of an entry's values: the bits of the size, and the value bits in
// the first byte.
func changelogValueBits(isReduced, hasNegatives bool) (sizeBits, firstBits int) {
	switch {
	case isReduced:
		return 2, 6
	case hasNegatives:
		return 3, 4
	default:
		return 3, 5
	}
}

// appendChangelogValue writes one value in the entry's format: the size, the sign when the entry has
// negatives, the low bits in the first byte, then the size in whole bytes.
func appendChangelogValue(out []byte, value int64, isReduced, hasNegatives bool) []byte {
	sizeBits, firstBits := changelogValueBits(isReduced, hasNegatives)
	magnitude, signBit := uint64(value), byte(0)
	if value < 0 {
		magnitude, signBit = uint64(-value), 1
	}
	extraBytes := 0
	for magnitude>>(firstBits+8*extraBytes) != 0 {
		extraBytes++
	}
	first := byte(extraBytes)<<(8-sizeBits) | byte(magnitude&(1<<firstBits-1))
	if hasNegatives && !isReduced {
		first |= signBit << 4
	}
	out = append(out, first)
	magnitude >>= firstBits
	for range extraBytes {
		out = append(out, byte(magnitude))
		magnitude >>= 8
	}
	return out
}

func decodeChangelog(content []byte) ([]changelogEntry, error) {
	var entries []changelogEntry
	for position := 0; position < len(content); {
		if len(content)-position < 3 {
			return nil, errChangelogCorrupt
		}
		entry := changelogEntry{delta: int64(content[position])<<8 | int64(content[position+1])}
		flags := content[position+2]
		position += 3
		columnCount, isReduced, hasNegatives := int(flags>>2), flags&2 != 0, flags&1 != 0
		sizeBits, firstBits := changelogValueBits(isReduced, hasNegatives)
		for range columnCount {
			if len(content)-position < 2 {
				return nil, errChangelogCorrupt
			}
			columnID, first := content[position], content[position+1]
			position += 2
			extraBytes := int(first >> (8 - sizeBits))
			if len(content)-position < extraBytes {
				return nil, errChangelogCorrupt
			}
			magnitude := uint64(first & (1<<firstBits - 1))
			for i := range extraBytes {
				magnitude |= uint64(content[position+i]) << (firstBits + 8*i)
			}
			position += extraBytes
			value := int64(magnitude)
			if hasNegatives && !isReduced && first&(1<<4) != 0 {
				value = -value
			}
			entry.values = append(entry.values, changelogValue{columnID: columnID, value: value})
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// History is what a stored record says of its past: its tracked values now, when it was created and
// last written (SUnixTimes), and its changelog. A backend builds one per record it reads for a run, a
// rebuild or a fresh read.
type History struct {
	RecordKey string
	values    []int64
	created   int64
	updated   int64
	entries   []changelogEntry
}

// NewHistory decodes a stored record's changelog into its History. values are its tracked values.
func NewHistory(recordKey string, values []int64, created, updated int64, changelog []byte) (History, error) {
	entries, err := decodeChangelog(changelog)
	if err != nil {
		return History{}, fmt.Errorf("%s: %w", recordKey, err)
	}
	return History{RecordKey: recordKey, values: values, created: created, updated: updated, entries: entries}, nil
}

// valuesAt is what the record held at the SUnixTime moment: false before it was created. A change at
// moment t counts from t on. Exact for moments after the record's Updated − MaxDelta.
func (table *Table) valuesAt(history History, moment int64) ([]int64, bool) {
	if moment < history.created {
		return nil, false
	}
	values := slices.Clone(history.values)
	for _, entry := range history.entries {
		if history.updated-entry.delta <= moment {
			break
		}
		for _, column := range entry.values {
			if trackedIndex := slices.IndexFunc(table.Tracked, func(tracked Column) bool { return tracked.ColumnID == int(column.columnID) }); trackedIndex >= 0 {
				values[trackedIndex] = column.value
			}
		}
	}
	return values, true
}

// changeMoments lists the moments of the record's changes in (from, to], oldest first.
func (history History) changeMoments(from, to int64) []int64 {
	var moments []int64
	for _, entry := range history.entries {
		if moment := history.updated - entry.delta; moment > from && moment <= to {
			moments = append(moments, moment)
		}
	}
	slices.Reverse(moments)
	return moments
}

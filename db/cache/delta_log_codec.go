package cache

import (
	"encoding/binary"
	"fmt"
	"slices"
)

// ─────────────────────────────────────────────────────────────────────────────
// Delta log codec: the entries a write appends to its TypeDelta index's 10-minute
// file (delta_log.go), and the bytes of the Metadata record that carries a file to
// the browser. genix-ui's delta-log.wire.ts decodes both, and
// testdata/delta_log_vectors.json pins the two implementations together.
//
// An entry is bit-packed big-endian, so its header is the top 2 bits of its first
// byte:
//
//	header 0..2  [header:2][deleted:1][log ID:w][version:13]
//	             w by header: 16, 24, 32 bits on the 32-bit scale (4, 5, 6 bytes),
//	                          32, 48, 64 bits on the 64-bit scale (6, 8, 10 bytes)
//	header 3     [3:2][deleted:1][version:13][count:8], then count uvarint log ID
//	             deltas, ascending, the first one absolute: records written at one
//	             version, typically a bulk insert at version 1
//
// The scale is the table's: 32-bit when its log ID takes at most 32 bits. The
// version is the record's Version folded into 13 bits (WireVersion). Every entry is
// a whole number of bytes and an append writes whole entries, so a file is a plain
// sequence of them.
// ─────────────────────────────────────────────────────────────────────────────

const (
	deltaLogVersionBits = 13
	// deltaLogVersionModulo: wire versions run 1..8191 and wrap, so they are compared by equality only.
	deltaLogVersionModulo = 1<<deltaLogVersionBits - 1
	deltaLogGroupHeader   = 3
	deltaLogMaxGroupCount = 255
)

// deltaLogIDWidths are the log ID widths of headers 0..2: [0] on the 32-bit scale, [1] on the 64-bit one.
var deltaLogIDWidths = [2][3]int{{16, 24, 32}, {32, 48, 64}}

// DeltaLogEntry is one change of a delta log: the record whose log ID it names reached Version, or
// left the index's folder (IsDeleted: soft- or hard-deleted, or its pinned value changed).
type DeltaLogEntry struct {
	LogID     uint64
	Version   uint16
	IsDeleted bool
}

// WireVersion folds a record's Version (1..32767) into an entry's 13 bits: 1..8191.
func WireVersion(version int16) uint16 {
	return uint16((int(version)-1)%deltaLogVersionModulo + 1)
}

func scaleIndex(is64BitScale bool) int {
	if is64BitScale {
		return 1
	}
	return 0
}

// EncodeDeltaLogEntries packs the entries of one append. Entries sharing a version and a deleted flag
// go as a group when that takes fewer bytes than one entry each.
func EncodeDeltaLogEntries(entries []DeltaLogEntry, is64BitScale bool) []byte {
	type groupKey struct {
		version   uint16
		isDeleted bool
	}
	logIDsByGroup := map[groupKey][]uint64{}
	var groupOrder []groupKey
	for _, entry := range entries {
		key := groupKey{entry.Version, entry.IsDeleted}
		if _, isSeen := logIDsByGroup[key]; !isSeen {
			groupOrder = append(groupOrder, key)
		}
		logIDsByGroup[key] = append(logIDsByGroup[key], entry.LogID)
	}

	writer := bitWriter{}
	for _, key := range groupOrder {
		logIDs := slices.Clone(logIDsByGroup[key])
		slices.Sort(logIDs)
		logIDs = slices.Compact(logIDs)
		for start := 0; start < len(logIDs); start += deltaLogMaxGroupCount {
			chunk := logIDs[start:min(start+deltaLogMaxGroupCount, len(logIDs))]
			singlesSize := 0
			for _, logID := range chunk {
				singlesSize += singleEntrySize(logID, is64BitScale)
			}
			groupSize := 3
			previousLogID := uint64(0)
			for _, logID := range chunk {
				groupSize += uvarintSize(logID - previousLogID)
				previousLogID = logID
			}
			if len(chunk) == 1 || singlesSize <= groupSize {
				for _, logID := range chunk {
					writer.writeSingleEntry(DeltaLogEntry{LogID: logID, Version: key.version, IsDeleted: key.isDeleted}, is64BitScale)
				}
				continue
			}
			writer.write(deltaLogGroupHeader, 2)
			writer.write(boolBit(key.isDeleted), 1)
			writer.write(uint64(key.version), deltaLogVersionBits)
			writer.write(uint64(len(chunk)), 8)
			previousLogID = 0
			for _, logID := range chunk {
				writer.bytes = binary.AppendUvarint(writer.bytes, logID-previousLogID)
				previousLogID = logID
			}
		}
	}
	return writer.bytes
}

// singleEntryHeader is the smallest header whose width holds logID.
func singleEntryHeader(logID uint64, is64BitScale bool) int {
	for header, width := range deltaLogIDWidths[scaleIndex(is64BitScale)] {
		if width == 64 || logID>>width == 0 {
			return header
		}
	}
	panic(fmt.Sprintf("db: delta log ID %d does not fit the 32-bit scale", logID))
}

func singleEntrySize(logID uint64, is64BitScale bool) int {
	return (16 + deltaLogIDWidths[scaleIndex(is64BitScale)][singleEntryHeader(logID, is64BitScale)]) / 8
}

func (writer *bitWriter) writeSingleEntry(entry DeltaLogEntry, is64BitScale bool) {
	header := singleEntryHeader(entry.LogID, is64BitScale)
	writer.write(uint64(header), 2)
	writer.write(boolBit(entry.IsDeleted), 1)
	writer.write(entry.LogID, deltaLogIDWidths[scaleIndex(is64BitScale)][header])
	writer.write(uint64(entry.Version), deltaLogVersionBits)
}

// DecodeDeltaLogEntries reads a file, or the part of it past a byte offset, entry by entry.
func DecodeDeltaLogEntries(content []byte, is64BitScale bool) ([]DeltaLogEntry, error) {
	var entries []DeltaLogEntry
	reader := bitReader{bytes: content}
	for reader.position < len(content)*8 {
		entryStart := reader.position / 8
		header := int(reader.read(2))
		isDeleted := reader.read(1) == 1
		if header == deltaLogGroupHeader {
			version := uint16(reader.read(deltaLogVersionBits))
			count := int(reader.read(8))
			if reader.isTruncated || count == 0 || version == 0 {
				return nil, fmt.Errorf("db: delta log group at byte %d is malformed", entryStart)
			}
			position := reader.position / 8
			logID := uint64(0)
			for range count {
				logIDDelta, width := binary.Uvarint(content[position:])
				if width <= 0 {
					return nil, fmt.Errorf("db: delta log group at byte %d is truncated", entryStart)
				}
				position += width
				logID += logIDDelta
				entries = append(entries, DeltaLogEntry{LogID: logID, Version: version, IsDeleted: isDeleted})
			}
			reader.position = position * 8
			continue
		}
		logID := reader.read(deltaLogIDWidths[scaleIndex(is64BitScale)][header])
		version := uint16(reader.read(deltaLogVersionBits))
		if reader.isTruncated || version == 0 {
			return nil, fmt.Errorf("db: delta log entry at byte %d is malformed", entryStart)
		}
		entries = append(entries, DeltaLogEntry{LogID: logID, Version: version, IsDeleted: isDeleted})
	}
	return entries, nil
}

// PackDeltaLogID packs the log ID columns of a record into one integer, the first column in the high
// bits, each taking its Size.
func PackDeltaLogID(values []uint64, columnBits []int8) uint64 {
	logID := uint64(0)
	for i, value := range values {
		logID = logID<<columnBits[i] | value
	}
	return logID
}

// UnpackDeltaLogID is the inverse of PackDeltaLogID.
func UnpackDeltaLogID(logID uint64, columnBits []int8) []uint64 {
	values := make([]uint64, len(columnBits))
	for i := len(columnBits) - 1; i >= 0; i-- {
		values[i] = logID & (^uint64(0) >> (64 - int(columnBits[i])))
		logID >>= columnBits[i]
	}
	return values
}

// The flags of a Metadata record (DeltaMetadataBytes).
const (
	DeltaMetadataFlag64BitScale = 1 << 0
	// DeltaMetadataFlagReplace: the response holds every record of the view (a first sync, or a cursor
	// too far behind), so the browser drops the records it holds that the response doesn't.
	DeltaMetadataFlagReplace = 1 << 1
	// DeltaMetadataFlagSealed: no append reaches this file anymore. The browser applies it and forgets
	// it, so it stops sending it in its cursor.
	DeltaMetadataFlagSealed = 1 << 2
)

// DeltaMetadataBytes is the content of a Metadata record before Safe85:
//
//	[flags: u8][bucket: uvarint][log ID column count: u8][Size of each column: u8 …][the file's bytes]
//
// The bucket is the file's, also the record's negative ID; it travels here too so the browser reads it
// whatever the table's key columns are. The column count is 0 when the log ID is a single column;
// with several, the browser packs a cached record's columns with these Sizes to match it.
func DeltaMetadataBytes(flags byte, bucket int64, logIDColumnBits []int8, fileContent []byte) []byte {
	metadata := binary.AppendUvarint([]byte{flags}, uint64(bucket))
	if len(logIDColumnBits) > 1 {
		metadata = append(metadata, byte(len(logIDColumnBits)))
		for _, bits := range logIDColumnBits {
			metadata = append(metadata, byte(bits))
		}
	} else {
		metadata = append(metadata, 0)
	}
	return append(metadata, fileContent...)
}

func boolBit(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}

func uvarintSize(value uint64) int { return len(binary.AppendUvarint(nil, value)) }

// bitWriter appends values most significant bit first.
type bitWriter struct {
	bytes       []byte
	pending     uint64
	pendingBits int
}

func (writer *bitWriter) write(value uint64, bitCount int) {
	if bitCount > 32 {
		writer.write(value>>32, bitCount-32)
		writer.write(value&0xFFFFFFFF, 32)
		return
	}
	writer.pending = writer.pending<<bitCount | value&(1<<bitCount-1)
	writer.pendingBits += bitCount
	for writer.pendingBits >= 8 {
		writer.pendingBits -= 8
		writer.bytes = append(writer.bytes, byte(writer.pending>>writer.pendingBits))
	}
	writer.pending &= 1<<writer.pendingBits - 1
}

// bitReader reads values most significant bit first; reading past the end sets isTruncated.
type bitReader struct {
	bytes       []byte
	position    int // in bits
	isTruncated bool
}

func (reader *bitReader) read(bitCount int) uint64 {
	if bitCount > 32 {
		high := reader.read(bitCount - 32)
		return high<<32 | reader.read(32)
	}
	if reader.position+bitCount > len(reader.bytes)*8 {
		reader.isTruncated = true
		reader.position = len(reader.bytes) * 8
		return 0
	}
	value := uint64(0)
	for range bitCount {
		bit := reader.bytes[reader.position/8] >> (7 - reader.position%8) & 1
		value = value<<1 | uint64(bit)
		reader.position++
	}
	return value
}

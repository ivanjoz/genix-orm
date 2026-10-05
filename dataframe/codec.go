package dataframe

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"math/bits"
	"slices"
)

// ─────────────────────────────────────────────────────────────────────────────
// DataFrame file codec (../DATA_FRAMES.md, section 5, "Formats"): raw bytes, columnar.
//
//	u8        format version
//	uvarint   snapshot: the UpdatedVersion whose state the file holds
//	uvarint   row count n (no columns follow when n = 0)
//	column    Rows: uvarint first, uvarint minStep, residuals of the n − 1 steps
//	column    each Sums column: varint min, residuals of the n values
//
// Each column is turned into small non-negative residuals by a transform its role
// fixes, so no transform byte is stored: the ascending Rows column is delta-coded
// and its smallest step subtracted, and each Sums column has its minimum
// subtracted (frame of reference), which leaves every residual >= 0 even when a
// value is negative. The residuals are then divided by the largest of 1000, 100,
// 10, 8, 5 and 2 that divides them all (quantities stored as multiples of 1000 lose
// 10 bits each) and written in blocks of 128:
//
//	residuals := uvarint divisor, block*
//	block     := u8 head      bits 0–6 width w (0..64), bit 7 patched
//	             [u8 patch count, when patched]
//	             packed       the low w bits of each residual, least significant first,
//	                          no gaps: ceil(count × w / 8) bytes (16 × w for a full block)
//	             patches      per patch: u8 position in the block, uvarint(residual >> w)
//
// The width of each block is the one with the smallest exact cost: the widest
// residual's bit length, or a narrower width whose few longer residuals keep their
// high bits in patches. 128 values at any width fill whole bytes, so a block never
// pads and no state crosses blocks (the layout of colbin's column package, plus the
// patches, which carry the heavy tail of sales sums: a few top sellers per block).
// ─────────────────────────────────────────────────────────────────────────────

const (
	formatVersion    = 1
	blockSize        = 128
	blockPatchedFlag = 0x80
	blockWidthMask   = 0x7F
	// decodePadding is how far past a packed run the bit unpack loads: a value is read with
	// one 8-byte load (a ninth byte for widths above 57), whatever its offset in the run.
	decodePadding = 8
)

var errCorrupt = errors.New("db: frame file is truncated or corrupt")

// File is a decoded frame file: the frame at Snapshot. RowIDs is strictly ascending and >= 0, and
// each column of Sums is parallel to it.
type File struct {
	Snapshot int64
	RowIDs   []int64
	Sums     [][]int64
}

// codecOptions turns off a compression step. Files are always written with the zero value
// (every step on); the others exist for the size comparisons in codec_test.go.
type codecOptions struct {
	skipsDivisor bool
	skipsPatches bool
}

// appendFile encodes file onto out.
func appendFile(out []byte, file File, options codecOptions) []byte {
	out = append(out, formatVersion)
	out = binary.AppendUvarint(out, uint64(file.Snapshot))
	out = binary.AppendUvarint(out, uint64(len(file.RowIDs)))
	if len(file.RowIDs) == 0 {
		return out
	}
	out = appendAscendingColumn(out, file.RowIDs, options)
	for _, sumColumn := range file.Sums {
		out = appendSumsColumn(out, sumColumn, options)
	}
	return out
}

// appendAscendingColumn writes a non-decreasing column of values >= 0: its first value, then the
// steps between neighbours less the smallest step. Consecutive IDs become a run of zeros.
func appendAscendingColumn(out []byte, values []int64, options codecOptions) []byte {
	out = binary.AppendUvarint(out, uint64(values[0]))
	if len(values) == 1 {
		return out
	}
	steps := make([]uint64, len(values)-1)
	minStep := uint64(math.MaxUint64)
	for i := 1; i < len(values); i++ {
		steps[i-1] = uint64(values[i] - values[i-1])
		minStep = min(minStep, steps[i-1])
	}
	for i := range steps {
		steps[i] -= minStep
	}
	out = binary.AppendUvarint(out, minStep)
	return appendResiduals(out, steps, options)
}

// appendSumsColumn writes a column of any int64 values: its minimum, then each value less it. The
// subtraction is done in uint64, where it is exact for any two int64s.
func appendSumsColumn(out []byte, values []int64, options codecOptions) []byte {
	minimum := slices.Min(values)
	out = binary.AppendVarint(out, minimum)
	residuals := make([]uint64, len(values))
	for i, value := range values {
		residuals[i] = uint64(value) - uint64(minimum)
	}
	return appendResiduals(out, residuals, options)
}

// appendResiduals writes the residuals' common divisor, then the residuals divided by it in
// blocks. It divides residuals in place.
func appendResiduals(out []byte, residuals []uint64, options codecOptions) []byte {
	divisor := uint64(1)
	if !options.skipsDivisor {
		divisor = commonDivisor(residuals)
	}
	out = binary.AppendUvarint(out, divisor)
	if divisor > 1 {
		for i := range residuals {
			residuals[i] /= divisor
		}
	}
	for start := 0; start < len(residuals); start += blockSize {
		out = appendBlock(out, residuals[start:min(start+blockSize, len(residuals))], options)
	}
	return out
}

// The divisors a column is tried against: the scales business numbers come in (decimals stored
// as integers, round prices, packs). Each one still dividing every residual so far keeps its bit.
const (
	dividesBy2 uint8 = 1 << iota
	dividesBy5
	dividesBy8
	dividesBy10
	dividesBy100
	dividesBy1000
)

// commonDivisor returns the largest of 1000, 100, 10, 8, 5 and 2 that divides every residual, or 1.
// A residual drops each candidate it isn't a multiple of, and only the candidates left are tested
// on the next one; the scan stops once none is left. The divisors are constants, so each % compiles
// to a multiply and a shift rather than a division. Only the encoder is limited to these: the file
// stores the divisor as a uvarint, and the decoder multiplies by whatever it reads.
func commonDivisor(residuals []uint64) uint64 {
	candidates := dividesBy2 | dividesBy5 | dividesBy8 | dividesBy10 | dividesBy100 | dividesBy1000
	hasNonZero := false
	for _, residual := range residuals {
		if residual == 0 {
			continue
		}
		hasNonZero = true
		if candidates&dividesBy2 != 0 && residual%2 != 0 {
			candidates &^= dividesBy2 | dividesBy8 | dividesBy10 | dividesBy100 | dividesBy1000
		}
		if candidates&dividesBy5 != 0 && residual%5 != 0 {
			candidates &^= dividesBy5 | dividesBy10 | dividesBy100 | dividesBy1000
		}
		if candidates&dividesBy8 != 0 && residual%8 != 0 {
			candidates &^= dividesBy8 | dividesBy1000
		}
		if candidates&dividesBy10 != 0 && residual%10 != 0 {
			candidates &^= dividesBy10 | dividesBy100 | dividesBy1000
		}
		if candidates&dividesBy100 != 0 && residual%100 != 0 {
			candidates &^= dividesBy100 | dividesBy1000
		}
		if candidates&dividesBy1000 != 0 && residual%1000 != 0 {
			candidates &^= dividesBy1000
		}
		if candidates == 0 {
			return 1
		}
	}
	// Every residual 0: any divisor reads them back, and 1 is the shortest to store.
	if !hasNonZero {
		return 1
	}
	// The largest saves the most bits: 10 beats 8 on a column of multiples of 40.
	switch {
	case candidates&dividesBy1000 != 0:
		return 1000
	case candidates&dividesBy100 != 0:
		return 100
	case candidates&dividesBy10 != 0:
		return 10
	case candidates&dividesBy8 != 0:
		return 8
	case candidates&dividesBy5 != 0:
		return 5
	case candidates&dividesBy2 != 0:
		return 2
	}
	return 1
}

// appendBlock bit-packs up to blockSize residuals at the width that makes the block smallest.
// A residual longer than that width keeps its low bits in the run and its high bits in a patch.
func appendBlock(out []byte, block []uint64, options codecOptions) []byte {
	var countByBitLength [65]int
	for _, residual := range block {
		countByBitLength[bits.Len64(residual)]++
	}
	widest := 64
	for widest > 0 && countByBitLength[widest] == 0 {
		widest--
	}
	width, smallestCost := widest, blockCost(len(block), &countByBitLength, widest)
	if !options.skipsPatches {
		for candidate := 0; candidate < widest; candidate++ {
			if cost := blockCost(len(block), &countByBitLength, candidate); cost < smallestCost {
				width, smallestCost = candidate, cost
			}
		}
	}

	patchCount := 0
	for bitLength := width + 1; bitLength <= 64; bitLength++ {
		patchCount += countByBitLength[bitLength]
	}
	if patchCount == 0 {
		out = append(out, byte(width))
	} else {
		// At most blockSize patches: the count fits its byte.
		out = append(out, byte(width)|blockPatchedFlag, byte(patchCount))
	}
	out = packBits(out, block, width)
	for position, residual := range block {
		if bits.Len64(residual) > width {
			out = append(out, byte(position))
			out = binary.AppendUvarint(out, residual>>width)
		}
	}
	return out
}

// blockCost is the bytes a block takes at width: its head, the packed run and, when residuals
// are longer than width, the patch count and one patch each. It needs only how many residuals have
// each bit length, so every width is scored without another pass over the block.
func blockCost(count int, countByBitLength *[65]int, width int) int {
	cost := 1 + (count*width+7)/8
	isPatched := false
	for bitLength := width + 1; bitLength <= 64; bitLength++ {
		if countByBitLength[bitLength] == 0 {
			continue
		}
		isPatched = true
		// The patch holds the bits above width: a uvarint of 7 bits per byte.
		highBytes := (bitLength - width + 6) / 7
		cost += countByBitLength[bitLength] * (1 + highBytes)
	}
	if isPatched {
		cost++
	}
	return cost
}

// lowBitsMask has the low width bits set; a shift by 64 is 0 in Go, so width 64 is every bit.
func lowBitsMask(width int) uint64 { return uint64(1)<<width - 1 }

// packBits appends the low width bits of each residual, least significant bit first, with no gap
// between residuals: ceil(len(block) × width / 8) bytes.
func packBits(out []byte, block []uint64, width int) []byte {
	if width == 0 {
		return out
	}
	mask := lowBitsMask(width)
	var accumulator uint64
	accumulated := 0
	for _, residual := range block {
		value := residual & mask
		accumulator |= value << accumulated
		if accumulated+width < 64 {
			accumulated += width
			continue
		}
		// The word is full: flush it and carry the bits of value that did not fit.
		out = binary.LittleEndian.AppendUint64(out, accumulator)
		writtenBits := 64 - accumulated
		accumulator, accumulated = 0, accumulated+width-64
		if writtenBits < width {
			accumulator = value >> writtenBits
		}
	}
	for ; accumulated > 0; accumulated -= 8 {
		out = append(out, byte(accumulator))
		accumulator >>= 8
	}
	return out
}

// byteDecoder reads a frame file. data carries decodePadding zero bytes past end, so the bit
// unpack loads whole words without a tail path; every read is still bounded by end.
type byteDecoder struct {
	data     []byte
	end      int
	position int
}

// DecodeFile reads a file written by appendFile; sumsCount comes from the frame's schema.
func DecodeFile(content []byte, sumsCount int) (File, error) {
	decoder := &byteDecoder{data: append(slices.Clip(content), make([]byte, decodePadding)...), end: len(content)}
	version, err := decoder.readByte()
	if err != nil || version != formatVersion {
		return File{}, errCorrupt
	}
	snapshot, err := decoder.readUvarint()
	if err != nil {
		return File{}, err
	}
	rowCount, err := decoder.readUvarint()
	// Every 128 rows take at least one block head per column, so a count past that is corrupt
	// rather than a huge allocation to attempt.
	if err != nil || rowCount > uint64(decoder.end)*blockSize+1 {
		return File{}, errCorrupt
	}
	file := File{Snapshot: int64(snapshot), Sums: make([][]int64, sumsCount)}
	if rowCount == 0 {
		return file, nil
	}
	if file.RowIDs, err = decoder.readAscendingColumn(int(rowCount)); err != nil {
		return File{}, err
	}
	for i := range file.Sums {
		if file.Sums[i], err = decoder.readSumsColumn(int(rowCount)); err != nil {
			return File{}, err
		}
	}
	if decoder.position != decoder.end {
		return File{}, errCorrupt
	}
	return file, nil
}

func (decoder *byteDecoder) readAscendingColumn(count int) ([]int64, error) {
	first, err := decoder.readUvarint()
	if err != nil {
		return nil, err
	}
	values := make([]int64, count)
	values[0] = int64(first)
	if count == 1 {
		return values, nil
	}
	minStep, err := decoder.readUvarint()
	if err != nil {
		return nil, err
	}
	steps, err := decoder.readResiduals(count - 1)
	if err != nil {
		return nil, err
	}
	for i, step := range steps {
		values[i+1] = values[i] + int64(step+minStep)
	}
	return values, nil
}

func (decoder *byteDecoder) readSumsColumn(count int) ([]int64, error) {
	minimum, bytesRead := binary.Varint(decoder.data[decoder.position:decoder.end])
	if bytesRead <= 0 {
		return nil, errCorrupt
	}
	decoder.position += bytesRead
	residuals, err := decoder.readResiduals(count)
	if err != nil {
		return nil, err
	}
	values := make([]int64, count)
	for i, residual := range residuals {
		values[i] = int64(uint64(minimum) + residual)
	}
	return values, nil
}

func (decoder *byteDecoder) readResiduals(count int) ([]uint64, error) {
	divisor, err := decoder.readUvarint()
	if err != nil {
		return nil, err
	}
	residuals := make([]uint64, count)
	for start := 0; start < count; start += blockSize {
		block := residuals[start:min(start+blockSize, count)]
		head, err := decoder.readByte()
		if err != nil {
			return nil, err
		}
		width := int(head & blockWidthMask)
		if width > 64 {
			return nil, errCorrupt
		}
		patchCount := 0
		if head&blockPatchedFlag != 0 {
			countByte, err := decoder.readByte()
			if err != nil {
				return nil, err
			}
			patchCount = int(countByte)
		}
		runBytes := (len(block)*width + 7) / 8
		if decoder.end-decoder.position < runBytes {
			return nil, errCorrupt
		}
		unpackBits(decoder.data[decoder.position:], block, width)
		decoder.position += runBytes
		for range patchCount {
			position, err := decoder.readByte()
			if err != nil || int(position) >= len(block) {
				return nil, errCorrupt
			}
			high, err := decoder.readUvarint()
			if err != nil {
				return nil, err
			}
			block[position] |= high << width
		}
	}
	if divisor > 1 {
		for i := range residuals {
			residuals[i] *= divisor
		}
	}
	return residuals, nil
}

// unpackBits reads len(block) residuals of width bits from the start of run: one unaligned 8-byte
// load, a shift and a mask each, with no dependency on the residual before it. run must carry
// decodePadding bytes past the packed bits.
func unpackBits(run []byte, block []uint64, width int) {
	if width == 0 {
		clear(block)
		return
	}
	mask := lowBitsMask(width)
	bitPosition := 0
	for i := range block {
		byteIndex, shift := bitPosition>>3, bitPosition&7
		value := binary.LittleEndian.Uint64(run[byteIndex:]) >> shift
		// Above 57 bits, a value that does not start on a byte boundary reaches a ninth byte.
		if shift+width > 64 {
			value |= uint64(run[byteIndex+8]) << (64 - shift)
		}
		block[i] = value & mask
		bitPosition += width
	}
}

func (decoder *byteDecoder) readByte() (byte, error) {
	if decoder.position >= decoder.end {
		return 0, errCorrupt
	}
	decoder.position++
	return decoder.data[decoder.position-1], nil
}

func (decoder *byteDecoder) readUvarint() (uint64, error) {
	value, bytesRead := binary.Uvarint(decoder.data[decoder.position:decoder.end])
	if bytesRead <= 0 {
		return 0, errCorrupt
	}
	decoder.position += bytesRead
	return value, nil
}

func (decoder *byteDecoder) readVarint() (int64, error) {
	value, bytesRead := binary.Varint(decoder.data[decoder.position:decoder.end])
	if bytesRead <= 0 {
		return 0, errCorrupt
	}
	decoder.position += bytesRead
	return value, nil
}

// FileHash is the CRC-32C of a file after its snapshot: a file rewritten at a later snapshot
// with the same rows keeps its hash. content is a file appendFile wrote.
func FileHash(content []byte) uint32 {
	_, snapshotBytes := binary.Uvarint(content[1:])
	return crc32.Checksum(content[1+snapshotBytes:], castagnoliTable)
}

// ─────────────────────────────────────────────────────────────────────────────
// Log entry (_log.<shape>, appended by every write that changes a frame's values).
// It is row-wise and short-lived, truncated by every run, so it uses Go's varints:
//
//	uvarint   entry length (the bytes after this field)
//	uvarint   newVersion      UpdatedVersion of the write that replaced the record
//	uvarint   createdVersion  of the record replaced
//	uvarint   sk length, then the record's sk
//	u8        0: the old version counted nowhere (Status 0)
//	          1: it counted, and its uvarint Keys, uvarint Rows and varint Sums follow
//	          2: a cancel marker: the write of this sk at newVersion lost its condition
// ─────────────────────────────────────────────────────────────────────────────

const (
	entryUncounted byte = 0
	entryCounted   byte = 1
	entryCancel    byte = 2
)

// LogEntry is one entry of a frame's log: the values the record SK (an incarnation of it, by
// CreatedVersion) held right below NewVersion, the version of the write that replaced it. A cancel
// marker (IsCancel) voids the entries of the same SK and NewVersion: their write lost its condition.
type LogEntry struct {
	NewVersion     int64
	CreatedVersion int64
	SK             string
	IsCancel       bool
	OldValues      *Values // nil: the old version counted in no file
}

func appendLogEntry(out []byte, entry LogEntry, keyCount int) []byte {
	body := binary.AppendUvarint(nil, uint64(entry.NewVersion))
	body = binary.AppendUvarint(body, uint64(entry.CreatedVersion))
	body = binary.AppendUvarint(body, uint64(len(entry.SK)))
	body = append(body, entry.SK...)
	switch {
	case entry.IsCancel:
		body = append(body, entryCancel)
	case entry.OldValues == nil:
		body = append(body, entryUncounted)
	default:
		body = append(body, entryCounted)
		for _, key := range entry.OldValues.Keys[:keyCount] {
			body = binary.AppendUvarint(body, uint64(key))
		}
		body = binary.AppendUvarint(body, uint64(entry.OldValues.Row))
		for _, sum := range entry.OldValues.Sums {
			body = binary.AppendVarint(body, sum)
		}
	}
	out = binary.AppendUvarint(out, uint64(len(body)))
	return append(out, body...)
}

// decodeLog reads every entry of a log.
func decodeLog(content []byte, keyCount, sumsCount int) ([]LogEntry, error) {
	decoder := &byteDecoder{data: content, end: len(content)}
	var entries []LogEntry
	for decoder.position < decoder.end {
		entryLength, err := decoder.readUvarint()
		if err != nil || entryLength > uint64(decoder.end-decoder.position) {
			return nil, errCorrupt
		}
		entryEnd := decoder.position + int(entryLength)
		entry, err := decoder.readLogEntry(keyCount, sumsCount)
		if err != nil || decoder.position != entryEnd {
			return nil, errCorrupt
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (decoder *byteDecoder) readLogEntry(keyCount, sumsCount int) (LogEntry, error) {
	var entry LogEntry
	newVersion, err := decoder.readUvarint()
	if err != nil {
		return entry, err
	}
	createdVersion, err := decoder.readUvarint()
	if err != nil {
		return entry, err
	}
	skLength, err := decoder.readUvarint()
	if err != nil || skLength > uint64(decoder.end-decoder.position) {
		return entry, errCorrupt
	}
	entry.NewVersion, entry.CreatedVersion = int64(newVersion), int64(createdVersion)
	entry.SK = string(decoder.data[decoder.position : decoder.position+int(skLength)])
	decoder.position += int(skLength)
	kind, err := decoder.readByte()
	if err != nil {
		return entry, err
	}
	switch kind {
	case entryCancel:
		entry.IsCancel = true
	case entryCounted:
		entry.OldValues = &Values{Sums: make([]int64, sumsCount)}
		for i := range keyCount {
			key, err := decoder.readUvarint()
			if err != nil {
				return entry, err
			}
			entry.OldValues.Keys[i] = int64(key)
		}
		row, err := decoder.readUvarint()
		if err != nil {
			return entry, err
		}
		entry.OldValues.Row = int64(row)
		for i := range entry.OldValues.Sums {
			if entry.OldValues.Sums[i], err = decoder.readVarint(); err != nil {
				return entry, err
			}
		}
	case entryUncounted:
	default:
		return entry, errCorrupt
	}
	return entry, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Day index (_idx of a 2–3-key frame's folder): the keys of its files, so a
// reader lists a day in one GET, and their hashes, so a rebuild rewrites only the
// files that differ:
//
//	uvarint      file count n
//	column       the second frame key: an ascending column (as Rows; on a 3-key frame steps may be 0)
//	column       the third frame key (3-key frames only): a Sums column
//	u32 LE × n   FileHash of each file, in the same order: hashes don't compress
// ─────────────────────────────────────────────────────────────────────────────

// IndexEntry is one file of a day folder: its keys after the first, and its hash.
type IndexEntry struct {
	Keys [MaxKeys - 1]int64
	Hash uint32
}

// appendIndex encodes entries, sorted by keys; keyCount is the frame's Keys count.
func appendIndex(out []byte, entries []IndexEntry, keyCount int) []byte {
	out = binary.AppendUvarint(out, uint64(len(entries)))
	if len(entries) == 0 {
		return out
	}
	for keyPosition := range keyCount - 1 {
		keyColumn := make([]int64, len(entries))
		for i, entry := range entries {
			keyColumn[i] = entry.Keys[keyPosition]
		}
		if keyPosition == 0 {
			out = appendAscendingColumn(out, keyColumn, codecOptions{})
		} else {
			out = appendSumsColumn(out, keyColumn, codecOptions{})
		}
	}
	for _, entry := range entries {
		out = binary.LittleEndian.AppendUint32(out, entry.Hash)
	}
	return out
}

func DecodeIndex(content []byte, keyCount int) ([]IndexEntry, error) {
	decoder := &byteDecoder{data: append(slices.Clip(content), make([]byte, decodePadding)...), end: len(content)}
	entryCount, err := decoder.readUvarint()
	// Each entry takes at least its 4-byte hash.
	if err != nil || entryCount > uint64(decoder.end)/4 {
		return nil, errCorrupt
	}
	entries := make([]IndexEntry, entryCount)
	if entryCount == 0 {
		return entries, nil
	}
	for keyPosition := range keyCount - 1 {
		var keyColumn []int64
		if keyPosition == 0 {
			keyColumn, err = decoder.readAscendingColumn(int(entryCount))
		} else {
			keyColumn, err = decoder.readSumsColumn(int(entryCount))
		}
		if err != nil {
			return nil, err
		}
		for i := range entries {
			entries[i].Keys[keyPosition] = keyColumn[i]
		}
	}
	if decoder.end-decoder.position != 4*len(entries) {
		return nil, errCorrupt
	}
	for i := range entries {
		entries[i].Hash = binary.LittleEndian.Uint32(decoder.data[decoder.position:])
		decoder.position += 4
	}
	return entries, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Day index extension (_ixt): what express compactions append to a day folder
// instead of rewriting its _idx (index.go). One block per append:
//
//	uvarint   block length (the bytes after this field)
//	body      an _idx body: the files one compaction wrote there, with their hashes
// ─────────────────────────────────────────────────────────────────────────────

func appendIndexExtensionBlock(out []byte, entries []IndexEntry, keyCount int) []byte {
	body := appendIndex(nil, entries, keyCount)
	out = binary.AppendUvarint(out, uint64(len(body)))
	return append(out, body...)
}

// DecodeIndexExtension reads the entries of every block, in the order they were appended.
func DecodeIndexExtension(content []byte, keyCount int) ([]IndexEntry, error) {
	var entries []IndexEntry
	for position := 0; position < len(content); {
		blockLength, lengthBytes := binary.Uvarint(content[position:])
		if lengthBytes <= 0 || blockLength > uint64(len(content)-position-lengthBytes) {
			return nil, errCorrupt
		}
		position += lengthBytes
		blockEntries, err := DecodeIndex(content[position:position+int(blockLength)], keyCount)
		if err != nil {
			return nil, err
		}
		entries = append(entries, blockEntries...)
		position += int(blockLength)
	}
	return entries, nil
}

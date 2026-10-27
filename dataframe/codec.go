package dataframe

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"slices"
)

// ─────────────────────────────────────────────────────────────────────────────
// DataFrame file codec: raw bytes, columnar.
//
//	u8        format version, bit 7 set when the file carries the zero sums mask
//	u8        slot: the file's slot in the versions file (versions.go), 0 = none yet
//	uvarint   snapshot: the SUnixTime the file holds its frame at
//	uvarint   row count n (no columns follow when n = 0)
//	rows      the Rows columns, nested (below)
//	[u8       zero sums mask: bit s set when summed column s holds a non-zero value.
//	          Only on a frame declared with UseZeroSumsMask (at most 8 summed columns)]
//	column    each summed column (with the mask: each one whose bit is set): varint min,
//	          residuals of the n values
//
// The mask makes a summed column empty in the whole file (no sub-units sold, no debt
// left) cost nothing; without it such a column is a constant column, 2 bytes.
//
// The rows are sorted by their Rows values, so with several Rows columns (department,
// program, employee) they nest like folders: a department once, the programs under
// it, the employees under each program. The file stores that tree level by level
// instead of repeating the outer values on every row:
//
//	[uvarint  g1, the distinct values of Rows[0]: only with 2 or 3 Rows columns]
//	level 1   the g1 values of Rows[0], an ascending column: uvarint first, steps
//	level k   per node of level k − 1 (each value of the columns before):
//	            sums column  how many values of Rows[k−1] it holds (≥ 1)
//	            sums column  the first of them
//	            steps        the steps between its next ones, every node's in a row
//	steps     := uvarint minStep, residuals of the steps less minStep (none when empty)
//
// The last level holds the n rows. With one Rows column the file is level 1 alone.
// Packing the IDs into one integer instead costs a patch of 4–6 bytes on every
// department and program boundary, where the packed step jumps by 2^31 or more.
//
// Each column is turned into small non-negative residuals by a transform its role
// fixes, so no transform byte is stored: ascending values are delta-coded and their
// smallest step subtracted, and each Sums column (and the counts and first values
// of the nested Rows) has its minimum subtracted (frame of reference), which leaves
// every residual >= 0 even when a value is negative. The residuals are then divided
// by the largest of 1000, 100, 10, 8, 5 and 2 that divides them all (quantities
// stored as multiples of 1000 lose 10 bits each) and written in blocks of 128:
//
//	residuals := uvarint divisor, block*   (divisor 0: every residual is 0, no blocks
//	                                          follow: a constant column costs its min
//	                                          and this one byte, whatever its length)
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
	formatVersion = 4
	// zeroSumsMaskFlag marks, in the format byte, a file whose summed columns follow a zero sums mask.
	zeroSumsMaskFlag = 0x80
	// MaxMaskedSums: the zero sums mask is one byte.
	MaxMaskedSums = 8
	// constantColumnDivisor, in the place of the divisor, says every residual of the column is 0.
	constantColumnDivisor = 0
	blockSize             = 128
	blockPatchedFlag      = 0x80
	blockWidthMask        = 0x7F
	// decodePadding is how far past a packed run the bit unpack loads: a value is read with
	// one 8-byte load (a ninth byte for widths above 57), whatever its offset in the run.
	decodePadding = 8
)

var errCorrupt = errors.New("dataframe: frame file is truncated or corrupt")

// File is a decoded frame file: the frame at Snapshot. Slot is the file's slot in the versions file,
// kept by every rewrite (0 = no run created the file yet). Rows holds one column per Rows column of
// the frame, all of one length: row i is (Rows[0][i], Rows[1][i]...), strictly ascending in that
// order and >= 0. Each column of Sums is parallel to them. UsesZeroSumsMask writes the zero sums
// mask (the frame's UseZeroSumsMask); a decoded file says whether it had one.
type File struct {
	Snapshot         int64
	Slot             uint8
	Rows             [][]int64
	Sums             [][]int64
	UsesZeroSumsMask bool
}

// rowCount is the number of rows of the file; 0 for the zero File, which a missing file reads as.
func (file File) rowCount() int {
	if len(file.Rows) == 0 {
		return 0
	}
	return len(file.Rows[0])
}

// codecOptions turns off a compression step. Files are always written with the zero value
// (every step on); the others exist for the size comparisons in codec_test.go.
type codecOptions struct {
	skipsDivisor bool
	skipsPatches bool
	// scratch holds the residuals of one column at a time: appendFile reuses it for every column of
	// the file instead of allocating each one's.
	scratch *[]uint64
}

// residualsBuffer is count residuals to fill, the scratch's when there is one.
func (options codecOptions) residualsBuffer(count int) []uint64 {
	if options.scratch == nil {
		return make([]uint64, count)
	}
	if cap(*options.scratch) < count {
		*options.scratch = make([]uint64, count)
	}
	return (*options.scratch)[:count]
}

// EncodeFile encodes a file with every compression step on.
func EncodeFile(file File) []byte { return appendFile(nil, file, codecOptions{}) }

// appendFile encodes file onto out.
func appendFile(out []byte, file File, options codecOptions) []byte {
	formatByte := byte(formatVersion)
	if file.UsesZeroSumsMask {
		formatByte |= zeroSumsMaskFlag
	}
	out = append(out, formatByte, file.Slot)
	if options.scratch == nil {
		options.scratch = new([]uint64)
	}
	out = binary.AppendUvarint(out, uint64(file.Snapshot))
	out = binary.AppendUvarint(out, uint64(file.rowCount()))
	if file.rowCount() == 0 {
		return out
	}
	out = appendRowColumns(out, file.Rows, options)
	var zeroSumsMask byte
	if file.UsesZeroSumsMask {
		zeroSumsMask = zeroSumsMaskOf(file.Sums)
		out = append(out, zeroSumsMask)
	}
	for s, sumColumn := range file.Sums {
		if !file.UsesZeroSumsMask || zeroSumsMask&(1<<s) != 0 {
			out = appendSumsColumn(out, sumColumn, options)
		}
	}
	return out
}

// zeroSumsMaskOf sets bit s for each summed column holding a non-zero value.
func zeroSumsMaskOf(sumColumns [][]int64) byte {
	var mask byte
	for s, sumColumn := range sumColumns {
		if slices.ContainsFunc(sumColumn, func(sum int64) bool { return sum != 0 }) {
			mask |= 1 << s
		}
	}
	return mask
}

// appendRowColumns writes the Rows columns as the tree of their values (the file layout above): the
// distinct values of each column, under the node of the columns before it.
func appendRowColumns(out []byte, rowColumns [][]int64, options codecOptions) []byte {
	levelCount := len(rowColumns)
	// valuesByLevel[k]: the nodes of level k, one per distinct prefix (Rows[0..k]), in row order.
	// childCountsByLevel[k] (k ≥ 1): how many nodes of level k each node of level k − 1 holds.
	rowCount := len(rowColumns[0])
	valuesByLevel := make([][]int64, levelCount)
	childCountsByLevel := make([][]int64, levelCount)
	for level := range levelCount {
		// A level holds at most a node per row: sized once, never grown.
		valuesByLevel[level] = make([]int64, 0, rowCount)
		if level > 0 {
			childCountsByLevel[level] = make([]int64, 0, rowCount)
		}
	}
	for row := range rowColumns[0] {
		// The first level whose prefix changes on this row: every level from it starts a node.
		changedLevel := 0
		if row > 0 {
			for changedLevel < levelCount-1 && rowColumns[changedLevel][row] == rowColumns[changedLevel][row-1] {
				changedLevel++
			}
		}
		for level := changedLevel; level < levelCount; level++ {
			valuesByLevel[level] = append(valuesByLevel[level], rowColumns[level][row])
			if level == 0 {
				continue
			}
			if level > changedLevel {
				childCountsByLevel[level] = append(childCountsByLevel[level], 1) // the first child of a new node
			} else {
				childCountsByLevel[level][len(childCountsByLevel[level])-1]++
			}
		}
	}

	if levelCount > 1 {
		out = binary.AppendUvarint(out, uint64(len(valuesByLevel[0])))
	}
	out = appendAscendingColumn(out, valuesByLevel[0], options)
	for level := 1; level < levelCount; level++ {
		childCounts, values := childCountsByLevel[level], valuesByLevel[level]
		firstValues := make([]int64, len(childCounts))
		steps := make([]uint64, 0, len(values)-len(childCounts))
		start := 0
		for node, childCount := range childCounts {
			firstValues[node] = values[start]
			for i := start + 1; i < start+int(childCount); i++ {
				steps = append(steps, uint64(values[i]-values[i-1]))
			}
			start += int(childCount)
		}
		out = appendSumsColumn(out, childCounts, options)
		out = appendSumsColumn(out, firstValues, options)
		out = appendSteps(out, steps, options)
	}
	return out
}

// appendAscendingColumn writes a non-decreasing column of values >= 0: its first value, then the
// steps between neighbours. Consecutive IDs become a run of zeros.
func appendAscendingColumn(out []byte, values []int64, options codecOptions) []byte {
	out = binary.AppendUvarint(out, uint64(values[0]))
	steps := options.residualsBuffer(len(values) - 1)
	for i := 1; i < len(values); i++ {
		steps[i-1] = uint64(values[i] - values[i-1])
	}
	return appendSteps(out, steps, options)
}

// appendSteps writes the smallest step, then each step less it; nothing when there are no steps.
func appendSteps(out []byte, steps []uint64, options codecOptions) []byte {
	if len(steps) == 0 {
		return out
	}
	minStep := slices.Min(steps)
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
	residuals := options.residualsBuffer(len(values))
	for i, value := range values {
		residuals[i] = uint64(value) - uint64(minimum)
	}
	return appendResiduals(out, residuals, options)
}

// appendResiduals writes the residuals' common divisor, then the residuals divided by it in
// blocks. It divides residuals in place.
func appendResiduals(out []byte, residuals []uint64, options codecOptions) []byte {
	if !slices.ContainsFunc(residuals, func(residual uint64) bool { return residual != 0 }) {
		return binary.AppendUvarint(out, constantColumnDivisor)
	}
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
	width, smallestCost := widest, blockCost(len(block), &countByBitLength, widest, widest)
	if !options.skipsPatches {
		for candidate := 0; candidate < widest; candidate++ {
			if cost := blockCost(len(block), &countByBitLength, candidate, widest); cost < smallestCost {
				width, smallestCost = candidate, cost
			}
		}
	}

	patchCount := 0
	for bitLength := width + 1; bitLength <= widest; bitLength++ {
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
// each bit length, so every width is scored without another pass over the block; none is longer
// than widest.
func blockCost(count int, countByBitLength *[65]int, width, widest int) int {
	cost := 1 + (count*width+7)/8
	isPatched := false
	for bitLength := width + 1; bitLength <= widest; bitLength++ {
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

// DecodeFile reads a file written by appendFile; rowColumnCount and sumsCount come from the frame's
// schema.
func DecodeFile(content []byte, rowColumnCount, sumsCount int) (File, error) {
	decoder := &byteDecoder{data: append(slices.Clip(content), make([]byte, decodePadding)...), end: len(content)}
	formatByte, err := decoder.readByte()
	usesZeroSumsMask := formatByte&zeroSumsMaskFlag != 0
	if err != nil || formatByte&^zeroSumsMaskFlag != formatVersion || (usesZeroSumsMask && sumsCount > MaxMaskedSums) {
		return File{}, errCorrupt
	}
	slot, err := decoder.readByte()
	if err != nil {
		return File{}, err
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
	file := File{Snapshot: int64(snapshot), Slot: slot, Rows: make([][]int64, rowColumnCount), Sums: make([][]int64, sumsCount), UsesZeroSumsMask: usesZeroSumsMask}
	if rowCount == 0 {
		return file, nil
	}
	if file.Rows, err = decoder.readRowColumns(int(rowCount), rowColumnCount); err != nil {
		return File{}, err
	}
	var zeroSumsMask byte
	if usesZeroSumsMask {
		if zeroSumsMask, err = decoder.readByte(); err != nil || zeroSumsMask>>sumsCount != 0 {
			return File{}, errCorrupt
		}
	}
	for s := range file.Sums {
		if usesZeroSumsMask && zeroSumsMask&(1<<s) == 0 {
			file.Sums[s] = make([]int64, rowCount)
			continue
		}
		if file.Sums[s], err = decoder.readSumsColumn(int(rowCount)); err != nil {
			return File{}, err
		}
	}
	if decoder.position != decoder.end {
		return File{}, errCorrupt
	}
	return file, nil
}

// readRowColumns reads the tree of the Rows values written by appendRowColumns back into rowCount
// rows of rowColumnCount columns.
func (decoder *byteDecoder) readRowColumns(rowCount, rowColumnCount int) ([][]int64, error) {
	nodeCount := rowCount
	if rowColumnCount > 1 {
		count, err := decoder.readUvarint()
		if err != nil || count == 0 || count > uint64(rowCount) {
			return nil, errCorrupt
		}
		nodeCount = int(count)
	}
	values, err := decoder.readAscendingColumn(nodeCount)
	if err != nil {
		return nil, err
	}
	rowColumns := make([][]int64, rowColumnCount)
	rowColumns[0] = values
	// childCountsByLevel[k]: how many nodes of level k each node of level k − 1 holds.
	childCountsByLevel := make([][]int64, rowColumnCount)
	for level := 1; level < rowColumnCount; level++ {
		childCounts, err := decoder.readSumsColumn(nodeCount)
		if err != nil {
			return nil, err
		}
		firstValues, err := decoder.readSumsColumn(nodeCount)
		if err != nil {
			return nil, err
		}
		// Every node holds 1 or more children and the last level holds the rows: checked before the
		// counts size anything.
		childTotal := 0
		for _, childCount := range childCounts {
			if childCount < 1 || childCount > int64(rowCount-childTotal) {
				return nil, errCorrupt
			}
			childTotal += int(childCount)
		}
		if level == rowColumnCount-1 && childTotal != rowCount {
			return nil, errCorrupt
		}
		steps, err := decoder.readSteps(childTotal - nodeCount)
		if err != nil {
			return nil, err
		}
		levelValues := make([]int64, 0, childTotal)
		for node, childCount := range childCounts {
			levelValues = append(levelValues, firstValues[node])
			for _, step := range steps[:childCount-1] {
				levelValues = append(levelValues, levelValues[len(levelValues)-1]+int64(step))
			}
			steps = steps[childCount-1:]
		}
		rowColumns[level], childCountsByLevel[level], nodeCount = levelValues, childCounts, childTotal
	}
	// Expand the outer levels to one value per row: each node's value repeated over the rows under
	// it, its span, the sum of its children's spans (a row of the last level spans itself).
	var childSpans []int64
	for level := rowColumnCount - 2; level >= 0; level-- {
		spans := make([]int64, len(rowColumns[level]))
		child := 0
		for node, childCount := range childCountsByLevel[level+1] {
			if childSpans == nil {
				spans[node] = childCount
				continue
			}
			for _, childSpan := range childSpans[child : child+int(childCount)] {
				spans[node] += childSpan
			}
			child += int(childCount)
		}
		expanded := make([]int64, 0, rowCount)
		for node, value := range rowColumns[level] {
			for range spans[node] {
				expanded = append(expanded, value)
			}
		}
		rowColumns[level], childSpans = expanded, spans
	}
	return rowColumns, nil
}

func (decoder *byteDecoder) readAscendingColumn(count int) ([]int64, error) {
	first, err := decoder.readUvarint()
	if err != nil {
		return nil, err
	}
	steps, err := decoder.readSteps(count - 1)
	if err != nil {
		return nil, err
	}
	values := make([]int64, count)
	values[0] = int64(first)
	for i, step := range steps {
		values[i+1] = values[i] + int64(step)
	}
	return values, nil
}

// readSteps reads count steps written by appendSteps.
func (decoder *byteDecoder) readSteps(count int) ([]uint64, error) {
	if count == 0 {
		return nil, nil
	}
	minStep, err := decoder.readUvarint()
	if err != nil {
		return nil, err
	}
	steps, err := decoder.readResiduals(count)
	if err != nil {
		return nil, err
	}
	for i := range steps {
		steps[i] += minStep
	}
	return steps, nil
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
	if divisor == constantColumnDivisor {
		return residuals, nil
	}
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

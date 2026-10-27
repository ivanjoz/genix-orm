package dataframe

import (
	"math/bits"
	"slices"
	"unsafe"
)

// ─────────────────────────────────────────────────────────────────────────────
// lineSums: the sums of a rebuild, every file of a partition at once. A rebuild adds
// the lines of every record, hundreds of thousands, and most name a row no other line
// does (a product on a day, a lot in a week). Keyed per file by a string and per row by
// a map with a []int64 per row, that was a string build, two map lookups and an
// allocation per line, then a sort of each file's rows; a hash table per group still
// pays a random access per line for little merging. Here a line is copied into an
// arena of fixed-size chunks (sequential writes, no copy on growth), its Keys and Rows
// (its group) are packed into a uint64 when their ranges fit (ids and days do), radix
// sorted once, and runs of one group are summed into rows that come out ascending by
// (Keys, Rows), file after file.
// ─────────────────────────────────────────────────────────────────────────────

const (
	chunkLinesShift = 14
	chunkLines      = 1 << chunkLinesShift
	// prefetchDistance is how many sorted lines ahead the summing pass asks for: far enough for a
	// miss to land before its turn.
	prefetchDistance = 16
)

type lineSums struct {
	frame      *Frame
	groupWidth int // len(Keys) + len(Rows)
	lineLength int
	chunks     [][]int64 // the lines added, lineLength values each, chunkLines per chunk
	lineCount  int
	// minimums and maximums bound each group column, for packing.
	minimums, maximums []int64
}

func newLineSums(frame *Frame) *lineSums {
	groupWidth := len(frame.Keys) + len(frame.Rows)
	return &lineSums{frame: frame, groupWidth: groupWidth, lineLength: frame.lineLength(),
		minimums: make([]int64, groupWidth), maximums: make([]int64, groupWidth)}
}

// add copies a line in.
func (sums *lineSums) add(line Line) {
	offset := (sums.lineCount & (chunkLines - 1)) * sums.lineLength
	if offset == 0 {
		sums.chunks = append(sums.chunks, make([]int64, chunkLines*sums.lineLength))
	}
	copy(sums.chunks[len(sums.chunks)-1][offset:offset+sums.lineLength], line)
	if sums.lineCount == 0 {
		copy(sums.minimums, line[:sums.groupWidth])
		copy(sums.maximums, line[:sums.groupWidth])
	}
	for c, value := range line[:sums.groupWidth] {
		sums.minimums[c], sums.maximums[c] = min(sums.minimums[c], value), max(sums.maximums[c], value)
	}
	sums.lineCount++
}

// lineAt is the i-th line added.
func (sums *lineSums) lineAt(i int) []int64 {
	offset := (i & (chunkLines - 1)) * sums.lineLength
	return sums.chunks[i>>chunkLinesShift][offset : offset+sums.lineLength : offset+sums.lineLength]
}

// sortedLines returns the lines' indexes ascending by group and, when the groups fit a uint64, each
// position's packed group (nil otherwise: they were compared column by column) and the columns'
// shifts in it.
func (sums *lineSums) sortedLines() ([]int32, []uint64, []uint) {
	order := make([]int32, sums.lineCount)
	for i := range order {
		order[i] = int32(i)
	}
	width := sums.groupWidth
	// Each column packs as its value less the column's minimum, in the bits its range needs, the
	// first column highest.
	shifts, totalBits := make([]uint, width), uint(0)
	for c := width - 1; c >= 0; c-- {
		shifts[c] = totalBits
		totalBits += uint(bits.Len64(uint64(sums.maximums[c] - sums.minimums[c])))
	}
	if totalBits > 64 {
		slices.SortStableFunc(order, func(a, b int32) int {
			return slices.Compare(sums.lineAt(int(a))[:width], sums.lineAt(int(b))[:width])
		})
		return order, nil, shifts
	}
	packed := make([]uint64, sums.lineCount)
	for i := range packed {
		var key uint64
		for c, value := range sums.lineAt(i)[:width] {
			key |= uint64(value-sums.minimums[c]) << shifts[c]
		}
		packed[i] = key
	}
	radixSort(packed, order, totalBits)
	return order, packed, shifts
}

// radixSort sorts values by their key ascending (stable): least significant byte first, a pass per
// byte of bitLength, a byte every key shares skipped. keys is sorted alongside.
func radixSort(keys []uint64, values []int32, bitLength uint) {
	if len(keys) < 2 {
		return
	}
	sortedKeys, sortedValues := keys, values
	scratchKeys, scratchValues := make([]uint64, len(keys)), make([]int32, len(values))
	for shift := uint(0); shift < bitLength; shift += 8 {
		var counts [256]int
		for _, key := range keys {
			counts[byte(key>>shift)]++
		}
		if counts[byte(keys[0]>>shift)] == len(keys) {
			continue
		}
		position := 0
		for digit, count := range counts {
			counts[digit] = position
			position += count
		}
		for i, key := range keys {
			digit := byte(key >> shift)
			scratchKeys[counts[digit]], scratchValues[counts[digit]] = key, values[i]
			counts[digit]++
		}
		keys, scratchKeys = scratchKeys, keys
		values, scratchValues = scratchValues, values
	}
	// After an odd number of passes the sorted run is in the scratch buffers.
	if &values[0] != &sortedValues[0] {
		copy(sortedValues, values)
		copy(sortedKeys, keys)
	}
}

// files are the sums as the files of a generation at snapshot, with their store keys, ascending by
// Keys: every file a line went to, its rows ascending, a row whose sums are all 0 dropped (a file can
// be left without rows). Every file's columns share one backing array.
func (sums *lineSums) files(generation string, partition int64, snapshot int64) ([]string, []File) {
	frame, width := sums.frame, sums.groupWidth
	keyCount, rowColumnCount, sumsCount := len(frame.Keys), len(frame.Rows), frame.SumsCount
	order, packed, shifts := sums.sortedLines()
	// isSameGroup and isSameFile compare two positions of order.
	isSameGroup := func(a, b int) bool {
		if packed != nil {
			return packed[a] == packed[b]
		}
		return slices.Equal(sums.lineAt(int(order[a]))[:width], sums.lineAt(int(order[b]))[:width])
	}
	fileShift := shifts[keyCount-1]
	isSameFile := func(a, b int) bool {
		if packed != nil {
			return packed[a]>>fileShift == packed[b]>>fileShift
		}
		return slices.Equal(sums.lineAt(int(order[a]))[:keyCount], sums.lineAt(int(order[b]))[:keyCount])
	}

	groupCount := 0
	for i := range order {
		if i == 0 || !isSameGroup(i, i-1) {
			groupCount++
		}
	}
	// Every file's columns, at most a row per group. A file's rows are summed into fileValues and
	// fileSums row by row (reused from file to file), then laid out as its columns; a row whose sums
	// are all 0 is dropped, but its file is still written.
	backing := make([]int64, groupCount*(rowColumnCount+sumsCount))
	var fileKeys []string
	var files []File
	var fileValues, fileSums []int64
	groupSums := make([]int64, sumsCount)
	for fileStart := 0; fileStart < len(order); {
		fileValues, fileSums = fileValues[:0], fileSums[:0]
		start := fileStart
		for start < len(order) && (start == fileStart || isSameFile(start, fileStart)) {
			clear(groupSums)
			end := start
			for ; end < len(order) && (end == start || isSameGroup(end, start)); end++ {
				if ahead := end + prefetchDistance; ahead < len(order) {
					prefetch(unsafe.Pointer(&sums.lineAt(int(order[ahead]))[width]))
				}
				for s, value := range sums.lineAt(int(order[end]))[width:] {
					groupSums[s] += value
				}
			}
			if frame.CountsRecords {
				groupSums[sumsCount-1] = int64(end - start)
			}
			if slices.ContainsFunc(groupSums, func(sum int64) bool { return sum != 0 }) {
				fileValues = append(fileValues, sums.lineAt(int(order[start]))[keyCount:width]...)
				fileSums = append(fileSums, groupSums...)
			}
			start = end
		}

		rowCount := len(fileSums) / sumsCount
		file := File{Snapshot: snapshot, Rows: make([][]int64, rowColumnCount), Sums: make([][]int64, sumsCount), UsesZeroSumsMask: frame.UsesZeroSumsMask}
		if rowCount > 0 {
			for c := range file.Rows {
				column := backing[:rowCount:rowCount]
				for r := range column {
					column[r] = fileValues[r*rowColumnCount+c]
				}
				file.Rows[c], backing = column, backing[rowCount:]
			}
			for s := range file.Sums {
				column := backing[:rowCount:rowCount]
				for r := range column {
					column[r] = fileSums[r*sumsCount+s]
				}
				file.Sums[s], backing = column, backing[rowCount:]
			}
		}
		fileKeys = append(fileKeys, frame.fileKeyOf(generation, partition, sums.lineAt(int(order[fileStart]))[:keyCount]))
		files = append(files, file)
		fileStart = start
	}
	return fileKeys, files
}

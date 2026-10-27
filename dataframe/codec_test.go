package dataframe

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// everyFrameCodecOptions is the codec with each compression step on and off: every one must read
// back what it wrote.
var everyFrameCodecOptions = []codecOptions{
	{}, {skipsDivisor: true}, {skipsPatches: true}, {skipsDivisor: true, skipsPatches: true},
}

// randomFrameFile builds a file of rowCount rows of rowColumnCount Rows columns, whose sums have at
// most sumBits significant bits (64: the whole int64 range, negatives included). Each row moves one
// Rows column, by up to maxStep, and restarts the later ones at a random value: with several columns
// the rows nest in groups of random sizes, many of a single row.
func randomFrameFile(rng *rand.Rand, rowCount, rowColumnCount, sumsCount, sumBits int, maxStep int64) File {
	file := File{Snapshot: rng.Int64N(1 << 31), Slot: uint8(rng.IntN(256)), Rows: make([][]int64, rowColumnCount), Sums: make([][]int64, sumsCount)}
	current := make([]int64, rowColumnCount)
	for c := range current {
		current[c] = rng.Int64N(1 << 20)
	}
	for row := range rowCount {
		if row > 0 {
			// The innermost column moves most often, as employees under a program.
			movedColumn := rowColumnCount - 1
			for movedColumn > 0 && rng.IntN(4) == 0 {
				movedColumn--
			}
			current[movedColumn] += 1 + rng.Int64N(maxStep)
			for c := movedColumn + 1; c < rowColumnCount; c++ {
				current[c] = rng.Int64N(1 << 20)
			}
		}
		for c := range current {
			file.Rows[c] = append(file.Rows[c], current[c])
		}
	}
	for column := range file.Sums {
		file.Sums[column] = make([]int64, rowCount)
		for row := range file.Sums[column] {
			file.Sums[column][row] = int64(rng.Uint64() & lowBitsMask(sumBits))
		}
	}
	return file
}

// cloneFrameFile copies the columns: the encoder transforms its own copies, but a test compares
// against the original after encoding.
func cloneFrameFile(file File) File {
	clone := File{Snapshot: file.Snapshot, Slot: file.Slot, Rows: make([][]int64, len(file.Rows)), Sums: make([][]int64, len(file.Sums)), UsesZeroSumsMask: file.UsesZeroSumsMask}
	for i := range file.Rows {
		clone.Rows[i] = slices.Clone(file.Rows[i])
	}
	for i := range file.Sums {
		clone.Sums[i] = slices.Clone(file.Sums[i])
	}
	return clone
}

// assertFrameFileRoundTrip encodes the file with every codec option, with and without the zero sums
// mask (when its sums fit one), and reads it back.
func assertFrameFileRoundTrip(t *testing.T, name string, file File) {
	t.Helper()
	for _, usesZeroSumsMask := range []bool{false, true} {
		if usesZeroSumsMask && len(file.Sums) > MaxMaskedSums {
			continue
		}
		file.UsesZeroSumsMask = usesZeroSumsMask
		for _, options := range everyFrameCodecOptions {
			content := appendFile(nil, cloneFrameFile(file), options)
			decoded, err := DecodeFile(content, len(file.Rows), len(file.Sums))
			if err != nil {
				t.Fatalf("%s %+v mask %v: decode: %v", name, options, usesZeroSumsMask, err)
			}
			if !reflect.DeepEqual(decoded, file) {
				t.Fatalf("%s %+v mask %v: decoded file differs from the one written", name, options, usesZeroSumsMask)
			}
		}
	}
}

// TestZeroSumsMaskAndConstantColumns: a day of 1,000 products without sub-units (6 sums, the two
// sub-unit sums and the debt all zero, the divisor row level constant) pays 1 byte for its 3 empty
// sums with the mask and 2 bytes each without, and its constant SubDivisor level a few bytes.
func TestZeroSumsMaskAndConstantColumns(t *testing.T) {
	const rowCount = 1000
	file := File{Rows: make([][]int64, 2), Sums: make([][]int64, 6)}
	for row := range rowCount {
		file.Rows[0] = append(file.Rows[0], int64(100+row))
		file.Rows[1] = append(file.Rows[1], 1) // SubDivisor 1: no sub-units
		file.Sums[0] = append(file.Sums[0], int64(1+row%40)*250)
		file.Sums[3] = append(file.Sums[3], int64(1+row%9))
	}
	for _, s := range []int{1, 2, 4, 5} {
		file.Sums[s] = make([]int64, rowCount)
	}
	file.Sums[4][3] = 2 // one product with units pending delivery
	withoutMask := len(EncodeFile(cloneFrameFile(file)))
	masked := file
	masked.UsesZeroSumsMask = true
	withMask := len(EncodeFile(cloneFrameFile(masked)))
	// 3 empty columns: 6 bytes as constant columns, 1 mask byte with the mask.
	if withoutMask-withMask != 5 {
		t.Fatalf("the mask saves %d bytes, want 5 (%d → %d)", withoutMask-withMask, withoutMask, withMask)
	}
	// The SubDivisor level: child counts and first values constant, 2 bytes each, no steps (past the
	// 2-byte count of distinct products every nested file carries).
	single := File{Rows: file.Rows[:1], Sums: file.Sums}
	if nestedCost := withoutMask - len(EncodeFile(cloneFrameFile(single))) - 2; nestedCost != 4 {
		t.Fatalf("the constant SubDivisor level costs %d bytes, want 4", nestedCost)
	}
	assertFrameFileRoundTrip(t, "a day without sub-units", file)
}

// TestFrameFileRoundTrip covers the shapes the transforms and the blocks special-case: no rows, one
// row, block boundaries (127, 128, 129), constant and consecutive columns, negatives, the int64
// extremes, every sum width from 0 to 64 bits, and 1 to 3 nested Rows columns.
func TestFrameFileRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	assertFrameFileRoundTrip(t, "empty", File{Snapshot: 9, Rows: make([][]int64, 1), Sums: make([][]int64, 2)})
	assertFrameFileRoundTrip(t, "empty, 3 Rows columns", File{Snapshot: 9, Rows: make([][]int64, 3), Sums: make([][]int64, 1)})
	assertFrameFileRoundTrip(t, "one row", File{Snapshot: 1, Rows: [][]int64{{77}}, Sums: [][]int64{{-5}, {0}}})
	assertFrameFileRoundTrip(t, "one row, 3 Rows columns", File{Snapshot: 1, Rows: [][]int64{{7}, {0}, {9}}, Sums: [][]int64{{-5}}})
	assertFrameFileRoundTrip(t, "int64 extremes", File{
		Rows: [][]int64{{0, 1, math.MaxInt64}},
		Sums: [][]int64{{math.MinInt64, 0, math.MaxInt64}, {-1, -1, -1}},
	})
	assertFrameFileRoundTrip(t, "int64 extremes, 2 Rows columns", File{
		Rows: [][]int64{{0, 0, math.MaxInt64}, {math.MaxInt64 - 1, math.MaxInt64, 0}},
		Sums: [][]int64{{1, 2, 3}},
	})
	assertFrameFileRoundTrip(t, "one outer value", File{Rows: [][]int64{{5, 5, 5}, {1, 2, 9}}, Sums: [][]int64{{1, 1, 1}}})
	assertFrameFileRoundTrip(t, "every outer value once", File{Rows: [][]int64{{1, 2, 3}, {4, 4, 4}, {9, 0, 3}}, Sums: [][]int64{{1, 1, 1}}})
	for rowColumnCount := 1; rowColumnCount <= MaxRowColumns; rowColumnCount++ {
		for _, rowCount := range []int{2, 127, 128, 129, 300, 1000} {
			consecutive := randomFrameFile(rng, rowCount, rowColumnCount, 1, 0, 1)
			assertFrameFileRoundTrip(t, fmt.Sprintf("%d Rows columns, consecutive, zero sums, %d rows", rowColumnCount, rowCount), consecutive)
			for sumBits := 0; sumBits <= 64; sumBits++ {
				file := randomFrameFile(rng, rowCount, rowColumnCount, 2, sumBits, 1+rng.Int64N(1000))
				assertFrameFileRoundTrip(t, fmt.Sprintf("%d Rows columns, %d rows, %d-bit sums", rowColumnCount, rowCount, sumBits), file)
			}
		}
	}
	// Multiples of a common divisor, with a heavy tail that the patches carry.
	tail := randomFrameFile(rng, 1000, 1, 1, 0, 3)
	for row := range tail.Sums[0] {
		tail.Sums[0][row] = 1000 * int64(1+rng.IntN(8))
		if rng.IntN(50) == 0 {
			tail.Sums[0][row] = 1000 * int64(rng.IntN(1_000_000))
		}
	}
	assertFrameFileRoundTrip(t, "multiples of 1000 with a tail", tail)
}

// TestCommonDivisorPicksTheLargestThatDividesAll covers each candidate, the ones a residual rules
// out together (an odd value rules out 2, 8, 10, 100 and 1000), and the divisors outside the set.
func TestCommonDivisorPicksTheLargestThatDividesAll(t *testing.T) {
	for _, testCase := range []struct {
		residuals []uint64
		divisor   uint64
	}{
		{nil, 1},
		{[]uint64{0, 0, 0}, 1},
		{[]uint64{0, 3000, 1000, 47000}, 1000},
		{[]uint64{200, 400, 1000}, 100},   // 200 is outside the set: 100 is the largest in it
		{[]uint64{40, 80, 120}, 10},       // 8 divides too, 10 saves more bits
		{[]uint64{16, 48, 8}, 8},          // 1000 needs 8, but 16 isn't a multiple of 5
		{[]uint64{25, 50, 75}, 5},         // 25 is outside the set
		{[]uint64{2, 4, 6}, 2},            //
		{[]uint64{1000, 1000, 1001}, 1},   // the last value rules out every candidate
		{[]uint64{12, 36, 3}, 1},          // 3 is outside the set
		{[]uint64{1000, 0, 500, 250}, 10}, // 250: not a multiple of 100 or 8
	} {
		if divisor := commonDivisor(testCase.residuals); divisor != testCase.divisor {
			t.Fatalf("commonDivisor(%v) = %d, want %d", testCase.residuals, divisor, testCase.divisor)
		}
	}
}

// TestFrameBlockCostIsExactAndSmallest pins the width choice: the block the encoder writes takes
// exactly the bytes blockCost predicts for the chosen width, and no width costs less.
func TestFrameBlockCostIsExactAndSmallest(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for range 2000 {
		block := make([]uint64, 1+rng.IntN(blockSize))
		baseBits, tailBits := rng.IntN(20), rng.IntN(65)
		for i := range block {
			block[i] = rng.Uint64() & lowBitsMask(baseBits)
			if rng.IntN(16) == 0 {
				block[i] = rng.Uint64() & lowBitsMask(tailBits)
			}
		}
		var countByBitLength [65]int
		for _, residual := range block {
			countByBitLength[bits.Len64(residual)]++
		}
		smallestCost := math.MaxInt
		for width := 0; width <= 64; width++ {
			smallestCost = min(smallestCost, blockCost(len(block), &countByBitLength, width))
		}
		if written := len(appendBlock(nil, block, codecOptions{})); written != smallestCost {
			t.Fatalf("block of %d residuals: wrote %d bytes, the smallest width costs %d", len(block), written, smallestCost)
		}
	}
}

// TestFrameFileTruncated: every proper prefix of a file is refused, never a panic.
func TestFrameFileTruncated(t *testing.T) {
	for rowColumnCount := 1; rowColumnCount <= MaxRowColumns; rowColumnCount++ {
		file := randomFrameFile(rand.New(rand.NewPCG(5, 6)), 300, rowColumnCount, 2, 40, 9)
		content := appendFile(nil, cloneFrameFile(file), codecOptions{})
		for length := range len(content) {
			if _, err := DecodeFile(content[:length], rowColumnCount, 2); err == nil {
				t.Fatalf("%d Rows columns: a %d-byte prefix of a %d-byte file decoded without an error", rowColumnCount, length, len(content))
			}
		}
	}
}

// FuzzFrameDecode: arbitrary input returns an error or a file, never a panic, read with 1 to 3 Rows
// columns whatever it was written with.
func FuzzFrameDecode(f *testing.F) {
	rng := rand.New(rand.NewPCG(7, 8))
	for rowColumnCount := 1; rowColumnCount <= MaxRowColumns; rowColumnCount++ {
		for _, sumBits := range []int{0, 7, 33, 64} {
			f.Add(appendFile(nil, randomFrameFile(rng, 200, rowColumnCount, 2, sumBits, 5), codecOptions{}))
		}
	}
	f.Fuzz(func(t *testing.T, content []byte) {
		for rowColumnCount := 1; rowColumnCount <= MaxRowColumns; rowColumnCount++ {
			_, _ = DecodeFile(content, rowColumnCount, 2)
		}
	})
}

// varint16RowsLength is the size of the plan's first draft for the same file: rows one after
// another, each the ID step as a uvarint and each sum as a varint16 (a uint16 with 15 value bits
// and a continuation bit, then a uvarint of the rest).
func varint16RowsLength(file File) int {
	length := 1 + len(binary.AppendUvarint(nil, uint64(file.Snapshot)))
	previousID := int64(0)
	for row, rowID := range file.Rows[0] {
		length += len(binary.AppendUvarint(nil, uint64(rowID-previousID)))
		previousID = rowID
		for _, sumColumn := range file.Sums {
			length += 2
			if value := uint64(sumColumn[row]); value >= 1<<15 {
				length += len(binary.AppendUvarint(nil, value>>15))
			}
		}
	}
	return length
}

// TestFrameFileSizeExample measures one day of a day-product frame: 5,000 products, each with
// the day's quantity, a multiple of 1000 (three implied decimals), and its unit price in cents, a
// multiple of 100 (whole currency units). The products are 5,000 of a 20,000-product catalog.
// Quantities are log-normal, median 5 units with a tail of top sellers; prices are
// log-uniform between 1.00 and 500.00. Run with -v for the report.
func TestFrameFileSizeExample(t *testing.T) {
	const productCount, catalogSize = 5000, 20000
	rng := rand.New(rand.NewPCG(20730, productCount))
	productIDs := make([]int64, productCount)
	for i, catalogIndex := range rng.Perm(catalogSize)[:productCount] {
		productIDs[i] = int64(catalogIndex) + 1
	}
	slices.Sort(productIDs)
	quantities, unitPrices, amounts := make([]int64, productCount), make([]int64, productCount), make([]int64, productCount)
	for i := range productCount {
		units := 1 + int64(math.Exp(1.5+1.2*rng.NormFloat64()))
		priceUnits := int64(math.Round(math.Exp(rng.Float64() * math.Log(500))))
		quantities[i], unitPrices[i] = units*1000, priceUnits*100
		amounts[i] = units * unitPrices[i]
	}
	consecutiveIDs := make([]int64, productCount)
	for i := range consecutiveIDs {
		consecutiveIDs[i] = int64(i) + 1
	}

	columnLength := func(values []int64, isAscending bool, options codecOptions) int {
		if isAscending {
			return len(appendAscendingColumn(nil, slices.Clone(values), options))
		}
		return len(appendSumsColumn(nil, slices.Clone(values), options))
	}
	var report strings.Builder
	fmt.Fprintf(&report, "\n%-46s %9s %9s %9s\n", "column, 5,000 values", "bytes", "bits/row", "no div/patch")
	for _, column := range []struct {
		name        string
		values      []int64
		isAscending bool
	}{
		{"ProductID (5,000 of IDs 1..20,000)", productIDs, true},
		{"ProductID (1..5,000, consecutive)", consecutiveIDs, true},
		{"Quantity (multiples of 1000)", quantities, false},
		{"UnitPrice (cents, multiples of 100)", unitPrices, false},
		{"Amount (Quantity / 1000 × UnitPrice)", amounts, false},
	} {
		length := columnLength(column.values, column.isAscending, codecOptions{})
		bare := columnLength(column.values, column.isAscending, codecOptions{skipsDivisor: true, skipsPatches: true})
		fmt.Fprintf(&report, "%-46s %9d %9.2f %9d\n", column.name, length, float64(length*8)/productCount, bare)
	}

	file := File{Snapshot: 1_250_000, Rows: [][]int64{productIDs}, Sums: [][]int64{quantities, unitPrices}}
	fullLength := 0
	fmt.Fprintf(&report, "\nfile: ProductID + Quantity + UnitPrice, one day\n")
	for _, variant := range []struct {
		name    string
		options codecOptions
	}{
		{"this codec", codecOptions{}},
		{"without the divisor", codecOptions{skipsDivisor: true}},
		{"without patches", codecOptions{skipsPatches: true}},
		{"without either", codecOptions{skipsDivisor: true, skipsPatches: true}},
	} {
		content := appendFile(nil, cloneFrameFile(file), variant.options)
		decoded, err := DecodeFile(content, 1, 2)
		if err != nil || !reflect.DeepEqual(decoded, file) {
			t.Fatalf("%s: the file does not read back: %v", variant.name, err)
		}
		if variant.options == (codecOptions{}) {
			fullLength = len(content)
		}
		fmt.Fprintf(&report, "  %-36s %7d B  %6.2f KB  %5.2f B/row\n", variant.name, len(content), float64(len(content))/1024, float64(len(content))/productCount)
	}
	rowsLength := varint16RowsLength(file)
	fmt.Fprintf(&report, "  %-36s %7d B  %6.2f KB  %5.2f B/row\n", "varint16 rows (first draft)", rowsLength, float64(rowsLength)/1024, float64(rowsLength)/productCount)
	t.Log(report.String())
	if fullLength >= rowsLength {
		t.Fatalf("the columnar file (%d B) is not smaller than varint16 rows (%d B)", fullLength, rowsLength)
	}
}

// TestFrameFileNestedRowsSize measures a volunteer-month file: one row per department × program ×
// employee of a month, as three nested Rows columns and as the one integer they could be packed into
// (department << 46 | program << 31 | employee). Employees are IDs 1..60,000 spread over the
// departments; six 0–3 sums. Run with -v for the report.
func TestFrameFileNestedRowsSize(t *testing.T) {
	var report strings.Builder
	fmt.Fprintf(&report, "\n%-34s %7s %7s %9s %9s\n", "file", "rows", "packed", "nested", "saved")
	for _, scenario := range []struct {
		departmentCount, programCount, rowCount int
	}{{20, 6, 1000}, {20, 6, 3000}, {200, 10, 20000}} {
		rng := rand.New(rand.NewPCG(uint64(scenario.rowCount), 1))
		departmentIDs := rng.Perm(1000)[:scenario.departmentCount]
		isTaken := map[[3]int64]bool{}
		var rows [][3]int64
		for len(rows) < scenario.rowCount {
			row := [3]int64{int64(departmentIDs[rng.IntN(len(departmentIDs))]), 1 + rng.Int64N(int64(scenario.programCount)), 1 + rng.Int64N(60000)}
			if !isTaken[row] {
				isTaken[row] = true
				rows = append(rows, row)
			}
		}
		slices.SortFunc(rows, func(a, b [3]int64) int { return slices.Compare(a[:], b[:]) })
		nested := File{Rows: make([][]int64, 3), Sums: make([][]int64, 6)}
		packed := File{Rows: make([][]int64, 1), Sums: nested.Sums}
		for _, row := range rows {
			for c := range row {
				nested.Rows[c] = append(nested.Rows[c], row[c])
			}
			packed.Rows[0] = append(packed.Rows[0], row[0]<<46|row[1]<<31|row[2])
		}
		for s := range nested.Sums {
			for range rows {
				nested.Sums[s] = append(nested.Sums[s], rng.Int64N(4))
			}
		}
		assertFrameFileRoundTrip(t, "nested volunteer-month", nested)
		nestedLength, packedLength := len(EncodeFile(cloneFrameFile(nested))), len(EncodeFile(cloneFrameFile(packed)))
		name := fmt.Sprintf("%d depts × %d programs", scenario.departmentCount, scenario.programCount)
		fmt.Fprintf(&report, "%-34s %7d %7d B %7d B %8.1f%%\n", name, len(rows), packedLength, nestedLength, 100*float64(packedLength-nestedLength)/float64(packedLength))
		if nestedLength >= packedLength {
			t.Fatalf("%s: nested Rows (%d B) is not smaller than the packed key (%d B)", name, nestedLength, packedLength)
		}
	}
	t.Log(report.String())
}

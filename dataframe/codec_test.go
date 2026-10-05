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

// randomFrameFile builds a file of rowCount rows whose sums have at most sumBits significant bits
// (64: the whole int64 range, negatives included) and whose row IDs step by up to maxStep.
func randomFrameFile(rng *rand.Rand, rowCount, sumsCount, sumBits int, maxStep int64) File {
	file := File{Snapshot: rng.Int64N(1 << 31), RowIDs: make([]int64, rowCount), Sums: make([][]int64, sumsCount)}
	nextID := rng.Int64N(1 << 20)
	for row := range file.RowIDs {
		file.RowIDs[row] = nextID
		nextID += 1 + rng.Int64N(maxStep)
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
	clone := File{Snapshot: file.Snapshot, RowIDs: slices.Clone(file.RowIDs), Sums: make([][]int64, len(file.Sums))}
	for i := range file.Sums {
		clone.Sums[i] = slices.Clone(file.Sums[i])
	}
	return clone
}

func assertFrameFileRoundTrip(t *testing.T, name string, file File) {
	t.Helper()
	for _, options := range everyFrameCodecOptions {
		content := appendFile(nil, cloneFrameFile(file), options)
		decoded, err := DecodeFile(content, len(file.Sums))
		if err != nil {
			t.Fatalf("%s %+v: decode: %v", name, options, err)
		}
		if !reflect.DeepEqual(decoded, file) {
			t.Fatalf("%s %+v: decoded file differs from the one written", name, options)
		}
	}
}

// TestFrameFileRoundTrip covers the shapes the transforms and the blocks special-case: no rows, one
// row, block boundaries (127, 128, 129), constant and consecutive columns, negatives, the int64
// extremes and every sum width from 0 to 64 bits.
func TestFrameFileRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	assertFrameFileRoundTrip(t, "empty", File{Snapshot: 9, Sums: make([][]int64, 2)})
	assertFrameFileRoundTrip(t, "one row", File{Snapshot: 1, RowIDs: []int64{77}, Sums: [][]int64{{-5}, {0}}})
	assertFrameFileRoundTrip(t, "int64 extremes", File{
		RowIDs: []int64{0, 1, math.MaxInt64},
		Sums:   [][]int64{{math.MinInt64, 0, math.MaxInt64}, {-1, -1, -1}},
	})
	for _, rowCount := range []int{2, 127, 128, 129, 300, 1000} {
		consecutive := randomFrameFile(rng, rowCount, 1, 0, 1)
		assertFrameFileRoundTrip(t, fmt.Sprintf("consecutive ids, zero sums, %d rows", rowCount), consecutive)
		for sumBits := 0; sumBits <= 64; sumBits++ {
			file := randomFrameFile(rng, rowCount, 2, sumBits, 1+rng.Int64N(1000))
			assertFrameFileRoundTrip(t, fmt.Sprintf("%d rows, %d-bit sums", rowCount, sumBits), file)
		}
	}
	// Multiples of a common divisor, with a heavy tail that the patches carry.
	tail := randomFrameFile(rng, 1000, 1, 0, 3)
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

// TestFrameFileHashIgnoresTheSnapshot: a file rewritten at a later snapshot with the same rows keeps
// its hash; other rows change it.
func TestFrameFileHashIgnoresTheSnapshot(t *testing.T) {
	file := randomFrameFile(rand.New(rand.NewPCG(9, 10)), 50, 2, 30, 4)
	hash := FileHash(appendFile(nil, cloneFrameFile(file), codecOptions{}))
	rewritten := cloneFrameFile(file)
	rewritten.Snapshot = file.Snapshot + 1_000_000_000
	if rewrittenHash := FileHash(appendFile(nil, rewritten, codecOptions{})); rewrittenHash != hash {
		t.Fatalf("the same rows at another snapshot hash to %08x, not %08x", rewrittenHash, hash)
	}
	rewritten.Sums[0][7]++
	if changedHash := FileHash(appendFile(nil, rewritten, codecOptions{})); changedHash == hash {
		t.Fatal("other rows kept the hash")
	}
}

// TestFrameIndexRoundTrip: an _idx of 2- and 3-key entries, and an _ixt of several blocks, read back
// their keys and hashes in order.
func TestFrameIndexRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 12))
	for _, keyCount := range []int{2, 3} {
		var blocks [][]IndexEntry
		var extension []byte
		for _, entryCount := range []int{0, 1, 129, 1000} {
			entries := make([]IndexEntry, entryCount)
			for i := range entries {
				entries[i] = IndexEntry{Hash: rng.Uint32()}
				entries[i].Keys[0] = int64(i / 3)
				if keyCount == 3 {
					entries[i].Keys[1] = rng.Int64N(1 << 40)
				}
			}
			decoded, err := DecodeIndex(appendIndex(nil, entries, keyCount), keyCount)
			if err != nil || !reflect.DeepEqual(decoded, entries) {
				t.Fatalf("%d-key _idx of %d entries: decoded %d entries, err %v", keyCount, entryCount, len(decoded), err)
			}
			blocks = append(blocks, entries)
			extension = appendIndexExtensionBlock(extension, entries, keyCount)
		}
		decoded, err := DecodeIndexExtension(extension, keyCount)
		if err != nil || !reflect.DeepEqual(decoded, slices.Concat(blocks...)) {
			t.Fatalf("%d-key _ixt: decoded %d entries, err %v", keyCount, len(decoded), err)
		}
	}
}

// TestFrameFileTruncated: every proper prefix of a file is refused, never a panic.
func TestFrameFileTruncated(t *testing.T) {
	file := randomFrameFile(rand.New(rand.NewPCG(5, 6)), 300, 2, 40, 9)
	content := appendFile(nil, cloneFrameFile(file), codecOptions{})
	for length := range len(content) {
		if _, err := DecodeFile(content[:length], 2); err == nil {
			t.Fatalf("a %d-byte prefix of a %d-byte file decoded without an error", length, len(content))
		}
	}
}

// FuzzFrameDecode: arbitrary input returns an error or a file, never a panic.
func FuzzFrameDecode(f *testing.F) {
	rng := rand.New(rand.NewPCG(7, 8))
	for _, sumBits := range []int{0, 7, 33, 64} {
		f.Add(appendFile(nil, randomFrameFile(rng, 200, 2, sumBits, 5), codecOptions{}))
	}
	f.Fuzz(func(t *testing.T, content []byte) {
		_, _ = DecodeFile(content, 2)
	})
}

// varint16RowsLength is the size of the plan's first draft for the same file: rows one after
// another, each the ID step as a uvarint and each sum as a varint16 (a uint16 with 15 value bits
// and a continuation bit, then a uvarint of the rest).
func varint16RowsLength(file File) int {
	length := 1 + len(binary.AppendUvarint(nil, uint64(file.Snapshot)))
	previousID := int64(0)
	for row, rowID := range file.RowIDs {
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

	file := File{Snapshot: 1_250_000, RowIDs: productIDs, Sums: [][]int64{quantities, unitPrices}}
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
		decoded, err := DecodeFile(content, 2)
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

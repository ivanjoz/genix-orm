package dataframe

import (
	"math/rand/v2"
	"slices"
	"testing"
	"unsafe"
)

type aggregateTestRecord struct {
	Day, Client, Product int32
	Amount               int64
	Status               int8
}

type aggregateTestFrame struct {
	Day     int32 `frame:"key,15"`
	Client  int32 `frame:"row,20"`
	Product int32 `frame:"row,20"`
	Amount  int64 `frame:"sum"`
}

func (record *aggregateTestRecord) toFrames(lines []aggregateTestFrame) []aggregateTestFrame {
	if record.Status == 0 {
		return lines
	}
	return append(lines, aggregateTestFrame{Day: record.Day, Client: record.Client, Product: record.Product, Amount: record.Amount})
}

func compileAggregateTestFrame(t *testing.T) *Frame {
	t.Helper()
	table, err := Compile("aggregate_test", 7, []Spec{{Name: "sales", Count: true, Projection: ProjectionOf((*aggregateTestRecord).toFrames)}})
	if err != nil {
		t.Fatal(err)
	}
	frame := &table.Frames[0]
	if err := frame.CheckPackedKey("aggregate_test"); err != nil {
		t.Fatal(err)
	}
	return frame
}

// TestAggregateMatchesBruteForce applies random inserts, updates and soft deletes as the SQL write path
// does (stored lines out, written lines in) and checks the selected rows against a brute-force sum of
// the records as they end.
func TestAggregateMatchesBruteForce(t *testing.T) {
	frame := compileAggregateTestFrame(t)
	random := rand.New(rand.NewPCG(1, 2))
	records := map[int]*aggregateTestRecord{}
	table := map[int64]AggregateRow{}
	for step := range 3000 {
		recordID := random.IntN(200)
		written := &aggregateTestRecord{Day: 20000 + random.Int32N(5), Client: random.Int32N(4), Product: random.Int32N(6),
			Amount: random.Int64N(1000) - 300, Status: int8(random.IntN(4) % 3)}
		deltas := map[int64][]int64{}
		if stored := records[recordID]; stored != nil {
			storedLines, _ := frame.linesOf(unsafe.Pointer(stored))
			if err := frame.AddLines(deltas, storedLines, -1); err != nil {
				t.Fatal(err)
			}
		}
		writtenLines, _ := frame.linesOf(unsafe.Pointer(written))
		if err := frame.AddLines(deltas, writtenLines, 1); err != nil {
			t.Fatal(err)
		}
		for _, delta := range SortedDeltas(deltas) {
			row := table[delta.PackedKey]
			if row.Sums == nil {
				row = AggregateRow{PackedKey: delta.PackedKey, Sums: make([]int64, frame.SumsCount)}
			}
			for position, sum := range delta.Sums {
				row.Sums[position] += sum
			}
			row.Updated = int64(step)
			table[delta.PackedKey] = row
		}
		records[recordID] = written
	}

	selection := Selection{FromKey: 20001, ToKey: 20003}
	from, to := frame.PackedKeyRange(selection)
	var scanned []AggregateRow
	for packed, row := range table {
		if packed >= from && packed <= to {
			scanned = append(scanned, row)
		}
	}
	slices.SortFunc(scanned, func(left, right AggregateRow) int { return int(left.PackedKey - right.PackedKey) })
	got := frame.RowsOfAggregate(scanned, selection)

	type rowKey struct{ day, client, product int32 }
	expectedSums := map[rowKey][2]int64{}
	for _, record := range records {
		if record.Status != 0 && record.Day >= 20001 && record.Day <= 20003 {
			key := rowKey{record.Day, record.Client, record.Product}
			sums := expectedSums[key]
			expectedSums[key] = [2]int64{sums[0] + record.Amount, sums[1] + 1}
		}
	}
	var expected []Row
	for key, sums := range expectedSums {
		if sums[0] != 0 || sums[1] != 0 {
			expected = append(expected, Row{Keys: []int64{int64(key.day)}, RowValues: []int64{int64(key.client), int64(key.product)}, Sums: sums[:]})
		}
	}
	slices.SortFunc(expected, func(left, right Row) int {
		return slices.Compare(append(slices.Clone(left.Keys), left.RowValues...), append(slices.Clone(right.Keys), right.RowValues...))
	})
	if len(got) != len(expected) {
		t.Fatalf("got %d rows, expected %d", len(got), len(expected))
	}
	for position := range got {
		if !slices.Equal(got[position].Keys, expected[position].Keys) || !slices.Equal(got[position].RowValues, expected[position].RowValues) ||
			!slices.Equal(got[position].Sums, expected[position].Sums) {
			t.Fatalf("row %d: got %+v, expected %+v", position, got[position], expected[position])
		}
	}

	// A client with no copies gets every selected file; decoded, they hold the same rows. Holding them
	// at the answer's snapshot, it gets nothing more until a row changes after it.
	answer := frame.ChangedFilesOfAggregate(scanned, selection, "", nil, 1_000_000)
	decoded, err := DecodeChangedFiles(AppendChangedFiles(nil, answer))
	if err != nil {
		t.Fatal(err)
	}
	var fromFiles []Row
	held := []HeldFile{}
	for _, changed := range decoded.Files {
		file, err := DecodeFile(changed.Content, len(frame.Rows), frame.SumsCount)
		if err != nil {
			t.Fatal(err)
		}
		for rowIndex := range file.Rows[0] {
			fromFiles = append(fromFiles, Row{Keys: changed.Keys, RowValues: []int64{file.Rows[0][rowIndex], file.Rows[1][rowIndex]},
				Sums: []int64{file.Sums[0][rowIndex], file.Sums[1][rowIndex]}})
		}
		held = append(held, HeldFile{Keys: changed.Keys, Snapshot: changed.Snapshot, Slot: changed.Slot})
	}
	if len(fromFiles) != len(expected) {
		t.Fatalf("files hold %d rows, expected %d", len(fromFiles), len(expected))
	}
	if again := frame.ChangedFilesOfAggregate(scanned, selection, decoded.Generation, held, 1_000_000); len(again.Files) != 0 {
		t.Fatalf("an up-to-date client got %d files", len(again.Files))
	}
	scanned[0].Updated = 2_000_000
	if again := frame.ChangedFilesOfAggregate(scanned, selection, decoded.Generation, held, 2_000_100); len(again.Files) != 1 {
		t.Fatalf("one changed file: got %d files", len(again.Files))
	}
}

func TestPackedKeyWidths(t *testing.T) {
	frame := compileAggregateTestFrame(t)
	if _, err := frame.PackKey(Line{1 << 15, 0, 0, 0}); err == nil {
		t.Fatal("a Day past 15 bits must fail")
	}
	packed, err := frame.PackKey(Line{20000, 3, 9, 0})
	if err != nil {
		t.Fatal(err)
	}
	if keys, rowValues := frame.unpackKey(packed); !slices.Equal(keys, []int64{20000}) || !slices.Equal(rowValues, []int64{3, 9}) {
		t.Fatalf("unpacked %v %v", keys, rowValues)
	}
}

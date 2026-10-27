package dataframe

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// frameWireVectors is testdata/frame_wire_vectors.json, which genix-ui's frame wire test reads too:
// the two implementations must read the same bytes. GENIX_UPDATE_VECTORS=1 rewrites it from
// makeFrameWireVectors; otherwise the encoder must still produce exactly it.
type frameWireVectors struct {
	Files []frameFileVector   `json:"files"`
	Wire  []frameAnswerVector `json:"wire"`
	Held  []frameHeldVector   `json:"held"`
}

type frameFileVector struct {
	Name             string    `json:"name"`
	Snapshot         int64     `json:"snapshot"`
	Slot             uint8     `json:"slot"`
	Rows             [][]int64 `json:"rows"`
	Sums             [][]int64 `json:"sums"`
	UsesZeroSumsMask bool      `json:"usesZeroSumsMask"`
	Hex              string    `json:"hex"`
}

type frameAnswerVector struct {
	Name           string                  `json:"name"`
	Generation     string                  `json:"generation"`
	RowColumnCount uint8                   `json:"rowColumnCount"`
	SumsCount      uint8                   `json:"sumsCount"`
	Snapshot       int64                   `json:"snapshot"`
	Files          []frameAnswerFileVector `json:"files"`
	Hex            string                  `json:"hex"`
}

type frameAnswerFileVector struct {
	Keys     []int64 `json:"keys"`
	Snapshot int64   `json:"snapshot"`
	Slot     uint8   `json:"slot"`
	// File names an entry of Files, whose bytes the wire carries; "" is a file without rows.
	File string `json:"file"`
}

type frameHeldVector struct {
	Files []HeldFile `json:"files"`
	Text  string     `json:"text"`
}

// makeFrameWireVectors covers each step of the codec (one and three nested Rows columns, a block
// boundary at 128 rows, the divisor, negative sums, patches past 32 bits, constant columns, the zero
// sums mask, a file without rows) and the wire around them.
func makeFrameWireVectors() frameWireVectors {
	single := frameFileVector{Name: "one-rows-column", Snapshot: 396172200, Slot: 7,
		Rows: [][]int64{{1001, 1002, 1005, 1100}}, Sums: [][]int64{{120, 60, 300, -40}, {10, 5, 20, 0}}}
	nested := frameFileVector{Name: "three-nested-rows-columns", Snapshot: 396172800, Slot: 255,
		Rows: make([][]int64, 3), Sums: make([][]int64, 3)}
	for row := range 130 {
		nested.Rows[0] = append(nested.Rows[0], int64(10+row/50))        // 3 activities
		nested.Rows[1] = append(nested.Rows[1], int64(row/10%5*7))       // departments under each
		nested.Rows[2] = append(nested.Rows[2], int64(1000+row*3+row%2)) // employees under each
		nested.Sums[0] = append(nested.Sums[0], int64(row%4)*1000)       // divisor 1000
		nested.Sums[1] = append(nested.Sums[1], int64(row%7-3))          // negative values
		points := int64(row % 9)
		if row == 77 || row == 129 {
			points = 5_000_000_000 // patched above the block's width
		}
		nested.Sums[2] = append(nested.Sums[2], points)
	}
	empty := frameFileVector{Name: "no-rows", Snapshot: 396173000, Slot: 3, Rows: [][]int64{{}, {}}, Sums: [][]int64{{}}}
	// A sale summary day: products × divisor (constant 1 but for one), the sub-unit and debt sums empty
	// but one sub sum, behind the zero sums mask.
	masked := frameFileVector{Name: "zero-sums-mask", Snapshot: 396174000, Slot: 9, UsesZeroSumsMask: true,
		Rows: [][]int64{{101, 102, 103, 107}, {1, 1, 1, 6}},
		Sums: [][]int64{{2500, 1250, 900, 4000}, {0, 0, 0, 0}, {3, 1, 2, 1}, {0, 0, 0, 4}, {0, 0, 0, 0}, {0, 0, 0, 0}}}
	vectors := frameWireVectors{Files: []frameFileVector{single, nested, empty, masked}}

	contentByName := map[string][]byte{}
	for i, vector := range vectors.Files {
		content := EncodeFile(File{Snapshot: vector.Snapshot, Slot: vector.Slot, Rows: vector.Rows, Sums: vector.Sums, UsesZeroSumsMask: vector.UsesZeroSumsMask})
		vectors.Files[i].Hex, contentByName[vector.Name] = hex.EncodeToString(content), content
	}
	vectors.Wire = []frameAnswerVector{
		{Name: "one-file", Generation: "P4F-6sV", RowColumnCount: 1, SumsCount: 2, Snapshot: 396172100,
			Files: []frameAnswerFileVector{{Keys: []int64{2601}, Snapshot: 396172200, Slot: 7, File: "one-rows-column"}}},
		{Name: "nested-and-empty", Generation: "P4F-6sW", RowColumnCount: 3, SumsCount: 3, Snapshot: 396172100,
			Files: []frameAnswerFileVector{
				{Keys: []int64{2601, 4}, Snapshot: 396172800, Slot: 255, File: "three-nested-rows-columns"},
				{Keys: []int64{2602, 4}, Snapshot: 396173000},
			}},
		{Name: "nothing-changed", Generation: "P4F-6sW", RowColumnCount: 3, SumsCount: 3, Snapshot: 396172100, Files: []frameAnswerFileVector{}},
		{Name: "masked-file", Generation: "P4F-6sX", RowColumnCount: 2, SumsCount: 6, Snapshot: 396174000,
			Files: []frameAnswerFileVector{{Keys: []int64{20500}, Snapshot: 396174000, Slot: 9, File: "zero-sums-mask"}}},
	}
	for i, vector := range vectors.Wire {
		answer := ChangedFiles{Generation: vector.Generation, RowColumnCount: vector.RowColumnCount, SumsCount: vector.SumsCount, Snapshot: vector.Snapshot}
		for _, file := range vector.Files {
			answer.Files = append(answer.Files, ChangedFile{Keys: file.Keys, Snapshot: file.Snapshot, Slot: file.Slot, Content: contentByName[file.File]})
		}
		vectors.Wire[i].Hex = hex.EncodeToString(AppendChangedFiles(nil, answer))
	}
	held := []HeldFile{{Keys: []int64{2601, 4}, Snapshot: 396172800, Slot: 255}, {Keys: []int64{2602, 4}, Snapshot: 396173000}}
	vectors.Held = []frameHeldVector{{Files: held, Text: FormatHeldFiles(held)}}
	return vectors
}

func TestFrameWireVectors(t *testing.T) {
	path := "testdata/frame_wire_vectors.json"
	vectors := makeFrameWireVectors()
	for _, vector := range vectors.Files {
		content, _ := hex.DecodeString(vector.Hex)
		decoded, err := DecodeFile(content, len(vector.Rows), len(vector.Sums))
		if err != nil || decoded.Snapshot != vector.Snapshot || decoded.Slot != vector.Slot || decoded.UsesZeroSumsMask != vector.UsesZeroSumsMask {
			t.Fatalf("%s: %+v, %v", vector.Name, decoded, err)
		}
		if decoded.rowCount() > 0 && (!reflect.DeepEqual(decoded.Rows, vector.Rows) || !reflect.DeepEqual(decoded.Sums, vector.Sums)) {
			t.Fatalf("%s does not read back", vector.Name)
		}
	}
	written, err := json.MarshalIndent(vectors, "", "\t")
	if err != nil {
		t.Fatal(err)
	}
	written = append(written, '\n')
	if os.Getenv("GENIX_UPDATE_VECTORS") == "1" {
		if err := os.WriteFile(path, written, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	stored, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored) != string(written) {
		t.Fatalf("%s is out of date with the codec: rerun with GENIX_UPDATE_VECTORS=1 and check genix-ui's frame.wire.test.ts", path)
	}
}

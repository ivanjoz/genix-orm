package dataframe

import (
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
)

// TestLineSumsMatchesFrameRows checks a rebuild's sums against the per-file maps the runs use: packed
// and radix sorted groups, groups too wide to pack, rows that sum to 0 and files left without rows.
func TestLineSumsMatchesFrameRows(t *testing.T) {
	for name, spread := range map[string]int64{"packed": 50, "too wide to pack": 1 << 40} {
		for _, frame := range compileTestTable(t).Frames {
			rng := rand.New(rand.NewPCG(7, uint64(spread)))
			sums := newLineSums(&frame)
			rowsByFileKey := map[string]frameRows{}
			for i := range 3 * chunkLines / 2 {
				line := make(Line, frame.lineLength())
				for v := range len(frame.Keys) + len(frame.Rows) {
					line[v] = rng.Int64N(4) * rng.Int64N(spread)
				}
				for v := len(frame.Keys) + len(frame.Rows); v < len(line); v++ {
					// Lines that cancel out: some rows, and some whole files, sum to 0.
					line[v] = int64(1 - 2*(i%2))
				}
				sums.add(line)
				fileKey := frame.fileKeyOf("g", 3, frame.keysOf(line))
				if rowsByFileKey[fileKey] == nil {
					rowsByFileKey[fileKey] = frameRows{}
				}
				rowsByFileKey[fileKey].add(&frame, line, 1)
			}
			fileKeys, files := sums.files("g", 3, 99)
			if len(fileKeys) != len(rowsByFileKey) {
				t.Fatalf("%s frame %s: %d files, want %d", name, frame.Name, len(fileKeys), len(rowsByFileKey))
			}
			for i, fileKey := range fileKeys {
				want := rowsByFileKey[fileKey].fileAt(99, &frame)
				if i > 0 && slices.Compare(frame.keysOfFileMust(fileKeys[i-1]), frame.keysOfFileMust(fileKey)) >= 0 {
					t.Fatalf("%s frame %s: files not ascending by Keys at %s", name, frame.Name, fileKey)
				}
				if !slices.Equal(EncodeFile(files[i]), EncodeFile(want)) || (files[i].rowCount() > 0 && !reflect.DeepEqual(files[i], want)) {
					t.Fatalf("%s frame %s file %s: %+v, want %+v", name, frame.Name, fileKey, files[i], want)
				}
			}
		}
	}
}

func (frame *Frame) keysOfFileMust(fileKey string) []int64 {
	keys, _ := frame.keysOfFile("g", 3, fileKey)
	return keys
}

func TestRadixSortIsStable(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, count := range []int{0, 1, 2, 1000} {
		keys, values := make([]uint64, count), make([]int32, count)
		for i := range keys {
			keys[i], values[i] = rng.Uint64N(1<<20)&^0xFF00, int32(i) // byte 1 shared: a skipped pass
		}
		type pair struct {
			key   uint64
			value int32
		}
		want := make([]pair, count)
		for i := range want {
			want[i] = pair{keys[i], values[i]}
		}
		slices.SortStableFunc(want, func(a, b pair) int { return int(int64(a.key) - int64(b.key)) })
		radixSort(keys, values, 20)
		for i := range want {
			if keys[i] != want[i].key || values[i] != want[i].value {
				t.Fatalf("count %d at %d: %d/%d, want %+v", count, i, keys[i], values[i], want[i])
			}
		}
	}
}

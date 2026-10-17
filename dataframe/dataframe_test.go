package dataframe

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ivanjoz/genix-orm/utils"
)

// ── Test table: enrollments summed per year and program, one row per employee ──

var (
	employeeColumn = Column{FieldName: "EmployeeID", ColumnID: 2}
	yearColumn     = Column{FieldName: "Year", ColumnID: 3}
	programColumn  = Column{FieldName: "ProgramID", ColumnID: 4}
	minutesColumn  = Column{FieldName: "Minutes", ColumnID: 5}
	pointsColumn   = Column{FieldName: "Points", ColumnID: 6}
	statusColumn   = Column{FieldName: "Status", ColumnID: 7}
	hoursColumn    = Column{FieldName: "Hours", ColumnID: 9, IsFloat: true}
)

// The tracked values of a test record, in column id order.
const (
	employeeIndex = iota
	yearIndex
	programIndex
	minutesIndex
	pointsIndex
	statusIndex
	hoursIndex
)

func compileTestTable(t *testing.T) *Table {
	t.Helper()
	table, err := Compile("enrollment", 78901377, []Spec{{
		Name: "year-program", Keys: []Column{yearColumn, programColumn}, Rows: employeeColumn,
		Sums: []Column{minutesColumn, pointsColumn, hoursColumn}, Count: true,
	}}, &statusColumn)
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func TestCompileRules(t *testing.T) {
	table := compileTestTable(t)
	frame := &table.Frames[0]
	if frame.Folder != "dtf/3gz10/year-program/" || frame.SumsCount != 4 || len(frame.Shape) != 8 || len(table.Tracked) != 7 || table.StatusIndex != statusIndex {
		t.Fatalf("compiled: %+v", table)
	}
	for name, spec := range map[string]Spec{
		"a name with capitals":   {Name: "Year", Keys: []Column{yearColumn}, Rows: employeeColumn, Count: true},
		"four Keys":              {Name: "a", Keys: []Column{yearColumn, programColumn, minutesColumn, pointsColumn}, Rows: employeeColumn, Count: true},
		"no Rows":                {Name: "a", Keys: []Column{yearColumn}, Count: true},
		"neither Sums nor Count": {Name: "a", Keys: []Column{yearColumn}, Rows: employeeColumn},
		"a float Key":            {Name: "a", Keys: []Column{hoursColumn}, Rows: employeeColumn, Count: true},
		"a column twice":         {Name: "a", Keys: []Column{yearColumn}, Rows: employeeColumn, Sums: []Column{yearColumn}},
		"a column id over 255":   {Name: "a", Keys: []Column{{FieldName: "Wide", ColumnID: 300}}, Rows: employeeColumn, Count: true},
	} {
		if _, err := Compile("bad", 1, []Spec{spec}, nil); err == nil {
			t.Fatalf("%s: compiled", name)
		}
	}
	twice := Spec{Name: "a", Keys: []Column{yearColumn}, Rows: employeeColumn, Count: true}
	if _, err := Compile("bad", 1, []Spec{twice, twice}, nil); err == nil {
		t.Fatal("two frames with one name compiled")
	}
}

func TestValuesAndTheWriteCheck(t *testing.T) {
	table := compileTestTable(t)
	frame := &table.Frames[0]
	tracked := []int64{4, 2026, 3, 240, 100, 1, 1_500_000}
	values := table.valuesOf(frame, tracked)
	if !slices.Equal(values.keys, []int64{2026, 3}) || values.row != 4 || !slices.Equal(values.sums, []int64{240, 100, 1_500_000, 1}) {
		t.Fatalf("frame values: %+v", values)
	}
	tracked[statusIndex] = 0
	if table.valuesOf(frame, tracked) != nil {
		t.Fatal("a deleted record (Status 0) counts in no file")
	}
	tracked[statusIndex], tracked[yearIndex] = 1, -1
	if err := table.CheckValues(tracked); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("a negative Keys value must fail the write, got %v", err)
	}
	if _, isKept := ScaledFloat(1e13); isKept {
		t.Fatal("a float past ±1.1e12 must not be kept")
	}
}

func TestFileKeysRoundTrip(t *testing.T) {
	frame := &compileTestTable(t).Frames[0]
	fileKey := frame.fileKeyOf("g1", []int64{2026, 3})
	if fileKey != "dtf/3gz10/year-program/g1/2026/3" {
		t.Fatalf("file key %q", fileKey)
	}
	if keys, isFrameFile := frame.keysOfFile("g1", fileKey); !isFrameFile || !slices.Equal(keys, []int64{2026, 3}) {
		t.Fatalf("keys of %q: %v %v", fileKey, keys, isFrameFile)
	}
	for _, otherKey := range []string{frame.Folder + "g2/2026/3", frame.Folder + "g1/2026"} {
		if _, isFrameFile := frame.keysOfFile("g1", otherKey); isFrameFile {
			t.Fatalf("%q read as a file of generation g1", otherKey)
		}
	}
}

// ── Changelog ──

func TestChangelogRoundTripAndSizes(t *testing.T) {
	for _, testCase := range []struct {
		values    []int64
		wantBytes int // 3 header bytes, then per column its id and its value bytes
	}{
		{[]int64{0, 63}, 3 + 2 + 2},                                 // reduced uint6
		{[]int64{64, 1<<14 - 1}, 3 + 3 + 3},                         // reduced uint14
		{[]int64{1<<30 - 1}, 3 + 5},                                 // reduced uint30
		{[]int64{1 << 30}, 3 + 1 + 5},                               // normal: 5 + 32 bits
		{[]int64{-1, 15}, 3 + 2 + 2},                                // normal with negatives: 4 bits in the first byte
		{[]int64{-16, 1}, 3 + 3 + 2},                                // −16 needs a second byte
		{[]int64{MaxMagnitude - 1, -(MaxMagnitude - 1)}, 3 + 9 + 9}, // 60 bits each
		{[]int64{1<<61 - 1}, 3 + 9},                                 // 61 bits without negatives
	} {
		entry := changelogEntry{delta: 300}
		for i, value := range testCase.values {
			entry.values = append(entry.values, changelogValue{columnID: uint8(i + 1), value: value})
		}
		entries := []changelogEntry{entry, {delta: MaxDelta, values: []changelogValue{{columnID: 255, value: 7}}}}
		content := encodeChangelog(entries)
		if firstEntryBytes := len(content) - 5; firstEntryBytes != testCase.wantBytes {
			t.Fatalf("values %v take %d bytes, want %d", testCase.values, firstEntryBytes, testCase.wantBytes)
		}
		decoded, err := decodeChangelog(content)
		if err != nil || !reflect.DeepEqual(decoded, entries) {
			t.Fatalf("values %v: decoded %+v, err %v", testCase.values, decoded, err)
		}
		if _, err := decodeChangelog(content[:len(content)-1]); !errors.Is(err, errChangelogCorrupt) {
			t.Fatalf("a truncated changelog decoded, err %v", err)
		}
	}
}

func TestNextChangelogShiftsAndDrops(t *testing.T) {
	table := compileTestTable(t)
	stored := []int64{1, 2, 3, 4, 5, 6, 7}
	written := slices.Clone(stored)
	written[yearIndex] = 30
	first, _ := table.NextChangelog(nil, 100, 100, stored, written)
	entries, _ := decodeChangelog(first)
	if len(entries) != 1 || entries[0].delta != 0 || len(entries[0].values) != 1 || entries[0].values[0] != (changelogValue{columnID: uint8(yearColumn.ColumnID), value: 2}) {
		t.Fatalf("one changed column gives one entry with its stored value, got %+v", entries)
	}
	// An unchanged write only shifts the deltas.
	second, _ := table.NextChangelog(first, 100, 1100, written, written)
	if entries, _ = decodeChangelog(second); len(entries) != 1 || entries[0].delta != 1000 {
		t.Fatalf("shifted entry: %+v", entries)
	}
	// A write past the reach drops it; a lagging clock shifts nothing.
	if third, _ := table.NextChangelog(second, 1100, 1100+MaxDelta-999, written, written); third != nil {
		t.Fatalf("an entry past %d ticks must be dropped, got %x", MaxDelta, third)
	}
	if third, _ := table.NextChangelog(second, 1100, 1000, written, written); !slices.Equal(third, second) {
		t.Fatal("a write stamped before the stored Updated must not shift the deltas")
	}
}

// ── Simulation: a random history of 60 records, as a backend would store it ──

// simStart is the SUnixTime the simulated history starts at: just after the real clock, so the lock
// deadlines a run derives from its simulated now are still ahead.
var simStart = utils.SUnixTimeOf(time.Now()) + 1000

// simWrite is one landed write of a record, as stored: its tracked values, Updated, created and changelog.
type simWrite struct {
	values    []int64
	updated   int64
	created   int64
	changelog []byte
}

type simHistory struct {
	table            *Table
	writesByEmployee map[int64][]simWrite
}

// simulateWrites writes records for duration ticks: inserts, updates of any tracked column (floats and
// negative Points included), soft deletes and restores, and writes that change nothing tracked.
func simulateWrites(t *testing.T, table *Table, rng *rand.Rand, duration int64) simHistory {
	sim := simHistory{table: table, writesByEmployee: map[int64][]simWrite{}}
	for tick := int64(simStart + 1); tick < simStart+duration; tick += 1 + rng.Int64N(30) {
		employeeID := int64(rng.IntN(60))
		previousWrites := sim.writesByEmployee[employeeID]
		write := simWrite{values: []int64{employeeID, 0, 0, 0, 0, 1, 0}, updated: tick, created: tick}
		if len(previousWrites) > 0 {
			stored := previousWrites[len(previousWrites)-1]
			write.values, write.created = slices.Clone(stored.values), stored.created
		}
		switch roll := rng.IntN(8); {
		case len(previousWrites) == 0 || roll == 0:
			write.values[yearIndex], write.values[programIndex] = int64(2025+rng.IntN(3)), int64(rng.IntN(5))
		case roll == 1:
			write.values[statusIndex] = 1 - write.values[statusIndex]
		}
		if len(previousWrites) == 0 || rng.IntN(3) == 0 {
			write.values[minutesIndex], write.values[pointsIndex] = int64(rng.IntN(600)), int64(rng.IntN(300)-100)
			write.values[hoursIndex], _ = ScaledFloat(float64(rng.IntN(1000)) / 8)
		}
		if len(previousWrites) > 0 {
			stored := previousWrites[len(previousWrites)-1]
			var err error
			if write.changelog, err = table.NextChangelog(stored.changelog, stored.updated, tick, stored.values, write.values); err != nil {
				t.Fatal(err)
			}
		}
		sim.writesByEmployee[employeeID] = append(previousWrites, write)
	}
	return sim
}

// storedAt is each record as stored at moment: the last write up to it.
func (sim simHistory) storedAt(moment int64) []simWrite {
	var stored []simWrite
	for _, writes := range sim.writesByEmployee {
		for i := len(writes) - 1; i >= 0; i-- {
			if writes[i].updated <= moment {
				stored = append(stored, writes[i])
				break
			}
		}
	}
	return stored
}

// historiesAt is what a read at moment returns of the records written after since.
func (sim simHistory) historiesAt(moment, since int64) []History {
	var histories []History
	for _, write := range sim.storedAt(moment) {
		if write.updated > since {
			history, _ := NewHistory(fmt.Sprint(write.values[employeeIndex]), write.values, write.created, write.updated, write.changelog)
			histories = append(histories, history)
		}
	}
	return histories
}

// bruteForceRows sums the records as stored at moment.
func (sim simHistory) bruteForceRows(frame *Frame, moment int64) []Row {
	rowsByFileKey := map[string]frameRows{}
	var fileKeys []string
	for _, write := range sim.storedAt(moment) {
		values := sim.table.valuesOf(frame, write.values)
		if values == nil {
			continue
		}
		fileKey := frame.fileKeyOf("g", values.keys)
		if rowsByFileKey[fileKey] == nil {
			rowsByFileKey[fileKey] = frameRows{}
			fileKeys = append(fileKeys, fileKey)
		}
		rowsByFileKey[fileKey].add(values, 1)
	}
	return frame.rowsOf("g", fileKeys, rowsByFileKey)
}

func TestValuesAtMatchesTheHistory(t *testing.T) {
	table := compileTestTable(t)
	sim := simulateWrites(t, table, rand.New(rand.NewPCG(3, 4)), 30000)
	histories := sim.historiesAt(simStart+30000, -1)
	for moment := int64(simStart - 5); moment <= simStart+30000; moment += 37 {
		storedByEmployee := map[int64]simWrite{}
		for _, write := range sim.storedAt(moment) {
			storedByEmployee[write.values[employeeIndex]] = write
		}
		for _, history := range histories {
			stored, existed := storedByEmployee[history.values[employeeIndex]]
			values, exists := table.valuesAt(history, moment)
			if exists != existed || (exists && !slices.Equal(values, stored.values)) {
				t.Fatalf("moment %d: values %v (%v), want %v (%v)", moment, values, exists, stored.values, existed)
			}
		}
	}
}

// memoryBackend keeps the frame's state in memory and answers the records as the simulation stored
// them at readAt.
type memoryBackend struct {
	sim    simHistory
	readAt int64
	state  State
	holder string
}

func (backend *memoryBackend) ReadState(frame *Frame) (State, error) { return backend.state, nil }
func (backend *memoryBackend) TakeLock(frame *Frame, holderID string, now, lockedUntil time.Time) (State, bool, error) {
	backend.holder = holderID
	return backend.state, true, nil
}
func (backend *memoryBackend) ReleaseLock(frame *Frame, holderID string) { backend.holder = "" }
func (backend *memoryBackend) CommitSnapshot(ctx context.Context, frame *Frame, holderID string, state State, snapshot int64) error {
	if holderID != backend.holder || state != backend.state {
		return errors.New("lock or state moved")
	}
	backend.state.Snapshot = snapshot
	return nil
}
func (backend *memoryBackend) CommitGeneration(ctx context.Context, frame *Frame, holderID string, generation string, snapshot int64) error {
	backend.state = State{Generation: generation, Shape: frame.Shape, Snapshot: snapshot}
	return nil
}
func (backend *memoryBackend) HistoriesWrittenAfter(moment int64) ([]History, error) {
	return backend.sim.historiesAt(backend.readAt, moment), nil
}
func (backend *memoryBackend) ScanHistories(onPage func([]History) error) error {
	return onPage(backend.sim.historiesAt(backend.readAt, -1))
}

// crashingFileStore is utils.MemoryFileStore whose writes fail half the time while isCrashing, like
// a run whose lock expires halfway.
type crashingFileStore struct {
	*utils.MemoryFileStore
	rngMutex   sync.Mutex // a run writes its files in parallel
	rng        *rand.Rand
	isCrashing bool
}

func (store *crashingFileStore) PutFileIfMatch(ctx context.Context, key string, content []byte, etag string) (string, error) {
	store.rngMutex.Lock()
	isCrashed := store.isCrashing && store.rng.IntN(2) == 0
	store.rngMutex.Unlock()
	if isCrashed {
		return "", errors.New("crashed")
	}
	return store.MemoryFileStore.PutFileIfMatch(ctx, key, content, etag)
}

func timeOfSUnix(moment int64) time.Time { return time.Unix(moment*2+1e9, 0) }

// readAll is every row of the test frame (its Years are 2025..2027).
func readAll(t *testing.T, table *Table, backend Backend, store utils.FileStore, isFresh bool) []Row {
	t.Helper()
	rows, err := Read(table, &table.Frames[0], backend, store, Selection{FromKey: 2025, ToKey: 2027}, isFresh)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// TestRunsSurviveCrashes: a rebuild, then runs at random moments, some crashing after a random part of
// their files, leave the files equal to the brute force at their target, and a fresh read equal to it
// at the read's moment.
func TestRunsSurviveCrashes(t *testing.T) {
	table := compileTestTable(t)
	frame := &table.Frames[0]
	for seed := range uint64(30) {
		rng := rand.New(rand.NewPCG(seed, 99))
		sim := simulateWrites(t, table, rng, 40000)
		store := &crashingFileStore{MemoryFileStore: utils.NewMemoryFileStore(), rng: rng}
		backend := &memoryBackend{sim: sim}

		// The first Materialize builds the frame; each record is read a little after the target.
		now := simStart + 2000 + rng.Int64N(3000)
		backend.readAt = now + rng.Int64N(30)
		if err := Materialize(table, backend, store, timeOfSUnix(now)); err != nil {
			t.Fatal(err)
		}
		if got, want := readAll(t, table, backend, store, false), sim.bruteForceRows(frame, now-TargetLag); !reflect.DeepEqual(got, want) {
			t.Fatalf("seed %d: the rebuild at %d differs from the brute force", seed, now-TargetLag)
		}

		for now += 300; now < simStart+40000; now += 300 + rng.Int64N(1500) {
			backend.readAt = now + rng.Int64N(30)
			store.isCrashing = rng.IntN(3) == 0
			err := Materialize(table, backend, store, timeOfSUnix(now))
			if store.isCrashing = false; err == nil {
				if got, want := readAll(t, table, backend, store, false), sim.bruteForceRows(frame, now-TargetLag); !reflect.DeepEqual(got, want) {
					t.Fatalf("seed %d: the files after the run at %d differ from the brute force", seed, now)
				}
			}
			if got, want := readAll(t, table, backend, store, true), sim.bruteForceRows(frame, backend.readAt); !reflect.DeepEqual(got, want) {
				t.Fatalf("seed %d: the fresh read at %d differs from the brute force", seed, backend.readAt)
			}
		}
	}
}

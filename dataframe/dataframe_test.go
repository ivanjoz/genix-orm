package dataframe

import (
	"context"
	"errors"
	"fmt"
	"maps"
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

// compileTestTable compiles two frames of the same sums: year-program, one file per year and program
// with a row per employee, and year, one file per year with a row per program × employee (nested
// Rows).
func compileTestTable(t *testing.T) *Table {
	t.Helper()
	table, err := Compile("enrollment", 78901377, []Spec{{
		Name: "year-program", Keys: []Column{yearColumn, programColumn}, Rows: []Column{employeeColumn},
		Sums: []Column{minutesColumn, pointsColumn, hoursColumn}, Count: true,
	}, {
		Name: "year", Keys: []Column{yearColumn}, Rows: []Column{programColumn, employeeColumn},
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
	if nested := &table.Frames[1]; len(nested.Rows) != 2 || nested.Rows[0].TrackedIndex != programIndex || nested.Rows[1].TrackedIndex != employeeIndex || nested.Shape == frame.Shape {
		t.Fatalf("compiled nested Rows: %+v", nested)
	}
	employeeRows := []Column{employeeColumn}
	for name, spec := range map[string]Spec{
		"a name with capitals":   {Name: "Year", Keys: []Column{yearColumn}, Rows: employeeRows, Count: true},
		"four Keys":              {Name: "a", Keys: []Column{yearColumn, programColumn, minutesColumn, pointsColumn}, Rows: employeeRows, Count: true},
		"no Rows":                {Name: "a", Keys: []Column{yearColumn}, Count: true},
		"four Rows":              {Name: "a", Keys: []Column{yearColumn}, Rows: []Column{programColumn, minutesColumn, pointsColumn, employeeColumn}, Count: true},
		"neither Sums nor Count": {Name: "a", Keys: []Column{yearColumn}, Rows: employeeRows},
		"a float Key":            {Name: "a", Keys: []Column{hoursColumn}, Rows: employeeRows, Count: true},
		"a float Rows column":    {Name: "a", Keys: []Column{yearColumn}, Rows: []Column{programColumn, hoursColumn}, Count: true},
		"a column twice":         {Name: "a", Keys: []Column{yearColumn}, Rows: employeeRows, Sums: []Column{yearColumn}},
		"a Key in the Rows":      {Name: "a", Keys: []Column{yearColumn}, Rows: []Column{yearColumn, employeeColumn}, Count: true},
		"a column id over 255":   {Name: "a", Keys: []Column{{FieldName: "Wide", ColumnID: 300}}, Rows: employeeRows, Count: true},
	} {
		if _, err := Compile("bad", 1, []Spec{spec}, nil); err == nil {
			t.Fatalf("%s: compiled", name)
		}
	}
	twice := Spec{Name: "a", Keys: []Column{yearColumn}, Rows: employeeRows, Count: true}
	if _, err := Compile("bad", 1, []Spec{twice, twice}, nil); err == nil {
		t.Fatal("two frames with one name compiled")
	}
}

func TestValuesAndTheWriteCheck(t *testing.T) {
	table := compileTestTable(t)
	frame := &table.Frames[0]
	tracked := []int64{4, 2026, 3, 240, 100, 1, 1_500_000}
	values := table.valuesOf(frame, tracked)
	if !slices.Equal(values.keys, []int64{2026, 3}) || values.row != (rowKey{4}) || !slices.Equal(values.sums, []int64{240, 100, 1_500_000, 1}) {
		t.Fatalf("frame values: %+v", values)
	}
	if nestedValues := table.valuesOf(&table.Frames[1], tracked); !slices.Equal(nestedValues.keys, []int64{2026}) || nestedValues.row != (rowKey{3, 4}) {
		t.Fatalf("nested frame values: %+v", nestedValues)
	}
	tracked[statusIndex] = 0
	if table.valuesOf(frame, tracked) != nil {
		t.Fatal("a deleted record (Status 0) counts in no file")
	}
	tracked[statusIndex], tracked[yearIndex] = 1, -1
	if err := table.CheckValues(tracked); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("a negative Keys value must fail the write, got %v", err)
	}
	tracked[yearIndex], tracked[programIndex] = 2026, -1
	if err := table.CheckValues(tracked); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("a negative Rows value must fail the write, got %v", err)
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

// memoryBackend keeps each frame's state in memory and answers the records as the simulation stored
// them at readAt.
type memoryBackend struct {
	sim           simHistory
	readAt        int64
	stateByFrame  map[string]State
	holderByFrame map[string]string
}

func newMemoryBackend(sim simHistory) *memoryBackend {
	return &memoryBackend{sim: sim, stateByFrame: map[string]State{}, holderByFrame: map[string]string{}}
}

func (backend *memoryBackend) ReadState(frame *Frame) (State, error) {
	return backend.stateByFrame[frame.Name], nil
}
func (backend *memoryBackend) TakeLock(frame *Frame, holderID string, now, lockedUntil time.Time) (State, bool, error) {
	backend.holderByFrame[frame.Name] = holderID
	return backend.stateByFrame[frame.Name], true, nil
}
func (backend *memoryBackend) ReleaseLock(frame *Frame, holderID string) {
	delete(backend.holderByFrame, frame.Name)
}
func (backend *memoryBackend) CommitSnapshot(ctx context.Context, frame *Frame, holderID string, state State, snapshot int64) error {
	if holderID != backend.holderByFrame[frame.Name] || state != backend.stateByFrame[frame.Name] {
		return errors.New("lock or state moved")
	}
	state.Snapshot = snapshot
	backend.stateByFrame[frame.Name] = state
	return nil
}
func (backend *memoryBackend) CommitGeneration(ctx context.Context, frame *Frame, holderID string, generation string, snapshot int64) error {
	backend.stateByFrame[frame.Name] = State{Generation: generation, Shape: frame.Shape, Snapshot: snapshot}
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

// readAll is every row of a test frame (its Years are 2025..2027).
func readAll(t *testing.T, table *Table, frame *Frame, backend Backend, store utils.FileStore, isFresh bool) []Row {
	t.Helper()
	rows, err := Read(table, frame, backend, store, Selection{FromKey: 2025, ToKey: 2027}, isFresh)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// simClient is a browser keeping raw frame files from ReadChanged answers.
type simClient struct {
	generation string
	fileByKeys map[string]ChangedFile
}

// sync asks for the selection's changed files and applies the answer.
func (client *simClient) sync(t *testing.T, table *Table, frame *Frame, backend Backend, store utils.FileStore, now int64) {
	t.Helper()
	var held []HeldFile
	for _, file := range client.fileByKeys {
		held = append(held, HeldFile{Keys: file.Keys, Snapshot: file.Snapshot, Slot: file.Slot})
	}
	// Through the wire, both ways, as the browser does.
	parsedHeld, err := ParseHeldFiles(FormatHeldFiles(held))
	if err != nil {
		t.Fatal(err)
	}
	answer, err := ReadChanged(table, frame, backend, store, Selection{FromKey: 2025, ToKey: 2027}, client.generation, parsedHeld, now)
	if err != nil {
		t.Fatal(err)
	}
	if answer, err = DecodeChangedFiles(AppendChangedFiles(nil, answer)); err != nil {
		t.Fatal(err)
	}
	if answer.Generation != client.generation {
		client.generation, client.fileByKeys = answer.Generation, map[string]ChangedFile{}
	}
	for _, file := range answer.Files {
		client.fileByKeys[fmt.Sprint(file.Keys)] = file
	}
}

// rows decodes the held files into rows, by Keys then by RowValues, as Read returns them.
func (client *simClient) rows(t *testing.T, frame *Frame) []Row {
	t.Helper()
	files := slices.Collect(maps.Values(client.fileByKeys))
	slices.SortFunc(files, func(a, b ChangedFile) int { return slices.Compare(a.Keys, b.Keys) })
	var rows []Row
	for _, changed := range files {
		if changed.Content == nil {
			continue
		}
		file, err := DecodeFile(changed.Content, len(frame.Rows), frame.SumsCount)
		if err != nil {
			t.Fatal(err)
		}
		for i := range file.rowCount() {
			row := Row{Keys: changed.Keys, RowValues: make([]int64, len(frame.Rows)), Sums: make([]int64, frame.SumsCount)}
			for c := range row.RowValues {
				row.RowValues[c] = file.Rows[c][i]
			}
			for s := range row.Sums {
				row.Sums[s] = file.Sums[s][i]
			}
			rows = append(rows, row)
		}
	}
	return rows
}

// TestRunsSurviveCrashes: a rebuild, then runs at random moments, some crashing after a random part of
// their files, leave the files equal to the brute force at their target, and a fresh read equal to it
// at the read's moment, for both test frames. A client syncing raw files with ReadChanged holds the
// brute force at every read too, through occasional rebuilds; half the seeds cycle 2 slots, so every
// slot is shared.
func TestRunsSurviveCrashes(t *testing.T) {
	table := compileTestTable(t)
	defer func() { slotCycle = slotCount }()
	for seed := range uint64(30) {
		slotCycle = slotCount
		if seed%2 == 1 {
			slotCycle = 2
		}
		rng := rand.New(rand.NewPCG(seed, 99))
		sim := simulateWrites(t, table, rng, 40000)
		store := &crashingFileStore{MemoryFileStore: utils.NewMemoryFileStore(), rng: rng}
		backend := newMemoryBackend(sim)
		clients := make([]*simClient, len(table.Frames))
		for i := range clients {
			clients[i] = &simClient{fileByKeys: map[string]ChangedFile{}}
		}

		// The first Materialize builds the frame; each record is read a little after the target.
		now := simStart + 2000 + rng.Int64N(3000)
		backend.readAt = now + rng.Int64N(30)
		if err := Materialize(table, backend, store, timeOfSUnix(now)); err != nil {
			t.Fatal(err)
		}
		for i := range table.Frames {
			frame := &table.Frames[i]
			if got, want := readAll(t, table, frame, backend, store, false), sim.bruteForceRows(frame, now-TargetLag); !reflect.DeepEqual(got, want) {
				t.Fatalf("seed %d frame %s: the rebuild at %d differs from the brute force", seed, frame.Name, now-TargetLag)
			}
		}

		for now += 300; now < simStart+40000; now += 300 + rng.Int64N(1500) {
			backend.readAt = now + rng.Int64N(30)
			store.isCrashing = rng.IntN(3) == 0
			err := Materialize(table, backend, store, timeOfSUnix(now))
			store.isCrashing = false
			if rng.IntN(12) == 0 {
				if err := Rebuild(table, backend, store, "", timeOfSUnix(now)); err != nil {
					t.Fatal(err)
				}
			}
			for i := range table.Frames {
				frame := &table.Frames[i]
				if err == nil {
					if got, want := readAll(t, table, frame, backend, store, false), sim.bruteForceRows(frame, now-TargetLag); !reflect.DeepEqual(got, want) {
						t.Fatalf("seed %d frame %s: the files after the run at %d differ from the brute force", seed, frame.Name, now)
					}
				}
				want := sim.bruteForceRows(frame, backend.readAt)
				if got := readAll(t, table, frame, backend, store, true); !reflect.DeepEqual(got, want) {
					t.Fatalf("seed %d frame %s: the fresh read at %d differs from the brute force", seed, frame.Name, backend.readAt)
				}
				if rng.IntN(3) > 0 { // the client doesn't look at every moment
					clients[i].sync(t, table, frame, backend, store, backend.readAt)
					if got := clients[i].rows(t, frame); !reflect.DeepEqual(got, want) {
						t.Fatalf("seed %d frame %s (slot cycle %d): the client's files at %d differ from the brute force", seed, frame.Name, slotCycle, backend.readAt)
					}
				}
			}
		}
	}
}

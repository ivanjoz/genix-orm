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
	"unsafe"

	"github.com/ivanjoz/genix-orm/utils"
)

// ── Test table: enrollments summed per year and program, one row per employee ──

var (
	employeeColumn = Column{FieldName: "EmployeeID", ColumnID: 2}
	yearColumn     = Column{FieldName: "Year", ColumnID: 3}
	programColumn  = Column{FieldName: "ProgramID", ColumnID: 4}
	minutesColumn  = Column{FieldName: "Minutes", ColumnID: 5}
	pointsColumn   = Column{FieldName: "Points", ColumnID: 6}
	hoursColumn    = Column{FieldName: "Hours", ColumnID: 9, IsFloat: true}
)

// testRecord is an enrollment as a backend stores it. TagIDs is a list column: the "tags" frame turns
// it into one line per tag (a tag listed twice adds twice).
type testRecord struct {
	EmployeeID int64
	Year       int64
	ProgramID  int64
	Minutes    int64
	Points     int64
	Status     int64
	Hours      float64
	TagIDs     []int64
}

func (record testRecord) clone() testRecord {
	record.TagIDs = slices.Clone(record.TagIDs)
	return record
}

// testTagFrame is one line of the "tags" frame, declared by a ToFrames method.
type testTagFrame struct {
	Year    int64  `frame:"key"`
	TagID   int32  `frame:"row"`
	Ignored string // untagged: not part of the frame
	Minutes int64  `frame:"sum"`
	Points  int32  `frame:"sum"`
}

func (record *testRecord) toTagFrames(frames []testTagFrame) []testTagFrame {
	if record.Status == 0 {
		return frames
	}
	for _, tagID := range record.TagIDs {
		frames = append(frames, testTagFrame{Year: record.Year, TagID: int32(tagID), Minutes: record.Minutes, Points: int32(record.Points)})
	}
	return frames
}

// enrollmentLinesOf is the one line of the two column frames, as a backend builds it from their
// columns: Year, ProgramID, EmployeeID, then the sums (both frames take that order, the Keys and Rows
// split differs); none for a soft-deleted record.
func enrollmentLinesOf(recordPointer unsafe.Pointer) ([]Line, error) {
	record := (*testRecord)(recordPointer)
	if record.Status == 0 {
		return nil, nil
	}
	hours, _ := ScaledFloat(record.Hours)
	return []Line{{record.Year, record.ProgramID, record.EmployeeID, record.Minutes, record.Points, hours}}, nil
}

var tagProjection = ProjectionOf((*testRecord).toTagFrames)

// compileTestTable compiles three frames: year-program, one file per year and program with a row per
// employee; year, one file per year with a row per program × employee (nested Rows) and the zero sums
// mask; and tags, declared by a ToFrames method, 0..N lines per record.
func compileTestTable(t *testing.T) *Table {
	t.Helper()
	table, err := Compile("enrollment", 78901377, []Spec{{
		Name: "year-program", Keys: []Column{yearColumn, programColumn}, Rows: []Column{employeeColumn},
		Sums: []Column{minutesColumn, pointsColumn, hoursColumn}, Count: true, LinesOf: enrollmentLinesOf,
	}, {
		Name: "year", Keys: []Column{yearColumn}, Rows: []Column{programColumn, employeeColumn},
		Sums: []Column{minutesColumn, pointsColumn, hoursColumn}, Count: true, UseZeroSumsMask: true, LinesOf: enrollmentLinesOf,
	}, {
		Name: "tags", Projection: tagProjection, Count: true, UseZeroSumsMask: true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func TestCompileRules(t *testing.T) {
	table := compileTestTable(t)
	frame := &table.Frames[0]
	if table.Folder != "dtf/3gz10/" || table.versionsKeyOf(12) != "dtf/3gz10/12.versions" || frame.Folder != "dtf/3gz10/year-program/" || frame.SumsCount != 4 || len(frame.Shape) != 8 || frame.Index != 0 {
		t.Fatalf("compiled: %+v", table)
	}
	if nested := &table.Frames[1]; len(nested.Rows) != 2 || !nested.UsesZeroSumsMask || nested.Shape == frame.Shape || nested.Index != 1 {
		t.Fatalf("compiled nested Rows: %+v", nested)
	}
	if tags := &table.Frames[2]; !tags.IsProjected || len(tags.Keys) != 1 || tags.Keys[0].FieldName != "Year" || tags.Rows[0].FieldName != "TagID" || len(tags.Sums) != 2 || tags.Sums[1].ColumnID != 5 {
		t.Fatalf("compiled projection: %+v", tags)
	}
	if _, err := Compile("bad", 1, []Spec{{Name: "a", Projection: tagProjection, Sums: []Column{minutesColumn}}}); err == nil {
		t.Fatal("a ToFrames frame that also lists columns compiled")
	}
	employeeRows := []Column{employeeColumn}
	nineSums := []Column{{FieldName: "S1", ColumnID: 11}, {FieldName: "S2", ColumnID: 12}, {FieldName: "S3", ColumnID: 13}, {FieldName: "S4", ColumnID: 14},
		{FieldName: "S5", ColumnID: 15}, {FieldName: "S6", ColumnID: 16}, {FieldName: "S7", ColumnID: 17}, {FieldName: "S8", ColumnID: 18}}
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
		"9 masked sums":          {Name: "a", Keys: []Column{yearColumn}, Rows: employeeRows, Sums: nineSums, Count: true, UseZeroSumsMask: true},
	} {
		spec.LinesOf = enrollmentLinesOf
		if _, err := Compile("bad", 1, []Spec{spec}); err == nil {
			t.Fatalf("%s: compiled", name)
		}
	}
	if _, err := Compile("ok", 1, []Spec{{Name: "a", Keys: []Column{yearColumn}, Rows: employeeRows, Sums: nineSums, Count: true, LinesOf: enrollmentLinesOf}}); err != nil {
		t.Fatalf("9 sums without the mask: %v", err)
	}
	if _, err := Compile("bad", 1, []Spec{{Name: "a", Keys: []Column{yearColumn}, Rows: employeeRows, Count: true}}); err == nil {
		t.Fatal("a frame without LinesOf compiled")
	}
	twice := Spec{Name: "a", Keys: []Column{yearColumn}, Rows: employeeRows, Count: true, LinesOf: enrollmentLinesOf}
	if _, err := Compile("bad", 1, []Spec{twice, twice}); err == nil {
		t.Fatal("two frames with one name compiled")
	}
	versioned := twice
	versioned.Version = 2
	if versionedTable, _ := Compile("ok", 1, []Spec{versioned}); versionedTable.Frames[0].Shape == mustCompile(t, twice).Frames[0].Shape {
		t.Fatal("a Version bump must change the shape")
	}
}

func mustCompile(t *testing.T, specs ...Spec) *Table {
	t.Helper()
	table, err := Compile("t", 1, specs)
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func TestLinesAndTheWriteCheck(t *testing.T) {
	table := compileTestTable(t)
	record := testRecord{EmployeeID: 4, Year: 2026, ProgramID: 3, Minutes: 240, Points: 100, Status: 1, Hours: 1.5, TagIDs: []int64{7, 9, 7}}
	lines, err := table.LinesOf(unsafe.Pointer(&record))
	if err != nil {
		t.Fatal(err)
	}
	if want := (Line{2026, 3, 4, 240, 100, 1_500_000}); !slices.Equal(lines[0][0], want) || !slices.Equal(lines[1][0], want) {
		t.Fatalf("column lines: %v", lines)
	}
	frame := &table.Frames[0]
	if line := lines[0][0]; !slices.Equal(frame.keysOf(line), []int64{2026, 3}) || frame.rowOf(line) != (rowKey{4}) || !slices.Equal(frame.sumsOf(line), []int64{240, 100, 1_500_000}) {
		t.Fatalf("line parts: %v", line)
	}
	if nested := &table.Frames[1]; nested.rowOf(lines[1][0]) != (rowKey{3, 4}) {
		t.Fatalf("nested row of %v", lines[1][0])
	}
	if len(lines[2]) != 3 || !slices.Equal(lines[2][2], Line{2026, 7, 240, 100}) {
		t.Fatalf("projected lines: %v", lines[2])
	}
	record.Status = 0
	if lines, _ := table.LinesOf(unsafe.Pointer(&record)); lines[0] != nil || lines[2] != nil {
		t.Fatalf("a deleted record puts no lines, got %v", lines)
	}
	record.Status, record.Year = 1, -1
	if _, err := table.LinesOf(unsafe.Pointer(&record)); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("a negative Keys value must fail the write, got %v", err)
	}
	record.Year, record.ProgramID = 2026, -1
	if _, err := table.LinesOf(unsafe.Pointer(&record)); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("a negative Rows value must fail the write, got %v", err)
	}
	if _, isKept := ScaledFloat(1e13); isKept {
		t.Fatal("a float past ±1.1e12 must not be kept")
	}
}

func TestFileKeysRoundTrip(t *testing.T) {
	frame := &compileTestTable(t).Frames[0]
	fileKey := frame.fileKeyOf("g1", 12, []int64{2026, 3})
	if fileKey != "dtf/3gz10/year-program/12/g1/2026/3" {
		t.Fatalf("file key %q", fileKey)
	}
	if keys, isFrameFile := frame.keysOfFile("g1", 12, fileKey); !isFrameFile || !slices.Equal(keys, []int64{2026, 3}) {
		t.Fatalf("keys of %q: %v %v", fileKey, keys, isFrameFile)
	}
	for _, otherKey := range []string{frame.Folder + "12/g2/2026/3", frame.Folder + "12/g1/2026", frame.Folder + "13/g1/2026/3", frame.Folder + "12/g1/-1/3"} {
		if _, isFrameFile := frame.keysOfFile("g1", 12, otherKey); isFrameFile {
			t.Fatalf("%q read as a file of partition 12 generation g1", otherKey)
		}
	}
}

// ── Changelog ──

func linesOfRecord(t *testing.T, table *Table, record testRecord) RecordLines {
	t.Helper()
	lines, err := table.LinesOf(unsafe.Pointer(&record))
	if err != nil {
		t.Fatal(err)
	}
	return lines
}

func TestChangelogRoundTrip(t *testing.T) {
	table := compileTestTable(t)
	stored := testRecord{EmployeeID: 4, Year: 2026, ProgramID: 3, Minutes: 240, Points: -100, Status: 1, Hours: 1.5, TagIDs: []int64{7, 9}}
	written := stored.clone()
	written.Minutes, written.TagIDs = 300, nil
	storedLines, writtenLines := linesOfRecord(t, table, stored), linesOfRecord(t, table, written)
	content, err := table.NextChangelog(nil, 100, 100, storedLines, writtenLines)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := table.decodeChangelog(content)
	if err != nil || len(entries) != 1 || len(entries[0].frames) != 3 || !reflect.DeepEqual(entries[0].frames[2].lines, storedLines[2]) {
		t.Fatalf("decoded %+v, err %v", entries, err)
	}
	if _, err := table.decodeChangelog(content[:len(content)-1]); !errors.Is(err, errChangelogCorrupt) {
		t.Fatalf("a truncated changelog decoded, err %v", err)
	}
	// A frame of another shape (a frame since redeclared) is skipped by its length.
	redeclared := mustCompile(t, Spec{Name: "year-program", Keys: []Column{yearColumn, programColumn}, Rows: []Column{employeeColumn},
		Sums: []Column{minutesColumn, pointsColumn, hoursColumn}, Count: true, Version: 2, LinesOf: enrollmentLinesOf})
	if entries, err := redeclared.decodeChangelog(content); err != nil || len(entries) != 1 || len(entries[0].frames) != 0 {
		t.Fatalf("a changelog of other shapes decoded to %+v, err %v", entries, err)
	}
	// Only the changed frames: a write that leaves the tags alone logs the two column frames.
	untagged := written.clone()
	untagged.Points = 5
	if body := table.EncodeChangedLines(writtenLines, linesOfRecord(t, table, untagged)); body == nil {
		t.Fatal("a changed sum must log its frames")
	} else if frames, length, err := table.readBody(body); err != nil || length != len(body) || len(frames) != 2 || frames[0].frameIndex != 0 || frames[1].frameIndex != 1 {
		t.Fatalf("body frames %+v, err %v", frames, err)
	}
	if body := table.EncodeChangedLines(writtenLines, writtenLines); body != nil {
		t.Fatal("an unchanged write logs nothing")
	}
}

func TestNextChangelogShiftsAndDrops(t *testing.T) {
	table := compileTestTable(t)
	stored := testRecord{EmployeeID: 1, Year: 2026, ProgramID: 3, Status: 1}
	written := stored.clone()
	written.Year = 2027
	storedLines, writtenLines := linesOfRecord(t, table, stored), linesOfRecord(t, table, written)
	first, _ := table.NextChangelog(nil, 100, 100, storedLines, writtenLines)
	entries, _ := table.decodeChangelog(first)
	if len(entries) != 1 || entries[0].delta != 0 || len(entries[0].frames) != 2 || !reflect.DeepEqual(entries[0].frames[0].lines, storedLines[0]) {
		t.Fatalf("one changed record gives one entry with its stored lines, got %+v", entries)
	}
	// An unchanged write only shifts the deltas.
	second, _ := table.NextChangelog(first, 100, 1100, writtenLines, writtenLines)
	if entries, _ = table.decodeChangelog(second); len(entries) != 1 || entries[0].delta != 1000 {
		t.Fatalf("shifted entry: %+v", entries)
	}
	// A write past the reach drops it; a lagging clock shifts nothing.
	if third, _ := table.NextChangelog(second, 1100, 1100+MaxDelta-999, writtenLines, writtenLines); third != nil {
		t.Fatalf("an entry past %d ticks must be dropped, got %x", MaxDelta, third)
	}
	if third, _ := table.NextChangelog(second, 1100, 1000, writtenLines, writtenLines); !slices.Equal(third, second) {
		t.Fatal("a write stamped before the stored Updated must not shift the deltas")
	}
}

// ── Simulation: a random history of 60 records, as a backend would store it ──

// simStart is the SUnixTime the simulated history starts at: just after the real clock, so the lock
// deadlines a run derives from its simulated now are still ahead.
var simStart = utils.SUnixTimeOf(time.Now()) + 1000

// simWrite is one landed write of a record, as stored: the record, Updated, created and changelog.
type simWrite struct {
	record    testRecord
	updated   int64
	created   int64
	changelog []byte
}

type simHistory struct {
	t                *testing.T
	table            *Table
	writesByEmployee map[int64][]simWrite
}

// simulateWrites writes records for duration ticks: inserts, updates of any field (floats and
// negative Points included), tags added and dropped, soft deletes and restores, and writes that change
// nothing the frames read.
func simulateWrites(t *testing.T, table *Table, rng *rand.Rand, duration int64) simHistory {
	sim := simHistory{t: t, table: table, writesByEmployee: map[int64][]simWrite{}}
	for tick := int64(simStart + 1); tick < simStart+duration; tick += 1 + rng.Int64N(30) {
		employeeID := int64(rng.IntN(60))
		previousWrites := sim.writesByEmployee[employeeID]
		write := simWrite{record: testRecord{EmployeeID: employeeID, Status: 1}, updated: tick, created: tick}
		if len(previousWrites) > 0 {
			stored := previousWrites[len(previousWrites)-1]
			write.record, write.created = stored.record.clone(), stored.created
		}
		switch roll := rng.IntN(8); {
		case len(previousWrites) == 0 || roll == 0:
			write.record.Year, write.record.ProgramID = int64(2025+rng.IntN(3)), int64(rng.IntN(5))
		case roll == 1:
			write.record.Status = 1 - write.record.Status
		case roll == 2:
			write.record.TagIDs = nil
			for range rng.IntN(4) {
				write.record.TagIDs = append(write.record.TagIDs, int64(rng.IntN(6)))
			}
		}
		if len(previousWrites) == 0 || rng.IntN(3) == 0 {
			write.record.Minutes, write.record.Points = int64(rng.IntN(600)), int64(rng.IntN(300)-100)
			write.record.Hours = float64(rng.IntN(1000)) / 8
		}
		if len(previousWrites) > 0 {
			stored := previousWrites[len(previousWrites)-1]
			var err error
			if write.changelog, err = table.NextChangelog(stored.changelog, stored.updated, tick, linesOfRecord(t, table, stored.record), linesOfRecord(t, table, write.record)); err != nil {
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

// simPartitions: a record's partition is its employee id modulo it, so the simulated table has several.
const simPartitions = 3

func simPartitionOf(write simWrite) int64 { return write.record.EmployeeID % simPartitions }

// historiesAt is what a read at moment returns of the records written after since.
func (sim simHistory) historiesAt(moment, since int64) []History {
	var histories []History
	for _, write := range sim.storedAt(moment) {
		if write.updated > since {
			history, err := sim.table.NewHistory(simPartitionOf(write), fmt.Sprint(write.record.EmployeeID), linesOfRecord(sim.t, sim.table, write.record), write.created, write.updated, write.changelog)
			if err != nil {
				sim.t.Fatal(err)
			}
			histories = append(histories, history)
		}
	}
	return histories
}

// partitionHistories keeps the histories of one partition.
func partitionHistories(histories []History, partition int64) []History {
	return slices.DeleteFunc(histories, func(history History) bool { return history.Partition != partition })
}

// bruteForceRows sums the records of a partition as stored at moment.
func (sim simHistory) bruteForceRows(frame *Frame, partition, moment int64) []Row {
	rowsByFileKey := map[string]*frameRows{}
	var fileKeys []string
	for _, write := range sim.storedAt(moment) {
		if simPartitionOf(write) != partition {
			continue
		}
		for _, line := range linesOfRecord(sim.t, sim.table, write.record)[frame.Index] {
			fileKey := frame.fileKeyOf("g", partition, frame.keysOf(line))
			if rowsByFileKey[fileKey] == nil {
				rowsByFileKey[fileKey] = rowsOfFile(File{})
				fileKeys = append(fileKeys, fileKey)
			}
			rowsByFileKey[fileKey].add(frame, line, 1)
		}
	}
	return frame.rowsOf("g", partition, fileKeys, rowsByFileKey)
}

// assertLinesAt checks a History against what the record was stored as at moment (nil: not yet).
func assertLinesAt(t *testing.T, table *Table, history History, stored *testRecord, moment int64) {
	t.Helper()
	for i := range table.Frames {
		lines, exists := history.linesAt(&table.Frames[i], moment)
		if exists != (stored != nil) {
			t.Fatalf("moment %d frame %d: exists %v, want %v", moment, i, exists, stored != nil)
		}
		if exists && !isSameLines(lines, linesOfRecord(t, table, *stored)[i]) {
			t.Fatalf("moment %d frame %d: lines %v, want %v", moment, i, lines, linesOfRecord(t, table, *stored)[i])
		}
	}
}

func TestLinesAtMatchesTheHistory(t *testing.T) {
	table := compileTestTable(t)
	sim := simulateWrites(t, table, rand.New(rand.NewPCG(3, 4)), 30000)
	histories := sim.historiesAt(simStart+30000, -1)
	for moment := int64(simStart - 5); moment <= simStart+30000; moment += 37 {
		storedByEmployee := map[string]testRecord{}
		for _, write := range sim.storedAt(moment) {
			storedByEmployee[fmt.Sprint(write.record.EmployeeID)] = write.record
		}
		for _, history := range histories {
			var stored *testRecord
			if record, isStored := storedByEmployee[history.RecordKey]; isStored {
				stored = &record
			}
			assertLinesAt(t, table, history, stored, moment)
		}
	}
}

// TestHistoryOfChangesMatchesTheHistory: a History built from a record's lines now and the changes a
// log keeps after a window's start (scylla: the body of every write that changed a frame's lines, the
// insert flagged) says what the record put in its frames at every moment from that start on.
func TestHistoryOfChangesMatchesTheHistory(t *testing.T) {
	table := compileTestTable(t)
	sim := simulateWrites(t, table, rand.New(rand.NewPCG(5, 6)), 30000)
	for windowStart := int64(simStart - 5); windowStart < simStart+30000; windowStart += 4111 {
		for _, writes := range sim.writesByEmployee {
			var changes []HistoryChange // newest first, as the backend hands them
			for i := len(writes) - 1; i >= 0 && writes[i].updated > windowStart; i-- {
				if i == 0 {
					changes = append(changes, HistoryChange{Moment: writes[i].updated, IsInsert: true})
					continue
				}
				// Only real changes are logged.
				if body := table.EncodeChangedLines(linesOfRecord(t, table, writes[i-1].record), linesOfRecord(t, table, writes[i].record)); body != nil {
					changes = append(changes, HistoryChange{Moment: writes[i].updated, Body: body})
				}
			}
			if len(changes) == 0 {
				continue
			}
			history, err := table.NewHistoryOfChanges(simPartitionOf(writes[0]), "r", linesOfRecord(t, table, writes[len(writes)-1].record), changes)
			if err != nil {
				t.Fatal(err)
			}
			for moment := windowStart; moment <= simStart+30000; moment += 13 {
				var stored *testRecord
				for _, write := range writes {
					if write.updated <= moment {
						stored = &write.record
					}
				}
				assertLinesAt(t, table, history, stored, moment)
			}
		}
	}
}

// memoryBackend keeps the state of each frame partition in memory and answers the records as the
// simulation stored them at readAt. compactions counts the locks taken, one per compaction.
type memoryBackend struct {
	sim                    simHistory
	readAt                 int64
	stateByFramePartition  map[string]State
	holderByFramePartition map[string]string
	compactions            int
}

func newMemoryBackend(sim simHistory) *memoryBackend {
	return &memoryBackend{sim: sim, stateByFramePartition: map[string]State{}, holderByFramePartition: map[string]string{}}
}

func framePartitionKey(frame *Frame, partition int64) string {
	return fmt.Sprint(frame.Name, "/", partition)
}

func (backend *memoryBackend) ReadState(frame *Frame, partition int64) (State, error) {
	return backend.stateByFramePartition[framePartitionKey(frame, partition)], nil
}
func (backend *memoryBackend) TakeLock(frame *Frame, partition int64, holderID string, now, lockedUntil time.Time, wait time.Duration) (State, bool, error) {
	backend.compactions++
	backend.holderByFramePartition[framePartitionKey(frame, partition)] = holderID
	return backend.stateByFramePartition[framePartitionKey(frame, partition)], true, nil
}
func (backend *memoryBackend) ReleaseLock(frame *Frame, partition int64, holderID string) {
	delete(backend.holderByFramePartition, framePartitionKey(frame, partition))
}
func (backend *memoryBackend) CommitSnapshot(ctx context.Context, frame *Frame, partition int64, holderID string, state State, snapshot int64) error {
	key := framePartitionKey(frame, partition)
	if holderID != backend.holderByFramePartition[key] || state != backend.stateByFramePartition[key] {
		return errors.New("lock or state moved")
	}
	state.Snapshot = snapshot
	backend.stateByFramePartition[key] = state
	return nil
}
func (backend *memoryBackend) CommitGeneration(ctx context.Context, frame *Frame, partition int64, holderID string, generation string, snapshot int64) error {
	backend.stateByFramePartition[framePartitionKey(frame, partition)] = State{Generation: generation, Shape: frame.Shape, Snapshot: snapshot}
	return nil
}
func (backend *memoryBackend) PartitionHistoriesWrittenAfter(partition int64, moment int64) ([]History, error) {
	return partitionHistories(backend.sim.historiesAt(backend.readAt, moment), partition), nil
}
func (backend *memoryBackend) ScanPartitionHistories(partition int64, onPage func([]History) error) error {
	return onPage(partitionHistories(backend.sim.historiesAt(backend.readAt, -1), partition))
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

// readAll is every row of a test frame in a partition (its Years are 2025..2027), read at now.
func readAll(t *testing.T, table *Table, frame *Frame, backend Backend, store utils.FileStore, partition int64, isFresh bool, now int64) []Row {
	t.Helper()
	rows, err := Read(table, frame, backend, store, partition, Selection{FromKey: 2025, ToKey: 2027}, isFresh, timeOfSUnix(now))
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// simClient is a browser of one partition keeping raw frame files from ReadChanged answers.
type simClient struct {
	partition  int64
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
	answer, err := ReadChanged(table, frame, backend, store, client.partition, Selection{FromKey: 2025, ToKey: 2027}, client.generation, parsedHeld, timeOfSUnix(now))
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
		if file.UsesZeroSumsMask != frame.UsesZeroSumsMask {
			t.Fatalf("frame %s: a file with the zero sums mask %v", frame.Name, file.UsesZeroSumsMask)
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

// TestReadsCompactWhenDue: the first read of a partition builds it; a fresh read compacts it once
// CompactOn.Records records changed before the target, and moves w to the target, and the next read
// doesn't compact again; CompactOn.Seconds compacts a partition with no change; a compaction that finds
// the state moved by the holder before it does nothing. Every read equals the brute force.
func TestReadsCompactWhenDue(t *testing.T) {
	table := mustCompile(t, Spec{Name: "year", Keys: []Column{yearColumn}, Rows: []Column{programColumn, employeeColumn},
		Sums: []Column{minutesColumn, pointsColumn, hoursColumn}, Count: true, LinesOf: enrollmentLinesOf,
		CompactOn: CompactOn{Records: 5, Seconds: 7200}})
	frame := &table.Frames[0]
	sim := simulateWrites(t, table, rand.New(rand.NewPCG(7, 8)), 20000)
	backend, store := newMemoryBackend(sim), utils.NewMemoryFileStore()
	const partition = 1
	readAt := func(now int64) {
		t.Helper()
		backend.readAt = now
		if got, want := readAll(t, table, frame, backend, store, partition, true, now), sim.bruteForceRows(frame, partition, now); !reflect.DeepEqual(got, want) {
			t.Fatalf("the read at %d differs from the brute force", now)
		}
	}

	now := simStart + 2000
	readAt(now)
	if state, _ := backend.ReadState(frame, partition); backend.compactions != 1 || state.Snapshot != now-TargetLag {
		t.Fatalf("the first read must build the partition: %d compactions, state %+v", backend.compactions, state)
	}
	now += 3000 // ~190 writes: most change nothing the frame reads, but over 5 records of the partition do
	readAt(now)
	if state, _ := backend.ReadState(frame, partition); backend.compactions != 2 || state.Snapshot != now-TargetLag {
		t.Fatalf("a read past CompactOn.Records must compact: %d compactions, state %+v", backend.compactions, state)
	}
	readAt(now + 1)
	if backend.compactions != 2 {
		t.Fatal("a read right after a compaction must not compact again")
	}

	// Past the simulated writes nothing changes: only Seconds (3600 ticks) makes the partition due.
	now = simStart + 20000 + 3*TargetLag
	readAt(now)
	compactionsAfterWrites := backend.compactions
	readAt(now + 3599)
	if backend.compactions != compactionsAfterWrites {
		t.Fatal("a read before CompactOn.Seconds must not compact")
	}
	readAt(now + 3600)
	if backend.compactions != compactionsAfterWrites+1 {
		t.Fatal("a read past CompactOn.Seconds must compact")
	}

	stateBefore, _ := backend.ReadState(frame, partition)
	staleState := stateBefore
	staleState.Snapshot--
	if err := table.compactFrame(backend, store, frame, partition, staleState, timeOfSUnix(now+5000)); err != nil {
		t.Fatal(err)
	}
	if stateAfter, _ := backend.ReadState(frame, partition); stateAfter != stateBefore {
		t.Fatal("a compaction that finds the state moved by the holder before it must leave it")
	}
}

// TestRunsSurviveCrashes: a rebuild, then compactions at random moments, some crashing after a random
// part of their files, leave the files equal to the brute force at their target, and a fresh read
// equal to it at the read's moment, for every test frame. A client syncing raw files with ReadChanged
// holds the brute force at every read too, through occasional rebuilds of one partition; half the
// seeds cycle 2 slots, so every slot is shared. Every check runs per partition.
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
		// clients[frame][partition]
		clients := make([][]*simClient, len(table.Frames))
		for i := range clients {
			for partition := range int64(simPartitions) {
				clients[i] = append(clients[i], &simClient{partition: partition, fileByKeys: map[string]ChangedFile{}})
			}
		}

		compactEveryPartition := func(now int64) error {
			var partitionErrors []error
			for partition := range int64(simPartitions) {
				partitionErrors = append(partitionErrors, Compact(table, backend, store, partition, timeOfSUnix(now)))
			}
			return errors.Join(partitionErrors...)
		}

		// The first compaction builds every partition; each record is read a little after the target.
		now := simStart + 2000 + rng.Int64N(3000)
		backend.readAt = now + rng.Int64N(30)
		if err := compactEveryPartition(now); err != nil {
			t.Fatal(err)
		}
		for i := range table.Frames {
			frame := &table.Frames[i]
			for partition := range int64(simPartitions) {
				if got, want := readAll(t, table, frame, backend, store, partition, false, now), sim.bruteForceRows(frame, partition, now-TargetLag); !reflect.DeepEqual(got, want) {
					t.Fatalf("seed %d frame %s partition %d: the rebuild at %d differs from the brute force", seed, frame.Name, partition, now-TargetLag)
				}
			}
		}

		for now += 300; now < simStart+40000; now += 300 + rng.Int64N(1500) {
			backend.readAt = now + rng.Int64N(30)
			store.isCrashing = rng.IntN(3) == 0
			err := compactEveryPartition(now)
			store.isCrashing = false
			if rng.IntN(6) == 0 {
				if err := Rebuild(table, backend, store, rng.Int64N(simPartitions), "", timeOfSUnix(now)); err != nil {
					t.Fatal(err)
				}
			}
			for i := range table.Frames {
				frame := &table.Frames[i]
				for partition := range int64(simPartitions) {
					if err == nil {
						if got, want := readAll(t, table, frame, backend, store, partition, false, now), sim.bruteForceRows(frame, partition, now-TargetLag); !reflect.DeepEqual(got, want) {
							t.Fatalf("seed %d frame %s partition %d: the files after the run at %d differ from the brute force", seed, frame.Name, partition, now)
						}
					}
					want := sim.bruteForceRows(frame, partition, backend.readAt)
					if got := readAll(t, table, frame, backend, store, partition, true, backend.readAt); !reflect.DeepEqual(got, want) {
						t.Fatalf("seed %d frame %s partition %d: the fresh read at %d differs from the brute force", seed, frame.Name, partition, backend.readAt)
					}
					if rng.IntN(3) > 0 { // the client doesn't look at every moment
						client := clients[i][partition]
						client.sync(t, table, frame, backend, store, backend.readAt)
						if got := client.rows(t, frame); !reflect.DeepEqual(got, want) {
							t.Fatalf("seed %d frame %s partition %d (slot cycle %d): the client's files at %d differ from the brute force", seed, frame.Name, partition, slotCycle, backend.readAt)
						}
					}
				}
			}
		}
	}
}

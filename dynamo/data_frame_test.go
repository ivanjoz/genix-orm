package dynamo

import (
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/ivanjoz/genix-orm/dataframe"
)

// ── DataFrame test entity: enrollments summed per year and program, one row per employee. The
// frame logic is tested in the dataframe module; these tests cover what dynamo gives it. ──

type frameEnrollment struct {
	ActivityID int32   `cb:"1"`
	EmployeeID int32   `cb:"2"`
	Year       int16   `cb:"3"`
	ProgramID  int16   `cb:"4"`
	Minutes    int16   `cb:"5"`
	Points     int16   `cb:"6"`
	Status     int8    `cb:"7"`
	Note       string  `cb:"8"`
	Hours      float64 `cb:"9"`
	Updated    int32   `json:"upd" cb:"10"`
	Version    int16   `json:"ver,omitempty" cb:"-"`
}

type frameEnrollmentTable struct {
	Model[frameEnrollmentTable, frameEnrollment]
	ActivityID Col[*frameEnrollmentTable, int32]
	EmployeeID Col[*frameEnrollmentTable, int32]
	Year       Col[*frameEnrollmentTable, int16]
	ProgramID  Col[*frameEnrollmentTable, int16]
	Minutes    Col[*frameEnrollmentTable, int16]
	Points     Col[*frameEnrollmentTable, int16]
	Status     Col[*frameEnrollmentTable, int8]
	Note       Col[*frameEnrollmentTable, string]
	Hours      Col[*frameEnrollmentTable, float64]
	Updated    Col[*frameEnrollmentTable, int32]
}

func (t frameEnrollmentTable) GetSchema() Schema {
	return Schema{
		Entity:  "frame_enrollment",
		TableID: 78901377,
		Keys:    Cols(t.ActivityID.Size(32), t.EmployeeID.Size(32)),
		Indexes: []Index{{Slot: G1, Keys: Cols(t.Updated.Size(32))}},
		DataFrames: []DataFrame{
			{Name: "year-program", Keys: Cols(t.Year, t.ProgramID), Rows: Cols(t.EmployeeID), Sums: Cols(t.Minutes, t.Points, t.Hours), Count: true},
			{Name: "year-activity", ToFrames: ToFrames((*frameEnrollment).toActivityLines), UseZeroSumsMask: true},
		},
	}
}

// frameActivityLine is one line of the "year-activity" frame, declared by ToFrames.
type frameActivityLine struct {
	Year       int16 `frame:"key"`
	ActivityID int32 `frame:"row"`
	Minutes    int16 `frame:"sum"`
}

func (e *frameEnrollment) toActivityLines() []frameActivityLine {
	if e.Status == 0 {
		return nil
	}
	return []frameActivityLine{{Year: e.Year, ActivityID: e.ActivityID, Minutes: e.Minutes}}
}

var frameEnrollments = NewRepo[frameEnrollmentTable, frameEnrollment]()

func TestDataFrameDeclarationRules(t *testing.T) {
	updatedGSI := func(t *frameEnrollmentTable) []Index { return []Index{{Slot: G1, Keys: Cols(t.Updated.Size(32))}} }
	for name, testCase := range map[string]struct {
		frames    func(t *frameEnrollmentTable) []DataFrame
		indexes   func(t *frameEnrollmentTable) []Index
		partition func(t *frameEnrollmentTable) []Coln
	}{
		"a string column": {frames: func(t *frameEnrollmentTable) []DataFrame {
			return []DataFrame{{Name: "a", Keys: Cols(t.Note), Rows: Cols(t.EmployeeID), Count: true}}
		}},
		"a rule of the dataframe module": {frames: func(t *frameEnrollmentTable) []DataFrame {
			return []DataFrame{{Name: "Year", Keys: Cols(t.Year), Rows: Cols(t.EmployeeID), Count: true}}
		}},
		"no GSI on Updated": {frames: func(t *frameEnrollmentTable) []DataFrame {
			return []DataFrame{{Name: "a", Keys: Cols(t.Year), Rows: Cols(t.EmployeeID), Count: true}}
		}, indexes: func(t *frameEnrollmentTable) []Index {
			return []Index{{Slot: G1, Keys: Cols(t.Year.Size(16), t.Updated.Size(32))}}
		}},
		"a Partition": {frames: func(t *frameEnrollmentTable) []DataFrame {
			return []DataFrame{{Name: "a", Keys: Cols(t.ProgramID), Rows: Cols(t.EmployeeID), Count: true}}
		}, partition: func(t *frameEnrollmentTable) []Coln { return Cols(t.Year.Size(16)) }},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s: expected a panic", name)
				}
			}()
			tablePtr := new(frameEnrollmentTable)
			populateColumnNames(tablePtr)
			indexes := updatedGSI
			if testCase.indexes != nil {
				indexes = testCase.indexes
			}
			schema := Schema{Entity: "bad_frame", TableID: 34567892, Keys: Cols(tablePtr.ActivityID.Size(32)), Indexes: indexes(tablePtr), DataFrames: testCase.frames(tablePtr)}
			if testCase.partition != nil {
				schema.Partition = testCase.partition(tablePtr)
			}
			buildTableMeta(schema, reflect.TypeFor[frameEnrollment]())
		}()
	}
}

func TestFrameLinesAndTheWriteCheck(t *testing.T) {
	m := frameEnrollments.meta
	if !m.readsStoredVersion() || len(m.frames.Frames) != 2 || !m.frames.Frames[1].IsProjected {
		t.Fatalf("compiled frames: %+v", m.frames)
	}
	record := frameEnrollment{ActivityID: 9, EmployeeID: 4, Year: 2026, ProgramID: 3, Minutes: 240, Points: 100, Hours: 1.5, Status: 1}
	lines, err := m.frames.LinesOf(unsafe.Pointer(&record))
	if err != nil || !slices.Equal(lines[0][0], dataframe.Line{2026, 3, 4, 240, 100, 1_500_000}) || !slices.Equal(lines[1][0], dataframe.Line{2026, 9, 240}) {
		t.Fatalf("lines %v, err %v", lines, err)
	}
	record.Status = 0
	if lines, _ := m.frames.LinesOf(unsafe.Pointer(&record)); lines[0] != nil || lines[1] != nil {
		t.Fatalf("a deleted record puts no lines, got %v", lines)
	}
	record.Status = 1
	item, err := m.marshalItem(unsafe.Pointer(&record), &record)
	if err != nil || item[createdColumn] == nil {
		t.Fatalf("an item carries crt: %v, err %v", item, err)
	}
	record.Year = -1
	if _, err := m.marshalItem(unsafe.Pointer(&record), &record); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("a negative Keys value must fail the write before it lands, got %v", err)
	}
	record.Year, record.Hours = 2026, math.NaN()
	if _, err := m.marshalItem(unsafe.Pointer(&record), &record); err == nil {
		t.Fatal("a NaN Sums value must fail the write")
	}
	if err := frameEnrollments.Delete(&record); err == nil || !strings.Contains(err.Error(), "never deleted") {
		t.Fatalf("Delete on a table with DataFrames must fail, got %v", err)
	}
}

// TestFrameExecChangedRefusesBeforeReading: a bad selection or held list fails before any storage
// read (the reads themselves are tested in the dataframe module).
func TestFrameExecChangedRefusesBeforeReading(t *testing.T) {
	table := frameEnrollments.T
	if _, err := frameEnrollments.QueryFrame("year-program").Eq(table.ProgramID, 3).ExecChanged("", ""); err == nil || !strings.Contains(err.Error(), "needs an Eq or a Between") {
		t.Fatalf("no Keys[0] predicate, got %v", err)
	}
	if _, err := frameEnrollments.QueryFrame("year-program").Eq(table.Year, 2026).ExecChanged("g", "2026.x.1"); err == nil {
		t.Fatal("a bad held list must fail")
	}
	if _, err := frameEnrollments.QueryFrame("missing").ExecChanged("", ""); err == nil {
		t.Fatal("an undeclared frame must fail")
	}
}

package dynamo

import (
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unsafe"
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
		},
	}
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

func TestFrameTrackedValuesAndTheWriteCheck(t *testing.T) {
	m := frameEnrollments.meta
	if !m.readsStoredVersion() || m.frames.Tracked[m.frames.StatusIndex].FieldName != statusFieldName {
		t.Fatalf("compiled frames: %+v", m.frames)
	}
	record := frameEnrollment{ActivityID: 9, EmployeeID: 4, Year: 2026, ProgramID: 3, Minutes: 240, Points: 100, Hours: 1.5, Status: 1}
	trackedValues, err := m.trackedValuesOf(unsafe.Pointer(&record))
	if err != nil || !slices.Equal(trackedValues, []int64{4, 2026, 3, 240, 100, 1, 1_500_000}) {
		t.Fatalf("tracked values %v, err %v", trackedValues, err)
	}
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

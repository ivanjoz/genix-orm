package scylla

import (
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// fakeExec stands in for a second driver. It records which calls it received so the
// tests can prove a query was routed to it rather than to Scylla — without needing a
// live connection to either database.
type fakeExec[TableT TableSchemaInterface[TableT], RecordT TableBaseInterface[TableT, RecordT]] struct {
	selects int
	inserts int
}

func (f *fakeExec[TableT, RecordT]) Name() string { return "fake" }

func (f *fakeExec[TableT, RecordT]) Select(
	schema *TableT, tableInfo *TableInfo, dst *[]RecordT, scan func(*RecordT) bool,
) error {
	f.selects++
	// The destination is statically *[]RecordT here: no assertion, no unsafe.Pointer.
	*dst = append(*dst, *new(RecordT))
	return nil
}

func (f *fakeExec[TableT, RecordT]) SelectGrouped(schema *TableT, tableInfo *TableInfo) error {
	return nil
}
func (f *fakeExec[TableT, RecordT]) Insert(records *[]RecordT, _ ...Coln) error {
	f.inserts++
	return nil
}
func (f *fakeExec[TableT, RecordT]) Update(records *[]RecordT, _ ...Coln) error        { return nil }
func (f *fakeExec[TableT, RecordT]) UpdateExclude(records *[]RecordT, _ ...Coln) error { return nil }

func (f *fakeExec[TableT, RecordT]) InsertUpdate(_, _ *[]RecordT, _ []Coln, _ ...Coln) error {
	return nil
}
func (f *fakeExec[TableT, RecordT]) InsertUpdateInclude(
	_ *[]RecordT, _ func(*RecordT) bool, _ []Coln, _ ...Coln,
) error {
	return nil
}
func (f *fakeExec[TableT, RecordT]) InsertUpdateExclude(
	_ *[]RecordT, _ func(*RecordT) bool, _ []Coln, _ ...Coln,
) error {
	return nil
}
func (f *fakeExec[TableT, RecordT]) Merge(
	_ *[]RecordT, _ []Coln, _ func(previous, current *RecordT) bool, _ func(*RecordT),
) error {
	return nil
}
func (f *fakeExec[TableT, RecordT]) QueryCachedIDs(_ *[]RecordT, _ []IDUpdatedVersion) error {
	return nil
}
func (f *fakeExec[TableT, RecordT]) SearchTextIDs(_ int32, _ string, _ int8, _ int) ([]db.IDWeight, error) {
	return nil, nil
}
func (f *fakeExec[TableT, RecordT]) SearchText(
	_ *[]RecordT, _ int32, _ string, _ int8, _ int,
) ([]db.IDWeight, error) {
	return nil, nil
}
func (f *fakeExec[TableT, RecordT]) CompileTable(schema *TableT) db.Table {
	return getOrCompileScyllaTable(schema)
}

// The declared default driver comes from the embedded TableStruct alias, so the
// declaration stays two-parameter exactly as every existing table is written.
type routedRecord struct {
	TableStruct[routedTable, routedRecord]
	EmpresaID int32
	ID        int32
}

type routedTable struct {
	TableStruct[routedTable, routedRecord]
	EmpresaID db.Col[routedTable, int32]
	ID        db.Col[routedTable, int32]
}

func (e routedTable) GetSchema() db.TableSchema {
	return db.TableSchema{
		Name:      "routed_probe",
		Partition: e.EmpresaID,
		Keys:      db.Cols(e.ID),
	}
}

func TestDeclaredDefaultDriverIsScylla(t *testing.T) {
	var records []routedRecord
	// No explicit type arguments: the driver is inferred from the record type.
	executor := Query(&records).GetRefSchema().Executor()
	if executor.Name() != "scylla" {
		t.Errorf("default driver = %q, want scylla", executor.Name())
	}
}

// Via routes one query to an explicitly chosen driver, which is what makes the
// driver a runtime decision rather than a compile-time one.
func TestViaRoutesQueryToChosenDriverAtRuntime(t *testing.T) {
	secondDriver := &fakeExec[routedTable, routedRecord]{}

	var records []routedRecord
	if err := Query(&records).Via(secondDriver).ID.Equals(1).Exec(); err != nil {
		t.Fatalf("Exec through the chosen driver: %v", err)
	}

	if secondDriver.selects != 1 {
		t.Errorf("chosen driver received %d selects, want 1", secondDriver.selects)
	}
	if len(records) != 1 {
		t.Errorf("collected %d records, want 1", len(records))
	}
}

// Two drivers, one record type, one binary — held in two variables, which is only
// possible because the driver is an interface value and not baked into the type.
func TestTwoDriversCoexistForOneRecordType(t *testing.T) {
	drivers := []db.Executor[routedTable, routedRecord]{
		Exec[routedTable, routedRecord]{},
		&fakeExec[routedTable, routedRecord]{},
	}

	names := []string{}
	for _, driver := range drivers {
		names = append(names, driver.Name())
	}
	if len(names) != 2 || names[0] != "scylla" || names[1] != "fake" {
		t.Errorf("driver names = %v, want [scylla fake]", names)
	}

	// Selecting by config is an ordinary assignment.
	useSecond := true
	chosen := drivers[0]
	if useSecond {
		chosen = drivers[1]
	}
	var records []routedRecord
	if err := Query(&records).Via(chosen).Exec(); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("collected %d records through the chosen driver, want 1", len(records))
	}
}

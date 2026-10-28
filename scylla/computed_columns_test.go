package scylla

import (
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/ivanjoz/colbin"
	"github.com/ivanjoz/genix-orm/db"
)

type computedLine struct {
	ProductID int32
	Quantity  int32
}

type computedRecord struct {
	TableStruct[computedSchema, computedRecord]
	CompanyID int32          `db:"empresa_id"`
	ID        int64          `db:"id"`
	Date      int16          `db:"date"`
	Status    int8           `db:"status"`
	Lines     []computedLine `db:"lines"`
}

func (e *computedRecord) LineProductIDs() []int32 {
	productIDs := []int32{}
	for _, line := range e.Lines {
		productIDs = append(productIDs, line.ProductID)
	}
	return productIDs
}

type computedSchema struct {
	TableStruct[computedSchema, computedRecord]
	CompanyID      Col[*computedSchema, int32]
	ID             Col[*computedSchema, int64]
	Date           Col[*computedSchema, int16]
	Status         Col[*computedSchema, int8]
	Lines          Col[*computedSchema, []computedLine]
	LineProductIDs Col[*computedSchema, []int32]
}

func (e computedSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10050,
		Namespace: "test_keyspace",
		Name:      "computed_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Computed: []db.ComputedCol{
			{Col: e.LineProductIDs, From: Cols(e.Lines), Value: db.Compute((*computedRecord).LineProductIDs)},
		},
		Indexes: []Index{
			{Keys: Cols(e.Date, e.LineProductIDs), UseIndexGroup: true},
		},
	}
}

func TestComputedColumnFeedsIndexGroupWithoutStorage(t *testing.T) {
	scyllaTable := MakeScyllaTable[computedRecord, computedSchema]()

	computedColumn := scyllaTable.ColumnsMap["line_product_ids"]
	if computedColumn == nil || !computedColumn.GetInfo().IsComputed {
		t.Fatalf("expected line_product_ids in ColumnsMap as a computed column, got %+v", computedColumn)
	}
	for _, column := range scyllaTable.Columns {
		if column.GetName() == "line_product_ids" {
			t.Fatal("a computed column must not be in Columns: it would be written, read and deployed")
		}
	}

	record := computedRecord{CompanyID: 1, ID: 9, Date: 20000, Lines: []computedLine{{ProductID: 4}, {ProductID: 7}}}
	indexGroup := scyllaTable.indexGroups[0]
	hashes := indexGroup.virtualColumn.GetRawValue(unsafe.Pointer(&record)).([]int32)
	expectedHashes := []int32{HashInt64(20000, 4), HashInt64(20000, 7)}
	slices.Sort(expectedHashes)
	if !slices.Equal(hashes, expectedHashes) {
		t.Fatalf("index group hashes = %v, want %v", hashes, expectedHashes)
	}
}

// A []Struct column encodes as a typed narrow colbin message, which has no root byte: the read must
// decode it anyway.
func TestSliceOfStructsBlobColumnRoundTrips(t *testing.T) {
	scyllaTable := MakeScyllaTable[computedRecord, computedSchema]()
	writtenLines := []computedLine{{ProductID: 4, Quantity: 2000}, {ProductID: 7, Quantity: 1000}}
	encodedLines, err := colbin.Marshal(writtenLines)
	if err != nil {
		t.Fatalf("colbin.Marshal: %v", err)
	}
	record := computedRecord{}
	scyllaTable.ColumnsMap["lines"].SetValue(unsafe.Pointer(&record), encodedLines)
	if !slices.Equal(record.Lines, writtenLines) {
		t.Fatalf("decoded lines = %+v, want %+v", record.Lines, writtenLines)
	}
}

func TestComputedColumnQueryUsesIndexGroup(t *testing.T) {
	scyllaTable := MakeScyllaTable[computedRecord, computedSchema]()

	servedQuery := &TableInfo{Statements: []ColumnStatement{
		{Col: "empresa_id", Operator: "=", Value: int32(1)},
		{Col: "date", Operator: "=", Value: int16(20000)},
		{Col: "line_product_ids", Operator: "CONTAINS", Value: int32(4)},
	}}
	compiled, err := compileSelectStatement(servedQuery, scyllaTable)
	if err != nil {
		t.Fatalf("compileSelectStatement: %v", err)
	}
	boundPlan, err := compiled.Compute(servedQuery, scyllaTable)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	queryString := boundPlan.Statements[0].QueryStr
	if !strings.Contains(queryString, "WHERE zz_igs_date_line_product_ids CONTAINS ?") || strings.Contains(queryString, " line_product_ids") {
		t.Fatalf("expected only the index group hash column in the WHERE, got %s", queryString)
	}
	projection := strings.Split(strings.TrimPrefix(strings.Split(queryString, " FROM ")[0], "SELECT "), ", ")
	if !slices.Contains(projection, "lines") || slices.Contains(projection, "line_product_ids") {
		t.Fatalf("expected the stored lines column in the projection, got %s", queryString)
	}

	unservedQuery := &TableInfo{Statements: []ColumnStatement{
		{Col: "empresa_id", Operator: "=", Value: int32(1)},
		{Col: "line_product_ids", Operator: "CONTAINS", Value: int32(4)},
	}, AllowFilter: true}
	if compiled, err := compileSelectStatement(unservedQuery, scyllaTable); err == nil {
		if _, err := compiled.Compute(unservedQuery, scyllaTable); err == nil {
			t.Fatal("a predicate on a computed column with no index group must fail")
		}
	}
}

func TestComputedColumnUpdateOfFromRewritesIndexGroup(t *testing.T) {
	scyllaTable := MakeScyllaTable[computedRecord, computedSchema]()
	table := TableOf[computedRecord, computedSchema]()
	records := []computedRecord{{CompanyID: 1, ID: 9, Date: 20000, Lines: []computedLine{{ProductID: 4}}}}

	_, columnsToUpdate := resolveUpdateColumnsForWrite(scyllaTable, len(records),
		func(index int) unsafe.Pointer { return unsafe.Pointer(&records[index]) }, []Coln{table.Lines}, nil, false)
	updatedNames := []string{}
	for _, column := range columnsToUpdate {
		updatedNames = append(updatedNames, column.GetName())
	}
	if !slices.Contains(updatedNames, "zz_igs_date_line_product_ids") {
		t.Fatalf("updating lines must rewrite the index group hashes, updated: %v", updatedNames)
	}
	if slices.Contains(updatedNames, "line_product_ids") {
		t.Fatalf("the computed column itself must never be written, updated: %v", updatedNames)
	}

	_, columnsToUpdate = resolveUpdateColumnsForWrite(scyllaTable, len(records),
		func(index int) unsafe.Pointer { return unsafe.Pointer(&records[index]) }, []Coln{table.Status}, nil, false)
	for _, column := range columnsToUpdate {
		if column.GetName() == "zz_igs_date_line_product_ids" {
			t.Fatal("an update that writes no From column must not rewrite the hashes")
		}
	}
}

type computedSingleKeyRecord struct {
	TableStruct[computedSingleKeySchema, computedSingleKeyRecord]
	CompanyID int32          `db:"empresa_id"`
	ID        int64          `db:"id"`
	Lines     []computedLine `db:"lines"`
}

func (e *computedSingleKeyRecord) LineProductIDs() []int32 { return nil }

type computedSingleKeySchema struct {
	TableStruct[computedSingleKeySchema, computedSingleKeyRecord]
	CompanyID      Col[*computedSingleKeySchema, int32]
	ID             Col[*computedSingleKeySchema, int64]
	Lines          Col[*computedSingleKeySchema, []computedLine]
	LineProductIDs Col[*computedSingleKeySchema, []int32]
}

func (e computedSingleKeySchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10051,
		Namespace: "test_keyspace",
		Name:      "computed_single_key_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Computed: []db.ComputedCol{
			{Col: e.LineProductIDs, From: Cols(e.Lines), Value: db.Compute((*computedSingleKeyRecord).LineProductIDs)},
		},
		// A single-column group indexes the column itself, which a computed column does not have.
		Indexes: []Index{{Keys: Cols(e.LineProductIDs), UseIndexGroup: true}},
	}
}

type computedUndeclaredRecord struct {
	TableStruct[computedUndeclaredSchema, computedUndeclaredRecord]
	CompanyID int32 `db:"empresa_id"`
	ID        int64 `db:"id"`
}

type computedUndeclaredSchema struct {
	TableStruct[computedUndeclaredSchema, computedUndeclaredRecord]
	CompanyID Col[*computedUndeclaredSchema, int32]
	ID        Col[*computedUndeclaredSchema, int64]
	Missing   Col[*computedUndeclaredSchema, int32]
}

func (e computedUndeclaredSchema) GetSchema() TableSchema {
	return TableSchema{ID: 10052, Namespace: "test_keyspace", Name: "computed_undeclared_records",
		Partition: e.CompanyID, Keys: Cols(e.ID)}
}

func TestComputedColumnRejectedUses(t *testing.T) {
	expectPanic := func(name, wantMessage string, compile func()) {
		t.Helper()
		defer func() {
			recovered := recover()
			if recovered == nil {
				t.Fatalf("%s: expected a panic", name)
			}
			if message, _ := recovered.(string); !strings.Contains(message, wantMessage) {
				t.Fatalf("%s: panic %q does not mention %q", name, recovered, wantMessage)
			}
		}()
		compile()
	}
	expectPanic("single-key index group", "has no storage", func() {
		MakeScyllaTable[computedSingleKeyRecord, computedSingleKeySchema]()
	})
	expectPanic("undeclared column", `"missing" has no field in the record struct`, func() {
		MakeScyllaTable[computedUndeclaredRecord, computedUndeclaredSchema]()
	})
}

package scylla

import (
	"slices"
	"strings"
	"testing"
)

// The projected fixture mirrors products' name-hash view: the MV selects two columns only, so its
// key columns are implicit in the CREATE and must still be reported as expected columns.
type projectedViewRecord struct {
	TableStruct[projectedViewSchema, projectedViewRecord]
	CompanyID int32  `db:"company_id"`
	ID        int32  `db:"id"`
	NameHash  int32  `db:"name_hash"`
	Status    int8   `db:"status"`
	Payload   string `db:"payload"`
}

type projectedViewSchema struct {
	TableStruct[projectedViewSchema, projectedViewRecord]
	CompanyID Col[*projectedViewSchema, int32]
	ID        Col[*projectedViewSchema, int32]
	NameHash  Col[*projectedViewSchema, int32]
	Status    Col[*projectedViewSchema, int8]
	Payload   Col[*projectedViewSchema, string]
}

func (e projectedViewSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10031,
		Name:      "projected_view_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Indexes: []Index{
			{Type: TypeView, Keys: Cols(e.NameHash), Cols: Cols(e.ID, e.Status)},
			{Type: TypeView, Keys: Cols(e.Status)},
		},
	}
}

// The view-table fixture fans out on a slice key, whose physical column stores the element type.
type fanoutViewTableRecord struct {
	TableStruct[fanoutViewTableSchema, fanoutViewTableRecord]
	CompanyID   int32   `db:"company_id"`
	ID          int32   `db:"id"`
	CategoryIDs []int32 `db:"category_ids,list"`
	Status      int8    `db:"status"`
	Payload     string  `db:"payload"`
}

type fanoutViewTableSchema struct {
	TableStruct[fanoutViewTableSchema, fanoutViewTableRecord]
	CompanyID   Col[*fanoutViewTableSchema, int32]
	ID          Col[*fanoutViewTableSchema, int32]
	CategoryIDs Col[*fanoutViewTableSchema, []int32]
	Status      Col[*fanoutViewTableSchema, int8]
	Payload     Col[*fanoutViewTableSchema, string]
}

func (e fanoutViewTableSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10032,
		Name:      "fanout_view_table_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Indexes: []Index{
			{Type: TypeViewTable, Keys: Cols(e.CategoryIDs)},
		},
	}
}

// parseCreateScriptColumnNames extracts the column set a view's CREATE statement really produces:
// the SELECT list plus the primary key for a materialized view, the column definitions for a view
// table. Deploy diffs getExpectedColumns() against the live catalog, so anything the CREATE emits
// but getExpectedColumns() omits would be flagged as missing on every single run.
func parseCreateScriptColumnNames(t *testing.T, createScript string) []string {
	t.Helper()

	columnNames := []string{}
	appendName := func(rawName string) {
		name := strings.TrimSpace(strings.Trim(strings.TrimSpace(rawName), "()"))
		if name == "" || slices.Contains(columnNames, name) {
			return
		}
		columnNames = append(columnNames, name)
	}

	_, afterPrimaryKey, hasPrimaryKey := strings.Cut(createScript, "PRIMARY KEY")
	if !hasPrimaryKey {
		t.Fatalf("create script has no PRIMARY KEY: %q", createScript)
	}
	primaryKeyClause := afterPrimaryKey[:strings.Index(afterPrimaryKey, ")\n")+1]
	if closingIndex := strings.LastIndex(primaryKeyClause, ")"); closingIndex >= 0 {
		primaryKeyClause = primaryKeyClause[:closingIndex]
	}

	if strings.Contains(createScript, "CREATE MATERIALIZED VIEW") {
		_, afterSelect, _ := strings.Cut(createScript, "SELECT ")
		selectList, _, _ := strings.Cut(afterSelect, " FROM ")
		for _, name := range strings.Split(selectList, ",") {
			appendName(name)
		}
		for _, name := range strings.Split(primaryKeyClause, ",") {
			appendName(name)
		}
		return columnNames
	}

	_, afterOpenParen, _ := strings.Cut(createScript, "(")
	definitions, _, _ := strings.Cut(afterOpenParen, "PRIMARY KEY")
	for _, definition := range strings.Split(definitions, ",") {
		nameAndType := strings.Fields(strings.TrimSpace(definition))
		if len(nameAndType) == 0 {
			continue
		}
		appendName(nameAndType[0])
	}
	return columnNames
}

func assertViewExpectedColumnsMatchCreateScript(t *testing.T, scyllaTable ScyllaTable) {
	t.Helper()
	if len(scyllaTable.views) == 0 {
		t.Fatalf("table %q compiled no views", scyllaTable.Name)
	}

	for _, view := range scyllaTable.views {
		if view.getExpectedColumns == nil {
			t.Fatalf("view %q has no getExpectedColumns", view.name)
		}

		expectedNames := []string{}
		for _, expectedColumn := range view.getExpectedColumns() {
			if expectedColumn.dbType == "" {
				t.Fatalf("view %q reports column %q without a DB type", view.name, expectedColumn.name)
			}
			expectedNames = append(expectedNames, expectedColumn.name)
		}
		slices.Sort(expectedNames)

		createdNames := parseCreateScriptColumnNames(t, view.getCreateScript())
		slices.Sort(createdNames)

		if !slices.Equal(expectedNames, createdNames) {
			t.Fatalf("view %q: expected columns %v do not match the CREATE script columns %v",
				view.name, expectedNames, createdNames)
		}
	}
}

func TestViewExpectedColumnsMatchCreateScript(t *testing.T) {
	resetORMTableCachesForTesting()
	assertViewExpectedColumnsMatchCreateScript(t, MakeScyllaTable[fullViewRecord, fullViewSchema]())

	resetORMTableCachesForTesting()
	assertViewExpectedColumnsMatchCreateScript(t, MakeScyllaTable[hashIndexedFullViewRecord, hashIndexedFullViewSchema]())

	resetORMTableCachesForTesting()
	assertViewExpectedColumnsMatchCreateScript(t, MakeScyllaTable[deltaViewRecord, deltaViewSchema]())

	resetORMTableCachesForTesting()
	assertViewExpectedColumnsMatchCreateScript(t, MakeScyllaTable[relocatedPartitionViewRecord, relocatedPartitionViewSchema]())

	resetORMTableCachesForTesting()
	assertViewExpectedColumnsMatchCreateScript(t, MakeScyllaTable[projectedViewRecord, projectedViewSchema]())

	resetORMTableCachesForTesting()
	assertViewExpectedColumnsMatchCreateScript(t, MakeScyllaTable[fanoutViewTableRecord, fanoutViewTableSchema]())
}

// A column added to the base table is the case that broke: the live view keeps the old column set,
// so deploy has to see it as missing and repair the view instead of leaving it half-applied.
func TestViewMissingColumnsAreDetectedForANewBaseColumn(t *testing.T) {
	resetORMTableCachesForTesting()
	scyllaTable := MakeScyllaTable[projectedViewRecord, projectedViewSchema]()

	fullPayloadView := (*viewInfo)(nil)
	for _, view := range scyllaTable.views {
		if view.name == "projected_view_records__pk_status_view" {
			fullPayloadView = view
		}
	}
	if fullPayloadView == nil {
		t.Fatalf("expected the status view, got %v", scyllaTable.views)
	}

	expectedColumns := fullPayloadView.getExpectedColumns()
	if !slices.ContainsFunc(expectedColumns, func(column viewExpectedColumn) bool { return column.name == "payload" }) {
		t.Fatalf("expected the full-payload view to carry every base column, got %v", expectedColumns)
	}

	// The live catalog as it looks before "payload" was declared on the record struct.
	liveViewColumns := []ScyllaColumns{}
	for _, expectedColumn := range expectedColumns {
		if expectedColumn.name == "payload" {
			continue
		}
		liveViewColumns = append(liveViewColumns, ScyllaColumns{Name: expectedColumn.name, Type: expectedColumn.dbType})
	}

	missingColumns := getViewMissingColumns(fullPayloadView, liveViewColumns)
	if len(missingColumns) != 1 || missingColumns[0].name != "payload" {
		t.Fatalf("expected only payload to be missing, got %v", missingColumns)
	}

	liveViewColumns = append(liveViewColumns, ScyllaColumns{Name: "payload", Type: "text"})
	if missingColumns := getViewMissingColumns(fullPayloadView, liveViewColumns); len(missingColumns) != 0 {
		t.Fatalf("expected no missing columns once the view carries payload, got %v", missingColumns)
	}
}

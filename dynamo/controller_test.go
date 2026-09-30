package dynamo

import "testing"

// Compile-time proof that a Repo is a Controller and that NewController returns
// one. If a Controller method is ever removed from Repo, this fails to build.
var (
	_ Controller = (*Repo[TicketTable, Ticket])(nil)
	_ Controller = NewController[TicketTable, Ticket]()
)

// TestControllerAccessors covers the non-generic surface the interface exposes
// (the DeleteRecordsAll path itself needs a live DynamoDB and is exercised via
// the `wipe` CLI command).
func TestControllerAccessors(t *testing.T) {
	var c Controller = NewController[TicketTable, Ticket]()

	if c.Entity() != "tick" {
		t.Fatalf("Entity(): got %q, want %q", c.Entity(), "tick")
	}
	if c.TableName() == "" {
		t.Fatal("TableName() should not be empty")
	}
	if got := c.Schema().Entity; got != "tick" {
		t.Fatalf("Schema().Entity: got %q, want %q", got, "tick")
	}
	if !c.Schema().Autoincrement {
		t.Fatal("Schema().Autoincrement should be true for TicketTable")
	}
}

// TestSchemaFieldsListEveryColumn checks Fields holds every table column in
// declaration order, slice columns included and the embedded Model skipped.
func TestSchemaFieldsListEveryColumn(t *testing.T) {
	fields := GetSchema[ProductTable]().Fields
	gotNames := make([]string, len(fields))
	for i, field := range fields {
		gotNames[i] = field.Field
	}
	wantNames := []string{"ID", "CategoryID", "Brand", "Price", "Stock", "Created", "Name", "TagIDs", "Labels"}
	if len(gotNames) != len(wantNames) {
		t.Fatalf("Fields: got %v, want %v", gotNames, wantNames)
	}
	for i := range wantNames {
		if gotNames[i] != wantNames[i] {
			t.Fatalf("Fields: got %v, want %v", gotNames, wantNames)
		}
	}
	if fields[1].Type != "int" || fields[2].Type != "string" {
		t.Fatalf("Fields types: got CategoryID=%q Brand=%q", fields[1].Type, fields[2].Type)
	}
}

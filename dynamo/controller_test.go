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

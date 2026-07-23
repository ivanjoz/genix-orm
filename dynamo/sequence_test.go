package dynamo

import (
	"testing"
	"unsafe"
)

// ── Autoincrement test entity: an integer ID with random-padded sequence ─────

type Ticket struct {
	ID      int64
	Subject string
	Created int64
}

type TicketTable struct {
	Model[TicketTable, Ticket]
	ID      Col[TicketTable, int64]
	Subject Col[TicketTable, string]
	Created Col[TicketTable, int64]
}

func (t TicketTable) GetSchema() Schema {
	return Schema{
		Entity:                     "tick",
		Partition:                  Keys(t.ID.Base(8)),
		Sort:                       Keys(t.Created.Base(8)),
		UseAutoincrement:           true,
		AutoincrementRandomPadding: 3,
	}
}

// TestAutoincrementCompiles verifies the schema resolves an autoinc config bound
// to the integer ID field with the requested padding.
func TestAutoincrementCompiles(t *testing.T) {
	r := NewRepo[TicketTable, Ticket]()
	if r.meta.autoinc == nil {
		t.Fatal("expected autoinc config to be resolved")
	}
	if r.meta.autoinc.padding != 3 {
		t.Fatalf("expected padding 3, got %d", r.meta.autoinc.padding)
	}
	if r.meta.autoinc.factor != 1000 {
		t.Fatalf("expected factor 1000, got %d", r.meta.autoinc.factor)
	}
	if r.meta.autoinc.seqName != "tick" {
		t.Fatalf("expected sequence name 'tick', got %q", r.meta.autoinc.seqName)
	}

	// The get/set accessors must round-trip through a real Ticket.
	tk := Ticket{}
	p := unsafe.Pointer(&tk)
	r.meta.autoinc.set(p, 42_837)
	if tk.ID != 42_837 {
		t.Fatalf("setter did not write ID: got %d", tk.ID)
	}
	if got := r.meta.autoinc.get(p); got != 42_837 {
		t.Fatalf("getter mismatch: got %d", got)
	}
}

// TestRangeStart checks the post-increment high-water mark maps to the first
// value of the reserved range.
func TestRangeStart(t *testing.T) {
	// Fresh sequence: DynamoDB ADD on a missing attr yields count, range starts 1.
	if got := rangeStart(5, 5); got != 1 {
		t.Fatalf("fresh reservation should start at 1, got %d", got)
	}
	// Continuing sequence: prior high 40, reserve 3 -> high 43, range [41,43].
	if got := rangeStart(43, 3); got != 41 {
		t.Fatalf("expected range start 41, got %d", got)
	}
	if got := rangeStart(1, 1); got != 1 {
		t.Fatalf("single reservation on fresh sequence should be 1, got %d", got)
	}
}

// TestComposeIDLayout confirms the sequence goes in the high digits and the
// random value in the low `padding` digits.
func TestComposeIDLayout(t *testing.T) {
	a := &autoincConfig{padding: 3, factor: 1000}
	if got := a.composeID(42, 837); got != 42_837 {
		t.Fatalf("expected 42837, got %d", got)
	}
	// padding 0 -> plain sequential (factor 1, random always 0).
	z := &autoincConfig{padding: 0, factor: 1}
	if got := z.composeID(42, 0); got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
	if z.randDigits() != 0 {
		t.Fatal("padding 0 must produce 0 random digits")
	}
}

// TestComposeIDNeverCollides is the uniqueness guarantee: because each sequence
// value owns a disjoint [seq*factor, seq*factor+factor) interval, distinct
// sequence values can never produce the same ID regardless of the random draw.
func TestComposeIDNeverCollides(t *testing.T) {
	a := &autoincConfig{padding: 3, factor: 1000}
	seen := map[int64]int64{}
	for seq := int64(1); seq <= 200; seq++ {
		for r := int64(0); r < a.factor; r++ {
			id := a.composeID(seq, r)
			if prev, dup := seen[id]; dup {
				t.Fatalf("collision: id %d from seq %d and seq %d", id, prev, seq)
			}
			seen[id] = seq
		}
	}
}

// TestPow10 covers the padding factor helper.
func TestPow10(t *testing.T) {
	cases := map[int]int64{0: 1, 1: 10, 3: 1000, 9: 1_000_000_000}
	for n, want := range cases {
		if got := pow10(n); got != want {
			t.Fatalf("pow10(%d)=%d, want %d", n, got, want)
		}
	}
}

// TestAutoincrementRejectsStringID verifies the compile-time guard: a non-integer
// ID field with UseAutoincrement must panic.
func TestAutoincrementRejectsStringID(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for string ID with UseAutoincrement")
		}
	}()
	NewRepo[badAutoincTable, badAutoinc]()
}

type badAutoinc struct {
	ID      string
	Created int64
}

type badAutoincTable struct {
	Model[badAutoincTable, badAutoinc]
	ID      Col[badAutoincTable, string]
	Created Col[badAutoincTable, int64]
}

func (t badAutoincTable) GetSchema() Schema {
	return Schema{
		Entity:           "bad",
		Partition:        Keys(t.ID),
		Sort:             Keys(t.Created.Base(8)),
		UseAutoincrement: true,
	}
}

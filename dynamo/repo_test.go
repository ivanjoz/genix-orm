package dynamo

import (
	"os"
	"testing"
	"time"
)

// TestPutIfAbsent needs a live DynamoDB whose table already exists, so it only runs when
// DYNAMO_ENDPOINT points at one (DYNAMO_TABLE names the table, as for every other call).
func TestPutIfAbsent(t *testing.T) {
	if os.Getenv("DYNAMO_ENDPOINT") == "" {
		t.Skip("DYNAMO_ENDPOINT not set: PutIfAbsent needs a live DynamoDB")
	}
	tickets := NewRepo[TicketTable, Ticket]()
	// An explicit ID keeps autoincrement out of the way and a fresh key per run.
	firstTicket := Ticket{ID: time.Now().UnixMilli(), Subject: "first", Created: 1}
	t.Cleanup(func() { tickets.Delete(&firstTicket) })

	wasWritten, err := tickets.PutIfAbsent(&firstTicket)
	if err != nil || !wasWritten {
		t.Fatalf("first write: got (%v, %v), want (true, nil)", wasWritten, err)
	}

	sameKeyTicket := Ticket{ID: firstTicket.ID, Subject: "second", Created: 1}
	wasWritten, err = tickets.PutIfAbsent(&sameKeyTicket)
	if err != nil || wasWritten {
		t.Fatalf("same key: got (%v, %v), want (false, nil)", wasWritten, err)
	}

	storedTicket, err := tickets.Get(Ticket{ID: firstTicket.ID, Created: 1})
	if err != nil || storedTicket == nil {
		t.Fatalf("reading back: got (%v, %v)", storedTicket, err)
	}
	if storedTicket.Subject != "first" {
		t.Fatalf("the losing write changed the row: Subject = %q", storedTicket.Subject)
	}
}

package dynamo

import (
	"strconv"
	"testing"
	"unsafe"
)

// TestEntityOfItemKey maps the items an entity writes back to it: base rows, write-version and
// autoincrement sequences (pk 0), and refuses named sequences and unknown TableIDs.
func TestEntityOfItemKey(t *testing.T) {
	ticketRepo := NewRepo[TicketTable, Ticket]()
	ticket := Ticket{ID: 7, Created: 100}
	basePK := ticketRepo.meta.pkValue(unsafe.Pointer(&ticket))
	tableID := strconv.Itoa(int(HashTableID("tick")))

	ownedKeys := [][2]string{
		{basePK, ticketRepo.meta.skValue(unsafe.Pointer(&ticket))},
		{sequencePartitionKey, tableID},
		{sequencePartitionKey, basePK + updatedVersionSeqSuffix},
	}
	for _, ownedKey := range ownedKeys {
		if entity, found := EntityOfItemKey(ownedKey[0], ownedKey[1]); !found || entity != "tick" {
			t.Fatalf("pk %s sk %s: got %q %v, want tick", ownedKey[0], ownedKey[1], entity, found)
		}
	}
	for _, foreignKey := range [][2]string{{sequencePartitionKey, "images"}, {"99999998", ""}, {"12", ""}} {
		if entity, found := EntityOfItemKey(foreignKey[0], foreignKey[1]); found {
			t.Fatalf("pk %s sk %s: got %q, want no entity", foreignKey[0], foreignKey[1], entity)
		}
	}
}

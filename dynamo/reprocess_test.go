package dynamo

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/ivanjoz/colbin"
)

// JSON stands in for the previous colbin: any decoder that reads the stored bytes into a *E.
func TestReencodeBlobCarriesALegacyBlobAndLeavesACurrentOne(t *testing.T) {
	tickets := NewRepo[TicketTable, Ticket]()
	ticket := Ticket{ID: 7, Subject: "café", Created: 3}
	legacyBlob, _ := json.Marshal(ticket)

	reencodedBlob, err := tickets.meta.reencodeBlob(legacyBlob, json.Unmarshal)
	if err != nil || reencodedBlob == nil {
		t.Fatalf("legacy blob: got (%v, %v), want a re-encoded blob", reencodedBlob, err)
	}
	decodedTicket := Ticket{}
	if err := colbin.Unmarshal(reencodedBlob, &decodedTicket); err != nil || decodedTicket != ticket {
		t.Fatalf("re-encoded blob reads back as %+v (%v), want %+v", decodedTicket, err, ticket)
	}

	// A second run finds the blob current (the legacy decoder rejects it): nothing to write.
	again, err := tickets.meta.reencodeBlob(reencodedBlob, json.Unmarshal)
	if err != nil || again != nil {
		t.Fatalf("current blob: got (%v, %v), want (nil, nil)", again, err)
	}
}

// Bytes both versions accept but read as different records are reported, never skipped or written.
func TestReencodeBlobReportsABlobBothVersionsReadDifferently(t *testing.T) {
	tickets := NewRepo[TicketTable, Ticket]()
	currentBlob, _ := colbin.Marshal(&Ticket{ID: 7, Subject: "current"})
	legacyReadsOtherTicket := func(_ []byte, dst any) error { *dst.(*Ticket) = Ticket{ID: 8}; return nil }
	if blob, err := tickets.meta.reencodeBlob(currentBlob, legacyReadsOtherTicket); !errors.Is(err, errAmbiguousBlob) {
		t.Fatalf("an ambiguous blob was accepted: got %v", blob)
	}
}

func TestReencodeBlobReportsABlobNeitherVersionReads(t *testing.T) {
	tickets := NewRepo[TicketTable, Ticket]()
	if _, err := tickets.meta.reencodeBlob([]byte{0xFF, 0x00, 0x13}, json.Unmarshal); err == nil {
		t.Fatal("an unreadable blob was accepted")
	}
}

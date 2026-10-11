package dynamo

import (
	"testing"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ivanjoz/colbin"
)

// ── Modify test entity: a plain table, which conditions its writes on Version too ──

type plainNote struct {
	ID      int32  `cb:"1"`
	Text    string `cb:"2"`
	Updated int32  `cb:"3"`
	Version int16  `json:"ver,omitempty" cb:"-"`
}

type plainNoteTable struct {
	Model[plainNoteTable, plainNote]
	ID      Col[*plainNoteTable, int32]
	Text    Col[*plainNoteTable, string]
	Updated Col[*plainNoteTable, int32]
}

func (t plainNoteTable) GetSchema() Schema {
	return Schema{Entity: "plain_note", TableID: 78901241, Keys: Cols(t.ID.Size(30))}
}

// Every item carries Version as the number "ver", outside the blob, where a condition can compare it.
func TestItemsCarryTheirVersionOutsideTheBlob(t *testing.T) {
	notes := NewRepo[plainNoteTable, plainNote]()
	note := plainNote{ID: 7, Text: "t", Updated: 400_000_000, Version: 5}
	item, err := notes.meta.marshalItem(unsafe.Pointer(&note), &note)
	if err != nil {
		t.Fatal(err)
	}
	if version, isNumber := item[versionColumn].(*types.AttributeValueMemberN); !isNumber || version.Value != "5" {
		t.Fatalf("item %q = %#v, want the number 5", versionColumn, item[versionColumn])
	}
	if _, hasUpdated := item["upd"]; hasUpdated {
		t.Fatal("Updated lives in the blob only")
	}
	var fromBlob plainNote
	if err := colbin.Unmarshal(item[dataColumn].(*types.AttributeValueMemberB).Value, &fromBlob); err != nil || fromBlob.Version != 0 {
		t.Fatalf("the blob holds Version %d (%v), want none", fromBlob.Version, err)
	}

	// unmarshalItem fills Version back from the attribute.
	item[versionColumn] = numberAttr(9)
	var read plainNote
	if err := notes.meta.unmarshalItem(item, &read); err != nil || read.Version != 9 || read.Text != "t" {
		t.Fatalf("read %+v (%v), want Version 9", read, err)
	}
}

// A write raises Version by one and starts over at 1 after the highest.
func TestNextVersionWrapsAfterTheHighest(t *testing.T) {
	for stored, want := range map[int64]int64{0: 1, 1: 2, maxVersion - 1: maxVersion, maxVersion: 1} {
		if got := nextVersion(stored); got != want {
			t.Fatalf("nextVersion(%d) = %d, want %d", stored, got, want)
		}
	}
}

package dynamo

import (
	"testing"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ── Modify test entities: one versioned by the flag alone, one not versioned ──

type versionedSecret struct {
	ID             int32  `cb:"1"`
	Secret         string `cb:"2"`
	UpdatedVersion int32  `cb:"3"`
}

type versionedSecretTable struct {
	Model[versionedSecretTable, versionedSecret]
	ID             Col[*versionedSecretTable, int32]
	Secret         Col[*versionedSecretTable, string]
	UpdatedVersion Col[*versionedSecretTable, int32]
}

func (t versionedSecretTable) GetSchema() Schema {
	return Schema{Entity: "versioned_secret", TableID: 78901240, Keys: Cols(t.ID.Size(30)), VersionedWrites: true}
}

type plainNote struct {
	ID   int32  `cb:"1"`
	Text string `cb:"2"`
}

type plainNoteTable struct {
	Model[plainNoteTable, plainNote]
	ID   Col[*plainNoteTable, int32]
	Text Col[*plainNoteTable, string]
}

func (t plainNoteTable) GetSchema() Schema {
	return Schema{Entity: "plain_note", TableID: 78901241, Keys: Cols(t.ID.Size(30))}
}

// A versioned item carries UpdatedVersion outside the blob, where a condition can compare it.
func TestVersionedItemsExposeTheirVersion(t *testing.T) {
	secrets := NewRepo[versionedSecretTable, versionedSecret]()
	secret := versionedSecret{ID: 7, Secret: "s", UpdatedVersion: 42}
	item, err := secrets.meta.marshalItem(unsafe.Pointer(&secret), &secret)
	if err != nil {
		t.Fatal(err)
	}
	version, isNumber := item[versionColumn].(*types.AttributeValueMemberN)
	if !isNumber || version.Value != "42" {
		t.Fatalf("item %q = %#v, want the number 42", versionColumn, item[versionColumn])
	}

	notes := NewRepo[plainNoteTable, plainNote]()
	note := plainNote{ID: 7, Text: "t"}
	if item, err = notes.meta.marshalItem(unsafe.Pointer(&note), &note); err != nil {
		t.Fatal(err)
	}
	if _, hasVersion := item[versionColumn]; hasVersion {
		t.Fatalf("an unversioned item carries %q", versionColumn)
	}
}

// A lost write deletes exactly the delta rows it put: on a table with only delta
// indexes, that is every row its write diff put.
func TestLostWriteDeletesItsDeltaRows(t *testing.T) {
	members := NewRepo[deltaMemberTable, deltaMember]()
	stored := deltaMember{ID: 9, TeamIDs: []int16{1, 2}, Status: 1, UpdatedVersion: 4}
	lost := deltaMember{ID: 9, TeamIDs: []int16{2}, Status: 1, UpdatedVersion: 5}
	puts, _ := members.meta.arrayIndexWrites(unsafe.Pointer(&stored), unsafe.Pointer(&lost), nil)
	deletes := members.meta.deltaRowDeletes(unsafe.Pointer(&lost))
	if len(deletes) != len(puts) {
		t.Fatalf("lost write deletes %d rows, put %d", len(deletes), len(puts))
	}
	for i := range puts {
		putSK := puts[i].PutRequest.Item["sk"].(*types.AttributeValueMemberS).Value
		deleteSK := deletes[i].DeleteRequest.Key["sk"].(*types.AttributeValueMemberS).Value
		if putSK != deleteSK {
			t.Fatalf("row %d: put %s, delete %s", i, putSK, deleteSK)
		}
	}
}

// The conditional writes refuse an unversioned table before reaching DynamoDB.
func TestConditionalWritesNeedAVersionedTable(t *testing.T) {
	notes := NewRepo[plainNoteTable, plainNote]()
	if _, err := notes.Modify(plainNote{ID: 7}, func(*plainNote, bool) error { return nil }); err == nil {
		t.Fatal("Modify ran on a table without VersionedWrites")
	}
	if _, err := notes.PutManyIfVersion([]plainNote{{ID: 7}}); err == nil {
		t.Fatal("PutManyIfVersion ran on a table without VersionedWrites")
	}
}

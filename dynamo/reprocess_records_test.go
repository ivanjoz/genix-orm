package dynamo

import (
	"context"
	"os"
	"strings"
	"testing"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// storedProductItem is the item a Product is stored as, at Version 3.
func storedProductItem(t *testing.T, r *Repo[ProductTable, Product], product Product) map[string]types.AttributeValue {
	t.Helper()
	product.Version = 3
	item, err := r.meta.marshalItem(unsafe.Pointer(&product), &product)
	if err != nil {
		t.Fatal(err)
	}
	return item
}

func TestReprocessPageWritesOnlyChangedRecords(t *testing.T) {
	r := newProducts(t)
	storedItems := []map[string]types.AttributeValue{
		storedProductItem(t, r, Product{ID: "a", CategoryID: 7, Created: 1, Name: "old"}),
		storedProductItem(t, r, Product{ID: "b", CategoryID: 7, Created: 2, Name: "kept"}),
	}
	report := RecordsReport{}
	changed, err := r.reprocessPage(storedItems, func(product *Product) (bool, error) {
		if product.Name != "old" {
			return false, nil
		}
		product.Name = "new"
		return true, nil
	}, false, &report)
	if err != nil || report.Scanned != 2 || report.Changed != 1 || report.Moved != 0 || len(changed) != 1 || len(report.Samples) != 1 {
		t.Fatalf("report = %+v, %d changed, %v", report, len(changed), err)
	}
	var written Product
	if err := r.meta.unmarshalItem(changed[0].item, &written); err != nil {
		t.Fatal(err)
	}
	// Version is written as the record holds it, never raised; the item stays at its key.
	if written.Name != "new" || written.Version != 3 || itemName(changed[0].item) != itemName(storedItems[0]) {
		t.Fatalf("written = %+v at %s", written, itemName(changed[0].item))
	}
}

func TestReprocessPageMovesARecordWhoseKeyChanged(t *testing.T) {
	r := newProducts(t)
	storedItem := storedProductItem(t, r, Product{ID: "a", CategoryID: 7, Created: 1})
	report := RecordsReport{}
	changed, err := r.reprocessPage([]map[string]types.AttributeValue{storedItem}, func(product *Product) (bool, error) {
		product.ID = "b"
		return true, nil
	}, false, &report)
	if err != nil || report.Moved != 1 || len(changed) != 1 || !changed[0].isMove {
		t.Fatalf("report = %+v, %v", report, err)
	}
	if !strings.Contains(attrText(changed[0].item["sk"]), "b") {
		t.Fatalf("the moved item is at %s", itemName(changed[0].item))
	}
}

func TestReprocessPageMovesARecordStoredUnderAnOldKey(t *testing.T) {
	r := newProducts(t)
	storedItem := storedProductItem(t, r, Product{ID: "a", CategoryID: 7, Created: 1})
	storedItem["sk"] = &types.AttributeValueMemberS{Value: "an-old-key-shape"}
	report := RecordsReport{}
	changed, err := r.reprocessPage([]map[string]types.AttributeValue{storedItem}, func(product *Product) (bool, error) {
		return false, nil
	}, false, &report)
	if err != nil || report.Moved != 1 || len(changed) != 1 || attrText(changed[0].item["sk"]) == "an-old-key-shape" {
		t.Fatalf("a record under a key the schema no longer builds must move: %+v, %v", report, err)
	}
}

func TestReprocessPageFailsOneRecordNotTheRun(t *testing.T) {
	r := newProducts(t)
	storedItems := []map[string]types.AttributeValue{
		storedProductItem(t, r, Product{ID: "a", CategoryID: 7, Created: 1}),
		storedProductItem(t, r, Product{ID: "b", CategoryID: 7, Created: 2}),
		{"pk": numberAttr(1), "sk": &types.AttributeValueMemberS{Value: "broken"}},
	}
	report := RecordsReport{}
	changed, err := r.reprocessPage(storedItems, func(product *Product) (bool, error) {
		if product.ID == "a" {
			product.CategoryID = -1 // a key the encoding refuses: it panics there
		}
		return true, nil
	}, false, &report)
	if err != nil || len(changed) != 1 || len(report.Failures) != 2 {
		t.Fatalf("report = %+v, %d changed, %v", report, len(changed), err)
	}
	if !strings.Contains(report.Failures[0], "missing binary column") || !strings.Contains(report.Failures[1], "negative") {
		t.Fatalf("failures = %v", report.Failures)
	}
}

func TestSampleOfChangeShowsBeforeAndAfter(t *testing.T) {
	r := newProducts(t)
	storedItem := storedProductItem(t, r, Product{ID: "a", CategoryID: 7, Created: 1, Name: "old"})
	changed := Product{ID: "a", CategoryID: 7, Created: 1, Name: "new"}
	sample := r.sampleOfChange(storedItem, &changed)
	before, after, found := strings.Cut(sample, " → ")
	if !found || !strings.Contains(before, `"Name":"old"`) || !strings.Contains(after, `"Name":"new"`) {
		t.Fatalf("sample = %s", sample)
	}
}

func TestFullCopyRowMatchesComparesBlobAndVersion(t *testing.T) {
	expected := map[string]types.AttributeValue{dataColumn: &types.AttributeValueMemberB{Value: []byte{1, 2}}, versionColumn: numberAttr(4)}
	cases := []struct {
		name       string
		storedRow  map[string]types.AttributeValue
		wantsMatch bool
	}{
		{"same blob and version", map[string]types.AttributeValue{dataColumn: &types.AttributeValueMemberB{Value: []byte{1, 2}}, versionColumn: numberAttr(4)}, true},
		{"older blob", map[string]types.AttributeValue{dataColumn: &types.AttributeValueMemberB{Value: []byte{1}}, versionColumn: numberAttr(4)}, false},
		{"older version", map[string]types.AttributeValue{dataColumn: &types.AttributeValueMemberB{Value: []byte{1, 2}}, versionColumn: numberAttr(3)}, false},
		{"keys-only row", map[string]types.AttributeValue{}, false},
	}
	for _, testCase := range cases {
		if got := fullCopyRowMatches(testCase.storedRow, expected); got != testCase.wantsMatch {
			t.Fatalf("%s: match = %v", testCase.name, got)
		}
	}
}

// TestReprocessLive runs ReprocessRecords then Reprocess against a live DynamoDB (DYNAMO_ENDPOINT +
// DYNAMO_TABLE, as for TestArrayIndexSyncLive).
func TestReprocessLive(t *testing.T) {
	if os.Getenv("DYNAMO_ENDPOINT") == "" {
		t.Skip("DYNAMO_ENDPOINT not set: Reprocess needs a live DynamoDB")
	}
	r := newProducts(t)
	if _, err := r.DeleteRecordsAll(); err != nil {
		t.Fatal(err)
	}
	if err := r.PutMany([]Product{
		{ID: "a", CategoryID: 7, Created: 1, Name: "old", Labels: []string{"red"}},
		{ID: "b", CategoryID: 7, Created: 2, Name: "old", TagIDs: []int32{5}},
	}); err != nil {
		t.Fatal(err)
	}
	renameOld := func(product *Product) (bool, error) {
		if product.Name != "old" {
			return false, nil
		}
		product.Name = "new"
		return true, nil
	}

	dryRun, err := r.ReprocessRecords(renameOld, false)
	if err != nil || dryRun.Scanned != 2 || dryRun.Changed != 2 || dryRun.Written != 0 || len(dryRun.Samples) != 2 {
		t.Fatalf("dry run = %+v, %v", dryRun, err)
	}
	written, err := r.ReprocessRecords(renameOld, true)
	if err != nil || written.Written != 2 || len(written.Failures) != 0 {
		t.Fatalf("write = %+v, %v", written, err)
	}
	if again, err := r.ReprocessRecords(renameOld, true); err != nil || again.Changed != 0 {
		t.Fatalf("an idempotent transform must change nothing the second time: %+v, %v", again, err)
	}
	stored, err := r.Get(Product{ID: "a", CategoryID: 7, Created: 1})
	if err != nil || stored.Name != "new" || stored.Version != 1 {
		t.Fatalf("stored = %+v, %v (the Version must not be raised)", stored, err)
	}

	// The FullCopy row of "a" still holds the old blob, and a row of an index no longer declared lingers.
	client, err := Client()
	if err != nil {
		t.Fatal(err)
	}
	orphanRow := itemKey(r.meta.pkValue(unsafe.Pointer(stored))+"099", "x#y")
	if _, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String(tableName()), Item: orphanRow}); err != nil {
		t.Fatal(err)
	}
	report, err := r.Reprocess(true)
	if err != nil || report.Records != 2 || report.HiddenRowsPut != 1 || report.HiddenRowsDeleted != 1 {
		t.Fatalf("Reprocess = %+v, %v", report, err)
	}
	var fromCopy []Product
	if err := r.Query().Eq(r.T.CategoryID, int32(7)).Contains(r.T.Labels, "red").Exec(&fromCopy); err != nil {
		t.Fatal(err)
	}
	if len(fromCopy) != 1 || fromCopy[0].Name != "new" {
		t.Fatalf("the FullCopy row was not refreshed: %+v", fromCopy)
	}
	if again, err := r.Reprocess(true); err != nil || again.HiddenRowsPut != 0 || again.HiddenRowsDeleted != 0 {
		t.Fatalf("a second Reprocess must find nothing to fix: %+v, %v", again, err)
	}
}

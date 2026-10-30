package dynamo

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// TestQueryScanRecordsKeepsWhatItReadPastTheBudget fills one partition with ~6 MB of records and
// filters them on a column no key serves: the QueryScan stops at its 5 MB budget, and
// QueryScanRecords still returns the records it matched until then, with ErrQueryScanBudget.
func TestQueryScanRecordsKeepsWhatItReadPastTheBudget(t *testing.T) {
	dynamoForTextSearchTest(t) // a fresh table on the DynamoDB Local of GENIX_ORM_DYNAMO_TEST
	products := newProducts(t)

	padding := strings.Repeat("x", 100<<10)
	records := []Product{}
	for index := range 60 {
		records = append(records, Product{ID: fmt.Sprint(index), CategoryID: 7, Created: int64(index + 1), Name: padding})
	}
	if err := products.PutMany(records); err != nil {
		t.Fatal(err)
	}

	matched, err := products.QueryScanRecords([]QueryPredicate{
		{Field: "CategoryID", Op: "=", Value: 7.0},
		{Field: "Name", Op: "begins_with", Value: "x"},
	}, 400, false)
	if !errors.Is(err, ErrQueryScanBudget) {
		t.Fatalf("got error %v, want ErrQueryScanBudget", err)
	}
	if len(matched) == 0 || len(matched) >= len(records) {
		t.Fatalf("got %d records, want the partial result read within the budget (0 < n < %d)", len(matched), len(records))
	}
}

package ormcheck

import (
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ivanjoz/genix-orm/dynamo"
)

// Reads right after a write may lag: GSIs are always eventually consistent, and the ORM's base
// Query and GetItem are too. A read that doesn't match yet is retried, and the capacity shown is
// that of the last attempt.
const (
	readAttempts   = 10
	readRetryDelay = 300 * time.Millisecond
)

const checkStoreID int32 = 7

// Run works on the ORM's configured table. It wipes both check entities, writes one record into
// each, reads them back through every access path (and once more after updating them), wipes
// them again, and prints one line per step with its result and consumed capacity. It returns an
// error when any check failed.
func Run(output io.Writer) error {
	runner := &checkRunner{output: output}
	// Leftovers of an interrupted run would change every expected result.
	if _, err := CheckOrders.DeleteRecordsAll(); err != nil {
		return err
	}
	if _, err := CheckProducts.DeleteRecordsAll(); err != nil {
		return err
	}
	fmt.Fprintln(output, "Capacity: an eventually consistent read costs 0.5 RCU per 4 KB, a consistent one 1 RCU;")
	fmt.Fprintln(output, "a write costs 1 WCU per 1 KB and item. Each line shows the calls the ORM made for that step.")

	order := CheckOrder{
		StoreID: checkStoreID, Created: 1_000, ID: 1, CustomerID: 55, Channel: "web", Status: 2,
		Code: "ORD-0001", ProductIDs: []int32{10, 20, 30}, Tags: []string{"gift", "express"}, Total: 4_500,
	}
	product := CheckProduct{ID: 5, Brand: "acme", Price: 1_299, Name: "Widget", CategoryIDs: []int16{3, 4}}

	runner.section("Writes: a Put first reads the stored version (consistent) to diff its array rows")
	if err := runner.write("Put order: base + 3 ProductIDs rows + 2 Tags rows (keys-only)", func() error { return CheckOrders.Put(&order) }); err != nil {
		return err
	}
	if err := runner.write("Put product: base + 2 CategoryIDs rows (FullCopy)", func() error { return CheckProducts.Put(&product) }); err != nil {
		return err
	}

	orders := CheckOrders.T
	storeOrders := func() *dynamo.QueryBuilder[CheckOrder] { return CheckOrders.Query().Eq(orders.StoreID, checkStoreID) }
	runner.section(fmt.Sprintf("Orders (TableID %d): pk = TableID ‖ StoreID, sk = Created ‖ ID", CheckOrders.Schema().TableID))
	runner.expect("GetItem: full key", []string{"1"}, func() ([]string, error) {
		storedOrder, err := CheckOrders.Get(CheckOrder{StoreID: checkStoreID, Created: 1_000, ID: 1})
		if storedOrder == nil {
			return nil, err
		}
		return orderIDs([]CheckOrder{*storedOrder}), err
	})
	runner.expect("Base pk: StoreID = 7", []string{"1"}, queryOrders(storeOrders()))
	runner.expect("Base pk: StoreID = 8 (another partition)", nil, queryOrders(CheckOrders.Query().Eq(orders.StoreID, int32(8))))
	runner.expect("Packed sk range: Created BETWEEN 900 AND 1100", []string{"1"}, queryOrders(storeOrders().Between(orders.Created, int32(900), int32(1_100))))
	runner.expect("Packed sk range: Created BETWEEN 1100 AND 2000", nil, queryOrders(storeOrders().Between(orders.Created, int32(1_100), int32(2_000))))
	// Created is not the last sort column, so the stored sk is Created#ID: each bound on the
	// record's own Created (1000) must still include or exclude it exactly.
	runner.expect("Packed sk range: Created BETWEEN 900 AND 1000 (upper = value)", []string{"1"}, queryOrders(storeOrders().Between(orders.Created, int32(900), int32(1_000))))
	runner.expect("Packed sk range: Created <= 1000", []string{"1"}, queryOrders(storeOrders().Lte(orders.Created, int32(1_000))))
	runner.expect("Packed sk range: Created < 1000", nil, queryOrders(storeOrders().Lt(orders.Created, int32(1_000))))
	runner.expect("Packed sk range: Created >= 1000", []string{"1"}, queryOrders(storeOrders().Gte(orders.Created, int32(1_000))))
	runner.expect("Packed sk range: Created > 1000", nil, queryOrders(storeOrders().Gt(orders.Created, int32(1_000))))
	runner.expect("Packed sk prefix: Created = 1000 (begins_with)", []string{"1"}, queryOrders(storeOrders().Eq(orders.Created, int32(1_000))))
	runner.expect("Packed sk: Created = 1000 AND ID >= 1 (bounded BETWEEN)", []string{"1"}, queryOrders(storeOrders().Eq(orders.Created, int32(1_000)).Gte(orders.ID, int32(1))))
	runner.expect("Packed sk: Created = 1000 AND ID > 1 (bounded BETWEEN)", nil, queryOrders(storeOrders().Eq(orders.Created, int32(1_000)).Gt(orders.ID, int32(1))))
	runner.expect("Packed sk: Created = 1000 AND ID < 1 (strict, post-filter)", nil, queryOrders(storeOrders().Eq(orders.Created, int32(1_000)).Lt(orders.ID, int32(1))))
	runner.expect("Base pk: Desc + Limit 1", []string{"1"}, queryOrders(storeOrders().Desc().Limit(1)))
	runner.expect("GSI n1 (number): CustomerID = 55", []string{"1"}, queryOrders(CheckOrders.Query().Eq(orders.CustomerID, int32(55))))
	runner.expect("GSI s1 (composite string): Channel = web, Status = 2", []string{"1"}, queryOrders(CheckOrders.Query().Eq(orders.Channel, "web").Eq(orders.Status, int8(2))))
	runner.expect("GSI s1 (composite string): Channel = web, Status = 3", nil, queryOrders(CheckOrders.Query().Eq(orders.Channel, "web").Eq(orders.Status, int8(3))))
	runner.expect("GSI s2 (string): Code = ORD-0001", []string{"1"}, queryOrders(CheckOrders.Query().Eq(orders.Code, "ORD-0001")))
	runner.expect("In-memory post-filter: StoreID = 7 AND Total >= 4000", []string{"1"}, queryOrders(storeOrders().Gte(orders.Total, int64(4_000))))
	runner.expect("In-memory post-filter: StoreID = 7 AND Total >= 5000", nil, queryOrders(storeOrders().Gte(orders.Total, int64(5_000))))
	runner.expect("Array keys-only: ProductIDs contains 20", []string{"1"}, queryOrders(storeOrders().Contains(orders.ProductIDs, 20)))
	runner.expect("Array keys-only: ProductIDs contains 99 or 30", []string{"1"}, queryOrders(storeOrders().Contains(orders.ProductIDs, 99, 30)))
	runner.expect("Array keys-only: ProductIDs contains 99", nil, queryOrders(storeOrders().Contains(orders.ProductIDs, 99)))
	runner.expect("Array keys-only: Tags contains gift AND Created 900..1100", []string{"1"}, queryOrders(storeOrders().Contains(orders.Tags, "gift").Between(orders.Created, int32(900), int32(1_100))))
	runner.expect("Array keys-only: ProductIDs contains 20 AND Created > 1000", nil, queryOrders(storeOrders().Contains(orders.ProductIDs, 20).Gt(orders.Created, int32(1_000))))

	products := CheckProducts.T
	runner.section(fmt.Sprintf("Products (TableID %d): no Partition, pk = TableID, sk = ID", CheckProducts.Schema().TableID))
	runner.expect("GetItem: full key", []string{"5 Widget"}, func() ([]string, error) {
		storedProduct, err := CheckProducts.Get(CheckProduct{ID: 5})
		if storedProduct == nil {
			return nil, err
		}
		return productLabels([]CheckProduct{*storedProduct}), err
	})
	runner.expect("Base pk: the whole entity", []string{"5 Widget"}, queryProducts(CheckProducts.Query()))
	runner.expect("Packed sk range: ID BETWEEN 1 AND 9", []string{"5 Widget"}, queryProducts(CheckProducts.Query().Between(products.ID, int32(1), int32(9))))
	runner.expect("GSI n1 (number): Price = 1299", []string{"5 Widget"}, queryProducts(CheckProducts.Query().Eq(products.Price, int32(1_299))))
	runner.expect("GSI s1 (string): Brand = acme", []string{"5 Widget"}, queryProducts(CheckProducts.Query().Eq(products.Brand, "acme")))
	runner.expect("Array FullCopy: CategoryIDs contains 4", []string{"5 Widget"}, queryProducts(CheckProducts.Query().Contains(products.CategoryIDs, 4)))
	runner.expect("Array FullCopy: CategoryIDs contains 3 or 4 (returned once)", []string{"5 Widget"}, queryProducts(CheckProducts.Query().Contains(products.CategoryIDs, 3, 4)))
	runner.expect("Array FullCopy: CategoryIDs contains 8", nil, queryProducts(CheckProducts.Query().Contains(products.CategoryIDs, 8)))

	runner.section("Updates: only keys-only rows that changed are written; FullCopy rewrites every row")
	order.ProductIDs, order.Total = []int32{20, 30, 40}, 5_200
	if err := runner.write("Put order: ProductIDs 10,20,30 -> 20,30,40 (+1 row, -1 row)", func() error { return CheckOrders.Put(&order) }); err != nil {
		return err
	}
	product.CategoryIDs, product.Name = []int16{4, 9}, "Widget v2"
	if err := runner.write("Put product: CategoryIDs 3,4 -> 4,9 and a new Name (2 rows, -1 row)", func() error { return CheckProducts.Put(&product) }); err != nil {
		return err
	}
	runner.expect("Order: ProductIDs contains 10 (removed)", nil, queryOrders(storeOrders().Contains(orders.ProductIDs, 10)))
	runner.expect("Order: ProductIDs contains 40 (added)", []string{"1"}, queryOrders(storeOrders().Contains(orders.ProductIDs, 40)))
	runner.expect("Product: CategoryIDs contains 3 (removed)", nil, queryProducts(CheckProducts.Query().Contains(products.CategoryIDs, 3)))
	runner.expect("Product: CategoryIDs contains 9 (added)", []string{"5 Widget v2"}, queryProducts(CheckProducts.Query().Contains(products.CategoryIDs, 9)))
	runner.expect("Product: CategoryIDs contains 4 (kept, copy rewritten)", []string{"5 Widget v2"}, queryProducts(CheckProducts.Query().Contains(products.CategoryIDs, 4)))

	// DeleteRecordsAll is not repeatable (a second run deletes nothing), so it is checked once.
	runner.section("Cleanup: the row counts prove no stale array rows were left behind")
	runner.expectOnce("DeleteRecordsAll orders: base + 3 ProductIDs + 2 Tags rows", []string{"6"}, func() ([]string, error) {
		deleted, err := CheckOrders.DeleteRecordsAll()
		return []string{strconv.Itoa(deleted)}, err
	})
	runner.expectOnce("DeleteRecordsAll products: base + 2 CategoryIDs rows", []string{"3"}, func() ([]string, error) {
		deleted, err := CheckProducts.DeleteRecordsAll()
		return []string{strconv.Itoa(deleted)}, err
	})

	fmt.Fprintf(output, "\n%d passed, %d failed\n", runner.passed, runner.failed)
	if runner.failed > 0 {
		return fmt.Errorf("%d ORM check(s) failed", runner.failed)
	}
	return nil
}

type checkRunner struct {
	output         io.Writer
	passed, failed int
}

func (runner *checkRunner) section(title string) {
	fmt.Fprintf(runner.output, "\n%s\n", title)
}

// write runs one write and prints its capacity. A failed write stops the check: every read after
// it would fail for the same reason.
func (runner *checkRunner) write(name string, write func() error) error {
	meter.reset()
	if err := write(); err != nil {
		runner.failed++
		fmt.Fprintf(runner.output, "  ✗ %-62s error: %v\n", name, err)
		return err
	}
	fmt.Fprintf(runner.output, "  ✎ %-62s %-15s %s\n", name, "", meter.summary())
	return nil
}

func (runner *checkRunner) expect(name string, want []string, read func() ([]string, error)) {
	runner.runCheck(name, want, readAttempts, read)
}

func (runner *checkRunner) expectOnce(name string, want []string, read func() ([]string, error)) {
	runner.runCheck(name, want, 1, read)
}

func (runner *checkRunner) runCheck(name string, want []string, maxAttempts int, read func() ([]string, error)) {
	var got []string
	var err error
	attempts := 0
	for attempts < maxAttempts {
		attempts++
		meter.reset()
		got, err = read()
		if err == nil && slices.Equal(got, want) {
			break
		}
		if attempts < maxAttempts {
			time.Sleep(readRetryDelay)
		}
	}

	passed := err == nil && slices.Equal(got, want)
	mark, result := "✓", "["+strings.Join(got, ", ")+"]"
	if passed {
		runner.passed++
	} else {
		runner.failed++
		mark = "✗"
	}
	if err != nil {
		result = "error: " + err.Error()
	}
	line := fmt.Sprintf("  %s %-62s %-15s %s", mark, name, result, meter.summary())
	if attempts > 1 {
		line += fmt.Sprintf("  (attempt %d)", attempts)
	}
	fmt.Fprintln(runner.output, line)
	if !passed {
		fmt.Fprintf(runner.output, "      want [%s]\n", strings.Join(want, ", "))
	}
}

func queryOrders(query *dynamo.QueryBuilder[CheckOrder]) func() ([]string, error) {
	return func() ([]string, error) {
		var found []CheckOrder
		err := query.Exec(&found)
		return orderIDs(found), err
	}
}

func queryProducts(query *dynamo.QueryBuilder[CheckProduct]) func() ([]string, error) {
	return func() ([]string, error) {
		var found []CheckProduct
		err := query.Exec(&found)
		return productLabels(found), err
	}
}

func orderIDs(found []CheckOrder) []string {
	ids := make([]string, len(found))
	for i, order := range found {
		ids[i] = strconv.Itoa(int(order.ID))
	}
	return ids
}

// productLabels include the Name, so a FullCopy row that was not rewritten shows its stale copy.
func productLabels(found []CheckProduct) []string {
	labels := make([]string, len(found))
	for i, product := range found {
		labels[i] = fmt.Sprintf("%d %s", product.ID, product.Name)
	}
	return labels
}

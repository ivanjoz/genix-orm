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

// Run works on the ORM's configured table. It wipes the check entities, writes records into
// each, reads them back through every access path (and once more after updating them), wipes
// them again, and prints one line per step with its result and consumed capacity. It returns an
// error when any check failed. It sets an in-memory DataFrame store (dataframe.Configure): run it from a
// process that serves no writes.
func Run(output io.Writer) error {
	runner := &checkRunner{output: output}
	// Leftovers of an interrupted run would change every expected result.
	for _, checkEntity := range []dynamo.Controller{CheckOrders, CheckProducts, CheckFrameLines} {
		if _, err := checkEntity.DeleteRecordsAll(); err != nil {
			return err
		}
	}
	fmt.Fprintln(output, "Capacity: an eventually consistent read costs 0.5 RCU per 4 KB, a consistent one 1 RCU;")
	fmt.Fprintln(output, "a write costs 1 WCU per 1 KB and item. Each line shows the calls the ORM made for that step.")

	order := CheckOrder{
		StoreID: checkStoreID, Created: 1_000, ID: 1, CustomerID: 55, Channel: "web", Status: 2,
		Code: "ORD-0001", ProductIDs: []int32{10, 20, 30}, Tags: []string{"gift", "express"}, Total: 4_500, Weight: 1.25,
	}
	product := CheckProduct{ID: 5, Brand: "acme", Price: 1_299, Name: "Widget", CategoryIDs: []int16{3, 4}, Status: 1, TeamIDs: []int16{1, 2}}

	runner.section("Writes: a Put first reads the stored version (consistent) to diff its array rows")
	if err := runner.write("Put order: base + 3 ProductIDs + 2 Tags rows, 4 GroupBy counters", func() error { return CheckOrders.Put(&order) }); err != nil {
		return err
	}
	if err := runner.write("Put product: base + 2 CategoryIDs rows (FullCopy) + 1 + 2 TeamIDs delta rows", func() error { return CheckProducts.Put(&product) }); err != nil {
		return err
	}
	// What a client holds after syncing the first version: its Updated, and the window's fingerprint.
	firstUpdated := product.Updated
	heldFirstVersion := dynamo.DeltaSince{Updated: firstUpdated, Fingerprint: dynamo.DeltaFingerprint([]int64{firstUpdated})}

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
	// Created is not the last Keys column, so the stored sk is Created#ID: each bound on the
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
	runner.expect("GSI g1 (own partition): CustomerID = 55", []string{"1"}, queryOrders(CheckOrders.Query().Eq(orders.CustomerID, int32(55))))
	runner.expect("GSI g1 (own partition): CustomerID = 55, Created > 1000", nil, queryOrders(CheckOrders.Query().Eq(orders.CustomerID, int32(55)).Gt(orders.Created, int32(1_000))))
	runner.expect("GSI g2 (sorted): Channel = web, Status = 2", []string{"1"}, queryOrders(storeOrders().Eq(orders.Channel, "web").Eq(orders.Status, int8(2))))
	runner.expect("GSI g2 (sorted): Channel = web, Status = 3", nil, queryOrders(storeOrders().Eq(orders.Channel, "web").Eq(orders.Status, int8(3))))
	runner.expect("GSI g2 (sorted): Channel = web, Status >= 2", []string{"1"}, queryOrders(storeOrders().Eq(orders.Channel, "web").Gte(orders.Status, int8(2))))
	runner.expect("GSI g2 (sorted): Channel prefix web", []string{"1"}, queryOrders(storeOrders().Eq(orders.Channel, "web")))
	runner.expect("GSI g3 (string): Code = ORD-0001", []string{"1"}, queryOrders(storeOrders().Eq(orders.Code, "ORD-0001")))
	storeOrdersScan := func() *dynamo.QueryBuilder[CheckOrder] {
		return CheckOrders.QueryScan().Eq(orders.StoreID, checkStoreID)
	}
	runner.expectRejected("Query(): StoreID = 7 AND Total >= 4000 (Total is not a key)", queryOrders(storeOrders().Gte(orders.Total, int64(4_000))))
	runner.expectRejected("Query(): CustomerID > 50 (range on a GSI hash)", queryOrders(CheckOrders.Query().Gt(orders.CustomerID, int32(50))))
	runner.expect("QueryScan(): StoreID = 7 AND Total >= 4000 (in memory)", []string{"1"}, queryOrders(storeOrdersScan().Gte(orders.Total, int64(4_000))))
	runner.expect("QueryScan(): StoreID = 7 AND Total >= 5000 (in memory)", nil, queryOrders(storeOrdersScan().Gte(orders.Total, int64(5_000))))
	runner.expect("QueryScan(): Tags contains gift AND Channel = web (in memory)", []string{"1"}, queryOrders(storeOrdersScan().Contains(orders.Tags, "gift").Eq(orders.Channel, "web")))
	runner.expectRejected("QueryScan(): Total >= 4000 alone (no index serves it)", queryOrders(CheckOrders.QueryScan().Gte(orders.Total, int64(4_000))))
	runner.expect("Array keys-only: ProductIDs contains 20", []string{"1"}, queryOrders(storeOrders().Contains(orders.ProductIDs, 20)))
	runner.expect("Array keys-only: ProductIDs contains 99 or 30", []string{"1"}, queryOrders(storeOrders().Contains(orders.ProductIDs, 99, 30)))
	runner.expect("Array keys-only: ProductIDs contains 99", nil, queryOrders(storeOrders().Contains(orders.ProductIDs, 99)))
	runner.expect("Array keys-only: Tags contains gift AND Created 900..1100", []string{"1"}, queryOrders(storeOrders().Contains(orders.Tags, "gift").Between(orders.Created, int32(900), int32(1_100))))
	runner.expect("Array keys-only: ProductIDs contains 20 AND Created > 1000", nil, queryOrders(storeOrders().Contains(orders.ProductIDs, 20).Gt(orders.Created, int32(1_000))))
	runner.expect("Array keys-only: ProductIDs = 20 (Eq) AND Created >= 1000", []string{"1"}, queryOrders(storeOrders().Eq(orders.ProductIDs, 20).Gte(orders.Created, int32(1_000))))
	runner.expect("Array keys-only: ProductIDs = 20, Created = 1000, ID >= 1", []string{"1"}, queryOrders(storeOrders().Eq(orders.ProductIDs, 20).Eq(orders.Created, int32(1_000)).Gte(orders.ID, int32(1))))

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
	runner.expect("GSI g1 (sorted): Price = 1299", []string{"5 Widget"}, queryProducts(CheckProducts.Query().Eq(products.Price, int32(1_299))))
	runner.expect("GSI g1 (sorted): Price BETWEEN 1000 AND 1500", []string{"5 Widget"}, queryProducts(CheckProducts.Query().Between(products.Price, int32(1_000), int32(1_500))))
	runner.expect("GSI g1 (sorted): Price > 1299", nil, queryProducts(CheckProducts.Query().Gt(products.Price, int32(1_299))))
	runner.expect("GSI g2 (string): Brand = acme", []string{"5 Widget"}, queryProducts(CheckProducts.Query().Eq(products.Brand, "acme")))
	runner.expect("Array FullCopy: CategoryIDs contains 4", []string{"5 Widget"}, queryProducts(CheckProducts.Query().Contains(products.CategoryIDs, 4)))
	runner.expect("Array FullCopy: CategoryIDs contains 3 or 4 (returned once)", []string{"5 Widget"}, queryProducts(CheckProducts.Query().Contains(products.CategoryIDs, 3, 4)))
	runner.expect("Array FullCopy: CategoryIDs contains 8", nil, queryProducts(CheckProducts.Query().Contains(products.CategoryIDs, 8)))
	runner.expect("Managed: Updated stamped by the Put", []string{"true"}, func() ([]string, error) {
		return []string{strconv.FormatBool(firstUpdated > 0)}, nil
	})
	runner.expect("Delta: first sync, Status 1", []string{"5 Widget"}, queryProducts(CheckProducts.Query().Delta(dynamo.DeltaSince{}, 1)))
	runner.expect("Delta: first sync, Status 3", nil, queryProducts(CheckProducts.Query().Delta(dynamo.DeltaSince{}, 3)))
	runner.expect("Delta: since the product's own version, window held", nil, queryProducts(CheckProducts.Query().Delta(heldFirstVersion, 1)))
	runner.expect("Delta: since its own version, window fingerprint off (resent)", []string{"5 Widget"},
		queryProducts(CheckProducts.Query().Delta(dynamo.DeltaSince{Updated: firstUpdated}, 1)))
	runner.expect("Delta: since the Updated before it", []string{"5 Widget"}, queryProducts(CheckProducts.Query().Delta(dynamo.DeltaSince{Updated: firstUpdated - 1}, 1)))
	runner.expect("Delta + Contains: TeamIDs contains 2, first sync", []string{"5 Widget"}, queryProducts(CheckProducts.Query().Contains(products.TeamIDs, 2).Delta(dynamo.DeltaSince{}, 1)))
	runner.expect("Delta + Contains: TeamIDs contains 9, first sync", nil, queryProducts(CheckProducts.Query().Contains(products.TeamIDs, 9).Delta(dynamo.DeltaSince{}, 1)))

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
	runner.expect("Delta: since the first version (moved rows)", []string{"5 Widget v2"}, queryProducts(CheckProducts.Query().Delta(heldFirstVersion, 1)))
	runner.expect("Delta + Contains: TeamIDs contains 1, since the first version", []string{"5 Widget v2"}, queryProducts(CheckProducts.Query().Contains(products.TeamIDs, 1).Delta(heldFirstVersion, 1)))

	if err := runGroupByChecks(runner, &order); err != nil {
		return err
	}

	// Modify writes, so each check runs once: a retry would race against its own earlier write.
	runner.section("Modify: a conditional read-modify-write that runs the change again when another write lands")
	runner.expectOnce("Modify product: a Put lands inside the change; both edits survive", []string{"2 runs, Widget v2 modified, price 1500"}, func() ([]string, error) {
		changeRuns := 0
		modified, err := CheckProducts.Modify(CheckProduct{ID: 5}, func(storedProduct *CheckProduct, exists bool) error {
			changeRuns++
			if changeRuns == 1 {
				concurrentProduct := *storedProduct
				concurrentProduct.Price = 1_500
				if err := CheckProducts.Put(&concurrentProduct); err != nil {
					return err
				}
			}
			storedProduct.Name = "Widget v2 modified"
			return nil
		})
		if err != nil || modified == nil {
			return nil, err
		}
		return []string{fmt.Sprintf("%d runs, %s, price %d", changeRuns, modified.Name, modified.Price)}, nil
	})
	runner.expectOnce("Modify product: a change that edits nothing writes nothing", []string{"same version"}, func() ([]string, error) {
		// Modify reads consistently, so two no-op runs must see the very same version.
		storedProduct, err := CheckProducts.Modify(CheckProduct{ID: 5}, func(*CheckProduct, bool) error { return nil })
		if err != nil || storedProduct == nil {
			return nil, err
		}
		modified, err := CheckProducts.Modify(CheckProduct{ID: 5}, func(*CheckProduct, bool) error { return nil })
		if err != nil || modified == nil {
			return nil, err
		}
		if modified.Updated != storedProduct.Updated {
			return []string{fmt.Sprintf("version %d -> %d", storedProduct.Updated, modified.Updated)}, nil
		}
		return []string{"same version"}, nil
	})

	runner.section("Soft delete: a first sync drops it, a later sync still sends it")
	product.Status, product.Name, product.TeamIDs = 0, "Widget deleted", []int16{2}
	if err := runner.write("Put product: Status 0, TeamIDs 1,2 -> 2 (every delta row moves)", func() error { return CheckProducts.Put(&product) }); err != nil {
		return err
	}
	runner.expect("Delta: first sync, Status 1", nil, queryProducts(CheckProducts.Query().Delta(dynamo.DeltaSince{}, 1)))
	runner.expect("Delta: since the first version, every status", []string{"5 Widget deleted"}, queryProducts(CheckProducts.Query().Delta(heldFirstVersion, 1)))
	runner.expect("Delta + Contains: TeamIDs contains 1 (removed)", nil, queryProducts(CheckProducts.Query().Contains(products.TeamIDs, 1).Delta(heldFirstVersion, 1)))

	if err := runDataFrameChecks(runner); err != nil {
		return err
	}

	// DeleteRecordsAll is not repeatable (a second run deletes nothing), so it is checked once.
	runner.section("Cleanup: the row counts prove no stale array rows were left behind")
	runner.expectOnce("DeleteRecordsAll orders: base + 3 ProductIDs + 2 Tags rows + 6 counters", []string{"12"}, func() ([]string, error) {
		deleted, err := CheckOrders.DeleteRecordsAll()
		return []string{strconv.Itoa(deleted)}, err
	})
	runner.expectOnce("DeleteRecordsAll products: base + 2 CategoryIDs + 1 delta + 1 TeamIDs delta rows", []string{"5"}, func() ([]string, error) {
		deleted, err := CheckProducts.DeleteRecordsAll()
		return []string{strconv.Itoa(deleted)}, err
	})
	runner.expectOnce("DeleteRecordsAll frame lines: 104 base + 104 delta rows + 2 frame states", []string{"210"}, func() ([]string, error) {
		deleted, err := CheckFrameLines.DeleteRecordsAll()
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

// expectRejected passes when the read fails before calling DynamoDB: the planner refused it.
func (runner *checkRunner) expectRejected(name string, read func() ([]string, error)) {
	meter.reset()
	_, err := read()
	mark, result := "✓", "rejected"
	if err == nil {
		mark, result = "✗", "was not rejected"
		runner.failed++
	} else {
		runner.passed++
	}
	fmt.Fprintf(runner.output, "  %s %-62s %-15s %s\n", mark, name, result, meter.summary())
	if err != nil {
		fmt.Fprintf(runner.output, "      %v\n", err)
	}
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

// runGroupByChecks follows the order counters through an update, a second order that joins, moves,
// is soft-deleted and deleted, and ends by proving the live counters equal a rebuild from the records.
func runGroupByChecks(runner *checkRunner, order *CheckOrder) error {
	orders := CheckOrders.T
	byChannelStatus := func() *dynamo.GroupQuery[CheckOrder] {
		return CheckOrders.QueryGroups(orders.Channel, orders.Status).Eq(orders.StoreID, checkStoreID)
	}
	byTag := func() *dynamo.GroupQuery[CheckOrder] {
		return CheckOrders.QueryGroups(orders.Tags).Eq(orders.StoreID, checkStoreID)
	}
	byCustomer := func() *dynamo.GroupQuery[CheckOrder] {
		return CheckOrders.QueryGroups(orders.CustomerID).Eq(orders.StoreID, checkStoreID)
	}

	runner.section("GroupBy counters: count and sums per group, ADDed after every write (order 1 Total is now 5200)")
	runner.expect("Channel+Status groups", []string{"web/2 c1 total 5200 weight 1.25"}, queryGroups(byChannelStatus()))
	runner.expect("Channel+Status: Channel = web, Status = 2 (exact sk)", []string{"web/2 c1 total 5200 weight 1.25"}, queryGroups(byChannelStatus().Eq(orders.Channel, "web").Eq(orders.Status, int8(2))))
	runner.expect("Channel+Status: Channel = web, Status < 2", nil, queryGroups(byChannelStatus().Eq(orders.Channel, "web").Lt(orders.Status, int8(2))))
	runner.expect("Fan-out: one group per Tags element", []string{"express c1 total 5200", "gift c1 total 5200"}, queryGroups(byTag()))
	runner.expect("Fan-out: Tags = gift", []string{"gift c1 total 5200"}, queryGroups(byTag().Eq(orders.Tags, "gift")))
	runner.expect("Slot-less: per CustomerID, float sum", []string{"55 c1 weight 1.25"}, queryGroups(byCustomer()))
	// Since resends the groups of the last DeltaOverlap below the watermark: a write may still be in flight.
	runner.expect("Since: the order's own Updated (resent from the overlap)", []string{"web/2 c1 total 5200 weight 1.25"},
		queryGroups(byChannelStatus().Since(order.Updated)))
	runner.expect("Since: a DeltaOverlap past the order's Updated", nil, queryGroups(byChannelStatus().Since(order.Updated+dynamo.DeltaOverlap.Milliseconds()+1)))
	runner.expect("Since: first sync", []string{"web/2 c1 total 5200 weight 1.25"}, queryGroups(byChannelStatus().Since(0)))
	runner.expectRejected("Since on a GroupBy without GroupDelta", queryGroups(byTag().Since(0)))

	secondOrder := CheckOrder{
		StoreID: checkStoreID, Created: 1_001, ID: 2, CustomerID: 56, Channel: "web", Status: 2,
		Code: "ORD-0002", ProductIDs: []int32{10}, Tags: []string{"gift"}, Total: 300, Weight: 0.5,
	}
	if err := runner.write("Put order 2: joins web/2 and gift", func() error { return CheckOrders.Put(&secondOrder) }); err != nil {
		return err
	}
	runner.expect("Channel+Status: web/2 holds both orders", []string{"web/2 c2 total 5500 weight 1.75"}, queryGroups(byChannelStatus()))
	runner.expect("Fan-out: gift holds both orders", []string{"express c1 total 5200", "gift c2 total 5500"}, queryGroups(byTag()))

	secondOrder.Status = 3
	if err := runner.write("Put order 2: Status 2 -> 3 (leaves web/2, joins web/3)", func() error { return CheckOrders.Put(&secondOrder) }); err != nil {
		return err
	}
	runner.expect("Channel+Status: the order moved", []string{"web/2 c1 total 5200 weight 1.25", "web/3 c1 total 300 weight 0.5"}, queryGroups(byChannelStatus()))

	updatedBeforeSoftDelete := secondOrder.Updated
	secondOrder.Status = 0
	if err := runner.write("Put order 2: Status 0 (soft delete: leaves every group)", func() error { return CheckOrders.Put(&secondOrder) }); err != nil {
		return err
	}
	runner.expect("Channel+Status: the emptied web/3 is left out", []string{"web/2 c1 total 5200 weight 1.25"}, queryGroups(byChannelStatus()))
	// web/2 comes too: the move it lost order 2 in was stamped updatedBeforeSoftDelete, inside the overlap.
	runner.expect("Since: the emptied group still reaches a later sync", []string{"web/2 c1 total 5200 weight 1.25", "web/3 c0 total 0 weight 0"},
		queryGroups(byChannelStatus().Since(updatedBeforeSoftDelete)))
	runner.expect("Fan-out: gift is back to order 1", []string{"express c1 total 5200", "gift c1 total 5200"}, queryGroups(byTag()))
	runner.expect("Slot-less: customer 56 emptied", []string{"55 c1 weight 1.25"}, queryGroups(byCustomer()))

	if err := runner.write("Delete order 2: it counted in no group anymore", func() error { return CheckOrders.Delete(&secondOrder) }); err != nil {
		return err
	}
	// A rebuild Puts only the counters that differ from the records: 0 proves every ADD above was exact.
	runner.expectOnce("RebuildGroups StoreID 7: counters rewritten", []string{"0"}, func() ([]string, error) {
		rewritten, err := CheckOrders.RebuildGroups(checkStoreID)
		return []string{strconv.Itoa(rewritten)}, err
	})
	runner.expectOnce("RebuildGroupsAll: counters rewritten", []string{"0"}, func() ([]string, error) {
		rewritten, err := CheckOrders.RebuildGroupsAll()
		return []string{strconv.Itoa(rewritten)}, err
	})
	return nil
}

// queryGroups labels each group by its Keys, count and sums, whichever GroupBy it belongs to.
func queryGroups(query *dynamo.GroupQuery[CheckOrder]) func() ([]string, error) {
	orders := CheckOrders.T
	return func() ([]string, error) {
		groups, err := query.Exec()
		labels := make([]string, len(groups))
		for i, group := range groups {
			switch {
			case group.Key.Channel != "":
				labels[i] = fmt.Sprintf("%s/%d c%d total %d weight %g", group.Key.Channel, group.Key.Status, group.Count, group.Sum(orders.Total), group.SumFloat(orders.Weight))
			case len(group.Key.Tags) > 0:
				labels[i] = fmt.Sprintf("%s c%d total %d", group.Key.Tags[0], group.Count, group.Sum(orders.Total))
			default:
				labels[i] = fmt.Sprintf("%d c%d weight %g", group.Key.CustomerID, group.Count, group.SumFloat(orders.Weight))
			}
		}
		return labels, err
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

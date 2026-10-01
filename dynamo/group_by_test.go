package dynamo

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/ivanjoz/colbin"
)

// ── GroupBy test entity: a delta GroupBy on a GSI, a fan-out one, a slot-less one ──

type groupSale struct {
	StoreID        int32   `cb:"1"`
	ID             int32   `cb:"2"`
	Channel        string  `cb:"3"`
	Status         int8    `cb:"4"`
	ProductIDs     []int16 `cb:"5"`
	Total          int64   `cb:"6"`
	Weight         float64 `cb:"7"`
	Updated        int32   `cb:"8"`
	UpdatedVersion int32   `cb:"9"`
}

type groupSaleTable struct {
	Model[groupSaleTable, groupSale]
	StoreID        Col[*groupSaleTable, int32]
	ID             Col[*groupSaleTable, int32]
	Channel        Col[*groupSaleTable, string]
	Status         Col[*groupSaleTable, int8]
	ProductIDs     ColSlice[*groupSaleTable, int16]
	Total          Col[*groupSaleTable, int64]
	Weight         Col[*groupSaleTable, float64]
	Updated        Col[*groupSaleTable, int32]
	UpdatedVersion Col[*groupSaleTable, int32]
}

func (t groupSaleTable) GetSchema() Schema {
	return Schema{
		Entity:    "group_sale",
		TableID:   78901250,
		Partition: Cols(t.StoreID.Size(16)),
		Keys:      Cols(t.ID.Size(30)),
		Indexes: []Index{
			{Slot: S1, Keys: Cols(t.Channel, t.Status.Size(8)), GroupBy: Cols(t.Total, t.Weight), GroupDelta: true},
			{Keys: Cols(t.ProductIDs.Size(16)), GroupBy: Cols(t.Total)},
			{Keys: Cols(t.Channel), GroupBy: Cols(t.Total)},
		},
	}
}

var groupSales = NewRepo[groupSaleTable, groupSale]()

// Counter sks, built the way the ORM documents them.
func channelStatusGroupSK(channel string, status uint64) string {
	return "g003.004#" + channel + "#" + EncodeOrderedUint(status, 2)
}
func productGroupSK(productID uint64) string { return "g005#" + EncodeOrderedUint(productID, 3) }
func channelGroupSK(channel string) string   { return "g003#" + channel }

// describeGroupDeltas renders the non-zero deltas as "sk: c<count> <sums>", sorted by sk.
func describeGroupDeltas(deltas map[string]*groupCounterDelta) map[string]string {
	described := map[string]string{}
	for _, delta := range deltas {
		if !delta.isZero() {
			described[delta.sk] = fmt.Sprintf("c%+d %v", delta.count, delta.sums)
		}
	}
	return described
}

func groupDeltasOf(t *testing.T, stored, written *groupSale) map[string]string {
	t.Helper()
	deltas := map[string]*groupCounterDelta{}
	if err := groupSales.meta.addGroupCounterDeltas(deltas, unsafe.Pointer(stored), unsafe.Pointer(written)); err != nil {
		t.Fatal(err)
	}
	return describeGroupDeltas(deltas)
}

func expectGroupDeltas(t *testing.T, got, want map[string]string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("deltas = %v\nwant     %v", got, want)
	}
}

func TestGroupByCompiles(t *testing.T) {
	m := groupSales.meta
	if len(m.groupIndexes) != 3 || len(m.indexes) != 1 || len(m.arrayIndexes) != 1 {
		t.Fatalf("groupIndexes %d, GSIs %d, fan-out %d", len(m.groupIndexes), len(m.indexes), len(m.arrayIndexes))
	}
	channelStatus, byProduct, byChannel := m.groupIndexes[0], m.groupIndexes[1], m.groupIndexes[2]
	if channelStatus.tag != "g003.004" || !channelStatus.isDelta || len(channelStatus.sums) != 2 ||
		channelStatus.sums[0].attr != "s006" || channelStatus.sums[1].attr != "s007" || !channelStatus.sums[1].isFloat {
		t.Fatalf("channel+status GroupBy = %+v", channelStatus)
	}
	if byProduct.tag != "g005" || byProduct.elementPosition != 0 || byProduct.isDelta {
		t.Fatalf("product GroupBy = %+v", byProduct)
	}
	if byChannel.tag != "g003" || byChannel.elementPosition != -1 {
		t.Fatalf("channel GroupBy = %+v", byChannel)
	}
	if m.status == nil || m.writeVersion == nil {
		t.Fatal("a GroupDelta table must resolve Status and UpdatedVersion")
	}
}

func TestGroupDeltasOfANewRecord(t *testing.T) {
	sale := groupSale{StoreID: 1, ID: 7, Channel: "web", Status: 2, ProductIDs: []int16{1, 2, 2}, Total: 100, Weight: 0.29}
	expectGroupDeltas(t, groupDeltasOf(t, nil, &sale), map[string]string{
		channelStatusGroupSK("web", 2): "c+1 [100 290000]",
		productGroupSK(1):              "c+1 [100]",
		productGroupSK(2):              "c+1 [100]", // a repeated element counts once
		channelGroupSK("web"):          "c+1 [100]",
	})
}

func TestGroupDeltasOfAnUpdate(t *testing.T) {
	stored := groupSale{StoreID: 1, ID: 7, Channel: "web", Status: 2, ProductIDs: []int16{1, 2}, Total: 100, Weight: 1.5}

	unchanged := stored
	unchanged.Updated = 99 // not a GroupBy column
	expectGroupDeltas(t, groupDeltasOf(t, &stored, &unchanged), map[string]string{})

	// Same groups, the value moves down: a signed delta on the same counters.
	lowerTotal := stored
	lowerTotal.Total = 60
	expectGroupDeltas(t, groupDeltasOf(t, &stored, &lowerTotal), map[string]string{
		channelStatusGroupSK("web", 2): "c+0 [-40 0]",
		productGroupSK(1):              "c+0 [-40]",
		productGroupSK(2):              "c+0 [-40]",
		channelGroupSK("web"):          "c+0 [-40]",
	})

	// Status moves the record to another channel+status group, with its new Total.
	moved := stored
	moved.Status, moved.Total = 3, 120
	expectGroupDeltas(t, groupDeltasOf(t, &stored, &moved), map[string]string{
		channelStatusGroupSK("web", 2): "c-1 [-100 -1500000]",
		channelStatusGroupSK("web", 3): "c+1 [120 1500000]",
		productGroupSK(1):              "c+0 [20]",
		productGroupSK(2):              "c+0 [20]",
		channelGroupSK("web"):          "c+0 [20]",
	})

	// An element swap leaves the group of the kept element alone.
	swapped := stored
	swapped.ProductIDs = []int16{2, 3}
	expectGroupDeltas(t, groupDeltasOf(t, &stored, &swapped), map[string]string{
		productGroupSK(1): "c-1 [-100]",
		productGroupSK(3): "c+1 [100]",
	})
}

func TestGroupDeltasOfADeletion(t *testing.T) {
	stored := groupSale{StoreID: 1, ID: 7, Channel: "web", Status: 2, Total: 100}
	wantRemoved := map[string]string{
		channelStatusGroupSK("web", 2): "c-1 [-100 0]",
		channelGroupSK("web"):          "c-1 [-100]",
	}
	expectGroupDeltas(t, groupDeltasOf(t, &stored, nil), wantRemoved)

	// Status 0 is a soft delete: the record leaves every group, as on a Delete.
	softDeleted := stored
	softDeleted.Status = 0
	expectGroupDeltas(t, groupDeltasOf(t, &stored, &softDeleted), wantRemoved)

	// And restoring it joins them again.
	expectGroupDeltas(t, groupDeltasOf(t, &softDeleted, &stored), map[string]string{
		channelStatusGroupSK("web", 2): "c+1 [100 0]",
		channelGroupSK("web"):          "c+1 [100]",
	})
}

func TestGroupDeltasMergeAcrossTheCall(t *testing.T) {
	first := groupSale{StoreID: 1, ID: 1, Channel: "web", Status: 2, Total: 100, UpdatedVersion: 4}
	second := groupSale{StoreID: 1, ID: 2, Channel: "web", Status: 2, Total: 50, UpdatedVersion: 4}
	otherStore := groupSale{StoreID: 2, ID: 3, Channel: "web", Status: 2, Total: 10, UpdatedVersion: 9}

	callDeltas := map[string]*groupCounterDelta{}
	for _, sale := range []*groupSale{&first, &second, &otherStore} {
		recordDeltas := map[string]*groupCounterDelta{}
		if err := groupSales.meta.addGroupCounterDeltas(recordDeltas, nil, unsafe.Pointer(sale)); err != nil {
			t.Fatal(err)
		}
		mergeGroupCounterDeltas(callDeltas, recordDeltas)
	}
	// Two base partitions, two counters each: one per store and group.
	if len(callDeltas) != 4 {
		t.Fatalf("%d counters, want 4", len(callDeltas))
	}
	storeOnePK := groupSales.meta.pkValue(unsafe.Pointer(&first))
	merged := callDeltas[storeOnePK+"#"+channelStatusGroupSK("web", 2)]
	if merged == nil || merged.count != 2 || merged.sums[0] != 150 || merged.writeVersion != 4 {
		t.Fatalf("store 1 web/2 counter = %+v", merged)
	}
}

func TestGroupFloatSumsAreExact(t *testing.T) {
	m := groupSales.meta
	channelStatus := &m.groupIndexes[0]
	var sum int64
	for _, weight := range []float64{0.1, 0.2, 0.29, 1234.567891} {
		values, err := m.groupSumValues(channelStatus, unsafe.Pointer(&groupSale{Weight: weight}))
		if err != nil {
			t.Fatal(err)
		}
		sum += values[1]
	}
	if sum != 100_000+200_000+290_000+1_234_567_891 {
		t.Fatalf("scaled sum = %d", sum)
	}
	group := Group[groupSale]{groupIndex: channelStatus, sums: []int64{0, sum}}
	if got := group.SumFloat(groupSales.T.Weight); got != 1235.157891 {
		t.Fatalf("SumFloat = %v", got)
	}

	_, err := m.groupSumValues(channelStatus, unsafe.Pointer(&groupSale{Weight: 1e13}))
	if err == nil || !strings.Contains(err.Error(), "Weight") {
		t.Fatalf("an out-of-range float must fail the write, got %v", err)
	}
}

func TestGroupKeyBlobHoldsOnlyTheGroupKeys(t *testing.T) {
	m := groupSales.meta
	sale := groupSale{StoreID: 1, ID: 7, Channel: "web", Status: 2, ProductIDs: []int16{4, 5}, Total: 100}
	for _, check := range []struct {
		groupIndex   *groupIndexMeta
		elementIndex int
		want         groupSale
	}{
		{&m.groupIndexes[0], -1, groupSale{Channel: "web", Status: 2}},
		{&m.groupIndexes[1], 1, groupSale{ProductIDs: []int16{5}}},
	} {
		blob, err := m.groupKeyBlob(check.groupIndex, unsafe.Pointer(&sale), check.elementIndex)
		if err != nil {
			t.Fatal(err)
		}
		var key groupSale
		if err := colbin.Unmarshal(blob, &key); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(key, check.want) {
			t.Fatalf("%s key = %+v, want %+v", check.groupIndex.tag, key, check.want)
		}
	}
}

func TestQueryGroupsPlanErrors(t *testing.T) {
	sales := groupSales.T
	for name, query := range map[string]*GroupQuery[groupSale]{
		"Keys no GroupBy has":     groupSales.QueryGroups(sales.Status),
		"Keys out of order":       groupSales.QueryGroups(sales.Status, sales.Channel).Eq(sales.StoreID, 1),
		"no partition Eq":         groupSales.QueryGroups(sales.Channel, sales.Status),
		"Since without delta":     groupSales.QueryGroups(sales.Channel).Eq(sales.StoreID, 1).Since(5),
		"a field no key serves":   groupSales.QueryGroups(sales.Channel, sales.Status).Eq(sales.StoreID, 1).Eq(sales.Total, 5),
		"skipping a leading Key":  groupSales.QueryGroups(sales.Channel, sales.Status).Eq(sales.StoreID, 1).Eq(sales.Status, 2),
		"a range before the last": groupSales.QueryGroups(sales.Channel, sales.Status).Eq(sales.StoreID, 1).Gt(sales.Channel, "a").Eq(sales.Status, 2),
	} {
		if _, err := query.Exec(); err == nil || !strings.HasPrefix(err.Error(), "db: ") {
			t.Fatalf("%s: expected a plan error, got %v", name, err)
		}
	}
}

// ── Declaration rules ─────────────────────────────────────────────────────────

type badGroupRecord struct {
	ID      int32   `cb:"1"`
	Channel string  `cb:"2"`
	Total   int64   `cb:"3"`
	Tags    []int16 `cb:"4"`
	Status  int8    `cb:"5"`
}

type badGroupTable struct {
	Model[badGroupTable, badGroupRecord]
	ID      Col[*badGroupTable, int32]
	Channel Col[*badGroupTable, string]
	Total   Col[*badGroupTable, int64]
	Tags    ColSlice[*badGroupTable, int16]
	Status  Col[*badGroupTable, int8]
}

func TestGroupByDeclarationRules(t *testing.T) {
	for name, indexes := range map[string]func(t badGroupTable) []Index{
		"GroupDelta without GroupBy": func(t badGroupTable) []Index {
			return []Index{{Slot: S1, Keys: Cols(t.Channel), GroupDelta: true}}
		},
		"GroupDelta without UpdatedVersion": func(t badGroupTable) []Index {
			return []Index{{Keys: Cols(t.Channel), GroupBy: Cols(t.Total), GroupDelta: true}}
		},
		"GroupBy on a TypeDelta index": func(t badGroupTable) []Index {
			return []Index{{Type: TypeDelta, Keys: Cols(t.Status), GroupBy: Cols(t.Total)}}
		},
		"a string GroupBy column": func(t badGroupTable) []Index {
			return []Index{{Keys: Cols(t.Status.Size(8)), GroupBy: Cols(t.Channel)}}
		},
		"a slice GroupBy column": func(t badGroupTable) []Index {
			return []Index{{Keys: Cols(t.Channel), GroupBy: Cols(t.Tags)}}
		},
		"a column summed twice": func(t badGroupTable) []Index {
			return []Index{{Keys: Cols(t.Channel), GroupBy: Cols(t.Total, t.Total)}}
		},
		"two GroupBy on the same Keys": func(t badGroupTable) []Index {
			return []Index{{Keys: Cols(t.Channel), GroupBy: Cols(t.Total)}, {Slot: S1, Keys: Cols(t.Channel), GroupBy: Cols(t.ID)}}
		},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s: expected a panic", name)
				}
			}()
			tablePtr := new(badGroupTable)
			populateColumnNames(tablePtr)
			schema := Schema{Entity: "bad_group", TableID: 34567891, Keys: Cols(tablePtr.ID.Size(32)), Indexes: indexes(*tablePtr)}
			buildTableMeta(schema, reflect.TypeFor[badGroupRecord]())
		}()
	}
}

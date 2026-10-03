// Package ormcheck is a live check of genix-orm/dynamo: it writes one record into each of two
// check tables, reads them back through every access path the ORM has, and reports whether each
// read returned what was expected and how much capacity it consumed (see Run).
package ormcheck

import "github.com/ivanjoz/genix-orm/dynamo"

// CheckOrder exercises every key shape on a partitioned entity. Its fan-out indexes are keys-only:
// Contains reads the matching base records in a second BatchGetItem. Three of its indexes keep
// GroupBy counters: a delta one on a GSI, one per Tags element, and a slot-less one.
type CheckOrder struct {
	StoreID        int32    `cb:"1"`
	Created        int32    `cb:"2"`
	ID             int32    `cb:"3"`
	CustomerID     int32    `cb:"4"`
	Channel        string   `cb:"5"`
	Status         int8     `cb:"6"`
	Code           string   `cb:"7"`
	ProductIDs     []int32  `cb:"8"`
	Tags           []string `cb:"9"`
	Total          int64    `cb:"10"`
	Weight         float64  `cb:"11"`
	UpdatedVersion int32    `cb:"12"`
}

type CheckOrderTable struct {
	dynamo.Model[CheckOrderTable, CheckOrder]
	StoreID        dynamo.Col[CheckOrderTable, int32]
	Created        dynamo.Col[CheckOrderTable, int32]
	ID             dynamo.Col[CheckOrderTable, int32]
	CustomerID     dynamo.Col[CheckOrderTable, int32]
	Channel        dynamo.Col[CheckOrderTable, string]
	Status         dynamo.Col[CheckOrderTable, int8]
	Code           dynamo.Col[CheckOrderTable, string]
	ProductIDs     dynamo.ColSlice[CheckOrderTable, int32]
	Tags           dynamo.ColSlice[CheckOrderTable, string]
	Total          dynamo.Col[CheckOrderTable, int64]
	Weight         dynamo.Col[CheckOrderTable, float64]
	UpdatedVersion dynamo.Col[CheckOrderTable, int32]
}

func (table CheckOrderTable) GetSchema() dynamo.Schema {
	return dynamo.Schema{
		Name:      "ORM check: orders",
		Entity:    "ormcheck_order",
		Partition: dynamo.Cols(table.StoreID.Size(16)), // pk = TableID ‖ StoreID (5 digits)
		// The packed integer sort key: Created then ID, each order-preserving Base64.
		Keys: dynamo.Cols(table.Created.Size(32), table.ID.Size(24)),
		Indexes: []dynamo.Index{
			// A GSI with its own partition: one customer across every store, ranged on Created.
			{Slot: dynamo.G1, Partition: dynamo.Cols(table.CustomerID.Size(32)), Keys: dynamo.Cols(table.Created.Size(32))},
			// A GSI under the store sorted by Channel+Status, with delta counters: count, sum(Total),
			// sum(Weight) per Channel+Status.
			{Slot: dynamo.G2, Keys: dynamo.Cols(table.Channel, table.Status.Size(8)), GroupBy: dynamo.Cols(table.Total, table.Weight), GroupDelta: true},
			{Slot: dynamo.G3, Keys: dynamo.Cols(table.Code)}, // a string lookup under the store
			// Fan-out: row sk = ProductID ‖ Created ‖ base sk, so a product ranges on Created.
			{Keys: dynamo.Cols(table.ProductIDs.Size(32), table.Created.Size(32))},
			{Keys: dynamo.Cols(table.Tags), GroupBy: dynamo.Cols(table.Total)},                 // fan-out on the element alone, counted per tag
			{Keys: dynamo.Cols(table.CustomerID.Size(32)), GroupBy: dynamo.Cols(table.Weight)}, // counters only, no GSI
		},
	}
}

var CheckOrders = dynamo.NewRepo[CheckOrderTable, CheckOrder]()

// CheckProduct is an entity without Partition (the whole entity is the pk TableID). Its fan-out
// index is FullCopy: every element row carries the record, so Contains is a single Query. Its two
// delta indexes are keys-only: one row per record, and one per TeamIDs element.
type CheckProduct struct {
	ID             int32   `cb:"1"`
	Brand          string  `cb:"2"`
	Price          int32   `cb:"3"`
	Name           string  `cb:"4"`
	CategoryIDs    []int16 `cb:"5"`
	Status         int8    `cb:"6"`
	TeamIDs        []int16 `cb:"7"`
	Updated        int32   `cb:"8"`
	UpdatedVersion int32   `cb:"9"`
}

type CheckProductTable struct {
	dynamo.Model[CheckProductTable, CheckProduct]
	ID             dynamo.Col[CheckProductTable, int32]
	Brand          dynamo.Col[CheckProductTable, string]
	Price          dynamo.Col[CheckProductTable, int32]
	Name           dynamo.Col[CheckProductTable, string]
	CategoryIDs    dynamo.ColSlice[CheckProductTable, int16]
	Status         dynamo.Col[CheckProductTable, int8]
	TeamIDs        dynamo.ColSlice[CheckProductTable, int16]
	Updated        dynamo.Col[CheckProductTable, int32]
	UpdatedVersion dynamo.Col[CheckProductTable, int32]
}

func (table CheckProductTable) GetSchema() dynamo.Schema {
	return dynamo.Schema{
		Name:   "ORM check: products",
		Entity: "ormcheck_product",
		Keys:   dynamo.Cols(table.ID.Size(24)),
		Indexes: []dynamo.Index{
			{Slot: dynamo.G1, Keys: dynamo.Cols(table.Price.Size(32))},
			{Slot: dynamo.G2, Keys: dynamo.Cols(table.Brand)},
			{Keys: dynamo.Cols(table.CategoryIDs.Size(16)), FullCopy: true},
			{Type: dynamo.TypeDelta, Keys: dynamo.Cols(table.Status)},
			{Type: dynamo.TypeDelta, Keys: dynamo.Cols(table.TeamIDs.Size(8), table.Status)},
		},
	}
}

var CheckProducts = dynamo.NewRepo[CheckProductTable, CheckProduct]()

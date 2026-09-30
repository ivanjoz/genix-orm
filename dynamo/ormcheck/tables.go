// Package ormcheck is a live check of genix-orm/dynamo: it writes one record into each of two
// check tables, reads them back through every access path the ORM has, and reports whether each
// read returned what was expected and how much capacity it consumed (see Run).
package ormcheck

import "github.com/ivanjoz/genix-orm/dynamo"

// CheckOrder exercises every key shape on a partitioned entity. Its array indexes are keys-only:
// Contains reads the matching base records in a second BatchGetItem.
type CheckOrder struct {
	StoreID    int32    `cb:"1"`
	Created    int32    `cb:"2"`
	ID         int32    `cb:"3"`
	CustomerID int32    `cb:"4"`
	Channel    string   `cb:"5"`
	Status     int8     `cb:"6"`
	Code       string   `cb:"7"`
	ProductIDs []int32  `cb:"8"`
	Tags       []string `cb:"9"`
	Total      int64    `cb:"10"`
}

type CheckOrderTable struct {
	dynamo.Model[CheckOrderTable, CheckOrder]
	StoreID    dynamo.Col[CheckOrderTable, int32]
	Created    dynamo.Col[CheckOrderTable, int32]
	ID         dynamo.Col[CheckOrderTable, int32]
	CustomerID dynamo.Col[CheckOrderTable, int32]
	Channel    dynamo.Col[CheckOrderTable, string]
	Status     dynamo.Col[CheckOrderTable, int8]
	Code       dynamo.Col[CheckOrderTable, string]
	ProductIDs dynamo.ColSlice[CheckOrderTable, int32]
	Tags       dynamo.ColSlice[CheckOrderTable, string]
	Total      dynamo.Col[CheckOrderTable, int64]
}

func (table CheckOrderTable) GetSchema() dynamo.Schema {
	return dynamo.Schema{
		Name:      "ORM check: orders",
		Entity:    "ormcheck_order",
		Partition: dynamo.Keys(table.StoreID.Size(16)), // pk = TableID ‖ StoreID (5 digits)
		// The packed integer sort key: Created then ID, each order-preserving Base64.
		Sort: dynamo.Keys(table.Created.Size(32), table.ID.Size(24)),
		Indexes: []dynamo.Index{
			{Slot: dynamo.N1, Keys: dynamo.Keys(table.CustomerID.Size(32))},           // numeric GSI
			{Slot: dynamo.S1, Keys: dynamo.Keys(table.Channel, table.Status.Size(8))}, // composite string GSI
			{Slot: dynamo.S2, Keys: dynamo.Keys(table.Code)},                          // single string GSI
		},
		ArrayIndexes: []dynamo.ArrayIndex{
			{Column: table.ProductIDs.Size(32)},
			{Column: table.Tags},
		},
	}
}

var CheckOrders = dynamo.NewRepo[CheckOrderTable, CheckOrder]()

// CheckProduct is an entity without Partition (the whole entity is the pk TableID). Its array
// index is FullCopy: every element row carries the record, so Contains is a single Query.
type CheckProduct struct {
	ID          int32   `cb:"1"`
	Brand       string  `cb:"2"`
	Price       int32   `cb:"3"`
	Name        string  `cb:"4"`
	CategoryIDs []int16 `cb:"5"`
}

type CheckProductTable struct {
	dynamo.Model[CheckProductTable, CheckProduct]
	ID          dynamo.Col[CheckProductTable, int32]
	Brand       dynamo.Col[CheckProductTable, string]
	Price       dynamo.Col[CheckProductTable, int32]
	Name        dynamo.Col[CheckProductTable, string]
	CategoryIDs dynamo.ColSlice[CheckProductTable, int16]
}

func (table CheckProductTable) GetSchema() dynamo.Schema {
	return dynamo.Schema{
		Name:   "ORM check: products",
		Entity: "ormcheck_product",
		Sort:   dynamo.Keys(table.ID.Size(24)),
		Indexes: []dynamo.Index{
			{Slot: dynamo.N1, Keys: dynamo.Keys(table.Price.Size(32))},
			{Slot: dynamo.S1, Keys: dynamo.Keys(table.Brand)},
		},
		ArrayIndexes: []dynamo.ArrayIndex{
			{Column: table.CategoryIDs.Size(16), FullCopy: true},
		},
	}
}

var CheckProducts = dynamo.NewRepo[CheckProductTable, CheckProduct]()

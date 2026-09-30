package dynamo

import (
	"fmt"
	"reflect"
)

// ─────────────────────────────────────────────────────────────────────────────
// Schema introspection
//
// GetSchema[Table]() returns a plain, JSON-serializable description of an
// entity's schema — physical table, entity namespace, the base-table primary
// key, and every GSI with its key columns and type. It's meant to be handed to
// a frontend (e.g. a table visualizer) over an API, so every field is exported
// with json tags and every enum is a stable string token, not an internal id.
//
//	schema := db.GetSchema[models.ProductTable]()
//	json.NewEncoder(w).Encode(schema)
//
// This is a cold, introspection-only path: it reads the schema straight from the
// table struct's GetSchema() and never builds accessors or touches metaCache
// (which the hot read/write paths own), so it needs only the table type — not
// the record type.
// ─────────────────────────────────────────────────────────────────────────────

// ColumnInfo describes one key column of a schema.
type ColumnInfo struct {
	Field string `json:"field"` // Go struct field name, e.g. "Category"
	Attr  string `json:"attr"`  // DynamoDB attribute name (defaults to Field)
	Type  string `json:"type"`  // value type: "string","int","uint","float","bool","other"
	Size  int8   `json:"size,omitempty"`
	// Size is the declared bit size (.Size(bits)) of this column when it is an
	// integer packed into a composite string key (sort key or a string GSI); the
	// key holds ceil(Size/6) Base64 chars. Zero for strings and native-number slots.
}

// IndexKind classifies an access path in a serialized schema.
const (
	IndexPrimary = "primary" // the base table's pk (+ shared sk range)
	IndexGSI     = "gsi"     // a global secondary index slot (n1..n5, s1..s5)
	IndexArray   = "array"   // a fan-out Index over a slice field: hidden rows under pk ‖ cb id
)

// IndexInfo describes one queryable access path: the base-table primary key or
// a GSI slot. In this store every GSI shares the base table's sort key as its
// range dimension, so Keys on TableSchema applies to the primary key and to
// every GSI alike (SharesSortKey is always true for a GSI here, and is exposed
// so the frontend can say so explicitly).
type IndexInfo struct {
	Kind          string       `json:"kind"`               // IndexPrimary | IndexGSI | IndexArray
	Name          string       `json:"name"`               // GSI name ("gsi-n1"...) or "" for the primary key and array indexes
	Attr          string       `json:"attr"`               // partition attribute: "pk", "n1".."n5" or "s1".."s5"
	IsNumber      bool         `json:"isNumber"`           // numeric partition attribute (pk, n1..n5) vs string
	SharesSortKey bool         `json:"sharesSortKey"`      // uses the table's shared sk as its range key
	Columns       []ColumnInfo `json:"columns"`            // the index's key columns, in order
	FullCopy      bool         `json:"fullCopy,omitempty"` // fan-out rows carry the record blob
}

// TableSchema is the JSON-serializable description of one entity's schema.
type TableSchema struct {
	Name      string       `json:"name"`      // optional label from Schema.Name (may be empty)
	Struct    string       `json:"struct"`    // Go table struct name, e.g. "ProductTable" (reflection)
	Entity    string       `json:"entity"`    // this entity's name
	TableID   int32        `json:"tableID"`   // the 8-digit prefix of every key of this entity
	TableName string       `json:"tableName"` // physical DynamoDB table (shared by all entities)
	Partition []ColumnInfo `json:"partition"` // base-table pk columns (after the TableID)
	Keys      []ColumnInfo `json:"keys"`      // sk columns: the record's key, shared as range by every GSI
	Indexes   []IndexInfo  `json:"indexes"`   // access paths: the primary key first, then the GSIs
	// Fields is every column of the table struct in declaration order — the whole
	// record shape, key columns included — so a visualizer can lay out its columns
	// before (or without) reading a single record.
	Fields []ColumnInfo `json:"fields"`

	// Autoincrement reports the schema's UseAutoincrement setting; AutoincPadding
	// is the random low-digit count (0 when disabled or unpadded). The ID field
	// filled by the ORM is always named "ID".
	Autoincrement  bool `json:"autoincrement"`
	AutoincPadding int  `json:"autoincPadding,omitempty"`
}

// GetSchema returns the serializable schema of a table type. T is the table
// struct (the one that embeds db.Model and defines GetSchema), e.g.
//
//	db.GetSchema[models.ProductTable]()
func GetSchema[T any]() TableSchema {
	tablePtr := new(T)
	populateColumnNames(tablePtr)

	sp, ok := any(*tablePtr).(schemaProvider)
	if !ok {
		var t T
		panic(fmt.Sprintf("db: %T does not implement GetSchema()", t))
	}
	schema := sp.GetSchema()

	out := TableSchema{
		Name:      schema.Name,
		Struct:    reflect.TypeOf((*T)(nil)).Elem().Name(),
		Entity:    schema.Entity,
		TableID:   resolveTableID(schema),
		TableName: tableName(),
		Partition: describeCols(schema.Partition),
		Keys:      describeCols(schema.Keys),
		Fields:    describeCols(tableColumns(tablePtr)),

		Autoincrement:  schema.UseAutoincrement,
		AutoincPadding: schema.AutoincrementRandomPadding,
	}
	if !schema.UseAutoincrement {
		out.AutoincPadding = 0
	}

	// The primary key is an access path too — list it first so a visualizer can
	// render all lookups uniformly.
	out.Indexes = append(out.Indexes, IndexInfo{
		Kind:          IndexPrimary,
		Attr:          "pk",
		IsNumber:      true,
		SharesSortKey: true,
		Columns:       out.Partition,
	})
	for _, idx := range schema.Indexes {
		if holdsSliceColumn(idx) {
			out.Indexes = append(out.Indexes, IndexInfo{
				Kind:          IndexArray,
				Attr:          "pk",
				IsNumber:      true,
				SharesSortKey: true, // the base sk follows the index Keys in the row sk
				Columns:       describeCols(idx.Keys),
				FullCopy:      idx.FullCopy,
			})
			continue
		}
		out.Indexes = append(out.Indexes, IndexInfo{
			Kind:          IndexGSI,
			Name:          idx.Slot.index,
			Attr:          idx.Slot.attr,
			IsNumber:      idx.Slot.isNumber,
			SharesSortKey: true,
			Columns:       describeCols(idx.Keys),
		})
	}
	return out
}

// Schema returns this repo's serializable schema — the method form of
// GetSchema[T], for when you already hold a *Repo.
func (r *Repo[T, E]) Schema() TableSchema { return GetSchema[T]() }

// tableColumns returns the column handles of a name-populated table struct, in
// field order, skipping the embedded Model.
func tableColumns(tablePtr any) []Coln {
	tableValue := reflect.ValueOf(tablePtr).Elem()
	columns := make([]Coln, 0, tableValue.NumField())
	for i := 0; i < tableValue.NumField(); i++ {
		if column, isColumn := tableValue.Field(i).Interface().(Coln); isColumn {
			columns = append(columns, column)
		}
	}
	return columns
}

// describeCols projects resolved key columns into their serializable form.
func describeCols(cols []Coln) []ColumnInfo {
	out := make([]ColumnInfo, 0, len(cols))
	for _, c := range cols {
		m := c.col()
		out = append(out, ColumnInfo{
			Field: m.fieldName,
			Attr:  m.attrName,
			Type:  m.kind.String(),
			Size:  m.bits,
		})
	}
	return out
}

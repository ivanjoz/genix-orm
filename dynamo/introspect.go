package dynamo

import (
	"fmt"
	"reflect"
	"slices"
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
	// integer packed into a composite string key (a sort key or a GSI range); the
	// key holds ceil(Size/6) Base64 chars. Partition columns take its decimal width.
}

// IndexKind classifies an access path in a serialized schema.
const (
	IndexPrimary = "primary" // the base table: pk + sk
	IndexGSI     = "gsi"     // a global secondary index slot (gsi-1..gsi-10): hN + rN
	IndexArray   = "array"   // a fan-out Index over a slice field: hidden rows under pk ‖ cb id
	IndexDelta   = "delta"   // a TypeDelta Index: hidden rows, its Keys then the managed UpdatedVersion
	IndexLocal   = "local"   // a TypeLocal Index: one hidden row per record, its Keys then the base Keys
)

// IndexInfo describes one access path: a hash (a number, TableID ‖ Partition,
// each needing an equality) and a sorted range over RangeColumns (an equality on
// a leading run of them, then one range).
type IndexInfo struct {
	Kind         string       `json:"kind"`               // IndexPrimary | IndexGSI | IndexArray | IndexDelta
	Name         string       `json:"name"`               // GSI name ("gsi-1"...) or "" for the base table and its hidden rows
	HashAttr     string       `json:"hashAttr"`           // "pk" or "h1".."h10"
	RangeAttr    string       `json:"rangeAttr"`          // "sk" or "r1".."r10"
	Partition    []ColumnInfo `json:"partition"`          // the hash columns after the TableID
	RangeColumns []ColumnInfo `json:"rangeColumns"`       // the range columns, in order
	FullCopy     bool         `json:"fullCopy,omitempty"` // fan-out rows carry the record blob
}

// TableSchema is the JSON-serializable description of one entity's schema.
type TableSchema struct {
	Name      string       `json:"name"`      // optional label from Schema.Name (may be empty)
	Struct    string       `json:"struct"`    // Go table struct name, e.g. "ProductTable" (reflection)
	Entity    string       `json:"entity"`    // this entity's name
	TableID   int32        `json:"tableID"`   // the 8-digit prefix of every key of this entity
	TableName string       `json:"tableName"` // physical DynamoDB table (shared by all entities)
	Partition []ColumnInfo `json:"partition"` // base-table pk columns (after the TableID)
	Keys      []ColumnInfo `json:"keys"`      // sk columns: the record's key
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

	// DataFrames names the entity's DataFrames, for the rebuild commands.
	DataFrames []string `json:"dataFrames,omitempty"`
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
	for _, frame := range schema.DataFrames {
		out.DataFrames = append(out.DataFrames, frame.Name)
	}

	// The primary key is an access path too — list it first so a visualizer can
	// render all lookups uniformly.
	out.Indexes = append(out.Indexes, IndexInfo{
		Kind:         IndexPrimary,
		HashAttr:     "pk",
		RangeAttr:    "sk",
		Partition:    out.Partition,
		RangeColumns: out.Keys,
	})
	for _, idx := range schema.Indexes {
		if idx.Type == TypeDelta || idx.Type == TypeLocal || holdsSliceColumn(idx) {
			kind := IndexArray
			if idx.Type == TypeDelta {
				kind = IndexDelta
			} else if idx.Type == TypeLocal {
				kind = IndexLocal
			}
			out.Indexes = append(out.Indexes, IndexInfo{
				Kind:         kind,
				HashAttr:     "pk",
				RangeAttr:    "sk",
				Partition:    out.Partition,
				RangeColumns: append(describeCols(idx.Keys), out.Keys...), // the row sk: index Keys, then base Keys
				FullCopy:     idx.FullCopy,
			})
			continue
		}
		// A slot-less GroupBy keeps counters only: it is no access path to the records.
		if idx.Slot.index == "" {
			continue
		}
		gsiPartition := out.Partition
		if len(idx.Partition) > 0 {
			gsiPartition = describeCols(idx.Partition)
		}
		out.Indexes = append(out.Indexes, IndexInfo{
			Kind:         IndexGSI,
			Name:         idx.Slot.index,
			HashAttr:     idx.Slot.hashAttr,
			RangeAttr:    idx.Slot.rangeAttr,
			Partition:    gsiPartition,
			RangeColumns: appendMissingColumns(describeCols(idx.Keys), out.Keys),
		})
	}
	return out
}

// appendMissingColumns appends the base Keys a GSI's Keys leave out: they close its range (compileGSI).
func appendMissingColumns(columns []ColumnInfo, baseKeys []ColumnInfo) []ColumnInfo {
	for _, baseKey := range baseKeys {
		if !slices.ContainsFunc(columns, func(column ColumnInfo) bool { return column.Field == baseKey.Field }) {
			columns = append(columns, baseKey)
		}
	}
	return columns
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

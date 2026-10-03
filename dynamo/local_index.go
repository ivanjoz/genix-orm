package dynamo

import (
	"fmt"
	"reflect"
)

// ─────────────────────────────────────────────────────────────────────────────
// Local indexes: a second sort order inside the record's own partition
//
// A DynamoDB LSI can only be declared when the table is created, and the ORM
// shares one table that already exists. A TypeLocal Index gives the same access
// path with the fan-out machinery (array_index.go): one hidden row per record,
//
//	pk = base pk ‖ the first Keys column's cb id (3 digits)
//	sk = composite(index Keys) # base sk
//	d  = the record blob, only with FullCopy
//
// kept in sync by every write like a fan-out row. The planner offers it next to
// the base table and the GSIs: Query().Eq(NameHash, h) reads the rows of one
// value, and the base records in a BatchGetItem (keys-only rows). Unlike a GSI it
// is in the base table, so Consistent() works on it.
// ─────────────────────────────────────────────────────────────────────────────

// compileLocalIndex resolves a TypeLocal Index into hidden rows without an
// element: elementPosition -1 makes arrayRowSKs write one row per record.
func compileLocalIndex(recordType reflect.Type, accessors map[string]*colAccessor, index Index) arrayIndexMeta {
	switch {
	case index.Slot.index != "":
		panic(fmt.Sprintf("db: %s local index declares Slot %s: it lives in the base table and takes none", recordType.Name(), index.Slot.index))
	case len(index.Keys) == 0:
		panic(fmt.Sprintf("db: %s local index declares no Keys", recordType.Name()))
	case holdsSliceColumn(index):
		panic(fmt.Sprintf("db: %s local index holds a ColSlice: declare a fan-out Index (no Type) instead", recordType.Name()))
	case len(index.GroupBy) > 0:
		panic(fmt.Sprintf("db: %s local index declares GroupBy: declare the counters on their own Index", recordType.Name()))
	}
	localIndex := resolveArrayIndex(recordType, accessors, index)
	localIndex.isLocal = true
	// The first key's cb id names the rows, as for a delta index: two hidden-rows
	// indexes on the same first column collide, and compile refuses them.
	localIndex.columnID = cbColumnID(recordType, localIndex.keys[0].fieldName)
	return localIndex
}

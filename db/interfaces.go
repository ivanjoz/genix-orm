package db

// The interface set below is what lets the ORM tie a record type and its table
// type together at compile time: T is always the *Table struct (typed column
// handles), E is always the record struct (plain data). Both embed TableStruct,
// which is what supplies these methods.

type TableSchemaInterface[T any] interface {
	GetSchema() TableSchema
	GetTableStruct() T
}

type TableBaseInterface[T any, E any] interface {
	GetBaseStruct() E
	GetTableStruct() T
}

type TableBaseInterfaceWithCounter[T any, E any] interface {
	TableBaseInterface[T, E]
	GetCounter(increment int, partValue any, secondPartValue ...any) (int64, error)
}

type TableStructInterfaceQuery[T any, E any] interface {
	SetRefSlice(*[]E)
}

type TableInterface[T any] interface {
	GetSchema() TableSchema
	GetTableStruct() T
}

// TableHandle is the minimum a Col's / ColSlice's table type argument must provide. It is
// deliberately NOT self-referential like TableInterface[T]: Col's argument is the pointer type
// *XTable (see column.go), and *XTable cannot satisfy TableInterface[*XTable] because
// GetTableStruct() T is inherited from TableStruct and returns the table by value.
//
// This is a partial check, and knowing what it does not catch matters. XTable and its record X
// embed the same TableStruct[XTable, X], so their method sets are identical — a record type
// argument (db.Col[*Expense, int32] where *ExpenseTable belongs) satisfies this and is caught by
// nothing until it nil-panics at query time. Only the self-referential form distinguishes the two,
// and that is the form the pointer argument rules out. Closing the gap fully would need
// Col[T TablePtr[D], D any, E any], and D is not inferrable in a field declaration, so all ~1,000
// declaration sites would have to name both types. Measured free: adding this constraint left the
// production binary byte-identical, because nothing inside Col calls a method on T.
type TableHandle interface {
	GetSchema() TableSchema
}

type TableQueryInterface[T any] interface {
	GetSchema() TableSchema
	SetWhere(string, string, any)
	Limit(int32) *T
	AllowFilter() *T
	Exec() error
}

// TableGenericQuery is the surface a read built entirely at runtime needs: every predicate and
// projection addressed by column name instead of by a typed column handle. It sits apart from
// TableQueryInterface so the narrower contract stays the one the driver's own admin paths assert.
type TableGenericQuery[T any] interface {
	TableQueryInterface[T]
	SetWhereIn(string, []any)
	SetBetween(string, any, any)
	Select(...Coln) *T
	OrderDesc() *T
}

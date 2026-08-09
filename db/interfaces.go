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

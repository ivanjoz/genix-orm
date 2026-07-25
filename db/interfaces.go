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

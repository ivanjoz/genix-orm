package db

import (
	"fmt"
	"reflect"
)

// Col is a statically-typed column handle. T is the table struct, E the column's
// Go value type. It serves two roles: at schema-declaration time it carries the
// packing modifiers (.DecimalSize, .Autoincrement, …), and in a query it builds
// predicates. Both are pure data — nothing here touches a storage engine.
type Col[T TableInterface[T], E any] struct {
	info         ColumnInfo
	schemaStruct *T
	tableInfo    *TableInfo
}

func (q Col[T, E]) GetInfo() ColumnInfo {
	if q.info.Type == 0 {
		typeOf := reflect.TypeOf((*E)(nil)).Elem().String()
		q.info.ColType = GetColTypeByName(typeOf)
		if q.info.Type == 0 {
			q.info.ColType = GetColTypeByID(TypeBlob)
		}
	}
	return q.info
}

func (q *Col[T, E]) GetInfoPointer() *ColumnInfo {
	if q.info.Type == 0 {
		q.info = q.GetInfo()
	}
	return &q.info
}

func (q Col[T, E]) DecimalSize(size int8) Col[T, E] {
	if size > 15 {
		panic("Decimal size TOO BIG in:" + q.GetName())
	}
	q.info.DecimalDigits = size
	return q
}

func (q Col[T, E]) Int32() Col[T, E] {
	q.info.UseInt32Packing = true
	return q
}

func (q Col[T, E]) CompositeBucketing(buketsSize ...int8) Col[T, E] {
	q.info.CompositeBucketSizes = buketsSize
	return q
}

func (q Col[T, E]) IsWeek() Col[T, E] {
	q.info.IsWeek = true
	return q
}

func (q Col[T, E]) Autoincrement(randSufixSize int8) Col[T, E] {
	if randSufixSize > 15 {
		panic("Rand sufix size TOO BIG in:" + q.GetName())
	}

	if randSufixSize == 0 {
		randSufixSize = -1
	}
	q.info.AutoincrementRandDigits = randSufixSize
	return q
}

func (q Col[T, E]) StoreAsWeek() Col[T, E] {
	q.info.StoreAsWeek = true
	return q
}

func (q Col[T, E]) GetName() string {
	return q.info.Name
}

func (c *Col[T, E]) SetTableInfo(tableInfo *TableInfo) {
	c.tableInfo = tableInfo
}

func (c *Col[T, E]) SetSchemaStruct(schemaStruct any) {
	if schema, ok := schemaStruct.(*T); ok {
		c.schemaStruct = schema
	} else {
		fmt.Println("no seteado!!")
	}
}

func (e *Col[T, E]) Exclude(v E) *T {
	return e.schemaStruct
}

func (e *Col[T, E]) Equals(v E) *T {
	return e.addStatement("=", any(v))
}

func (e *Col[T, E]) Contains(v any) *T {
	return e.addStatement("CONTAINS", v)
}

func (e *Col[T, E]) GreaterThan(v E) *T {
	return e.addStatement(">", any(v))
}

func (e *Col[T, E]) GreaterEqual(v E) *T {
	return e.addStatement(">=", any(v))
}

func (e *Col[T, E]) LessThan(v E) *T {
	return e.addStatement("<", any(v))
}

func (e *Col[T, E]) LessEqual(v E) *T {
	return e.addStatement("<=", any(v))
}

func (e *Col[T, E]) In(values_ ...E) *T {
	values := []any{}
	for _, v := range values_ {
		values = append(values, any(v))
	}
	e.tableInfo.Statements = append(e.tableInfo.Statements,
		ColumnStatement{Col: e.info.Name, Operator: "IN", Values: values})
	return e.schemaStruct
}

func (e *Col[T, E]) Between(v1 E, v2 E) *T {
	e.tableInfo.Statements = append(e.tableInfo.Statements, ColumnStatement{
		Col:      e.info.Name,
		Operator: "BETWEEN",
		From:     []ColumnStatement{{Col: e.info.Name, Value: v1}},
		To:       []ColumnStatement{{Col: e.info.Name, Value: v2}},
	})
	return e.schemaStruct
}

func (e *Col[T, E]) addStatement(operator string, value any) *T {
	e.tableInfo.Statements = append(e.tableInfo.Statements,
		ColumnStatement{Col: e.info.Name, Operator: operator, Value: value})
	return e.schemaStruct
}

func (e Col[T, E]) Sum() Col[T, E] {
	e.info.AggregateFn = "SUM"
	return e
}

func (e Col[T, E]) Avg() Col[T, E] {
	e.info.AggregateFn = "AVG"
	return e
}

func (e Col[T, E]) Max() Col[T, E] {
	e.info.AggregateFn = "MAX"
	return e
}

// ColSlice is the handle for a column holding a collection of primitives. E is
// the *element* type, so a []int32 column is declared ColSlice[XTable, int32].
type ColSlice[T TableInterface[T], E any] struct {
	info         ColumnInfo
	schemaStruct *T
	tableInfo    *TableInfo
}

func (q ColSlice[T, E]) GetInfo() ColumnInfo {
	if q.info.Type == 0 {
		typeOf := reflect.TypeOf((*E)(nil)).Elem().String()
		if typeOf[0] == '*' {
			typeOf = "*[]" + typeOf[1:]
		} else {
			typeOf = "[]" + typeOf
		}
		q.info.ColType = GetColTypeByName(typeOf)
		if q.info.ColType.Type == 0 {
			panic("No se reconoió el slice type:" + typeOf)
		}
	}
	return q.info
}

func (q *ColSlice[T, E]) GetInfoPointer() *ColumnInfo {
	if q.info.Type == 0 {
		q.info = q.GetInfo()
	}
	return &q.info
}

func (q ColSlice[T, E]) GetName() string {
	return q.info.Name
}

func (c *ColSlice[T, E]) SetTableInfo(tableInfo *TableInfo) {
	c.tableInfo = tableInfo
}

func (c *ColSlice[T, E]) SetSchemaStruct(schemaStruct any) {
	if schema, ok := schemaStruct.(*T); ok {
		c.schemaStruct = schema
	}
}

func (e *ColSlice[T, E]) Contains(v E) *T {
	e.tableInfo.Statements = append(e.tableInfo.Statements,
		ColumnStatement{Col: e.info.Name, Operator: "CONTAINS", Value: any(v)})
	return e.schemaStruct
}

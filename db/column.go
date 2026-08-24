package db

import (
	"fmt"
	"reflect"
)

// Col is a statically-typed column handle. T is the table struct, E the column's
// Go value type. It serves two roles: at schema-declaration time it carries the
// packing modifiers (.DecimalSize, .Autoincrement, …), and in a query it builds
// predicates. Both are pure data — nothing here touches a storage engine.
// colCore is the part of a column handle that does not depend on the type parameters. It is
// embedded (not named) so every existing q.info / c.tableInfo reference keeps resolving, while the
// method bodies that touch only these two fields can live here and be compiled exactly once
// instead of once per (table, column-type) pair. See BINARY_SIZE_FINDINGS.md §6.
type colCore struct {
	info      ColumnInfo
	tableInfo *TableInfo
}

// T is the pointer type *<X>Table, never <X>Table. A pointer type argument collapses to a
// single gcshape, so all tables share one stencil per column value type rather than one stencil
// each. See BINARY_SIZE_FINDINGS.md §1.
//
// The constraint is TableHandle, not TableInterface[T]: *XTable cannot satisfy the latter because
// GetTableStruct() T is inherited from TableStruct and returns the table by value. TableHandle
// documents what that costs in checking power.
type Col[T TableHandle, E any] struct {
	colCore
	schemaStruct T
}

// --- non-generic cores. The generic methods below are shims over these. ---
//
// go:noinline is deliberate. These are the shared bodies behind ~2,600 generic method
// instantiations; if the compiler inlines them back into each one, the whole point of having a
// non-generic core is lost (measured: no saving at all). They run once per predicate at
// query-build time, never per row, so an out-of-line call costs nothing that matters.

//go:noinline
func (c *colCore) setDecimalSize(size int8) {
	if size > 15 {
		panic("Decimal size TOO BIG in:" + c.info.Name)
	}
	c.info.DecimalDigits = size
}

//go:noinline
func (c *colCore) setAutoincrement(randSufixSize int8) {
	if randSufixSize > 15 {
		panic("Rand sufix size TOO BIG in:" + c.info.Name)
	}
	if randSufixSize == 0 {
		randSufixSize = -1
	}
	c.info.AutoincrementRandDigits = randSufixSize
}

//go:noinline
func (c *colCore) addStatement(operator string, value any) {
	c.tableInfo.Statements = append(c.tableInfo.Statements,
		ColumnStatement{Col: c.info.Name, Operator: operator, Value: value})
}

//go:noinline
func (c *colCore) addInStatement(values []any) {
	c.tableInfo.Statements = append(c.tableInfo.Statements,
		ColumnStatement{Col: c.info.Name, Operator: "IN", Values: values})
}

//go:noinline
func (c *colCore) addBetweenStatement(v1, v2 any) {
	c.tableInfo.Statements = append(c.tableInfo.Statements, ColumnStatement{
		Col:      c.info.Name,
		Operator: "BETWEEN",
		From:     []ColumnStatement{{Col: c.info.Name, Value: v1}},
		To:       []ColumnStatement{{Col: c.info.Name, Value: v2}},
	})
}

func (c colCore) GetName() string           { return c.info.Name }
func (c *colCore) SetTableInfo(t *TableInfo) { c.tableInfo = t }
//go:noinline
func (c *colCore) setAggregateFn(fn string) { c.info.AggregateFn = fn }
func (c *colCore) setInt32Packing()          { c.info.UseInt32Packing = true }
func (c *colCore) setIsWeek()                { c.info.IsWeek = true }
//go:noinline
func (c *colCore) setCompositeBucketing(sizes []int8) { c.info.CompositeBucketSizes = sizes }

//go:noinline
func (c colCore) resolveInfo(elemTypeName string) ColumnInfo {
	if c.info.Type == 0 {
		c.info.ColType = GetColTypeByName(elemTypeName)
		if c.info.Type == 0 {
			c.info.ColType = GetColTypeByID(TypeBlob)
		}
	}
	return c.info
}

//go:noinline
func (c *colCore) resolveInfoPointer(elemTypeName string) *ColumnInfo {
	if c.info.Type == 0 {
		c.info = c.resolveInfo(elemTypeName)
	}
	return &c.info
}

func (q Col[T, E]) GetInfo() ColumnInfo {
	return q.colCore.resolveInfo(reflect.TypeFor[E]().String())
}

func (q *Col[T, E]) GetInfoPointer() *ColumnInfo {
	return q.colCore.resolveInfoPointer(reflect.TypeFor[E]().String())
}

// colRef carries the result of a declaration-time modifier. The modifiers return this instead of
// Col[T, E] so their bodies stop copying the whole generic handle: that copy is stencilled per
// (table, column type) pair and was 64% of db.Col's code. colRef is one non-generic type, and
// every call site already consumes the result as a Coln — db.Cols(...), FixedValues{Col: ...},
// GroupBy(...). See BINARY_SIZE_FINDINGS.md §6.
type colRef struct{ info ColumnInfo }

func (c colRef) GetInfo() ColumnInfo { return c.info }
func (c colRef) GetName() string     { return c.info.Name }

func (q Col[T, E]) DecimalSize(size int8) Coln {
	q.setDecimalSize(size)
	return colRef{q.GetInfo()}
}

func (q Col[T, E]) Int32() Coln {
	q.setInt32Packing()
	return colRef{q.GetInfo()}
}

func (q Col[T, E]) CompositeBucketing(buketsSize ...int8) Coln {
	q.setCompositeBucketing(buketsSize)
	return colRef{q.GetInfo()}
}

func (q Col[T, E]) IsWeek() Coln {
	q.setIsWeek()
	return colRef{q.GetInfo()}
}

func (q Col[T, E]) Autoincrement(randSufixSize int8) Coln {
	q.setAutoincrement(randSufixSize)
	return colRef{q.GetInfo()}
}

func (c *Col[T, E]) SetSchemaStruct(schemaStruct any) {
	if schema, ok := schemaStruct.(T); ok {
		c.schemaStruct = schema
	} else {
		fmt.Println("no seteado!!")
	}
}

func (e *Col[T, E]) Exclude(v E) T {
	return e.schemaStruct
}

func (e *Col[T, E]) Equals(v E) T {
	return e.addStatementReturningTable("=", any(v))
}

func (e *Col[T, E]) Contains(v any) T {
	return e.addStatementReturningTable("CONTAINS", v)
}

func (e *Col[T, E]) GreaterThan(v E) T {
	return e.addStatementReturningTable(">", any(v))
}

func (e *Col[T, E]) GreaterEqual(v E) T {
	return e.addStatementReturningTable(">=", any(v))
}

func (e *Col[T, E]) LessThan(v E) T {
	return e.addStatementReturningTable("<", any(v))
}

func (e *Col[T, E]) LessEqual(v E) T {
	return e.addStatementReturningTable("<=", any(v))
}

func (e *Col[T, E]) In(values_ ...E) T {
	values := make([]any, 0, len(values_))
	for _, v := range values_ {
		values = append(values, any(v))
	}
	e.addInStatement(values)
	return e.schemaStruct
}

func (e *Col[T, E]) Between(v1 E, v2 E) T {
	e.addBetweenStatement(any(v1), any(v2))
	return e.schemaStruct
}

func (e *Col[T, E]) addStatementReturningTable(operator string, value any) T {
	e.addStatement(operator, value)
	return e.schemaStruct
}

func (e Col[T, E]) Sum() Coln {
	e.setAggregateFn("SUM")
	return colRef{e.GetInfo()}
}

func (e Col[T, E]) Avg() Coln {
	e.setAggregateFn("AVG")
	return colRef{e.GetInfo()}
}

func (e Col[T, E]) Max() Coln {
	e.setAggregateFn("MAX")
	return colRef{e.GetInfo()}
}

// ColSlice is the handle for a column holding a collection of primitives. E is
// the *element* type, so a []int32 column is declared ColSlice[XTable, int32].
// T is the pointer type *<X>Table, unconstrained, for the same reason as Col.
type ColSlice[T TableHandle, E any] struct {
	colCore
	schemaStruct T
}

//go:noinline
func (c colCore) resolveSliceInfo(elemTypeName string) ColumnInfo {
	if c.info.Type == 0 {
		typeOf := elemTypeName
		if typeOf[0] == '*' {
			typeOf = "*[]" + typeOf[1:]
		} else {
			typeOf = "[]" + typeOf
		}
		c.info.ColType = GetColTypeByName(typeOf)
		if c.info.ColType.Type == 0 {
			panic("No se reconoió el slice type:" + typeOf)
		}
	}
	return c.info
}

func (q ColSlice[T, E]) GetInfo() ColumnInfo {
	return q.colCore.resolveSliceInfo(reflect.TypeFor[E]().String())
}

func (q *ColSlice[T, E]) GetInfoPointer() *ColumnInfo {
	if q.info.Type == 0 {
		q.info = q.colCore.resolveSliceInfo(reflect.TypeFor[E]().String())
	}
	return &q.info
}

func (c *ColSlice[T, E]) SetSchemaStruct(schemaStruct any) {
	if schema, ok := schemaStruct.(T); ok {
		c.schemaStruct = schema
	}
}

func (e *ColSlice[T, E]) Contains(v E) T {
	e.addStatement("CONTAINS", any(v))
	return e.schemaStruct
}

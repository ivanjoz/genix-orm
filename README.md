# genix-orm

A statically-typed, reflection-free-at-runtime ORM for Go, built so the database
underneath can be replaced without touching application code.

Tables are declared once as a pair of Go structs. Reads and writes are fully
type-checked at compile time — a predicate cannot take the wrong value type, a
query cannot write into the wrong slice, and a table cannot be handed a driver
built for a different record. Switching databases is a **one-line change** in the
consuming project.

---

## 1. Why it is shaped this way

Two forces pulled in opposite directions.

**The ORM must be portable.** Table declarations, predicates, column metadata and
the record-access machinery have nothing to do with Cassandra or DynamoDB. Keeping
a private copy per driver is how the two ORMs in this repo originally drifted apart
— identical `Col[T,E]`, `Coln`, embedded-marker and accessor implementations,
maintained twice.

**The ORM must stay type-safe.** The obvious way to make a driver swappable is a
runtime registry behind a non-generic interface. That erases the record type at the
driver boundary: the destination slice becomes `any`, `new(E)` becomes
`unsafe.Pointer`, and a whole class of mistake moves from compile time to run time.

The resolution is that **Go permits generic interfaces**. `Executor[ProductTable,
Product]` and `Executor[AlmacenTable, Almacen]` are distinct types, so a driver can
be reached through an interface *without* the record type being erased:

```go
type Executor[TableT any, RecordT any] interface {
    Select(schema *TableT, ti *TableInfo, dst *[]RecordT, scan func(*RecordT) bool) error
    Insert(records *[]RecordT, columnsToExclude ...Coln) error
    // …
}
```

Because it is an interface *value* rather than a type parameter, the driver can
also be chosen at runtime, and two drivers can be live at once for the same record
type. Because each table declares its driver as a *default* type argument, call
sites still infer everything and need no explicit type arguments.

The one thing this cannot be is a constructor function. Go cannot instantiate a
generic type from a runtime value, so the driver has to be named where the table
and record types are statically known. A type alias is the only construct that
does that — which is why driver selection is one `type` line, not one `func` call.

---

## 2. Module layout

Three Go modules, so a consumer only downloads the drivers it uses.

```
genix-orm/
  db/       module github.com/ivanjoz/genix-orm/db      deps: xunsafe
            The driver-agnostic layer. Everything a table declaration, a query and
            a record access need. Knows nothing about any database.

  scylla/   module github.com/ivanjoz/genix-orm         deps: + gocql, colbin
            ScyllaDB / Cassandra driver: CQL generation, materialized views,
            packed keys, index groups, text search, cache versions.

  dynamo/   module github.com/ivanjoz/genix-orm/dynamo  deps: + AWS SDK
            DynamoDB single-table driver.
```

The dependency direction is strictly `scylla → db` and `dynamo → db`. `db` never
imports a driver; drivers register themselves into it at import time. Splitting
`dynamo` into its own module keeps the AWS SDK out of Scylla-only consumers, and
splitting `db` out keeps gocql out of DynamoDB-only consumers.

### Wiring

```go
// go.mod of the consuming project
require (
    github.com/ivanjoz/genix-orm    v0.0.0   // the scylla driver
    github.com/ivanjoz/genix-orm/db v0.0.0
)

replace github.com/ivanjoz/genix-orm    => ./genix-orm
replace github.com/ivanjoz/genix-orm/db => ./genix-orm/db
```

---

## 3. Choosing the driver — the one line

A consuming project declares a small package that is its single ORM entry point.
Only one declaration in it names a database:

```go
// yourproject/db/driver.go
package db

import (
    orm "github.com/ivanjoz/genix-orm/db"
    "github.com/ivanjoz/genix-orm/scylla"
)

// THE DATABASE CHOICE. Point this at another driver's TableStruct and the whole
// project switches — no table declaration and no query changes.
//
//   scylla.TableStruct  ->  ScyllaDB / Cassandra
//   dynamo.TableStruct  ->  DynamoDB
type TableStruct[TableT Schema[TableT], RecordT orm.TableBaseInterface[TableT, RecordT]] = scylla.TableStruct[TableT, RecordT]

type (
    Schema[TableT any]                     = orm.TableSchemaInterface[TableT]
    Record[TableT any, RecordT any, D any] = orm.RecordWithExecutor[TableT, RecordT, D]
)
```

The rest of that package re-exports `genix-orm/db` — type aliases where possible,
thin generic wrappers where not (Go cannot alias a generic function). Nothing in it
names a driver. See `backend/db/` in this repo for a complete example.

Application code then imports only that one package:

```go
import "app/db"
```

---

## 4. Declaring a table

Every table is two structs with identical field names: the **record** (plain data)
and the **table** (typed column handles). Both embed `db.TableStruct`.

```go
type Product struct {
    db.TableStruct[ProductTable, Product]
    EmpresaID int32  `json:",omitempty"`
    ID        int32  `json:",omitempty"`
    Nombre    string `json:",omitempty"`
    Tags      []string
    Status    int8   `json:"ss,omitempty"`
    Updated   int32  `json:"upd,omitempty"`
}

type ProductTable struct {
    db.TableStruct[ProductTable, Product]
    EmpresaID db.Col[ProductTable, int32]
    ID        db.Col[ProductTable, int32]
    Nombre    db.Col[ProductTable, string]
    Tags      db.ColSlice[ProductTable, string]   // element type, not []string
    Status    db.Col[ProductTable, int8]
    Updated   db.Col[ProductTable, int32]
}

func (e ProductTable) GetSchema() db.TableSchema {
    return db.TableSchema{
        Name:      "products",
        Partition: e.EmpresaID,
        Keys:      db.Cols(e.ID.Autoincrement(0)),
        // Declared value ranges let a TypeDelta index size its packed digit slots.
        FixedValues: []db.FixedValues{
            {Col: e.Status, Values: []int64{0, 1}},
        },
        Indexes: []db.Index{
            // TypeDelta appends the managed "updated" column implicitly and serves both halves
            // of a delta-cache sync. Keys[0] is the column query.Delta() filters on.
            {Type: db.TypeDelta, Keys: db.Cols(e.Status)},
            {Type: db.TypeLocalIndex, Keys: db.Cols(e.Nombre)},
        },
    }
}
```

`SelfParse()` on the record, if defined, runs before every insert and update — use
it for derived fields.

For the full schema reference (key patterns, `KeyIntPacking`, `KeyConcatenated`,
index kinds, column modifiers) see the `create-database-tables` skill.

---

## 5. Reading and writing

No explicit type arguments anywhere: the table struct and the driver are both
inferred from the record type.

```go
products := []Product{}
err := db.Query(&products).
    EmpresaID.Equals(companyID).
    Status.Equals(1).
    Limit(100).
    Exec()

// Predicates are typed: passing a string to an int32 column will not compile.
err = db.Query(&products).ID.In(12, 87, 412).Exec()

// Stream instead of collecting, for large results.
err = db.Query(&products).Status.Equals(1).ExecScan(func(p *Product) bool {
    process(p)
    return true // true = do not store
})

// Writes. Column handles for a partial update come from TableOf, which binds a
// table struct with no read destination.
q := db.TableOf[Product]()

err = db.Insert(&products)
err = db.Update(&products, q.Nombre, q.Updated)      // only these columns
err = db.UpdateExclude(&products, q.Tags)            // everything but these
err = db.Merge(&products, db.Cols(q.EmpresaID), onUpdate, onInsert)
```

Available operations: `Query`, `QueryIndexGroup`, `QueryCachedIDs`, `TableOf`,
`Insert`, `InsertOne`, `Update`, `UpdateOne`, `UpdateExclude`, `InsertUpdate`,
`InsertUpdateInclude`, `InsertUpdateExclude`, `Merge`, `SearchText`,
`SearchTextIDs`, `MakeTable`, `MakeSchema`.

---

## 6. Two databases at once

The declared driver is a *default*. `.Via()` overrides it for one query, and the
argument is typed to the table's record — passing another table's driver is a
compile error.

```go
var (
    primary  db.Executor[ProductTable, Product] = scylla.Exec[ProductTable, Product]{}
    fallback db.Executor[ProductTable, Product] = otherdriver.Exec[ProductTable, Product]{}
)

chosen := primary
if config.UseFallback {
    chosen = fallback
}

err := db.Query(&products).Via(chosen).EmpresaID.Equals(companyID).Exec()
```

Two drivers for one record type is only possible because the executor is a value.
Had the driver been a bare type parameter, it would be baked into the record's type
and you would need a duplicate record struct per database.

> **Status:** `scylla` implements `Executor` today. The `dynamo` package predates
> this design and still carries its own `Col`/`Coln`/`Schema`/`Model` and accessor
> engine; it does not implement `Executor` yet, so it cannot yet be the target of
> the driver alias or of `.Via()`. Porting it is the remaining work — the contract
> is 14 methods, most of them thin wrappers over its existing `Repo`.

---

## 7. What is portable, and what is not

The shared layer covers table declaration, predicates, record access and the
managed columns (`Created`, `Updated`, `UpdateCounter`, sequences, cache
versions). Beyond that, drivers differ. The contract is that a driver **fails at
table-compile time with a named error** rather than silently dropping a feature it
cannot express — a table quietly losing an index is worse than a table that refuses
to compile.

| Feature | Portable |
| --- | --- |
| Partition + clustering key reads, equality and range | yes |
| Autoincrement IDs, `Status`/`Updated` delta-cache views | yes |
| Whole-record insert / update / merge | yes |
| Column projection, `Limit`, `OrderDesc` | yes |
| `KeyIntPacking`, `KeyConcatenated` | Scylla-specific |
| `TypeInheritFromKey`, `UseIndexGroup`, `CompositeBucketing` | Scylla-specific |
| Text search, `GenericRecord` by-IDs reads | Scylla-specific |
| Materialized views with arbitrary key reordering | Scylla-specific |

"Portable" here means the shared layer expresses it and any conforming driver can
implement it — not that a second driver has already done so (see the status note in
§6).

Roughly 60% of the `scylla` package is CQL generation, view maintenance and query
planning that has no counterpart elsewhere. That is the intended outcome: the
abstraction sits at the *schema and record* layer, not the storage layer.

Startup, deployment and backup are deliberately **not** behind the abstraction.
Connecting, creating a keyspace, emitting DDL and dropping views are
database-specific by nature, so those call the driver directly.

---

## 8. Documentation

- **`INTERNALS.md`** — architecture, the driver contract, and how to add a driver.
- **`scylla/ORM_INTERNALS.md`** — deep dive into the ScyllaDB driver: memory model,
  query routing, view compilation, write pipeline.
- **`dynamo/README.md`** — the DynamoDB single-table design.

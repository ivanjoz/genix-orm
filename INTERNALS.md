# genix-orm Internals

How the shared layer works, what a driver must supply, and where the type system
stops guarding you. For the ScyllaDB driver's own internals — query routing, view
compilation, the write pipeline — see `scylla/ORM_INTERNALS.md`.

---

## 1. Layers and the dependency rule

```
                 application code
                        │  imports one project-local package
                        ▼
              yourproject/db  (facade: aliases + generic wrappers)
                        │
                        ▼
   ┌────────────────  genix-orm/db  ────────────────┐
   │  schema declaration   TableSchema, Index, Coln │
   │  column handles       Col[T,E], ColSlice[T,E]  │
   │  query state          TableInfo, ColumnStatement│
   │  record access        ColumnInfo, accessors     │
   │  compiled table       Table, TableCore          │
   │  driver contract      Executor, Codec           │
   │  registries           name registry, metadata   │
   └────────▲───────────────────────────▲────────────┘
            │ implements                │ implements
        scylla/                     dynamo/
```

**The rule: `db` never imports a driver.** Drivers depend on `db` and install
themselves into it. Every extension point is therefore an interface `db` declares
and a driver satisfies, or a function variable `db` declares and a driver assigns.

`db`'s only third-party dependency is `xunsafe`. Not gocql, not the AWS SDK, not
even `colbin` — serialization of values with no native storage type is the driver's
job (§4).

---

## 2. The two type parameters, and why order matters

Every table involves two Go types, and the codebase uses these names consistently:

- **`RecordT`** — the plain-data struct that rows decode into (`Product`)
- **`TableT`** — the companion struct of typed column handles (`ProductTable`)

Both embed `TableStruct`, which is what makes each resolvable from the other:

```go
func (e TableStruct[D, T, E]) GetBaseStruct() E  { return *new(E) }   // -> RecordT
func (e TableStruct[D, T, E]) GetTableStruct() T { return *new(T) }   // -> TableT
func (e TableStruct[D, T, E]) GetExecutor() D    { return *new(D) }   // -> driver
```

That triple is what lets `db.Query(&products)` take **no explicit type arguments**.
`RecordT` is inferred from the argument; `TableT` and `D` are then inferred from
`RecordT`'s method set via the `RecordWithExecutor` constraint. Removing
`GetExecutor` would force every one of the ~120 call sites to spell out the driver.

> Historical wart: the older internal generics in `scylla` use `[T, E]` with `T` as
> the *record* and `E` as the *table*, the opposite of `TableStruct[D, T, E]`. When
> reading that package, check which convention a signature is using.

---

## 3. The driver contract: `Executor`

`db.Executor[TableT, RecordT]` is a **generic interface** — the design decision the
whole architecture rests on. `Executor[ProductTable, Product]` is a distinct type
from `Executor[AlmacenTable, Almacen]`, so the record type reaches the driver
untouched and `Select` can take a real `*[]RecordT`.

```go
type Executor[TableT any, RecordT any] interface {
    Name() string

    Select(schema *TableT, ti *TableInfo, dst *[]RecordT, scan func(*RecordT) bool) error
    SelectGrouped(schema *TableT, ti *TableInfo) error

    Insert(records *[]RecordT, columnsToExclude ...Coln) error
    Update(records *[]RecordT, columnsToInclude ...Coln) error
    UpdateExclude(records *[]RecordT, columnsToExclude ...Coln) error
    InsertUpdate(recordsForInsert, recordsForUpdate *[]RecordT, columnsToIncludeUpdate []Coln, columnsToExcludeInsert ...Coln) error
    InsertUpdateInclude(records *[]RecordT, isInsert func(*RecordT) bool, columnsToIncludeUpdate []Coln, columnsToExcludeInsert ...Coln) error
    InsertUpdateExclude(records *[]RecordT, isInsert func(*RecordT) bool, columnsToExcludeUpdate []Coln, columnsToExcludeInsert ...Coln) error
    Merge(records *[]RecordT, columnsToExcludeUpdate []Coln, onUpdate func(previous, current *RecordT) bool, onInsert func(*RecordT)) error

    QueryCachedIDs(refSlice *[]RecordT, cachedIDs []IDUpdatedVersion) error
    SearchTextIDs(partition int32, query string, statusGroup int8, limit int) ([]IDWeight, error)
    SearchText(refSlice *[]RecordT, partition int32, query string, statusGroup int8, limit int) ([]IDWeight, error)

    CompileTable(schema *TableT) Table
}
```

An `Exec` implementation is expected to be **zero-size** (`struct{}`): all per-table
state belongs in the compiled-table cache, so creating and copying one is free.

### How a query reaches it

`TableStruct` holds the executor as a *typed field*, defaulting to the type
parameter `D`:

```go
func (e *TableStruct[D, T, E]) Executor() Executor[T, E] {
    if e.exec == nil {
        var declaredDefault D   // the driver named in the table declaration
        e.exec = declaredDefault
    }
    return e.exec
}

func (e *TableStruct[D, T, E]) Via(exec Executor[T, E]) *T { e.exec = exec; return e.schemaStruct }
```

So the *default* is compile-time (zero cost, statically checked) and the
*override* is runtime (`Via`). One interface dispatch per query, not per row.

### Non-generic entry points

Three operations key off a string rather than table types, so they cannot be
`Executor` methods. `db` declares them as function variables and the driver assigns
them in its `init()`:

```go
var (
    GetAutoincrementID      func(key string, recordsSize int) (int64, error)
    QueryCachedGenericByIDs func(tableName string, cachedIDs []IDUpdatedVersion) ([]GenericRecord, error)
    SetDebugLogging         func(level int)
)
```

`QueryCachedGenericByIDs` is inherently type-erased — it resolves a table by name
through the registry (§7) and returns a flat `GenericRecord`. That is the endpoint's
whole purpose, not a compromise.

---

## 4. The value seam: `Codec`

The accessor engine (§5) derives everything it can from the Go type alone. What it
cannot know is how a value is *stored*. That is `db.Codec`, installed via
`SetCodec`:

```go
type Codec interface {
    // Statement form for a column with no precompiled fast path.
    RenderValue(c *ColumnInfo, ptr unsafe.Pointer) any
    // Encoding for types the engine has no native representation for.
    EncodeStatementValue(c *ColumnInfo, ptr unsafe.Pointer) any
    // Coerce a value coming back from the driver into the record field.
    AssignValue(c *ColumnInfo, ptr unsafe.Pointer, v any)
    // Overlay accessors only the driver can build (collection literals, …).
    CompileDriverAccessors(c *ColumnInfo)
    // Settle a slice column's storage type from an explicit db tag …
    ApplyCollectionOptions(recordTypeName, fieldName string, ct ColType, tag DBTag) ColType
    // … or from schema defaults when no tag pinned it.
    ApplyCollectionDefaults(ct ColType, useListAsDefault, applyFrozenDefault, frozen bool) ColType
}
```

Concretely, in the Scylla driver: `RenderValue` builds a CQL literal;
`EncodeStatementValue` turns unsigned ints and complex Go values into blobs (via
`colbin`); `AssignValue` decodes them back; `CompileDriverAccessors` installs
`GetValueFn` for slice columns because a collection literal needs CQL bracket
syntax; the two `ApplyCollection*` methods decide `set<text>` vs `list<text>` vs
`frozen<…>`.

### Type IDs

`ColType.Type` is a **stable ORM type ID** — the accessor engine switches on it and
it leaks into persisted data (packed keys, blob encodings), so **IDs must never be
renumbered**. `db` owns the ID→Go-type table; the driver names each ID in its own
type system through a resolver:

```go
var DBTypeResolver func(typeID int8) string   // scylla: 2 -> "bigint", 11 -> "set<text>"
```

The resolver is consulted on every resolve rather than baked into the cached table,
so swapping drivers cannot leave stale type names behind. `TypeBlob` (9) is the
catch-all for anything with no native mapping.

---

## 5. Record access: compile once, then no reflection

Reflection appears exactly twice per record *type* — never per row.

`ColumnInfo.CompileFastAccessors()` builds type-specialized closures over an
`xunsafe.Field` (a precomputed struct offset):

| Closure | Purpose |
| --- | --- |
| `GetRawValueFn` | typed read, no interface boxing |
| `GetStatementValueFn` | value as the driver wants it for a statement |
| `GetValueFn` | rendered literal (driver-supplied for collections) |
| `SetValueFn` | typed write from a scanned value |
| `GetValueStringFn` | stable string token without `fmt` |
| `FieldsEqualFn` | compare one column across two records, zero-alloc |

Each closure is exported (`…Fn`) because drivers live in other packages and overlay
their own. A nil closure means "fall back to the method", which routes to the
`Codec`.

Two safety properties worth preserving if you touch this file:

1. **Slice setters are element-type-pinned.** `setSliceExact[T]` accepts only `[]T`
   or `*[]T` for a `[]T` column; anything else goes to the codec fallback. A
   permissive `switch` here would write a `[]int32` into a `[]string` field through
   `SetField` — silent memory corruption, not a type error.
2. **Custom accessors are never overwritten.** `CompileFastAccessors` returns early
   if any closure is already set, so virtual-key and view columns keep their
   business-specific logic.

The rest of the row path is `unsafe.Pointer` throughout: a driver scans into
`xunsafe.AsPointer(record)` and writes fields via `IColInfo.SetValue(ptr, value)`.

---

## 6. Caches

Two, both keyed by type and both immutable once built.

**Record metadata** (`db/metacache.go`) — per record struct: one `ColumnInfo` per
field with its resolved type, `db` tag, offset and accessors, plus the
column metadata. Built once, then copied into per-query column handles so
cache entries are never mutated.

**Compiled table** (`scylla/table_cache.go`) — per table struct, behind a
`sync.Once`, keyed by `PkgPath + "." + Name`. This is where schema validation
panics surface, which is why `MakeTable` is called at startup by generated registry
code: an invalid schema fails on boot, not on the first request.

`InitStructTable` runs **per query** and is the one thing on the hot path that walks
struct fields. It only binds: it copies cached metadata into each `Col` handle,
stamps the resolved column name, and points every handle at a fresh `TableInfo`.

---

## 7. Compiled tables and the name registry

`db.Table` is the interface everything driver-agnostic depends on — controllers,
backup/restore, the registry:

```go
type Table interface {
    GetName() string
    GetFullName() string
    GetColumns() map[string]IColInfo
    GetKeys() []IColInfo
    GetPartKey() IColInfo
    GetPartValue(ptr unsafe.Pointer) int64
    GetKeyValues(ptr unsafe.Pointer) []any
}
```

`db.TableCore` implements it and holds the metadata that means the same thing
everywhere: `Name`, `Namespace`, `Keys`, `PartKey`, `Columns`, `ColumnsMap`,
`ColumnsIdxMap`, `KeysIdx`, the managed columns (`CreatedCol`, `UpdatedCol`,
`UpdatedVersionCol`), sequence/autoincrement metadata, by-IDs slot metadata, and
`MaxColIdx`.

A driver embeds it and adds its own. `scylla.ScyllaTable` adds views, indexes,
packed indexes, key concatenation, composite buckets, index groups, the text-search
index and the select-plan cache — none of which has a counterpart elsewhere.

**Driver-specific code downcasts.** `DeployScylla` and `QueryCachedGenericByIDs`
assert `.(ScyllaTable)` and report clearly when a table was compiled by a different
driver, rather than misbehaving.

The name registry exists because generics cannot resolve a table from a runtime
string. Generated code registers one lazy factory per table:

```go
db.RegisterTableFactory("products", func() db.Table { return db.MakeTable[Product]() })
```

Only closures are built at init, so cold start stays cheap.

---

## 8. Query state

`TableInfo` is the mutable per-query state, allocated fresh by `InitStructTable`
and never shared. It holds `Statements` (`[]ColumnStatement`), include/exclude
column lists, `GroupByColumns`, `OrderBy`, `Limit`, `AllowFilter`,
`CachedIndexGroups`, `UseIndexGroupSelect`, and `RefSlice`.

`ColumnStatement` is a plain description of intent — `{Col, Operator, Value,
Values, From, To}` — that each driver compiles into its own form. The fluent
predicate methods on `Col[T,E]` only append to `Statements`; they touch no storage
concept at all, which is why the whole column DSL lives in `db`.

`RefSlice` is `any`: it is the one place the destination's element type is erased.
It is recovered by assertion in `TableStruct.Exec` and `ExecScan`, where `E` is
statically known, so the driver never performs that assertion.

---

## 9. Where the type system does and does not protect you

**Statically checked:**

- predicate value types (`Col[T,E].Equals(v E)`)
- the record↔table pairing (`TableBaseInterface[T,E]`)
- the destination slice element type
- the driver↔record pairing — an `Executor[Almacen]` cannot reach a `Product` query,
  at declaration or via `.Via()`
- which columns belong to which table (`Query` returns `*TableT`)

**Not checked, by design:**

- **`TableInfo.RefSlice`** — `any`, asserted once per query in generic code.
- **The per-row write path** — `IColInfo.SetValue(ptr unsafe.Pointer, v any)`. Every
  field write during a scan goes through this. Correctness rests on the accessor
  engine's element-type pinning (§5), not on the compiler.
- **`Coln`** — type-erased on purpose, so `TableSchema` and `Index` can hold columns
  of mixed value types.
- **The name registry** — `map[string]func() Table`. An unregistered or
  wrong-driver table is a runtime error.

---

## 10. Adding a driver

1. **New module** if it brings heavy dependencies (`dynamo` does, for the AWS SDK).
   Require `genix-orm/db`.
2. **`Exec[TableT, RecordT] struct{}`** implementing `db.Executor` (§3).
3. **`Codec`** implementation (§4) plus a `DBTypeResolver`, installed from `init()`
   along with the three function-variable hooks.
4. **Compiled table**: `struct { db.TableCore; …driver metadata… }`. `TableCore`
   already satisfies `db.Table`, so no adapter is needed.
5. **`TableStruct` alias** so consumers get a two-parameter declaration:
   ```go
   type TableStruct[TableT …, RecordT …] = db.TableStruct[Exec[TableT, RecordT], TableT, RecordT]
   ```
6. **Reject what you cannot express**, at `CompileTable` time, naming the table and
   the field. A table silently losing an index is the one outcome worse than a table
   that refuses to compile.

A conformance suite that runs one shared table declaration against every driver —
insert, query, update, delete, plus assertions for the features a driver rejects —
is the natural way to keep drivers honest. It does not exist yet.

---

## 11. Tooling that couples to this design

Three tools match tables by package path or type shape, and each has silently
matched *nothing* at least once during refactors:

| Tool | Couples to |
| --- | --- |
| `scripts/validation/check_tables.go` | resolves the embedded `TableStruct` through `go/types`. Must `types.Unalias` first — Go 1.23+ models aliases as `*types.Alias` — and read the table/record from the **last two** type arguments, since the driver alias adds a leading one. |
| `scripts/controllers/controllers_generator.go` | AST-matches `db.TableStruct[XTable, X]`. Its selector is derived from the `ormPackage` constant so the two cannot diverge. |
| `scripts/table/create_edit_table.go` | emits table scaffolding, so it must emit the project facade's import path. |

If you change the facade's import path, the embedded type's shape, or the number of
`TableStruct` type parameters, check all three — and check their **counts**, not
just their exit codes.

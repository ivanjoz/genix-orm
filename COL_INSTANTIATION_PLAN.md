# Plan — Cut `db.Col` Instantiation Cost

Status: **both phases implemented.** Evidence and measurements: `BINARY_SIZE_FINDINGS.md`,
which also supersedes the project's earlier binary-size analysis (findings §7). Design decisions
are recorded in `scylla/RATIONALE.md`.

Re-measured on the tree as shipped (which has advanced since the findings were written, so the
baseline is 45,023,392 rather than 44,957,856). Every delta reproduced exactly:

| Step | Binary | Delta |
| --- | --- | --- |
| Baseline | 45,023,392 | — |
| Phase A — `Col[*T, E]` | 44,630,176 | **−393,216** |
| Phase B — modifiers return `Coln` | 44,236,960 | **−393,216** |
| Combined | | **−786,432 (−1.75%)** |

Scope actually touched by Phase A: **1,014 declaration sites across 58 files** — the plan's 737
counted only `db.Col`/`db.ColSlice` in `backend/`, and A.4 (genix-orm's own tables: `IncrementTable`,
the dynamo schemas, the scylla test schemas) plus the dynamo/scylla test declarations add the rest.
`scripts/table/create_edit_table.go` (A.5) is done.

Verified: `go build ./...` and `go vet ./...` clean on `backend/`, `genix-orm/` and `genix-orm/db/`;
`genix-orm` and `genix-orm/db` module tests pass (`scylla` included — `shared_schema_test.go` and
`group_by_test.go` declare schemas through the changed `.DecimalSize()` / `.Autoincrement()` path);
`app/accounting`, `app/libs`, `app/finance`, `app/exec` tests pass; `check_tables` reports
**"Found 53 table struct pairs"**, identical to before the conversion.

One deviation from B.5's prediction: the modifier group's symbol count rose 4,128 -> 4,552 rather
than staying flat, and `Int32`/`IsWeek` each grew by 8,781 bytes instead of shrinking — their
`colCore` setters are inlinable one-liners, so `colRef{q.GetInfo()}` is a larger body than the
struct copy it replaced. The group still fell 1,506,584 -> 1,073,237 (**−433,347**, against the
predicted −412,848) and the predicate and interface groups moved by exactly 0, so the diagnosis
holds; only the per-method arithmetic differs.

Two phases, independently shippable, independently measurable, in this order. Phase A edits table
declarations and the ORM; Phase B edits two ORM files and nothing else. Neither touches a query
call site, a `GetSchema()` semantic, or a per-row hot path. Both figures below are measured on the
production target, not estimated.

| Phase | Change | Saving | Confidence | Risk |
| --- | --- | --- | --- | --- |
| **A** | `Col[*T, E]` — pointer table type argument | **−393,216 (measured)** | measured end-to-end | low |
| **B** | Declaration-time modifiers return `Coln`, not `Col[T, E]` | **−393,216 (measured)** | measured end-to-end | low |
| — | **TODO**: re-enable dead-method elimination | ~2.4 MB text + up to 1.6 MB names | measured on a 2-table program | not an ORM change |

Combined A+B, measured end to end: **44,957,856 -> 44,171,424, −786,432 (−1.75%)**. Phase A's own
value multiplies by about 4x if the TODO is ever resolved (findings §4.2), but neither phase depends
on it.

**Neither phase changes the query API.** Phase A edits table *declarations* only; Phase B edits two
ORM files and no call site anywhere. An earlier draft of Phase B introduced a public `ColRef` /
`db.Mod()` declaration API — that was rejected and is recorded under Out of scope.

---

## Phase A — `Col[*T, E]`

**Goal.** Collapse gcshape duplication. `ExpenseTable` contains `Col[ExpenseTable, int32]`, so
each table's shape is self-referential and unique by construction. A pointer type argument
unifies to `go.shape.*uint8`, so all tables share one stencil per column value type.

**Why now, given it is only −393 KB.** It is 737 mechanical edits with zero API change, it is
already built and verified, and it is the prerequisite that makes the TODO worth ~54% of what
remains of `db.Col` instead of nothing.

### A.1 `genix-orm/db/column.go`

`T` becomes the pointer-to-table type. Drop the constraint — `*ExpenseTable` cannot satisfy
`TableInterface[*ExpenseTable]` because `GetTableStruct() T` is inherited from `TableStruct` and
returns a value. Nothing inside `Col`/`ColSlice` calls a constraint method.

```go
type Col[T any, E any] struct {   // was: Col[T TableInterface[T], E any]
	colCore
	schemaStruct T                // was: *T
}

func (e *Col[T, E]) Equals(v E) T { return e.addStatementReturningTable("=", any(v)) }  // was: *T

func (c *Col[T, E]) SetSchemaStruct(schemaStruct any) {
	if schema, ok := schemaStruct.(T); ok {   // was: .(*T)
		c.schemaStruct = schema
	}
}
```

Same three edits for `ColSlice`. Every `) *T {` on a `Col`/`ColSlice` method becomes `) T {`;
the `return e.schemaStruct` bodies are unchanged.

Document the intent at the declaration, because the `*` is the entire point and is easy to
"tidy away":

```go
// T is the pointer type *<X>Table, never <X>Table. A pointer type argument collapses to a
// single gcshape, so all tables share one stencil per column value type rather than one each.
// See BINARY_SIZE_FINDINGS.md §1.
```

### A.2 `backend/db/db.go`

Relax the two aliases to match:

```go
Col[TableT any, ValueT any]     = orm.Col[TableT, ValueT]
ColSlice[TableT any, ElemT any] = orm.ColSlice[TableT, ElemT]
```

### A.3 Declaration sweep — 737 sites, 46 files

`db.Col[XTable, V]` -> `db.Col[*XTable, V]`, same for `db.ColSlice`. Mechanical:

```python
re.subn(r'\b(db\.Col(?:Slice)?\[)(?!\*)([A-Za-z_]\w*)(,)', r'\1*\2\3', src)
```

The record struct and `GetSchema()` are untouched. Only the table struct changes:

```go
type ExpenseTable struct {
	db.TableStruct[ExpenseTable, Expense]
	CompanyID          db.Col[*ExpenseTable, int32]
	ID                 db.Col[*ExpenseTable, int32]
	// … remaining 22 columns
}
```

### A.4 genix-orm's own tables

`scylla.IncrementTable` and the other internal tables were **not** in the sweep and still show a
non-pointer struct shape. Convert them the same way, or Phase A is incomplete.

### A.5 `scripts/table/create_edit_table.go` — required, easy to miss

The column generator behind `CREATE_EDIT_TABLE.md` (marked "USE ALWAYS") builds the field type from
a bare identifier, so after Phase A it would silently emit non-pointer declarations for every newly
added column and quietly reintroduce per-table shapes:

```go
// scripts/table/create_edit_table.go — builds db.Col[TableType, FieldType]
Indices: []ast.Expr{
	ast.NewIdent(tableName + "Table"),          // before
	&ast.StarExpr{X: ast.NewIdent(tableName + "Table")},  // after
	ast.NewIdent(elemType),
},
```

`scylla.TableStruct[XTable, X]` emitted at lines 132 and 144 of the same file is **not** affected —
Phase A does not touch `TableStruct`'s type arguments.

### A.6 Verification

```sh
cd backend && go build ./... && go vet ./...
cd scripts && go run . check_tables        # schema binding is reflection-driven; compilation proves little
cd backend && go test ./accounting/... ./libs/... ./finance/... ./exec/
```

`check_tables` is the one that matters — it exercises the runtime path where a wrong pointer type
would surface. Expected binary: **44,564,640** (reproduced twice).

---

## Phase B — declaration-time modifiers return `Coln`

**Goal.** Shrink the *bodies* of the eight declaration-time modifiers. They are 64% of `db.Col`'s
bytes (1,178,032 of 1,841,856) and, at 285 bytes per symbol against the predicates' 175, the
largest methods on `Col` despite being the simplest:

`DecimalSize` `Int32` `IsWeek` `CompositeBucketing` `Autoincrement` `Sum` `Avg` `Max`

The overhead is not the logic — an earlier pass already reduced their bodies to shims over
`colCore` and recovered only −65,536. It is the **return type**: each returns
`Col[T, E]` *by value*, so every one of the 258 instantiations carries a full struct copy, and
`ColumnInfo` is large (six function fields plus an embedded `ColType`).

The methods stay on `Col`, so they are still retained per instantiation while `reflectSeen` is set
(findings §3). Only the copy goes.

### B.1 Why no call site changes

Two properties of the existing code make the return type free to change, both verified across the
repo:

- **No modifier is ever chained** — there is no `.DecimalSize(5).Int32()` anywhere.
- **No modifier result is ever assigned to a variable.** All 54 call sites pass it directly into a
  `Coln` position: `db.Cols(...)`, `FixedValues{Col: ...}`, `GroupBy(...)`.

So returning `Coln` satisfies every consumer. `TableSchema`, `Index` and `FixedValues` keep their
`Coln` fields untouched.

### B.2 `genix-orm/db/column.go`

A non-generic carrier, and the eight methods return it as a `Coln`:

```go
// colRef carries the result of a declaration-time modifier. The modifiers return this instead of
// Col[T, E] so their bodies stop copying the whole generic handle: that copy is stencilled per
// (table, column type) pair and was 64% of db.Col's code. colRef is one non-generic type, and
// every call site already consumes the result as a Coln. See BINARY_SIZE_FINDINGS.md §6.
type colRef struct{ info ColumnInfo }

func (c colRef) GetInfo() ColumnInfo { return c.info }
func (c colRef) GetName() string     { return c.info.Name }
```

```go
// before
func (q Col[T, E]) DecimalSize(size int8) Col[T, E] { q.setDecimalSize(size); return q }

// after — db.Cols(e.Updated.DecimalSize(10)) still compiles unchanged
func (q Col[T, E]) DecimalSize(size int8) Coln { q.setDecimalSize(size); return colRef{q.GetInfo()} }
```

Same shape for `Int32`, `IsWeek`, `CompositeBucketing`, `Autoincrement`, and the three aggregates
`Sum`/`Avg`/`Max`. The `colCore` setter cores are unchanged — they already carry the panics on
oversized widths, and those must keep firing.

`ColSlice` has no modifiers, so it needs nothing.

### B.3 `genix-orm/db/tablestruct.go`

`TableStruct.Autoincrement` synthesises a virtual key column and also returns `Col[T, E]`. It is
used as `e.Autoincrement(n)` at 6 sites (`invoice_summary.go`, `invoice_document.go`,
`credit_history.go`, `cash_banks.go`, `product-stock-movement.go`):

```go
func (e *TableStruct[D, T, E]) Autoincrement(randDecimalSize int8) Coln {
	if randDecimalSize > 8 {
		panic("randDecimalSize TOO BIG.")
	}
	synthetic := Col[T, E]{colCore: colCore{info: ColumnInfo{AutoincrementRandDigits: randDecimalSize}}}
	return colRef{synthetic.GetInfo()}
}
```

Routing through `synthetic.GetInfo()` rather than building a `ColumnInfo` directly is deliberate:
it preserves today's `ColType` resolution exactly, including the fact that `E` here is the *record*
type, not a column value type.

### B.4 Behaviour to watch

`GetInfo()` is now called **eagerly**, at declaration time, where previously the boxed `Col` was
resolved lazily by whoever read the schema. Two consequences:

- `InitStructTable` calls `GetSchema()` once *before* columns are bound, to read
  `UseListAsDefault`. `colRef` values built during that call carry an unresolved, empty-`Name`
  `ColumnInfo`. The result is discarded, and this is already true of the boxed `Col` values today —
  but nothing added to `colRef` construction may panic on an unbound column.
- The schema is otherwise read after binding, so the resolved info is correct.

### B.5 Verification — measured

| | Syms | Bytes |
| --- | --- | --- |
| modifier group before | 4,128 | 1,178,032 |
| modifier group after | 4,128 | **765,184** |
| delta | 0 | **−412,848** |

Symbol count is unchanged by design — the methods still exist, only the copy is gone. Modifiers
drop to 185 bytes/symbol, in line with the predicates' 175, which is the signal that the struct
copy was the whole overhead. Predicate and interface groups moved by exactly 0.

Binary: **44,564,640 -> 44,171,424 (−393,216)**; the finer −412,848 is rounded by 65,536-byte
quantization.

All checks green on the converted tree: `go build ./...`, `go vet ./...`, the `genix-orm` module
tests (`scylla` included — `shared_schema_test.go` and `group_by_test.go` declare schemas through
the changed `.DecimalSize()` / `.Autoincrement()` path), the app tests, and
`check_tables` reporting **"Found 53 table struct pairs"**, identical to the unconverted repo so it
is not passing vacuously.

---

## TODO — re-enable dead-method elimination (~2.4 MB, not an ORM change)

Not planned here. Recorded so the constraint is visible.

`reflect.Value.MethodByName` is reachable, so the linker retains every exported method of every
reachable type program-wide (findings §3). This is what makes Factor 2 dominate, and no ORM-side
change can avoid it. Three independent sources, and the flag is boolean — all three must go or
nothing changes:

1. `aws-lambda-go` -> `net/rpc`. **Free**: `-tags lambda.norpc`, AWS's own flag. Measured −65,536.
2. `gocql/recreate.go` -> `text/template`. **Free of features**: only provides
   `KeyspaceMetadata.ToCQL()`, unreferenced in this repo. Needs a fork of the existing gocql
   replace. Measured −65,536.
3. `qdrant/go-client` -> `grpc` -> `x/net/trace` -> `html/template`. **Not a build flag.** Qdrant
   is a live feature. The question is whether the agent/RAG code ships in the same binary as the
   API; a separate deployable would do it without losing anything. Architecture decision, deferred.

Until (3) is answered, (1) and (2) are worth only their own −131,072 and should not be presented
as progress toward the 2.4 MB.

If it is ever resolved, revisit in this order: the `db.Col` predicates, interface surface and the
now-slimmer modifiers all collapse to shape stencils (Phase A then carries them), then
`db.TableStruct` (315,424),
`scylla.Exec` (178,848) and `scylla.ScyllaController` (87,344). Those three are boxed through
`Executor[T, E]`, which is a deliberate interface — `Via()` depends on it — so they keep concrete
instantiations via `ifaceMethod` even with pruning on, and need the pointer treatment plus a
method-count review rather than descriptor elimination.

---

## Out of scope

- **Per-row hot paths.** Untouched. Anything inside the row encode/decode loop stays exactly as
  it is: that is where direct calls matter, and it is not where the bytes are.
- **`Col[E]` with the table on the method.** Not expressible (findings §2).
- **De-reflecting `InitStructTable`.** Measured at exactly zero and would add `unsafe` pointer
  arithmetic to the schema binder (findings §4, H1). Do not retry.
- **Removing `Coln` boxing from schema declarations.** Measured slightly negative on its own
  (findings §4, H2). What the linker counts is the method set, not the boxing.
- **A public `ColRef` / `db.Mod()` declaration API.** An earlier draft of Phase B moved the eight
  modifiers off `Col` onto an exported non-generic type, which would have removed them from the
  method set entirely and recovered the full 1,178,032 rather than 412,848. Rejected: it rewrites
  54 declaration sites and changes how the ORM is used. The extra ~765 KB is not worth an API
  change, and Phase B as specified gets the majority of it for two files and no call sites.

## `TableHandle` — recover the part of the constraint a pointer argument can satisfy

**Context** — Phase A (below) had to drop `TableInterface[T]` from `Col` and `ColSlice`, which left
the table type argument completely unchecked: `db.Col[*int32, int32]` compiled. The question was
whether any of that checking can come back without giving up the pointer argument, since restoring
`TableInterface[T]` forces `T` back to the value form and costs the whole −393,216.

**Decision** — constrain on a new non-generic `TableHandle interface{ GetSchema() TableSchema }`,
which `*XTable` *does* satisfy (value-receiver methods are in the pointer's method set). Applied to
`Col`, `ColSlice` and the two aliases in `scylla/aliases.go` and `backend/db/db.go`. Verified free:
the production binary is the same 44,236,960 and, holding comments constant so line tables do not
shift, the only bytes that differ are the 77 in `.note.go.buildid`. Nothing inside `Col` calls a
method on `T`, so there is no dictionary and no codegen difference.

**Rationale** — this is a *partial* check and the boundary is what matters:
`db.Col[*int32, int32]` now fails with `*int32 does not satisfy db.TableHandle (missing method
GetSchema)`, but `db.Col[*Expense, int32]` — the record type where the table type belongs, the
mistake actually worth catching — still compiles. `XTable` and its record `X` embed the same
`TableStruct[XTable, X]`, so their method sets are identical (`GetSchema`, `GetTableStruct`,
`GetBaseStruct`); only the self-referential form tells them apart, and that is exactly the form the
pointer rules out. Taken because it is free and strictly better than nothing, not because it closes
the gap. Do not read `TableHandle` as proof that a declaration is well-formed.

## Declaration-time modifiers return `Coln`, not `Col[T, E]`

**Context** — after the pointer type argument below, the eight declaration-time modifiers
(`DecimalSize` `Int32` `IsWeek` `CompositeBucketing` `Autoincrement` `Sum` `Avg` `Max`) were still
the largest methods on `Col` despite having the simplest bodies — 1,506,584 bytes across 4,128
symbols, 364 bytes each against the predicates' 162. An earlier pass had already reduced their
bodies to shims over `colCore` and recovered only 65 KB, so the logic was not the cost. The cost
was the **return type**: each returned `Col[T, E]` *by value*, and `ColumnInfo` is large (six
function fields plus an embedded `ColType`), so every instantiation carried a full struct copy.

**Decision** — introduce a non-generic `colRef struct{ info ColumnInfo }` satisfying `Coln`, and
have the eight modifiers plus `TableStruct.Autoincrement` return `Coln` instead of `Col[T, E]`.
Two ORM files change and **no call site anywhere**: no modifier is ever chained, and all 54 call
sites already pass the result straight into a `Coln` position (`db.Cols(...)`,
`FixedValues{Col: ...}`, `GroupBy(...)`). `TableStruct.Autoincrement` routes through
`synthetic.GetInfo()` rather than building a `ColumnInfo` directly, which preserves today's
`ColType` resolution exactly — including the fact that `E` there is the *record* type, not a column
value type. Measured: modifier group 1,506,584 -> 1,073,237 (**−433,347**), predicate and interface
groups moved by exactly 0, binary 44,630,176 -> 44,236,960 (**−393,216**).

**Rationale** — the methods stay on `Col`, so they are still retained per instantiation while
`reflect.Value.MethodByName` keeps dead-method elimination off; only the copy goes. Moving them off
`Col` onto an exported non-generic type would have removed them from the method set entirely and
recovered the full 1.5 MB, but that rewrites 54 declaration sites and changes how the ORM is used —
rejected. Two side effects to know: `GetInfo()` is now called **eagerly**, at declaration time, so
nothing added to `colRef` construction may panic on an unbound column (`InitStructTable` calls
`GetSchema()` once before columns are bound, and discards the result); and `Int32`/`IsWeek` got
*larger* by 8,781 bytes each, because their `colCore` setters are inlinable one-liners, so the new
`colRef{q.GetInfo()}` body costs more than the struct copy it replaced. The other six more than pay
for it.

## `Col[*T, E]` — the table type argument is a pointer

**Context** — `ExpenseTable` contains `Col[ExpenseTable, int32]`, so each table's gcshape is
self-referential and unique by construction: the compiler emits one stencil per (table, column
value type) pair, ~2,000 concrete instantiations of `db.Col` in the binary. A pointer type argument
unifies to `go.shape.*uint8`, so all tables share one stencil per column value type.

**Decision** — `Col`/`ColSlice` take the pointer-to-table type: `db.Col[*ExpenseTable, int32]`.
1,014 declaration sites across 58 files, `db/column.go`, the two aliases in `scylla/aliases.go` and
`backend/db/db.go`, genix-orm's own internal tables (`scylla.IncrementTable`, the dynamo schemas,
the scylla test schemas), and — easy to miss — the column generator in
`scripts/table/create_edit_table.go`, which builds the field type from a bare identifier and would
otherwise silently reintroduce a per-table stencil on every newly added column. **Zero query call
sites change**: `Equals` returns `T`, and `T` *is* `*ExpenseTable`, so
`eq.CompanyID.Equals(x).ID.Equals(y)` still chains. `GetSchema()` bodies are untouched. Measured:
45,023,392 -> 44,630,176 (**−393,216**).

**Rationale** — the `TableInterface[T]` constraint had to be dropped from `Col` and `ColSlice`:
`*ExpenseTable` cannot satisfy `TableInterface[*ExpenseTable]`, because `GetTableStruct() T` is
inherited from the embedded `TableStruct` and returns the table by value. Nothing inside `Col` or
`ColSlice` calls a constraint method, so this is safe. What it costs, measured against the
compiler rather than assumed:

- **Forgetting the `*` is a hard compile error**, and a better one than the constraint gave:
  `schemaStruct` is now a value field, so `type XTable struct { ID db.Col[XTable, int32] }` is
  `invalid recursive type` — `XTable` would contain itself. The old `schemaStruct *T` broke that
  cycle, so this class of error is *newly* caught, not newly missed.
- **A pointer to a non-table compiles silently.** `db.Col[*Expense, int32]` — the record type
  instead of the table type — is what the constraint used to reject. It typechecks, and the
  predicate chain quietly changes type: `q.CompanyID.Equals(7)` returns `*Expense`, not
  `*ExpenseTable`. At runtime `InitStructTable` prints `no seteado!!` and leaves a nil handle,
  because `SetSchemaStruct`'s `schemaStruct.(T)` assertion fails; the first chained dereference
  then nil-panics. Verified: **`check_tables` does not catch this** — it still reports 53 pairs
  with a deliberately broken declaration in place. The `TableHandle` constraint added afterwards
  (entry above) narrows this to record types specifically; it does not close it.

On its own this phase is worth only −393 KB, because dead-method elimination is globally disabled
(`reflect.Value.MethodByName` is reachable) so the linker retains every concrete descriptor
regardless of shape sharing; its value multiplies by roughly 4x if that is ever resolved.

## A shared non-generic core only helps if the compiler cannot inline it back

**Context** — `db/column.go` was the largest single concentration of generic bloat: 4.33 MB across
6,325 compiled functions, because `Col[T, E]` stencils every method once per (table, column-type)
pair. Only 5 of its 30 methods reference `T` or `E` in their bodies at all. Extracting the other 25
into a non-generic `colCore` — embedded, so every existing `q.info` / `c.tableInfo` reference kept
resolving unchanged — was expected to recover ~2.5 MB. It recovered **65 KB**.

`go build -gcflags=-m` showed why: every core was small enough to inline, so the compiler put it
straight back into each of the ~6,000 instantiations. The shared body was never shared. Measuring
the file afterwards showed the deeper problem — pclntab (2,410,098 bytes) exceeds the actual code
(1,836,768), because each instantiation pays a fixed metadata floor of name, pcsp, pcln and pcdata
tables that shrinking its body cannot touch.

**Decision** — mark the shared statement builders `//go:noinline` (`addStatement`,
`addInStatement`, `addBetweenStatement`, and the schema-time modifier cores), and collapse
`GetInfo`/`GetInfoPointer` so they take the element type *name* rather than the type parameter.
That recovered 590 KB and 131 KB respectively — nearly 12× what the façade alone achieved. The
remaining ~4 MB in this file is the metadata floor of 6,000 distinct functions and is not reachable
without cutting the instantiation count itself.

**Rationale** — `//go:noinline` is normally a smell, and it is load-bearing here, so it carries a
comment saying so. The cost is one out-of-line call per predicate at query-build time and per
column at schema-declaration time; neither is per row. `Col[T, E].GetInfo()` was checked
specifically: the runtime `IColInfo` interface is implemented by `*ColumnInfo`, not by `Col`, and
`Coln.GetInfo` is called only from `makeTable` inside a `sync.Once` — so nothing marked here sits on
a row path. The alternative that *would* reach the remaining 4 MB is dropping `T` from `Col[T, E]`,
still rejected for the reasons in the entry below: it rewrites 480 call sites across 119 files.

## Heavy generic functions get a thin generic façade over a non-generic core

**Context** — genix-orm was 20.5 MB of a 54 MB production binary, the single heaviest thing in it,
more than the whole `app/` tree. Measured with `go-size-analyzer`: 19,225 compiled functions from
19,234 lines of source. 427 distinct source functions had become 14,728 compiled copies — 34×
duplication — because Go stencils one machine-code copy per concrete type pair, not per gcshape.
`Col.Between` existed 258 times, one per (table, column-type) combination. Function *names* alone
cost 2.9 MB in pclntab. The cost scales at roughly 130 KB per table, so it compounds as tables are
added.

The functions were only nominally generic. `makeTable[T]` is 417 lines that call `GetSchema()` and
`reflect.ValueOf` on their first two lines and then work entirely through `reflect` and the `Coln`
interface. `ResetCounter` and `DeleteViewsAndIndexes` reference `T`/`E` **zero** times in their
bodies and touch only `e.Table`. The type parameters were plumbing at the signature that the body
never used.

**Decision** — keep the generic signature so call sites stay type-safe and unchanged; make the body
a shim that type-erases and calls one non-generic implementation. Applied to `makeTable` (now takes
`db.TableSchema` + `reflect.Value`), and to four `ScyllaController` methods, which became shims over
`resetCounterForTable`, `deleteViewsAndIndexesForTable`, `recalcVirtualColumnsForTable` and
`recalcGroupIndexHashesForTable`. The two recalc bodies keep `T` only as a `reflect.Type` used to
allocate one record per scanned row. Toolchain also moved to Go 1.27. Result: 54,263,970 →
50,790,560 bytes (−6.40%), genix-orm 20.5 MB → 17.3 MB. Full analysis in `BINARY_SIZE_FINDINGS.md`.

**Rationale** — the alternative was dropping `T` from `Col[T, E]`, which is where the largest single
concentration of duplication sits (`db/column.go`, 4.33 MB across 6,325 compiled functions). It was
rejected: `db.Query` returns the *schema struct*, and every predicate returning `*T` is what makes
`query.ClientID.Equals(id).DetailProductsIDs.Contains(pid)` resolve — a non-generic handle cannot
carry per-table fields. It would have rewritten 480 call sites across 119 files to buy an estimated
1.4 MB over what the shim approach gets for free. The cost of the façade is one indirect call where
there was a direct one; every function converted runs at init, deploy or per-batch granularity,
never per row, so this is not on any hot path. Generic methods in Go 1.27 do not help — stencil
count is driven by distinct type arguments at instantiation sites, and rebuilding on 1.27 changed
the ORM's function count by 13.

## Named integer types survive the numeric type switches

**Context** — `db.Col[T, E any]` has no constraint on `E`, so a column declared with a named
numeric type (`type CashMovementType int8`) compiles. It then misbehaves at runtime, because a
type switch tests the exact dynamic type and not the kind: `case int8:` never fires for a named
int8. Five paths were affected, and the failures were not uniform — verified by running the new
tests against the unpatched code:

- `db.ToInt64` / `ToFloat64` / `ToFloat32` → returned 0. This is the write path for any column
  whose accessor is built in `db/accessors.go`, and for the partition key in `db/table.go`.
- `scylla.convertToInt64` / `convertToInt32` → printed "Value is not an integer" and returned 0.
- `scylla.valueToCSVBase64` → fell to its `%v` default and emitted the DECIMAL TEXT `"10"` where
  every reader expects the base64 integer encoding `"a"`. A corrupt value, not a missing one.
- `scylla.makeNumericSlice` → its constraint is written with `~` (`~int8`), so `[]CashMovementType`
  satisfies it, but the inner switch matched nothing and appended nothing: a 4-element slice
  encoded to `",,,"`. Every element silently dropped.
- `scylla.HashInt` → hashed the decimal text instead of the binary value, so a named type hashed
  differently from the type it wraps.
- `scylla.isNonPositiveNumericValue` → returned false for a named zero, so a temporary key would
  be mistaken for a real one and change the insert-vs-update decision in `merge.go`.

No named integer type existed anywhere in the consuming project, so none of this had ever been
exercised.

**Decision** — one `db.NormalizeNamedNumeric(value any) (any, bool)` unwraps a named numeric (or a
pointer to one) into its plain underlying Go type. Every affected switch calls it from its
**default/fallback branch only** and re-dispatches on the result, so the exact-type fast paths stay
reflection-free. `HashInt` and `makeNumericSlice` needed their switch bodies extracted into
`appendHashValue` and `appendNumericElement` to make that re-dispatch possible. `ToInt64` also
gained the unsigned exact-type cases it was missing, which the normalizer's `uint*` results need in
order to resolve. `convert_named_test.go` and `named_numeric_test.go` assert that a named type is
byte-for-byte identical to the type it wraps at every one of these sites.

**Rationale** — normalizing to the plain type, rather than widening straight to int64, is what keeps
width-sensitive callers correct: `HashInt` writes 1 byte for an int8 and 8 for an int64, so widening
would have changed the hash of every existing int8 column and silently invalidated persisted hashes.
Termination is guaranteed by the `PkgPath() == ""` guard — a predeclared type is never normalized, so
the recursion runs at most once. Putting the fallback in the default branch means this cannot regress
anything: those branches were only ever reached by values the code already handled wrongly. The cost
is one reflection call per named-type value on the slow path, and a normalizer that must be
remembered whenever a new numeric type switch is added.


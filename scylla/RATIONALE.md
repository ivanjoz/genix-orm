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


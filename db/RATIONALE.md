# RATIONALE — db

## The kind fallback covers every sized scalar except `int`

**Context** — `GetColTypeByGoType` resolves a named scalar by the kind underneath it, so
`type CashMovementType int8` stores as `tinyint` rather than reaching the blob catch-all. `int` is
the one kind with no entry in `goTypes`: the table names `int8`…`int64` and nothing unsized.

**Decision** — `underlyingScalarName` returns "" for `reflect.Int`, so a column declared `int` (or a
named type over it) still falls to the catch-all and is refused by `assertColumnIsEncodable` at
table-compile time.

**Rationale** — mapping it would mean picking a width, and that is the declaration's to state, not
this function's: `int` is 64-bit on every platform this runs on today, but a column's storage type
leaks into persisted data, and silently choosing `bigint` for it is the same class of mistake as the
blob fallback this change removes — a guess that looks right until the data is already written. A
startup panic naming the column costs one line in the struct to fix. The cost is that `int` is the
one scalar kind that cannot be used as a column without being spelled at a width.

## `db.DebugLevel` mirrors the driver's verbosity instead of asking it

**Context** — `Delta()` decides the whole shape of a delta read — which index it routes to, which
filter values fan out, what watermark bound is emitted — and it does all of it in `db`, before any
driver sees a statement. The flag that turns ORM logging on lives in the driver (`scylla.DebugNormal`,
set through `SetDebugLogging`), and `db` cannot import the driver: the dependency runs the other way.

**Decision** — `db.DebugLevel` + `db.ShouldLog()`, written by `scylla.SetDebugLogging` at the same
moment it sets its own flags. `Delta()` logs the watermark and the filter it resolved under it.

**Rationale** — The alternative was a `ShouldLog func() bool` hook in the driver-installed var block,
which is what that block is for. A plain int wins because it is read on a query-building path and a
function-pointer call is not free there, and because a driver that forgets to install it reads 0 —
silent, which is the safe default for a log flag. The cost is that the two flags can drift if a
future driver sets its own and skips this one; `SetDebugLogging` is the single place that can happen.

## Rejected: splitting TableStruct into a shape-collapsed queryBuilder half
**Context** — `db.(*TableStruct)` was 287,601 bytes across 1,166 symbols. A stencil is keyed on the
whole `[D, T, E]` tuple — driver, table, record, all three value types — so `Limit`, `OrderDesc`,
`Select`, `GroupBy`, `Exclude`, `SetWhere`, `Delta` and the rest were compiled once per table even
though they touch only `tableInfo` and `schemaStruct` and never mention the driver or the record.
Moving them onto a `queryBuilder[TP any]` embedded as `queryBuilder[*T]` should have collapsed them
to one gcshape, the same argument that made `Col[*T, E]` work.
**Decision** — Built, measured, **reverted**. Do not retry in this form.
**Rationale** — The shape collapse worked exactly as predicted: **1 shape stencil, 3,242 bytes, for
all 55 tables**. It still lost, because the bodies were never the cost. `db.(*TableStruct)` fell
287,601 → 190,182, but `db.(*queryBuilder)` arrived at 138,134 across **594 concrete symbols** —
11 per table. Promotion through embedding needs a concrete wrapper per instantiation at every call
site, so where there had been one symbol per method per table there were now two: the shared body
plus the forwarding wrapper. At an average body of 246 bytes against a wrapper of roughly 227, the
indirection costs about what it saves. Net **+44,320** of ORM text and **+65,536** on the production
binary.

This is the same lesson as hypothesis H2 in `../BINARY_SIZE_FINDINGS.md`: what the linker counts is
the **method set**, not where the code lives. Shape sharing only pays when the per-instantiation
body is large — which is why the same move was worth 766,236 bytes in `scylla` (bodies of
2,000–4,000 bytes) and negative here (246).

Two notes for anyone revisiting it. The design itself is sound and compiles: `TP` must be
**unconstrained**, because inside the generic `TableStruct` the argument is `*T`, a pointer to a
type parameter, and Go rejects those against any non-empty constraint — *"\*T is pointer to type
parameter, not type parameter"*. `Delta()` therefore has to reach `GetSchema` through a
`TableHandle` assertion rather than a constraint method. And no call site changes: promotion keeps
`t.Limit(10).OrderDesc()` resolving. It is purely the wrapper arithmetic that kills it, so it would
only become worth doing if `TableStruct`'s query methods grew substantially.

# RATIONALE — db

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

# Genix ORM Internals: Current Architecture

This document describes the current internal architecture of the Genix ORM in `backend/db`, focused on performance-critical execution paths for ScyllaDB.

---

## 1. Core Runtime Model

The ORM separates immutable metadata from per-query state.

- **Immutable metadata**: schema-derived table/column/index/view structures, column type mappings, and compiled accessors.
- **Per-query state**: `TableInfo` (`WHERE` statements, selected/excluded columns, order, limit, ref slice).

This separation keeps setup overhead low while preserving the fluent API.

### 1.1 Main Types

- **`ScyllaTable`** (`main.go`): table runtime descriptor (keys, partition, columns, maps, views/indexes, capabilities, by-IDs slot metadata).
- **`columnInfo` / `colInfo`** (`reflect_accessors.go`): column runtime metadata + getter/setter function pointers.
- **`TableInfo`** (`main.go`): mutable query builder state.
- **`ColumnStatement`** (`main.go`): normalized predicate unit used by planner and query execution.

---

## 2. Metadata Caching and Initialization

### 2.1 Struct Field Metadata Cache

`table_cache.go` provides `structFieldMetadataCache` keyed by record `reflect.Type`.

Cached items include:
- field name/index/type
- `xunsafe.Field` pointer for direct memory access
- inferred ORM type (`colType`)

This avoids rebuilding reflection metadata for each call to `initStructTable`.

### 2.2 Table Compilation Cache

`table_cache.go` provides `scyllaTableCache` keyed by schema type (`pkgpath.TypeName`) and guarded by `sync.Once`.

Each cache entry stores one compiled `ScyllaTable`:
- all columns and maps
- generated virtual columns
- configured indexes/views
- computed query capabilities
- precompiled accessors

### 2.3 `initStructTable` Binding Flow

`initStructTable` (`main.go`) now:
1. fetches cached field metadata,
2. binds each fluent column field to schema/table info,
3. copies immutable metadata into per-column runtime state,
4. assigns per-call `TableInfo`.

The function still validates schema struct shape and preserves existing fluent behavior.

---

## 3. Memory Access and Reflection Elimination

### 3.1 `xunsafe` Field Access

Instead of `reflect.Value.Set` / `Interface` in hot loops, the ORM uses `xunsafe.Field` typed methods and pointer arithmetic.

Benefits:
- lower allocation pressure
- reduced interface boxing
- predictable scalar assignment costs

### 3.2 Column Accessor Compilation

`compileFastAccessors` (`reflect_accessors.go`) configures function pointers once per column.

Compiled categories:
- scalar: `string`, `int*`, `float*`, `bool`
- hot slices: `[]string`, `[]int64`, `[]int32`, `[]int16`, `[]int8`
- pointer scalars: `*string`, `*int*`, `*float*`
- pointer slices: `*[]string`, `*[]int64`, `*[]int32`, `*[]int16`, `*[]int8`

### 3.3 Setter Semantics

For hot slice and pointer-slice types, fast setters are exact-type only:
- accepted: `[]T` or `*[]T` for the target type
- fallback: generic converter path for non-exact inputs

This keeps fast paths simple while preserving compatibility for edge inputs.

---

## 4. Type System and Conversion Layer

`converter.go` defines the type matrix used by schema inference and runtime assignment.

### 4.1 Internal Type IDs

Representative mappings:
- `1`: `string` <-> `text`
- `2`: `int64` <-> `bigint`
- `3`: `int32` <-> `int`
- `9`: `[]byte` / complex blob
- `11`: `[]string` <-> `set<text>` (default; can be overridden with db tag flags)
- `22`: `*int64` <-> nullable bigint

### 4.2 Conversion Paths

- **Fast path**: precompiled accessors in `columnInfo`.
- **Fallback path**: `assingValue` for uncommon or mismatched assignments.
- **Statement serialization**: `GetValue` / `GetStatementValue` use either compiled functions or `makeScyllaValue`.

### 4.3 Unsigned Blob Compatibility

Unsigned primitive/slice compatibility is handled via:
- `encodeUnsignedValueToBlob`
- `decodeUnsignedValueFromBlob`

This supports backward-compatible blob encoding for unsupported CQL unsigned native types.

### 4.4 colbin for Complex Types

Complex fields that do not map directly to CQL types are persisted as blob using `app/libs/colbin`.

- write: marshal to bytes
- read: in-place unmarshal into field memory

---

## 5. Fallback Telemetry and Diagnostics

`converter.go` tracks fallback usage by `colType`.

Available APIs:
- `GetAssignFallbackUsageByType()`
- `ResetAssignFallbackUsageByType()`

Behavior:
- every `assingValue` call increments per-type atomic counters
- optional first-hit debug log per type when full logging is enabled

Purpose:
- identify hotspots still missing fast accessors
- guide incremental converter simplification safely

---

## 6. Query Builder State Machine

Fluent query methods append statements to `TableInfo`.

- `Equals(v)` -> `Operator: "="`
- `In(...v)` -> `Operator: "IN"`, `Values` populated
- `Between(v1,v2)` -> `Operator: "BETWEEN"`, `From`/`To` statements

Additional state:
- include/exclude projections
- order and limit
- allow-filter flag

`TableInfo` is ephemeral and never cached globally.

---

## 7. Capability-Based Query Routing

### 7.1 Signature Generation

At table compile time, `ComputeCapabilities()` creates normalized signatures:
- format: `column|operator|column|operator...`

Each signature ties a predicate pattern to a source:
- base table keys
- index
- materialized/virtual view

### 7.2 Matching Strategy

`MatchQueryCapability` selects the best source by:
1. checking required predicates/operators,
2. scoring candidates by specificity,
3. preferring exact/high-selectivity paths.

Equal-priority candidates are broken by first-seen order, so `indexViews` is sorted by name once at
compile time. Both of its sources are maps, and Go randomizes map iteration — without the sort, two
sources advertising the same signature at the same priority would make the chosen plan differ
between processes.

This is the primary mechanism used to avoid accidental `ALLOW FILTERING` queries.

### 7.3 FixedValues Fan-Out

A signature only matches when the query constrains every column it names, so a query that skips a
*leading* key of an index cannot reach it — `company_id = ? AND type = ?` misses a view keyed
`[company_id, status, type]` and falls back to a table scan.

When the skipped column declares `FixedValues`, the gap can be closed by enumeration:
`status IN (0,1) AND type = 1` is logically identical to the original predicate and *is* a valid key
prefix. `buildFixedValueFanoutStatements` synthesizes that `IN` for every unconstrained column whose
declared set is small enough, and matching is retried with it. Each resulting value group becomes one
contiguous key range, and the ranges execute concurrently through the existing bound-statement
fan-out (§13).

Two caps bound the cost: at most 8 values per column, and at most 8 queries across the product of all
enumerated columns. A `Min`/`Max` span wider than 32 is never enumerated at all.

The fan-out is kept only when it lets the source bind **strictly more of the query's own
predicates** — not when it merely reaches a higher-priority capability. Capability priority ranks a
longer packed-key prefix above a shorter one, but both resolve to one contiguous range over the same
packed column, so enumerating a gap to reach the longer prefix would cost several queries and return
the very same rows. The synthesized predicates are stored on the compiled `SelectStatement`; being
schema constants, bind time re-appends them in the same position and every cached statement index
stays valid.

---

## 8. Indexes, Views, and Virtual Columns

### 8.1 Local and Global Secondary Indexes

`TableSchema` supports:
- `Indexes []Index`

Inference rules:
- one key, no explicit type: local secondary index
- two or more numeric keys, no explicit type: packed local index
- `Type: TypeGlobalIndex`: global secondary index
- `Cols != nil`: materialized view payload declaration
- `Type: TypeViewTable`: derived table with write-side maintenance
- `Type: TypeDelta`: a slim materialized view `(ID, updated, version, filter)` that serves `Delta()`
  only (§11); it is not in the query planner, so plain reads on its keys need their own `TypeView`
- `UseIndexGroup: true`: write-maintained hash group metadata

An index group compiles to exactly one virtual hash column per declaration (`zz_ig_` for scalars,
`zz_igs_` for collections). There was once a second, week-coded variant per declaration
(`zz_iwk_` / `zz_iwks_`, opted into with `Col.StoreAsWeek()`), meant to answer wide date ranges with
one hash per week instead of one per day. It was never implemented on the read side: both
`scoreIndexGroupCandidate` and `resolveIndexGroupQueryValues` refused it. `ComputeCapabilities`
missed that exclusion, so it advertised range signatures identical to the raw variant's and the
planner could route to a view that failed at bind time. `StoreAsWeek` and the week variant are gone;
week-coded bucketing is a separate feature that lives in composite bucketing (§8.4, `Col.IsWeek()`).

### 8.2 Packed Indexes

Packed indexes concatenate numeric components into one sortable number, one bit slot per
component, most significant first (`db.PackSlotValues`).

Rules:
- first component width is inferred: it takes the bits the others leave
- trailing components require `.Size(bits)`
- `.Size(n)` is a range cap: values stay below 2^n. A value past its slot panics on write; nothing
  is truncated, because a truncation shift depends on the value's own magnitude and breaks the
  sort order when a value crosses a power of two
- `.Int32()` stores the packed value in an `int` instead of a `bigint`
- virtual packed columns (packed indexes, range and delta views) use all 64 (or 32) bits: the
  unsigned layout is stored with its top bit flipped (`storeVirtualPacked`), which maps unsigned
  order onto the signed order Scylla compares with. Every write and query bound goes through it
- `KeyIntPacking` writes the record's own `ID`, so it keeps a 63-bit budget and stays a positive
  `int64` (`db.KeyIntPackingSlotBits`); `db.DecodePackedKey` splits one back for debugging

### 8.3 Views

- **Hash views**: equality/IN routing via computed hash columns.
- **Range (radix) views**: multi-column range routing via radix-weighted composite values.

A range view's packed key is prefix-searchable, so it serves its full key and every leading subset:
equality/IN on `columns[0..i-1]` plus equality or a range on `columns[i]` is one contiguous span of
the packed column. Absent trailing columns therefore count as range columns, not as an equality on
zero. A filter *behind* an unconstrained column cannot be expressed as one span, so
`getStatementPrepared` returns nil and the planner falls back rather than dropping the predicate.

Capability priorities reflect selectivity, not just prefix length: the full key keeps the view's own
tier, while a partial prefix sits above the bare-partition plan (100) and below a local index
equality (120) — a narrow index beats a broad prefix scan, and picking the view instead would leave
the narrow predicate as an unservable leftover filter.

A `TypeView` with several keys and no `Size()` hint sizes its range layout from
`TableSchema.FixedValues` (`planFixedValueViewSlots`, `fixed_values.go`). Each declared key takes
`bits.Len64(max)` bits; one key without a range takes the remainder in a `bigint`, otherwise the view
picks `int` when the slots fit 32 bits. Two keys without a range compile to a hash view.

### 8.4 Composite Bucketing

For hash indexes with range constraints:
- bucket IDs are generated for one designated numeric column
- tuple hash sets are materialized into virtual set columns
- select planner chooses bucket coverage and emits indexed statements
- post-filter is applied to guarantee exact semantics after controlled overfetch

---

## 9. Primary Key Compression Strategies

### 9.1 KeyConcatenated

Multiple fields are flattened into one string key with `db.MakeKeyConcat`: integers in `utils.EncodeUintBase64`, components joined by `.` (`KeyConcatSeparator`).

### 9.2 KeyIntPacking

Multiple numeric fields are packed into one `int64` key with decimal slot sizing.

Supports autoincrement placeholders as packing components.

---

## 10. Write Pipeline

### 10.1 Insert and Update Execution

- inserts use `gocql.UnloggedBatch`
- updates generate deterministic `SET` and `WHERE` clauses
- struct values are extracted with column accessors

### 10.2 Pre-Insert Autoincrement

`handlePreInsert`:
- groups rows by partition/autoincrement partition
- requests counters in bulk
- fills autoincrement field (optional random suffix)
- applies key packing when configured

### 10.3 Virtual Consistency Checks

Update path enforces dependency integrity:
- if a virtual index/view depends on source columns,
- partial updates that would break derived values panic early.

---

## 11. The cache protocols (`db/cache`)

The protocols, their wire formats and their rules live in `genix-orm/db/cache`; this driver brings
the keys, the record reads and the write hooks. The by-IDs cache keeps its slot files in the ORM's
file store (`db.SetFileStore`), shared with the dynamo driver; the delta sync keeps nothing outside
Scylla. A partition is named
`<keyspace>/<table ID>/<partition>` in every file key.

**By-IDs** (`CacheByIDs`, `cache_by_ids.go`). Records are bucketed into 256 slots by `uint8(ID)`;
each partition has a year-cycle slot file of 256 ticks. After the rows are written, a write raises
its records' slots to the tick of their `updated`. `QueryCachedIDs` reads the slot files, selects
only the IDs whose slot moved (`part = ? AND key IN (…)`, at LocalQuorum), and overwrites each
returned record's `Updated` with the tick the client now holds. The table needs one integer key, an
integer partition, and an `Updated int32` field.

**Delta** (`TypeDelta`, `delta_view.go`): the window protocol of `db/cache/delta_window.go`, no file
store. The last Key is the sync filter, the others are pinned, and the table needs one integer key
and an integer partition. Each index compiles to `<table>__dlt_<keys>_view`:
`PRIMARY KEY ((part), zz_dlt_<pinned>, id)` ordered `DESC`, where the virtual bigint `zz_dlt_…` packs
the pinned values over `updated` (31 bits) — Scylla allows one non-key base column in a view's key,
so pinned and `updated` share it. Without pinned keys the view orders by `updated` itself.

Writes: before the CQL a write reads the stored `version` and pinned values of its keys
(`readStoredWrittenRows`), stamps `version + 1`, and computes each packed value from the final
pinned values — the stored one when a partial update does not write the column, which is why the
packed column is always rewritten. A record whose pinned values changed gets a row in
`<table>__delta_moves` (`(partition, index tag, old prefix)`, never expires), written in the
marker phase after the rows. The delta views stay out of `table.views`, so the generic
"columns updated together" check does not apply to them.

`Delta(cursor, filter…)` first reads the changed rows (`updated ≥ C − 10 s`) and the moves since
`C − 15 min`, each with `LIMIT threshold + 1`: the view lists newest first, so deciding costs at most
that many rows whatever the cursor's age. Not a replace: the changed records are read by ID
(honouring a `Select` projection), the window `[C − 15 min, C − 10 s)` is read and hashed with the
filter values, and moved IDs not back in the prefix are sent as removed. A cursor of 0, or more
changed or moved records than `Index.DeltaReplaceThreshold` (default 100), answers a replace: the whole prefix filtered by the values, capped at `Limit` newest
`updated` first. The result ends with one Metadata record (key = −1). Two concurrent writers of one
record can store the same version: one write is then not re-applied until the record's next change
(the hash compares versions, not content).

The by-IDs raise runs in the marker phase, after the derived data, so a client never holds a marker
newer than the row it read.

---

## 12. Schema Deployment and Homologation

`deploy.go` compares declared schema with live Scylla metadata.

Capabilities:
- add missing table columns
- create/manage indexes and materialized views
- generate required `IS NOT NULL` clauses for view key parts

`DeployScylla` also calls `EnsureInternalTables()` before homologating anything. The ORM's own
tables — `sequences` — are raw CQL, not declared `TableSchema`s, so the
homologation pass cannot discover them: it only inspects the controllers it was handed. Without that
call a standalone deploy would produce a keyspace with every application table and no counters,
and the first write would fail. All the statements are `IF NOT EXISTS`, so it is free to re-run.

Homologation never drops anything. A table that stops being declared — like the old `cache_version`
or `cache_updated_version` — stays in the keyspace until it is dropped by hand.

---

## 13. Parallel Query Execution

When a logical query fans out into multiple statements (e.g. IN-based expansions):
- execution uses `errgroup`
- subqueries run concurrently
- result sets merge into final output
- reconnection logic retries on transient no-host conditions

---

## 14. Current Performance Profile Summary

1. `xunsafe` typed access avoids reflection overhead in hot paths.
2. Struct metadata cache removes repeated reflection setup costs.
3. Table compile cache removes repeated index/view/capability construction.
4. Compiled accessors cover high-frequency scalar/slice/pointer types.
5. Capability matching routes queries to index/view-aware plans.
6. Packed/hash/radix strategies support complex predicate patterns.
7. Post-filtering enforces exactness when optimized plans overfetch.
8. Fallback telemetry provides measurable guidance for next optimizations.

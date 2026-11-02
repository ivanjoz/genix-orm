## MaxColIdx starts past the highest record-field Idx
**Context** — A column's Idx is its record field index + 1, but MaxColIdx started at the table
struct's field count + 1. A record with more fields than its table struct (SaleOrder: 31 vs 28)
handed generated columns (computed, packed, index groups) an Idx a real column already had:
writing `period` looked like writing the computed `detail_product_ids`, and a partial update
panicked on an index group.
**Decision** — `makeTable` raises MaxColIdx past every mapped column's Idx.
**Rationale** — Idx is in-memory only, so the renumbering needs no migration.

## A blob column is decoded without the colbin.IsColbin gate
**Context** — `AssignValue` skipped any blob not starting with a colbin root byte. colbin documents
that a typed narrow message usually has none, and a top-level `[]Struct` column (`SaleOrder.Lines`)
is one: every such column was written and then read back empty, silently.
**Decision** — Every non-empty blob goes to `colbin.Unmarshal`, and a failure is logged with its
first byte.
**Rationale** — A blob that is not colbin still fails, now in the decoder. It costs one noisier log
line: `companies.invoice_series` holds blobs that are not readable colbin, and it now reports "the
message ends inside a field" where it used to report "not a colbin message".

## A computed column is any table field with no record field; the driver validates it
**Context** — `bindStructTable` runs per query on the *unbound* schema, where `Computed[i].Col` has no
name yet, so it cannot tell a computed column from a typo in the table struct.
**Decision** — Binding names any table field with no record field and marks it `IsComputed`. The
driver's `makeTable` panics on one that `TableSchema.Computed` does not declare. The panic that used to
fire at bind time ("No se encontró el field") now fires when the table compiles, which happens at
boot.
**Rationale** — The check stays, one step later, and the schema needs no second declaration (a tag)
to say what `Computed` already says.

## A computed column lives in ColumnsMap but not in Columns, and needs From
**Context** — Index groups and the query planner resolve source columns by name in `ColumnsMap`.
Every write, select, deploy, backup and by-IDs read walks `Columns`. An update only rehashes an
index group when one of its source columns is in the update.
**Decision** — The computed column goes into `ColumnsMap`, with `IsVirtual` set so the view
projections skip it, and never into `Columns`. Deploy now adds missing columns from `Columns`.
`ComputedCol.From` names the stored columns it derives from, and an update that writes one of them
counts the computed column as written (`includeComputedColumns`). A predicate on a computed column
that no index group consumed fails in `Compute` instead of reaching CQL.
**Rationale** — It touches 4 places, not one guard per column walk. The cost is that `From` must be
declared by hand: if it is missing, an update of `Lines` alone would leave the hashes stale, so it is
required at compile time.

## The DataFrame log keeps its (half_hour) partition key now that every read is per partition
**Context** — Compactions and reads are per company now; nothing reads every partition's log at
once. A `((partition_id, half_hour))` key would spread the load better.
**Decision** — The `<table>__dtf_log` schema is unchanged: a read selects `half_hour = ? AND
partition_id = ?`, a clustering-prefix slice of each half hour.
**Rationale** — No table migration. Every company's writes of a half hour still land on one
Scylla partition, which is fine at genix's write rate; revisit if a half hour gets hot.

## DataFrame log rows hold before values only; Histories start from the table
**Context** — The agreed plan logged each write's after values with the same explicit
`USING TIMESTAMP` as its rows, so the log replayed Scylla's last-write-wins exactly. Logging
"only real changes" (the choice made) already gives up exactness under a race, and after values
plus timestamps cost bytes on every row and a timestamp on every write of the table.
**Decision** — A log row is the `cgl` entry: the before values of the changed columns, or an
insert flag. A History is the record's values read from the table, undone by its logged changes.
No explicit write timestamps. The log is read after the values, and again after them for
`PartitionHistoriesWrittenAfter`; a rebuild reads the log after each page of 5000 records. The column is
`partition_id`, not `empresa_id`: the ORM names no tenant.
**Rationale** — A frame always ends on what the table holds, whatever the writers raced, because
every History starts from the stored row. It costs a read of the changed records' values per
run and fresh read (one select per partition) and a second log read.

## The DataFrame log is written before the rows, in its own batch
**Context** — An unlogged batch is not atomic across partitions, and the log's partition (the half
hour) is never the record's. A row change that lands without its log row would be wrong in the
frames until a rebuild.
**Decision** — `writeDataFrameLog` runs before the row batches, as its own unlogged batch at
LocalQuorum (one partition, so it is cheap). A write that logged and then failed leaves a log row
for a change the table doesn't hold.
**Rationale** — That stray row is harmless: the History starts from the stored values, and undoing
a change to the value the row still holds changes nothing. It costs one more round trip of latency
on writes to tables with frames, and none on other tables.

## Frame columns: field position as the id, a name hash in the log
**Context** — `dataframe` names columns by an id 1..255 in shapes; scylla has no `cb` tag. The id
must also decode log rows written up to 48 h earlier.
**Decision** — The frame column id is the field's position (`Idx`); the log names a column by 16
bits of the CRC32 of its name, and a collision among a table's tracked columns panics at compile.
Keys, Rows and Sums take signed integers and floats only.
**Rationale** — Moving a field changes the shapes, which rebuilds the frames: correct and
automatic. The log rows keep their meaning across that deploy. A collision is unlikely and fails
loudly with a "rename" message.

## Type-erased read, merge and controller paths
**Context** — Go compiles a generic function once per record struct, so every body that took `T`
existed 55 times in genix. The write path and the row scan were already erased (`recordSlice`,
`recordSink`); the outer read path, Merge, the grouped read, InsertUpdateInclude and every
`ScyllaController` method were not. The plan assumed they needed `reflect.MakeSlice` for their
intermediate `[]T`.
**Decision** — The exported API stays generic. Each entry point is a shim over a non-generic body:
- Intermediate results are `[]unsafe.Pointer` collected through `recordSink.collectInto`, not new
  `[]T`. Only the caller's destination is typed.
- When a body must build a slice of its own, it uses a reflect-built `[]T` from
  `recordSlice.recordType` (`newRecordList`, `appendCopy`) or `makeReflectRecordSlice` /
  `makeReflectRecordSink`. Those bodies are Merge and InsertUpdate*'s insert/update split, plus the
  controller's JSON, colbin and reload paths.
- `ScyllaController` methods pass `&e.Table` and `reflect.TypeFor[T]()` to a free function, and
  build reads as a plain `TableInfo`. The predicate setters moved onto `db.TableInfo`, and
  `db.ApplyFilters` takes one.
- `getOrCompileScyllaTable` and `db.InitStructTable`'s body take the table struct as `any`.
**Rationale** — The production binary went from 33,357,984 to 28,901,536 bytes (−4.46 MB, −13.4%).
Reflection costs only on paths that run per call: admin, backup, JSON, Merge and InsertUpdate*.
Those paths already pay a round-trip to Scylla. Hot reads keep the typed `makeRecordSink`. Pointer
lists avoid the copy a `MakeSlice` would add.
`erased_paths_integration_test.go` passes unchanged on the typed code and on the erased code.
Not erased: the small `Col` methods (interface-retained, see BINARY_SIZE_FINDINGS.md) and the
generic shims themselves, which are now under ~800 bytes per table.

## TypeDelta is a slim view, not a change log
**Context** — The delta log in the file store cost an S3 append per write and a per-sync file read,
and still needed a view on the keys for the first sync. Scylla can list what changed by itself.
**Decision** — A TypeDelta index is a materialized view of `(ID, updated, version, filter)` ordered
by updated, and a sync reads it past the client's cursor, then the changed records by ID. The client
checks the 15 minutes behind its cursor with a hash of `(ID, version)` and asks for a replace on a
mismatch. Pinned-value changes go to a small moves table. The view stays out of the planner;
tables that also filter on its keys declare a `TypeView`.
**Rationale** — No S3 on the write path, and missed writes (late landings, view lag) heal on the next
sync instead of never. Cost: a second view per listing table, and a replace when more than the
threshold changed.

## The view's key packs the pinned values with updated
**Context** — Scylla refuses more than one non-primary-key base column in a view's primary key
("Cannot include more than one non-primary key column 'updated'"), and a sync must range over
`updated` inside one pinned prefix.
**Decision** — One virtual bigint packs the pinned values (their `Size`, else `FixedValues`, else the
remaining bits) over 31 bits of `updated`. Every write computes it, partial updates included, from
the stored pinned values it does not write.
**Rationale** — One contiguous `DESC` range per prefix, read by one query.

## TypeDelta needs one integer key and an integer partition
**Context** — The window hash and the moves table name a record by one integer, and the view's
partition is the table's.
**Decision** — `compileSchemaDeltaView` panics unless the table has exactly one integer key and an
integer partition. A `Partition` override on a TypeDelta is refused. A table keyed otherwise answers
its route with `db.DeltaReplaceMetadata()`.
**Rationale** — The stored-version read is then one `key IN (…)` per partition, and no packing of
string or composite keys is needed. Cost: such tables resend their whole list on every sync.

## A full sync depends on how much changed, never on the cursor's age
**Context** — A first version expired the moves after 7 days and answered a replace to any older
cursor, so a cache 40 days old with 4 changes was resent whole.
**Decision** — The moves table has no TTL. A replace comes only from an empty cursor, or more than
`DeltaReplaceThreshold` changed or moved records; both are read with `LIMIT threshold + 1`.
**Rationale** — The view and the moves never expire, so any cursor can be answered incrementally,
and the limit keeps the decision as cheap for an old cursor as for a recent one. Cost: one moves row
per pinned-value change kept forever; such changes are rare and each row is a few bytes.

## A record leaves the view by its filter value
**Context** — No hard deletes on TypeDelta tables; the delta log turned a status-0 write into a
deleted entry.
**Decision** — Changed records come back in every status. genix-ui removes a returned record whose
sync-filter value is not in the call's values; a replace lists only matching records. Moving out of
a pinned prefix is a moves-table row instead.
**Rationale** — One rule covers soft deletes, annulments and filters on `State`, and the server never
has to know which column means "deleted". Cost: a table filtered on `State` keeps a soft-deleted
(`status` 0) record whose `State` still matches.

## What a partial update writes to the view
**Context** — An Update with an include list writes some columns; the view's packed column needs the
pinned values after the write, and the moves table the prefix the record left.
**Decision** — The pre-write read fetches `version` and every pinned column. A column the write does
not include keeps its stored value; a record that was not stored takes the values on the struct.
**Rationale** — One read serves the version, the packed value and the move. LocalQuorum on it, so a
write right after another sees its version.

## The sync filter applies on every sync
**Context** — The window hash must cover the same rows on both sides.
**Decision** — The server hashes and replaces only rows whose filter value is in `Delta()`'s values,
and sends the field name and the values; the client hashes its held records with the same rule.
**Rationale** — A client that holds only active records would otherwise mismatch on every inactive
row in the window.

## A replace honours Limit
**Context** — Routes such as the sale orders cap their list (`Limit(5000)`).
**Decision** — A replace keeps the newest `updated` first, up to `Limit`.
**Rationale** — The cap keeps its meaning. Cost: more than `Limit` writes in one window would answer
a replace on every sync.

## CacheByIDs keeps created/updated even with DisableDefaultColumns
**Context** — The by-IDs slot raise reads each record's `updated`.
**Decision** — A `CacheByIDs` table always gets the managed `created` and `updated` columns, and must
declare `Updated` in its structs.
**Rationale** — Without `updated` the slot tick has no source.

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

## TypeDelta needs one integer key and an integer partition
**Context** — A log entry names its record by one integer (the log ID), and each partition has its
own log. dynamo packs several key columns into the log ID; scylla tables have clustering keys of any
type.
**Decision** — `compileSchemaDeltaView` panics unless the table has exactly one integer key (the log
ID; int64 logs at the 64-bit scale) and an integer partition. A `Partition` override on a TypeDelta
is refused. A table keyed otherwise answers its route with `db.DeltaReplaceMetadata()`.
**Rationale** — The stored-version read is then one `key IN (…)` per partition, and no packing of
string or composite keys is needed. Cost: such tables resend their whole list on every sync.

## Soft delete reads the column named "status"
**Context** — dynamo logs a deleted entry when Status is 0. Two genix tables filter their delta sync
on `State`, not `Status`.
**Decision** — A written record logs a deleted entry when its `status` column (if the table has one)
is 0, whatever the sync filter column is. A table without `status` never logs a delete.
**Rationale** — The soft delete is the app's one delete; the sync filter only decides what a first
sync lists.

## What a partial update logs
**Context** — An Update with an include list writes some columns; the log needs the record's status
and pinned values after the write, and the folder it left.
**Decision** — The pre-write read fetches `version`, `status` and every pinned column. A column the
write does not include keeps its stored value for the log; a record that was not stored takes the
values on the struct.
**Rationale** — One read serves the version, the soft delete and the folder move. LocalQuorum on it,
so a write right after another sees its version.

## The sync filter applies on a first sync only
**Context** — A replace is a first sync (empty cursor) or a cursor older than the log.
**Decision** — `Delta()`'s filter values are applied only when the cursor is empty; a replace from an
old cursor lists every status.
**Rationale** — Same as dynamo. A replace makes the client drop what the response does not bring, so
listing inactive records too is harmless.

## CacheByIDs keeps created/updated even with DisableDefaultColumns
**Context** — The by-IDs slot raise reads each record's `updated`.
**Decision** — A `CacheByIDs` table always gets the managed `created` and `updated` columns, and must
declare `Updated` in its structs.
**Rationale** — Without `updated` the slot tick has no source.

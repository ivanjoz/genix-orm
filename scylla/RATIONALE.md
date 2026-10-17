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

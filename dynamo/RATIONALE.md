# RATIONALE — dynamo

## Write latency: parallel counters, one-trip reservations, no read for new records
**Context** — A berryapps sale (header + 5 lines, 4–5 GroupBys per table) made ~37 DynamoDB calls in sequence: 6.4 s from a laptop at 173 ms per call, and ~60% of them were counter UpdateItems sent one by one.
**Decision** —
- `applyGroupCounterDeltas` builds every UpdateItem first (reserving delete versions in that sequential loop) and sends them through `runInParallel`, at most `writeParallelism = 10` at a time. `PutManyIfVersion`'s conditional puts share the helper.
- `prepareWrite` runs the autoincrement and the `UpdatedVersion` reservations in parallel, except when the ID is a Partition column (the version is reserved per base pk).
- `PutMany` skips the stored-version read of records whose ID it has just assigned. `InsertMany` skips it for every record, and `AssignIDs` reserves the IDs ahead, so a caller can key child records by them and write parent and children together.
**Rationale** — 10 matches the AWS SDK's 10 idle connections per host, so a parallel step reuses warm connections. The ADDs commute and each touches its own item, and they still run after the base items, so ordering and crash safety are unchanged. Parallelism does not change WCU or the per-item 1,000 WCU/s ceiling of hot counters. Cost: `InsertMany` on a record that was in fact stored leaves its old hidden rows and counts it twice in its groups until `RebuildGroups`. Measured in berryapps: a 5-line sale went from 6.4 s to 1.8 s locally (~37 → ~10 sequential calls).

## GroupBy counters: implementation choices beyond GROUP_BY_PLAN.md
**Context** — The plan (answered Q1–Q9, F1–F5) fixed the storage, the diff rules, best-effort ADDs, never-deleted counters, 1e6 float scaling and the GroupDelta flag. Several details were left to the implementation.
**Decision** —
- `Group.Key` is decoded from a colbin blob `d` stored on each counter (`SET d = if_not_exists(d, :d)`): a record holding only the group Keys, its slice set to the one element on a fan-out GroupBy.
- A `Delete` on a `GroupDelta` table reserves one fresh `UpdatedVersion` per base pk for the counters it decrements, because a delete stamps no version on any record.
- Counter `upd` is the write time taken when the counters are applied, not the record's `Updated` field.
- `QueryGroups` reuses the query planner's sk builder: `resolveKeys` became a plain function, and the GroupBy tag is pinned as a synthetic first sort column. The strict `<` case drops the one sk equal to the BETWEEN's upper bound, instead of decoding the record to filter it.
- A slot-less GroupBy is left out of `Schema()` introspection. It is no access path to records, and the Database viewer would otherwise treat it as a GSI with no name.
- `DeleteRecordsAll` now also deletes counters, which share the bookkeeping pk with the slot-versions item. Only sk `"v"` is kept.
- `DecodeRecords` (fn-db's validation) also builds the group keys and checks the float ranges.
- `fn-db rebuild-groups` requires `"apply": true`, like every fn-db write, and does nothing without it (no dry-run diff).

**Rationale** —
- The key blob avoids decoding sk parts back into typed fields (there is no string setter on the accessors). Its cost is a few bytes per counter.
- Without a fresh version, a delete's decrement would carry version 0 and never reach a `Since(W)` client.
- Sequential updates keep the write path short. The cost is latency for a call touching many groups (fan-out GroupBys), which a bounded parallel loop can fix later.
- Reusing `resolveKeys` gives group reads the exact range semantics records have, with no second implementation.

## Operation log: always-installed middleware, entity from the TableID, approximate item size
**Context** — `LogOperations` was asked for as a flag that prints one line per DynamoDB call with its records, size and RCU/WCU. Where the size comes from, how the line names the table, and when the flag can be set were left open.
**Decision** — `Client()` always installs the `dynamoOperationLog` middleware (`operation_log.go`), and the middleware reads `LogOperations` on every call. When the flag is off it passes the call straight through. When it is on, it sets `ReturnConsumedCapacity: TOTAL` and prints the line with the standard `log` package, failed calls included. The entity comes from the first 8 digits of the call's pk (`:pk` for a Query, `:lo` for a Scan), looked up in `entityByTableID`. Sequence rows log as `sequence:<entity>`. Size is the approximate DynamoDB item size (attribute names + values, numbers ≈ digits/2 + 1) of the items written, or of the items returned on reads. UpdateItem and DeleteItem only carry a key, so for them it is the request's key + values.
**Rationale** — If the flag only mattered when the client is built, it would silently do nothing whenever something builds the client first. Billing size is the number capacity units are charged on, so it lines up with the RCU/WCU next to it; HTTP payload bytes would not. Fan-out and delta rows share their entity's TableID, so they log under it with no extra lookup. Costs: one extra map lookup per call while the flag is off, and the ORM writes to `log` instead of the host app's logger.

## Write cache: version-checked stored blobs, so a conditional write diffs hidden rows without a second read
**Context** — `PutManyIfVersion` diffs each record's hidden rows against the stored record, and it read that again with a consistent BatchGetItem right after the caller had read the same records. The cache was asked for as a process-wide, opt-in read method. Its keying, what it stores, its bounds beyond the 15 s TTL, and who may use it were left open.
**Decision** — `write_cache.go` maps pk#sk to `(upv, blob, cachedAt)`, behind a mutex. `GetManyForUpdate` fills it, and so does the GetItem inside `Modify`, but only on tables with hidden rows. `storedVersionsForWrite` decodes an entry only when its `upv` equals the version the write is conditioned on and it is under 15 s old, and it reads only the rest. A record expected new (version 0) reads nothing. A successful write moves an existing entry to its new version, and keys never read for update are not added. Capacity is 10,000 entries: a full cache drops its expired entries, and if none has expired it refuses new keys. Only `PutManyIfVersion` reads it.
**Rationale** — The conditional write proves the entry: it only lands if the stored item is exactly the cached version, and a lost race leaves only extra rows, which reads re-check. So staleness can cost a miss but never a wrong diff, and the TTL only bounds memory. A plain Put has no condition, so it must never diff against a cached copy. The blob is cached rather than the decoded record because callers edit what they read in place, and a shared slice would silently change the "stored" copy. Refusing new keys when full, instead of evicting live ones, keeps the code to one scan, and a refused key only costs the read it would have saved. Costs: up to 10,000 blobs per process, and the hit rate is per Lambda container.

## Optimistic concurrency: PutManyIfVersion and Modify, on the managed UpdatedVersion exposed as `upv`
**Context** — A record is one colbin blob, so DynamoDB can't compare any field of it, and every write replaces the whole record: two read-modify-writes of one record lose one of them. A conditional write needs a top-level attribute that changes on every write. Callers recompute derived records in batches, so a single-record primitive would cost a read and a version reservation per record.
**Decision** — Versioned tables (`VersionedWrites`, or the existing `SaveUpdatedVersion` / `TypeDelta`, which already stamp it) copy the managed `UpdatedVersion` into the item attribute `upv` on every write. `PutManyIfVersion(records)` is PutMany with each base item as a PutItem conditioned on `upv = <the record's UpdatedVersion>` (`attribute_not_exists(pk)` for 0). It runs 10 at a time and returns the records that lost, unwritten; the hidden rows, the version reservation and the slot bump stay batched per call. `GetMany(keys)` is its read: a consistent BatchGetItem. `Modify(key, change)` is the one-record loop on top: a consistent GetItem, the change, then `PutManyIfVersion` conditioned on the version read. It runs up to 8 attempts, sleeping `ConflictBackoff(attempt)` = `attempt² × 10ms`, then returns `ErrWriteConflict`. A byte-identical result writes nothing. A lost condition returns the old item (`ALL_OLD`). If that item has no `upv`, the call fails with an error naming it ("Put it once") instead of retrying forever. Hidden rows sync as in PutMany: new rows go before the conditional puts, and stale rows are deleted only for the winners. A loser deletes the delta rows it put, because their sk holds the version only its call reserved. It leaves its fan-out rows, which another writer may share, as extra rows that reads re-check. The live check found the delta-row leftovers: one lost race left 3 rows. Known gap: a loser may already have rewritten a FullCopy row with its own never-stored record, and a Contains serves that copy until the next successful write. Modify always ends in one, but a PutManyIfVersion caller that drops a lost record does not. berryapps has no FullCopy index. `QueryBuilder.Consistent()` was added for the input reads, and it fails on a GSI plan.
**Rationale** — Reusing `UpdatedVersion` works without a second managed field. It is reserved once per write call and base pk, shared by every record of a `PutMany`, and that is enough: the condition compares one item with its own earlier value, and the sequence only grows, so every write of a record changes its `upv`. The cost is one sequence ADD per write call on a `VersionedWrites` table. `Modify` writes one record per call, so it pays that per record; a batch pays it once. A per-record counter would need its own field, and the delta tables already pay for the sequence. Batch callers keep their own retry loop, because only they know how to re-derive a record. Costs: the base items can't use BatchWriteItem, since it takes no condition, and a lost race is still billed.

## Delta index: the sync filter column stays out of the row sk
**Context** — genix's packed delta view is `[Keys..., updated_version]`. A later sync reads every declared value of the filter column (`FixedValues`), one range per value. This ORM has no `FixedValues`, and a Query reads one sk range.
**Decision** — In a `TypeDelta` Index, the last Key that is not a ColSlice is the sync filter column, and it is not part of the row sk: `sk = <other Keys>#<UpdatedVersion>#<base sk>`. `Delta(W, values...)` filters it in memory, and only when `W = 0`. Every other Key must be pinned by an Eq or a Contains.
**Rationale** — A later sync is one exact range with no value enumeration, which is the read that repeats. The cost lands on first syncs: they also read soft-deleted records and drop them in memory. A table whose only Key is meant to be pinned (`Keys(WarehouseID)`) can't express that: declare a filter Key after it.

## Delta rows reuse the fan-out hidden rows, named by a cb id
**Context** — Every GSI shares the base sk, so no GSI can range on `UpdatedVersion`. `UpdatedVersion` also changes on every write.
**Decision** — A delta index is an `arrayIndexMeta` with `UpdatedVersion` appended to its keys. Without a ColSlice it writes one row per record. Its pk suffix is the ColSlice's cb id, else its first pinned Key's, else `UpdatedVersion`'s. Two hidden-rows indexes on one suffix panic at compile. Rows are keys-only unless `FullCopy`.
**Rationale** — One sync path (diff, write order, read-side re-check) serves fan-out and delta rows. Costs: every write rewrites all of a record's delta rows (a put and a delete per row), and a keys-only Delta read does a BatchGetItem of the base records. A slice field takes either a fan-out index or a delta index, not both.

## The managed Updated is always overwritten
**Context** — genix's ORM keeps a caller-set `updated > 0` and only fills a zero one. Handlers here post client records, which come back carrying the `upd` the client cached.
**Decision** — Every Put/PutMany/PutIfAbsent sets `Updated` to the current SUnixTime from `dynamo.Now`, whatever the record holds.
**Rationale** — Keeping a posted value would store the client's stale timestamp. Cost: no way to backdate `Updated` through the field. `dynamo.Now` is the only hook.

## By-IDs slot versions: a per-slot write counter, bumped after the write
**Context** — genix-orm/scylla stamps every write with a per-partition sequence value (`upv`) and blind-writes it into the slot row. genix-ui's cache-by-ids only needs a uint16 per slot that changes whenever a record of the slot changes.
**Decision** — The slot version is a counter: each write does an `ADD v<slot> :one` on the partition's slot-versions item. It runs **after** the record write, and `QueryCachedIDs` reads the records with a consistent BatchGetItem, while the slot-versions GetItem is eventually consistent. The record's `UpdatedVersion` is the managed `int32` write sequence (delta.go), overwritten with the slot version on a by-IDs read, as in genix. A request ID outside the key's `Size(bits)` returns an error. Duplicate IDs are read once, since BatchGetItem rejects them. Records written before the flag was enabled get no backfill: their slot is "unknown", so they are read on every request until they are written once.
**Rationale** — An atomic ADD needs no read and no sequence reservation. Bumping after the write, with a consistent record read, means a stamped version is never newer than the content it's stamped on. A lagging slot read only makes the client ask again. A client-posted `upv` is never stored, because the write sequence overwrites it. Costs: about 2 WCU extra per write batch per partition (the item grows to ~256 counters), and a consistent read (2× RCU) for the records that did change.

## Type-erased writes: DecodeRecords validates keys, PutRecords takes E values, QueryScanRecords is its own method
**Context** — berryapps' `fn-db` reads and writes any entity through `Controller`, which had only reads. A type-erased caller has no `E` to build records with, and a key value over its `Size(bits)` (or negative) panics on the write path.
**Decision** — `DecodeRecords(json)` unmarshals into `[]E` and builds every key (the item and its fan-out rows), turning those panics into an error, so a payload can be validated without writing. `PutRecords([]any)` asserts each value is an `E`, runs `PutMany` and returns the records as written (autoincrement IDs assigned). The in-memory filter is a separate `QueryScanRecords` with `QueryRecords`' signature, not a bool on `QueryRecords`.
**Rationale** — Returning `E` values lets the caller inspect and merge records by reflection before writing, which is how `fn-db` keeps `json:"-"` fields on an update. A separate method keeps the Database viewer's strict call unchanged and makes the scan greppable. Cost: three more methods every `Controller` implementation must carry.

## Fan-out rows are validated against the full row sk; internals keep the "array index" name
**Context** — `ArrayIndexes` became `Index` entries whose `Keys` hold a `ColSlice` plus scalar columns. Before, Contains dropped a stale row when the record no longer held the element. With scalar columns in the row sk, a row can also go stale when a scalar changes (e.g. `Updated`): the record still holds the element, but the row's range position is wrong. A stale keys-only row can now also point at the same record as a live one, and BatchGetItem rejects duplicate keys.
**Decision** — A returned record must still write the exact row that led to it (`writesArrayRow`: its current row sks contain that row's sk). Keys-only reads fetch each base record once per page. Internally the code keeps `arrayIndexMeta`, `array_index.go` and the introspection kind `"array"`, which the Database viewer reads. Only the public API changed, and the docs call these "fan-out indexes".
**Rationale** — Comparing the full row sk covers both the removed-element case and the changed-scalar case with one check. It costs one pass over the slice per returned record, the same as before. Keeping the internal names avoids churn in the frontend contract. The cost is two names for one concept (the public `Index` holding a `ColSlice`, the internal "array index").

## TableSchema.Fields comes from the table struct; QueryRecords takes a desc flag
**Context** — A table visualizer needs every column before it reads a record, but `TableSchema` only described key columns. It also needs newest-first reads, which `QueryRecords` could not do.
**Decision** — `Fields` lists every `Coln` field of the table struct in declaration order, described like the key columns. `QueryRecords(preds, limit, desc bool)` calls `Desc()` when desc is set, which is a breaking signature change on `Controller`.
**Rationale** — `GetSchema[T]` only knows the table type, and the table struct mirrors the record field by field, so no record type or metaCache is needed. A plain bool is the smallest change; an options struct would only pay off with a second option.

## A no-partition entity routes a GSI equality to the GSI, not to its whole-entity pk
**Context** — An entity without `Partition` columns can always be read from its base pk (the TableID alone), and the planner preferred the base table whenever it was available. So `Eq(Username)` on Users read every user and filtered in memory instead of using gsi-s1. The strict `Query()` exposed it by rejecting those queries, which would have broken login.
**Decision** — The base table wins only when the entity has partition columns and all of them are pinned by `=`. Otherwise the first GSI whose key columns all have `=` is used, and the whole-entity base pk is the last resort.
**Rationale** — A pinned partition is as narrow as a GSI key, so keeping base first there saves the GSI's extra replication lag. A whole-entity pk is never narrower than a GSI key.

## QueryScan's 5 MB cap is counted in read units, checked before each Query page
**Context** — The cap on `QueryScan()` reads was set at 5 MB. A page count doesn't measure it: a keys-only `Contains` also spends BatchGetItem reads outside the Query pages, and pages can be smaller than 1 MB.
**Decision** — `QueryScan()` requests `ConsumedCapacity` and adds up the RCU of Query pages and BatchGets. Before issuing another Query call it fails if the total has passed 640 RCU (5 MB at 0.5 RCU per 4 KB, eventually consistent). The error is `QueryScan read more than 5 MB … narrow its index predicates`. It never returns partial results. `Query()` has no cap, because every row it reads is a match.
**Rationale** — Read units are what DynamoDB bills and they cover both read paths. Checking between calls means one call can overshoot by up to one page (≤1 MB) or one batch. Failing instead of truncating means a handler can't silently serve an incomplete delta sync.

## Query() rejects non-key predicates; strict `<` under a prefix stays allowed
**Context** — Before this change, predicates on fields inside `d` were silently filtered in memory, so any query could cost a whole partition read.
**Decision** — `Query()` fails at plan time if a predicate isn't served by a key. The in-memory check of a strict `<` under an equality prefix (the key condition can't express it exactly) is a separate `keyFilter`, and `Query()` accepts it. Only unindexed fields require `QueryScan()`. `QueryRecords` no longer has its own post-filter check.
**Rationale** — The `keyFilter` only drops rows at the edge of a range the index already bounded, so it doesn't change the read cost. Treating it as a scan would push ordinary range queries onto `QueryScan()` for no reason.

## Partition packing is TableID · 10^w, not TableID · 100
**Context** — The request was `pk = TableID * 100 + partitionID`. A ×100 factor only holds partitions 0..99, and the cron tables partition on `Slot.Size(32)`, which takes up to 10 digits.
**Decision** — Each integer partition column is zero-padded to the decimal width of its `Size(bits)` and appended after the TableID, so `pk = TableID * 10^w + partition`, and several columns chain the same way. It is built as a decimal string, so it isn't limited to int64. DynamoDB's 38-digit limit is checked at compile.
**Rationale** — It is the same formula with the factor taken from the column's declared size, so no partition value can spill into the TableID's digits. Cost: a `Size(64)` partition eats 20 of the 30 digits left after the TableID.

## Array row pk is base pk ‖ cb id, and the cb tag is required
**Context** — Array index rows need their own pk space per field, derived from the base pk ("pk + column"), and the column needs a numeric id.
**Decision** — `rowPK = basePK ‖ cb id` as 3 digits (colbin ids go up to 256). `resolveArrayIndex` panics when the field has no `cb:"N"` tag. Because every row pk is exactly 3 digits longer than a base pk of its table, the two can't collide, and all array rows of a table sit in one contiguous number range, which `DeleteRecordsAll` scans.
**Rationale** — The cb id survives a Go field rename, which the field name or position would not. Cost: array-indexed fields must be tagged even in a record that is otherwise untagged.

## Contains takes many values as one Query per value
**Context** — `Contains` had to accept several values. An array row sk begins with the element, so one Query can only cover one element prefix.
**Decision** — `plans()` builds one plan per value. `Exec` runs them in turn, returns a record matching several values only once (deduplicated by its sk), and stops at `Limit` across all of them. Results come value by value, each in sk order, not merged into one global order. Only one `Contains` is allowed per query, and the dynamic `QueryRecords` doesn't accept it.
**Rationale** — It is the minimum that gives ANY semantics without a merge sort. Cost: N round trips for N values, and no global ordering when N > 1.

## Sort ranges are exact at any position of a composite sort key
**Context** — There were two bugs in `resolveKeys`, both older than this work. First, `Eq(A).Gt(B)` became `sk > "a#b"`, which also matches every row with a larger A; every `Contains(...).Gt(...)` hits this, because the element is always a prefix. Second, a bound on a non-last Keys column compared against a key that continues past it: `Created <= 1000` became `sk <= enc(1000)`, which misses the stored `enc(1000)#<ID>`. The live check (`./deploy.sh 4`) caught the second one. The README called part of this "`>` behaves as `>=`".
**Decision** — The rows whose ranged column equals `v` sort in `[v, v$)` (`$` is the byte after `#`). So `<= v` becomes `< v$`, `> v` becomes `>= v$`, and `BETWEEN a AND b` becomes `BETWEEN a AND b$`. After an equality prefix, a one-sided range becomes a `BETWEEN` bounded by `[prefix#, prefix$]`. Only a strict `<` there stays in the in-memory post-filter, because BETWEEN includes its upper bound.
**Rationale** — The results are what was asked for, with no post-filter except in that one case. Cost: `QueryRecords`, which refuses any post-filter, rejects `Eq(A).Lt(B)` instead of silently returning wrong rows.

## Capacity is measured through a client-options hook, installed by importing ormcheck
**Context** — The live check has to report the capacity of each ORM call, and the ORM builds its DynamoDB client privately.
**Decision** — `dynamo.ClientOptions` (a `[]func(*dynamodb.Options)`) is applied when the shared client is built. `ormcheck`'s `init()` appends a middleware that sets `ReturnConsumedCapacity: TOTAL` on every input and records `ConsumedCapacity` from every output, using reflection over the field names that all DynamoDB operations share.
**Rationale** — The ORM gains one generic hook, and the measurement code stays in the check package. Cost: any process that imports `ormcheck` (today only `scripts/deploy`) measures every ORM call, including the admin seeding in action [1]. The only overhead is a slightly larger response.

## PutIfAbsent writes array rows after the base item; Put is PutMany of one
**Context** — The sync order is new rows → base item → stale rows. For a conditional put, rows written first would index a record whose write then loses the race for its key.
**Decision** — `PutIfAbsent` writes the conditional item first, then all element rows (a new record has no stale rows). `Put` now delegates to `PutMany` (one `BatchWriteItem` instead of `PutItem`), so the sync logic lives in one place.
**Rationale** — A crash between the two steps of `PutIfAbsent` leaves a record missing from its array index until its next write, which is the one exception to "never a missing row". No entity using `PutIfAbsent` today (cron) has array indexes.

## The sync reads with ConsistentRead; Contains reads eventually consistent
**Context** — The diff needs the stored version of the record, and a keys-only Contains reads the base records it points to.
**Decision** — `storedVersions` uses a consistent `BatchGetItem` (twice the read cost). `baseItemsOfArrayRows` uses an eventually consistent one.
**Rationale** — A stale read in the diff would leave rows out of sync, while a stale read on Contains is caught by the element check or fixed on the next read.

## Sequence counters get a per-repo absolute get/set
**Context** — Callers need to move an entity's autoincrement counter, for example to keep a reserved ID out of the sequence or to realign it after data is wiped. The scylla driver has `SetCounterValue(keyspace, name, value)`: an absolute set that returns the previous value.
**Decision** — `Repo.SetAutoincrementValue(value)` uses the same semantics (`SET cv`, `UPDATED_OLD`), and `Repo.GetAutoincrementValue()` does a consistent read of the counter. Both are methods on the repo, keyed by the entity's sequence, and return an error for entities without `UseAutoincrement`.
**Rationale** — Being methods on the repo means callers don't hardcode the `seq#<entity>` naming. The getter exists so a caller can make a conditional move without a DynamoDB condition expression. The set is not guarded against moving below IDs already in use: that check is the caller's, as in scylla.

## Omit-empty colbin encoding is set on import
**Context** — This driver puts the *whole* record in one colbin blob (the `"d"` attribute), not one
column per field, so every field a record leaves untouched is paid for inside that blob. colbin's
omit-empty flag is process-global and encoder-side, and has to be on before the first `Marshal`.
**Decision** — `func init() { colbin.SetOmitEmpty(true) }` in `client.go`, next to the marshaling
section it applies to. The scylla driver does the same in its own `init()`; both are needed because
this is a separate Go module with its own copy of the dependency, and either driver can be the only
one an entry point imports.
**Rationale** — Setting it from a caller means every entry point has to remember, and one that
forgets writes dense blobs with nothing to report it. The flag changes encoding only — both forms
are self-describing per column, so a reader never has to match its writer — and the one semantic
price, a `*T` pointing at `T`'s zero value decoding back as `nil`, is not a shape this project
stores. See `scylla/RATIONALE.md` for the version break the v0.1.0 upgrade carries.

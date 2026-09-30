# RATIONALE — dynamo

## By-IDs slot versions: a per-slot write counter, bumped after the write; UpdatedVersion zeroed on write
**Context** — genix-orm/scylla stamps every write with a per-partition sequence value (`upv`) and blind-writes it into the slot row. This ORM has no per-write sequence, and genix-ui's cache-by-ids only needs a uint16 per slot that changes whenever a record of the slot changes.
**Decision** — The slot version is a counter: each write does an `ADD v<slot> :one` on the partition's slot-versions item. It runs **after** the record write, and `QueryCachedIDs` reads the records with a consistent BatchGetItem, while the slot-versions GetItem is eventually consistent. The record's `UpdatedVersion` must be `uint16`. The ORM zeroes it before every write and only sets it on a by-IDs read. A request ID outside the key's `Size(bits)` returns an error. Duplicate IDs are read once, since BatchGetItem rejects them. Records written before the flag was enabled get no backfill: their slot is "unknown", so they are read on every request until they are written once.
**Rationale** — An atomic ADD needs no read and no sequence reservation. Bumping after the write, with a consistent record read, means a stamped version is never newer than the content it's stamped on. A lagging slot read only makes the client ask again. Zeroing the field keeps a client-posted `upv` from being stored and served as if it were valid. Costs: about 2 WCU extra per write batch per partition (the item grows to ~256 counters), and a consistent read (2× RCU) for the records that did change.

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

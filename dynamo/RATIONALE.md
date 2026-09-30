# RATIONALE — dynamo

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
**Context** — There were two bugs in `resolveSort`, both older than this work. First, `Eq(A).Gt(B)` became `sk > "a#b"`, which also matches every row with a larger A; every `Contains(...).Gt(...)` hits this, because the element is always a prefix. Second, a bound on a non-last sort column compared against a key that continues past it: `Created <= 1000` became `sk <= enc(1000)`, which misses the stored `enc(1000)#<ID>`. The live check (`./deploy.sh 4`) caught the second one. The README called part of this "`>` behaves as `>=`".
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

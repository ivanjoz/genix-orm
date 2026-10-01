# Plan — GroupBy counters on an Index (`dynamo`)

Status: **implemented (2026-10-01), not committed; the live check (`ormcheck`) has not been run
yet.** Where this document and the code disagree, the code wins. The choices made during
implementation are in `RATIONALE.md` ("GroupBy counters: implementation choices").

Decisions taken:
- Q1: no transactions. Counters are updated best-effort after the write; the rebuild (Q7)
  reprocesses them.
- Q2: declared on the `Index`, allowed on an Index with no Slot.
- Q3: counters are per base partition only.
- Q4: a record with `Status = 0` is deleted and counts in no group.
- Q5: the read API is `QueryGroups(...)` → `[]Group[E]`.
- Q6: fan-out GroupBy (a `ColSlice` among the Keys) is in v1.
- Q7: a backfill method that rebuilds the counters, runnable from `fn-db` (F5).
- Q8 → F4: counters are **never deleted**. An emptied group stays with `count = 0`.
- Q9 → F3: floats are summed as `int64(math.Round(value * 1e6))`, 6 decimals, exact.
- F1/G1: two flat fields. `len(GroupBy) > 0` enables the counters (count is always kept next to
  the sums). `GroupDelta: true` also stamps `upv`/`upd` and enables `Since(upv)`. There is no
  count-only GroupBy.

## Goal

An Index can declare a GroupBy over N ≥ 1 numeric columns. For every distinct
value of the index Keys (the group), the ORM keeps one counter item. It holds the **count** of the
records in the group and the **sum** of each declared column. A grouped read is one Query over the
counter items, never a scan of the records.

```go
{Slot: dynamo.S1, Keys: dynamo.Cols(table.Channel, table.Status.Size(8)),
	GroupBy: dynamo.Cols(table.Total), // count + sum(Total) per (Channel, Status)
	GroupDelta: true},                 // also upv/upd on each counter, for Since(upv)
```

## Diff rules

Each record maps to a set of groups:
- **Scalar Keys:** one group.
- **Fan-out Keys:** one group per distinct element. The record's full values count in each group.
  An empty slice means no group.
- **`Status == 0`:** no group at all, when the record has an integer `Status` field.

Per write, the ORM compares the groups of the stored version with those of the version being
written. "Up or down" is one signed `ADD` of `new - old`:

| Case | Old group | New group |
|---|---|---|
| New record (no stored version), or `Status 0 → ≠0` | — | `count +1`, `sum +new` |
| Same group, value changed | `sum += new - old` (same item) | — |
| Same group, value unchanged | nothing written | — |
| Group key moved (or a slice element swapped) | `count -1`, `sum -old` | `count +1`, `sum +new` |
| Delete, or `Status ≠0 → 0` | `count -1`, `sum -old` | — |

This is a pure function, unit-tested without DynamoDB:

```go
// groupCounterDeltas returns, per counter item (GroupBy + group key), the signed count and sum
// deltas that turn the stored record into the written one. storedPtr is nil for a new record,
// writtenPtr nil for a delete. Counters whose deltas are all zero are left out.
func (m *tableMeta) groupCounterDeltas(storedPtr, writtenPtr unsafe.Pointer) map[string]groupCounterDelta
```

The deltas of every record in a call are **merged per counter item**, so 500 records in one group
cost one counter update.

**Float columns** are summed as `int64(math.Round(value * 1e6))`, and the delta is taken in that
integer domain (`scaled(new) - scaled(old)`). So adding and then subtracting the same value always
lands back on the exact previous sum. `math.Round`, not a plain `int64()` cast, because
`0.29 * 1e6` can come out a hair under `290000` and a cast would truncate it. Values outside
±9.2e12 fail the write with an error.

## Storage

Counters live in the entity's bookkeeping pk, which already holds the slot-versions item
(`cache_updated_version.go`). So a GroupBy uses no fan-out cb-id suffix and can't collide with a
fan-out or delta index:

```text
pk   = base pk ‖ 000                                  (the slot-versions item is sk "v" here)
sk   = g<cb ids of the group Keys, dot-joined>#<composite group key>
       e.g. g005.006#web#<Status b64>
c    = record count                                   (native number, ADD)
sNNN = sum of the column with cb id NNN, e.g. s010    (native number, ADD; floats scaled 1e6)
upv, upd = UpdatedVersion / Updated of the last write that touched it (delta GroupBy only)
```

- The cb ids name both the GroupBy and its sums, so renaming a Go field doesn't orphan counters,
  and two GroupBys over the same Keys panic at compile.
- The group key is the sk, so a grouped read pins a Keys prefix and ranges on the next column. It
  reuses the sk-range builder from `query.go`.
- Counters are never deleted. A group whose count reaches 0 stays as an item with `c = 0`, so a
  delta client sees it empty out. The only cost is a few stale items per partition.
- `DeleteRecordsAll` already scans `partitionRange(3)`, so it wipes the counters too.

## Write path (best-effort, no transactions)

The base write stays exactly as it is today (`BatchWriteItem` / conditional `PutItem`). After the
base items land, every merged counter gets one `UpdateItem`:

```text
ADD c :dc, s010 :ds010              (every GroupBy)
SET upv = :upv, upd = :upd          (delta GroupBy only)
```

The new full order of a write: new hidden rows → base items → stale hidden rows → counter updates →
`bumpSlotVersions`. Counters go after the base items, so they never count a record that isn't
stored.

| Writer | Change |
|---|---|
| `PutMany` / `Put` / `Controller.PutRecords` | `storedVersions` also runs when the table has a GroupBy; counters are applied after the base batch. |
| `PutManyIfVersion` / `Modify` | Deltas only for the records that won their conditional write. The condition proves their stored read, so these counters are exact (barring a crash). |
| `PutIfAbsent` | Deltas (no stored version) after a successful write. |
| `Delete` | Negative deltas after the delete. |

`GetManyForUpdate` and `Modify` also feed the write cache when the table has a GroupBy (today they
only do so when it has hidden rows).

**Known drift sources**, all repaired by the rebuild:
1. Two concurrent plain `PutMany` of one record both diff against the same stored version.
2. A crash between the base write and the counter `ADD`s.
3. An SDK retry of an `ADD` that had already landed.

Re-running a whole write is safe: it re-reads the stored version, which already holds the new
values, so the delta is 0.

**Compile rules** (`compile.go`):
- Summed columns are integer or float `Col`s with a `cb` tag.
- Group Keys follow the Index's own rules.
- An Index with a GroupBy may go without a Slot: counters only, no GSI used.
- A GroupBy adds the 3 pk digits to the `pkDigits` check.
- `GroupDelta` without `GroupBy` columns panics.
- A **delta** GroupBy (`GroupDelta: true`) needs a versioned table: it implies `writeVersion`, so
  the record needs the `int32 UpdatedVersion` field, as with `TypeDelta`.

## Read path

```go
groups, err := CheckOrders.QueryGroups(CheckOrders.T.Channel, CheckOrders.T.Status).
	Eq(CheckOrders.T.StoreID, 1).     // every Partition column
	Eq(CheckOrders.T.Channel, "web"). // optional Keys prefix, then one range
	Since(req.GetUpVersion()).        // delta GroupBy only
	Exec()

type Group[E any] struct {
	Key            E     // only the group Keys fields are set
	Count          int64
	Updated        int32 // delta GroupBy only
	UpdatedVersion int32 // delta GroupBy only
	sums           map[string]int64
}
func (g Group[E]) Sum(column Coln) int64        // integer columns
func (g Group[E]) SumFloat(column Coln) float64 // float columns: stored / 1e6
```

- `QueryGroups(keys...)` picks the GroupBy whose Keys are exactly those columns.
- **`Since`:**
  - It filters `upv >= W+1` in memory over the partition's counters of that GroupBy. A GroupBy
    holds few groups, so this costs about the same as the full read.
  - It returns `count = 0` groups, so the client drops them.
  - Without `Since`, `Exec()` skips groups with `count = 0`.
  - Calling `Since` on a GroupBy without the delta flag fails at plan time, like `Delta()` without
    a `TypeDelta` index.
- Sums come back as `int64` rather than written into `E`'s fields, because a sum of `int32` prices
  overflows an `int32` field.
- Calling `Sum` on a float column, or `SumFloat` on an integer one, panics: it's a programming
  error, like the ORM's other misuse panics.

## Backfill (Q7)

```go
func (r *Repo[T, E]) RebuildGroups(partitionValues ...any) error // one base partition
func (r *Repo[T, E]) RebuildGroupsAll() (int, error)             // every partition; returns counters written
```

1. **Read.** `RebuildGroups` reads the whole partition (paginated `Query`).
2. **Compute.** It builds every counter in memory with the same group rules (`Status 0` skipped,
   fan-out per element, floats scaled 1e6).
3. **Compare.** It reads the existing counters (a `begins_with "g"` Query on the bookkeeping pk).
4. **Write.** It `PutItem`s each counter whose values differ, and sets to `c = 0` (sums 0) every
   existing counter it didn't produce. Counters are never deleted.
5. **Stamp (delta GroupBy).** Every counter the rebuild changes gets **one fresh `UpdatedVersion`
   reserved for the partition** (`reserveSequence(basePK#upv)`), so clients past their watermark
   still refetch the corrected groups. Unchanged counters keep theirs.
6. **All partitions.** `RebuildGroupsAll` does a `Scan` of the entity's base pk range, groups the
   records by pk, and also scans the counter range, so partitions with no records left get their
   counters zeroed.
7. **Run it from the CLI.** It is on `Controller`, so berryapps' `fn-db rebuild-groups <entity>`
   runs it for any entity.

This is a maintenance op: writes running during a rebuild can be lost from or doubled in it, and a
second run converges. It is also the fix after adding a GroupBy to a table with data, adding or
removing a summed column, or changing the group Keys.

## Cost

- One `UpdateItem` per merged counter per write call (~1 WCU each).
- A hot group is a hot item: DynamoDB caps one item at ~1000 WCU/s.
- Every write to a GroupBy table pays the consistent read of the stored version, unless the write
  cache hits or the record is new.

## Tests and docs

- `group_by_test.go`: table tests of `groupCounterDeltas` covering every row of the diff table,
  `Status 0` transitions, fan-out element swaps, merging, float scaling round-trips (`0.29`,
  negatives, out of range), and the compile panics (including `Since` on a non-delta GroupBy).
- `ormcheck`: add to `CheckOrder` an `UpdatedVersion`, a float column, the delta GroupBy on S1
  above, and a non-delta GroupBy on a slot-less Index summing the float column. Then insert, change
  `Total`, move `Status`, set `Status = 0`, delete, and assert the counts, sums and `Since` results
  after each step. Run `RebuildGroups` and assert it reproduces the same counters.
- README section "GroupBy counters", skill `SKILL.md`, and a RATIONALE entry.

## Known gap (delta GroupBy, shared with `Delta()`)

`upv` is reserved before the write lands. A call that reserved 9 can land after one that reserved
10, so the counter's `upv` goes back to 9, and a client that synced at watermark 10 misses that
change until the group is next written. Base records have the same window in `Delta()`. v1 matches
it.

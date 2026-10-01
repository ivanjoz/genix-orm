# `db` — a tiny statically-typed DynamoDB ORM

Inspired by the genix ScyllaDB ORM (`genix/backend/db`), trimmed down for the
demo's single-table DynamoDB store. You declare a table as two Go structs and a
`GetSchema()`; the ORM derives the physical keys, runs the queries, and marshals
rows — all with compile-time-checked column references.

Coding agents: `skill/SKILL.md` is the usage guide for an agent (a Claude Code skill). Symlink it
into a project's `.claude/skills/genix-dynamo-orm`.

## Storage model: keys + one binary blob

Every item contains **only**: the key columns (`pk`, `sk`), the index columns
(`n1`..`n5`/`s1`..`s5`), a single binary column **`d`** holding the whole
record serialized with [`colbin`](https://github.com/ivanjoz/colbin) (a columnar binary codec),
and on a versioned table the number **`upv`**, a copy of `UpdatedVersion` that
`Modify`'s conditional write compares. Nothing else is a top-level attribute.

```
{ pk, sk, n1?..n5?, s1?..s5?, d, upv? }
```

This keeps the table schemaless — adding a record field never changes the item
shape or needs a migration — and matches genix persisting complex values as a
colbin blob. The trade-off: DynamoDB can't see inside `d`, so a predicate on a
non-key field is applied **in memory after decode** (see the planner below).

## The physical table it targets

One table, created by the application (the ORM never creates tables): base key
`pk` (**Number**) + `sk` (String) and ten **sparse GSIs** that all share `sk` as
their range key:

| Slot         | Attribute    | Type   |
| ------------ | ------------ | ------ |
| `N1`..`N5`   | `n1`..`n5`   | Number |
| `S1`..`S5`   | `s1`..`s5`   | String |

Sparse means an item only enters a GSI when its schema fills that slot, so
unused slots cost nothing.

## Table IDs: every key starts with 8 digits

Each entity owns an 8-digit **TableID** (`10_000_000..99_999_999`): `Schema.TableID`,
or `HashTableID(Entity)` (FNV-1a 32 folded into that range) when left at 0. It is
the prefix of every key the entity writes:

```
pk = TableID ‖ partition columns, zero-padded decimals     (Number)
nN = TableID ‖ the slot column, zero-padded decimal        (Number)
sN = "<TableID>#" + composite(slot columns)                (String)
```

Each integer column is padded to the decimal width of its `Size(bits)`
(`Size(16)` → 5 digits, `Size(32)` → 10), so `pk = TableID * 10^w + partition`.
The TableID never has a leading zero and every width is fixed per schema, so two
tables can never produce the same key. Partition columns are therefore
**integers only**, and the pk must fit DynamoDB's 38 digits (checked at compile).

Two entities on one TableID — a hash collision, or a copied explicit ID — panic
at boot. Pin an explicit `TableID` to resolve it, or to keep the data in place
when renaming an `Entity`.

## The core idea: order-preserving Base64

A composite key concatenates several columns into one string. Strings go in
verbatim; **numbers are encoded to fixed-width, order-preserving Base64** so that
for equal-length strings, lexicographic comparison equals numeric comparison —
which is what makes `BETWEEN` / `>` / `<` work on a string sort key.

Two things make it work (`encoding.go`):

1. **Fixed width** per column, declared with `.Size(bits)` (1..64, the same unit
   as genix-orm/db's `Size(bits)`). The key stores `ceil(bits/6)` Base64 chars,
   and values `>= 2^bits` are rejected: `Size(32)` is 6 chars that still cap at
   2^32 - 1.
2. An **alphabet in ascending ASCII order** (`-` `0-9` `A-Z` `_` `a-z`), so a
   bigger digit is also a bigger byte.

```
EncodeOrderedUint(1700000000, 8) < EncodeOrderedUint(1800000000, 8)   // as strings
```

Genix packs numbers into a 19-digit `int64`; here we pack into an
arbitrary-length Base64 string that lives inside the DynamoDB string key (no
2^63 ceiling). Key numbers must be `>= 0` (negatives panic, as in genix).

## Declaring a table

The record is a plain struct (colbin identifies fields from the Go type on both
encode and decode, so no tags are needed); only the table struct embeds
`dynamo.Model`.

```go
type Product struct {
    ID         string  `cb:"1"`
    CategoryID int32   `cb:"2"`
    Brand      string  `cb:"3"`
    Price      int64   `cb:"4"`
    Created    int64   `cb:"5"`
    TagIDs     []int32 `cb:"6"`
}

type ProductTable struct {
    dynamo.Model[ProductTable, Product]
    ID         dynamo.Col[ProductTable, string]
    CategoryID dynamo.Col[ProductTable, int32]
    Brand      dynamo.Col[ProductTable, string]
    Price      dynamo.Col[ProductTable, int64]
    Created    dynamo.Col[ProductTable, int64]
    TagIDs     dynamo.ColSlice[ProductTable, int32]  // element type, as in genix-orm/db
}

func (t ProductTable) GetSchema() dynamo.Schema {
    return dynamo.Schema{
        Entity: "prod",                                       // TableID = HashTableID("prod"), pk = TableID
        Keys:   dynamo.Cols(t.ID),                            // -> sk: the record's key
        Indexes: []dynamo.Index{
            {Slot: dynamo.N1, Keys: dynamo.Cols(t.CategoryID.Size(16))}, // numeric GSI
            {Slot: dynamo.S1, Keys: dynamo.Cols(t.Brand)},               // string GSI
            {Keys: dynamo.Cols(t.TagIDs.Size(32))},                      // fan-out: Contains(TagIDs, ...)
        },
    }
}
```

Derived attributes per item, for a TableID of `12345678` (plus `d` = colbin blob
of the whole record):

```
pk = 12345678                                    (number: no Partition, the TableID alone)
sk = ID                                          (Keys, order-preserving composite)
n1 = 12345678 ‖ CategoryID as 5 digits           (number: 1234567800007)
s1 = "12345678#" + Brand
d  = colbin.Marshal(product)                     (binary)
```

### `Keys` is the record's identity, and its only range

`pk + sk` identify a record: `Get`/`Delete` take the `Keys` values, and a `Put`
with different ones writes another item. The `sk` is also the **only** range and
order dimension: GSI keys are equality only, and every GSI shares the base `sk`
as its range key. So a key column is chosen for two reasons at once:

- **Identity** — the usual case is `Keys: dynamo.Cols(t.ID)`.
- **Ranges** — a range on a field (`Created` between two dates) is only possible
  when that field is in `Keys`, before the ID: `Cols(t.Created.Size(32), t.ID)`.
  The cost is identity: a `Get` then needs `Created` as well, and changing
  `Created` means `Delete` + `Put`. Put a field in `Keys` only when it never
  changes and its range query is a real access path.

`Partition` is optional; without it the whole entity is one pk (the TableID).
Declare one for a large entity that is always read per tenant/store/slot, so its
reads and writes spread over DynamoDB partitions. A leading `Keys` column narrows
a read the same way through an sk prefix, so a partition is a throughput choice,
not a query one. `Contains` needs an equality on every Partition column.

```go
// orders: always read per store, listed by date
Partition: dynamo.Cols(t.StoreID.Size(16)),               // pk = TableID ‖ StoreID (5 digits)
Keys:      dynamo.Cols(t.Created.Size(32), t.ID.Size(24)), // sk = enc(Created)#enc(ID)
```

## Fan-out indexes: query a slice by element (`array_index.go`)

DynamoDB can't index inside a list, so an `Index` whose `Keys` hold a
`ColSlice` fans the record out into hidden rows, one per distinct element, which
the ORM keeps in sync. The row sk is the index `Keys` with the slice replaced by
the element, then the base sk:

```
pk = base pk ‖ the slice field's cb id, 3 digits          (number: 12345678006)
sk = composite(index Keys, slice → element) + "#" + base sk
d  = the record blob, only with FullCopy: true
```

```go
// orders of a store holding product 5, updated after 10000: one exact sk range
Indexes: []dynamo.Index{{Keys: dynamo.Cols(t.ProductIDs.Size(32), t.Updated.Size(32))}},
Orders.Query().Eq(Orders.T.StoreID, 7).Contains(Orders.T.ProductIDs, 5).Gt(Orders.T.Updated, 10000).Exec(&out)
Orders.Query().Eq(Orders.T.StoreID, 7).Eq(Orders.T.ProductIDs, 5).Exec(&out) // Eq on a ColSlice = Contains of one value
```

- **Declaration:** an `Index` with no `Slot` and exactly one `ColSlice` among its
  `Keys`, anywhere in the list (`Cols(ProductIDs, Updated)` or
  `Cols(Channel, Tags)`). The handle is a `ColSlice[T, E]` with `E` the
  **element** type (as in genix-orm/db), checked against the record field.
  Elements are integers declaring `.Size(bits)`, or strings, and the field must
  carry a `cb:"N"` tag (1..999): that stable id, not the Go name, names its rows,
  so a slice field takes **one** fan-out index. The other `Keys` are scalar key
  columns, as in a GSI.
- **Contains** (or `Eq` on the `ColSlice`) matches records holding ANY of the
  values: one Query per value, a record matching several comes back once,
  results come value by value. It needs an equality on every Partition column
  (the rows live under the base pk) and on every index `Keys` column before the
  slice. The columns after it take ranges, and once they are all pinned the base
  `Keys` do too (`Contains(...).BeginsWith(ID, "sku")`). One Contains per query;
  the dynamic `QueryRecords` does not accept it.
- **Changing an index's `Keys`** changes the row sk of existing data, and there
  is no rebuild yet. Re-putting the records does not fix it: a write diffs both
  versions with the new shape, so it neither rewrites keys-only rows nor deletes
  the old-shape ones. `Cols(Slice)` alone keeps the pre-`Index` (`ArrayIndexes`)
  row format byte for byte.
- **A scalar `Keys` column that changes** (e.g. `Updated`) moves every element
  row on each write that changes it: a delete and a put per element. Prefer
  columns that never change, like `Created`.
- **Keys only (default)** — a row is `{pk, sk}`. Contains reads the matching
  base records with a second `BatchGetItem`. A write that leaves the slice and
  the scalar index columns alone writes no rows.
- **`FullCopy: true`** — each row also carries `d`, so Contains is one Query,
  at the price of rewriting every element row on every write of the record.
- **Sync on write:** `Put`/`PutMany` read the stored version (consistent
  `BatchGetItem`), diff its rows against the new ones and write new rows →
  base item → stale rows. `Delete` reads the stored version and deletes its rows
  after the item. `PutIfAbsent` writes the rows after the conditional put, since
  written first they could index a record that loses the race.
- **Crash / race safety:** that order leaves extra rows, never a missing one,
  and Contains re-checks every record it returns against the row that led to it
  (the record must still write that exact row: same element, same scalar index
  columns), so an extra keys-only row never becomes a result. A stale FullCopy row
  carries its own stale slice and can't be told apart until the next write of
  that record.

A row pk is 3 digits longer than any base pk of its table, so they never meet,
and rows carry no `nN`/`sN`, so they never show in a GSI query.

## Auto-increment IDs (`sequence.go`)

Set two fields on the schema and the ORM assigns the record's integer `ID` on
write when it is still zero:

```go
type Invoice struct {
    ID      int64      // filled by the ORM
    Number  string
    Created int64
}

func (t InvoiceTable) GetSchema() dynamo.Schema {
    return dynamo.Schema{
        Entity:                     "inv",
        Keys:                       dynamo.Cols(t.ID.Size(48)),
        UseAutoincrement:           true,   // fill ID on Put/PutMany when zero
        AutoincrementRandomPadding: 3,       // low 3 digits are random
    }
}

inv := Invoice{Number: "A-1", Created: now}
Invoices.Put(&inv)   // inv.ID is now e.g. 1_837 (sequence 1, random 837)
```

How it works — modeled on genix's `sequences` table / `GetCounter`, adapted to
DynamoDB:

- Each entity has one counter item in the shared table:
  `pk = 0`, `sk = "<TableID>"`, `cv = <current value>` (native number).
  No entity can produce pk 0 (a TableID has 8 digits), so no `Repo.Scan` ever
  returns them, and renaming an `Entity` with a pinned TableID keeps its counter.
- A write reserves the whole batch in **one** `UpdateItem` with an atomic
  `ADD cv :n` and `RETURN UPDATED_NEW`. The increment and the read of the
  reserved high-water mark are a single round-trip — no read-modify-write window
  for two writers to race. DynamoDB treats a missing attribute as `0`, so a fresh
  sequence starts at `1` with no seeding step.
- The reserved value goes in the high digits, a random value in the low
  `AutoincrementRandomPadding` digits: `id = sequence*10^padding + random`.
  Because each sequence value owns a disjoint `[seq*10^p, seq*10^p + 10^p)`
  interval, IDs are **always unique** — the random padding just makes them
  non-consecutive (harder to enumerate) and adds a collision margin. Padding `0`
  gives plain `1, 2, 3, …`.

`dynamo.ReserveIDs(name, count)` exposes the raw reservation directly (genix's
`GetAutoincrementID`) when you need IDs before building records, or a sequence
not tied to an entity.

`repo.GetAutoincrementValue()` reads an entity's counter (the last value handed
out, `0` if none) and `repo.SetAutoincrementValue(v)` moves it to an absolute
value, returning the previous one (genix's `SetCounterValue`). The next ID uses
`v+1`. Moving it below an ID in use re-issues that ID, and `Put` is an upsert.

Requirements: the record/table must declare an integer field named `ID`; padding
is `0..9`. Both are checked at compile time (`NewRepo` panics otherwise).

## By-IDs cache (`cache_updated_version.go`)

Ported from genix-orm/scylla's slot versions. It answers "give me records
[12, 87, 412]" with only the ones that changed since the version the client
holds, which is what genix-ui's cache-by-ids sends (`ids`, `cc-ids`, `cc-ver`).

```go
type Customer struct {
    ID             int32  `cb:"1"`
    Name           string `cb:"2"`
    UpdatedVersion int32  `json:"upv,omitempty" cb:"3"` // managed by the ORM (delta.go)
}

func (t CustomerTable) GetSchema() dynamo.Schema {
    return dynamo.Schema{
        Entity:             "cust",
        Keys:               dynamo.Cols(t.ID.Size(32)), // exactly one integer column
        SaveUpdatedVersion: true,
    }
}

changed, err := Customers.QueryCachedIDs([]dynamo.IDUpdatedVersion{{ID: 12}, {ID: 87, UpdatedVersion: 4}})
```

- A record belongs to slot `uint8(ID)`. Each base pk has one slot-versions
  item, `pk = base pk ‖ 000`, `sk = "v"`, with one counter per slot
  (`v0`..`v255`). `000` is the fan-out suffix no cb id takes, so no query or
  Scan ever reads it, and `DeleteRecordsAll` keeps it.
- **Write:** `Put`/`PutMany`/`PutIfAbsent`/`Delete` write the records, then
  one UpdateItem per touched pk `ADD`s 1 to each touched slot. The record's
  `UpdatedVersion` is its write sequence (see Delta sync).
- **Read:** one GetItem of the slot versions, then one consistent BatchGetItem
  of the IDs whose client version differs (0 always differs). Returned records
  carry the slot version in `UpdatedVersion` instead of their write sequence,
  truncated to `uint16` (0 is reserved for "unknown"), as in genix.
- Bumping after the write keeps it race-free: a reader that sees the new
  version reads the new record (the read is consistent); one that sees the old
  version only makes the client ask again.
- A write refetches the whole slot (IDs 256 apart). A record not written since
  the flag was enabled has no slot version and is read on every request, until
  it is written once.

## Delta sync (`delta.go`)

The port of genix-orm's managed `updated` / `updated_version` columns, its
`TypeDelta` index and `Delta()`. It answers "the records written since the
watermark I hold", which is what genix-ui's delta cache (`GetHandler`, `?up=<upv>.<upd>`) asks.

```go
type Customer struct {
    ID             int32  `cb:"1"`
    Status         int8   `json:"ss" cb:"2"`
    Updated        int32  `json:"upd" cb:"3"`           // managed: write time, SUnixTime
    UpdatedVersion int32  `json:"upv,omitempty" cb:"4"` // managed: write sequence
}

Indexes: []dynamo.Index{{Type: dynamo.TypeDelta, Keys: dynamo.Cols(t.Status)}},

Customers.Query().Delta(watermark, 1).Exec(&out)  // active on a first sync, every status after
```

- **Managed fields, stamped on every Put/PutMany/PutIfAbsent.** An integer
  field named `Updated` gets the write time as a SUnixTime, `(unix - 1e9) / 2`,
  read from `dynamo.Now`. On a table with a `TypeDelta` index or
  `SaveUpdatedVersion`, the `int32` field `UpdatedVersion` gets the write
  sequence: one value per write call per base pk, from the sequence item
  `pk = 0, sk = "<base pk>#upv"`. It never repeats, so "> watermark" misses
  nothing and resends nothing, which a 2-second timestamp can't promise.
- **A delta index lives in hidden rows**, like a fan-out index: `pk = base pk ‖
  cb id`, `sk = <pinned Keys>#<UpdatedVersion>#<base sk>`. The cb id is the
  ColSlice's, else the first Key's, else `UpdatedVersion`'s.
  - The **last Key, unless it is a ColSlice, is the sync filter column**. It is
    not in the row sk. `Delta(W, values...)` keeps only those values on a first
    sync (`W = 0`), in memory. A later sync returns every value, so soft-deleted
    records reach the clients still caching them.
  - The **other Keys are pinned**: Delta needs an Eq on each, or the Contains
    on its ColSlice, which fans the rows out per element.
    `Cols(t.ModuleIDs.Size(8), t.Status)` serves
    `Query().Contains(ModuleIDs, 3).Delta(W, 1)`.
- **`Delta()` goes last.** It picks the delta index whose pinned Keys all have an
  Eq or a Contains, the most specific when several fit (a tie fails), and adds
  `UpdatedVersion >= W+1`: one exact sk range per Contains value.
- **Every write moves every delta row** (UpdatedVersion changed): a put and a
  delete per row, with the fan-out sync and its read-side re-check. Keys-only
  rows read their base records with a BatchGetItem, so a first sync costs
  about 0.5 RCU per record.
- A record written before its table got the index has no delta row, so Delta
  never returns it. Rewrite such records once (Put) to backfill.

## GroupBy counters (`group_by.go`)

An Index declaring `GroupBy` keeps, per base partition and per distinct value of
its Keys (the group), one counter: the record count and the sum of each GroupBy
column. A grouped read is one Query over the counters, never over the records.

```go
{Slot: dynamo.S1, Keys: dynamo.Cols(t.Channel, t.Status.Size(8)),
    GroupBy: dynamo.Cols(t.Total, t.Weight), GroupDelta: true},
{Keys: dynamo.Cols(t.Tags), GroupBy: dynamo.Cols(t.Total)},                // per element
{Keys: dynamo.Cols(t.CustomerID.Size(32)), GroupBy: dynamo.Cols(t.Total)}, // counters only

groups, err := Orders.QueryGroups(Orders.T.Channel, Orders.T.Status).Eq(Orders.T.StoreID, 7).Exec()
groups[0].Count; groups[0].Sum(Orders.T.Total); groups[0].SumFloat(Orders.T.Weight)
```

```text
pk   = base pk ‖ 000                      (shared with the slot-versions item, sk "v")
sk   = g<cb ids of the Keys>#<group key>  e.g. g005.006#web#<Status b64>
c    = record count            sNNN = sum of the column with cb id NNN
d    = a record holding only the group Keys (Group.Key)
upv, upd = the last write that touched it (GroupDelta only)
```

- **Diff on write.** Every write already reads the stored version. The groups
  of the stored and the written version are diffed: a group in both moves its
  sums by `new - old`, a group only written gains the record (`c +1`), a group
  only stored loses it (`c -1`). The deltas are merged per counter over the call
  and ADDed after the base items land, one UpdateItem per counter, 10 in parallel
  (each touches its own item and an ADD commutes). A record with
  `Status == 0` counts in no group, so a soft delete leaves them all.
- **Floats** are summed as `int64(round(v * 1e6))`, so adding and subtracting
  the same value is exact. A value outside ±9.2e12 fails the write.
- **Counters are never deleted.** An emptied group stays with `c = 0`: plain
  reads skip it, and `Since(W)` on a `GroupDelta` returns it, so a client drops
  the group. `Since(0)` skips it too.
- **Best-effort, rebuilt from the records.** No transaction ties the ADD to the
  base write: two plain Puts racing on one record, a crash between the two, or a
  retried ADD drift a counter. `PutManyIfVersion`/`Modify` winners are exact,
  because their condition proves the stored read. `RebuildGroups(partition...)`
  and `RebuildGroupsAll()` recompute the counters, rewrite only those that
  differ and zero the ones no record produces. On a `GroupDelta` the rewritten
  ones get a freshly reserved version. They are the backfill after adding a
  GroupBy, a column or changing its Keys.
- A `GroupDelta` counter's `upv` has `Delta()`'s window: a write reserving 9 can
  land after one reserving 10, so a client synced at 10 misses it until the
  group is written again.

## Optimistic concurrency: `PutManyIfVersion`, `Modify` (`modify.go`)

A write replaces the whole record (it is one blob), so two read-modify-writes of
the same record lose one of them. These two don't:

```go
// batch: read consistently, edit, write only what nobody wrote in between
accounts, err := Accounts.GetMany([]Account{{ID: 7}, {ID: 8}})  // consistent BatchGetItem
for i := range accounts { accounts[i].Balance += 100 }
lost, err := Accounts.PutManyIfVersion(accounts) // re-read, re-edit and retry these
```

- **PutManyIfVersion** writes each record with a conditional PutItem: the item's
  `upv` must still equal the record's `UpdatedVersion` (0: no item may exist).
  Everything else is PutMany's: one version reserved per call and base pk,
  hidden rows diffed against the stored versions (one consistent BatchGetItem)
  and batched, the slot versions bumped once. Only the base items are single
  PutItems (BatchWriteItem takes no condition), run 10 at a time. It returns the
  records that lost a race, unwritten; the caller re-reads, re-edits and retries
  them (`ConflictBackoff(attempt)` is the pause Modify uses).
- **GetMany** is the read before it: a consistent BatchGetItem (100 keys per
  call), missing keys left out, any order. Each item is billed rounded up to 4 KB.
- **GetManyForUpdate** is GetMany that also keeps the stored blobs in the
  process **write cache** (`write_cache.go`), so the PutManyIfVersion that
  follows diffs the hidden rows without its own stored-version read. `Modify`
  keeps its read there too. An entry (pk#sk → `upv`, blob) is used only at the
  exact version the write is conditioned on, which the condition then proves; a
  record expected new (version 0) needs no read at all. Entries live 15 s, up to
  10,000; a full cache drops expired ones, else stops taking new ones. Only
  tables with hidden rows are cached, and plain Put/PutMany never use it (with no
  condition, a stale entry would diff wrong rows).

For one record, `Modify` runs the whole loop:

```go
Schema{..., VersionedWrites: true} // or SaveUpdatedVersion, or a TypeDelta index

saved, err := Accounts.Modify(Account{ID: 7}, func(account *Account, exists bool) error {
    account.Balance += 100 // edit the record as stored right now
    return nil
})
```

- **Versioned table:** `VersionedWrites`, `SaveUpdatedVersion` or a `TypeDelta`
  index stamp the managed `int32` `UpdatedVersion` on every write and store it
  as the item attribute `upv`. The version is reserved once per write call and
  base pk (one atomic ADD on the sequence item, shared by every record of the
  call), and that is enough: each write of a record still gives it a value no
  earlier call had. A `Modify` writes one record, so it pays one ADD per record.
  A record stored before its table was versioned has no `upv`:
  `Modify` returns an error until it is `Put` once.
- **Modify** reads the item consistently, runs the change, and writes it with
  `PutManyIfVersion`, conditioned on the version it read whatever the change did
  to `UpdatedVersion`. If another write landed in between, it reads and runs the
  change again: up to 8 attempts with a growing pause (about 2s in all), then
  `ErrWriteConflict`. **The change must be safe to run more than once.**
- `exists` is false when nothing is stored: the change gets the bare key and may
  create the record. A change that leaves the record byte-identical writes
  nothing and moves no version. The change must not edit the Keys.
- A lost race is still billed. A stored item with no `upv` fails with an error
  naming it, instead of losing every attempt.
- **Rows of a lost write:** the new hidden rows go before the conditional put.
  On a lost race its delta rows are deleted (their sk holds a version only that
  call reserved); its fan-out rows stay as extra rows, which reads re-check.
  **FullCopy caveat:** a lost write may have rewritten a FullCopy row with its
  own, never-stored record, and a Contains returns that copy until the next
  successful write of the record. Modify always ends in one; a PutManyIfVersion
  caller that drops a lost record instead of retrying it leaves the stale copy.
- **Derived data:** a record computed from other records is read first, then
  its inputs (with `Consistent()`), and every writer of those inputs recomputes
  the derived record after its own write. The write that lands last was then
  computed from the newest inputs. In a batch, read all the derived records,
  then the inputs once for all of them.
- `Query().Consistent()` reads the base table with strong consistency (2× the
  read units), the BatchGetItem of a keys-only Contains included. A query planned
  on a GSI fails: GSIs can't read consistently.

## Using it

```go
var Products = dynamo.NewRepo[ProductTable, Product]()   // compile once, reuse

// writes
Products.Put(&p)
Products.PutMany(list)          // batched (25/req) with unprocessed-item retry
Products.InsertMany(list)       // PutMany for records known to be new: no stored-version read
err := Orders.AssignIDs(orders) // reserve the autoincrement IDs now, write later with InsertMany
written, err := Products.PutIfAbsent(&p) // false when the key already exists: one conditional PutItem
saved, err := Products.Modify(Product{ID: "sku1"}, change) // read-modify-write, retried on a race
lost, err := Products.PutManyIfVersion(read)               // batch: write unless written since the read
Products.Delete(&Product{ID: "sku1"})

// point read (only the Partition and Keys fields needed)
got, err := Products.Get(Product{ID: "sku1"})

// top N of one partition (one value per Partition column, in schema order;
// none for an entity without Partition)
top, err := Products.TopN(10)

// list an entity regardless of partition (a Query when the entity has no
// Partition, else a Scan over its pk range; admin/debug)
all, err := Products.Scan(10)

// queries — Products.T carries the named, typed columns
var out []Product
err := Products.Query().
    Between(Products.T.ID, "sku1", "sku5").            // -> sk range (order-preserving)
    Desc().Limit(50).
    Exec(&out)

// query a GSI: an equality on a full index key routes to that slot, and the
// shared sk still narrows it
Products.Query().Eq(Products.T.CategoryID, int32(7)).Exec(&out)                          // gsi-n1
Products.Query().Eq(Products.T.Brand, "acme").BeginsWith(Products.T.ID, "sku").Exec(&out) // gsi-s1

// query a fan-out index: records whose TagIDs hold 3 or 9
Products.Query().Contains(Products.T.TagIDs, 3, 9).Exec(&out)

// filter a non-key field (Price is in no key): only QueryScan() allows it,
// and it reads at most 5 MB
Products.QueryScan().Eq(Products.T.Brand, "acme").Gt(Products.T.Price, int64(1000)).Exec(&out)
```

### `Query()` vs `QueryScan()`

`Query()` is strict: every predicate must be served by a key (pk, sk or a GSI
key). A predicate on a field that lives only inside `d` fails at plan time with
`Query() cannot filter <field> through an index: use QueryScan()`. `QueryScan()`
plans the same index read, then filters the decoded records in memory — so it
pays for every row the index returns, matched or not. To keep that bounded it
stops with an error once it has read more than **5 MB** (640 RCU, eventually
consistent: 4 KB per 0.5 RCU) without finishing. The budget is counted in read
units, not pages, because a keys-only `Contains` also spends BatchGetItem reads
that are not Query pages. Predicate order never matters: the planner picks the
index, not the call order.

### How a query is planned (`query.go`)

1. **Partition source** — a `Contains` targets its fan-out rows (and needs
   `=` on every `Partition` column). Otherwise, if the entity has `Partition`
   columns and all of them have an `=`, use the base table (`pk`); else the first
   GSI whose key columns all have `=`; else the base table, when it is available
   (an entity without `Partition` always is: its pk is the TableID alone). So a
   no-partition entity still routes an `=` on a GSI key to that GSI instead of
   reading the whole entity.
2. **Keys condition** — predicates on the `Keys` columns become the shared `sk`
   key condition (`=`, `begins_with`, range, `between`). On fan-out rows the
   sort columns are the index `Keys` (the element pins the slice like an `=`)
   followed by the `Keys`. What the key
   condition can't express exactly (a strict `<` under an equality prefix) is
   kept as a key filter, checked in memory — still a key predicate, so `Query()`
   allows it.
3. **Leftovers** — predicates on non-key fields (which live inside `d`) are
   rejected by `Query()` and evaluated **in memory** by `QueryScan()`.

> Because the physical table shares one `sk` across the base table and all GSIs,
> the sort/range dimension is uniform. Every range is exact at any position of a
> composite sort key: the rows whose ranged column equals `v` sort in `[v, v$)`
> (`$` is the byte after the `#` separator), so `<= v` is `< v$`, `> v` is
> `>= v$` and `BETWEEN a AND b` is `BETWEEN a AND b$`. After an equality prefix a
> one-sided range becomes a `BETWEEN` bounded by that prefix (else it would run
> into the next prefix value's rows); only a strict `<` there also needs the
> in-memory filter.

## Live check (`ormcheck/`)

`ormcheck.Run` writes one record into each of two check tables (`ormcheck_order`:
partitioned, packed sort key, numeric/composite/string GSIs, keys-only array
indexes; `ormcheck_product`: no partition, FullCopy array index, a keyless and a
ColSlice delta index), reads them back
through every access path, updates them and reads again, then wipes them. Each
line shows the result, and the calls and capacity (RCU/WCU) the ORM spent. In
berryapps it runs with `./deploy.sh 4` against the real table. Importing the
package installs its capacity meter through `dynamo.ClientOptions`.

## Introspection: schema as data (`introspect.go`)

`dynamo.GetSchema[Table]()` returns a JSON-serializable `TableSchema` — an optional
label, the Go struct name (via reflection), the entity namespace, the physical
table, the base-table primary key, and every GSI with its key columns and type.
It's the backend half of a frontend table visualizer: expose it from an endpoint
and render it.

Give a schema a human label with the optional `Name` field on `GetSchema()`:

```go
func (t ProductTable) GetSchema() dynamo.Schema {
    return dynamo.Schema{
        Name:   "Products",   // optional, informational only
        Entity: "prod",
        // ...
    }
}

schema := dynamo.GetSchema[models.ProductTable]()   // or Products.Schema()
json.NewEncoder(w).Encode(schema)
```

```jsonc
{
  "name": "Products",           // Schema.Name (may be "")
  "struct": "ProductTable",     // Go table struct name (reflection)
  "entity": "prod",
  "tableID": 12345678,          // Schema.TableID, or HashTableID(Entity)
  "tableName": "demo-app",      // physical DynamoDB table
  "partition": null,            // no Partition: pk = TableID
  "keys": [
    { "field": "ID", "attr": "ID", "type": "string" }
  ],
  "indexes": [
    { "kind": "primary", "attr": "pk", "isNumber": true, "sharesSortKey": true, "columns": [ /* pk cols */ ] },
    { "kind": "array", "attr": "pk", "isNumber": true, "sharesSortKey": true, "columns": [ /* TagIDs */ ] },
    { "kind": "gsi", "name": "gsi-n1", "attr": "n1", "isNumber": true,  "sharesSortKey": true, "columns": [ /* CategoryID */ ] },
    { "kind": "gsi", "name": "gsi-s1", "attr": "s1", "isNumber": false, "sharesSortKey": true, "columns": [ /* Brand */ ] }
  ],
  "fields": [                   // every table struct column, in declaration order
    { "field": "ID", "attr": "ID", "type": "string" },
    { "field": "Name", "attr": "Name", "type": "string" }
  ]
}
```

Every GSI shares the base table's `sk` as its range key (`sharesSortKey`), so
the top-level `keys` applies to the primary key and every index alike. It's a
cold path that reads the table struct's `GetSchema()` directly — no accessors,
no `metaCache` — so it needs only the table type, not the record type.

## Controllers: uniform entity operations (`controller.go`)

Following genix's `ScyllaController` / `ScyllaControllerInterface`, the ORM
exposes a non-generic `Controller` handle so a heterogeneous set of entities can
be driven by one command. `*Repo[T,E]` implements it, so the same value used for
reads/writes is its controller; `dynamo.NewController[T,E]()` builds one directly.

```go
type Controller interface {
    Entity() string             // entity name (keys are namespaced by its TableID)
    TableName() string          // physical DynamoDB table
    Schema() TableSchema        // serializable schema (see introspection)
    DeleteRecordsAll() (int, error)
    // Strict, index-only dynamic query by field name; newest first when desc.
    QueryRecords(preds []QueryPredicate, limit int32, desc bool) ([]any, error)
    // The same, filtering what no key serves in memory (QueryScan, 5 MB cap).
    QueryScanRecords(preds []QueryPredicate, limit int32, desc bool) ([]any, error)
    // JSON array → E values, every key built: a bad payload fails before a write.
    DecodeRecords(recordsJSON []byte) ([]any, error)
    // PutMany of E values; returns them as written (autoincrement IDs assigned).
    PutRecords(records []any) ([]any, error)
}

// Registry — the analogue of genix's MakeScyllaControllers().
controllers := []dynamo.Controller{ models.Products, models.Orders }
for _, c := range controllers {
    n, err := c.DeleteRecordsAll()   // wipe every record of the entity
    fmt.Printf("%s: %d deleted (%v)\n", c.Entity(), n, err)
}
```

`DeleteRecordsAll` scans the entity's two pk ranges — base rows
(`TableID ‖ 0…0` to `TableID ‖ 9…9`) and array index rows (3 digits longer) —
projecting only the key attributes (it never decodes `d`), then deletes in
`BatchWriteItem` batches of 25 with the same unprocessed-item retry as `PutMany`.
The returned count includes array index rows. Sibling entities and the internal
sequence counters (pk 0) are outside those ranges and untouched.
**Destructive, no undo.** Exposed on the CLI as `go run . wipe`.

## Performance: cached metadata + precompiled accessors

Like genix, the ORM separates immutable metadata from per-query state:

- **Table metadata is compiled once and cached** (`metaCache`, keyed by record
  type): schema validation, key/index resolution and the accessor table are
  built a single time.
- **Field reads on the hot paths use precompiled `xunsafe` accessors**, not
  runtime reflection. During compilation each record field gets a
  type-specialized closure (`getStr`/`getI64`/`getU64`/`getF64`/`getBool`, built
  off `xunsafe.NewField`); building `pk`/`sk`/index slots and evaluating
  post-filters reads straight from the struct pointer (`unsafe.Pointer`) with the
  width-correct getter — no `reflect.Value.Field` per row.
- The **whole-record encode/decode** into column `d` goes through `colbin`,
  which is itself `xunsafe`-based.

So the only reflection left is one-time, at compile: after that, both the key
path and the record body avoid it.

## What was intentionally dropped from genix

Materialized/hash/radix views, `int64` packing, index groups, the generic-record
by-IDs reads, and CQL deploy/homologation. DynamoDB's fixed physical schema and string
keys make most of that unnecessary — so this is, as expected, a much smaller
ORM. (Cached metadata, precompiled `xunsafe` accessors, autoincrement sequences,
by-IDs slot versions and entity controllers are kept/ported — see above.)

## Config & tests

- Table name from `dynamo.TableName` (set at startup); falls back to the
  `DYNAMO_TABLE` environment variable, then `demo-app`.
- Client uses the standard AWS chain; set `DYNAMO_ENDPOINT` for a local DynamoDB.
- `go test ./dynamo/` runs fully offline (encoding, marshaling and query
  planning); it does not require AWS.

## Quick listing from the CLI

```sh
cd backend
go run . seed       # insert demo products and orders
go run . fn1        # prints the top 10 records of each sample table
go run . wipe       # delete ALL records of every entity (via controllers)
```

`fn1` needs live DynamoDB access (standard AWS env, or `DYNAMO_ENDPOINT` for a
local DynamoDB). It calls `Products.Scan(10)` / `Orders.Scan(10)` and prints
each decoded record; connection/permission errors are reported per table.
```

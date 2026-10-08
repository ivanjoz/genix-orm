---
name: genix-dynamo-orm
description: Declare tables, design keys and indexes, and read/write DynamoDB records with the genix-orm ORM. Use whenever backend code queries or writes the database, or adds or changes a table.
---

# genix-orm/dynamo

A statically typed single-table DynamoDB ORM. Every entity shares one physical table:

```
{ pk (Number), sk (String), h1/r1..h10/r10 (GSI hash Number / range String), d (colbin blob of the whole record) }
```

DynamoDB can only use the **key attributes**. Every field lives inside `d`, so a field that is
not a key can't be queried by DynamoDB, only filtered in memory after the read. **Design the keys
from the queries you need, before writing the table.**

The full reference is `README.md` next to this skill, and the design decisions are in
`RATIONALE.md`. Read the README before designing a non-trivial key.

> **A project may wrap the ORM.** In berryapps, modules import `"app/db"` (aliases: `db.Schema`,
> `db.Col`, `db.ColSlice`, `db.Keys`, `db.G1`…, `db.NewRepo`) and never
> `github.com/ivanjoz/genix-orm/dynamo` directly. Tables live in `backend/<module>/types/`.
> The examples below use `dynamo.`; in berryapps, write `db.` instead.

## 1. Declaring a table

```go
type Order struct {
    ID         int32   `json:"ID" cb:"1"`      // cb ids are the stored field ids: never renumber
    StoreID    int32   `json:"StoreID" cb:"2"`
    CustomerID int32   `json:"CustomerID" cb:"3"`
    Status     string  `json:"Status" cb:"4"`
    ProductIDs []int32 `json:"ProductIDs" cb:"5"`
    Total      int64   `json:"Total" cb:"6"`   // in no key: only QueryScan can filter it
}

type OrderTable struct {
    dynamo.Model[OrderTable, Order]
    ID         dynamo.Col[OrderTable, int32]
    StoreID    dynamo.Col[OrderTable, int32]
    CustomerID dynamo.Col[OrderTable, int32]
    Status     dynamo.Col[OrderTable, string]
    ProductIDs dynamo.ColSlice[OrderTable, int32] // ColSlice: E is the ELEMENT type
    Total      dynamo.Col[OrderTable, int64]
}

func (t OrderTable) GetSchema() dynamo.Schema {
    return dynamo.Schema{
        Name:   "Orders",                     // optional label
        Entity: "sales_order",                // TableID = HashTableID(Entity) unless TableID is set; pk = TableID
        Keys:   dynamo.Cols(t.ID.Size(32)),   // the record's key → sk. Required
        Indexes: []dynamo.Index{
            // GSIs: Keys are the GSI's sort key (then the base Keys), ranged like sk.
            {Slot: dynamo.G1, Keys: dynamo.Cols(t.CustomerID.Size(32))},          // a customer's orders, by ID
            {Slot: dynamo.G2, Keys: dynamo.Cols(t.StoreID.Size(16), t.Status)},   // Eq(StoreID), then Status ranges
            // Fan-out (a ColSlice in Keys, no Slot): one row per element, sk = ProductID ‖ StoreID ‖ base sk.
            {Keys: dynamo.Cols(t.ProductIDs.Size(32), t.StoreID.Size(16))}, // keys-only rows (default)
            // {Keys: dynamo.Cols(t.X), FullCopy: true}                     // rows also carry d: 1 read, costlier writes
        },
        UseAutoincrement: true,               // fills an integer field named ID when 0
    }
}

var Orders = dynamo.NewRepo[OrderTable, Order]() // package-level: a bad schema panics at boot
```

### Choosing `Keys` and `Partition`

- **`Keys` is the record's identity.** `pk + sk` identify a record: `Get`/`Delete` take the
  `Keys` values, and a `Put` with different ones writes **another** item. The default is
  `Keys: dynamo.Cols(t.ID...)`.
- **`Keys` is also the base table's range dimension.** A range through the base table ("orders
  between two dates") needs that field in `Keys` before the ID: `Cols(t.Created.Size(32), t.ID.Size(32))`.
  - The cost is identity: `Get` then needs `Created` too, and changing `Created` is a
    `Delete` + `Put`. Only add a field when it never changes and the range is a real access path.
  - With an autoincrement ID the ID order is already the creation order, so a range on `ID` often does the job.
- **A GSI is a second sorted copy.** Its `Keys` are its sort key, followed by the base `Keys` they
  leave out, and take an `Eq` on a leading run then one range, exactly like `sk`. So one GSI covers
  every prefix: `{Slot: G1, Keys: Cols(t.ClientID.Size(32), t.Fecha.Size(16))}` serves
  `Eq(ClientID)`, `Eq(ClientID).Between(Fecha)` and `Between(ClientID)`. Write the columns you
  range on explicitly, even when they are base Keys.
  - Its hash is the entity's `Partition` (an `Eq` on each column, like the base table). An Index may
    declare its own `Partition` (integers): to spread a hot index, or to read across the entity's
    partitions — `{Slot: G1, Partition: Cols(t.CustomerID.Size(32)), Keys: Cols(t.Created.Size(32))}`.
  - Put a GroupBy on the same Index to also get counters over those Keys (section 3d).
- **`Partition` is optional.** Without it, pk = the TableID and the whole entity is one pk. That
  is the usual case: users, profiles and items in berryapps have no partition.
  - Add one only for a large entity that is always read per tenant, store or slot. It spreads the
    throughput. berryapps' cron partitions by `Slot` so each tick reads only its own slot.
  - Partition columns are integers.
  - A leading `Keys` column narrows a read the same way through an sk prefix, so a partition is
    a throughput choice, not a query one.

### Schema rules

A violation makes `NewRepo` panic at boot:

- Every **integer key column** (Partition, Keys, GSI, fan-out element) declares `.Size(bits)`.
  Values `>= 2^bits` and negative values panic when written.
- The pk (`TableID ‖ zero-padded partition decimals`) must fit in 38 digits.
- **GSI slots** `G1`..`G10` take one column or a composite of several as `Keys`, and an optional
  integer `Partition`. Each slot is used once per table. An unused slot costs nothing; each used
  one costs a GSI write per record write.
- **String key parts must not contain `#`,** the composite separator. Validate or normalize user
  input that goes into a key.
- **Fan-out indexes** (an `Index` whose `Keys` hold a `ColSlice`):
  - exactly one `ColSlice` per index, anywhere in `Keys`; the other `Keys` are scalar key columns;
  - no `Slot` (the rows live in the base table); `FullCopy` is only valid here;
  - one fan-out index per slice field (its rows are located by the field's cb id), several per table allowed;
  - the field needs a `cb:"N"` tag with N in 1..999;
  - integer elements need `.Size`;
  - the `ColSlice` element type must match the field.
- **TableID:**
  - leave it at 0 to use the Entity hash;
  - pin an explicit 8-digit `TableID` to resolve a collision (two entities on one TableID panic at boot);
  - pin it when renaming an `Entity` without moving its data.
- **Autoincrement** needs an integer field named `ID`. `AutoincrementRandomPadding` (0..9) adds
  random low digits.
- **`SaveUpdatedVersion`** needs exactly one integer `Keys` column and an `int32` field named
  `UpdatedVersion` (`json:"upv"`) in the record and the table struct. See section 3b.
- **Delta indexes** (`{Type: TypeDelta, Keys: ...}`) need the same `int32` `UpdatedVersion`, and
  a `cb` tag on their first Key (on `UpdatedVersion` when they have none). See section 3c.
- **Local indexes** (`{Type: TypeLocal, Keys: ...}`) take scalar Keys only, the first one with a
  `cb` tag, and no Slot or GroupBy. See section 3c'.
- **`VersionedWrites`** needs the same `int32` `UpdatedVersion`; it only enables `Modify` (section 2)
  on a table that has neither `SaveUpdatedVersion` nor a delta index (those imply it).
- **DataFrames** need the `int32` `UpdatedVersion` and `CreatedVersion`, a whole-entity delta
  index and no `Partition`. See section 3e.
- **Managed fields:** every Put stamps an integer field named `Updated` with the write time
  (SUnixTime) and, on a versioned table (a delta index, `SaveUpdatedVersion` or `VersionedWrites`),
  `UpdatedVersion` with the write sequence, also stored as the item attribute `upv`. On a table
  with DataFrames, `CreatedVersion` is the `UpdatedVersion` of the insert. Never set them yourself.

## 2. Writing and reading by key

```go
err := Orders.Put(&order)              // upsert; assigns ID when autoincrement and ID == 0
err  = Orders.PutMany(orders)          // batches of 25, retries unprocessed items
err  = Orders.InsertMany(orders)       // PutMany for records known to be new: skips the stored-version read
err  = Orders.AssignIDs(orders)        // reserve autoincrement IDs before writing (then InsertMany)
ok, err := Orders.PutIfAbsent(&order)  // false when the key already exists
err  = Orders.Delete(&Order{ID: 9})    // only Partition + Keys fields needed
got, err := Orders.Get(Order{ID: 9})   // (nil, nil) when missing
top, err := Orders.TopN(10)            // first N by sk; one value per Partition column, none here
all, err := Orders.Scan(100)           // admin/debug only: reads the whole entity
```

- Changing a `Partition`/`Keys` field and calling `Put` writes a new item, and the old one stays.
  `Delete` the old key first.
- Fan-out rows are synced automatically by `Put`/`PutMany`/`PutIfAbsent`/`Delete`. Never
  write them yourself. A scalar fan-out `Keys` column that changes (e.g. `Updated`) rewrites
  every element row (a delete + a put each) on the write that changes it.
- Soft delete (a status field set to 0) is a normal `Put`. `Delete` removes the item and its array rows.
- **Read-modify-write → `Modify`, never Get + Put.** A Put replaces the whole record, so a Get + Put
  loses any write that landed in between. On a versioned table:

  ```go
  saved, err := Orders.Modify(Order{ID: 9}, func(order *Order, exists bool) error {
      order.Status = 2 // edit the record as stored now; may run several times
      return nil
  })
  ```

  It reads consistently and writes only if `upv` still holds the version read, re-running the
  change on a race (8 attempts, then `dynamo.ErrWriteConflict`). An unchanged record writes nothing.
  Records stored before the table was versioned have no `upv`: `Put` them once.
- **Many records → `GetMany` + `PutManyIfVersion`.** `GetMany(keys)` reads consistently;
  `PutManyIfVersion(records)` writes each one only if its `UpdatedVersion` is still the stored one,
  with one version reservation for the call, and returns the ones that lost a race: re-read, re-edit
  and retry those (pause with `ConflictBackoff(attempt)`). On a table with fan-out or delta indexes,
  read with `GetManyForUpdate` instead: it keeps the stored blobs in the process write cache (15 s),
  so the write diffs the index rows without reading them again.
- **Derived data** (a record computed from others): read the derived records first, then their
  inputs with `Query().Consistent()` / `GetMany`, and have every writer of those inputs recompute
  the derived records after its own write.

## 3. Querying

```go
var out []Order
err := Orders.Query().
    Gte(Orders.T.ID, int32(1000)).           // Keys range → sk condition
    Desc().Limit(50).
    Exec(&out)

Orders.Query().Eq(Orders.T.CustomerID, int32(77)).Exec(&out)                                 // gsi-1
Orders.Query().Eq(Orders.T.CustomerID, int32(77)).Gte(Orders.T.ID, int32(1000)).Exec(&out)   // gsi-1, then the base Keys
Orders.Query().Eq(Orders.T.StoreID, int32(3)).Eq(Orders.T.Status, "paid").Exec(&out)         // gsi-2
Orders.Query().Eq(Orders.T.StoreID, int32(3)).BeginsWith(Orders.T.Status, "pa").Exec(&out)   // gsi-2, ranged
Orders.Query().Contains(Orders.T.ProductIDs, 5, 8).Exec(&out)                                // fan-out index
Orders.Query().Eq(Orders.T.ProductIDs, 5).Gte(Orders.T.StoreID, int32(3)).Exec(&out)          // Eq on a ColSlice = Contains
found, err := Orders.Query().Eq(Orders.T.CustomerID, int32(77)).First(&order)
```

Operators: `Eq`, `Gt`, `Gte`, `Lt`, `Lte`, `Between`, `BeginsWith`, `Contains`, `Desc`, `Limit`,
`Exec`, `First`. Integer values can be any integer Go type. The order of the calls doesn't matter.

**How the planner picks the index:**

1. **Access path:**
   - `Contains` (or `Eq` on a `ColSlice`) → that fan-out index. It needs `Eq` on every
     Partition column.
   - Otherwise the base table and every GSI are candidates. One is usable when each of its
     partition columns (the entity's, or the Index's own) has an `Eq`; an entity without
     Partition always is. The usable one that serves the **most predicates** wins; a tie goes to
     the base table, then to the GSIs in declaration order.
   - Nothing usable → error `no usable partition`.
2. **Range condition**, on the chosen path's range columns (the `Keys` for the base table; the
   index Keys then the base Keys for a GSI; the index Keys, element pinned, then the `Keys` for a
   fan-out row): an `Eq` on a leading run, then **one** range or `BeginsWith` on the next column.
3. **Everything else is a non-key predicate.**

### `Query()` vs `QueryScan()`

- **`Query()` is strict.** A non-key predicate fails with
  `Query() cannot filter <field> through an index: use QueryScan()`. That includes a predicate
  on a range column after the ranged one, or on a column the chosen path does not hold.
  Prefer `Query()`: every row it reads is a result.
- **`QueryScan()`** runs the same index read, then filters the decoded records in memory.
  - It pays for every row the index range holds, so narrow the index first.
  - It fails with `QueryScan read more than 5 MB` once it passes 640 RCU without finishing.
    It never returns partial results.
  - Use it for status filters on small entities and for filters on non-key fields inside a
    narrow index read. Delta syncs use `Delta()` (section 3c), not `Gt(Updated)`.
- In-memory filters compare strings, bools and numbers. A predicate on a slice/struct field
  matches nothing. Use `Contains` on a fan-out indexed `ColSlice` instead.

### `Contains` (fan-out indexes)

- Matches records holding **any** of the values: one Query per value, deduplicated. Results come
  value by value, not in sk order.
- Every index `Keys` column **before** the slice needs an `Eq`; the ones after it take ranges.
  With `Cols(ProductIDs, Created)`: `Contains(ProductIDs, 5).Gt(Created, t)` is one exact range.
  With every index column pinned, the base `Keys` range too: `Contains(...).Eq(Created, t).Gte(ID, x)`.
- Only one `Contains` per query.
- Keys-only rows take a second BatchGetItem for the records. `FullCopy: true` answers in one
  Query but rewrites every element row on every `Put`. Choose FullCopy for read-heavy, rarely
  written records with short slices.

## 3b. By-IDs cache (`SaveUpdatedVersion`, `QueryCachedIDs`)

Use it for "give me records [12, 87, 412], skip the ones I already have unchanged". It backs
genix-ui's cache-by-ids (`getRecordByID`, `RecordByIDText`, `GetHandler.routeByID`).

```go
// schema: Cols(t.ID.Size(32)) and nothing else in Keys, plus
SaveUpdatedVersion: true,
// record and table struct:
UpdatedVersion int32 `json:"upv,omitempty" cb:"N"`
UpdatedVersion dynamo.Col[OrderTable, int32]

changed, err := Orders.QueryCachedIDs(cachedIDs)          // []dynamo.IDUpdatedVersion{ID, UpdatedVersion}
changed, err  = Orders.QueryCachedIDs(cachedIDs, storeID) // one value per Partition column
```

- A record is in slot `uint8(ID)`. Each partition has one hidden slot-versions item
  (`pk = base pk ‖ 000`), where every write ADDs 1 to the slots it touched, after the record is
  written.
- `QueryCachedIDs` does one GetItem for the slot versions, then one consistent BatchGetItem for
  the IDs whose client version (0 = none) differs. Unchanged and missing IDs are left out, and the
  returned records carry the **slot** version in `UpdatedVersion`.
- The ORM owns `UpdatedVersion`. As stored it is the write sequence (section 3c). Only a by-IDs
  read overwrites it with the slot version, as in genix.
- A write to one record makes the other records of its slot (IDs 256 apart) come back once too.
- A record never written since the flag was turned on has no slot version, so it is read on every
  request. Re-`Put` the existing records once after enabling the flag.

## 3c. Delta sync (`TypeDelta`, `Delta()`)

Use it for "the records written since watermark W": genix-ui's delta cache (skill `delta-cache-api`).

```go
// schema:
Indexes: []dynamo.Index{
    {Type: dynamo.TypeDelta, Keys: dynamo.Cols(t.Status)},                       // the whole entity
    {Type: dynamo.TypeDelta, Keys: dynamo.Cols(t.TeamIDs.Size(8), t.Status)},    // per team, fan-out
},

Orders.Query().Delta(watermark, 1).Exec(&out)                          // W = the client's highest upv
Orders.Query().Contains(Orders.T.TeamIDs, 3).Delta(watermark, 1).Exec(&out)
```

- **The last Key, unless it is a ColSlice, is the sync filter column.** It is not in the row sk.
  `Delta(W, values...)` keeps only those values on a first sync (`W = 0`) and every value on a
  later one, so soft-deleted records reach the clients caching them.
- **The other Keys are pinned:** `Eq` on each, or `Contains` on the ColSlice (rows per element).
  Partition columns need their `Eq` too.
- **Call `Delta()` last.** It picks the delta index whose pinned Keys the query pins (the most
  specific wins, a tie fails), and adds `UpdatedVersion >= W+1`: one exact range.
- Rows are hidden base-table rows (`pk = base pk ‖ cb id`), keys-only unless `FullCopy`. Every
  write moves all of them, because `UpdatedVersion` changed. A slice field takes either a fan-out
  index or a delta index, not both.
- A record written before the index was declared has no delta row: re-`Put` existing records once.

## 3c'. Local indexes (`TypeLocal`)

Use it for a second sort order that must be read **consistently** (a GSI can't be): "is this name
already taken?" right after another write. berryapps' products use it for the name hash.

```go
// schema (scalar Keys only: no Slot, no ColSlice, no GroupBy):
{Type: dynamo.TypeLocal, Keys: dynamo.Cols(t.NameHash.Size(32))},

Products.Query().Eq(Products.T.NameHash, hash).Consistent().Exec(&out)
```

- It is **not** a DynamoDB LSI (those can only be declared when the table is created). Its rows
  are hidden base-table rows, `pk = base pk ‖ cb id of the first Key`, `sk = Keys ‖ base sk`,
  keys-only: a read is a Query plus a BatchGetItem, and each row is re-checked against the record.
- The planner treats it like a GSI over the entity's Partition: `Eq` on a leading run of its Keys,
  then one range. `Consistent()` works, unlike on a GSI.
- Writes keep the rows in sync (a changed Key deletes the old row and puts the new one), at one
  extra write per changed record.
- The first Key needs a `cb` tag, and no other hidden-row index (fan-out, delta) may use the same
  field first. A record written before the index was declared has no row: re-`Put` it once.

## 3d. GroupBy counters (`GroupBy`, `QueryGroups`)

Use it for "count and totals per group" without reading the records: one counter item per base
partition and per distinct value of the Index Keys.

```go
// schema: on a GSI (counters over its Keys), a fan-out Index, or a slot-less Index (counters only)
{Slot: dynamo.G1, Keys: dynamo.Cols(t.Channel, t.Status.Size(8)),
    GroupBy: dynamo.Cols(t.Total, t.Weight), GroupDelta: true},
{Keys: dynamo.Cols(t.Tags), GroupBy: dynamo.Cols(t.Total)},                 // one group per element
{Keys: dynamo.Cols(t.CustomerID.Size(32)), GroupBy: dynamo.Cols(t.Total)},  // no GSI used

groups, err := Orders.QueryGroups(Orders.T.Channel, Orders.T.Status).
    Eq(Orders.T.StoreID, 7).Eq(Orders.T.Channel, "web").Exec()
groups[0].Key.Status; groups[0].Count; groups[0].Sum(Orders.T.Total); groups[0].SumFloat(Orders.T.Weight)
Orders.QueryGroups(Orders.T.Channel, Orders.T.Status).Eq(Orders.T.StoreID, 7).Since(watermark).Exec()
```

- **`GroupBy` columns are integer or float `Col`s**; the count is always kept. Floats are summed as
  `round(v * 1e6)`: exact to 6 decimals, ±9.2e12 at most (a write outside it fails).
- **`Status == 0` counts in no group** (soft delete). A fan-out GroupBy counts the record once per
  distinct element, with its whole values.
- **`QueryGroups(keys...)`** picks the GroupBy with exactly those Keys, in order. It needs an `Eq` on
  every Partition column, then `Eq` on a leading run of the Keys and one range, as `Keys` do.
  Groups whose count fell to 0 are skipped.
- **`GroupDelta: true`** stamps each counter with the `upv`/`upd` of the last write that touched it
  (needs the managed `UpdatedVersion`). `Since(W)` returns only the groups changed after W, emptied
  ones included (count 0) so the client drops them; `Since(0)` skips them.
- **Best-effort:** the counters are ADDed after the base write, from the diff against the stored
  version. Two plain `Put`s racing on one record, or a crash in between, drift a counter.
  `Repo.RebuildGroups(partition...)` / `RebuildGroupsAll()` (berryapps: `fn-db rebuild-groups`)
  recompute them. Run it too after adding a GroupBy to a table with records, and after removing one:
  the rebuild deletes the counters of a GroupBy no longer declared.
- Every write pays one `UpdateItem` per touched group (merged over the call), and a hot group is a
  hot item.

## 3e. DataFrames (`DataFrames`, `QueryFrame`)

Use it for "totals per day and product (and client)" over long ranges without reading DynamoDB:
compact files in S3, one per distinct value of the frame Keys, kept up to date by a 10-minute run.
A write shows up 10–20 minutes later. GroupBy counters are live; frames are cheaper to read in bulk.

```go
// record and table struct:
UpdatedVersion int32 `json:"upv,omitempty" cb:"12"`
CreatedVersion int32 `json:"crv,omitempty" cb:"13"`
// schema:
Indexes: []dynamo.Index{{Type: dynamo.TypeDelta, Keys: dynamo.Cols(t.Status)}}, // the run reads through it
DataFrames: []dynamo.DataFrame{
    {Name: "day-product", Keys: dynamo.Cols(t.Fecha), Rows: t.ProductID, Sums: dynamo.Cols(t.Quantity)},
    {Name: "day-client-product", Keys: dynamo.Cols(t.Fecha, t.ClientID), Rows: t.ProductID,
        Sums: dynamo.Cols(t.Quantity, t.Amount)},
},

rows, err := SaleLines.QueryFrame("day-client-product").
    Eq(SaleLines.T.ClientID, 412).Between(SaleLines.T.Fecha, from, to).Exec()
rows[0].Key.Fecha; rows[0].Key.ClientID; rows[0].Key.ProductID; rows[0].Sum(SaleLines.T.Amount)

// up to the last landed write, not the last run:
rows, err = SaleLines.QueryFrame("day-product").Between(SaleLines.T.Fecha, from, to).Fresh().Exec()
```

- **Keys** (1–3), **Rows** and **Sums** are integer `Col`s with `cb` tags. `Keys[0]` (usually the
  day) must lead the entity `Keys`, a GSI or a local index: it is the rebuild unit.
- **Values must be ≥ 0:** a negative Keys/Rows value fails the write, and a negative Sums value
  too unless the frame sets `AllowNegativeSums`. `Status == 0` counts in no frame.
- **`QueryFrame`:** `Eq` or `Between` (≤ 400 values) on `Keys[0]`, then `Eq` on a leading run of
  the later Keys. `dataframe.ErrNotBuilt` (berryapps: `db.ErrFrameNotBuilt`) until the frame's
  first build (two runs after it is declared). Rows lag the records by 10 to 30 minutes.
- **`.Fresh()`** before `Exec()` returns the rows as the records hold them now: the files plus the
  changes since the last run, computed in memory. It costs a consistent read of every record of
  the entity written in the last 10 to 30 minutes. Use it when a screen or report must include the
  sale just made; leave it off for history and dashboards.
- **Writes** read the stored version and append the old values to S3 before the base write: an
  update or delete of a frame table costs an S3 append per changed frame. A write fails when no
  frame store is set (berryapps sets it at boot).
- **Repair:** `RebuildDataFrames(frame, fromDay, toDay)` / `RebuildDataFramesAll(frame)`
  (berryapps: `fn-db rebuild-frames`). Needed after an `InsertMany` of records already stored and
  after editing files by hand.
- Changing a frame's Keys, Rows or Sums rebuilds it automatically on the next two runs. Renaming
  a frame orphans its old files.

## 4. Pitfalls

- **Put a two-sided range in one `Between`, never `Gte(f, a).Lte(f, b)`.** The planner keeps one
  predicate per field, so the other one is silently dropped. Use one predicate per field.
- **The path serving the most predicates wins, counting only an `Eq` run plus one range.**
  `(ClientID)` and `(ClientID, Fecha)` on one entity are redundant: the second covers the first.
  On a tie the base table wins, then the first declared GSI.
- **Indexes on transactional records carry the date.** A GroupBy or GSI keyed by a column alone
  (all time) never closes: a longer period is the range of its days, `Fecha > 0` its whole history.
- GSI queries and `Get` are **eventually consistent**. A read right after a write may miss it.
  Fan-out sync reads use consistent reads internally.
- **A `Size(bits)` is part of the stored key.** Changing it, changing the `Keys`/`Partition`
  columns, changing a slot's columns or changing the `Entity` (with no pinned TableID) orphans
  the stored keys. Reprocess the data (a script re-`Put`s every record, and the old items get
  deleted). Never add a read-time fallback.
- **Changing a fan-out index's `Keys` has no rebuild yet.** Re-`Put`ting does not fix it: the
  write diffs both versions with the new shape, so the old-shape rows stay and keys-only rows are
  not rewritten.
- A field referenced by a **GSI or `Keys`** must be set on every write. A zero value is still a key.
- `Controller.DeleteRecordsAll()` wipes the entity's base and fan-out rows, its GroupBy counters
  and its DataFrame states (the frames then rebuild). It is destructive and there is no undo. It
  keeps the slot-versions items.

## 5. Checking your work

- `go test ./...` in `genix-orm/dynamo` runs offline: encoding, marshaling and query planning. To
  assert which index a query picks, follow the plan tests in `query_test.go`.
- `ormcheck/` is a live check against a real table: three entities, every access path, the
  DataFrame runs, and the capacity used. In berryapps: action 4 of `./app.sh deploy` (that table is
  production — pre-alpha, single environment).
- Introspect a schema with `dynamo.GetSchema[OrderTable]()` / `Orders.Schema()` (JSON:
  `partition`, `keys`, every index).

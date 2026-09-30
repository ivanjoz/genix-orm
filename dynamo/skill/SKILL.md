---
name: genix-dynamo-orm
description: How to declare tables, design keys, write and query records with the genix-orm DynamoDB ORM (genix-orm/dynamo) — Schema Keys/Partition, GSI slots, Size(bits), fan-out Index/ColSlice/Contains, Query() vs QueryScan(), autoincrement, the by-IDs cache (SaveUpdatedVersion/QueryCachedIDs). Use whenever code reads or writes DynamoDB through this ORM, or adds/changes a table (in berryapps: anything under backend/**/types/ or importing "app/db").
---

# genix-orm/dynamo

A statically typed single-table DynamoDB ORM. Every entity shares one physical table:

```
{ pk (Number), sk (String), n1..n5 (Number GSIs), s1..s5 (String GSIs), d (colbin blob of the whole record) }
```

DynamoDB can only use the **key attributes**. Every field lives inside `d`, so a field that is
not a key can't be queried by DynamoDB, only filtered in memory after the read. **Design the keys
from the queries you need, before writing the table.**

The full reference is `README.md` next to this skill, and the design decisions are in
`RATIONALE.md`. Read the README before designing a non-trivial key.

> **A project may wrap the ORM.** In berryapps, modules import `"app/db"` (aliases: `db.Schema`,
> `db.Col`, `db.ColSlice`, `db.Keys`, `db.N1`…, `db.NewRepo`) and never
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
        Keys:   dynamo.Keys(t.ID.Size(32)),   // the record's key → sk. Required
        Indexes: []dynamo.Index{
            {Slot: dynamo.N1, Keys: dynamo.Keys(t.CustomerID.Size(32))}, // N slot: exactly one integer column
            {Slot: dynamo.S1, Keys: dynamo.Keys(t.StoreID.Size(16), t.Status)}, // S slot: string or composite
            // Fan-out (a ColSlice in Keys, no Slot): one row per element, sk = ProductID ‖ StoreID ‖ base sk.
            {Keys: dynamo.Keys(t.ProductIDs.Size(32), t.StoreID.Size(16))}, // keys-only rows (default)
            // {Keys: dynamo.Keys(t.X), FullCopy: true}                     // rows also carry d: 1 read, costlier writes
        },
        UseAutoincrement: true,               // fills an integer field named ID when 0
    }
}

var Orders = dynamo.NewRepo[OrderTable, Order]() // package-level: a bad schema panics at boot
```

### Choosing `Keys` and `Partition`

- **`Keys` is the record's identity.** `pk + sk` identify a record: `Get`/`Delete` take the
  `Keys` values, and a `Put` with different ones writes **another** item. The default is
  `Keys: dynamo.Keys(t.ID...)`.
- **`Keys` is also the only range dimension.** GSI keys are equality only, and every GSI shares
  the base `sk` as its range key.
  - A range on a field ("orders between two dates") is only possible with that field in `Keys`
    before the ID: `Keys(t.Created.Size(32), t.ID.Size(32))`.
  - The cost is identity: `Get` then needs `Created` too, and changing `Created` is a
    `Delete` + `Put`. Only add a field when it never changes and the range is a real access path.
  - With an autoincrement ID the ID order is already the creation order, so a range on `ID` often does the job.
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
- **N slots** take exactly one integer column. **S slots** take a string or a composite of several
  columns. Each slot is used once per table. A table has 10 slots at most: N1..N5 and S1..S5.
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
- **`SaveUpdatedVersion`** needs exactly one integer `Keys` column and a `uint16` field named
  `UpdatedVersion` (`json:"upv"`) in the record and the table struct. See section 3b.

## 2. Writing and reading by key

```go
err := Orders.Put(&order)              // upsert; assigns ID when autoincrement and ID == 0
err  = Orders.PutMany(orders)          // batches of 25, retries unprocessed items
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

## 3. Querying

```go
var out []Order
err := Orders.Query().
    Gte(Orders.T.ID, int32(1000)).           // Keys range → sk condition
    Desc().Limit(50).
    Exec(&out)

Orders.Query().Eq(Orders.T.CustomerID, int32(77)).Exec(&out)                                 // gsi-n1
Orders.Query().Eq(Orders.T.StoreID, int32(3)).Eq(Orders.T.Status, "paid").Exec(&out)         // gsi-s1 (full key)
Orders.Query().Eq(Orders.T.CustomerID, int32(77)).Gte(Orders.T.ID, int32(1000)).Exec(&out)   // gsi-n1 + shared sk
Orders.Query().Contains(Orders.T.ProductIDs, 5, 8).Exec(&out)                                // fan-out index
Orders.Query().Eq(Orders.T.ProductIDs, 5).Gte(Orders.T.StoreID, int32(3)).Exec(&out)          // Eq on a ColSlice = Contains
found, err := Orders.Query().Eq(Orders.T.CustomerID, int32(77)).First(&order)
```

Operators: `Eq`, `Gt`, `Gte`, `Lt`, `Lte`, `Between`, `BeginsWith`, `Contains`, `Desc`, `Limit`,
`Exec`, `First`. Integer values can be any integer Go type. The order of the calls doesn't matter.

**How the planner picks the index:**

1. **Partition source, one of:**
   - `Contains` (or `Eq` on a `ColSlice`) → that fan-out index. It needs `Eq` on every
     Partition column.
   - `Eq` on every Partition column (entity with a Partition) → the base table.
   - Otherwise, `Eq` on **every** key column of a GSI → the first such GSI, in declaration
     order. A partial GSI key is not usable, and a range on a GSI key column is not possible:
     GSI keys are equality only.
   - No GSI match on an entity without Partition → its whole pk (the TableID).
   - Nothing matches → error `no usable partition`.
2. **Keys condition:**
   - The leading `Keys` columns can take `Eq`, then **one** range or `BeginsWith` on the next column.
   - The sk is shared by the base table and every GSI, so `Keys` predicates narrow any of them.
   - A fan-out row sk is the index `Keys` (the element pins the slice like an `Eq`) followed by
     the `Keys`, and the same rule applies over that whole list.
3. **Everything else is a non-key predicate.**

### `Query()` vs `QueryScan()`

- **`Query()` is strict.** A non-key predicate fails with
  `Query() cannot filter <field> through an index: use QueryScan()`. That includes a predicate
  on a `Keys` column after the range column, or on a GSI column when another source was chosen.
  Prefer `Query()`: every row it reads is a result.
- **`QueryScan()`** runs the same index read, then filters the decoded records in memory.
  - It pays for every row the index range holds, so narrow the index first.
  - It fails with `QueryScan read more than 5 MB` once it passes 640 RCU without finishing.
    It never returns partial results.
  - Use it for delta syncs (`Gt(Updated, since)`), status filters on small entities, and filters
    on non-key fields inside a narrow index read.
- In-memory filters compare strings, bools and numbers. A predicate on a slice/struct field
  matches nothing. Use `Contains` on a fan-out indexed `ColSlice` instead.

### `Contains` (fan-out indexes)

- Matches records holding **any** of the values: one Query per value, deduplicated. Results come
  value by value, not in sk order.
- Every index `Keys` column **before** the slice needs an `Eq`; the ones after it take ranges.
  With `Keys(ProductIDs, Created)`: `Contains(ProductIDs, 5).Gt(Created, t)` is one exact range.
  With every index column pinned, the base `Keys` range too: `Contains(...).Eq(Created, t).Gte(ID, x)`.
- Only one `Contains` per query.
- Keys-only rows take a second BatchGetItem for the records. `FullCopy: true` answers in one
  Query but rewrites every element row on every `Put`. Choose FullCopy for read-heavy, rarely
  written records with short slices.

## 3b. By-IDs cache (`SaveUpdatedVersion`, `QueryCachedIDs`)

Use it for "give me records [12, 87, 412], skip the ones I already have unchanged". It backs
genix-ui's cache-by-ids (`getRecordByID`, `RecordByIDText`, `GetHandler.routeByID`).

```go
// schema: Keys(t.ID.Size(32)) and nothing else in Keys, plus
SaveUpdatedVersion: true,
// record and table struct:
UpdatedVersion uint16 `json:"upv,omitempty" cb:"N"`
UpdatedVersion dynamo.Col[OrderTable, uint16]

changed, err := Orders.QueryCachedIDs(cachedIDs)          // []dynamo.IDUpdatedVersion{ID, UpdatedVersion}
changed, err  = Orders.QueryCachedIDs(cachedIDs, storeID) // one value per Partition column
```

- A record is in slot `uint8(ID)`. Each partition has one hidden slot-versions item
  (`pk = base pk ‖ 000`), where every write ADDs 1 to the slots it touched, after the record is
  written.
- `QueryCachedIDs` does one GetItem for the slot versions, then one consistent BatchGetItem for
  the IDs whose client version (0 = none) differs. Unchanged and missing IDs are left out, and the
  returned records carry the **slot** version in `UpdatedVersion`.
- The ORM owns `UpdatedVersion`: it is zeroed on every write, so a delta list returns `upv` 0,
  which costs one revalidation on the by-IDs path.
- A write to one record makes the other records of its slot (IDs 256 apart) come back once too.
- A record never written since the flag was turned on has no slot version, so it is read on every
  request. Re-`Put` the existing records once after enabling the flag.

## 4. Pitfalls

- **Put a two-sided range in one `Between`, never `Gte(f, a).Lte(f, b)`.** The planner keeps one
  predicate per field, so the other one is silently dropped. Use one predicate per field.
- **Index precedence is fixed, not "most specific wins":**
  - With a Partition, `Eq` on every Partition column always takes the base table. A GSI that
    repeats the partition columns is then never used, and its other columns become non-key
    predicates.
  - Otherwise the **first declared** GSI whose key is fully matched wins. With
    `N1 = CustomerID` and `S2 = CustomerID + Status`, `Eq(CustomerID).Eq(Status)` picks N1.
    Declare the more specific index first.
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
- `Controller.DeleteRecordsAll()` wipes the entity's base and fan-out rows. It is destructive and
  there is no undo. It keeps the slot-versions items.

## 5. Checking your work

- `go test ./...` in `genix-orm/dynamo` runs offline: encoding, marshaling and query planning. To
  assert which index a query picks, follow the plan tests in `query_test.go`.
- `ormcheck/` is a live check against a real table: two entities, every access path, and the
  capacity used. In berryapps: `./deploy.sh 4` (that table is production — pre-alpha, single environment).
- Introspect a schema with `dynamo.GetSchema[OrderTable]()` / `Orders.Schema()` (JSON:
  `partition`, `keys`, every index).

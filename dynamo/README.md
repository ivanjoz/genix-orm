# `db` — a tiny statically-typed DynamoDB ORM

Inspired by the genix ScyllaDB ORM (`genix/backend/db`), trimmed down for the
demo's single-table DynamoDB store. You declare a table as two Go structs and a
`GetSchema()`; the ORM derives the physical keys, runs the queries, and marshals
rows — all with compile-time-checked column references.

## Storage model: keys + one binary blob

Every item contains **only**: the key columns (`pk`, `sk`), the index columns
(`n1`..`n5`/`s1`..`s5`) and a single binary column **`d`** holding the whole
record serialized with [`colbin`](https://github.com/ivanjoz/colbin) (a columnar binary codec).
Nothing else is a top-level attribute.

```
{ pk, sk, n1?..n5?, s1?..s5?, d }
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
        Entity:    "prod",                                    // TableID = HashTableID("prod")
        Partition: dynamo.Keys(t.CategoryID.Size(16)),        // -> pk = TableID ‖ 5 digits
        Sort:      dynamo.Keys(t.Created.Size(48), t.ID),     // -> sk, order-preserving
        Indexes: []dynamo.Index{
            {Slot: dynamo.N1, Keys: dynamo.Keys(t.Price.Size(40))}, // numeric GSI
            {Slot: dynamo.S1, Keys: dynamo.Keys(t.Brand)},          // string GSI
        },
        ArrayIndexes: []dynamo.ArrayIndex{
            {Column: t.TagIDs.Size(32)},                      // Contains(TagIDs, ...)
        },
    }
}
```

Derived attributes per item, for a TableID of `12345678` (plus `d` = colbin blob
of the whole record):

```
pk = 12345678 ‖ CategoryID as 5 digits           (number: 1234567800007)
sk = EncodeOrderedUint(Created, 8) + "#" + ID    (Size(48) = 8 Base64 chars)
n1 = 12345678 ‖ Price as 13 digits               (number)
s1 = "12345678#" + Brand
d  = colbin.Marshal(product)                     (binary)
```

## Array indexes: query a slice by element (`array_index.go`)

DynamoDB can't index inside a list, so an `ArrayIndexes` field is fanned out into
hidden rows, one per distinct element, which the ORM keeps in sync:

```
pk = base pk ‖ the field's cb id, 3 digits       (number: 1234567800007006)
sk = composite(element) + "#" + base sk
d  = the record blob, only with FullCopy: true
```

```go
Products.Query().Eq(Products.T.CategoryID, int32(7)).Contains(Products.T.TagIDs, 3, 9).Exec(&out)
```

- **Declaration:** one `ArrayIndex` per slice field; a table can have several.
  The field's handle is a `ColSlice[T, E]` with `E` the **element** type (as in
  genix-orm/db): `ArrayIndex.Column` and `Contains` accept nothing else, and the
  compile checks `E` against the record field. Elements are integers declaring
  `.Size(bits)`, or strings, and the field must carry a `cb:"N"` tag (1..999):
  that stable id, not the Go name, names its rows.
- **Contains** matches records holding ANY of the values: one Query per value,
  a record matching several comes back once, results come value by value. It
  needs an equality on every Partition column (the rows live under the base pk),
  and base sort predicates narrow it further (`Contains(...).Gte(Created, x)`).
  One Contains per query; the dynamic `QueryRecords` does not accept it.
- **Keys only (default)** — a row is `{pk, sk}`. Contains reads the matching
  base records with a second `BatchGetItem`. A write that leaves the slice alone
  writes no rows.
- **`FullCopy: true`** — each row also carries `d`, so Contains is one Query,
  at the price of rewriting every element row on every write of the record.
- **Sync on write:** `Put`/`PutMany` read the stored version (consistent
  `BatchGetItem`), diff its elements against the new ones and write new rows →
  base item → stale rows. `Delete` reads the stored version and deletes its rows
  after the item. `PutIfAbsent` writes the rows after the conditional put, since
  written first they could index a record that loses the race.
- **Crash / race safety:** that order leaves extra rows, never a missing one,
  and Contains re-checks every record it returns against the element it asked
  for, so an extra keys-only row never becomes a result. A stale FullCopy row
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
        Partition:                  dynamo.Keys(t.ID.Size(48)),
        Sort:                       dynamo.Keys(t.Created.Size(48)),
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

## Using it

```go
var Products = dynamo.NewRepo[ProductTable, Product]()   // compile once, reuse

// writes
Products.Put(&p)
Products.PutMany(list)          // batched (25/req) with unprocessed-item retry
written, err := Products.PutIfAbsent(&p) // false when the key already exists: one conditional PutItem
Products.Delete(&Product{CategoryID: 7, ID: "sku1", Created: 1700000000})

// point read (only key fields needed)
got, err := Products.Get(Product{CategoryID: 7, ID: "sku1", Created: 1700000000})

// top N of one partition (one value per Partition column, in schema order)
top, err := Products.TopN(10, int32(7))

// list an entity regardless of partition (a Query when the entity has no
// Partition, else a Scan over its pk range; admin/debug)
all, err := Products.Scan(10)

// queries — Products.T carries the named, typed columns
var out []Product
err := Products.Query().
    Eq(Products.T.CategoryID, int32(7)).               // -> pk
    Between(Products.T.Created, from, to).             // -> sk range (order-preserving)
    Desc().Limit(50).
    Exec(&out)

// query a GSI: an equality on a full index key routes to that slot
Products.Query().Eq(Products.T.Price, int64(1299)).Exec(&out)   // gsi-n1
Products.Query().Eq(Products.T.Brand, "acme").Exec(&out)        // gsi-s1

// query an array index: records whose TagIDs hold 3 or 9
Products.Query().Eq(Products.T.CategoryID, int32(7)).Contains(Products.T.TagIDs, 3, 9).Exec(&out)
```

### How a query is planned (`query.go`)

1. **Partition source** — a `Contains` targets its array index rows (and needs
   `=` on every `Partition` column). Otherwise, if every `Partition` column has
   an `=`, use the base table (`pk`); otherwise the first GSI whose key columns
   all have `=`. Base wins when both are available.
2. **Sort condition** — predicates on the `Sort` columns become the shared `sk`
   key condition (`=`, `begins_with`, range, `between`). On array rows the
   element leads the sk, so it is a fixed prefix ahead of them.
3. **Leftovers** — predicates on non-key fields (which live inside `d`) are
   evaluated **in memory** against each decoded record.

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
indexes; `ormcheck_product`: no partition, FullCopy array index), reads them back
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
  "partition": [{ "field": "CategoryID", "attr": "CategoryID", "type": "int", "size": 16 }],
  "sort": [
    { "field": "Created", "attr": "Created", "type": "int", "size": 48 },
    { "field": "ID",      "attr": "ID",      "type": "string" }
  ],
  "indexes": [
    { "kind": "primary", "attr": "pk", "isNumber": true, "sharesSortKey": true, "columns": [ /* pk cols */ ] },
    { "kind": "array", "attr": "pk", "isNumber": true, "sharesSortKey": true, "columns": [ /* TagIDs */ ] },
    { "kind": "gsi", "name": "gsi-n1", "attr": "n1", "isNumber": true,  "sharesSortKey": true, "columns": [ /* Price */ ] },
    { "kind": "gsi", "name": "gsi-s1", "attr": "s1", "isNumber": false, "sharesSortKey": true, "columns": [ /* Brand */ ] }
  ]
}
```

Every GSI shares the base table's `sk` as its range key (`sharesSortKey`), so
the top-level `sort` applies to the primary key and every index alike. It's a
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

Materialized/hash/radix views, `int64` packing, index groups, by-IDs slot versions
hooks, and CQL deploy/homologation. DynamoDB's fixed physical schema and string
keys make most of that unnecessary — so this is, as expected, a much smaller
ORM. (Cached metadata, precompiled `xunsafe` accessors, autoincrement sequences
and entity controllers are kept/ported — see above.)

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

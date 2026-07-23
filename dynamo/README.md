# `db` — a tiny statically-typed DynamoDB ORM

Inspired by the genix ScyllaDB ORM (`genix/backend/db`), trimmed down for the
demo's single-table DynamoDB store. You declare a table as two Go structs and a
`GetSchema()`; the ORM derives the physical keys, runs the queries, and marshals
rows — all with compile-time-checked column references.

## Storage model: keys + one binary blob

Every item contains **only**: the key columns (`pk`, `sk`), the index columns
(`n1`/`n2`/`s1`/`s2`/`s3`) and a single binary column **`d`** holding the whole
record serialized with [`colbin`](https://github.com/ivanjoz/colbin) (a columnar binary codec).
Nothing else is a top-level attribute.

```
{ pk, sk, n1?, n2?, s1?, s2?, s3?, d }
```

This keeps the table schemaless — adding a record field never changes the item
shape or needs a migration — and matches genix persisting complex values as a
colbin blob. The trade-off: DynamoDB can't see inside `d`, so a predicate on a
non-key field is applied **in memory after decode** (see the planner below).

## The physical table it targets

One table (`cloud/cloudformation.yml`): base key `pk`+`sk` (both strings) and
five **sparse GSIs** that all share `sk` as their range key:

| Slot | Attribute | Type   |
| ---- | --------- | ------ |
| `N1` | `n1`      | Number |
| `N2` | `n2`      | Number |
| `S1` | `s1`      | String |
| `S2` | `s2`      | String |
| `S3` | `s3`      | String |

## The core idea: order-preserving Base64

A composite key concatenates several columns into one string. Strings go in
verbatim; **numbers are encoded to fixed-width, order-preserving Base64** so that
for equal-length strings, lexicographic comparison equals numeric comparison —
which is what makes `BETWEEN` / `>` / `<` work on a string sort key.

Two things make it work (`encoding.go`):

1. **Fixed width** per column, declared with `.Base(n)` (n Base64 chars = 6·n
   bits) — the DynamoDB analogue of genix's `DecimalSize`.
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
    ID       string
    Category string
    Brand    string
    Price    int64
    Created  int64
}

type ProductTable struct {
    dynamo.Model[ProductTable, Product]
    ID       dynamo.Col[ProductTable, string]
    Category dynamo.Col[ProductTable, string]
    Brand    dynamo.Col[ProductTable, string]
    Price    dynamo.Col[ProductTable, int64]
    Created  dynamo.Col[ProductTable, int64]
}

func (t ProductTable) GetSchema() dynamo.Schema {
    return dynamo.Schema{
        Entity:    "prod",                             // namespaces pk & string slots
        Partition: dynamo.Keys(t.Category),                // -> pk = "prod#coffee"
        Sort:      dynamo.Keys(t.Created.Base(8), t.ID),   // -> sk, order-preserving
        Indexes: []dynamo.Index{
            {Slot: dynamo.N1, Keys: dynamo.Keys(t.Price)},     // numeric GSI (native number)
            {Slot: dynamo.S1, Keys: dynamo.Keys(t.Brand)},     // string GSI
        },
    }
}
```

Derived attributes per item (plus `d` = colbin blob of the whole record):

```
pk = "prod#" + Category
sk = EncodeOrderedUint(Created, 8) + "#" + ID
n1 = Price                        (native DynamoDB number)
s1 = "prod#" + Brand
d  = colbin.Marshal(product)      (binary)
```

Two ready-to-use sample entities (`Product`, `Order`, the latter with a nested
`[]OrderItem` and a composite `s2 = Country + Amount` index) live in
[`../models`](../models/models.go).

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
        Partition:                  dynamo.Keys(t.ID.Base(8)),
        Sort:                       dynamo.Keys(t.Created.Base(8)),
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
  `pk = "seq#<entity>"`, `sk = "seq"`, `cv = <current value>` (native number).
  These live under their own `seq` prefix, so no `Repo.Scan` ever returns them.
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

Requirements: the record/table must declare an integer field named `ID`; padding
is `0..9`. Both are checked at compile time (`NewRepo` panics otherwise).

## Using it

```go
var Products = dynamo.NewRepo[ProductTable, Product]()   // compile once, reuse

// writes
Products.Put(&p)
Products.PutMany(list)          // batched (25/req) with unprocessed-item retry
Products.Delete(&Product{Category: "coffee", ID: "sku1", Created: 1700000000})

// point read (only key fields needed)
got, err := Products.Get(Product{Category: "coffee", ID: "sku1", Created: 1700000000})

// top N of one partition (one value per Partition column, in schema order)
top, err := Products.TopN(10, "coffee")

// list an entity regardless of partition (base-table Scan; admin/debug)
all, err := Products.Scan(10)

// queries — Products.T carries the named, typed columns
var out []Product
err := Products.Query().
    Eq(Products.T.Category, "coffee").                 // -> pk
    Between(Products.T.Created, from, to).             // -> sk range (order-preserving)
    Desc().Limit(50).
    Exec(&out)

// query a GSI: an equality on a full index key routes to that slot
Products.Query().Eq(Products.T.Price, int64(1299)).Exec(&out)   // gsi-n1
Products.Query().Eq(Products.T.Brand, "acme").Exec(&out)        // gsi-s1
```

### How a query is planned (`query.go`)

1. **Partition source** — if every `Partition` column has an `=`, use the base
   table (`pk`); otherwise the first GSI whose key columns all have `=`. Base
   wins when both are available.
2. **Sort condition** — predicates on the `Sort` columns become the shared `sk`
   key condition (`=`, `begins_with`, range, `between`).
3. **Leftovers** — predicates on non-key fields (which live inside `d`) are
   evaluated **in memory** against each decoded record.

> Because the physical table shares one `sk` across the base table and all GSIs,
> the sort/range dimension is uniform. `>`/`<` are exact when the ranged column
> is the last `Sort` column and behave as `>=`/`<=` when a composite suffix
> follows.

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
  "tableName": "demo-app",      // physical DynamoDB table
  "partition": [{ "field": "Category", "attr": "Category", "type": "string" }],
  "sort": [
    { "field": "Created", "attr": "Created", "type": "int", "base": 8 },
    { "field": "ID",      "attr": "ID",      "type": "string" }
  ],
  "indexes": [
    { "kind": "primary", "attr": "pk", "sharesSortKey": true, "columns": [ /* pk cols */ ] },
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
    Entity() string             // entity namespace within the shared table
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

`DeleteRecordsAll` scans the entity's `pk` namespace projecting only the key
attributes (it never decodes `d`), then deletes in `BatchWriteItem` batches of 25
with the same unprocessed-item retry as `PutMany`. It is scoped to the entity's
prefix, so sibling entities and the internal sequence counters are untouched.
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

Materialized/hash/radix views, `int64` packing, index groups, cache-version
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

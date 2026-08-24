# Binary Size — Root-Cause Findings

Status: **investigation complete, nothing shipped.** The executable work is in
`COL_INSTANTIATION_PLAN.md`.

This document records what was measured while testing whether Go 1.27 generic methods could
simplify the `Col[Table, Value]` declaration pair. The answer is no, but the investigation
found that **the project's earlier binary-size analysis attributed the cost to the wrong
mechanism**.
Three hypotheses were tested and two were killed by measurement; they are recorded here so
nobody pays for them twice.

Environment: Go 1.27.0. All binaries built with the production target
`GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -ldflags '-s -w'`, in a scratch copy of
`backend/`. Baseline here is **44,957,856**, not the 49,152,160 the earlier analysis
recorded — the tree advanced in between (notably the `avif` build-tag commit). Compare deltas,
not absolutes.

---

## 1. The cost model

Every `Col` symbol in the binary is a product of two independent factors:

```text
db.Col code = (number of distinct Col types) x (methods emitted per type)
            =            258                 x            ~32            = 8,256 symbols / 1,841,856 bytes
```

258 is the number of distinct `(table, column value type)` instantiations. 22 distinct exported
method names are retained per instantiation; the symbol count per instantiation is higher (~32)
because value- and pointer-receiver forms plus compiler-generated wrappers are counted separately.

- **Factor 1** is controlled by **gcshape sharing**. `Col[ExpenseTable, int32]` and
  `Col[AssetTable, int32]` are separate machine code because `ExpenseTable` and `AssetTable` are
  different structs. A *pointer* type argument unifies to `go.shape.*uint8`, collapsing all
  tables into one.
- **Factor 2** is controlled by the **linker's dead-method elimination**, which is a
  program-global on/off switch (§3).

The two factors are multiplicative, and each is worthless while the other dominates. That is the
single most important thing in this document.

---

## 2. Go 1.27 generic methods do not help (confirms §5.3, different reason)

Moving the table parameter off `Col` and onto its methods is not expressible in the shape the
query API needs:

```go
func (c *Col1[E]) Equals[T any](v E) *T { ... }
// ./main.go:16:20: in call to t.CompanyID.Equals, cannot infer T
```

`T` appears only in return position, so it cannot be inferred. A struct field cannot reference a
method type parameter either (`schemaStruct *T` -> `undefined: T`). Making `T` inferable by
passing the table as an argument compiles, but emits **the same stencil count and a larger
binary** — 54 distinct tables, 6 predicates:

| Scheme | Binary | Stencils |
| --- | --- | --- |
| `Col[T, E]` (today) | 4,544,977 | 2,011 |
| `Col[E]` + `Eq[T](tbl *T, v E) *T` | 4,964,472 | 2,011 |
| `Col[E]` + `Eq[T](tbl *T, v any) *T` | 5,008,354 | 2,011 |
| `Col[E]` + `Eq[P](tbl P, v E) P` | 2,404,820 | 43 |
| **`Col[*T, E]`** | **2,371,134** | **43** |
| `Col[*T, E]` + non-empty constraint | 2,400,208 | 97 |

The last two rows are the finding: what matters is not *where* the type parameter is declared but
whether its argument is a **pointer**. Generic methods are irrelevant; `Col[*T, E]` needs no new
language feature and no call-site change.

Shape unification survives a non-empty method-set constraint (constraint methods arrive through
the dictionary), so `Col[P TableInterface[P], E any]` would have worked in principle — but see
§5 for why the constraint has to go anyway.

---

## 3. Root cause: dead-method elimination is globally disabled

The earlier analysis attributed the duplication to the type parameters. It is actually the
linker. From `cmd/link/internal/ld/deadcode.go`:

```go
reflectSeen bool  // whether we have seen a reflect method call
...
d.reflectSeen = d.reflectSeen || d.ldr.IsReflectMethod(symIdx)
...
if (d.reflectSeen && (m.isExported() || d.dynlink)) || d.ifaceMethod[m.m] || … {
	// keep the method
}
```

`reflectSeen` is **one boolean for the whole program**. `MethodByName(name)` takes the method name
as a runtime string, so the linker cannot know what will be looked up and must conservatively
retain **every exported method of every reachable type**. One reachable dynamic `MethodByName`
call anywhere flips it and the entire binary pays. The ORM is not targeted; it is simply the
largest payer, having by far the most (types x exported methods).

Measured on real ORM types — a two-table program, identical but for one `MethodByName` call:

| | Binary | Concrete `Col` syms |
| --- | --- | --- |
| No `MethodByName` | 12,463,615 | 66 |
| With `MethodByName` | 17,619,649 | 416 |
| **Delta** | **+5,156,034 (+41%)** | **6.3x** |

Note the retention condition is `reflectSeen && m.isExported()`. **Unexported methods are never
retained by this rule** — which is the lever §6 exploits.

### 3.1 Three independent sources, and it is boolean

Any one of these keeps the flag set, so partial removal buys nothing:

| Source | Chain | Cost to remove |
| --- | --- | --- |
| `aws-lambda-go/lambda` | -> `net/rpc` | **Free.** `-tags lambda.norpc` is AWS's own flag; it drops the legacy local-RPC entry path, not the Lambda runtime. Measured −65,536. |
| `gocql` | `recreate.go` -> `text/template` | **Free of features.** `recreate.go` holds package-level `template.Must(...)` vars and only provides `KeyspaceMetadata.ToCQL()`, a scylla-manager helper. Neither `ToCQL` nor `KeyspaceMetadata` is referenced anywhere in this repo. Needs a fork of the existing gocql replace. Measured −65,536. |
| `qdrant/go-client` | -> `grpc` -> `x/net/trace` -> `html/template` | **Not a flag.** Qdrant is a live feature. This is an architecture question — whether the agent/RAG code ships in the same binary as the API — not something a build tag can resolve. Left as a TODO. |

---

## 4. Two hypotheses killed by measurement

Both were plausible, both produced zero, and both are recorded so they are not retried.

**H1 — "the itab from `InitStructTable` forces it."** `InitStructTable` reached every column
through `fieldAddr.Interface().(ColGetInfoPointer)`. Replaced with binding through a shared
non-generic layout (`colCore` + schema pointer, identical for every instantiation) via
`unsafe.Pointer`. Result: **binary byte-identical, 44,564,640 -> 44,564,640**, concrete `Col`
symbols unchanged at 8,256. Would have added `unsafe` pointer arithmetic to the schema binder for
nothing.

**H2 — "the `Coln` boxing in `GetSchema()` forces it."** Schema declarations box a `Col` into an
interface 13 ways (`TableSchema.Keys/Partition/...`, `Index.Keys/Cols/Partition`,
`FixedValues.Col`). Introduced a non-generic `colRef` wrapper so declarations box one shared type
instead of 258 generic ones, and applied it to the real `Expense` and `Asset` schemas. Result:
**slightly worse** — 160 -> 170 concrete symbols per table, binary 17,619,649 -> 17,649,864. The
extra 10 is the new accessor method, itself emitted per instantiation.

H2 failed because it changed only what got *boxed*, while leaving all 22 methods on `Col`. The
linker counts the **method set**, not the boxing.

### 4.1 The isolating experiment

54 tables, 10 predicates, ORM in a separate package, nothing boxing `Col` anywhere:

| Variant | `MethodByName` reachable? | Reflect binding? | Binary | Concrete | Shape |
| --- | --- | --- | --- | --- | --- |
| x | yes | yes | 4,713,068 | 3,350 | 87 |
| y | yes | **none at all** | 4,695,924 | 3,350 | 87 |
| z | **no** | none | **2,720,408** | **0** | 87 |

Removing reflection entirely changed nothing (x -> y). Removing `MethodByName` reachability
changed everything (y -> z: **−1,975,516, −42%**). No ORM-side change avoids Factor 2.

### 4.2 What `Col[*T, E]` is worth once Factor 2 is fixed

Same program, pruning on, only the table type argument differing:

| | Binary | Shape stencils |
| --- | --- | --- |
| `Col[Table0, int32]` | 5,916,339 | 3,367 |
| `Col[*Table0, int32]` | **2,720,408** | **87** |

**−3,195,931 (−54%).** Synthetic, so treat the ratio as indicative. This is why the real-tree
measurement of `Col[*T, E]` was only −393,216: with pruning off, concrete full method sets
dominate and they exist per `(table, column type)` **regardless of shape**.

---

## 5. `Col[*T, E]` — measured, and one blocker

| Step | Binary | Delta |
| --- | --- | --- |
| Baseline | 44,957,856 | — |
| `Col[*T, E]` | 44,564,640 | **−393,216 (−0.87%)** |
| + declaration-time modifiers return `Coln` (§6.2) | 44,171,424 | **−393,216 (−0.87%)** |
| + H1 de-reflection | no change | 0 |
| + `-tags lambda.norpc` | 44,499,104 | −65,536 |
| + gocql fork without `recreate.go` | 44,433,568 | −65,536 |

Scope: 737 `db.Col`/`db.ColSlice` declarations across 46 files, plus `column.go` and the two
aliases in `backend/db/db.go`. **Zero query call sites change** — `Equals` returns `T`, and `T`
*is* `*ExpenseTable`, so `eq.CompanyID.Equals(x).ID.Equals(y)` still chains. `GetSchema()` bodies
are untouched. `go build ./...` and `go vet ./...` clean; `accounting`, `libs`, `finance`, `exec`
tests pass.

**Blocker:** the `TableInterface[T]` constraint on `Col` must be dropped. `*ExpenseTable` cannot
satisfy `TableInterface[*ExpenseTable]`, because `GetTableStruct() T` is inherited from the
embedded `TableStruct` and returns `ExpenseTable` **by value**. This is safe: nothing inside
`Col` or `ColSlice` calls a constraint method.

Also outstanding: genix-orm's own internal tables (`scylla.IncrementTable` and friends) were not
in the sweep and still show a non-pointer struct shape.

---

## 6. Where the ORM bytes actually are

Concrete generic instantiations in the current binary — 12,820 symbols, **2,445,008 bytes of
text** plus **1,592,645 bytes of symbol names** in pclntab:

| Bucket | Syms | Text bytes |
| --- | --- | --- |
| `db.Col` | 8,256 | 1,841,856 |
| `db.TableStruct` | 2,052 | 315,424 |
| `scylla.Exec` | 1,512 | 178,848 |
| `scylla.ScyllaController` | 848 | 87,344 |
| `db.ColSlice` | 152 | 21,536 |

### 6.1 `db.Col` by method group — the actionable split

All 22 retained methods are exported, each appearing 258 times:

| Group | Methods | Syms | Bytes | Share |
| --- | --- | --- | --- | --- |
| **Schema-declaration modifiers** | `DecimalSize` `Int32` `IsWeek` `CompositeBucketing` `Autoincrement` `Sum` `Avg` `Max` | 4,128 | **1,178,032** | **64.0%** |
| Query predicates | `Equals` `GreaterThan` `GreaterEqual` `LessThan` `LessEqual` `In` `Between` `Contains` `Exclude` | 2,322 | 407,424 | 22.1% |
| Interface surface | `GetInfo` `GetName` `GetInfoPointer` `SetSchemaStruct` `SetTableInfo` | 1,806 | 256,400 | 13.9% |

**Eight methods are 64% of `db.Col`.** They are the *largest* despite being the simplest, because
each returns `Col[T, E]` **by value** — so every instantiation carries a full struct copy, and
`ColumnInfo` is large (six function fields plus an embedded `ColType`). An earlier pass already
shrank their *bodies* to shims over `colCore` and recovered only −65,536: the body was never the
cost, the copy is.

These eight are only ever called inside a `GetSchema()` body, and — unlike everything else in this
document — they can be made cheaper **today, with `reflectSeen` left exactly as it is**. See §6.2
for what was measured and §6.3 for the option that was rejected.

The interface-surface group is retained via `ifaceMethod` regardless of `reflectSeen` and cannot
be removed while `Coln` and `ColGetInfoPointer` exist. The predicates need `E` and `T` and must
stay generic.

### 6.2 The return type is the cost, not the body — measured

Each modifier returns `Col[T, E]` **by value**, so every one of the 258 instantiations carries a
full struct copy. Changing the return type to `Coln` (a non-generic `colRef`) and leaving the
methods where they are:

| Group | Syms | Bytes before | Bytes after | Delta |
| --- | --- | --- | --- | --- |
| modifiers | 4,128 -> 4,128 | 1,178,032 | **765,184** | **−412,848** |
| predicates | 2,322 | 407,424 | 407,424 | 0 |
| interface | 1,806 | 256,400 | 256,400 | 0 |

Binary 44,564,640 -> 44,171,424. Symbol count is unchanged — the methods remain on `Col` and are
still retained — but modifiers drop from 285 to **185 bytes/symbol**, in line with the predicates'
175. That convergence is the evidence that the struct copy was the entire overhead.

This is also why the earlier shim pass recovered so little (−65,536): it shrank the modifier
*bodies* over `colCore`, and the bodies were never the cost.

Zero call sites change, because no modifier is ever chained and no modifier result is ever assigned
to a variable — all 54 sites pass it straight into a `Coln` position (`db.Cols(...)`,
`FixedValues{Col: ...}`, `GroupBy(...)`).

### 6.3 Rejected: moving the modifiers off `Col`

Relocating the eight onto an exported non-generic type would remove them from `Col`'s method set
entirely and recover the full 1,178,032 instead of 412,848. Rejected: it rewrites 54 declaration
sites and changes how the ORM is used. The extra ~765 KB does not justify an API change.

---

## 7. Corrections to the earlier analysis

The project's previous binary-size document has been removed; its conclusions are corrected here so
the reasoning is not repeated. Phases 0–3 of that effort shipped and account for the
54,263,970 → 49,152,160 already banked; `scylla/RATIONALE.md` records what was changed.

- The earlier **§5.1** rejected `Col[E]` on the grounds of a 480-call-site rewrite. The rewrite is
  unnecessary: `Col[*T, E]` gets the shape collapse with zero call-site changes. But the section's
  conclusion — that this is not where the win is — happens to be right, for the reason in §3, not
  the one it gives.
- The earlier **§5.3** said generic methods are "a namespacing feature" and that stencil count is driven by
  distinct type arguments. Correct as far as it goes, and the empirical result stands. What it
  misses is that stencil count is not the dominant term at all while `reflectSeen` is set.
- The earlier **§2.2** ("`T` is the multiplier", "~130 KB of binary per table") measures a real correlation but
  names the wrong cause. The per-table scaling comes from 258 instantiations each retaining a full
  exported method set, not from gcshape duplication.

---

## 8. Reproducing

```sh
# production target
cd backend && GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -ldflags '-s -w' -o /tmp/main .

# stencil inventory (needs an unstripped build)
go build -o /tmp/uns . && go tool nm -size /tmp/uns > /tmp/syms.txt
rg 'db\.\(?\*?Col\[' /tmp/syms.txt | rg -v 'go\.shape' | wc -l   # concrete instantiations
rg 'db\.\(?\*?Col\[' /tmp/syms.txt | rg -c 'go\.shape'           # shape stencils

# is dead-method elimination off?
go tool nm /tmp/uns | rg 'reflect\.Value\.MethodByName'

# who flips it
go list -deps . | rg -x 'text/template|net/rpc|google.golang.org/grpc'
go mod why google.golang.org/grpc
```

# DataFrame generation: benchmark folder and a 10× plan

Goal: make building a DataFrame partition in memory (records → lines → aggregated, encoded
files) about **10× faster**, measured on the Lambda that runs it, without changing a byte of the
files it writes. Scylla reads and S3 writes are out of scope here (see "Out of scope").

## 1. Where it stands (2026-10-27)

genix's only company with sales: 1,773 sale orders → 9,797 lines → 14 files (one per day), 13 KB.
Laptop, in memory, median of 21 (temporary exec, since deleted):

| volume | sale orders | ToFrames + LinesOf | Histories | Rebuild (sum + encode + slots) | total |
|---|---:|---:|---:|---:|---:|
| ×1 | 1,773 | 0.86 ms | 0.08 ms | 3.16 ms | **4.1 ms** |
| ×10 | 17,730 | 8.37 ms | 1.20 ms | 13.13 ms | **22.7 ms** |
| ×100 | 177,300 | 75.7 ms | 7.7 ms | 120.4 ms | **203.7 ms** |

A synthetic sale of the same shape (1–10 lines, 400 products), `GOMAXPROCS=1`:

| case | LinesOf | Rebuild |
|---|---|---|
| 1,773 sales, 14 days | 0.66 ms, 7,092 allocs | 3.1 ms, 26,516 allocs |
| 17,730 sales, 14 days | 7.4 ms, 70,920 allocs | 16.5 ms, 203,591 allocs |
| 17,730 sales, 365 days | 7.8 ms | **54 ms**, 45.9 MB, 320,438 allocs |

**The Lambda is the multiplier.** The backend function is arm64 at `MemorySize: 256`
(`cloud/template.yml`); Lambda gives a full vCPU at 1,769 MB, so it runs on ~15% of one. The
54 ms year case is several hundred ms there, and its 46 MB of garbage is a large share of a 256 MB
heap. More goroutines don't help CPU-bound work on a fraction of a vCPU: the win has to come from
doing less.

### Where the time goes (CPU profile, all cases)

| share | what | code |
|---:|---|---|
| ~42% | `ToFrames` + the projection copy: 4 allocations per record (the method's `[]F`, the line backing, `[]Line`, `RecordLines`), a `switch` on the field kind per value | `projection.go`, `table.LinesOf` |
| ~15% | `frameRows.add`: a Go map keyed by `[3]int64`, a `[]int64` allocated per new row | `read.go` |
| ~11% | `fileKeyOf` **per line**: string concatenation plus `strconv` to build the file's S3 key, then a string-keyed map lookup | `table.go`, `run.go` `sumAt` |
| ~14% | `fileAt`: collecting and sorting the map's keys per file | `read.go` |
| ~20% | garbage collection (`tryDeferToSpanScan`, `scanObject`): the allocations above | — |
| **~3%** | `EncodeFile`, the columnar codec | `codec.go` |

The file codec, which is what colbin optimized, is not the bottleneck: the grouping in front of it
is. colbin's lessons transfer (allocations before instructions, no maps or reflection on the hot
path, one columnar pass, measure A/B), its kernels mostly don't.

## 2. The benchmark folder

```text
genix-orm/dataframe/bench/
  README.md          how to run, how to A/B, how to read the numbers
  corpus.go          deterministic generators (seeded), no database
  shapes.go          the frames under test, declared like genix's and berryapps'
  golden_test.go     the encoded bytes of every case hashed and pinned
  bench_test.go      one Benchmark per phase × case
  ab.sh              A/B against another commit through a git worktree
  lambda/            a main that runs the benchmarks inside a Lambda and prints them
```

A package of its own (`dataframebench`) inside the `dataframe` module, driving only the public API
(`Compile`, `ProjectionOf`, `Table.LinesOf`, `NewHistoryOfChanges`, `Rebuild`, `Compact`,
`ReadChanged`) through an in-memory `Backend` and `utils.NewMemoryFileStore`. Micro-benchmarks of
unexported functions stay as `_test.go` files in `dataframe` itself.

### Corpus (`corpus.go`)

Seeded generators, so every run and every commit sees the same records:

| case | records | lines/record | Keys values (files) | Rows values | why |
|---|---:|---|---:|---:|---|
| `sales-today` | 1,773 | 1–10 | 14 days | 400 products | genix's real company now |
| `sales-month` | 18,000 | 1–10 | 31 | 1,000 | a busy store |
| `sales-year` | 18,000 | 1–10 | 365 | 1,000 | a full year: many small files |
| `sales-big` | 200,000 | 1–20 | 365 | 5,000 | the ceiling we design for |
| `enrollment` | 50,000 | 1 | 3 years × 20 programs | 2,000 employees | a column frame (berryapps' shape), nested Rows |

A sale record mirrors `SaleOrder`'s parallel-array lines and `ToSaleSummaryFrames`' frame struct
(1 key, 2 rows, 6 int32 sums, `UseZeroSumsMask`), including annulled and pending statuses. A
`sales-*-changes` variant adds logged changes to 5% of the records, for the incremental run and
fresh reads.

### Benchmarks (`bench_test.go`), per case

1. `LinesOf`: record → lines, the whole corpus.
2. `Rebuild`: what a build does in memory (sum at the target, `fileAt`, encode, slots, commit).
3. `Compact`: the incremental run with 100 and 1,000 changed records on a built partition.
4. `ReadChanged`: a fresh read replaying 100 changes over a client holding every file.

Each one reports `ns/op`, `B/op`, `allocs/op` and a custom `ns/line` metric
(`b.ReportMetric`), so cases of different sizes compare.

### Rules (taken from colbin's PERFORMANCE.md)

- **Bytes identical.** `golden_test.go` hashes every file of every case; an optimization that
  changes one hash is a format change and needs its own decision.
- **A/B, never a stale baseline.** `ab.sh <commit>` builds the old commit in a `git worktree` and
  runs both builds in alternating rounds, `GOMAXPROCS=1`, `-count 10`, compared with `benchstat`.
  The laptop drifts ±5–9% between runs, the size of many changes.
- **Allocations before instructions.** The GC's share does not show under the code that caused it.
- **Decide on the corpus, confirm on Lambda.** `lambda/` is deployed as a temporary function at
  arm64 256 MB (what production runs) and 1,769 MB, invoked a few times, and deleted.
- Check inlining (`go build -gcflags=-m=2`) after touching a hot helper.

## 3. Optimizations, in order of expected gain

Each lands as its own commit with its A/B numbers in `dataframe/RATIONALE.md`.

### A. Aggregate in one columnar pass instead of maps (~45% of the time)

Today every line builds its file's S3 key string, looks it up in a string map, then looks its row
up in a `map[[3]int64][]int64` that allocates per row, and `fileAt` sorts each map again.

Instead: append every line's values into one flat `[]int64` arena, sort an index of the lines by
(Keys, Rows) — a radix sort on the packed key, since Keys and Rows are small non-negative ints — and
reduce runs of equal (Keys, Rows) into rows. The rows come out sorted, so `fileAt` only slices them,
and a file's key string is built once per file. No map, no per-row allocation.
Expected: `sumAt` + `fileAt` + `fileKeyOf` from ~40% to a few percent.

### B. Lines without per-record allocations (~42%)

- `Table.AppendLines(dst []int64, record)`: the projection writes straight into the caller's
  arena, which removes the line backing, `[]Line` and `RecordLines` (3 of the 4 allocations per
  record).
- Compile the field readers at `ProjectionOf`: the frame struct's fields are usually same-kind runs
  (sale-summary: six int32 sums in a row), read by one loop per run instead of a `switch` per
  value.
- The 4th allocation is the `[]F` the ToFrames method returns. Removing it changes the method's
  signature to `func(*R, []F) []F`, appending into a reused buffer. **That is an API change of
  every ToFrames method: yours to decide.**

### C. A rebuild without Histories for records the log doesn't touch

A rebuild wraps every record in a `History` to ask it what it held at the target, but a record
with no logged change after the target holds its lines now. Feed those lines straight into A's
arena; build Histories only for the records the log names (a handful).

### D. Pool the arenas

`sync.Pool` for the line arena, the sort index and the encode buffer, released by size the way
colbin's `74f2c8d` does, so a 256 MB Lambda does not allocate them every compaction.

### E. Measured, expected small, left for last

- `EncodeFile` (3%): reuse one buffer across files; colbin's NEON/AVX kernels are not worth it at
  this size.
- Parallel aggregation per file: useless on 15% of a vCPU; only if the function moves to ≥1,769 MB.

### Target

| case | today (laptop) | target |
|---|---:|---:|
| `sales-today` LinesOf + Rebuild | 3.8 ms, ~33k allocs | ≤ 0.4 ms, < 100 allocs |
| `sales-year` LinesOf + Rebuild | 62 ms, 46 MB | ≤ 6 ms, < 5 MB |
| `sales-big` | to measure | linear in lines, ≤ 40 ns/line |

## 4. Out of scope, but bigger on a real rebuild

Reading the 1,773 orders from Scylla took ~790 ms, ~200× the in-memory generation.
`scanHistories` reads every row with `MapScan` into a `map[string]any` and then sets each field by
reflection (`dataFrameInfo.newRecord`). A typed scan straight into the record struct is likely the
largest single win on a real rebuild; it belongs to its own plan.

## 5. Decisions for you

1. **ToFrames signature** (`func(*R, []F) []F`, B): worth the API change for the last allocation?
2. **Lambda memory**: 256 MB is ~15% of a vCPU. Is raising the backend function (or sending
   rebuilds to `backend_2`, 2,048 MB) on the table, or must the code alone reach the target?

## 6. Order of work

1. The `bench/` folder: corpus, golden hashes, benchmarks, `ab.sh`. Record the baseline.
2. The Lambda runner; record the baseline at 256 MB and 1,769 MB.
3. A (columnar aggregation), A/B.
4. B (lines into an arena, compiled readers), A/B.
5. C (no Histories on a rebuild), A/B.
6. D (pools), A/B.
7. Re-run on Lambda; write the numbers into `dataframe/RATIONALE.md`, and this plan's results
   section.

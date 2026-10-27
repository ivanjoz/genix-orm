# DataFrame benchmarks (branch `benchmarks` only)

This folder lives only on the `benchmarks` branch, which is permanent and never merged into `main`:
the corpus, the benchmarks and whatever they depend on stay out of `main`. Everything the branch
adds is in this folder.

It measures building a DataFrame partition in memory — records → lines → Histories → aggregated,
encoded files — through `dataframe`'s public API, on three shapes of 100,000 records each
(`shapes.go`, `corpus.go`):

| case | declared by | Keys | Rows | Sums | lines | files |
|---|---|---|---|---|---:|---:|
| `sales` | genix's `SaleOrder.ToSaleSummaryFrames` | day | product, sub-divisor | 6 int32, zero sums mask | ~523k | 365 |
| `enrollment` | columns (scylla's `columnLinesOf`) | year, program | department, team, employee | 2 int + 1 float, count | ~97k | 120 |
| `movements` | `ToFrames` with floats | warehouse, week | product, lot | 3 float64 + 1 int64, count, mask | ~636k | 1,040 |

## Running

```sh
./link.sh [checkout]   # ./orm → the genix-orm checkout under test (default: .., this branch)
./bench.sh             # golden check, then every benchmark at GOMAXPROCS=1, -count 6
./bench.sh 'Build/sales' -count 10
./ab.sh main           # A/B: a worktree of <ref> (A) against ./orm (B), alternating rounds, benchstat
```

`go.mod` reaches `dataframe` and `utils` through the `orm` symlink, so the same benchmarks run against
any checkout. `testdata/golden.txt` pins the bytes of every file a build at t0 and a compaction at t1
write: an optimization that moves a hash changes the format. `go test -run Golden -update` rewrites
it, only for a format change that was decided.

## Phases

- `LinesOf`: record → lines (`Table.LinesOf`).
- `Histories`: lines + logged changes → `History` (`NewHistoryOfChanges`).
- `Rebuild`: the engine alone, from precomputed Histories to the stored files.
- `Build`: a real rebuild in memory, records → files, page by page. **The number to bring down.**
- `Compact`: the run at t1 over a partition built at t0, ~1,200 records written in between.

Each reports `ns/op`, `B/op`, `allocs/op` and `ns/line`.

## Working on `main` from here

An optimization touches only folders that ship (`dataframe/`, `utils/`), never this one. It is
committed on this branch, cherry-picked onto `main`, and `main` merged back here:

```sh
git commit -- dataframe/ ...            # on benchmarks
git switch main && git cherry-pick <c>  # main gets the code, not the benchmarks
git switch benchmarks && git merge main
```

Never merge `benchmarks` into `main`. Results of each step are in `results/`.

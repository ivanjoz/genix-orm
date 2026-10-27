# RATIONALE — dataframe

Design decisions, newest first.

## A ToFrames frame is filtered by the record column of the key's name; its rows come back without a key
**Context** — A ToFrames frame's columns are the frame struct's fields, not the record's. scylla
filters name the record column (`date`), while the frame's key is the struct field `Date`, and a
typed `FrameRow.Key` record cannot hold a frame struct's values.
**Decision** — scylla maps each filter to the record column's Go field name before
`FrameSelection` (dynamo already filters by field name), so `Between(table.Date, …)` selects the
key `Date`. `FrameRow.Key` stays empty on a ToFrames frame; its rows carry only `Sums`. The typed
read (`FrameRowsAs`) planned for it was dropped: the only consumer reads raw files (`ExecChanged`).
**Rationale** — No new filter API. A frame key with no record column of the same name cannot be
filtered from the typed query; a typed row reader can be added when a server-side reader needs it.

## Column frames compile into one-line LinesOf; Count is allowed on a ToFrames frame
**Context** — Two declaration ways (columns, ToFrames) had to share one engine.
**Decision** — The backend turns a column frame into a `LinesOf` that gives one line, none when
`Status = 0`; a ToFrames frame's `LinesOf` comes from its `Projection`. `Count` adds 1 per line in
both.
**Rationale** — One code path in the engine. On a ToFrames frame Count counts lines, not records.

## Changelog frames are tagged by 16 bits of their shape and carry their length
**Context** — The changelog names lines per frame; frames are added, removed or redeclared
between deploys while old entries are still inside the ~36 h window.
**Decision** — Each frame in an entry is `u16 shapeTag` (CRC32 of `Frame.Shape`, truncated) plus a
payload length. A tag no frame has is skipped by its length. Compile refuses two frames of a table
with the same tag.
**Rationale** — A redeclared frame changes its shape and is rebuilt, so its old lines are never
needed; the other frames' entries stay readable. Costs 3+ bytes per changed frame per entry.

## The zero sums mask flag is bit 7 of the format byte; a constant column is divisor 0
**Context** — The plan proposed a `0xFF` marker for constant columns and left the mask flag's place
open.
**Decision** — Format 4: bit 7 of the format byte says the file carries the mask, so a frame
without `UseZeroSumsMask` pays nothing. A column whose residuals are all zero is written with
divisor 0 (never a real divisor) and no blocks.
**Rationale** — Divisor 0 reuses a byte every column already has, so `0xFF` would have added
nothing. The flag in the format byte keeps the header the same length.

## The scylla stored-row read includes the key columns
**Context** — The write path now rebuilds a whole record from the stored row to run its ToFrames
method, which may read the key.
**Decision** — `readStoredRows` keeps every column it reads in the stored map, the key included.
**Rationale** — A method reading the ID would otherwise see 0 on the stored side and log a false
change. The map grows by one value per record.

## The partition comes before the generation in the file path; the layout version moved to v7
**Context** — State and lock are now per partition, so each partition has its own generation and a
build cleans up only its partition. Files already stored follow the old global layout.
**Decision** — `dtf/<table id>/<frame>/<partition>/<generation>/<keys>`, state at
`dtf/<table id>/<frame>/<partition>.state`. `shapeOf` says `v7`, so every stored frame reads as
another shape and is rebuilt on its first read (dynamo too, whose state item still names the old
generation). A partition rebuild is a full build of the partition in a new generation; the old
"rewrite in the current generation, keep w" partition rebuild is gone.
**Rationale** — The cleanup is one listing of the partition's folder. The old files and the old
`<frame>.state` are left orphaned in the store: nothing reads them, and they must be deleted once by
hand.

## A read compacts by CompactOn only on changes before the target; a needed build is not optional
**Context** — A compaction moves w to now − 2 min (TargetLag), so the writes of the last two minutes
stay in the log after it.
**Decision** — `Records` counts the records with an insert or change in (w, target]; `Seconds` is
measured from w to the target, so right after a compaction neither is due. A partition that needs a
build (not built, other shape, w out of the log's reach) is built by the read whatever `CompactOn`
says, and a failed build fails the read. A failed CompactOn compaction is only `log.Printf`'d and
the read replays the log. A plain (not fresh) read reads no log, so only `Seconds` triggers it.
**Rationale** — Counting every history since w would make each read compact again while 100
records were written in the last two minutes. The log reaches back only ~36 h, so a partition no
one compacted would otherwise read wrong.

## Locks lease and wait 40 s, timed from the grant
**Context** — A read that finds a partition due queues behind the compaction under way; fareward
clamps leases to 60 s.
**Decision** — `lockLease = lockWait = 40s`, writes stop 5 s before the lease ends, and that
deadline is counted from when the lock is granted, not from the read's `now` (which can be up to
40 s earlier). dynamo's conditional-write lock ignores `wait` and refuses at once.
**Rationale** — A compaction of one partition is expected to take seconds. The cost: a dynamo
rebuild of a large table (berryapps) that takes longer than 35 s now stops writing and fails, where
the old 5-minute lock let it finish.

## A rebuild sums its lines by sorting them, not in per-file maps
**Context** — A rebuild built each line's file key string, looked it up in a map of files, then its
row in a `map[rowKey][]int64` that allocated per row, and sorted each file's map keys to write it.
On 100k records (benchmarks branch) most lines name a row no other line does: 523k lines → 305k
rows (sales), 636k → 605k (stock movements), so the maps merged little and cost ~45% of a rebuild,
plus the garbage the GC scanned.
**Decision** — `lineSums` (aggregate.go) copies every line into chunked arenas, packs each line's
Keys and Rows into a uint64 (each column less its minimum, in the bits its range needs), radix sorts
once and sums runs of one group into rows, which come out ascending, file after file; the file key is
built once per file. Groups wider than 64 bits fall back to a comparison sort. Files are produced
ascending by Keys, so a rebuild hands slots out in that order instead of map order.
**Rationale** — Bytes identical (golden hashes). A/B at GOMAXPROCS=1, 100k records: Rebuild
sales 326 → 89 ms, enrollment 58 → 15 ms, movements 559 → 185 ms; allocations −98%, bytes −40%. The
arena holds every line until the files are built (sales: 37 MB), about what the maps held.

## The encoder reuses one residuals buffer per file and scores widths up to the widest
**Context** — After the rebuild's sorting, encoding was a quarter of a rebuild: every column
allocated its residuals, the Rows levels grew by append, and `blockCost` walked all 64 bit lengths
for each candidate width (~2,000 steps per block of 128 values).
**Decision** — `codecOptions.scratch` holds one column's residuals at a time, set once by
`appendFile`; the levels of `appendRowColumns` are sized once; `blockCost` and the patch count stop
at the block's widest residual (no residual is longer, so the costs are the same).
**Rationale** — Bytes identical. Rebuild of 100k records: movements 185 → 135 ms, sales 89 → 77 ms;
allocations of a rebuild halved.

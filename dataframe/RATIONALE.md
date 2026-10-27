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

## The partition comes after the generation in the file path
**Context** — Frames now follow the table's partition. A generation is built for every partition at
once, and a build deletes every other generation by listing the frame's folder.
**Decision** — `dtf/<table id>/<frame>/<generation>/<partition>/<keys>`. One state, one lock and
one run per frame cover every partition. The versions file is per (table, partition),
`dtf/<table id>/<partition>.versions`, and the state file `dtf/<table id>/<frame>.state`, both
beside the frames' folders with a suffix, never inside one.
**Rationale** — The build's cleanup stays one listing, and a run reads one log for all partitions.
A rebuild of a frame rebuilds every partition; `RebuildPartition` is the per-partition fix.

## RebuildPartition rewrites a file left with no rows instead of deleting it
**Context** — A partition rebuild writes its files at a target past w. A later run that moves a
record out of a file subtracts what the record held there, and a missing file reads as snapshot 0,
so it would subtract from w on a file the rebuild already brought past it.
**Decision** — Stored files of the partition that no record is in any more are written empty at
the target, keeping their slot. A run creates no empty file either way.
**Rationale** — The file's own snapshot is what every later subtraction starts from, so it must
exist. An empty file costs a few bytes.

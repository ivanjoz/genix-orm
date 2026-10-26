# RATIONALE — dataframe

Design decisions, newest first.

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

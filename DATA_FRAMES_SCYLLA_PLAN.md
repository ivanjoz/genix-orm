# DataFrames on ScyllaDB — plan

Status: plan, nothing built. Phase 1 (done) moved DataFrames and FrameSQL into the `dataframe` module
and left `dynamo` only what needs DynamoDB (`DATA_FRAMES.md`, section 2). This plan is phase 2: a
`scylla` driver for the same frames, files and FrameSQL.

## 1. What phase 1 already gives Scylla

The `scylla` driver gets these unchanged: the files, codecs, lock and store; the compile rules
(`dataframe.Compile`); the write path's logic (`CheckValues`, `LoggedWrite`, `AppendLogEntries`,
`AppendCancelMarkers`, `WriteWindow`); the run, the rebuilds, plain and fresh reads, the express
compaction (`Materialize`, `RebuildRange`, `RebuildAll`, `Read`); and FrameSQL (`FrameSource`).

What the driver has to provide:

1. **The declaration**: `DataFrames []DataFrame` in `db.TableSchema`, with `db.Coln`, resolved into
   `dataframe.Declaration`s (getters from `scylla`'s reflect accessors).
2. **`dataframe.Table`**: a state record, the write sequence, four consistent record reads.
3. **The write path**: stored read → version → log append → land → cancel markers, inside the window.

Points 2 and 3 are where Scylla differs from DynamoDB. Section 2 is the work; section 3 lists what the
user decides before it starts.

## 2. The work, against the invariants

The invariants are in `DATA_FRAMES.md`, section 14. Each one below is checked against how `scylla`
works today.

### 2.1 Consistency: every frame read and write at QUORUM

The cluster default is `LocalOne` (`connection.go`). A run that reads at ONE can miss a write that
landed, and leave the files wrong until a rebuild. The driver must:
- read and write tables with frames, and the state record, at `LOCAL_QUORUM`;
- use `LOCAL_SERIAL` for the LWTs.

These are per-query consistency levels, so tables without frames keep `LocalOne`.

### 2.2 The write sequence (`UpdatedVersion`)

`GetCounter` reads the counter and then increments it, so two concurrent writers can get the same
value (`main.go`, the `ReserveCounterRange` comment). Frames need versions that are:
- unique;
- reserved after the stored read;
- readable as "the last reserved" (`CurrentWriteVersion`).

So:
- **Tables with frames require `ReserveCounterRange`** (the external allocator that serializes
  reservations). Without it, the boot fails.
- **`CurrentWriteVersion` reads the sequence row at QUORUM.** It must hold every value the allocator
  has handed out, not a range it reserved ahead. If the allocator pre-reserves ranges, it needs a
  "last served" read instead.
- **The sequence is per partition today** (`counterValueByPartition`). The scope of a frame follows
  from that (decision 3.1).

### 2.3 The state record

New table `<keyspace>.dataframe_state`:
- primary key: `(table_id, partition, frame)`;
- columns: `w bigint, nx bigint, nxt bigint, px bigint, ix set<bigint>, sh bigint`.

**Every write to that row is an LWT.** Scylla does not order LWT writes against plain writes on the
same row:

| Call | Statement |
|---|---|
| `PushCheckpoint` | `UPDATE ... SET px = ?, nx = ?, nxt = ? IF nxt = ?`, or `IF NOT EXISTS` (an INSERT) for the first push |
| `ResetState` | `UPDATE ... SET sh, nx, nxt; w, px, ix = null IF sh = ?` (the shape read), or `IF NOT EXISTS` |
| `CommitSnapshot` | `UPDATE ... SET w = ?, ix = ix + {...}` (or `- {...}`) `IF w = ?` (the snapshot read) |
| `ReadState` | a `SERIAL` read |

The lock already makes the holder the only one moving `w`. The `IF w = ?` condition adds no new
rule: it is the cheapest condition that keeps every write of the row an LWT.

### 2.4 Records written after W (`ReadRecordsWrittenAfter`): the hard one

The run reads inserts from the table, not from the log. So this read must return every record with
`UpdatedVersion > W` once W has settled. Scylla's delta index is a **materialized view**
(`index_view_compile.go`), and Scylla fills views asynchronously, with no bound on the lag. A run
that reads the view can miss a settled insert, and the frame drifts silently. Options:

| Option | Cost | Exact |
|---|---|---|
| A. A "changes" row in the base table, `(partition, upv_bucket, upv, key)`, written in the same logged batch as the record (same partition → atomic). The run reads the buckets above W at QUORUM, then the records by key | One more row per write; a cleanup of rows at or below `w` | Yes |
| B. A QUORUM scan of the partition, keeping `upv > W` in memory | The whole partition every 10 minutes | Yes |
| C. The materialized view | None | No: drift until the nightly rebuild |

Recommended: **A**. It is what `dynamo` does (its delta index is a hidden base-table row), and its
cost grows with the writes, not with the table.

### 2.5 The write path in `insert-update.go`

On a table with frames, `InsertUpdate*` / `Update*` would:
1. call `CheckValues`;
2. **read the stored rows by key at QUORUM** (today the writes are blind upserts);
3. reserve the version;
4. stamp `CreatedVersion` (a new managed column, `created_version`);
5. `AppendLogEntries` for the records whose frame values change;
6. land the batch at QUORUM within the `WriteWindow`;
7. `AppendCancelMarkers` for the records not sent.

Details:
- **Partial updates** (`columnsToUpdate`, `UpdateExclude`): the written frame values are the stored
  record with the updated columns laid over it. The stored read gives the rest.
- **Inserts** (`Insert*`) log nothing, like `InsertMany`: the caller says the records are new.
- **`Merge`** already reads first, so it can reuse that read.
- **Deletes**: Scylla records are soft-deleted (Status 0) through updates, so there is no separate
  delete path.
- **The cost**: a QUORUM read before every write to a table with frames. Batches stay one round trip.

### 2.6 The rest

- **Column ids**: the Scylla column name (`db` column name). A Scylla column is its name, so a rename
  is already a migration, and the shape changing with it is correct.
- **`ReadRecordsInRange`**: `Keys[0]` must lead the table's clustering key or a secondary/view index
  read at QUORUM. A view can't be read consistently, so the rule is stricter than dynamo's: it must
  lead the base keys.
- **`ReadRecordsBySK`**: the "sk" is the record's primary key, encoded as a string. It must round-trip
  through the log.
- **Tests**: port `TestDataFrameRunsMatchBruteForce` to drive the Scylla write-path hooks. It needs
  no database: the simulation fakes the reads, as it does in `dynamo`. Add an `ormcheck`-style live
  check against a Scylla keyspace.

## 3. Decisions for the user

1. **The scope of a frame.** Scylla tables are partitioned (by company) and the write sequence is
   per partition. Recommended: **one frame instance per partition**. The folder becomes
   `<table>/<partition>/<frame>/`, and the state record and the sequence are per partition, so
   companies never share files or a run. The alternative is a frame across partitions, which needs a
   table-wide sequence.
2. **How a run finds inserts**: option A, B or C of 2.4 (recommended A).
3. **The allocator requirement** of 2.2: tables with frames refuse to boot without
   `ReserveCounterRange`. The alternative is an LWT-based sequence for those tables (slower, about 4
   round trips per reservation).
4. **One store per process, or per driver.** `dataframe.Configure` is process-wide. Recommended:
   keep it. A process running both drivers writes both into one bucket, and their folders can't
   collide if Scylla's folder starts with a prefix (`s/`).
5. **The folder encoding**: `TableSchema.ID` (int16, stable) in the same order-preserving base64,
   behind the `s/` prefix.

## 4. Steps

1. `db.TableSchema.DataFrames` and `db.DataFrame`. `scylla` resolves them into `dataframe.Compile`
   and checks its own rules (QUORUM-readable `Keys[0]`, `created_version`, the allocator, the
   changes rows).
2. The `dataframe_state` table and the `Table` implementation (2.3, 2.4 and the reads), with
   QUORUM / SERIAL consistency.
3. The write path (2.5), behind "the table has frames", and tables without frames untouched.
4. `MaterializeDataFrames`, `RebuildDataFrames*`, `QueryFrame`, `FrameSource` on the Scylla
   controller, as thin calls into `dataframe`.
5. Port the randomized spec; a live check on a keyspace; extend `DATA_FRAMES.md` with a Scylla
   column wherever it names a DynamoDB call.

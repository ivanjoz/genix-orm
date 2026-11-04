## The delta cursor carries the server's read time
**Context** — The window protocol resent every record with `updated ≥ C − 10 s` and hashed the 15
minutes before that, all measured from the client's cursor alone. After a bulk write (10k products
stamped in one tick) the cursor stays at that tick, so every sync resent the 10k, over the threshold:
a replace on every sync, forever, until another write moved C.
**Decision** — The cursor is `C.V`, V the server clock when the read that answered the client began
(taken before any read). Writes become visible at most `DeltaWriteLagTicks` (16 s) after their
stamp, so only the slice `[V − 16 s, C]` can hold a write the client missed: the server sends
`updated > C`, hashes the slice instead of resending it, and the client compares. A mismatch keeps
the held V and asks again with `C.V.r`, which resends the slice's records. Once V − 16 s passes C,
nothing is hashed: the steady state is one empty read.
**Rationale** — A reload right after a bulk write hashes the slice (slim rows, no records) instead of
downloading it again. Only the client can compare: it knows which held copies the response replaced
(a record edited again after C leaves the slice on the server and not in the client), so a
server-side compare would resend the whole slice on any edit made within 16 s of a bulk write. The
cost is a second round trip on a real late write, and trusting the 16 s bound: a write landing later
than that is not seen until the record is written again.

## The shared cache package lives inside the db module
**Context** — The phase-5 plan named a new module `genix-orm/cache` depending only on `utils`. Every
consumer (dynamo, scylla, berryapps, genix) already requires `genix-orm/db`, and `db` itself needs
`cache.DeltaCursor` and `cache.CachedIDsRequest` in `TableInfo` and the `Executor` interface.
**Decision** — It is the package `genix-orm/db/cache`, inside the `db` module. It imports only
`utils`; `db` imports it, never the reverse.
**Rationale** — A separate module would add a `require` and a `replace` to every consumer's go.mod
for no isolation gain, since `db` depends on it anyway. The cost: it versions with `db`.

## Delta snapshots are written only by a database read
**Context** — A replace reads every record of its prefixes whatever happens next, so the server
keeps them as a snapshot file and answers the next replace from the file plus the changes since it.
A served merge is itself a valid snapshot at a newer cursor, and storing it would keep the file
fresh for one PUT and no read.
**Decision** — Only a full read of the database writes a snapshot (`delta_snapshot.go`). A merge
is served and dropped. A snapshot ages until more than the threshold changed since it; that replace
reads the database and writes a new one.
**Rationale** — The hash only sees the unsettled slice behind the cursor. A miss it can't see (a
write later than the 16 s lag bound, a move row lost to a crash) would be copied into every snapshot
built from a merge, forever; built from the database, a bad snapshot lives until the next full
read, at most 2 hours (`RecordSnapshotMaxAgeTicks`): a settled snapshot is never hashed again, so
without that age limit a slow table would serve a miss to every first sync until 100 changes piled up. Only data read from the database is ever written to the store. The cost is bounded: a merge
reads at most `threshold + 1` changed rows and `threshold + 1` moves per prefix.

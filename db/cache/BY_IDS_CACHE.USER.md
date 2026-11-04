# The by-IDs cache, explained

How a browser resolves "give me records 12, 87 and 412" (the user who signed a movement, the
products on a sale order, the client of an invoice) without downloading them again every time it
needs them. Written for people, not for the compiler. The precise specification lives in the code:
`by_ids.go` (this folder), `scylla/cache_by_ids.go`, and genix-ui's `cache/cache-by-ids.svelte.ts`
and `cache/cache-by-ids.wire.ts`.

The by-IDs cache is the sibling of the delta cache (`DELTA_CACHE.USER.md`):

| | Delta cache | By-IDs cache |
|---|---|---|
| The question | "Keep this **list** up to date" | "Give me **these records**" |
| What the browser sends | A cursor: how far it got | The IDs it wants, with the version it holds |
| What comes back | What changed in the list since the cursor | The requested records that changed |
| Typical use | Products page, clients page | A name or label next to an ID somewhere else |

---

## 1. The idea in one paragraph

The browser keeps every record it has received (in memory and in IndexedDB). When it needs some
records it sends their IDs along with **the version it holds of each one**, and the server answers
with **only the ones whose version moved**. A record the browser doesn't hold goes with version
"nothing", so it always comes back. Most of the time the answer is empty: everything the browser
held was still current.

```
 First time                            Every time after
 ──────────                            ────────────────
 browser: "12, 87, 412: I hold none"   browser: "12 @v1, 87 @v1, 412 @v1"
 server:  12, 87, 412                  server:  nothing (or only 87, if 87 changed)
```

---

## 2. Words used below

| Word | Meaning |
|---|---|
| `Updated` (`upd`) | The time of a record's last write, in 2-second ticks (SUnixTime). |
| **Partition** | The table's partition column, in practice the company. The handler fills it in from the token: a genix client never chooses it. |
| **Slot** | One of 256 buckets a record belongs to: `slot = ID % 256` (`uint8(ID)`). IDs 44, 300 and 556 share slot 44. |
| **Slot version** | When the slot was last written: the highest `Updated` among the writes of any record in that slot. |
| **Year cycle** | One year, starting January 1 at 07:00 UTC. |
| **Year tick** | A slot version as the browser sees it: 2-second steps counted from the start of the cycle. Fits in 3 bytes. |
| **Trust window** | 8 seconds. A slot version younger than that isn't reported yet. Section 5. |
| **Tombstone** | A deleted record (`ss = 0`) the browser keeps and answers from its cache without asking again. |

---

## 3. Why slots, not records

The obvious approach is "the browser sends each record's `Updated`, the server compares it with the
stored one". To compare, the server would have to **read every requested record** from the database,
which is the very cost the cache is meant to save.

So the server keeps a much smaller thing: per partition, **256 slot versions**. A write to any record
raises the version of its slot. To answer a request the server reads those 256 numbers (one tiny
row), compares, and then reads from the database **only the records whose slot moved**.

```
 partition = company 7                    slot  →  last write
 ┌────────┬────────┬────────┬─────┬──────────┐
 │ slot 0 │ slot 1 │ slot 2 │ ... │ slot 255 │
 │ 09:12  │ never  │ 10:00  │     │  08:30   │
 └────────┴────────┴────────┴─────┴──────────┘
             records 1, 257, 513… all live in slot 1
```

The price is precision: a write to record 300 raises slot 44, so a browser asking for record 44 or
556 reads them again even though they didn't change. That is a wasted read, never a wrong answer.

On this path a record's `upd` is **its slot's version**, not its own write time. The server overwrites
it on the way out, because that is the value the browser must send back next time.

---

## 4. A request, step by step

```mermaid
sequenceDiagram
    participant C as Component
    participant M as Memory / IndexedDB
    participant S as Server
    participant SL as Slot versions
    participant DB as Scylla (table)

    C->>M: getRecordsByID("users-ids", [12, 87, 412])
    M-->>C: all held, fetched 2 min ago (stale: > 5 s)
    Note over C: build the request: each slot with the version held
    C->>S: GET /users-ids?cc=<slots + versions + IDs>
    S->>SL: slot versions of partition 7
    SL-->>S: slot 12: same, slot 87: moved, slot 156: same
    S->>DB: SELECT … WHERE id IN (87)
    DB-->>S: record 87
    S-->>C: [87] with upd = slot 87's version
    Note over C: store 87; mark 12 and 412 as checked now
```

**In the browser** (`cache-by-ids.svelte.ts`):

1. **Memory first.** A record fetched less than 5 seconds ago (`CACHE_TIME`) answers at once, with
   no request.
2. **Then IndexedDB.** A record found there is moved into memory.
3. **A request only if needed.** If any requested record is missing or older than 5 s, the request
   goes out with **every** requested ID, so all of them are revalidated at once.
4. **Batching.** Single-record calls (`getRecordByID`) made within 80 ms of each other go out as one
   request. A record already being fetched by another call waits for that call.
5. **Tombstones are final.** A deleted record held locally answers from the cache and is never sent,
   unless the route opts out with `disableTombstone` because its records can be reactivated.

**On the server:**

1. The handler decodes `?cc=`, puts the company in as the partition
   (`req.GetCachedIDsRequest(companyID)`), and calls `db.QueryCachedIDs[T](request)`.
2. The ORM reads the partition's 256 slot versions.
3. For each requested ID: if the version the browser holds **equals** its slot's current version,
   the record is skipped. Otherwise it is read.
4. The records read come back with `upd` set to their slot's version.

**Back in the browser:**

- Returned records are stored in memory and IndexedDB.
- Requested records the answer left out were **confirmed unchanged**: only their "fetched at" time is
  refreshed, so they count as fresh for another 5 seconds.
- If the request fails, the browser answers from what it holds and logs a warning.

---

## 5. Writes in flight: the 8-second trust window

The delta cache trusts that a write is visible within 16 seconds of its stamp. The by-IDs cache has
the same kind of rule, at **8 seconds**. It needs less because it reads the table directly by
primary key, with no materialized view catching up, and because the slot is raised **after** the
row is written.

### Why the slot is raised after the row

If the slot moved first, a browser could read the old row and the new version together, and would
then hold "new version, old data": since its version now matches, it would never read the record
again. Raising the slot after the row means a browser that sees the new version can read the new
row.

### What can still go wrong: two records in one slot

```
 10:00:00   record 300 written, slot 44 raised to 10:00:00
 10:00:00   record 556 (also slot 44) stamped 10:00:00, its write still in flight
 10:00:01   browser asks for 556: slot 44 = 10:00:00, reads 556 → OLD row
            browser now holds 556 @ 10:00:00
 10:00:02   556's write lands; slot 44: max(10:00:00, 10:00:00) = 10:00:00, no change
 later      browser sends 556 @ 10:00:00, slot says 10:00:00 → skipped FOREVER
```

### The fix: young versions aren't reported

A slot version is reported only once its 2-second tick ended **at least 8 seconds ago**
(`SlotTrustTicks`). Before that, the record comes back with `upd = 0`, which means "nothing held",
so the browser reads it again on its next revalidation. In the example the browser holds
556 @ 0, asks again a few seconds later, and gets the new row.

```
 slot version   10:00:00
                   │◄── tick ──►│◄────────── 8 s ──────────►│
 reported as:      0 (untrusted, read again next time)       10:00:00 (trusted)
```

The rule this protocol trusts:

> **A write is visible within 8 seconds of the end of its stamp's 2-second tick,** clock skew
> between servers included.

A write that breaks it can be missed until another write touches its slot.

---

## 6. Where the slot versions live

Each database driver keeps them in its own way. The browser can't tell the difference: every driver
answers in year ticks.

### Scylla: a table

```
 <keyspace>.cache_by_ids_slots
   PRIMARY KEY ((table_id, partition_id))
   slots map<tinyint, int>        slot → the full Updated of its last write
```

- **One row per table and partition**, shared by every `CacheByIDs` table of the keyspace. Deploy
  creates it.
- **A write** sets the touched slots with `slots = slots + {…}` at `LocalQuorum`, and writes each
  cell with `USING TIMESTAMP` equal to the stamp's time. Scylla keeps the cell with the highest
  timestamp, so a late write with an older stamp can never lower a slot: it behaves as `max()`
  without reading first.
- **A read** fetches the rows of the requested partitions at `LocalQuorum`, then converts each full
  `Updated` into the current cycle's year tick. A slot last written in an earlier year reads as "no
  write this cycle".

### DynamoDB and the browser build: slot files

```
 <file store>/ids-cache/<cycle year>/<base pk>     768 bytes: 256 × 3-byte year ticks
```

- **A write** reads the file with its ETag, raises the touched slots to `max(stored, new)`, and
  writes it back only if nobody changed it meanwhile (`PutFileIfMatch`), retrying up to 30 times.
- **A read** fetches one file per partition, 10 at a time.

---

## 7. The year cycle and the yearly reset

The browser holds and sends **year ticks**: 2-second steps from January 1 07:00 UTC. Three bytes
hold a whole year (15.8 million ticks in a leap year, under the 16.7 million that fit).

| Tick | Meaning |
|---|---|
| `0` | Unknown: nothing held, or a version still inside the trust window. Always forces a read. |
| `1` | The slot had no write this cycle. |
| `≥ 2` | The slot's last write, `Updated − cycle start + 2`. |

When the cycle changes on January 1, every version the browser holds belongs to the previous cycle
and converts to 0. So on that day **every record is read once more**. That is the yearly reset, and
it needs no job. After it, records nobody wrote this year sit at tick 1, and the browser holds them
as such.

---

## 8. The request on the wire

`?cc=` is a compact binary request in base64 (`DecodeCachedIDsRequest`,
`encodeCachedIDsRequest`):

```
 [cycle year − 2000: 1 byte]
 [partition count]
 per partition, ascending:
   [partition, as a difference from the previous one]
   [slot count]
   [slot: 1 byte][year tick: 3 bytes] × slot count
   [ID count]
   [ID, as a difference from the previous one] × ID count
```

The browser sends **one version per slot, not per ID**: the lowest one it holds among the slot's
requested IDs. That is enough, because a held version is never above the slot's real one:

- **lowest = slot's version** means every requested record of the slot is current;
- **lowest < slot's version** makes the server read all of them, after which they all agree.

A requested ID the browser doesn't hold counts as 0, which leaves its slot out of the list: every
requested ID of that slot is read.

A request for 50 users spread over 50 slots weighs about 50 × 4 bytes of versions plus 50 small ID
differences, so a few hundred bytes of query string.

---

## 9. Worked example: the user column of a movements table

A page lists 200 warehouse movements. Each row shows the name of the user who recorded it: 12
distinct users, IDs 3, 8, 15… The component calls `getRecordsByID("users-ids", [3, 8, 15, …])`.

| Moment | Browser holds | Request | Server reads | Answer |
|---|---|---|---|---|
| First visit | nothing | 12 IDs, no slots listed | 256 slot versions + 12 users | 12 users |
| Reload, 3 s later | 12 users, fetched 3 s ago | **none** (< 5 s) | nothing | from memory |
| Next morning | 12 users in IndexedDB | 12 IDs with their 12 slot versions | 256 slot versions | **empty**: all confirmed |
| Admin renames user 8 at 09:00; reload at 09:05 | 12 users | same | slot versions; slot 8 moved → user 8 | **user 8** |
| Reload at 09:00:03, right after the rename | 12 users | same | slot 8 moved → user 8, its slot still in the trust window | user 8 with `upd = 0` |
| Reload at 09:00:20 | user 8 @ 0 | user 8's slot left out (held 0) | user 8 again | user 8, now with its real version |

### The same page, with a coincidence

User 264 (slot 8, since 264 % 256 = 8) changes their password. The next reload sends slot 8 with
the version held, the server sees slot 8 moved, and **user 8 is read again** even though it didn't
change. One extra row read, and the answer is still right.

---

## 10. Static records

Some tables never change a record once it exists (lots, catalog rows). For them
`getStaticRecordsByID` skips the whole version protocol:

- memory, then IndexedDB, then the server **only for IDs held nowhere**;
- the request carries no versions, and the server returns every ID it was asked for;
- nothing is ever revalidated.

---

## 11. What each side stores

```
 Browser
 ├─ memory: one map per route (and per partition): ID → record
 └─ IndexedDB "<company>_cached_ids_<env>", table cacheByIDs
      key [storeName + ID], storeName = "<route>" or "<route>:<partition>"
      value: the record, with upd (its slot's version) and _fch (when it was fetched)

 Server, nothing per browser. Per table and partition:
 ├─ Scylla: one row in <keyspace>.cache_by_ids_slots
 └─ DynamoDB / browser build: one 768-byte slot file per year in the file store
```

---

## 12. Limits to know

- **The 8-second rule is trusted.** A write visible more than 8 s after the end of its tick can be
  missed until its slot is written again.
- **Slots are shared.** A write to one record makes the browser read again every requested record of
  its slot. With 256 slots and IDs spread evenly, that is rare for a handful of records and common
  for thousands.
- **A fresh write is read twice.** For 8 s it comes back with `upd = 0`, so the next revalidation
  reads it again.
- **The yearly reset.** On January 1 at 07:00 UTC every held record is read once more.
- **Rows are written at `LocalOne`.** The slot is raised at `LocalQuorum` and read at `LocalQuorum`,
  but the record row itself is not yet written at `LocalQuorum`. Until it is, "row written, then slot
  raised" is a near-certainty rather than a guarantee, and the 8-second window covers the gap in
  practice.
- **A failed slot raise is not repaired.** If raising the slot fails after the row was written, the
  write call returns an error, and browsers holding the old version won't see that write until its
  slot is written again.
- **Deleted records are final** for routes that keep tombstones (the default).

---

## 13. Constants

| Constant | Value | Where |
|---|---|---|
| Slots per partition | 256 (`slot = uint8(ID)`) | `by_ids.go` |
| `SlotTrustTicks` | 4 ticks = 8 s | `by_ids.go` |
| Year cycle start | January 1, 07:00 UTC | `by_ids.go`, `cache-by-ids.wire.ts` |
| IDs per Scylla `IN` query | 100 | `scylla/cache_by_ids.go` |
| Slot file write attempts | 30 | `by_ids.go` |
| `CACHE_TIME` (browser freshness) | 5 s | `cache-by-ids.svelte.ts` |
| Batching window (`getRecordByID`) | 80 ms | `cache-by-ids.svelte.ts` |
| Tick | 2 seconds (SUnixTime) | project convention |

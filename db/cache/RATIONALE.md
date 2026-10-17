## The shared cache package lives inside the db module
**Context** — The phase-5 plan named a new module `genix-orm/cache` depending only on `utils`. Every
consumer (dynamo, scylla, berryapps, genix) already requires `genix-orm/db`, and `db` itself needs
`cache.DeltaCursor` and `cache.CachedIDsRequest` in `TableInfo` and the `Executor` interface.
**Decision** — It is the package `genix-orm/db/cache`, inside the `db` module. It imports only
`utils`; `db` imports it, never the reverse.
**Rationale** — A separate module would add a `require` and a `replace` to every consumer's go.mod
for no isolation gain, since `db` depends on it anyway. The cost: it versions with `db`.

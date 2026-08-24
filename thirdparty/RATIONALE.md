# RATIONALE — thirdparty

## The gocql fork lives in genix-orm, not in the consuming app
**Context** — The fork was first placed in the app's `backend/thirdparty/` alongside a protobuf
fork, because both were made in the same pass. But gocql is imported only by `scylla/`; the app
never names it, and this module's `go.mod` already declared the scylladb swap itself.
**Decision** — Moved to `genix-orm/thirdparty/gocql`. `../go.mod` replaces against it directly; the
consuming app replaces against `./genix-orm/thirdparty/gocql`, the same tree.
**Rationale** — The fork belongs with the module that owns the dependency, or an ORM upgrade means
editing a directory in someone else's repo. The wrinkle is that **a `replace` in a non-main module
is ignored by Go**: this module's replace covers only its own build and tests, so the app must
repeat it. That is a duplicated line, not a duplicated tree — both paths resolve to one directory,
so there is still one copy to patch. It also means the two can silently disagree: if the app's
replace is ever dropped, this module's tests keep passing against the fork while the app builds the
unpatched driver and quietly grows 262,144 bytes.

## Delete recreate.go rather than patch it
**Context** — `recreate.go` pulls `text/template` for `KeyspaceMetadata.ToCQL()`, a scylla-manager
helper that nothing in this repo or its consumers calls.
**Decision** — The regeneration script deletes the file and then asserts `text/template` is no
longer reachable anywhere in the fork.
**Rationale** — There is nothing to patch: the file is entirely `ToCQL` plus the package-level
`template.Must(...)` vars it needs. Deleting it is the whole change, which keeps the fork a pure
subset of upstream and makes an upgrade a re-copy rather than a merge. The assertion exists because
the failure mode is silent — if upstream moves those templates into another file, the deletion
stops working and the only symptom is a binary that is 262,144 bytes larger.

Worth being honest about the ratio: this is 20,437 vendored lines for 0.8% of a consuming binary,
and it makes every scylladb bump a manual re-fork of the database driver. Unlike the app's protobuf
fork, this one is **not** a `reflectSeen` source — `ToCQL` is unreachable, so `text/template`'s
dynamic lookup never links regardless. It buys dead-code removal and nothing else, and it is built
to be reverted in two lines if that trade stops being worth it.

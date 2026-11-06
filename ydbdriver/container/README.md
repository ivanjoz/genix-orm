# YDB container (benchmark and local development)

Image `docker.io/ydbplatform/local-ydb:26.3.1.21` (always pin the full tag). Single node, gRPC `:2136`,
UI `http://localhost:8765`, database `/local`, no auth.

```sh
podman volume create genix-ydb-data
podman run -d --name genix-bench-ydb --hostname localhost \
  -p 2136:2136 -p 8765:8765 \
  -e GRPC_PORT=2136 -e MON_PORT=8765 \
  -e YDB_USE_IN_MEMORY_PDISKS=false \
  -v genix-ydb-data:/ydb_data \
  --memory 3g \
  docker.io/ydbplatform/local-ydb:26.3.1.21

# Pin to cores 2,3 like the FDB container. `--cpuset-cpus` fails on a rootless setup without the cpuset
# controller delegated, so pin the host processes instead. Repeat after every `podman restart`.
for pid in $(podman top genix-bench-ydb hpid | tail -n +2); do taskset -acp 2,3 $pid; done
```

Verified (2026-11-06):
- Ready within a few seconds of start; the port opens before the database is ready, so probe with a
  query (`GENIX_YDB_IT=1 go test -run TestSpikePing .`), not with the port.
- Data survives `podman restart` (`TestSpikePersistence`, `GENIX_YDB_PERSIST=write|check`).
- The image has no `ydb` CLI; use the UI or the Go tests.
- Port 2136/8765 do not collide with the Scylla (127.0.0.2:9042) or FDB containers.

Tests: `GENIX_YDB_IT=1 go test .` (spikes and integration), plus `GENIX_YDB_BENCH=1` for the
microbenchmark. Results are in `docs/YDB_PLAN.md` §18b.

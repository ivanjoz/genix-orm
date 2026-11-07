# OceanBase server for obdriver

The driver's integration tests, conformance runs and benchmarks use a native OceanBase install (no
container) on the `homelab` node. The install itself is managed outside this repository.

| | |
|---|---|
| Version | OceanBase CE 5.0.1.0 (`5.7.25-OceanBase_CE-v5.0.1.0`), mini mode, `memory_limit = 8G` |
| Host | `homelab`, AMD Ryzen 7 5700U (16 threads), 14 GB RAM |
| Address | `100.64.0.4:2881` over Tailscale; `127.0.0.1:2881` on the host itself |
| Tenant / user | `test` (MySQL mode) / `root@test`, no password today |

## Connect

```sh
mariadb --skip-ssl -h100.64.0.4 -P2881 -uroot@test
```

## Run the spike tests

From any machine on the tailnet (timings include the network):

```sh
cd backend/genix-orm/obdriver
GENIX_OCEANBASE_IT=1 go test -run TestSpike -v -count=1 ./...
```

On the server itself, which is how the plan's timings were taken:

```sh
CGO_ENABLED=0 go test -c -o /tmp/obspike.test .
scp /tmp/obspike.test 100.64.0.4:/tmp/
ssh 100.64.0.4 'GENIX_OCEANBASE_IT=1 GENIX_OCEANBASE_BENCH=1 GENIX_OCEANBASE_HOST=127.0.0.1 /tmp/obspike.test -test.run TestSpike -test.v'
```

Environment: `GENIX_OCEANBASE_HOST`, `GENIX_OCEANBASE_PORT`, `GENIX_OCEANBASE_USER`,
`GENIX_OCEANBASE_PASSWORD`. Every test creates a `spike_<n>` database and drops it.

## Verified facts (details in `docs/OCEANBASE_PLAN.md` §18b)

- The tenant's default `ob_query_timeout` is 10 s; the driver sets 600 s per connection.
- The server's default collation is `utf8mb4_general_ci` (case-insensitive); the driver uses
  `utf8mb4_bin` on the connection and on every table.
- TTL purge needs `enable_ttl = True` **and** `enable_kv_ttl = True` on the tenant; both were set
  on 2026-11-06 for the DataFrame log (plan D4). Purge runs daily at `ttl_duty_time` (01:00), or on
  demand with `ALTER SYSTEM TRIGGER TTL`.
- `max_allowed_packet` = 16 MB: a larger statement makes the server drop the connection.
- The recycle bin is off; `DROP` frees tablets immediately.
- `GV$OB_SQL_AUDIT.query_sql` is empty for this tenant; find an execution by `LAST_TRACE_ID()`.

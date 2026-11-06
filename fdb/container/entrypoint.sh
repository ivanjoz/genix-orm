#!/bin/bash
# Starts fdbserver (via the image's own script), creates the database once, then runs fdbgate.
set -uo pipefail

/var/fdb/scripts/fdb.bash "$@" &

# `fdbcli status` blocks on an unconfigured database, so probe with `configure new` instead: it
# creates the database on a fresh volume and answers "already exists" on a restart.
until configure_output=$(fdbcli --timeout 5 --exec "configure new single ssd-redwood-1" 2>&1) \
  || grep -qi "already exists" <<<"$configure_output"; do
  sleep 1
done
echo "fdb: database ready (${configure_output%%$'\n'*})"

mkdir -p "$(dirname "${FDBGATE_SOCKET:-/run/genix-fdb/fdbgate.sock}")"
# libfdb_c.so lives in /usr/lib, which is not on this Debian-based image's loader path.
LD_LIBRARY_PATH=/usr/lib exec /usr/bin/fdbgate

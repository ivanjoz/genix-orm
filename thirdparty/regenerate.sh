#!/usr/bin/env bash
# Rebuilds the vendored gocql fork from the module cache. Run it after bumping the version in
# ../go.mod (and in the parent app's go.mod, which points at this same directory).
#
# Why the fork exists: see ./README.md.
set -euo pipefail

cd "$(dirname "$0")"
ORM_DIR="$(cd .. && pwd)"
MODULE_CACHE="$(go env GOMODCACHE)"

# Must match the `replace` targets in ../go.mod and in the app's go.mod.
GOCQL_MODULE="github.com/scylladb/gocql"
GOCQL_VERSION="v1.13.0"

log() { printf '\n\033[1m%s\033[0m\n' "$*"; }

source_root="${MODULE_CACHE}/${GOCQL_MODULE}@${GOCQL_VERSION}"
[ -d "$source_root" ] || { echo "not in module cache: $source_root (run 'go mod download' first)"; exit 1; }

log "gocql ${GOCQL_VERSION}: copying"
rm -rf gocql
mkdir -p gocql
# Copied whole rather than trimmed: gocql is one flat package plus two subpackages, so there is
# little to gain, and the driver is reached through paths that vary with build tags.
rsync -a --exclude '*_test.go' --exclude 'testdata' --exclude '.git' --exclude '.github' \
	"$source_root/" gocql/
chmod -R u+w gocql

# recreate.go is deleted rather than patched: it holds package-level template.Must(...) vars and
# provides only KeyspaceMetadata.ToCQL(), a scylla-manager helper nothing here references. Keeping
# it links text/template for nothing.
log "gocql: dropping recreate.go (text/template)"
rm -f gocql/recreate.go

if grep -rqs 'text/template' gocql/; then
	echo "text/template is still reachable in the fork -- upstream moved the templates"
	exit 1
fi
echo "  verified: no text/template"

log "done. Verify with:"
echo "  cd $ORM_DIR && go build ./... && go test ./..."
echo "  and rebuild the app, which points at this same directory"

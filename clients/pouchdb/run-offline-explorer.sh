#!/bin/sh
# Drive examples/offline-explorer in the pinned Playwright image against a
# running three-node douchdb cluster.
#
#   run-offline-explorer.sh <node 0 url> <node 2 url> [user:password]
#
# The urls must be reachable from the container (http://host.docker.internal:...).
# The cluster must run with --cors-origins http://127.0.0.1:8099 --cors-credentials
# (8099 is the port the page is served on inside the container), and
# offline_demo must have been seeded:
#   DOUCHDB_PASSWORD=... python3 examples/offline-explorer/seed.py --url <node 0> --fixture 300
set -eu
node0=${1:?usage: run-offline-explorer.sh <node 0 url> <node 2 url> [user:password]}
node2=${2:?usage: run-offline-explorer.sh <node 0 url> <node 2 url> [user:password]}
auth=${3:-admin:secret}
here=$(cd "$(dirname "$0")" && pwd)
page=$(cd "$here/../../examples/offline-explorer" && pwd)
name=douchdb-offline-explorer-$$
trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT
# Keep this tag in step with the playwright-core version in package.json.
timeout 600 docker run --rm --init --ipc=host --name "$name" -e WAIT_MS \
  --add-host=host.docker.internal:host-gateway \
  -v "$here":/src:ro -v "$page":/page:ro \
  mcr.microsoft.com/playwright:v1.49.1-noble \
  sh -c 'mkdir /work && cd /src && cp package.json package-lock.json offline-explorer.js /work \
    && cd /work && npm ci --no-audit --no-fund >/dev/null && node offline-explorer.js "$0" "$1" "$2"' \
  "$node0" "$node2" "$auth"

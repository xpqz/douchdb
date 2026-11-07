#!/bin/sh
# Browser PouchDB sync against a running douchdb, in the pinned Playwright image.
#
#   run-browser.sh <douchdb url reachable from the container> [user:password]
#
# douchdb must have been started with
#   --admin user:password --cors-origins http://127.0.0.1:8099 --cors-credentials
# (8099 is the port the test serves its page on, a different origin from douchdb).
set -eu
url=${1:?usage: run-browser.sh <douchdb url> [user:password]}
auth=${2:-admin:pass}
here=$(cd "$(dirname "$0")" && pwd)
name=douchdb-browser-$$
trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT
# Keep this tag in step with the playwright-core version in package.json.
timeout 600 docker run --rm --init --ipc=host --name "$name" \
  --add-host=host.docker.internal:host-gateway \
  -v "$here":/src:ro \
  mcr.microsoft.com/playwright:v1.49.1-noble \
  sh -c 'mkdir /work && cd /src && cp package.json package-lock.json browser.js /work \
    && cd /work && npm ci --no-audit --no-fund >/dev/null && node browser.js "$0" "$1"' \
  "$url" "$auth"

# Client compatibility suites

## PouchDB 9

`clients/pouchdb/run.js` drives the real `pouchdb-core` 9.0.0 client against a
running douchdb:

- local memory adapter document CRUD, updates and deletes;
- `bulkDocs`, `allDocs` with keys/pagination and `include_docs`;
- one-shot replication push and pull;
- remote `bulkGet`, tombstones and `_changes`;
- a conflict created through `_bulk_docs?new_edits=false`, read with
  `?conflicts=true`.

This is the PouchDB client path used by browser and Node applications: PouchDB
performs the HTTP requests, revision handling and replication bookkeeping.

Run it against a local server:

```sh
cd clients/pouchdb
npm ci
node run.js http://127.0.0.1:5984
```

CI builds the douchdb image, starts it on a temporary port and runs the same
script.

## Upstream PouchDB mocha suite

The upstream PouchDB integration suite is also run against douchdb from a
pinned checkout of `pouchdb/pouchdb` at commit
`f17b374172eca95f321e2c9d2c4ddfa0956ad0ef`. Because that tree's test harness
predates Node 18, the run installs dependencies with `--ignore-scripts`, builds
the Node packages with `npm run build-node`, and adds a one-line `URL.parse`
compatibility shim to `tests/integration/node.setup.js`.

```sh
git clone https://github.com/pouchdb/pouchdb.git pouchdb
git -C pouchdb checkout f17b374172eca95f321e2c9d2c4ddfa0956ad0ef
cd pouchdb
npm install --ignore-scripts --no-audit --no-fund
npm run build-node
# Add `URL.parse = require('url').parse` to tests/integration/node.setup.js.
COUCH_HOST=http://127.0.0.1:5984 CLIENT=node ADAPTERS=memory BAIL=0 \
  docker run --rm --network host -v "$PWD:/work" -w /work \
  -e CLIENT=node -e ADAPTERS=memory -e COUCH_HOST \
  node:18 ./node_modules/.bin/mocha --exit --timeout 10000 \
  --require=./tests/integration/node.setup.js --reporter=spec \
  tests/integration/test.changes.js
```

Run douchdb with raised admission limits for the upstream stress tests (some
suites issue more than the default 16 concurrent requests per user):

```sh
douchdb --listen 127.0.0.1:5984 --data /tmp/douchdb \
  --max-in-flight 512 --per-user-in-flight 512
```

Results against the current tree:

| Suite | Passing | Failing | Pending | Known failures |
| --- | ---: | ---: | ---: | --- |
| `test.basics.js` | 390 | 2 | 0 | both are attachment tests; attachments are intentionally unsupported (#19) |
| `test.bulk_docs.js` | 88 | 0 | 0 | none |
| `test.changes.js` | 128 | 0 | 5 | none |
| `test.conflicts.js` | 34 | 0 | 0 | none |
| `test.get.js` | 50 | 0 | 6 | none |
| `test.all_docs.js` | 53 | 0 | 0 | none (with raised admission limits) |
| `test.bulk_get.js` | 17 | 3 | 0 | all three are attachment tests |
| `test.design_docs.js` | 12 | 0 | 0 | none |

The `test.changes.js` run covers named filters, `_design`, `_view`, filter
errors, `style=all_docs` conflict ordering, `latest=true` and `_revisions` ids
that are not hex. The pending tests are skipped by the upstream suite itself,
not by douchdb.

## Upstream CouchDB clients

DouchDB's conformance tests already exercise CouchDB's HTTP shapes directly,
and the `jepsen/` Python harness drives the replication protocol with
`requests`. A full CouchDB client-library matrix (couchdb-python, nano,
CouchDB's own integration suite) still needs a maintained harness that can be
pinned. Coverage and gaps are tracked in issue #122.

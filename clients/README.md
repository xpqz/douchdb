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
script. The full upstream PouchDB mocha monorepo suite is not vendored here;
its old test harness targets Node 14-era tooling and needs a separate pinned
environment. The suite above covers the client-visible surfaces that matter for
this project and is small enough to run on every pull request.

## Upstream CouchDB clients

DouchDB's conformance tests already exercise CouchDB's HTTP shapes directly,
and the `jepsen/` Python harness drives the replication protocol with
`requests`. A full CouchDB client-library matrix (couchdb-python, nano,
CouchDB's own integration suite) should be added when those projects have a
maintained harness that can be pinned. Coverage and gaps are tracked in
issue #122.

# Differential fuzzer: douchdb against Apache CouchDB

`run.py` sends the same random document operations to a douchdb instance and a
real CouchDB instance, then compares status codes, error codes, revision ids
and subsequent state. When the two disagree it minimizes the operation list and
writes a replayable repro.

This is a single-node API test. It complements, but does not replace, the
cluster and fault-injection tests in [`jepsen-clj/`](../jepsen-clj/).

## Quick start

```sh
python3 differential/run.py --seed 1
```

By default this builds the `douchdb:differential` image from the repository,
starts a one-node douchdb (`q=1`, `n=1`) and a `couchdb:3.5` container, and runs
60 operations. Docker must be available.

Common options:

```sh
# Reuse an already-built image
python3 differential/run.py --seed 7 --skip-build

# Run more operations, print each one
python3 differential/run.py --seed 7 --steps 500 --verbose

# Use servers you already started
python3 differential/run.py \
  --douchdb-url http://127.0.0.1:5984 \
  --couchdb-url http://admin:pass@127.0.0.1:5985

# Replay a saved repro against fresh servers
python3 differential/run.py --replay differential/artifacts/7/repro.json

# Keep the containers for inspection
python3 differential/run.py --seed 7 --keep
```

The CouchDB URL passed through `--couchdb-url` is expected to include basic
auth credentials. The Docker path supplies `admin:pass` automatically.

## What is compared

Each operation is executed against both servers before the next one is chosen,
so both see the same revision history. Operations cover:

- `PUT` with no revision, the current revision, and a deliberately stale
  revision;
- `GET`, including `?rev=` for an old revision;
- `DELETE` with no revision, the current revision, and a stale revision;
- document ids containing slashes, spaces, `?`, `#`, `%` and non-ASCII
  characters;
- values with nested objects and arrays, unicode, booleans, null, floats,
  integers around the `i64`/`u64` boundaries, integers beyond them, key order,
  escaped characters, duplicate keys, and insignificant whitespace.

For each operation the fuzzer compares:

1. HTTP status;
2. `error` for responses at or above 400;
3. the full JSON body when the operation succeeds.

`_rev` is part of the body comparison, so a different revision hash is a
divergence even when the visible document value happens to match. That is
intentional: CouchDB clients and replication depend on revision identity.

## Normalization

Only genuinely non-semantic differences are normalized:

- generated UUIDs and timestamps, where they appear;
- sequence numbers, which are opaque and differ between implementations;
- prose in error messages, which is compared by `error` code rather than text.

Nothing else is normalized. Bodies are compared as parsed JSON values, so key
order inside a response is not compared directly; for writes, the revision id
independently covers document key order and number representation. Duplicate
keys are sent as raw bodies so both servers decode them exactly as written.

## Multi-document operations

Run the multi-document suite:

```sh
python3 differential/run.py --multi --seed 1
```

It keeps a model of winners and revision history while sending seeded random
operations to both servers: `_bulk_docs` writes and deletes, revision-shaped
GETs, `open_revs=all`, `_all_docs` keys/ranges/include_docs/conflicts,
`_changes`, `_bulk_get` and `_revs_diff`. `--multi-steps N` controls the
operation count (default 120).

Sequence values are opaque and are removed before comparison. DouchDB currently
emits every `_changes` revision event while CouchDB emits the latest change per
document (issue #125), so `_changes` rows are deduplicated by document id and
the latest row per id is compared.

## Map/reduce views

Run the view suite instead of document CRUD:

```sh
python3 differential/run.py --views --seed 1
```

This seeds a fixed corpus of 41 documents and a design document with map,
`_count`, `_sum`, `_stats` and custom-reduce views, then runs systematic and
seeded-random queries. Coverage includes array keys, grouping and group levels,
keys/multi-keys, ranges, descending order, include_docs, skip/limit, Unicode
collation, missing fields and CouchDB's query-validation errors.

`--view-queries N` controls the number of random queries after the systematic
set (default 200).

CouchDB's `offset` and row order for multi-key queries follow internal legacy
rules that are not part of its API contract. Those rows are compared as a
multiset and the multi-key offset is ignored; single-key and range queries are
compared exactly, including offset.

Search is not covered: stock `couchdb:3.5` has no `_search` endpoint (it
requires Clouseau), so there is nothing to compare against. DouchDB's search
endpoint is covered by the conformance tests.

## Known divergences

`known_divergences.json` maps a divergence kind to an open issue. A known
divergence is reported with its issue number and does not fail the run unless
`--strict` is passed. Every entry must name the issue and the condition under
which it can be removed.

Current entries: none. Large integers and duplicate object keys are covered by
the default generator and match CouchDB.

## Exit status

| Status | Meaning |
| --- | --- |
| `0` | No divergence, or only a known divergence in non-strict mode. |
| `1` | New divergence, or a known divergence with `--strict`. |
| `2` | Invalid arguments. |

## Repros

A failing seed writes `differential/artifacts/<seed>/repro.json` with the full
operation list, the minimized list, both responses, and container logs. Replay
it with `--replay`; the same seed also reproduces it from scratch:

```sh
python3 differential/run.py --seed 7 --steps 500
python3 differential/run.py --replay differential/artifacts/7/repro.json
```

Minimization removes operations greedily while preserving the divergence kind
and the operation name. It is bounded by `--max-minimize-replays` (default 200)
because every candidate is replayed against fresh databases. Use
`--no-minimize` to skip it.

## Adding a new operation

1. Add the operation to `generate_ops`.
2. Execute it in `execute_op` against both servers.
3. Update state in `update_state` when the response succeeds.
4. Classify differences in `divergence_kind`.
5. Run several seeds with `--steps 500` before committing.

Keep the generator deterministic for a given seed. Do not call `random` outside
the seeded `rng`.

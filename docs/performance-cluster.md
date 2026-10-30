# Performance: three-node cluster

A reproducible benchmark for a local three-node douchdb cluster. It exercises
the paths that fan out across nodes — document routing, `_all_docs`, `_changes`
and views — and records the cost of replication and coordinator merging.

## Environment

| | |
|---|---|
| Date | 2026-10-30 |
| CPU | AMD Ryzen 7 5700U, 8 cores / 16 threads |
| Memory | 28 GiB |
| OS | Linux 7.2.8 / glibc 2.44 |
| Build | `cargo build --release` |
| Cluster | 3 nodes, `q=4`, `n=3`, one process per node |
| Client | Python 3.14 `http.client`, keep-alive, local loopback |
| Coordinator | node 0 |
| Seed data | 6,000 small documents |
| Anti-entropy | `--sync-secs 3600` (writes are synchronously replicated) |

## Reproduce

```sh
cargo build --release
python3 scripts/perf_cluster.py --docs 6000
```

The script starts three nodes on ephemeral ports, creates its own database,
loads documents through node 0, runs the measurements, writes
`target/perf-cluster.json`, and shuts the processes down. It does not reuse an
existing cluster.

`--spread` round-robins benchmark clients across every coordinator (the same
shape as the nginx front); `--nodes N --n 1 --spread` runs a scaling
experiment. `--native` also runs `target/release/loadgen` for GET throughput,
so build it first:

```sh
cargo build --release --bin loadgen
python3 scripts/perf_cluster.py --nodes 3 --spread --native
```

The load generator can also be run directly:

```sh
cargo run --release --bin loadgen -- \
  --base http://127.0.0.1:5984,http://127.0.0.1:5985 \
  --path '/perf/doc-{id}' --count 10000 --concurrency 64
```

## Results

These are the numbers after the cluster fan-out (#32) and live-document index
(#33) passes. Latency is in milliseconds. Throughput for bulk operations is
documents per second.

| Operation | n | Throughput | p50 | p99 |
|---|---:|---:|---:|---:|
| Single PUT, sequential | 500 | 1,374 req/s | 0.663 | 1.116 |
| Single PUT, 8 writers | 500 | 4,002 req/s | 1.727 | 3.684 |
| Single GET, sequential | 1,000 | 6,070 req/s | 0.150 | 0.278 |
| Single GET, 8 writers | 500 | 7,922 req/s | 0.534 | 4.328 |
| `_bulk_docs`, 250 docs/request | 2,500 | 29,562 docs/s (118 req/s) | 7.828 | 9.192 |
| `_bulk_get`, 250 docs/request | 2,500 | 36,396 docs/s (146 req/s) | 6.560 | 7.276 |
| `_all_docs?limit=100` | 50 | 538 req/s | 1.680 | 2.201 |
| `_all_docs?startkey=doc-005000&limit=100` | 50 | 599 req/s | 1.617 | 2.327 |
| `_all_docs` with 100 keys | 20 | 646 req/s | 1.447 | 1.842 |
| `_changes?since=0&limit=1000` | 20 | 47 req/s | 20.505 | 24.723 |
| `_changes?since=0` (full feed) | 5 | 6.6 req/s | 144.239 | 158.575 |
| `GET /db` info | 100 | 1,749 req/s | 0.505 | 1.742 |
| View map, cold index build | 1 | — | 27.98 | — |
| View map, warm (reduce default) | 20 | 550 req/s | 1.905 | 2.079 |
| View map-only, warm | 20 | 355 req/s | 2.626 | 4.495 |
| View reduce, warm | 20 | 548 req/s | 1.833 | 2.272 |

## Before and after

The cluster pass made fan-out owner-aware and concurrent.

| Operation | Before (#31) | After (#32) |
|---|---|---|
| `_all_docs?limit=100` | 9.3 req/s, p50 106.3 ms | 431 req/s, p50 2.23 ms |
| `_all_docs?startkey=...&limit=100` | 9.5 req/s, p50 104.4 ms | 118 req/s, p50 8.27 ms |
| `_all_docs` with 100 keys | 9.0 req/s, p50 107.8 ms | 606 req/s, p50 1.57 ms |
| View cold index build | 169.4 ms | 77.8 ms |
| View map, warm (reduce default) | 19.8 req/s, p50 50.6 ms | 399 req/s, p50 2.34 ms |
| View map-only, warm | 20.0 req/s, p50 49.9 ms | 299 req/s, p50 3.35 ms |
| View reduce, warm | 19.9 req/s, p50 50.1 ms | 425 req/s, p50 2.22 ms |

## What changed

- **`_all_docs` owner-only fan-out.** Each node now returns only documents whose
  shard it owns. Since each document has exactly one primary, the coordinator
  unions complete rows without replica duplicates.
- **`_all_docs` window push-down.** Nodes receive the user's `startkey`/`endkey`
  and `skip + limit` window, and return their local `total_rows` and `offset`;
  the coordinator sums those and applies the global skip/limit. The old path
  transferred every live document from every node.
- **Concurrent fan-out.** All-docs and view requests to peers now run with
  `join_all` instead of serially.
- **Views are owner-only too.** Each peer returns map rows for shards it owns;
  replica copies are not merged. The coordinator computes the global
  `total_rows` from the owner row counts.
- **Map queries push `skip + limit` down.** A map-only view query now asks each
  owner for only the needed window. Reduce queries still need the full range.
- **Distributed reduces.** Global and grouped `_count`, `_sum` and `_stats`
  are reduced per owner and combined by the coordinator. Custom JavaScript
  reducers are recombined with `rereduce=true`; `_approx_count_distinct` still
  falls back to the map-row merge path because per-owner counts cannot be
  unioned exactly.
- **Single-replica write fast path.** When every document in a batch has one
  replica, the coordinator returns after the primary write instead of doing an
  unnecessary `_bulk_get` and secondary-copy pass. Design documents still copy
  to all nodes.

### Live-document index (#33)

Each node builds its ascending `_all_docs` live-document map lazily on the first
range query and maintains it in memory. Owner-only range queries no longer walk
metadata to compute their local offsets, including with a far-away `startkey`.

| Operation | Before (#33) | After (#33) |
|---|---|---|
| `_all_docs?limit=100` | p50 2.23 ms | p50 1.83 ms |
| `_all_docs?startkey=doc-005000&limit=100` | 118 req/s, p50 8.27 ms | 524 req/s, p50 1.86 ms |
| `_bulk_docs`, 250 docs/request | 11,956 docs/s | 12,177 docs/s |

## Scaling with node count

This run keeps one replica per document (`q=4`, `n=1`) and spreads clients
across all coordinators, so it measures the effect of adding nodes rather than
replica fan-out:

```sh
python3 scripts/perf_cluster.py --nodes N --q 4 --n 1 --spread \
  --workers 32 --per-level 5000
```

| Operation | 1 node | 2 nodes | 3 nodes | 4 nodes |
|---|---:|---:|---:|---:|
| Single PUT, 32 writers | 5,595 req/s | 5,135 | 4,918 | 4,919 |
| Single GET, 32 writers | 7,783 req/s | 7,086 | 6,781 | 6,690 |
| `_bulk_docs`, 250 docs/request | 44,045 docs/s | 32,510 | 29,640 | 26,680 |
| `_all_docs?limit=100` | 963 req/s | 558 | 482 | 463 |
| Warm map view | 2,764 req/s* | 366 | 528 | 652 |
| `GET /db` info | 5,322 req/s | 2,005 | 1,356 | 1,021 |
| `_changes?since=0&limit=1000` | 77 req/s | 84 | 112 | 101 |

\* The one-node row uses the single-node view path; at two or more nodes the
coordinator performs a cluster fan-out, so the view row is not directly
comparable across that boundary.

The table above is the historical `n=1` Python-client run. A native Rust client
on the same host, with `n=3` and 32 concurrent GETs, measured:

| Nodes | GET req/s | p50 |
|---|---:|---:|
| 1 | 44,307 | 0.67 ms |
| 2 | 40,873 | 0.67 ms |
| 3 | 41,437 | 0.66 ms |

Even the native client flattens around 40k because the client and all server
processes share the same 16-thread host. This is now a load-generator/host
ceiling, not a Python client ceiling. A separate load-generator host is required
to measure true multi-node scaling.

**This is not linear scaling yet.** The host and client cap absolute throughput,
and two architectural effects remain:

- A client request that lands on a non-owner coordinator proxies to the owner.
  With `n=1`, most requests take that extra hop, so point GET throughput stays
  roughly flat instead of rising with nodes.
- `_all_docs` and views are scatter-gather operations: every request touches
  every node and the coordinator merges the result. Adding nodes increases the
  work per request, not the number of independent requests the coordinator can
  serve.
- Writes are now sent directly to every replica in parallel; batches are still
  split into per-owner requests, but there is no fetch-and-copy pass.

The nginx front is deliberately simple round-robin/least-connection routing. It
does not know which node owns a document, matching CouchDB’s deployment model.
With `n=3` on three nodes every node is a replica, so point reads are local; the
remaining scaling limit in these runs is the shared client/server host. Cold
view builds do scale: each node
maps its own shards in parallel.

### Parallel update vector (#36)

`GET /db` and `since=now` now fetch every node's update sequence concurrently
instead of one peer at a time. The vector itself is unchanged; only the
round-trip pattern changed.

| Operation | Before (#36) | After (#36) |
|---|---|---|
| `GET /db` info | 1,380 req/s, p50 0.679 ms | 1,936 req/s, p50 0.489 ms |

### Parallel full changes and grouped reduces (#37)

An unlimited `_changes` feed (initial sync or replication) now requests every
node concurrently and merges the responses in node order. Limited feeds were
later made global in #40. A full 6,000-document seed feed is ~124 ms p50.

Grouped `_count`, `_sum` and `_stats` queries are now reduced per owner and
combined by group key instead of shipping every map row to the coordinator.
Grouped tests cover both `_count` and `_sum`.

### Custom reducers (#38)

Custom JavaScript reducers are now distributed too. Each owner reduces its own
rows, and the coordinator calls the reducer again with `rereduce=true` over the
partials. Grouped custom reducers merge by group key and then rereduce the
partial values. `_approx_count_distinct` is intentionally excluded because
per-owner distinct counts cannot be combined exactly.

### Owner-only view indexes (#39)

Each node now builds its view index only over the primary shards it owns,
instead of also indexing replicas it will never return in an owner-only cluster
query. The index design marker includes the ownership bit string, so indexes
created by older builds rebuild automatically. Incremental `_changes` updates
and `include_docs` lookups apply the same owner filter.

| Operation | Before (#39) | After (#39) |
|---|---|---|
| View cold index build | 67.31 ms | 38.92 ms |
| View map, warm (reduce default) | p50 2.506 ms | p50 1.584 ms |
| View map-only, warm | p50 3.251 ms | p50 2.824 ms |
| View reduce, warm | p50 3.020 ms | p50 1.561 ms |

### Global limited changes (#40)

A limited `_changes` feed now queries every node with the requested limit and
walks the responses round-robin, consuming each node's changes as a prefix and
stopping at the global limit of unique documents. `last_seq` advances only over
rows actually consumed, so the next poll resumes safely. Replica duplicates
advance the source node's progress but are emitted once.

The cost is more work per limited poll: a 1,000-row feed fetches up to 1,000
changes from each node. It moved from ~12 ms to ~19 ms, but it now covers the
whole cluster fairly instead of filling the window from one node at a time.

### Direct replica writes (#42)

New edits are now sent to every replica in the document's shard set in
parallel; the coordinator waits for `w` acknowledgements and returns 201/202.
This removes the old primary-write → `_bulk_get` → secondary-copy cycle. Design
documents still go to every node, and `new_edits=false` replication writes still
go to every replica.

| Operation | Before (#42), n=2 | After, n=2 | After, n=3 |
|---|---:|---:|---:|
| Single PUT, sequential | 1,159 req/s | 1,685 | 1,374 |
| `_bulk_docs`, 250 docs/request | 12,177 docs/s | 35,339 | 29,562 |
| `_bulk_docs` p50 | 19.8 ms | 6.8 ms | 7.8 ms |

## Reading the numbers

- **Writes pay for `n=3`.** A sequential PUT is ~1.4k/s versus ~3.8k/s on one
  node; bulk ingestion is ~30k docs/s versus ~37k/s. Documents are written to
  all three replicas in parallel and the request waits for `w`; there is no
  primary fetch-and-copy pass.
- **Single-document reads** are ~6.1k/s sequentially and ~7.9k/s with eight
  clients. With n=3 on three nodes every coordinator holds every document.
- **Bulk reads** reach ~36k docs/s; `_bulk_get` is routed per document and is
  still the fastest way to read many documents through a coordinator.
- **`_all_docs`** is ~1.3 ms with or without a `startkey`. Each owner answers
  its offset from the in-memory live-document map instead of walking earlier
  metadata.
- **Warm views** are ~1.6–2.8 ms. The remaining cost is mostly coordinator
  merge and JSON/HTTP for the selected rows. Cold builds are ~39 ms because
  each node indexes only its primary shards.
- **`_changes`** is ~19 ms for a 1,000-row limited feed; a full 6,000-document
  feed is ~124 ms with nodes queried concurrently. Limited feeds now merge
  every node's prefix round-robin up to the global limit.
- **`GET /db`** is ~0.5 ms because it fetches the per-node update-sequence
  vector concurrently with small responses.

## Caveats and next steps

- No TLS, authentication, compression or proxy is in the path.
- Anti-entropy is effectively disabled (`--sync-secs 3600`) so the numbers
  measure request routing and synchronous replication. A node that dies
  mid-write is healed by the background replicator when it returns.
- The coordinator and all peers run on the same host, so loopback and shared CPU
  are part of the measurement.
- `_approx_count_distinct` still uses the map-row merge path because partial
  distinct counts cannot be combined.
- Every node keeps its live `_all_docs` map in memory after the first range
  query. That is what removes the offset scan, but large databases should budget
  for it.

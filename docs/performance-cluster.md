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
| Cluster | 3 nodes, `q=4`, `n=2`, one process per node |
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

## Results

These are the numbers after the cluster fan-out (#32) and live-document index
(#33) passes. Latency is in milliseconds. Throughput for bulk operations is
documents per second.

| Operation | n | Throughput | p50 | p99 |
|---|---:|---:|---:|---:|
| Single PUT, sequential | 500 | 1,159 req/s | 0.856 | 1.185 |
| Single PUT, 8 writers | 500 | 3,676 req/s | 2.012 | 3.680 |
| Single GET, sequential | 1,000 | 3,558 req/s | 0.283 | 0.623 |
| Single GET, 8 writers | 500 | 6,738 req/s | 0.805 | 3.650 |
| `_bulk_docs`, 250 docs/request | 2,500 | 12,177 docs/s (49 req/s) | 19.817 | 21.360 |
| `_bulk_get`, 250 docs/request | 2,500 | 39,380 docs/s (158 req/s) | 5.897 | 6.968 |
| `_all_docs?limit=100` | 50 | 489 req/s | 1.832 | 2.506 |
| `_all_docs?startkey=doc-005000&limit=100` | 50 | 524 req/s | 1.861 | 2.572 |
| `_all_docs` with 100 keys | 20 | 586 req/s | 1.647 | 1.934 |
| `_changes?since=0&limit=1000` | 20 | 105 req/s | 9.417 | 10.470 |
| `GET /db` info | 100 | 1,380 req/s | 0.679 | 1.034 |
| View map, cold index build | 1 | — | 67.31 | — |
| View map, warm (reduce default) | 20 | 377 req/s | 2.506 | 3.430 |
| View map-only, warm | 20 | 284 req/s | 3.251 | 4.625 |
| View reduce, warm | 20 | 329 req/s | 3.020 | 3.468 |

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
- **Distributed built-in reduces.** Global `_count`, `_sum` and `_stats` without
  grouping are reduced per owner and combined by the coordinator, instead of
  shipping every map row to the coordinator.

### Live-document index (#33)

Each node builds its ascending `_all_docs` live-document map lazily on the first
range query and maintains it in memory. Owner-only range queries no longer walk
metadata to compute their local offsets, including with a far-away `startkey`.

| Operation | Before (#33) | After (#33) |
|---|---|---|
| `_all_docs?limit=100` | p50 2.23 ms | p50 1.83 ms |
| `_all_docs?startkey=doc-005000&limit=100` | 118 req/s, p50 8.27 ms | 524 req/s, p50 1.86 ms |
| `_bulk_docs`, 250 docs/request | 11,956 docs/s | 12,177 docs/s |

## Reading the numbers

- **Writes pay for `n=2`.** A sequential PUT is ~1.2k/s versus ~3.5k/s on one
  node; bulk ingestion is ~12k docs/s versus ~37k/s. The coordinator writes the
  primary, fetches the revision, and copies it to the other replica. That is
  the cost of synchronous replication, not a regression in the storage path.
- **Single-document reads** are ~3.3k/s sequentially and ~7.1k/s with eight
  clients. Some requests are served locally and some proxy to the owner.
- **Bulk reads** reach ~38k docs/s; `_bulk_get` is routed per document and is
  still the fastest way to read many documents through a coordinator.
- **`_all_docs`** is ~1.8 ms with or without a `startkey`. Each owner answers
  its offset from the in-memory live-document map instead of walking earlier
  metadata.
- **Warm views** are ~2.5–3.3 ms. The remaining cost is mostly coordinator
  merge and JSON/HTTP for the selected rows. Cold builds are ~67 ms because
  every node still indexes its local replicas; only the output rows are
  owner-filtered.
- **`_changes`** is ~10 ms for a 1,000-row initial feed. The coordinator still
  polls nodes in order and stops when the limit is filled.
- **`GET /db`** is ~0.66 ms because it fans out the per-node update-sequence
  vector with small responses.

## Caveats and next steps

- No TLS, authentication, compression or proxy is in the path.
- Anti-entropy is effectively disabled (`--sync-secs 3600`) so the numbers
  measure request routing and synchronous replication. A node that dies
  mid-write is healed by the background replicator when it returns.
- The coordinator and all peers run on the same host, so loopback and shared CPU
  are part of the measurement.
- The next cold-build win is to build each node's view over owned shards only
  (or maintain a separate replica index), which would roughly halve the map work
  in this `n=2` configuration.
- `_changes` fan-out could be made concurrent and its `limit` global across all
  nodes. Grouped/custom reduces still use the map-row merge path; only global
  built-in reduces are combined today.
- Every node keeps its live `_all_docs` map in memory after the first range
  query. That is what removes the offset scan, but large databases should budget
  for it.

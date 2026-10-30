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

These are the numbers after the cluster fan-out pass (#32). Latency is in
milliseconds. Throughput for bulk operations is documents per second.

| Operation | n | Throughput | p50 | p99 |
|---|---:|---:|---:|---:|
| Single PUT, sequential | 500 | 1,217 req/s | 0.824 | 1.115 |
| Single PUT, 8 writers | 500 | 3,671 req/s | 1.891 | 3.782 |
| Single GET, sequential | 1,000 | 3,309 req/s | 0.294 | 0.741 |
| Single GET, 8 writers | 500 | 7,091 req/s | 0.850 | 2.962 |
| `_bulk_docs`, 250 docs/request | 2,500 | 11,956 docs/s (48 req/s) | 20.523 | 21.240 |
| `_bulk_get`, 250 docs/request | 2,500 | 38,223 docs/s (153 req/s) | 6.159 | 6.999 |
| `_all_docs?limit=100` | 50 | 431 req/s | 2.225 | 2.865 |
| `_all_docs?startkey=doc-005000&limit=100` | 50 | 118 req/s | 8.271 | 9.945 |
| `_all_docs` with 100 keys | 20 | 606 req/s | 1.574 | 1.818 |
| `_changes?since=0&limit=1000` | 20 | 100 req/s | 9.819 | 10.450 |
| `GET /db` info | 100 | 1,444 req/s | 0.661 | 0.957 |
| View map, cold index build | 1 | — | 77.77 | — |
| View map, warm (reduce default) | 20 | 399 req/s | 2.341 | 3.158 |
| View map-only, warm | 20 | 299 req/s | 3.346 | 3.543 |
| View reduce, warm | 20 | 425 req/s | 2.222 | 2.829 |

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

## Reading the numbers

- **Writes pay for `n=2`.** A sequential PUT is ~1.2k/s versus ~3.5k/s on one
  node; bulk ingestion is ~12k docs/s versus ~37k/s. The coordinator writes the
  primary, fetches the revision, and copies it to the other replica. That is
  the cost of synchronous replication, not a regression in the storage path.
- **Single-document reads** are ~3.3k/s sequentially and ~7.1k/s with eight
  clients. Some requests are served locally and some proxy to the owner.
- **Bulk reads** reach ~38k docs/s; `_bulk_get` is routed per document and is
  still the fastest way to read many documents through a coordinator.
- **`_all_docs` without a start key** is now a few milliseconds. A start key
  still costs ~8 ms because each owner must count live rows before the key to
  produce the exact cluster-wide `offset`.
- **Warm views** are ~2–3 ms. The remaining cost is mostly coordinator merge
  and JSON/HTTP for the selected rows. Cold builds are ~78 ms because every
  node still indexes its local replicas; only the output rows are owner-filtered.
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
- `_all_docs` with a far-away `startkey` still walks earlier metadata to compute
  `offset`; a live-document key index would remove that scan.

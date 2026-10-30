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
| Single GET, sequential (default r) | 1,000 | 2,341 req/s | 0.399 | 0.779 |
| Single GET, 32 writers (default r) | 1,000 | 5,492 req/s | 3.226 | 24.214 |
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
on the same host, with `n=3`, `?r=1`, and 32 concurrent GETs, measured:

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

### View index locality (#39, revised #45)

Normal cluster queries still return only owner rows, but each node’s view
index now covers every locally held replica. The owner-only index was faster
(38.9 ms cold at n=2), but it could not answer for a dead owner’s shards. The
replica-backed index restores node-loss failover. With n=3 on three nodes every
node holds every replica, but only one node is primary for each shard; indexing
every local replica makes the index about 3× larger, so cold build is ~71 ms
instead of ~28 ms. Normal queries still filter to primary rows.

At n=2 on three nodes, after replica-backed indexes: cold build ~47.5 ms, warm
map p50 ~1.91 ms, warm reduce p50 ~1.83 ms.

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

### Read quorum (#44)

Clustered point GET/HEAD now defaults to CouchDB’s read quorum,
`r = (n / 2) + 1`. The coordinator prefers its local replica when it has one,
then reads the remaining replicas concurrently and returns the winning
revision. `?r=1` is the local fast path.

| Native GET, 3 nodes, n=3 | Throughput | p50 |
|---|---:|---:|
| Default r=2 | 18,522 req/s | 1.47 ms |
| `?r=1` | 39,296 req/s | 0.68 ms |

The quorum read roughly doubles the per-request work. Use `?r=1` for read-heavy
workloads that can tolerate a single replica, or keep the default for
consistency.

### View failover (#45)

If an owner-only view fan-out hits an unavailable node, the coordinator retries
in degraded mode: it reads all local replicas from every reachable node and
merges duplicate rows, keeping the maximum emit count. This is slower, but a
view remains readable as long as each shard has one live replica. The
node-loss conformance test now expects a successful view with both documents
after killing one of three nodes.

### All-docs failover (#46)

`_all_docs` has the same degraded path. If any owner-only response fails, the
coordinator reads every live replica from every reachable node, deduplicates by
document id, and answers range/keys queries plus totals/offset from the union.
A node-loss test kills one of three nodes and still requires all 12 documents,
correct keys, a range window and a cluster-wide offset.

### Shard-map metadata and movement (#47)

Each database now carries a mem3-style shard map: one ordered replica list per
shard, persisted as `shard-map.json` and cached in memory. It defaults to the
rendezvous placement, and all routing, anti-entropy filters and owner filters
read it.

Inspect it on any node:

```sh
curl localhost:5984/_node/_local/_dbs/mydb
```

Move shards by writing a map through any node; it is persisted and broadcast to
peers:

```sh
curl -X PUT localhost:5984/_node/_local/_dbs/mydb \
  -H 'Content-Type: application/json' \
  -d '{"q":4,"node_count":3,"replicas":[[1],[1],[1],[1]]}'
```

Anti-entropy checkpoints include a hash of the shard map, so a new replica
starts from zero and pulls documents that were filtered out under the previous
placement. A conformance test pins all shards to node0, writes a document,
moves all shards to node1, waits for replication, then reads through node0 and
node1 and after killing node0.

### Fabric extraction (#48)

Cluster routing is now in `src/fabric.rs`: shard-map replica selection, quorum
reads/writes, cluster `_all_docs`/view/changes fan-out, shard-mask replica
fallback, and internal node calls (`proxy`, `forward_bulk`, `forward_json`,
replica reads).
HTTP handlers in `src/api.rs` are thin callers. This is a structural change; a
before/after run on the same three-node n=3 config showed no material
regression:

| Operation | Before `fabric.rs` | After `fabric.rs` |
|---|---:|---:|
| Sequential PUT | 1,542 req/s | 1,451 req/s |
| `_bulk_docs` | 30,302 docs/s | 32,747 docs/s |
| Sequential GET | 2,388 req/s | 2,301 req/s |
| `_all_docs?limit=100` | 481 req/s | 534 req/s |
| Cold view build | 71.3 ms | 71.8 ms |

### Automatic rebalance and cleanup (#49, #52)

`POST /_node/_local/_dbs/{db}/rebalance` computes a balanced shard map
(`(shard + replica) % node_count`), applies/broadcasts it, then:

1. reads per-shard live counts from every node,
2. waits until every new replica reports at least the old shard’s count,
3. invokes cleanup on every node, clearing local shard data it no longer owns.

`?cleanup=false` skips the wait/cleanup phase. `?timeout_ms=` bounds the
convergence wait (default 30 s); timeout returns 504 and does not delete old
data. The lower-level `POST /_node/_local/_dbs/{db}/cleanup` endpoint remains
available for controlled/manual runs.

Two tests cover both shapes: the manual move from node0 to node1 (node0 local
docs become 0, node1 9), and the automatic planner+cleanup path (sum of local
shard docs stays 9 and every non-owned shard is empty).

### Shard-mask degraded reads (#54)

Owner reads still use disjoint owner masks. If an owner is unreachable, the
coordinator computes the shards that owner was responsible for and asks one
live replica per missing shard for only that shard set. Both `_all_docs` and
views accept the internal `shards=` mask; local and remote nodes filter to the
selected shards before returning rows. The full-database fallback scan is gone.

The conformance test pins all shards to node0, writes 24 documents and a design
document, then queries each shard mask individually. Shard masks are disjoint,
their `total_rows` sum to the full database, and the union of `_all_docs` rows
matches the union of view rows (design documents are excluded from views).
Node-loss tests continue to verify that `_all_docs` and views return complete
results from replicas.

### Fabric and shard-map metrics (#54)

With `--otel-endpoint` (or `OTEL_EXPORTER_OTLP_ENDPOINT`) set, douchdb exports:

| Metric | Meaning |
|---|---|
| `fabric.local_reads` | point reads served by the local replica |
| `fabric.remote_reads` | point reads sent to a peer replica |
| `fabric.quorum_failures` | reads/writes that did not reach `r`/`w` |
| `fabric.degraded_reads` | fallbacks caused by an unreachable node |
| `fabric.fanout` | nodes contacted per fan-out operation |
| `shard_map.updates` | external shard-map updates |
| `shard_map.pending` | updates that missed a metadata quorum |
| `rebalance.started` | rebalance requests started |
| `rebalance.completed` | rebalances that converged and cleaned |
| `rebalance.timeout` | convergence waits that returned 504 |
| `rebalance.cleaned_shards` | old shard copies cleared |

Without an endpoint these recorders are no-ops, so the normal build has no
collector dependency.

### Partitioned databases (#55)

See [partitioned-databases.md](partitioned-databases.md) for the user-facing
guide. Databases created with `PUT /{db}?partitioned=true` carry
`props.partitioned: true` and enforce CouchDB partition ids: everything before
the first colon is the partition, everything after it is the document id.
Design documents are exempt. Document ids without a colon, empty partitions and
underscore-leading partitions return `illegal_docid`/`illegal_partition`.

Placement hashes the partition key rather than the full document id, so every
document in a partition shares a shard and its replicas. This makes
`_partition/{partition}/_all_docs` and partitioned views single-shard reads.

Implemented partition API:

- `GET /{db}/_partition/{partition}` — live/deleted counts and sizes.
- `GET|POST /{db}/_partition/{partition}/_all_docs` — ranges, keys, skip/limit,
  descending and `include_docs`, all scoped to the partition.
- `GET|POST /{db}/_partition/{partition}/_design/{ddoc}/_view/{view}` — map and
  reduce queries scoped to the partition.

Design documents default to partitioned views in a partitioned database.
`"options": {"partitioned": false}` makes a global design document; global
queries use the normal view path and partitioned queries reject it. Global
`_all_docs` across partitions remains available. Cluster coordinators forward
partition metadata to a replica when they do not own the partition shard, and
partition reads fan out through the normal owner masks.

Tests cover document validation, bulk per-document errors, partition counts and
tombstones, range/key/descending `_all_docs`, partitioned and global views,
reduce views, partition-based shard placement, cluster creation propagation,
n=1 owner routing, and partition queries from every coordinator.

### View reduce replica failover (#57)

Cluster reduce queries now use the same shard-mask fan-out as map views. Each
owner reduces only its shards; if an owner is unreachable, the coordinator asks
a live replica for exactly the missing shard masks and recombines those
partials. Global and grouped built-in reducers and custom `rereduce` reducers
all take this path, including partition-scoped reduces.

The node-loss view test now kills one of three nodes and verifies both the map
query and `_count`/grouped `_count` results. The partitioned cluster test kills
a node and verifies partitioned map and reduce queries still return the full
partition.

### Changes replica failover (#53)

Cluster `_changes` now skips unavailable nodes and continues with reachable
replicas in both limited and full feeds. The failed node’s version-vector
component remains at its last checkpoint; when the node returns it resumes from
there. Surviving replicas can supply the replicated changes meanwhile.

The node-loss test kills one of three nodes, writes a new document, and resumes
`_changes` from the pre-kill `last_seq`; the new change is present. The existing
kill-mid-replication test now expects `_changes` to succeed from replicas.

### Isolated native scaling (#50)

This run pins two CPUs per server and a disjoint client CPU set, removing the
client/server contention from the earlier single-host measurements:

```sh
cargo build --release --bin douchdb --bin loadgen
python3 scripts/perf_isolated.py --nodes 1,2,3
```

Config: `q=4`, `n=3`, 12,000 docs, 64 concurrent GETs, five rounds, medians.
Server CPUs: `0-1`, `2-3`, `4-5`; client CPUs: `8-15`.

| Nodes | `?r=1` req/s | p50 | default r req/s | p50 |
|---|---:|---:|---:|---:|
| 1 | 19,521 | 3.22 ms | 19,040 | 3.25 ms |
| 2 | 34,971 | 1.62 ms | 11,885 | 5.27 ms |
| 3 | 45,644 | 0.97 ms | 14,738 | 4.51 ms |

With local reads (`?r=1`), throughput scales from 19.5k to 45.6k req/s at three
nodes (2.34×), and p50 latency falls from 3.2 to 1.0 ms. Default quorum reads
do local + remote replica work, so their throughput falls when the second
replica is reached; that is replicas/quorum work, not evidence that the HTTP
transport alone is the bottleneck.

**Transport decision:** do not rewrite the internal transport yet. The local
path scales, and the default-r gap is explained by reading two replicas. Keep
`?r=1` as the read-heavy fast path and revisit an internal RPC/HTTP-2 transport
only after an off-host, multi-client run shows transport cost dominating the
local path.

### Versioned shard maps and reconciliation (#51)

Shard maps now carry a monotonic `version`. External updates increment it,
peers reject older versions, and the PUT response includes `"pending": true`
when fewer than a metadata quorum acknowledged. The coordinator persists a
`pending-shard-map.json`, and anti-entropy reconciles versions with peers:
newer maps are pulled, older peers are pushed, and the pending marker clears
once a metadata quorum acknowledges. A test takes two of three nodes down,
observes a pending version-3 update, restarts one peer, and verifies the map
replicates and the pending file disappears.

## Reading the numbers

- **Writes pay for `n=3`.** A sequential PUT is ~1.4k/s versus ~3.8k/s on one
  node; bulk ingestion is ~30k docs/s versus ~37k/s. Documents are written to
  all three replicas in parallel and the request waits for `w`; there is no
  primary fetch-and-copy pass.
- **Single-document reads** use quorum by default: ~2.3k/s sequentially and
  ~5.5k/s with 32 clients. `?r=1` reads one local replica and reaches ~39k/s
  with the native client. With n=3 on three nodes every coordinator holds every
  document.
- **Bulk reads** reach ~36k docs/s; `_bulk_get` is routed per document and is
  still the fastest way to read many documents through a coordinator.
- **`_all_docs`** is ~1.3–1.5 ms with or without a `startkey`. Each owner
  answers its offset from the in-memory live-document map instead of walking
  earlier metadata. If an owner is down, the coordinator asks a live replica
  for exactly the missing shard mask (`?shards=0,2`), so it does not rescan the
  whole database.
- **Warm views** are ~1.6–2.8 ms. The remaining cost is mostly coordinator
  merge and JSON/HTTP for the selected rows. Cold builds are ~47–72 ms because
  each node indexes every locally held replica, which is what enables node-loss
  failover. Map and reduce queries both fall back to replicas when an owner is
  unreachable.
- **`_changes`** is ~19 ms for a 1,000-row limited feed; a full 6,000-document
  feed is ~124 ms with nodes queried concurrently. Limited feeds merge every
  node's prefix round-robin up to the global limit. Unavailable nodes are
  skipped and covered by replicas; their vector component resumes when they
  return.
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

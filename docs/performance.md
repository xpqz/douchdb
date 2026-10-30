# Performance baseline: single node

A reproducible HTTP benchmark for one douchdb node. This is a baseline, not a
capacity claim: it measures the current implementation on one machine with a
warm local filesystem and no authentication. Multi-node numbers are in
[performance-cluster.md](performance-cluster.md).

## Environment

| | |
|---|---|
| Date | 2026-10-30 |
| CPU | AMD Ryzen 7 5700U, 8 cores / 16 threads |
| Memory | 28 GiB |
| OS | Linux 7.2.8 / glibc 2.44 |
| Build | `cargo build --release` |
| Server | single node, `--q 1 --n 1`, no `--admin`, sync writes enabled |
| Client | Python 3.14 `http.client`, keep-alive, local loopback |
| Seed data | 6,000 small documents; ~13,000 total at metadata/view points |
| Large docs | 10 × 1 MiB bodies, measured separately |

## Reproduce

```sh
cargo build --release
python3 scripts/perf_sweep.py --docs 6000
```

The script starts its own server on an ephemeral port, writes
`target/perf-baseline.json`, and uses `target/perf-data` for storage. It does not
reuse an existing server.

## Results

These are the numbers after the metadata scan (#30), view map (#31) and
live-document index (#33) passes. Latency is in milliseconds. Throughput for bulk operations is documents per
second; the request rate is shown separately.

| Operation | n | Throughput | p50 | p99 |
|---|---:|---:|---:|---:|
| Single PUT, sequential | 500 | 3,782 req/s | 0.211 | 0.362 |
| Single PUT, 8 writers | 1,000 | 5,867 req/s | 1.226 | 3.046 |
| Single PUT, 32 writers | 1,000 | 5,378 req/s | 4.749 | 17.202 |
| Single PUT, 64 writers | 1,000 | 4,754 req/s | 9.773 | 40.251 |
| Single GET, sequential | 1,000 | 5,943 req/s | 0.158 | 0.263 |
| Single GET, 8 writers | 1,000 | 7,893 req/s | 0.601 | 4.585 |
| Single GET, 32 writers | 1,000 | 7,246 req/s | 2.254 | 18.250 |
| Single GET, 64 writers | 1,000 | 6,819 req/s | 4.344 | 36.596 |
| `_bulk_docs`, 250 docs/request | 2,500 | 37,040 docs/s (148 req/s) | 6.047 | 6.873 |
| `_bulk_get`, 250 docs/request | 2,500 | 51,179 docs/s (205 req/s) | 4.466 | 4.757 |
| `_all_docs?limit=100` | 50 | 1,204 req/s | 0.394 | 0.613 |
| `_all_docs?startkey=doc-005000&limit=100` | 50 | 2,987 req/s | 0.325 | 0.424 |
| `_changes?since=0&limit=1000` | 20 | 117.3 req/s | 8.512 | 8.638 |
| `GET /db` info | 200 | 6,264 req/s | 0.138 | 0.369 |
| View map, cold index build | 1 | — | 99.54 | — |
| View map, warm (reduce default) | 20 | 2,753 req/s | 0.324 | 0.584 |
| View map-only, warm | 20 | 2,331 req/s | 0.428 | 0.445 |
| View reduce, warm | 20 | 3,910 req/s | 0.256 | 0.281 |
| 1 MiB document PUT | 10 | — | 6.491 | 13.110 |
| 1 MiB document GET | 10 | — | 3.200 | 3.857 |

### Metadata scan pass (#30)

The three metadata endpoints were the target of this pass. Values are from the
same host, data shape and script; the "before" column is the #29 baseline.

| Operation | Before (#29) | After (#30) |
|---|---|---|
| `_all_docs?limit=100` | 41.5 req/s, p50 23.92 ms | 1,433 req/s, p50 0.63 ms |
| `_changes?since=0&limit=1000` | 16.0 req/s, p50 62.65 ms | 114 req/s, p50 8.78 ms |
| `GET /db` info | 50.5 req/s, p50 19.74 ms | 6,376 req/s, p50 0.14 ms |

Warm views also improved as a side effect: every view query calls `GET /db` for
the current update sequence, and that now reads counters instead of scanning all
metadata. Single-document write throughput is within run-to-run variance; the
counters add one metadata read and JSON decode per written document, which is
small relative to the group-commit flush. rusty-leveldb does not fsync; the
write numbers are OS-buffered, not power-loss durable.

### View map pass (#31)

Views now build map rows from raw stored bodies, batch QuickJS calls, keep a
sorted in-memory row snapshot, and avoid allocating key arrays for built-in
reducers. Values are from the same host, data shape and script; the "before"
column is the #30 baseline.

| Operation | Before (#30) | After (#31) |
|---|---|---|
| View cold index build | 342.7 ms | 115.7 ms |
| View map, warm (reduce default) | 56.1 req/s, p50 17.02 ms | 2,999 req/s, p50 0.31 ms |
| View map-only, warm | 65.8 req/s, p50 15.03 ms | 2,373 req/s, p50 0.40 ms |
| View reduce, warm | 57.0 req/s, p50 16.89 ms | 4,746 req/s, p50 0.20 ms |

The cold build no longer parses every body into `Value` and serializes it again
before mapping: `_id`/`_rev` are spliced onto the stored JSON, documents are
mapped in batches, and the rebuilt index seeds the sorted row cache directly.
Warm queries reuse the cached snapshot, so they no longer scan, parse, and sort
every index row per request. The cache holds one `Row` per emitted map row per
open view index.

### Live-document index (#33)

Ascending `_all_docs` range queries now use a sorted in-memory map of live
document ids to winning revisions. It is built lazily on the first range query
per shard and updated on writes, so there is no extra LevelDB write on the
ingestion path.

| Operation | Before (#33) | After (#33) |
|---|---|---|
| `_all_docs?limit=100` | p50 0.637 ms | p50 0.366 ms |
| `_all_docs?startkey=doc-005000&limit=100` | 92.5 req/s, p50 10.84 ms | 2,317 req/s, p50 0.376 ms |
| `_bulk_docs`, 250 docs/request | 38,239 docs/s | 37,457 docs/s |

A far-away `startkey` no longer walks and decodes metadata to compute `offset`;
the index answers it with a BTreeMap range count. Descending `_all_docs` still
uses the metadata scan. The first range query on a shard pays a one-time
metadata scan to build the map; the numbers above include it. The index adds no
LevelDB writes; after it is built, writes pay an in-memory BTreeMap update
(bulk ingestion measured within ~2% of the previous throughput).

### View builds from the live map (#35)

Cold view builds now take their document ids and winning revisions from the
live-document map instead of scanning and decoding every `DocMeta` again. They
still read one body per document and fall back to the full document shape only
for revisions with attachments.

| Operation | Before (#35) | After (#35) |
|---|---|---|
| View cold index build | 115.1 ms | 99.5 ms |

The remaining cold cost is body reads, QuickJS mapping, and index writes. In
the cluster the change was within run-to-run variance; body reads and parallel
index builds dominate there.

Storage at the end of the run:

```json
{
  "data_dir_bytes_before_compact": 15984667,
  "data_dir_bytes_after_compact": 15996605,
  "compact_seconds": 0.093
}
```

## Reading the numbers

- **Single-document writes** are dominated by the group-commit flush path.
  rusty-leveldb does not fsync by default, so these are OS-buffered writes, not
  power-loss durable. With
  one client they reach ~3.6k/s; eight concurrent clients get ~5.9k/s. More than
  eight writers mostly increases queueing latency (p99 3 ms → 41 ms) rather than
  throughput. Live/deleted counters are updated in the same batch as the
  metadata, so document writes pay one extra metadata read and decode.
- **Reads** are very cheap (~0.15 ms p50 sequential) and scale to ~8k/s. At 64
  clients p99 is 33 ms, mostly scheduling/queueing on the client host.
- **Bulk is the ingestion path**: 250-document batches sustain ~37k docs/s for
  writes and ~51k docs/s for reads. This is why cluster bulk paths are batched.
- **Metadata endpoints no longer scan everything.** `GET /db` reads two
  per-shard counters. Ascending `_all_docs` range queries read a sorted
  in-memory live-document map, so they skip deleted metadata entirely and
  compute `offset` with a range count even for a far-away `startkey`.
  `_all_docs` keys queries fetch each metadata record directly. The live map is
  built lazily per shard and costs memory proportional to the number of live
  documents; descending ranges still decode metadata.
- **`_changes` with `since=0`** now reads sequence entries only until `limit`
  matches and returns 1,000 rows in ~8.8 ms instead of scanning the whole
  sequence index (~63 ms). Incremental `since` values avoid most of that work.
- **Views** build the persistent index on the first query (~100 ms for ~13k
  documents), then warm map queries are ~0.4 ms and reduce queries ~0.3 ms.
  Cold builds read bodies directly and batch QuickJS calls; warm queries reuse a
  sorted in-memory row snapshot instead of scanning, parsing and sorting the
  index on every request. The snapshot is invalidated and updated incrementally
  when `_changes` reports new document revisions.
- **Large documents** are fast on loopback: ~6.3 ms p50 for a 1 MiB PUT, ~3.0 ms
  for a GET.
- **Compaction** is negligible on this small, mostly append-only dataset
  (0.1 s). The 10 GB spike in [PLAN.md](../PLAN.md#storage-spike) is the relevant
  compaction measurement: a shard stalls for ~27 s while `compact_range` owns
  the actor.

## Caveats

- Python and loopback are part of the measurement; a Rust or native client may
  show different throughput. Multiple clients on one host also compete for CPU.
- The filesystem/device cache is warm. Cold-disk and large-database numbers will
  be lower.
- No TLS, authentication, compression or proxy is in the path.
- `q=1` isolates the single shard actor. With `q=4`, metadata scans and the
  view query fan out across shards and can use more cores.
- This is a snapshot on one machine, not an SLO. Run the same script on the
  target hardware before sizing a deployment.
- Open view indexes keep their sorted row snapshot in memory, and each shard
  keeps its live `_all_docs` map in memory once a range query has used it. Those
  are what make reads cheap, but a deployment with many large views or databases
  should account for the memory.

## Future optimizations suggested by the baseline

1. Reduce cold view build cost further: the remaining time is mostly metadata
   decode plus one body read per document, then index writes.
2. Reduce queueing latency under high write concurrency, or expose a batch
   write endpoint for more than 250 documents.
3. Use the live-document map for descending `_all_docs` and for view builds,
   which still decode metadata; or persist it if the startup scan/memory
   trade-off becomes limiting.
4. Re-measure with a native keep-alive client to separate client overhead from
   server latency.

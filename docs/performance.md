# Performance baseline: single node

A reproducible HTTP benchmark for one douchdb node. This is a baseline, not a
capacity claim: it measures the current implementation on one machine with a
warm local filesystem and no authentication.

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

These are the numbers after the `_changes`/`_all_docs` metadata pass (issue
#30). Latency is in milliseconds. Throughput for bulk operations is documents
per second; the request rate is shown separately.

| Operation | n | Throughput | p50 | p99 |
|---|---:|---:|---:|---:|
| Single PUT, sequential | 500 | 3,770 req/s | 0.209 | 0.424 |
| Single PUT, 8 writers | 1,000 | 5,845 req/s | 1.208 | 2.969 |
| Single PUT, 32 writers | 1,000 | 5,371 req/s | 4.708 | 16.020 |
| Single PUT, 64 writers | 1,000 | 4,881 req/s | 9.817 | 39.431 |
| Single GET, sequential | 1,000 | 6,083 req/s | 0.154 | 0.240 |
| Single GET, 8 writers | 1,000 | 8,012 req/s | 0.590 | 4.752 |
| Single GET, 32 writers | 1,000 | 7,441 req/s | 2.279 | 16.739 |
| Single GET, 64 writers | 1,000 | 6,864 req/s | 5.435 | 33.476 |
| `_bulk_docs`, 250 docs/request | 2,500 | 37,744 docs/s (151 req/s) | 5.981 | 6.748 |
| `_bulk_get`, 250 docs/request | 2,500 | 51,130 docs/s (204 req/s) | 4.561 | 5.263 |
| `_all_docs?limit=100` | 50 | 1,433 req/s | 0.631 | 1.064 |
| `_changes?since=0&limit=1000` | 20 | 113.9 req/s | 8.780 | 8.970 |
| `GET /db` info | 200 | 6,376 req/s | 0.137 | 0.303 |
| View map, cold index build | 1 | — | 320.58 | — |
| View map, warm | 20 | 56.5 req/s | 17.665 | 18.650 |
| View reduce, warm | 20 | 57.2 req/s | 17.434 | 18.104 |
| 1 MiB document PUT | 10 | — | 5.642 | 10.532 |
| 1 MiB document GET | 10 | — | 3.168 | 3.854 |

### Before and after

The three metadata endpoints were the target of this pass. Values are from the
same host, data shape and script; the "before" column is the #29 baseline.

| Operation | Before (#29) | After (#30) |
|---|---|---|
| `_all_docs?limit=100` | 41.5 req/s, p50 23.92 ms | 1,433 req/s, p50 0.63 ms |
| `_changes?since=0&limit=1000` | 16.0 req/s, p50 62.65 ms | 114 req/s, p50 8.78 ms |
| `GET /db` info | 50.5 req/s, p50 19.74 ms | 6,376 req/s, p50 0.14 ms |
| View map, warm | 27.4 req/s, p50 35.37 ms | 56.5 req/s, p50 17.67 ms |
| View reduce, warm | 27.8 req/s, p50 35.28 ms | 57.2 req/s, p50 17.43 ms |

Warm views improved as a side effect: every view query calls `GET /db` for the
current update sequence, and that now reads counters instead of scanning all
metadata. Single-document write throughput is within run-to-run variance; the
counters add one metadata read and JSON decode per written document, which is
small relative to the group-commit fsync.

Storage at the end of the run:

```json
{
  "data_dir_bytes_before_compact": 15984647,
  "data_dir_bytes_after_compact": 15996605,
  "compact_seconds": 0.095
}
```

## Reading the numbers

- **Single-document writes** are dominated by the group-commit fsync path. With
  one client they reach ~3.8k/s; eight concurrent clients get ~5.8k/s. More than
  eight writers mostly increases queueing latency (p99 3 ms → 39 ms) rather than
  throughput. Live/deleted counters are updated in the same batch as the
  metadata, so document writes pay one extra metadata read and decode.
- **Reads** are very cheap (~0.15 ms p50 sequential) and scale to ~8k/s. At 64
  clients p99 is 33 ms, mostly scheduling/queueing on the client host.
- **Bulk is the ingestion path**: 250-document batches sustain ~38k docs/s for
  writes and ~51k docs/s for reads. This is why cluster bulk paths are batched.
- **Metadata endpoints no longer scan everything.** `GET /db` reads two
  per-shard counters, and `_all_docs` range queries stop after `skip + limit`
  live rows. `_all_docs` keys queries fetch each metadata record directly
  instead of scanning the database. A range with a `startkey` still walks
  metadata before the start key to compute `offset`; the common case without a
  `startkey` does not.
- **`_changes` with `since=0`** now reads sequence entries only until `limit`
  matches and returns 1,000 rows in ~8.8 ms instead of scanning the whole
  sequence index (~63 ms). Incremental `since` values avoid most of that work.
- **Views** build the persistent index on the first query (~321 ms for ~13k
  documents), then warm queries are ~17 ms. Warm queries still scan and sort the
  index rows; the index avoids re-running JavaScript, not the sort.
- **Large documents** are fast on loopback: ~5.6 ms p50 for a 1 MiB PUT, ~3.2 ms
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

## Future optimizations suggested by the baseline

1. Keep a sorted view row cache instead of sorting every warm query.
2. Reduce queueing latency under high write concurrency, or expose a batch
   write endpoint for more than 250 documents.
3. Maintain a live-document key index so `_all_docs` with a far-away
   `startkey` can compute `offset` without walking earlier metadata.
4. Re-measure with a native keep-alive client to separate client overhead from
   server latency.

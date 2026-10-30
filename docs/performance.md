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

Latency is in milliseconds. Throughput for bulk operations is documents per
second; the request rate is shown separately.

| Operation | n | Throughput | p50 | p99 |
|---|---:|---:|---:|---:|
| Single PUT, sequential | 500 | 3,590 req/s | 0.223 | 0.361 |
| Single PUT, 8 writers | 1,000 | 5,952 req/s | 1.197 | 3.057 |
| Single PUT, 32 writers | 1,000 | 5,414 req/s | 4.684 | 16.805 |
| Single PUT, 64 writers | 1,000 | 5,227 req/s | 9.375 | 33.849 |
| Single GET, sequential | 1,000 | 5,817 req/s | 0.156 | 0.313 |
| Single GET, 8 writers | 1,000 | 8,332 req/s | 0.470 | 3.896 |
| Single GET, 32 writers | 1,000 | 7,730 req/s | 2.422 | 15.408 |
| Single GET, 64 writers | 1,000 | 7,099 req/s | 4.644 | 33.293 |
| `_bulk_docs`, 250 docs/request | 2,500 | 35,756 docs/s (143 req/s) | 4.593 | 5.193 |
| `_bulk_get`, 250 docs/request | 2,500 | 47,947 docs/s (191 req/s) | 4.731 | 5.230 |
| `_all_docs?limit=100` | 50 | 41.5 req/s | 23.92 | 28.83 |
| `_changes?since=0&limit=1000` | 20 | 16.0 req/s | 62.65 | 62.94 |
| `GET /db` info | 200 | 50.5 req/s | 19.74 | 20.47 |
| View map, cold index build | 1 | — | 322.95 | — |
| View map, warm | 20 | 27.4 req/s | 35.37 | 39.81 |
| View reduce, warm | 20 | 27.8 req/s | 35.28 | 38.35 |
| 1 MiB document PUT | 10 | — | 5.39 | 12.22 |
| 1 MiB document GET | 10 | — | 2.97 | 3.33 |

Storage at the end of the run:

```json
{
  "data_dir_bytes_before_compact": 15798238,
  "data_dir_bytes_after_compact": 15809122,
  "compact_seconds": 0.105
}
```

## Reading the numbers

- **Single-document writes** are dominated by the group-commit fsync path. With
  one client they reach ~3.6k/s; eight concurrent clients get ~6k/s. More than
  eight writers mostly increases queueing latency (p99 3 ms → 34 ms) rather than
  throughput.
- **Reads** are very cheap (~0.15 ms p50 sequential) and scale to ~8k/s. At 64
  clients p99 is 33 ms, mostly scheduling/queueing on the client host.
- **Bulk is the ingestion path**: 250-document batches sustain ~36k docs/s for
  writes and ~48k docs/s for reads. This is why cluster bulk paths are batched.
- **Metadata endpoints are O(database size)** in this implementation:
  `_all_docs` scans metadata to build the sorted index even with `limit=100`
  (~24 ms), and `GET /db` counts every document (~20 ms). Plan accordingly for
  large databases.
- **`_changes` with `since=0`** scans the sequence index and returns 1,000 rows
  in ~63 ms. Incremental `since` values avoid most of that scan.
- **Views** build the persistent index on the first query (~323 ms for ~13k
  documents), then warm queries are ~35 ms. Warm queries still scan and sort the
  index rows; the index avoids re-running JavaScript, not the sort.
- **Large documents** are fast on loopback: ~5 ms p50 for a 1 MiB PUT, ~3 ms for
  a GET.
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

1. Avoid full metadata scans for `GET /db` and `_all_docs` counts.
2. Keep a sorted view row cache instead of sorting every warm query.
3. Reduce queueing latency under high write concurrency, or expose a batch
   write endpoint for more than 250 documents.
4. Re-measure with a native keep-alive client to separate client overhead from
   server latency.

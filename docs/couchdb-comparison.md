# Docker comparison: CouchDB vs douchdb

A direct 3-node Docker comparison of Apache CouchDB 3.5.3 and douchdb at
commit `13ffcef`. Both clusters use the same shape, resource limits, document
set and client.

## Setup

| | CouchDB | douchdb |
|---|---|---|
| Image / build | `couchdb:3.5` (3.5.3) | `douchdb:bench` from this tree |
| Nodes | 3 | 3 |
| Q | 8 | 8 |
| N | 3 | 3 |
| Per node | 2 CPUs, 2 GiB | 2 CPUs, 2 GiB |
| Storage | named Docker volume | named Docker volume |
| Auth | `admin:password` | `admin:secret` |
| Front end | none; client round-robins nodes | none; client round-robins nodes |
| Anti-entropy | CouchDB defaults | `--sync-secs 3600` (writes are synchronous n=3) |

Compose files:

```sh
docker compose -f docker/bench/couchdb.yml up -d
docker compose -f docker/bench/douchdb.yml up -d --build
```

CouchDB needs its nodes joined and q/n configured before the benchmark:

```sh
for port in 15984 15985 15986; do
  for node in 10.99.0.11 10.99.0.12 10.99.0.13; do
    curl -s -u admin:password -X PUT -H 'Content-Type: application/json' \
      -d '{}' "http://127.0.0.1:$port/_node/_local/_nodes/couchdb@$node" >/dev/null
  done
  for pair in q:8 n:3; do
    key=${pair%%:*}; value=${pair##*:}
    curl -s -u admin:password -X PUT -H 'Content-Type: application/json' \
      -d "\"$value\"" "http://127.0.0.1:$port/_node/_local/_config/cluster/$key" >/dev/null
  done
done
```

Run one cluster at a time, so the other cluster does not consume host CPU:

```sh
docker compose -f docker/bench/douchdb.yml stop
python3 scripts/docker_compare.py --engine couchdb --json-out couch.json

docker compose -f docker/bench/couchdb.yml stop
docker compose -f docker/bench/douchdb.yml up -d
python3 scripts/docker_compare.py --engine douchdb --json-out douch.json
```

## Workload

- 6,000 seeded documents, each with a numeric `value` and 100-byte `text`.
- 500 sequential PUTs, 500 concurrent PUTs (8 workers).
- 2,500 documents via ten 250-document `_bulk_docs`.
- Point reads, `_bulk_get`, `_all_docs` ranges/keys, `_changes`, views and
  database info.
- Views warmed on every coordinator before measurement.
- Changes indices warmed on every coordinator before measurement.
- Two runs per engine; the values below are averages. The second run of each
  engine was measured with the other cluster stopped.

Host: 16 CPUs, 30 GiB RAM, Docker 29.9.2, Linux. Client runs on the host and
talks to the published node ports through the Docker bridge.

## Durability caveat

**The write comparison is not durability-equivalent.** CouchDB 3.x removed
`delayed_commits`; every write is a full commit (CouchDB 3.0 upgrade note
`#2208`). douchdb uses rusty-leveldb 4.0.2, whose `DB::write(batch, true)`
only flushes a `BufWriter` to the operating-system page cache. The crate has no
`fsync`, `fdatasync`, `sync_all` or `sync_data` call at all.

Measured directly with:

```sh
strace -f -e trace=fsync,fdatasync target/release/douchdb ...
curl -X PUT .../db/doc1
```

the successful PUT produced **zero** `fsync`/`fdatasync` syscalls. A process
crash preserves the data because it is in the page cache, but a host crash or
power loss can lose acknowledged writes.

Therefore:

- **Read, view, `_all_docs` and `_changes` comparisons are meaningful.**
- **Write throughput comparisons are not equal-durability.** They compare
  CouchDB full commits against douchdb OS-buffered writes.
- douchdb intentionally follows rusty-leveldb's default no-fsync behaviour.
  That is a valid deployment choice, but it means these write numbers are a
  different durability class. A durable-mode benchmark would need an fsynced
  LevelDB log or WAL; CouchDB 3.x cannot be switched back to delayed commits.

The table below keeps the measured numbers, but write rows should be read with
that caveat.

## Results

| Operation | CouchDB | douchdb | douchdb / CouchDB |
|---|---:|---:|---:|
| Sequential PUT * | 96 req/s | 1,198 req/s | **12.5×** |
| Concurrent PUT, 8 clients * | 238 req/s | 3,547 req/s | **14.9×** |
| `_bulk_docs`, 250 docs * | 6,255 docs/s | 29,984 docs/s | **4.8×** |
| Sequential GET | 743 req/s | 1,774 req/s | **2.4×** |
| Concurrent GET, 8 clients | 2,572 req/s | 4,997 req/s | **1.9×** |
| `_bulk_get`, 250 docs | 20,917 docs/s | 64,155 docs/s | **3.1×** |
| `_all_docs?limit=100` | 259 req/s | 693 req/s | **2.7×** |
| `_all_docs?startkey&limit=100` | 315 req/s | 862 req/s | **2.7×** |
| `_all_docs` keys=100 | 63 req/s | 749 req/s | **11.9×** |
| `_changes?limit=1000` | 31 req/s | 293 req/s | **9.6×** |
| Full `_changes` (9,501 rows) | 3.8 req/s | 210 req/s | **56×** |
| `_changes?style=all_docs` | 3.5 req/s | 227 req/s | **66×** |
| `_changes?include_docs` | 2.3 req/s | 46.9 req/s | **20×** |
| `style=all_docs&include_docs` | 2.3 req/s | 49.0 req/s | **21×** |
| Database info | 628 req/s | 1,338 req/s | **2.1×** |
| Cold view build | 755 ms | 52.6 ms | **14.4× faster** |
| Warm view map | 176 req/s | 410 req/s | **2.3×** |
| Warm map-only view | 127 req/s | 276 req/s | **2.2×** |
| Warm reduce view | 294 req/s | 394 req/s | **1.3×** |

Latency at p50:

| Operation | CouchDB | douchdb |
|---|---:|---:|
| Sequential PUT | 8.91 ms | 0.79 ms |
| Concurrent PUT | 28.4 ms | 1.96 ms |
| Sequential GET | 1.32 ms | 0.56 ms |
| `_all_docs` keys=100 | 14.8 ms | 1.31 ms |
| `_changes?limit=1000` | 32.4 ms | 3.26 ms |
| Full `_changes` | 255 ms | 4.30 ms |
| `_changes?include_docs` | 404 ms | 20.4 ms |
| Cold view build | 755 ms | 52.6 ms |
| Warm view map | 3.82 ms | 2.36 ms |
| Warm reduce view | 2.69 ms | 2.45 ms |

## Reading the results

* Write rows are not durability-equivalent: CouchDB fsyncs every commit while
  douchdb currently acknowledges after writing to the OS page cache.

- douchdb's replica-write fan-out is faster in this container setup, but part
  of the write gap is its default no-fsync LevelDB behaviour.
- With that caveat, the read-only and index paths show douchdb ahead by roughly
  2–3× for point/range reads, 9–10× for limited changes and much more for full
  changes. Write ratios must not be quoted as an apples-to-apples durability
  result.
- Bulk operations narrow the gap: CouchDB reaches ~6.3k docs/s versus ~30k
  docs/s for douchdb.
- Point reads are the closest path: CouchDB is within ~2×.
- View cold builds are much slower in CouchDB's external JavaScript query
  server than douchdb's embedded QuickJS build.
- Full changes feeds are the largest difference. douchdb serves them from its
  in-memory sequence index; CouchDB performs substantially more disk and
  serialization work per poll.
- `_all_docs` with explicit keys is also a large gap in CouchDB's cluster
  implementation for this workload.

## Caveats

- Single host, loopback Docker networking, no TLS or reverse proxy. Absolute
  numbers will differ on real hardware and networks; the ratios are the useful
  signal.
- CouchDB is at default container settings apart from `q`, `n` and auth.
  douchdb disables background anti-entropy in this run because writes are
  already synchronous to all replicas.
- Two runs are enough to expose large differences but not a statistical
  benchmark. CouchDB `_all_docs?limit=100` and `_bulk_get` showed the largest
  run-to-run variance (roughly ±30%).
- Basic auth was used. Session cookies changed CouchDB point-read latency by
  only a few percent, so auth is not the explanation for the large read and
  changes differences.
- douchdb follows rusty-leveldb's default and does not fsync. The write numbers
  are throughput measurements of OS-buffered commits, not durable commits.

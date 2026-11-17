# Docker comparison: CouchDB vs douchdb

A direct 3-node Docker comparison of Apache CouchDB 3.5.3 and douchdb at
commit `13ffcef`. Both clusters use the same shape, resource limits, document
set and client.

## Setup

| | CouchDB | douchdb |
|---|---|---|
| Image / build | `couchdb:3.5.3` (digest-pinned) | `douchdb:bench` from this tree |
| Nodes | 3 | 3 |
| Q | 8 | 8 |
| N | 3 | 3 |
| Per node | 2 CPUs, 2 GiB | 2 CPUs, 2 GiB |
| Storage | named Docker volume | named Docker volume |
| Auth | `COUCHDB_USER` / `COUCHDB_PASSWORD` | `DOUCHDB_ADMIN` |
| Front end | none; client round-robins nodes | none; client round-robins nodes |
| Anti-entropy | CouchDB defaults | `--sync-secs 3600` (writes are synchronous n=3) |

Compose files. The credentials have no defaults: export them first, and use the
same values for `scripts/docker_compare.py`. The ports are bound to
127.0.0.1.

```sh
export COUCHDB_PASSWORD=$(openssl rand -hex 16)
export COUCHDB_SECRET=$(openssl rand -hex 16)
export COUCHDB_ERLANG_COOKIE=$(openssl rand -hex 16)
export DOUCHDB_ADMIN=admin:$(openssl rand -hex 16)
export DOUCHDB_CLUSTER_SECRET=$(openssl rand -hex 16)
```

```sh
docker compose -f docker/bench/couchdb.yml up -d
docker compose -f docker/bench/douchdb.yml up -d --build
```

CouchDB needs its nodes joined and q/n configured before the benchmark:

```sh
for port in 15984 15985 15986; do
  for node in 10.99.0.11 10.99.0.12 10.99.0.13; do
    curl -s -u "admin:$COUCHDB_PASSWORD" -X PUT -H 'Content-Type: application/json' \
      -d '{}' "http://127.0.0.1:$port/_node/_local/_nodes/couchdb@$node" >/dev/null
  done
  for pair in q:8 n:3; do
    key=${pair%%:*}; value=${pair##*:}
    curl -s -u "admin:$COUCHDB_PASSWORD" -X PUT -H 'Content-Type: application/json' \
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

**The measured write numbers below predate the storage change and are not
durability-equivalent.** They were taken on the earlier storage engine, which
acknowledged after writing to the OS page cache without `fsync`. douchdb now
stores shards in redb and every commit is a durable two-phase commit that syncs
before the write is acknowledged, as CouchDB 3.x does (it removed
`delayed_commits`; CouchDB 3.0 upgrade note `#2208`). Write rows should be
re-measured before being quoted.

- **Read, view, `_all_docs` and `_changes` comparisons were meaningful then and
  still indicate the shape.**
- **Write throughput rows compare CouchDB full commits against the old
  OS-buffered writes.** They are not an equal-durability result.

The table below keeps the measured numbers, but write rows should be read with
that caveat.

## Reading the results

* Write rows are not durability-equivalent: they were measured before douchdb
  fsynced every commit, as CouchDB does.

- douchdb's replica-write fan-out is faster in this container setup, but part
  of the write gap was the earlier storage engine's lack of fsync.
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
- The write numbers are throughput measurements of OS-buffered commits on the
  earlier storage engine, not durable commits; douchdb now fsyncs every commit.

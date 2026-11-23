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
| Server | single node, `--q 1 --n 1`, no `--admin`; writes flushed to the OS, not fsynced (pre-redb) |
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
live-document index (#33) passes. They predate the redb storage engine: write
figures are OS-buffered, not durable on disk (see
[Storage change: redb](#storage-change-redb-2026-11)). Latency is in milliseconds. Throughput for bulk operations is documents per
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
small relative to the group-commit flush. The write numbers in this
document were measured on the earlier storage engine, which did not fsync, so
they are OS-buffered; douchdb now fsyncs every commit.

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
per shard and updated on writes, so there is no extra storage write on the
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
storage writes; after it is built, writes pay an in-memory BTreeMap update
(bulk ingestion measured within ~2% of the previous throughput).

### View builds from the live map (#35)

Cold view builds now take their document ids and winning revisions from the
live-document map instead of scanning and decoding every `DocMeta` again. They
still read one body per document.

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

## Storage change: redb (2026-11)

The redb storage engine (PR #76) acknowledges a write only once it is synced
to disk. The engine before it acknowledged writes once they were flushed to the
OS. Write throughput fell accordingly. Throughput is the median of three
rounds; the range across rounds is in parentheses.

| Workload | Nodes | Pre-redb (`cf9295a`) | redb (`12cadce`) | Ratio |
|---|---|---:|---:|---:|
| Bulk import, 150,959 docs, 250 per batch | 3, `n=3` | 9,866 docs/s (9,628–9,946), 15.3 s | 3,379 docs/s (3,326–3,421), 44.7 s | 0.34× |
| Bulk import, 150,959 docs, 250 per batch | 1 | 17,131 docs/s (16,697–17,162), 8.8 s | 7,254 docs/s (6,747–7,256), 20.8 s | 0.42× |
| Single-document PUT, 16 concurrent | 3, `n=3` | 9,466 docs/s, p50 1.5 ms | 260 docs/s, p50 58.5 ms | 0.027× |
| Single-document PUT, 16 concurrent | 1 | 18,698 docs/s, p50 0.53 ms | 786 docs/s, p50 20.3 ms | 0.042× |

Conditions: release builds of `cf9295a` (pre-redb) and `12cadce`, `q=2`,
`--sync-secs 3600`, default `--cache-mb 16`. Each configuration ran three
interleaved rounds after a discarded pilot round. The single-document PUTs
were creates only, about 260 bytes each. Apple M2 Pro, 10 cores, 32 GiB, data
on an external APFS disk, on a loaded machine.

The cause is durability, not a defect. The pre-redb engine acknowledged a
group once rusty-leveldb's `write(batch, sync=true)` returned, and that call
only flushed the log buffer to the OS: nothing in the engine called `fsync`.
redb commits each group in two phases (see [durability.md](durability.md)),
which is two `sync_data` calls per commit. On macOS Rust's `sync_data` issues
`fcntl(F_FULLFSYNC)`, and on this disk `F_FULLFSYNC` took 3.0 ms median against
0.13 ms for a plain `fsync`.

Caveats:

- Linux `fdatasync` was not measured.
- The write mix was creates only.
- Concurrent single-document writes shared commits poorly under the synced
  group commit (issue #90); see the next section.

### Single-document writes: the group linger (#90)

Concurrent single-document writers split into two cohorts that took turns.
The shard actor started the next group the instant a commit ended, about
0.4 ms before the clients that commit answered came back, so each commit
carried about half the writers (8 of 16). Each request waited about one full
commit in the queue and then its own.

The actor now lingers. After a group that answered more than one write job,
it waits before starting the next group until as many write jobs are queued
as the group answered, on top of those that were already queued when it
answered them. Without that addition, two cohorts of the same size would take
turns for ever. The wait lasts at most a tenth of the last commit, and never
more than 1 ms (`GROUP_LINGER` in `src/store.rs`).

- It waits on the queue, so the group starts the moment the jobs are there.
- A read, close or other control message ends the wait at once.
- After a group of one it never waits, so a lone writer is unchanged.
- Durability is unchanged: two syncs per commit.
- `/_node/_local/_metrics` counts the lingers (`shard.lingers`) and those
  that ran out of time (`shard.linger_timeouts`).

`loadgen --method PUT` of new documents (about 260 B) on a single node, one
level after another on a fresh server: c=1 300 writes, c=4 1,000, c=16 3,000,
after 200 warm-up writes.

- Before (`039cc34`) and after binaries ran interleaved in five rounds: three
  with before first, two with after first.
- Each cell is the median over the five rounds. T is the shard commit time
  per group.
- The machine was very heavily loaded (load average 20–56 on 10 cores), so T
  drifted between 6 and 21 ms. Read the group counts first; the rates and
  latencies are noisy.

| q | c | docs/s before → after | p50 ms | p99 ms | jobs/group | syncs/group | T ms | lingers (timed out) |
|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 78 → 72 | 9.4 → 12.6 | 35.0 → 39.0 | 1.00 → 1.00 | 2.00 → 2.00 | 10.2 → 11.7 | 1 (1) |
| 1 | 4 | 122 → 299 | 21.6 → 11.9 | 89.7 → 34.7 | 2.08 → 3.98 | 2.00 → 2.01 | 17.7 → 11.1 | 249 (2) |
| 1 | 16 | 714 → 844 | 19.3 → 15.6 | 51.6 → 101.4 | 8.47 → 15.23 | 2.00 → 2.01 | 11.3 → 14.8 | 195 (26) |
| 2 | 1 | 82 → 86 | 10.2 → 9.8 | 36.0 → 26.8 | 1.00 → 1.00 | 2.00 → 2.00 | 10.4 → 9.4 | 0 (0) |
| 2 | 4 | 147 → 188 | 21.3 → 18.1 | 67.9 → 63.0 | 1.37 → 1.63 | 2.01 → 2.01 | 17.0 → 14.9 | 294 (224) |
| 2 | 16 | 589 → 703 | 25.5 → 20.2 | 58.9 → 77.8 | 4.51 → 5.49 | 2.00 → 2.01 | 14.6 → 13.2 | 512 (488) |

- **q=1, c=4 and c=16:** the cohorts merge in every round.
  - c=4: 3.92–4.00 jobs/group, against 2.0–2.2.
  - c=16: 14.2–15.96 jobs/group, against 8.4–8.7.
  - At c=4, docs/s rose 1.5–4× per round.
  - At c=16, docs/s rose 1.4× and 2.2× in the two rounds where the after
    binary ran first. In the three where it ran second (load average 44),
    the after commits were slower (T 15–20 ms against 10–13) and docs/s was
    0.85–1.0×.
  - An earlier four-round run, with the first form of the rule (no
    addition), had c=16 at 738 → 1,243 docs/s and p50 at 16.6 → 10.1 ms.
- **c=1** is unchanged by construction. The one linger per run is the first
  write after the c=8 warm-up. The rates move with noise.
- **q=2:** most lingers run out of time. A client that one shard answered
  sends its next document to either shard, so a shard rarely gets back as
  many writes as it answered. Each such linger costs at most a tenth of a
  commit. Jobs per group still rise a little: 4.5 → 5.5 at c=16. Shards
  that share one device cost throughput separately; issue #90's measurement
  covers that.
- **Not measured:** c=64 (in the first run: 36.6 → 60.7 jobs/group), the n=3
  cluster, and mixed read/write loads, where a read that ends a linger can
  leave a group of one and so switch the linger off until a larger group
  forms.

### Bulk write path against the redb floor

`scripts/bulk_bench.py` imports the airline fixture (150,959 documents)
through `_bulk_docs` into a fresh single node, and interleaves the import with
`examples/redb_floor`. The floor is a bare redb program that writes the same
stored bytes, in the same tables and batches, with the same two-phase
`Immediate` commits. The shard figures come from the `shard` counters of
`/_node/_local/_metrics`.

The bulk write path work moved per-document work off the shard actor. It also
commits a request's shards concurrently, streams the revision hash into MD5,
opens each table once per job, and starts the local and remote cluster writes
together. It then trimmed what the actor does per document while staging a
job:

- one slot per distinct id, found by sorting, in place of three `String`-keyed
  hash maps, with each document's metadata changed in place instead of cloned
  in and out;
- the new revision formatted once, before the job, and written into a new
  document's tree directly;
- revision ids rendered as hex without a `format!` per byte;
- no pruning walk for a tree of roots only, which every new document has.

Column definitions:

- **jobs/insert** is the shard actors' job time (`shard.jobs_us`, syncs
  excluded) divided by the floor's insert time (begin to last insert, syncs
  excluded), both per document and from the same round. It is the median over
  the rounds.
- **Before** is `3210349`, measured before the job timer also counted
  beginning the transaction, so its jobs/insert reads slightly low.
- **Now** is `8dbade9`, the build with the staging trimmed. It ran in the
  same rounds as `4d1f3d1` (the work before the staging was trimmed) and the
  floor. The head was not re-measured. The later `2c03501` and `4f67e2b`
  change only an edit over a revision at position `u64::MAX`, plus a
  `checked_add` for the next position, in `prepare` (before the shard job,
  off the actor) and in the shard's staging (one add per document, on the
  actor).
- **Now ÷ 4d1f3d1** is the median of the same-round ratios of job time.
- **docs/s** is end to end, the floor's docs/s in parentheses.

| q / batch / clients | Before jobs/insert | `4d1f3d1` jobs/insert | Now jobs/insert | Now ÷ 4d1f3d1 | Now docs/s | Now syncs/commit (douchdb / floor) | Now KB/commit (douchdb / floor) |
|---|---:|---:|---:|---:|---:|---:|---:|
| q1 / 250 / 1 | 3.31 | 1.62 | **1.03** | 0.65 | 14,913 (18,275) | 2.02 / 2.02 | 882 / 897 |
| q1 / 1,000 / 1 | 3.58 | 1.84 | **1.146** | 0.62 | 25,874 (40,660) | 2.09 / 2.09 | 2,597 / 2,651 |
| q1 / 10,000 / 1 | 2.68 | 1.47 | **1.08** | 0.73 | 31,100 (53,910) | 2.81 / 2.81 | 41,591 / 41,189 |
| q2 / 250 / 1 | 3.30 | 1.56 | **0.90** | 0.64 | 8,189 (8,750) | 2.02 / 2.02 | 475 / 482 |
| q2 / 1,000 / 1 | 2.51 | 1.48 | **0.96** | 0.64 | 21,569 (27,244) | 2.08 / 2.08 | 1,311 / 1,336 |
| q2 / 10,000 / 1 | 2.03 | 1.54 | **1.14** | 0.74 | 28,205 (53,099) | 2.75 / 2.75 | 16,936 / 16,567 |
| q1 / 250 / 4 | 3.08 | 1.58 | 1.02 | 0.64 | 31,032 (17,969) | 2.07 / 2.02 | 2,210 / 897 |
| q1 / 1,000 / 4 | 3.52 | 2.22 | 1.49 | 0.67 | 41,730 (40,557) | 2.29 / 2.09 | 10,029 / 2,651 |
| q1 / 10,000 / 4 | 3.02 | 1.57 | 1.21 | 0.77 | 44,749 (53,580) | 4.60 / 2.81 | 132,846 / 41,189 |
| q2 / 250 / 4 | 3.09 | 1.06 | 0.91 | 0.76 | 15,610 (10,484) | 2.04 / 2.02 | 832 / 482 |
| q2 / 1,000 / 4 | 2.09 | 1.67 | 1.10 | 0.68 | 36,056 (36,964) | 2.23 / 2.08 | 3,700 / 1,336 |
| q2 / 10,000 / 4 | — | 1.85 | 1.62 | 0.92 | 50,506 (48,684) | 4.40 / 2.75 | 64,409 / 16,567 |

Conditions:

- Release builds, `--n 1`, `--cache-mb 16` (the floor's cache is the same).
- Apple M2 Pro, data on an external APFS disk.
- Now: 5 interleaved rounds per one-client configuration and 3 per
  four-client one, with the order of the two builds rotated each round.
- The q=1 rows ran at load averages of 2–3 and their rounds agree closely
  (q1/1,000/1: 1.12–1.16; one round in five, at 1.155, was over 1.15, and the
  median, 1.146, passes). The q=2 rows ran at load averages of 10–37 and
  single rounds vary widely (q2/10,000/1: 0.98–1.34). Read docs/s as
  indicative only. The jobs/insert ratios come from the same round, and the
  sync and byte counts are exact.

Caveat on the floor: it is not a strict lower bound.
`examples/redb_floor.rs` inserts the body, by_seq and meta rows of each
document in turn, interleaving the three tables, and its insert span also
times opening SETTINGS and the two settings inserts. douchdb writes table by
table, and its settings writes fall in `commit_us`, not `jobs_us`. Ratios near
or below 1 (the q=2 rows at 0.90 and 0.96) therefore partly reflect the floor,
and q1/1,000/1 (median 1.146) is at the limit within measurement precision,
not clearly under it. Aligning the floor and re-measuring is issue #95.

End to end, with one client, douchdb runs at 0.53-0.94x the floor's docs/s
(q1: 0.82, 0.64, 0.58; q2: 0.94, 0.79, 0.53 at 250, 1,000 and 10,000
documents), up from roughly 0.42-0.69. The gap is request work the floor does
not pay: HTTP, JSON parsing, and preparing each document (serialising its body
and hashing its revision).

What the numbers say:

- **Job time.** With one client the shard actor's median job time is now
  0.90–1.146× the floor's insert time, against a goal of 1.15×, which the
  medians meet at every size and q against this floor (see the caveat above);
  q=1 with 1,000 documents, at 1.146, is at the limit within measurement
  precision. It
  was 2.0–3.6× before the bulk write path work, and `4d1f3d1` measured
  1.47–1.84× in the same rounds (1.35–1.72× in an earlier, more loaded run).
  The trimmed staging alone cut job time by 26–38% against `4d1f3d1` in the
  same rounds.
  - A sampled profile of the shard actor at q=1, one client, splits the job
    as follows. The inserts (bodies, by_seq, metadata) are 79–89% of it.
    Staging, which includes building the tree, encoding the record and
    pruning, is 8–16%, down from 26–41%. Reading the metadata is 2–6%.
  - With four clients a group serves 2.2–3.4 requests and its job time is
    0.91–1.62× the floor's; those rows are not judged against the goal.
- **Syncs and bytes.**
  - With one client, syncs per commit equal the floor's at every size and q.
  - Bytes per commit are within 2.2% of the floor's: 1.5–2.0% under at 250
    and 1,000 documents, 1.0–2.2% over at 10,000.
  - The metadata records of a job are now written in the order their
    documents arrive, as the floor writes them, instead of in hash map order.
  - With four clients a group serves 2.2–3.4 requests (3.2 at 10,000 documents),
    so its syncs and bytes per commit are not comparable with the floor's
    single batch.
- **Sorted inserts were measured and not kept.** They were measured on
  `4d1f3d1`, before the staging was trimmed. Sorting each job's bodies by
  `(id, rev)` and its metadata by id before inserting had these effects:
  - It cut bytes per commit by 8–16% everywhere.
  - It cut job time at 10,000 documents with one client, by 16% at q=1 and
    32% at q=2 (paired medians).
  - It made q=1, 10,000 documents, four clients 23% slower. That held in 10 of
    13 paired rounds, including rounds with the build order alternated.
  - The rule for keeping it was a gain of at least 10% at 10,000 documents and
    no loss at 250 or 1,000; the four-client loss at 10,000 failed it.

## JavaScript sandbox throughput

Design-document map/reduce/search-index work can run in four places: in-process
QuickJS (`native`, the default), a worker process (`process`), or the WASI
runner under `wasmi` or Wasmtime. The wasm boundary is memory-safe and portable,
but the host runtime matters more than the JSON bridge.

Benchmark: 2,000 documents, release build, Linux x86_64, 10 warm queries.
`scripts/js_sandbox_bench.py --modes native,process,wasm-wasmi,wasm-wasmtime`.

| Metric | native | process | wasm/wasmi | wasm/wasmtime |
|---|---:|---:|---:|---:|
| cold map (2,000 docs) | 14.5 ms | 28.3 ms | 254 ms | 33.9 ms |
| cold custom reduce | 13.6 ms | 24.8 ms | 282 ms | 41.9 ms |
| cold search index | 52.1 ms | 69.7 ms | 307 ms | 81.7 ms |
| warm map p50 | 0.29 ms | 0.30 ms | 0.32 ms | 0.30 ms |
| warm custom reduce p50 | 2.70 ms | 5.09 ms | 51.9 ms | 10.8 ms |
| warm search query p50 | 1.97 ms | 1.96 ms | 1.95 ms | 2.00 ms |

Warm map and search queries use the persisted index and do not enter the
engine, so they are effectively identical. Cold index builds and custom reduces
are the costs that differ. `wasmi` interprets QuickJS and is ~17–22× slower than
native; Wasmtime JITs it and brings that down to ~2–3×. Build the optional
backend with `cargo build --release --features wasmtime` and select it with
`--js-sandbox wasm --wasm-runtime wasmtime`. Wasmtime precompiles the runner at
startup (~200 ms in release), so the first query does not pay JIT compilation.

See [quickjs-sandbox.md](quickjs-sandbox.md) for the threat model, capability
surface and deadline/memory limits.

## Reading the numbers

- **Single-document writes** are dominated by the group-commit flush path.
  These numbers predate redb and fsynced commits; they are OS-buffered writes. With
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
  compaction measurement, taken on the earlier storage engine. `_compact` now
  runs online in the background: a shard's writes pause only for the final
  catch-up and the swap. `compact_seconds`
  runs from the request until `compact_running` clears.

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

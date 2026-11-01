# douchdb — CouchDB wire-compatible database in Rust on LevelDB

Status: implemented, living design and status. Target: a small, clustered,
PouchDB-and-CouchDB compatible document store.

## Locked decisions

| Decision | Choice | Consequence |
|---|---|---|
| Compatibility | CouchDB **wire** compatibility | Revision ids must be byte-identical to CouchDB's. This forces an Erlang External Term Format encoder and order-preserving JSON. See "The rev hash". |
| Cluster membership | Static node list from config | No gossip, no membership protocol, no Raft. The default shard map is a pure function of (node list, q); explicit rebalance maps are versioned and reconciled by anti-entropy. Discovery probes peers for management but does not change placement. |
| Storage | `rusty-leveldb` (pure Rust) | One implementation, no C++ toolchain, single static binary. Risk is concentrated here: its API is `&mut self` for *all* operations, including reads. |
| JavaScript isolation | QuickJS with no host capabilities | Design-document JS sees only the CouchDB helpers (`emit`, `sum`, `log`, `index`); there are no network or filesystem APIs. A pooled worker process with seccomp/Landlock, or a WASI/WASM engine, contains interpreter bugs. |
| Not used | Raft, Mango, `_replicator`, `_users` | Where the code-size win comes from. Views and search use QuickJS rather than a general server-side JS platform. |

## Goal and non-goals

**Goal:** a server that PouchDB can `replicate()` against in both directions and
that CouchDB can replicate to and from, storing data in LevelDB, running as a
Q-shard × N-replica cluster.

**Non-goals, deliberately:** attachments, Mango, `_replicator` database,
`_users` database machinery beyond auth, multipart `_bulk_docs`. Attachments
are rejected rather than stored. Map/reduce views and search indexes are
implemented with QuickJS; lists, shows, update handlers and a general
server-side JavaScript platform are not. Design-document JavaScript never gets
network or filesystem access in any engine mode.

## The one architectural idea

**Implement the CouchDB replication protocol once and use it for everything.**

`_changes` + `_revs_diff` + `_bulk_docs?new_edits=false` + `_local` is
simultaneously:

1. the API PouchDB replicates against,
2. the anti-entropy protocol between cluster nodes,
3. the shard-rebalance protocol,
4. the backup/restore mechanism.

One implementation, four features. Any shortcut that writes "directly to
LevelDB" forks into a second implementation later.

Corollary: **single-node is the cluster with Q=1, N=1.** No separate code path.

## Architecture

```
        PouchDB / curl / CouchDB
              │ HTTP
      ┌───────▼────────┐
      │  axum router   │  handlers, params, CouchDB error shapes
      └───────┬────────┘
              │ docid → md5 → shard slot → replica set (rendezvous hash)
      ┌───────▼────────┐
      │  fabric        │  call local shard actor, or HTTP-proxy to a replica
      └───────┬────────┘
      ┌───────▼────────┐
      │  shard actor   │  one task per shard, owns the DB (mandatory: the
      │                │  LevelDB API is &mut self even for reads),
      │                │  group-commits into one WriteBatch
      └───────┬────────┘
      ┌───────▼────────┐
      │    LevelDB     │  one DB per (database, shard)
      └────────────────┘
```

Crates: `axum`, `tokio`, `serde_json` (**with `preserve_order`**), `rusty-leveldb`,
`reqwest`, `md-5`, `tracing`, `rquickjs`, `tantivy`, `icu_collator`,
`wasmi`/`wasmi_wasi`, and Linux-only `seccompiler`/`landlock`.

Map/reduce views and search indexes are evaluated by QuickJS. The default
engine is in-process behind memory/stack/interrupt limits;
`--js-sandbox process` moves evaluations to a pooled worker with cleared
environment, rlimits, seccomp and Landlock; `--js-sandbox wasm` runs a checked-in
QuickJS WASI module under `wasmi` with fuel and linear-memory limits. None of
these exposes network or filesystem APIs to JavaScript.

Because the LevelDB handle is `&mut` for reads as well as writes, the shard
actor is not an optimisation — it is the only way to share a shard. Reads and
writes both funnel through its mailbox. Snapshots are taken inside the actor;
a read that needs a consistent multi-key view does it in one message.

**One DB per (database, shard)**, not one big DB with key prefixes:
drop-database is `rm -rf`, shard migration is a directory copy, and compaction
stalls stay isolated to one database. Document the operational ceiling (~hundreds
of databases, not thousands).

## Storage layout

Keys are chosen so LevelDB's own sort order *is* the index.

| Key | Value | Purpose |
|---|---|---|
| `m/<docid>` | revtree + winner + deleted flag + update seq | doc metadata; sorted = `_all_docs` order |
| `d/<docid>/<rev>` | JSON body (without `_id`/`_rev`) | one body per revision |
| `s/<u64 BE seq>/<docid>` | 1 byte | by-seq index for `_changes` |
| `l/<localid>` | JSON | `_local` docs, replication checkpoints |
| `z/doc_count`, `z/doc_deleted_count` | u64 BE | live/deleted counters for `GET /db` and `_all_docs` totals |
| `x/...` | — | reserved |

Doc ids are raw UTF-8 bytes, which sorts identically to CouchDB's binary
ordering, so `_all_docs` pagination and `startkey`/`endkey` match with no
collation layer. `_local` lives in its own prefix and is excluded from
`_all_docs`, as in CouchDB.

Each database directory also contains:

| Path | Purpose |
|---|---|
| `db.json` | database uuid, shard count and partitioned flag |
| `shard-map.json`, `pending-shard-map.json` | versioned placement metadata |
| `shard-0000`, `shard-0001`, ... | one LevelDB per shard |
| `_view_indexes/` | persistent map/reduce indexes, one directory per view |
| `_search_indexes/` | Tantivy indexes plus a JSON sidecar of indexed values |

View and search indexes are derived data: they can be deleted and rebuilt from
documents and design documents. `_view_cleanup` and `_search_cleanup` remove
them.

**Write path.** Sequence numbers are allocated in memory and persisted *in the
same WriteBatch* as the document, so a crash can never leave a gap or a document
without a seq. The actor drains its mailbox and commits everything as one
`WriteBatch` with `sync=true`. rusty-leveldb's sync flag flushes its
`BufWriter` to the OS but does not call `fsync`, so this gives process-crash
durability, not power-loss durability. Group commit still turns many small
writes into one flushed batch and is ~60 lines.

## The rev hash

Wire compatibility means our `new_revid` must produce the same 16 bytes as
CouchDB's. From `src/couch/src/couch_db.erl`:

```erlang
couch_hash:md5_hash(
    term_to_binary([Deleted, OldStart, OldRev, Body, Atts2], [{minor_version, 1}]))
```

* `Deleted` — atom `true`/`false`.
* `OldStart` — parent revision's *position* (integer), or `0` if none.
* `OldRev` — parent revision id as a **binary of decoded hex bytes** (16 bytes),
  or integer `0` if none.
* `Body` — a 1-tuple `{Proplist}` of `{Key, Value}` pairs, with `_id`, `_rev`,
  `_revisions`, `_deleted`, `_attachments`, `_conflicts`, `_deleted_conflicts`,
  `_revs_info`, `_local_seq` already removed. **Key order is the order they
  appeared in the JSON document** — `couch_doc:from_json_obj` builds a proplist
  and never sorts it.
* `Atts2` — list of `{Name, Type, Md5}` triples, **reversed**, for attachments
  that carry an md5. The API rejects attachments now, but the hash code keeps
  this path for legacy documents and CouchDB rev compatibility.

Three consequences that are easy to miss:

1. **`serde_json` must be built with `preserve_order`.** With the default
   `BTreeMap` backing, two documents differing only in key order hash
   identically, and CouchDB would consider them distinct revisions. Any
   `serde_json::Value` we re-serialise must survive a round trip byte-order
   faithfully.
2. **We need an ETF encoder.** ~250 lines: `SMALL_INTEGER_EXT`, `INTEGER_EXT`,
   `SMALL_BIG_EXT`, `NEW_FLOAT_EXT`, `BINARY_EXT`, `LIST_EXT`/`NIL_EXT`,
   `SMALL_TUPLE_EXT`, the leading `131` version byte, and — because
   `minor_version 1` predates UTF-8 atoms — Latin-1 `ATOM_EXT` rather than
   `SMALL_ATOM_UTF8_EXT`. That last detail changes every hash, and the version
   byte changes every hash too.
3. **Attachment bytes never enter the body hash** — only name, content type and
   md5. That decouples large attachments from revision identity.

Validation is differential, not unit: hash a corpus of documents through both
CouchDB's `GET /db/doc?rev=` and our encoder and compare rev ids byte for byte.
This must be green before any storage code is written.

**Status: green.** `src/rev.rs` reproduces revs from a live CouchDB 3.5.3 and
from OTP 27's `term_to_binary/2` for plain, nested, unicode and reordered-key
documents. Two bugs surfaced during that check, both invisible to unit tests:

* the message starts with the ETF version byte `131`, which we were omitting;
* `minor_version 1` encodes atoms as Latin-1 `ATOM_EXT`, not UTF-8.

## Winner selection

CouchDB sorts leaf revisions descending by `{not Deleted, {Pos, RevId}}`
(`couch_doc:to_doc_info_path`):

1. non-deleted beats deleted,
2. then higher position wins,
3. then greater revision id (decoded bytes, byte order) wins.

PouchDB implements the same rule client-side; any divergence shows up as client
and server disagreeing about the current `_rev`.

## Revision trees

Two algorithms carry the correctness weight; the rest is plumbing.

*Growing* a tree from a local edit is easy. The hard one is `new_edits=false`:
receive a path (`_rev` + `_revisions.ids`), find the stem, attach the path, and
let any unattached leaves become conflicts. Budget ~300 LOC and treat it as the
most-tested file in the project. Serialise the tree as JSON initially;
`revs_limit` (default 1000) prunes stems, never leaves.

## HTTP surface

Must work, because PouchDB calls it:

`GET /` (welcome, with `couchdb` version field), `PUT/GET/DELETE /db`,
`GET/PUT/DELETE /db/doc` (`?rev=`, `?new_edits=false`, `?revs=`, `?open_revs=`,
`?conflicts=`), `POST /db`, `_bulk_docs`, `_bulk_get`, `_all_docs`,
`_changes` (`normal`/`longpoll`/`continuous`/SSE, `since`, `style=all_docs`,
`filter=_doc_ids`, `heartbeat`), `_revs_diff`, `_local/*`, `_ensure_full_commit`,
`_revs_limit`, `_purge`, `_compact`, `_uuids`, `_session`, `_security`,
`_db_updates`, `_dbs_info`, `_all_dbs` pagination and `_membership`.

Views and search add:

`_design/{ddoc}/_view/{view}` and `_view_cleanup`; `_design/{ddoc}/_search/{index}`,
`_search_info`, `_search_cleanup` and `_index_info?type=search`; partitioned
`_partition/{p}/...` forms for documents, views and search.

Cluster management adds:

`_node/_local/_dbs`, `_node/_local/_dbs/{db}/shards`, `_node/_local/_dbs/{db}/rebalance`,
`_node/_local/_dbs/{db}/cleanup`, `_node/_local/_cluster`,
`_node/_local/_cluster/discover` and `_node/_local/_replication` plus
`run`/`pause`/`resume`. The embedded console is served at `/_utils/`.

Wire compatibility also means matching the boring parts: `{"error","reason"}`
bodies, status codes (`409` on conflict, `412` on bad rev, `404` on missing db),
`ETag` on document responses, `Content-Type: application/json`, and the
`X-Couch-Request-ID`/`Server` headers that some clients sniff.

## Cluster

Not Raft. CouchDB's document model — revision trees with deterministic conflict
resolution — is already a join-semilattice, so concurrent writes on different
replicas converge without consensus. Raft would buy linearizable `_changes`
ordering; a version vector buys the same resumption correctness for ~60 lines.

* **Sharding:** `md5(docid)` → first four bytes `% q` → shard index. `q` is
  fixed at database creation, as in CouchDB.
* **Placement:** rendezvous score of `(node, shard)`; the top `n` distinct nodes
  form the replica set in preference order, and the first is the primary. The
  default map is a pure function of `(nodes, q, n)`, so every node computes it
  identically. Operators can persist a versioned balanced map through the
  rebalance endpoint; peers reconcile map versions through anti-entropy. `n` is
  clamped to the node count.
* **Routing:** every node is a router; there is no coordinator role. A local
  replica calls the shard actor, a remote replica gets the same HTTP request
  proxied. Writes are sent to every replica in the shard set and return once
  the requested quorum acknowledges; reads try replicas in order and fall back
  past a dead primary.
* **Membership:** `--nodes a:5984,b:5984,c:5984` is static and served verbatim
  by `GET /_membership`. Changing membership is an operator action: restart
  every node with the new list. Discovery probes configured nodes,
  `--seed-nodes` and reported memberships for the management console without
  changing placement. Rendezvous hashing and anti-entropy move data to new
  replicas; the rebalance endpoint broadcasts a balanced map and waits for
  replica catch-up, then `cleanup` removes copies on nodes that no longer own
  the shard.
* **Sequence numbers:** one counter per database; `since` and `update_seq` are
  base64url **version vectors** keyed by node `{node → seq}`. Clients treat
  `since` as opaque (CouchDB 2 already returns strings like `"12-g1AAA…"`), and
  `0`/`now` still work. Node keys are coarser than per-shard-replica keys, but
  resumption remains exact because a node's changes feed is one cursor. A
  single hybrid-logical-clock integer would be smaller but silently drops
  changes under clock skew.
* **Anti-entropy:** one pull per peer per database every `--sync-secs`, using
  the same `_changes` + `_bulk_get` + `new_edits=false` protocol clients use,
  with documents filtered to the shards this node replicates. `_bulk_get` asks
  for `revs=true`, so revision-history metadata survives; conflict leaves and
  tombstones are regression-tested. Runs without a client request; observable
  and controllable through `/_node/_local/_replication` and the console. Discovery probes configured
  nodes, `--seed-nodes` and reported memberships without changing placement.
  Internal calls authenticate with the shared `--admin` credentials; a client's
  Basic auth or `_session` cookie is forwarded when routing/fan-out needs a
  peer.
* **Database lifecycle:** create and delete are broadcast to every configured
  node. Design documents are replicated to every node (views need them to map
  the node's local shards). A node that is offline during the operation must be
  repaired manually.
* **Consistency:** implement CouchDB's `?w=`/`?r=`; default quorum write, single
  read. An unmet write quorum returns `202` and anti-entropy heals the replica.
  Do not pretend to offer linearizability.
* **Operational limits:** `_purge`, `_compact` and `_revs_limit` are local to the
  receiving node; there is no inter-node TLS or separate service account, so
  cluster nodes must sit on a trusted network.

## Code size budget

| Component | LOC |
|---|---|
| HTTP handlers, params, error/shape compatibility | ~3,550 |
| Fabric: routing, replica fan-out, quorum, view/search merge | ~2,200 |
| Database, rev trees, key codec, shard actor | ~3,200 |
| Views | ~1,900 |
| Search | ~900 |
| QuickJS controls and sandbox modes | ~700 |
| Cluster metadata, discovery, replication, status | ~1,000 |
| ETF, rev hashing, winner selection | ~700 |
| Auth, telemetry, console hosting, main | ~700 |
| **Total** | **~15,000** |

The original ~4k budget was for documents and cluster routing. Views, search,
Tantivy, cluster management and the JavaScript sandbox were added later and are
the bulk of the growth.

## Milestones

0. **Wire-compat spike (blocking).** ETF encoder + rev hash, validated against a
   real CouchDB on a document corpus. Done.
1. **Single-node store.** Doc CRUD, rev trees, `_bulk_docs`, `_all_docs`. Done.
2. **Replication round-trips.** `_changes`, `_revs_diff`, `_bulk_get`, `_local`,
   `new_edits=false`. Done, including PouchDB and CouchDB round-trips.
3. ~~**Attachments.**~~ Removed in v0.2; the API rejects `_attachments`.
4. **Cluster.** Shard map, routing, quorum, anti-entropy. Done, including
   versions, rebalance/cleanup and node-loss failover.
5. **Ersatz CouchDB.** `_purge`, `_compact`, `_session`, `_security`,
   `_membership`, metrics. Done.
6. **Views, search and cluster management.** Partitioned databases, durable view
   indexes, Tantivy search, the cluster/replication console and the
   native/process/WASM QuickJS sandbox. Done.

## Risks

1. **Rev hash divergence.** Silent until it manifests as phantom conflicts.
   Differential testing from day one, gated in milestone 0.
2. **`rusty-leveldb` semantics under load.** `&mut self` on every operation
   serialises all access per shard — that is the design, but compaction stalls
   and write amplification are unproven. Spike: 10 GB, group-commit throughput,
   p99 latency during compaction.
3. ~~**Attachment bloat.**~~ No longer applicable: attachments are not stored.
4. **Wire-compat surface creep.** Every quirk discovered (`ETag` format,
   `_changes` line framing, `open_revs` shape) is small but there are many.
   Keep a conformance test file per endpoint; refuse features by default.
5. **JavaScript memory safety.** QuickJS is C. In-process native mode is an
   availability boundary, not a security boundary. Untrusted deployments
   should use `--js-sandbox process` (Linux seccomp/Landlock) or
   `--js-sandbox wasm` (memory-safe, portable).
6. **WASM performance.** The WASI engine is roughly 20–30× slower than native
   for cold map/reduce work; keep it optional until that trade-off is
   acceptable for the deployment.

## Open questions

* **WASM QuickJS upstream.** The checked-in runner uses the maintained
  `rquickjs` crate and a pinned WASI SDK; the artifact is checksum-tested.
* **Default JavaScript engine.** Native is fastest and trusted-only;
  process is the Linux containment boundary; WASM is portable and
  memory-safe but slower. Keep the default explicit and deployment-driven.
* **Non-Linux process sandboxing.** Resolved by guidance: process syscall/
  filesystem confinement is Linux-only; use the portable `--js-sandbox wasm`
  engine on other hosts that need a hard boundary.
* **Compaction placement.** Measured at 10 GB: compaction blocks a shard for
  ~27 s. More shards or an observable `compact_running` flag plus off-path
  scheduling remain options; `_compact` maps to `compact_range` and returns
  `202 Accepted`.

## Progress

* **M0 green.** `src/rev.rs` reproduces CouchDB 3.5.3 rev ids byte for byte;
  ETF is version-byte + Latin-1 atoms. Unit tests cover the corpus.
* **M1 green.** `src/db.rs` implements CRUD, rev trees, conflict winners,
  `_bulk_docs`, `_all_docs`, `_local`, `_purge` and `_revs_limit`. Attachments
  were removed in v0.2. `tests/conformance.rs` starts a real server and
  asserts CouchDB's status codes, bodies, ETag and digest formats.
* **M2 green.** `_changes`, `_revs_diff`, `_bulk_get` and `new_edits=false` work.
  Verified manually: PouchDB ⇄ douchdb round-trips docs, deletions and
  and CouchDB's own `_replicate` pushes and pulls in both directions with
  identical revision ids.
* **Group commit.** The shard actor drains up to 64 queued jobs, merges their
  `Tx`es into one `WriteBatch`, and acknowledges after the batch is flushed to
  the OS. rusty-leveldb does not fsync by default, so acknowledged writes are
  not power-loss durable; comparisons must state this. A write overlay makes
  later jobs in the same group read their predecessors. A SIGKILL/restart
  conformance test confirms acknowledged documents and changes survive a
  process crash; power loss remains explicitly out of scope.
* **Cluster routing green.** Shard map, rendezvous placement and per-document
  proxying cover CRUD, `_bulk_docs`, `_bulk_get`, `_revs_diff` and `POST /db`
  (generated ids are routed after parsing). `_all_docs` fans out to every node,
  merges by id preferring the shard owner, then applies `startkey`/`skip`/
  `limit`; `keys` queries also surface tombstones and preserve request order,
  including descending keys. `_changes` fans out per node,
  dedupes replicated documents and returns a base64url version-vector `since`/
  `last_seq`; `GET /db` reports the same vector as `update_seq`. Forwarded
  subrequests carry `x-douchdb-forwarded` so the receiving peer serves them
  locally instead of re-fanning out.
* **Cluster replicas green.** `--n N` selects N distinct nodes per shard by
  rendezvous score. A write is sent to every replica in the shard set and
  returns once the requested quorum acknowledges; remaining replica writes
  continue detached. Reads try replicas in preference order and fall back past
  a dead primary. `?w=` and `?r=` are honoured (default write quorum, single read);
  an unmet write quorum returns `202` and anti-entropy repairs the replica.
  Anti-entropy advances a checkpoint only after every requested revision is
  applied, already local, or explicitly unavailable from the peer; partial
  `_bulk_get` responses leave the checkpoint behind. The streamed changes merge
  is covered by `changes_pagination_resumes_across_interleaved_shard_pages`.
  Database create/drop are broadcast, anti-entropy pulls only the shards a node
  replicates, and internal calls authenticate with the shared admin credentials
  (client Basic/cookie auth is forwarded to peers). The v0.1 gate — kill a node mid-replication and keep serving
  from the survivors — is covered by
  `cluster_survives_killing_a_node_mid_replication`.
* **Version vectors.** `since`/`update_seq` are keyed by node, not by shard
  replica: sequence allocation is one counter per database, so a node's changes
  feed is a single cursor and the coarser key still resumes exactly (no change
  is skipped). Per-shard-replica keys would require per-shard sequence
  allocation, which the storage layout does not currently do.
* **Views.** Design-document map/reduce views run on embedded QuickJS with
  CouchDB's `emit`/`sum`/`log` helpers, `key`/`keys`/range/page/include_docs
  query options, `_count`/`_sum`/`_stats`/`_approx_count_distinct` and custom
  reducers, grouping and cluster-wide row merging. `_count`, `_sum`, `_stats`
  and `_approx_count_distinct` are evaluated in Rust, locally and when cluster
  partials are combined; custom reducers use QuickJS and `rereduce`. Simple
  single-emit maps that read top-level document fields or JSON literals are
  recognized and evaluated without JavaScript; anything more complex falls back
  to QuickJS. Native cold builds extract top-level fields from raw document
  JSON and parse only the emitted slices. Warm queries use the atomic update
  sequence instead of per-shard info calls, and `include_docs` loads bodies
  only for the selected rows.
  Indexes persist under
  `_view_indexes/` and update from `_changes`; design changes rebuild them.
  Strings use ICU4X collation, large groups rereduce in 1000-row batches, heap
  and time limits are configurable, and `_view_cleanup` removes stale indexes.
  QuickJS runs in-process behind memory/stack/interrupt limits by default;
  `--js-sandbox process` sends map/reduce/index calls to a warmed worker pool
  with a cleared environment, `/` cwd, rlimits, a seccomp denylist, a
  best-effort Landlock filesystem layer and a parent-side kill watchdog. That
  contains crashes and host-handle access. A checked-in QuickJS WASI runner under
  `wasmi` is available via `--js-sandbox wasm`, with fuel and linear-memory
  limits. See `docs/quickjs-sandbox.md`.
  Design documents replicate to every node; a cluster view query falls back to
  live replicas when an owner is unreachable and fails with `502` only when no
  replica can answer.
* **v0.2 protocol pass.** `_changes` is a true chunked continuous feed with
  heartbeat and an SSE `feed=eventsource` variant; bulk reads/writes batch shard and peer work; COPY, conditional
  ETags, gzip, request IDs and bodyless HEAD are implemented; attachments were
  removed and rejected; `_db_updates`, `_dbs_info` and `_all_dbs` pagination
  are available; optional OTLP tracing/metrics are wired through
  `--otel-endpoint`.
* **Hardening.** Internal routing headers require a shared
  `--cluster-secret`, so clients cannot forge peer requests; `_all_dbs` and
  `_membership` require admin auth; session cookies are `HttpOnly; SameSite=Strict`
  and HMAC verification is constant-time; peer HTTP calls have connect/request
  timeouts; request bodies and view execution are bounded; security headers are
  added to every response; a poisoned view-index mutex no longer takes down
  future queries.
* **Observability.** Optional OTLP traces and metrics via `--otel-endpoint` /
  `OTEL_EXPORTER_OTLP_ENDPOINT`: `http.request` spans plus request count and
  latency metrics, with a no-op default. The issue/branch/merge workflow is
  documented in `docs/process.md`.
* **Single-node baseline.** `scripts/perf_sweep.py` records CRUD, bulk,
  metadata, changes, view, large-document and compaction numbers on release
  builds; results and caveats are in `docs/performance.md`.
* **Metadata scans removed from read paths.** Per-shard live/deleted counters
  back `GET /db` and `_all_docs` totals; `_all_docs` range queries stop after
  `skip + limit` live rows (and keys queries fetch metadata directly);
  `_changes` stops each shard once `limit` matches are found. The #30
  before/after numbers are in `docs/performance.md`.
* **View map builds and warm queries.** Cold builds take ids/winner revisions
  from the live-document map, splice `_id`/`_rev` onto raw bodies, and batch
  QuickJS calls instead of parsing and re-encoding every document through
  `Value`; the rebuilt index seeds a sorted in-memory snapshot that warm queries
  reuse, and built-in reducers skip per-row key/value allocation. See the #31
  and #35 before/after numbers in `docs/performance.md`.
* **Partitioned databases.** `PUT /db?partitioned=true` stores a partitioned
  flag, enforces `partition:docid` ids, and hashes by partition so a partition
  shares one shard set. `_partition/{p}` exposes counts; partition-scoped
  `_all_docs` and views filter at the shard, with ranges and keys clamped to the
  partition prefix. Design docs are partitioned by default and can opt into
  global views with `options.partitioned=false`.
* **Search.** Design-document `indexes` run through QuickJS and build
  Tantivy-backed indexes under `_search_indexes/`. The API supports field
  queries, `include_docs`, drilldown, counts, ranges, grouping, bookmarks and
  partitioned search. Indexes rebuild when the update sequence or definition
  changes. Custom sorting, highlighting, request timeouts,
  `_index_info?type=search` and owner-aware cluster fan-out with merge/dedup
  are implemented.
* **Cluster fan-out is owner-aware and concurrent.** Cluster `_all_docs` and
  view queries fetch only shards a node owns, run peer requests concurrently,
  push the requested window down to owners, and combine global/grouped built-in
  reduces plus custom reducers via `rereduce`. Cluster view indexes cover all
  locally held replicas; normal queries, `_changes`, `_bulk_get` and
  `_revs_diff` use the coordinator's local replica when it holds the shard,
  otherwise the primary owner, with replica retry on failure. When an owner is
  down, map views, reduce views, `_all_docs` and `_changes` request only the
  missing shard masks from live replicas instead of rescanning every reachable
  node. Reduce partials are recombined after fallback, including
  grouped and custom `rereduce` reducers. Changes use the live-document index
  for winning revisions and compact sequence tokens, avoiding per-row metadata
  decodes and duplicate detection. Reassigned shard masks after an owner loss
  resume from zero because the survivor's checkpoint belongs to a different
  shard set. Cluster `_all_docs` keys are ordered after the merge, including
  descending requests; streamed changes are tested for strict sequence order,
  resume boundaries and duplicate-free pagination across interleaved shards.
  A lazy per-shard sequence index caches
  winning revisions and conflict leaves, so steady-state feeds do not scan the
  LevelDB sequence table or decode metadata. Normal changes responses are
  streamed by merging shard pages and serializing bounded chunks, with
  `include_docs` bodies embedded verbatim rather than parsed and rebuilt. Full `_changes` feeds
  also query peers concurrently; limited
  feeds merge node prefixes round-robin up to the global limit. Unavailable
  change nodes are skipped and covered by replicas, with their vector
  component resuming on return. New edits are written directly to every
  replica in parallel and return once `w` acknowledgements are reached;
  documents are shared as `Arc<Value>` across replica groups and forwarded
  payloads are serialized directly, avoiding one deep clone and one
  intermediate JSON tree per peer. Remaining replica writes continue detached,
  so slow replicas do not set tail latency. There is no primary fetch-and-copy pass. Point reads default to `r=(n/2)+1`, prefer the local replica, and read
  the remaining replicas concurrently; `?r=1` is the local fast path. Fabric,
  shard-map and rebalance metrics export through the optional OpenTelemetry
  setup and are no-ops without an endpoint. A
  reproducible three-node `q=4`/`n=3` benchmark and before/after numbers are in
  `docs/performance-cluster.md`.
* **Load-balanced edge.** The Compose stack puts nginx in front of all three
  nodes; any node is an equal coordinator. Clustered defaults use `n=3`, so a
  three-node cluster has every document on every node. `scripts/perf_cluster.py
  --spread --native` measures round-robin routing with the Rust load generator.
  `scripts/perf_isolated.py` pins servers and client to disjoint CPUs and shows
  `?r=1` GET scaling from 19.5k to 45.6k req/s across 1-3 nodes; default quorum
  reads remain flat because they read two replicas. Internal transport was not
  rewritten based on that result. Owner-aware edge routing was deliberately not
  added: CouchDB uses cheap internal replica calls and a dumb round-robin LB.
* **Shard-map metadata and movement.** Each database has a persisted, cached
  mem3-style shard map, exposed at `/_node/_local/_dbs/{db}` and broadcast to
  peers. Maps carry monotonic versions; updates track metadata quorum and a
  pending map file, and anti-entropy reconciles newer/older peers after a
  missed update. Routing and anti-entropy read the map; anti-entropy
  checkpoints are tagged by map hash so new replicas backfill after a move. A
  balanced planner endpoint waits for per-shard replica counts and then
  automatically cleans old replicas; a manual cleanup endpoint remains for
  controlled runs. Cluster routing, replica selection, quorum reads/writes and
  internal node calls live in `src/fabric.rs`; HTTP handlers only call that
  layer.
* **Cluster management and replication controls.** `GET /_node/_local/_cluster`
  reports membership, discovery probes, shard maps and replicator state.
  `--advertise` / `--seed-nodes` drive best-effort peer discovery without
  changing placement. `GET/POST /_node/_local/_replication` exposes per-db/peer
  checkpoints, manual sync, and pause/resume for the background anti-entropy
  puller. The console cluster page uses these endpoints and adds rebalance and
  cleanup actions.
* **JavaScript sandbox.** QuickJS has no host network/filesystem APIs in any
  mode. `--js-sandbox process` uses a pooled worker with cleared environment,
  rlimits, a seccomp denylist, a best-effort Landlock filesystem layer and a
  watchdog; `--js-sandbox wasm` runs a checked-in QuickJS WASI module under
  `wasmi` with fuel, linear-memory and output limits. The default remains
  in-process with memory/stack/interrupt limits.
* **Live-document index for `_all_docs`.** Each shard lazily builds an in-memory
  sorted map of live ids to winning revisions on the first ascending range
  query, then maintains it on writes. This removes metadata decodes and makes
  `startkey` offsets a range count without adding an extra LevelDB write per
  ingested document. Descending ranges still use the metadata scan.
* **Storage spike green.** A 10 GB run on one shard (worst-case `&mut self`
  serialisation) is recorded below.
* **Web console.** `/_utils/` serves an embedded Vue 3 console (`ui/`, vendored
  runtime, no build step). It browses databases and documents, edits JSON, tails
  `_changes`, shows database info, signs in through `_session`, has a cluster
  page for discovery, shard maps, replication status and rebalance/cleanup, and
  has a toggleable request log with both directions' JSON. It talks only to the
  public HTTP API; the static assets are the only addition to the server.
  CORS is opt-in via `--cors-origins` (exact origins or `https://*.example.com`,
  or `*`), with echoed origins, credentials, preflight and CORS headers on
  errors, mirroring CouchDB's `[cors]` behaviour.

## Storage spike

`scripts/storage_spike.py` writes incompressible 1 MB documents through the HTTP
API, keeps concurrent writers running (group commit), probes read latency, then
compacts the shard and kills the server with `SIGKILL` to check process-crash
durability. This does not cover power loss: rusty-leveldb does not fsync.
Release build, `--q 1`, 4 writers, 10,001 documents, 10.002 GB logical:

| Metric | Result |
|---|---|
| Load throughput | 79.9 MB/s (125.2 s for 10 GB) |
| Write latency p50 / p99 / max | 123 ms / 532 ms / 22.0 s |
| Disk after load / after compact | 10.04 GB / 10.04 GB |
| Write amplification | 1.004 |
| Probe p50 / p99, idle | 2.19 ms / 2.73 ms |
| `_compact` duration | 26.9 s |
| Probe latency during compaction | 26.9 s (blocked for the whole compaction) |
| Docs after kill -9 + restart | 10,001 of 10,001 |

The interesting number is the last one: `compact_range` owns the shard actor
for its whole duration, so every request behind it waits. At 10 GB that is a
27-second p99 for that shard. The write path itself is acceptable (80 MB/s,
~0.5 s p99) with one 22-second outlier where LevelDB compaction stalled the
writer. Keeping `_compact` synchronous is no longer defensible for large
databases; the options are a background job that still takes the shard lock
(same stall, but observable via `compact_running`) or sharding compaction work
across more shards (`q > 1` divides the stall).


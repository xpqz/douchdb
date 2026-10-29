# douchdb — CouchDB wire-compatible database in Rust on LevelDB

Status: plan, decisions locked. Target: a small, clustered, PouchDB-and-CouchDB
compatible document store.

## Locked decisions

| Decision | Choice | Consequence |
|---|---|---|
| Compatibility | CouchDB **wire** compatibility | Revision ids must be byte-identical to CouchDB's. This forces an Erlang External Term Format encoder and order-preserving JSON. See "The rev hash". |
| Cluster membership | Static node list from config | No gossip, no membership protocol, no Raft. The shard map is a pure function of (node list, q), computed identically at startup on every node. No metadata to distribute. |
| Storage | `rusty-leveldb` (pure Rust) | One implementation, no C++ toolchain, single static binary. Risk is concentrated here: its API is `&mut self` for *all* operations, including reads. |
| Not used | Raft, JS engine, Mango, views, `_replicator`, `_users` | Where the code-size win comes from. |

## Goal and non-goals

**Goal:** a server that PouchDB can `replicate()` against in both directions and
that CouchDB can replicate to and from, storing data in LevelDB, running as a
Q-shard × N-replica cluster.

**Non-goals, deliberately:** server-side JavaScript, view engine, Mango,
`_replicator` database, `_users` database machinery beyond auth, per-document
ETags, multipart `_bulk_docs`. PouchDB runs `query()`/`find()` client-side, so
none of these are needed for compatibility — only for CouchDB ecosystem parity.

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
`reqwest`, `md-5`, `tracing`.

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
| `a/<docid>/<rev>/<attname>` | attachment chunk, 64 KiB | streaming attachments |
| `s/<u64 BE seq>/<docid>` | 1 byte | by-seq index for `_changes` |
| `l/<localid>` | JSON | `_local` docs, replication checkpoints |
| `x/...` | — | reserved |

Doc ids are raw UTF-8 bytes, which sorts identically to CouchDB's binary
ordering, so `_all_docs` pagination and `startkey`/`endkey` match with no
collation layer. `_local` lives in its own prefix and is excluded from
`_all_docs`, as in CouchDB.

**Write path.** Sequence numbers are allocated in memory and persisted *in the
same WriteBatch* as the document, so a crash can never leave a gap or a document
without a seq. The actor drains its mailbox and commits everything as one
`WriteBatch` with `sync=true` — group commit turns fsync-per-write into
something usable, and is ~60 lines.

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
  that carry an md5 (CouchDB falls back to a random id for legacy attachments).

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
`_changes` (`normal`/`longpoll`/`continuous`, `since`, `style=all_docs`,
`filter=_doc_ids`, `heartbeat`), `_revs_diff`, `_local/*`, `_ensure_full_commit`,
`_revs_limit`, `_purge`, `_compact`, `_uuids`, `_session`.

Wire compatibility also means matching the boring parts: `{"error","reason"}`
bodies, status codes (`409` on conflict, `412` on bad rev, `404` on missing db),
`ETag` on document responses, `Content-Type: application/json`, and the
`X-Couch-Request-ID`/`Server` headers that some clients sniff.

## Cluster

Not Raft. CouchDB's document model — revision trees with deterministic conflict
resolution — is already a join-semilattice, so concurrent writes on different
replicas converge without consensus. Raft would buy linearizable `_changes`
ordering; a version vector buys the same resumption correctness for ~60 lines.

* **Sharding:** `md5(docid)` → the top `log2(q)` bits of a 2³² ring → shard
  index. `q` is fixed at database creation, as in CouchDB.
* **Placement:** rendezvous hashing of `(shard, replica_index)` over the sorted
  node list. ~20 lines, no rebalance table, adding a node moves ~1/N of shards.
* **Routing:** every node is a router; there is no coordinator role.
  Local replica → shard actor. Remote → HTTP proxy of the same shard-level API.
* **Membership:** `--nodes a:5984,b:5984,c:5984` in config, served verbatim by
  `GET /_membership`. Changing membership is an operator action: restart with a
  new list, then let anti-entropy move shards.
* **Sequence numbers:** per-shard-replica monotonic counters; `since` and
  `update_seq` are base64url **version vectors** `{replica → seq}`. Clients treat
  `since` as opaque (CouchDB 2 already returns strings like `"12-g1AAA…"`), and
  `0`/`now` still work. A single hybrid-logical-clock integer would be smaller
  but silently drops changes under clock skew.
* **Anti-entropy:** a background task runs the replication protocol node to node,
  per shard, so the internal sync path is exercised constantly by the same code
  clients use.
* **Consistency:** implement CouchDB's `?w=`/`?r=`; default quorum write, single
  read. Do not pretend to offer linearizability.

## Code size budget

| Component | LOC |
|---|---|
| ETF encoder | 250 |
| Rev hashing, rev trees, winner selection | 650 |
| Store: key codec, shard actor, group commit | 700 |
| HTTP handlers, params, error/shape compatibility | 1400 |
| Cluster: shard map, rendezvous, proxy, anti-entropy, version vector | 700 |
| Attachments | 350 |
| Auth (JWT issued by `_session`, cluster-friendly) | 250 |
| Config, main, misc | 300 |
| **Total** | **~4600** |

Above ~6k means something was built that nobody asked for.

## Milestones

0. **Wire-compat spike (blocking).** ETF encoder + rev hash, validated against a
   real CouchDB on a document corpus. Nothing else starts until this is green.
1. **Single-node store.** Doc CRUD, rev trees, `_bulk_docs`, `_all_docs`.
2. **Replication round-trips.** `_changes`, `_revs_diff`, `_bulk_get`, `_local`,
   `new_edits=false`. CI gate: `pouch.replicate()` both directions, and
   CouchDB→douchdb, douchdb→CouchDB.
3. **Attachments.** Chunked; base64 first, multipart second.
4. **Cluster.** Shard map, routing, quorum, anti-entropy. Ship v0.1 only when a
   three-node cluster survives killing a node mid-replication.
5. **Ersatz CouchDB.** `_purge`, `_compact`, `_session`, `_security`,
   `_membership`, metrics.

## Risks

1. **Rev hash divergence.** Silent until it manifests as phantom conflicts.
   Differential testing from day one, gated in milestone 0.
2. **`rusty-leveldb` semantics under load.** `&mut self` on every operation
   serialises all access per shard — that is the design, but compaction stalls
   and write amplification are unproven. Spike: 10 GB, group-commit throughput,
   p99 latency during compaction.
3. **Attachment bloat.** Large values wreck LevelDB compaction. Chunk at 64 KiB
   from the start, never "add chunking later".
4. **Wire-compat surface creep.** Every quirk discovered (`ETag` format,
   `_changes` line framing, `open_revs` shape) is small but there are many.
   Keep a conformance test file per endpoint; refuse features by default.

## Open questions

* ~~Should `_compact` map to `compact_range`, or be a no-op that returns `ok`?~~
  **Decided:** map to `compact_range`, return `202 Accepted` immediately. A
  no-op would make PouchDB's `compact()` lie about reclaiming space, and
  `compact_range` is what LevelDB is for. The call is synchronous inside the
  shard actor, so a large compaction delays that shard's next request; that is
  the same trade-off the write path already makes for `&mut self`. If it becomes
  a latency problem, move it to a background job with a `compact_running` flag.
* Attachment chunk size vs read amplification for `pouch.getAttachment`.

## Progress

* **M0 green.** `src/rev.rs` reproduces CouchDB 3.5.3 rev ids byte for byte;
  ETF is version-byte + Latin-1 atoms. Unit tests cover the corpus.
* **M1 green.** `src/db.rs` implements CRUD, rev trees, conflict winners,
  `_bulk_docs`, `_all_docs`, `_local`, `_purge`, `_revs_limit`, attachments
  (64 KiB chunks, base64). `tests/conformance.rs` starts a real server and
  asserts CouchDB's status codes, bodies, ETag and digest formats.
* **M2 green.** `_changes`, `_revs_diff`, `_bulk_get` and `new_edits=false` work.
  Verified manually: PouchDB ⇄ douchdb round-trips docs, deletions and
  attachments; CouchDB's own `_replicate` pushes and pulls in both directions
  with identical revision ids.
* **Group commit.** The shard actor drains up to 64 queued jobs, merges their
  `Tx`es into one synced `WriteBatch`, and acknowledges only after the fsync.
  A write overlay makes later jobs in the same group read their predecessors.
* **Cluster, partial.** Shard map + rendezvous placement + per-document HTTP
  proxy work; two nodes with `q=4` split writes 6/2 and converge via
  anti-entropy. `_bulk_docs`, `_all_docs` and `_changes` still operate on the
  local node only, and `since` is an integer rather than a version vector.


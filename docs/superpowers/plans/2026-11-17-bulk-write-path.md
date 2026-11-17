# Bulk Write Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `_bulk_docs` (and the paths built on it) cost what redb costs to commit the documents, and nothing more, without changing durability or results.

**Architecture:** Per-document work that needs no stored state (split, body serialisation, revision hash) moves from the shard actor to the request task. A request commits all the shards it touches concurrently. The revision hash streams into MD5. Each job opens each table once. In a cluster, the coordinator's local and remote writes start together without cloning documents. A bench compares the result against a bare redb floor.

**Tech Stack:** Rust, redb 4.6, tokio, serde_json (`preserve_order`, `arbitrary_precision`), md-5, crc32c; Python 3 for the bench driver.

**Spec:** `docs/superpowers/specs/2026-11-17-bulk-write-path-design.md`

## Global Constraints

- Durability unchanged: every write transaction begins through `kv::begin_write` (two-phase, `Durability::Immediate`); acknowledgement only after commit.
- Revision ids byte-identical to main for the same input; response bodies, error precedence and status codes unchanged.
- Follow current CouchDB behaviour; a conflict wins over a malformed design document.
- Shard actor contract unchanged: one write transaction per group, journal records after commit, recovery before reply.
- Peer-call counts unchanged (no protocol change between nodes).
- `cargo clippy --all-targets -- -D warnings` clean; never `#[allow]`; delete unused code.
- Tests wait on conditions (channels, barriers), never sleeps or wall-clock bounds.
- Builds: `CARGO_TARGET_DIR=/Volumes/Space2/douchdb-scratch/target`, `TMPDIR=/Volumes/Space2/douchdb-scratch/tmp`; every run under `timeout 900`; one build at a time.
- Gate (spec Goals): shard actor job time per group, syncs excluded, within 15% of the floor's insert time at 250/1,000/10,000 docs, q=1 and q=2; syncs and bytes per commit equal the floor's. End-to-end docs/s reported, not gated.

## Review Focus

1. A new document posted without `_rev` over a deleted winner: the actor picks the parent, so the prepared hash must be recomputed; the rev must equal main's. → Task 2 golden test case `recreate_over_deleted_winner`.
2. The same id twice in one batch: the second sees the first's staged metadata (conflict or update chain) exactly as on main. → Task 2 golden case `same_id_twice`.
3. A design document with a compile error whose `_rev` also conflicts: conflict wins. → Task 2 golden case `design_conflict_beats_compile_error`.
4. q=2, one shard's commit fails while the other's succeeds: same response as main, validator cache dropped after both answered. → Task 4 test `one_failing_shard_fails_the_request_after_both_answer`.
5. `new_edits=false` bodies with key order, arbitrary-precision numbers and unicode: stored body bytes identical to main. → Task 2 golden case `replicated_body_bytes_preserved`.

---

### Task 1: Measurement — metrics, floor program, bench, golden responses

**Files:**
- Modify: `src/kv.rs` (count syncs), `src/store.rs` (`run_group` timing), `src/metrics.rs` (expose).
- Create: `examples/redb_floor.rs`, `scripts/bulk_bench.py`, `tests/golden/bulk_write.json`.
- Test: `src/metrics.rs` tests, `src/db.rs` tests (golden).

**Interfaces:**
- Produces: `metrics::record_shard_group(jobs_us: u64, commit_us: u64)`; `metrics::record_sync()`; `GET /_node/_local/_metrics` gains `"shard": {"groups", "jobs_us", "commit_us", "syncs"}` (cumulative counters).
- Produces: `examples/redb_floor.rs` CLI `--docs <path.jsonl> --batch <n> --q <1|2> [--sorted]`, printing one JSON line per run `{"batch","q","docs_per_s","insert_us_per_batch","commit_us_per_batch","syncs_per_commit"}`.
- Produces: `scripts/bulk_bench.py --binary <douchdb> --fixture <dir> --batches 250,1000,10000 --q 1,2 --clients 1,4 --rounds 3`, printing a markdown table (floor, douchdb, ratio, jobs-time vs floor insert time) and exiting 0.
- Produces: golden fixture `tests/golden/bulk_write.json`: a list of `{name, requests:[{new_edits, docs}], responses, stored_bodies}` captured from main.

- [ ] **Step 1: Failing test** `shard_metrics_count_groups_and_syncs` in `src/metrics.rs` (or `src/db.rs` tests): put one document into a fresh q=1 db; snapshot `shard.groups` increased by ≥1, `shard.syncs` == 2 × groups delta, `jobs_us`/`commit_us` increased.
- [ ] **Step 2: Run** `cargo test --lib -- shard_metrics_count_groups_and_syncs` — FAIL (no such field).
- [ ] **Step 3: Implement.** In `kv::open`, wrap redb's `FileBackend` in a `StorageBackend` that forwards everything and calls `metrics::record_sync()` in `sync_data`. In `run_group`, time the job loop and the commit separately with `Instant` and call `record_shard_group`.
- [ ] **Step 4: Run** the test — PASS.
- [ ] **Step 5: Golden capture test** `bulk_write_matches_golden` in `src/db.rs` tests: replay each case's requests through `Db::put_docs` on a fresh db (q=1 and q=2), assert responses equal `responses` and stored body bytes (via `get_body`) equal `stored_bodies`. Cases (names fixed): `new_docs`, `updates`, `deletions`, `conflicts`, `recreate_over_deleted_winner`, `same_id_twice`, `design_conflict_beats_compile_error`, `design_compile_error`, `replicated_body_bytes_preserved` (key order, `1.0000000000000000001`, `"é"`), `replicated_known_rev`. Revision ids in the fixture are literal strings. Generate the fixture by running the cases on this commit (before any later task) and commit it; the test must pass now and stay passing.
- [ ] **Step 6: Floor and bench.** Write `examples/redb_floor.rs` with redb and crc32c directly: tables and key types as in `src/kv.rs`/`src/gate.rs` (meta: `&str`; bodies: `(&str,&str)`; by_seq: `u64`), sealed values (`value ‖ crc32c`), one two-phase `Durability::Immediate` transaction per batch per shard, shards on separate threads, input meta/body/by_seq bytes taken from a real shard file produced by importing the fixture (the bench produces it). `scripts/bulk_bench.py` builds nothing; it runs the given binary (`--n 1 --q <q>`), imports via `_bulk_docs`, reads `_metrics` deltas, runs the floor, prints the table. Bound every run with a timeout, kill children in `finally`.
- [ ] **Step 7: Run** `cargo build --release --examples --bins` then `timeout 900 python3 scripts/bulk_bench.py --binary $CARGO_TARGET_DIR/release/douchdb --fixture <airline dir> --rounds 1` — prints a table. Save output as the baseline in the task report.
- [ ] **Step 8: QA and commit** — fmt, clippy, `cargo test --lib`; commit `Measure bulk writes against a bare redb floor`.

### Task 2: Streaming revision hash

**Files:**
- Modify: `src/etf.rs`, `src/rev.rs`.
- Test: `src/rev.rs` tests.

**Interfaces:**
- Consumes: none.
- Produces: `rev::new_revid_doc(deleted: bool, parent: Option<&Rev>, doc: &serde_json::Map<String, Value>) -> [u8; 16]` — hashes the document's body members (every key not in `db::META_FIELDS`, in order) without building a body map or term tree; `etf::hash_json(value: &Value, hasher: &mut impl md5::Digest)` / an `encode`-equivalent writer into a `Digest`. `new_revid` becomes a thin call or is deleted if unused after Task 3.

- [ ] **Step 1: Failing test** `streaming_revid_matches_term_encoding`: for a corpus (empty object, nested arrays/objects, big and fractional arbitrary-precision numbers, negative zero, unicode keys and values, `true/false/null`, 300-member object, deep nesting 64), assert `new_revid_doc(d, p, &doc)` == `Md5::digest(Term::List([...]).encode())` built as `new_revid` builds it today from the body with META_FIELDS removed in order; parents `None` and `Some(3-…)`; `deleted` both.
- [ ] **Step 2: Run** `cargo test --lib -- streaming_revid_matches_term_encoding` — FAIL.
- [ ] **Step 3: Implement** encoding straight into the hasher (same bytes as `Term::encode`), skipping META_FIELDS at the top level.
- [ ] **Step 4: Run** the test and `bulk_write_matches_golden` — PASS.
- [ ] **Step 5: Commit** `Hash revisions straight into MD5`.

### Task 3: Prepare documents off the shard actor

**Files:**
- Create: `src/prepare.rs` (declared `mod prepare;` in `src/lib.rs`, crate-private).
- Modify: `src/db.rs` (`put_docs`, `put_many`, `Stage::stage`, `Stage::stage_replicated`, `split_doc`), `src/fabric.rs` call sites if the signature changes.
- Test: `src/prepare.rs` tests, `src/db.rs` tests.

**Interfaces:**
- Consumes: `rev::new_revid_doc` (Task 2).
- Produces:
  ```rust
  pub(crate) struct Prepared {
      pub id: String,
      pub rev: Option<String>,          // canonical, as split_doc gives today
      pub deleted: bool,
      pub revisions: Option<Value>,     // new_edits=false only
      pub body: Vec<u8>,                // serde_json bytes of the body members, in order
      pub members: Option<Map<String, Value>>, // Some only for _design/ ids (validate_design_doc)
      pub rev_hash: Option<[u8; 16]>,   // new_edits=true: hash for the client-named parent (or none)
  }
  pub(crate) fn prepare(doc: &Value, new_edits: bool) -> DbResult<Prepared>;
  ```
  `Db::put_docs<D: std::borrow::Borrow<Value> + Send + 'static>(&self, docs: Vec<D>, new_edits: bool) -> DbResult<Vec<WriteResult>>` (existing `Vec<Value>` callers keep compiling). `put_many` takes `Vec<(usize, Prepared)>`.

- [ ] **Step 1: Failing tests** in `src/prepare.rs`: `prepare_matches_split_doc` (for the golden corpus docs: id, rev, deleted, revisions equal `split_doc`'s; `body` equals `serde_json::to_vec(&Value::Object(split.body))`); `prepare_errors_match_split_doc` (non-object, non-string `_id`, bad `_rev` type, empty id, `_attachments` → same `ApiError` status/error/reason as today); `prepared_hash_is_for_the_client_parent` (equals `new_revid_doc` with the client `_rev`'s parent).
- [ ] **Step 2: Run** `cargo test --lib -- prepare::` — FAIL.
- [ ] **Step 3: Implement `prepare`** by one ordered pass over the borrowed object (no map rebuild except `members` for design docs), serialising body members with a serializer over the filtered iterator.
- [ ] **Step 4: Move preparation into `put_docs`.** Prepare on the request task; for batches over 1,000 documents prepare on `tokio::task::spawn_blocking`. A document whose `prepare` fails gets its error result without reaching a shard. `Stage::stage` uses `rev_hash` when the parent it settles on is the client's; otherwise (no `_rev` and a deleted winner, or any other parent) it computes `new_revid_doc` from `members`/the body. To recompute without `members`, decode `body` bytes back to a map (rare path). `stage_replicated` stores `body` bytes as-is.
- [ ] **Step 5: Run** `bulk_write_matches_golden`, `cargo test --lib`, `cargo test --test conformance` — PASS.
- [ ] **Step 6: Commit** `Prepare bulk documents before the shard actor`.

### Task 4: Commit a request's shards concurrently

**Files:**
- Modify: `src/db.rs` (`Db::put_docs`).
- Test: `src/db.rs` tests.

**Interfaces:**
- Consumes: `Db::put_docs` from Task 3.
- Produces: unchanged signature; per-shard calls issued together and joined (`futures::future::join_all`).

- [ ] **Step 1: Failing test** `shards_of_one_request_commit_concurrently`: q=2 db; hold shard 0's actor with an `alone` job that waits on a oneshot; call `put_docs` with ids landing on both shards in a spawned task; wait (condition loop with `yield_now`) until shard 1's committed seq advanced; assert shard 0 has not; release; the request returns both ok.
- [ ] **Step 2: Failing test** `one_failing_shard_fails_the_request_after_both_answer`: q=2; inject a commit failure on shard 0 (PowerCut `fail_syncs_after` on its backend, as existing store tests do via the test opener); the request returns the same error as main; shard 1's documents are committed (equal to main: earlier shards committed); design validator cache reset.
- [ ] **Step 3: Run** both — FAIL (first).
- [ ] **Step 4: Implement** `join_all` over the non-empty shard batches; collect results; return the first shard error by shard index after all answered.
- [ ] **Step 5: Run** both, golden, lib — PASS.
- [ ] **Step 6: Commit** `Commit a request's shards concurrently`.

### Task 5: One table open per job, metadata read in one pass

**Files:**
- Modify: `src/store.rs` (`ShardTxn`), `src/db.rs` (`put_many`, `commit_ops`).
- Test: `src/store.rs` tests, `src/db.rs` tests.

**Interfaces:**
- Produces: `ShardTxn::get_metas(&self, ids: &[&str]) -> Result<Vec<Option<DocMeta>>>` (one META open); `ShardTxn::put_bodies`, `put_seqs`, `remove_seqs`, `put_metas` taking slices and opening their table once each, journaling every key as `put_value` does; `#[cfg(test)] ShardTxn::table_opens() -> u64`.

- [ ] **Step 1: Failing test** `a_bulk_job_opens_each_table_once`: a 500-document `put_many` job; `table_opens()` delta ≤ 5 (meta read, meta write, bodies, by_seq, settings).
- [ ] **Step 2: Failing test** `batched_writes_are_journaled_like_single_ones`: with a recording journal, a bulk job records the same key set as the per-key path on main (compare against a run with the old functions kept in the test or against the golden journal keys captured in the test).
- [ ] **Step 3: Run** both — FAIL.
- [ ] **Step 4: Implement**: `put_many` reads all metas first via `get_metas` (keeping `staged` precedence for repeated ids), `commit_ops` groups ops by table and calls the batched writers. Delete the per-key helpers that lose their last caller.
- [ ] **Step 5: Run** both, golden, lib, conformance — PASS.
- [ ] **Step 6: Commit** `Open each table once per bulk job`.

### Task 6: Cluster fan-out without serial local write or clones

**Files:**
- Modify: `src/fabric.rs` (`spawn_quorum_writes`, `replicate_to_all`).
- Test: `src/fabric.rs` tests (or `tests/conformance.rs` cluster harness).

**Interfaces:**
- Consumes: `Db::put_docs<D: Borrow<Value>>` (Task 3).
- Produces: local writes take `Vec<Arc<Value>>` directly (no `(*doc).clone()`); `replicate_to_all` joins the local write and the remote round together.

- [ ] **Step 1: Failing test** `replicated_writes_start_remote_before_local_finishes`: two-node cluster harness; hold the coordinator's local shard actor with an `alone` job on a oneshot; issue a `new_edits=false` bulk; wait until the peer's shard committed seq advanced; release; both copies hold the docs. Peer-call count for the request equals main's (`record_peer` calls).
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement** with `tokio::join!` (local future, remote `in_one_round(join_all(..))`); pass `Arc<Value>` to `put_docs`.
- [ ] **Step 4: Run** the test, cluster conformance tests (`cargo test --test conformance -- cluster`) — PASS.
- [ ] **Step 5: Commit** `Start local and remote bulk writes together`.

### Task 7: Sorted inserts (measured) and the gate

**Files:**
- Modify: `src/db.rs` (`commit_ops`), `docs/performance.md`, `README.md` (only if a documented number changes).

**Interfaces:**
- Consumes: Task 1 bench, Task 5 batched writers.

- [ ] **Step 1:** Run `scripts/bulk_bench.py --rounds 3` on the branch head. Record the table.
- [ ] **Step 2:** Sort each table's batch by key before insert (bodies by `(id, rev)`, metas by id, by_seq already ascending); sequence assignment order unchanged. Run the bench again.
- [ ] **Step 3:** Keep the sort only if 10,000-doc batches gain ≥10% and 250/1,000 do not lose; otherwise revert it. State which in the commit message.
- [ ] **Step 4: Gate.** Shard job time per group (from `_metrics` deltas) within 15% of floor insert time at every batch size and q; syncs per commit equal the floor's. If not met, report the remaining gap by stage instead of committing changes outside this plan.
- [ ] **Step 5:** Add the before/after table and conditions to `docs/performance.md` under a new subsection of "Storage change: redb (2026-11)".
- [ ] **Step 6: Full QA** — fmt, clippy, lib, doc, conformance, `storage_faults.py` (release), `differential/run.py` modes per CI; commit `Record the bulk write path against the redb floor`.

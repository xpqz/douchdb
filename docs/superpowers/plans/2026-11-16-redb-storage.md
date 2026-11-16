# redb Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the vendored rusty-leveldb with redb so every acknowledged write is durable, follow CouchDB's per-shard design, and delete the machinery douchdb built around LevelDB.

**Architecture:** Four phases, each merged as its own PR with every suite green. A: per-shard sequences and opaque `since` tokens, still on LevelDB (removes the global counter and watermarks independently of the engine). B: each shard and view becomes a redb file; group commit is one write transaction; reads take leases through a gate and run off the actor; database metadata moves into redb; creation is staged. C: online, journal-driven compaction. D: end-to-end verification.

**Tech Stack:** Rust, redb 4.6 (`Durability::Immediate`), `crc32c`, tokio, Jepsen (Clojure) harness, Python fault scripts.

**Spec:** `docs/superpowers/specs/2026-11-16-redb-storage-design.md` (approved at `add7bab`). Read it before any task.

## Global Constraints

- redb `= "4.6"`; every write transaction commits with `Durability::Immediate` (the default; never set `None`).
- Every stored value carries a CRC32C appended on write and verified on every read; a mismatch is a corruption error (HTTP 500), never a miss.
- `Builder::set_check_integrity_on_open(true)` on every shard and view file.
- `ConcurrencyMode::ExclusiveWriter` (default) on every file.
- Page cache per file: shard files `--cache-mb` (new flag, default 16), view files a fixed 4 MiB.
- File names: shard `<db>/shard-NNNN.redb`; view `<db>/_view_indexes/<md5>.redb`; compaction `shard-NNNN.compact.redb`; staging `.creating-<uuid>`; tombstone `.deleted-<uuid>` (a leading `.` no database name can have).
- Follow current CouchDB for every observable behaviour; any divergence must be one already named in the spec.
- No migration or compatibility code for earlier douchdb data (none exists).
- Project rules: `cargo clippy --all-targets -- -D warnings` clean, never `#[allow(...)]`; delete unused code; tests wait on conditions, never fixed sleeps or wall-clock bounds (use tokio's paused clock for timer behaviour); every run bounded (≤ 15 min); `CARGO_TARGET_DIR` and temp data under `/Volumes/Space2/douchdb-scratch`; one cargo build or agent at a time; per-PR QA is fmt, clippy, lib tests, conformance once; branch every PR from `main`.

## Review Focus

1. **Client `since` values that are not douchdb tokens.** PouchDB and CouchDB clients send `since=0`, `since=now`, or a token from another server. Expected: `0` and `now` behave as in CouchDB; anything unparseable is `400 bad_request` as CouchDB answers, never a silent restart from 0. Test in Task A2.
2. **Database names that need encoding** (`a/b`, `$x`, `(y)+z`). Expected: created, listed, deleted and recovered exactly like plain names; staging and tombstone directories never collide with them. Test in Task B5.
3. **A continuous `_changes` feed open across a compaction swap or a failed-commit reopen.** Expected: the feed does not block the reopen (leases are per page), keeps streaming afterwards, and neither skips nor repeats. Test in Tasks B4 and C2.
4. **Disk full during a commit.** Expected: the group fails with 500, nothing acknowledged is lost, the shard recovers through the gate once writes succeed again, and it fails closed if reopening fails. Test in Task B4 (power-cut backend with an injected `ENOSPC`).
5. **A view or search query while its database compacts.** Expected: the shard uuid is unchanged by compaction, so the index keeps its cursors and does not rebuild. Test in Task C2.

---

## Phase A — per-shard sequences (still on LevelDB)

### Task A1: Each shard owns its sequence

**Files:**
- Modify: `src/store.rs` (sequence allocation, `ShardHandle`), `src/db.rs` (remove global counter, `allocate_seq`, `committed_seq`, `reserved_seq`, `changes_watermark`, `changes_watermark_from`, global-order merge), `src/view.rs`, `src/search.rs` (callers of the removed items).
- Test: `src/store.rs` tests, `src/db.rs` tests.

**Interfaces:**
- Produces: `Shard::next_seq(&mut self) -> u64` (shard-local, starts at 1); `Shard::uuid(&self) -> &str` (32 hex chars, created once when the shard is created, persisted under `z/uuid` for now; moves to `settings` in B2); `ShardHandle::uuid(&self) -> String`; `Db::shard_seqs(&self) -> Vec<(String /*uuid*/, u64)>` (committed position per shard, in shard order).
- Produces: `ChangesOpts.since: ShardSince` where `pub struct ShardSince(pub Vec<u64>)` (one position per shard, length q); `Db::changes_rows` returns rows plus `ShardSince` reached.

- [ ] **Step 1: Write failing tests.** `shard_sequences_are_independent`: write 3 docs to shard 0 and 1 doc to shard 1 of a q=2 db; assert `db.shard_seqs()` positions are `[3, 1]`. `changes_from_a_shard_position_returns_only_later_rows`: with positions `[2, 0]` the rows are shard 0's third write and shard 1's write. `shard_uuid_survives_reopen`.
- [ ] **Step 2: Run** `cargo test --lib shard_sequences_are_independent changes_from_a_shard_position shard_uuid_survives` — expect FAIL (no such API).
- [ ] **Step 3: Implement.** Allocate sequences inside the shard job from the shard's own counter; delete the global counter and every watermark item listed above; `_changes` reads each shard from its own position and concatenates per shard (no global order). Delete the tests that exercised the watermark (`changes_cursor_stops_at_allocated_but_uncommitted_shard_sequence` and siblings).
- [ ] **Step 4: Run** lib tests — PASS.
- [ ] **Step 5: Commit** `Give each shard its own sequence`.

### Task A2: Opaque `since` tokens on a single node

**Files:**
- Create: `src/since.rs` (token encoding).
- Modify: `src/api/changes.rs`, `src/api/database.rs` (`update_seq`, `purge_seq` in db info), `src/fabric.rs` (`encode_since`/`decode_since`/`since_component` replaced).
- Test: `tests/conformance.rs`.

**Interfaces:**
- Produces: `pub struct CopyPos { pub node: usize, pub shard: usize, pub seq: u64, pub uuid: String }`; `pub fn encode(sum: u64, copies: &[CopyPos]) -> String` producing CouchDB's shape `"<sum>-<opaque base64url>"`; `pub fn decode(raw: &str, q: usize) -> Result<Vec<CopyPos>, ApiError>`; `pub fn parse_since(raw: Option<&str>) -> Since` with `enum Since { Zero, Now, Token(Vec<CopyPos>) }`. Single node uses `node = 0`.

- [ ] **Step 1: Failing conformance tests.** `since_token_round_trips` (feed with `limit=1` repeatedly from `last_seq` returns every doc exactly once); `since_zero_and_now_match_couchdb` (`since=0` returns all, `since=now` returns none and a usable `last_seq`); `malformed_since_is_400` (`since=xyz` → 400 `bad_request`); `update_seq_is_opaque_and_usable_as_since`; `live_changes_cursor_misses_nothing_under_concurrent_writes` (port of the 150,959-document scenario at 20,000 documents in 500-doc batches, four shards, readers forwarding `last_seq`; asserts every id received).
- [ ] **Step 2: Run** `cargo test --test conformance since_ update_seq_is_opaque live_changes_cursor` — FAIL.
- [ ] **Step 3: Implement** `src/since.rs` and wire it through `_changes`, db info and feeds (normal, longpoll, continuous, eventsource).
- [ ] **Step 4: Run** conformance — PASS.
- [ ] **Step 5: Commit** `Use opaque per-shard since tokens`.

### Task A3: Cluster tokens per shard copy

**Files:**
- Modify: `src/fabric.rs` (`cluster_changes`, `fetch_node_changes`, `cluster_seq_vectors`, `cluster_seq_tokens`; delete `FallbackMark`, `UNKNOWN_SEQ`), `src/api/changes.rs`.
- Test: `tests/conformance.rs`.

**Interfaces:**
- Consumes: `src::since::{CopyPos, encode, decode}`.
- Produces: `fn resume_position(want: &CopyPos, serving: (usize /*node*/, &str /*uuid*/), checkpoints: &Checkpoints) -> u64` — the same copy resumes at `want.seq`; a different copy translates through the anti-entropy checkpoint that recorded `want.uuid`'s position (CouchDB `mem3_rep:find_source_seq`), else 0.

- [ ] **Step 1: Failing tests.** `cluster_since_resumes_on_the_same_copy`; `cluster_since_translates_through_checkpoints_when_a_copy_is_down` (stop the node the token names; the feed resumes on another copy without skipping any document written before the stop); `cluster_since_restarts_a_range_when_no_checkpoint_matches` (replays, never skips); `since_from_a_recreated_database_restarts` (delete and recreate; old token's uuid mismatch → from 0).
- [ ] **Step 2: Run** those tests — FAIL.
- [ ] **Step 3: Implement** per-copy tokens and `resume_position`; delete the per-node vector code.
- [ ] **Step 4: Run** conformance — PASS.
- [ ] **Step 5: Commit** `Key cluster since tokens by shard copy`.

### Task A4: Anti-entropy checkpoints per (peer, shard)

**Files:**
- Modify: `src/replicate.rs` (`pull`, `pull_feed`, `checkpoint_id`), `src/api/changes.rs` (internal `_changes?shards=` returns per-shard positions).
- Test: `src/replicate.rs` tests, `tests/conformance.rs`.

**Interfaces:**
- Produces: checkpoint id `ae3:<peer>:<shard>:<peer shard uuid>`; value `{ "seq": u64, "source_uuid": String, "target_seq": u64 }` (the pair `resume_position` needs).

- [ ] **Step 1: Failing tests.** `anti_entropy_resumes_each_shard_from_its_own_checkpoint`; `a_peer_shard_with_a_new_uuid_is_pulled_from_zero`.
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** lib and conformance — PASS.
- [ ] **Step 5: Commit** `Checkpoint anti-entropy per shard copy`.

### Task A5: View and search cursors per shard

**Files:**
- Modify: `src/view.rs` (`IndexMeta.last_seq` → per-shard), `src/search.rs` (`indexable_changes`, commit payload), `src/db.rs` (`changes_rows_only` takes `ShardSince` bounds).
- Test: `src/view.rs`, `src/search.rs` tests.

**Interfaces:**
- Produces: `pub struct IndexCursor(pub Vec<(String /*uuid*/, u64)>)` serialised in the view meta and as the Tantivy commit payload; `fn cursor_valid(cursor: &IndexCursor, db: &Db) -> bool` (false when any uuid differs → full rebuild).

- [ ] **Step 1: Failing tests.** `view_update_reads_each_shard_after_its_own_cursor` (shard 1 lagging behind shard 0 still has its changes indexed); `search_update_reads_each_shard_after_its_own_cursor`; `index_rebuilds_when_a_shard_uuid_changes`; `incremental_update_is_bounded_by_each_shards_start_position` (writes during an update are picked up by the next update, not skipped).
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** lib and conformance — PASS.
- [ ] **Step 5: Commit** `Track view and search cursors per shard`; open PR "Per-shard sequences", merge when CI is green; dispatch the Jepsen nightly on the branch first and require it green.

---

## Phase B — redb

### Task B1: redb foundation: open, checksummed values, test backend

**Files:**
- Create: `src/kv.rs` (open, tables, value codec), `src/kv/powercut.rs` (`#[cfg(test)]` backend).
- Modify: `Cargo.toml` (`redb = "4.6"`, `crc32c`), `src/cli.rs` (`--cache-mb`, default 16).
- Test: `src/kv.rs` tests.

**Interfaces:**
- Produces: `pub fn open(path: &Path, cache_bytes: usize) -> Result<redb::Database>` (sets integrity-on-open and cache size); `pub fn open_with(backend: impl redb::StorageBackend, cache_bytes: usize) -> Result<redb::Database>`; table definitions `META: TableDefinition<&str, &[u8]>`, `BY_SEQ: <u64, &str>`, `BODIES: <(&str, &str), &[u8]>`, `LOCAL: <&str, &[u8]>`, `PURGE_INFOS: <u64, &[u8]>`, `PURGED_REVS: <(&str, &str), u64>`, `PURGE_UUIDS: <&str, u64>`, `CHECKPOINTS: <&str, &[u8]>`, `SETTINGS: <&str, &[u8]>`; `pub fn seal(value: &[u8]) -> Vec<u8>` and `pub fn unseal(stored: &[u8]) -> Result<&[u8], Corrupt>` (CRC32C, little-endian, appended); every value table stores sealed bytes (key-only tables `BY_SEQ`, `PURGED_REVS`, `PURGE_UUIDS` seal their values too).
- Produces (test only): `PowerCut::new() -> (PowerCut, PowerSwitch)`; `PowerSwitch::cut(&self)` discards every write since the last `sync_data`; `PowerSwitch::fail_writes(&self, error: std::io::ErrorKind)` makes later writes fail (used for `ENOSPC`); the backend survives the cut so it can be reopened.

- [ ] **Step 1: Failing tests.** `unseal_rejects_a_flipped_bit`; `committed_writes_survive_a_power_cut` (commit, cut, reopen: present); `uncommitted_writes_vanish_in_a_power_cut`; `a_build_without_sync_loses_data` (a backend whose `sync_data` is a no-op loses the commit after a cut — proves the test can fail).
- [ ] **Step 2: Run** `cargo test --lib kv::` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** `Add the redb storage foundation`.

### Task B2: Shards on redb with one write transaction per group

**Files:**
- Modify: `src/store.rs` (replace `DB`, `Tx`, `pending`, `overlay`, `flush_pending`, `restore_from_disk`, `flush_failure`, `JobMark`, `rollback_to`, prefix keys, `prefix_successor`, `seek_last`, corruption probes, recount-on-open; counters and `revs_limit`, `purged_infos_limit`, uuid, seq into `SETTINGS`), `src/db.rs` (callers).
- Test: `src/store.rs` tests; new model test in `src/store/model_test.rs`.

**Interfaces:**
- Produces: shard jobs receive `&mut ShardTxn` (the open `redb::WriteTransaction` plus the shard's in-memory indexes) instead of `&mut Shard`; `ShardTxn::put_doc`, `delete_doc`, `put_local`, `record_purge`, … keep today's names and semantics. A job validates (reads and decodes every record it will change) before its first insert. A job that returns `Err` after its first insert, or panics, aborts the group: the transaction is dropped and every job in the group gets the error. The actor commits once per group and acknowledges only after `commit()` returns.
- Produces: in-memory `LatestChanges` and live-key rank updated only after a successful commit (from the committed keys), so actor-side readers see exactly the committed state.

- [ ] **Step 1: Failing tests.** `a_group_commits_atomically_and_later_jobs_see_earlier_writes`; `a_panicking_job_fails_its_whole_group_and_writes_nothing`; `a_job_error_before_writing_fails_only_that_job`; `a_job_error_after_writing_fails_the_whole_group`; `acknowledged_writes_survive_a_power_cut` (shard over `PowerCut`, random groups, cut at random points, reopen, every acknowledged write present); model test `shard_matches_a_model_under_random_operations` (puts, deletes, purges, `_local`, groups, reopen, power cuts vs a `BTreeMap` model; 20,000 operations, fixed seed, plus a seeded proptest).
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement.** Delete every item listed under Files; nothing LevelDB-shaped stays in `store.rs`.
- [ ] **Step 4: Run** lib and conformance — PASS.
- [ ] **Step 5: Commit** `Store shards in redb`.

### Task B3: Lease gate and reads off the actor

**Files:**
- Create: `src/store/gate.rs`.
- Modify: `src/store.rs`, `src/db.rs` (`get_doc`, `get_docs`, `revs_diff`, `open_revs`, `_bulk_get`, quorum-read and anti-entropy fetch paths move to leases), `src/view.rs` (page reads).
- Test: `src/store/gate.rs` tests, `src/db.rs` tests.

**Interfaces:**
- Produces: `pub struct Gate`; `Gate::read<R: Send + 'static>(&self, f: impl FnOnce(&redb::ReadTransaction) -> Result<R> + Send) -> impl Future<Output = Result<R>>`: takes a lease, opens a read transaction, runs `f` on the blocking pool, returns, and drops the transaction and the lease. Because `f` receives a borrow and must return an owned `R: 'static`, no transaction, table, iterator or value guard can outlive its lease — this is the structural rule; no other API hands out the `Database`. `Gate::reopen(&self, open: impl FnOnce() -> Result<redb::Database>) -> Result<()>`: closes the gate (new `read` calls wait), waits until every lease is returned, drops the old `Database`, opens the new one, opens the gate. Streamed reads call `read` once per page.
- Reads needing `LatestChanges` or the live-key rank (`_changes` `pending`, `_all_docs` `offset`/`total_rows`, partition counts) stay on the actor (spec, call 2).

- [ ] **Step 1: Failing tests.** `reopen_waits_for_an_outstanding_lease` (a lease held; `reopen` completes only after it is released, observed with a oneshot, no sleeps); `reads_requested_during_reopen_run_on_the_new_handle`; `reads_run_concurrently_with_a_long_write_group` (a read completes while a group commit is blocked on a barrier inside its job).
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** lib and conformance — PASS.
- [ ] **Step 5: Commit** `Read shards through leases off the actor`.

### Task B4: Recovery from a failed commit

**Files:**
- Modify: `src/store.rs` (actor loop), `src/store/gate.rs`.
- Test: `src/store.rs` tests.

**Interfaces:**
- Consumes: `Gate::reopen`, `PowerSwitch::fail_writes`.
- Produces: `ShardState::{Serving, Failed(String)}` on the handle; after a commit error the actor fails the group, calls `Gate::reopen` with a fresh `kv::open`, rebuilds settings and in-memory indexes from the committed tables, and serves again; if reopen fails it enters `Failed` and every request to the shard returns 500 until the database is reopened. Recovery bumps `ShardHandle::generation: AtomicU64` (consumed by C1).

- [ ] **Step 1: Failing tests.** `a_failed_commit_reopens_the_shard_and_keeps_acknowledged_writes` (inject `ENOSPC`, then clear it: group fails with 500, earlier acknowledged writes present, next write succeeds); `a_shard_that_cannot_reopen_fails_closed`; `recovery_waits_for_a_reader_holding_a_lease`; `a_continuous_feed_survives_recovery_without_skipping` (Review Focus 3).
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** `Recover shards from failed commits`.

### Task B5: Database metadata in redb; staged create; tombstoned delete

**Files:**
- Modify: `src/db.rs` (delete `read_metadata`, `write_json_atomic`, `db.json`, `shard-map.json`, `pending-shard-map.json`, `security-versioned.json` handling, stale-temp cleanup; read and write those values in shard 0's `SETTINGS`), `src/state.rs` (create, delete, start-up recovery, `db_names`).
- Test: `tests/conformance.rs`, `src/state.rs` tests.

**Interfaces:**
- Produces: `fn create_db_staged(root: &Path, name: &str, settings: &DbSettings) -> Result<()>` (staging dir, shard files, settings committed last, fsync staging dir, rename to the encoded name, fsync parent); `fn delete_db(root, name)` (rename to `.deleted-<uuid>`, fsync parent, remove in background); `fn recover_root(root: &Path) -> Result<()>` (removes only `.creating-*` and `.deleted-*`, fsyncs parent). A published database whose shard is missing or unopenable is listed and returns 500 (fail closed); its files are untouched.

- [ ] **Step 1: Failing tests.** `a_crash_during_create_leaves_no_database` (stop after each step via a test hook; restart; not listed, staging gone); `a_crash_after_publish_keeps_the_database`; `a_crash_during_delete_completes_the_delete`; `a_missing_shard_fails_closed_and_keeps_the_other_files`; `names_needing_encoding_create_list_delete_and_recover` (Review Focus 2: `a/b`, `$x`, `(y)+z`).
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** lib and conformance — PASS.
- [ ] **Step 5: Commit** `Keep database metadata in redb and stage creation`.

### Task B6: View indexes on redb

**Files:**
- Modify: `src/view.rs` (one `.redb` per view via `kv::open` with 4 MiB cache; rows, meta and `IndexCursor` in the same transaction; sealed values).
- Test: `src/view.rs` tests, conformance view tests.

- [ ] **Step 1: Failing test.** `view_rows_and_cursor_commit_together` (power cut mid-update: on reopen the cursor and rows agree); `a_corrupt_view_row_is_a_500_not_a_missing_row`.
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** lib and conformance — PASS.
- [ ] **Step 5: Commit** `Store view indexes in redb`.

### Task B7: Corruption and crash scripts for redb

**Files:**
- Modify: `scripts/storage_faults.py` (redb single files: SIGKILL mid-write, truncation, bit flips in a value page and a branch page, removed shard file, damage while open), `.github/workflows/*.yml` if the script's invocation changes.
- Test: the script itself, run locally (≤ 15 min).

- [ ] **Step 1: Rewrite the scenarios** with split acceptance: SIGKILL keeps every acknowledged document; deliberate damage fails closed (500 or refused open), never wrong data, never a crash loop; damage to a value while open is a 500 on read; damage to a branch page while open is detected at the next open (spec's documented limit).
- [ ] **Step 2: Run** `timeout 900 python3 scripts/storage_faults.py` against a release build — every scenario PASS.
- [ ] **Step 3: Commit** `Fault-test redb storage`.

### Task B8: Remove LevelDB

**Files:**
- Delete: `vendor/rusty-leveldb/`, `docs/storage-dependency.md`, `docs/upstream/rusty-leveldb-issues.md`.
- Modify: `Cargo.toml` (drop `rusty-leveldb` and `[patch.crates-io]`), `Cargo.lock`, `Dockerfile` (vendor `COPY`), CI workflow (vendored-tests step), `README.md`, `PLAN.md`, `docs/consistency.md` (durability now real; the tree-page corruption limit), `docs/couchdb-comparison.md`, `jepsen-clj/README.md` (fsync note).

- [ ] **Step 1:** `grep -rn "leveldb\|LevelDB" src tests scripts docs Dockerfile .github README.md PLAN.md` returns only historical bug notes under `docs/bugs/`.
- [ ] **Step 2: Run** full local QA and `cargo test --test dependencies` — PASS.
- [ ] **Step 3: Commit** `Remove rusty-leveldb`; open PR "Store data in redb", dispatch the Jepsen nightly on the branch, merge when it and CI are green.

---

## Phase C — online compaction

### Task C1: Commit journal with atomic detach and generations

**Files:**
- Create: `src/store/journal.rs`.
- Modify: `src/store.rs` (record after each successful commit).

**Interfaces:**
- Produces: `pub struct Journal { inner: Mutex<Option<(u64 /*generation*/, BTreeSet<(Table, Vec<u8>)>)>> }`; `Journal::start(&self, generation: u64)`; `Journal::record(&self, keys: impl IntoIterator<Item = (Table, Vec<u8>)>)` (called by the actor after `commit()` returns, before acknowledging; no-op when stopped; aborted transactions record nothing); `Journal::take(&self) -> Option<(u64, BTreeSet<(Table, Vec<u8>)>)>` swaps the set out under the lock (`std::mem::take`) so a commit recording concurrently lands either in this batch or the next, never lost; `Journal::stop(&self)`. `Table` is an enum over every table in `kv`.

- [ ] **Step 1: Failing tests.** `concurrent_records_and_takes_lose_no_key` (two threads: one records 100,000 distinct keys, one repeatedly `take`s; the union of batches equals the recorded set); `an_aborted_transaction_records_nothing`; `take_reports_the_generation_it_started_with`.
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** `Journal committed keys for compaction`.

### Task C2: Copy, catch up, swap

**Files:**
- Create: `src/store/compact.rs`.
- Modify: `src/store.rs` (`_compact` entry), `src/api/database.rs` (`_compact` and `_active_tasks` progress as CouchDB reports it), start-up removes leftover `*.compact.redb`.

**Interfaces:**
- Consumes: `Journal`, `Gate::read`, `Gate::reopen`, `ShardHandle::generation`.
- Produces: `pub async fn compact_shard(handle: &ShardHandle) -> Result<()>`: starts the journal at the current generation; copies a snapshot (metadata, leaf bodies only, all other tables, seq, uuid) into `shard-NNNN.compact.redb`; catch-up rounds `take` the journal and, for each key, read its current value from a fresh lease: present → upsert, absent → delete; a body key is upserted only if its revision is still a leaf, otherwise deleted from the new file even if copied earlier; final round on the actor with writes paused, then fsync, `Gate::reopen` performing the rename and directory fsync, journal stopped. If `take` returns a generation other than the one started (a failed-commit recovery happened), the compaction deletes its file and restarts from step one.

- [ ] **Step 1: Failing tests**, each with the mutation landing during the copy, during catch-up, and during the final swap (barriers, no sleeps): `compaction_keeps_concurrent_updates_and_tombstones`; `a_document_purged_during_compaction_does_not_return`; `local_docs_settings_security_and_checkpoints_changed_during_compaction_survive`; `purge_info_trimming_during_compaction_is_kept`; `bodies_that_became_non_leaf_during_compaction_are_removed_including_already_copied_ones`; `compaction_restarts_after_failed_commit_recovery` (inject a commit failure mid-compaction; final file equals a fresh compaction of the recovered state); `uuid_is_unchanged_so_indexes_do_not_rebuild` (Review Focus 5); `a_continuous_feed_streams_across_the_swap_without_skip_or_repeat` (Review Focus 3); `non_leaf_bodies_are_dropped`.
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** `Compact shards online`.

### Task C3: Crash safety of compaction

**Files:**
- Modify: `src/store/compact.rs` (test hooks), `scripts/storage_faults.py` (kill during compaction).

- [ ] **Step 1: Failing tests.** `a_crash_before_the_rename_leaves_the_live_file_and_removes_the_leftover`; `a_power_cut_after_the_rename_keeps_the_compacted_file_complete` (PowerCut).
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** lib, conformance and `storage_faults.py` — PASS.
- [ ] **Step 5: Commit** `Make compaction crash-safe`; open PR "Online compaction", merge when CI and the Jepsen nightly on the branch are green.

---

## Phase D — verification

### Task D1: End-to-end and measurement

- [ ] **Step 1:** Run, each bounded: `differential/run.py` against CouchDB (all modes), the PouchDB client suite, and the three Jepsen soaks locally (`no-fault`, `kill`, `partition`, 5 min each). All PASS.
- [ ] **Step 2:** Measure import and write throughput (bulk import of the 150,959-document airline fixture; `loadgen` write mix) on the last pre-redb commit and on `main`, same machine, same flags. Record both in the final PR description. Not a gate.
- [ ] **Step 3:** Clear every build, data and Docker artifact created by the runs.

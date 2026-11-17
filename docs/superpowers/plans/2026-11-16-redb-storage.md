# redb Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the vendored rusty-leveldb with redb so every acknowledged write is durable, follow CouchDB's per-shard design, and delete the machinery douchdb built around LevelDB.

**Architecture:** Four phases, each merged as its own PR with every suite green. A: per-shard sequences and opaque `since` tokens, still on LevelDB (removes the global counter and watermarks independently of the engine). B: each shard and view becomes a redb file; group commit is one write transaction; reads take leases through a gate and run off the actor; database metadata moves into redb; creation is staged; `_compact` is an interim online old-body sweep. C: replaces that sweep with online, journal-driven copy-and-swap compaction. D: end-to-end verification.

**Tech Stack:** Rust, redb 4.6 (`Durability::Immediate`), `crc32c`, tokio, Jepsen (Clojure) harness, Python fault scripts.

**Spec:** `docs/superpowers/specs/2026-11-16-redb-storage-design.md` (approved at `add7bab`). Read it before any task.

## Global Constraints

- redb `= "4.6"`; every write transaction commits with `Durability::Immediate` (the default; never set `None`) and in two phases (`set_two_phase_commit(true)`), begun only through `kv::begin_write`, so damage to the newest commit is reported, never rolled back; cost: a second fsync per commit.
- Every stored value carries a CRC32C appended on write and verified on every read; a mismatch is a corruption error (HTTP 500), never a miss.
- Full integrity check on the **first open of each file in a process** (`Builder::set_check_integrity_on_open(true)`) and on **idle close** (`Database::check_integrity`, in the background, gate drained); a file that passed its close-time check opens without the check later in the same process. Never on every open.
- `ConcurrencyMode::ExclusiveWriter` (default) on every file.
- Page cache per file: shard files `--cache-mb` (new flag, default 16), view files a fixed 4 MiB.
- File names: shard `<db>/shard-NNNN.redb`; view `<db>/_view_indexes/<md5>.redb`; compaction `shard-NNNN.compact.redb`; staging `.creating-<uuid>`; tombstone `.deleted-<uuid>` (a leading `.` no database name can have).
- Follow current CouchDB for every observable behaviour; any divergence must be one already named in the spec.
- No migration or compatibility code for earlier douchdb data (none exists).
- Every value type in every redb table is `&[u8]` holding sealed bytes (`kv::seal`); keys are typed. No table stores an unsealed value.
- Run several named tests with libtest filters after `--`: `cargo test --lib -- name_a name_b`, `cargo test --test conformance -- name_a name_b`.
- Execution: one fresh implementer per task, then an independent reviewer, sequentially. The integration owner (the coordinating session) owns every **Interfaces** block, resolves cross-task mismatches, and runs the full phase review before each merge.
- Merge gate for every PR: CI and the Jepsen nightly dispatched on the branch must both be green **on the exact head commit being merged**; check the run's `headSha` equals the PR head before merging.
- Project rules: `cargo clippy --all-targets -- -D warnings` clean, never `#[allow(...)]`; delete unused code; tests wait on conditions, never fixed sleeps or wall-clock bounds (use tokio's paused clock for timer behaviour); every run bounded (≤ 15 min); `CARGO_TARGET_DIR` and temp data under `/Volumes/Space2/douchdb-scratch`; one cargo build or agent at a time; per-PR QA is fmt, clippy, lib tests, conformance once; branch every PR from `main`.

## Review Focus

1. **Client `since` values that are not douchdb tokens.** PouchDB and CouchDB clients send `since=0`, `since=now`, or a token from another server. Expected: `0` and `now` behave as in CouchDB; anything unparseable is `400 bad_request` as CouchDB answers, never a silent restart from 0. Test in Task A2.
2. **Database names that need encoding** (`a/b`, `$x`, `(y)+z`). Expected: created, listed, deleted and recovered exactly like plain names; staging and tombstone directories never collide with them. Test in Task B5.
3. **A continuous `_changes` feed open across a compaction swap or a failed-commit reopen.** Expected: the feed does not block the reopen (leases are per page), keeps streaming afterwards, and neither skips nor repeats. Test in Tasks B4 and C2.
4. **Disk full during a commit.** Expected: the group fails with 500, nothing acknowledged is lost, the shard recovers through the gate once writes succeed again, and it fails closed if reopening fails. Test in Task B4 (power-cut backend with an injected `ENOSPC`).
5. **A view or search query while its database compacts.** Expected: the shard uuid is unchanged by compaction, so the index keeps its cursors and does not rebuild. Test in Task C2.

---

## Phase A — per-shard sequences (still on LevelDB)

### Task A1: The `since` token module

**Files:**
- Create: `src/since.rs` (`pub mod since` in `src/lib.rs`, so it is reachable and testable before its callers exist).
- Test: `src/since.rs` tests.

**Interfaces:**
- Produces: `pub struct CopyPos { pub node: usize, pub shard: usize, pub seq: u64, pub uuid: String }`; `pub fn encode(copies: &[CopyPos]) -> String` producing CouchDB's shape `"<sum of seqs>-<opaque base64url>"`; `pub fn decode(raw: &str) -> Result<Vec<CopyPos>, SinceError>`; `pub enum Since { Zero, Now, Token(Vec<CopyPos>) }`; `pub fn parse(raw: Option<&str>) -> Result<Since, SinceError>` (`None`/`"0"` → `Zero`, `"now"` → `Now`, anything undecodable → `SinceError`, which callers map to `400 bad_request`).

- [ ] **Step 1: Failing tests.** `encode_decode_round_trips`; `zero_and_now_parse_like_couchdb`; `garbage_is_an_error_not_zero`; `encoded_prefix_is_the_sum_of_positions`.
- [ ] **Step 2: Run** `cargo test --lib -- since::` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `cargo test --lib -- since::` — PASS.
- [ ] **Step 5: Commit** `Add opaque since tokens`.

### Task A2: Switch to per-shard sequences everywhere

One task because every consumer of the sequence changes together; a partial switch would compile but skip changes. The integration owner reviews it against every caller listed.

**Files:**
- Modify: `src/store.rs` (shard-local sequence, shard uuid persisted under `z/uuid` until B2 moves it), `src/db.rs` (delete the global counter, `allocate_seq`, `committed_seq`, `reserved_seq`, `changes_watermark`, `changes_watermark_from`, global-order merge; `ChangesOpts.since: ShardSince`), `src/api/changes.rs` and `src/api/database.rs` (`since`/`last_seq`/`update_seq` via `since`), `src/fabric.rs` (cluster tokens keyed by copy via `since::CopyPos`; delete `encode_since`, `decode_since`, `since_component`, `FallbackMark`, `UNKNOWN_SEQ`; a token naming a copy that cannot serve restarts that shard range from 0 — replay, never skip), `src/replicate.rs` (checkpoint per (peer, shard): id `ae3:<peer>:<shard>:<peer shard uuid>`, value `{ "seq": u64, "source_uuid": String, "target_seq": u64 }`), `src/view.rs` and `src/search.rs` (cursor per shard).
- Test: `src/db.rs`, `src/store.rs`, `src/view.rs`, `src/search.rs` tests; `tests/conformance.rs`. Delete the watermark tests (`changes_cursor_stops_at_allocated_but_uncommitted_shard_sequence` and siblings).

**Interfaces:**
- Consumes: `since::{CopyPos, Since, encode, parse}`.
- Produces: `Shard::next_seq(&mut self) -> u64`; `Shard::uuid(&self) -> &str`; `ShardHandle::uuid(&self) -> String`; `pub struct ShardSince(pub Vec<u64>)` (one position per shard); `Db::shard_seqs(&self) -> Vec<(String, u64)>` (uuid, committed position per shard); `pub struct IndexCursor(pub Vec<(String, u64)>)` stored in view meta and as the Tantivy commit payload; `fn cursor_valid(cursor: &IndexCursor, db: &Db) -> bool` (any uuid mismatch → rebuild from 0).

- [ ] **Step 1: Failing tests.** Lib: `shard_sequences_are_independent` (q=2, three writes to shard 0 and one to shard 1 → `[3, 1]`); `shard_uuid_survives_reopen`; `view_update_reads_each_shard_after_its_own_cursor`; `search_update_reads_each_shard_after_its_own_cursor`; `index_rebuilds_when_a_shard_uuid_changes`; `incremental_update_is_bounded_by_each_shards_start_position`; `anti_entropy_resumes_each_shard_from_its_own_checkpoint`; `a_peer_shard_with_a_new_uuid_is_pulled_from_zero`. Conformance: `since_token_round_trips`; `since_zero_and_now_match_couchdb`; `malformed_since_is_400` (Review Focus 1); `update_seq_is_opaque_and_usable_as_since`; `live_changes_cursor_misses_nothing_under_concurrent_writes` (20,000 documents in 500-document batches, q=4, one reader per node forwarding `last_seq`; every id received); `cluster_since_resumes_on_the_same_copy`; `cluster_since_replays_a_range_when_its_copy_is_down` (no skipped document); `since_from_a_recreated_database_restarts`.
- [ ] **Step 2: Run** `cargo test --lib -- shard_sequences shard_uuid view_update search_update index_rebuilds incremental_update anti_entropy_resumes a_peer_shard` and `cargo test --test conformance -- since_ update_seq_is_opaque live_changes_cursor cluster_since` — FAIL.
- [ ] **Step 3: Implement** across every file listed.
- [ ] **Step 4: Run** full lib and conformance — PASS; `grep -n "allocate_seq\|reserved_seq\|changes_watermark\|FallbackMark\|UNKNOWN_SEQ" -r src` is empty.
- [ ] **Step 5: Commit** `Give each shard its own sequence`.

### Task A3: Resume on another copy through checkpoints

> **Superseded.** This task was implemented and then reverted. The
> `resume_position` rule below is inverted: it translates through a
> checkpoint of the serving copy reading the named copy, which says nothing
> about changes the serving copy took from elsewhere below its `target_seq`,
> so a resumed feed could skip them. A feed whose copy cannot serve now
> replays that range from 0 on the serving copy. The correct translation
> needs the serving copy's push history to the named copy (the direction of
> CouchDB's `mem3_rep:find_source_seq`) and is issue #52. The original text
> is kept below for the record; do not implement it.

**Files:**
- Modify: `src/fabric.rs` (cluster `_changes` when a token's copy cannot serve), `src/since.rs`.
- Test: `src/since.rs`, `tests/conformance.rs`.

**Interfaces:**
- Produces: `pub fn resume_position(want: &CopyPos, serving_uuid: &str, checkpoints: &[Checkpoint]) -> u64` in `src/since.rs`, where `pub struct Checkpoint { pub source_uuid: String, pub seq: u64, pub target_uuid: String, pub target_seq: u64 }`: same uuid → `want.seq`; a checkpoint from `want.uuid` into `serving_uuid` with `seq <= want.seq` → its `target_seq` (CouchDB `mem3_rep:find_source_seq`); else 0.

- [ ] **Step 1: Failing tests.** `resume_on_same_copy_uses_its_position`; `resume_on_other_copy_uses_the_latest_checkpoint_not_past_the_position`; `resume_without_a_matching_checkpoint_is_zero`; conformance `cluster_since_translates_through_checkpoints_when_a_copy_is_down` (fewer rows replayed than from 0, none skipped).
- [ ] **Step 2: Run** `cargo test --lib -- since::resume` and `cargo test --test conformance -- cluster_since_translates` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** lib and conformance — PASS.
- [ ] **Step 5: Commit** `Resume cluster feeds on another copy via checkpoints`; open PR "Per-shard sequences"; merge per the merge gate.

---

## Phase B — redb

### Task B1: redb foundation: open, checksummed values, test backend

**Files:**
- Create: `src/kv.rs` (open, tables, value codec), `src/kv/powercut.rs` (`#[cfg(test)]` backend).
- Modify: `Cargo.toml` (`redb = "4.6"`, `crc32c`), `src/cli.rs` (`--cache-mb`, default 16).
- Test: `src/kv.rs` tests.

**Interfaces:**
- Produces: `pub fn open(path: &Path, cache_bytes: usize) -> Result<redb::Database>` (sets the cache size; enables the integrity check unless `path` is in the process-wide verified set); `pub fn close_verified(db: redb::Database, path: &Path) -> Result<()>` (runs `check_integrity` and records `path` as verified on success, removes it on failure; called off the request path when an idle database closes); the verified set is `static VERIFIED: Mutex<HashSet<PathBuf>>`, and a path leaves it whenever its file is replaced (compaction swap, delete, recreate); `pub fn open_with(backend: impl redb::StorageBackend, cache_bytes: usize) -> Result<redb::Database>`; table definitions, every value `&[u8]` holding sealed bytes: `META: TableDefinition<&str, &[u8]>` (id → DocMeta JSON), `BY_SEQ: <u64, &[u8]>` (seq → id), `BODIES: <(&str, &str), &[u8]>` ((id, rev) → body), `LOCAL: <&str, &[u8]>`, `PURGE_INFOS: <u64, &[u8]>`, `PURGED_REVS: <(&str, &str), &[u8]>` (→ purge seq as 8 bytes BE), `PURGE_UUIDS: <&str, &[u8]>` (→ purge seq), `CHECKPOINTS: <&str, &[u8]>`, `SETTINGS: <&str, &[u8]>`; `pub fn seal(value: &[u8]) -> Vec<u8>` and `pub fn unseal(stored: &[u8]) -> Result<&[u8], Corrupt>` (CRC32C, little-endian, appended).
- Produces (test only): `PowerCut::new() -> (PowerCut, PowerSwitch)`; `PowerSwitch::cut(&self)` discards every write since the last `sync_data`; `PowerSwitch::fail_writes(&self, error: std::io::ErrorKind)` makes later writes fail (used for `ENOSPC`); the backend survives the cut so it can be reopened.

- [ ] **Step 1: Failing tests.** `first_open_rejects_a_file_damaged_while_closed`; `reopen_after_a_verified_close_skips_the_check` (observed through a counting backend: the reopen reads only the header, not every page); `a_failed_close_check_forces_a_check_on_the_next_open`; `unseal_rejects_a_flipped_bit`; `committed_writes_survive_a_power_cut` (commit, cut, reopen: present); `uncommitted_writes_vanish_in_a_power_cut`; `a_build_without_sync_loses_data` (a backend whose `sync_data` is a no-op loses the commit after a cut — proves the test can fail).
- [ ] **Step 2: Run** `cargo test --lib -- kv::` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `cargo test --lib -- kv::` — PASS.
- [ ] **Step 5: Commit** `Add the redb storage foundation`.

### Task B2: Shards on redb with one write transaction per group

**Files:**
- Modify: `src/store.rs` (replace `DB`, `Tx`, `pending`, `overlay`, `flush_pending`, `restore_from_disk`, `flush_failure`, `JobMark`, `rollback_to`, prefix keys, `prefix_successor`, `seek_last`, corruption probes, recount-on-open; counters and `revs_limit`, `purged_infos_limit`, uuid, seq into `SETTINGS`), `src/db.rs` (callers).
- Test: `src/store.rs` tests; new model test in `src/store/model_test.rs`.

**Interfaces:**
- Produces: shard jobs receive `&mut ShardTxn` (the open `redb::WriteTransaction` plus the shard's in-memory indexes) instead of `&mut Shard`; `ShardTxn::put_doc`, `delete_doc`, `put_local`, `record_purge`, … keep today's names and semantics. A job validates (reads and decodes every record it will change) before its first insert. A job that returns `Err` after its first insert, or panics, aborts the group: the transaction is dropped and every job in the group gets the error. The actor commits once per group and acknowledges only after `commit()` returns.
- Produces: in-memory `LatestChanges` and live-key rank updated only after a successful commit (from the committed keys), so actor-side readers see exactly the committed state.

- [ ] **Step 1: Failing tests.** `a_group_commits_atomically_and_later_jobs_see_earlier_writes`; `a_panicking_job_fails_its_whole_group_and_writes_nothing`; `a_job_error_before_writing_fails_only_that_job`; `a_job_error_after_writing_fails_the_whole_group`; `acknowledged_writes_survive_a_power_cut` (shard over `PowerCut`, random groups, cut at random points, reopen, every acknowledged write present); model test `shard_matches_a_model_under_random_operations` (puts, deletes, purges, `_local`, groups, reopen, power cuts vs a `BTreeMap` model; 20,000 operations, fixed seed, plus a seeded proptest).
- [ ] **Step 2: Run** `cargo test --lib -- a_group_commits a_panicking_job a_job_error acknowledged_writes_survive shard_matches_a_model` — FAIL.
- [ ] **Step 3: Implement.** Delete every item listed under Files; nothing LevelDB-shaped stays in `store.rs`. Interim `_compact` (until C2): the existing old-body sweep, run as chunked write transactions on the actor; no file shrink. Existing compaction tests that check which bodies survive must pass unchanged; any that assert a smaller file move to C2.
- [ ] **Step 4: Run** lib and conformance — PASS.
- [ ] **Step 5: Commit** `Store shards in redb`.

### Task B3: Lease gate and reads off the actor

**Files:**
- Create: `src/store/gate.rs` (in a `#[doc(hidden)] pub mod` so its doctests compile against it).
- Modify: `src/store.rs`, `src/db.rs` (`get_doc`, `get_docs`, `revs_diff`, `open_revs`, `_bulk_get`, quorum-read and anti-entropy fetch paths move to leases), `src/view.rs` (page reads).
- Test: `src/store/gate.rs` unit tests and `compile_fail` doctests; `src/db.rs` tests.

**Interfaces:**
- Produces: `pub struct Gate`; `pub struct LeaseTxn<'l>` wrapping a `redb::ReadTransaction` it never exposes. Its only methods return owned data or run a callback inside the call: `get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>>` (unsealed, checked), `scan(&self, table: Table, range: impl RangeBounds<Vec<u8>>, reverse: bool, f: impl FnMut(&[u8], &[u8]) -> Result<bool>) -> Result<()>` (key and unsealed value borrowed only for the callback; `false` stops), `len(&self, table: Table) -> Result<u64>`. No method returns a redb table, transaction, range, iterator or `AccessGuard`, and `LeaseTxn` is `!Send` and not `Clone`. `Gate::read<R: Send + 'static>(&self, f: impl for<'l> FnOnce(&LeaseTxn<'l>) -> Result<R> + Send) -> impl Future<Output = Result<R>>`: takes a lease, opens the transaction, runs `f` on the blocking pool, then drops the transaction and the lease. `Gate::reopen(&self, open: impl FnOnce() -> Result<redb::Database>) -> Result<()>`: close the gate (new `read` calls wait), wait until every lease is returned, drop the old `Database`, open, reopen the gate. Streamed reads call `read` once per page.
- Reads needing `LatestChanges` or the live-key rank (`_changes` `pending`, `_all_docs` `offset`/`total_rows`, partition counts) stay on the actor (spec, call 2).

- [ ] **Step 1: Failing tests.** `compile_fail` doctests: returning `&LeaseTxn`, returning a value borrowed from `scan`'s callback, and moving `LeaseTxn` into a spawned thread each fail to compile. Unit: `reopen_succeeds_after_many_reads` (10,000 `read` calls, then `reopen` completes: no leaked handle keeps the exclusive lock); `reopen_waits_for_an_outstanding_lease` (a lease held inside `read` on a barrier; `reopen` completes only after release, observed with a oneshot, no sleeps); `reads_requested_during_reopen_run_on_the_new_handle`; `reads_run_concurrently_with_a_long_write_group`.
- [ ] **Step 2: Run** `cargo test --lib -- store::gate` and `cargo test --doc store::gate` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** lib, doc and conformance tests — PASS.
- [ ] **Step 5: Commit** `Read shards through leases off the actor`.

### Task B4: Recovery from a failed commit

**Files:**
- Modify: `src/store.rs` (actor loop), `src/store/gate.rs`.
- Test: `src/store.rs` tests.

**Interfaces:**
- Consumes: `Gate::reopen`, `PowerSwitch::fail_writes`.
- Produces: `ShardState::{Serving, Failed(String)}` on the handle; after a commit error the actor fails the group, calls `Gate::reopen` with a fresh `kv::open`, rebuilds settings and in-memory indexes from the committed tables, and serves again; if reopen fails it enters `Failed` and every request to the shard returns 500 until the database is reopened. Recovery runs on the actor and is the hook C1 uses to invalidate a running compaction's journal (`Journal::invalidate`, added in C1, called here).

- [ ] **Step 1: Failing tests.** `a_failed_commit_reopens_the_shard_and_keeps_acknowledged_writes` (inject `ENOSPC`, then clear it: group fails with 500, earlier acknowledged writes present, next write succeeds); `a_shard_that_cannot_reopen_fails_closed`; `recovery_waits_for_a_reader_holding_a_lease`; `a_continuous_feed_survives_recovery_without_skipping` (Review Focus 3).
- [ ] **Step 2: Run** `cargo test --lib -- a_failed_commit a_shard_that_cannot_reopen recovery_waits a_continuous_feed_survives` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `cargo test --lib -- a_failed_commit a_shard_that_cannot_reopen recovery_waits a_continuous_feed_survives` — PASS.
- [ ] **Step 5: Commit** `Recover shards from failed commits`.

### Task B5: Database metadata in redb; staged create; tombstoned delete

**Files:**
- Modify: `src/db.rs` (delete `read_metadata`, `write_json_atomic`, `db.json`, `shard-map.json`, `pending-shard-map.json`, `security-versioned.json` handling, stale-temp cleanup; read and write those values in shard 0's `SETTINGS`), `src/state.rs` (create, delete, start-up recovery, `db_names`).
- Test: `tests/conformance.rs`, `src/state.rs` tests.

**Interfaces:**
- Produces: `fn create_db_staged(root: &Path, name: &str, settings: &DbSettings) -> Result<()>` (staging dir, shard files, settings committed last, fsync staging dir, rename to the encoded name, fsync parent); `fn delete_db(root, name)` (rename to `.deleted-<uuid>`, fsync parent, remove in background); `fn recover_root(root: &Path) -> Result<()>` (removes only `.creating-*` and `.deleted-*`, fsyncs parent). A published database whose shard is missing or unopenable is listed and returns 500 (fail closed); its files are untouched.

- [ ] **Step 1: Failing tests.** `a_crash_during_create_leaves_no_database` (stop after each step via a test hook; restart; not listed, staging gone); `a_crash_after_publish_keeps_the_database`; `a_crash_during_delete_completes_the_delete`; `a_missing_shard_fails_closed_and_keeps_the_other_files`; `names_needing_encoding_create_list_delete_and_recover` (Review Focus 2: `a/b`, `$x`, `(y)+z`).
- [ ] **Step 2: Run** `cargo test --test conformance -- a_crash_during_create a_crash_after_publish a_crash_during_delete a_missing_shard names_needing_encoding` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** lib and conformance — PASS.
- [ ] **Step 5: Commit** `Keep database metadata in redb and stage creation`.

### Task B6: View indexes on redb

**Files:**
- Modify: `src/view.rs` (one `.redb` per view via `kv::open` with 4 MiB cache; rows, meta and `IndexCursor` in the same transaction; sealed values).
- Test: `src/view.rs` tests, conformance view tests.

- [ ] **Step 1: Failing test.** `view_rows_and_cursor_commit_together` (power cut mid-update: on reopen the cursor and rows agree); `a_corrupt_view_row_is_a_500_not_a_missing_row`.
- [ ] **Step 2: Run** `cargo test --lib -- view_rows_and_cursor a_corrupt_view_row` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** lib and conformance — PASS.
- [ ] **Step 5: Commit** `Store view indexes in redb`.

### Task B7: Corruption and crash scripts for redb

**Files:**
- Modify: `scripts/storage_faults.py` (redb single files: SIGKILL mid-write, truncation, bit flips in a value page and a branch page, removed shard file, damage while open), `.github/workflows/*.yml` if the script's invocation changes.
- Test: the script itself, run locally (≤ 15 min).

- [ ] **Step 1: Rewrite the scenarios** with split acceptance: SIGKILL keeps every acknowledged document; deliberate damage fails closed (500 or refused open), never wrong data, never a crash loop; damage to a value while open is a 500 on read; damage to a branch page while open is detected by the next integrity check (idle close, or the first open after a restart; spec's documented limit).
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

### Task C1: Commit journal with atomic detach and invalidation

**Files:**
- Create: `src/store/journal.rs`.
- Modify: `src/store.rs` (record after each successful commit; recovery from B4 calls `invalidate`).

**Interfaces:**
- Produces: `pub enum Table { Meta, BySeq, Bodies, Local, PurgeInfos, PurgedRevs, PurgeUuids, Checkpoints, Settings }`; `pub struct Journal(Mutex<State>)` with `enum State { Stopped, Recording(BTreeSet<(Table, Vec<u8>)>), Invalidated }`; `Journal::start(&self)` (→ `Recording(empty)`); `Journal::record(&self, keys: impl IntoIterator<Item = (Table, Vec<u8>)>)` (actor, after `commit()` returns and before acknowledging; only in `Recording`; aborted transactions record nothing); `Journal::take(&self) -> Taken` with `enum Taken { Batch(BTreeSet<(Table, Vec<u8>)>), Invalidated, Stopped }` swapping the set out under the lock (`std::mem::take`), so a concurrent record lands in this batch or the next, never lost; `Journal::invalidate(&self)` (→ `Invalidated`, called by failed-commit recovery on the actor); `Journal::is_recording(&self) -> bool`; `Journal::stop(&self)`.

- [ ] **Step 1: Failing tests.** `concurrent_records_and_takes_lose_no_key` (one thread records 100,000 distinct keys while another repeatedly `take`s; the union of batches equals the recorded set); `an_aborted_transaction_records_nothing`; `invalidate_is_reported_by_the_next_take`; `failed_commit_recovery_invalidates_a_recording_journal`.
- [ ] **Step 2: Run** `cargo test --lib -- store::journal failed_commit_recovery_invalidates` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `cargo test --lib -- store::journal failed_commit_recovery_invalidates` — PASS.
- [ ] **Step 5: Commit** `Journal committed keys for compaction`.

### Task C2: Copy, catch up, swap

**Files:**
- Create: `src/store/compact.rs`.
- Modify: `src/store.rs` (`_compact` entry), `src/api/database.rs` (`_compact` and `_active_tasks` progress as CouchDB reports it), start-up removes leftover `*.compact.redb`.

**Interfaces:**
- Consumes: `Journal`, `Taken`, `Gate::read`, `Gate::reopen`.
- Produces: `pub async fn compact_shard(handle: &ShardHandle) -> Result<()>`: starts the journal at the current generation; copies a snapshot (metadata, leaf bodies only, all other tables, seq, uuid) into `shard-NNNN.compact.redb`; catch-up rounds `take` the journal: every non-`Meta`, non-`Bodies` key is re-read from a fresh lease and upserted if present, deleted if absent; every document id appearing in a `Meta` or `Bodies` key is **reconciled**: its metadata is copied (or deleted), its leaf set is computed from that metadata, every destination body of that id whose revision is not a leaf is deleted (including bodies copied earlier), and every missing leaf body is copied. The final round runs on the actor with writes paused, then fsync, `Gate::reopen` performing the rename and directory fsync, journal stopped. If any `take` returns `Taken::Invalidated`, or `is_recording()` is false when checked on the actor immediately before the swap, the compaction deletes its file and restarts from the beginning.

- [ ] **Step 1: Failing tests**, each with the mutation landing during the copy, during catch-up, and during the final swap (barriers, no sleeps): `compaction_keeps_concurrent_updates_and_tombstones`; `a_document_purged_during_compaction_does_not_return`; `local_docs_settings_security_and_checkpoints_changed_during_compaction_survive`; `purge_info_trimming_during_compaction_is_kept`; `bodies_that_became_non_leaf_during_compaction_are_removed_including_already_copied_ones` (update a document whose old body was already copied; the old body's key is never journaled, yet it is absent from the compacted file); `compaction_restarts_after_failed_commit_recovery` (inject a commit failure mid-compaction; final file equals a fresh compaction of the recovered state); `uuid_is_unchanged_so_indexes_do_not_rebuild` (Review Focus 5); `a_continuous_feed_streams_across_the_swap_without_skip_or_repeat` (Review Focus 3); `non_leaf_bodies_are_dropped`.
- [ ] **Step 2: Run** `cargo test --lib -- store::compact` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `cargo test --lib -- store::compact` — PASS.
- [ ] **Step 5: Commit** `Compact shards online`.

### Task C3: Crash safety of compaction

**Files:**
- Modify: `src/store/compact.rs` (test hooks), `scripts/storage_faults.py` (kill during compaction).

- [ ] **Step 1: Failing tests.** `a_crash_before_the_rename_leaves_the_live_file_and_removes_the_leftover`; `a_power_cut_after_the_rename_keeps_the_compacted_file_complete` (PowerCut).
- [ ] **Step 2: Run** `cargo test --lib -- a_crash_before_the_rename a_power_cut_after_the_rename` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** lib, conformance and `storage_faults.py` — PASS.
- [ ] **Step 5: Commit** `Make compaction crash-safe`; open PR "Online compaction", merge when CI and the Jepsen nightly on the branch are green.

---

## Phase D — verification

### Task D1: End-to-end and measurement

- [ ] **Step 1:** Run, each bounded: `differential/run.py` against CouchDB (all modes), the PouchDB client suite, and the three Jepsen soaks locally (`no-fault`, `kill`, `partition`, 5 min each). All PASS.
- [ ] **Step 2:** Measure import and write throughput (bulk import of the 150,959-document airline fixture; `loadgen` write mix) on the last pre-redb commit and on `main`, same machine, same flags. Record both in the final PR description. Not a gate.
- [ ] **Step 3:** Clear every build, data and Docker artifact created by the runs.

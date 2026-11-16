# Replace rusty-leveldb with redb — design

Date: 2026-11-16. Status: approved in conversation; this document records it.

## Intent

douchdb's storage engine, a vendored fork of `rusty-leveldb` 4.0.2, is not a
foundation we can trust. We have patched it seven times (undefined behaviour
in its cache, a Stacked Borrows violation, swallowed table errors, two
wrong-answer read bugs, the latest found in one second by a 20-line model
test), and it never calls `fsync`, so an acknowledged write can be lost on a
power cut or kernel crash. Upstream is slow to merge fixes.

Replace it with `redb` (4.6.0, the current release): a pure-Rust,
copy-on-write, MVCC B-tree with ACID transactions and real `fsync`. That is
the same shape as CouchDB's storage (an append-only copy-on-write B+tree,
one file and one updater per shard, readers walking a committed root while
the updater writes), so douchdb can follow CouchDB more closely and delete
the machinery it built to make LevelDB behave like a transactional store.

## Goals

- Every acknowledged write is durable: each commit is `Durability::Immediate`
  (redb's default; the commit `fsync`s before it returns), as CouchDB syncs
  every commit. Creating or deleting a database is durable too: file
  `fsync` does not persist a new directory entry, so the directories involved
  are `fsync`ed as well (see "Database creation and deletion").
- Use everything redb provides and delete what douchdb built in its place.
  Write as little new code as possible.
- Follow current CouchDB wherever the change touches behaviour, without
  asking (standing project rule).
- Remove `vendor/rusty-leveldb` and everything that exists only for it.

## Non-goals

- Speed. It is measured (see Testing), never a reason to weaken durability.
  douchdb is likely faster than CouchDB anyway; that is not a goal.
- Migrating existing data. No earlier douchdb data or binaries exist
  (standing rule); there is no upgrade path and no compatibility code.
- Re-architecting search (Tantivy) beyond its change cursor, or anything else
  that does not touch storage. (View and search *cursors* are in scope; see
  "Index cursors".)
- Grouping view indexes per design document (CouchDB's `.view` file per
  signature). Out of scope; noted as a follow-up.

## Anti-goals (failure even if it "works")

- Any write acknowledged before it is durable.
- Keeping a LevelDB-era workaround "just in case" (an overlay, a hand-rolled
  rollback, prefix encodings, atomic JSON files) when redb does the job.
- An abstraction layer with a single implementation. redb is used directly.
- Behaviour that diverges from current CouchDB without being named here.

## Approaches considered

1. **Straight swap.** Replace each LevelDB call with a redb call, keep the
   shard actor, overlay, global sequence and watermark. Lowest risk, but
   carries every LevelDB-shaped workaround forward. Rejected.
2. **MVCC inside the shard only.** Group commit becomes one write
   transaction; reads still queue on the actor. Rejected as half-measure.
3. **Take everything redb gives, follow CouchDB (chosen).** One redb file and
   one writer per shard; readers use their own read transactions
   concurrently with the writer; typed tables instead of key prefixes;
   per-shard sequences with opaque `since` tokens; database metadata inside
   redb. Chosen because it deletes the most code and matches CouchDB.

## Mapping (decided)

| CouchDB | douchdb today | With redb | Deleted |
|---|---|---|---|
| Shard = one `.couch` file, one updater | one LevelDB dir per shard, one actor per shard | one redb file per shard (`shard-NNNN.redb`); the actor is the only writer | LevelDB dirs, `LOCK` handling |
| Commit writes a new root + header, synced | grouped jobs staged in `pending` + `overlay`, flushed as one unsynced `WriteBatch` | a group commit is one `WriteTransaction`, committed `Immediate`; later jobs in the group see earlier jobs' writes through the transaction | `Tx`, `pending`, `overlay`, `flush_pending`, `restore_from_disk`, `flush_failure`, `JobMark`/`rollback_to` |
| Readers walk the committed tree while the updater writes | every read is a job on the shard actor | reads open a `ReadTransaction` (a snapshot) on the caller's task, concurrently with the writer | read jobs routed through the actor |
| `by_id`, `by_seq`, `_local`, purge trees | key prefixes `m/`, `s/`, `b/`, `l/`, purge and checkpoint prefixes in one keyspace | typed tables: `meta` (id → DocMeta), `by_seq` (seq → id), `bodies` ((id, rev) → body), `local`, `purge_infos`, `purged_revs`, `purge_uuids`, `checkpoints`, `settings` | prefix builders and parsers, `prefix_successor`, `seek_last`, prefix scans |
| Per-shard `update_seq`; database `since` is an opaque token of every shard's position | one global counter per database allocated across shards, with `committed_seq`/`reserved_seq` watermarks so a cursor never passes an uncommitted write | each shard has its own sequence, assigned inside its write transaction; a shard's feed is exactly its committed snapshot; database `since`/`last_seq`/`update_seq` are opaque tokens holding each shard's position (single node and cluster alike) | the global counter, `allocate_seq`, `committed_seq`, `reserved_seq`, `changes_watermark*`, the cross-shard global-order merge |
| Header/page checksums, recovery on open | `paranoid_checks`, "tables but no `z/seq`" corruption probe, fault script aimed at LevelDB files | redb's XXH3 Merkle checksums and automatic repair on open; `Builder::set_check_integrity_on_open(true)` | the corruption probes, `had_tables` heuristics |
| Doc counts via B-tree reductions | counters updated by hand beside each write, recount on open | counters in `settings`, updated in the same write transaction so they cannot drift (`meta.len()` is O(1) and may replace one of them) | the recount-on-open path |
| Security object, shard map kept inside databases | `db.json`, `shard-map.json`, `pending-shard-map.json`, `security-versioned.json` with atomic-write and temp-file cleanup | rows in the `settings` table of shard 0's file (written in a transaction) | the JSON metadata files, `write_json_atomic`, stale-temp cleanup |
| `_compact` rewrites the file, dropping non-leaf bodies | stale-body sweep, then LevelDB `compact_range` | online copy-and-swap, as CouchDB (see "Compaction") | `compact_range`, the in-place sweep |
| View file per design doc | one LevelDB per view | one redb file per view (unchanged granularity) | LevelDB usage in `view.rs` |
| Internal replication (`mem3_sync`) per shard copy | anti-entropy checkpoints per peer over a numeric local seq | checkpoints per (peer, shard) over that shard's sequence | per-peer global-seq checkpoint logic |

## Kept, because redb does not provide it

- `LatestChanges` (pending counts) and the live-key rank (`_all_docs`
  offsets): CouchDB gets these from count reductions stored in B-tree nodes;
  redb has no order statistics, so replacing them would mean a scan per
  request. They are rebuilt from the committed tables and updated after each
  commit.
- The shard actor as the single writer and the group commit itself (redb
  amortises nothing across separate transactions; grouping is what makes
  one `fsync` serve many writes).

## Behaviour decisions

- **A panicking job fails its whole group commit.** redb savepoints cannot be
  taken after a table is opened in the transaction, so per-job rollback is not
  available; panics are bugs. The transaction is dropped (aborted) and every
  job in the group gets an error. Commit errors likewise fail every job in the
  group (unchanged).
- **`_changes` order follows CouchDB:** rows are not in one global order;
  each shard's rows are in that shard's order. `since` is opaque everywhere.
- **File locking:** redb's default `ConcurrencyMode::ExclusiveWriter` locks
  the file; a second process opening a shard fails, replacing LevelDB's
  `LOCK`.
- **Integrity:** see "Corruption detection".

## Calls made after the builder review

Each follows the standing rules (follow current CouchDB; durability and
correctness before speed). Marked here so they can be checked.

1. **Cluster `since` tokens follow CouchDB 3.** An entry is keyed by shard
   copy (node, shard range) and carries that copy's sequence plus a copy
   identity (a uuid created with the shard file, and its epochs), so a
   position is never applied to a copy that was cleared, re-seeded or
   recreated. When a feed must read a range from a different copy (node down,
   placement change), it translates the position through the internal
   replication checkpoints, as `mem3_rep:find_source_seq` does, and otherwise
   restarts that range from 0. Replay is allowed; skipping never is. This
   replaces `FallbackMark`/`UNKNOWN_SEQ` and the per-node vector.
2. **Counts and offsets come from the same snapshot as the rows**, as
   CouchDB's B-tree reductions do. Reads that need `LatestChanges` or the
   live-key rank (`_changes` `pending`, `_all_docs` `offset`/`total_rows`,
   partition counts) stay on the shard actor, where the in-memory indexes and
   the committed state agree. Every other read (document, `_bulk_get`,
   `_revs_diff`, `open_revs`, quorum reads, view and anti-entropy fetches) runs
   on its own read transaction off the actor.
3. **Corruption detection:** see the section of that name.
4. **A job validates before it writes.** It reads and decodes every record it
   will change before its first insert into the shared write transaction, as
   `commit_at` does today. Ordinary per-document failures (conflicts,
   validation) are results, not errors, and write nothing. A job error after
   its first write is treated like a panic: the group's transaction is
   aborted and every job in it fails.
5. **Page-cache memory is bounded.** redb's cache is per file, so each file
   gets an explicit cache size set at open: a new `--cache-mb` (per shard
   file, default 16) and a fixed small size for view files (4 MiB). The total
   is bounded by `--max-open-dbs` × q × the per-file size; the throughput
   comparison runs with the defaults.

## Corruption detection

redb 4.6 checksums every page at commit (an XXH3 Merkle tree) but, as far as
its source shows, verifies them only in its integrity walk, not on ordinary
reads; a plain open of a damaged file may even panic. CouchDB checks each
term's MD5 as it reads it. So:

- **Every stored value carries an application checksum** (a 32-bit CRC32C,
  appended on write, verified on every read), in every table: document
  metadata, bodies, sequence entries, `_local` docs, purge records, settings,
  view rows. A mismatch is a corruption error (500, fail closed), never a
  miss. This is one encode/decode pair used by every table.
- **`set_check_integrity_on_open(true)`** on every shard and view file, so a
  file damaged while closed fails to open (`StorageError::Corrupted`) rather
  than being trusted or panicking. Its cost on reopen is accepted.
- **Guarantee, stated narrowly:** a damaged *value* is detected when read. A
  damaged *tree page* (a branch page, which can misroute a lookup into a
  false "not found") is detected at the next open, not at read time. This is
  weaker than CouchDB, which checksums tree nodes too, and is documented as
  such in `docs/consistency.md`.
- Tests corrupt a file **while it is open** (bytes of a value page and of a
  branch page) as well as while closed, and assert the outcomes above.

## Database creation and deletion

- **Create:** make the database directory and every shard file; write shard
  0's `settings` (shard map, security, q, n) and commit it `Immediate` last;
  then `fsync` the database directory (new shard files) and its parent (the
  new directory); only then acknowledge. The committed `settings` row in
  shard 0 is the marker that the database exists.
- **Recovery on start:** a database directory whose shard 0 has no committed
  `settings` row, or that is missing a shard file, was partially created. It
  is removed, and the parent directory is `fsync`ed. It never appears in
  `_all_dbs`.
- **Delete:** rename the directory to a tombstone name, `fsync` the parent,
  acknowledge, then remove the tombstone in the background. A tombstone left
  by a crash is removed on start.
- View and search index files are rebuildable from the shards, so losing a
  newly created one costs a rebuild, not data; they need no directory sync.

## Failed commits

redb fails every later write on a handle after an I/O error during commit
(`StorageError::PreviousIo`); its allocator state is then marked for rebuild
on the next open. Policy:

1. The group's jobs all fail (unchanged).
2. The shard actor drops its `Database` handle. Readers holding a clone finish
   or fail on their own; no new reads are handed the old handle.
3. The actor reopens the file (redb runs recovery), rebuilds its in-memory
   indexes (`LatestChanges`, live-key rank) and settings from the committed
   tables, and resumes.
4. If reopening fails, the shard fails closed: every request to it returns
   500 until the database is next opened. It is never left half-usable in the
   cache.

## Compaction

Online, as CouchDB's compactor does; the shard keeps serving throughout.

1. Open a read transaction (a snapshot) at the shard's current sequence S, and
   copy the live data into a new file `shard-NNNN.compact.redb`: every
   document's metadata and only its leaf bodies (CouchDB drops non-leaf
   bodies), `_local` docs, purge records, checkpoints, settings and the
   shard's sequence and uuid.
2. Catch up: copy what changed after S from the live file's `by_seq` table
   into the new file, repeatedly, until the remaining gap is small.
3. Swap, briefly on the shard actor: copy the last changes, `fsync` the new
   file, rename it over the live file, `fsync` the directory, reopen, and
   carry on. Readers holding the old handle finish on their snapshot.
4. A crash at any point leaves the live file untouched; a leftover
   `.compact.redb` is deleted on start, as CouchDB discards a half-built
   `.compact` file.

The shard file's uuid is preserved, since the data and sequences are the same,
so `since` tokens and index cursors stay valid across compaction.
`Database::compact()` is not used. Tests: compact under concurrent writes and
reads, with a crash at each step; nothing acknowledged is lost, and feeds
neither skip nor repeat beyond what CouchDB allows.

## Index cursors (views and search)

Both index kinds today persist one numeric cursor and read the database's
changes as numeric `(since, until]` ranges (`search.rs`, `view.rs`). With
per-shard sequences a single number would skip changes from slower shards, so:

- Each view index and each search index persists **one cursor per shard**:
  that shard's sequence plus the shard file's identity (uuid). Views store it
  in the index's own redb file, in the same transaction as the rows; search
  stores it in the Tantivy commit payload, as today.
- An incremental update reads each shard's changes after its cursor, up to
  that shard's committed sequence at the start of the update, and advances
  each shard's cursor to exactly what it applied.
- A stored uuid that no longer matches the shard file (cleared, re-seeded,
  recreated) invalidates the index, which rebuilds from 0.

## Testing

- **Model test (the one that found the LevelDB bug):** random puts, deletes,
  group commits, concurrent snapshot reads, reopen and compaction against a
  shard, compared with an in-memory model. Runs in CI.
- **Power cut (the failure that motivated this):** a test `StorageBackend`
  (redb lets the caller supply the layer behind a file) that keeps writes in
  memory until `sync_data` and can "lose power", discarding everything not
  yet synced. Random workloads, power cut at random points, reopen: every
  acknowledged write is present. A build that skipped `fsync` fails it.
  SIGKILL alone cannot catch this, because the kernel's page cache survives.
- **Process crash:** `scripts/storage_faults.py`, rewritten for redb's single
  file, kills nodes mid-write and mid-compaction.
- **Acceptance is split.** Crashes and power cuts: every acknowledged write
  must be recovered, no exceptions. Deliberate damage (truncation, bit flips,
  removed files): the affected shard must fail closed; recovering data is not
  required.
- **Creation and deletion:** crash at each step of create and delete; on
  restart no partial database is listed, and every acknowledged create is.
- **Per-shard sequences:** conformance tests for opaque `since` round-trips,
  `_changes` with overlapping writes (the 150,959-document live-cursor
  scenario must miss nothing), `limit`/`descending`/filters, and cluster
  `since` tokens.
- **Existing suites:** lib, conformance, differential against CouchDB, PouchDB
  clients, and the three Jepsen soaks (no-fault, kill, partition), all green.
- **Measurement, not a gate:** import and write throughput compared with the
  current build, reported with the PR.

## Removals checklist

`vendor/rusty-leveldb`, the `[patch.crates-io]` entry, the vendored-tests CI
step, the Dockerfile vendor `COPY`, `docs/storage-dependency.md`,
`docs/upstream/rusty-leveldb-issues.md`, the LevelDB-specific parts of
`scripts/storage_faults.py`, README/PLAN/consistency docs mentions, and every
code path listed in the "Deleted" column above.

## Left to the builder

- Exact table key/value types (`&[u8]`, `&str`, `u64`, tuples) and whether a
  body table is keyed by `(id, rev)` tuple or a joined key.
- How the opaque `since` token is encoded, as long as it is opaque and
  carries every shard's position.
- How concurrent readers obtain the shard's `Database` (shared `Arc`) and how
  `_compact` takes exclusive access.
- Order of work, provided each step keeps the suites green.

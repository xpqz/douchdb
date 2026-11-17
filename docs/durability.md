# Durability

DouchDB stores each shard in one redb file and acknowledges a write only after
it is durable on disk. This is the same contract as CouchDB 3.x, which syncs
every commit.

## What is guaranteed

- **Every acknowledged write is durable.** Each write transaction (a group
  commit on the shard actor, a view index update, a database metadata change)
  commits in two phases through one helper, `kv::begin_write`. The pages are
  synced first, then the header that makes them the newest commit is written
  and synced, and only then is the write acknowledged. Power lost between the
  two syncs leaves the previous commit in force and the write unacknowledged.
  A group commit pays one pair of syncs for all the writes in the group.
- **Process crash, `SIGKILL`, kernel crash and power loss** lose no
  acknowledged write. `kill9_preserves_acknowledged_documents_and_changes` and
  `scripts/storage_faults.py` cover the process crash; the `kv::powercut` tests
  run a storage backend that discards everything not yet synced and check every
  acknowledged write is present after reopening.
- **Creating or deleting a database is durable.** A database is built in a
  staging directory, synced, published with one rename, and the parent
  directory is synced before the create is acknowledged. A delete renames the
  directory to a tombstone and syncs the parent before it is acknowledged.
  Recovery on start removes only staging and tombstone directories.
- **A failed commit does not leave a half-usable shard.** If a commit fails,
  every write in the group gets an error and the shard reopens (running redb's
  recovery); if that fails the shard answers 500 until its database is next
  opened.
- **Damage is detected, not served.** Every stored value carries a CRC32C that
  is checked on every read; a mismatch is a 500, never a missing document. A
  full page-checksum check runs on the first open of each file in a process and
  when an idle database closes. A file that fails it refuses to open and the
  database answers 500 while the rest of the server keeps running. A view
  index that fails to open or verify is deleted and rebuilt from the shards.
- Replication copies acknowledged revisions to other nodes. It is repair and
  availability, not what makes a single write durable.

## What is not guaranteed

- **Tree-page damage is caught late.** A damaged value page is detected when
  read. A damaged branch page, which could misroute a lookup into a false "not
  found", is detected by the next integrity check: when the idle database
  closes or on the first open after a restart, not at read time. Damage done to
  a closed file while the server keeps running goes unnoticed until a read hits
  a damaged value or the process restarts. CouchDB checksums tree nodes as it
  reads them, so this is weaker than CouchDB.
- **Two header bits are unchecksummed.** The primary-slot bit and the two-phase
  bit of redb's header sit outside every checksum. Flipping the primary-slot
  bit of a file closed by a crash makes redb serve the older commit, which
  verifies, silently. This is the only single-event path known to roll a commit
  back without detection.
- **Correlated host failure.** If every replica shares one disk, power or
  failure domain, replication cannot save a lost device.
- **Backups.** There is no built-in backup, point-in-time recovery or
  write-ahead archive.
- **Search indexes** (Tantivy) are derived from the shards and written without
  fsync; after a crash they rebuild from the shards.
- **`_compact` is an interim sweep.** It deletes bodies of non-leaf revisions
  in batches, but the file does not shrink: redb reuses the freed pages. Online
  copy-and-swap compaction is planned.

## Operational guidance

- A device that lies about flushing (a volatile write cache without power-loss
  protection) breaks the guarantee below douchdb; nothing at this level can
  detect it.
- Keep independent backups of irreplaceable data.
- `scripts/storage_faults.py` checks crash recovery and fail-closed behaviour
  on a release build; run it after changing anything under `src/kv.rs` or
  `src/store.rs`.

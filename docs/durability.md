# Durability

DouchDB is **not a power-loss-durable primary store** in this version. This is a
deliberate, documented limitation of the current storage layer, not a hidden
failure mode.

## What is guaranteed

- A write acknowledged by the HTTP API has been written to the LevelDB write
  log and flushed to the operating system.
- If the **process** crashes or is killed with `SIGKILL`, the OS page cache and
  the LevelDB log remain. Restarting the process recovers acknowledged writes.
  The test `kill9_preserves_acknowledged_documents_and_changes` covers this.
- Replication copies acknowledged revisions to other nodes. It is repair, not a
  substitute for a durable commit when hosts fail together.

## What is not guaranteed

- **Power loss, kernel crash, VM reset or storage-device failure.** The storage
  dependency does not call `fsync`; an acknowledged write may still be lost if
  the operating system or device loses its volatile cache.
- **Correlated host failure.** If all replicas share the same power/network
  failure domain, replication cannot make the write durable.
- **`_ensure_full_commit` as an fsync promise.** It flushes the shard log to the
  OS; it does not add a device-level sync barrier.
- **Backups.** There is no built-in backup, point-in-time recovery or
  write-ahead archive.

## Operational guidance

- Do not use DouchDB as the only copy of irreplaceable data.
- A stronger block device or filesystem beneath the page cache does **not**
  repair the missing application commit barrier. The application must submit a
  sync before it can promise power-loss durability.
- Keep independent backups or replicate to a separately durable system with an
  explicit acknowledgement/RPO contract. Do not describe that as local
  durability.
- The `scripts/storage_spike.py` check covers `kill -9`, not power loss. Do not
  read it as a power-loss guarantee.

Making DouchDB power-loss durable would require an fsync-capable write path or a
WAL with a durable commit barrier, plus a benchmark that measures the cost. That
is a future storage project, not a configuration switch.

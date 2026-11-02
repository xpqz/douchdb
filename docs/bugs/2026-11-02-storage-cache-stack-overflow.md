# Storage-cache stack overflow during large imports (2026-11-02)

## Symptom

A Linux ARM64 release container aborted with a Tokio worker stack overflow
while importing aviation datasets and reading changes feeds. GDB stopped on a
`SIGSEGV` with repeated `core::ptr::drop_glue::<rusty_leveldb::cache::LRUNode>`
frames, i.e. recursive destruction of the storage cache's owning LRU chain.
The truncated capture is in
[`evidence/storage-cache-gdb-2026-11-02.txt`](evidence/storage-cache-gdb-2026-11-02.txt).

## Root cause and fix

`rusty-leveldb` 4.0.2 owns each LRU node's successor through
`Option<Box<LRUNode<T>>>`. Dropping a long list therefore recurses once per
node. The same cache also linked a duplicate key without unlinking the old
node; that leaves the list and map at different lengths and can corrupt later
evictions.

The dependency is now vendored under `vendor/rusty-leveldb` with:

- an iterative `LRUList` drop (bounded so a corrupt list cannot spin forever);
- duplicate-key replacement that removes the old node first;
- a debug list/map invariant check;
- cache tests for duplicate replacement, random LRU invariants and dropping
  100,000 nodes on a 64 KiB stack.

See [`vendor/rusty-leveldb/PATCHES.md`](../../vendor/rusty-leveldb/PATCHES.md).

## Limits

The exact live-volume corruption was not reproduced here, and the original
capture was truncated before the caller, so the fix should not be read as proof
that every possible cache-corruption source is gone. It removes the recursive
destruction visible in the trace and the identified list/map divergence path.
Release validation covered a 100,000-document write, complete changes-feed
pagination, process restart and a Docker restart with 20,000 documents. The
ARM64 image was rebuilt from the fix; a 5,000-document write, complete changes
pagination and container restart passed under QEMU.

Keep the vendored cache tests in CI, and re-run a large ARM64 import plus
concurrent changes/repair before treating the storage layer as fully signed
off.

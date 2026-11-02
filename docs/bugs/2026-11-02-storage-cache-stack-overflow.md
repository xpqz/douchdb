# Storage-cache stack overflow during large imports (2026-11-02)

## Symptom

A Linux ARM64 release container aborted with a Tokio worker stack overflow
while importing aviation datasets and reading changes feeds. GDB stopped on a
`SIGSEGV` with repeated `core::ptr::drop_glue::<rusty_leveldb::cache::LRUNode>`
frames, i.e. recursive destruction of the storage cache's owning LRU chain.
The truncated capture is in
[`evidence/storage-cache-gdb-2026-11-02.txt`](evidence/storage-cache-gdb-2026-11-02.txt).

## Root cause and fix

The immediate symptom is recursive node destruction, but the underlying defect
is undefined behaviour in `rusty-leveldb` 4.0.2's `LRUList`:

- it takes raw pointers into `Box<LRUNode>` values and then moves those boxes
  into the list while handles and `prev` pointers still refer to them;
- every node keeps raw pointers to the inline sentinel `head`, which become
  invalid when the list is moved;
- `remove_last` can therefore detach a node that still owns a long successor
  chain, causing recursive destruction during block-cache eviction.

The list was replaced with the circular, heap-allocated sentinel design from
upstream PR #69: every node, including the sentinel, is owned through
`Box::into_raw`/`Box::from_raw`; no `Box` is moved while a handle points at it;
link/unlink operations are O(1); teardown is iterative. The vendored copy also
fixes duplicate-key insertion, which linked a new node without unlinking the old
one, and adds debug list/map invariant checks.

See [`vendor/rusty-leveldb/PATCHES.md`](../../vendor/rusty-leveldb/PATCHES.md).

## Limits

An earlier 0.2.4 attempt only made whole-list teardown iterative. It did not
cover `remove_last`, which detaches a node and then drops any successors it
still owns, and it did not remove the undefined behaviour. That version failed
the existing-volume ARM64 validation. Version 0.2.5 replaces the list
implementation with the UB-free design above.

The exact live-volume corruption has not been reproduced in this environment,
and the original capture was truncated before the caller. The corrected cache
tests pass in debug and release on x86_64 and on `linux/arm64` Rust 1.97; the
ARM64 release suite previously aborted on the unsafe list. Release validation
also covered a 120,000-document write, complete changes-feed pagination,
process restart, a 20,000-document amd64 Docker restart and a 5,000-document
arm64 Docker restart.

Re-run a large ARM64 import plus concurrent changes/repair against the exact
existing volumes before treating the storage layer as fully signed off.

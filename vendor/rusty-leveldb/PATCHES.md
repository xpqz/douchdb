# Local rusty-leveldb patches

Base: `rusty-leveldb` 4.0.2 (MIT), matching the checksum in `Cargo.lock`.

This fork exists because the upstream LRU cache has two availability defects
that are exposed by large bulk imports and changes-feed reads:

1. **Recursive cache teardown.** `LRUList` owns its successor chain through
   `Option<Box<LRUNode<T>>>`. Dropping a long chain recursively consumes one
   stack frame per node and can overflow a Tokio worker stack. The nested
   `Option<Box<LRUNode>>` drop frames appear in the captured Linux ARM64
   backtrace. `LRUList` now has an iterative `Drop`; it also stops after the
   recorded count and leaks the remainder if the list is already corrupt,
   rather than spinning forever.
2. **Duplicate-key insertion.** `Cache::insert` used to link a new node without
   removing an existing node for the same key. The map then pointed at the new
   node while the old node stayed in the list, so list length and map length
   diverged and later evictions could corrupt the LRU. Insert now removes the
   old entry and node first.

`src/cache.rs` also has regression tests for duplicate-key replacement, list/
map/chain invariants under random LRU operations, and dropping a 100,000-node
cache on a 64 KiB stack.

Upstream master still had the recursive design when this fork was created
(2026-11-02). If a fixed upstream release appears, prefer upgrading and remove
this patch. Re-run the cache tests and the import workload before doing so.

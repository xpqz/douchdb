# Local rusty-leveldb patches

Base: `rusty-leveldb` 4.0.2 (MIT), matching the checksum in `Cargo.lock`.

This fork exists because the upstream LRU cache has two availability defects
that are exposed by large bulk imports and changes-feed reads:

1. **Undefined behaviour in the LRU list.** Upstream stores each node's
   successor in `Option<Box<LRUNode<T>>>`, takes raw pointers into those boxes,
   and moves the boxes while handles and `prev` pointers still point into them.
   It also keeps raw pointers to the inline sentinel `head`, so moving the list
   invalidates them. This is undefined behaviour; on Linux ARM64 release builds
   it corrupts the list and produces the captured recursive destruction in
   `remove_last`. The list is replaced with the circular, heap-allocated
   sentinel design from upstream PR #69: every node (including the sentinel) is
   owned through `Box::into_raw`/`Box::from_raw`, no `Box` is moved while a
   handle points at it, and `insert`, `remove`, `remove_last` and
   `reinsert_front` are O(1) unlink/link operations. Teardown is iterative.
2. **Duplicate-key insertion.** `Cache::insert` used to link a new node without
   removing an existing node for the same key. The map then pointed at the new
   node while the old node stayed linked, so list length and map length
   diverged and later evictions could corrupt the LRU. Insert now removes the
   old entry and node first.
3. **Silent corruption on reads.** `DB::get_internal` and `Version::get`
   discarded table errors (`if let Ok(Some(...))`), so a bad checksum became a
   false "not found". They now return the error; DouchDB's document reads use
   `get_at` for the same reason. `Footer::decode` returns `None` instead of
   asserting on a damaged table footer. Bloom filters are disabled because
   their blocks are not checksummed; a corrupt filter could otherwise hide a
   present key. The filter reader also validates offsets defensively.

`src/cache.rs` also has regression tests for duplicate-key replacement, list/
map/forward-chain/back-link invariants under random LRU operations, and dropping
a 100,000-node cache on a 64 KiB stack. The cache tests pass in debug and
release on x86_64 and on `linux/arm64` Rust 1.97.

`scripts/storage_faults.py` exercises clean SIGKILL, kill-during-compaction,
truncated tables, bit flips, removed tables and truncated manifests. Every
scenario must preserve all 200 acknowledged documents or fail closed; it runs
in CI.

This is a **temporary** vendoring decision. The rationale, risks, revisit
triggers and removal checklist are in
[`docs/storage-dependency.md`](../../docs/storage-dependency.md), tracked by
issue #105.

Upstream master still had the unsafe design when this fork was created
(2026-11-02); PR #69 is open, not merged. If a fixed upstream release appears,
prefer upgrading and remove this patch. Re-run the cache tests and the import
workload before doing so.

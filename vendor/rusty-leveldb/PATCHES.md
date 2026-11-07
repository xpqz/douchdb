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

4. **Neighbouring tombstone read as a deletion.** `Version::get` looks a key up
   in each table that covers its range and receives the next entry at or after
   it. Upstream treated any `TypeDeletion` entry as proof that the looked-up
   key was deleted without comparing user keys, so a table that covered the key
   but did not contain it, followed by a tombstone for a different key, made
   the key read as missing even though an older table held it. The deletion
   branch now requires the found user key to equal the looked-up one, as the
   value branch already did. Regression test:
   `db_impl::tests::test_db_impl_get_ignores_neighbouring_tombstone`.

## Upstream status of each patch

Each patch has its own gate: it can be dropped only when a published
`rusty-leveldb` release contains an equivalent fix. Ready-to-file issue texts
are in [`docs/upstream/rusty-leveldb-issues.md`](../../docs/upstream/rusty-leveldb-issues.md).
Nothing below has been filed upstream yet (filing needs approval).

| # | Patch | Files | Upstreamed? |
|---|-------|-------|-------------|
| 1 | Circular heap-sentinel LRU list | `cache.rs` | PR #69 open, unmerged, not released |
| 2 | Duplicate-key `Cache::insert` | `cache.rs` | No; issue text drafted |
| 3a | `get_internal` / `Version::get` propagate table errors | `db_impl.rs`, `version.rs` | No; issue text drafted |
| 3b | `Footer::decode` returns `None` on a damaged footer | `table_builder.rs` | No; issue text drafted |
| 3c | Filter-block offset bounds checks | `filter_block.rs` | No; issue text drafted |
| 3d | `filter_policy.name() == "_"` skips the filter block (bloom disabled) | `table_builder.rs` | No; issue text drafted |
| 4 | Deletion branch compares user keys | `version.rs` | No; issue text drafted |
| 5 | Skipmap nodes owned via raw pointers (Miri/Stacked Borrows) | `skipmap.rs` | No; issue text drafted |

The fork can be removed only when **every** row is upstreamed and released, or
the dependency is replaced; see `docs/storage-dependency.md`.

5. **Skipmap aliasing (Stacked Borrows violation).** Miri
   (`cargo miri test skipmap`, nightly-2026-11-07, default Stacked Borrows)
   rejected `InnerSkipMap::insert`: raw `*mut Node` skip pointers were taken
   from a `Box`, which was then moved into its predecessor's `next` field,
   invalidating them. The redundant owning `next: Option<Box<Node>>` chain is
   removed; every node (and the head) is now owned through
   `Box::into_raw`/`Box::from_raw` and linked only through `skips`, with
   iterative teardown. Miri passes for the skipmap tests under both Stacked
   and Tree Borrows. Safety comments were added to `cache.rs` and
   `skipmap.rs`. Upstreamed? No; issue text drafted.

`src/cache.rs` also has regression tests for duplicate-key replacement, list/
map/forward-chain/back-link invariants under random LRU operations, and dropping
a 100,000-node cache on a 64 KiB stack. Re-run Miri with the Docker recipe below after touching `cache.rs` or
`skipmap.rs` (needs nightly; not run in CI). The 100,000-node drop test and the
random-operation test are too slow under Miri; skip them with
`-- --skip dropping_a_long --skip duplicate_and_lru_operations`:

    docker run --rm --init --name miri -v "$PWD/vendor/rusty-leveldb:/src" -w /src \
      rustlang/rust:nightly sh -c 'rustup component add miri && cargo miri test --lib skipmap'

The cache tests pass in debug and
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

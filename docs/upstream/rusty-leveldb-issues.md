# Upstream issue texts for `rusty-leveldb`

Ready-to-file texts for each local patch in
[`vendor/rusty-leveldb/PATCHES.md`](../../vendor/rusty-leveldb/PATCHES.md).
Target repository: <https://github.com/dermesser/leveldb-rs>, base version
4.0.2. **None of these has been filed yet**; filing needs the maintainer's
approval. Record the issue/PR URL and date on the matching "Upstreamed?" line
in `PATCHES.md` once filed.

## Patch 2: duplicate-key `Cache::insert` corrupts the LRU

`Cache::insert` links a new node without removing an existing node for the same
key. The map then points at the new node while the old one stays in the list,
so `list.count()` and `map.len()` diverge and a later eviction removes a node
the map no longer tracks. Reproduce: insert the same key twice into a cache,
compare `count()` with the map length, then insert past capacity.
Fix: remove the existing entry (map and node) before linking the new one.

## Patch 3a: `DB::get_internal` and `Version::get` swallow table errors

Both use `if let Ok(Some(..))` on the table lookup, so a checksum or I/O error
becomes a false "key not found", and an older value (or absence) is returned as
if authoritative. Fix: propagate the `Err`.

## Patch 3b: `Footer::decode` asserts on a damaged footer

A truncated or corrupted table footer panics instead of returning an error.
Fix: return `None`/an error and let the caller report corruption.

## Patch 3c: filter-block reader lacks bounds checks

`FilterBlockReader` trusts offsets read from the block; a corrupt filter block
can index out of range. Fix: validate offsets before slicing and treat an
invalid block as an error (or "may match").

## Patch 3d: no-filter policy convention in `TableBuilder`

`table_builder.rs` skips writing a filter block when
`filter_policy.name() == "_"` (the no-filter policy). DouchDB relies on this to
disable bloom filters, whose blocks are not checksummed. Propose documenting and
testing the convention, or adding a first-class `Options` switch.

## Patch 4: `Version::get` treats a neighbouring tombstone as a deletion

`Version::get` receives the next entry at or after the key from each table that
covers its range. The `TypeDeletion` branch does not compare user keys, so a
tombstone for a different key makes the looked-up key read as deleted even when
an older table holds it. The value branch already compares keys. Fix: require
the found user key to equal the looked-up key in the deletion branch too.
Regression test: `db_impl::tests::test_db_impl_get_ignores_neighbouring_tombstone`.

## Patch 1: LRU list undefined behaviour

Already reported upstream as PR #69 (open, unmerged at the time of writing).
Nothing new to file; comment with the aarch64 release-build evidence if useful.

## Patch 5: `SkipMap::insert` violates Stacked Borrows

`cargo miri test skipmap` fails in `InnerSkipMap::insert`: the raw `*mut Node`
skip pointers are taken from the new node's `Box`, which is then moved into the
predecessor's `next` field, invalidating them (`trying to retag from <tag> for
Unique permission ... but that tag does not exist in the borrow stack`). Fix:
own nodes through `Box::into_raw` and link them only through `skips`, dropping
the `next: Option<Box<Node>>` chain.

## Soundness notes from the #71 audit

`unsafe impl Send for DB` is only sound while no `Rc`-holding value
(`DBIterator`, `Snapshot`) leaves the owning thread; see `Shard` in
`src/store.rs`. Worth documenting upstream.

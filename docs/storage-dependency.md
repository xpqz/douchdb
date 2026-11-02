# Storage dependency: temporary vendored `rusty-leveldb` patch

Status: **temporary**, revisit before any 1.0 or production sign-off.
Tracking issue: **#105**.
Decision date: 2 November 2026.

## Decision

DouchDB currently vendors `rusty-leveldb` 4.0.2 under
`vendor/rusty-leveldb` and applies two changes:

1. the circular, heap-allocated-sentinel LRU list from **upstream PR #69**
   (open and unmerged at the time of writing);
2. a local duplicate-key eviction fix in `Cache::insert`.

The patch is wired through `[patch.crates-io]` in `Cargo.toml`, copied into the
Docker build, and its cache tests run in CI. See
[`vendor/rusty-leveldb/PATCHES.md`](../vendor/rusty-leveldb/PATCHES.md).

## Why this exists

The published 4.0.2 LRU list has undefined behaviour: it takes raw pointers
into `Box` nodes and then moves those boxes, and it keeps pointers to an inline
sentinel whose address changes when the list moves. On Linux ARM64 release
builds this corrupts the cache list and causes recursive node destruction and
worker-stack overflow during large imports and changes-feed reads. The
development profile hid the failure; a normal release image did not.

Upstream issue/PR #69 documents the same aarch64 release heap corruption and
Miri failure, and provides the heap-sentinel list we currently use. The fix is
not merged into a published `rusty-leveldb` release.

## Why not another crate now

Alternatives were surveyed and are not low-risk drop-ins:

- `roughdb` is LevelDB-format-compatible and featureful, but very young and
  explicitly experimental; not suitable for this workload without a deep audit.
- `anchored-leveldb` is alpha and read-oriented.
- `rusty-leveldb-arc` is the same unsafe list under a different fork.
- Google LevelDB bindings preserve the format but require a C++ toolchain and
  the current Rust wrappers are stale or explicitly unmaintained.
- `fjall`/`redb` are attractive safe Rust engines, but changing to them needs a
  storage-backend trait and an on-disk data migration.

Vendoring the minimal cache fix was the smallest change that made the current
release build safe enough to continue operating.

## Risks accepted

- The vendored list comes from an **unmerged** upstream PR, not a released
  crate.
- The fork is maintained locally and must be re-checked when upstream changes.
- The duplicate-key fix and cache-invariant tests are our own code.
- A vendored dependency adds review and supply-chain burden.

Mitigations: the vendored source is pinned in-tree, the only edits are
documented in `PATCHES.md`, cache tests run in CI, and the incident evidence is
retained under `docs/bugs/`.

## When to revisit

Revisit immediately when any of these happens:

- upstream PR #69 is merged **and** a new `rusty-leveldb` release contains it;
- another cache corruption or stack overflow is observed;
- the storage backend is migrated to a safe engine (`fjall`, `redb`, or a
  maintained LevelDB binding);
- a release/1.0 or production durability/availability review is scheduled;
- the dependency-review interval (at most six months) expires.

## Removal checklist

1. Switch `Cargo.toml` back to the registry version containing the fix.
2. Delete the `[patch.crates-io]` entry and `vendor/rusty-leveldb`.
3. Remove the vendor `COPY` from the Dockerfile.
4. Remove the vendored-cache CI step.
5. Update `Cargo.lock`, README/PLAN and this document.
6. Run `cargo fmt`, Clippy, both test configurations, and the large ARM64
   import/changes/repair workload against a copy of the existing volumes.

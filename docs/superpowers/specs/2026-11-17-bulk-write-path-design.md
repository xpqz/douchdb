# Bulk write path: as fast as redb allows

Date: 2026-11-17. Status: approved 2026-11-17; implemented (see docs/performance.md, 'Bulk write path against the redb floor').

## Intent

A bulk upload (`_bulk_docs`, and the paths built on it: `POST /{db}` batches,
replication with `new_edits=false`, internal replication and read repair) must
cost what redb itself costs to commit those documents, and nothing more.
Durability is not traded: every commit stays `Durability::Immediate` and
two-phase. Semantics stay CouchDB's: each document succeeds or fails on its
own; a crash never leaves part of a shard's batch on disk.

## Why

Measured on main 7e352b6 (single node, airline fixture, release build; report
`/Volumes/Space2/douchdb-scratch/bulk-spike-report.md`): `_bulk_docs` runs at
0.3–0.5x a bare redb program writing the same bytes with the same syncs.
douchdb writes the same bytes and makes the same number of syncs (two per
commit) as that floor, so the storage layout is not the cost. The gap is:

1. About 30 µs per document of CPU work (document split, revision hash, body
   serialisation) done on the shard actor while it holds the write
   transaction, in series with the commit. With 4 clients the actor spends
   22.9 ms staging and 25.4 ms committing per 1,000 documents.
2. `Db::put_docs` awaits each shard's call in turn, so with q=2 a batch makes
   four syncs back to back instead of two pairs in parallel.
3. JSON is parsed (8.4 µs/doc), then each document rebuilt by `split_doc`
   (3 µs/doc) and its body serialised again (1.4 µs/doc).
4. The revision hash builds an Erlang term tree and encodes it before MD5
   (14% of the actor's time).
5. Each insert opens its table again, and each document's metadata is read
   separately.

## Goals

- On the shard: the actor's time per batch, syncs excluded, is within 15% of
  the floor's insert time at 250, 1,000 and 10,000 documents per batch, q=1
  and q=2. This is the gate. Syncs are excluded because their cost drifts
  (5–60 ms per `F_FULLFSYNC` in the spike); sync and byte counts must equal
  the floor's.
- End to end: q=2 request wall time is about one shard commit, not the sum.
  End-to-end docs/s against the floor is reported, not gated: request parsing
  (8.4 µs/doc) is HTTP cost the floor does not pay. (Controller's call.)
- 4 concurrent clients: the shard actor's time is dominated by inserts and the
  commit, not staging.
- Same results, byte for byte: revision ids, response bodies, error
  precedence and status codes unchanged.

## Non-goals

- One redb file per database (rejected; see Approaches).
- Fewer syncs per commit: two-phase commit stays.
- Single-document PUT latency (#90 covers commit coalescing for it).
- Replacing the JSON request parser. Its duplicate-key and number-spelling
  rules feed the revision hash; a specialised parser is a separate,
  measured follow-up issue if parsing dominates after this work.
  (Controller's call.)
- Changing what nodes send each other (for example prepared records instead
  of documents): a protocol change, filed as a follow-up idea.
  (Controller's call.)
- Cluster-wide all-or-nothing, or CouchDB's removed `all_or_nothing`.

## Anti-goals

- Any change to durability, acknowledgement point or crash behaviour.
- Any change in a revision id for the same input.
- A second write path for bulk alongside the single-document one: there is
  one path, made lean.

## Constraints

- Follow current CouchDB behaviour, including the order in which errors win
  (a conflict wins over a malformed design document; validation errors per
  document).
- Shard actor semantics from the redb storage spec stay: one write
  transaction per group, journal recording after commit, recovery before
  reply.
- No `#[allow]`, clippy `-D warnings`; tests wait on conditions; as little
  code as possible.

## Approaches

- **A. Lean bulk path, one file per shard (chosen by the user).** Move all
  per-document work that does not need stored state off the shard actor,
  commit every shard a request touches concurrently, and remove the
  redundant parse, rebuild, serialise and table-open work. Expected: close to
  the floor; +65–90% single client at q=2, about +45% with 4 clients. No file
  format change.
- **B. A plus one redb file per database per node.** The only way to have
  literally one transaction for q>1. Rejected: redb has one writer per file,
  so writes to different shards would queue instead of running in parallel;
  compaction, recovery, shard moves and failure isolation would need
  rebuilding; it departs from CouchDB's one file per shard. With A's
  concurrent commits, B mostly saves syncs that already overlap.
- **One-phase commit** (one sync per commit): rejected; it reintroduces
  silent rollback of damaged recent commits.

## Design (decided)

1. **Prepare off the actor.** Before a shard call, on the request's task (or
   the blocking pool for large batches), turn each document into a prepared
   record: id, client `_rev` (parsed, canonical), deleted flag, `_revisions`,
   body bytes serialised once, and the revision id computed for the parent
   the client named (or none). The actor keeps only what needs stored state:
   the metadata read, the conflict check, the tree merge, sequence
   assignment, and the inserts.
2. **Revision hash recomputed only when the parent differs.** CouchDB derives
   a new document's parent from the stored tree when the client gives no
   `_rev` and the winner is a deletion. In that case (and any other where the
   actor picks a parent other than the client's) the actor recomputes the
   hash; otherwise it uses the prepared one. Revision ids are identical to
   today's in every case.
3. **Concurrent shard commits.** `Db::put_docs` issues all per-shard calls at
   once and joins them. Error handling keeps today's meaning: a failed shard
   fails the request; the design-document validator cache is still dropped
   after every shard has answered.
4. **Parse once.** `split_doc` works from the parsed object without
   rebuilding it where it can, and the body bytes from step 1 are what gets
   stored.
5. **Streaming revision hash.** Encode the term straight into the MD5 hasher
   instead of building the term tree and a byte vector. Same bytes hashed.
6. **One table open per job; metadata read for the batch.** A job opens each
   table once and reads every document's metadata in one pass before its
   writes (which also matches the purge rule "read before write").
7. **Cluster write path** (`src/fabric.rs`). Bulk uploads into a cluster
   go through `fabric::commit_docs` first. `replicate_to_all` (the
   `new_edits=false` path) starts the local write and the remote fan-out
   together instead of awaiting the local write first, and the coordinator
   stops deep-cloning each document for its own write
   (`spawn_quorum_writes`, `replicate_to_all`). Peer-call counts are
   unchanged. (Controller's call.)
8. **Optional, measured:** sort a large batch by id before insert (helped
   +22–45% at 10,000 documents in the spike, noise at 250 and 1,000).

## Verification

- A bench (script under `scripts/`) that runs the floor program and
  `_bulk_docs` side by side at 250/1,000/10,000, q=1 and q=2, 1 and 4
  clients, and prints the ratio; results recorded in docs/performance.md.
- Golden tests: revision ids and full responses for a fixed batch (new docs,
  updates, deletions, conflicts, deleted-winner recreation, design docs with
  compile errors, `new_edits=false`) are unchanged from main.
- Existing lib, conformance, differential (vs CouchDB), PouchDB and Jepsen
  stay green.

## Left to the builder

- Exact shape of the prepared record and where preparation runs (task vs
  blocking pool, threshold).
- Whether the floor program lives in `examples/` or a bench target.
- Whether step 8 is worth keeping after measurement.
- Sorting, if kept, reorders inserts only, never sequence assignment.

# Live changes cursor gaps under overlapping writes (2026-11-02)

## Symptom

A live `_changes` reader that forwarded each numeric `last_seq` while a large
import was still running reached the final sequence without receiving every
document id. Replaying the feed from zero after writes finished returned all
ids, and all stored documents were present. On the 150,959-record probe the
three local readers missed 4,712, 6,005 and 4,327 ids respectively; a
10,000-document run did not expose the gap reliably.

## Cause

Sequence numbers are global to the database but are allocated inside individual
shard actors while a write is being staged. Different shard actors run
concurrently. A shard can allocate a sequence and be preempted before its rows
become visible, while another shard publishes a higher sequence. A reader that
advances its single numeric cursor to the higher sequence will never ask for
the lower sequence again, so that document is skipped.

A second sequence-bookkeeping bug was found while fixing this:
`commit_ops` recorded the **last** `s/<seq>` operation in a transaction, not the
highest. An update/delete emits the new sequence key and then deletes the old
one, so the shard's stored sequence (and the restart baseline) could move
backwards. This is fixed to take the maximum.

## Fix

- Track two atomics per shard: last committed sequence (updated by
  `commit_at`) and highest allocated sequence (updated when staging allocates).
- A changes request computes a watermark across selected shards. Only shards
  with an allocated-but-uncommitted sequence constrain it; for those the safe
  bound is `max(committed, since)`. A shard with nothing in flight cannot later
  publish a sequence below `since`.
- Rows above the watermark are withheld, not emitted. The cursor is the last
  emitted row, so the next request re-reads the withheld range instead of
  skipping it. This keeps rows ordered and avoids duplicates in normal
  operation.
- The watermark is applied to the non-streaming path, the per-shard page fetch
  in the streaming path, and the per-node `last_seq` used by cluster changes.
- Watermarks are recomputed after collecting rows/pages so an allocation that
  starts during the read also constrains the cursor.

## Validation

- Unit regression simulates an allocated-but-uncommitted shard, verifies high
  rows are withheld, and verifies a resumed cursor sees them.
- The same test updates an existing document, covering the `commit_ops`
  highest-sequence fix.
- Concurrent 150,000-document release probe: three local readers and one
  public cluster-feed reader accumulated all 150,000 ids, zero missing; each
  local cursor reached 150,000 and the cluster vector reached
  `v1:150000,0,0`.
- Full suites: 68 unit + 125 integration; 69 + 126 with `wasmtime`.
- Clippy clean; release build passes.

## Limits

The exact 150,959-document ARM64 workload should be repeated against the
release image and the public cluster-feed path, not only local forwarded
feeds. The watermark intentionally trades duplicate re-reads for completeness
while a shard has a write in flight.

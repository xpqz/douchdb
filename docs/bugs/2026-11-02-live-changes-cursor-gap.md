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

## Follow-up (2026-11-04): the watermark was taken too late

A synthetic probe still lost documents with the fix above: a 3-node `q=4`,
`n=3` release cluster, four concurrent writers and six cursor-driven readers
(a local feed on each node, plus public normal, longpoll and continuous feeds)
missed hundreds to thousands of IDs at only 20,000 documents.

Two ordering faults remained:

- **The watermark was computed after the shards were read.** Shard A is read
  while it has sequence 10 allocated but not committed; shard B is read with 11
  committed; A then commits 10; the watermark, computed now, no longer sees A
  in flight, so 11 is emitted and the cursor passes 10. The same applied to
  the per-page check in the streaming path.
- **Allocation took the global sequence before marking the shard in flight.**
  A snapshot between the two steps saw a new sequence on a shard that did not
  yet look busy.

Fix: `changes_watermark` is a snapshot taken **before** any shard read. It
loads the global counter first and caps the bound at it, so every sequence up
to the bound is committed and visible to the read that follows, and anything
allocated later is higher. `allocate_seq` raises the shard's reserved sequence
above the counter before moving the counter. The streaming path takes one
snapshot per stream and only stops a shard once its rows pass the bound.

Validation: the release probe at 20,000 (three runs) and 150,000 documents (two
runs) received every ID on all six readers. The new conformance test
`live_changes_cursor_sees_every_document_under_concurrent_writes` fails on the
previous code in about two of three runs (it is a timing race) and passed in
every run with the fix.
The probe is
[`evidence/live-changes-gap-probe-2026-11-04.py.txt`](evidence/live-changes-gap-probe-2026-11-04.py.txt):
`python3 -I <probe> target/release/douchdb <scratch-dir> [docs]`.

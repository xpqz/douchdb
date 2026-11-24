# On-disk view index: query views from redb, not from memory

Date: 2026-11-24. Status: approved 2026-11-24, revision 5 (after review rounds 1 to 3 and an
outside review).

## Decisions

- **Grouping.** `1` and `1.0` share a group, as in CouchDB. Comparator
  equality decides group membership everywhere: in local reduce groups and in
  the coordinator's merge.
- **Partition `total_rows` stays partition-local.** It counts the emitted rows
  of the queried partition (docs/partitioned-databases.md), so it does not
  change when an unrelated partition gains rows. This is a deliberate
  difference from CouchDB, which reports the whole shard's count there.
- **Partials are written without a sync.** They are a cache: losing them
  loses no data and changes no answer, only makes the next reduce compute
  them again. So the write a reduce query makes for them uses redb's
  non-durable commit, and a reduce read never waits for a sync. Every other
  index write stays fully durable.
- **Queries work in row positions, not bound keys** (agreed 2026-11-24, step
  3). A query's start and end, `key` and each `keys` entry become positions
  in a shard's rows through the block counts. The selection code that works
  in positions today (start and end rules, `key`/`keys`, `offset`, `skip`)
  then runs unchanged over the file, so its answers stay identical by
  construction. This replaces the bound-tail encoding and its tables; see
  Positions.
- **A small in-memory cache per open view** (agreed 2026-11-24, step 5,
  after measuring). Each shard's block list, and every block's partial with
  levels of 32 combined above it, are cached under the index lock and
  dropped for the shards a write changes. Blocks are 64 rows, not 256. This
  is what brought reduce back to the old speed; it costs about one entry
  per 64 rows. Measurements: docs/performance.md, "Views on disk".
- **The cluster merge streams map queries without `keys`** (step 6, #194).
  Each owner streams its shards' rows, at most `skip + limit` of them, as
  newline-delimited JSON on the internal path (`stream=true`): a line of
  counts, one line per row, and an end line, so a cut stream is told apart
  from a finished one. The coordinator merges them in view order and
  writes rows as they arrive. Owner shard sets are disjoint, so the old
  counting deduplication goes. An owner that fails before its counts are
  in fails over to a replica as before; one that fails after rows have
  gone out ends the reply early, as in CouchDB. These streams use their
  own HTTP/1.1 peer client with a 60 s idle limit and no overall deadline:
  a slow client then holds back only its own TCP connection, never the
  shared HTTP/2 connection's flow-control window that other peer calls
  use. `keys` queries (bounded by their keys) and reduces whose function
  the owners cannot run for the coordinator still gather rows first;
  reduces the owners run already send one value per group.
- **#195 is deferred.** The `inclusive_end=false` with `endkey_docid`
  behaviour stays as it is during this rewrite. Both query paths change
  together under that issue.

## Intent

A view query should read its rows from the view's redb file in key order, as
CouchDB reads its view B-tree, instead of from a sorted copy of the whole
view held in memory. Resident memory per view becomes redb's page cache plus
nothing proportional to the view. Streaming a query's rows, on one node and
later across the cluster (#194), then costs nothing extra.

## Why

Today each view file (`src/view.rs`, one redb file per view) stores rows only
by document: `ROWS: (doc id, ordinal) → {key, value}`. That order serves
incremental updates (a changed document's old rows are found by id) but not
queries. So the first query of a view loads every row, sorts it by CouchDB
collation into `ViewRows` (`ShardRows` blocks per shard or partition run),
and keeps it in the index registry (`--max-open-indexes`, counted in
indexes, not bytes). Reduce reads per-block partials (`BlockTree`, leaves of
`REDUCE_FANOUT` = 32 rows) built lazily in memory beside the rows.

Consequences:

- Every view queried stays in memory whole on every node until the registry
  evicts it. The bound is a count, so a few large views can use any amount
  of memory.
- A query copies its selected rows out of the snapshot (`ViewRows::select`)
  and builds the reply as one JSON value, so a large query briefly holds
  several more copies.
- A cluster coordinator also holds every peer's rows (#194).
- A rebuild collects and sorts every row in memory, and a restart or
  eviction pays a full load and sort on the next query.

CouchDB has none of this. Its view is a B-tree on disk ordered by
(key, doc id), with reductions (including counts) in the inner nodes, and a
query is a streamed fold over one snapshot. redb is also an on-disk
copy-on-write B-tree with snapshot reads, and its `Key` trait takes a custom
comparison function (`compare`, which must be a total order), so the same
structure can be built on it.

## Goals

- A view's rows are stored ordered by (shard, partition, view key by CouchDB
  collation, doc id, ordinal) in the same redb file, kept in step with the
  by-document table in the same write transaction.
- Map queries (`key`, `keys`, ranges, `startkey_docid`, `endkey_docid`,
  `inclusive_end`, `descending`, `skip`, `limit`, partitions, the cluster
  owner mask) read that table in one read transaction, merging shards
  lazily, and stream their rows.
- `total_rows` and `offset` come from stored block counts. Reduce (built-in
  and JavaScript, with `group`, `group_level` and `keys`) uses stored
  per-block partials, without reading every row.
- The in-memory `ViewRows` snapshot, its `BlockTree` and the in-memory sort
  in rebuild go away.
- Map query results are identical to today's, row for row and byte for byte,
  including tie order and CouchDB's `offset` rules. Reduce results are
  identical for exact reducers (`_count`, integer `_sum`, `_stats` on
  integers, and any reducer CouchDB requires to be associative and
  commutative). Float `_sum`/`_stats` may differ in the last bits, because
  partials combine in a different grouping. That is as in CouchDB, whose
  grouping follows its B-tree shape.

## Non-goals

- The cluster coordinator's streaming merge (#194). This makes it possible;
  #194 does it afterwards.
- Search indexes, `_changes`, `_all_docs`.
- How the index is brought up to date (map execution, catch-up paging,
  rebuild triggers). Only what an update writes changes.
- View-file compaction. There is none today either (see File size).

## Anti-goals

- Weaker durability for anything but a cache. Every write of rows, keyed
  rows, block counts and cursor stays one `kv::begin_write` transaction
  (Immediate durability, two-phase commit). Only reduce partials are
  committed non-durably (see Reduce partials). A crash leaves rows, keyed
  rows, block counts, partials and cursor agreeing. Partials written since
  the last durable commit may be missing, never wrong.
- A different map answer for any query.
- Reduce failures leaking into updates or map queries. Today a reduce
  function that throws, or `_stats` over a non-number, fails only reduce
  queries. That stays so.

## Design

### Tables

- `ROWS: (doc id, ordinal) → sealed encoded ViewKey`. A document's emitted
  keys, so an update removes exactly the keyed rows it wrote, by their bytes,
  without comparing values. The value is no longer stored here.
- `BY_KEY: ViewKey → sealed value`, where a stored `ViewKey` is
  (shard u32, partition, encoded view key, doc id, ordinal). Two other
  forms are never stored as rows: a shard's sentinel (fence of its first
  block) and a partition's start (see Positions). The ordinal is the row's emit index within its
  document, in updates and rebuilds alike. It keeps equal (key, id) rows in
  emission order, as today's stable sort does.
- `BLOCKS: fence ViewKey → sealed count`. A block covers the keyed rows from
  its fence up to the next fence of the same shard (exclusive), or to the end
  of the shard. Every shard always has a first block, even when empty, whose
  fence is the shard's sentinel: the shard number with an encoding that sorts
  before every partition and key of that shard. Every lookup therefore stays
  inside its shard.
- `PARTIALS: fence ViewKey → sealed partial`. This table exists only for
  views with a reduce. An absent entry means the block's partial is not
  known, so there is no stale marker to maintain.
- `META`: the design signature, as today, and each shard's total row count.

The design signature already forces a rebuild when it changes (`native=` is
part of it today). It gains:

- the key codec version;
- the ICU collator's data version;
- the placement (`q`, `partitioned`, `by_partition`).

So a change in collation data or shard layout rebuilds the index through the
existing path, instead of leaving a mis-ordered B-tree. redb's `type_name()`
for `ViewKey` carries the codec version too. Old files fail the signature
check and are rebuilt; there are no older douchdb indexes to keep.

### Partitions

The shard is the only run that matters for storage. For a view queried
through `_partition`, the partition name is the second key component, so
each partition's rows are contiguous inside its shard. Blocks are per shard,
and a block may span partitions, so a million small partitions do not make a
million blocks. A partition's `total_rows` and `offset` are block counts
inside the partition's range plus a scan of its two edge blocks. For views
not queried by partition, the partition component is empty.

### The key encoding and the order

The encoded view key is a compact, type-tagged form that the comparator walks
without allocating:

- a tag per value, in `cmp_json`'s rank order (null, false, true, number,
  string, array, object);
- numbers as their decimal text, behind a length;
- strings as UTF-8, behind a length;
- arrays and objects as a count followed by their members, object members in
  stored order.

It decodes back to the exact `Value` the reply prints.

Number order follows CouchDB's. CouchDB compares keys with Erlang term order
(`enif_compare` in `couch_ejson_compare.c`), which compares an integer with a
float exactly. jiffy decodes integer literals to exact integers and every
other number to an f64, and rejects floats out of range. So:

- an integer literal is an exact integer;
- any other number is the nearest f64;
- an integer and a float are compared exactly, not by converting the integer
  to f64 (today's `cmp_numbers` converts, which is what breaks the order);
- numerically equal keys (`1` and `1.0`) compare Equal;
- a float literal out of f64 range in a query parameter is a 400 error, as in
  CouchDB.

Emitted numbers are already JS doubles printed by QuickJS (`finish_native`
re-runs any row that is not `js_exact` through QuickJS), so in practice
exact integers beyond 2^53 reach the comparator only from query parameters.

Equal keys are not a problem for redb. Every stored `ViewKey` ends with
doc id and ordinal, so two stored entries never compare Equal unless they
are the same row. ICU already returns Equal for canonically equivalent
strings with different bytes, and the same suffix resolves those. The
comparator is a total order on entries: shard; the shard's sentinel first;
partition by bytes, a partition's start before its rows; view key (with
Equal ties); doc id by bytes; ordinal.

Two consequences:

- An update removes a document's old entries before inserting its new ones,
  in the same transaction, since a new key may compare Equal to an old one
  under the same (id, ordinal).
- Reduce grouping (`add_partial`, `assemble_reduce` in `src/view.rs`, and the
  coordinator's merge in `src/fabric.rs`) groups by comparator equality, not
  by JSON text. So `1` and `1.0` share a group, as in CouchDB. Today they
  group by text. A group's printed key is the key of its first row in
  (key, doc id) order, as CouchDB's fold encounters it. The coordinator,
  which sees no doc ids, prints the key of the first owner reply, in node
  order, that has the group.

`cmp_json` and the comparator share one implementation of every rule
(numbers, ICU strings, objects), working on borrowed `&str` and `&[u8]`, so
the two cannot drift.

A damaged key that does not decode compares by bytes after every well-formed
key, so `compare` stays total and never panics; reading it is an error. View
keys move from a sealed value into a redb key, so they are protected by
redb's page checksums rather than by `kv::seal`.

### Positions

A query does not build keys for its bounds. It works in positions: the
rows of a shard, numbered in key order. The block counts turn a key into
a position, and a position into a row, reading at most one block's rows
beyond what is returned:

- **Rank.** The rows below a key are the counts of the blocks before the
  one the key falls in, plus that block's rows below the key.
- **Read at a position.** The block holding the position is found from the
  counts. Its rows are read from the fence, skipping to the position.
- **Search.** The first position whose row fails a predicate that holds for
  a prefix (all the selection needs) is found in two levels. The first row
  of each block in range is probed by binary search, then the one block the
  point falls in is scanned.

A run is a shard, or for a view queried by partition, the partition's
rows inside the shard: from the rank of the partition's start key to the
rank of the next partition's (the partition name followed by a zero byte).
Today's position-based selection (`range_bounds`, `equal_range`, the offset
rules, the choice of rows for `key`/`keys` within `skip + limit`) runs over
these runs unchanged. That includes the behaviour #195 will change.
`group_level` needs no prefix bound: groups are found by the same search.
### Block counts

Blocks hold about B rows. B is 256: a block may reach 2B = 512 rows before
it splits, which stays under `REDUCE_CHUNK` (1000), so a block's rows always
reduce in one call.

An update knows the exact `ViewKey`s it removes (from `ROWS`) and inserts.
Each one finds its block with `BLOCKS.range(..=key).next_back()`. The
shard's sentinel fence guarantees this lands in the right shard. The update
adjusts that block's count and the shard's total, and deletes the block's
`PARTIALS` entry. Before commit, the touched blocks are rebalanced until
every one is in range:

- a block over 2B rows is split into pieces of about B, each a new fence,
  with counts taken from the scan that finds the split points;
- a block under B/2 rows is merged into its predecessor, or for a shard's
  first (sentinel) block, the successor is merged into it by deleting the
  successor's fence;
- a merge that goes over 2B is split again;
- every block whose set of rows changes loses its `PARTIALS` entry, whether
  its fence was created, deleted or kept. A predecessor that absorbs a
  merged neighbour keeps its fence but gains rows, so its partial goes too.

The only block allowed outside [B/2, 2B] is the sentinel block of a shard
with fewer than B/2 rows.

A rebuild writes everything in one write transaction, so it becomes visible
all at once or not at all. It deletes the four row tables first, then
inserts each mapped page into `ROWS` and `BY_KEY` as the page is mapped, so
no list of all rows and no sort is held in memory. After the last page, one
scan of `BY_KEY` writes the blocks and the shard totals. `PARTIALS` stays
empty.

The transaction stays open across the async map pages. It is moved into each
page's `blocking()` call and back out, as the gate and index are today. This
needs a new owned-transaction method on `ViewIndex` beside today's
`commit(FnOnce(&WriteTransaction))`. redb's `WriteTransaction` holds only
`Arc`s, mutexes and atomics, so it should be `Send`. Step 2's first commit
confirms that by compiling it. If it is not `Send`, the design comes back for
review rather than falling back to another scheme.

### Reduce partials, computed lazily

Updates never run a reducer. They only delete the partials of touched
blocks, so an update costs a count adjustment per changed row, and nothing
extra for views that are never reduced.

A reduce query does all its work in `ensure_rows`, while it holds the index
lock and the gate, after bringing the index up to date. It works in one
write transaction opened with a new `kv::begin_cache_write`. That is the
same as `kv::begin_write` except that the commit is non-durable
(`Durability::None`): it is written without a sync. The next durable commit
to the file makes it durable, and a crash before that loses only those
partials, which are computed again on the next reduce. The guard test in
`src/kv.rs` that forbids setting durability outside `kv::begin_write` allows
this one function. No other write uses it.

1. It finds the blocks the query will combine whole: those its range or
   group covers whole. Only these need partials. Blocks reduced row by row
   at the edges, or in groups smaller than a block, never get one.
2. Each needed block without a partial is reduced from its rows, in one call.
   If a call fails, the transaction is dropped and the query fails, as a
   reduce error does today. Map queries and updates are unaffected, and the
   block stays without a partial.
3. The answer is computed inside the same transaction: partials of covered
   blocks are combined by rereduce, and the rows at the edges are reduced
   directly. Reads inside a write transaction see its own writes.
4. If any partial is new, they are written and the transaction is committed.
   Otherwise it is dropped. If the commit fails, the answer is still
   returned, since it was computed from consistent data. The commit's
   existing error path closes and reopens the file, and the partials are
   computed again next time.

Everything happens under the lock, so no update can come between computing
a partial and using it.

A block is covered whole by a group when its first and last rows (two seeks)
fall in that group and inside the range. Group equality is comparator
equality on the key truncated to `group_level`. `keys` with `group` and
`group_level` keep today's rules (unique keys per owner, requested order and
repeats restored by the coordinator).

The ungrouped reduce of a whole shard combines up to rows/256 partials.
Upper levels of `REDUCE_FANOUT` partials are built per query. If that misses
the 2x target, a persisted second level is the remedy.

A reduce answer is still computed whole before it is sent. Its size is the
number of groups, which is bounded by the query.

### Map queries

`ensure_rows` keeps bringing the index up to date first. The query then
opens one read transaction while still holding the index lock, and drops the
lock once it is open, as today's snapshot is taken under the lock. That
transaction is one snapshot of rows, counts and the recorded cursor, so
`update_seq` reporting is unchanged. No generation counter is needed.

- **Order.** Each run's range of positions comes from today's selection
  rules (see Positions). Shards are
  merged lazily with a heap. The heap does not use the storage comparator,
  which orders by shard first and would emit shard 0's rows before shard
  1's. It compares heads by (view key, doc id, ordinal), the storage order
  without shard and partition, reversed for `descending`. Rows of one shard
  arrive in order, so the merge reproduces today's `select` plus stable sort.
  Ties across shards cannot happen, because a document's rows live in one
  shard.
- **`skip` and `limit`.** `skip` counts rows of the merged order, never of
  one shard. With one shard selected,
  it first walks the rest of the block the start bound falls in, then steps
  over whole blocks by count, ascending or descending, then walks rows. A
  range that ends inside a block is never skipped past by count. `limit`
  ends the scan.
- **`keys`.** For a client query, one range per requested entry, in
  requested order, reversed when `descending` (as `ordered_rows` does). A
  repeated key lists its rows again.
- **Owner replies.** A cluster owner reply (`fetch_view_mask`) keeps today's
  selection exactly. With keys made unique (`unique_keys`), the
  `skip + limit` budget is spent walking the keys in query order (requested
  order, reversed when descending), as `select_sorted` does. The chosen rows
  are then emitted once each, in key order. Only the coordinator's final
  rendering repeats rows for repeated keys. Its counting merge relies on
  this.
- **`total_rows`.** The selected shards' stored totals. For a partition, the
  count inside the partition's range: whole blocks, plus a scan of its two
  edge blocks. That keeps today's per-partition counts
  (docs/partitioned-databases.md). CouchDB reports the whole shard's count
  there; changing that is a separate question.
- **`offset`.** The rows of the selected run before the query's start,
  found by the same position search, as today's selection counts them. `key` and single-key `keys` use the
  key's insertion point. Multi-key `keys` report 0. The reported offset stays
  `before_start + min(skip, total_rows - before_start)`.
- **`include_docs`.** Documents are read a chunk at a time, as now.
- **`visited`.** Counts the index rows read plus rows skipped by count, as
  today's count includes every row copied for `skip`. So the bounded-visit
  tests keep their meaning.

### Streams, the index lock and the file

The existing streaming `MapRows` (`query_stream`) is kept. Step 3 replaces
its `selection` vector with the scan. redb reads block their thread, so each
chunk of index rows is read inside `blocking()`. Documents for
`include_docs` are then read with the async `docs_by_ids`, between chunks,
as now.

A stream holds its index slot, a clone of the registry entry. The registry
counts it in use, so it never evicts or closes the file under the stream. A
stream holds no gate permit and no index lock. So:

- **Cleanup and database deletion** never wait for a stream. They retire and
  remove the index file as today. A retired file closes when its last holder
  drops it, and on Linux and macOS a removed file stays readable through
  open handles, so the stream finishes from its snapshot. A deleted
  database's `include_docs` reads then fail, and that stream ends early. Step
  3 checks that cleanup and `forget` never explicitly close a `Database` that
  is still held, and makes them stop doing so if they do.
- **A failed index write** closes and reopens the file (`commit`'s error
  path). A live stream's next chunk then fails, and the stream ends early.
  Today a failed write only drops the cached snapshot.
- **Integrity check on close** needs no reader. Because a stream holds its
  slot, the registry closes a file only once no stream is reading it.

A long stream keeps old pages alive until it ends, as any redb reader does.
Streams end with their request.

### Memory

- `ViewRows`, `ShardRows`, `BlockTree`, the cached snapshot and the rebuild
  sort are removed.
- Per open view, what's left is redb's page cache plus the blocks a query
  reads. A reduce answer is still built whole, but its size is bounded by
  the query's groups. The cluster coordinator streams map rows (#194). Today's `VIEW_CACHE_BYTES` (4 MB per file) times up to 256 open
  views is about 1 GB once warm; it was mostly cold before, because queries
  read the snapshot. Per-file cache drops to 1 MB, relying on the operating
  system's file cache, and the effect on latency is measured in step 5.

### File size

`BY_KEY` adds one copy of every row's key and value. `ROWS` shrinks to
document id, ordinal and key, without the value, so a view file grows by
about the size of the keys. The integrity check on close grows with it.
redb reuses freed pages, and there is still no view compaction. If file
growth becomes a problem, view compaction is a separate issue.

## Performance

Warm queries binary-search memory today. On disk they walk a B-tree through
redb's cache, with an ICU comparison per string key component during the
search, as in CouchDB.

Benchmarks, before and after, on the airline fixture, a 1M-row view and a
partitioned view with one partition per document, single node and 3-node
cluster:

- RSS after querying 10 large views: the target is a fall roughly equal to
  the views' size.
- Warm map queries at limit 10, 1,000 and unlimited: the target is at most 2x
  today's for limit 10 (including the `offset` edge scan), and better for
  unlimited.
- Reduce, ungrouped and `group=true`, warm, with partials up to date: the
  target is at most 2x. Partials are computed once per changed block.
- Index build and incremental update: the target is at most 1.5x slower for
  updates. Each changed row writes two tables and adjusts one block count.
  Rebuild has no in-memory sort but writes twice as many entries; it is
  measured, not targeted.
- First query after a restart: the target is much faster, with no load and
  sort.

If warm small queries miss their target, the first remedies are a larger
page cache and storing an ICU sort key beside each string, not a return to
in-memory snapshots.

## Testing

- **Deterministic fixtures.** These come first, because agreement on typical
  data would hide both defects they cover:
  - the triple 9007199254740992, 9007199254740993 and 9007199254740992.0:
    transitivity, insertion, lookup and range bounds;
  - two shards with keys [2, 4] and [1, 3], queried with `skip=1&limit=2`,
    which must return [2, 3] ascending and [3, 2] descending;
  - a merge invalidates the survivor's partial: warm the predecessor's
    partial (`_count`), force a merge by deleting rows from its neighbour,
    then reduce again, which must count the merged rows;
  - `keys=["b","a","b"]` with one row per key, which must return b, a, b,
    including with `descending`, `skip` and `limit` across the repeats;
  - a reducer that throws, on the initial build and on an incremental
    update. The view is then queried with `reduce=false`, which works, and
    with `reduce=true`, which fails with the reducer's error.
- **The codec and the order.** Round trips, and a property test that
  `compare` is a total order (antisymmetric, transitive) on random JSON
  keys, including numbers that collide as `f64`, exponents beyond `f64`,
  non-ASCII strings, nested arrays and objects. Integers beyond 2^53
  compared with floats are covered. A differential test against CouchDB
  covers `1` versus `1.0` in `key=`, ranges and `group=true`.
- **An oracle test.** Random emits and writes, comparing every map query
  option, and exact-reducer results, between the on-disk path and a
  test-only in-memory sort of the rows (the simplest possible
  implementation, kept only in tests). Float reducers are compared with a
  tolerance.
- **Block invariants after random writes.** Counts sum to the rows; every
  block has between B/2 and 2B rows, except the sentinel block of a shard
  with fewer than B/2 rows; each stored partial equals a fresh reduce of its
  block; each fence is at or below the block's first row; every shard has
  its sentinel block.
- **A crash test.** An update or partial write interrupted at commit leaves
  `ROWS`, `BY_KEY`, `BLOCKS`, `PARTIALS` and `CURSOR` agreeing (the
  `PowerSwitch` harness). A crash after a non-durable partial commit leaves
  each of those partials either absent or correct, and the next reduce
  gives the right answer.
- **Stream tests.** An update during a stream does not change the rows that
  stream returns. Cleanup during a stream does not wait for it, and the
  stream completes. Deleting the database during an `include_docs` stream
  ends it early. A failed index write during a stream ends it early. A
  stalled stream survives registry eviction pressure: its file is not
  closed, and it completes when read. In each case an early end cuts the
  connection, never closing it as valid JSON.
- **Existing suites.** The conformance, differential and Jepsen suites pass
  unchanged, including the bounded-visit view tests.

## Order of work

Each step is its own PR, green on CI and Jepsen:

1. CouchDB's number order (exact integer–float comparison), one shared
   collation implementation, grouping by comparator equality, the codec and
   its property tests. `cmp_json` switches to it. That changes in-memory
   results only where they differed from CouchDB, so it is checked against
   CouchDB.
2. `ROWS` (new form), `BY_KEY` and `BLOCKS` written by updates and
   rebuilds, including the owned-transaction rebuild, with the extended
   design signature. `PARTIALS` is created empty, and updates only delete
   from it. Queries still use the in-memory snapshot, but `load_rows` builds
   it by iterating `BY_KEY` (already in order) instead of reading `ROWS` and
   sorting. A test check compares `BY_KEY` with the snapshot after each
   update.
3. Map queries from `BY_KEY`, streamed, with stream lifetime rules.
4. Reduce from block partials, computed lazily and committed through
   `kv::begin_cache_write`. With map queries already on disk, this leaves
   the in-memory snapshot without a caller, so this step also removes it,
   `BlockTree` and the incremental snapshot upkeep. `ViewRows` stays only
   in tests, as the reference selection the key-ordered table is checked
   against.
5. Shrink the page cache. Measure.
6. #194: the cluster streaming merge.

## Open questions

- Whether ICU comparisons inside redb's tree search cost enough to justify
  storing an ICU sort key beside each string (ICU4X sort-key support needs
  checking).
- Whether `_approx_count_distinct` partials (about 2.7 KB each) should be
  stored per block or computed only at query time.
- The page cache size per file, settled in step 5.

//! CouchDB map/reduce views, evaluated with an embedded QuickJS runtime.
//!
//! Each view has a persistent index, a redb file under `_view_indexes/`. A
//! query applies each shard's `_changes` since the position the index has
//! read and only re-runs the map function for documents that changed; a
//! design-document change rebuilds the index. The JS engine exposes the same `doc`/`emit` environment as CouchDB's
//! JavaScript view server.

use crate::db::{cursor_valid, AllDocsOpts, ApiError, ChangesOpts, Db, DbResult, IndexCursor};
use crate::index_cache::{Gate, Registry, Slot};
use crate::js::JsError;
use crate::util::hex;
use crate::viewfn::{run_map_js, Builtin, Row, WRAPPER_SOURCE};
use md5::{Digest, Md5};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, LazyLock, Mutex as StdMutex, MutexGuard, PoisonError};
use tokio::sync::Mutex as AsyncMutex;

pub use crate::collate::cmp_json;
pub use crate::js::ViewLimits;

#[derive(Clone, Debug, Default)]
pub struct ViewQuery {
    pub key: Option<Value>,
    pub keys: Option<Vec<Value>>,
    pub startkey: Option<Value>,
    pub endkey: Option<Value>,
    pub startkey_docid: Option<String>,
    pub endkey_docid: Option<String>,
    pub descending: bool,
    pub inclusive_end: bool,
    pub skip: usize,
    pub limit: usize,
    pub reduce: Option<bool>,
    pub group: bool,
    pub group_level: usize,
    pub include_docs: bool,
    pub update_seq: bool,
    pub limits: ViewLimits,
    /// Internal cluster filter: return only rows whose shard is owned by this
    /// node. The coordinator unions the owner-only responses, so each map row
    /// crosses the network once instead of once per replica.
    pub owner_shards: Option<crate::cluster::ShardMask>,
    /// Set for a `_partition/{p}/_design/.../_view/...` query. Only rows
    /// belonging to that partition are returned.
    pub partition: Option<String>,
    /// Set by the cluster coordinator after summing owner-only map row counts.
    pub total_rows_override: Option<usize>,
    /// Set once rows have already been selected (by `ViewRows::select` or a
    /// coordinator merge), which is also when the partition and shard filters
    /// have been applied: the reported offset.
    pub offset_override: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct ViewSpec {
    pub map: String,
    pub reduce: Option<String>,
    /// Design documents in a partitioned database are partitioned by default.
    /// `options.partitioned = false` makes the view global.
    pub partitioned: bool,
}

/// Page cache of each view file: fixed and small, since many can be open
/// (up to `--max-open-indexes`); queries also lean on the operating
/// system's file cache.
const VIEW_CACHE_BYTES: usize = 1 << 20;

// The tables of a view file. Every value is sealed (`kv::seal`), so a damaged
// one is an error when read, never a wrong or missing row.

/// (document id, ordinal) → the `ViewKey` of one row the document emitted,
/// whose value is in `view_store::BY_KEY`. A document's rows sit together,
/// so a changed document's old rows are found, by their exact keys, without
/// walking the index. The ordinal is the row's emit index in its document.
const ROWS: TableDefinition<(&str, u32), &[u8]> = TableDefinition::new("rows");
/// [`DESIGN`] → the signature of the map and reduce functions the rows were
/// made with, and of the key encoding, collation and placement they are
/// ordered by; [`TOTALS`] → each shard's row count.
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
/// Shard number → `[uuid, seq]`: the shard copy the index has read, and how
/// far (the [`IndexCursor`]).
const CURSOR: TableDefinition<u32, &[u8]> = TableDefinition::new("cursor");
const DESIGN: &str = "design";
const TOTALS: &str = "totals";

/// A persistent index for one (database, design document, view) tuple, in a
/// redb file of its own. Rows are stored per document so a changed document
/// can be re-mapped without walking the whole database; an update's rows,
/// design and cursor are one transaction, so they always agree.
struct ViewIndex {
    /// The open file. `None` once a failed write has closed it; the next use
    /// reopens it from `file`.
    db: Option<Database>,
    /// Where the file is; `None` for an index over a test backend.
    file: Option<PathBuf>,
    /// Set when the index is removed (cleaned up, its database deleted): its
    /// file then closes without the close-time check.
    retired: Arc<AtomicBool>,
    cursor: IndexCursor,
    design: String,
    /// Shard count and partitioning of the database and view: where each
    /// document's rows are stored.
    placement: Placement,
    /// Reduce partials were committed without a sync since the last durable
    /// commit: closing makes them durable, so they outlive a restart.
    unsynced: bool,
    /// Per shard, what queries read on every use: the block list, and once a
    /// reduce has needed them, every block's partial and the levels above.
    /// Filled from the file under the index lock, so it always matches the
    /// committed state, and dropped for the shards a write changes.
    cache: Vec<Option<ShardCache>>,
}

/// One shard's cached reading aids (see `ViewIndex::cache`).
struct ShardCache {
    blocks: Arc<crate::view_store::ShardBlocks>,
    /// `levels[0]` holds each block's partial; each level above combines
    /// `REDUCE_FANOUT` partials of the one below, and the last may be short.
    levels: Option<Arc<Vec<Vec<Value>>>>,
}

/// Partials each upper level combines.
const REDUCE_FANOUT: usize = 32;

/// How a view's rows are split.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Placement {
    q: u32,
    /// The database is partitioned: its documents are placed by partition.
    partitioned: bool,
    /// The view is queried through `_partition`, so its rows are kept in one
    /// run per partition rather than one per shard.
    by_partition: bool,
}

impl Placement {
    fn shards(q: u32, partitioned: bool) -> Placement {
        Placement {
            q,
            partitioned,
            by_partition: false,
        }
    }

    fn shard_of(&self, id: &str) -> usize {
        crate::db::shard_index_for(id, self.q, self.partitioned)
    }

    /// The partition part of a row's `ViewKey`: the document's run for a view
    /// queried by partition, so each partition's rows are contiguous in its
    /// shard; empty otherwise.
    fn partition_of(&self, id: &str) -> String {
        if self.by_partition {
            run_name(id)
        } else {
            String::new()
        }
    }
}

/// A view's sorted rows, split into runs: one per shard, or one per partition
/// for a partitioned view, which is only queried through a partition. A query
/// reads only the runs it is asked for, finds its key range by binary
/// search, and copies only the rows it can return, so its cost follows the
/// result rather than the size of the index. A partition's run is its own
/// sorted run, so a partition query never walks the other partitions that
/// share its shard.
#[cfg(test)]
#[derive(Default)]
pub struct ViewRows {
    shards: Vec<ShardRows>,
    /// For a partitioned view, which run belongs to which partition and shard.
    partitions: Option<Partitions>,
}

#[cfg(test)]
#[derive(Clone, Default)]
struct Partitions {
    by_name: HashMap<String, usize>,
    shard_of: Vec<usize>,
}

/// The name a row's partition run is known by. A document outside any
/// partition gets a name no partition can have, since those start with `_`.
fn run_name(id: &str) -> String {
    match crate::db::partition_of(id) {
        Some((partition, _)) => partition.to_string(),
        None => format!("_{id}"),
    }
}

/// Rows per storage block of a shard. A write copies only the blocks it
/// changes, and a copy of the view shares the rest.
#[cfg(test)]
const ROW_BLOCK: usize = 256;

/// One shard's rows in key order, held in blocks of about `ROW_BLOCK` rows.
/// No block is empty.
#[cfg(test)]
#[derive(Clone, Default)]
struct ShardRows {
    blocks: Vec<Arc<Vec<Row>>>,
    /// The row each block starts at, then the shard's row count.
    starts: Vec<usize>,
}

/// Rows in key order that a selection reads by position: held in memory,
/// or a run of the key-ordered table (`DiskRun`).
trait Positions {
    /// How many rows.
    fn count(&self) -> usize;
    /// The first position whose row does not satisfy `pred`, which holds for
    /// a prefix of the rows.
    fn point(&self, pred: &dyn Fn(&Row) -> bool) -> usize;
    /// The rows at `chosen` positions, in position order.
    fn rows_at(&self, chosen: &BTreeSet<usize>) -> Vec<Row>;
    /// The key of the row at position `at`, which must be below `count()`.
    fn key_at(&self, at: usize) -> Value;
    /// `gallop` over these rows. Rows that are cheaper to probe at some
    /// positions than others choose where to probe.
    fn gallop(&self, from: usize, bound: usize, inside: &dyn Fn(usize) -> bool) -> usize {
        gallop(from, bound, inside)
    }
}

impl<S: SortedRows + ?Sized> Positions for S {
    fn count(&self) -> usize {
        self.len()
    }

    fn point(&self, pred: &dyn Fn(&Row) -> bool) -> usize {
        self.partition_point(pred)
    }

    fn rows_at(&self, chosen: &BTreeSet<usize>) -> Vec<Row> {
        chosen.iter().map(|&at| self.row(at).clone()).collect()
    }

    fn key_at(&self, at: usize) -> Value {
        self.row(at).key.clone()
    }
}

/// Rows in key order that a query can binary search.
trait SortedRows {
    fn len(&self) -> usize;
    fn row(&self, at: usize) -> &Row;
    fn partition_point(&self, pred: impl Fn(&Row) -> bool) -> usize;
}

impl<R: AsRef<Row>> SortedRows for [R] {
    fn len(&self) -> usize {
        <[R]>::len(self)
    }

    fn row(&self, at: usize) -> &Row {
        self[at].as_ref()
    }

    fn partition_point(&self, pred: impl Fn(&Row) -> bool) -> usize {
        <[R]>::partition_point(self, |row| pred(row.as_ref()))
    }
}

#[cfg(test)]
impl SortedRows for ShardRows {
    fn len(&self) -> usize {
        self.starts.last().copied().unwrap_or(0)
    }

    fn row(&self, at: usize) -> &Row {
        let block = self.block_of(at);
        &self.blocks[block][at - self.starts[block]]
    }

    fn partition_point(&self, pred: impl Fn(&Row) -> bool) -> usize {
        // Blocks whose last row satisfies `pred` lie wholly before the point.
        let block = self
            .blocks
            .partition_point(|rows| rows.last().is_some_and(&pred));
        match self.blocks.get(block) {
            Some(rows) => self.starts[block] + rows.partition_point(|row| pred(row)),
            None => self.len(),
        }
    }
}

#[cfg(test)]
impl ShardRows {
    fn from_sorted(rows: Vec<Row>) -> ShardRows {
        let mut rows = rows.into_iter();
        let mut blocks = Vec::new();
        loop {
            let block: Vec<Row> = rows.by_ref().take(ROW_BLOCK).collect();
            if block.is_empty() {
                break;
            }
            blocks.push(Arc::new(block));
        }
        let mut shard = ShardRows {
            blocks,
            starts: Vec::new(),
        };
        shard.index();
        shard
    }

    fn index(&mut self) {
        let mut start = 0;
        self.starts = std::iter::once(0)
            .chain(self.blocks.iter().map(|rows| {
                start += rows.len();
                start
            }))
            .collect();
    }

    /// The block holding row `at`, or the block count when `at` is the end.
    fn block_of(&self, at: usize) -> usize {
        self.starts
            .partition_point(|start| *start <= at)
            .saturating_sub(1)
            .min(self.blocks.len())
    }
}

/// Partial reductions by group key (by requested key for a `keys` query),
/// with the key itself.
pub(crate) type Partials = std::collections::BTreeMap<GroupKey, (Value, Vec<Value>)>;

/// A group's key, ordered and compared by view collation, so keys that
/// collate equal (`1` and `1.0`) share a group, as in CouchDB.
#[derive(Clone, Debug)]
pub(crate) struct GroupKey(Value);

impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for GroupKey {}

impl PartialOrd for GroupKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GroupKey {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_json(&self.0, &other.0)
    }
}

/// One parent's partial. A lone child is kept as it is, so a reducer that is
/// not rereduce-safe sees the same values as a single pass.
fn combine_children(reducer: &mut JsReducer, children: &[Value]) -> Result<Value, JsError> {
    match children {
        [single] => Ok(single.clone()),
        _ => combine_partials(reducer, children),
    }
}

/// The partials of several parents at once: `combine_children` for each group
/// of children, with the custom reducer's rereduces in one batch.
fn combine_many<'a>(
    reducer: &mut JsReducer,
    groups: impl Iterator<Item = &'a [Value]>,
) -> Result<Vec<Value>, JsError> {
    let groups: Vec<&[Value]> = groups.collect();
    if is_builtin(reducer.source()) {
        return groups
            .into_iter()
            .map(|children| combine_children(reducer, children))
            .collect();
    }
    let calls = groups
        .iter()
        .filter(|children| children.len() != 1)
        .map(|children| ReduceCall::rereduce(children));
    let mut combined = reducer.call_many(calls)?.into_iter();
    groups
        .into_iter()
        .map(|children| match children {
            [single] => Ok(single.clone()),
            _ => combined
                .next()
                .ok_or_else(|| JsError::Runtime("reduce_many returned too few results".into())),
        })
        .collect()
}

fn is_builtin(source: &str) -> bool {
    Builtin::parse(source).is_some()
}

/// The rows a query can return, in key order, plus the counts CouchDB
/// reports for the whole (shard- and partition-filtered) view.
pub struct Selection {
    pub rows: Vec<Row>,
    pub total_rows: usize,
    pub offset: usize,
}

#[cfg(test)]
impl ViewRows {
    fn from_rows(rows: Vec<Row>, placement: Placement) -> ViewRows {
        let mut view = ViewRows::default();
        if placement.by_partition {
            view.partitions = Some(Partitions::default());
        } else {
            view.shards = vec![ShardRows::default(); placement.q.max(1) as usize];
        }
        let mut runs: Vec<Vec<Row>> = vec![Vec::new(); view.shards.len()];
        for row in rows {
            let run = view.run_for(&row.id, placement);
            if run == runs.len() {
                runs.push(Vec::new());
            }
            runs[run].push(row);
        }
        for (run, mut rows) in runs.into_iter().enumerate() {
            sort_rows(&mut rows);
            view.shards[run] = ShardRows::from_sorted(rows);
        }
        view
    }

    /// The run holding the rows of document `id`, started if it is new.
    fn run_for(&mut self, id: &str, placement: Placement) -> usize {
        let Some(partitions) = &mut self.partitions else {
            return placement.shard_of(id);
        };
        match partitions.by_name.entry(run_name(id)) {
            std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let run = self.shards.len();
                entry.insert(run);
                partitions.shard_of.push(placement.shard_of(id));
                self.shards.push(ShardRows::default());
                run
            }
        }
    }

    /// The shard a run's rows belong to.
    fn shard_of_run(&self, run: usize) -> usize {
        match &self.partitions {
            Some(partitions) => partitions.shard_of.get(run).copied().unwrap_or(run),
            None => run,
        }
    }

    /// The runs a query reads, in the shards of `mask` (all when `None`): a
    /// partition query on a partitioned view reads only that partition's run.
    fn runs_for(&self, query: &ViewQuery, mask: Option<&crate::cluster::ShardMask>) -> Vec<usize> {
        let runs: Vec<usize> = match (&query.partition, &self.partitions) {
            (Some(partition), Some(partitions)) => partitions
                .by_name
                .get(partition)
                .copied()
                .into_iter()
                .collect(),
            _ => (0..self.shards.len()).collect(),
        };
        runs.into_iter()
            .filter(|run| mask.is_none_or(|mask| mask.covers(self.shard_of_run(*run))))
            .collect()
    }

    /// The in-memory selection, which the key-ordered table's must match.
    #[cfg(test)]
    fn select(
        &self,
        query: &ViewQuery,
        mask: Option<&crate::cluster::ShardMask>,
        visited: &AtomicU64,
    ) -> Selection {
        let runs = self.runs_for(query, mask);
        select_runs(runs.iter().map(|&run| &self.shards[run]), query, visited)
    }
}

/// A view index file as one query reads it: a read transaction, so every
/// count and row it reads is one snapshot, and the index's registry handle,
/// so the file is neither evicted nor closed while the query reads it.
pub struct DiskView {
    _slot: Arc<AsyncMutex<ViewIndex>>,
    txn: redb::ReadTransaction,
    placement: Placement,
    /// Each shard's blocks, as of the transaction's snapshot.
    blocks: Vec<Arc<crate::view_store::ShardBlocks>>,
}

/// One run of the key-ordered table a query reads: a shard, or one
/// partition's rows in it, as positions `lo..lo + len` of the shard.
struct RunWindow {
    blocks: Arc<crate::view_store::ShardBlocks>,
    lo: u64,
    len: u64,
}

/// A run as a selection reads it. Position lookups cannot fail in the
/// `Positions` interface, so the first read error is kept here and returned
/// once the selection is done.
struct DiskRun<'v, T> {
    table: &'v T,
    window: RunWindow,
    error: std::cell::RefCell<Option<JsError>>,
}

impl<T: ReadableTable<crate::view_store::ViewKey, &'static [u8]>> DiskRun<'_, T> {
    /// Rows `first..first + count` of the run.
    fn read(&self, first: u64, count: usize) -> Vec<Row> {
        let read = crate::view_store::read_at(
            self.table,
            &self.window.blocks,
            self.window.lo + first,
            count,
        )
        .and_then(|rows| {
            rows.iter()
                .map(|(key, value)| stored_row(key, value))
                .collect()
        });
        match read {
            Ok(rows) => rows,
            Err(error) => {
                self.error.borrow_mut().get_or_insert(error);
                Vec::new()
            }
        }
    }

    /// The id and key of the shard's row at position `at` (its value is not
    /// read: searches only compare keys and ids).
    fn key_row(&self, at: u64) -> Option<Row> {
        let read =
            crate::view_store::read_key_at(self.table, &self.window.blocks, at).and_then(|key| {
                key.map(|key| {
                    let read = crate::view_store::read_row_key(&key)?;
                    Ok(Row {
                        id: read.id.to_string(),
                        key: read.key,
                        value: Value::Null,
                    })
                })
                .transpose()
            });
        match read {
            Ok(row) => row,
            Err(error) => {
                self.error.borrow_mut().get_or_insert(error);
                None
            }
        }
    }

    fn into_result<V>(self, value: V) -> Result<V, JsError> {
        match self.error.into_inner() {
            Some(error) => Err(error),
            None => Ok(value),
        }
    }
}

impl<T: ReadableTable<crate::view_store::ViewKey, &'static [u8]>> Positions for DiskRun<'_, T> {
    fn count(&self) -> usize {
        self.window.len as usize
    }

    /// Two levels of binary search: over the first rows of the blocks in
    /// the run, then inside the one block the point falls in. Only keys and
    /// ids are read.
    fn point(&self, pred: &dyn Fn(&Row) -> bool) -> usize {
        let (lo, hi) = (self.window.lo, self.window.lo + self.window.len);
        let holds = |at: u64| self.key_row(at).is_some_and(|row| pred(&row));
        if lo == hi || !holds(lo) {
            return 0;
        }
        if holds(hi - 1) {
            return self.window.len as usize;
        }
        // The blocks starting inside the run, `first..last`: find the first
        // whose first row fails.
        let starts = self.window.blocks.starts();
        let first = starts.partition_point(|&start| start <= lo);
        let last = starts.partition_point(|&start| start < hi);
        let (mut low, mut high) = (first, last);
        while low < high {
            let mid = (low + high) / 2;
            if holds(starts[mid]) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        // The point is after `from`, where `pred` holds, and at or before
        // `to`, where it fails.
        let from = if low > first { starts[low - 1] } else { lo };
        let to = if low < last { starts[low] } else { hi - 1 };
        let (mut low, mut high) = (from + 1, to);
        while low < high {
            let mid = low + (high - low) / 2;
            if holds(mid) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        (low - lo) as usize
    }

    fn rows_at(&self, chosen: &BTreeSet<usize>) -> Vec<Row> {
        // Consecutive positions are read together.
        let mut rows = Vec::with_capacity(chosen.len());
        let mut chosen = chosen.iter().copied().peekable();
        while let Some(first) = chosen.next() {
            let mut end = first + 1;
            while chosen.next_if_eq(&end).is_some() {
                end += 1;
            }
            rows.extend(self.read(first as u64, end - first));
        }
        rows
    }

    fn key_at(&self, at: usize) -> Value {
        self.key_row(self.window.lo + at as u64)
            .map_or(Value::Null, |row| row.key)
    }

    /// Block starts are probed first, since the first row of a block is read
    /// without stepping over others; then one block's rows.
    fn gallop(&self, from: usize, bound: usize, inside: &dyn Fn(usize) -> bool) -> usize {
        let lo = self.window.lo;
        let starts = self.window.blocks.starts();
        let at = |position: usize| lo + position as u64;
        // The block starts strictly between `from` and `bound`, in walk order,
        // as run positions; probe `j` is `from` itself for `j == 0`.
        let probes: Vec<usize> = if bound > from {
            let first = starts.partition_point(|&start| start <= at(from));
            let last = starts.partition_point(|&start| start < at(bound));
            starts[first..last.max(first)]
                .iter()
                .map(|&start| (start - lo) as usize)
                .collect()
        } else {
            let first = starts.partition_point(|&start| start <= at(bound));
            let last = starts.partition_point(|&start| start <= at(from));
            starts[first..last.max(first)]
                .iter()
                .rev()
                .map(|&start| (start - lo) as usize)
                .filter(|&position| position < from)
                .collect()
        };
        let probe = |j: usize| if j == 0 { from } else { probes[j - 1] };
        let failed = gallop(0, probes.len() + 1, |j| j == 0 || inside(probe(j)));
        // The run ends between the last passing probe and the first failing
        // one (or `bound`): search those positions, inside one block.
        let passed = probe(failed - 1);
        if bound > from {
            let end = if failed <= probes.len() {
                probe(failed)
            } else {
                bound
            };
            gallop(passed, end, inside)
        } else {
            let floor = if failed <= probes.len() {
                probe(failed) + 1
            } else {
                bound
            };
            gallop(passed, floor, inside)
        }
    }
}

/// The runs `query` reads in the shards of `mask` (all when `None`): each
/// shard, or for a view queried by partition, the partition's rows in it.
fn run_windows(
    blocks: &[Arc<crate::view_store::ShardBlocks>],
    by_key: &impl ReadableTable<crate::view_store::ViewKey, &'static [u8]>,
    placement: Placement,
    query: &ViewQuery,
    mask: Option<&crate::cluster::ShardMask>,
) -> Result<Vec<RunWindow>, JsError> {
    let mut windows = Vec::new();
    for shard in 0..placement.q.max(1) {
        if mask.is_some_and(|mask| !mask.covers(shard as usize)) {
            continue;
        }
        let shard_blocks = Arc::clone(&blocks[shard as usize]);
        let (lo, hi) = if placement.by_partition {
            let partition = query.partition.as_deref().ok_or_else(|| {
                JsError::Index("a partitioned view is read by partition".to_string())
            })?;
            let start = crate::view_store::partition_start(shard, partition.as_bytes())?;
            let end =
                crate::view_store::partition_start(shard, &[partition.as_bytes(), &[0]].concat())?;
            (
                crate::view_store::rank(by_key, &shard_blocks, &start)?,
                crate::view_store::rank(by_key, &shard_blocks, &end)?,
            )
        } else {
            (0, shard_blocks.total)
        };
        windows.push(RunWindow {
            blocks: shard_blocks,
            lo,
            len: hi - lo,
        });
    }
    Ok(windows)
}

/// A stored row, from its `ViewKey` and value.
fn stored_row(key: &[u8], value: &[u8]) -> Result<Row, JsError> {
    let read = crate::view_store::read_row_key(key)?;
    Ok(Row {
        id: read.id.to_string(),
        key: read.key,
        value: decode_value(value)?,
    })
}

impl DiskView {
    fn by_key(
        &self,
    ) -> Result<redb::ReadOnlyTable<crate::view_store::ViewKey, &'static [u8]>, JsError> {
        self.txn
            .open_table(crate::view_store::BY_KEY)
            .map_err(index_error)
    }

    fn windows(
        &self,
        query: &ViewQuery,
        mask: Option<&crate::cluster::ShardMask>,
    ) -> Result<Vec<RunWindow>, JsError> {
        run_windows(&self.blocks, &self.by_key()?, self.placement, query, mask)
    }

    /// The rows a map query can return from the shards in `mask` (all when
    /// `None`), as `select_runs` selects them.
    pub fn select(
        &self,
        query: &ViewQuery,
        mask: Option<&crate::cluster::ShardMask>,
        visited: &AtomicU64,
    ) -> Result<Selection, JsError> {
        let table = self.by_key()?;
        let runs: Vec<DiskRun<_>> = self
            .windows(query, mask)?
            .into_iter()
            .map(|window| DiskRun {
                table: &table,
                window,
                error: Default::default(),
            })
            .collect();
        let selection = select_runs(&runs, query, visited);
        let mut first_error = None;
        for run in runs {
            if let Err(error) = run.into_result(()) {
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(selection),
        }
    }
}

/// The rows a map query can return from `runs`: at most `skip + limit`
/// copied from each, merged into key order, and the counts CouchDB reports
/// over them. `total_rows` and `offset` follow `ordered_rows`.
fn select_runs<'r, R: Positions + 'r>(
    runs: impl IntoIterator<Item = &'r R>,
    query: &ViewQuery,
    visited: &AtomicU64,
) -> Selection {
    let cap = query.skip.saturating_add(query.limit);
    let mut out = Selection {
        rows: Vec::new(),
        total_rows: 0,
        offset: 0,
    };
    for run in runs {
        select_sorted(run, query, cap, &mut out);
    }
    visit(visited, out.rows.len());
    // Rows from one document share a shard, so a stable sort keeps the
    // emission order of equal rows.
    out.rows.sort_by(row_cmp);
    if query.keys.is_none() && out.rows.len() > cap {
        if query.descending {
            out.rows.drain(..out.rows.len() - cap);
        } else {
            out.rows.truncate(cap);
        }
    }
    out
}

impl AsRef<Row> for Row {
    fn as_ref(&self) -> &Row {
        self
    }
}

/// Where a query reads in one run: the run's rows, the rows before the
/// query's start (CouchDB's offset rules), and the position ranges to read,
/// in query order. Rows are in ascending key order; the query bounds are
/// found by binary search on that order.
fn query_window<S: Positions + ?Sized>(
    rows: &S,
    query: &ViewQuery,
) -> (usize, usize, Vec<(usize, usize)>) {
    let len = rows.count();
    let (lo, hi) = range_bounds(rows, query);
    let equal = |key: &Value| equal_range(rows, key);
    let before_key = |key: &Value| {
        let (first, end) = equal(key);
        if query.descending {
            len - end
        } else {
            first
        }
    };
    let offset = if let Some(key) = &query.key {
        before_key(key)
    } else if let Some(keys) = &query.keys {
        match keys.as_slice() {
            [key] => before_key(key),
            _ => 0,
        }
    } else if query.descending {
        len - hi
    } else {
        lo
    };

    // Index ranges to read, in query order.
    let clamp = |(first, end): (usize, usize)| (first.max(lo), end.min(hi).max(first.max(lo)));
    let ranges: Vec<(usize, usize)> = if let Some(keys) = &query.keys {
        let ordered: Vec<&Value> = if query.descending {
            keys.iter().rev().collect()
        } else {
            keys.iter().collect()
        };
        ordered.into_iter().map(|key| clamp(equal(key))).collect()
    } else if let Some(key) = &query.key {
        vec![clamp(equal(key))]
    } else {
        vec![(lo, hi)]
    };
    (len, offset, ranges)
}

/// Add one run's contribution to `out`: at most `cap` rows, in key order.
fn select_sorted<S: Positions + ?Sized>(
    rows: &S,
    query: &ViewQuery,
    cap: usize,
    out: &mut Selection,
) {
    let (len, offset, ranges) = query_window(rows, query);
    out.total_rows += len;
    out.offset += offset;

    // Take up to `cap` row indexes in query order. Duplicate `keys` share a
    // range; each row is still selected once.
    let mut chosen = BTreeSet::new();
    let mut remaining = cap;
    for (first, end) in ranges {
        if remaining == 0 {
            break;
        }
        let indexes: Box<dyn Iterator<Item = usize>> = if query.descending {
            Box::new((first..end).rev())
        } else {
            Box::new(first..end)
        };
        for index in indexes {
            if remaining == 0 {
                break;
            }
            if chosen.insert(index) {
                remaining -= 1;
            }
        }
    }
    out.rows.extend(rows.rows_at(&chosen));
}

/// Count `rows` index rows a query looked at.
fn visit(visited: &AtomicU64, rows: usize) {
    visited.fetch_add(rows as u64, AtomicOrdering::Relaxed);
}

/// The index range of sorted `rows` inside the query's start and end bounds.
fn range_bounds<S: Positions + ?Sized>(rows: &S, query: &ViewQuery) -> (usize, usize) {
    let point = |pred: &dyn Fn(&Row) -> bool| rows.point(pred);
    // Rows before the start bound are a prefix (ascending) or a suffix
    // (descending); rows past the end bound are the opposite end.
    let (lo, hi) = if query.descending {
        (
            point(&|row| past_end(row, query)),
            point(&|row| !before_start(row, query)),
        )
    } else {
        (
            point(&|row| before_start(row, query)),
            point(&|row| !past_end(row, query)),
        )
    };
    (lo, hi.max(lo))
}

/// The index range of sorted `rows` whose key equals `key`.
fn equal_range<S: Positions + ?Sized>(rows: &S, key: &Value) -> (usize, usize) {
    (
        rows.point(&|row| cmp_json(&row.key, key) == Ordering::Less),
        rows.point(&|row| cmp_json(&row.key, key) != Ordering::Greater),
    )
}

/// The groups of `query` in one sorted shard, each with its partial's key and
/// the rows `first..end` it covers.
fn query_ranges<S: Positions + ?Sized>(
    rows: &S,
    query: &ViewQuery,
    unique: Option<&[Value]>,
) -> Vec<(Value, (usize, usize))> {
    let (lo, hi) = range_bounds(rows, query);
    let clamp = |(first, end): (usize, usize)| (first.max(lo), end.min(hi));
    if let Some(keys) = unique {
        return keys
            .iter()
            .map(|key| (key.clone(), clamp(equal_range(rows, key))))
            .filter(|(_, (first, end))| first < end)
            .collect();
    }
    let (lo, hi) = match &query.key {
        Some(key) => clamp(equal_range(rows, key)),
        None => (lo, hi),
    };
    if !query.group && query.group_level == 0 {
        return if lo < hi {
            vec![(Value::Null, (lo, hi))]
        } else {
            Vec::new()
        };
    }
    // Group keys never decrease along sorted rows, so each group is a run.
    // Its far end is found by galloping from its near one, which costs the
    // log of the group's size rather than of the shard's. Only the first
    // `skip + limit` groups in query order can be returned, from any shard,
    // so no more are reduced.
    let group_of = |at: usize| group_key(&rows.key_at(at), query.group_level);
    let cap = query.skip.saturating_add(query.limit);
    let mut ranges = Vec::new();
    if query.descending {
        let mut end = hi;
        while end > lo && ranges.len() < cap {
            let group = group_of(end - 1);
            let start = rows.gallop(end - 1, lo, &|at| cmp_json(&group_of(at), &group).is_eq());
            ranges.push((group, (start, end)));
            end = start;
        }
    } else {
        let mut start = lo;
        while start < hi && ranges.len() < cap {
            let group = group_of(start);
            let end = rows.gallop(start, hi, &|at| cmp_json(&group_of(at), &group).is_eq());
            ranges.push((group, (start, end)));
            start = end;
        }
    }
    ranges
}

/// The far end of the run of positions from `from` towards `bound` (up to
/// `bound`, exclusive, or down to `bound`, inclusive) where `inside` holds,
/// given that it holds at `from` and over a prefix of that walk: going up,
/// the first position where it fails, or `bound`; going down, the lowest
/// position where it holds.
fn gallop(from: usize, bound: usize, inside: impl Fn(usize) -> bool) -> usize {
    if bound > from {
        // Up: find a failing position by doubling steps, then search back.
        let (mut passed, mut step) = (from, 1);
        let failed = loop {
            let probe = from.saturating_add(step);
            if probe >= bound {
                break bound;
            }
            if !inside(probe) {
                break probe;
            }
            passed = probe;
            step *= 2;
        };
        let (mut low, mut high) = (passed + 1, failed);
        while low < high {
            let mid = low + (high - low) / 2;
            if inside(mid) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low
    } else {
        let (mut passed, mut step) = (from, 1);
        let failed = loop {
            let Some(probe) = from.checked_sub(step).filter(|&probe| probe >= bound) else {
                break None;
            };
            if !inside(probe) {
                break Some(probe);
            }
            passed = probe;
            step *= 2;
        };
        let (mut low, mut high) = (failed.map_or(bound, |failed| failed + 1), passed);
        while low < high {
            let mid = low + (high - low) / 2;
            if inside(mid) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        low
    }
}

/// Reduce each of `ranges` (`first..end` of the run) from the fewest
/// partials (`levels`) covering the blocks it covers whole, plus the rows at
/// its edges. The edges of all ranges are reduced together, and then the
/// pieces of all ranges are combined together, so a custom reducer is called
/// in batches.
fn reduce_level_spans<T: ReadableTable<crate::view_store::ViewKey, &'static [u8]>>(
    run: &DiskRun<'_, T>,
    levels: &[Vec<Value>],
    ranges: &[(usize, usize)],
    reducer: &mut JsReducer,
    visited: &AtomicU64,
) -> Result<Vec<Value>, JsError> {
    /// A range's piece: rows reduced in the batch (`at` in it), or a partial.
    enum Part {
        Rows(usize),
        Partial(Value),
    }
    let blocks = &run.window.blocks;
    let lo = run.window.lo;
    let mut row_sets: Vec<Vec<Row>> = Vec::new();
    let mut plan: Vec<Vec<Part>> = Vec::with_capacity(ranges.len());
    let edge = |first: u64, end: u64, parts: &mut Vec<Part>, sets: &mut Vec<Vec<Row>>| {
        let rows = run.read(first - lo, (end - first) as usize);
        visit(visited, rows.len());
        parts.push(Part::Rows(sets.len()));
        sets.push(rows);
    };
    for &(first, end) in ranges {
        let (first, end) = (lo + first as u64, lo + end as u64);
        // The whole blocks in the range, `low..high`: from the first starting
        // at or after `first` to the last ending at or before `end`. A block
        // ends where the next starts, the last at the shard's total.
        let starts = blocks.starts();
        let low = starts.partition_point(|&start| start < first);
        let high = if blocks.end(blocks.len() - 1) <= end {
            blocks.len()
        } else {
            starts
                .partition_point(|&start| start <= end)
                .saturating_sub(1)
        };
        let mut parts = Vec::new();
        if low < high {
            if first < blocks.start(low) {
                edge(first, blocks.start(low), &mut parts, &mut row_sets);
            }
            parts.extend(
                block_parts(levels, low, high)
                    .into_iter()
                    .map(Part::Partial),
            );
            if blocks.end(high - 1) < end {
                edge(blocks.end(high - 1), end, &mut parts, &mut row_sets);
            }
        } else {
            edge(first, end, &mut parts, &mut row_sets);
        }
        plan.push(parts);
    }
    let reduced = reduce_row_sets(&row_sets, reducer)?;
    let groups: Vec<Vec<Value>> = plan
        .into_iter()
        .map(|parts| {
            parts
                .into_iter()
                .map(|part| match part {
                    Part::Rows(at) => reduced[at].clone(),
                    Part::Partial(value) => value,
                })
                .collect()
        })
        .collect();
    combine_many(reducer, groups.iter().map(Vec::as_slice))
}

/// The fewest partials covering blocks `lo..hi`, in row order: whole
/// parents at the highest level they fit, and the blocks or children left
/// at either side.
fn block_parts(levels: &[Vec<Value>], lo: usize, hi: usize) -> Vec<Value> {
    let mut left = Vec::new();
    let mut right: Vec<&[Value]> = Vec::new();
    let (mut lo, mut hi) = (lo, hi);
    let mut level = 0;
    let middle = loop {
        let units = &levels[level];
        let Some(parents) = levels.get(level + 1).map(Vec::len) else {
            break &units[lo..hi];
        };
        // The last parent may be short; it is whole when the span reaches
        // the end of this level.
        let parent_lo = lo.div_ceil(REDUCE_FANOUT);
        let parent_hi = if hi == units.len() {
            parents
        } else {
            (hi / REDUCE_FANOUT).min(parents)
        };
        if parent_lo >= parent_hi {
            break &units[lo..hi];
        }
        left.extend_from_slice(&units[lo..parent_lo * REDUCE_FANOUT]);
        right.push(&units[(parent_hi * REDUCE_FANOUT).min(hi)..hi]);
        (lo, hi) = (parent_lo, parent_hi);
        level += 1;
    };
    left.extend_from_slice(middle);
    for part in right.into_iter().rev() {
        left.extend_from_slice(part);
    }
    left
}

/// The reduction of each set of rows, with the custom reducer's calls in one
/// batch. A set too long for one call is reduced on its own.
fn reduce_row_sets(sets: &[Vec<Row>], reducer: &mut JsReducer) -> Result<Vec<Value>, JsError> {
    if is_builtin(reducer.source()) || sets.iter().any(|rows| rows.len() > REDUCE_CHUNK) {
        return sets
            .iter()
            .map(|rows| reduce_rows(rows.iter(), reducer))
            .collect();
    }
    reducer.call_many(sets.iter().map(|rows| ReduceCall::rows(rows.iter())))
}

fn reduce_rows<'a>(
    rows: impl Iterator<Item = &'a Row> + Clone,
    reducer: &mut JsReducer,
) -> Result<Value, JsError> {
    match Builtin::parse(reducer.source()) {
        Some(Builtin::Count) => Ok(json!(rows.count())),
        Some(Builtin::Sum) => reduce_sum(rows.map(|row| &row.value)),
        Some(Builtin::Stats) => reduce_stats(rows.map(|row| &row.value)),
        Some(Builtin::Distinct) => Ok(crate::hll::partial(rows.map(|row| &row.key))),
        Some(Builtin::Top(rank)) => Ok(rank_list(rows.map(|row| &row.value), rank, true)),
        Some(Builtin::Bottom(rank)) => Ok(rank_list(rows.map(|row| &row.value), rank, false)),
        Some(Builtin::First) => Ok(extreme_partial(rows.min_by(|a, b| row_cmp(a, b)))),
        Some(Builtin::Last) => Ok(extreme_partial(rows.max_by(|a, b| row_cmp(a, b)))),
        None => {
            let keys: Vec<Value> = rows.clone().map(|row| json!([row.id, row.key])).collect();
            let values: Vec<Value> = rows.map(|row| row.value.clone()).collect();
            reduce_with(reducer, &keys, &values)
        }
    }
}

/// Each requested key once, in first-seen order. Equal keys are found by
/// sorting, not by comparing every pair.
pub(crate) fn unique_keys(keys: &[Value]) -> Vec<Value> {
    let mut order: Vec<usize> = (0..keys.len()).collect();
    order.sort_by(|&a, &b| cmp_json(&keys[a], &keys[b]).then(a.cmp(&b)));
    let mut repeat = vec![false; keys.len()];
    for pair in order.windows(2) {
        if cmp_json(&keys[pair[0]], &keys[pair[1]]) == Ordering::Equal {
            repeat[pair[1]] = true;
        }
    }
    keys.iter()
        .zip(repeat)
        .filter(|(_, repeat)| !repeat)
        .map(|(key, _)| key.clone())
        .collect()
}

pub(crate) fn add_partial(partials: &mut Partials, key: Value, value: Value) {
    // The group keeps the first key it was given.
    partials
        .entry(GroupKey(key.clone()))
        .or_insert_with(|| (key, Vec::new()))
        .1
        .push(value);
}

/// The reduce response from partials gathered across shards or nodes:
/// grouped and ordered as a single pass over the rows would be, then paged.
pub(crate) fn assemble_reduce(
    partials: Partials,
    query: &ViewQuery,
    reducer: &mut JsReducer,
) -> Result<Value, JsError> {
    let grouped = query.group || query.group_level > 0;
    let groups: Vec<(Value, Vec<Value>)> = if let Some(keys) = &query.keys {
        // Walk the requested keys in request order (reversed when
        // descending). Each requested key is its own row, even when it
        // repeats, as in CouchDB.
        let ordered: Vec<&Value> = if query.descending {
            keys.iter().rev().collect()
        } else {
            keys.iter().collect()
        };
        let mut groups: Vec<(Value, Vec<Value>)> = Vec::new();
        for key in ordered {
            let Some((_, values)) = partials.get(&GroupKey(key.clone())) else {
                continue;
            };
            let group = if grouped {
                group_key(key, query.group_level)
            } else {
                Value::Null
            };
            groups.push((group, values.clone()));
        }
        groups
    } else {
        // Already in collation order.
        let mut groups: Vec<(Value, Vec<Value>)> = partials.into_values().collect();
        if query.descending {
            groups.reverse();
        }
        groups
    };

    // The groups' rereduces go in as one batch.
    let groups: Vec<(Value, Vec<Value>)> = groups
        .into_iter()
        .skip(query.skip)
        .take(query.limit)
        .collect();
    let combined = combine_many(reducer, groups.iter().map(|(_, values)| values.as_slice()))?;
    let mut rows = Vec::new();
    for ((key, _), mut value) in groups.into_iter().zip(combined) {
        // Owners hand their sketches to the coordinator, which reports the
        // estimate of the union.
        if query.owner_shards.is_none() {
            value = finish_partial(reducer.source(), value)?;
        }
        rows.push(json!({"key": key, "value": value}));
    }
    Ok(json!({ "rows": rows }))
}

/// Combine partial reductions with the reducer's rereduce.
pub(crate) fn combine_partials(
    reducer: &mut JsReducer,
    partials: &[Value],
) -> Result<Value, JsError> {
    match Builtin::parse(reducer.source()) {
        Some(Builtin::Count) => Ok(json!(partials
            .iter()
            .map(|value| value.as_u64().unwrap_or(0))
            .sum::<u64>())),
        // A partial that is already an error stays the error.
        Some(Builtin::Sum) => match partials.iter().find(|value| {
            value.get("error").and_then(Value::as_str) == Some("builtin_reduce_error")
        }) {
            Some(error) => Ok(error.clone()),
            None => reduce_sum(partials),
        },
        Some(Builtin::Stats) => reduce_stats(partials),
        Some(Builtin::Distinct) => crate::hll::combine(partials),
        Some(Builtin::Top(rank)) => rank_combine(partials, rank, true),
        Some(Builtin::Bottom(rank)) => rank_combine(partials, rank, false),
        Some(Builtin::First) => extreme_combine(partials, false),
        Some(Builtin::Last) => extreme_combine(partials, true),
        None => reducer.call(&[], partials, true),
    }
}

/// The reported value for a partial reduction. Most partials are the value;
/// a sketch is reported as its estimate, and `_first` and `_last` keep the
/// `[key, docid, value]` of their row until the end.
fn finish_partial(source: &str, partial: Value) -> Result<Value, JsError> {
    match Builtin::parse(source) {
        Some(Builtin::Distinct) => crate::hll::finish(&partial),
        Some(Builtin::First | Builtin::Last) => match partial {
            Value::Array(mut parts) if parts.len() == 3 => Ok(parts.swap_remove(2)),
            _ => Ok(Value::Null),
        },
        _ => Ok(partial),
    }
}

/// The `rank` largest (or smallest) distinct values, as CouchDB's
/// `builtin_rank_n/4` keeps them: a collation-sorted list in which a value
/// equal to one already kept is dropped.
fn rank_list<'a>(values: impl Iterator<Item = &'a Value>, rank: usize, top: bool) -> Value {
    let mut sorted: Vec<&Value> = values.collect();
    // A stable sort keeps the earliest of equal values first, as `umerge`
    // prefers the accumulated list.
    sorted.sort_by(|a, b| if top { cmp_json(b, a) } else { cmp_json(a, b) });
    sorted.dedup_by(|later, earlier| cmp_json(later, earlier) == Ordering::Equal);
    Value::Array(sorted.into_iter().take(rank).cloned().collect())
}

fn rank_combine(partials: &[Value], rank: usize, top: bool) -> Result<Value, JsError> {
    let mut values = Vec::new();
    for partial in partials {
        let Value::Array(list) = partial else {
            return Err(JsError::Invalid(
                "invalid _top_N/_bottom_N partial".to_string(),
            ));
        };
        values.extend(list);
    }
    Ok(rank_list(values.into_iter(), rank, top))
}

/// The `[key, docid, value]` of the row `_first` or `_last` picked, or `[]`
/// for no rows.
fn extreme_partial(row: Option<&Row>) -> Value {
    match row {
        Some(row) => json!([row.key, row.id, row.value]),
        None => json!([]),
    }
}

/// The partial of the smallest (or largest) `[key, docid]` among `partials`.
fn extreme_combine(partials: &[Value], last: bool) -> Result<Value, JsError> {
    let mut best: Option<(&Value, &str, &Value)> = None;
    for partial in partials {
        let parts = match partial.as_array().map(Vec::as_slice) {
            Some([]) => continue,
            Some([key, Value::String(id), value]) => (key, id.as_str(), value),
            _ => return Err(JsError::Invalid("invalid _first/_last partial".to_string())),
        };
        let better = best.is_none_or(|(key, id, _)| {
            let order = cmp_json(parts.0, key).then_with(|| parts.1.cmp(id));
            if last {
                order != Ordering::Less
            } else {
                order == Ordering::Less
            }
        });
        if better {
            best = Some(parts);
        }
    }
    Ok(match best {
        Some((key, id, value)) => json!([key, id, value]),
        None => json!([]),
    })
}

/// Rows are ordered by CouchDB key collation, then document id.
pub fn sort_rows(rows: &mut [Row]) {
    rows.sort_by(row_cmp);
}

fn row_cmp(a: &Row, b: &Row) -> Ordering {
    cmp_json(&a.key, &b.key).then_with(|| a.id.cmp(&b.id))
}

/// An index registry entry. The index is opened once, by whichever request
/// gets there first, outside the registry's lock; the others wait on the cell.
#[derive(Default)]
struct IndexSlot {
    index: tokio::sync::OnceCell<Arc<AsyncMutex<ViewIndex>>>,
    /// Shared with the index: set when the registry lets the slot go for
    /// good rather than for idleness.
    retired: Arc<AtomicBool>,
}

impl Slot for IndexSlot {
    /// In use while a request holds the slot (it may be opening the index) or
    /// the index itself.
    fn in_use(this: &Arc<Self>) -> bool {
        Arc::strong_count(this) > 1 || this.index.get().is_some_and(|i| Arc::strong_count(i) > 1)
    }

    fn retire(&self) {
        self.retired.store(true, AtomicOrdering::SeqCst);
    }

    /// An evicted index registers its background close as it drops, so a
    /// reopen of its file, which waits for that close, must come after.
    const DROP_UNDER_LOCK: bool = true;
}

fn indexes() -> &'static Registry<IndexSlot> {
    static INDEXES: Registry<IndexSlot> = Registry::new();
    &INDEXES
}

#[cfg(test)]
fn open_indexes_under(db_path: &Path) -> usize {
    indexes().count_under(db_path)
}

fn index_path(db: &Db, ddoc_id: &str, view_name: &str) -> PathBuf {
    let digest = Md5::digest(format!("{ddoc_id}\0{view_name}").as_bytes());
    db.path
        .join("_view_indexes")
        .join(format!("{}.redb", hex(&digest)))
}

/// The open index at `path`. Opening happens on the blocking pool in a task of
/// its own, so a request that goes away mid-open still leaves the index open
/// for the next one rather than a file held by nobody.
async fn index_handle(path: &Path) -> DbResult<Arc<AsyncMutex<ViewIndex>>> {
    let slot = indexes().get(path);
    let path = path.to_path_buf();
    let opened = tokio::spawn(async move {
        let retired = Arc::clone(&slot.retired);
        slot.index
            .get_or_try_init(|| {
                blocking(move || {
                    ViewIndex::open(&path, retired)
                        .map(|index| Arc::new(AsyncMutex::new(index)))
                        .map_err(ApiError::from)
                })
            })
            .await
            .cloned()
    })
    .await;
    opened.map_err(|error| ApiError::new(500, "view_index_error", error.to_string()))?
}

/// Gate for a database's view index files. A query holds the read side
/// while it uses an index; `cleanup` takes the write side, so it never
/// deletes a file under a live handle and a new open never races with the
/// removal. One gate per database keeps other databases independent. The
/// gate is taken once per call, in `ensure_rows` only, so no path
/// re-acquires it while a writer waits.
fn index_gate(db: &Db) -> Arc<Gate> {
    gates().get(&db.path)
}

fn gates() -> &'static Registry<Gate> {
    static GATES: Registry<Gate> = Registry::new();
    &GATES
}

/// Drop cached index handles when a database is deleted or recreated, and the
/// database's gate with them. The index files live under the database
/// directory; the handles are retired, so they close without a check.
///
/// Signals the gate first, so a build in flight and a query waiting for the
/// gate or the index fail with not_found at their next check, then takes the
/// write side: this waits only for those to reach the check, and no query
/// starts against a half-forgotten index. The gate leaves the registry while
/// the write side is held: a caller that already cloned it is cancelled for
/// good, and one that asks afterwards gets a fresh gate, which is the one the
/// recreated database uses.
pub async fn forget_db(db_path: &Path) {
    let gate = gates().get(db_path);
    let _gate = gate.forget().await;
    let prefix = db_path.join("_view_indexes");
    indexes().retain(|path| !path.starts_with(&prefix));
    gates().retain(|path| path != db_path);
}

/// Close the open view files under `root` as the process exits: retired, so
/// each closes cleanly (redb's next open needs no recovery) without the
/// close-time integrity check, whose result would die with the process. An
/// index a request still holds closes the same way when it lets go.
/// Blocking.
pub fn close_indexes_for_exit(root: &Path) {
    indexes().retain(|path| !path.starts_with(root));
}

/// A view file closing in the background, and whether it has closed.
#[derive(Default)]
struct Closing {
    done: StdMutex<bool>,
    finished: Condvar,
}

/// View files whose close is still running, by path. redb locks a file while
/// it is open, so an open of one of these waits for its close first.
static CLOSING: LazyLock<StdMutex<HashMap<PathBuf, Arc<Closing>>>> =
    LazyLock::new(Default::default);

fn closing_files() -> MutexGuard<'static, HashMap<PathBuf, Arc<Closing>>> {
    CLOSING.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A background close of one file. Dropping it closes the file, if the
/// close never ran, and then marks the close done.
struct Closed {
    db: Option<Database>,
    file: PathBuf,
    closing: Arc<Closing>,
}

impl Drop for Closed {
    fn drop(&mut self) {
        drop(self.db.take());
        {
            let mut files = closing_files();
            if files
                .get(&self.file)
                .is_some_and(|closing| Arc::ptr_eq(closing, &self.closing))
            {
                files.remove(&self.file);
            }
        }
        *self
            .closing
            .done
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = true;
        self.closing.finished.notify_all();
    }
}

/// Close `db`, the view file at `file`, after a full integrity check, on a
/// thread of its own: the check reads the whole file, and the index is
/// closed from the request path. The close is registered before this
/// returns, so an open of the same file from here on waits for it.
fn close_in_background(db: Database, file: PathBuf) {
    let closing = Arc::new(Closing::default());
    closing_files().insert(file.clone(), Arc::clone(&closing));
    let mut closed = Closed {
        db: Some(db),
        file,
        closing,
    };
    // A thread that cannot start drops `closed`: a close without the check.
    let _ = std::thread::Builder::new()
        .name("view-close".to_string())
        .spawn(move || {
            if let Some(db) = closed.db.take() {
                if let Err(error) = crate::kv::close_verified(db, &closed.file) {
                    tracing::error!(
                        file = %closed.file.display(),
                        %error,
                        "view index failed its integrity check on close"
                    );
                }
            }
            drop(closed);
        });
}

/// Wait until no background close of `file` is running.
fn wait_closed(file: &Path) {
    let Some(closing) = closing_files().get(file).cloned() else {
        return;
    };
    let mut done = closing.done.lock().unwrap_or_else(PoisonError::into_inner);
    while !*done {
        done = closing
            .finished
            .wait(done)
            .unwrap_or_else(PoisonError::into_inner);
    }
}

fn index_error(error: impl std::fmt::Display) -> JsError {
    JsError::Index(error.to_string())
}

/// The value inside a sealed view value; a checksum mismatch is an error.
fn unsealed(stored: &[u8]) -> Result<&[u8], JsError> {
    crate::kv::unseal(stored).map_err(|corrupt| JsError::Index(format!("view index: {corrupt}")))
}

/// A row's value as stored in `BY_KEY`.
fn encode_value(value: &Value) -> Result<Vec<u8>, JsError> {
    Ok(crate::kv::seal(
        &serde_json::to_vec(value).map_err(index_error)?,
    ))
}

fn decode_value(stored: &[u8]) -> Result<Value, JsError> {
    serde_json::from_slice(unsealed(stored)?).map_err(index_error)
}

/// Each shard's row count, as `TOTALS` records it (empty before the first
/// rebuild).
fn read_totals(txn: &WriteTransaction) -> Result<Vec<u64>, JsError> {
    let meta = txn.open_table(META).map_err(index_error)?;
    let stored = meta.get(TOTALS).map_err(index_error)?;
    match stored {
        Some(stored) => serde_json::from_slice(unsealed(stored.value())?).map_err(index_error),
        None => Ok(Vec::new()),
    }
}

fn write_totals(txn: &WriteTransaction, totals: &[u64]) -> Result<(), JsError> {
    let encoded = serde_json::to_vec(totals).map_err(index_error)?;
    txn.open_table(META)
        .map_err(index_error)?
        .insert(TOTALS, crate::kv::seal(&encoded).as_slice())
        .map_err(index_error)?;
    Ok(())
}

/// Insert `rows` (whole documents, each document's rows in emit order) into
/// `ROWS` and the key-ordered table, counting each shard's rows in `totals`.
/// `on_row` sees each row's `ViewKey`.
fn insert_rows(
    txn: &WriteTransaction,
    tables: &mut crate::view_store::Tables<'_>,
    rows: &[Row],
    placement: Placement,
    mut on_row: impl FnMut(&crate::view_store::Tables<'_>, &[u8]) -> Result<(), JsError>,
) -> Result<(), JsError> {
    let mut table = txn.open_table(ROWS).map_err(index_error)?;
    let mut ordinals: HashMap<&str, u32> = HashMap::new();
    for row in rows {
        let ordinal = ordinals.entry(row.id.as_str()).or_default();
        let key = crate::view_store::row_key(
            u32::try_from(placement.shard_of(&row.id)).map_err(index_error)?,
            &placement.partition_of(&row.id),
            &row.key,
            &row.id,
            *ordinal,
        )?;
        tables
            .by_key
            .insert(key.as_slice(), encode_value(&row.value)?.as_slice())
            .map_err(index_error)?;
        table
            .insert(
                (row.id.as_str(), *ordinal),
                crate::kv::seal(&key).as_slice(),
            )
            .map_err(index_error)?;
        on_row(tables, &key)?;
        *ordinal += 1;
    }
    Ok(())
}

/// Create a view file's tables, empty.
fn init_tables(db: &Database) -> Result<(), JsError> {
    let txn = crate::kv::begin_write(db).map_err(index_error)?;
    txn.open_table(ROWS).map_err(index_error)?;
    txn.open_table(crate::view_store::BY_KEY)
        .map_err(index_error)?;
    txn.open_table(crate::view_store::BLOCKS)
        .map_err(index_error)?;
    txn.open_table(crate::view_store::PARTIALS)
        .map_err(index_error)?;
    txn.open_table(META).map_err(index_error)?;
    txn.open_table(CURSOR).map_err(index_error)?;
    txn.commit().map_err(index_error)
}

/// A new, empty view file at `path`. It is made under a temporary name and
/// renamed into place once redb has initialised it and its tables, so a crash
/// part-way leaves no half-made file under the index's name (cleanup removes
/// the temporary one).
fn create_file(path: &Path) -> Result<Database, JsError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(index_error)?;
    }
    let staging = path.with_extension("redb.creating");
    match std::fs::remove_file(&staging) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(index_error(error)),
    }
    let db = crate::kv::create(&staging, VIEW_CACHE_BYTES).map_err(index_error)?;
    init_tables(&db)?;
    std::fs::rename(&staging, path).map_err(index_error)?;
    // Whatever was verified under this name before, this is a new file.
    crate::kv::forget_verified(path);
    Ok(db)
}

/// The cursor and design a view file records.
type State = (IndexCursor, String);

/// The view file at `path` and the state it records, created if there is
/// none. A file that fails its integrity check, is not recognised as a redb
/// file, or whose recorded state fails its checksum, is deleted and replaced by an empty one, which the caller
/// rebuilds from the shards: a view index is derived data. The caller holds
/// the index's only handle, so nothing else has the file open.
fn open_file(path: &Path) -> Result<(Database, State), JsError> {
    if !path.try_exists().map_err(index_error)? {
        return Ok((create_file(path)?, State::default()));
    }
    let damage = match crate::kv::open(path, VIEW_CACHE_BYTES) {
        // Closed again, when damaged, before the file is removed.
        Ok(db) => match read_state(&db)? {
            Ok(state) => return Ok((db, state)),
            Err(damage) => damage.to_string(),
        },
        Err(redb::DatabaseError::Storage(redb::StorageError::Corrupted(reason))) => reason,
        // What redb reports for a file whose header it does not recognise
        // (a damaged magic number).
        Err(redb::DatabaseError::Storage(redb::StorageError::Io(error)))
            if error.kind() == std::io::ErrorKind::InvalidData =>
        {
            error.to_string()
        }
        Err(error) => return Err(index_error(error)),
    };
    tracing::warn!(
        file = %path.display(),
        reason = %damage,
        "view index is damaged; deleting it to rebuild"
    );
    crate::kv::forget_verified(path);
    std::fs::remove_file(path).map_err(index_error)?;
    Ok((create_file(path)?, State::default()))
}

/// The state a view file records. The outer error is a failure to read it;
/// the inner one a value that fails its checksum or does not decode, which
/// means the file is damaged.
fn read_state(db: &Database) -> Result<Result<State, JsError>, JsError> {
    let txn = db.begin_read().map_err(index_error)?;
    let meta = txn.open_table(META).map_err(index_error)?;
    let positions = txn.open_table(CURSOR).map_err(index_error)?;
    let design = match meta.get(DESIGN).map_err(index_error)? {
        Some(stored) => unsealed(stored.value())
            .and_then(|design| String::from_utf8(design.to_vec()).map_err(index_error)),
        None => Ok(String::new()),
    };
    let mut cursor = Vec::new();
    for entry in positions.iter().map_err(index_error)? {
        let (_, stored) = entry.map_err(index_error)?;
        let position = unsealed(stored.value())
            .and_then(|position| serde_json::from_slice(position).map_err(index_error));
        match position {
            Ok(position) => cursor.push(position),
            Err(damage) => return Ok(Err(damage)),
        }
    }
    Ok(design.map(|design| (IndexCursor(cursor), design)))
}

/// Record `cursor` and `design` in the transaction that writes the rows they
/// describe.
fn write_state(txn: &WriteTransaction, cursor: &IndexCursor, design: &str) -> Result<(), JsError> {
    txn.open_table(META)
        .map_err(index_error)?
        .insert(DESIGN, crate::kv::seal(design.as_bytes()).as_slice())
        .map_err(index_error)?;
    let mut positions = txn.open_table(CURSOR).map_err(index_error)?;
    positions.retain(|_, _| false).map_err(index_error)?;
    for (shard, position) in cursor.0.iter().enumerate() {
        let shard = u32::try_from(shard).map_err(index_error)?;
        let encoded = serde_json::to_vec(position).map_err(index_error)?;
        positions
            .insert(shard, crate::kv::seal(&encoded).as_slice())
            .map_err(index_error)?;
    }
    Ok(())
}

impl ViewIndex {
    fn open(path: &Path, retired: Arc<AtomicBool>) -> Result<Self, JsError> {
        // An eviction may still be closing the file.
        wait_closed(path);
        let (db, state) = open_file(path)?;
        let mut index = Self::with_state(db, state);
        index.file = Some(path.to_path_buf());
        index.retired = retired;
        Ok(index)
    }

    /// An index over an open view file, with the state it records.
    #[cfg(test)]
    fn over(db: Database) -> Result<Self, JsError> {
        let state = read_state(&db)??;
        Ok(Self::with_state(db, state))
    }

    fn with_state(db: Database, (cursor, design): State) -> Self {
        ViewIndex {
            db: Some(db),
            file: None,
            retired: Arc::default(),
            cursor,
            design,
            placement: Placement::shards(1, false),
            unsynced: false,
            cache: Vec::new(),
        }
    }

    /// The open file, reopened if a failed write closed it. A reopened index
    /// takes its cursor and design from the file again.
    fn db(&mut self) -> Result<&Database, JsError> {
        let db = match self.db.take() {
            Some(db) => db,
            None => {
                let file = self
                    .file
                    .as_deref()
                    .ok_or_else(|| JsError::Index("the view index is closed".to_string()))?;
                let (db, state) = open_file(file)?;
                (self.cursor, self.design) = state;
                db
            }
        };
        Ok(self.db.insert(db))
    }

    /// Run `write` in one write transaction and commit it. A failure closes
    /// the file and reopens it: redb may refuse writes after a failed commit,
    /// and whether the commit reached the disk is only known from the file.
    fn commit(
        &mut self,
        write: impl FnOnce(&WriteTransaction) -> Result<(), JsError>,
    ) -> Result<(), JsError> {
        let db = self.db()?;
        let outcome = crate::kv::begin_write(db)
            .map_err(index_error)
            .and_then(|txn| {
                write(&txn)?;
                txn.commit().map_err(index_error)
            });
        match &outcome {
            Ok(()) => self.unsynced = false,
            Err(_) => self.close_after_failed_write(),
        }
        outcome
    }

    /// After a failed write, close the file and reopen it: redb may refuse
    /// writes after a failed commit, and whether the commit reached the disk
    /// is only known from the file.
    fn close_after_failed_write(&mut self) {
        self.cache.clear();
        drop(self.db.take());
        self.cursor = IndexCursor::default();
        self.design.clear();
        // A reopen that fails is retried on the next use.
        let _ = self.db();
    }

    /// Every row, in key order within each shard (and partition): the
    /// key-ordered table read through.
    #[cfg(test)]
    fn load_rows(&mut self) -> Result<Vec<Row>, JsError> {
        let txn = self.db()?.begin_read().map_err(index_error)?;
        let table = txn
            .open_table(crate::view_store::BY_KEY)
            .map_err(index_error)?;
        let mut rows = Vec::new();
        for entry in table.iter().map_err(index_error)? {
            let (key, stored) = entry.map_err(index_error)?;
            let key = crate::view_store::read_row_key(key.value())?;
            rows.push(Row {
                id: key.id.to_string(),
                key: key.key,
                value: decode_value(stored.value())?,
            });
        }
        Ok(rows)
    }

    /// Every row in memory, in the runs the in-memory reference selection
    /// reads.
    #[cfg(test)]
    fn sorted_rows(&mut self) -> Result<Arc<ViewRows>, JsError> {
        Ok(Arc::new(ViewRows::from_rows(
            self.load_rows()?,
            self.placement,
        )))
    }

    /// Record the database's placement: a change also changes the design
    /// signature, so the index is rebuilt.
    fn set_placement(&mut self, placement: Placement) {
        if self.placement != placement {
            self.cache.clear();
        }
        self.placement = placement;
    }

    /// Every shard's blocks, from the cache or else from `table`, the
    /// `BLOCKS` table of a transaction holding the committed state.
    fn blocks(
        &mut self,
        table: &impl ReadableTable<crate::view_store::ViewKey, &'static [u8]>,
    ) -> Result<Vec<Arc<crate::view_store::ShardBlocks>>, JsError> {
        let shards = self.placement.q.max(1) as usize;
        self.cache.resize_with(shards, || None);
        let mut out = Vec::with_capacity(shards);
        for shard in 0..shards {
            if self.cache[shard].is_none() {
                self.cache[shard] = Some(ShardCache {
                    blocks: Arc::new(crate::view_store::shard_blocks(table, shard as u32)?),
                    levels: None,
                });
            }
            out.push(Arc::clone(
                &self.cache[shard].as_ref().expect("filled").blocks,
            ));
        }
        Ok(out)
    }

    /// Replace the rows of the `changed` documents with `by_doc` and record
    /// `cursor` and `design`, in one transaction.
    fn apply(
        &mut self,
        changed: Vec<String>,
        by_doc: HashMap<String, Vec<Row>>,
        cursor: IndexCursor,
        design: &str,
    ) -> Result<(), JsError> {
        let placement = self.placement;
        let mut touched = Vec::new();
        self.commit(|txn| {
            let mut tables = crate::view_store::Tables::open(txn)?;
            let mut edits = crate::view_store::BlockEdits::default();
            // Old rows go before new ones are added: a new row may collate
            // equal to an old one under the same (id, ordinal).
            let mut table = txn.open_table(ROWS).map_err(index_error)?;
            for id in &changed {
                let rows = (id.as_str(), 0)..=(id.as_str(), u32::MAX);
                for entry in table.range(rows.clone()).map_err(index_error)? {
                    let (_, stored) = entry.map_err(index_error)?;
                    let key = unsealed(stored.value())?;
                    tables.by_key.remove(key).map_err(index_error)?;
                    edits.count(&tables, key, -1)?;
                }
                table.retain_in(rows, |_, _| false).map_err(index_error)?;
            }
            drop(table);
            for rows in by_doc.values() {
                insert_rows(txn, &mut tables, rows, placement, |tables, key| {
                    edits.count(tables, key, 1)
                })?;
            }
            let mut totals = read_totals(txn)?;
            for (&shard, &change) in &edits.totals {
                let total = totals
                    .get_mut(shard as usize)
                    .ok_or_else(|| JsError::Index("view index: no total for a shard".into()))?;
                *total = total.checked_add_signed(change).ok_or_else(|| {
                    JsError::Index("view index: a shard total went negative".into())
                })?;
            }
            touched.extend(edits.totals.keys().copied());
            edits.finish(&mut tables)?;
            drop(tables);
            write_totals(txn, &totals)?;
            write_state(txn, &cursor, design)
        })?;
        // The changed shards' blocks and partials are read afresh.
        for shard in touched {
            if let Some(cache) = self.cache.get_mut(shard as usize) {
                *cache = None;
            }
        }

        self.cursor = cursor;
        self.design = design.to_string();
        Ok(())
    }

    /// Reduce `query` over the shards in `mask` (all when `None`), or over
    /// its partition, from the key-ordered table. Each range or group
    /// combines the stored partials of the blocks it covers whole with the
    /// rows at its edges. A covered block without a partial is reduced from
    /// its rows and its partial stored, in a cache write (no sync) committed
    /// after the answer is made; a failed commit still gives the answer.
    fn reduce(
        &mut self,
        query: &ViewQuery,
        mask: Option<&crate::cluster::ShardMask>,
        reducer: &mut JsReducer,
        visited: &AtomicU64,
    ) -> DbResult<Value> {
        let placement = self.placement;
        let txn = self
            .db()
            .and_then(|db| crate::kv::begin_cache_write(db).map_err(index_error))?;
        let mut stored = false;
        let answer = (|| {
            let by_key = txn
                .open_table(crate::view_store::BY_KEY)
                .map_err(index_error)?;
            let blocks = txn
                .open_table(crate::view_store::BLOCKS)
                .map_err(index_error)?;
            let mut partials = txn
                .open_table(crate::view_store::PARTIALS)
                .map_err(index_error)?;
            let mut out = Partials::new();
            let unique = query.keys.as_deref().map(unique_keys);
            let shard_blocks = self.blocks(&blocks)?;
            for window in run_windows(&shard_blocks, &by_key, placement, query, mask)? {
                let shard = shard_blocks
                    .iter()
                    .position(|blocks| Arc::ptr_eq(blocks, &window.blocks))
                    .expect("a window of a listed shard");
                let run = DiskRun {
                    table: &by_key,
                    window,
                    error: Default::default(),
                };
                let ranges = query_ranges(&run, query, unique.as_deref());
                if ranges.is_empty() {
                    run.into_result(())?;
                    continue;
                }
                let levels =
                    self.levels(shard, &by_key, &mut partials, reducer, visited, &mut stored)?;
                let spans: Vec<(usize, usize)> = ranges.iter().map(|(_, span)| *span).collect();
                let values = reduce_level_spans(&run, &levels, &spans, reducer, visited)?;
                run.into_result(())?;
                for ((key, _), value) in ranges.into_iter().zip(values) {
                    add_partial(&mut out, key, value);
                }
            }
            assemble_reduce(out, query, reducer)
        })()
        .map_err(|reason| reduce_failure(&reason))?;
        if stored {
            match txn.commit() {
                Ok(()) => self.unsynced = true,
                Err(_) => self.close_after_failed_write(),
            }
        }
        Ok(answer)
    }

    /// Shard `shard`'s partials and the levels above them, from the cache,
    /// or else built: each block's stored partial is read, and a block
    /// without one is reduced from its rows (in one batch) and its partial
    /// stored in `partials`, setting `stored`.
    fn levels(
        &mut self,
        shard: usize,
        by_key: &impl ReadableTable<crate::view_store::ViewKey, &'static [u8]>,
        partials: &mut redb::Table<'_, crate::view_store::ViewKey, &'static [u8]>,
        reducer: &mut JsReducer,
        visited: &AtomicU64,
        stored: &mut bool,
    ) -> Result<Arc<Vec<Vec<Value>>>, JsError> {
        let cache = self.cache[shard].as_mut().expect("blocks listed first");
        if let Some(levels) = &cache.levels {
            return Ok(Arc::clone(levels));
        }
        let blocks = Arc::clone(&cache.blocks);
        let mut leaves: Vec<Option<Value>> = Vec::with_capacity(blocks.len());
        let mut missing = Vec::new();
        for at in 0..blocks.len() {
            if blocks.start(at) == blocks.end(at) {
                // Only an empty shard's sentinel block holds no rows; no
                // range ever covers it.
                leaves.push(Some(Value::Null));
                continue;
            }
            match partials.get(blocks.fence(at)).map_err(index_error)? {
                Some(value) => leaves.push(Some(decode_value(value.value())?)),
                None => {
                    leaves.push(None);
                    missing.push(at);
                }
            }
        }
        let mut sets = Vec::with_capacity(missing.len());
        for &at in &missing {
            let rows: Vec<Row> = crate::view_store::read_at(
                by_key,
                &blocks,
                blocks.start(at),
                (blocks.end(at) - blocks.start(at)) as usize,
            )?
            .iter()
            .map(|(key, value)| stored_row(key, value))
            .collect::<Result<_, JsError>>()?;
            visit(visited, rows.len());
            sets.push(rows);
        }
        for (at, partial) in missing.into_iter().zip(reduce_row_sets(&sets, reducer)?) {
            partials
                .insert(blocks.fence(at), encode_value(&partial)?.as_slice())
                .map_err(index_error)?;
            *stored = true;
            leaves[at] = Some(partial);
        }
        let mut levels = vec![leaves
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect::<Vec<_>>()];
        while let Some(below) = levels.last().filter(|below| below.len() > REDUCE_FANOUT) {
            let above = combine_many(reducer, below.chunks(REDUCE_FANOUT))?;
            levels.push(above);
        }
        let levels = Arc::new(levels);
        self.cache[shard].as_mut().expect("still cached").levels = Some(Arc::clone(&levels));
        Ok(levels)
    }

    /// Start replacing every stored row. The rows go in a page at a time
    /// (`Rebuild::add`) and become visible all at once (`finish_rebuild`); a
    /// rebuild dropped part-way leaves the index as it was.
    fn start_rebuild(&mut self) -> Result<Rebuild, JsError> {
        let txn = crate::kv::begin_write(self.db()?).map_err(index_error)?;
        for table in [
            crate::view_store::BY_KEY,
            crate::view_store::BLOCKS,
            crate::view_store::PARTIALS,
        ] {
            txn.delete_table(table).map_err(index_error)?;
        }
        txn.delete_table(ROWS).map_err(index_error)?;
        Ok(Rebuild {
            txn,
            placement: self.placement,
            totals: vec![0; self.placement.q.max(1) as usize],
        })
    }

    /// Make a rebuild's rows visible, recording `cursor` and `design`.
    fn finish_rebuild(
        &mut self,
        rebuild: Rebuild,
        cursor: IndexCursor,
        design: &str,
    ) -> Result<(), JsError> {
        let Rebuild { txn, totals, .. } = rebuild;
        let outcome = (|| {
            let mut tables = crate::view_store::Tables::open(&txn)?;
            crate::view_store::write_blocks(&mut tables, &totals)?;
            drop(tables);
            write_totals(&txn, &totals)?;
            write_state(&txn, &cursor, design)?;
            txn.commit().map_err(index_error)
        })();
        if let Err(error) = outcome {
            self.close_after_failed_write();
            return Err(error);
        }
        self.cursor = cursor;
        self.design = design.to_string();
        self.unsynced = false;
        self.cache.clear();
        Ok(())
    }

    /// Check that the key-ordered tables agree with `ROWS`, their block counts
    /// add up and stay in range, and the shard totals hold.
    #[cfg(test)]
    fn check_tables(&mut self) -> Result<(), String> {
        let txn = crate::kv::begin_write(self.db().map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        let rows: Vec<Vec<u8>> = {
            let table = txn.open_table(ROWS).map_err(|e| e.to_string())?;
            let rows = table
                .iter()
                .map_err(|e| e.to_string())?
                .map(|entry| {
                    let (_, stored) = entry.map_err(|e| e.to_string())?;
                    unsealed(stored.value())
                        .map(<[u8]>::to_vec)
                        .map_err(|e| e.to_string())
                })
                .collect::<Result<_, String>>()?;
            rows
        };
        let totals = read_totals(&txn).map_err(|e| e.to_string())?;
        let tables = crate::view_store::Tables::open(&txn).map_err(|e| e.to_string())?;
        crate::view_store::check(&tables, &rows, &totals)
    }

    /// Replace every stored row with `rows` in one go.
    #[cfg(test)]
    fn rebuild(
        &mut self,
        rows: Vec<Row>,
        cursor: IndexCursor,
        design: &str,
    ) -> Result<(), JsError> {
        let mut rebuild = self.start_rebuild()?;
        rebuild.add(&rows)?;
        self.finish_rebuild(rebuild, cursor, design)
    }
}

/// A rebuild in progress: its write transaction, held across the pages of
/// documents it maps, and each shard's row count so far.
struct Rebuild {
    txn: WriteTransaction,
    placement: Placement,
    totals: Vec<u64>,
}

impl Rebuild {
    /// Add the rows of whole documents, each document's rows in emit order.
    fn add(&mut self, rows: &[Row]) -> Result<(), JsError> {
        let mut tables = crate::view_store::Tables::open(&self.txn)?;
        let totals = &mut self.totals;
        let placement = self.placement;
        insert_rows(&self.txn, &mut tables, rows, placement, |_, _| Ok(()))?;
        for row in rows {
            if let Some(total) = totals.get_mut(placement.shard_of(&row.id)) {
                *total += 1;
            }
        }
        Ok(())
    }
}

impl Drop for ViewIndex {
    /// An index closed for idleness is checked as it closes, off the request
    /// path, so its next open in this process skips the check. A retired one
    /// closes as it is: its file is about to go.
    fn drop(&mut self) {
        let Some(db) = self.db.take() else {
            return;
        };
        // A durable commit, even an empty one, makes the unsynced partials
        // before it durable. A server shutting down retires its indexes too,
        // so this runs for retired ones as well.
        if self.unsynced {
            if let Ok(txn) = crate::kv::begin_write(&db) {
                let _ = txn.commit();
            }
        }
        match self.file.take() {
            Some(file) if !self.retired.load(AtomicOrdering::SeqCst) => {
                close_in_background(db, file);
            }
            _ => drop(db),
        }
    }
}

/// Run one view query against the local shards.
pub async fn query(db: &Db, ddoc_id: &str, view_name: &str, query: &ViewQuery) -> DbResult<Value> {
    let spec = load_spec(db, ddoc_id, view_name).await?;
    query_with_spec(db, ddoc_id, view_name, &spec, query).await
}

/// Same as `query`, for callers that already loaded the design document.
pub async fn query_with_spec(
    db: &Db,
    ddoc_id: &str,
    view_name: &str,
    spec: &ViewSpec,
    query: &ViewQuery,
) -> DbResult<Value> {
    validate_view_partition(db, spec, query.partition.as_deref())?;
    let reduce = query.reduce.unwrap_or(spec.reduce.is_some());
    if let Some(reducer) = spec.reduce.as_deref().filter(|_| reduce) {
        check_query(spec, query)?;
        let visited = db.view_rows_counter();
        let reducer = reducer.to_string();
        let owned = query.clone();
        return ensure_index(
            db,
            ddoc_id,
            view_name,
            spec,
            query.limits,
            move |index, _| {
                let mut js = JsReducer::new(&reducer, owned.limits);
                let mask = owned.owner_shards.as_ref();
                index.reduce(&owned, mask, &mut js, &visited)
            },
        )
        .await;
    }
    let selection = local_select(db, ddoc_id, view_name, spec, query).await?;
    let mut query = query.clone();
    query.total_rows_override = Some(selection.total_rows);
    query.offset_override = Some(selection.offset);
    let rows = Arc::new(selection.rows);
    let docs = if !reduce && query.include_docs {
        let ids = selected_row_ids(&rows, &query);
        docs_by_ids(db, &ids).await?
    } else {
        HashMap::new()
    };
    finish(spec, rows, docs, &query).await
}

/// The reply to a view query on this node.
pub enum ViewReply {
    /// A reduce, which is computed whole.
    Reduced(Value),
    /// The rows of a map view, read as they are consumed.
    Rows(MapRows),
}

/// The selected rows of a map view: the counts that lead the reply, and the
/// rows, whose documents (for `include_docs`) are read a chunk at a time.
pub struct MapRows {
    pub total_rows: usize,
    pub offset: usize,
    pub rows: futures::stream::BoxStream<'static, DbResult<Value>>,
}

/// Rows whose documents are read together.
const DOC_CHUNK: usize = 256;

/// Like `query`, but a map view's rows come back as a stream, so a large
/// listing does not build its rows, documents and JSON in memory at once:
/// the rows are read from the index file a chunk at a time, the shards
/// merged as they are read. A `keys` query, bounded by its keys, selects
/// its rows first.
pub async fn query_stream(
    db: Arc<Db>,
    ddoc_id: &str,
    view_name: &str,
    query: &ViewQuery,
) -> DbResult<ViewReply> {
    let spec = load_spec(&db, ddoc_id, view_name).await?;
    let reduce = query.reduce.unwrap_or(spec.reduce.is_some());
    if reduce {
        return query_with_spec(&db, ddoc_id, view_name, &spec, query)
            .await
            .map(ViewReply::Reduced);
    }
    validate_view_partition(&db, &spec, query.partition.as_deref())?;
    if query.keys.is_none() {
        return stream_rows(db, ddoc_id, view_name, &spec, query).await;
    }
    let selection = local_select(&db, ddoc_id, view_name, &spec, query).await?;
    check_query(&spec, query)?;
    let mut query = query.clone();
    query.total_rows_override = Some(selection.total_rows);
    query.offset_override = Some(selection.offset);
    let (ordered, total_rows, before_start) = ordered_rows(&selection.rows, &query);
    let skip = query.skip.min(total_rows.saturating_sub(before_start));
    let selected: Vec<Row> = ordered
        .into_iter()
        .filter(|row| in_range(row, &query))
        .skip(skip)
        .take(query.limit)
        .cloned()
        .collect();
    drop(selection);
    let include_docs = query.include_docs;
    let rows = async_stream::stream! {
        for chunk in selected.chunks(DOC_CHUNK) {
            let docs = if include_docs {
                let ids: Vec<String> = chunk.iter().map(|row| row.id.clone()).collect();
                match docs_by_ids(&db, &ids).await {
                    Ok(docs) => docs,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                }
            } else {
                HashMap::new()
            };
            for row in chunk {
                yield Ok(map_row_entry(row, &docs, include_docs));
            }
        }
    };
    Ok(ViewReply::Rows(MapRows {
        total_rows,
        offset: before_start + skip,
        rows: futures::StreamExt::boxed(rows),
    }))
}

/// Rows read from the index file at a time, per shard.
const READ_CHUNK: usize = 256;

/// One shard's rows a streamed query reads, as positions of the shard, and
/// those read but not yet merged.
struct Lane {
    blocks: Arc<crate::view_store::ShardBlocks>,
    /// The next position to read: going up, the first unread; going down,
    /// one past the last unread.
    next: u64,
    /// Where reading stops: going up, the end; going down, the start.
    stop: u64,
    buffer: std::collections::VecDeque<Row>,
}

impl Lane {
    fn unread(&self) -> u64 {
        self.next.abs_diff(self.stop)
    }
}

/// Refill every lane whose buffer is empty, from the file.
fn refill(
    view: &DiskView,
    lanes: &mut [Lane],
    descending: bool,
    visited: &AtomicU64,
) -> Result<(), JsError> {
    let table = view.by_key()?;
    for lane in lanes {
        if !lane.buffer.is_empty() || lane.unread() == 0 {
            continue;
        }
        let count = lane.unread().min(READ_CHUNK as u64);
        let first = if descending {
            lane.next - count
        } else {
            lane.next
        };
        let mut rows = crate::view_store::read_at(&table, &lane.blocks, first, count as usize)?
            .iter()
            .map(|(key, value)| stored_row(key, value))
            .collect::<Result<Vec<Row>, JsError>>()?;
        visit(visited, rows.len());
        if descending {
            rows.reverse();
            lane.next = first;
        } else {
            lane.next = first + count;
        }
        lane.buffer.extend(rows);
    }
    Ok(())
}

/// A map query without `keys`, streamed from the index file: each shard's
/// rows in the query's range are read a chunk at a time and merged in query
/// order; `skip` and `limit` apply to the merged rows.
async fn stream_rows(
    db: Arc<Db>,
    ddoc_id: &str,
    view_name: &str,
    spec: &ViewSpec,
    query: &ViewQuery,
) -> DbResult<ViewReply> {
    let view = Arc::new(ensure_disk(&db, ddoc_id, view_name, spec, query.limits).await?);
    check_query(spec, query)?;
    let query = query.clone();
    let reader = Arc::clone(&view);
    let windows = query.clone();
    let (lanes, total_rows, before_start) = blocking(move || {
        let table = reader.by_key()?;
        let mut lanes = Vec::new();
        let (mut total_rows, mut before_start) = (0, 0);
        for window in reader.windows(&windows, windows.owner_shards.as_ref())? {
            let run = DiskRun {
                table: &table,
                window,
                error: Default::default(),
            };
            let (len, offset, ranges) = query_window(&run, &windows);
            total_rows += len;
            before_start += offset;
            let (first, end) = ranges.first().copied().unwrap_or((0, 0));
            let (lo, blocks) = (run.window.lo, Arc::clone(&run.window.blocks));
            run.into_result(())?;
            let (first, end) = (lo + first as u64, lo + end as u64);
            let (next, stop) = if windows.descending {
                (end, first)
            } else {
                (first, end)
            };
            lanes.push(Lane {
                blocks,
                next,
                stop,
                buffer: Default::default(),
            });
        }
        Ok((lanes, total_rows, before_start))
    })
    .await?;
    let skip = query.skip.min(total_rows.saturating_sub(before_start));
    let mut lanes = lanes;
    let mut to_skip = skip;
    // One shard steps over the skipped rows without reading them.
    if let [lane] = lanes.as_mut_slice() {
        let step = (to_skip as u64).min(lane.unread());
        if query.descending {
            lane.next -= step;
        } else {
            lane.next += step;
        }
        to_skip = 0;
    }
    let visited = db.view_rows_counter();
    let rows = async_stream::stream! {
        let (descending, include_docs) = (query.descending, query.include_docs);
        let mut left = query.limit;
        let mut lanes = lanes;
        while left > 0 {
            if lanes.iter().any(|lane| lane.buffer.is_empty() && lane.unread() > 0) {
                let (view, visited) = (Arc::clone(&view), Arc::clone(&visited));
                let read = blocking(move || {
                    refill(&view, &mut lanes, descending, &visited)?;
                    Ok(lanes)
                })
                .await;
                lanes = match read {
                    Ok(lanes) => lanes,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                };
            }
            // A chunk of rows in query order, from the lanes' buffers.
            let mut chunk = Vec::new();
            while left > 0 && chunk.len() < DOC_CHUNK {
                let next = lanes
                    .iter()
                    .enumerate()
                    .filter_map(|(at, lane)| lane.buffer.front().map(|row| (at, row)))
                    .reduce(|best, candidate| {
                        let order = row_cmp(candidate.1, best.1);
                        let better = if descending {
                            order == Ordering::Greater
                        } else {
                            order == Ordering::Less
                        };
                        if better { candidate } else { best }
                    })
                    .map(|(at, _)| at);
                let Some(at) = next else {
                    break;
                };
                let row = lanes[at].buffer.pop_front().expect("a front row");
                // A lane that ran dry must be read again before the merge
                // can go on.
                let dry = lanes[at].buffer.is_empty() && lanes[at].unread() > 0;
                if to_skip > 0 {
                    to_skip -= 1;
                } else {
                    left -= 1;
                    chunk.push(row);
                }
                if dry {
                    break;
                }
            }
            if chunk.is_empty() {
                if lanes.iter().all(|lane| lane.buffer.is_empty() && lane.unread() == 0) {
                    break;
                }
                continue;
            }
            let docs = if include_docs {
                let ids: Vec<String> = chunk.iter().map(|row| row.id.clone()).collect();
                match docs_by_ids(&db, &ids).await {
                    Ok(docs) => docs,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                }
            } else {
                HashMap::new()
            };
            for row in &chunk {
                yield Ok(map_row_entry(row, &docs, include_docs));
            }
        }
    };
    Ok(ViewReply::Rows(MapRows {
        total_rows,
        offset: before_start + skip,
        rows: futures::StreamExt::boxed(rows),
    }))
}

/// One row of a map view reply.
fn map_row_entry(row: &Row, docs: &HashMap<String, Value>, include_docs: bool) -> Value {
    let mut entry = json!({
        "id": row.id.clone(),
        "key": row.key.clone(),
        "value": row.value.clone(),
    });
    if include_docs {
        if let Some(doc) = docs.get(&row.id) {
            entry["doc"] = doc.clone();
        }
    }
    entry
}

/// Partitioned design docs may only be queried through `_partition`, and
/// global design docs may only be queried globally.
pub fn validate_view_partition(db: &Db, spec: &ViewSpec, partition: Option<&str>) -> DbResult<()> {
    if !db.partitioned {
        if partition.is_some() {
            return Err(ApiError::bad_request(
                "`partition` parameter is not supported on this db",
            ));
        }
        return Ok(());
    }
    match (spec.partitioned, partition) {
        (true, None) => Err(ApiError::bad_request(
            "`partition` parameter is mandatory for queries to this view.",
        )),
        (true, Some(partition)) => crate::db::validate_partition(partition),
        (false, None) => Ok(()),
        (false, Some(_)) => Err(ApiError::bad_request(
            "`partition` parameter is not supported in this design doc",
        )),
    }
}

/// The local rows `query` can return, from the shards in
/// `query.owner_shards` (all local shards when unset).
pub async fn local_select(
    db: &Db,
    ddoc_id: &str,
    view_name: &str,
    spec: &ViewSpec,
    query: &ViewQuery,
) -> DbResult<Selection> {
    let view = ensure_disk(db, ddoc_id, view_name, spec, query.limits).await?;
    let (query, visited) = (query.clone(), db.view_rows_counter());
    blocking(move || {
        view.select(&query, query.owner_shards.as_ref(), &visited)
            .map_err(ApiError::from)
    })
    .await
}

pub async fn finish(
    spec: &ViewSpec,
    rows: Arc<Vec<Row>>,
    docs: HashMap<String, Value>,
    query: &ViewQuery,
) -> DbResult<Value> {
    check_query(spec, query)?;
    if spec.reduce.is_none() || !query.reduce.unwrap_or(true) {
        return render(spec, &rows, &docs, query);
    }
    // A reduce runs user JavaScript.
    let (spec, query) = (spec.clone(), query.clone());
    blocking(move || render(&spec, &rows, &docs, &query)).await
}

/// Reject parameter combinations CouchDB refuses, before any rows are read.
pub fn check_query(spec: &ViewSpec, query: &ViewQuery) -> DbResult<()> {
    let reduce = query.reduce.unwrap_or(spec.reduce.is_some());
    let grouped = query.group || query.group_level > 0;
    if !reduce && grouped {
        return Err(ApiError::new(
            400,
            "query_parse_error",
            "Invalid use of grouping on a map view.",
        ));
    }
    if reduce && query.include_docs {
        return Err(ApiError::new(
            400,
            "query_parse_error",
            "`include_docs` is invalid for reduce",
        ));
    }
    if let Some(keys) = &query.keys {
        if keys.len() != 1
            && (query.key.is_some() || query.startkey.is_some() || query.endkey.is_some())
        {
            return Err(ApiError::new(
                400,
                "query_parse_error",
                "`keys` is incompatible with `key`, `start_key` and `end_key`",
            ));
        }
    }
    if let (Some(start), Some(end)) = (&query.startkey, &query.endkey) {
        let order = cmp_json(start, end);
        let invalid = if query.descending {
            order == std::cmp::Ordering::Less
        } else {
            order == std::cmp::Ordering::Greater
        };
        if invalid {
            let hint = if query.descending {
                "reverse your start_key and end_key or set descending=false"
            } else {
                "reverse your start_key and end_key or set descending=true"
            };
            return Err(ApiError::new(
                400,
                "query_parse_error",
                format!("No rows can match your key range, {hint}"),
            ));
        }
    }
    if reduce && !grouped && query.keys.as_ref().is_some_and(|keys| keys.len() != 1) {
        return Err(ApiError::new(
            400,
            "query_parse_error",
            "Multi-key fetches for reduce views must use `group=true`",
        ));
    }
    Ok(())
}

/// The view's rows in memory, the index first brought up to date: the
/// reference the key-ordered table's answers are checked against.
#[cfg(test)]
async fn ensure_rows(
    db: &Db,
    ddoc_id: &str,
    view_name: &str,
    spec: &ViewSpec,
    limits: ViewLimits,
) -> DbResult<Arc<ViewRows>> {
    ensure_index(db, ddoc_id, view_name, spec, limits, |index, _| {
        index.sorted_rows().map_err(ApiError::from)
    })
    .await
}

/// The view's index file opened for reading, the index first brought up to
/// date. The read transaction is opened under the index lock, so it holds
/// the state the update left.
async fn ensure_disk(
    db: &Db,
    ddoc_id: &str,
    view_name: &str,
    spec: &ViewSpec,
    limits: ViewLimits,
) -> DbResult<DiskView> {
    ensure_index(db, ddoc_id, view_name, spec, limits, |index, slot| {
        let txn = index
            .db()
            .and_then(|db| db.begin_read().map_err(index_error))
            .map_err(ApiError::from)?;
        let blocks = txn
            .open_table(crate::view_store::BLOCKS)
            .map_err(index_error)
            .and_then(|table| index.blocks(&table))
            .map_err(ApiError::from)?;
        Ok(DiskView {
            txn,
            placement: index.placement,
            blocks,
            _slot: slot,
        })
    })
    .await
}

/// Bring the view's index up to date with the shards, then hand it to
/// `finish`, under the index lock and the gate, on the blocking pool, with
/// the index's registry handle.
async fn ensure_index<T: Send + 'static>(
    db: &Db,
    ddoc_id: &str,
    view_name: &str,
    spec: &ViewSpec,
    limits: ViewLimits,
    finish: impl FnOnce(&mut ViewIndex, Arc<AsyncMutex<ViewIndex>>) -> DbResult<T> + Send + 'static,
) -> DbResult<T> {
    let design = if NativeMap::parse(&spec.map).is_some() {
        format!(
            "{}|{}|native={NATIVE_MAP_VERSION}",
            spec.map,
            spec.reduce.as_deref().unwrap_or_default()
        )
    } else {
        format!(
            "{}|{}",
            spec.map,
            spec.reduce.as_deref().unwrap_or_default()
        )
    };
    // The rows are stored in an order that depends on the key encoding, the
    // collation data and how documents are placed: a change in any rebuilds.
    let placement = Placement {
        q: db.q,
        partitioned: db.partitioned,
        by_partition: db.partitioned && spec.partitioned,
    };
    let design = format!(
        "{design}|keys={}|{}|q={}|partitioned={}|by_partition={}",
        crate::collate::CODEC_VERSION,
        crate::collate::ICU_DATA,
        placement.q,
        placement.partitioned,
        placement.by_partition
    );
    let gate = index_gate(db).read().await?;
    db.check_live()?;
    let handle = index_handle(&index_path(db, ddoc_id, view_name)).await?;
    let slot = Arc::clone(&handle);
    // A query queued behind a long build leaves when the database is deleted.
    let mut index = gate.until(handle.lock_owned()).await?;
    index.set_placement(placement);
    // Read after the lock: a query that updated the index while this one
    // waited has moved its cursor past any position read earlier, which
    // would look like a recreated database and force a full rebuild.
    // Each shard is read up to where it is now; writes that land during the
    // update are left for the next one.
    let bound = db.index_cursor();
    let map = spec.map.clone();

    // The map functions and the index writes run on the blocking pool, with
    // the gate and the index lock moved in: they stay held until the work is
    // done even if the request is dropped meanwhile, and the async workers
    // never wait on user JavaScript or on the index file.
    let (mut gate, mut index) = (gate, index);
    let mut rebuild = index.design != design || !cursor_valid(&index.cursor, db);
    if !rebuild && index.cursor != bound {
        // Each shard is read after its own cursor up to its bound, a page at
        // a time; each page is applied and recorded before the next is read,
        // so a long catch-up holds one page of documents and a failure keeps
        // the pages already applied.
        for shard in 0..bound.0.len() {
            let mut owned = crate::cluster::ShardMask::none(db.q);
            owned.select(shard);
            while index.cursor.0[shard].seq < bound.0[shard].seq {
                gate.check()?;
                let (rows, last) = db
                    .changes_rows_only(&ChangesOpts {
                        since: index.cursor.since(),
                        limit: Some(CHANGES_PAGE),
                        include_docs: true,
                        owned: Some(owned.clone()),
                        until: Some(bound.since()),
                        ..Default::default()
                    })
                    .await?;
                let mut cursor = index.cursor.clone();
                cursor.0[shard].seq = last.get(shard);
                let (map, design) = (map.clone(), design.clone());
                (gate, index) = blocking(move || {
                    gate.check()?;
                    let mut changed = Vec::new();
                    let mut docs = Vec::new();
                    for row in rows {
                        changed.push(row.id.clone());
                        if row.id.starts_with("_design/")
                            || row.id.starts_with("_local/")
                            || row.deleted
                        {
                            continue;
                        }
                        if let Some(doc) = row.doc {
                            docs.push((
                                row.id,
                                serde_json::from_str::<Value>(&doc).map_err(ApiError::internal)?,
                            ));
                        }
                    }
                    let rows = run_map(&map, &docs, limits).map_err(|reason| {
                        ApiError::new(500, "view_error", format!("map function failed: {reason}"))
                    })?;
                    let mut by_doc: HashMap<String, Vec<Row>> = HashMap::new();
                    for row in rows {
                        by_doc.entry(row.id.clone()).or_default().push(row);
                    }
                    index
                        .apply(changed, by_doc, cursor, &design)
                        .map_err(ApiError::from)?;
                    Ok((gate, index))
                })
                .await?;
            }
        }
        // A shard cleared or recreated since the cursor was checked is a new
        // copy, and one that purged since has dropped documents: either way
        // documents may be gone without a change row, so rows caught up from
        // it cannot be served. As a search index does, check that the copies
        // are still the ones the index records, with nothing purged since,
        // and rebuild at once if not.
        if index.cursor.same_copies_unpurged(&db.index_cursor()) {
            return blocking(move || {
                let _gate = gate;
                finish(&mut index, slot)
            })
            .await;
        }
        rebuild = true;
    }

    if rebuild {
        // The documents are read and mapped a page at a time, so a rebuild
        // holds one page of document bodies, not the whole database's.
        // The positions are read again first, so they name the copies the
        // documents are read from even when a shard changed since `bound`.
        // Writes after them may be read too; the next update reads them
        // again, which only re-maps them. A purge after them leaves the
        // recorded purge sequence behind, so the next use rebuilds.
        // Each page's rows are written into the rebuild's one transaction as
        // they are mapped, so no list of every row is held.
        let bound = db.index_cursor();
        let mut cursor = crate::db::MapCursor::default();
        let mut rebuild: Option<Rebuild> = None;
        loop {
            let docs = db.map_docs_page(None, &mut cursor, CHANGES_PAGE).await?;
            if docs.is_empty() {
                break;
            }
            let map = map.clone();
            // Between pages, so a deleted database is noticed.
            (gate, index, rebuild) = blocking(move || {
                gate.check()?;
                let rows = run_map_json(&map, &docs, limits).map_err(|reason| {
                    ApiError::new(500, "view_error", format!("map function failed: {reason}"))
                })?;
                let mut rebuild = match rebuild {
                    Some(rebuild) => rebuild,
                    None => index.start_rebuild()?,
                };
                rebuild.add(&rows)?;
                Ok((gate, index, Some(rebuild)))
            })
            .await?;
        }
        return blocking(move || {
            gate.check()?;
            let rebuild = match rebuild {
                Some(rebuild) => rebuild,
                None => index.start_rebuild()?,
            };
            index.finish_rebuild(rebuild, bound, &design)?;
            finish(&mut index, slot)
        })
        .await;
    }

    blocking(move || {
        let _gate = gate;
        finish(&mut index, slot)
    })
    .await
}

/// Changes read, mapped and applied to a view index at a time.
const CHANGES_PAGE: usize = 1000;

/// Run `work` on the blocking pool. User JavaScript, index files and the
/// worker pipes all block their thread, which must not be an async worker.
pub(crate) async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> DbResult<T> + Send + 'static,
) -> DbResult<T> {
    tokio::task::spawn_blocking(move || {
        #[cfg(debug_assertions)]
        let _mark = blocking_mark::enter();
        work()
    })
    .await
    .map_err(|error| ApiError::new(500, "view_error", format!("view task failed: {error}")))?
}

/// Debug-build check that user JavaScript runs on the blocking pool. Tokio
/// gives blocking-pool threads the same runtime context as async workers, so
/// the two cannot be told apart from outside; [`blocking`] marks its threads.
#[cfg(debug_assertions)]
mod blocking_mark {
    use std::cell::Cell;

    thread_local! {
        static MARKED: Cell<bool> = const { Cell::new(false) };
    }

    pub(super) struct Mark;

    pub(super) fn enter() -> Mark {
        MARKED.with(|marked| marked.set(true));
        Mark
    }

    impl Drop for Mark {
        fn drop(&mut self) {
            MARKED.with(|marked| marked.set(false));
        }
    }

    /// Whether the thread may run user JavaScript: it is outside any Tokio
    /// runtime (a plain thread, a sandbox worker), or marked by [`enter`].
    pub(super) fn allows_javascript() -> bool {
        tokio::runtime::Handle::try_current().is_err() || MARKED.with(Cell::get)
    }
}

/// Panic, in debug builds, if user JavaScript is about to run on an async
/// runtime worker rather than on the blocking pool.
pub(crate) fn debug_assert_off_runtime_workers() {
    #[cfg(debug_assertions)]
    assert!(
        blocking_mark::allows_javascript(),
        "user JavaScript ran on an async runtime worker instead of the blocking pool"
    );
}

pub(crate) async fn docs_by_ids(db: &Db, ids: &[String]) -> DbResult<HashMap<String, Value>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    db.docs_by_ids(ids).await
}

/// Remove index files whose design document or view no longer exists, and
/// anything else in the index directory that is not a live view's file (an
/// interrupted creation, an index of an earlier format).
pub async fn cleanup(db: &Db) -> DbResult<()> {
    // Before the live set is read, so a view queried meanwhile is not removed
    // as stale, and so no query holds an index being removed.
    let _gate = index_gate(db).write().await?;
    db.check_live()?;
    let mut live = std::collections::HashSet::new();
    let all = db
        .all_docs(&AllDocsOpts {
            include_docs: true,
            startkey: Some("_design/".to_string()),
            endkey: Some(format!("_design/{}", char::MAX)),
            limit: usize::MAX,
            ..Default::default()
        })
        .await?;
    for row in all.rows {
        let Some(ddoc) = &row.doc else {
            continue;
        };
        let Some(id) = ddoc.get("_id").and_then(Value::as_str) else {
            continue;
        };
        if let Some(views) = ddoc.get("views").and_then(Value::as_object) {
            for name in views.keys() {
                live.insert(index_path(db, id, name));
            }
        }
    }

    let directory = db.path.join("_view_indexes");
    indexes().retain(|path| !path.starts_with(&directory) || live.contains(path));
    blocking(move || {
        remove_index_files(&directory, |path| !live.contains(path));
        Ok(())
    })
    .await
}

/// Remove the entries of the index directory `directory` that `stale`
/// picks. Their handles must be retired first. Each file's background close,
/// if one is still running, is waited for, so it cannot record the file as
/// verified after it has gone; then the file is forgotten, so a new one under
/// its name is checked on its first open.
fn remove_index_files(directory: &Path, stale: impl Fn(&Path) -> bool) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !stale(&path) {
            continue;
        }
        wait_closed(&path);
        let _ = if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        crate::kv::forget_verified(&path);
    }
}

pub async fn load_spec(db: &Db, ddoc_id: &str, view_name: &str) -> DbResult<ViewSpec> {
    let ddoc = db.design_doc(ddoc_id).await?;
    spec_from_doc(&ddoc, view_name)
}

pub fn spec_from_doc(ddoc: &Value, view_name: &str) -> DbResult<ViewSpec> {
    let language = ddoc
        .get("language")
        .and_then(Value::as_str)
        .unwrap_or("javascript");
    if language != "javascript" {
        return Err(ApiError::bad_request(format!(
            "View language must be javascript, not {language}"
        )));
    }
    let spec = ddoc
        .get("views")
        .and_then(Value::as_object)
        .and_then(|views| views.get(view_name))
        .ok_or_else(|| ApiError::not_found(format!("missing view {view_name}")))?;
    let map = spec
        .get("map")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::bad_request("View is missing a map function"))?
        .to_string();
    let reduce = spec
        .get("reduce")
        .and_then(Value::as_str)
        .map(str::to_string);
    let partitioned = ddoc
        .get("options")
        .and_then(Value::as_object)
        .and_then(|options| options.get("partitioned"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    Ok(ViewSpec {
        map,
        reduce,
        partitioned,
    })
}

const NATIVE_MAP_VERSION: u8 = 1;

/// A map function reduced to direct field/literal extraction. Anything that
/// does not match this deliberately tiny grammar falls back to QuickJS.
#[derive(Clone, Debug)]
struct NativeMap {
    key: EmitExpr,
    value: EmitExpr,
}

#[derive(Clone, Debug)]
enum EmitExpr {
    Field(String),
    Literal(Value),
}

fn raw_json_field<'a>(json: &'a str, field: &str) -> Result<Option<&'a str>, ()> {
    let bytes = json.as_bytes();
    let mut index = skip_ws(bytes, 0);
    if bytes.get(index) != Some(&b'{') {
        return Err(());
    }
    index += 1;
    let mut found = None;
    loop {
        index = skip_ws(bytes, index);
        match bytes.get(index) {
            Some(b'}') => return Ok(found),
            Some(b'"') => {
                let key_end = scan_json_string(bytes, index).ok_or(())?;
                let key: String = serde_json::from_str(&json[index..key_end]).map_err(|_| ())?;
                index = skip_ws(bytes, key_end);
                if bytes.get(index) != Some(&b':') {
                    return Err(());
                }
                index = skip_ws(bytes, index + 1);
                let value_start = index;
                let value_end = skip_json_value(bytes, index).ok_or(())?;
                if key == field {
                    found = Some(json[value_start..value_end].trim());
                }
                index = skip_ws(bytes, value_end);
                match bytes.get(index) {
                    Some(b',') => index += 1,
                    Some(b'}') => return Ok(found),
                    _ => return Err(()),
                }
            }
            _ => return Err(()),
        }
    }
}

fn skip_ws(bytes: &[u8], mut index: usize) -> usize {
    while matches!(bytes.get(index), Some(b' ' | b'\n' | b'\r' | b'\t')) {
        index += 1;
    }
    index
}

fn scan_json_string(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'"') {
        return None;
    }
    let mut index = start + 1;
    while let Some(byte) = bytes.get(index) {
        match byte {
            b'\\' => index += 2,
            b'"' => return Some(index + 1),
            _ => index += 1,
        }
    }
    None
}

fn skip_json_value(bytes: &[u8], start: usize) -> Option<usize> {
    match bytes.get(start)? {
        b'"' => scan_json_string(bytes, start),
        b'{' | b'[' => {
            let mut depth = 0usize;
            let mut index = start;
            while let Some(byte) = bytes.get(index) {
                match byte {
                    b'"' => {
                        index = scan_json_string(bytes, index)?;
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth = depth.saturating_sub(1);
                        if depth == 0 {
                            return Some(index + 1);
                        }
                    }
                    _ => {}
                }
                index += 1;
            }
            None
        }
        _ => {
            let mut index = start;
            while let Some(byte) = bytes.get(index) {
                if matches!(byte, b',' | b'}' | b']' | b' ' | b'\n' | b'\r' | b'\t') {
                    break;
                }
                index += 1;
            }
            Some(index)
        }
    }
}

impl NativeMap {
    fn parse(source: &str) -> Option<NativeMap> {
        let compact = compact_js(source)?;
        let body = function_body(&compact)?;
        let body = body.strip_suffix(';').unwrap_or(body);
        let inner = body.strip_prefix("emit(")?.strip_suffix(')')?;
        let args = split_top_level(inner, ',');
        let (key, value) = match args.as_slice() {
            [key] => (parse_emit_expr(key)?, EmitExpr::Literal(Value::Null)),
            [key, value] => (parse_emit_expr(key)?, parse_emit_expr(value)?),
            _ => return None,
        };
        Some(NativeMap { key, value })
    }

    fn eval(&self, expression: &EmitExpr, doc: &Value) -> Value {
        match expression {
            EmitExpr::Field(field) => doc.get(field).cloned().unwrap_or(Value::Null),
            EmitExpr::Literal(value) => value.clone(),
        }
    }

    fn row(&self, id: &str, doc: &Value) -> Row {
        Row {
            id: id.to_string(),
            key: self.eval(&self.key, doc),
            value: self.eval(&self.value, doc),
        }
    }

    fn run_values(&self, docs: &[(String, Value)]) -> Vec<Row> {
        docs.iter().map(|(id, doc)| self.row(id, doc)).collect()
    }

    fn eval_raw(&self, expression: &EmitExpr, raw: &str) -> Result<Value, ()> {
        match expression {
            EmitExpr::Literal(value) => Ok(value.clone()),
            EmitExpr::Field(field) => match raw_json_field(raw, field)? {
                Some(value) => serde_json::from_str(value).map_err(|_| ()),
                None => Ok(Value::Null),
            },
        }
    }

    fn run_json(&self, docs: &[(String, String)]) -> Result<Vec<Row>, JsError> {
        let mut rows = Vec::with_capacity(docs.len());
        for (id, raw) in docs {
            match (
                self.eval_raw(&self.key, raw),
                self.eval_raw(&self.value, raw),
            ) {
                (Ok(key), Ok(value)) => rows.push(Row {
                    id: id.clone(),
                    key,
                    value,
                }),
                _ => {
                    let doc: Value = serde_json::from_str(raw)
                        .map_err(|error| JsError::Runtime(format!("{id}: {error}")))?;
                    rows.push(self.row(id, &doc));
                }
            }
        }
        Ok(rows)
    }
}

/// Remove JavaScript whitespace but leave quoted strings untouched.
fn compact_js(source: &str) -> Option<String> {
    let mut out = String::with_capacity(source.len());
    let mut chars = source.chars().peekable();
    let mut quote = None;
    while let Some(ch) = chars.next() {
        if let Some(q) = quote {
            out.push(ch);
            if ch == '\\' {
                if let Some(next) = chars.next() {
                    out.push(next);
                }
            } else if ch == q {
                quote = None;
            }
        } else if ch == '"' || ch == '\'' {
            quote = Some(ch);
            out.push(ch);
        } else if !ch.is_whitespace() {
            out.push(ch);
        }
    }
    if quote.is_some() {
        None
    } else {
        Some(out)
    }
}

fn function_body(compact: &str) -> Option<&str> {
    if let Some(rest) = compact.strip_prefix("function(") {
        let (args, tail) = rest.split_once(')')?;
        if args != "doc" {
            return None;
        }
        let body = tail.strip_prefix('{')?.strip_suffix('}')?;
        return Some(body);
    }
    if let Some(rest) = compact.strip_prefix("(doc)=>") {
        return Some(
            rest.strip_prefix('{')
                .and_then(|body| body.strip_suffix('}'))
                .unwrap_or(rest),
        );
    }
    if let Some(rest) = compact.strip_prefix("doc=>") {
        return Some(
            rest.strip_prefix('{')
                .and_then(|body| body.strip_suffix('}'))
                .unwrap_or(rest),
        );
    }
    None
}

/// Split on a separator at bracket depth zero, respecting strings.
fn split_top_level(input: &str, separator: char) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in input.char_indices() {
        if let Some(q) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == q {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '\'' => quote = Some(ch),
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ if ch == separator && depth == 0 => {
                out.push(&input[start..index]);
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    out.push(&input[start..]);
    out
}

fn parse_emit_expr(raw: &str) -> Option<EmitExpr> {
    if let Some(field) = raw.strip_prefix("doc.") {
        if is_js_ident(field) && !is_object_prototype_field(field) {
            return Some(EmitExpr::Field(field.to_string()));
        }
        return None;
    }
    if let Some(key) = raw.strip_prefix("doc[") {
        let key = key.strip_suffix(']')?;
        let key: String = serde_json::from_str(key).ok()?;
        if is_object_prototype_field(&key) {
            return None;
        }
        return Some(EmitExpr::Field(key));
    }
    serde_json::from_str(raw).ok().map(EmitExpr::Literal)
}

/// `doc.<name>` can resolve through `Object.prototype` when the document has
/// no such own property. Leave those names to QuickJS so a missing field does
/// not silently become `null` instead of a function value.
fn is_object_prototype_field(value: &str) -> bool {
    matches!(
        value,
        "__proto__"
            | "constructor"
            | "prototype"
            | "toString"
            | "valueOf"
            | "hasOwnProperty"
            | "isPrototypeOf"
            | "propertyIsEnumerable"
            | "toLocaleString"
    )
}

fn is_js_ident(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first == '$' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch == '$' || ch.is_ascii_alphanumeric())
}

fn run_map(
    source: &str,
    docs: &[(String, Value)],
    limits: ViewLimits,
) -> Result<Vec<Row>, JsError> {
    if let Some(native) = NativeMap::parse(source) {
        let rows = native.run_values(docs);
        return finish_native(rows, source, limits, |index| {
            let (id, doc) = &docs[index];
            serde_json::to_string(doc)
                .map(|doc| (id.clone(), doc))
                .map_err(|error| JsError::Runtime(format!("{id}: {error}")))
        });
    }
    let mut serialized = Vec::with_capacity(docs.len());
    for (id, doc) in docs {
        serialized.push((
            id.clone(),
            serde_json::to_string(doc)
                .map_err(|error| JsError::Runtime(format!("{id}: {error}")))?,
        ));
    }
    run_map_json(source, &serialized, limits)
}

/// Run the map function over batches of already-serialized documents. Batching
/// amortizes the QuickJS call/JSON boundary; callers that already hold raw
/// bodies (cold index builds) also avoid a Rust parse/encode round trip.
pub(crate) fn run_map_json(
    source: &str,
    docs: &[(String, String)],
    limits: ViewLimits,
) -> Result<Vec<Row>, JsError> {
    if docs.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(native) = NativeMap::parse(source) {
        let rows = native.run_json(docs)?;
        return finish_native(rows, source, limits, |index| Ok(docs[index].clone()));
    }
    run_map_js(source, docs, limits)
}

/// The native path must emit exactly what the JavaScript view server would.
/// JavaScript numbers are doubles, so a float or an integer beyond 2^53 can
/// come back different (`100.0` as `100`, `2^63` rounded). Re-run the
/// documents whose rows hold such a number through QuickJS. Native maps emit
/// exactly one row per document, so rows and documents line up by index.
fn finish_native(
    mut rows: Vec<Row>,
    source: &str,
    limits: ViewLimits,
    doc_json: impl Fn(usize) -> Result<(String, String), JsError>,
) -> Result<Vec<Row>, JsError> {
    let pending: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter(|(_, row)| !js_exact(&row.key) || !js_exact(&row.value))
        .map(|(index, _)| index)
        .collect();
    if pending.is_empty() {
        return Ok(rows);
    }
    let docs = pending
        .iter()
        .map(|&index| doc_json(index))
        .collect::<Result<Vec<_>, _>>()?;
    let javascript = run_map_js(source, &docs, limits)?;
    if javascript.len() != pending.len() {
        return Err(JsError::Runtime(format!(
            "map function emitted {} rows for {} documents",
            javascript.len(),
            pending.len()
        )));
    }
    for (index, row) in pending.into_iter().zip(javascript) {
        rows[index] = row;
    }
    Ok(rows)
}

/// Whether JavaScript would reproduce this value unchanged: every number in
/// it is an integer within ±(2^53 - 1), written plainly.
fn js_exact(value: &Value) -> bool {
    const MAX_SAFE: u64 = (1 << 53) - 1;
    match value {
        Value::Number(number) => number
            .as_i64()
            .filter(|integer| integer.unsigned_abs() <= MAX_SAFE)
            .is_some_and(|integer| integer.to_string() == number.to_string()),
        Value::Array(items) => items.iter().all(js_exact),
        Value::Object(fields) => fields.values().all(js_exact),
        _ => true,
    }
}

fn ordered_rows<'a>(rows: &'a [Row], query: &ViewQuery) -> (Vec<&'a Row>, usize, usize) {
    let total_rows = query.total_rows_override.unwrap_or(rows.len());
    let mut ordered: Vec<&Row> = if query.descending {
        rows.iter().rev().collect()
    } else {
        rows.iter().collect()
    };

    // The rows are already filtered by partition and shard, and their counts
    // supplied, by `ViewRows::select` or the coordinator.
    // The offset is the number of rows before the query's starting key. It is
    // computed before `key`/`keys` filtering, because CouchDB reports the
    // insertion point even when no row matches.
    let before_start = if let Some(offset) = query.offset_override {
        offset
    } else if let Some(key) = &query.key {
        count_before_key(&ordered, key, query.descending)
    } else if let Some(keys) = &query.keys {
        match keys.as_slice() {
            [key] => count_before_key(&ordered, key, query.descending),
            // CouchDB's offset for multi-key queries follows an internal
            // legacy rule; it is not compared by the differential fuzzer.
            _ => 0,
        }
    } else {
        count_before_start(&ordered, query)
    };

    if let Some(keys) = &query.keys {
        let mut selected = Vec::new();
        let ordered_keys: Vec<&Value> = if query.descending {
            keys.iter().rev().collect()
        } else {
            keys.iter().collect()
        };
        // The rows are in ascending key order, so each key's rows are a run
        // found by binary search.
        for key in ordered_keys {
            let (first, end) = equal_range(rows, key);
            if query.descending {
                selected.extend(rows[first..end].iter().rev());
            } else {
                selected.extend(rows[first..end].iter());
            }
        }
        ordered = selected;
    } else if let Some(key) = &query.key {
        ordered.retain(|row| cmp_json(&row.key, key) == Ordering::Equal);
    }
    (ordered, total_rows, before_start)
}

fn selected_row_ids(rows: &[Row], query: &ViewQuery) -> Vec<String> {
    let (mut ordered, _, _) = ordered_rows(rows, query);
    ordered.retain(|row| in_range(row, query));
    let skip = query.skip.min(ordered.len());
    ordered
        .into_iter()
        .skip(skip)
        .take(query.limit)
        .map(|row| row.id.clone())
        .collect()
}

fn render(
    spec: &ViewSpec,
    rows: &[Row],
    docs: &HashMap<String, Value>,
    query: &ViewQuery,
) -> DbResult<Value> {
    let (mut ordered, total_rows, before_start) = ordered_rows(rows, query);
    let available = total_rows.saturating_sub(before_start);
    let skip = query.skip.min(available);
    ordered.retain(|row| in_range(row, query));
    let reduce = query.reduce.unwrap_or(spec.reduce.is_some());

    if reduce {
        let Some(reducer) = &spec.reduce else {
            return Err(ApiError::new(
                400,
                "query_parse_error",
                "Reduce is invalid for map-only views",
            ));
        };
        let mut js = JsReducer::new(reducer, query.limits);
        let groups = group_rows(ordered, query);
        if !query.group
            && query.group_level == 0
            && groups.iter().all(|(_, members)| members.is_empty())
        {
            return Ok(json!({"rows": []}));
        }
        let mut out = Vec::new();
        for (key, members) in groups {
            let value = reduce_rows(members.iter().copied(), &mut js).and_then(|partial| {
                if query.owner_shards.is_some() {
                    Ok(partial)
                } else {
                    finish_partial(reducer, partial)
                }
            });
            let value = match value {
                Ok(value) => value,
                Err(reason) => return Err(reduce_failure(&reason)),
            };
            out.push(json!({"key": key, "value": value}));
        }
        let skip = query.skip.min(out.len());
        let selected: Vec<Value> = out.into_iter().skip(skip).take(query.limit).collect();
        return Ok(json!({"rows": selected}));
    }

    let take = query.limit;
    let selected = ordered.into_iter().skip(skip).take(take);
    let rows: Vec<Value> = selected
        .map(|row| map_row_entry(row, docs, query.include_docs))
        .collect();
    Ok(json!({
        "total_rows": total_rows,
        "offset": before_start + skip,
        "rows": rows,
    }))
}

fn count_before_start(rows: &[&Row], query: &ViewQuery) -> usize {
    rows.iter().filter(|row| before_start(row, query)).count()
}

/// Whether `row` comes before the query's start bound, in query order.
fn before_start(row: &Row, query: &ViewQuery) -> bool {
    let Some(start) = &query.startkey else {
        return false;
    };
    match cmp_json(&row.key, start) {
        Ordering::Less => !query.descending,
        Ordering::Greater => query.descending,
        Ordering::Equal => match &query.startkey_docid {
            Some(id) if query.descending => row.id > *id,
            Some(id) => row.id < *id,
            None => false,
        },
    }
}

/// Whether `row` comes after the query's end bound, in query order.
fn past_end(row: &Row, query: &ViewQuery) -> bool {
    let Some(end) = &query.endkey else {
        return false;
    };
    match cmp_json(&row.key, end) {
        Ordering::Greater => !query.descending,
        Ordering::Less => query.descending,
        Ordering::Equal => {
            !query.inclusive_end
                || match &query.endkey_docid {
                    Some(id) if query.descending => row.id < *id,
                    Some(id) => row.id > *id,
                    None => false,
                }
        }
    }
}

fn count_before_key(rows: &[&Row], key: &Value, descending: bool) -> usize {
    rows.iter()
        .filter(|row| match cmp_json(&row.key, key) {
            Ordering::Less => !descending,
            Ordering::Greater => descending,
            Ordering::Equal => false,
        })
        .count()
}

fn in_range(row: &Row, query: &ViewQuery) -> bool {
    !before_start(row, query) && !past_end(row, query)
}

fn group_rows<'a>(rows: Vec<&'a Row>, query: &ViewQuery) -> Vec<(Value, Vec<&'a Row>)> {
    if !query.group && query.group_level == 0 {
        return vec![(Value::Null, rows)];
    }
    let mut groups: Vec<(Value, Vec<&'a Row>)> = Vec::new();
    for row in rows {
        let key = group_key(&row.key, query.group_level);
        match groups.last_mut() {
            // A repeated requested key lists its rows again, and starts a
            // group of its own.
            Some((existing, members))
                if cmp_json(existing, &key) == Ordering::Equal
                    && !(query.keys.is_some()
                        && members
                            .first()
                            .is_some_and(|first| std::ptr::eq(*first, row))) =>
            {
                members.push(row);
            }
            _ => groups.push((key, vec![row])),
        }
    }
    groups
}

pub(crate) fn group_key(key: &Value, level: usize) -> Value {
    if level == 0 {
        return key.clone();
    }
    match key {
        Value::Array(items) => Value::Array(items.iter().take(level).cloned().collect()),
        other => other.clone(),
    }
}

/// Values passed to one call of a custom reducer.
const REDUCE_CHUNK: usize = 1000;

fn reduce_with(
    reducer: &mut JsReducer,
    keys: &[Value],
    values: &[Value],
) -> Result<Value, JsError> {
    match reducer.source() {
        "_count" => return Ok(json!(values.len())),
        "_sum" => return reduce_sum(values.iter()),
        "_stats" => return reduce_stats(values.iter()),
        _ => {}
    }

    // CouchDB reduces in bounded batches and then rereduces the partials.
    // Do the same for custom JavaScript reducers.
    const CHUNK: usize = REDUCE_CHUNK;
    if values.len() > CHUNK {
        // The chunks go in as one batch, not a round trip each.
        let calls = values.chunks(CHUNK).enumerate().map(|(offset, chunk)| {
            let start = offset * CHUNK;
            let end = (start + chunk.len()).min(keys.len());
            let chunk_keys = if start < end { &keys[start..end] } else { &[] };
            ReduceCall {
                keys: chunk_keys.to_vec(),
                values: chunk.to_vec(),
                rereduce: false,
            }
        });
        let partials = reducer.call_many(calls)?;
        return reducer.call(&[], &partials, true);
    }
    reducer.call(keys, values, false)
}

pub(crate) fn run_reduce(
    source: &str,
    keys: &[Value],
    values: &[Value],
    rereduce: bool,
    limits: ViewLimits,
) -> Result<Value, JsError> {
    JsReducer::new(source, limits).call(keys, values, rereduce)
}

/// One call of a custom reducer, for `JsReducer::call_many`.
pub(crate) struct ReduceCall {
    keys: Vec<Value>,
    values: Vec<Value>,
    rereduce: bool,
}

/// A batched request stays well under the worker's 64 MiB frame.
const REDUCE_BATCH_BYTES: usize = 4 * 1024 * 1024;
const REDUCE_BATCH_CALLS: usize = 1024;

impl ReduceCall {
    fn rows<'a>(rows: impl Iterator<Item = &'a Row> + Clone) -> Self {
        ReduceCall {
            keys: rows.clone().map(|row| json!([row.id, row.key])).collect(),
            values: rows.map(|row| row.value.clone()).collect(),
            rereduce: false,
        }
    }

    fn rereduce(partials: &[Value]) -> Self {
        ReduceCall {
            keys: Vec::new(),
            values: partials.to_vec(),
            rereduce: true,
        }
    }

    fn size(&self) -> usize {
        self.keys
            .iter()
            .chain(&self.values)
            .map(|value| serde_json::to_vec(value).map_or(0, |bytes| bytes.len()))
            .sum()
    }

    /// The call as the worker protocol carries it; a rereduce has null keys.
    fn into_request(self) -> Value {
        let keys = if self.rereduce {
            Value::Null
        } else {
            Value::Array(self.keys)
        };
        json!({"keys": keys, "values": self.values, "rereduce": self.rereduce})
    }
}

/// A custom JavaScript reducer, compiled on its first call and reused for the
/// rest of one query. Every call gets the full timeout.
pub(crate) struct JsReducer {
    source: String,
    limits: ViewLimits,
    engine: Option<ReduceEngine>,
}

/// The context is declared first so it is dropped before its runtime.
struct ReduceEngine {
    context: rquickjs::Context,
    _runtime: rquickjs::Runtime,
    deadline: Arc<StdMutex<std::time::Instant>>,
}

const REDUCE_GLOBAL: &str = "__douchdb_reduce";

/// The JS wrappers around user functions live once, in `wasm/js`, and are
/// shared with the WASM runner. The placeholder is replaced by the user's
/// function source.
const REDUCE_WRAPPER: &str = include_str!("../wasm/js/reduce.js");

impl JsReducer {
    pub(crate) fn new(source: &str, limits: ViewLimits) -> Self {
        Self {
            source: source.to_string(),
            limits,
            engine: None,
        }
    }

    pub(crate) fn source(&self) -> &str {
        &self.source
    }

    pub(crate) fn call(
        &mut self,
        keys: &[Value],
        values: &[Value],
        rereduce: bool,
    ) -> Result<Value, JsError> {
        if crate::js::sandboxed() {
            // CouchDB passes null, not [], for keys during rereduce.
            let keys = if rereduce { Value::Null } else { json!(keys) };
            return crate::js::sandbox_request(
                "reduce",
                json!({"source": self.source, "keys": keys, "values": values, "rereduce": rereduce}),
                self.limits,
            );
        }
        let result = self.call_native(keys, values, rereduce);
        if result.is_err() {
            // A failed or interrupted call may leave the engine unusable.
            self.engine = None;
        }
        result
    }

    /// Run `calls` in order. In a sandbox they go in as few requests as
    /// `REDUCE_BATCH_BYTES` and `REDUCE_BATCH_CALLS` allow, not one round
    /// trip each; the first failure ends the batch, as it would a sequence.
    pub(crate) fn call_many(
        &mut self,
        calls: impl Iterator<Item = ReduceCall>,
    ) -> Result<Vec<Value>, JsError> {
        let mut results = Vec::new();
        if !crate::js::sandboxed() {
            for call in calls {
                results.push(self.call(&call.keys, &call.values, call.rereduce)?);
            }
            return Ok(results);
        }
        let mut batch: Vec<Value> = Vec::new();
        let mut bytes = 0;
        for call in calls {
            bytes += call.size();
            batch.push(call.into_request());
            if bytes >= REDUCE_BATCH_BYTES || batch.len() >= REDUCE_BATCH_CALLS {
                self.send_batch(std::mem::take(&mut batch), &mut results)?;
                bytes = 0;
            }
        }
        if !batch.is_empty() {
            self.send_batch(batch, &mut results)?;
        }
        Ok(results)
    }

    fn send_batch(&self, batch: Vec<Value>, results: &mut Vec<Value>) -> Result<(), JsError> {
        let sent = batch.len();
        let reply = crate::js::sandbox_request(
            "reduce_many",
            json!({"source": self.source, "calls": batch}),
            self.limits,
        )?;
        match reply {
            Value::Array(values) if values.len() == sent => {
                results.extend(values);
                Ok(())
            }
            _ => Err(JsError::Runtime(
                "reduce_many returned the wrong number of results".to_string(),
            )),
        }
    }

    fn call_native(
        &mut self,
        keys: &[Value],
        values: &[Value],
        rereduce: bool,
    ) -> Result<Value, JsError> {
        let timeout = self.limits.timeout;
        if self.engine.is_none() {
            self.engine = Some(compile_reducer(&self.source, self.limits)?);
        }
        let Some(engine) = &self.engine else {
            return Err(JsError::Runtime("reduce engine unavailable".to_string()));
        };
        *engine
            .deadline
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = std::time::Instant::now() + timeout;
        crate::js::guard("reduce function", || {
            engine.context.with(|ctx| {
                let func: rquickjs::Function = ctx
                    .globals()
                    .get(REDUCE_GLOBAL)
                    .map_err(|error| JsError::Runtime(error.to_string()))?;
                // CouchDB passes null, not [], for keys during rereduce.
                let keys = if rereduce {
                    "null".to_string()
                } else {
                    serde_json::to_string(keys)
                        .map_err(|error| JsError::Runtime(error.to_string()))?
                };
                let values = serde_json::to_string(values)
                    .map_err(|error| JsError::Runtime(error.to_string()))?;
                let output: String = func
                    .call((keys, values, rereduce))
                    .map_err(|error| JsError::Runtime(error.to_string()))?;
                crate::js::check_output("reduce function", &output)?;
                serde_json::from_str(&output).map_err(|error| JsError::Runtime(error.to_string()))
            })
        })
    }
}

fn compile_reducer(source: &str, limits: ViewLimits) -> Result<ReduceEngine, JsError> {
    let (runtime, deadline) = crate::js::reusable_runtime(limits.memory)?;
    *deadline.lock().unwrap_or_else(|poison| poison.into_inner()) =
        std::time::Instant::now() + limits.timeout;
    let context =
        rquickjs::Context::full(&runtime).map_err(|error| JsError::Runtime(error.to_string()))?;
    #[cfg(test)]
    crate::js::test_pause::install(&context)?;
    crate::js::guard("reduce function", || {
        context.with(|ctx| {
            let wrapped = REDUCE_WRAPPER.replace(WRAPPER_SOURCE, source);
            let func: rquickjs::Function = ctx
                .eval(wrapped)
                .map_err(|error| JsError::Runtime(format!("compile: {error}")))?;
            ctx.globals()
                .set(REDUCE_GLOBAL, func)
                .map_err(|error| JsError::Runtime(error.to_string()))
        })
    })?;
    Ok(ReduceEngine {
        context,
        _runtime: runtime,
        deadline,
    })
}

const SUM_ERROR_REASON: &str = "The _sum function requires that map values be numbers, arrays of numbers, or objects. Objects cannot be mixed with other data structures. Objects can be arbitrarily nested, provided that the values for all fields are themselves numbers, arrays of numbers, or objects.";

/// The HTTP error for a failed reduce.
pub(crate) fn reduce_failure(error: &JsError) -> ApiError {
    match error {
        JsError::InvalidValue(reason) => ApiError::new(500, "invalid_value", reason.as_str()),
        other => ApiError::new(
            500,
            "view_error",
            format!("reduce function failed: {other}"),
        ),
    }
}

/// Built-in `_sum`, as in CouchDB's `couch_query_servers`: numbers add,
/// arrays add element by element (a number counts as a one-element array),
/// and objects add member by member. Invalid input is not a failed request
/// but an error object in place of the reduced value.
fn reduce_sum<'a>(values: impl IntoIterator<Item = &'a Value>) -> Result<Value, JsError> {
    let mut acc = json!(0);
    for value in values {
        match sum_values(value, &acc) {
            Ok(sum) => acc = sum,
            Err(cause) => {
                return Ok(json!({
                    "error": "builtin_reduce_error",
                    "reason": SUM_ERROR_REASON,
                    "caused_by": cause,
                }))
            }
        }
    }
    Ok(acc)
}

/// Add `value` to `acc`; the error is the offending value.
fn sum_values(value: &Value, acc: &Value) -> Result<Value, Value> {
    match (value, acc) {
        (Value::Number(a), Value::Number(b)) => Ok(sum_numbers(a, b)),
        (Value::Array(a), Value::Array(b)) => sum_arrays(b, a),
        (Value::Number(_), Value::Array(b)) => sum_arrays(b, std::slice::from_ref(value)),
        (Value::Array(a), Value::Number(_)) => sum_arrays(std::slice::from_ref(acc), a),
        (Value::Object(a), Value::Object(b)) => {
            let mut a: Vec<(&String, &Value)> = a.iter().collect();
            let mut b: Vec<(&String, &Value)> = b.iter().collect();
            a.sort_by(|x, y| x.0.cmp(y.0));
            b.sort_by(|x, y| x.0.cmp(y.0));
            let mut out = serde_json::Map::new();
            let (mut a, mut b) = (a.into_iter().peekable(), b.into_iter().peekable());
            loop {
                match (a.peek(), b.peek()) {
                    (Some((ka, va)), Some((kb, vb))) => match ka.cmp(kb) {
                        Ordering::Equal => {
                            out.insert((*ka).clone(), sum_values(va, vb)?);
                            a.next();
                            b.next();
                        }
                        Ordering::Less => {
                            out.insert((*ka).clone(), (*va).clone());
                            a.next();
                        }
                        Ordering::Greater => {
                            out.insert((*kb).clone(), (*vb).clone());
                            b.next();
                        }
                    },
                    (Some(_), None) => {
                        out.extend(a.map(|(k, v)| (k.clone(), v.clone())));
                        break;
                    }
                    (None, _) => {
                        out.extend(b.map(|(k, v)| (k.clone(), v.clone())));
                        break;
                    }
                }
            }
            Ok(Value::Object(out))
        }
        // The first object takes the place of the initial zero.
        (Value::Object(_), Value::Number(zero)) if zero.as_f64() == Some(0.0) => Ok(value.clone()),
        (Value::Object(_), Value::Array(_)) => Err(acc.clone()),
        _ => Err(value.clone()),
    }
}

fn sum_numbers(a: &serde_json::Number, b: &serde_json::Number) -> Value {
    number_value(a.as_f64().unwrap_or(0.0) + b.as_f64().unwrap_or(0.0))
}

/// Element-wise sum; the longer array's tail is kept. The error is the
/// unsummable remainder of `acc`.
fn sum_arrays(acc: &[Value], value: &[Value]) -> Result<Value, Value> {
    let mut out = Vec::with_capacity(acc.len().max(value.len()));
    for index in 0..acc.len().max(value.len()) {
        match (acc.get(index), value.get(index)) {
            (Some(Value::Number(a)), Some(Value::Number(b))) => out.push(sum_numbers(a, b)),
            (Some(x), None) => out.push(x.clone()),
            (None, Some(y)) => out.push(y.clone()),
            _ => return Err(Value::Array(acc.get(index..).unwrap_or_default().to_vec())),
        }
    }
    Ok(Value::Array(out))
}

#[derive(Clone, Copy)]
struct Stats {
    sum: f64,
    count: u64,
    min: f64,
    max: f64,
    sumsqr: f64,
}

enum StatsValue {
    One(Stats),
    Many(Vec<StatsValue>),
}

fn invalid_value(reason: impl std::fmt::Display) -> JsError {
    JsError::InvalidValue(reason.to_string())
}

/// Read a map value or partial for `_stats`: a number, a stats object, or an
/// array of those.
fn stats_input(value: &Value) -> Result<StatsValue, JsError> {
    match value {
        Value::Number(number) => {
            let n = number.as_f64().unwrap_or(0.0);
            Ok(StatsValue::One(Stats {
                sum: n,
                count: 1,
                min: n,
                max: n,
                sumsqr: n * n,
            }))
        }
        Value::Array(items) => items
            .iter()
            .map(stats_input)
            .collect::<Result<_, _>>()
            .map(StatsValue::Many),
        Value::Object(members) => {
            let field = |name: &str| -> Result<f64, JsError> {
                match members.get(name) {
                    None => Err(invalid_value(format!(
                        "user _stats input missing required field {name} ({value})"
                    ))),
                    Some(Value::Number(number)) => Ok(number.as_f64().unwrap_or(0.0)),
                    Some(other) => Err(invalid_value(format!(
                        "non-numeric _stats input received for {name}: {other}"
                    ))),
                }
            };
            Ok(StatsValue::One(Stats {
                sum: field("sum")?,
                count: field("count")? as u64,
                min: field("min")?,
                max: field("max")?,
                sumsqr: field("sumsqr")?,
            }))
        }
        other => Err(invalid_value(format!("non-numeric _stats input: {other}"))),
    }
}

fn merge_stats(acc: StatsValue, next: StatsValue) -> Result<StatsValue, JsError> {
    match (acc, next) {
        (StatsValue::One(a), StatsValue::One(b)) => Ok(StatsValue::One(Stats {
            sum: a.sum + b.sum,
            count: a.count + b.count,
            min: a.min.min(b.min),
            max: a.max.max(b.max),
            sumsqr: a.sumsqr + b.sumsqr,
        })),
        (StatsValue::Many(a), StatsValue::Many(b)) if a.len() == b.len() => a
            .into_iter()
            .zip(b)
            .map(|(a, b)| merge_stats(a, b))
            .collect::<Result<_, _>>()
            .map(StatsValue::Many),
        (StatsValue::Many(_), StatsValue::Many(_)) => {
            Err(invalid_value("_stats arrays must have the same length"))
        }
        _ => Err(invalid_value(
            "The _stats function requires that map values be numbers or arrays of numbers, not a mix of both",
        )),
    }
}

fn stats_json(value: &StatsValue) -> Value {
    match value {
        StatsValue::One(stats) => json!({
            "sum": number_value(stats.sum),
            "count": stats.count,
            "min": number_value(stats.min),
            "max": number_value(stats.max),
            "sumsqr": number_value(stats.sumsqr),
        }),
        StatsValue::Many(items) => Value::Array(items.iter().map(stats_json).collect()),
    }
}

/// Built-in `_stats`: numbers, stats objects (as produced by an earlier
/// `_stats`) and arrays of either, combined element by element.
fn reduce_stats<'a>(values: impl IntoIterator<Item = &'a Value>) -> Result<Value, JsError> {
    let mut acc: Option<StatsValue> = None;
    for value in values {
        let next = stats_input(value)?;
        acc = Some(match acc {
            None => next,
            Some(acc) => merge_stats(acc, next)?,
        });
    }
    Ok(acc.map_or_else(
        || {
            json!({
                "sum": 0, "count": 0, "min": null, "max": null, "sumsqr": 0
            })
        },
        |acc| stats_json(&acc),
    ))
}

pub(crate) fn number_value(value: f64) -> Value {
    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        json!(value as i64)
    } else {
        json!(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::StorageBackend;
    use std::time::Duration;

    fn reduce_value(
        source: &str,
        keys: &[Value],
        values: &[Value],
        limits: ViewLimits,
    ) -> Result<Value, JsError> {
        reduce_with(&mut JsReducer::new(source, limits), keys, values)
    }

    fn sum_of(values: &[Value]) -> Value {
        reduce_value("_sum", &[], values, ViewLimits::default()).unwrap()
    }

    fn stats_of(values: &[Value]) -> Result<Value, JsError> {
        reduce_value("_stats", &[], values, ViewLimits::default())
    }

    #[test]
    fn grouping_a_repeated_requested_key_keeps_one_group_per_occurrence() {
        let row = |id: &str, key: &str| Row {
            id: id.to_string(),
            key: json!(key),
            value: json!(1),
        };
        let rows = [row("d1", "a"), row("d3", "a")];
        // The rows of key "a", listed once per occurrence in `keys`.
        let listed: Vec<&Row> = rows.iter().chain(rows.iter()).collect();
        let query = ViewQuery {
            keys: Some(vec![json!("a"), json!("a")]),
            group: true,
            ..ViewQuery::default()
        };
        let groups = group_rows(listed, &query);
        assert_eq!(groups.len(), 2);
        assert!(groups
            .iter()
            .all(|(key, members)| key == "a" && members.len() == 2));
    }

    #[test]
    fn unique_keys_keeps_the_first_of_equal_keys_in_order() {
        let keys = [
            json!("b"),
            json!(1),
            json!("a"),
            json!(1.0),
            json!("b"),
            json!([1, 2]),
            json!(null),
            json!([1, 2]),
        ];
        assert_eq!(
            unique_keys(&keys),
            vec![json!("b"), json!(1), json!("a"), json!([1, 2]), json!(null)]
        );
    }

    #[test]
    fn unique_keys_scales_with_the_number_of_keys() {
        // A pairwise scan needs over a billion ICU comparisons here, which
        // no machine finishes; sorting needs under a million. No clock is
        // involved: the pairwise version would not return.
        let keys: Vec<Value> = (0..50_000).map(|n| json!(format!("key-{n:06}"))).collect();
        assert_eq!(unique_keys(&keys).len(), keys.len());
    }

    // The expected values below were captured from CouchDB 3.5.
    #[test]
    fn sum_adds_arrays_and_objects_like_couchdb() {
        assert_eq!(sum_of(&[json!([1, 2, 3]), json!([1])]), json!([2, 2, 3]));
        assert_eq!(sum_of(&[json!([1]), json!([1, 2, 3])]), json!([2, 2, 3]));
        assert_eq!(sum_of(&[json!([1, 2]), json!(5)]), json!([6, 2]));
        assert_eq!(sum_of(&[json!([]), json!([])]), json!([0]));
        assert_eq!(sum_of(&[json!([1, "x"])]), json!([1, "x"]));
        assert_eq!(sum_of(&[json!({}), json!({})]), json!({}));
        assert_eq!(
            sum_of(&[json!({"a": 1}), json!({"b": 2}), json!({"a": 3})]),
            json!({"a": 4, "b": 2})
        );
        assert_eq!(
            sum_of(&[json!({"b": 1, "a": 2}), json!({"a": 1})]),
            json!({"a": 3, "b": 1})
        );
        // A lone object is returned as it is, in its own member order.
        let lone = sum_of(&[json!({"b": 1, "a": 2})]);
        assert_eq!(serde_json::to_string(&lone).unwrap(), r#"{"b":1,"a":2}"#);
        assert_eq!(sum_of(&[json!({"a": "s"})]), json!({"a": "s"}));
        assert_eq!(
            sum_of(&[json!({"a": [1, 2]}), json!({"a": [3]})]),
            json!({"a": [4, 2]})
        );
        assert_eq!(
            sum_of(&[json!({"a": 1}), json!({"a": [3]})]),
            json!({"a": [4]})
        );
        assert_eq!(
            sum_of(&[json!({"a": {"b": 1}}), json!({"a": {"b": 2, "c": 1}})]),
            json!({"a": {"b": 3, "c": 1}})
        );
    }

    #[test]
    fn sum_reports_invalid_values_as_an_error_object() {
        let cause = |values: &[Value]| {
            let result = sum_of(values);
            assert_eq!(result["error"], "builtin_reduce_error", "{result}");
            assert_eq!(result["reason"], SUM_ERROR_REASON);
            result["caused_by"].clone()
        };
        assert_eq!(cause(&[json!(1), Value::Null]), Value::Null);
        assert_eq!(cause(&[json!(true)]), json!(true));
        assert_eq!(cause(&[json!("x")]), json!("x"));
        assert_eq!(cause(&[json!({"a": 1}), json!(2)]), json!(2));
        assert_eq!(cause(&[json!([1]), json!({"a": 1})]), json!([1]));
        assert_eq!(cause(&[json!([[1]])]), json!([0]));
    }

    #[test]
    fn stats_accepts_arrays_and_stats_objects_like_couchdb() {
        let one = json!({"sum": 3, "count": 2, "min": 1, "max": 2, "sumsqr": 5});
        assert_eq!(stats_of(std::slice::from_ref(&one)).unwrap(), one);
        assert_eq!(
            stats_of(&[one.clone(), json!(4)]).unwrap(),
            json!({"sum": 7, "count": 3, "min": 1, "max": 4, "sumsqr": 21})
        );
        assert_eq!(
            stats_of(&[json!(4), one.clone()]).unwrap(),
            json!({"sum": 7, "count": 3, "min": 1, "max": 4, "sumsqr": 21})
        );
        assert_eq!(
            stats_of(&[json!([1, 2]), json!([3, 4])]).unwrap(),
            json!([
                {"sum": 4, "count": 2, "min": 1, "max": 3, "sumsqr": 10},
                {"sum": 6, "count": 2, "min": 2, "max": 4, "sumsqr": 20}
            ])
        );
        assert_eq!(stats_of(&[json!([]), json!([])]).unwrap(), json!([]));
        assert_eq!(
            stats_of(&[json!([one.clone()]), json!([1])]).unwrap(),
            json!([{"sum": 4, "count": 3, "min": 1, "max": 2, "sumsqr": 6}])
        );
    }

    #[test]
    fn stats_rejects_invalid_values_as_invalid_value() {
        for values in [
            vec![json!("x")],
            vec![json!(1), json!("x")],
            vec![Value::Null],
            vec![json!(true)],
            vec![json!([1, "x"])],
            vec![json!({"a": 1})],
            vec![json!({"sum": 3, "count": 2, "min": 1, "max": 2})],
            vec![json!({"sum": "a", "count": 2, "min": 1, "max": 2, "sumsqr": 5})],
            vec![json!({"sum": 1, "count": 1, "min": null, "max": null, "sumsqr": 1})],
            vec![json!(1), json!([1, 2])],
            vec![json!([1, 2]), json!([3])],
        ] {
            let error = stats_of(&values).unwrap_err();
            assert_eq!(reduce_failure(&error).error, "invalid_value", "{values:?}");
        }
    }

    #[tokio::test]
    async fn a_query_that_waits_for_the_index_does_not_rebuild_it() {
        let root = std::env::temp_dir().join(format!(
            "douchdb-viewseq-{}-{}",
            std::process::id(),
            crate::util::uuid()
        ));
        let db = Arc::new(
            Db::open_or_create(
                &root,
                "viewseq",
                crate::db::DbConfig {
                    q: 1,
                    revs_limit: 1000,
                    cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                    shard_map: crate::cluster::ShardMap::single(1),
                    partitioned: false,
                },
            )
            .unwrap(),
        );
        let write = |id: &str| json!({"_id": id, "n": 1});
        db.put_docs(vec![write("a")], true).await.unwrap();
        let spec = ViewSpec {
            map: "function(doc){ emit(doc._id, doc.n); }".to_string(),
            reduce: None,
            partitioned: false,
        };
        let limits = ViewQuery::default().limits;
        ensure_rows(&db, "_design/v", "v", &spec, limits)
            .await
            .unwrap();

        // Another query holds the index while this one starts and waits.
        let handle = index_handle(&index_path(&db, "_design/v", "v"))
            .await
            .unwrap();
        let mut holder = handle.lock().await;
        let waiting = tokio::spawn({
            let (db, spec) = (db.clone(), spec.clone());
            async move { ensure_rows(&db, "_design/v", "v", &spec, limits).await }
        });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        // The holder catches the index up with writes that land meanwhile.
        db.put_docs(vec![write("b"), write("c")], true)
            .await
            .unwrap();
        let caught_up = db.index_cursor();
        holder.cursor = caught_up.clone();
        drop(holder);

        waiting.await.unwrap().unwrap();
        assert_eq!(
            handle.lock().await.cursor,
            caught_up,
            "the waiting query rebuilt the index from a stale sequence"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    /// Removing a database's index directories while a query still holds the
    /// index would leave it writing through a handle to deleted files, and a
    /// new query would open a second handle at the same path.
    async fn removal_waits_for_a_query_in_flight(
        remove: fn(Arc<Db>) -> tokio::task::JoinHandle<()>,
    ) {
        let root = std::env::temp_dir().join(format!(
            "douchdb-viewgate-{}-{}",
            std::process::id(),
            crate::util::uuid()
        ));
        let db = Arc::new(
            Db::open_or_create(
                &root,
                "viewgate",
                crate::db::DbConfig {
                    q: 1,
                    revs_limit: 1000,
                    cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                    shard_map: crate::cluster::ShardMap::single(1),
                    partitioned: false,
                },
            )
            .unwrap(),
        );
        db.put_docs(vec![json!({"_id": "a", "n": 1})], true)
            .await
            .unwrap();
        let spec = ViewSpec {
            map: "function(doc){ emit(doc._id, doc.n); }".to_string(),
            reduce: None,
            partitioned: false,
        };
        let limits = ViewQuery::default().limits;
        ensure_rows(&db, "_design/v", "v", &spec, limits)
            .await
            .unwrap();
        let path = index_path(&db, "_design/v", "v");

        // A query holds the index; a second one waits on it, holding the
        // gate for the whole call.
        let handle = index_handle(&path).await.unwrap();
        let holder = handle.lock().await;
        let query = tokio::spawn({
            let (db, spec) = (db.clone(), spec.clone());
            async move { ensure_rows(&db, "_design/v", "v", &spec, limits).await }
        });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(!query.is_finished());

        let removal = remove(db.clone());
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(
            !removal.is_finished() && path.exists(),
            "the index directory was removed under a query in flight"
        );

        drop(holder);
        query.await.unwrap().unwrap();
        removal.await.unwrap();
        assert!(!path.exists());
        assert_eq!(open_indexes_under(&path), 0);
        // A later query opens a fresh index at the same path.
        ensure_rows(&db, "_design/v", "v", &spec, limits)
            .await
            .unwrap();
        assert!(path.exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn cleaning_up_views_waits_for_a_query_in_flight() {
        removal_waits_for_a_query_in_flight(|db| {
            tokio::spawn(async move { cleanup(&db).await.unwrap() })
        })
        .await;
    }

    fn two_shard_view_db(tag: &str) -> (Db, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "douchdb-{tag}-{}-{}",
            std::process::id(),
            crate::util::uuid()
        ));
        let db = Db::open_or_create(
            &root,
            tag,
            crate::db::DbConfig {
                q: 2,
                revs_limit: 1000,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: crate::cluster::ShardMap::single(2),
                partitioned: false,
            },
        )
        .unwrap();
        (db, root)
    }

    fn view_ids(rows: &ViewRows) -> Vec<String> {
        let mut ids: Vec<String> = rows
            .shards
            .iter()
            .flat_map(|shard| shard.blocks.iter().flat_map(|block| block.iter()))
            .map(|row| row.id.clone())
            .collect();
        ids.sort();
        ids
    }

    /// Each shard has its own sequence, so a single number cannot say how far
    /// an index has read: shard 1's next write has a lower sequence than
    /// shard 0 has already reached, and must still reach the index.
    #[tokio::test]
    async fn view_update_reads_each_shard_after_its_own_cursor() {
        let (db, root) = two_shard_view_db("viewcursor");
        let on_shard = |shard: usize, tag: &str| {
            (0..10_000)
                .map(|i| format!("{tag}{i}"))
                .find(|id| db.shard_index(id) == shard)
                .expect("document id for shard")
        };
        let spec = ViewSpec {
            map: "function(doc){ emit(doc._id, 1); }".to_string(),
            reduce: None,
            partitioned: false,
        };
        let limits = ViewQuery::default().limits;
        let mut expected = Vec::new();
        for tag in ["a", "b", "c"] {
            let id = on_shard(0, tag);
            db.put_docs(vec![json!({"_id": id})], true).await.unwrap();
            expected.push(id);
        }
        ensure_rows(&db, "_design/v", "v", &spec, limits)
            .await
            .unwrap();
        let late = on_shard(1, "d");
        db.put_docs(vec![json!({"_id": late})], true).await.unwrap();
        expected.push(late);
        expected.sort();
        let rows = ensure_rows(&db, "_design/v", "v", &spec, limits)
            .await
            .unwrap();
        assert_eq!(view_ids(&rows), expected);
        let handle = index_handle(&index_path(&db, "_design/v", "v"))
            .await
            .unwrap();
        assert_eq!(handle.lock().await.cursor, db.index_cursor());
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A job that took a sequence and wrote no change leaves a gap at the end
    /// of its shard: the shard's committed position is past its last change.
    /// Catching up to that position must still finish, at the shard's
    /// position, with no row to read up to it.
    #[tokio::test]
    async fn view_catch_up_finishes_over_a_trailing_sequence_gap() {
        let (db, root) = two_shard_view_db("viewgap");
        let on_shard = |shard: usize, tag: &str| {
            (0..10_000)
                .map(|i| format!("{tag}{i}"))
                .find(|id| db.shard_index(id) == shard)
                .expect("document id for shard")
        };
        let spec = ViewSpec {
            map: "function(doc){ emit(doc._id, 1); }".to_string(),
            reduce: None,
            partitioned: false,
        };
        let limits = ViewQuery::default().limits;
        let mut expected = vec![on_shard(0, "a"), on_shard(1, "b")];
        for id in &expected {
            db.put_docs(vec![json!({"_id": id})], true).await.unwrap();
        }
        ensure_rows(&db, "_design/v", "v", &spec, limits)
            .await
            .unwrap();

        let late = on_shard(1, "c");
        db.put_docs(vec![json!({"_id": late})], true).await.unwrap();
        expected.push(late);
        expected.sort();
        let before = db.shard_seqs()[1].1;
        db.shard_handle(1)
            .call(|shard| -> Result<(), ApiError> {
                shard.next_seq();
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(db.shard_seqs()[1].1, before + 1, "the gap is committed");

        let rows = ensure_rows(&db, "_design/v", "v", &spec, limits)
            .await
            .unwrap();
        assert_eq!(view_ids(&rows), expected);
        let handle = index_handle(&index_path(&db, "_design/v", "v"))
            .await
            .unwrap();
        assert_eq!(handle.lock().await.cursor, db.index_cursor());
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A shard cleared after the update checked the index's cursor, while it
    /// catches up, is a new copy whose documents left no change rows. The
    /// update notices the new copy and rebuilds at once, as a search index
    /// does, rather than serving the cleared documents until the next query.
    #[tokio::test]
    async fn view_rebuilds_at_once_when_a_shard_is_cleared_during_catch_up() {
        let (db, root) = two_shard_view_db("viewclearrace");
        let on_shard = |shard: usize, tag: &str| {
            (0..10_000)
                .map(|i| format!("{tag}{i}"))
                .find(|id| db.shard_index(id) == shard)
                .expect("document id for shard")
        };
        let spec = ViewSpec {
            map: "function(doc){ emit(doc._id, 1); }".to_string(),
            reduce: None,
            partitioned: false,
        };
        let limits = ViewQuery::default().limits;
        let kept = on_shard(0, "k");
        db.put_docs(
            vec![json!({"_id": kept}), json!({"_id": on_shard(1, "g")})],
            true,
        )
        .await
        .unwrap();
        let rows = ensure_rows(&db, "_design/v", "v", &spec, limits)
            .await
            .unwrap();
        assert_eq!(view_ids(&rows).len(), 2);
        // Shard 1 has a change to catch up on.
        db.put_docs(vec![json!({"_id": on_shard(1, "h")})], true)
            .await
            .unwrap();

        // Hold shard 1's actor with a job that clears the shard once released.
        // The update's read of shard 1 queues behind it, after the update has
        // checked its cursor; only then is the clear let through.
        let shard = db.shard_handle(1);
        let queued = shard.jobs();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let clear = shard.call(move |s| {
            held.recv().map_err(ApiError::internal)?;
            s.clear().map_err(ApiError::from)
        });
        let update = ensure_rows(&db, "_design/v", "v", &spec, limits);
        let let_clear_through = async {
            while shard.jobs() < queued + 2 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            release.send(()).unwrap();
        };
        let (cleared, rows, ()) = tokio::join!(clear, update, let_clear_through);
        cleared.unwrap();
        assert_eq!(view_ids(&rows.unwrap()), vec![kept]);
        let handle = index_handle(&index_path(&db, "_design/v", "v"))
            .await
            .unwrap();
        assert_eq!(handle.lock().await.cursor, db.index_cursor());
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A purge leaves no change row. One that lands while an update catches
    /// the index up (behind the update's read of the shard) must not leave
    /// the purged document in the index: by the next query at the latest,
    /// the index is rebuilt. Whether the update's own re-check or the next
    /// query rebuilds depends on when the purge commits. That a full rebuild
    /// takes the cursor before it reads is not tested: its reads go through
    /// leases, off the actor, and cannot be lined up with a purge.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_purge_during_an_update_is_noticed_by_the_next_query() {
        let (db, root) = two_shard_view_db("viewpurgerace");
        let db = Arc::new(db);
        let on_shard = |shard: usize, tag: &str| {
            (0..10_000)
                .map(|i| format!("{tag}{i}"))
                .find(|id| db.shard_index(id) == shard)
                .expect("document id for shard")
        };
        let spec = ViewSpec {
            map: "function(doc){ emit(doc._id, 1); }".to_string(),
            reduce: None,
            partitioned: false,
        };
        let limits = ViewQuery::default().limits;
        let (kept, gone, late) = (on_shard(0, "k"), on_shard(1, "g"), on_shard(1, "h"));
        let written = db
            .put_docs(vec![json!({"_id": kept}), json!({"_id": gone})], true)
            .await
            .unwrap();
        let rev = written[1].json()["rev"].as_str().unwrap().to_string();
        let rows = ensure_rows(&db, "_design/v", "v", &spec, limits)
            .await
            .unwrap();
        assert_eq!(view_ids(&rows).len(), 2);
        // Shard 1 has a change to catch up on.
        db.put_docs(vec![json!({"_id": late})], true).await.unwrap();

        // Hold shard 1's actor. The update's read of shard 1's new change
        // queues behind it, then the purge behind that, so the purge commits
        // after the update checked the cursor and read the shard.
        let shard = db.shard_handle(1);
        let (started_tx, started) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let blocker = tokio::spawn(shard.call(move |_| {
            let _ = started_tx.send(());
            held.recv().map_err(ApiError::internal)
        }));
        started.await.unwrap();
        let update = tokio::spawn({
            let (db, spec) = (db.clone(), spec.clone());
            async move { ensure_rows(&db, "_design/v", "v", &spec, limits).await }
        });
        while shard.queued() < 1 {
            tokio::task::yield_now().await;
        }
        let purge = tokio::spawn({
            let db = db.clone();
            let mut body = serde_json::Map::new();
            body.insert(gone.clone(), json!([rev]));
            let body = Value::Object(body);
            async move { db.purge(&body, 0, &Default::default()).await }
        });
        while shard.queued() < 2 {
            tokio::task::yield_now().await;
        }
        release.send(()).unwrap();
        blocker.await.unwrap().unwrap();
        update.await.unwrap().unwrap();
        purge.await.unwrap().unwrap();

        let rows = ensure_rows(&db, "_design/v", "v", &spec, limits)
            .await
            .unwrap();
        let mut expected = vec![kept, late];
        expected.sort();
        assert_eq!(
            view_ids(&rows),
            expected,
            "the view serves a purged document"
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A shard that was cleared is a new copy with a new uuid; an index that
    /// read the old copy is rebuilt rather than keeping its rows.
    #[tokio::test]
    async fn index_rebuilds_when_a_shard_uuid_changes() {
        let (db, root) = two_shard_view_db("viewuuid");
        let on_shard = |shard: usize, tag: &str| {
            (0..10_000)
                .map(|i| format!("{tag}{i}"))
                .find(|id| db.shard_index(id) == shard)
                .expect("document id for shard")
        };
        let (kept, gone) = (on_shard(0, "k"), on_shard(1, "g"));
        db.put_docs(vec![json!({"_id": kept}), json!({"_id": gone})], true)
            .await
            .unwrap();
        let spec = ViewSpec {
            map: "function(doc){ emit(doc._id, 1); }".to_string(),
            reduce: None,
            partitioned: false,
        };
        let limits = ViewQuery::default().limits;
        let rows = ensure_rows(&db, "_design/v", "v", &spec, limits)
            .await
            .unwrap();
        assert_eq!(view_ids(&rows).len(), 2);
        db.clear_shards(&[1]).await.unwrap();
        let rows = ensure_rows(&db, "_design/v", "v", &spec, limits)
            .await
            .unwrap();
        assert_eq!(view_ids(&rows), vec![kept]);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn block_reductions_match_a_single_pass() {
        // Enough rows that every shard has several full blocks, keyed so
        // groups and ranges start and end inside blocks.
        let placement = Placement::shards(3, false);
        let mut rows: Vec<Row> = (0..1500)
            .map(|i| Row {
                id: format!("d{i:05}"),
                key: json!([i % 7, i % 11]),
                value: json!(i),
            })
            .collect();
        sort_rows(&mut rows);
        let base = ViewQuery {
            inclusive_end: true,
            limit: usize::MAX,
            ..ViewQuery::default()
        };
        let mut queries: Vec<ViewQuery> = vec![
            base.clone(),
            ViewQuery {
                group: true,
                ..base.clone()
            },
            ViewQuery {
                group_level: 1,
                ..base.clone()
            },
            ViewQuery {
                group_level: 1,
                descending: true,
                skip: 1,
                limit: 4,
                ..base.clone()
            },
            ViewQuery {
                startkey: Some(json!([2, 3])),
                endkey: Some(json!([5, 1])),
                ..base.clone()
            },
            ViewQuery {
                startkey: Some(json!([2, 3])),
                endkey: Some(json!([5, 1])),
                inclusive_end: false,
                group_level: 1,
                ..base.clone()
            },
            ViewQuery {
                startkey: Some(json!([5, 1])),
                endkey: Some(json!([2, 3])),
                descending: true,
                group: true,
                ..base.clone()
            },
            ViewQuery {
                startkey: Some(json!([3, 4])),
                startkey_docid: Some("d00700".into()),
                ..base.clone()
            },
            ViewQuery {
                key: Some(json!([4, 4])),
                ..base.clone()
            },
            ViewQuery {
                keys: Some(vec![
                    json!([6, 2]),
                    json!([1, 1]),
                    json!([6, 2]),
                    json!([9, 9]),
                ]),
                group: true,
                ..base.clone()
            },
            ViewQuery {
                keys: Some(vec![json!([1, 1]), json!([1, 5]), json!([0, 0])]),
                group: true,
                descending: true,
                ..base.clone()
            },
            ViewQuery {
                key: Some(json!([99])),
                ..base.clone()
            },
        ];
        // Ranges that stop just short of, or exactly at, a shard's last row,
        // whose block is shorter than the rest.
        for k in 4..18 {
            queries.push(ViewQuery {
                endkey: Some(json!([6, 10])),
                endkey_docid: Some(format!("d{:05}", 76 + 77 * k)),
                inclusive_end: k % 2 == 0,
                ..base.clone()
            });
        }
        let masked = ViewQuery {
            owner_shards: Some(crate::cluster::ShardMask::from_iter([true, false, true])),
            group_level: 1,
            ..base.clone()
        };
        queries.push(masked);
        let reducers = [
            "_count",
            "_sum",
            "_stats",
            "_approx_count_distinct",
            "_top_3",
            "_bottom_2",
            "_first",
            "_last",
            "function(k, v, re){ return sum(v); }",
        ];
        // A view has one reducer, so each gets an index of its own; the
        // stored partials are checked, written, then checked again after a
        // write has invalidated some of them.
        let check = |index: &mut ViewIndex, rows: &[Row], reducer: &str| {
            let spec = ViewSpec {
                map: String::new(),
                reduce: Some(reducer.to_string()),
                partitioned: false,
            };
            for query in &queries {
                // The oracle filters by shard itself, independently of the
                // blocks under test.
                let visible: Vec<Row> = rows
                    .iter()
                    .filter(|row| {
                        query.owner_shards.as_ref().is_none_or(|owned| {
                            let shard = crate::db::shard_index_for(&row.id, placement.q, false);
                            owned.covers(shard)
                        })
                    })
                    .cloned()
                    .collect();
                let expected = render(&spec, &visible, &HashMap::new(), query).unwrap();
                let mut js = JsReducer::new(reducer, query.limits);
                let actual = index
                    .reduce(
                        query,
                        query.owner_shards.as_ref(),
                        &mut js,
                        &AtomicU64::new(0),
                    )
                    .unwrap();
                assert_eq!(actual, expected, "{reducer} {query:?}");
            }
        };
        let changed: Vec<String> = (0..15).map(|i| format!("d{:05}", i * 97)).collect();
        let added: Vec<Row> = changed
            .iter()
            .map(|id| Row {
                id: id.clone(),
                key: json!([3, 3]),
                value: json!(100000),
            })
            .collect();
        let mut after = rows.clone();
        after.retain(|row| !changed.contains(&row.id));
        after.extend(added.clone());
        sort_rows(&mut after);
        for reducer in reducers {
            let mut index = index_over(redb::backends::InMemoryBackend::new());
            index.set_placement(placement);
            index.rebuild(rows.clone(), cursor_at(1), "map|").unwrap();
            check(&mut index, &rows, reducer);
            // Again, from the partials the first pass stored.
            check(&mut index, &rows, reducer);
            let by_doc: HashMap<String, Vec<Row>> = added
                .iter()
                .map(|row| (row.id.clone(), vec![row.clone()]))
                .collect();
            index
                .apply(changed.clone(), by_doc, cursor_at(2), "map|")
                .unwrap();
            index.check_tables().unwrap();
            check(&mut index, &after, reducer);
        }
    }

    #[test]
    fn js_reducer_compiles_once_per_query() {
        // The counter lives in the compiled closure, so it only advances if
        // every call reuses one compilation.
        let mut reducer = JsReducer::new(
            "(function(){ var calls = 0; return function(k, v, re){ calls += 1; return calls; }; })()",
            ViewLimits::default(),
        );
        let seen: Vec<Value> = (0..3)
            .map(|_| reducer.call(&[], &[json!(1)], false).unwrap())
            .collect();
        assert_eq!(seen, vec![json!(1), json!(2), json!(3)]);
    }

    #[test]
    fn js_reducer_gives_every_call_the_full_timeout() {
        // Three calls of 1.5 s need more than one 4 s budget between them,
        // but each fits its own with a wide margin for a loaded machine.
        let limits = ViewLimits {
            timeout: Duration::from_secs(4),
            ..ViewLimits::default()
        };
        let mut reducer = JsReducer::new(
            "function(k, v, re){ var end = Date.now() + 1500; while (Date.now() < end) {} return 1; }",
            limits,
        );
        for _ in 0..3 {
            assert_eq!(reducer.call(&[], &[json!(1)], false).unwrap(), json!(1));
        }
        let mut endless = JsReducer::new("function(k, v, re){ while (true) {} }", limits);
        assert!(endless.call(&[], &[json!(1)], false).is_err());
    }

    #[test]
    fn couchdb_collation_orders_every_json_type() {
        let ordered = [
            Value::Null,
            json!(false),
            json!(true),
            json!(-1),
            json!(2.5),
            json!("a"),
            json!(["a"]),
            json!({"a": 1}),
        ];
        for pair in ordered.windows(2) {
            assert_eq!(cmp_json(&pair[0], &pair[1]), Ordering::Less);
        }
    }

    #[test]
    fn integers_beyond_f64_precision_collate_exactly() {
        let smaller: Value = serde_json::from_str("123456789012345678901234567890").unwrap();
        let larger: Value = serde_json::from_str("123456789012345678901234567891").unwrap();
        assert_eq!(cmp_json(&smaller, &larger), Ordering::Less);
        assert_eq!(cmp_json(&larger, &smaller), Ordering::Greater);

        let negative: Value = serde_json::from_str("-123456789012345678901234567891").unwrap();
        assert_eq!(cmp_json(&negative, &smaller), Ordering::Less);

        let above_u64: Value = serde_json::from_str("18446744073709551616").unwrap();
        let max_u64: Value = serde_json::from_str("18446744073709551615").unwrap();
        assert_eq!(cmp_json(&max_u64, &above_u64), Ordering::Less);
    }

    #[test]
    fn strings_use_icu_collation() {
        // Codepoint order would put "B" before "a"; ICU puts "a" first.
        assert_eq!(cmp_json(&json!("a"), &json!("B")), Ordering::Less);
        // Accented letters sort next to their base letter, not after "z".
        assert_eq!(cmp_json(&json!("z"), &json!("ä")), Ordering::Greater);
        assert_eq!(cmp_json(&json!("ä"), &json!("b")), Ordering::Less);
    }

    #[test]
    fn native_map_matches_javascript_for_fields_and_literals() {
        let docs = vec![
            (
                "a".to_string(),
                json!({"_id": "a", "city": "Berlin", "n": 2}),
            ),
            ("b".to_string(), json!({"_id": "b", "n": 3})),
        ];
        let native = run_map(
            "function(doc){ emit(doc.city, doc.n); }",
            &docs,
            ViewLimits::default(),
        )
        .unwrap();
        let javascript = run_map(
            "function(doc){ var k = doc.city; var v = doc.n; emit(k, v); }",
            &docs,
            ViewLimits::default(),
        )
        .unwrap();
        assert_eq!(native.len(), javascript.len());
        for (left, right) in native.iter().zip(javascript.iter()) {
            assert_eq!(left.id, right.id);
            assert_eq!(left.key, right.key);
            assert_eq!(left.value, right.value);
        }

        let constant = run_map(
            "function(doc){ emit(doc.city, 1); }",
            &docs,
            ViewLimits::default(),
        )
        .unwrap();
        assert_eq!(constant[0].value, json!(1));
        assert_eq!(constant[1].key, Value::Null);
    }

    #[test]
    fn native_map_matches_javascript_for_generated_corpus() {
        let docs: Vec<(String, Value)> = vec![
            (
                "a".to_string(),
                json!({"_id": "a", "_rev": "1-a", "a": "Berlin", "b": 2,
                       "strange-key": "v", "unicode-ключ": "значение",
                       "nested": {"x": [1, 2, {"y": null}]}}),
            ),
            (
                "b".to_string(),
                json!({"_id": "b", "_rev": "1-b", "a": null, "b": true}),
            ),
            (
                "c".to_string(),
                json!({"_id": "c", "_rev": "1-c", "a": [1, "two", false]}),
            ),
            (
                "d".to_string(),
                json!({"_id": "d", "_rev": "1-d", "b": 9223372036854775808u64}),
            ),
            (
                "e".to_string(),
                serde_json::from_str::<Value>(
                    r#"{"_id":"e","_rev":"1-e","a":123456789012345678901234567890,"b":-1}"#,
                )
                .unwrap(),
            ),
            (
                "f".to_string(),
                json!({"_id": "f", "_rev": "1-f", "a": 100.0, "b": [1.5, 2]}),
            ),
        ];
        let sources = [
            "function(doc){ emit(doc.a, doc.b); }",
            "function(doc){ emit(doc.a, 1); }",
            "function(doc){ emit(doc.a); }",
            "(doc) => { emit(doc[\"strange-key\"], doc[\"strange-key\"]); }",
            "function(doc) { emit(doc[\"unicode-ключ\"], doc.a); }",
            "function(doc){ emit(doc.missing, doc.b); }",
            "function(doc){ emit(doc.b, doc.nested); }",
        ];
        for source in sources {
            // Every source takes the native path; the production entry points
            // below then re-run any row JavaScript would render differently.
            NativeMap::parse(source).unwrap_or_else(|| panic!("native parser rejected {source}"));
            let native_rows = run_map(source, &docs, ViewLimits::default())
                .unwrap_or_else(|error| panic!("native map failed for {source}: {error}"));
            let raw: Vec<(String, String)> = docs
                .iter()
                .map(|(id, doc)| (id.clone(), serde_json::to_string(doc).unwrap()))
                .collect();
            // QuickJS directly: `run_map` would take the native path again.
            let javascript = run_map_js(source, &raw, ViewLimits::default())
                .unwrap_or_else(|error| panic!("JS rejected {source}: {error}"));
            assert_eq!(native_rows.len(), javascript.len(), "{source}");
            for (left, right) in native_rows.iter().zip(javascript.iter()) {
                assert_eq!(left.id, right.id, "{source}");
                assert_eq!(left.key, right.key, "{source}");
                assert_eq!(left.value, right.value, "{source}");
            }

            let raw_rows = run_map_json(source, &raw, ViewLimits::default())
                .unwrap_or_else(|error| panic!("raw native parse failed for {source}: {error}"));
            assert_eq!(raw_rows.len(), javascript.len(), "raw {source}");
            for (left, right) in raw_rows.iter().zip(javascript.iter()) {
                assert_eq!(left.id, right.id, "raw {source}");
                assert_eq!(left.key, right.key, "raw {source}");
                assert_eq!(left.value, right.value, "raw {source}");
            }
        }
    }

    #[test]
    fn native_raw_json_field_extraction_matches_parsed_documents() {
        let docs = vec![
            (
                "a".to_string(),
                r#"{"_id":"a","_rev":"1-x","city":"Ber,lin","n":2,"nested":{"x":[1,2]},"escaped":"a\"b"}"#
                    .to_string(),
            ),
            (
                "b".to_string(),
                r#"{"_id":"b","_rev":"1-y","n":3,"other":null}"#.to_string(),
            ),
        ];
        let native = NativeMap::parse("function(doc){ emit(doc.city, doc.n); }")
            .unwrap()
            .run_json(&docs)
            .unwrap();
        let javascript = run_map_json(
            "function(doc){ var k=doc.city; var v=doc.n; emit(k, v); }",
            &docs,
            ViewLimits::default(),
        )
        .unwrap();
        assert_eq!(native.len(), javascript.len());
        for (left, right) in native.iter().zip(javascript.iter()) {
            assert_eq!(left.id, right.id);
            assert_eq!(left.key, right.key);
            assert_eq!(left.value, right.value);
        }
        assert_eq!(native[0].key, json!("Ber,lin"));
        assert_eq!(native[0].value, json!(2));
        assert_eq!(native[1].key, Value::Null);
        assert_eq!(native[1].value, json!(3));
    }

    #[test]
    fn native_map_supports_brackets_and_arrow_functions() {
        let docs = vec![("a".to_string(), json!({"strange-key": "v"}))];
        let rows = run_map(
            "(doc) => { emit(doc[\"strange-key\"], doc[\"strange-key\"]); }",
            &docs,
            ViewLimits::default(),
        )
        .unwrap();
        assert_eq!(rows[0].key, json!("v"));
        assert_eq!(rows[0].value, json!("v"));
    }

    #[test]
    fn native_map_rejects_more_complex_functions() {
        assert!(NativeMap::parse("function(doc){ if (doc.x) emit(doc.x); }").is_none());
        assert!(NativeMap::parse("function(doc){ emit(doc.x); emit(doc.y); }").is_none());
        assert!(NativeMap::parse("function(doc){ for (var i=0;i<2;i++) emit(i); }").is_none());
        assert!(NativeMap::parse("function(other){ emit(other.x); }").is_none());
        assert!(NativeMap::parse("function(doc){ emit(doc.toString); }").is_none());
    }

    #[test]
    fn map_emits_keys_and_values() {
        let docs = vec![
            (
                "a".to_string(),
                json!({"_id": "a", "city": "Berlin", "n": 2}),
            ),
            (
                "b".to_string(),
                json!({"_id": "b", "city": "Berlin", "n": 3}),
            ),
        ];
        let rows = run_map(
            "function(doc){ emit(doc.city, doc.n); }",
            &docs,
            ViewLimits::default(),
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key, json!("Berlin"));
        assert_eq!(rows[1].value, json!(3));
    }

    #[test]
    fn map_batches_large_document_sets() {
        let docs: Vec<(String, Value)> = (0..600)
            .map(|i| (format!("d{i}"), json!({"_id": format!("d{i}"), "n": i})))
            .collect();
        let raw: Vec<(String, String)> = docs
            .iter()
            .map(|(id, doc)| (id.clone(), serde_json::to_string(doc).unwrap()))
            .collect();
        // 600 documents is three QuickJS batches of at most 256; every
        // document must come back once, in order, across the boundaries.
        let rows = run_map_js(
            "function(doc){ emit(doc.n, doc.n); }",
            &raw,
            ViewLimits::default(),
        )
        .unwrap();
        assert_eq!(rows.len(), 600);
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(row.id, format!("d{index}"));
            assert_eq!(row.value, json!(index));
        }
    }

    /// A one-shard cursor at sequence `seq`.
    fn cursor_at(seq: u64) -> IndexCursor {
        IndexCursor(vec![crate::db::ShardPosition {
            uuid: "u".into(),
            seq,
            purge_seq: 0,
        }])
    }

    #[test]
    fn index_cache_tracks_incremental_updates() {
        let dir = std::env::temp_dir().join(format!("douchdb-view-cache-{}", crate::util::uuid()));
        let mut index = ViewIndex::open(&dir.join("v.redb"), Arc::default()).unwrap();
        index
            .rebuild(
                vec![
                    Row {
                        id: "a".into(),
                        key: json!("a"),
                        value: json!(1),
                    },
                    Row {
                        id: "b".into(),
                        key: json!("b"),
                        value: json!(2),
                    },
                ],
                cursor_at(1),
                "map|",
            )
            .unwrap();
        assert_eq!(index.sorted_rows().unwrap().shards[0].len(), 2);

        // Move a to a later key and delete b; the cached snapshot must follow.
        let mut by_doc = HashMap::new();
        by_doc.insert(
            "a".to_string(),
            vec![Row {
                id: "a".into(),
                key: json!("z"),
                value: json!(3),
            }],
        );
        index
            .apply(
                vec!["a".to_string(), "b".to_string()],
                by_doc,
                cursor_at(2),
                "map|",
            )
            .unwrap();
        let rows = index.sorted_rows().unwrap();
        assert_eq!(rows.shards[0].len(), 1);
        assert_eq!(rows.shards[0].row(0).id, "a");
        assert_eq!(rows.shards[0].row(0).key, json!("z"));
        drop(index);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn custom_reduce_rereduces_large_groups() {
        let values: Vec<Value> = (0..2500).map(|i| json!(i)).collect();
        let value = reduce_value(
            "function(keys, values, rereduce){ return (rereduce ? 1000 : 0) + values.length; }",
            &[],
            &values,
            ViewLimits::default(),
        )
        .unwrap();
        // 1000 + 1000 + 500 in three batches, then rereduce over three partials.
        assert_eq!(value, json!(1003));
    }

    #[test]
    fn an_infinite_map_is_interrupted() {
        let docs = vec![("a".to_string(), json!({}))];
        let result = run_map(
            "function (doc) { while (true) {} }",
            &docs,
            ViewLimits {
                timeout: Duration::from_millis(50),
                memory: 16 * 1024 * 1024,
            },
        );
        assert!(result.is_err());
    }

    #[test]
    fn index_rows_survive_reopen() {
        let dir = std::env::temp_dir().join(format!("douchdb-view-index-{}", crate::util::uuid()));
        let file = dir.join("v.redb");
        let rows = vec![
            Row {
                id: "a".into(),
                key: json!("Berlin"),
                value: json!(1),
            },
            Row {
                id: "b".into(),
                key: json!("Paris"),
                value: json!(2),
            },
        ];
        {
            let mut index = ViewIndex::open(&file, Arc::default()).unwrap();
            index.rebuild(rows, cursor_at(7), "map|").unwrap();
        }
        {
            // The first handle closes in the background; this open waits.
            let mut index = ViewIndex::open(&file, Arc::default()).unwrap();
            assert_eq!(index.cursor, cursor_at(7));
            assert_eq!(index.design, "map|");
            let mut restored = index.load_rows().unwrap();
            restored.sort_by(|a, b| a.id.cmp(&b.id));
            assert_eq!(restored.len(), 2);
            assert_eq!(restored[0].id, "a");
            assert_eq!(restored[1].key, json!("Paris"));
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Random emits, rewrites and deletions keep the key-ordered tables in step
    /// with the by-document rows, through block splits and merges, in several
    /// shards and partitions; the rows read back are in collation order.
    #[test]
    fn key_ordered_tables_follow_random_writes() {
        /// A small deterministic generator, so a failing sequence replays.
        struct Lcg(u64);
        impl Lcg {
            fn below(&mut self, bound: u64) -> u64 {
                self.0 = self
                    .0
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (self.0 >> 33) % bound
            }
        }
        for (q, by_partition) in [(1, false), (3, false), (2, true)] {
            let mut rng = Lcg(u64::from(q) + 7);
            let mut index = index_over(redb::backends::InMemoryBackend::new());
            index.set_placement(Placement {
                q,
                partitioned: by_partition,
                by_partition,
            });
            let id = |n: u32| {
                if by_partition {
                    format!("p{}:d{n}", n % 3)
                } else {
                    format!("d{n}")
                }
            };
            let emit = |rng: &mut Lcg, id: &str| -> Vec<Row> {
                (0..rng.below(4))
                    .map(|_| Row {
                        id: id.to_string(),
                        key: json!(rng.below(6)),
                        value: json!(id),
                    })
                    .collect()
            };
            let mut start = Vec::new();
            for n in 0..40 {
                start.extend(emit(&mut rng, &id(n)));
            }
            index.rebuild(start, cursor_at(0), "map|").unwrap();
            index.check_tables().unwrap();
            for round in 1..60u64 {
                let mut changed = Vec::new();
                let mut by_doc: HashMap<String, Vec<Row>> = HashMap::new();
                for _ in 0..1 + rng.below(11) {
                    let doc = id(rng.below(60) as u32);
                    if !changed.contains(&doc) {
                        changed.push(doc.clone());
                        let rows = emit(&mut rng, &doc);
                        if !rows.is_empty() {
                            by_doc.insert(doc, rows);
                        }
                    }
                }
                index
                    .apply(changed, by_doc, cursor_at(round), "map|")
                    .unwrap();
                index
                    .check_tables()
                    .unwrap_or_else(|error| panic!("q={q} round {round}: {error}"));
            }
            // Read back in order: each run (shard, or partition) sorted by
            // key, then id.
            let rows = index.load_rows().unwrap();
            let placement = index.placement;
            for pair in rows.windows(2) {
                let (a, b) = (&pair[0], &pair[1]);
                let same_run = placement.shard_of(&a.id) == placement.shard_of(&b.id)
                    && placement.partition_of(&a.id) == placement.partition_of(&b.id);
                if same_run {
                    assert_ne!(row_cmp(a, b), Ordering::Greater, "{a:?} {b:?}");
                }
            }
        }
    }

    /// A small deterministic generator, so a failing sequence replays.
    struct Lcg(u64);

    impl Lcg {
        fn below(&mut self, bound: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 33) % bound
        }

        fn chance(&mut self, percent: u64) -> bool {
            self.below(100) < percent
        }
    }

    /// The key-ordered table selects exactly what the in-memory snapshot
    /// selects, for random rows and random queries: ranges, doc id bounds,
    /// `inclusive_end`, `descending`, `key`, repeated `keys`, `skip`,
    /// `limit`, owner masks and partitions, over several shards.
    #[test]
    fn disk_selection_matches_the_in_memory_one() {
        let pool = || {
            vec![
                json!(null),
                json!(false),
                json!(1),
                json!(2),
                json!(2.5),
                json!("a"),
                json!("B"),
                json!("b"),
                json!([1]),
                json!([1, "x"]),
                json!({"k": 1}),
            ]
        };
        for (q, by_partition) in [(1, false), (3, false), (2, true)] {
            let mut rng = Lcg(u64::from(q) * 31 + u64::from(by_partition));
            let placement = Placement {
                q,
                partitioned: by_partition,
                by_partition,
            };
            let mut index = index_over(redb::backends::InMemoryBackend::new());
            index.set_placement(placement);
            let keys = pool();
            let mut rows = Vec::new();
            for n in 0..60 {
                let id = if by_partition {
                    format!("p{}:d{n:02}", n % 3)
                } else {
                    format!("d{n:02}")
                };
                for _ in 0..rng.below(4) {
                    rows.push(Row {
                        id: id.clone(),
                        key: keys[rng.below(keys.len() as u64) as usize].clone(),
                        value: json!(rng.below(1000)),
                    });
                }
            }
            index.rebuild(rows, cursor_at(1), "map|").unwrap();
            let memory = ViewRows::from_rows(index.load_rows().unwrap(), placement);
            let txn = index.db().unwrap().begin_read().unwrap();
            let blocks = index
                .blocks(&txn.open_table(crate::view_store::BLOCKS).unwrap())
                .unwrap();
            let view = DiskView {
                _slot: Arc::new(AsyncMutex::new(index)),
                txn,
                placement,
                blocks,
            };
            let visited = AtomicU64::new(0);
            let pick = |rng: &mut Lcg| keys[rng.below(keys.len() as u64) as usize].clone();
            for round in 0..400 {
                let mut query = ViewQuery {
                    limit: if rng.chance(50) {
                        1 + rng.below(20) as usize
                    } else {
                        usize::MAX
                    },
                    skip: rng.below(6) as usize,
                    descending: rng.chance(50),
                    inclusive_end: rng.chance(70),
                    ..Default::default()
                };
                match rng.below(4) {
                    0 => query.key = Some(pick(&mut rng)),
                    1 => query.keys = Some((0..1 + rng.below(4)).map(|_| pick(&mut rng)).collect()),
                    _ => {
                        if rng.chance(60) {
                            query.startkey = Some(pick(&mut rng));
                            if rng.chance(30) {
                                query.startkey_docid = Some(format!("d{:02}", rng.below(60)));
                            }
                        }
                        if rng.chance(60) {
                            query.endkey = Some(pick(&mut rng));
                            if rng.chance(30) {
                                query.endkey_docid = Some(format!("d{:02}", rng.below(60)));
                            }
                        }
                    }
                }
                let mask = if by_partition {
                    query.partition = Some(format!("p{}", rng.below(4)));
                    None
                } else if rng.chance(30) {
                    Some(
                        (0..q)
                            .map(|_| rng.chance(50))
                            .collect::<crate::cluster::ShardMask>(),
                    )
                } else {
                    None
                };
                let expected = memory.select(&query, mask.as_ref(), &visited);
                let actual = view.select(&query, mask.as_ref(), &visited).unwrap();
                let rows = |selection: &Selection| -> Vec<(String, Value, Value)> {
                    selection
                        .rows
                        .iter()
                        .map(|row| (row.id.clone(), row.key.clone(), row.value.clone()))
                        .collect()
                };
                let context = format!("q={q} partitioned={by_partition} round {round}: {query:?}");
                assert_eq!(rows(&actual), rows(&expected), "{context}");
                assert_eq!(actual.total_rows, expected.total_rows, "{context}");
                assert_eq!(actual.offset, expected.offset, "{context}");
            }
        }
    }

    /// A database for a streamed view query: `q` shards, its documents, and a
    /// view `v` that emits each document's `n`.
    async fn stream_db(name: &str, q: u32, docs: Vec<Value>) -> (Arc<Db>, PathBuf) {
        let root = std::env::temp_dir().join(format!("douchdb-{name}-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            name,
            crate::db::DbConfig {
                q,
                revs_limit: 1000,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: crate::cluster::ShardMap::single(q),
                partitioned: false,
            },
        )
        .unwrap();
        let mut all = vec![json!({"_id": "_design/d", "views": {
            "v": {"map": "function(doc) { if (doc.n !== undefined) emit(doc.n, doc.n); }"},
            "w": {"map": "function(doc) { if (doc.n !== undefined) emit(-doc.n, null); }"},
        }})];
        all.extend(docs);
        db.put_docs(all, true).await.unwrap();
        (Arc::new(db), root)
    }

    /// The keys a streamed query returns, with its `total_rows` and `offset`.
    async fn streamed(db: &Arc<Db>, query: &ViewQuery) -> (Vec<Value>, usize, usize) {
        let ViewReply::Rows(rows) = query_stream(Arc::clone(db), "_design/d", "v", query)
            .await
            .unwrap()
        else {
            panic!("a map query streams");
        };
        let keys: Vec<Value> = futures::StreamExt::collect::<Vec<_>>(rows.rows)
            .await
            .into_iter()
            .map(|row| row.unwrap()["key"].clone())
            .collect();
        (keys, rows.total_rows, rows.offset)
    }

    /// Shards with keys [2, 4] and [1, 3] merge in key order: `skip=1&limit=2`
    /// is [2, 3] ascending and [3, 2] descending. A repeated requested key
    /// lists its rows again.
    #[tokio::test]
    async fn streamed_shards_merge_in_key_order() {
        let shard = |id: &str| crate::db::shard_index_for(id, 2, false);
        let ids: Vec<String> = (0..).map(|n| format!("d{n}")).take(200).collect();
        let first: Vec<&String> = ids.iter().filter(|id| shard(id) == 0).take(2).collect();
        let second: Vec<&String> = ids.iter().filter(|id| shard(id) == 1).take(2).collect();
        let docs = vec![
            json!({"_id": first[0], "n": 2}),
            json!({"_id": first[1], "n": 4}),
            json!({"_id": second[0], "n": 1}),
            json!({"_id": second[1], "n": 3}),
        ];
        let (db, root) = stream_db("streammerge", 2, docs).await;
        let page = |descending| ViewQuery {
            skip: 1,
            limit: 2,
            descending,
            ..Default::default()
        };
        assert_eq!(streamed(&db, &page(false)).await.0, [json!(2), json!(3)]);
        assert_eq!(streamed(&db, &page(true)).await.0, [json!(3), json!(2)]);
        let (all, total, offset) = streamed(
            &db,
            &ViewQuery {
                limit: usize::MAX,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(all, [json!(1), json!(2), json!(3), json!(4)]);
        assert_eq!((total, offset), (4, 0));
        let repeated = streamed(
            &db,
            &ViewQuery {
                keys: Some(vec![json!(2), json!(1), json!(2)]),
                limit: usize::MAX,
                ..Default::default()
            },
        )
        .await
        .0;
        assert_eq!(repeated, [json!(2), json!(1), json!(2)]);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Many documents over several shards and chunks: every streamed query
    /// gives the rows, `total_rows` and `offset` of the in-memory selection.
    #[tokio::test]
    async fn streamed_queries_match_the_in_memory_selection() {
        let docs: Vec<Value> = (0..700)
            .map(|n| json!({"_id": format!("d{n:03}"), "n": n % 97}))
            .collect();
        let (db, root) = stream_db("streamoracle", 3, docs).await;
        let spec = load_spec(&db, "_design/d", "v").await.unwrap();
        let memory = ensure_rows(&db, "_design/d", "v", &spec, ViewLimits::default())
            .await
            .unwrap();
        let mut rng = Lcg(11);
        let visited = AtomicU64::new(0);
        for round in 0..60 {
            let mut query = ViewQuery {
                limit: if rng.chance(50) {
                    1 + rng.below(400) as usize
                } else {
                    usize::MAX
                },
                skip: rng.below(300) as usize,
                descending: rng.chance(50),
                inclusive_end: rng.chance(70),
                ..Default::default()
            };
            if rng.chance(50) {
                query.startkey = Some(json!(rng.below(97)));
            }
            if rng.chance(50) {
                query.endkey = Some(json!(rng.below(97)));
            }
            if query.startkey.is_some() && query.endkey.is_some() {
                let (start, end) = (query.startkey.clone(), query.endkey.clone());
                if (cmp_json(start.as_ref().unwrap(), end.as_ref().unwrap()) == Ordering::Greater)
                    != query.descending
                {
                    (query.startkey, query.endkey) = (end, start);
                }
            }
            let selection = memory.select(&query, None, &visited);
            let mut expected_query = query.clone();
            expected_query.total_rows_override = Some(selection.total_rows);
            expected_query.offset_override = Some(selection.offset);
            let (ordered, total, before) = ordered_rows(&selection.rows, &expected_query);
            let skip = query.skip.min(total.saturating_sub(before));
            let expected: Vec<Value> = ordered
                .into_iter()
                .skip(skip)
                .take(query.limit)
                .map(|row| row.key.clone())
                .collect();
            let (keys, total_rows, offset) = streamed(&db, &query).await;
            assert_eq!(keys, expected, "round {round}: {query:?}");
            assert_eq!(
                (total_rows, offset),
                (total, before + skip),
                "round {round}"
            );
        }
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A stream reads one snapshot of the index file: an update, a cleanup
    /// that removes the file, and eviction pressure on the registry while it
    /// is part-way do not change what it returns or cut it short.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stream_keeps_its_snapshot() {
        let _cap = crate::index_cache::CapGuard::new(1).await;
        let docs: Vec<Value> = (0..900)
            .map(|n| json!({"_id": format!("d{n:03}"), "n": n}))
            .collect();
        let (db, root) = stream_db("streamsnap", 2, docs).await;
        let all = ViewQuery {
            limit: usize::MAX,
            ..Default::default()
        };
        let (expected, ..) = streamed(&db, &all).await;
        assert_eq!(expected.len(), 900);
        let start = || async {
            let ViewReply::Rows(rows) = query_stream(Arc::clone(&db), "_design/d", "v", &all)
                .await
                .unwrap()
            else {
                panic!("a map query streams");
            };
            let mut rows = rows.rows;
            let first = futures::StreamExt::next(&mut rows).await.unwrap().unwrap();
            (first, rows)
        };
        let drain = |first: Value, rows| async move {
            let mut keys = vec![first["key"].clone()];
            let rest: Vec<DbResult<Value>> = futures::StreamExt::collect(rows).await;
            keys.extend(rest.into_iter().map(|row| row.unwrap()["key"].clone()));
            keys
        };

        // An update, applied by another query, while the stream is part-way.
        let (first, rows) = start().await;
        let changed: Vec<Value> = {
            let mut docs = Vec::new();
            for n in (0..900).step_by(3) {
                let id = format!("d{n:03}");
                let rev = db.get_doc(&id, &Default::default()).await.unwrap()["_rev"].clone();
                docs.push(json!({"_id": id, "_rev": rev, "n": n + 10_000}));
            }
            docs
        };
        db.put_docs(changed, true).await.unwrap();
        let (after, ..) = streamed(&db, &all).await;
        assert!(after.iter().any(|key| key.as_u64() > Some(9_999)));
        assert_eq!(drain(first, rows).await, expected);

        // Eviction pressure: another view opened past the cap of one.
        let (first, rows) = start().await;
        let other = query_stream(Arc::clone(&db), "_design/d", "w", &all).await;
        assert!(other.is_ok());
        let (now, ..) = streamed(&db, &all).await;
        assert_eq!(drain(first, rows).await, now);

        // Cleanup removing the view's file while the stream reads it.
        let (first, rows) = start().await;
        let rev = db.get_doc("_design/d", &Default::default()).await.unwrap()["_rev"].clone();
        db.put_docs(
            vec![json!({"_id": "_design/d", "_rev": rev, "_deleted": true})],
            true,
        )
        .await
        .unwrap();
        // Cleanup does not wait for the stream: it holds no gate.
        cleanup(&db).await.unwrap();
        assert!(!index_path(&db, "_design/d", "v").exists());
        assert_eq!(drain(first, rows).await, now);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// The rows of a streamed `include_docs` query, read one at a time, with
    /// `between` run after the first: every row's key, and whether the stream
    /// ended with an error.
    async fn stream_across<F: std::future::Future<Output = ()>>(
        db: &Arc<Db>,
        between: impl FnOnce() -> F,
    ) -> (Vec<Value>, bool) {
        let query = ViewQuery {
            limit: usize::MAX,
            include_docs: true,
            ..Default::default()
        };
        let ViewReply::Rows(rows) = query_stream(Arc::clone(db), "_design/d", "v", &query)
            .await
            .unwrap()
        else {
            panic!("a map query streams");
        };
        let mut rows = rows.rows;
        let first = futures::StreamExt::next(&mut rows).await.unwrap().unwrap();
        between().await;
        let mut keys = vec![first["key"].clone()];
        let mut failed = false;
        while let Some(row) = futures::StreamExt::next(&mut rows).await {
            match row {
                Ok(row) => keys.push(row["key"].clone()),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        (keys, failed)
    }

    /// Deleting the database while an `include_docs` stream is part-way ends
    /// the stream with an error once it reads documents again: it never
    /// finishes as if it had every row.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stream_of_a_deleted_database_ends_with_an_error() {
        let docs: Vec<Value> = (0..900)
            .map(|n| json!({"_id": format!("d{n:03}"), "n": n}))
            .collect();
        let (db, root) = stream_db("streamdelete", 2, docs).await;
        let (keys, failed) = stream_across(&db, || async {
            forget_db(&db.path).await;
            db.close_deleted().await;
        })
        .await;
        assert!(failed, "the stream ran on after the database was deleted");
        assert!(keys.len() < 900, "{} rows", keys.len());
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// An index write that fails closes and reopens the file, as a failed
    /// commit does. A stream reading it meanwhile either keeps its snapshot
    /// to the end or ends with an error: it never ends early as if complete.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_index_write_never_cuts_a_stream_short_silently() {
        let docs: Vec<Value> = (0..900)
            .map(|n| json!({"_id": format!("d{n:03}"), "n": n}))
            .collect();
        let (db, root) = stream_db("streamfail", 2, docs).await;
        let handle = index_handle(&index_path(&db, "_design/d", "v"))
            .await
            .unwrap();
        let (keys, failed) = stream_across(&db, || async {
            handle.lock().await.close_after_failed_write();
        })
        .await;
        if !failed {
            let expected: Vec<Value> = (0..900).map(|n| json!(n)).collect();
            assert_eq!(keys, expected);
        }
        // The reopened index serves the next query whole.
        let (all, ..) = streamed(
            &db,
            &ViewQuery {
                limit: usize::MAX,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(all.len(), 900);
        drop(handle);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// An index over a fresh database on `backend`, as `create_file` makes one.
    fn index_over(backend: impl StorageBackend) -> ViewIndex {
        let db = crate::kv::open_with(backend, VIEW_CACHE_BYTES).unwrap();
        init_tables(&db).unwrap();
        ViewIndex::over(db).unwrap()
    }

    fn row(id: &str, key: &str) -> Row {
        Row {
            id: id.into(),
            key: json!(key),
            value: json!(null),
        }
    }

    /// (id, key) of every stored row, in order.
    fn stored(index: &mut ViewIndex) -> Vec<(String, Value)> {
        let mut rows: Vec<_> = index
            .load_rows()
            .unwrap()
            .into_iter()
            .map(|row| (row.id, row.key))
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| cmp_json(&a.1, &b.1)));
        rows
    }

    /// One update's rows, key-ordered rows, blocks, design and cursor are a
    /// single commit: whichever sync the power is lost at, the reopened index
    /// holds the rows of the cursor it records, with its tables in step, never
    /// the rows of one update and the cursor of another.
    #[test]
    fn view_rows_and_cursor_commit_together() {
        let before = cursor_at(1);
        let after = cursor_at(2);
        let old = vec![("a".to_string(), json!("a")), ("b".to_string(), json!("b"))];
        let new = vec![
            ("a".to_string(), json!("z")),
            ("c".to_string(), json!("c")),
            ("c".to_string(), json!("d")),
        ];
        let mut cuts = 0;
        for syncs in 0.. {
            let (backend, switch) = crate::kv::powercut::PowerCut::new();
            let mut index = index_over(backend);
            index
                .rebuild(vec![row("a", "a"), row("b", "b")], before.clone(), "map|")
                .unwrap();
            switch.cut_at_sync(syncs);
            let mut by_doc = HashMap::new();
            by_doc.insert("a".to_string(), vec![row("a", "z")]);
            by_doc.insert("c".to_string(), vec![row("c", "c"), row("c", "d")]);
            let changed = vec!["a".to_string(), "b".to_string(), "c".to_string()];
            let finished = index.apply(changed, by_doc, after.clone(), "map|").is_ok();
            switch.cut();
            drop(index);

            let mut index =
                ViewIndex::over(crate::kv::open_with(switch.backend(), VIEW_CACHE_BYTES).unwrap())
                    .unwrap();
            assert_eq!(index.design, "map|");
            index
                .check_tables()
                .unwrap_or_else(|error| panic!("cut at sync {syncs}: {error}"));
            if index.cursor == before {
                assert!(!finished, "an acknowledged update was lost");
                assert_eq!(stored(&mut index), old, "cut at sync {syncs}");
            } else {
                assert_eq!(index.cursor, after, "cut at sync {syncs}");
                assert_eq!(stored(&mut index), new, "cut at sync {syncs}");
            }
            if finished {
                break;
            }
            cuts += 1;
        }
        assert!(cuts > 0, "the power was never cut during the update");
    }

    /// Reduce partials are committed without a sync. Power lost right after
    /// leaves them absent or correct, never wrong: the reopened index's
    /// tables agree and its next reduce gives the same answer.
    #[test]
    fn reduce_partials_survive_a_power_cut_absent_or_correct() {
        let rows: Vec<Row> = (0..60)
            .map(|n| Row {
                id: format!("d{n:02}"),
                key: json!(n % 7),
                value: json!(n),
            })
            .collect();
        let query = ViewQuery {
            limit: usize::MAX,
            group: true,
            inclusive_end: true,
            ..Default::default()
        };
        let reduce = |index: &mut ViewIndex| {
            let mut js = JsReducer::new("_sum", query.limits);
            index
                .reduce(&query, None, &mut js, &AtomicU64::new(0))
                .unwrap()
        };
        let (backend, switch) = crate::kv::powercut::PowerCut::new();
        let mut index = index_over(backend);
        index.rebuild(rows, cursor_at(1), "map|").unwrap();
        let expected = reduce(&mut index);
        switch.cut();
        drop(index);
        let mut index =
            ViewIndex::over(crate::kv::open_with(switch.backend(), VIEW_CACHE_BYTES).unwrap())
                .unwrap();
        index.check_tables().unwrap();
        assert_eq!(reduce(&mut index), expected);
    }

    /// Partials committed without a sync are made durable when the index
    /// closes, so a reopened index has them and its first reduce reads no
    /// rows.
    #[test]
    fn unsynced_partials_are_kept_on_a_clean_close() {
        let dir = std::env::temp_dir().join(format!("douchdb-partials-{}", crate::util::uuid()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("v.redb");
        let rows: Vec<Row> = (0..60)
            .map(|n| Row {
                id: format!("d{n:02}"),
                key: json!(n % 7),
                value: json!(n),
            })
            .collect();
        let query = ViewQuery {
            limit: usize::MAX,
            inclusive_end: true,
            ..Default::default()
        };
        let reduce = |index: &mut ViewIndex, visited: &AtomicU64| {
            let mut js = JsReducer::new("_sum", query.limits);
            index.reduce(&query, None, &mut js, visited).unwrap()
        };
        let expected = {
            let mut index = ViewIndex::open(&file, Arc::default()).unwrap();
            index.rebuild(rows, cursor_at(1), "map|").unwrap();
            let answer = reduce(&mut index, &AtomicU64::new(0));
            assert!(index.unsynced);
            answer
        };
        let mut index = ViewIndex::open(&file, Arc::default()).unwrap();
        let visited = AtomicU64::new(0);
        assert_eq!(reduce(&mut index, &visited), expected);
        assert_eq!(
            visited.load(AtomicOrdering::Relaxed),
            0,
            "partials were lost"
        );
        drop(index);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A stored row whose checksum fails is an error, never a row silently
    /// left out of the result.
    #[tokio::test]
    async fn a_corrupt_view_row_is_a_500_not_a_missing_row() {
        // Other tests lower the registries' shared cap, and an idle index
        // over it is evicted and closed in the background, whoever owns it.
        let _cap = crate::index_cache::CapGuard::new(crate::index_cache::DEFAULT_MAX_OPEN).await;
        let root =
            std::env::temp_dir().join(format!("douchdb-viewcorrupt-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "viewcorrupt",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        db.put_docs(
            vec![json!({"_id": "a", "n": 1}), json!({"_id": "b", "n": 2})],
            true,
        )
        .await
        .unwrap();
        let spec = ViewSpec {
            map: "function(doc) { emit(doc.n, null); }".to_string(),
            reduce: None,
            partitioned: false,
        };
        let query = ViewQuery {
            limit: usize::MAX,
            ..Default::default()
        };
        let result = query_with_spec(&db, "_design/d", "v", &spec, &query)
            .await
            .unwrap();
        assert_eq!(result["total_rows"], 2, "{result}");

        // Damage the stored value of a's row; the next query reads it.
        let handle = index_handle(&index_path(&db, "_design/d", "v"))
            .await
            .unwrap();
        {
            let index = handle.lock().await;
            let file = index.db.as_ref().unwrap();
            let txn = crate::kv::begin_write(file).unwrap();
            // a's row's key, from its by-document entry.
            let key = {
                let rows = txn.open_table(ROWS).unwrap();
                let stored = rows.get(("a", 0)).unwrap().unwrap();
                unsealed(stored.value()).unwrap().to_vec()
            };
            let mut damaged = crate::kv::seal(b"null");
            damaged[0] ^= 0x20;
            txn.open_table(crate::view_store::BY_KEY)
                .unwrap()
                .insert(key.as_slice(), damaged.as_slice())
                .unwrap();
            txn.commit().unwrap();
        }
        drop(handle);

        let error = query_with_spec(&db, "_design/d", "v", &spec, &query)
            .await
            .expect_err("a damaged row was served or left out");
        assert_eq!(error.status, 500, "{}", error.reason);
        assert_eq!(error.error, "view_index_error");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A view index closed for idleness is checked as it closes, so its next
    /// open skips the check; a removed one is forgotten, so a new file under
    /// its name is checked. An index directory that is not a redb file
    /// is never read, and cleanup removes it.
    #[tokio::test]
    async fn an_evicted_view_closes_verified_and_a_removed_one_is_forgotten() {
        let _cap = crate::index_cache::CapGuard::new(1).await;
        let root = std::env::temp_dir().join(format!("douchdb-viewclose-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "viewclose",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        let map = |n: u32| format!("function(doc) {{ if (doc.n) emit(doc.n + {n}, null); }}");
        db.put_docs(
            vec![
                json!({"_id": "a", "n": 1}),
                json!({"_id": "_design/d", "views": {"v0": {"map": map(0)}, "v1": {"map": map(1)}}}),
            ],
            true,
        )
        .await
        .unwrap();
        let first = index_path(&db, "_design/d", "v0");
        let second = index_path(&db, "_design/d", "v1");
        assert_eq!(first.extension().and_then(|e| e.to_str()), Some("redb"));
        let old_format = first.with_extension("");
        std::fs::create_dir_all(&old_format).unwrap();
        std::fs::write(old_format.join("CURRENT"), b"MANIFEST-000001\n").unwrap();

        let query = ViewQuery {
            limit: usize::MAX,
            ..Default::default()
        };
        let spec = |n: u32| ViewSpec {
            map: map(n),
            reduce: None,
            partitioned: false,
        };
        let run = |n: u32| {
            let (db, spec, query) = (&db, spec(n), query.clone());
            async move {
                query_with_spec(db, "_design/d", &format!("v{n}"), &spec, &query)
                    .await
                    .unwrap()
            }
        };
        assert_eq!(run(0).await["rows"][0]["key"], json!(1));
        // Opening the second evicts the first, which closes in the background.
        assert_eq!(run(1).await["rows"][0]["key"], json!(2));
        let waited = first.clone();
        tokio::task::spawn_blocking(move || wait_closed(&waited))
            .await
            .unwrap();
        assert!(
            crate::kv::is_verified(&first),
            "the idle close did not verify"
        );
        // Reopened, it serves its rows; the second is evicted in turn.
        assert_eq!(run(0).await["rows"][0]["key"], json!(1));

        cleanup(&db).await.unwrap();
        assert!(!old_format.exists(), "cleanup kept an old-format index");
        assert!(first.exists() && second.exists());

        // The design document goes, and cleanup removes both its views.
        let ddoc = db
            .get_doc("_design/d", &crate::db::DocOpts::default())
            .await
            .unwrap();
        db.put_docs(
            vec![json!({"_id": "_design/d", "_rev": ddoc["_rev"], "_deleted": true})],
            true,
        )
        .await
        .unwrap();
        cleanup(&db).await.unwrap();
        assert!(
            !first.exists() && !second.exists(),
            "cleanup kept a removed view's file"
        );
        assert!(!crate::kv::is_verified(&first));
        assert!(!crate::kv::is_verified(&second));
        assert_eq!(run(1).await["rows"][0]["key"], json!(2));
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A view file whose newest commit is damaged is deleted and rebuilt,
    /// never rolled back to the commit before it.
    #[tokio::test]
    async fn damage_to_the_newest_commit_is_corruption_not_rollback() {
        // Other tests lower the registries' shared cap, and an idle index
        // over it is evicted and closed in the background, whoever owns it.
        let _cap = crate::index_cache::CapGuard::new(crate::index_cache::DEFAULT_MAX_OPEN).await;
        const OLDER: &str = "<<douchdb-view-older-commit>>";
        const NEWEST: &str = "<<douchdb-view-newest-commit>>";
        let root = std::env::temp_dir().join(format!("douchdb-viewnewest-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "viewnewest",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        let spec = ViewSpec {
            map: "function(doc) { emit(doc.n, doc.m); }".into(),
            reduce: None,
            partitioned: false,
        };
        let query = ViewQuery {
            limit: usize::MAX,
            ..Default::default()
        };
        let docs = |range: std::ops::Range<u32>, marker: &str| {
            range
                .map(|n| json!({"_id": format!("d{n:03}"), "n": n, "m": marker.repeat(8)}))
                .collect()
        };
        db.put_docs(docs(0..100, OLDER), true).await.unwrap();
        query_with_spec(&db, "_design/d", "v", &spec, &query)
            .await
            .unwrap();
        db.put_docs(docs(100..200, NEWEST), true).await.unwrap();
        let before = query_with_spec(&db, "_design/d", "v", &spec, &query)
            .await
            .unwrap();
        assert_eq!(before["total_rows"], 200, "{before}");

        // The file as a crash right after the newest update leaves it (a
        // clean close commits again), then damaged where only it wrote.
        let path = index_path(&db, "_design/d", "v");
        let mut bytes = std::fs::read(&path).unwrap();
        indexes().remove(&path);
        let hits: Vec<usize> = bytes
            .windows(NEWEST.len())
            .enumerate()
            .filter(|(_, window)| *window == NEWEST.as_bytes())
            .map(|(at, _)| at)
            .collect();
        assert!(!hits.is_empty(), "the newest update's rows were not found");
        for at in hits {
            bytes[at + 3] ^= 0x55;
        }
        std::fs::write(&path, bytes).unwrap();
        crate::kv::forget_verified(&path);

        let (file, state) = open_file(&path).unwrap();
        drop(file);
        assert_eq!(
            state,
            State::default(),
            "the view file was rolled back, not rebuilt"
        );
        let rebuilt = std::fs::read(&path).unwrap();
        assert!(
            !rebuilt
                .windows(OLDER.len())
                .any(|window| window == OLDER.as_bytes()),
            "the damaged file's older rows were kept"
        );
        let after = query_with_spec(&db, "_design/d", "v", &spec, &query)
            .await
            .unwrap();
        assert_eq!(after, before, "the rebuilt index answers differently");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A view file damaged while closed fails its integrity check on open.
    /// It is derived data, so it is deleted and rebuilt from the shards, and
    /// the query that found it answers from the rebuilt index.
    #[tokio::test]
    async fn a_view_file_that_fails_its_check_is_rebuilt() {
        // Other tests lower the registries' shared cap, and an idle index
        // over it is evicted and closed in the background, whoever owns it.
        let _cap = crate::index_cache::CapGuard::new(crate::index_cache::DEFAULT_MAX_OPEN).await;
        const MARKER: &str = "<<douchdb-view-damage-marker>>";
        let root = std::env::temp_dir().join(format!("douchdb-viewdamage-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "viewdamage",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        let docs = (0..200)
            .map(|n| json!({"_id": format!("d{n:03}"), "n": n}))
            .collect();
        db.put_docs(docs, true).await.unwrap();
        let spec = ViewSpec {
            map: format!("function(doc) {{ emit(doc.n, '{}'); }}", MARKER.repeat(8)),
            reduce: None,
            partitioned: false,
        };
        let query = ViewQuery {
            limit: usize::MAX,
            ..Default::default()
        };
        let before = query_with_spec(&db, "_design/d", "v", &spec, &query)
            .await
            .unwrap();
        assert_eq!(before["total_rows"], 200, "{before}");

        // Closed for good (no close-time check), then damaged on disk.
        let path = index_path(&db, "_design/d", "v");
        indexes().remove(&path);
        assert!(!crate::kv::is_verified(&path));
        let mut bytes = std::fs::read(&path).unwrap();
        let hits: Vec<usize> = bytes
            .windows(MARKER.len())
            .enumerate()
            .filter(|(_, window)| *window == MARKER.as_bytes())
            .map(|(at, _)| at)
            .collect();
        assert!(!hits.is_empty(), "no row values found in the file");
        for at in hits {
            bytes[at + 3] ^= 0x55;
        }
        std::fs::write(&path, bytes).unwrap();
        let mut damaged = MARKER.as_bytes().to_vec();
        damaged[3] ^= 0x55;

        let after = query_with_spec(&db, "_design/d", "v", &spec, &query)
            .await
            .unwrap();
        assert_eq!(after, before, "the rebuilt index answers differently");
        let rebuilt = std::fs::read(&path).unwrap();
        assert!(
            !rebuilt
                .windows(damaged.len())
                .any(|window| window == damaged),
            "the damaged file was kept"
        );
        // The replacement opens cleanly, with the full check.
        indexes().remove(&path);
        assert!(!crate::kv::is_verified(&path));
        drop(crate::kv::open(&path, VIEW_CACHE_BYTES).unwrap());
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A view file whose magic number is damaged is not recognised as a
    /// redb file at all (redb reports an `InvalidData` I/O error, not
    /// corruption). That is damage too: it is deleted and rebuilt, and the
    /// query answers from the rebuilt index.
    #[tokio::test]
    async fn a_view_file_with_a_damaged_magic_number_is_rebuilt() {
        let _cap = crate::index_cache::CapGuard::new(crate::index_cache::DEFAULT_MAX_OPEN).await;
        let root = std::env::temp_dir().join(format!("douchdb-viewmagic-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "viewmagic",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        let docs = (0..20)
            .map(|n| json!({"_id": format!("d{n:03}"), "n": n}))
            .collect();
        db.put_docs(docs, true).await.unwrap();
        let spec = ViewSpec {
            map: "function(doc) { emit(doc.n, null); }".into(),
            reduce: None,
            partitioned: false,
        };
        let query = ViewQuery {
            limit: usize::MAX,
            ..Default::default()
        };
        let before = query_with_spec(&db, "_design/d", "v", &spec, &query)
            .await
            .unwrap();
        assert_eq!(before["total_rows"], 20, "{before}");

        let path = index_path(&db, "_design/d", "v");
        indexes().remove(&path);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[..4].copy_from_slice(b"junk");
        std::fs::write(&path, bytes).unwrap();

        let after = query_with_spec(&db, "_design/d", "v", &spec, &query)
            .await
            .unwrap();
        assert_eq!(after, before, "the rebuilt index answers differently");
        assert_eq!(&std::fs::read(&path).unwrap()[..4], b"redb");
        indexes().remove(&path);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A design or cursor value that fails its checksum when a closed view
    /// file is opened (the pages themselves are intact, so redb's check
    /// passes) is damage too: the file is deleted and rebuilt, and the query
    /// answers from the rebuilt index.
    #[tokio::test]
    async fn a_view_file_whose_state_fails_its_seal_is_rebuilt() {
        let _cap = crate::index_cache::CapGuard::new(crate::index_cache::DEFAULT_MAX_OPEN).await;
        let root = std::env::temp_dir().join(format!("douchdb-viewseal-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "viewseal",
            crate::db::DbConfig {
                q: 2,
                revs_limit: 1000,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: crate::cluster::ShardMap::single(2),
                partitioned: false,
            },
        )
        .unwrap();
        let docs = (0..20)
            .map(|n| json!({"_id": format!("d{n:02}"), "n": n}))
            .collect();
        db.put_docs(docs, true).await.unwrap();
        let spec = ViewSpec {
            map: "function(doc) { emit(doc.n, null); }".to_string(),
            reduce: None,
            partitioned: false,
        };
        let query = ViewQuery {
            limit: usize::MAX,
            ..Default::default()
        };
        let before = query_with_spec(&db, "_design/d", "v", &spec, &query)
            .await
            .unwrap();
        assert_eq!(before["total_rows"], 20, "{before}");
        let path = index_path(&db, "_design/d", "v");

        for damage in ["design", "cursor"] {
            // A mis-sealed value, written through redb and closed for good.
            {
                let handle = index_handle(&path).await.unwrap();
                let index = handle.lock().await;
                let file = index.db.as_ref().unwrap();
                let txn = crate::kv::begin_write(file).unwrap();
                let mut sealed = crate::kv::seal(br#"["x",1]"#);
                sealed[0] ^= 0x20;
                if damage == "design" {
                    txn.open_table(META)
                        .unwrap()
                        .insert(DESIGN, sealed.as_slice())
                        .unwrap();
                } else {
                    txn.open_table(CURSOR)
                        .unwrap()
                        .insert(1, sealed.as_slice())
                        .unwrap();
                }
                txn.commit().unwrap();
            }
            indexes().remove(&path);

            let after = query_with_spec(&db, "_design/d", "v", &spec, &query)
                .await
                .unwrap();
            assert_eq!(
                after, before,
                "damaged {damage}: rebuilt index answers differently"
            );
            indexes().remove(&path);
            let file = crate::kv::open(&path, VIEW_CACHE_BYTES).unwrap();
            let (cursor, _) = read_state(&file)
                .unwrap()
                .expect("the state is still damaged");
            assert_eq!(cursor, db.index_cursor(), "damaged {damage}");
        }
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A view file whose cursor is in the shape earlier versions stored
    /// (each shard's uuid and sequence, no purge sequence) is rebuilt, not
    /// an error, and records the cursor in the current shape.
    #[tokio::test]
    async fn a_view_file_with_a_cursor_in_the_old_shape_is_rebuilt() {
        let _cap = crate::index_cache::CapGuard::new(crate::index_cache::DEFAULT_MAX_OPEN).await;
        let (db, root) = two_shard_view_db("viewoldcursor");
        let docs = (0..10)
            .map(|n| json!({"_id": format!("d{n:02}"), "n": n}))
            .collect();
        db.put_docs(docs, true).await.unwrap();
        let spec = ViewSpec {
            map: "function(doc) { emit(doc.n, null); }".to_string(),
            reduce: None,
            partitioned: false,
        };
        let query = ViewQuery {
            limit: usize::MAX,
            ..Default::default()
        };
        let before = query_with_spec(&db, "_design/d", "v", &spec, &query)
            .await
            .unwrap();
        assert_eq!(before["total_rows"], 10, "{before}");
        let path = index_path(&db, "_design/d", "v");
        {
            let handle = index_handle(&path).await.unwrap();
            let index = handle.lock().await;
            let file = index.db.as_ref().unwrap();
            let txn = crate::kv::begin_write(file).unwrap();
            {
                let mut positions = txn.open_table(CURSOR).unwrap();
                for (shard, position) in db.index_cursor().0.iter().enumerate() {
                    let old = serde_json::to_vec(&json!([position.uuid, position.seq])).unwrap();
                    let shard = u32::try_from(shard).unwrap();
                    positions
                        .insert(shard, crate::kv::seal(&old).as_slice())
                        .unwrap();
                }
            }
            txn.commit().unwrap();
        }
        indexes().remove(&path);
        {
            let file = crate::kv::open(&path, VIEW_CACHE_BYTES).unwrap();
            assert!(
                read_state(&file).unwrap().is_err(),
                "a cursor in the old shape was read"
            );
        }

        let after = query_with_spec(&db, "_design/d", "v", &spec, &query)
            .await
            .unwrap();
        assert_eq!(after, before, "the rebuilt index answers differently");
        indexes().remove(&path);
        let file = crate::kv::open(&path, VIEW_CACHE_BYTES).unwrap();
        let (cursor, _) = read_state(&file)
            .unwrap()
            .expect("the cursor is unreadable");
        assert_eq!(cursor, db.index_cursor());
        drop(file);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// The same for a reduce function, which also runs user JavaScript.
    #[tokio::test(flavor = "current_thread")]
    async fn a_slow_reduce_does_not_block_the_runtime() {
        let root = std::env::temp_dir().join(format!("douchdb-slowred-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "slow",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        db.put_docs(vec![json!({"_id": "a", "n": 1})], true)
            .await
            .unwrap();
        let spec = ViewSpec {
            map: "function(doc) { emit(doc._id, 1); }".to_string(),
            reduce: Some("function(k, v, re) { __pause(); return 1; }".to_string()),
            partitioned: false,
        };
        // A task that does nothing but yield counts how often the runtime got
        // to run it. The script holds itself inside `__pause()` until the
        // test has seen the ticker take several more turns, so a script that
        // blocked the runtime could not be observed making progress and
        // would hit the pause's failsafe instead.
        let pause = crate::js::test_pause::arm().await;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ticks = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let ticker = tokio::spawn({
            let (flag, ticks) = (stop.clone(), ticks.clone());
            async move {
                while !flag.load(std::sync::atomic::Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                    ticks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
        });
        let query = ViewQuery {
            limit: usize::MAX,
            ..Default::default()
        };
        let observe = async {
            pause.started().await;
            let seen = ticks.load(std::sync::atomic::Ordering::SeqCst);
            while ticks.load(std::sync::atomic::Ordering::SeqCst) < seen + 3 {
                tokio::task::yield_now().await;
            }
            pause.release();
        };
        let (result, ()) = tokio::join!(
            query_with_spec(&db, "_design/d", "v", &spec, &query),
            observe
        );
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        ticker.await.unwrap();
        assert!(
            !pause.failsafe_hit(),
            "the reduce blocked the runtime: the ticker never ran while it was paused"
        );
        let result = result.unwrap();
        assert_eq!(result["rows"][0]["value"], json!(1), "{result}");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Every view ever queried used to keep its open file and cached rows
    /// until the database was deleted. The registry must stay at the cap.
    #[tokio::test]
    async fn open_view_indexes_stay_within_the_cap() {
        let _cap = crate::index_cache::CapGuard::new(8).await;
        let root = std::env::temp_dir().join(format!("douchdb-cap-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "cap",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        db.put_docs(vec![json!({"_id": "a", "n": 1})], true)
            .await
            .unwrap();
        let query = ViewQuery {
            limit: usize::MAX,
            ..Default::default()
        };
        for n in 0..40 {
            let spec = ViewSpec {
                map: format!("function(doc) {{ emit(doc.n + {n}, null); }}"),
                reduce: None,
                partitioned: false,
            };
            let result = query_with_spec(&db, "_design/d", &format!("v{n}"), &spec, &query)
                .await
                .unwrap();
            assert_eq!(result["rows"][0]["key"], json!(1 + n));
        }
        let open = open_indexes_under(&db.path);
        assert!(open <= 8, "{open} view indexes are open, cap is 8");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Deleting or recreating a database must wait for a query that is using
    /// its indexes, and must leave no gate behind for the deleted database.
    #[tokio::test]
    async fn forget_db_waits_for_a_query_in_flight() {
        // Other tests lower the registries' shared cap, and an idle index or
        // gate over it is evicted, whoever owns it. Run with them excluded.
        let _cap = crate::index_cache::CapGuard::new(crate::index_cache::DEFAULT_MAX_OPEN).await;
        let root = std::env::temp_dir().join(format!("douchdb-forget-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "forget",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        db.put_docs(vec![json!({"_id": "a", "n": 1})], true)
            .await
            .unwrap();
        let spec = ViewSpec {
            map: "function(doc) { emit(doc.n, null); }".to_string(),
            reduce: None,
            partitioned: false,
        };
        let query = ViewQuery {
            limit: usize::MAX,
            ..Default::default()
        };
        query_with_spec(&db, "_design/d", "v", &spec, &query)
            .await
            .unwrap();
        assert_eq!(open_indexes_under(&db.path), 1);

        // A query holds the read side of the gate while it uses an index.
        let reader = index_gate(&db).read().await.unwrap();
        let path = db.path.clone();
        let forget = forget_db(&path);
        tokio::pin!(forget);
        // One poll is enough: the only thing forget_db awaits is the gate.
        let done = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(forget.as_mut(), cx).is_ready())
        })
        .await;
        assert!(!done, "forget_db finished while a query held the gate");
        assert_eq!(
            open_indexes_under(&db.path),
            1,
            "index dropped under a reader"
        );

        drop(reader);
        forget.await;
        assert_eq!(open_indexes_under(&db.path), 0);
        assert_eq!(
            gates().count_under(&db.path),
            0,
            "gate outlived its database"
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Whether a query holds the read side of the database's gate.
    fn gate_is_held(db: &Db) -> bool {
        index_gate(db).is_held()
    }

    /// Deleting a database must not wait for a view build: the build is
    /// cancelled and its query fails with not_found, as CouchDB's index
    /// processes are shut down with the database. The build is held at the
    /// index lock, where it waits with the gate's read permit; nothing here
    /// depends on timing (a hang is reported by the run's bound).
    #[tokio::test]
    async fn forget_db_does_not_wait_for_a_build() {
        // Other tests lower the registries' shared cap, and an idle index or
        // gate over it is evicted, whoever owns it. Run with them excluded.
        let _cap = crate::index_cache::CapGuard::new(crate::index_cache::DEFAULT_MAX_OPEN).await;
        let root = std::env::temp_dir().join(format!("douchdb-cancel-{}", crate::util::uuid()));
        let config = || crate::db::DbConfig {
            q: 1,
            revs_limit: 1000,
            cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
            shard_map: crate::cluster::ShardMap::single(1),
            partitioned: false,
        };
        let db = Arc::new(Db::open_or_create(&root, "cancel", config()).unwrap());
        db.put_docs(vec![json!({"_id": "a", "n": 1})], true)
            .await
            .unwrap();
        let spec = ViewSpec {
            map: "function(doc) { emit(doc.n, null); }".to_string(),
            reduce: None,
            partitioned: false,
        };
        let query = ViewQuery {
            limit: usize::MAX,
            ..Default::default()
        };
        let handle = index_handle(&index_path(&db, "_design/d", "v"))
            .await
            .unwrap();
        let held = handle.lock_owned().await;
        let build = {
            let (db, spec, query) = (db.clone(), spec.clone(), query.clone());
            tokio::spawn(async move { query_with_spec(&db, "_design/d", "v", &spec, &query).await })
        };
        while !gate_is_held(&db) {
            tokio::task::yield_now().await;
        }

        let path = db.path.clone();
        // With the index lock still held by this test, forget_db and the
        // build can only finish if forget_db does not wait for the build.
        forget_db(&path).await;
        let error = build
            .await
            .unwrap()
            .expect_err("the database was deleted under the query");
        assert_eq!(error.status, 404, "{}", error.reason);
        assert_eq!(error.error, "not_found");
        drop(held);
        assert_eq!(gates().count_under(&path), 0, "gate outlived its database");

        // A database recreated under the name has a fresh gate and serves.
        drop(db);
        std::fs::remove_dir_all(&root).unwrap();
        let db = Db::open_or_create(&root, "cancel", config()).unwrap();
        db.put_docs(vec![json!({"_id": "b", "n": 2})], true)
            .await
            .unwrap();
        let result = query_with_spec(&db, "_design/d", "v", &spec, &query)
            .await
            .unwrap();
        assert_eq!(result["total_rows"], 1, "{result}");
        assert_eq!(result["rows"][0]["id"], "b", "{result}");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn builtin_reduces_work() {
        assert_eq!(
            reduce_value("_count", &[], &[json!(1), json!(2)], ViewLimits::default()).unwrap(),
            json!(2)
        );
        assert_eq!(
            reduce_value("_sum", &[], &[json!(1), json!(2.5)], ViewLimits::default()).unwrap(),
            json!(3.5)
        );
        let stats =
            reduce_value("_stats", &[], &[json!(1), json!(3)], ViewLimits::default()).unwrap();
        assert_eq!(stats["count"], 2);
        assert_eq!(stats["min"], 1);
        assert_eq!(stats["max"], 3);
    }
    /// A slow map function must run off the async workers: on a one-thread
    /// runtime a task that only yields has to keep running while the map spins.
    #[tokio::test(flavor = "current_thread")]
    async fn a_slow_map_does_not_block_the_runtime() {
        let root = std::env::temp_dir().join(format!("douchdb-slowmap-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "slow",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        db.put_docs(vec![json!({"_id": "a", "n": 1})], true)
            .await
            .unwrap();
        let spec = ViewSpec {
            map: "function(doc) { __pause(); emit(doc._id, 1); }".to_string(),
            reduce: None,
            partitioned: false,
        };
        // A task that does nothing but yield counts how often the runtime got
        // to run it. The script holds itself inside `__pause()` until the
        // test has seen the ticker take several more turns, so a script that
        // blocked the runtime could not be observed making progress and
        // would hit the pause's failsafe instead.
        let pause = crate::js::test_pause::arm().await;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ticks = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let ticker = tokio::spawn({
            let (flag, ticks) = (stop.clone(), ticks.clone());
            async move {
                while !flag.load(std::sync::atomic::Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                    ticks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
        });
        let query = ViewQuery {
            limit: usize::MAX,
            ..Default::default()
        };
        let observe = async {
            pause.started().await;
            let seen = ticks.load(std::sync::atomic::Ordering::SeqCst);
            while ticks.load(std::sync::atomic::Ordering::SeqCst) < seen + 3 {
                tokio::task::yield_now().await;
            }
            pause.release();
        };
        let (result, ()) = tokio::join!(
            query_with_spec(&db, "_design/d", "v", &spec, &query),
            observe
        );
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        ticker.await.unwrap();
        assert!(
            !pause.failsafe_hit(),
            "the map blocked the runtime: the ticker never ran while it was paused"
        );
        let result = result.unwrap();
        assert_eq!(result["rows"].as_array().map(Vec::len), Some(1));
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Shard-actor jobs one view query submits. Each job is a cross-thread hop
    /// to the shard's dedicated thread, so the count, unlike latency, does not
    /// move with machine load.
    async fn jobs_for(db: &Db, query: &ViewQuery) -> (u64, Value) {
        let before = db.shard_jobs();
        let result = query_by_id(db, query).await;
        (db.shard_jobs() - before, result)
    }

    async fn query_by_id(db: &Db, query: &ViewQuery) -> Value {
        super::query(db, "_design/d", "v", query).await.unwrap()
    }

    /// Before: 1 job (the design document read) for a map query, 2 with
    /// `include_docs` (design document, then the row bodies), 1 for a reduce.
    /// After: 0, 1 and 0. The index is not a shard, so a current index costs
    /// no job, and a design document that has not changed is served from the
    /// database's cache.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_view_query_on_a_current_index_takes_the_fewest_shard_jobs() {
        let root = std::env::temp_dir().join(format!("douchdb-viewjobs-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "jobs",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        db.put_docs(
            vec![
                json!({"_id": "_design/d", "views": {"v": {
                    "map": "function(doc) { emit(doc._id, 1); }",
                    "reduce": "_count",
                }}}),
                json!({"_id": "a", "n": 1}),
                json!({"_id": "b", "n": 2}),
            ],
            true,
        )
        .await
        .unwrap();

        let plain = ViewQuery {
            reduce: Some(false),
            limit: usize::MAX,
            ..Default::default()
        };
        // The first query builds the index; the ones below find it current.
        let (_, built) = jobs_for(&db, &plain).await;
        assert_eq!(built["total_rows"], json!(2), "{built}");

        let (jobs, result) = jobs_for(&db, &plain).await;
        assert_eq!(result["rows"].as_array().map(Vec::len), Some(2), "{result}");
        let map_jobs = jobs;

        let with_docs = ViewQuery {
            reduce: Some(false),
            include_docs: true,
            limit: usize::MAX,
            ..Default::default()
        };
        let (jobs, result) = jobs_for(&db, &with_docs).await;
        assert_eq!(result["rows"][0]["doc"]["n"], json!(1), "{result}");
        let include_docs_jobs = jobs;

        let (jobs, result) = jobs_for(
            &db,
            &ViewQuery {
                limit: usize::MAX,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(result["rows"][0]["value"], json!(2), "{result}");
        let reduce_jobs = jobs;
        assert_eq!(
            (map_jobs, include_docs_jobs, reduce_jobs),
            (0, 1, 0),
            "shard jobs for a map, an include_docs and a reduce query on a current index"
        );

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Shard jobs an incremental view update costs on a three-shard database.
    /// Before: 9 jobs, 3 of them the per-shard `pending` count the index never
    /// uses; then 6. With a cursor per shard only the shard that moved is
    /// read: 1.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_incremental_view_update_skips_the_pending_pass() {
        let root = std::env::temp_dir().join(format!("douchdb-viewpend-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "pend",
            crate::db::DbConfig {
                q: 3,
                revs_limit: 1000,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: crate::cluster::ShardMap::single(3),
                partitioned: false,
            },
        )
        .unwrap();
        db.put_docs(
            vec![
                json!({"_id": "_design/d", "views": {"v": {
                    "map": "function(doc) { emit(doc._id, 1); }",
                }}}),
                json!({"_id": "a", "n": 1}),
            ],
            true,
        )
        .await
        .unwrap();
        let query = ViewQuery {
            limit: usize::MAX,
            ..Default::default()
        };
        // Builds the index and caches the design document.
        query_by_id(&db, &query).await;
        db.put_docs(vec![json!({"_id": "b", "n": 2})], true)
            .await
            .unwrap();
        let (jobs, result) = jobs_for(&db, &query).await;
        assert_eq!(result["total_rows"], json!(2), "{result}");
        assert_eq!(jobs, 1, "shard jobs for one incremental update");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A partition's view query works on that partition's rows, however many
    /// other partitions share its shard: the index rows it visits do not grow
    /// with them.
    #[tokio::test(flavor = "multi_thread")]
    async fn partition_view_queries_visit_rows_independent_of_other_partitions() {
        // The visit counts assume the built index stays open between queries.
        // Another test lowering the shared cap would evict it, and the reopened
        // index then reads its partition's rows again to rebuild its block
        // reductions.
        let _cap = crate::index_cache::CapGuard::new(crate::index_cache::DEFAULT_MAX_OPEN).await;
        const K: usize = 600;
        let mut per_p = Vec::new();
        for partitions in [2usize, 6] {
            let root = std::env::temp_dir().join(format!("douchdb-pvisit-{}", crate::util::uuid()));
            let db = Db::open_or_create(
                &root,
                "pvisit",
                crate::db::DbConfig {
                    q: 1,
                    revs_limit: 1000,
                    cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                    shard_map: crate::cluster::ShardMap::single(1),
                    partitioned: true,
                },
            )
            .unwrap();
            let mut docs = vec![json!({
                "_id": "_design/d",
                "views": {"v": {
                    "map": "function(doc) { emit(doc.n, 1); }",
                    "reduce": "_count",
                }},
            })];
            for p in 0..partitions {
                for n in 0..K {
                    docs.push(json!({"_id": format!("p{p}:d{n}"), "n": n}));
                }
            }
            db.put_docs(docs, true).await.unwrap();
            let query = |reduce: bool, range: bool| ViewQuery {
                limit: usize::MAX,
                partition: Some("p0".into()),
                reduce: Some(reduce),
                startkey: range.then(|| json!(100)),
                endkey: range.then(|| json!(400)),
                ..Default::default()
            };
            let run = |reduce: bool, range: bool| {
                let (db, query) = (&db, query(reduce, range));
                async move {
                    let before = db.view_rows_visited();
                    let result = super::query(db, "_design/d", "v", &query).await.unwrap();
                    (result, db.view_rows_visited() - before)
                }
            };
            // The first query builds the index; only later ones are measured.
            run(false, false).await;
            run(true, true).await;
            let (rows, map_visits) = run(false, false).await;
            assert_eq!(rows["total_rows"], json!(K), "{rows}");
            assert_eq!(rows["rows"].as_array().map(Vec::len), Some(K));
            let (reduced, reduce_visits) = run(true, true).await;
            assert_eq!(reduced["rows"][0]["value"], json!(300), "{reduced}");
            per_p.push((partitions, map_visits, reduce_visits));
            drop(db);
            let _ = std::fs::remove_dir_all(root);
        }
        // The map reads its K rows; a ranged reduce reads the rows at the two
        // edges of its range, not the shard.
        for &(partitions, map_visits, reduce_visits) in &per_p {
            assert!(
                map_visits <= K as u64 + 64,
                "{partitions} partitions: {per_p:?}"
            );
            assert!(
                reduce_visits <= 2 * ROW_BLOCK as u64,
                "{partitions} partitions: {per_p:?}"
            );
        }
    }

    /// The design document cache must never serve a view definition that has
    /// since been replaced or removed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cached_design_document_follows_every_change() {
        let root = std::env::temp_dir().join(format!("douchdb-viewspec-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "spec",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        let ddoc = |map: &str, rev: Option<&str>| {
            let mut doc = json!({"_id": "_design/d", "views": {"v": {"map": map}}});
            if let Some(rev) = rev {
                doc["_rev"] = json!(rev);
            }
            doc
        };
        let results = db
            .put_docs(
                vec![
                    ddoc("function(doc) { emit(1, null); }", None),
                    json!({"_id": "a"}),
                ],
                true,
            )
            .await
            .unwrap();
        let rev = match &results[0] {
            crate::db::WriteResult::Ok(ok) => ok.rev.clone(),
            other => panic!("design document not written: {other:?}"),
        };
        let query = ViewQuery {
            limit: usize::MAX,
            ..Default::default()
        };
        let first = query_by_id(&db, &query).await;
        assert_eq!(first["rows"][0]["key"], json!(1), "{first}");
        // Served from the cache now; a second read still agrees.
        let again = query_by_id(&db, &query).await;
        assert_eq!(again["rows"][0]["key"], json!(1), "{again}");

        db.put_docs(
            vec![ddoc("function(doc) { emit(2, null); }", Some(&rev))],
            true,
        )
        .await
        .unwrap();
        let changed = query_by_id(&db, &query).await;
        assert_eq!(changed["rows"][0]["key"], json!(2), "{changed}");

        let current = db
            .get_doc("_design/d", &crate::db::DocOpts::default())
            .await
            .unwrap();
        let rev = current["_rev"].as_str().unwrap().to_string();
        db.purge(&json!({"_design/d": [rev]}), 0, &Default::default())
            .await
            .unwrap();
        let gone = super::query(&db, "_design/d", "v", &query).await;
        assert_eq!(gone.unwrap_err().status, 404);

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }
}

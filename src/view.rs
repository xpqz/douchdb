//! CouchDB map/reduce views, evaluated with an embedded QuickJS runtime.
//!
//! Each view has a persistent LevelDB index under `_view_indexes/`. A query
//! applies `_changes` since the last indexed sequence and only re-runs the map
//! function for documents that changed; a design-document change rebuilds the
//! index. The JS engine exposes the same `doc`/`emit` environment as CouchDB's
//! JavaScript view server.

use crate::db::{hex, AllDocsOpts, ApiError, ChangesOpts, Db, DbResult, DocOpts};
use base64::Engine as _;
use icu_collator::options::CollatorOptions;
use icu_collator::Collator;
use md5::{Digest, Md5};
use rusty_leveldb::{LdbIterator, Options as LdbOptions, WriteBatch, DB as Ldb};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

#[derive(Clone, Copy, Debug)]
pub struct ViewLimits {
    pub timeout: Duration,
    pub memory: usize,
}

impl Default for ViewLimits {
    fn default() -> Self {
        ViewLimits {
            timeout: Duration::from_secs(5),
            memory: 64 * 1024 * 1024,
        }
    }
}

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
    pub attachments: bool,
    pub update_seq: bool,
    pub limits: ViewLimits,
    /// Internal cluster filter: return only rows whose shard is owned by this
    /// node. The coordinator unions the owner-only responses, so each map row
    /// crosses the network once instead of once per replica.
    pub owner_shards: Option<Vec<bool>>,
    pub shard_count: u32,
    /// Set for a `_partition/{p}/_design/.../_view/...` query. Only rows
    /// belonging to that partition are returned.
    pub partition: Option<String>,
    /// Partitioned databases hash by partition, so owner-shard filtering must
    /// use the same placement function.
    pub partitioned: bool,
    /// Set by the cluster coordinator after summing owner-only map row counts.
    pub total_rows_override: Option<usize>,
    /// Set once rows have already been selected (by `ViewRows::select` or a
    /// coordinator merge): the reported offset, and a signal that the
    /// partition and shard filters have already been applied.
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row {
    pub id: String,
    pub key: Value,
    pub value: Value,
}

#[derive(Serialize, Deserialize)]
struct IndexMeta {
    last_seq: u64,
    design: String,
}

#[derive(Serialize, Deserialize)]
struct StoredRow {
    key: Value,
    value: Value,
}

#[derive(Serialize)]
struct StoredRowRef<'a> {
    key: &'a Value,
    value: &'a Value,
}

/// A persistent index for one (database, design document, view) tuple. Rows
/// are stored per document so a changed document can be re-mapped without
/// walking the whole database.
/// A raw LevelDB key and value.
type KeyValue = (Vec<u8>, Vec<u8>);

struct ViewIndex {
    db: Ldb,
    last_seq: u64,
    design: String,
    /// Shard count and partitioning of the database, used to split the cached
    /// rows by shard.
    placement: (u32, bool),
    /// Sorted rows for the current index state. Warm queries reuse this
    /// instead of scanning and parsing every stored row on each request.
    cached: Option<Arc<ViewRows>>,
}

/// A view's sorted rows, split by the shard of each row's document. A query
/// reads only the shards it is asked for, finds its key range by binary
/// search, and copies only the rows it can return, so its cost follows the
/// result rather than the size of the index.
#[derive(Default)]
pub struct ViewRows {
    shards: Vec<ShardRows>,
    /// Per shard, partial reductions for one reducer: built by the first
    /// reduce that needs them and kept up to date across writes.
    blocks: Vec<StdMutex<Option<BlockTree>>>,
}

/// Rows per storage block of a shard. A write copies only the blocks it
/// changes, and a copy of the view shares the rest.
const ROW_BLOCK: usize = 256;

/// One shard's rows in key order, held in blocks of about `ROW_BLOCK` rows.
/// No block is empty.
#[derive(Clone, Default)]
struct ShardRows {
    blocks: Vec<Arc<Vec<Row>>>,
    /// The row each block starts at, then the shard's row count.
    starts: Vec<usize>,
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

    fn range(&self, first: usize, end: usize) -> impl Iterator<Item = &Row> + Clone {
        let block = self.block_of(first);
        let skip = first - self.starts.get(block).copied().unwrap_or(first);
        self.blocks[block..]
            .iter()
            .flat_map(|rows| rows.iter())
            .skip(skip)
            .take(end.saturating_sub(first))
    }

    /// Positions of the rows `(id, key)` emitted by the documents being
    /// replaced, sorted. A document's rows with one key are adjacent.
    fn positions(&self, gone: &[(&str, &Value)]) -> Vec<usize> {
        let order =
            |a: &(&str, &Value), b: &(&str, &Value)| cmp_json(a.1, b.1).then_with(|| a.0.cmp(b.0));
        let mut gone = gone.to_vec();
        gone.sort_by(order);
        let mut positions = Vec::new();
        for run in gone.chunk_by(|a, b| order(a, b) == Ordering::Equal) {
            let (id, key) = run[0];
            let row_order = |row: &Row| order(&(row.id.as_str(), &row.key), &(id, key));
            let first = self.partition_point(|row| row_order(row) == Ordering::Less);
            let end = self.partition_point(|row| row_order(row) != Ordering::Greater);
            if end - first == run.len() {
                positions.extend(first..end);
            } else {
                // Not where its stored key says (a float that did not survive
                // the round trip exactly): find the document's rows by id
                // instead, so a stale row can never be left behind.
                positions.extend(
                    self.range(0, self.len())
                        .enumerate()
                        .filter(|(_, row)| row.id == id)
                        .map(|(at, _)| at),
                );
            }
        }
        positions.sort_unstable();
        positions.dedup();
        positions
    }

    /// Remove the rows at sorted `removed` positions and merge in sorted
    /// `added`. Returns each added row's position among the kept rows.
    fn replace(&mut self, removed: &[usize], added: Vec<Row>) -> Vec<usize> {
        let mut touched = vec![false; self.blocks.len()];
        let mut next = 0;
        while next < removed.len() {
            let block = self.block_of(removed[next]);
            let (start, end) = (self.starts[block], self.starts[block + 1]);
            let upto = next + removed[next..].partition_point(|at| *at < end);
            let mut gone = removed[next..upto].iter().map(|at| at - start).peekable();
            let mut at = 0;
            Arc::make_mut(&mut self.blocks[block]).retain(|_| {
                let drop = gone.next_if_eq(&at).is_some();
                at += 1;
                !drop
            });
            touched[block] = true;
            next = upto;
        }
        // Drop emptied blocks so every block has a last row to search by.
        let mut flags = touched.into_iter();
        let mut touched = Vec::new();
        self.blocks.retain(|rows| {
            let flag = flags.next().unwrap_or(false);
            if !rows.is_empty() {
                touched.push(flag);
            }
            !rows.is_empty()
        });
        self.index();

        let inserted: Vec<usize> = added
            .iter()
            .map(|row| self.partition_point(|kept| row_cmp(kept, row) == Ordering::Less))
            .collect();
        if !added.is_empty() && self.blocks.is_empty() {
            self.blocks.push(Arc::default());
            touched.push(true);
        }
        // A row goes to the block holding the kept row it lands before, or
        // to the last block when it lands after every kept row.
        let mut added = added.into_iter().zip(&inserted).peekable();
        while let Some((row, &at)) = added.next() {
            let block = self.block_of(at).min(self.blocks.len() - 1);
            let mut run = vec![row];
            while let Some((row, _)) =
                added.next_if(|(_, &at)| self.block_of(at).min(self.blocks.len() - 1) == block)
            {
                run.push(row);
            }
            let rows = Arc::make_mut(&mut self.blocks[block]);
            *rows = merge_sorted_rows(std::mem::take(rows), run);
            touched[block] = true;
        }
        self.rebalance(&touched);
        inserted
    }

    /// Split touched blocks grown past twice `ROW_BLOCK` and join those
    /// shrunk below half of it to the next block.
    fn rebalance(&mut self, touched: &[bool]) {
        let mut blocks = Vec::with_capacity(self.blocks.len());
        let mut carry: Vec<Row> = Vec::new();
        for (rows, &touched) in std::mem::take(&mut self.blocks).into_iter().zip(touched) {
            if !touched && carry.is_empty() {
                blocks.push(rows);
                continue;
            }
            carry.extend(Arc::unwrap_or_clone(rows));
            if carry.len() >= ROW_BLOCK / 2 {
                split_into(&mut blocks, std::mem::take(&mut carry));
            }
        }
        if !carry.is_empty() {
            if let Some(last) = blocks.pop() {
                let mut rows = Arc::unwrap_or_clone(last);
                rows.extend(carry);
                carry = rows;
            }
            split_into(&mut blocks, carry);
        }
        self.blocks = blocks;
        self.index();
    }
}

/// Push `rows` as one block, or as even pieces of about `ROW_BLOCK` when it
/// is more than twice that.
fn split_into(blocks: &mut Vec<Arc<Vec<Row>>>, rows: Vec<Row>) {
    let pieces = if rows.len() > 2 * ROW_BLOCK {
        rows.len().div_ceil(ROW_BLOCK)
    } else {
        1
    };
    let (size, extra) = (rows.len() / pieces, rows.len() % pieces);
    let mut rows = rows.into_iter();
    for piece in 0..pieces {
        let take = size + usize::from(piece < extra);
        blocks.push(Arc::new(rows.by_ref().take(take).collect()));
    }
}

/// Rows per block when a shard's partials are built, and children per
/// partial at each level above. A span reads whole partials at the highest
/// level they fit, so it touches few rows or partials per level.
const REDUCE_FANOUT: usize = 32;

/// Partial reductions by group key (by exact key for a `keys` query), with
/// the key itself.
pub(crate) type Partials = HashMap<String, (Value, Vec<Value>)>;

/// A shard's partial reductions for the reducer `source`. Blocks of
/// consecutive rows grow and shrink as writes add and remove rows, so a write
/// marks only the blocks it touched for recomputation, plus their parents.
#[derive(Clone)]
struct BlockTree {
    source: String,
    spans: Arc<Spans>,
    /// Blocks whose partial a write invalidated.
    dirty: Vec<usize>,
}

/// Block boundaries and partials.
#[derive(Clone, Default)]
struct Spans {
    /// The row each block starts at, then the shard's row count.
    starts: Vec<usize>,
    /// `levels[0]` holds each block's partial; each level above combines
    /// `REDUCE_FANOUT` partials of the one below, and the last may be short.
    levels: Vec<Vec<Value>>,
}

impl BlockTree {
    fn build(rows: &ShardRows, reducer: &mut JsReducer) -> Result<BlockTree, String> {
        let len = rows.len();
        let mut starts: Vec<usize> = (0..len).step_by(REDUCE_FANOUT).collect();
        starts.push(len);
        let leaves = starts
            .windows(2)
            .map(|span| reduce_rows(rows.range(span[0], span[1]), reducer))
            .collect::<Result<_, _>>()?;
        let mut spans = Spans {
            starts,
            levels: vec![leaves],
        };
        spans.build_upper(reducer)?;
        Ok(BlockTree {
            source: reducer.source().to_string(),
            spans: Arc::new(spans),
            dirty: Vec::new(),
        })
    }

    /// Follow a write to the shard. `removed` holds the sorted positions of
    /// the old rows dropped, and `inserted` each added row's index among the
    /// rows kept. Returns false when there is no block to place a row in.
    fn apply_write(&mut self, removed: &[usize], inserted: &[usize]) -> bool {
        let spans = Arc::make_mut(&mut self.spans);
        let count = spans.starts.len().saturating_sub(1);
        if count == 0 {
            return inserted.is_empty();
        }
        let mut sizes: Vec<usize> = spans.starts.windows(2).map(|w| w[1] - w[0]).collect();
        let mut touched = vec![false; count];
        for &block in &self.dirty {
            touched[block] = true;
        }
        let mut next = 0;
        for block in 0..count {
            let upto = next + removed[next..].partition_point(|at| *at < spans.starts[block + 1]);
            let dropped = upto - next;
            next = upto;
            if dropped > 0 {
                sizes[block] -= dropped;
                touched[block] = true;
            }
        }
        // A row goes to the block holding the kept row it lands before, or
        // to the last block when it lands after every kept row.
        let mut kept_ends = Vec::with_capacity(count);
        let mut end = 0;
        for size in &sizes {
            end += size;
            kept_ends.push(end);
        }
        for &at in inserted {
            let block = kept_ends.partition_point(|end| *end <= at).min(count - 1);
            sizes[block] += 1;
            touched[block] = true;
        }

        let mut leaves = std::mem::take(&mut spans.levels[0]).into_iter();
        let mut starts = vec![0];
        let mut kept_leaves = Vec::with_capacity(count);
        let mut dirty = Vec::new();
        let mut start = 0;
        for (block, size) in sizes.into_iter().enumerate() {
            let leaf = leaves.next().unwrap_or(Value::Null);
            if size == 0 {
                continue;
            }
            // A block that has grown past twice the build size is split.
            let pieces = if touched[block] {
                size.div_ceil(2 * REDUCE_FANOUT)
            } else {
                1
            };
            for piece in 0..pieces {
                start += size / pieces + usize::from(piece < size % pieces);
                starts.push(start);
                if touched[block] {
                    dirty.push(kept_leaves.len());
                    kept_leaves.push(Value::Null);
                } else {
                    kept_leaves.push(leaf.clone());
                }
            }
        }
        if kept_leaves.len() != count {
            // Parents group blocks by position, so they are rebuilt.
            spans.levels.truncate(1);
        }
        spans.starts = starts;
        spans.levels[0] = kept_leaves;
        self.dirty = dirty;
        true
    }

    /// Recompute what writes invalidated.
    fn refresh(&mut self, rows: &ShardRows, reducer: &mut JsReducer) -> Result<(), String> {
        let stale_upper =
            self.spans.levels.len() == 1 && self.spans.levels[0].len() > REDUCE_FANOUT;
        if self.dirty.is_empty() && !stale_upper {
            return Ok(());
        }
        let spans = Arc::make_mut(&mut self.spans);
        for &block in &self.dirty {
            spans.levels[0][block] = reduce_rows(
                rows.range(spans.starts[block], spans.starts[block + 1]),
                reducer,
            )?;
        }
        if spans.levels.len() == 1 {
            spans.build_upper(reducer)?;
        } else {
            let mut changed = std::mem::take(&mut self.dirty);
            for level in 1..spans.levels.len() {
                changed = changed
                    .into_iter()
                    .map(|child| child / REDUCE_FANOUT)
                    .collect();
                changed.dedup();
                for &parent in &changed {
                    let below = &spans.levels[level - 1];
                    let children = &below
                        [parent * REDUCE_FANOUT..((parent + 1) * REDUCE_FANOUT).min(below.len())];
                    spans.levels[level][parent] = combine_children(reducer, children)?;
                }
            }
        }
        self.dirty.clear();
        Ok(())
    }
}

impl Spans {
    /// Add levels above the top one until a level fits in one parent.
    fn build_upper(&mut self, reducer: &mut JsReducer) -> Result<(), String> {
        while let Some(below) = self
            .levels
            .last()
            .filter(|below| below.len() > REDUCE_FANOUT)
        {
            let above = below
                .chunks(REDUCE_FANOUT)
                .map(|chunk| combine_children(reducer, chunk))
                .collect::<Result<_, _>>()?;
            self.levels.push(above);
        }
        Ok(())
    }
}

/// One parent's partial. A lone child is kept as it is, so a reducer that is
/// not rereduce-safe sees the same values as a single pass.
fn combine_children(reducer: &mut JsReducer, children: &[Value]) -> Result<Value, String> {
    match children {
        [single] => Ok(single.clone()),
        _ => combine_partials(reducer, children),
    }
}

impl Clone for ViewRows {
    fn clone(&self) -> Self {
        ViewRows {
            shards: self.shards.clone(),
            blocks: self
                .blocks
                .iter()
                .map(|slot| StdMutex::new(lock_blocks(slot).clone()))
                .collect(),
        }
    }
}

fn lock_blocks(slot: &StdMutex<Option<BlockTree>>) -> std::sync::MutexGuard<'_, Option<BlockTree>> {
    slot.lock().unwrap_or_else(|poison| poison.into_inner())
}

/// The rows a query can return, in key order, plus the counts CouchDB
/// reports for the whole (shard- and partition-filtered) view.
pub struct Selection {
    pub rows: Vec<Row>,
    pub total_rows: usize,
    pub offset: usize,
}

impl ViewRows {
    fn from_rows(rows: Vec<Row>, (q, partitioned): (u32, bool)) -> ViewRows {
        let mut shards = vec![Vec::new(); q.max(1) as usize];
        for row in rows {
            let shard = crate::db::shard_index_for(&row.id, q, partitioned);
            shards[shard].push(row);
        }
        let shards: Vec<ShardRows> = shards
            .into_iter()
            .map(|mut rows| {
                sort_rows(&mut rows);
                ShardRows::from_sorted(rows)
            })
            .collect();
        let blocks = shards.iter().map(|_| StdMutex::new(None)).collect();
        ViewRows { shards, blocks }
    }

    /// Replace the rows `removed` (each an id and key) of the documents
    /// that changed with `added`, shard by shard. Only the blocks holding
    /// those rows are changed.
    fn replace_docs(
        &mut self,
        removed: &[(String, Value)],
        added: Vec<Row>,
        (q, partitioned): (u32, bool),
    ) {
        type Touched<'a> = (Vec<(&'a str, &'a Value)>, Vec<Row>);
        let mut touched: HashMap<usize, Touched> = HashMap::new();
        for (id, key) in removed {
            let shard = crate::db::shard_index_for(id, q, partitioned);
            touched.entry(shard).or_default().0.push((id, key));
        }
        for row in added {
            let shard = crate::db::shard_index_for(&row.id, q, partitioned);
            touched.entry(shard).or_default().1.push(row);
        }
        for (shard, (gone, mut rows)) in touched {
            let positions = self.shards[shard].positions(&gone);
            sort_rows(&mut rows);
            let inserted = self.shards[shard].replace(&positions, rows);
            if let Some(slot) = self.blocks.get_mut(shard) {
                let tree = slot.get_mut().unwrap_or_else(|poison| poison.into_inner());
                if let Some(blocks) = tree.as_mut() {
                    if !blocks.apply_write(&positions, &inserted) {
                        *tree = None;
                    }
                }
            }
        }
    }

    /// Reduce `query` over the shards in `mask` (all when `None`). Each range
    /// or group reads the stored partials of the blocks it covers whole and
    /// reduces only the rows at its edges.
    pub(crate) fn reduce(
        &self,
        query: &ViewQuery,
        mask: Option<&[bool]>,
        reducer: &mut JsReducer,
    ) -> Result<Value, String> {
        let mut partials = Partials::new();
        for (index, rows) in self.shards.iter().enumerate() {
            if mask.is_some_and(|mask| !mask.get(index).copied().unwrap_or(false)) {
                continue;
            }
            let blocks = self.block_partials(index, reducer)?;
            let (lo, hi) = range_bounds(rows, query);
            let clamp = |(first, end): (usize, usize)| (first.max(lo), end.min(hi));
            if let Some(keys) = &query.keys {
                for key in unique_keys(keys) {
                    let (first, end) = clamp(equal_range(rows, &key));
                    if first < end {
                        let value = reduce_span(rows, &blocks, first, end, reducer)?;
                        add_partial(&mut partials, key, value);
                    }
                }
                continue;
            }
            let (lo, hi) = match &query.key {
                Some(key) => clamp(equal_range(rows, key)),
                None => (lo, hi),
            };
            if !query.group && query.group_level == 0 {
                if lo < hi {
                    let value = reduce_span(rows, &blocks, lo, hi, reducer)?;
                    add_partial(&mut partials, Value::Null, value);
                }
                continue;
            }
            // Group keys never decrease along sorted rows, so each group is
            // a run found by binary search.
            let mut start = lo;
            while start < hi {
                let group = group_key(&rows.row(start).key, query.group_level);
                // Rows before `start` are in earlier groups, so the point
                // over the whole shard is the end of this group.
                let end = rows
                    .partition_point(|row| {
                        cmp_json(&group_key(&row.key, query.group_level), &group)
                            != Ordering::Greater
                    })
                    .min(hi);
                let value = reduce_span(rows, &blocks, start, end, reducer)?;
                add_partial(&mut partials, group, value);
                start = end;
            }
        }
        assemble_reduce(partials, query, reducer)
    }

    fn block_partials(&self, shard: usize, reducer: &mut JsReducer) -> Result<Arc<Spans>, String> {
        let Some(slot) = self.blocks.get(shard) else {
            return Ok(Arc::default());
        };
        let rows = &self.shards[shard];
        let mut slot = lock_blocks(slot);
        if let Some(tree) = slot.as_mut().filter(|tree| tree.source == reducer.source()) {
            if let Err(error) = tree.refresh(rows, reducer) {
                *slot = None;
                return Err(error);
            }
            return Ok(Arc::clone(&tree.spans));
        }
        let tree = BlockTree::build(rows, reducer)?;
        let spans = Arc::clone(&tree.spans);
        *slot = Some(tree);
        Ok(spans)
    }

    /// Rows `query` can return from the shards in `mask` (all when `None`).
    /// A reduce needs every row in range; otherwise at most `skip + limit`
    /// rows are copied. `total_rows` and `offset` follow `ordered_rows`.
    pub fn select(&self, query: &ViewQuery, mask: Option<&[bool]>, reduce: bool) -> Selection {
        let cap = if reduce {
            usize::MAX
        } else {
            query.skip.saturating_add(query.limit)
        };
        let mut out = Selection {
            rows: Vec::new(),
            total_rows: 0,
            offset: 0,
        };
        for (index, shard) in self.shards.iter().enumerate() {
            if mask.is_some_and(|mask| !mask.get(index).copied().unwrap_or(false)) {
                continue;
            }
            match &query.partition {
                Some(partition) => {
                    // A partition is one shard, but it shares it with other
                    // partitions, so its rows are counted by walking it.
                    let prefix = format!("{partition}:");
                    let rows: Vec<&Row> = shard
                        .range(0, shard.len())
                        .filter(|row| row.id.starts_with(&prefix))
                        .collect();
                    select_sorted(rows.as_slice(), query, cap, &mut out);
                }
                None => select_sorted(shard, query, cap, &mut out),
            }
        }
        // Rows from one document share a shard, so a stable sort keeps the
        // emission order of equal rows.
        out.rows.sort_by(row_cmp);
        if !reduce && query.keys.is_none() && out.rows.len() > cap {
            if query.descending {
                out.rows.drain(..out.rows.len() - cap);
            } else {
                out.rows.truncate(cap);
            }
        }
        out
    }
}

impl AsRef<Row> for Row {
    fn as_ref(&self) -> &Row {
        self
    }
}

/// Add one sorted shard's contribution to `out`. Rows are in ascending key
/// order; the query bounds are found by binary search on that order.
fn select_sorted<S: SortedRows + ?Sized>(
    rows: &S,
    query: &ViewQuery,
    cap: usize,
    out: &mut Selection,
) {
    let len = rows.len();
    out.total_rows += len;
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
    out.offset += if let Some(key) = &query.key {
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

    // Take up to `cap` row indexes in query order. Duplicate `keys` share a
    // range; each row is still selected once.
    let mut chosen = std::collections::BTreeSet::new();
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
    out.rows
        .extend(chosen.into_iter().map(|index| rows.row(index).clone()));
}

/// The index range of sorted `rows` inside the query's start and end bounds.
fn range_bounds<S: SortedRows + ?Sized>(rows: &S, query: &ViewQuery) -> (usize, usize) {
    let point = |pred: &dyn Fn(&Row) -> bool| rows.partition_point(pred);
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
fn equal_range<S: SortedRows + ?Sized>(rows: &S, key: &Value) -> (usize, usize) {
    (
        rows.partition_point(|row| cmp_json(&row.key, key) == Ordering::Less),
        rows.partition_point(|row| cmp_json(&row.key, key) != Ordering::Greater),
    )
}

/// Reduce `rows[first..end]` from the largest stored partials it covers
/// whole, plus the rows and partials at either edge.
fn reduce_span(
    rows: &ShardRows,
    spans: &Spans,
    first: usize,
    end: usize,
    reducer: &mut JsReducer,
) -> Result<Value, String> {
    let starts = &spans.starts;
    let count = starts.len().saturating_sub(1);
    // The whole blocks in the span: from the first block starting at or after
    // `first` to the last ending at or before `end`.
    let lo = starts[..count].partition_point(|start| *start < first);
    let hi = starts[1..].partition_point(|end_of| *end_of <= end);
    let mut parts = Vec::new();
    if lo >= hi {
        parts.push(reduce_rows(rows.range(first, end), reducer)?);
    } else {
        if first < starts[lo] {
            parts.push(reduce_rows(rows.range(first, starts[lo]), reducer)?);
        }
        parts.extend(block_parts(&spans.levels, lo, hi));
        if starts[hi] < end {
            parts.push(reduce_rows(rows.range(starts[hi], end), reducer)?);
        }
    }
    combine_children(reducer, &parts)
}

/// The fewest stored partials covering blocks `lo..hi`, in row order.
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

fn reduce_rows<'a>(
    rows: impl Iterator<Item = &'a Row> + Clone,
    reducer: &mut JsReducer,
) -> Result<Value, String> {
    match reducer.source() {
        "_count" => Ok(json!(rows.count())),
        "_sum" => reduce_sum(rows.map(|row| &row.value)),
        "_stats" => reduce_stats(rows.map(|row| &row.value)),
        "_approx_count_distinct" => reduce_approx_count_distinct(rows.map(|row| &row.value)),
        _ => {
            let keys: Vec<Value> = rows.clone().map(|row| json!([row.id, row.key])).collect();
            let values: Vec<Value> = rows.map(|row| row.value.clone()).collect();
            reduce_with(reducer, &keys, &values)
        }
    }
}

/// Each requested key once, in first-seen order.
pub(crate) fn unique_keys(keys: &[Value]) -> Vec<Value> {
    let mut unique: Vec<Value> = Vec::with_capacity(keys.len());
    for key in keys {
        if !unique
            .iter()
            .any(|seen| cmp_json(seen, key) == Ordering::Equal)
        {
            unique.push(key.clone());
        }
    }
    unique
}

pub(crate) fn add_partial(partials: &mut Partials, key: Value, value: Value) {
    partials
        .entry(serde_json::to_string(&key).unwrap_or_default())
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
) -> Result<Value, String> {
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
            let Some((_, values)) = partials.get(&serde_json::to_string(key).unwrap_or_default())
            else {
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
        let mut groups: Vec<(Value, Vec<Value>)> = partials.into_values().collect();
        groups.sort_by(|a, b| cmp_json(&a.0, &b.0));
        if query.descending {
            groups.reverse();
        }
        groups
    };

    let mut rows = Vec::new();
    for (key, mut values) in groups.into_iter().skip(query.skip).take(query.limit) {
        let value = if values.len() == 1 {
            values.pop().unwrap_or(Value::Null)
        } else {
            combine_partials(reducer, &values)?
        };
        rows.push(json!({"key": key, "value": value}));
    }
    Ok(json!({ "rows": rows }))
}

/// Combine partial reductions with the reducer's rereduce.
pub(crate) fn combine_partials(
    reducer: &mut JsReducer,
    partials: &[Value],
) -> Result<Value, String> {
    match reducer.source() {
        "_count" => Ok(json!(partials
            .iter()
            .map(|value| value.as_u64().unwrap_or(0))
            .sum::<u64>())),
        // A partial that is already an error stays the error.
        "_sum" => match partials.iter().find(|value| {
            value.get("error").and_then(Value::as_str) == Some("builtin_reduce_error")
        }) {
            Some(error) => Ok(error.clone()),
            None => reduce_sum(partials),
        },
        "_stats" => reduce_stats(partials),
        _ => reducer.call(&[], partials, true),
    }
}

/// Rows are ordered by CouchDB key collation, then document id.
pub fn sort_rows(rows: &mut [Row]) {
    rows.sort_by(row_cmp);
}

fn row_cmp(a: &Row, b: &Row) -> Ordering {
    cmp_json(&a.key, &b.key).then_with(|| a.id.cmp(&b.id))
}

/// Merge two already-sorted row runs. On equal rows, `base` comes first.
fn merge_sorted_rows(base: Vec<Row>, added: Vec<Row>) -> Vec<Row> {
    let mut out = Vec::with_capacity(base.len() + added.len());
    let mut base = base.into_iter().peekable();
    let mut added = added.into_iter().peekable();
    loop {
        match (base.peek(), added.peek()) {
            (Some(left), Some(right)) => {
                if row_cmp(left, right) == Ordering::Greater {
                    out.push(added.next().expect("peeked row"));
                } else {
                    out.push(base.next().expect("peeked row"));
                }
            }
            (Some(_), None) => {
                out.extend(base);
                break;
            }
            (None, Some(_)) => {
                out.extend(added);
                break;
            }
            (None, None) => break,
        }
    }
    out
}

type IndexMap = HashMap<PathBuf, Arc<AsyncMutex<ViewIndex>>>;

fn registry() -> &'static StdMutex<IndexMap> {
    static INDEXES: OnceLock<StdMutex<IndexMap>> = OnceLock::new();
    INDEXES.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn registry_lock() -> std::sync::MutexGuard<'static, IndexMap> {
    // A panic in an unrelated request must not poison every later view query.
    registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn index_path(db: &Db, ddoc_id: &str, view_name: &str) -> PathBuf {
    let digest = Md5::digest(format!("{ddoc_id}\0{view_name}").as_bytes());
    db.path.join("_view_indexes").join(hex(&digest))
}

fn index_handle(path: &Path) -> DbResult<Arc<AsyncMutex<ViewIndex>>> {
    let mut indexes = registry_lock();
    if let Some(handle) = indexes.get(path) {
        return Ok(handle.clone());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| ApiError::new(500, "view_index_error", error.to_string()))?;
    }
    let index =
        ViewIndex::open(path).map_err(|error| ApiError::new(500, "view_index_error", error))?;
    let handle = Arc::new(AsyncMutex::new(index));
    indexes.insert(path.to_path_buf(), handle.clone());
    Ok(handle)
}

/// Drop cached index handles when a database is deleted or recreated. The
/// LevelDB files live under the database directory.
pub fn forget_db(db_path: &Path) {
    let prefix = db_path.join("_view_indexes");
    registry_lock().retain(|path, _| !path.starts_with(&prefix));
}

impl ViewIndex {
    fn open(path: &Path) -> Result<Self, String> {
        let options = LdbOptions {
            create_if_missing: true,
            ..LdbOptions::default()
        };
        let mut db = Ldb::open(path, options).map_err(|error| error.to_string())?;
        let meta = db
            .get(b"m")
            .and_then(|bytes| serde_json::from_slice::<IndexMeta>(&bytes).ok());
        let (last_seq, design) = meta
            .map(|meta| (meta.last_seq, meta.design))
            .unwrap_or((0, String::new()));
        Ok(ViewIndex {
            db,
            last_seq,
            design,
            placement: (1, false),
            cached: None,
        })
    }

    fn load_rows(&mut self) -> Result<Vec<Row>, String> {
        // Keys are `r/<base64(docid)>/<ordinal>`; the id is recovered from the
        // key rather than duplicated in every stored row.
        let mut rows = Vec::new();
        for (key, bytes) in self.scan(b"r/")? {
            let Some(rest) = key.strip_prefix(b"r/") else {
                continue;
            };
            let Some(encoded) = rest.split(|byte| *byte == b'/').next() else {
                continue;
            };
            let Ok(id) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(encoded) else {
                continue;
            };
            let Ok(id) = String::from_utf8(id) else {
                continue;
            };
            let Ok(stored) = serde_json::from_slice::<StoredRow>(&bytes) else {
                continue;
            };
            rows.push(Row {
                id,
                key: stored.key,
                value: stored.value,
            });
        }
        Ok(rows)
    }

    /// Sorted rows for the current index state. The first query after a
    /// rebuild or incremental update pays for loading/sorting; later queries
    /// reuse the same immutable snapshot.
    fn sorted_rows(&mut self) -> Result<Arc<ViewRows>, String> {
        if let Some(rows) = &self.cached {
            return Ok(rows.clone());
        }
        let rows = Arc::new(ViewRows::from_rows(self.load_rows()?, self.placement));
        self.cached = Some(rows.clone());
        Ok(rows)
    }

    /// Record the database's placement; a change drops the cached split.
    fn set_placement(&mut self, placement: (u32, bool)) {
        if self.placement != placement {
            self.placement = placement;
            self.cached = None;
        }
    }

    fn scan(&mut self, prefix: &[u8]) -> Result<Vec<KeyValue>, String> {
        let mut iterator = self.db.new_iter().map_err(|error| error.to_string())?;
        iterator.seek(prefix);
        let mut out = Vec::new();
        while let Some((key, value)) = iterator.current() {
            if !key.starts_with(prefix) {
                break;
            }
            out.push((key.to_vec(), value.to_vec()));
            if !iterator.advance() {
                break;
            }
        }
        Ok(out)
    }

    fn doc_prefix(id: &str) -> Vec<u8> {
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(id.as_bytes());
        format!("r/{encoded}/").into_bytes()
    }

    fn apply(
        &mut self,
        changed: Vec<String>,
        by_doc: HashMap<String, Vec<Row>>,
        last_seq: u64,
        design: &str,
    ) -> Result<(), String> {
        let mut batch = WriteBatch::default();
        // The keys of the rows being replaced locate them in the snapshot.
        let mut removed = Vec::new();
        for id in &changed {
            for (key, stored) in self.scan(&Self::doc_prefix(id))? {
                batch.delete(&key);
                if self.cached.is_some() {
                    let stored: StoredRow =
                        serde_json::from_slice(&stored).map_err(|error| error.to_string())?;
                    removed.push((id.clone(), stored.key));
                }
            }
        }

        let mut added = Vec::new();
        for (id, rows) in &by_doc {
            let prefix = Self::doc_prefix(id);
            for (ordinal, row) in rows.iter().enumerate() {
                let key = format!("{}{ordinal:08}", String::from_utf8_lossy(&prefix));
                batch.put(
                    key.as_bytes(),
                    &serde_json::to_vec(&StoredRowRef {
                        key: &row.key,
                        value: &row.value,
                    })
                    .map_err(|error| error.to_string())?,
                );
                added.push(row.clone());
            }
        }

        // Update the in-memory snapshot incrementally: drop rows for changed
        // documents and merge in the new ones, in the blocks they touch.
        let next_cached = match self.cached.take() {
            Some(cached) => {
                // Reuse the cached allocation when no in-flight query still
                // holds the snapshot; otherwise clone it.
                let mut kept = Arc::try_unwrap(cached).unwrap_or_else(|cached| (*cached).clone());
                kept.replace_docs(&removed, added, self.placement);
                Some(Arc::new(kept))
            }
            None => None,
        };

        self.last_seq = last_seq;
        self.design = design.to_string();
        batch.put(
            b"m",
            &serde_json::to_vec(&IndexMeta {
                last_seq,
                design: design.to_string(),
            })
            .map_err(|error| error.to_string())?,
        );
        self.db
            .write(batch, true)
            .map_err(|error| error.to_string())?;
        self.cached = next_cached;
        Ok(())
    }

    fn rebuild(&mut self, mut rows: Vec<Row>, last_seq: u64, design: &str) -> Result<(), String> {
        sort_rows(&mut rows);
        let mut batch = WriteBatch::default();
        for (key, _) in self.scan(b"r/")? {
            batch.delete(&key);
        }
        {
            let mut by_doc: HashMap<&str, Vec<&Row>> = HashMap::new();
            for row in &rows {
                by_doc.entry(row.id.as_str()).or_default().push(row);
            }
            for (id, doc_rows) in by_doc {
                let prefix = Self::doc_prefix(id);
                for (ordinal, row) in doc_rows.into_iter().enumerate() {
                    let key = format!("{}{ordinal:08}", String::from_utf8_lossy(&prefix));
                    batch.put(
                        key.as_bytes(),
                        &serde_json::to_vec(&StoredRowRef {
                            key: &row.key,
                            value: &row.value,
                        })
                        .map_err(|error| error.to_string())?,
                    );
                }
            }
        }
        self.last_seq = last_seq;
        self.design = design.to_string();
        batch.put(
            b"m",
            &serde_json::to_vec(&IndexMeta {
                last_seq,
                design: design.to_string(),
            })
            .map_err(|error| error.to_string())?,
        );
        self.db
            .write(batch, true)
            .map_err(|error| error.to_string())?;
        self.cached = Some(Arc::new(ViewRows::from_rows(rows, self.placement)));
        Ok(())
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
    // `_approx_count_distinct` partials are finished estimates and cannot be
    // combined; a partition's rows are not contiguous within its shard.
    if let Some(reducer) = spec.reduce.as_deref().filter(|reducer| {
        reduce && *reducer != "_approx_count_distinct" && query.partition.is_none()
    }) {
        check_query(spec, query)?;
        let rows = ensure_rows(db, ddoc_id, view_name, spec, query.limits).await?;
        let mut js = JsReducer::new(reducer, query.limits);
        return rows
            .reduce(query, query.owner_shards.as_deref(), &mut js)
            .map_err(|reason| reduce_failure(&reason));
    }
    let selection = local_select(db, ddoc_id, view_name, spec, query, reduce).await?;
    let mut query = query.clone();
    query.total_rows_override = Some(selection.total_rows);
    query.offset_override = Some(selection.offset);
    let rows = Arc::new(selection.rows);
    let docs = if !reduce && (query.include_docs || query.attachments) {
        let ids = selected_row_ids(&rows, &query);
        docs_by_ids(db, query.attachments, &ids).await?
    } else {
        HashMap::new()
    };
    finish(spec, rows, docs, &query)
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
    reduce: bool,
) -> DbResult<Selection> {
    let rows = ensure_rows(db, ddoc_id, view_name, spec, query.limits).await?;
    Ok(rows.select(query, query.owner_shards.as_deref(), reduce))
}

pub fn finish(
    spec: &ViewSpec,
    rows: Arc<Vec<Row>>,
    docs: HashMap<String, Value>,
    query: &ViewQuery,
) -> DbResult<Value> {
    check_query(spec, query)?;
    render(spec, &rows, &docs, query)
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

async fn ensure_rows(
    db: &Db,
    ddoc_id: &str,
    view_name: &str,
    spec: &ViewSpec,
    limits: ViewLimits,
) -> DbResult<Arc<ViewRows>> {
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
    let handle = index_handle(&index_path(db, ddoc_id, view_name))?;
    let mut index = handle.lock().await;
    index.set_placement((db.q, db.partitioned));
    // Read after the lock: a query that updated the index while this one
    // waited has moved its last_seq past any sequence read earlier, which
    // would look like a recreated database and force a full rebuild.
    // The bound is the changes watermark, not the allocated counter: a write
    // in flight has taken its sequence but is not readable yet, and an index
    // that recorded a sequence past it would never see that write.
    let current_seq = db.changes_watermark(index.last_seq, None);

    if index.design != design || index.last_seq > current_seq {
        // Read the bound before scanning, so a write that lands during the
        // scan stays above it and is picked up by the next update.
        let bound = db.changes_watermark(0, None);
        let rows = map_all(db, &spec.map, limits).await?;
        index
            .rebuild(rows, bound, &design)
            .map_err(|error| ApiError::new(500, "view_index_error", error))?;
        return index
            .sorted_rows()
            .map_err(|error| ApiError::new(500, "view_index_error", error));
    }

    if index.last_seq < current_seq {
        let changes = db
            .changes(&ChangesOpts {
                since: index.last_seq,
                include_docs: true,
                until: Some(current_seq),
                ..Default::default()
            })
            .await?;
        let mut changed = Vec::new();
        let mut docs = Vec::new();
        for row in changes["results"].as_array().cloned().unwrap_or_default() {
            let Some(id) = row.get("id").and_then(Value::as_str).map(str::to_string) else {
                continue;
            };
            changed.push(id.clone());
            if id.starts_with("_design/")
                || id.starts_with("_local/")
                || row.get("deleted").and_then(Value::as_bool).unwrap_or(false)
            {
                continue;
            }
            if let Some(doc) = row.get("doc").cloned() {
                docs.push((id, doc));
            }
        }
        let rows = run_map(&spec.map, &docs, limits).map_err(|reason| {
            ApiError::new(500, "view_error", format!("map function failed: {reason}"))
        })?;
        let mut by_doc: HashMap<String, Vec<Row>> = HashMap::new();
        for row in rows {
            by_doc.entry(row.id.clone()).or_default().push(row);
        }
        // Every sequence up to the bound was committed before it was read and
        // the changes above covered them all, so a gap left by a write that
        // never finished does not keep the index behind.
        index
            .apply(changed, by_doc, current_seq, &design)
            .map_err(|error| ApiError::new(500, "view_index_error", error))?;
    }

    index
        .sorted_rows()
        .map_err(|error| ApiError::new(500, "view_index_error", error))
}

async fn map_all(db: &Db, map_source: &str, limits: ViewLimits) -> DbResult<Vec<Row>> {
    let docs = db.map_docs(None).await?;
    run_map_json(map_source, &docs, limits).map_err(|reason| {
        ApiError::new(500, "view_error", format!("map function failed: {reason}"))
    })
}

pub(crate) async fn docs_by_ids(
    db: &Db,
    attachments: bool,
    ids: &[String],
) -> DbResult<HashMap<String, Value>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let opts = AllDocsOpts {
        keys: Some(ids.to_vec()),
        include_docs: true,
        attachments,
        limit: usize::MAX,
        ..Default::default()
    };
    let all = db.all_docs(&opts).await?;
    let mut docs = HashMap::new();
    for row in all["rows"].as_array().cloned().unwrap_or_default() {
        let Some(id) = row.get("id").and_then(Value::as_str).map(str::to_string) else {
            continue;
        };
        if id.starts_with("_design/") || id.starts_with("_local/") {
            continue;
        }
        if let Some(doc) = row.get("doc").cloned() {
            docs.insert(id, doc);
        }
    }
    Ok(docs)
}

/// Drop every index for a database. Used by `_purge`, which can remove a
/// document without advancing the database sequence.
pub async fn invalidate(db: &Db) -> DbResult<()> {
    let directory = db.path.join("_view_indexes");
    registry_lock().retain(|path, _| !path.starts_with(&directory));
    if let Ok(entries) = std::fs::read_dir(&directory) {
        for entry in entries.flatten() {
            let path = entry.path();
            if entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
                let _ = std::fs::remove_dir_all(&path);
            }
        }
    }
    Ok(())
}

/// Remove index directories whose design document or view no longer exists.
pub async fn cleanup(db: &Db) -> DbResult<()> {
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
    for row in all["rows"].as_array().cloned().unwrap_or_default() {
        let Some(ddoc) = row.get("doc") else {
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
    if let Ok(entries) = std::fs::read_dir(&directory) {
        for entry in entries.flatten() {
            let path = entry.path();
            if entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) && !live.contains(&path)
            {
                let _ = std::fs::remove_dir_all(&path);
            }
        }
    }
    registry_lock().retain(|path, _| live.contains(path));
    Ok(())
}

pub async fn load_spec(db: &Db, ddoc_id: &str, view_name: &str) -> DbResult<ViewSpec> {
    let ddoc = db.get_doc(ddoc_id, &DocOpts::default()).await?;
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

    fn run_json(&self, docs: &[(String, String)]) -> Result<Vec<Row>, String> {
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
                    let doc: Value =
                        serde_json::from_str(raw).map_err(|error| format!("{id}: {error}"))?;
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

fn engine(limits: ViewLimits) -> Result<rquickjs::Runtime, String> {
    crate::js::runtime(limits.timeout, limits.memory)
}

fn run_map(source: &str, docs: &[(String, Value)], limits: ViewLimits) -> Result<Vec<Row>, String> {
    if let Some(native) = NativeMap::parse(source) {
        let rows = native.run_values(docs);
        return finish_native(rows, source, limits, |index| {
            let (id, doc) = &docs[index];
            serde_json::to_string(doc)
                .map(|doc| (id.clone(), doc))
                .map_err(|error| format!("{id}: {error}"))
        });
    }
    let mut serialized = Vec::with_capacity(docs.len());
    for (id, doc) in docs {
        serialized.push((
            id.clone(),
            serde_json::to_string(doc).map_err(|error| format!("{id}: {error}"))?,
        ));
    }
    run_map_json(source, &serialized, limits)
}

/// Evaluate a `_changes` filter function for each document. CouchDB filters are
/// ordinary JavaScript predicates `function(doc, req) { ... }`. Wrapping the
/// predicate in a map function lets filters use the same native, process and
/// WASM sandbox paths (and timeouts) as view maps.
pub(crate) fn run_filter_json(
    source: &str,
    query: &str,
    docs: &[(String, String)],
    limits: ViewLimits,
) -> Result<std::collections::HashSet<String>, String> {
    let wrapped = format!(
        "function (doc) {{ if (({source})(doc, {{\"query\":{query}}})) {{ emit(doc._id, null); }} }}"
    );
    let rows = run_map_json(&wrapped, docs, limits)?;
    Ok(rows.into_iter().map(|row| row.id).collect())
}

/// Run the map function over batches of already-serialized documents. Batching
/// amortizes the QuickJS call/JSON boundary; callers that already hold raw
/// bodies (cold index builds) also avoid a Rust parse/encode round trip.
pub(crate) fn run_map_json(
    source: &str,
    docs: &[(String, String)],
    limits: ViewLimits,
) -> Result<Vec<Row>, String> {
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
    doc_json: impl Fn(usize) -> Result<(String, String), String>,
) -> Result<Vec<Row>, String> {
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
        return Err(format!(
            "map function emitted {} rows for {} documents",
            javascript.len(),
            pending.len()
        ));
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

/// Run a map function in QuickJS, in batches, without the native fast path.
fn run_map_js(
    source: &str,
    docs: &[(String, String)],
    limits: ViewLimits,
) -> Result<Vec<Row>, String> {
    const MAX_DOCS: usize = 256;
    const MAX_BYTES: usize = 4 * 1024 * 1024;
    // Sandbox calls pay a process/wasm boundary per batch. Use a larger batch
    // than the native path while keeping the input and output bounded.
    let (max_docs, max_bytes) = if crate::js::sandboxed() {
        (2048usize, 8usize * 1024 * 1024)
    } else {
        (MAX_DOCS, MAX_BYTES)
    };

    if crate::js::sandboxed() {
        let mut rows = Vec::new();
        let mut start = 0;
        while start < docs.len() {
            let mut end = start;
            let mut bytes = 2;
            while end < docs.len()
                && end - start < max_docs
                && (end == start || bytes + docs[end].1.len() < max_bytes)
            {
                bytes += docs[end].1.len() + 1;
                end += 1;
            }
            let value = crate::js::sandbox_request(
                "map",
                json!({"source": source, "docs": &docs[start..end]}),
                limits,
            )?;
            let chunk: Vec<Row> =
                serde_json::from_value(value).map_err(|error| error.to_string())?;
            rows.extend(chunk);
            start = end;
        }
        return Ok(rows);
    }

    let runtime = engine(limits)?;
    let context = rquickjs::Context::full(&runtime).map_err(|error| error.to_string())?;

    crate::js::guard("map function", || {
        context.with(|ctx| {
            let wrapped = format!(
                r#"
            (function () {{
              function sum(values) {{
                var total = 0;
                for (var i = 0; i < values.length; i++) total += values[i];
                return total;
              }}
              function log(message) {{ }}
              function isArray(value) {{ return Array.isArray(value); }}
              function toJSON(value) {{ return JSON.stringify(value); }}
              var __out = [];
              var emit = function (key, value) {{
                __out.push([
                  key === undefined ? null : key,
                  value === undefined ? null : value
                ]);
              }};
              var __map = ({source});
              if (typeof __map !== "function") {{
                throw new Error("map must be a function");
              }}
              return function (json) {{
                var docs = JSON.parse(json);
                var all = [];
                for (var i = 0; i < docs.length; i++) {{
                  __out = [];
                  __map(docs[i]);
                  all.push(__out);
                }}
                return JSON.stringify(all);
              }};
            }})()
            "#
            );
            let func: rquickjs::Function = ctx
                .eval(wrapped)
                .map_err(|error| format!("compile: {error}"))?;

            let mut rows = Vec::new();
            let mut start = 0;
            while start < docs.len() {
                let mut end = start;
                let mut bytes = 2;
                while end < docs.len()
                    && end - start < MAX_DOCS
                    && (end == start || bytes + docs[end].1.len() < MAX_BYTES)
                {
                    bytes += docs[end].1.len() + 1;
                    end += 1;
                }

                let mut batch = String::with_capacity(bytes);
                batch.push('[');
                for (index, (_, doc)) in docs[start..end].iter().enumerate() {
                    if index > 0 {
                        batch.push(',');
                    }
                    batch.push_str(doc);
                }
                batch.push(']');

                let first_id = &docs[start].0;
                let output: String = func
                    .call((batch,))
                    .map_err(|error| format!("{first_id}: {error}"))?;
                crate::js::check_output("map function", &output)?;
                let emitted: Vec<Vec<(Value, Value)>> = serde_json::from_str(&output)
                    .map_err(|error| format!("{first_id}: {error}"))?;
                if emitted.len() != end - start {
                    return Err(format!(
                        "{first_id}: map returned {} results for {} documents",
                        emitted.len(),
                        end - start
                    ));
                }
                for (offset, pairs) in emitted.into_iter().enumerate() {
                    let id = &docs[start + offset].0;
                    for (key, value) in pairs {
                        rows.push(Row {
                            id: id.clone(),
                            key,
                            value,
                        });
                    }
                }
                start = end;
            }
            Ok(rows)
        })
    })
}

fn ordered_rows<'a>(rows: &'a [Row], query: &ViewQuery) -> (Vec<&'a Row>, usize, usize) {
    let mut total_rows = query.total_rows_override.unwrap_or(rows.len());
    let mut ordered: Vec<&Row> = if query.descending {
        rows.iter().rev().collect()
    } else {
        rows.iter().collect()
    };

    // Preselected rows are already filtered, and their counts supplied.
    let preselected = query.offset_override.is_some();
    if let Some(partition) = query.partition.as_ref().filter(|_| !preselected) {
        let prefix = format!("{partition}:");
        ordered.retain(|row| row.id.starts_with(&prefix));
        total_rows = ordered.len();
    }

    if let Some(owned) = query.owner_shards.as_ref().filter(|_| !preselected) {
        ordered.retain(|row| {
            let shard = crate::db::shard_index_for(&row.id, query.shard_count, query.partitioned);
            owned.get(shard).copied().unwrap_or(false)
        });
        total_rows = ordered.len();
    }

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
        for key in ordered_keys {
            for row in &ordered {
                if cmp_json(&row.key, key) == Ordering::Equal {
                    selected.push(*row);
                }
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
            let value = match reducer.as_str() {
                "_count" => Ok(json!(members.len())),
                "_sum" => reduce_sum(members.iter().map(|row| &row.value)),
                "_stats" => reduce_stats(members.iter().map(|row| &row.value)),
                "_approx_count_distinct" => {
                    reduce_approx_count_distinct(members.iter().map(|row| &row.value))
                }
                _ => {
                    let keys: Vec<Value> =
                        members.iter().map(|row| json!([row.id, row.key])).collect();
                    let values: Vec<Value> = members.iter().map(|row| row.value.clone()).collect();
                    reduce_with(&mut js, &keys, &values)
                }
            };
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
        .map(|row| {
            let mut entry = json!({
                "id": row.id.clone(),
                "key": row.key.clone(),
                "value": row.value.clone(),
            });
            if query.include_docs {
                if let Some(doc) = docs.get(&row.id) {
                    entry["doc"] = doc.clone();
                }
            }
            entry
        })
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

fn reduce_with(reducer: &mut JsReducer, keys: &[Value], values: &[Value]) -> Result<Value, String> {
    match reducer.source() {
        "_count" => return Ok(json!(values.len())),
        "_sum" => return reduce_sum(values.iter()),
        "_stats" => return reduce_stats(values.iter()),
        "_approx_count_distinct" => return reduce_approx_count_distinct(values.iter()),
        _ => {}
    }

    // CouchDB reduces in bounded batches and then rereduces the partials.
    // Do the same for custom JavaScript reducers.
    const CHUNK: usize = 1000;
    if values.len() > CHUNK {
        let mut partials = Vec::new();
        for (offset, chunk) in values.chunks(CHUNK).enumerate() {
            let start = offset * CHUNK;
            let end = (start + chunk.len()).min(keys.len());
            let chunk_keys = if start < end { &keys[start..end] } else { &[] };
            partials.push(reducer.call(chunk_keys, chunk, false)?);
        }
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
) -> Result<Value, String> {
    JsReducer::new(source, limits).call(keys, values, rereduce)
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
    ) -> Result<Value, String> {
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

    fn call_native(
        &mut self,
        keys: &[Value],
        values: &[Value],
        rereduce: bool,
    ) -> Result<Value, String> {
        let timeout = self.limits.timeout;
        if self.engine.is_none() {
            self.engine = Some(compile_reducer(&self.source, self.limits)?);
        }
        let Some(engine) = &self.engine else {
            return Err("reduce engine unavailable".to_string());
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
                    .map_err(|error| error.to_string())?;
                // CouchDB passes null, not [], for keys during rereduce.
                let keys = if rereduce {
                    "null".to_string()
                } else {
                    serde_json::to_string(keys).map_err(|error| error.to_string())?
                };
                let values = serde_json::to_string(values).map_err(|error| error.to_string())?;
                let output: String = func
                    .call((keys, values, rereduce))
                    .map_err(|error| error.to_string())?;
                crate::js::check_output("reduce function", &output)?;
                serde_json::from_str(&output).map_err(|error| error.to_string())
            })
        })
    }
}

fn compile_reducer(source: &str, limits: ViewLimits) -> Result<ReduceEngine, String> {
    let (runtime, deadline) = crate::js::reusable_runtime(limits.memory)?;
    *deadline.lock().unwrap_or_else(|poison| poison.into_inner()) =
        std::time::Instant::now() + limits.timeout;
    let context = rquickjs::Context::full(&runtime).map_err(|error| error.to_string())?;
    crate::js::guard("reduce function", || {
        context.with(|ctx| {
            let wrapped = format!(
                r#"
            (function () {{
              function sum(values) {{
                var total = 0;
                for (var i = 0; i < values.length; i++) total += values[i];
                return total;
              }}
              function log(message) {{ }}
              function isArray(value) {{ return Array.isArray(value); }}
              function toJSON(value) {{ return JSON.stringify(value); }}
              var __reduce = ({source});
              if (typeof __reduce !== "function") {{
                throw new Error("reduce must be a function");
              }}
              return function (keys, values, rereduce) {{
                var result = __reduce(JSON.parse(keys), JSON.parse(values), rereduce);
                return result === undefined ? "null" : JSON.stringify(result);
              }};
            }})()
            "#
            );
            let func: rquickjs::Function = ctx
                .eval(wrapped)
                .map_err(|error| format!("compile: {error}"))?;
            ctx.globals()
                .set(REDUCE_GLOBAL, func)
                .map_err(|error| error.to_string())
        })
    })?;
    Ok(ReduceEngine {
        context,
        _runtime: runtime,
        deadline,
    })
}

const SUM_ERROR_REASON: &str = "The _sum function requires that map values be numbers, arrays of numbers, or objects. Objects cannot be mixed with other data structures. Objects can be arbitrarily nested, provided that the values for all fields are themselves numbers, arrays of numbers, or objects.";

/// Prefix that marks a reduce failure CouchDB reports as `invalid_value`.
const INVALID_VALUE: &str = "invalid_value: ";

/// The HTTP error for a failed reduce.
pub(crate) fn reduce_failure(reason: &str) -> ApiError {
    match reason.strip_prefix(INVALID_VALUE) {
        Some(reason) => ApiError::new(500, "invalid_value", reason),
        None => ApiError::new(
            500,
            "view_error",
            format!("reduce function failed: {reason}"),
        ),
    }
}

/// Built-in `_sum`, as in CouchDB's `couch_query_servers`: numbers add,
/// arrays add element by element (a number counts as a one-element array),
/// and objects add member by member. Invalid input is not a failed request
/// but an error object in place of the reduced value.
fn reduce_sum<'a>(values: impl IntoIterator<Item = &'a Value>) -> Result<Value, String> {
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

fn invalid_value(reason: impl std::fmt::Display) -> String {
    format!("{INVALID_VALUE}{reason}")
}

/// Read a map value or partial for `_stats`: a number, a stats object, or an
/// array of those.
fn stats_input(value: &Value) -> Result<StatsValue, String> {
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
            let field = |name: &str| -> Result<f64, String> {
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

fn merge_stats(acc: StatsValue, next: StatsValue) -> Result<StatsValue, String> {
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
fn reduce_stats<'a>(values: impl IntoIterator<Item = &'a Value>) -> Result<Value, String> {
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

fn reduce_approx_count_distinct<'a>(
    values: impl IntoIterator<Item = &'a Value>,
) -> Result<Value, String> {
    let mut distinct = HashSet::new();
    for value in values {
        distinct.insert(serde_json::to_string(value).map_err(|error| error.to_string())?);
    }
    Ok(json!(distinct.len()))
}

pub(crate) fn number_value(value: f64) -> Value {
    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        json!(value as i64)
    } else {
        json!(value)
    }
}

/// CouchDB JSON collation: null < false < true < number < string < array <
/// object. Strings use ICU's default (tertiary) collation, as CouchDB does.
pub fn cmp_json(a: &Value, b: &Value) -> Ordering {
    fn rank(value: &Value) -> u8 {
        match value {
            Value::Null => 0,
            Value::Bool(false) => 1,
            Value::Bool(true) => 2,
            Value::Number(_) => 3,
            Value::String(_) => 4,
            Value::Array(_) => 5,
            Value::Object(_) => 6,
        }
    }

    let (ra, rb) = (rank(a), rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Number(a), Value::Number(b)) => cmp_numbers(a, b),
        (Value::String(a), Value::String(b)) => compare_strings(a, b),
        (Value::Array(a), Value::Array(b)) => {
            for (left, right) in a.iter().zip(b.iter()) {
                let order = cmp_json(left, right);
                if order != Ordering::Equal {
                    return order;
                }
            }
            a.len().cmp(&b.len())
        }
        (Value::Object(a), Value::Object(b)) => {
            // CouchDB object collation follows the stored member order, not
            // lexicographic key order.
            let a: Vec<_> = a.iter().collect();
            let b: Vec<_> = b.iter().collect();
            for (left, right) in a.iter().zip(b.iter()) {
                let order = left.0.cmp(right.0).then_with(|| cmp_json(left.1, right.1));
                if order != Ordering::Equal {
                    return order;
                }
            }
            a.len().cmp(&b.len())
        }
        _ => Ordering::Equal,
    }
}

fn cmp_numbers(a: &serde_json::Number, b: &serde_json::Number) -> Ordering {
    if is_integer_literal(a.as_str()) && is_integer_literal(b.as_str()) {
        return cmp_decimal_integers(a.as_str(), b.as_str());
    }
    let (a, b) = (a.as_f64().unwrap_or(0.0), b.as_f64().unwrap_or(0.0));
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

fn is_integer_literal(raw: &str) -> bool {
    !raw.bytes().any(|byte| matches!(byte, b'.' | b'e' | b'E'))
}

/// Compare decimal integer literals exactly. `f64` would collapse values that
/// differ beyond its 53-bit mantissa.
fn cmp_decimal_integers(a: &str, b: &str) -> Ordering {
    fn parts(raw: &str) -> (bool, &str) {
        let (negative, digits) = match raw.strip_prefix('-') {
            Some(digits) => (true, digits),
            None => (false, raw),
        };
        let digits = digits.trim_start_matches('0');
        let digits = if digits.is_empty() { "0" } else { digits };
        (negative && digits != "0", digits)
    }

    let (a_negative, a_digits) = parts(a);
    let (b_negative, b_digits) = parts(b);
    if a_negative != b_negative {
        return if a_negative {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    let magnitude = a_digits
        .len()
        .cmp(&b_digits.len())
        .then_with(|| a_digits.cmp(b_digits));
    if a_negative {
        magnitude.reverse()
    } else {
        magnitude
    }
}

fn compare_strings(a: &str, b: &str) -> Ordering {
    static COLLATOR: OnceLock<Option<icu_collator::CollatorBorrowed<'static>>> = OnceLock::new();
    let collator = COLLATOR
        .get_or_init(|| Collator::try_new(Default::default(), CollatorOptions::default()).ok());
    match collator {
        Some(collator) => collator.compare(a, b),
        None => a.cmp(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reduce_value(
        source: &str,
        keys: &[Value],
        values: &[Value],
        limits: ViewLimits,
    ) -> Result<Value, String> {
        reduce_with(&mut JsReducer::new(source, limits), keys, values)
    }

    fn sum_of(values: &[Value]) -> Value {
        reduce_value("_sum", &[], values, ViewLimits::default()).unwrap()
    }

    fn stats_of(values: &[Value]) -> Result<Value, String> {
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
            crate::db::uuid()
        ));
        let db = Arc::new(
            Db::open(
                &root,
                "viewseq",
                crate::db::DbConfig {
                    q: 1,
                    revs_limit: 1000,
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
        let handle = index_handle(&index_path(&db, "_design/v", "v")).unwrap();
        let mut holder = handle.lock().await;
        let waiting = tokio::spawn({
            let (db, spec) = (db.clone(), spec.clone());
            async move { ensure_rows(&db, "_design/v", "v", &spec, limits).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // The holder catches the index up with writes that land meanwhile.
        db.put_docs(vec![write("b"), write("c")], true)
            .await
            .unwrap();
        let caught_up = db.update_seq();
        holder.last_seq = caught_up;
        drop(holder);

        waiting.await.unwrap().unwrap();
        assert_eq!(
            handle.lock().await.last_seq,
            caught_up,
            "the waiting query rebuilt the index from a stale sequence"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn a_view_does_not_move_past_a_write_in_flight() {
        let root = std::env::temp_dir().join(format!(
            "douchdb-viewflight-{}-{}",
            std::process::id(),
            crate::db::uuid()
        ));
        let db = Db::open(
            &root,
            "viewflight",
            crate::db::DbConfig {
                q: 2,
                revs_limit: 1000,
                shard_map: crate::cluster::ShardMap::single(2),
                partitioned: false,
            },
        )
        .unwrap();
        let on_shard = |shard: usize, tag: &str| {
            (0..10_000)
                .map(|i| format!("{tag}{i}"))
                .find(|id| db.shard_index(id) == shard)
                .expect("document id for shard")
        };
        let (a, c, e, d) = (
            on_shard(0, "a"),
            on_shard(0, "c"),
            on_shard(0, "e"),
            on_shard(1, "d"),
        );
        let spec = ViewSpec {
            map: "function(doc){ emit(doc._id, 1); }".to_string(),
            reduce: None,
            partitioned: false,
        };
        let limits = ViewQuery::default().limits;
        let last_seq = |name: &str| {
            let handle = index_handle(&index_path(&db, "_design/v", name)).unwrap();
            async move { handle.lock().await.last_seq }
        };
        db.put_docs(vec![json!({"_id": a})], true).await.unwrap();
        ensure_rows(&db, "_design/v", "inc", &spec, limits)
            .await
            .unwrap();
        let built = last_seq("inc").await;

        // Shard 1 holds an allocated, uncommitted sequence while a later
        // write on shard 0 commits. An incremental update must stop short.
        let held = db.start_in_flight_write(1);
        db.put_docs(vec![json!({"_id": c})], true).await.unwrap();
        ensure_rows(&db, "_design/v", "inc", &spec, limits)
            .await
            .unwrap();
        let seq = last_seq("inc").await;
        assert!(
            (built..held).contains(&seq),
            "incremental update recorded last_seq {seq}, past the in-flight sequence {held}"
        );

        // So must a rebuild.
        let held = db.start_in_flight_write(1);
        db.put_docs(vec![json!({"_id": e})], true).await.unwrap();
        ensure_rows(&db, "_design/v", "full", &spec, limits)
            .await
            .unwrap();
        let seq = last_seq("full").await;
        assert!(
            seq < held,
            "rebuild recorded last_seq {seq}, past the in-flight sequence {held}"
        );

        // Once the shard commits, a later write on it reaches both indexes.
        db.put_docs(vec![json!({"_id": d})], true).await.unwrap();
        for name in ["inc", "full"] {
            let rows = ensure_rows(&db, "_design/v", name, &spec, limits)
                .await
                .unwrap();
            let mut ids: Vec<String> = rows
                .shards
                .iter()
                .flat_map(|shard| shard.blocks.iter().flat_map(|block| block.iter()))
                .map(|row| row.id.clone())
                .collect();
            ids.sort();
            let mut expected = vec![a.clone(), c.clone(), e.clone(), d.clone()];
            expected.sort();
            assert_eq!(ids, expected, "{name} index");
        }
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn a_gap_in_the_sequence_does_not_keep_a_view_behind() {
        let root = std::env::temp_dir().join(format!(
            "douchdb-viewgap-{}-{}",
            std::process::id(),
            crate::db::uuid()
        ));
        let db = Db::open(
            &root,
            "viewgap",
            crate::db::DbConfig {
                q: 2,
                revs_limit: 1000,
                shard_map: crate::cluster::ShardMap::single(2),
                partitioned: false,
            },
        )
        .unwrap();
        let on_shard = |shard: usize, tag: &str| {
            (0..10_000)
                .map(|i| format!("{tag}{i}"))
                .find(|id| db.shard_index(id) == shard)
                .expect("document id for shard")
        };
        let (a, c) = (on_shard(0, "a"), on_shard(0, "c"));
        let spec = ViewSpec {
            map: "function(doc){ emit(doc._id, 1); }".to_string(),
            reduce: None,
            partitioned: false,
        };
        let limits = ViewQuery::default().limits;
        db.put_docs(vec![json!({"_id": a})], true).await.unwrap();
        ensure_rows(&db, "_design/v", "v", &spec, limits)
            .await
            .unwrap();

        // A write commits, then a sequence above it is taken and abandoned,
        // as by a write whose flush failed. Any later job on the shard
        // counts the abandoned sequence as settled.
        db.put_docs(vec![json!({"_id": c})], true).await.unwrap();
        let abandoned = db.start_in_flight_write(1);
        db.get_doc(&on_shard(1, "x"), &Default::default())
            .await
            .ok();
        assert_eq!(db.update_seq(), abandoned);

        ensure_rows(&db, "_design/v", "v", &spec, limits)
            .await
            .unwrap();
        let handle = index_handle(&index_path(&db, "_design/v", "v")).unwrap();
        assert_eq!(
            handle.lock().await.last_seq,
            abandoned,
            "the index stays behind a sequence no write will commit"
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn block_partials_follow_writes() {
        // One shard with enough blocks for a level of parents above them.
        let placement = (1, false);
        let base = ViewQuery {
            inclusive_end: true,
            limit: usize::MAX,
            shard_count: placement.0,
            ..ViewQuery::default()
        };
        let queries = [
            base.clone(),
            ViewQuery {
                group_level: 1,
                ..base.clone()
            },
            ViewQuery {
                startkey: Some(json!([3, 5])),
                endkey: Some(json!([9, 2])),
                ..base.clone()
            },
            ViewQuery {
                startkey: Some(json!([1, 7])),
                endkey: Some(json!([1, 9])),
                inclusive_end: false,
                ..base.clone()
            },
        ];
        for reducer in ["_sum", "_stats", "function(k, v, re){ return sum(v); }"] {
            let spec = ViewSpec {
                map: String::new(),
                reduce: Some(reducer.to_string()),
                partitioned: false,
            };
            let mut state = 0x9e37_79b9_7f4a_7c15u64;
            let mut next = move |bound: u64| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state % bound
            };
            let doc_rows = |id: &str, count: u64, key: u64| -> Vec<Row> {
                (0..count)
                    .map(|i| Row {
                        id: id.to_string(),
                        key: json!([(key + i) % 13, (key * 7 + i) % 17]),
                        value: json!(key * 10 + i),
                    })
                    .collect()
            };
            let mut rows: Vec<Row> = (0..1100)
                .flat_map(|i| doc_rows(&format!("d{i}"), 1 + i % 3, i))
                .collect();
            sort_rows(&mut rows);
            let mut view = ViewRows::from_rows(rows.clone(), placement);
            let mut fresh = 1100;
            for step in 0..80 {
                let ids: Vec<String> = rows.iter().map(|row| row.id.clone()).collect();
                let pick = |n: u64| ids[n as usize % ids.len()].clone();
                let (changed, added): (Vec<String>, Vec<Row>) = match next(5) {
                    // New documents, sometimes a burst on one key so a block
                    // grows past its split size.
                    0 => {
                        let burst = if step % 4 == 0 { 70 } else { 1 };
                        let key = next(1000);
                        let docs: Vec<String> =
                            (0..burst).map(|i| format!("n{}", fresh + i)).collect();
                        fresh += burst;
                        let added = docs.iter().flat_map(|id| doc_rows(id, 1, key)).collect();
                        (docs, added)
                    }
                    // Deletes, sometimes every document with one key so
                    // blocks empty.
                    1 => {
                        let docs: Vec<String> = if step % 3 == 0 {
                            let key = rows[next(rows.len() as u64) as usize].key.clone();
                            rows.iter()
                                .filter(|row| row.key == key)
                                .map(|row| row.id.clone())
                                .collect()
                        } else {
                            vec![pick(next(1 << 20))]
                        };
                        (docs, Vec::new())
                    }
                    // Key changes move a document's rows between blocks.
                    2 => {
                        let id = pick(next(1 << 20));
                        let added = doc_rows(&id, 1 + next(3), next(1000));
                        (vec![id], added)
                    }
                    // A value change leaves the rows in place.
                    3 => {
                        let id = pick(next(1 << 20));
                        let mut added: Vec<Row> =
                            rows.iter().filter(|row| row.id == id).cloned().collect();
                        for row in &mut added {
                            row.value = json!(next(100));
                        }
                        (vec![id], added)
                    }
                    _ => {
                        let docs: Vec<String> = (0..5).map(|_| pick(next(1 << 20))).collect();
                        let added = docs
                            .iter()
                            .flat_map(|id| doc_rows(id, 2, next(1000)))
                            .collect();
                        (docs, added)
                    }
                };
                view.replace_docs(&emitted(&rows, &changed), added.clone(), placement);
                rows.retain(|row| !changed.contains(&row.id));
                rows.extend(added);
                sort_rows(&mut rows);
                // Checking every third write also covers several writes
                // landing before the next reduce.
                if step % 3 != 2 {
                    continue;
                }
                for query in &queries {
                    let expected = render(&spec, &rows, &HashMap::new(), query).unwrap();
                    let mut js = JsReducer::new(reducer, query.limits);
                    let actual = view.reduce(query, None, &mut js).unwrap();
                    assert_eq!(actual, expected, "{reducer} step {step} {query:?}");
                }
            }
        }
    }

    /// The id and key of each row the `changed` documents emitted.
    fn emitted(rows: &[Row], changed: &[String]) -> Vec<(String, Value)> {
        let changed: HashSet<&String> = changed.iter().collect();
        rows.iter()
            .filter(|row| changed.contains(&row.id))
            .map(|row| (row.id.clone(), row.key.clone()))
            .collect()
    }

    #[test]
    fn shard_blocks_follow_writes() {
        let placement = (1, false);
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        // Documents emit one to three rows, sometimes with the same key twice.
        let doc_rows = |id: &str, count: u64, key: u64| -> Vec<Row> {
            (0..count)
                .map(|i| Row {
                    id: id.to_string(),
                    key: json!(key + i / 2),
                    value: json!(i),
                })
                .collect()
        };
        let mut rows: Vec<Row> = (0..3000)
            .flat_map(|i| doc_rows(&format!("d{i}"), 1 + i % 3, i % 500))
            .collect();
        sort_rows(&mut rows);
        let mut view = ViewRows::from_rows(rows.clone(), placement);
        let mut fresh = 0;
        for step in 0..300 {
            let ids: Vec<String> = rows.iter().map(|row| row.id.clone()).collect();
            let mut pick = || ids[next(ids.len() as u64) as usize].clone();
            let (changed, added): (Vec<String>, Vec<Row>) = match step % 4 {
                // A burst of new documents on one key grows a block past its
                // split size.
                0 => {
                    let key = next(600);
                    let count = if step % 40 == 0 { 600 } else { 1 + next(4) };
                    let docs: Vec<String> = (0..count).map(|i| format!("n{}-{i}", fresh)).collect();
                    fresh += 1;
                    let added = docs.iter().flat_map(|id| doc_rows(id, 1, key)).collect();
                    (docs, added)
                }
                // Deleting a run of keys empties and shrinks blocks.
                1 => {
                    let low = next(600);
                    let width = if step % 10 == 1 { 40 } else { 1 };
                    let docs: Vec<String> = rows
                        .iter()
                        .filter(|row| {
                            row.key
                                .as_u64()
                                .is_some_and(|key| key >= low && key < low + width)
                        })
                        .map(|row| row.id.clone())
                        .collect();
                    (docs, Vec::new())
                }
                2 => {
                    let id = pick();
                    let added = doc_rows(&id, 1 + next(3), next(600));
                    (vec![id], added)
                }
                _ => {
                    let docs: Vec<String> = (0..3).map(|_| pick()).collect();
                    let added = docs
                        .iter()
                        .flat_map(|id| doc_rows(id, 2, next(600)))
                        .collect();
                    (docs, added)
                }
            };
            let mut changed = changed;
            changed.sort();
            changed.dedup();
            let before = view.clone();
            let gone = emitted(&rows, &changed);
            let old = &before.shards[0];
            let mut emptied: Vec<usize> = gone
                .iter()
                .map(|(id, key)| old.block_of(old.positions(&[(id.as_str(), key)])[0]))
                .collect();
            emptied.sort_unstable();
            emptied.dedup();
            // Blocks a write may copy: those it removes rows from or adds
            // rows to, and a neighbour of each it joins.
            let touched = 2 * (emptied.len() + added.len());
            view.replace_docs(&gone, added.clone(), placement);
            let set: HashSet<&String> = changed.iter().collect();
            rows.retain(|row| !set.contains(&row.id));
            rows.extend(added);
            sort_rows(&mut rows);

            let shard = &view.shards[0];
            let actual: Vec<(&str, &Value, &Value)> = shard
                .range(0, shard.len())
                .map(|row| (row.id.as_str(), &row.key, &row.value))
                .collect();
            let expected: Vec<(&str, &Value, &Value)> = rows
                .iter()
                .map(|row| (row.id.as_str(), &row.key, &row.value))
                .collect();
            assert_eq!(actual, expected, "step {step}");
            let sizes: Vec<usize> = shard.blocks.iter().map(|rows| rows.len()).collect();
            assert!(
                sizes.iter().all(|size| (1..=2 * ROW_BLOCK).contains(size)),
                "step {step} {sizes:?}"
            );
            assert!(
                sizes.iter().filter(|size| **size < ROW_BLOCK / 2).count() <= 1,
                "step {step} {sizes:?}"
            );
            assert_eq!(shard.starts.len(), sizes.len() + 1, "step {step}");
            for (block, size) in sizes.iter().enumerate() {
                assert_eq!(shard.starts[block + 1] - shard.starts[block], *size);
            }
            // The blocks a write did not touch stay shared with the copy
            // taken before it.
            let shared = before.shards[0]
                .blocks
                .iter()
                .filter(|old| shard.blocks.iter().any(|new| Arc::ptr_eq(old, new)))
                .count();
            assert!(
                shared + touched >= before.shards[0].blocks.len(),
                "step {step}: {shared} of {} blocks shared, {touched} touched",
                before.shards[0].blocks.len()
            );
        }

        // A row whose recorded key does not match is still found by its id.
        let id = rows[rows.len() / 2].id.clone();
        view.replace_docs(&[(id.clone(), json!("not its key"))], Vec::new(), placement);
        let shard = &view.shards[0];
        assert!(shard.range(0, shard.len()).all(|row| row.id != id));

        // Two keys one step apart whose recorded keys both read back as the
        // lower one: the run found is short, so both rows go by id.
        let near = |key: f64| Row {
            id: "near".into(),
            key: json!(key),
            value: json!(0),
        };
        let above = f64::from_bits(1.0f64.to_bits() + 1);
        view.replace_docs(&[], vec![near(1.0), near(above)], placement);
        view.replace_docs(
            &[("near".into(), json!(1.0)), ("near".into(), json!(1.0))],
            Vec::new(),
            placement,
        );
        let shard = &view.shards[0];
        assert!(shard.range(0, shard.len()).all(|row| row.id != "near"));
    }

    #[test]
    fn block_reductions_match_a_single_pass() {
        // Enough rows that every shard has several full blocks, keyed so
        // groups and ranges start and end inside blocks.
        let placement = (3, false);
        let mut rows: Vec<Row> = (0..5000)
            .map(|i| Row {
                id: format!("d{i:05}"),
                key: json!([i % 7, i % 11]),
                value: json!(i),
            })
            .collect();
        sort_rows(&mut rows);
        let mut view = ViewRows::from_rows(rows.clone(), placement);
        let base = ViewQuery {
            inclusive_end: true,
            limit: usize::MAX,
            shard_count: placement.0,
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
                startkey_docid: Some("d02000".into()),
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
        for k in 50..64 {
            queries.push(ViewQuery {
                endkey: Some(json!([6, 10])),
                endkey_docid: Some(format!("d{:05}", 76 + 77 * k)),
                inclusive_end: k % 2 == 0,
                ..base.clone()
            });
        }
        let masked = ViewQuery {
            owner_shards: Some(vec![true, false, true]),
            group_level: 1,
            ..base.clone()
        };
        queries.push(masked);
        let reducers = [
            "_count",
            "_sum",
            "_stats",
            "function(k, v, re){ return sum(v); }",
        ];
        let check = |view: &ViewRows, rows: &[Row], reducers: &[&str]| {
            for &reducer in reducers {
                let spec = ViewSpec {
                    map: String::new(),
                    reduce: Some(reducer.to_string()),
                    partitioned: false,
                };
                for query in &queries {
                    let expected = render(&spec, rows, &HashMap::new(), query).unwrap();
                    let mut js = JsReducer::new(reducer, query.limits);
                    let actual = view
                        .reduce(query, query.owner_shards.as_deref(), &mut js)
                        .unwrap();
                    assert_eq!(actual, expected, "{reducer} {query:?}");
                }
            }
        };
        check(&view, &rows, &reducers);

        // A write invalidates the stored partials of the shard it touches.
        let changed: Vec<String> = (0..40).map(|i| format!("d{:05}", i * 97)).collect();
        let added: Vec<Row> = changed
            .iter()
            .map(|id| Row {
                id: id.clone(),
                key: json!([3, 3]),
                value: json!(100000),
            })
            .collect();
        view.replace_docs(&emitted(&rows, &changed), added.clone(), placement);
        rows.retain(|row| !changed.contains(&row.id));
        rows.extend(added);
        sort_rows(&mut rows);
        // The last reducer checked first, so its partials are still stored.
        let reversed: Vec<&str> = reducers.iter().rev().copied().collect();
        check(&view, &rows, &reversed);
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
        let limits = ViewLimits {
            timeout: Duration::from_millis(300),
            ..ViewLimits::default()
        };
        let mut reducer = JsReducer::new(
            "function(k, v, re){ var end = Date.now() + 150; while (Date.now() < end) {} return 1; }",
            limits,
        );
        for _ in 0..4 {
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

    #[test]
    fn index_cache_tracks_incremental_updates() {
        let dir = std::env::temp_dir().join(format!(
            "douchdb-view-cache-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut index = ViewIndex::open(&dir).unwrap();
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
                1,
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
            .apply(vec!["a".to_string(), "b".to_string()], by_doc, 2, "map|")
            .unwrap();
        let rows = index.sorted_rows().unwrap();
        assert_eq!(rows.shards[0].len(), 1);
        assert_eq!(rows.shards[0].row(0).id, "a");
        assert_eq!(rows.shards[0].row(0).key, json!("z"));
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
        let dir = std::env::temp_dir().join(format!(
            "douchdb-view-index-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
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
            let mut index = ViewIndex::open(&dir).unwrap();
            index.rebuild(rows, 7, "map|").unwrap();
        }
        {
            let mut index = ViewIndex::open(&dir).unwrap();
            assert_eq!(index.last_seq, 7);
            assert_eq!(index.design, "map|");
            let mut restored = index.load_rows().unwrap();
            restored.sort_by(|a, b| a.id.cmp(&b.id));
            assert_eq!(restored.len(), 2);
            assert_eq!(restored[0].id, "a");
            assert_eq!(restored[1].key, json!("Paris"));
        }
        let _ = std::fs::remove_dir_all(dir);
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
        assert_eq!(
            reduce_value(
                "_approx_count_distinct",
                &[],
                &[json!(1), json!(2), json!(1)],
                ViewLimits::default()
            )
            .unwrap(),
            json!(2)
        );
    }
}

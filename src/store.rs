//! LevelDB access and the key codec.
//!
//! One LevelDB database per (database, shard). Every key is chosen so that
//! LevelDB's sort order is the index we need.

use crate::error::ApiError;
use crate::revtree::RevTree;
use anyhow::Result;
use rusty_leveldb::{LdbIterator, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// Body keys examined per round of the compaction sweep.
#[cfg(not(test))]
const BODY_SWEEP_BATCH: usize = 1000;
#[cfg(test)]
const BODY_SWEEP_BATCH: usize = 3;

/// Sequence of the newest purge info, big-endian like the other `z/`
/// counters. Every purge info takes the next number, so it is also a count.
const PURGE_SEQ_KEY: &[u8] = b"z/purge_seq";

/// Default for how many purge infos a shard keeps (CouchDB's
/// `purged_infos_limit`); `_purged_infos_limit` changes it per shard.
const DEFAULT_PURGED_INFOS_LIMIT: u64 = 1000;
const PURGED_INFOS_LIMIT_KEY: &[u8] = b"z/purged_infos_limit";

/// `pc/<node>`: the purge sequence replica `node` has confirmed reading.
fn purge_checkpoint_key(node: usize) -> Vec<u8> {
    key(b"pc/", &node.to_string())
}

/// One purge request for one document: what CouchDB stores as a purge info
/// (`{PurgeSeq, UUID, DocId, Revs}`) so replicas can learn of the purge.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PurgeInfo {
    pub uuid: String,
    pub id: String,
    pub revs: Vec<String>,
}

/// `p/<seq>`: the purge info with that sequence.
fn purge_key(seq: u64) -> Vec<u8> {
    let mut out = b"p/".to_vec();
    out.extend_from_slice(&seq.to_be_bytes());
    out
}

/// `pu/<uuid>`: the sequence of the purge info with that uuid.
fn purge_uuid_key(uuid: &str) -> Vec<u8> {
    key(b"pu/", uuid)
}

/// `pr/<docid>/<rev>`: the sequence of the purge info that lists the revision.
fn purged_rev_key(docid: &str, rev: &str) -> Vec<u8> {
    key(b"pr/", &format!("{docid}/{rev}"))
}

pub fn meta_key(docid: &str) -> Vec<u8> {
    key(b"m/", docid)
}

pub fn body_key(docid: &str, rev: &str) -> Vec<u8> {
    key(b"d/", &format!("{docid}/{rev}"))
}

/// Big-endian so byte order is numeric order.
pub fn seq_key(seq: u64) -> Vec<u8> {
    let mut out = b"s/".to_vec();
    out.extend_from_slice(&seq.to_be_bytes());
    out
}

pub fn local_key(id: &str) -> Vec<u8> {
    key(b"l/", id)
}

/// `ck/<id>`: a node-local anti-entropy checkpoint. Not a `_local` document:
/// it is never listed, shared or moved with a shard.
pub fn checkpoint_key(id: &str) -> Vec<u8> {
    key(b"ck/", id)
}

fn key(prefix: &[u8], rest: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + rest.len());
    out.extend_from_slice(prefix);
    out.extend_from_slice(rest.as_bytes());
    out
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DocMeta {
    pub tree: RevTree,
    pub seq: u64,
}

#[derive(Clone, Debug)]
pub struct LiveDoc {
    pub rev: String,
}

/// Counts plus an optional content digest for rebalance verification.
#[derive(Clone, Debug, Default)]
pub struct ShardState {
    pub docs: u64,
    pub deleted: u64,
    pub seq: u64,
    pub digest: Option<String>,
}

/// Each document's latest sequence, indexed both ways: by id to update it
/// and to find design documents, and by sequence so a `_changes` read only
/// touches the rows after its cursor instead of every document in the shard.
#[derive(Default)]
pub struct LatestChanges {
    by_id: BTreeMap<String, u64>,
    by_seq: BTreeSet<(u64, String)>,
    /// How many documents have their latest sequence in each bucket of
    /// `1 << SEQ_BUCKET_BITS` sequences, so counting the documents on either
    /// side of a cursor (the `pending` field of `_changes`) does not walk them.
    buckets: Fenwick,
}

const SEQ_BUCKET_BITS: u32 = 8;

impl LatestChanges {
    pub fn insert(&mut self, id: String, seq: u64) {
        if let Some(old) = self.by_id.insert(id.clone(), seq) {
            self.by_seq.remove(&(old, id.clone()));
            self.buckets.add(bucket_of(old), -1);
        }
        self.by_seq.insert((seq, id));
        self.buckets.add(bucket_of(seq), 1);
    }

    /// How many documents have their latest sequence at or below `seq`.
    fn count_through(&self, seq: u64) -> usize {
        let bucket = bucket_of(seq);
        let bucket_start = (bucket as u64) << SEQ_BUCKET_BITS;
        let below = self.buckets.prefix(bucket);
        let within = self
            .by_seq
            .range((bucket_start, String::new())..)
            .take_while(|(entry, _)| *entry <= seq)
            .count();
        below + within
    }

    /// How many documents `after(since)` yields.
    pub fn count_after(&self, since: u64) -> usize {
        self.by_seq.len() - self.count_through(since)
    }

    /// How many documents `before(before)` yields.
    pub fn count_before(&self, before: u64) -> usize {
        before
            .checked_sub(1)
            .map_or(0, |seq| self.count_through(seq))
    }

    /// Documents whose latest sequence is above `since`, in sequence order.
    pub fn after(&self, since: u64) -> impl DoubleEndedIterator<Item = (u64, &str)> {
        let start = since.checked_add(1).map(|seq| (seq, String::new()));
        start
            .into_iter()
            .flat_map(|start| self.by_seq.range(start..))
            .map(|(seq, id)| (*seq, id.as_str()))
    }

    /// Every document, in sequence order.
    pub fn all(&self) -> impl DoubleEndedIterator<Item = (u64, &str)> {
        self.by_seq.iter().map(|(seq, id)| (*seq, id.as_str()))
    }

    pub fn seq_of(&self, id: &str) -> Option<u64> {
        self.by_id.get(id).copied()
    }
}

fn bucket_of(seq: u64) -> usize {
    (seq >> SEQ_BUCKET_BITS) as usize
}

/// A Fenwick (binary indexed) tree of counts that grows as higher indexes are
/// used.
#[derive(Default)]
struct Fenwick {
    tree: Vec<usize>,
}

impl Fenwick {
    fn add(&mut self, index: usize, delta: isize) {
        while self.tree.len() <= index {
            // Node `i` (1-based) sums the entries ending at `i`, as many as
            // its lowest set bit; a new node starts with whatever earlier adds
            // put in that range.
            let i = self.tree.len() + 1;
            let span = 1 << i.trailing_zeros();
            let sum = self.prefix(i - 1) - self.prefix(i - span);
            self.tree.push(sum);
        }
        let mut i = index + 1;
        while i <= self.tree.len() {
            self.tree[i - 1] = self.tree[i - 1].wrapping_add_signed(delta);
            i += 1 << i.trailing_zeros();
        }
    }

    /// Sum of the entries before `end`.
    fn prefix(&self, end: usize) -> usize {
        let mut i = end.min(self.tree.len());
        let mut sum = 0;
        while i > 0 {
            sum += self.tree[i - 1];
            i -= 1 << i.trailing_zeros();
        }
        sum
    }
}

/// A batch of changes plus the sequence counter, committed atomically.
///
/// Staged as plain operations rather than a `WriteBatch` so the shard actor can
/// merge several request batches into one OS flush.
#[derive(Default)]
pub struct Tx {
    ops: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    /// Metadata writes whose before and after states the caller already
    /// knows, by index into `ops`. `commit_at` would otherwise read and decode
    /// both records to learn them.
    known: std::collections::HashMap<usize, (Option<MetaState>, Option<MetaState>)>,
}

impl Tx {
    pub fn new() -> Tx {
        Tx::default()
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.ops.push((key.to_vec(), Some(value.to_vec())));
    }

    pub fn put_json<T: Serialize>(&mut self, key: &[u8], value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        self.ops.push((key.to_vec(), Some(bytes)));
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.ops.push((key.to_vec(), None));
    }

    /// Delete a metadata record that cannot be decoded. Its old state is
    /// unknown, so the commit leaves the counters alone; the caller recounts.
    fn delete_corrupt_meta(&mut self, key: &[u8]) {
        self.known.insert(self.ops.len(), (None, None));
        self.delete(key);
    }

    /// Write a document's metadata record, saying what the document's state
    /// was in the shard before (`old`) and is after the write (`new`), so the
    /// commit need not decode either. `old` must be the state of the record
    /// the shard holds when the transaction commits.
    pub fn put_meta(
        &mut self,
        key: &[u8],
        value: &[u8],
        old: Option<MetaState>,
        new: Option<MetaState>,
    ) {
        self.known.insert(self.ops.len(), (old, new));
        self.put(key, value);
    }
}

/// The first key that sorts after every key beginning with `prefix`.
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(out);
        }
        out.pop();
    }
    None
}

/// Position an iterator on the last key under `prefix`, if any.
fn seek_last(iter: &mut rusty_leveldb::DBIterator, prefix: &[u8]) -> Result<()> {
    if let Some(successor) = prefix_successor(prefix) {
        iter.seek(&successor);
        if iter.valid() {
            if iter.prev() {
                if let Some((key, _)) = iter.current() {
                    if key.starts_with(prefix) {
                        return Ok(());
                    }
                }
            }
            return Ok(());
        }
    }

    // No key at or after the successor, so walk the prefix to find its last
    // key. This only happens for a descending scan with no upper bound, and it
    // visits keys without decoding values.
    iter.seek(prefix);
    if !iter.valid() {
        return Ok(());
    }
    let mut last = match iter.current() {
        Some((key, _)) => key.to_vec(),
        None => return Ok(()),
    };
    while iter.advance() {
        match iter.current() {
            Some((key, _)) if key.starts_with(prefix) => last = key.to_vec(),
            _ => break,
        }
    }
    iter.seek(&last);
    Ok(())
}

struct JobMark {
    pending: usize,
    seq: u64,
    docs: u64,
    deleted: u64,
}

/// Invariant: a `Shard` lives and dies on its actor thread (see
/// `ShardHandle::spawn`) and must never leave it. rusty-leveldb's `DB` holds
/// `Rc`s internally and is only `Send` through an upstream `unsafe impl`, so
/// moving it between threads after use, or sharing it, is unsound.
pub struct Shard {
    db: DB,
    name: ShardName,
    seq: u64,
    seq_tracker: Option<Arc<AtomicU64>>,
    revs_limit: u64,
    purged_infos_limit: u64,
    /// Number of live documents, maintained on every metadata write so `GET
    /// /db` and `_all_docs` totals do not scan the whole `m/` namespace.
    docs: u64,
    /// Number of documents whose winning revision is deleted.
    deleted: u64,
    /// Live document id -> winning revision, built lazily on the first
    /// `_all_docs` range query and then maintained in memory. Keeping this out
    /// of LevelDB avoids an extra write per ingested document.
    live: Option<BTreeMap<String, LiveDoc>>,
    /// The last `live_rank` answer: how many live ids sort before this key.
    /// Paging `_all_docs` by `startkey` asks for nearby keys, so the next rank
    /// is found by counting only the ids between the two keys. Kept in step
    /// with `live` on every write and dropped whenever `live` is.
    rank_cursor: Option<(String, usize)>,
    /// Index entries this shard has walked to answer reads: change rows read
    /// from the sequence index and ids counted to rank a key. Shared with the
    /// shard's handle, so a request's cost can be judged by how many entries
    /// it visited rather than by how long it took.
    scanned: Arc<AtomicU64>,
    /// Document id -> latest sequence, built lazily for `_changes`. CouchDB
    /// returns one row per document at its latest change, not one row per
    /// historical sequence entry.
    latest: Option<LatestChanges>,
    /// Uncommitted operations, in order, plus the same view as a key overlay so
    /// jobs later in the same group observe earlier writes.
    pending: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    overlay: std::collections::HashMap<Vec<u8>, Option<Vec<u8>>>,
    /// Test seam: make the next `flush_pending` fail as a LevelDB write error
    /// would, after the queued operations have been drained.
    #[cfg(test)]
    fail_next_flush: bool,
    /// Set when a flush failed and the shard was reset to its on-disk state.
    /// A flush can run inside a job (`flush`, `compact`), where the error
    /// reaches only that job's caller; the group commit still has to fail
    /// every other job whose queued writes were discarded with it.
    flush_failure: Option<String>,
}

/// What a document's metadata says about it: the winning revision, whether it
/// is live.
pub(crate) type MetaState = (String, bool);

pub(crate) fn meta_state(meta: &DocMeta) -> Option<MetaState> {
    meta.tree.winner().map(|winner| {
        let live = !meta.tree.is_deleted(&winner);
        (winner, live)
    })
}

fn db_get_checked(db: &mut DB, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let snapshot = db.get_snapshot();
    let value = db
        .get_at(&snapshot, key)
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    Ok(value.map(|bytes| bytes.to_vec()))
}

/// Which database and shard a shard actor serves, for log lines.
#[derive(Clone, Debug)]
pub struct ShardName {
    db: String,
    shard: String,
}

impl ShardName {
    fn of(dir: &Path) -> ShardName {
        let name = |path: Option<&Path>| {
            path.and_then(Path::file_name)
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default()
        };
        ShardName {
            db: name(dir.parent()),
            shard: name(Some(dir)),
        }
    }

    /// Log and count a record a scan or listing is about to skip.
    pub fn report_corrupt(&self, id: &str, error: &dyn std::fmt::Display) {
        tracing::error!(
            db = %self.db,
            shard = %self.shard,
            doc_id = %id,
            error = %error,
            "skipping corrupt record"
        );
        crate::telemetry::record_corrupt_record_skipped();
    }
}

impl ShardName {
    /// Log and count a corrupt record that compaction leaves in place.
    fn report_compaction_skip(&self, id: &str, error: &dyn std::fmt::Display) {
        tracing::warn!(
            db = %self.db,
            shard = %self.shard,
            doc_id = %id,
            error = %error,
            "compaction is leaving a corrupt record in place; purge the document to repair it"
        );
        crate::telemetry::record_corrupt_record_skipped();
    }
}

#[cfg(test)]
thread_local! {
    static META_PARSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many metadata records this thread has decoded, for tests that pin the
/// cost of a write.
#[cfg(test)]
pub(crate) fn meta_parses() -> usize {
    META_PARSES.with(std::cell::Cell::get)
}

/// The document id in a `m/<id>` metadata key. A key too short to hold the
/// prefix is corruption, not a reason to panic.
fn doc_id_from_meta_key(key: &[u8]) -> Result<&str> {
    let id = key
        .get(2..)
        .ok_or_else(|| anyhow::anyhow!("corrupt metadata key of {} bytes", key.len()))?;
    std::str::from_utf8(id).map_err(|error| anyhow::anyhow!("doc id is not UTF-8: {error}"))
}

/// Decode a stored metadata record. A record that does not parse is
/// corruption: point reads and writes fail on it, and listings skip it
/// through `ShardName::report_corrupt`.
fn parse_meta(
    key: &[u8],
    value: &[u8],
) -> std::result::Result<(String, DocMeta), crate::error::CorruptRecord> {
    #[cfg(test)]
    META_PARSES.with(|count| count.set(count.get() + 1));
    let id = key
        .get(2..)
        .and_then(|id| std::str::from_utf8(id).ok())
        .ok_or_else(|| crate::error::CorruptRecord {
            id: String::from_utf8_lossy(key.get(2..).unwrap_or_default()).into_owned(),
            reason: "key is not UTF-8".to_string(),
        })?;
    let meta = serde_json::from_slice(value).map_err(|error| crate::error::CorruptRecord {
        id: id.to_string(),
        reason: error.to_string(),
    })?;
    Ok((id.to_string(), meta))
}

fn read_counter(db: &mut DB, key: &[u8]) -> Result<Option<u64>> {
    Ok(db_get_checked(db, key)?
        .filter(|bytes| bytes.len() == 8)
        .map(|bytes| u64::from_be_bytes(bytes[..].try_into().unwrap())))
}

impl Shard {
    pub fn open(dir: impl AsRef<Path>, revs_limit: u64) -> Result<Shard> {
        let path = dir.as_ref();
        let had_tables = std::fs::read_dir(path)
            .map(|entries| {
                entries.flatten().any(|entry| {
                    matches!(
                        entry.path().extension().and_then(|ext| ext.to_str()),
                        Some("ldb" | "sst")
                    )
                })
            })
            .unwrap_or(false);
        let opts = Options {
            create_if_missing: true,
            paranoid_checks: true,
            // Bloom filters are not checksummed. A corrupt filter can turn a
            // present key into a false "not found"; without one, the
            // checksummed data block is always read.
            filter_policy: Rc::new(Box::new(rusty_leveldb::NoFilterPolicy::new())),
            ..Options::default()
        };
        let mut db = DB::open(path, opts)?;
        // A shard that has only seen `_local` writes, or was cleared, legitimately
        // has sequence 0; what marks corruption is the key being absent.
        let stored_seq = read_counter(&mut db, b"z/seq")?;
        let seq = stored_seq.unwrap_or(0);
        if had_tables && stored_seq.is_none() {
            anyhow::bail!(
                "corrupt shard {}: table files are present but no sequence state",
                path.display()
            );
        }
        let revs_limit = db_get_checked(&mut db, b"z/revs_limit")?
            .filter(|b| b.len() == 8)
            .map(|b| u64::from_be_bytes(b[..].try_into().unwrap()))
            .unwrap_or(revs_limit);

        let purged_infos_limit = db_get_checked(&mut db, PURGED_INFOS_LIMIT_KEY)?
            .and_then(|b| <[u8; 8]>::try_from(b).ok())
            .map(u64::from_be_bytes)
            .unwrap_or(DEFAULT_PURGED_INFOS_LIMIT);

        // A shard without counters (a new one, or one whose counters were
        // lost) counts its metadata once on open; every metadata write then
        // keeps them in sync.
        let stored = read_counter(&mut db, b"z/doc_count")?
            .zip(read_counter(&mut db, b"z/doc_deleted_count")?);
        let (docs, deleted) = match stored {
            Some(pair) => pair,
            None => {
                let mut docs = 0u64;
                let mut deleted = 0u64;
                let mut iter = db.new_iter()?;
                iter.seek(b"m/");
                while let Some((key, value)) = iter.current() {
                    if !key.starts_with(b"m/") {
                        break;
                    }
                    let (_, meta) = parse_meta(&key, &value)?;
                    match meta_state(&meta) {
                        Some((_, true)) => docs += 1,
                        Some((_, false)) => deleted += 1,
                        None => {}
                    }
                    if !iter.advance() {
                        break;
                    }
                }
                db.put(b"z/doc_count", &docs.to_be_bytes())?;
                db.put(b"z/doc_deleted_count", &deleted.to_be_bytes())?;
                db.flush()?;
                (docs, deleted)
            }
        };

        Ok(Shard {
            db,
            name: ShardName::of(path),
            seq,
            seq_tracker: None,
            revs_limit,
            purged_infos_limit,
            docs,
            deleted,
            live: None,
            rank_cursor: None,
            scanned: Arc::new(AtomicU64::new(0)),
            latest: None,
            pending: Vec::new(),
            overlay: std::collections::HashMap::new(),
            #[cfg(test)]
            fail_next_flush: false,
            flush_failure: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn inject_flush_failure(&mut self) {
        self.fail_next_flush = true;
    }

    /// Write a record straight to disk, bypassing the overlay, counters and
    /// indexes, so tests can plant corrupt or missing data.
    #[cfg(test)]
    pub(crate) fn put_raw(&mut self, key: &[u8], value: &[u8]) {
        self.db.put(key, value).unwrap();
        self.db.flush().unwrap();
    }

    #[cfg(test)]
    pub(crate) fn delete_raw(&mut self, key: &[u8]) {
        self.db.delete(key).unwrap();
        self.db.flush().unwrap();
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Share the shard's last committed sequence with the database so changes
    /// cursors can use a safe cross-shard watermark.
    pub fn set_seq_tracker(&mut self, tracker: Arc<AtomicU64>) {
        tracker.fetch_max(self.seq, Ordering::SeqCst);
        self.seq_tracker = Some(tracker);
    }

    /// Live and deleted document counts for this shard.
    pub fn counts(&self) -> (u64, u64) {
        (self.docs, self.deleted)
    }

    /// Content digest over document ids, winning revisions, deleted flags,
    /// conflict leaves and attachment metadata. It deliberately excludes
    /// per-replica sequence numbers, which differ between replicas. Two
    /// replicas with identical replicated content produce the same digest.
    pub fn fingerprint(&mut self) -> Result<String> {
        let mut hasher = Sha256::new();
        let mut iter = self.db.new_iter()?;
        iter.seek(b"m/");
        let mut documents = 0u64;
        while let Some((key, value)) = iter.current() {
            let key = key.as_ref();
            if !key.starts_with(b"m/") {
                break;
            }
            let id = doc_id_from_meta_key(key)?;
            let meta: DocMeta = serde_json::from_slice(value.as_ref())?;
            hasher.update(b"doc");
            hasher.update(id.as_bytes());
            match meta_state(&meta) {
                Some((winner, live)) => {
                    hasher.update(winner.as_bytes());
                    hasher.update([live as u8]);
                }
                None => hasher.update(b"no-winner"),
            }
            let mut leaves = meta.tree.leaves();
            leaves.sort();
            for leaf in leaves {
                hasher.update(b"leaf");
                hasher.update(leaf.as_bytes());
                hasher.update([meta.tree.is_deleted(&leaf) as u8]);
            }
            documents += 1;
            if !iter.advance() {
                break;
            }
        }
        hasher.update(documents.to_be_bytes());
        Ok(crate::util::hex(&hasher.finalize()))
    }

    pub fn state(&mut self, with_digest: bool) -> Result<ShardState> {
        let (docs, deleted) = self.counts();
        Ok(ShardState {
            docs,
            deleted,
            seq: self.seq,
            digest: if with_digest {
                Some(self.fingerprint()?)
            } else {
                None
            },
        })
    }

    /// Live document count for one partition, using the in-memory live map.
    pub fn partition_live_count(&mut self, partition: &str) -> Result<u64> {
        let first = format!("{partition}:");
        let last = format!("{partition};");
        let live = self.live_docs()?;
        let count = live
            .range::<str, _>((
                std::ops::Bound::Included(first.as_str()),
                std::ops::Bound::Excluded(last.as_str()),
            ))
            .count();
        self.add_scanned(count);
        Ok(count as u64)
    }

    /// Record `entries` index entries walked by a read.
    pub fn add_scanned(&self, entries: usize) {
        self.scanned.fetch_add(entries as u64, Ordering::Relaxed);
    }

    /// Share `counter` as this shard's scanned-entries count.
    fn set_scan_counter(&mut self, counter: Arc<AtomicU64>) {
        self.scanned = counter;
    }

    /// Live and deleted document counts for one partition. The partition
    /// prefix keeps the scan inside that partition's key range.
    pub fn partition_counts(&mut self, partition: &str) -> Result<(u64, u64)> {
        let prefix = format!("m/{partition}:");
        let mut docs = 0u64;
        let mut deleted = 0u64;
        let name = self.name.clone();
        self.scan_range_while(prefix.as_bytes(), None, false, |key, value| {
            match parse_meta(key, value) {
                Ok((_, meta)) => {
                    if let Some((_, live)) = meta_state(&meta) {
                        if live {
                            docs += 1;
                        } else {
                            deleted += 1;
                        }
                    }
                }
                Err(corrupt) => name.report_corrupt(&corrupt.id, &corrupt),
            }
            true
        })?;
        Ok((docs, deleted))
    }

    /// Sorted live document ids and their winning revisions. The index is
    /// built on first use and maintained in memory from then on.
    pub fn live_docs(&mut self) -> Result<&BTreeMap<String, LiveDoc>> {
        if self.live.is_none() {
            let mut live = BTreeMap::new();
            let name = self.name.clone();
            // `scan_range_while` merges the uncommitted overlay, so a read in
            // the same group commit sees writes queued before it. A record
            // that does not decode is left out of the index; LevelDB errors
            // still fail the scan.
            self.scan_range_while(b"m/", None, false, |key, value| {
                match parse_meta(key, value) {
                    Ok((id, meta)) => {
                        if let Some((rev, true)) = meta_state(&meta) {
                            live.insert(id, LiveDoc { rev });
                        }
                    }
                    Err(corrupt) => name.report_corrupt(&corrupt.id, &corrupt),
                }
                true
            })?;
            self.live = Some(live);
            self.rank_cursor = None;
        }
        Ok(self.live.as_ref().expect("live index initialized"))
    }

    /// How many live documents have an id below `key`. Counts from the
    /// previous call's key when there is one, so walking through the keys in
    /// either direction costs the distance moved, not the position.
    pub fn live_rank(&mut self, key: &str) -> Result<usize> {
        use std::ops::Bound::{Excluded, Unbounded};
        self.live_docs()?;
        let (Some(live), cursor) = (self.live.as_ref(), self.rank_cursor.take()) else {
            return Ok(0);
        };
        let (rank, walked) = match &cursor {
            Some((from, rank)) if key == from.as_str() => (*rank, 0),
            Some((from, rank)) if key > from.as_str() => {
                let walked = live
                    .range::<str, _>((Excluded(from.as_str()), Excluded(key)))
                    .count();
                (
                    rank + walked + usize::from(live.contains_key(from.as_str())),
                    walked,
                )
            }
            Some((from, rank)) => {
                let walked = live
                    .range::<str, _>((Excluded(key), Excluded(from.as_str())))
                    .count();
                (
                    rank.saturating_sub(walked + usize::from(live.contains_key(key))),
                    walked,
                )
            }
            None => {
                let walked = live.range::<str, _>((Unbounded, Excluded(key))).count();
                (walked, walked)
            }
        };
        self.scanned.fetch_add(walked as u64, Ordering::Relaxed);
        self.rank_cursor = Some((key.to_string(), rank));
        Ok(rank)
    }

    /// Each document's latest sequence, built lazily from metadata. Used by
    /// `_changes`, which returns one row per document.
    pub fn latest_changes(&mut self) -> Result<&LatestChanges> {
        if self.latest.is_none() {
            let mut latest = LatestChanges::default();
            let name = self.name.clone();
            self.scan_range_while(b"m/", None, false, |key, value| {
                match parse_meta(key, value) {
                    Ok((id, meta)) => latest.insert(id, meta.seq),
                    Err(corrupt) => name.report_corrupt(&corrupt.id, &corrupt),
                }
                true
            })?;
            self.latest = Some(latest);
        }
        Ok(self.latest.as_ref().expect("latest changes initialized"))
    }

    /// The ids of the design documents (deleted ones included), and those
    /// among them whose metadata is corrupt. A corrupt one's
    /// `validate_doc_update` cannot be loaded, so callers must fail instead of
    /// skipping it. A range scan of the `_design/` keys: it neither builds nor
    /// needs the id-to-sequence index.
    pub fn design_docs(&mut self) -> Result<(Vec<String>, Vec<String>)> {
        let name = self.name.clone();
        let mut ids = Vec::new();
        let mut corrupt_design = Vec::new();
        self.scan_range_while(b"m/_design/", None, false, |key, value| {
            match parse_meta(key, value) {
                Ok((id, _)) => ids.push(id),
                Err(corrupt) => {
                    name.report_corrupt(&corrupt.id, &corrupt);
                    corrupt_design.push(corrupt.id);
                }
            }
            true
        })?;
        Ok((ids, corrupt_design))
    }

    #[cfg(test)]
    pub fn latest_built(&self) -> bool {
        self.latest.is_some()
    }

    pub fn update_latest(
        &mut self,
        staged: &std::collections::HashMap<String, DocMeta>,
        ids: &std::collections::HashSet<String>,
    ) {
        let Some(latest) = &mut self.latest else {
            return;
        };
        for id in ids {
            if let Some(meta) = staged.get(id) {
                latest.insert(id.clone(), meta.seq);
            }
        }
    }

    pub fn invalidate_latest(&mut self) {
        self.latest = None;
    }

    pub fn pending_changes(&mut self, after: u64, descending: bool) -> Result<usize> {
        let latest = self.latest_changes()?;
        Ok(if descending {
            latest.count_before(after)
        } else {
            latest.count_after(after)
        })
    }

    pub fn revs_limit(&self) -> u64 {
        self.revs_limit
    }

    pub fn purged_infos_limit(&self) -> u64 {
        self.purged_infos_limit
    }

    pub fn set_purged_infos_limit(&mut self, limit: u64) -> Result<()> {
        self.purged_infos_limit = limit.max(1);
        self.db.put(
            PURGED_INFOS_LIMIT_KEY,
            &self.purged_infos_limit.to_be_bytes(),
        )?;
        Ok(())
    }

    pub fn set_revs_limit(&mut self, limit: u64) -> Result<()> {
        self.revs_limit = limit.max(1);
        self.db
            .put(b"z/revs_limit", &self.revs_limit.to_be_bytes())?;
        Ok(())
    }

    /// Read a key, merging the uncommitted overlay. LevelDB I/O and checksum
    /// errors surface as errors instead of turning into `None`, so corruption
    /// is a 500, never a silent 404. Every read goes through here.
    pub fn get_checked(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.overlay.get(key) {
            return Ok(value.clone());
        }
        let snapshot = self.db.get_snapshot();
        let value = self
            .db
            .get_at(&snapshot, key)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        Ok(value.map(|bytes| bytes.to_vec()))
    }

    /// Sequence of the newest purge info of this shard: how many have been
    /// recorded.
    pub fn purge_seq(&mut self) -> Result<u64> {
        Ok(self
            .get_checked(PURGE_SEQ_KEY)?
            .filter(|bytes| bytes.len() == 8)
            .map(|bytes| {
                bytes
                    .iter()
                    .fold(0, |acc, byte| (acc << 8) | u64::from(*byte))
            })
            .unwrap_or(0))
    }

    /// Record `info` in `tx` under the next purge sequence, and drop the
    /// infos that are past the shard's `purged_infos_limit` and that every
    /// replica in `peers` has confirmed (CouchDB never trims past the lowest
    /// purge checkpoint of a peer). A peer that never reported holds trimming
    /// back. One call per `tx`.
    pub fn record_purge(&mut self, tx: &mut Tx, info: &PurgeInfo, peers: &[usize]) -> Result<()> {
        let seq = self.purge_seq()?.saturating_add(1);
        let seq_bytes = seq.to_be_bytes();
        tx.put_json(&purge_key(seq), info)?;
        tx.put(&purge_uuid_key(&info.uuid), &seq_bytes);
        for rev in &info.revs {
            tx.put(&purged_rev_key(&info.id, rev), &seq_bytes);
        }
        tx.put(PURGE_SEQ_KEY, &seq_bytes);
        let mut floor = seq.saturating_sub(self.purged_infos_limit);
        for peer in peers {
            floor = floor.min(self.purge_checkpoint(*peer)?);
        }
        let mut old = Vec::new();
        let mut corrupt = false;
        self.scan_range_while(b"p/", None, false, |key, value| {
            let Some(old_seq) = key.get(2..).and_then(|b| <[u8; 8]>::try_from(b).ok()) else {
                corrupt = true;
                return false;
            };
            let old_seq = u64::from_be_bytes(old_seq);
            if old_seq > floor {
                return false;
            }
            match serde_json::from_slice::<PurgeInfo>(value) {
                Ok(gone) => old.push((old_seq, gone)),
                Err(_) => corrupt = true,
            }
            !corrupt
        })?;
        if corrupt {
            anyhow::bail!("an unreadable purge info");
        }
        for (old_seq, gone) in old {
            tx.delete(&purge_key(old_seq));
            tx.delete(&purge_uuid_key(&gone.uuid));
            for rev in &gone.revs {
                // A later info may list the same revision again.
                let key = purged_rev_key(&gone.id, rev);
                if self.get_checked(&key)?.as_deref() == Some(old_seq.to_be_bytes().as_slice()) {
                    tx.delete(&key);
                }
            }
        }
        Ok(())
    }

    /// The purge sequence replica `node` last confirmed reading from this
    /// shard; 0 when it never reported.
    fn purge_checkpoint(&mut self, node: usize) -> Result<u64> {
        Ok(self
            .get_checked(&purge_checkpoint_key(node))?
            .and_then(|b| <[u8; 8]>::try_from(b).ok())
            .map_or(0, u64::from_be_bytes))
    }

    /// Remember that replica `node` has confirmed purge infos up to `seq`.
    pub fn report_purge_checkpoint(&mut self, node: usize, seq: u64) -> Result<()> {
        if self.purge_checkpoint(node)? == seq {
            return Ok(());
        }
        let mut tx = Tx::new();
        tx.put(&purge_checkpoint_key(node), &seq.to_be_bytes());
        self.commit(tx)
    }

    /// The purge infos after sequence `since`, with the shard's purge sequence
    /// read at the same point.
    pub fn purged_since(&mut self, since: u64) -> Result<(u64, Vec<PurgeInfo>)> {
        let seq = self.purge_seq()?;
        let mut infos = Vec::new();
        let mut corrupt = false;
        self.scan_range_while(
            b"p/",
            Some(&purge_key(since.saturating_add(1))),
            false,
            |_, value| match serde_json::from_slice(value) {
                Ok(info) => {
                    infos.push(info);
                    true
                }
                Err(_) => {
                    corrupt = true;
                    false
                }
            },
        )?;
        if corrupt {
            anyhow::bail!("an unreadable purge info");
        }
        Ok((seq, infos))
    }

    /// Whether the purge info `uuid` was already recorded here.
    pub fn has_purge(&mut self, uuid: &str) -> Result<bool> {
        Ok(self.get_checked(&purge_uuid_key(uuid))?.is_some())
    }

    /// The revisions of `id` that a recorded purge info lists.
    pub fn purged_revs(&mut self, id: &str, revs: &[String]) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for rev in revs {
            if self.get_checked(&purged_rev_key(id, rev))?.is_some() {
                out.push(rev.clone());
            }
        }
        Ok(out)
    }

    /// Remove a document whose metadata record cannot be decoded: the record,
    /// its sequence entry and its revision bodies, and recount the document
    /// counters, which the record's old state can no longer correct. The
    /// purge is recorded as `info`. Bodies of a revision that itself contains
    /// a `/` cannot be told from another document's here; compaction removes
    /// them as orphans.
    pub fn purge_corrupt(&mut self, id: &str, info: &PurgeInfo, peers: &[usize]) -> Result<()> {
        let mut tx = Tx::new();
        let body_prefix = format!("d/{id}/");
        let mut keys = Vec::new();
        self.scan_range_while(body_prefix.as_bytes(), None, false, |key, _| {
            if key
                .get(body_prefix.len()..)
                .is_some_and(|rev| !rev.contains(&b'/'))
            {
                keys.push(key.to_vec());
            }
            true
        })?;
        self.scan_range_while(b"s/", None, false, |key, value| {
            if value == id.as_bytes() {
                keys.push(key.to_vec());
            }
            true
        })?;
        for key in keys {
            tx.delete(&key);
        }
        tx.delete_corrupt_meta(&meta_key(id));
        self.record_purge(&mut tx, info, peers)?;
        self.commit(tx)?;

        let (mut docs, mut deleted) = (0u64, 0u64);
        self.scan_range_while(b"m/", None, false, |key, value| {
            if let Ok((_, meta)) = parse_meta(key, value) {
                match meta_state(&meta) {
                    Some((_, true)) => docs += 1,
                    Some((_, false)) => deleted += 1,
                    None => {}
                }
            }
            true
        })?;
        if (docs, deleted) != (self.docs, self.deleted) {
            self.docs = docs;
            self.deleted = deleted;
            for (key, value) in [
                (b"z/doc_count".as_slice(), docs),
                (b"z/doc_deleted_count".as_slice(), deleted),
            ] {
                let value = value.to_be_bytes().to_vec();
                self.overlay.insert(key.to_vec(), Some(value.clone()));
                self.pending.push((key.to_vec(), Some(value)));
            }
        }
        self.live = None;
        self.rank_cursor = None;
        self.latest = None;
        Ok(())
    }

    pub fn get_meta(&mut self, docid: &str) -> Result<Option<DocMeta>> {
        let Some(bytes) = self.get_checked(&meta_key(docid))? else {
            return Ok(None);
        };
        let (_, meta) = parse_meta(&meta_key(docid), &bytes)?;
        Ok(Some(meta))
    }

    /// Where this shard lives, for logging skipped records.
    pub fn name(&self) -> &ShardName {
        &self.name
    }

    pub fn get_body(&mut self, docid: &str, rev: &str) -> Result<Option<Vec<u8>>> {
        self.get_checked(&body_key(docid, rev))
    }

    /// Every key/value pair under `prefix`, starting at `start`. Only used by
    /// tests now; production paths use `scan_range_while` so they can stop
    /// early.
    #[cfg(test)]
    pub fn scan_from(&mut self, prefix: &[u8], start: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        self.scan_range_while(prefix, Some(start), false, |key, value| {
            out.push((key.to_vec(), value.to_vec()));
            true
        })?;
        Ok(out)
    }

    /// Visit key/value pairs under `prefix` in LevelDB order, merging the
    /// uncommitted overlay. `start` is an inclusive lower bound when scanning
    /// forward and an inclusive upper bound when scanning in reverse. Returning
    /// `false` from the visitor stops the scan early; this is what keeps
    /// `_all_docs?limit=100` from reading every document metadata record.
    pub fn scan_range_while<F>(
        &mut self,
        prefix: &[u8],
        start: Option<&[u8]>,
        reverse: bool,
        mut visit: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        // Overlay entries are few (one group commit's worth), so collecting and
        // merging them in memory stays cheap.
        let overlay: BTreeMap<Vec<u8>, Option<Vec<u8>>> = self
            .overlay
            .iter()
            .filter(|(key, _)| {
                key.starts_with(prefix)
                    && match start {
                        Some(bound) if reverse => key.as_slice() <= bound,
                        Some(bound) => key.as_slice() >= bound,
                        None => true,
                    }
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();

        let mut iter = self.db.new_iter()?;
        if reverse {
            match start {
                Some(upper) => {
                    iter.seek(upper);
                    if iter.valid() {
                        let past = iter
                            .current()
                            .map(|(key, _)| key.as_ref() > upper)
                            .unwrap_or(false);
                        if past {
                            iter.prev();
                        }
                    } else {
                        seek_last(&mut iter, prefix)?;
                    }
                }
                None => seek_last(&mut iter, prefix)?,
            }
        } else {
            match start {
                Some(lower) => iter.seek(lower),
                None => iter.seek(prefix),
            }
        }

        // Merge the disk and overlay streams in scan order. An overlay entry
        // shadows a disk entry with the same key, and a `None` value deletes it.
        let mut overlay: Vec<_> = overlay.into_iter().collect();
        if reverse {
            overlay.reverse();
        }
        let mut overlay = overlay.into_iter().peekable();
        loop {
            let disk = if iter.valid() {
                iter.current()
                    .map(|(key, value)| (key.to_vec(), value.to_vec()))
            } else {
                None
            };
            let disk = disk.filter(|(key, _)| key.starts_with(prefix));
            let take_disk = match (&disk, overlay.peek()) {
                (Some((disk_key, _)), Some((overlay_key, _))) if reverse => {
                    disk_key.as_slice() >= overlay_key.as_slice()
                }
                (Some((disk_key, _)), Some((overlay_key, _))) => {
                    disk_key.as_slice() <= overlay_key.as_slice()
                }
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            let next = if take_disk {
                let Some((key, value)) = disk else { break };
                if reverse {
                    iter.prev();
                } else {
                    iter.advance();
                }
                match overlay.next_if(|(overlay_key, _)| *overlay_key == key) {
                    Some((_, overlay_value)) => (key, overlay_value),
                    None => (key, Some(value)),
                }
            } else {
                match overlay.next() {
                    Some(entry) => entry,
                    None => break,
                }
            };
            if let (key, Some(value)) = next {
                if !visit(&key, &value) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub fn commit(&mut self, tx: Tx) -> Result<()> {
        self.commit_at(tx, None)
    }

    /// Commit, recording `seq` as this shard's last written sequence. Sequence
    /// numbers are global to the database, so a shard stores the latest one it
    /// happened to write; the database takes the max across shards on open.
    ///
    /// The operations are queued rather than written: the shard actor merges
    /// everything queued while it drains its mailbox into one synced batch.
    pub fn commit_at(&mut self, mut tx: Tx, seq: Option<u64>) -> Result<()> {
        // Read every old record before changing anything, so a read or decode
        // error leaves the shard exactly as it was. Earlier operations in this
        // same transaction count as the old value, so batched updates to one
        // document still produce one final delta.
        let mut written: std::collections::HashMap<&[u8], &Option<Vec<u8>>> =
            std::collections::HashMap::new();
        let mut states = Vec::with_capacity(tx.ops.len() + 1);
        for (index, (key, value)) in tx.ops.iter().enumerate() {
            if let Some(known) = tx.known.remove(&index) {
                states.push(Some(known));
            } else if key.starts_with(b"m/") {
                let old = match written.get(key.as_slice()) {
                    Some(earlier) => (*earlier).clone(),
                    None => self.get_checked(key)?,
                };
                let old = match old {
                    Some(bytes) => Some(parse_meta(key, &bytes)?.1),
                    None => None,
                };
                let new = match value {
                    Some(bytes) => Some(parse_meta(key, bytes)?.1),
                    None => None,
                };
                states.push(Some((
                    old.as_ref().and_then(meta_state),
                    new.as_ref().and_then(meta_state),
                )));
            } else {
                states.push(None);
            }
            written.insert(key.as_slice(), value);
        }
        drop(written);

        if let Some(seq) = seq {
            self.seq = seq;
        }
        if let Some(tracker) = &self.seq_tracker {
            tracker.fetch_max(self.seq, Ordering::SeqCst);
        }
        tx.ops
            .push((b"z/seq".to_vec(), Some(self.seq.to_be_bytes().to_vec())));
        states.push(None);

        let mut docs = self.docs as i64;
        let mut deleted = self.deleted as i64;
        for ((key, value), state) in tx.ops.into_iter().zip(states) {
            if let Some((old_state, new_state)) = state {
                match old_state.as_ref().map(|(_, live)| *live) {
                    Some(true) => docs -= 1,
                    Some(false) => deleted -= 1,
                    None => {}
                }
                match new_state.as_ref().map(|(_, live)| *live) {
                    Some(true) => docs += 1,
                    Some(false) => deleted += 1,
                    None => {}
                }
                if let Some(live) = self.live.as_mut() {
                    if let Ok(id) = doc_id_from_meta_key(&key) {
                        let (was_live, is_live) = match new_state {
                            Some((rev, true)) => {
                                let old = live.insert(id.to_string(), LiveDoc { rev });
                                (old.is_some(), true)
                            }
                            _ => (live.remove(id).is_some(), false),
                        };
                        if let Some((from, rank)) = self.rank_cursor.as_mut() {
                            if id < from.as_str() && was_live != is_live {
                                if is_live {
                                    *rank += 1;
                                } else {
                                    *rank = rank.saturating_sub(1);
                                }
                            }
                        }
                    }
                }
            }
            self.overlay.insert(key.clone(), value.clone());
            self.pending.push((key, value));
        }

        if docs != self.docs as i64 || deleted != self.deleted as i64 {
            self.docs = docs.max(0) as u64;
            self.deleted = deleted.max(0) as u64;
            for (key, value) in [
                (b"z/doc_count".as_slice(), self.docs),
                (b"z/doc_deleted_count".as_slice(), self.deleted),
            ] {
                let value = value.to_be_bytes().to_vec();
                self.overlay.insert(key.to_vec(), Some(value.clone()));
                self.pending.push((key.to_vec(), Some(value)));
            }
        }
        Ok(())
    }

    /// Position to restore if the job that starts here panics.
    fn job_mark(&self) -> JobMark {
        JobMark {
            pending: self.pending.len(),
            seq: self.seq,
            docs: self.docs,
            deleted: self.deleted,
        }
    }

    /// Undo whatever a panicked job staged. Operations queued by earlier jobs
    /// in the same group commit are kept (their callers have not been told
    /// anything yet); everything the panicked job queued is dropped, the
    /// overlay is rebuilt from what remains, and the counters are restored.
    /// The lazily built in-memory indexes may have been updated halfway, so
    /// they are dropped and rebuild from disk plus the overlay on next use.
    fn rollback_to(&mut self, mark: JobMark) {
        self.pending.truncate(mark.pending);
        self.overlay.clear();
        for (key, value) in &self.pending {
            self.overlay.insert(key.clone(), value.clone());
        }
        self.seq = mark.seq;
        self.docs = mark.docs;
        self.deleted = mark.deleted;
        self.live = None;
        self.rank_cursor = None;
        self.latest = None;
    }

    /// Write every queued operation as one synced `WriteBatch`.
    pub fn flush_pending(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        for (key, value) in self.pending.drain(..) {
            match value {
                Some(value) => batch.put(&key, &value),
                None => batch.delete(&key),
            }
        }
        #[cfg(test)]
        let injected = std::mem::take(&mut self.fail_next_flush);
        #[cfg(not(test))]
        let injected = false;
        let written = if injected {
            Err(anyhow::anyhow!("injected write failure"))
        } else {
            self.db
                .write(batch, true)
                .map_err(|error| anyhow::anyhow!("{error}"))
        };
        if let Err(error) = written {
            // The queued operations are gone, so nothing in memory may still
            // reflect them.
            let message = match self.restore_from_disk() {
                Ok(()) => error.to_string(),
                Err(restore) => format!("{error}; could not restore shard state: {restore}"),
            };
            self.flush_failure = Some(message.clone());
            anyhow::bail!(message);
        }
        self.overlay.clear();
        Ok(())
    }

    /// Error from a flush that failed since the last call, if any.
    fn take_flush_failure(&mut self) -> Option<String> {
        self.flush_failure.take()
    }

    /// Reset the in-memory state to what is on disk after a failed write: the
    /// overlay and counters described operations that never happened, and the
    /// lazily built indexes were updated alongside them.
    ///
    /// The shared committed-sequence tracker is left alone. It only moves
    /// forward and readers may already have passed the lost sequences; those
    /// are now gaps, which `_changes` tolerates (every update already leaves
    /// one) and which keep `reserved == committed`, so no watermark stalls.
    fn restore_from_disk(&mut self) -> Result<()> {
        self.pending.clear();
        self.overlay.clear();
        self.live = None;
        self.rank_cursor = None;
        self.latest = None;
        self.seq = read_counter(&mut self.db, b"z/seq")?.unwrap_or(0);
        self.docs = read_counter(&mut self.db, b"z/doc_count")?.unwrap_or(0);
        self.deleted = read_counter(&mut self.db, b"z/doc_deleted_count")?.unwrap_or(0);
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.flush_pending()?;
        self.db.flush()?;
        Ok(())
    }

    /// Drop all document data from this local shard, preserving only the
    /// shard's revs-limit setting. Used after a shard has moved elsewhere and
    /// anti-entropy has seeded the new replicas.
    pub fn clear(&mut self) -> Result<()> {
        let mut keys = Vec::new();
        {
            let mut iter = self.db.new_iter()?;
            iter.reset();
            iter.advance();
            while let Some((key, _)) = iter.current() {
                if key.as_ref() != b"z/revs_limit"
                    && key.as_ref() != PURGED_INFOS_LIMIT_KEY
                    && !key.starts_with(b"ck/")
                {
                    keys.push(key.to_vec());
                }
                if !iter.advance() {
                    break;
                }
            }
        }
        let mut batch = WriteBatch::default();
        for key in keys {
            batch.delete(&key);
        }
        // Keep the sequence (so numbers never go backwards and `open` still
        // finds sequence state next to the table files) and persist the
        // emptied counters.
        batch.put(b"z/seq", &self.seq.to_be_bytes());
        batch.put(b"z/doc_count", &0u64.to_be_bytes());
        batch.put(b"z/doc_deleted_count", &0u64.to_be_bytes());
        self.db.write(batch, true)?;
        self.docs = 0;
        self.deleted = 0;
        self.live = None;
        self.rank_cursor = None;
        self.latest = None;
        self.pending.clear();
        self.overlay.clear();
        self.db
            .put(b"z/revs_limit", &self.revs_limit.to_be_bytes())?;
        self.db.put(
            PURGED_INFOS_LIMIT_KEY,
            &self.purged_infos_limit.to_be_bytes(),
        )?;
        self.db.flush()?;
        Ok(())
    }

    /// Delete every revision body that is not a leaf of its document's tree:
    /// superseded revisions, revisions stemmed by `revs_limit` and orphans.
    /// CouchDB keeps them readable until compaction and then drops them;
    /// leaves (conflicts and tombstones included) always stay.
    fn drop_stale_bodies(&mut self) -> Result<()> {
        // Memory stays bounded: each round reads at most BODY_SWEEP_BATCH body
        // keys, deletes the stale ones and resumes after the last key seen.
        let mut start = b"d/".to_vec();
        // Documents whose corrupt metadata was already reported this sweep.
        let mut reported: BTreeSet<String> = BTreeSet::new();
        let name = self.name.clone();
        loop {
            let mut keys = Vec::new();
            self.scan_range_while(b"d/", Some(&start), false, |key, _| {
                keys.push(key.to_vec());
                keys.len() < BODY_SWEEP_BATCH
            })?;
            let Some(last) = keys.last() else {
                return Ok(());
            };
            start = last.clone();
            start.push(0);

            // Leaf sets by document id, for this batch only.
            let mut leaves: std::collections::HashMap<String, Option<BTreeSet<String>>> =
                std::collections::HashMap::new();
            let mut tx = Tx::new();
            for key in &keys {
                let Some(rest) = key.get(2..).and_then(|rest| std::str::from_utf8(rest).ok())
                else {
                    continue;
                };
                // Both the document id and the revision may contain '/', so
                // d/{id}/{rev} is ambiguous. Keep the body if any split makes
                // it a leaf; delete it only when none does.
                let mut is_leaf = false;
                for (at, _) in rest.match_indices('/') {
                    let (id, rev) = (&rest[..at], &rest[at + 1..]);
                    if !leaves.contains_key(id) {
                        // A record that cannot be decoded cannot say which
                        // bodies are stale: keep them all, report it once and
                        // carry on with the rest of the shard.
                        let found = match self.get_meta(id) {
                            Ok(meta) => meta.map(|meta| meta.tree.leaves().into_iter().collect()),
                            Err(error) => {
                                let corrupt = error.downcast::<crate::error::CorruptRecord>()?;
                                if reported.insert(id.to_string()) {
                                    name.report_compaction_skip(id, &corrupt);
                                }
                                is_leaf = true;
                                break;
                            }
                        };
                        leaves.insert(id.to_string(), found);
                    }
                    if leaves
                        .get(id)
                        .and_then(Option::as_ref)
                        .is_some_and(|set| set.contains(rev))
                    {
                        is_leaf = true;
                        break;
                    }
                }
                if !is_leaf {
                    tx.delete(key);
                }
            }
            self.commit(tx)?;
            self.flush_pending()?;
        }
    }

    pub fn compact(&mut self) -> Result<()> {
        self.flush_pending()?;
        self.drop_stale_bodies()?;
        // Compaction is a long, blocking LevelDB call (tens of seconds on a
        // large shard). It runs on this shard's dedicated actor thread, so it
        // never ties up a Tokio worker.
        self.db.compact_range(&[], &[])?;
        self.db.flush()?;
        Ok(())
    }
}

type Ack = Box<dyn FnOnce(Option<String>) + Send>;
type Job = Box<dyn FnOnce(&mut Shard) -> Ack + Send>;

/// Every shard is owned by exactly one dedicated OS thread: rusty-leveldb
/// takes `&mut self` even for reads, so serialised access is not a choice, and
/// its calls are synchronous file I/O that must not run on Tokio workers.
/// Dropping the handle closes the queue, lets the thread finish the queued
/// jobs, flush and close the database, and waits for it, so the LevelDB lock
/// is released before `drop` returns.
pub struct ShardHandle {
    tx: Option<mpsc::Sender<Job>>,
    thread: Option<std::thread::JoinHandle<()>>,
    initial_seq: u64,
    seq_tracker: Arc<AtomicU64>,
    /// Highest sequence allocated for a write on this shard, committed or not.
    reserved_seq: Arc<AtomicU64>,
    /// Jobs submitted to this shard's actor since it opened. Each one is a
    /// cross-thread hop, so the count is the shard round-trip cost of a request.
    jobs: AtomicU64,
    /// Index entries the shard has walked, shared with the shard.
    scanned: Arc<AtomicU64>,
}

impl ShardHandle {
    /// `committed` is pinged after every batch that advanced this shard's
    /// committed sequence and has been flushed, so a feed can wait for a
    /// commit instead of polling.
    pub fn spawn(
        dir: impl AsRef<Path>,
        revs_limit: u64,
        committed: Arc<tokio::sync::watch::Sender<()>>,
    ) -> Result<ShardHandle> {
        let dir = dir.as_ref().to_path_buf();
        let name = format!(
            "shard-{}",
            dir.file_name().and_then(|n| n.to_str()).unwrap_or("?")
        );
        let seq_tracker = Arc::new(AtomicU64::new(0));
        let (tx, mut rx) = mpsc::channel::<Job>(256);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<u64>>();
        let tracker = seq_tracker.clone();
        let scanned = Arc::new(AtomicU64::new(0));
        let shard_scanned = scanned.clone();
        let thread = std::thread::Builder::new().name(name).spawn(move || {
            // Open on this thread so the database never crosses threads.
            let mut shard = match Shard::open(&dir, revs_limit) {
                Ok(shard) => shard,
                Err(e) => {
                    let _ = ready_tx.send(Err(e));
                    return;
                }
            };
            let initial_seq = shard.seq();
            tracker.store(initial_seq, Ordering::SeqCst);
            shard.set_seq_tracker(tracker.clone());
            shard.set_scan_counter(shard_scanned);
            if ready_tx.send(Ok(initial_seq)).is_err() {
                return;
            }
            // Group commit: run a job, then everything already queued, and
            // acknowledge only after the merged batch has been flushed to the
            // OS. rusty-leveldb does not fsync, so this is not power-loss
            // durable.
            while let Some(job) = rx.blocking_recv() {
                let before = tracker.load(Ordering::SeqCst);
                let mut acks = vec![job(&mut shard)];
                for _ in 1..64 {
                    match rx.try_recv() {
                        Ok(job) => acks.push(job(&mut shard)),
                        Err(_) => break,
                    }
                }
                let _ = shard.flush_pending();
                let flush_error = shard.take_flush_failure();
                if tracker.load(Ordering::SeqCst) > before {
                    committed.send_replace(());
                }
                for ack in acks {
                    ack(flush_error.clone());
                }
            }
            let _ = shard.flush_pending();
        })?;
        let initial_seq = ready_rx
            .recv()
            .map_err(|_| anyhow::anyhow!("shard thread exited before opening its database"))??;
        Ok(ShardHandle {
            tx: Some(tx),
            thread: Some(thread),
            initial_seq,
            seq_tracker,
            reserved_seq: Arc::new(AtomicU64::new(initial_seq)),
            jobs: AtomicU64::new(0),
            scanned,
        })
    }

    /// Index entries the shard has walked to answer reads so far.
    pub fn scanned(&self) -> u64 {
        self.scanned.load(Ordering::Relaxed)
    }

    /// Jobs submitted to this shard's actor so far.
    pub fn jobs(&self) -> u64 {
        self.jobs.load(Ordering::Relaxed)
    }

    /// Sequence number this shard had when it was opened.
    pub fn initial_seq(&self) -> u64 {
        self.initial_seq
    }

    /// Shared last committed sequence for this shard.
    pub fn seq_tracker(&self) -> Arc<AtomicU64> {
        self.seq_tracker.clone()
    }

    /// Shared highest allocated sequence for this shard.
    pub fn reserved_seq(&self) -> Arc<AtomicU64> {
        self.reserved_seq.clone()
    }

    /// Run a job on the shard task, which owns the database handle.
    pub async fn call<R, F>(&self, f: F) -> std::result::Result<R, ApiError>
    where
        R: Send + 'static,
        F: FnOnce(&mut Shard) -> std::result::Result<R, ApiError> + Send + 'static,
    {
        self.jobs.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = oneshot::channel();
        let seq_tracker = self.seq_tracker.clone();
        let reserved_seq = self.reserved_seq.clone();
        let job: Job = Box::new(move |shard| {
            // A panic must fail only this job's caller, never kill the shard
            // actor (every later call would then answer "shard task is gone").
            let mark = shard.job_mark();
            let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(shard))) {
                Ok(result) => result,
                Err(payload) => {
                    shard.rollback_to(mark);
                    let reason = payload
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| payload.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic".to_string());
                    Err(ApiError::new(
                        500,
                        "internal_server_error",
                        format!("shard job panicked: {reason}"),
                    ))
                }
            };
            // The job is over, so any sequence it reserved but did not commit
            // (it panicked, or returned an error after allocating) never
            // will be. Count it as committed so the changes watermark does not
            // wait on it forever; this only leaves a gap in the sequence
            // numbers.
            seq_tracker.fetch_max(reserved_seq.load(Ordering::SeqCst), Ordering::SeqCst);
            Box::new(move |flush_error: Option<String>| {
                let result = match flush_error {
                    Some(message) => Err(ApiError::new(500, "internal_server_error", message)),
                    None => result,
                };
                let _ = reply_tx.send(result);
            })
        });
        let tx = self
            .tx
            .as_ref()
            .ok_or_else(|| ApiError::new(500, "internal_server_error", "shard task is gone"))?;
        tx.send(job)
            .await
            .map_err(|_| ApiError::new(500, "internal_server_error", "shard task is gone"))?;
        reply_rx
            .await
            .map_err(|_| ApiError::new(500, "internal_server_error", "shard task dropped reply"))?
    }
}

impl Drop for ShardHandle {
    fn drop(&mut self) {
        self.tx = None;
        if let Some(thread) = self.thread.take() {
            // A job that owns the last reference to its own database would be
            // dropped on the actor thread; joining ourselves would deadlock.
            if thread.thread().id() != std::thread::current().id() {
                let _ = thread.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rev::Rev;

    #[test]
    fn docids_sort_like_couchdb() {
        assert!(meta_key("a") < meta_key("b"));
        assert!(meta_key("Z") < meta_key("a"));
        assert!(meta_key("a") < meta_key("aa"));
        assert!(meta_key("_design/x") < meta_key("a"));
    }

    #[test]
    fn a_short_metadata_key_is_an_error_not_a_panic() {
        assert_eq!(doc_id_from_meta_key(&meta_key("abc")).unwrap(), "abc");
        assert_eq!(doc_id_from_meta_key(b"m/").unwrap(), "");
        assert!(doc_id_from_meta_key(b"m").is_err());
        assert!(doc_id_from_meta_key(b"").is_err());
        assert!(doc_id_from_meta_key(b"m/\xff").is_err());
    }

    #[test]
    fn seq_keys_sort_numerically() {
        assert!(seq_key(2) < seq_key(10));
        assert!(seq_key(255) < seq_key(256));
        assert_eq!(seq_key(42)[2..10], 42u64.to_be_bytes());
    }

    #[test]
    fn prefixes_are_disjoint() {
        assert!(local_key("x") < meta_key("x"));
        assert!(meta_key("x") < seq_key(0));
    }

    #[test]
    fn body_keys_are_per_revision_not_per_position() {
        let a = Rev::parse("2-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let b = Rev::parse("2-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        assert_ne!(body_key("d", &a.to_string()), body_key("d", &b.to_string()));
    }
}

#[cfg(test)]
mod panic_tests {
    use super::*;

    fn handle() -> ShardHandle {
        let dir = std::env::temp_dir().join(format!(
            "douchdb-panic-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        ShardHandle::spawn(&dir, 1000, Default::default()).unwrap()
    }

    fn put(key: &'static [u8]) -> impl FnOnce(&mut Shard) -> std::result::Result<(), ApiError> {
        move |shard| {
            let mut tx = Tx::new();
            tx.put(key, b"v");
            shard.commit_at(tx, None).map_err(ApiError::from)
        }
    }

    #[tokio::test]
    async fn a_panicking_job_fails_only_its_caller() {
        let handle = handle();
        let result: std::result::Result<(), ApiError> =
            handle.call(|_| panic!("boom in job")).await;
        let error = result.unwrap_err();
        assert_eq!(error.status, 500);
        assert!(error.reason.contains("boom in job"), "{}", error.reason);
        handle.call(|_| Ok(())).await.unwrap();
    }

    #[tokio::test]
    async fn a_panicked_reservation_does_not_hold_back_the_committed_sequence() {
        let handle = handle();
        let reserved = handle.reserved_seq();
        let committed = handle.seq_tracker();
        let result: std::result::Result<(), ApiError> = handle
            .call(move |_| {
                reserved.fetch_max(5, Ordering::SeqCst);
                panic!("after reserving");
            })
            .await;
        assert!(result.is_err());
        assert!(committed.load(Ordering::SeqCst) >= handle.reserved_seq().load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn an_errored_reservation_does_not_hold_back_the_committed_sequence() {
        let handle = handle();
        let reserved = handle.reserved_seq();
        let result: std::result::Result<(), ApiError> = handle
            .call(move |_| {
                reserved.fetch_max(5, Ordering::SeqCst);
                Err(ApiError::bad_request("failed after reserving"))
            })
            .await;
        assert!(result.is_err());
        assert!(handle.seq_tracker().load(Ordering::SeqCst) >= 5);
    }

    #[tokio::test]
    async fn a_failed_group_flush_fails_the_caller_and_leaves_no_trace() {
        let handle = handle();
        handle.call(put(b"k/one")).await.unwrap();
        handle
            .call(|shard| {
                shard.inject_flush_failure();
                Ok(())
            })
            .await
            .unwrap();
        assert!(handle.call(put(b"k/two")).await.is_err());
        let seen = handle
            .call(|shard| Ok((shard.get_checked(b"k/one")?, shard.get_checked(b"k/two")?)))
            .await
            .unwrap();
        assert_eq!(seen, (Some(b"v".to_vec()), None));
        handle.call(put(b"k/three")).await.unwrap();
    }

    #[tokio::test]
    async fn jobs_run_on_a_dedicated_thread_not_a_tokio_worker() {
        let handle = handle();
        let (in_runtime, name) = handle
            .call(|_| {
                Ok((
                    tokio::runtime::Handle::try_current().is_ok(),
                    std::thread::current().name().map(str::to_string),
                ))
            })
            .await
            .unwrap();
        assert!(!in_runtime, "shard job ran inside a Tokio runtime");
        assert!(
            name.as_deref().is_some_and(|n| n.starts_with("shard-")),
            "unexpected thread name {name:?}"
        );
    }

    #[tokio::test]
    async fn compact_works_from_the_actor_outside_a_multi_thread_runtime() {
        let handle = handle();
        handle.call(put(b"x/a")).await.unwrap();
        handle
            .call(|shard| shard.compact().map_err(ApiError::from))
            .await
            .unwrap();
        let value = handle
            .call(|shard| Ok(shard.get_checked(b"x/a")?))
            .await
            .unwrap();
        assert_eq!(value.as_deref(), Some(b"v".as_slice()));
    }

    #[tokio::test]
    async fn dropping_the_handle_releases_the_database_lock() {
        let dir = std::env::temp_dir().join(format!(
            "douchdb-drop-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let handle = ShardHandle::spawn(&dir, 1000, Default::default()).unwrap();
        handle.call(put(b"x/kept")).await.unwrap();
        drop(handle);
        // No waiting: the LevelDB LOCK must already be released.
        let mut reopened = Shard::open(&dir, 1000).unwrap();
        assert_eq!(
            reopened.get_checked(b"x/kept").unwrap().as_deref(),
            Some(b"v".as_slice())
        );
        drop(reopened);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn data_written_before_a_panic_survives_and_the_partial_job_is_discarded() {
        let handle = handle();
        handle.call(put(b"x/before")).await.unwrap();
        let result: std::result::Result<(), ApiError> = handle
            .call(|shard| {
                put(b"x/partial")(shard)?;
                panic!("after partial write");
            })
            .await;
        assert!(result.is_err());
        let (before, partial) = handle
            .call(|shard| {
                Ok((
                    shard.get_checked(b"x/before")?,
                    shard.get_checked(b"x/partial")?,
                ))
            })
            .await
            .unwrap();
        assert_eq!(before.as_deref(), Some(b"v".as_slice()));
        assert_eq!(partial, None);
    }
}

#[cfg(test)]
mod scan_tests {
    use super::*;
    use crate::rev::Rev;

    fn meta(deleted: bool, seq: u64) -> DocMeta {
        let rev = Rev::parse("1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut tree = RevTree::default();
        tree.merge_path(&[rev], deleted);
        DocMeta { tree, seq }
    }

    #[test]
    fn compaction_keeps_leaf_bodies_whose_revision_contains_a_slash() {
        let dir = temp_shard_dir("sweep-slash");
        let mut shard = Shard::open(&dir, 1000).unwrap();
        let tree = |edges: &[(&str, Option<&str>)]| {
            let edges: BTreeMap<&str, Option<&str>> = edges.iter().copied().collect();
            let tree: RevTree =
                serde_json::from_value(serde_json::json!({"edges": edges, "deleted": []})).unwrap();
            DocMeta { tree, seq: 1 }
        };
        let mut tx = Tx::new();
        // The only leaf of "doc" has a '/' in its revision id.
        tx.put_json(&meta_key("doc"), &tree(&[("1-x/y", None)]))
            .unwrap();
        tx.put(&body_key("doc", "1-x/y"), b"{}");
        // "p" has a stale parent and a leaf, both with '/'.
        tx.put_json(
            &meta_key("p"),
            &tree(&[("1-p/q", None), ("2-r/s", Some("1-p/q"))]),
        )
        .unwrap();
        tx.put(&body_key("p", "1-p/q"), b"{}");
        tx.put(&body_key("p", "2-r/s"), b"{}");
        // d/a/1-b/c is "a" with leaf "1-b/c" or "a/1-b" with rev "c": it
        // stays because one reading makes it a leaf.
        tx.put_json(&meta_key("a"), &tree(&[("1-b/c", None)]))
            .unwrap();
        tx.put_json(&meta_key("a/1-b"), &tree(&[("2-z", None)]))
            .unwrap();
        tx.put(&body_key("a", "1-b/c"), b"{}");
        tx.put(&body_key("a/1-b", "2-z"), b"{}");
        shard.commit_at(tx, Some(1)).unwrap();
        shard.flush_pending().unwrap();

        shard.compact().unwrap();

        let mut left = Vec::new();
        shard
            .scan_range_while(b"d/", None, false, |key, _| {
                left.push(key.to_vec());
                true
            })
            .unwrap();
        let mut expected = vec![
            body_key("doc", "1-x/y"),
            body_key("p", "2-r/s"),
            body_key("a", "1-b/c"),
            body_key("a/1-b", "2-z"),
        ];
        expected.sort();
        assert_eq!(left, expected);
        drop(shard);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_sweeps_stale_bodies_in_several_batches() {
        let dir = temp_shard_dir("sweep");
        let mut shard = Shard::open(&dir, 1000).unwrap();
        let old = Rev::parse("1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let new = Rev::parse("2-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        let mut tree = RevTree::default();
        tree.merge_path(&[old.clone(), new.clone()], false);
        let doc_meta = DocMeta { tree, seq: 1 };
        // Ids with '/' sort between and around each other's keys.
        let ids = ["a", "a/b", "a/b/c", "a0", "b", "b/", "c", "d/e", "e", "f"];
        let mut tx = Tx::new();
        for id in ids {
            tx.put_json(&meta_key(id), &doc_meta).unwrap();
            tx.put(&body_key(id, &old.to_string()), b"{}");
            tx.put(&body_key(id, &new.to_string()), b"{}");
        }
        tx.put(&body_key("ghost/x", &old.to_string()), b"{}");
        shard.commit_at(tx, Some(1)).unwrap();
        shard.flush_pending().unwrap();

        shard.compact().unwrap();

        let mut left = Vec::new();
        shard
            .scan_range_while(b"d/", None, false, |key, _| {
                left.push(key.to_vec());
                true
            })
            .unwrap();
        let mut expected: Vec<Vec<u8>> = ids
            .iter()
            .map(|id| body_key(id, &new.to_string()))
            .collect();
        expected.sort();
        assert_eq!(left, expected, "only leaf bodies survive");
        drop(shard);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn counters_track_live_and_deleted_documents_across_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "douchdb-counts-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut shard = Shard::open(&dir, 1000).unwrap();
        assert_eq!(shard.counts(), (0, 0));

        let mut tx = Tx::new();
        tx.put_json(&meta_key("a"), &meta(false, 1)).unwrap();
        tx.put_json(&meta_key("b"), &meta(false, 2)).unwrap();
        tx.put_json(&meta_key("c"), &meta(true, 3)).unwrap();
        shard.commit_at(tx, Some(3)).unwrap();
        assert_eq!(shard.counts(), (2, 1));

        // Turn a live doc into a deleted one, and purge another entirely.
        let mut tx = Tx::new();
        tx.put_json(&meta_key("a"), &meta(true, 4)).unwrap();
        tx.delete(&meta_key("b"));
        shard.commit_at(tx, Some(4)).unwrap();
        assert_eq!(shard.counts(), (0, 2));

        shard.flush_pending().unwrap();
        drop(shard);
        let mut shard = Shard::open(&dir, 1000).unwrap();
        assert_eq!(shard.counts(), (0, 2), "counters survive reopen");

        // A shard whose counters are missing: open must recount them from the
        // metadata.
        shard.db.delete(b"z/doc_count").unwrap();
        shard.db.delete(b"z/doc_deleted_count").unwrap();
        shard.db.flush().unwrap();
        drop(shard);
        let shard = Shard::open(&dir, 1000).unwrap();
        assert_eq!(shard.counts(), (0, 2), "missing counters are backfilled");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn failed_flush_restores_the_state_on_disk() {
        let dir = temp_shard_dir("flush-fail");
        let mut shard = Shard::open(&dir, 1000).unwrap();
        let mut tx = Tx::new();
        tx.put_json(&meta_key("a"), &meta(false, 1)).unwrap();
        shard.commit_at(tx, Some(1)).unwrap();
        shard.flush_pending().unwrap();
        // Build the lazy indexes so the failure has to repair them too.
        assert_eq!(live_ids(&mut shard), vec!["a"]);
        assert_eq!(shard.latest_changes().unwrap().seq_of("a"), Some(1));

        let mut tx = Tx::new();
        tx.put_json(&meta_key("b"), &meta(false, 2)).unwrap();
        shard.commit_at(tx, Some(2)).unwrap();
        shard.inject_flush_failure();
        assert!(shard.flush_pending().is_err());

        assert!(
            shard.get_checked(&meta_key("b")).unwrap().is_none(),
            "unwritten doc visible"
        );
        assert_eq!(shard.counts(), (1, 0));
        assert_eq!(shard.seq(), 1);
        assert_eq!(live_ids(&mut shard), vec!["a"]);
        assert_eq!(shard.latest_changes().unwrap().seq_of("b"), None);

        let mut tx = Tx::new();
        tx.put_json(&meta_key("c"), &meta(false, 3)).unwrap();
        shard.commit_at(tx, Some(3)).unwrap();
        shard.flush_pending().unwrap();
        drop(shard);
        let mut shard = Shard::open(&dir, 1000).unwrap();
        assert_eq!(live_ids(&mut shard), vec!["a", "c"]);
        assert_eq!(shard.counts(), (2, 0));
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn temp_shard_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "douchdb-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn has_tables(dir: &std::path::Path) -> bool {
        std::fs::read_dir(dir).unwrap().flatten().any(|entry| {
            matches!(
                entry.path().extension().and_then(|ext| ext.to_str()),
                Some("ldb" | "sst")
            )
        })
    }

    #[test]
    fn cleared_shard_reopens_and_keeps_its_sequence() {
        let dir = temp_shard_dir("clear-reopen");
        let mut shard = Shard::open(&dir, 1000).unwrap();
        let mut tx = Tx::new();
        tx.put_json(&meta_key("a"), &meta(false, 1)).unwrap();
        tx.put_json(&meta_key("b"), &meta(true, 2)).unwrap();
        shard.commit_at(tx, Some(7)).unwrap();
        shard.flush_pending().unwrap();
        shard.db.compact_range(&[], &[]).unwrap();
        assert!(has_tables(&dir));
        shard.clear().unwrap();
        assert_eq!(shard.counts(), (0, 0));
        drop(shard);

        let shard = Shard::open(&dir, 1000).unwrap();
        assert_eq!(shard.seq, 7, "sequence numbers never go backwards");
        assert_eq!(shard.counts(), (0, 0));
        drop(shard);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shard_with_tables_but_no_sequence_key_is_corrupt() {
        let dir = temp_shard_dir("no-seq");
        let mut shard = Shard::open(&dir, 1000).unwrap();
        let mut tx = Tx::new();
        tx.put_json(&meta_key("a"), &meta(false, 1)).unwrap();
        shard.commit_at(tx, Some(1)).unwrap();
        shard.flush_pending().unwrap();
        shard.db.compact_range(&[], &[]).unwrap();
        shard.db.delete(b"z/seq").unwrap();
        shard.db.flush().unwrap();
        drop(shard);
        assert!(has_tables(&dir));
        let error = Shard::open(&dir, 1000).err().expect("must refuse to open");
        assert!(error.to_string().contains("corrupt shard"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn local_only_shard_reopens() {
        let dir = temp_shard_dir("local-only");
        let mut shard = Shard::open(&dir, 1000).unwrap();
        let mut tx = Tx::new();
        tx.put(&local_key("x"), b"{}");
        shard.commit_at(tx, None).unwrap();
        shard.flush_pending().unwrap();
        shard.db.compact_range(&[], &[]).unwrap();
        drop(shard);
        assert!(has_tables(&dir));
        let shard = Shard::open(&dir, 1000).unwrap();
        assert_eq!(shard.seq, 0);
        drop(shard);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_metadata_is_skipped_by_scans_and_fails_closed_elsewhere() {
        let dir = temp_shard_dir("corrupt-meta");
        let mut shard = Shard::open(&dir, 1000).unwrap();
        let mut tx = Tx::new();
        tx.put_json(&meta_key("a"), &meta(false, 1)).unwrap();
        shard.commit_at(tx, Some(1)).unwrap();
        shard.flush_pending().unwrap();
        shard.put_raw(&meta_key("p:bad"), b"{not json");

        // Scans that serve many documents skip the record and count it.
        let before = crate::metrics::corrupt_records_skipped();
        assert_eq!(live_ids(&mut shard), ["a"]);
        assert_eq!(shard.latest_changes().unwrap().all().count(), 1);
        assert_eq!(shard.partition_counts("p").unwrap(), (0, 0));
        assert!(crate::metrics::corrupt_records_skipped() >= before + 3);
        // The anti-entropy fingerprint must not paper over it.
        assert!(shard.fingerprint().is_err());

        // Overwriting a record that cannot be parsed would make the counters
        // guess what it used to be.
        let mut tx = Tx::new();
        tx.put_json(&meta_key("p:bad"), &meta(false, 2)).unwrap();
        assert!(shard.commit_at(tx, Some(2)).is_err());
        assert_eq!(shard.counts(), (1, 0));
        assert_eq!(shard.seq(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn counter_backfill_fails_on_corrupt_metadata() {
        let dir = temp_shard_dir("corrupt-backfill");
        let mut shard = Shard::open(&dir, 1000).unwrap();
        let mut tx = Tx::new();
        tx.put_json(&meta_key("a"), &meta(false, 1)).unwrap();
        shard.commit_at(tx, Some(1)).unwrap();
        shard.flush_pending().unwrap();
        shard.put_raw(&meta_key("p:bad"), b"{not json");
        shard.delete_raw(b"z/doc_count");
        shard.delete_raw(b"z/doc_deleted_count");
        drop(shard);
        assert!(Shard::open(&dir, 1000).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_skips_a_corrupt_metadata_record_and_keeps_its_bodies() {
        let dir = temp_shard_dir("corrupt-compact");
        let mut shard = Shard::open(&dir, 1000).unwrap();
        // A stale-looking body next to an unreadable metadata record: the
        // sweep cannot know it is stale, so it must keep it and carry on.
        shard.put_raw(&meta_key("bad"), b"{not json");
        shard.put_raw(&body_key("bad", "1-aaa"), b"{}");
        shard.compact().unwrap();
        assert!(shard.get_body("bad", "1-aaa").unwrap().is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn live_ids(shard: &mut Shard) -> Vec<String> {
        shard.live_docs().unwrap().keys().cloned().collect()
    }

    #[test]
    fn live_index_tracks_documents_and_rebuilds_on_open() {
        let dir = std::env::temp_dir().join(format!(
            "douchdb-live-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut shard = Shard::open(&dir, 1000).unwrap();
        let mut tx = Tx::new();
        tx.put_json(&meta_key("a"), &meta(false, 1)).unwrap();
        tx.put_json(&meta_key("b"), &meta(true, 2)).unwrap();
        shard.commit_at(tx, Some(2)).unwrap();
        assert_eq!(live_ids(&mut shard), ["a"]);

        // Once built, the index follows deletions and new writes.
        let mut tx = Tx::new();
        tx.put_json(&meta_key("a"), &meta(true, 3)).unwrap();
        tx.put_json(&meta_key("c"), &meta(false, 4)).unwrap();
        shard.commit_at(tx, Some(4)).unwrap();
        assert_eq!(live_ids(&mut shard), ["c"]);
        shard.flush_pending().unwrap();
        drop(shard);

        // A fresh process rebuilds the index from metadata on first use.
        let mut shard = Shard::open(&dir, 1000).unwrap();
        assert_eq!(live_ids(&mut shard), ["c"]);
        assert_eq!(shard.counts(), (1, 2));

        let mut tx = Tx::new();
        tx.put_json(&meta_key("d"), &meta(false, 5)).unwrap();
        shard.commit_at(tx, Some(5)).unwrap();
        assert_eq!(live_ids(&mut shard), ["c", "d"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn live_rank_follows_the_cursor_through_writes() {
        let dir = temp_shard_dir("live-rank");
        let mut shard = Shard::open(&dir, 1000).unwrap();
        let mut seq = 0;
        let mut tx = Tx::new();
        for id in ["b", "d", "f", "h", "j"] {
            seq += 1;
            tx.put_json(&meta_key(id), &meta(false, seq)).unwrap();
        }
        shard.commit_at(tx, Some(seq)).unwrap();
        let probes = ["a", "d", "e", "j", "z", "c", "f", "a", "h"];
        let check = |shard: &mut Shard| {
            for probe in probes {
                let expected = live_ids(shard)
                    .iter()
                    .filter(|id| id.as_str() < probe)
                    .count();
                assert_eq!(shard.live_rank(probe).unwrap(), expected, "rank of {probe}");
            }
        };
        check(&mut shard);
        // Writes below, at and above the cursor key: insert, delete, update.
        for (id, deleted) in [
            ("a", false),
            ("d", true),
            ("e", false),
            ("z", false),
            ("j", true),
        ] {
            shard.live_rank("f").unwrap();
            seq += 1;
            let mut tx = Tx::new();
            tx.put_json(&meta_key(id), &meta(deleted, seq)).unwrap();
            shard.commit_at(tx, Some(seq)).unwrap();
            check(&mut shard);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn scan_range_merges_overlay_and_stops_early_in_both_directions() {
        let dir = std::env::temp_dir().join(format!(
            "douchdb-range-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut shard = Shard::open(&dir, 1000).unwrap();
        // a, c and e are flushed to disk; b and d stay in the overlay, so
        // every scan below has to interleave the two sources.
        let mut tx = Tx::new();
        for id in ["a", "c", "e"] {
            tx.put_json(&meta_key(id), &meta(false, 1)).unwrap();
        }
        shard.commit_at(tx, Some(1)).unwrap();
        shard.flush_pending().unwrap();
        let mut tx = Tx::new();
        for id in ["b", "d"] {
            tx.put_json(&meta_key(id), &meta(false, 1)).unwrap();
        }
        shard.commit_at(tx, Some(2)).unwrap();

        let mut forward = Vec::new();
        shard
            .scan_range_while(b"m/", Some(&meta_key("c")), false, |key, _| {
                forward.push(String::from_utf8(key[2..].to_vec()).unwrap());
                forward.len() < 2
            })
            .unwrap();
        assert_eq!(forward, ["c", "d"]);

        let mut reverse = Vec::new();
        shard
            .scan_range_while(b"m/", Some(&meta_key("d")), true, |key, _| {
                reverse.push(String::from_utf8(key[2..].to_vec()).unwrap());
                reverse.len() < 2
            })
            .unwrap();
        assert_eq!(reverse, ["d", "c"]);

        // Starting exactly on a flushed key must include it.
        let mut from_disk = Vec::new();
        shard
            .scan_range_while(b"m/", Some(&meta_key("c")), true, |key, _| {
                from_disk.push(String::from_utf8(key[2..].to_vec()).unwrap());
                from_disk.len() < 2
            })
            .unwrap();
        assert_eq!(from_disk, ["c", "b"]);

        let mut all_reverse = Vec::new();
        shard
            .scan_range_while(b"m/", None, true, |key, _| {
                all_reverse.push(String::from_utf8(key[2..].to_vec()).unwrap());
                true
            })
            .unwrap();
        assert_eq!(all_reverse, ["e", "d", "c", "b", "a"]);

        let mut all_forward = Vec::new();
        shard
            .scan_range_while(b"m/", None, false, |key, _| {
                all_forward.push(String::from_utf8(key[2..].to_vec()).unwrap());
                true
            })
            .unwrap();
        assert_eq!(all_forward, ["a", "b", "c", "d", "e"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn scan_from_exact_key_includes_it() {
        let dir = std::env::temp_dir().join(format!("douchdb-scan-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut shard = Shard::open(&dir, 1000).unwrap();
        for seq in 1..=5u64 {
            let mut tx = Tx::new();
            tx.put(&seq_key(seq), format!("doc{seq}").as_bytes());
            shard.commit_at(tx, Some(seq)).unwrap();
            // Sequences 1-3 on disk, 4-5 still in the overlay.
            if seq == 3 {
                shard.flush_pending().unwrap();
            }
        }
        let all = shard.scan_from(b"s/", &seq_key(1)).unwrap();
        assert_eq!(all.len(), 5, "scan from seq 1 must include seq 1");
        let after = shard.scan_from(b"s/", &seq_key(2)).unwrap();
        assert_eq!(after.len(), 4, "scan from seq 2 must skip seq 1");
        let from_disk = shard.scan_from(b"s/", &seq_key(3)).unwrap();
        assert_eq!(
            from_disk.len(),
            3,
            "scan from seq 3 must include the flushed key"
        );
        let tail = shard.scan_from(b"s/", &seq_key(4)).unwrap();
        assert_eq!(tail.len(), 2, "scan from seq 4 must include 4 and 5");
        assert_eq!(shard.scan_from(b"s/", &seq_key(9)).unwrap().len(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn latest_changes_counts_match_walking_the_index() {
        let mut latest = LatestChanges::default();
        // A fixed pseudo-random walk: new documents and updates that move a
        // document from an old bucket to a new one, with gaps between
        // sequences so many buckets stay empty.
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut seq = 0;
        for _ in 0..5000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            seq += 1 + state % 7;
            latest.insert(format!("doc{}", state % 1500), seq);
            if seq % 97 == 0 {
                for cursor in [0, 1, seq / 3, seq / 2, seq - 1, seq, seq + 1, u64::MAX] {
                    assert_eq!(latest.count_after(cursor), latest.after(cursor).count());
                    assert_eq!(
                        latest.count_before(cursor),
                        latest.all().take_while(|(seq, _)| *seq < cursor).count()
                    );
                }
            }
        }
        for cursor in 0..=seq + 1 {
            assert_eq!(latest.count_after(cursor), latest.after(cursor).count());
            assert_eq!(
                latest.count_before(cursor),
                latest.all().take_while(|(seq, _)| *seq < cursor).count()
            );
        }
    }

    #[test]
    fn latest_changes_index_moves_documents_to_their_newest_sequence() {
        let mut latest = LatestChanges::default();
        latest.insert("a".into(), 1);
        latest.insert("_design/v".into(), 2);
        latest.insert("b".into(), 3);
        latest.insert("a".into(), 4); // a is updated: its old sequence goes
        let after = |since| {
            latest
                .after(since)
                .map(|(seq, id)| (seq, id.to_string()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            after(0),
            [(2, "_design/v".into()), (3, "b".into()), (4, "a".into())]
        );
        assert_eq!(after(3), [(4, "a".to_string())]);
        assert!(after(4).is_empty());
        assert!(after(u64::MAX).is_empty());
        assert_eq!(
            latest.all().rev().map(|(seq, _)| seq).collect::<Vec<_>>(),
            [4, 3, 2]
        );
        assert_eq!(latest.seq_of("a"), Some(4));
    }

    /// Updates delete the previous sequence entry. Once memtables have rolled
    /// over, a newer table can hold a sequence tombstone right after a
    /// metadata key range that does not contain a given document; that must
    /// not make the document disappear from point reads.
    #[test]
    fn point_reads_survive_updates_across_memtable_rollovers() {
        let dir = std::env::temp_dir().join(format!(
            "douchdb-rollover-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut shard = Shard::open(&dir, 1000).unwrap();
        let ids: Vec<String> = (0..40).map(|n| format!("doc{n:02}")).collect();
        let mut seqs = vec![0u64; ids.len()];
        let mut seq = 0u64;

        for round in 0..8usize {
            let mut tx = Tx::new();
            for (n, id) in ids.iter().enumerate() {
                // Round 0 creates everything; later rounds update a rotating
                // subset of the low ids, so newer tables cover the range of
                // the untouched high ids without containing them.
                if round > 0 && (n >= 20 || (n + round) % 3 != 0) {
                    continue;
                }
                seq += 1;
                if seqs[n] != 0 {
                    tx.delete(&seq_key(seqs[n]));
                }
                tx.put_json(&meta_key(id), &meta(false, seq)).unwrap();
                tx.put(&seq_key(seq), id.as_bytes());
                seqs[n] = seq;
            }
            shard.commit_at(tx, Some(seq)).unwrap();
            shard.flush_pending().unwrap();
            // Roll the memtable over into a table without compacting the
            // existing ones (the range lies beyond every key in the shard).
            shard.db.compact_range(b"\xfe", b"\xff").unwrap();
        }

        for (n, id) in ids.iter().enumerate() {
            let found = shard.get_meta(id).unwrap();
            assert_eq!(found.map(|m| m.seq), Some(seqs[n]), "{id}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

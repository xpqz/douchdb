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

pub const ATTEST_CHUNK: usize = 64 * 1024;

pub fn meta_key(docid: &str) -> Vec<u8> {
    key(b"m/", docid)
}

pub fn body_key(docid: &str, rev: &str) -> Vec<u8> {
    key(b"d/", &format!("{docid}/{rev}"))
}

pub fn blob_prefix(digest: &str, chunk: u32) -> Vec<u8> {
    let mut out = key(b"b/", &format!("{digest}/"));
    out.extend_from_slice(&chunk.to_be_bytes());
    out
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

fn key(prefix: &[u8], rest: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + rest.len());
    out.extend_from_slice(prefix);
    out.extend_from_slice(rest.as_bytes());
    out
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AttMeta {
    pub content_type: String,
    /// Lowercase hex md5 of the content.
    pub digest: String,
    pub length: u64,
    pub revpos: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DocMeta {
    pub tree: RevTree,
    pub seq: u64,
    /// revision -> attachments in document order (order is part of the hash)
    #[serde(default)]
    pub atts: BTreeMap<String, Vec<(String, AttMeta)>>,
}

#[derive(Clone, Debug)]
pub struct LiveDoc {
    pub rev: String,
    pub has_attachments: bool,
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

    /// Documents whose latest sequence is below `before`, in sequence order.
    pub fn before(&self, before: u64) -> impl DoubleEndedIterator<Item = (u64, &str)> {
        self.by_seq
            .range(..(before, String::new()))
            .map(|(seq, id)| (*seq, id.as_str()))
    }

    /// Every document, in sequence order.
    pub fn all(&self) -> impl DoubleEndedIterator<Item = (u64, &str)> {
        self.by_seq.iter().map(|(seq, id)| (*seq, id.as_str()))
    }

    pub fn seq_of(&self, id: &str) -> Option<u64> {
        self.by_id.get(id).copied()
    }

    /// Ids starting with `prefix`, in id order.
    pub fn ids_with_prefix<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = &'a String> {
        self.by_id
            .range::<str, _>((
                std::ops::Bound::Included(prefix),
                std::ops::Bound::Unbounded,
            ))
            .map(|(id, _)| id)
            .take_while(move |id| id.starts_with(prefix))
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
    seq: u64,
    seq_tracker: Option<Arc<AtomicU64>>,
    revs_limit: u64,
    /// Number of live documents, maintained on every metadata write so `GET
    /// /db` and `_all_docs` totals do not scan the whole `m/` namespace.
    docs: u64,
    /// Number of documents whose winning revision is deleted.
    deleted: u64,
    /// Live document id -> winning revision, built lazily on the first
    /// `_all_docs` range query and then maintained in memory. Keeping this out
    /// of LevelDB avoids an extra write per ingested document.
    live: Option<BTreeMap<String, LiveDoc>>,
    /// Document id -> latest sequence, built lazily for `_changes`. CouchDB
    /// returns one row per document at its latest change, not one row per
    /// historical sequence entry.
    latest: Option<LatestChanges>,
    /// Uncommitted operations, in order, plus the same view as a key overlay so
    /// jobs later in the same group observe earlier writes.
    pending: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    overlay: std::collections::HashMap<Vec<u8>, Option<Vec<u8>>>,
}

fn meta_state(meta: &DocMeta) -> Option<(String, bool, bool)> {
    meta.tree.winner().map(|winner| {
        let live = !meta.tree.is_deleted(&winner);
        let has_attachments = meta
            .atts
            .get(&winner)
            .map(|atts| !atts.is_empty())
            .unwrap_or(false);
        (winner, live, has_attachments)
    })
}

fn db_get_checked(db: &mut DB, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let snapshot = db.get_snapshot();
    let value = db
        .get_at(&snapshot, key)
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    Ok(value.map(|bytes| bytes.to_vec()))
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

        // Counters are new in this version. Existing databases get them
        // backfilled once on first open, then every metadata write keeps them
        // in sync.
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
                    if let Ok(meta) = serde_json::from_slice::<DocMeta>(&value) {
                        match meta_state(&meta) {
                            Some((_, true, _)) => docs += 1,
                            Some((_, false, _)) => deleted += 1,
                            None => {}
                        }
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
            seq,
            seq_tracker: None,
            revs_limit,
            docs,
            deleted,
            live: None,
            latest: None,
            pending: Vec::new(),
            overlay: std::collections::HashMap::new(),
        })
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
            let id = std::str::from_utf8(&key[2..])
                .map_err(|error| anyhow::anyhow!("doc id is not UTF-8: {error}"))?;
            let meta: DocMeta = serde_json::from_slice(value.as_ref())?;
            hasher.update(b"doc");
            hasher.update(id.as_bytes());
            match meta_state(&meta) {
                Some((winner, live, _)) => {
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
            for (rev, atts) in &meta.atts {
                // Empty attachment vectors can differ between replicas without
                // changing the document content.
                if atts.is_empty() {
                    continue;
                }
                hasher.update(b"att");
                hasher.update(rev.as_bytes());
                for (name, att) in atts {
                    hasher.update(name.as_bytes());
                    hasher.update(att.digest.as_bytes());
                    hasher.update(att.length.to_be_bytes());
                    hasher.update(att.revpos.to_be_bytes());
                }
            }
            documents += 1;
            if !iter.advance() {
                break;
            }
        }
        hasher.update(documents.to_be_bytes());
        Ok(crate::db::hex(&hasher.finalize()))
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
        Ok(live
            .range::<str, _>((
                std::ops::Bound::Included(first.as_str()),
                std::ops::Bound::Excluded(last.as_str()),
            ))
            .count() as u64)
    }

    /// Live and deleted document counts for one partition. The partition
    /// prefix keeps the scan inside that partition's key range.
    pub fn partition_counts(&mut self, partition: &str) -> Result<(u64, u64)> {
        let prefix = format!("m/{partition}:");
        let mut docs = 0u64;
        let mut deleted = 0u64;
        self.scan_range_while(prefix.as_bytes(), None, false, |_, value| {
            if let Ok(meta) = serde_json::from_slice::<DocMeta>(value) {
                if let Some((_, live, _)) = meta_state(&meta) {
                    if live {
                        docs += 1;
                    } else {
                        deleted += 1;
                    }
                }
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
            // `scan_range_while` merges the uncommitted overlay, so a read in
            // the same group commit sees writes queued before it.
            self.scan_range_while(b"m/", None, false, |key, value| {
                if let Ok(id) = std::str::from_utf8(&key[2..]) {
                    if let Ok(meta) = serde_json::from_slice::<DocMeta>(value) {
                        if let Some((rev, true, has_attachments)) = meta_state(&meta) {
                            live.insert(
                                id.to_string(),
                                LiveDoc {
                                    rev,
                                    has_attachments,
                                },
                            );
                        }
                    }
                }
                true
            })?;
            self.live = Some(live);
        }
        Ok(self.live.as_ref().expect("live index initialized"))
    }

    /// Each document's latest sequence, built lazily from metadata. Used by
    /// `_changes`, which returns one row per document.
    pub fn latest_changes(&mut self) -> Result<&LatestChanges> {
        if self.latest.is_none() {
            let mut latest = LatestChanges::default();
            self.scan_range_while(b"m/", None, false, |key, value| {
                if let Ok(id) = std::str::from_utf8(&key[2..]) {
                    if let Ok(meta) = serde_json::from_slice::<DocMeta>(value) {
                        latest.insert(id.to_string(), meta.seq);
                    }
                }
                true
            })?;
            self.latest = Some(latest);
        }
        Ok(self.latest.as_ref().expect("latest changes initialized"))
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

    pub fn set_revs_limit(&mut self, limit: u64) -> Result<()> {
        self.revs_limit = limit.max(1);
        self.db
            .put(b"z/revs_limit", &self.revs_limit.to_be_bytes())?;
        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(value) = self.overlay.get(key) {
            return value.clone();
        }
        self.db.get(key).map(|b| b.to_vec())
    }

    /// Like `get`, but surfaces LevelDB I/O and checksum errors instead of
    /// turning them into `None`. Document reads use this so corruption is a
    /// 500, never a silent 404.
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

    pub fn get_meta(&mut self, docid: &str) -> Result<Option<DocMeta>> {
        let Some(bytes) = self.get_checked(&meta_key(docid))? else {
            return Ok(None);
        };
        let meta = serde_json::from_slice(&bytes)
            .map_err(|error| anyhow::anyhow!("corrupt metadata for {docid}: {error}"))?;
        Ok(Some(meta))
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

            let mut overlay = overlay.into_iter().rev().peekable();
            loop {
                let disk = if iter.valid() {
                    iter.current()
                        .map(|(key, value)| (key.to_vec(), value.to_vec()))
                } else {
                    None
                };
                let disk = disk.filter(|(key, _)| key.starts_with(prefix));
                let take_disk = match (&disk, overlay.peek()) {
                    (Some((disk_key, _)), Some((overlay_key, _))) => {
                        disk_key.as_slice() >= overlay_key.as_slice()
                    }
                    (Some(_), None) => true,
                    (None, Some(_)) => false,
                    (None, None) => break,
                };
                if take_disk {
                    let (key, value) = disk.expect("disk item");
                    iter.prev();
                    if let Some((overlay_key, _)) = overlay.peek() {
                        if overlay_key.as_slice() == key.as_slice() {
                            let (_, overlay_value) = overlay.next().unwrap();
                            if let Some(value) = overlay_value {
                                if !visit(&key, &value) {
                                    return Ok(());
                                }
                            }
                            continue;
                        }
                    }
                    if !visit(&key, &value) {
                        return Ok(());
                    }
                } else {
                    let (key, value) = overlay.next().unwrap();
                    if let Some(value) = value {
                        if !visit(&key, &value) {
                            return Ok(());
                        }
                    }
                }
            }
            for (key, value) in overlay {
                if let Some(value) = value {
                    if !visit(&key, &value) {
                        break;
                    }
                }
            }
        } else {
            match start {
                Some(lower) => iter.seek(lower),
                None => iter.seek(prefix),
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
                    (Some((disk_key, _)), Some((overlay_key, _))) => {
                        disk_key.as_slice() <= overlay_key.as_slice()
                    }
                    (Some(_), None) => true,
                    (None, Some(_)) => false,
                    (None, None) => break,
                };
                if take_disk {
                    let (key, value) = disk.expect("disk item");
                    iter.advance();
                    if let Some((overlay_key, _)) = overlay.peek() {
                        if overlay_key.as_slice() == key.as_slice() {
                            let (_, overlay_value) = overlay.next().unwrap();
                            if let Some(value) = overlay_value {
                                if !visit(&key, &value) {
                                    return Ok(());
                                }
                            }
                            continue;
                        }
                    }
                    if !visit(&key, &value) {
                        return Ok(());
                    }
                } else {
                    let (key, value) = overlay.next().unwrap();
                    if let Some(value) = value {
                        if !visit(&key, &value) {
                            return Ok(());
                        }
                    }
                }
            }
            for (key, value) in overlay {
                if let Some(value) = value {
                    if !visit(&key, &value) {
                        break;
                    }
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
        if let Some(seq) = seq {
            self.seq = seq;
        }
        if let Some(tracker) = &self.seq_tracker {
            tracker.fetch_max(self.seq, Ordering::SeqCst);
        }
        tx.ops
            .push((b"z/seq".to_vec(), Some(self.seq.to_be_bytes().to_vec())));

        let mut docs = self.docs as i64;
        let mut deleted = self.deleted as i64;
        for (key, value) in tx.ops {
            if key.starts_with(b"m/") {
                // `get` includes earlier operations in this same transaction,
                // so batched updates to one document still produce one final
                // delta.
                let old = self
                    .get(&key)
                    .and_then(|bytes| serde_json::from_slice::<DocMeta>(&bytes).ok());
                let new = value
                    .as_ref()
                    .and_then(|bytes| serde_json::from_slice::<DocMeta>(bytes).ok());
                let old_state = old.as_ref().and_then(meta_state);
                let new_state = new.as_ref().and_then(meta_state);
                match old_state.as_ref().map(|(_, live, _)| *live) {
                    Some(true) => docs -= 1,
                    Some(false) => deleted -= 1,
                    None => {}
                }
                match new_state.as_ref().map(|(_, live, _)| *live) {
                    Some(true) => docs += 1,
                    Some(false) => deleted += 1,
                    None => {}
                }
                if let Some(live) = self.live.as_mut() {
                    if let Ok(id) = std::str::from_utf8(&key[2..]) {
                        match new_state {
                            Some((rev, true, has_attachments)) => {
                                live.insert(
                                    id.to_string(),
                                    LiveDoc {
                                        rev,
                                        has_attachments,
                                    },
                                );
                            }
                            _ => {
                                live.remove(id);
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
        self.db.write(batch, true)?;
        self.overlay.clear();
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
                if key.as_ref() != b"z/revs_limit" {
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
        self.latest = None;
        self.pending.clear();
        self.overlay.clear();
        self.db
            .put(b"z/revs_limit", &self.revs_limit.to_be_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    pub fn compact(&mut self) -> Result<()> {
        self.flush_pending()?;
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
}

impl ShardHandle {
    pub fn spawn(dir: impl AsRef<Path>, revs_limit: u64) -> Result<ShardHandle> {
        let dir = dir.as_ref().to_path_buf();
        let name = format!(
            "shard-{}",
            dir.file_name().and_then(|n| n.to_str()).unwrap_or("?")
        );
        let seq_tracker = Arc::new(AtomicU64::new(0));
        let (tx, mut rx) = mpsc::channel::<Job>(256);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<u64>>();
        let tracker = seq_tracker.clone();
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
            shard.set_seq_tracker(tracker);
            if ready_tx.send(Ok(initial_seq)).is_err() {
                return;
            }
            // Group commit: run a job, then everything already queued, and
            // acknowledge only after the merged batch has been flushed to the
            // OS. rusty-leveldb does not fsync, so this is not power-loss
            // durable.
            while let Some(job) = rx.blocking_recv() {
                let mut acks = vec![job(&mut shard)];
                for _ in 1..64 {
                    match rx.try_recv() {
                        Ok(job) => acks.push(job(&mut shard)),
                        Err(_) => break,
                    }
                }
                let flush_error = shard.flush_pending().err().map(|e| e.to_string());
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
        })
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
                    // The job may have reserved a sequence it will never
                    // commit. Count it as committed so the changes watermark
                    // does not wait on it forever; the write was discarded, so
                    // this only leaves a gap in the sequence numbers.
                    seq_tracker.fetch_max(reserved_seq.load(Ordering::SeqCst), Ordering::SeqCst);
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
        ShardHandle::spawn(&dir, 1000).unwrap()
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
        let value = handle.call(|shard| Ok(shard.get(b"x/a"))).await.unwrap();
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
        let handle = ShardHandle::spawn(&dir, 1000).unwrap();
        handle.call(put(b"x/kept")).await.unwrap();
        drop(handle);
        // No waiting: the LevelDB LOCK must already be released.
        let mut reopened = Shard::open(&dir, 1000).unwrap();
        assert_eq!(reopened.get(b"x/kept").as_deref(), Some(b"v".as_slice()));
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
            .call(|shard| Ok((shard.get(b"x/before"), shard.get(b"x/partial"))))
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
        DocMeta {
            tree,
            seq,
            atts: BTreeMap::new(),
        }
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

        // Simulate a database written before counters existed: open must
        // backfill them from the metadata.
        shard.db.delete(b"z/doc_count").unwrap();
        shard.db.delete(b"z/doc_deleted_count").unwrap();
        shard.db.flush().unwrap();
        drop(shard);
        let shard = Shard::open(&dir, 1000).unwrap();
        assert_eq!(shard.counts(), (0, 2), "missing counters are backfilled");
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
            tx.put(&meta_key(id), id.as_bytes());
        }
        shard.commit_at(tx, Some(1)).unwrap();
        shard.flush_pending().unwrap();
        let mut tx = Tx::new();
        for id in ["b", "d"] {
            tx.put(&meta_key(id), id.as_bytes());
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
                    assert_eq!(latest.count_before(cursor), latest.before(cursor).count());
                }
            }
        }
        for cursor in 0..=seq + 1 {
            assert_eq!(latest.count_after(cursor), latest.after(cursor).count());
            assert_eq!(latest.count_before(cursor), latest.before(cursor).count());
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
            latest.before(3).map(|(seq, _)| seq).collect::<Vec<_>>(),
            [2]
        );
        assert_eq!(
            latest.all().rev().map(|(seq, _)| seq).collect::<Vec<_>>(),
            [4, 3, 2]
        );
        assert_eq!(latest.seq_of("a"), Some(4));
        assert_eq!(
            latest.ids_with_prefix("_design/").collect::<Vec<_>>(),
            ["_design/v"]
        );
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

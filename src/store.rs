//! LevelDB access and the key codec.
//!
//! One LevelDB database per (database, shard). Every key is chosen so that
//! LevelDB's sort order is the index we need.

use crate::error::ApiError;
use crate::revtree::RevTree;
use anyhow::Result;
use rusty_leveldb::{LdbIterator, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
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

pub fn seq_of_key(key: &[u8]) -> Option<u64> {
    key.get(2..10)
        .and_then(|b| b.try_into().ok())
        .map(u64::from_be_bytes)
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

/// A batch of changes plus the sequence counter, committed atomically.
///
/// Staged as plain operations rather than a `WriteBatch` so the shard actor can
/// merge several request batches into one fsync.
pub struct Tx {
    ops: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl Tx {
    pub fn new() -> Tx {
        Tx { ops: Vec::new() }
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

pub struct Shard {
    db: DB,
    seq: u64,
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
    /// Uncommitted operations, in order, plus the same view as a key overlay so
    /// jobs later in the same group observe earlier writes.
    pending: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    overlay: std::collections::HashMap<Vec<u8>, Option<Vec<u8>>>,
}

/// Winner revision, whether it is live, and whether that revision has
/// attachments. `None` when the revision tree has no winner.
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

fn read_counter(db: &mut DB, key: &[u8]) -> Option<u64> {
    db.get(key)
        .filter(|bytes| bytes.len() == 8)
        .map(|bytes| u64::from_be_bytes(bytes[..].try_into().unwrap()))
}

impl Shard {
    pub fn open(dir: impl AsRef<Path>, revs_limit: u64) -> Result<Shard> {
        let mut opts = Options::default();
        opts.create_if_missing = true;
        let mut db = DB::open(dir, opts)?;
        let seq = db
            .get(b"z/seq")
            .filter(|b| b.len() == 8)
            .map(|b| u64::from_be_bytes(b[..].try_into().unwrap()))
            .unwrap_or(0);
        let revs_limit = db
            .get(b"z/revs_limit")
            .filter(|b| b.len() == 8)
            .map(|b| u64::from_be_bytes(b[..].try_into().unwrap()))
            .unwrap_or(revs_limit);

        // Counters are new in this version. Existing databases get them
        // backfilled once on first open, then every metadata write keeps them
        // in sync.
        let stored = read_counter(&mut db, b"z/doc_count")
            .zip(read_counter(&mut db, b"z/doc_deleted_count"));
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
            revs_limit,
            docs,
            deleted,
            live: None,
            pending: Vec::new(),
            overlay: std::collections::HashMap::new(),
        })
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Live and deleted document counts for this shard.
    pub fn counts(&self) -> (u64, u64) {
        (self.docs, self.deleted)
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

    pub fn get_meta(&mut self, docid: &str) -> Option<DocMeta> {
        self.get(&meta_key(docid))
            .and_then(|b| serde_json::from_slice(&b).ok())
    }

    pub fn get_body(&mut self, docid: &str, rev: &str) -> Option<Vec<u8>> {
        self.get(&body_key(docid, rev))
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

    pub fn compact(&mut self) -> Result<()> {
        self.flush_pending()?;
        self.db.compact_range(&[], &[])?;
        self.db.flush()?;
        Ok(())
    }
}

type Ack = Box<dyn FnOnce(Option<String>) + Send>;
type Job = Box<dyn FnOnce(&mut Shard) -> Ack + Send>;

/// Every shard is owned by exactly one task: rusty-leveldb takes `&mut self`
/// even for reads, so serialised access is not a choice.
pub struct ShardHandle {
    tx: mpsc::Sender<Job>,
    initial_seq: u64,
}

impl ShardHandle {
    pub fn spawn(dir: impl AsRef<Path>, revs_limit: u64) -> Result<ShardHandle> {
        let mut shard = Shard::open(dir, revs_limit)?;
        let initial_seq = shard.seq();
        let (tx, mut rx) = mpsc::channel::<Job>(256);
        tokio::spawn(async move {
            // Group commit: run a job, then everything already queued, and
            // acknowledge only after the merged batch has been fsynced.
            while let Some(job) = rx.recv().await {
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
        });
        Ok(ShardHandle { tx, initial_seq })
    }

    /// Sequence number this shard had when it was opened.
    pub fn initial_seq(&self) -> u64 {
        self.initial_seq
    }

    /// Run a job on the shard task, which owns the database handle.
    pub async fn call<R, F>(&self, f: F) -> std::result::Result<R, ApiError>
    where
        R: Send + 'static,
        F: FnOnce(&mut Shard) -> std::result::Result<R, ApiError> + Send + 'static,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        let job: Job = Box::new(move |shard| {
            let result = f(shard);
            Box::new(move |flush_error: Option<String>| {
                let result = match flush_error {
                    Some(message) => Err(ApiError::new(500, "internal_server_error", message)),
                    None => result,
                };
                let _ = reply_tx.send(result);
            })
        });
        self.tx
            .send(job)
            .await
            .map_err(|_| ApiError::new(500, "internal_server_error", "shard task is gone"))?;
        reply_rx
            .await
            .map_err(|_| ApiError::new(500, "internal_server_error", "shard task dropped reply"))?
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
        assert_eq!(seq_of_key(&seq_key(42)).unwrap(), 42);
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
        let mut tx = Tx::new();
        for id in ["a", "b", "c"] {
            tx.put(&meta_key(id), id.as_bytes());
        }
        shard.commit_at(tx, Some(1)).unwrap();
        // Uncommitted, so this also exercises the overlay merge path.
        let mut tx = Tx::new();
        for id in ["d", "e"] {
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

        let mut all_reverse = Vec::new();
        shard
            .scan_range_while(b"m/", None, true, |key, _| {
                all_reverse.push(String::from_utf8(key[2..].to_vec()).unwrap());
                true
            })
            .unwrap();
        assert_eq!(all_reverse, ["e", "d", "c", "b", "a"]);
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
        }
        let all = shard.scan_from(b"s/", &seq_key(1)).unwrap();
        assert_eq!(all.len(), 5, "scan from seq 1 must include seq 1");
        let after = shard.scan_from(b"s/", &seq_key(2)).unwrap();
        assert_eq!(after.len(), 4, "scan from seq 2 must skip seq 1");
        let tail = shard.scan_from(b"s/", &seq_key(4)).unwrap();
        assert_eq!(tail.len(), 2, "scan from seq 4 must include 4 and 5");
        assert_eq!(shard.scan_from(b"s/", &seq_key(9)).unwrap().len(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

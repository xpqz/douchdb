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

pub struct Shard {
    db: DB,
    seq: u64,
    revs_limit: u64,
    /// Uncommitted operations, in order, plus the same view as a key overlay so
    /// jobs later in the same group observe earlier writes.
    pending: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    overlay: std::collections::HashMap<Vec<u8>, Option<Vec<u8>>>,
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
        Ok(Shard {
            db,
            seq,
            revs_limit,
            pending: Vec::new(),
            overlay: std::collections::HashMap::new(),
        })
    }

    pub fn seq(&self) -> u64 {
        self.seq
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

    /// Every key/value pair under `prefix`, in LevelDB order.
    pub fn scan(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_from(prefix, prefix)
    }

    /// Every key/value pair under `prefix`, starting at `start`. `start` must
    /// sort within the prefix's range; this is how sequence ranges are read
    /// without scanning the whole namespace.
    pub fn scan_from(&mut self, prefix: &[u8], start: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Merge the uncommitted overlay over the on-disk view. Overlay entries
        // are few (one group commit's worth), so this stays cheap.
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        let mut iter = self.db.new_iter()?;
        iter.seek(start);
        while let Some((k, v)) = iter.current() {
            if !k.starts_with(prefix) {
                break;
            }
            merged.insert(k.to_vec(), Some(v.to_vec()));
            if !iter.advance() {
                break;
            }
        }
        for (key, value) in &self.overlay {
            if key.starts_with(prefix) && key.as_slice() >= start {
                merged.insert(key.clone(), value.clone());
            }
        }
        Ok(merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect())
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
        for (key, value) in tx.ops {
            self.overlay.insert(key.clone(), value.clone());
            self.pending.push((key, value));
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

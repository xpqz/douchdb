//! LevelDB access and the key codec.
//!
//! One LevelDB database per (database, shard). Every key is chosen so that
//! LevelDB's sort order is the index we need.

#![allow(dead_code)] // codec is complete; callers arrive in milestone 1

use crate::rev::Rev;
use rusty_leveldb::{Options, WriteBatch, DB};
use std::path::Path;

pub fn meta_key(docid: &str) -> Vec<u8> {
    key(b"m/", docid)
}

pub fn body_key(docid: &str, rev: &Rev) -> Vec<u8> {
    key(b"d/", &format!("{docid}/{}", rev.pos))
}

pub fn att_key(docid: &str, rev: &Rev, name: &str, chunk: u32) -> Vec<u8> {
    key(b"a/", &format!("{docid}/{}", rev.pos))
        .into_iter()
        .chain(name.as_bytes().iter().copied())
        .chain(b"/".iter().copied())
        .chain(chunk.to_be_bytes())
        .collect()
}

/// Big-endian so that byte order is numeric order.
pub fn seq_key(seq: u64, docid: &str) -> Vec<u8> {
    key(b"s/", "")
        .into_iter()
        .chain(seq.to_be_bytes())
        .chain(b"/".iter().copied())
        .chain(docid.as_bytes().iter().copied())
        .collect()
}

pub fn local_key(id: &str) -> Vec<u8> {
    key(b"l/", id)
}

pub fn seq_of(key: &[u8]) -> Option<u64> {
    key.get(2..10)
        .and_then(|b| b.try_into().ok())
        .map(u64::from_be_bytes)
}

pub fn docid_of_seq_key(key: &[u8]) -> Option<&str> {
    std::str::from_utf8(key.get(11..)?).ok()
}

fn key(prefix: &[u8], rest: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + rest.len());
    out.extend_from_slice(prefix);
    out.extend_from_slice(rest.as_bytes());
    out
}

/// A single shard. The LevelDB API takes `&mut self` for reads too, so a shard
/// is owned by exactly one actor task and reached only through its mailbox.
pub struct Shard {
    db: DB,
}

#[allow(dead_code)] // wired up in milestone 1
impl Shard {
    pub fn open(dir: impl AsRef<Path>) -> anyhow::Result<Shard> {
        let mut opts = Options::default();
        opts.create_if_missing = true;
        Ok(Shard {
            db: DB::open(dir, opts)?,
        })
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.get(key).map(|bytes| bytes.to_vec())
    }

    /// Commit a batch of changes atomically, fsynced.
    pub fn commit(&mut self, batch: WriteBatch) -> anyhow::Result<()> {
        self.db.write(batch, true)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docids_sort_like_couchdb() {
        // Raw UTF-8 byte order, which is what CouchDB's binary sort gives.
        assert!(meta_key("a") < meta_key("b"));
        assert!(meta_key("Z") < meta_key("a"));
        assert!(meta_key("a") < meta_key("aa"));
        assert!(meta_key("_design/x") < meta_key("a"));
    }

    #[test]
    fn seq_keys_sort_numerically() {
        assert!(seq_key(2, "z") < seq_key(10, "a"));
        assert!(seq_key(255, "a") < seq_key(256, "a"));
        assert_eq!(seq_of(&seq_key(42, "doc")).unwrap(), 42);
        assert_eq!(docid_of_seq_key(&seq_key(42, "doc")).unwrap(), "doc");
    }

    #[test]
    fn prefixes_are_disjoint() {
        assert!(local_key("x") < meta_key("x"));
        assert!(meta_key("x") < seq_key(0, "x"));
    }
}

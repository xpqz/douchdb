//! Cluster placement: static node list, rendezvous hashing, request routing.

use md5::{Digest, Md5};

#[derive(Clone, Default)]
pub struct Cluster {
    /// Base URLs, e.g. `http://127.0.0.1:5984`.
    pub nodes: Vec<String>,
    /// Index of this node in `nodes`.
    pub me: usize,
}

impl Cluster {
    pub fn is_clustered(&self) -> bool {
        self.nodes.len() > 1
    }

    pub fn shard_of(&self, docid: &str, q: u32) -> u32 {
        if q <= 1 {
            return 0;
        }
        let digest = Md5::digest(docid.as_bytes());
        u32::from_be_bytes(digest[..4].try_into().unwrap()) % q
    }

    /// Rendezvous hashing: the node with the highest score for this shard owns
    /// it. Adding a node moves roughly 1/N of the shards and needs no table.
    pub fn owner_of_shard(&self, shard: u32) -> usize {
        if self.nodes.is_empty() {
            return 0;
        }
        self.nodes
            .iter()
            .enumerate()
            .map(|(i, node)| (score(node, shard), i))
            .max()
            .map(|(_, i)| i)
            .unwrap_or(0)
    }

    pub fn owner_of_doc(&self, docid: &str, q: u32) -> usize {
        self.owner_of_shard(self.shard_of(docid, q))
    }

    pub fn is_local(&self, docid: &str, q: u32) -> bool {
        !self.is_clustered() || self.owner_of_doc(docid, q) == self.me
    }

    pub fn owner_url(&self, docid: &str, q: u32) -> Option<&str> {
        if self.is_local(docid, q) {
            return None;
        }
        self.nodes
            .get(self.owner_of_doc(docid, q))
            .map(String::as_str)
    }

    pub fn peers(&self) -> impl Iterator<Item = &String> {
        self.nodes
            .iter()
            .enumerate()
            .filter(move |(i, _)| *i != self.me)
            .map(|(_, n)| n)
    }
}

fn score(node: &str, shard: u32) -> u64 {
    let digest = Md5::digest(format!("{node}#{shard}").as_bytes());
    u64::from_be_bytes(digest[..8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shards_are_stable_and_spread() {
        let cluster = Cluster {
            nodes: vec!["a".into(), "b".into(), "c".into()],
            me: 0,
        };
        assert_eq!(cluster.shard_of("doc", 8), cluster.shard_of("doc", 8));
        let spread: std::collections::HashSet<_> = (0..200)
            .map(|i| cluster.owner_of_doc(&format!("d{i}"), 8))
            .collect();
        assert_eq!(spread.len(), 3, "all nodes own something");
    }

    #[test]
    fn single_node_owns_everything() {
        let cluster = Cluster::default();
        assert!(!cluster.is_clustered());
        assert!(cluster.is_local("anything", 8));
        assert_eq!(cluster.owner_url("anything", 8), None);
    }

    #[test]
    fn ownership_is_deterministic_across_nodes() {
        let nodes = vec!["a".into(), "b".into()];
        let a = Cluster {
            nodes: nodes.clone(),
            me: 0,
        };
        let b = Cluster { nodes, me: 1 };
        for i in 0..50 {
            let id = format!("doc-{i}");
            assert_eq!(a.owner_of_doc(&id, 4), b.owner_of_doc(&id, 4));
        }
    }
}

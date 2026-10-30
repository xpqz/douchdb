//! Cluster placement: static node list, rendezvous hashing, request routing.

use md5::{Digest, Md5};

#[derive(Clone, Default)]
pub struct Cluster {
    /// Base URLs, e.g. `http://127.0.0.1:5984`.
    pub nodes: Vec<String>,
    /// Index of this node in `nodes`.
    pub me: usize,
    /// Replication factor: how many distinct nodes hold each shard.
    pub n: usize,
}

impl Cluster {
    pub fn is_clustered(&self) -> bool {
        self.nodes.len() > 1
    }

    /// Effective replication factor, clamped to the node count.
    pub fn replication_factor(&self) -> usize {
        self.n.max(1).min(self.nodes.len().max(1))
    }

    pub fn shard_of(&self, docid: &str, q: u32) -> u32 {
        if q <= 1 {
            return 0;
        }
        let digest = Md5::digest(docid.as_bytes());
        u32::from_be_bytes(digest[..4].try_into().unwrap()) % q
    }

    /// Rendezvous hashing over `(node, shard)`. The highest `n` scores are the
    /// replica set, in preference order. Every node computes the same list from
    /// the same static configuration, so no metadata needs distributing.
    pub fn replicas_of_shard(&self, shard: u32) -> Vec<usize> {
        if self.nodes.is_empty() {
            return vec![0];
        }
        let mut ranked: Vec<(u64, usize)> = self
            .nodes
            .iter()
            .enumerate()
            .map(|(i, node)| (score(node, shard), i))
            .collect();
        ranked.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)));
        ranked
            .into_iter()
            .take(self.replication_factor())
            .map(|(_, i)| i)
            .collect()
    }

    /// The first replica is the primary: reads and writes coordinate through
    /// it, while the remaining replicas receive the same revision.
    pub fn owner_of_shard(&self, shard: u32) -> usize {
        self.replicas_of_shard(shard).first().copied().unwrap_or(0)
    }

    pub fn owner_of_doc(&self, docid: &str, q: u32) -> usize {
        self.owner_of_shard(self.shard_of(docid, q))
    }

    pub fn replicas_of_doc(&self, docid: &str, q: u32) -> Vec<usize> {
        // Every node needs design documents to evaluate views over its local
        // shards, even when its replica set for the document would be smaller.
        // `_local` documents stay node-local.
        if docid.starts_with("_design/") {
            if self.nodes.is_empty() {
                return vec![0];
            }
            return (0..self.nodes.len()).collect();
        }
        self.replicas_of_shard(self.shard_of(docid, q))
    }

    pub fn is_replica_of(&self, docid: &str, q: u32) -> bool {
        !self.is_clustered() || self.replicas_of_doc(docid, q).contains(&self.me)
    }

    #[allow(dead_code)]
    pub fn is_local(&self, docid: &str, q: u32) -> bool {
        !self.is_clustered() || self.owner_of_doc(docid, q) == self.me
    }

    #[allow(dead_code)]
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

    fn cluster(n: usize) -> Cluster {
        Cluster {
            nodes: vec!["a".into(), "b".into(), "c".into()],
            me: 0,
            n,
        }
    }

    #[test]
    fn shards_are_stable_and_spread() {
        let cluster = cluster(1);
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
        assert_eq!(cluster.replicas_of_doc("anything", 8), vec![0]);
    }

    #[test]
    fn ownership_is_deterministic_across_nodes() {
        let nodes = vec!["a".into(), "b".into()];
        let a = Cluster {
            nodes: nodes.clone(),
            me: 0,
            n: 2,
        };
        let b = Cluster { nodes, me: 1, n: 2 };
        for i in 0..50 {
            let id = format!("doc-{i}");
            assert_eq!(a.owner_of_doc(&id, 4), b.owner_of_doc(&id, 4));
            assert_eq!(a.replicas_of_doc(&id, 4), b.replicas_of_doc(&id, 4));
        }
    }

    #[test]
    fn replica_sets_are_distinct_and_have_the_requested_size() {
        let c = cluster(2);
        for shard in 0..64 {
            let replicas = c.replicas_of_shard(shard);
            assert_eq!(replicas.len(), 2);
            assert_ne!(replicas[0], replicas[1]);
            assert_eq!(replicas[0], c.owner_of_shard(shard));
        }
        // A larger factor is clamped to the node count.
        assert_eq!(cluster(9).replicas_of_shard(0).len(), 3);
    }

    #[test]
    fn design_documents_replicate_to_every_node() {
        let c = cluster(1);
        assert_eq!(c.replicas_of_doc("_design/foo", 4).len(), 3);
        assert_eq!(c.replicas_of_doc("ordinary", 4).len(), 1);
    }

    #[test]
    fn replica_sets_are_stable_when_a_node_is_added() {
        let three = cluster(1);
        let four = Cluster {
            nodes: vec!["a".into(), "b".into(), "c".into(), "d".into()],
            me: 0,
            n: 1,
        };
        let moved = (0..200)
            .filter(|shard| three.owner_of_shard(*shard) != four.owner_of_shard(*shard))
            .count();
        assert!(moved > 10 && moved < 100, "moved {moved} of 200 shards");
    }
}

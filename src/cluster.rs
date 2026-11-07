//! Cluster placement: the static node list, the per-database shard map, and
//! the rendezvous hashing that seeds new shard maps.

use axum::http::HeaderMap;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};

#[derive(Clone, Default)]
pub struct Cluster {
    /// Base URLs, e.g. `http://127.0.0.1:5984`.
    pub nodes: Vec<String>,
    /// Index of this node in `nodes`.
    pub me: usize,
    /// Replication factor: how many distinct nodes hold each shard.
    pub n: usize,
    /// Shared secret for internal routing headers. Empty on a single node.
    pub secret: String,
}

/// Per-database placement metadata, modelled on CouchDB's `_dbs`/mem3 shard
/// map: one ordered replica list per shard. The first entry is the preferred
/// primary; all entries are full replicas.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ShardMap {
    pub q: u32,
    pub node_count: usize,
    pub replicas: Vec<Vec<usize>>,
    /// Monotonically increasing metadata generation. Higher versions win
    /// when peers reconcile after a missed update.
    #[serde(default)]
    pub version: u64,
}

impl ShardMap {
    pub fn single(q: u32) -> ShardMap {
        let q = q.max(1);
        ShardMap {
            q,
            node_count: 1,
            replicas: vec![vec![0]; q as usize],
            version: 0,
        }
    }

    /// Evenly spread shard replicas across nodes. This is intentionally
    /// simple: for shard `s`, replica `k` goes to `(s + k) % node_count`.
    pub fn balanced(q: u32, node_count: usize, n: usize) -> ShardMap {
        let q = q.max(1);
        let node_count = node_count.max(1);
        let n = n.max(1).min(node_count);
        ShardMap {
            q,
            node_count,
            replicas: (0..q)
                .map(|shard| {
                    (0..n)
                        .map(|replica| (shard as usize + replica) % node_count)
                        .collect()
                })
                .collect(),
            version: 0,
        }
    }

    pub fn replicas_of(&self, shard: u32) -> &[usize] {
        self.replicas
            .get(shard as usize)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn owner_of(&self, shard: u32) -> usize {
        self.replicas_of(shard).first().copied().unwrap_or(0)
    }

    /// Copies of each shard. Every shard has the same count unless a
    /// rebalance is part way through.
    pub fn n(&self) -> usize {
        self.replicas.iter().map(Vec::len).max().unwrap_or(1).max(1)
    }

    pub fn is_valid(&self, q: u32, node_count: usize) -> bool {
        self.q == q.max(1)
            && self.node_count == node_count
            && self.replicas.len() == self.q as usize
            && self.replicas.iter().all(|replicas| {
                !replicas.is_empty() && replicas.iter().all(|node| *node < node_count)
            })
    }
}

impl Cluster {
    pub fn shard_map(&self, q: u32) -> ShardMap {
        self.shard_map_n(q, self.n)
    }

    /// A shard map with `n` copies of each shard, clamped to the node count.
    pub fn shard_map_n(&self, q: u32, n: usize) -> ShardMap {
        let q = q.max(1);
        if self.nodes.is_empty() {
            return ShardMap::single(q);
        }
        let n = n.max(1).min(self.nodes.len());
        ShardMap {
            q,
            node_count: self.nodes.len(),
            replicas: (0..q)
                .map(|shard| {
                    let mut replicas = self.replicas_of_shard_ranked(shard);
                    replicas.truncate(n);
                    replicas
                })
                .collect(),
            version: 0,
        }
    }

    pub fn is_clustered(&self) -> bool {
        self.nodes.len() > 1
    }

    /// Effective replication factor, clamped to the node count.
    pub fn replication_factor(&self) -> usize {
        self.n.max(1).min(self.nodes.len().max(1))
    }

    /// Every node, in rendezvous preference order for `shard`: the highest
    /// scores of `(node, shard)` first. Only used to seed a database's
    /// `ShardMap`; once created, the stored map is the source of truth for
    /// placement.
    fn replicas_of_shard_ranked(&self, shard: u32) -> Vec<usize> {
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
        ranked.into_iter().map(|(_, i)| i).collect()
    }

    /// True only for a peer that presents the shared cluster secret. A client
    /// cannot make a request look internal just by adding a header.
    pub fn is_internal(&self, headers: &HeaderMap) -> bool {
        !self.secret.is_empty()
            && headers.get("x-douchdb-forwarded").is_some()
            && self.secret_matches(headers)
    }

    pub fn is_internal_db(&self, headers: &HeaderMap) -> bool {
        !self.secret.is_empty()
            && headers.get("x-douchdb-local-db").is_some()
            && self.secret_matches(headers)
    }

    fn secret_matches(&self, headers: &HeaderMap) -> bool {
        headers
            .get("x-douchdb-secret")
            .and_then(|value| value.to_str().ok())
            .map(|value| constant_time_eq(value.as_bytes(), self.secret.as_bytes()))
            .unwrap_or(false)
    }

    pub fn peers(&self) -> impl Iterator<Item = &String> {
        self.nodes
            .iter()
            .enumerate()
            .filter(move |(i, _)| *i != self.me)
            .map(|(_, n)| n)
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
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
            secret: String::new(),
        }
    }

    /// Shards in the test maps below; large enough for every shard index used.
    const Q: u32 = 256;

    fn replicas_of_shard(cluster: &Cluster, shard: u32) -> Vec<usize> {
        cluster.shard_map(Q).replicas_of(shard).to_vec()
    }

    fn owner_of_shard(cluster: &Cluster, shard: u32) -> usize {
        cluster.shard_map(Q).owner_of(shard)
    }

    #[test]
    fn shard_owners_are_spread() {
        let cluster = cluster(1);
        let spread: std::collections::HashSet<_> = (0..64)
            .map(|shard| owner_of_shard(&cluster, shard))
            .collect();
        assert_eq!(spread.len(), 3, "all nodes own something");
    }

    #[test]
    fn single_node_owns_everything() {
        let cluster = Cluster::default();
        assert!(!cluster.is_clustered());
        assert_eq!(replicas_of_shard(&cluster, 0), vec![0]);
        assert_eq!(owner_of_shard(&cluster, 0), 0);
    }

    #[test]
    fn balanced_map_spreads_shards_evenly() {
        let map = ShardMap::balanced(6, 3, 2);
        assert_eq!(map.replicas.len(), 6);
        for replicas in &map.replicas {
            assert_eq!(replicas.len(), 2);
            assert_ne!(replicas[0], replicas[1]);
        }
        let mut counts = [0usize; 3];
        for shard in 0..6 {
            for node in map.replicas_of(shard) {
                counts[*node] += 1;
            }
        }
        assert!(counts.iter().max().unwrap() - counts.iter().min().unwrap() <= 1);
    }

    #[test]
    fn ownership_is_deterministic_across_nodes() {
        let nodes = vec!["a".into(), "b".into()];
        let a = Cluster {
            nodes: nodes.clone(),
            me: 0,
            n: 2,
            secret: String::new(),
        };
        let b = Cluster {
            nodes,
            me: 1,
            n: 2,
            secret: String::new(),
        };
        for shard in 0..50 {
            assert_eq!(owner_of_shard(&a, shard), owner_of_shard(&b, shard));
            assert_eq!(replicas_of_shard(&a, shard), replicas_of_shard(&b, shard));
        }
    }

    #[test]
    fn replica_sets_are_distinct_and_have_the_requested_size() {
        let c = cluster(2);
        for shard in 0..64 {
            let replicas = replicas_of_shard(&c, shard);
            assert_eq!(replicas.len(), 2);
            assert_ne!(replicas[0], replicas[1]);
            assert_eq!(replicas[0], owner_of_shard(&c, shard));
        }
        // A larger factor is clamped to the node count.
        assert_eq!(replicas_of_shard(&cluster(9), 0).len(), 3);
    }

    #[test]
    fn replica_sets_are_stable_when_a_node_is_added() {
        let three = cluster(1);
        let four = Cluster {
            nodes: vec!["a".into(), "b".into(), "c".into(), "d".into()],
            me: 0,
            n: 1,
            secret: String::new(),
        };
        let moved = (0..200)
            .filter(|shard| owner_of_shard(&three, *shard) != owner_of_shard(&four, *shard))
            .count();
        assert!(moved > 10 && moved < 100, "moved {moved} of 200 shards");

        // With two replicas per shard, adding node d may only swap d into a
        // replica set; the surviving members keep their copies.
        let three = cluster(2);
        let four = Cluster { n: 2, ..four };
        let mut changed = 0;
        for shard in 0..200 {
            let before: std::collections::BTreeSet<usize> =
                replicas_of_shard(&three, shard).into_iter().collect();
            let after: std::collections::BTreeSet<usize> =
                replicas_of_shard(&four, shard).into_iter().collect();
            if before != after {
                changed += 1;
                assert!(after.contains(&3), "shard {shard}: {before:?} -> {after:?}");
                assert_eq!(
                    before.intersection(&after).count(),
                    1,
                    "shard {shard}: {before:?} -> {after:?}"
                );
            }
        }
        assert!(
            changed > 10 && changed < 150,
            "changed {changed} of 200 sets"
        );
    }
}

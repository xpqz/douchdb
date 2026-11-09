//! Cluster placement: the static node list, the per-database shard map, and
//! the mem3-style balanced assignment that seeds new shard maps.

use axum::http::HeaderMap;
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

/// The shards of one database that a request covers: the cluster fan-out
/// gives each node the shards it answers for, and every shard-aware read
/// filters by it. A shard beyond the mask is not covered.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShardMask(Vec<bool>);

impl ShardMask {
    /// A mask over `q` shards that covers none of them.
    pub fn none(q: u32) -> ShardMask {
        ShardMask(vec![false; q as usize])
    }

    /// Cover `shard`; a shard beyond the mask is ignored.
    pub fn select(&mut self, shard: usize) {
        if let Some(flag) = self.0.get_mut(shard) {
            *flag = true;
        }
    }

    pub fn covers(&self, shard: usize) -> bool {
        self.0.get(shard).copied().unwrap_or(false)
    }

    /// No shard is covered.
    pub fn is_empty(&self) -> bool {
        !self.0.iter().any(|flag| *flag)
    }

    /// The number of shards the mask spans, covered or not.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// The covered shards, in order.
    pub fn shards(&self) -> Vec<usize> {
        (0..self.0.len()).filter(|shard| self.0[*shard]).collect()
    }

    /// The covered shards as the `shards` query parameter peers read back.
    pub fn query_value(&self) -> String {
        self.shards()
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl FromIterator<bool> for ShardMask {
    fn from_iter<I: IntoIterator<Item = bool>>(flags: I) -> ShardMask {
        ShardMask(flags.into_iter().collect())
    }
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
    /// Tests use it for a predictable placement; real placement is
    /// `Cluster::shard_map_n`.
    #[cfg(test)]
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
    pub fn shard_map(&self, name: &str, q: u32) -> ShardMap {
        self.shard_map_n(name, q, self.n)
    }

    /// A shard map with `n` copies of each shard, clamped to the node count.
    ///
    /// Mirrors CouchDB's `mem3:choose_shards/3` and
    /// `mem3_util:create_partition_map/5`: copies are dealt to the node list
    /// in order, `n` per range, wrapping around, so every node holds the same
    /// number of copies give or take one and no range has two copies on one
    /// node. CouchDB starts the deal at a random node; here the start is
    /// `crc32(name) % nodes` (as `mem3_util:rotate_list/2`), so every node
    /// computes the same map for a given database name.
    pub fn shard_map_n(&self, name: &str, q: u32, n: usize) -> ShardMap {
        let q = q.max(1);
        if self.nodes.is_empty() {
            return ShardMap::single(q);
        }
        let n = n.max(1).min(self.nodes.len());
        // mem3:nodes() is sorted; sort here too so the map does not depend on
        // the order the node list was configured in.
        let mut order: Vec<usize> = (0..self.nodes.len()).collect();
        order.sort_by(|a, b| self.nodes[*a].cmp(&self.nodes[*b]));
        let start = crc32(name.as_bytes()) as usize % order.len();
        ShardMap {
            q,
            node_count: self.nodes.len(),
            replicas: (0..q as usize)
                .map(|shard| {
                    (0..n)
                        .map(|copy| order[(start + shard * n + copy) % order.len()])
                        .collect()
                })
                .collect(),
            version: 0,
        }
    }

    /// The placement a rebalance moves `old` to.
    pub fn rebalance_target(&self, old: &ShardMap, n: usize) -> ShardMap {
        self.shard_map_n("", old.q, n)
    }

    pub fn is_clustered(&self) -> bool {
        self.nodes.len() > 1
    }

    /// Effective replication factor, clamped to the node count.
    pub fn replication_factor(&self) -> usize {
        self.n.max(1).min(self.nodes.len().max(1))
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

/// CRC-32 (IEEE), the checksum Erlang's `erlang:crc32/1` computes.
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_mask_covers_only_selected_shards() {
        let mut mask = ShardMask::none(4);
        assert!(mask.is_empty());
        mask.select(1);
        mask.select(3);
        mask.select(9);
        assert!(!mask.is_empty());
        assert_eq!(mask.len(), 4);
        assert!(mask.covers(1) && mask.covers(3));
        assert!(!mask.covers(0) && !mask.covers(9));
        assert_eq!(mask.shards(), vec![1, 3]);
        assert_eq!(mask.query_value(), "1,3");
    }

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
        cluster.shard_map("db", Q).replicas_of(shard).to_vec()
    }

    fn owner_of_shard(cluster: &Cluster, shard: u32) -> usize {
        cluster.shard_map("db", Q).owner_of(shard)
    }

    fn copies_per_node(map: &ShardMap, nodes: usize) -> Vec<usize> {
        let mut counts = vec![0; nodes];
        for replicas in &map.replicas {
            for node in replicas {
                counts[*node] += 1;
            }
        }
        counts
    }

    fn cluster_of(nodes: usize) -> Cluster {
        Cluster {
            nodes: (0..nodes).map(|i| format!("http://node{i}:5984")).collect(),
            me: 0,
            n: 1,
            secret: String::new(),
        }
    }

    fn assert_balanced(name: &str, nodes: usize, q: u32, n: usize) {
        let map = cluster_of(nodes).shard_map_n(name, q, n);
        let counts = copies_per_node(&map, nodes);
        let (min, max) = (counts.iter().min(), counts.iter().max());
        assert!(
            max.zip(min).is_some_and(|(max, min)| max - min <= 1),
            "{name} q={q} n={n}: copies per node {counts:?}"
        );
        for (shard, replicas) in map.replicas.iter().enumerate() {
            let distinct: std::collections::HashSet<_> = replicas.iter().collect();
            assert_eq!(replicas.len(), n, "{name} shard {shard}");
            assert_eq!(distinct.len(), n, "{name} shard {shard} reuses a node");
        }
    }

    #[test]
    fn default_placement_spreads_copies_evenly() {
        for name in ["a", "db", "users", "orders-2024", "x1", "x2", "x3", "x4"] {
            assert_balanced(name, 3, 4, 1);
            assert_balanced(name, 5, 8, 3);
            assert_balanced(name, 3, 2, 2);
            assert_balanced(name, 4, 8, 4);
        }
    }

    #[test]
    fn default_placement_is_deterministic_and_rotates_per_database() {
        let cluster = cluster_of(3);
        assert_eq!(
            cluster.shard_map_n("db", 4, 1),
            cluster.shard_map_n("db", 4, 1)
        );
        let firsts: std::collections::HashSet<_> = (0..16)
            .map(|i| cluster.shard_map_n(&format!("db{i}"), 4, 1).replicas[0][0])
            .collect();
        assert!(firsts.len() > 1, "every database starts on the same node");
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

    fn moved_copies(old: &ShardMap, new: &ShardMap) -> usize {
        old.replicas
            .iter()
            .zip(&new.replicas)
            .map(|(a, b)| b.iter().filter(|node| !a.contains(node)).count())
            .sum()
    }

    fn assert_target_invariants(target: &ShardMap, nodes: usize, n: usize) {
        let counts = copies_per_node(target, nodes);
        let spread =
            counts.iter().max().copied().unwrap_or(0) - counts.iter().min().copied().unwrap_or(0);
        assert!(spread <= 1, "copies per node {counts:?}");
        for replicas in &target.replicas {
            let distinct: std::collections::HashSet<_> = replicas.iter().collect();
            assert_eq!(replicas.len(), n);
            assert_eq!(distinct.len(), n, "range reuses a node: {replicas:?}");
            assert!(replicas.iter().all(|node| *node < nodes));
        }
    }

    #[test]
    fn rebalance_target_of_an_unchanged_cluster_is_the_old_map() {
        let cluster = cluster_of(3);
        for name in ["db", "alpha", "beta", "gamma"] {
            let old = cluster.shard_map_n(name, 8, 3);
            assert_eq!(cluster.rebalance_target(&old, 3), old, "{name}");
            let old = cluster.shard_map_n(name, 4, 1);
            assert_eq!(cluster.rebalance_target(&old, 1), old, "{name}");
        }
    }

    #[test]
    fn rebalance_target_moves_only_what_a_new_node_needs() {
        for name in ["db", "alpha", "beta", "gamma"] {
            let old = cluster_of(3).shard_map_n(name, 8, 3);
            let four = cluster_of(4);
            let target = four.rebalance_target(&old, 3);
            assert_target_invariants(&target, 4, 3);
            assert_eq!(copies_per_node(&target, 4), vec![6, 6, 6, 6]);
            assert_eq!(
                moved_copies(&old, &target),
                6,
                "{name}: {old:?} -> {target:?}"
            );
        }
    }

    #[test]
    fn rebalance_target_rehomes_only_a_removed_nodes_copies() {
        let old = cluster_of(4).shard_map_n("db", 8, 2);
        let three = cluster_of(3);
        let target = three.rebalance_target(&old, 2);
        assert_target_invariants(&target, 3, 2);
        let on_removed = old.replicas.iter().flatten().filter(|n| **n == 3).count();
        assert_eq!(moved_copies(&old, &target), on_removed);
        for (before, after) in old.replicas.iter().zip(&target.replicas) {
            for node in before.iter().filter(|node| **node != 3) {
                assert!(after.contains(node), "{before:?} -> {after:?}");
            }
        }
    }

    #[test]
    fn rebalance_target_holds_its_invariants_over_a_sweep() {
        for from in 1..=5usize {
            for to in 1..=5usize {
                for q in [1u32, 2, 3, 4, 7, 8, 16] {
                    for n in 1..=4usize {
                        let old = cluster_of(from).shard_map_n("sweep", q, n);
                        let target = cluster_of(to).rebalance_target(&old, n);
                        assert_target_invariants(&target, to, n.min(to));
                    }
                }
            }
        }
    }

    #[test]
    fn crc32_matches_the_standard_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }
}

//! Revision trees as an edge map.
//!
//! CouchDB stores a tree per document; we store the same thing as
//! `child -> parent` edges plus a set of tombstones. Leaves are revisions with
//! no children, and everything the API needs (conflicts, `_revisions`,
//! `_revs_info`, winner) is derived from that.

use crate::rev::Rev;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct RevTree {
    /// Every known revision maps to its parent, or `None` for a root.
    pub edges: BTreeMap<String, Option<String>>,
    /// Leaf revisions written as tombstones.
    pub deleted: BTreeSet<String>,
}

impl RevTree {
    /// Merge a root-to-leaf path, growing the tree where it already exists.
    pub fn merge_path(&mut self, path: &[Rev], deleted: bool) {
        for (i, rev) in path.iter().enumerate() {
            let parent = if i == 0 {
                None
            } else {
                Some(path[i - 1].to_string())
            };
            self.edges.entry(rev.to_string()).or_insert(parent);
        }
        if let Some(leaf) = path.last() {
            if deleted {
                self.deleted.insert(leaf.to_string());
            }
        }
    }

    pub fn leaves(&self) -> Vec<String> {
        let parents: BTreeSet<&String> = self.edges.values().flatten().collect();
        self.edges
            .keys()
            .filter(|rev| !parents.contains(rev))
            .cloned()
            .collect()
    }

    pub fn is_leaf(&self, rev: &str) -> bool {
        self.edges.contains_key(rev) && !self.edges.values().flatten().any(|p| p == rev)
    }

    pub fn is_deleted(&self, rev: &str) -> bool {
        self.deleted.contains(rev)
    }

    /// CouchDB's rule: non-deleted beats deleted, then higher position, then
    /// greater revision id.
    pub fn winner(&self) -> Option<String> {
        self.leaves().into_iter().max_by_key(|rev| {
            let parsed = Rev::parse(rev).expect("well-formed rev in tree");
            (!self.is_deleted(rev), parsed.pos, parsed.id)
        })
    }

    pub fn conflicts(&self, winner: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .leaves()
            .into_iter()
            .filter(|rev| rev != winner && !self.is_deleted(rev))
            .collect();
        out.sort();
        out
    }

    pub fn deleted_conflicts(&self, winner: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .leaves()
            .into_iter()
            .filter(|rev| rev != winner && self.is_deleted(rev))
            .collect();
        out.sort();
        out
    }

    /// Root-to-leaf path containing `rev`.
    pub fn path(&self, rev: &str) -> Vec<String> {
        let mut path = Vec::new();
        let mut cur = Some(rev.to_string());
        while let Some(rev) = cur {
            path.push(rev.clone());
            cur = self.edges.get(&rev).cloned().flatten();
        }
        path.reverse();
        path
    }

    #[allow(dead_code)]
    pub fn is_ancestor_of(&self, ancestor: &str, of: &str) -> bool {
        self.path(of).iter().any(|r| r == ancestor)
    }

    /// Drop history older than `limit` revisions behind the deepest leaf.
    pub fn prune(&mut self, limit: u64) {
        let limit = limit.max(1);
        let Some(max) = self
            .edges
            .keys()
            .filter_map(|rev| Rev::parse(rev).map(|r| r.pos))
            .max()
        else {
            return;
        };
        let floor = max.saturating_sub(limit - 1);

        let dead: Vec<String> = self
            .edges
            .keys()
            .filter(|rev| Rev::parse(rev).map(|r| r.pos < floor).unwrap_or(false))
            .cloned()
            .collect();
        for rev in &dead {
            self.edges.remove(rev);
        }
        let orphans: Vec<String> = self
            .edges
            .iter()
            .filter(|(_, parent)| match parent {
                Some(p) => !self.edges.contains_key(p),
                None => false,
            })
            .map(|(rev, _)| rev.clone())
            .collect();
        for rev in orphans {
            self.edges.insert(rev, None);
        }
        let live: BTreeSet<&String> = self.edges.keys().collect();
        let gone: Vec<String> = self
            .deleted
            .iter()
            .filter(|r| !live.contains(r))
            .cloned()
            .collect();
        for rev in gone {
            self.deleted.remove(&rev);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn revs(spec: &str) -> Vec<Rev> {
        spec.split(',').map(|s| Rev::parse(s).unwrap()).collect()
    }

    fn id(n: u8) -> String {
        format!("{n:02x}").repeat(16)
    }

    #[test]
    fn single_path_winner_is_the_tip() {
        let mut tree = RevTree::default();
        tree.merge_path(&revs(&format!("1-{},2-{}", id(1), id(2))), false);
        tree.merge_path(
            &revs(&format!("1-{},2-{},3-{}", id(1), id(2), id(3))),
            false,
        );
        assert_eq!(tree.leaves(), vec![format!("3-{}", id(3))]);
        assert_eq!(tree.winner().unwrap(), format!("3-{}", id(3)));
        assert_eq!(
            tree.path(&format!("3-{}", id(3))).len(),
            3,
            "shared prefix is not duplicated"
        );
    }

    #[test]
    fn conflicting_branches_both_remain() {
        let mut tree = RevTree::default();
        tree.merge_path(&revs(&format!("1-{},2-{}", id(1), id(2))), false);
        tree.merge_path(&revs(&format!("1-{},2-{}", id(1), id(9))), false);
        assert_eq!(tree.leaves().len(), 2);
        let winner = tree.winner().unwrap();
        assert_eq!(winner, format!("2-{}", id(9)), "higher rev id wins the tie");
        assert_eq!(tree.conflicts(&winner).len(), 1);
    }

    #[test]
    fn deleted_loses_to_undeleted_regardless_of_position() {
        let mut tree = RevTree::default();
        // Two real leaves: a short live branch and a longer tombstoned one.
        tree.merge_path(&revs(&format!("1-{},2-{}", id(1), id(2))), false);
        tree.merge_path(&revs(&format!("1-{},2-{},3-{}", id(1), id(3), id(4))), true);
        assert_eq!(tree.leaves().len(), 2);
        assert_eq!(
            tree.winner().unwrap(),
            format!("2-{}", id(2)),
            "live leaf beats a higher tombstone"
        );
        assert_eq!(tree.deleted_conflicts(&format!("2-{}", id(2))).len(), 1);
    }

    #[test]
    fn a_single_deleted_branch_is_deleted() {
        let mut tree = RevTree::default();
        tree.merge_path(&revs(&format!("1-{},2-{},3-{}", id(1), id(2), id(3))), true);
        assert_eq!(
            tree.winner().unwrap(),
            format!("3-{}", id(3)),
            "the only leaf wins even when deleted"
        );
    }

    #[test]
    fn ancestors_and_leaf_detection() {
        let mut tree = RevTree::default();
        tree.merge_path(&revs(&format!("1-{},2-{}", id(1), id(2))), false);
        assert!(tree.is_ancestor_of(&format!("1-{}", id(1)), &format!("2-{}", id(2))));
        assert!(!tree.is_leaf(&format!("1-{}", id(1))));
        assert!(tree.is_leaf(&format!("2-{}", id(2))));
    }

    #[test]
    fn pruning_keeps_recent_revisions_and_leaves() {
        let mut tree = RevTree::default();
        let path: Vec<String> = (1..=10).map(|n| format!("{n}-{}", id(n))).collect();
        tree.merge_path(
            &path
                .iter()
                .map(|s| Rev::parse(s).unwrap())
                .collect::<Vec<_>>(),
            false,
        );
        tree.prune(3);
        assert_eq!(tree.edges.len(), 3);
        assert_eq!(tree.winner().unwrap(), path[9]);
        assert!(tree.edges[&path[7]].is_none(), "pruned rev becomes a root");
    }
}

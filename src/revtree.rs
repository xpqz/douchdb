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
        // A path may include ancestors that were already pruned from this
        // tree. Merging them back would turn a stemmed root into an extra
        // leaf, so start at the first revision the tree already knows and
        // ignore the older prefix.
        let start = path
            .iter()
            .position(|rev| self.edges.contains_key(&rev.to_string()))
            .unwrap_or(0);
        for i in start..path.len() {
            let rev = &path[i];
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

    /// Follow a revision to the tip of its branch. CouchDB's `latest=true`
    /// returns that tip even when the caller names an interior revision.
    pub fn latest_leaf(&self, rev: &str) -> Option<String> {
        if !self.edges.contains_key(rev) {
            return None;
        }
        let mut current = rev.to_string();
        loop {
            match self
                .edges
                .iter()
                .find(|(_, parent)| parent.as_deref() == Some(current.as_str()))
            {
                Some((child, _)) => current = child.clone(),
                None => return Some(current),
            }
        }
    }

    pub fn is_deleted(&self, rev: &str) -> bool {
        self.deleted.contains(rev)
    }

    /// CouchDB's rule: non-deleted beats deleted, then higher position, then
    /// greater revision id.
    pub fn winner(&self) -> Option<String> {
        // A leaf that does not parse (a corrupt on-disk tree) cannot win; it
        // is skipped rather than allowed to crash the caller.
        self.leaves()
            .into_iter()
            .filter_map(|rev| {
                let parsed = Rev::parse(&rev)?;
                Some(((!self.is_deleted(&rev), parsed.pos, parsed.id), rev))
            })
            .max_by(|a, b| a.0.cmp(&b.0))
            .map(|(_, rev)| rev)
    }

    pub fn conflicts(&self, winner: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .leaves()
            .into_iter()
            .filter(|rev| rev != winner && !self.is_deleted(rev))
            .collect();
        self.sort_leaves_descending(&mut out);
        out
    }

    pub fn deleted_conflicts(&self, winner: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .leaves()
            .into_iter()
            .filter(|rev| rev != winner && self.is_deleted(rev))
            .collect();
        self.sort_leaves_descending(&mut out);
        out
    }

    fn sort_leaves_descending(&self, leaves: &mut [String]) {
        leaves.sort_by(|a, b| {
            let a = Rev::parse(a)
                .map(|rev| (rev.pos, rev.id))
                .unwrap_or_default();
            let b = Rev::parse(b)
                .map(|rev| (rev.pos, rev.id))
                .unwrap_or_default();
            b.cmp(&a)
        });
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

    /// Keep the newest `limit` revisions on every root-to-leaf path. Leaves are
    /// never removed, even when another branch is much deeper. Shared ancestors
    /// are retained once; a revision whose parent was pruned becomes a root.
    pub fn prune(&mut self, limit: u64) {
        let limit = limit.max(1) as usize;
        let leaves = self.leaves();
        if leaves.is_empty() {
            return;
        }
        let mut keep: BTreeSet<String> = BTreeSet::new();
        for leaf in &leaves {
            let path = self.path(leaf);
            let start = path.len().saturating_sub(limit);
            for rev in &path[start..] {
                keep.insert(rev.clone());
            }
        }

        let dead: Vec<String> = self
            .edges
            .keys()
            .filter(|rev| !keep.contains(*rev))
            .cloned()
            .collect();
        for rev in &dead {
            self.edges.remove(rev);
        }
        let known: BTreeSet<String> = self.edges.keys().cloned().collect();
        for parent in self.edges.values_mut() {
            let missing = parent
                .as_ref()
                .map(|parent| !known.contains(parent))
                .unwrap_or(false);
            if missing {
                *parent = None;
            }
        }
        self.deleted.retain(|rev| keep.contains(rev));
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
    fn winner_ignores_an_unparsable_leaf() {
        let mut tree = RevTree::default();
        tree.merge_path(&revs(&format!("1-{}", id(1))), false);
        tree.edges.insert("garbage".to_string(), None);
        assert_eq!(tree.winner().unwrap(), format!("1-{}", id(1)));

        let mut only_bad = RevTree::default();
        only_bad.edges.insert("garbage".to_string(), None);
        assert_eq!(only_bad.winner(), None);
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
        tree.merge_path(&revs(&format!("1-{},2-{}", id(1), id(5))), true);
        assert_eq!(tree.leaves().len(), 3);
        let winner = tree.winner().unwrap();
        assert_eq!(
            winner,
            format!("2-{}", id(9)),
            "a live leaf beats a deleted leaf and the higher id wins"
        );
        assert_eq!(tree.conflicts(&winner).len(), 1);
        assert_eq!(tree.deleted_conflicts(&winner).len(), 1);
    }

    #[test]
    fn conflicts_and_deleted_conflicts_exclude_the_winner_and_sort() {
        let mut tree = RevTree::default();
        tree.merge_path(&revs(&format!("1-{},2-{}", id(1), id(8))), false);
        tree.merge_path(&revs(&format!("1-{},2-{}", id(1), id(3))), false);
        tree.merge_path(&revs(&format!("1-{},2-{}", id(1), id(7))), true);
        tree.merge_path(&revs(&format!("1-{},2-{}", id(1), id(2))), true);
        let winner = tree.winner().unwrap();
        assert_eq!(winner, format!("2-{}", id(8)));
        assert_eq!(
            tree.conflicts(&winner),
            vec![format!("2-{}", id(3))],
            "live conflicts only, sorted"
        );
        assert_eq!(
            tree.deleted_conflicts(&winner),
            vec![format!("2-{}", id(7)), format!("2-{}", id(2))]
        );
    }

    #[test]
    fn ancestor_checks_follow_the_stored_path() {
        let root = format!("1-{}", id(1));
        let middle = format!("2-{}", id(2));
        let leaf = format!("3-{}", id(3));
        let mut tree = RevTree::default();
        tree.merge_path(&revs(&format!("{root},{middle},{leaf}")), false);
        assert!(tree.path(&leaf).contains(&root));
        assert!(tree.path(&leaf).contains(&middle));
        assert!(!tree.path(&middle).contains(&leaf));
        assert!(!tree.path(&leaf).contains(&format!("1-{}", id(9))));
    }

    #[test]
    fn higher_position_wins_among_live_leaves() {
        let mut tree = RevTree::default();
        // The longer branch has the lower revision id, so only position can
        // make it win.
        tree.merge_path(&revs(&format!("1-{},2-{}", id(1), id(9))), false);
        tree.merge_path(
            &revs(&format!("1-{},2-{},3-{}", id(1), id(2), id(3))),
            false,
        );
        assert_eq!(tree.winner().unwrap(), format!("3-{}", id(3)));
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
        assert!(tree
            .path(&format!("2-{}", id(2)))
            .contains(&format!("1-{}", id(1))));
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

    #[test]
    fn pruning_preserves_short_live_conflict_leaf() {
        let mut tree = RevTree::default();
        let live = format!("2-{}", id(2));
        let live_parent = format!("1-{}", id(1));
        let deep = [
            format!("1-{}", id(1)),
            format!("2-{}", id(3)),
            format!("3-{}", id(4)),
            format!("4-{}", id(5)),
        ];
        tree.merge_path(
            &[
                Rev::parse(&live_parent).unwrap(),
                Rev::parse(&live).unwrap(),
            ],
            false,
        );
        tree.merge_path(
            &deep
                .iter()
                .map(|rev| Rev::parse(rev).unwrap())
                .collect::<Vec<_>>(),
            true,
        );

        tree.prune(2);

        assert!(tree.edges.contains_key(&live), "short live leaf was pruned");
        assert!(
            tree.edges.contains_key(&deep[3]),
            "deep tombstone leaf was pruned"
        );
        assert_eq!(tree.leaves().len(), 2);
        assert_eq!(tree.winner().unwrap(), live);
        assert!(tree.is_deleted(&deep[3]));
        assert!(!tree.is_deleted(&live));
        // The deep branch's parent was pruned, so it is now a root.
        assert!(tree.edges[&deep[2]].is_none());
        assert_eq!(tree.edges[&live], Some(live_parent));
    }

    #[test]
    fn pruning_never_removes_a_leaf_at_limit_one() {
        let mut tree = RevTree::default();
        let live = format!("2-{}", id(2));
        let deep = [
            format!("1-{}", id(1)),
            format!("2-{}", id(3)),
            format!("3-{}", id(4)),
            format!("4-{}", id(5)),
        ];
        tree.merge_path(
            &[
                Rev::parse(&format!("1-{}", id(1))).unwrap(),
                Rev::parse(&live).unwrap(),
            ],
            false,
        );
        tree.merge_path(
            &deep
                .iter()
                .map(|rev| Rev::parse(rev).unwrap())
                .collect::<Vec<_>>(),
            false,
        );

        tree.prune(1);

        assert_eq!(tree.leaves().len(), 2);
        assert!(tree.edges.contains_key(&live));
        assert!(tree.edges.contains_key(&deep[3]));
        assert!(tree.edges.values().all(Option::is_none));
    }

    #[test]
    fn merge_ignores_ancestors_already_pruned_from_the_tree() {
        let base = format!("1-{}", id(1));
        let parent = format!("2-{}", id(2));
        let leaf = format!("3-{}", id(3));
        let sibling = format!("3-{}", id(4));
        let mut tree = RevTree::default();
        tree.merge_path(&revs(&format!("{parent},{leaf}")), false);
        tree.prune(2);
        assert!(!tree.edges.contains_key(&base));

        // A later replicated path still carries the pruned base. It must not
        // become an orphan root/leaf.
        tree.merge_path(&revs(&format!("{base},{parent},{sibling}")), false);
        assert!(!tree.edges.contains_key(&base));
        assert_eq!(tree.leaves(), vec![leaf, sibling]);
    }

    #[test]
    fn merge_extends_an_existing_leaf_without_losing_its_parent() {
        let root = format!("1-{}", id(1));
        let middle = format!("2-{}", id(2));
        let leaf = format!("3-{}", id(3));
        let mut tree = RevTree::default();
        tree.merge_path(&revs(&format!("{root},{middle}")), false);
        tree.merge_path(&revs(&format!("{root},{middle},{leaf}")), false);
        assert_eq!(tree.leaves(), vec![leaf]);
        assert_eq!(tree.edges[&middle], Some(root));
    }

    use proptest::prelude::*;

    fn branch_strategy() -> impl Strategy<Value = (Vec<Rev>, bool)> {
        (
            1usize..8,
            prop::collection::vec(any::<u8>(), 8),
            any::<bool>(),
        )
            .prop_map(|(length, ids, deleted)| {
                let mut path = vec![Rev {
                    pos: 1,
                    id: b"root".to_vec(),
                    hex: false,
                }];
                for (index, id) in ids.into_iter().take(length.saturating_sub(1)).enumerate() {
                    path.push(Rev {
                        pos: (index + 2) as u64,
                        id: format!("{id:02x}").into_bytes(),
                        hex: false,
                    });
                }
                (path, deleted)
            })
    }

    proptest! {
        #[test]
        fn pruning_preserves_leaves_and_winner(
            branches in prop::collection::vec(branch_strategy(), 1..20),
            limit in 1u64..8,
        ) {
            let mut tree = RevTree::default();
            for (path, deleted) in &branches {
                tree.merge_path(path, *deleted);
            }
            let leaves = tree.leaves();
            let winner = tree.winner();
            for leaf in &leaves {
                prop_assert!(tree.edges.contains_key(leaf));
            }

            tree.prune(limit);

            prop_assert_eq!(tree.leaves(), leaves);
            prop_assert_eq!(tree.winner(), winner);
            for leaf in tree.leaves() {
                let path = tree.path(&leaf);
                prop_assert_eq!(path.last(), Some(&leaf));
            }

            let once = tree.clone();
            tree.prune(limit);
            prop_assert_eq!(&tree, &once);
        }

        #[test]
        fn prune_one_leaves_only_tips(
            branches in prop::collection::vec(branch_strategy(), 1..20),
        ) {
            let mut tree = RevTree::default();
            for (path, deleted) in &branches {
                tree.merge_path(path, *deleted);
            }
            let leaves = tree.leaves();
            tree.prune(1);
            let mut edges: Vec<String> = tree.edges.keys().cloned().collect();
            edges.sort();
            let mut expected = leaves;
            expected.sort();
            prop_assert_eq!(edges, expected);
            prop_assert!(tree.edges.values().all(Option::is_none));
        }
    }
}

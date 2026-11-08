//! Revision trees as an edge map.
//!
//! CouchDB stores a tree per document; we store the same thing as
//! `child -> parent` edges plus a set of tombstones. Leaves are revisions with
//! no children, and everything the API needs (conflicts, `_revisions`,
//! `_revs_info`, winner) is derived from that.
//!
//! The leaf set is kept alongside the edges and updated as revisions are
//! merged, pruned or purged, so asking for leaves, the winner or whether a
//! revision is a leaf does not rescan every edge. Only the edges and the
//! tombstones are stored; the leaf set is rebuilt when a tree is loaded, so
//! the on-disk shape is the one older builds wrote and can read.

use crate::rev::Rev;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(from = "StoredTree")]
pub struct RevTree {
    /// Every known revision maps to its parent, or `None` for a root.
    edges: BTreeMap<String, Option<String>>,
    /// Leaf revisions written as tombstones.
    deleted: BTreeSet<String>,
    /// Revisions with no children: the keys of `edges` that are nobody's
    /// parent. Always derivable from `edges`.
    leaves: BTreeSet<String>,
    /// Parent references to revisions that are not in `edges`. A well-formed
    /// tree has none; a damaged stored one may, and merging such a revision
    /// back in must not make it look like a leaf. May hold stale entries,
    /// which are re-checked before use.
    dangling: BTreeSet<String>,
}

/// The stored shape of a tree, unchanged from when the fields were public.
#[derive(Deserialize)]
struct StoredTree {
    edges: BTreeMap<String, Option<String>>,
    deleted: BTreeSet<String>,
}

impl From<StoredTree> for RevTree {
    fn from(stored: StoredTree) -> Self {
        let parents: HashSet<&str> = stored
            .edges
            .values()
            .flatten()
            .map(String::as_str)
            .collect();
        let leaves = stored
            .edges
            .keys()
            .filter(|rev| !parents.contains(rev.as_str()))
            .cloned()
            .collect();
        let dangling = parents
            .into_iter()
            .filter(|parent| !stored.edges.contains_key(*parent))
            .map(|parent| parent.to_string())
            .collect();
        RevTree {
            edges: stored.edges,
            deleted: stored.deleted,
            leaves,
            dangling,
        }
    }
}

impl Serialize for RevTree {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut out = serializer.serialize_struct("RevTree", 2)?;
        out.serialize_field("edges", &self.edges)?;
        out.serialize_field("deleted", &self.deleted)?;
        out.end()
    }
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
            let rev = path[i].to_string();
            if self.edges.contains_key(&rev) {
                continue;
            }
            let parent = if i == 0 {
                None
            } else {
                Some(path[i - 1].to_string())
            };
            if let Some(parent) = &parent {
                self.leaves.remove(parent);
            }
            if !self.has_child(&rev) {
                self.leaves.insert(rev.clone());
            }
            self.edges.insert(rev, parent);
        }
        if let Some(leaf) = path.last() {
            if deleted {
                self.deleted.insert(leaf.to_string());
            }
        }
    }

    /// Whether some revision already names `rev` as its parent. Only a
    /// damaged tree can have such a revision before `rev` is known, so the
    /// scan runs only then.
    fn has_child(&self, rev: &str) -> bool {
        self.dangling.contains(rev) && self.edges.values().flatten().any(|parent| parent == rev)
    }

    /// Whether the revision is known to the tree.
    pub fn contains(&self, rev: &str) -> bool {
        self.edges.contains_key(rev)
    }

    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    /// Every known revision, in string order.
    #[cfg(test)]
    pub fn revs(&self) -> impl Iterator<Item = &String> {
        self.edges.keys()
    }

    /// The parent of a revision; `None` for a root or an unknown revision.
    pub fn parent(&self, rev: &str) -> Option<&str> {
        self.edges.get(rev)?.as_deref()
    }

    pub fn leaves(&self) -> Vec<String> {
        self.leaves.iter().cloned().collect()
    }

    pub fn is_leaf(&self, rev: &str) -> bool {
        self.leaves.contains(rev)
    }

    /// Every leaf at or below a revision. CouchDB's `latest=true` returns all
    /// of them when the caller names an interior revision of a forked branch.
    /// Empty when the revision is unknown.
    pub fn leaves_from(&self, rev: &str) -> Vec<String> {
        if !self.edges.contains_key(rev) {
            return Vec::new();
        }
        // Walk each leaf up towards the root, remembering what every revision
        // on the way led to so shared ancestors are visited once. The length
        // bound only matters for a damaged tree whose parent links loop.
        let mut reaches: HashMap<&str, bool> = HashMap::from([(rev, true)]);
        let mut chain: Vec<&str> = Vec::new();
        let mut out = Vec::new();
        for leaf in &self.leaves {
            chain.clear();
            let mut current = Some(leaf.as_str());
            let found = loop {
                match current {
                    Some(node) => match reaches.get(node) {
                        Some(&known) => break known,
                        None if chain.len() > self.edges.len() => break false,
                        None => {
                            chain.push(node);
                            current = self.parent(node);
                        }
                    },
                    None => break false,
                }
            };
            for node in chain.drain(..) {
                reaches.insert(node, found);
            }
            if found {
                out.push(leaf.clone());
            }
        }
        out
    }

    pub fn is_deleted(&self, rev: &str) -> bool {
        self.deleted.contains(rev)
    }

    /// CouchDB's rule: non-deleted beats deleted, then higher position, then
    /// greater revision id.
    pub fn winner(&self) -> Option<String> {
        self.best(self.leaves.iter().map(String::as_str))
            .map(str::to_string)
    }

    /// The winner of a set of leaves under the same rule.
    pub fn winner_among(&self, leaves: Vec<String>) -> Option<String> {
        self.best(leaves.iter().map(String::as_str))
            .map(str::to_string)
    }

    fn best<'a>(&self, leaves: impl Iterator<Item = &'a str>) -> Option<&'a str> {
        // A leaf that does not parse (a corrupt on-disk tree) cannot win; it
        // is skipped rather than allowed to crash the caller.
        leaves
            .filter_map(|rev| {
                let parsed = Rev::parse(rev)?;
                Some(((!self.is_deleted(rev), parsed.pos, parsed.id), rev))
            })
            .max_by(|a, b| a.0.cmp(&b.0))
            .map(|(_, rev)| rev)
    }

    pub fn conflicts(&self, winner: &str) -> Vec<String> {
        self.other_leaves(winner, false)
    }

    pub fn deleted_conflicts(&self, winner: &str) -> Vec<String> {
        self.other_leaves(winner, true)
    }

    /// Leaves other than `winner` whose deleted state is `deleted`, in
    /// descending revision order.
    fn other_leaves(&self, winner: &str, deleted: bool) -> Vec<String> {
        let mut out: Vec<String> = self
            .leaves
            .iter()
            .filter(|rev| *rev != winner && self.is_deleted(rev) == deleted)
            .cloned()
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

    /// Remove the leaf `rev` and every ancestor no other branch shares, as
    /// CouchDB's `remove_leafs` does. Returns the removed revisions, leaf
    /// first; empty when `rev` is not a leaf.
    pub fn purge_leaf(&mut self, rev: &str) -> Vec<String> {
        let mut removed = Vec::new();
        if !self.is_leaf(rev) {
            return removed;
        }
        let mut current = rev.to_string();
        loop {
            let parent = self.edges.remove(&current).flatten();
            self.leaves.remove(&current);
            self.deleted.remove(&current);
            removed.push(current);
            match parent {
                // The parent becomes a leaf when this was its last child.
                Some(parent)
                    if self.edges.contains_key(&parent)
                        && !self.edges.values().flatten().any(|p| *p == parent) =>
                {
                    self.leaves.insert(parent.clone());
                    current = parent;
                }
                _ => return removed,
            }
        }
    }

    /// Keep the newest `limit` revisions on every root-to-leaf path. Leaves are
    /// never removed, even when another branch is much deeper. Shared ancestors
    /// are retained once; a revision whose parent was pruned becomes a root.
    pub fn prune(&mut self, limit: u64) {
        let limit = limit.max(1) as usize;
        if self.leaves.is_empty() {
            return;
        }
        let mut keep: BTreeSet<String> = BTreeSet::new();
        for leaf in &self.leaves {
            let path = self.path(leaf);
            let start = path.len().saturating_sub(limit);
            for rev in &path[start..] {
                keep.insert(rev.clone());
            }
        }

        self.edges.retain(|rev, _| keep.contains(rev));
        let known = &self.edges.keys().cloned().collect::<BTreeSet<String>>();
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
        // Every kept revision keeps the child on its way to a kept leaf, so
        // the leaves are unchanged, and no parent link dangles any more.
        self.dangling.clear();
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

    /// A damaged stored tree whose parent links loop: `x` and `y` are each
    /// other's parent and the leaf `z` hangs off the loop.
    fn cyclic_tree() -> RevTree {
        serde_json::from_str(r#"{"edges":{"x":"y","y":"x","z":"x"},"deleted":[]}"#).unwrap()
    }

    #[test]
    fn path_on_a_cyclic_tree_terminates() {
        let tree = cyclic_tree();
        assert_eq!(tree.leaves(), vec!["z".to_string()]);
        let _ = tree.path("z");
    }

    #[test]
    fn prune_on_a_cyclic_tree_terminates() {
        let mut tree = cyclic_tree();
        tree.prune(2);
    }

    #[test]
    fn winner_ignores_an_unparsable_leaf() {
        let mut tree = RevTree::default();
        tree.merge_path(&revs(&format!("1-{}", id(1))), false);
        let stored = serde_json::to_string(&tree).unwrap();
        let stored = stored.replace("\"edges\":{", "\"edges\":{\"garbage\":null,");
        let tree: RevTree = serde_json::from_str(&stored).unwrap();
        assert_eq!(tree.leaves().len(), 2);
        assert_eq!(tree.winner().unwrap(), format!("1-{}", id(1)));

        let only_bad: RevTree =
            serde_json::from_str(r#"{"edges":{"garbage":null},"deleted":[]}"#).unwrap();
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

    /// The tree as the previous implementation stored it, kept only as an
    /// oracle for the equivalence property below.
    mod reference {
        use super::*;

        #[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
        pub struct OldTree {
            pub edges: BTreeMap<String, Option<String>>,
            pub deleted: BTreeSet<String>,
        }

        impl OldTree {
            pub fn merge_path(&mut self, path: &[Rev], deleted: bool) {
                let start = path
                    .iter()
                    .position(|rev| self.edges.contains_key(&rev.to_string()))
                    .unwrap_or(0);
                for i in start..path.len() {
                    let parent = if i == 0 {
                        None
                    } else {
                        Some(path[i - 1].to_string())
                    };
                    self.edges.entry(path[i].to_string()).or_insert(parent);
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

            pub fn leaves_from(&self, rev: &str) -> Vec<String> {
                if !self.edges.contains_key(rev) {
                    return Vec::new();
                }
                let mut children: BTreeMap<&str, Vec<&String>> = BTreeMap::new();
                for (child, parent) in &self.edges {
                    if let Some(parent) = parent {
                        children.entry(parent.as_str()).or_default().push(child);
                    }
                }
                let mut leaves = Vec::new();
                let mut pending = vec![rev];
                while let Some(current) = pending.pop() {
                    match children.get(current) {
                        Some(kids) => pending.extend(kids.iter().map(|kid| kid.as_str())),
                        None => leaves.push(current.to_string()),
                    }
                }
                leaves.sort();
                leaves
            }

            pub fn winner_among(&self, leaves: Vec<String>) -> Option<String> {
                leaves
                    .into_iter()
                    .filter_map(|rev| {
                        let parsed = Rev::parse(&rev)?;
                        Some(((!self.deleted.contains(&rev), parsed.pos, parsed.id), rev))
                    })
                    .max_by(|a, b| a.0.cmp(&b.0))
                    .map(|(_, rev)| rev)
            }

            pub fn winner(&self) -> Option<String> {
                self.winner_among(self.leaves())
            }

            fn sorted(&self, mut out: Vec<String>) -> Vec<String> {
                out.sort_by(|a, b| {
                    let key = |s: &String| {
                        Rev::parse(s)
                            .map(|rev| (rev.pos, rev.id))
                            .unwrap_or_default()
                    };
                    key(b).cmp(&key(a))
                });
                out
            }

            pub fn conflicts(&self, winner: &str) -> Vec<String> {
                self.sorted(
                    self.leaves()
                        .into_iter()
                        .filter(|rev| rev != winner && !self.deleted.contains(rev))
                        .collect(),
                )
            }

            pub fn deleted_conflicts(&self, winner: &str) -> Vec<String> {
                self.sorted(
                    self.leaves()
                        .into_iter()
                        .filter(|rev| rev != winner && self.deleted.contains(rev))
                        .collect(),
                )
            }

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

            pub fn purge_leaf(&mut self, rev: &str) -> Vec<String> {
                let mut removed = Vec::new();
                if !self.is_leaf(rev) {
                    return removed;
                }
                let mut current = rev.to_string();
                loop {
                    let parent = self.edges.remove(&current).flatten();
                    self.deleted.remove(&current);
                    removed.push(current);
                    match parent {
                        Some(parent) if self.is_leaf(&parent) => current = parent,
                        _ => return removed,
                    }
                }
            }

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
                self.edges.retain(|rev, _| keep.contains(rev));
                let known: BTreeSet<String> = self.edges.keys().cloned().collect();
                for parent in self.edges.values_mut() {
                    if parent.as_ref().is_some_and(|p| !known.contains(p)) {
                        *parent = None;
                    }
                }
                self.deleted.retain(|rev| keep.contains(rev));
            }
        }
    }

    /// Metas captured from the previous implementation. They must load and
    /// re-serialize byte for byte, so documents already on disk keep working
    /// and anything written stays readable by older builds.
    const STORED_TREES: &[&str] = &[
        // Empty.
        r#"{"edges":{},"deleted":[]}"#,
        // Linear.
        r#"{"edges":{"1-aa":null,"2-bb":"1-aa","3-cc":"2-bb"},"deleted":[]}"#,
        // Conflicts, two of them tombstones.
        r#"{"edges":{"1-aa":null,"2-bb":"1-aa","2-cc":"1-aa","2-dd":"1-aa","3-ee":"2-dd"},"deleted":["2-cc","3-ee"]}"#,
        // Stemmed: two roots, the older ancestors are gone.
        r#"{"edges":{"3-cc":null,"4-dd":"3-cc","4-ee":"3-cc","6-ff":null},"deleted":["4-ee"]}"#,
        // Unparsable revisions, including a dangling parent.
        r#"{"edges":{"1-aa":null,"2-bb":"1-aa","9-zz":"8-missing","garbage":null},"deleted":["garbage"]}"#,
    ];

    #[test]
    fn stored_trees_round_trip_byte_for_byte() {
        for stored in STORED_TREES {
            let tree: RevTree = serde_json::from_str(stored).unwrap();
            assert_eq!(&serde_json::to_string(&tree).unwrap(), stored);
            let old: reference::OldTree = serde_json::from_str(stored).unwrap();
            assert_eq!(tree.leaves(), old.leaves(), "{stored}");
            assert_eq!(tree.winner(), old.winner(), "{stored}");
            for rev in old.edges.keys() {
                assert_eq!(tree.is_leaf(rev), old.is_leaf(rev), "{stored} {rev}");
                assert_eq!(tree.leaves_from(rev), old.leaves_from(rev), "{stored}");
                assert_eq!(tree.path(rev), old.path(rev), "{stored}");
            }
        }
    }

    #[test]
    fn edited_stored_trees_stay_consistent() {
        // Merging into and pruning a loaded tree keeps every derived answer
        // in step with the plain edge-map definition.
        for stored in STORED_TREES {
            let mut tree: RevTree = serde_json::from_str(stored).unwrap();
            let mut old: reference::OldTree = serde_json::from_str(stored).unwrap();
            let path = revs(&format!("1-aa,2-bb,3-{}", id(7)));
            tree.merge_path(&path, false);
            old.merge_path(&path, false);
            let path = revs("8-missing,9-zz,10-yy");
            tree.merge_path(&path, true);
            old.merge_path(&path, true);
            assert_eq!(tree.leaves(), old.leaves(), "{stored}");
            assert_eq!(tree.winner(), old.winner(), "{stored}");
            tree.prune(2);
            old.prune(2);
            assert_eq!(tree.leaves(), old.leaves(), "{stored}");
            assert_eq!(
                serde_json::to_value(&tree).unwrap(),
                serde_json::to_value(&old).unwrap(),
                "{stored}"
            );
        }
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

    #[derive(Clone, Debug)]
    enum Op {
        Merge(Vec<Rev>, bool),
        Prune(u64),
        Purge(usize),
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        prop_oneof![
            6 => branch_strategy().prop_map(|(path, deleted)| Op::Merge(path, deleted)),
            1 => (1u64..6).prop_map(Op::Prune),
            2 => any::<usize>().prop_map(Op::Purge),
        ]
    }

    fn assert_same(tree: &RevTree, old: &reference::OldTree) -> Result<(), TestCaseError> {
        prop_assert_eq!(
            serde_json::to_value(tree).unwrap(),
            serde_json::to_value(old).unwrap()
        );
        prop_assert_eq!(tree.leaves(), old.leaves());
        let winner = old.winner();
        prop_assert_eq!(tree.winner(), winner.clone());
        if let Some(winner) = winner {
            prop_assert_eq!(tree.conflicts(&winner), old.conflicts(&winner));
            prop_assert_eq!(
                tree.deleted_conflicts(&winner),
                old.deleted_conflicts(&winner)
            );
        }
        for rev in old.edges.keys() {
            prop_assert_eq!(tree.is_leaf(rev), old.is_leaf(rev));
            prop_assert_eq!(tree.leaves_from(rev), old.leaves_from(rev));
            prop_assert_eq!(tree.path(rev), old.path(rev));
            prop_assert_eq!(tree.is_deleted(rev), old.deleted.contains(rev));
        }
        prop_assert!(!tree.is_leaf("0-unknown"));
        Ok(())
    }

    proptest! {
        #[test]
        fn matches_the_edge_map_reference(
            ops in prop::collection::vec(op_strategy(), 1..30),
        ) {
            let mut tree = RevTree::default();
            let mut old = reference::OldTree::default();
            for op in ops {
                match op {
                    Op::Merge(path, deleted) => {
                        tree.merge_path(&path, deleted);
                        old.merge_path(&path, deleted);
                    }
                    Op::Prune(limit) => {
                        tree.prune(limit);
                        old.prune(limit);
                    }
                    Op::Purge(pick) => {
                        let leaves = old.leaves();
                        if !leaves.is_empty() {
                            let leaf = &leaves[pick % leaves.len()];
                            prop_assert_eq!(tree.purge_leaf(leaf), old.purge_leaf(leaf));
                        }
                    }
                }
                assert_same(&tree, &old)?;
                // A stored copy loads back to the same tree.
                let reloaded: RevTree =
                    serde_json::from_str(&serde_json::to_string(&tree).unwrap()).unwrap();
                prop_assert_eq!(&reloaded, &tree);
                assert_same(&reloaded, &old)?;
            }
        }

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

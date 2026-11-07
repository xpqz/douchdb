#![no_main]

use libfuzzer_sys::fuzz_target;

#[path = "../../src/etf.rs"]
mod etf;
// etf.rs uses crate::json::is_integer.
#[path = "../../src/json.rs"]
mod json;
#[path = "../../src/rev.rs"]
mod rev;
#[path = "../../src/revtree.rs"]
mod revtree;

use rev::Rev;
use revtree::RevTree;

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    let mut tree = RevTree::default();
    for line in text.lines().take(64) {
        let path: Vec<Rev> = line
            .split(',')
            .take(64)
            .filter_map(Rev::parse)
            .collect();
        if !path.is_empty() {
            tree.merge_path(&path, false);
        }
    }
    let _ = tree.leaves();
    let _ = tree.winner();
    let _ = tree.conflicts("0-deadbeef");
    tree.prune(3);
    let _ = tree.leaves();
    let _ = tree.winner();
});

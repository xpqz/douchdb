//! Bounds on the index handles kept open between queries. Not enforced yet.

use std::sync::atomic::{AtomicUsize, Ordering};

static MAX_OPEN: AtomicUsize = AtomicUsize::new(256);

pub fn set_max_open(max: usize) {
    MAX_OPEN.store(max, Ordering::SeqCst);
}

//! Bounded registries of the index handles kept open between queries.
//!
//! A view index holds a LevelDB handle and its sorted rows; a search index
//! holds a Tantivy index and every stored document. Without a bound, every
//! index ever queried stays resident until its database is deleted. A
//! registry keeps at most `--max-open-indexes` entries and, on inserting past
//! that, drops the least recently used ones that nothing is using.
//!
//! Entries are `Arc`s: a query clones the one it uses, and an entry is in use
//! while any clone besides the registry's exists. Only entries that are not in
//! use are evicted, so an eviction never pulls an index out from under a query
//! or under a gate held by `invalidate` or `cleanup`. When every entry is in
//! use the registry runs over the cap until they are released, then comes
//! back down on the next insert.

use crate::error::{ApiError, DbResult};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// Default for `--max-open-indexes`.
pub const DEFAULT_MAX_OPEN: usize = 256;

static MAX_OPEN: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_OPEN);

/// Set the cap shared by the view, search and gate registries.
pub fn set_max_open(max: usize) {
    MAX_OPEN.store(max.max(1), Ordering::SeqCst);
}

/// Lowers the shared cap for the length of a test, then restores the default.
/// Tests that lower it run one at a time, since they would otherwise reset
/// each other's cap.
#[cfg(test)]
pub struct CapGuard {
    _serial: tokio::sync::OwnedMutexGuard<()>,
}

#[cfg(test)]
impl CapGuard {
    pub async fn new(max: usize) -> Self {
        static SERIAL: std::sync::OnceLock<Arc<tokio::sync::Mutex<()>>> =
            std::sync::OnceLock::new();
        let serial = SERIAL.get_or_init(Default::default).clone();
        let guard = CapGuard {
            _serial: serial.lock_owned().await,
        };
        set_max_open(max);
        guard
    }
}

#[cfg(test)]
impl Drop for CapGuard {
    fn drop(&mut self) {
        set_max_open(DEFAULT_MAX_OPEN);
    }
}

/// What a registry stores.
pub trait Slot: Default {
    /// Whether anyone but the registry holds this entry. The registry's own
    /// clone is the one that is not counted.
    fn in_use(this: &Arc<Self>) -> bool {
        Arc::strong_count(this) > 1
    }

    /// Whether an evicted entry must be dropped before the registry's lock is
    /// released. A LevelDB handle holds the database's lock file until it is
    /// dropped, and a reopen of the same path must not see it still held.
    const DROP_UNDER_LOCK: bool = false;
}

struct Inner<V> {
    tick: u64,
    entries: HashMap<PathBuf, (u64, Arc<V>)>,
}

pub struct Registry<V: Slot> {
    inner: Mutex<Option<Inner<V>>>,
    /// Overrides the shared cap; zero means use it.
    cap: usize,
}

impl<V: Slot> Registry<V> {
    pub const fn new() -> Self {
        Registry {
            inner: Mutex::new(None),
            cap: 0,
        }
    }

    #[cfg(test)]
    const fn with_cap(cap: usize) -> Self {
        Registry {
            inner: Mutex::new(None),
            cap,
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<Inner<V>>> {
        // A panic in an unrelated request must not poison every later query.
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The entry for `key`, created if there is none, marked as most recently
    /// used. The lock covers map operations only: opening what the entry
    /// holds is the caller's business and happens outside it.
    pub fn get(&self, key: &Path) -> Arc<V> {
        let mut guard = self.lock();
        let inner = guard.get_or_insert_with(|| Inner {
            tick: 0,
            entries: HashMap::new(),
        });
        inner.tick += 1;
        let tick = inner.tick;
        if let Some((used, entry)) = inner.entries.get_mut(key) {
            *used = tick;
            return entry.clone();
        }
        let entry = Arc::new(V::default());
        inner
            .entries
            .insert(key.to_path_buf(), (tick, entry.clone()));
        let mut evicted = Vec::new();
        let cap = if self.cap > 0 {
            self.cap
        } else {
            MAX_OPEN.load(Ordering::SeqCst)
        };
        while inner.entries.len() > cap {
            let oldest = inner
                .entries
                .iter()
                .filter(|(_, (_, entry))| !V::in_use(entry))
                .min_by_key(|(_, (used, _))| *used)
                .map(|(path, _)| path.clone());
            let Some(oldest) = oldest else { break };
            if let Some((_, entry)) = inner.entries.remove(&oldest) {
                evicted.push(entry);
            }
        }
        if V::DROP_UNDER_LOCK {
            drop(evicted);
            drop(guard);
        } else {
            drop(guard);
            drop(evicted);
        }
        entry
    }

    /// Forget the entry for `key`.
    #[cfg(test)]
    pub fn remove(&self, key: &Path) {
        self.retain(|path| path != key);
    }

    /// Forget every entry whose key `keep` rejects.
    pub fn retain(&self, keep: impl Fn(&Path) -> bool) {
        let mut guard = self.lock();
        if let Some(inner) = guard.as_mut() {
            inner.entries.retain(|path, _| keep(path));
        }
    }

    #[cfg(test)]
    pub fn count_under(&self, prefix: &Path) -> usize {
        self.lock().as_ref().map_or(0, |inner| {
            inner
                .entries
                .keys()
                .filter(|path| path.starts_with(prefix))
                .count()
        })
    }
}

impl<V: Slot> Default for Registry<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl Slot for tokio::sync::RwLock<()> {}

/// A database's gate for its index directories, with the signal that the
/// database is being deleted.
///
/// Queries and builds hold the read side (a `Permit`) while they use an index;
/// anything that removes index files takes the write side. `forget_db` fires
/// the signal before it takes the write side, so a build or a queued query
/// ends at its next check with `not_found` and drops its permit, instead of
/// the delete waiting for it. CouchDB does the same: `couch_server` closes a
/// deleted database and its `couch_index` processes are shut down with it, so
/// a query in flight fails.
///
/// A gate that has been signalled never serves again. `forget_db` removes it
/// from its registry, so a database created afterwards under the same name is
/// given a fresh, unsignalled gate; a reader holding the old one still sees
/// the signal and cannot touch the new database's indexes.
#[derive(Default)]
pub struct Gate {
    lock: Arc<tokio::sync::RwLock<()>>,
    deleted: tokio_util::sync::CancellationToken,
}

impl Slot for Gate {}

fn deleted_error() -> ApiError {
    ApiError::not_found("Database does not exist.")
}

impl Gate {
    /// Take the read side, or fail with `not_found` if the database is being
    /// deleted, including while waiting behind the deleting writer.
    pub async fn read(self: &Arc<Self>) -> DbResult<Permit> {
        let guard = self.until(self.lock.clone().read_owned()).await?;
        let permit = Permit {
            _guard: guard,
            gate: self.clone(),
        };
        permit.check()?;
        Ok(permit)
    }

    /// Take the write side, or fail with `not_found` if the database is being
    /// deleted.
    pub async fn write(&self) -> DbResult<tokio::sync::OwnedRwLockWriteGuard<()>> {
        let guard = self.until(self.lock.clone().write_owned()).await?;
        self.check()?;
        Ok(guard)
    }

    /// Signal the deletion, then take the write side. The readers notice the
    /// signal and release their permits, so this waits only for them to reach
    /// their next check.
    pub async fn forget(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        self.deleted.cancel();
        self.lock.clone().write_owned().await
    }

    /// `not_found` once the database is being deleted.
    pub fn check(&self) -> DbResult<()> {
        if self.deleted.is_cancelled() {
            Err(deleted_error())
        } else {
            Ok(())
        }
    }

    /// Await `work`, giving it up with `not_found` as soon as the database is
    /// being deleted.
    pub async fn until<T>(&self, work: impl std::future::Future<Output = T>) -> DbResult<T> {
        tokio::select! {
            biased;
            () = self.deleted.cancelled() => Err(deleted_error()),
            value = work => Ok(value),
        }
    }

    #[cfg(test)]
    pub fn is_held(&self) -> bool {
        self.lock.clone().try_write_owned().is_err()
    }
}

/// The read side of a `Gate`, held while an index is in use.
pub struct Permit {
    _guard: tokio::sync::OwnedRwLockReadGuard<()>,
    gate: Arc<Gate>,
}

impl Permit {
    /// `not_found` once the database is being deleted. Builders call this
    /// between batches.
    pub fn check(&self) -> DbResult<()> {
        self.gate.check()
    }

    /// Await `work`, giving it up with `not_found` as soon as the database is
    /// being deleted.
    pub async fn until<T>(&self, work: impl std::future::Future<Output = T>) -> DbResult<T> {
        self.gate.until(work).await
    }
}

impl<T> Slot for tokio::sync::Mutex<Option<T>> {}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: usize) -> PathBuf {
        PathBuf::from(format!("/index/{n}"))
    }

    #[test]
    fn the_least_recently_used_idle_entry_goes_first() {
        let registry: Registry<tokio::sync::RwLock<()>> = Registry::with_cap(3);
        for n in 0..3 {
            registry.get(&key(n));
        }
        // Touch 0, so 1 is the oldest.
        registry.get(&key(0));
        registry.get(&key(3));
        let root = Path::new("/index");
        assert_eq!(registry.count_under(root), 3);
        assert_eq!(registry.count_under(&key(1)), 0, "1 was the oldest");
        assert_eq!(registry.count_under(&key(0)), 1);
    }

    #[test]
    fn an_entry_in_use_is_never_evicted() {
        let registry: Registry<tokio::sync::RwLock<()>> = Registry::with_cap(2);
        let held: Vec<_> = (0..4).map(|n| registry.get(&key(n))).collect();
        let root = Path::new("/index");
        assert_eq!(registry.count_under(root), 4, "all four are in use");
        // Released entries are evicted by the next insert, oldest first.
        drop(held);
        registry.get(&key(4));
        assert_eq!(registry.count_under(root), 2);
        assert_eq!(registry.count_under(&key(4)), 1);
    }
}

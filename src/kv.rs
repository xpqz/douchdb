//! The redb storage foundation: opening a file, the tables, and the checksum
//! every stored value carries.
//!
//! redb checksums its pages but verifies them only in a full integrity check,
//! so two things stand guard here (see "Corruption detection" in
//! `docs/superpowers/specs/2026-11-16-redb-storage-design.md`):
//!
//! - every value in every table is [`seal`]ed: a CRC32C of the value is
//!   appended on write, and [`unseal`] checks it on every read, so a damaged
//!   value is a [`Corrupt`] error, never a wrong answer or a miss;
//! - a full integrity check runs on the first [`open`] of each file in a
//!   process and in [`close_verified`] when an idle database closes. A file
//!   that passed its close-time check opens without the check later in the
//!   same process, so reopening an idle database costs no full read.

#[cfg(test)]
mod powercut;

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};

use redb::backends::FileBackend;
use redb::{Builder, Database, DatabaseError, StorageBackend, StorageError, TableDefinition};

pub type Result<T> = std::result::Result<T, DatabaseError>;

/// Document id → DocMeta JSON.
pub const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
/// Sequence → document id.
pub const BY_SEQ: TableDefinition<u64, &[u8]> = TableDefinition::new("by_seq");
/// (document id, rev) → body.
pub const BODIES: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("bodies");
/// `_local` document id → body.
pub const LOCAL: TableDefinition<&str, &[u8]> = TableDefinition::new("local");
/// Purge sequence → purge request.
pub const PURGE_INFOS: TableDefinition<u64, &[u8]> = TableDefinition::new("purge_infos");
/// (document id, rev) → the purge sequence that removed it, 8 bytes big-endian.
pub const PURGED_REVS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("purged_revs");
/// Purge request uuid → purge sequence.
pub const PURGE_UUIDS: TableDefinition<&str, &[u8]> = TableDefinition::new("purge_uuids");
/// Checkpoint id → checkpoint.
pub const CHECKPOINTS: TableDefinition<&str, &[u8]> = TableDefinition::new("checkpoints");
/// Setting name → value.
pub const SETTINGS: TableDefinition<&str, &[u8]> = TableDefinition::new("settings");

/// One variant per table definition above.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Table {
    Meta,
    BySeq,
    Bodies,
    Local,
    PurgeInfos,
    PurgedRevs,
    PurgeUuids,
    Checkpoints,
    Settings,
}

/// A stored value whose checksum does not match: the bytes on disk are not
/// the bytes that were written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Corrupt;

impl std::fmt::Display for Corrupt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("stored value fails its checksum")
    }
}

impl std::error::Error for Corrupt {}

/// Bytes of the checksum [`seal`] appends.
const CHECKSUM_LEN: usize = 4;

/// `value` followed by its CRC32C, little-endian: the form every table stores.
pub fn seal(value: &[u8]) -> Vec<u8> {
    let mut sealed = Vec::with_capacity(value.len() + CHECKSUM_LEN);
    sealed.extend_from_slice(value);
    sealed.extend_from_slice(&crc32c::crc32c(value).to_le_bytes());
    sealed
}

/// The value inside bytes written by [`seal`], or [`Corrupt`] when they fail
/// their checksum. This is the only place a checksum mismatch surfaces.
pub fn unseal(stored: &[u8]) -> std::result::Result<&[u8], Corrupt> {
    let split = stored.len().checked_sub(CHECKSUM_LEN).ok_or(Corrupt)?;
    let (value, checksum) = stored.split_at(split);
    if checksum == crc32c::crc32c(value).to_le_bytes() {
        Ok(value)
    } else {
        Err(Corrupt)
    }
}

/// Files that passed an integrity check when they last closed in this
/// process, and so can open without one. An open takes its file out, so
/// only a [`close_verified`] puts it back: a file closed any other way is
/// checked on its next open. The lock is held across an open (decision and
/// file open, integrity check included) and across a verified close's
/// release and record, so opens are serialised on it.
static VERIFIED: LazyLock<Mutex<HashSet<PathBuf>>> = LazyLock::new(Default::default);

fn verified_set() -> MutexGuard<'static, HashSet<PathBuf>> {
    VERIFIED.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Open (creating if absent) the redb file at `path` with a page cache of
/// `cache_bytes`. The open runs a full integrity check, failing with
/// `StorageError::Corrupted` on a damaged file, unless the file passed
/// [`close_verified`] when it last closed in this process.
pub fn open(path: &Path, cache_bytes: usize) -> Result<Database> {
    open_file(path, cache_bytes, |file| file)
}

/// [`open`] through the backend `wrap` makes of the file.
fn open_file<B: StorageBackend>(
    path: &Path,
    cache_bytes: usize,
    wrap: impl FnOnce(FileBackend) -> B,
) -> Result<Database> {
    // Held until the file is open: a close_verified of this path finishing
    // between the decision and the open would mark this session verified.
    let mut verified = verified_set();
    let check = !verified.remove(path);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let db = builder(cache_bytes, check).create_with_backend(wrap(FileBackend::new(file)?));
    drop(verified);
    db
}

/// Open a database over `backend`. With no path to remember a verification
/// by, the open always runs the full integrity check.
pub fn open_with(backend: impl StorageBackend, cache_bytes: usize) -> Result<Database> {
    builder(cache_bytes, true).create_with_backend(backend)
}

fn builder(cache_bytes: usize, check_integrity: bool) -> Builder {
    let mut builder = Builder::new();
    builder
        .set_cache_size(cache_bytes)
        .set_check_integrity_on_open(check_integrity);
    builder
}

/// Close `db`, the file at `path`, after a full integrity check. A file that
/// passes opens without the check later in this process; one that fails (or
/// had to be repaired), or whose close fails, is checked again on its next
/// open, and the error is returned. Call it off the request path, with no transaction open: the
/// check reads the whole file.
pub fn close_verified(mut db: Database, path: &Path) -> Result<()> {
    let outcome = db.check_integrity();
    // Held from the release of the file until it is recorded: an open of
    // this path in between would be marked verified by the insert below. A
    // close that fails leaves the file for its next open to recover, so it
    // is not verified either.
    let mut verified = verified_set();
    let closed = db.close();
    match outcome {
        Ok(true) => {
            if let Err(error) = closed {
                verified.remove(path);
                return Err(error);
            }
            verified.insert(path.to_path_buf());
            Ok(())
        }
        Ok(false) => {
            verified.remove(path);
            Err(StorageError::Corrupted(format!(
                "{} failed its integrity check and was repaired",
                path.display()
            ))
            .into())
        }
        Err(error) => {
            verified.remove(path);
            Err(error)
        }
    }
}

/// Forget that the file at `path` was verified, because it has been replaced
/// (compaction swap, delete, recreate): its next open runs the check.
pub fn forget_verified(path: &Path) {
    verified_set().remove(path);
}

#[cfg(test)]
mod tests {
    use super::powercut::PowerCut;
    use super::*;
    use redb::{ReadableDatabase, TableError};
    use std::io::ErrorKind;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// A temporary directory, removed when dropped.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(tag: &str) -> Scratch {
            let dir =
                std::env::temp_dir().join(format!("douchdb-kv-{tag}-{}", crate::util::uuid()));
            std::fs::create_dir_all(&dir).unwrap();
            Scratch(dir)
        }

        fn file(&self) -> PathBuf {
            self.0.join("shard-0000.redb")
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            forget_verified(&self.file());
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A file backend that counts the bytes redb reads through it.
    #[derive(Debug)]
    struct Counting {
        inner: FileBackend,
        read: Arc<AtomicU64>,
    }

    impl StorageBackend for Counting {
        fn len(&self) -> std::result::Result<u64, std::io::Error> {
            self.inner.len()
        }

        fn read(&self, offset: u64, out: &mut [u8]) -> std::result::Result<(), std::io::Error> {
            self.read.fetch_add(out.len() as u64, Ordering::SeqCst);
            self.inner.read(offset, out)
        }

        fn set_len(&self, len: u64) -> std::result::Result<(), std::io::Error> {
            self.inner.set_len(len)
        }

        fn sync_data(&self) -> std::result::Result<(), std::io::Error> {
            self.inner.sync_data()
        }

        fn write(&self, offset: u64, data: &[u8]) -> std::result::Result<(), std::io::Error> {
            self.inner.write(offset, data)
        }

        fn close(&self) -> std::result::Result<(), std::io::Error> {
            self.inner.close()
        }
    }

    /// A file backend that runs a hook once redb has closed it: the moment a
    /// [`close_verified`] has released the file but may not yet have
    /// recorded it.
    struct OnClose {
        inner: FileBackend,
        hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }

    impl std::fmt::Debug for OnClose {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("OnClose").finish_non_exhaustive()
        }
    }

    impl StorageBackend for OnClose {
        fn len(&self) -> std::result::Result<u64, std::io::Error> {
            self.inner.len()
        }

        fn read(&self, offset: u64, out: &mut [u8]) -> std::result::Result<(), std::io::Error> {
            self.inner.read(offset, out)
        }

        fn set_len(&self, len: u64) -> std::result::Result<(), std::io::Error> {
            self.inner.set_len(len)
        }

        fn sync_data(&self) -> std::result::Result<(), std::io::Error> {
            self.inner.sync_data()
        }

        fn write(&self, offset: u64, data: &[u8]) -> std::result::Result<(), std::io::Error> {
            self.inner.write(offset, data)
        }

        fn close(&self) -> std::result::Result<(), std::io::Error> {
            let closed = self.inner.close();
            if let Some(hook) = self.hook.lock().unwrap().take() {
                hook();
            }
            closed
        }
    }

    /// Whether another thread could change the verified set right now. When
    /// it could, a racing open or close is run in place; when the set is
    /// held, the race cannot happen there and the caller runs that step
    /// after instead.
    fn set_is_free() -> bool {
        VERIFIED.try_lock().is_ok()
    }

    /// Bytes read by an `open` of `path` (with its usual verified-set rule),
    /// counted through the backend.
    fn bytes_read_by_open(path: &Path) -> u64 {
        let read = Arc::new(AtomicU64::new(0));
        let db = open_file(path, CACHE, |inner| Counting {
            inner,
            read: Arc::clone(&read),
        })
        .unwrap();
        drop(db);
        read.load(Ordering::SeqCst)
    }

    const CACHE: usize = 1 << 20;
    /// Written into every value, so a test can find value pages in the file.
    const MARKER: &[u8] = b"<<douchdb-kv-test-marker>>";

    /// Store `count` values of about 1 KiB, each holding [`MARKER`].
    fn fill(db: &Database, count: usize) {
        let value: Vec<u8> = MARKER.iter().copied().cycle().take(1024).collect();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(META).unwrap();
            for i in 0..count {
                table
                    .insert(format!("doc-{i:06}").as_str(), seal(&value).as_slice())
                    .unwrap();
            }
        }
        txn.commit().unwrap();
    }

    fn put(db: &Database, key: &str, value: &[u8]) {
        let txn = db.begin_write().unwrap();
        txn.open_table(META)
            .unwrap()
            .insert(key, seal(value).as_slice())
            .unwrap();
        txn.commit().unwrap();
    }

    /// The unsealed value under `key`, `None` when it (or its table) is absent.
    fn get(db: &Database, key: &str) -> Option<Vec<u8>> {
        let txn = db.begin_read().unwrap();
        let table = match txn.open_table(META) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return None,
            Err(error) => panic!("{error}"),
        };
        let guard = table.get(key).unwrap()?;
        Some(unseal(guard.value()).unwrap().to_vec())
    }

    /// Flip a byte inside every copy of [`MARKER`] in the file: value pages
    /// of the committed tree, never the header.
    fn damage(path: &Path) {
        let mut bytes = std::fs::read(path).unwrap();
        let hits: Vec<usize> = bytes
            .windows(MARKER.len())
            .enumerate()
            .filter(|(_, window)| *window == MARKER)
            .map(|(at, _)| at)
            .collect();
        assert!(!hits.is_empty(), "no value pages found");
        assert!(hits[0] >= 4096, "the header holds no values");
        for at in hits {
            bytes[at + 3] ^= 0x55;
        }
        std::fs::write(path, bytes).unwrap();
    }

    fn is_corrupted(error: &DatabaseError) -> bool {
        matches!(error, DatabaseError::Storage(StorageError::Corrupted(_)))
    }

    fn verified(path: &Path) -> bool {
        verified_set().contains(path)
    }

    #[test]
    fn unseal_rejects_a_flipped_bit() {
        let value = b"{\"_id\":\"a\",\"_rev\":\"1-x\"}";
        let sealed = seal(value);
        assert_eq!(sealed.len(), value.len() + CHECKSUM_LEN);
        assert_eq!(unseal(&sealed), Ok(&value[..]));
        assert_eq!(
            sealed[value.len()..],
            crc32c::crc32c(value).to_le_bytes(),
            "CRC32C, little-endian, appended"
        );
        for bit in 0..sealed.len() * 8 {
            let mut damaged = sealed.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert_eq!(unseal(&damaged), Err(Corrupt), "bit {bit}");
        }
        for short in 0..CHECKSUM_LEN {
            assert_eq!(unseal(&sealed[..short]), Err(Corrupt), "{short} bytes");
        }
        assert_eq!(unseal(&seal(b"")), Ok(&b""[..]));
    }

    #[test]
    fn first_open_rejects_a_file_damaged_while_closed() {
        let scratch = Scratch::new("damaged");
        let path = scratch.file();
        let db = open(&path, CACHE).unwrap();
        fill(&db, 200);
        drop(db);
        damage(&path);
        let error = open(&path, CACHE).expect_err("a damaged file opened");
        assert!(is_corrupted(&error), "{error:?}");
    }

    #[test]
    fn reopen_after_a_verified_close_skips_the_check() {
        let scratch = Scratch::new("verified");
        let path = scratch.file();
        let db = open(&path, CACHE).unwrap();
        fill(&db, 4000);
        drop(db);
        let size = std::fs::metadata(&path).unwrap().len();
        assert!(size > 4 << 20, "{size} bytes");

        // Never verified in this process: the open checks every page.
        let unverified = bytes_read_by_open(&path);
        assert!(unverified > size / 2, "{unverified} of {size} bytes read");

        close_verified(open(&path, CACHE).unwrap(), &path).unwrap();
        assert!(verified(&path));
        let reopened = bytes_read_by_open(&path);
        assert!(reopened < size / 50, "{reopened} of {size} bytes read");

        // That open consumed the verification and its close was a plain one.
        assert!(!verified(&path));
        assert!(bytes_read_by_open(&path) > size / 2);

        // A replaced file is forgotten.
        close_verified(open(&path, CACHE).unwrap(), &path).unwrap();
        forget_verified(&path);
        assert!(bytes_read_by_open(&path) > size / 2);
    }

    #[test]
    fn a_failed_close_check_forces_a_check_on_the_next_open() {
        let scratch = Scratch::new("failed-close");
        let path = scratch.file();
        let db = open(&path, CACHE).unwrap();
        fill(&db, 200);
        close_verified(db, &path).unwrap();
        assert!(verified(&path));

        // Every value is read, and so cached, before the file is damaged: the
        // check must go to the disk, not trust the cache.
        let db = open(&path, CACHE).unwrap();
        for i in 0..200 {
            assert!(get(&db, &format!("doc-{i:06}")).is_some());
        }
        damage(&path);
        let error = close_verified(db, &path).expect_err("a damaged file verified");
        assert!(is_corrupted(&error), "{error:?}");
        assert!(!verified(&path));

        let error = open(&path, CACHE).expect_err("a damaged file opened");
        assert!(is_corrupted(&error), "{error:?}");

        // Control: without the check the damaged file opens, and only the
        // value checksum notices.
        let plain = builder(CACHE, false).create(&path).unwrap();
        let txn = plain.begin_read().unwrap();
        let table = txn.open_table(META).unwrap();
        let stored = table.get("doc-000001").unwrap().unwrap();
        assert_eq!(unseal(stored.value()), Err(Corrupt));
    }

    #[test]
    fn an_open_racing_a_verified_close_is_not_left_verified() {
        let scratch = Scratch::new("race-close");
        let path = scratch.file();
        let db = open(&path, CACHE).unwrap();
        fill(&db, 10);
        drop(db);

        let second: Arc<Mutex<Option<Database>>> = Arc::default();
        let (racer, racer_path) = (Arc::clone(&second), path.clone());
        let db = open_file(&path, CACHE, |inner| OnClose {
            inner,
            hook: Mutex::new(Some(Box::new(move || {
                // Another thread opens the file between its release and
                // close_verified recording it.
                if set_is_free() {
                    *racer.lock().unwrap() = Some(open(&racer_path, CACHE).unwrap());
                }
            }))),
        })
        .unwrap();
        close_verified(db, &path).unwrap();
        let mut second = second.lock().unwrap().take();
        if second.is_none() {
            second = Some(open(&path, CACHE).unwrap());
        }
        assert!(
            !verified(&path),
            "verified while a session that will not end in close_verified is open"
        );
        drop(second);
    }

    #[test]
    fn a_verified_close_racing_an_open_does_not_verify_that_open() {
        let scratch = Scratch::new("race-open");
        let path = scratch.file();
        let first = open(&path, CACHE).unwrap();
        fill(&first, 10);
        let first = Mutex::new(Some(first));
        let second = open_file(&path, CACHE, |inner| {
            // The first session closes verified between this open deciding
            // to check and opening the file.
            let first = first.lock().unwrap().take().unwrap();
            if set_is_free() {
                close_verified(first, &path).unwrap();
            } else {
                drop(first);
            }
            inner
        })
        .unwrap();
        assert!(
            !verified(&path),
            "verified while a session that will not end in close_verified is open"
        );
        drop(second);
    }

    #[test]
    fn committed_writes_survive_a_power_cut() {
        let (backend, switch) = PowerCut::new();
        let db = open_with(backend, CACHE).unwrap();
        put(&db, "a", b"1");
        switch.cut();
        drop(db);
        let db = open_with(switch.backend(), CACHE).unwrap();
        assert_eq!(get(&db, "a").as_deref(), Some(&b"1"[..]));
    }

    #[test]
    fn uncommitted_writes_vanish_in_a_power_cut() {
        let (backend, switch) = PowerCut::new();
        let db = open_with(backend, CACHE).unwrap();
        put(&db, "base", b"1");
        let txn = db.begin_write().unwrap();
        txn.open_table(META)
            .unwrap()
            .insert("pending", seal(b"2").as_slice())
            .unwrap();
        switch.cut();
        drop(txn);
        drop(db);
        let db = open_with(switch.backend(), CACHE).unwrap();
        assert_eq!(get(&db, "base").as_deref(), Some(&b"1"[..]));
        assert_eq!(get(&db, "pending"), None);
    }

    #[test]
    fn a_build_without_sync_loses_data() {
        let (backend, switch) = PowerCut::new();
        let db = open_with(backend, CACHE).unwrap();
        put(&db, "base", b"1");
        switch.ignore_syncs();
        put(&db, "unsynced", b"2");
        assert_eq!(get(&db, "unsynced").as_deref(), Some(&b"2"[..]));
        switch.cut();
        drop(db);
        let db = open_with(switch.backend(), CACHE).unwrap();
        assert_eq!(get(&db, "base").as_deref(), Some(&b"1"[..]));
        assert_eq!(
            get(&db, "unsynced"),
            None,
            "a commit survived without a sync"
        );
    }

    #[test]
    fn a_failed_write_fails_the_commit() {
        let (backend, switch) = PowerCut::new();
        let db = open_with(backend, CACHE).unwrap();
        put(&db, "base", b"1");
        switch.fail_writes(ErrorKind::StorageFull);
        let txn = db.begin_write().unwrap();
        txn.open_table(META)
            .unwrap()
            .insert("full", seal(b"2").as_slice())
            .unwrap();
        let error = txn.commit().expect_err("a commit succeeded on a full disk");
        assert!(
            matches!(&error, redb::CommitError::Storage(StorageError::Io(io)) if io.kind() == ErrorKind::StorageFull),
            "{error:?}"
        );
    }
}

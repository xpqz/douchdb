//! The redb storage foundation: opening a file, the tables, and the checksum
//! every stored value carries.
//!
//! redb checksums its pages but verifies them only in a full integrity check,
//! so three things stand guard here (see "Corruption detection" in
//! `docs/superpowers/specs/2026-11-16-redb-storage-design.md`):
//!
//! - every value in every table is [`seal`]ed: a CRC32C of the value is
//!   appended on write, and [`unseal`] checks it on every read, so a damaged
//!   value is a [`Corrupt`] error, never a wrong answer or a miss;
//! - a full integrity check runs on the first [`open`] of each file in a
//!   process and in [`close_verified`] when an idle database closes. A file
//!   that passed its close-time check opens without the check later in the
//!   same process, so reopening an idle database costs no full read.
//! - every write transaction begins in [`begin_write`] and commits in two
//!   phases, so that check reports damage to the newest commit instead of
//!   rolling back to the commit before it.

#[cfg(test)]
pub(crate) mod powercut;

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};

use redb::backends::FileBackend;
use redb::{
    Builder, Database, DatabaseError, ReadableDatabase, StorageBackend, StorageError,
    TableDefinition, TransactionError, WriteTransaction,
};

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
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

/// Per file: whether it passed an integrity check when it last closed in
/// this process, and so can open without one. An open clears the flag, so
/// only a [`close_verified`] sets it again: a file closed any other way is
/// checked on its next open.
///
/// Each file has its own lock, held across an open (decision, file open,
/// integrity check) and across a verified close's release and record, so
/// those are atomic for that file while files never wait on each other. The
/// map's lock is held only to find a file's entry.
static VERIFIED: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<bool>>>>> =
    LazyLock::new(Default::default);

/// The verification flag of the file at `path`, for the caller to lock.
fn verified_flag(path: &Path) -> Arc<Mutex<bool>> {
    let mut files = VERIFIED.lock().unwrap_or_else(PoisonError::into_inner);
    Arc::clone(files.entry(path.to_path_buf()).or_default())
}

fn lock(flag: &Mutex<bool>) -> MutexGuard<'_, bool> {
    flag.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Open the existing redb file at `path` with a page cache of
/// `cache_bytes`. The open runs a full integrity check, failing with
/// `StorageError::Corrupted` on a damaged file, unless the file passed
/// [`close_verified`] when it last closed in this process.
///
/// A file that is not there is an error, never created: only [`create`]
/// makes files, so a shard that went missing is never replaced by an empty
/// one. An empty file is refused for the same reason (redb would initialise
/// it as new).
pub fn open(path: &Path, cache_bytes: usize) -> Result<Database> {
    open_file(path, cache_bytes, |file| file)
}

/// Create a new redb file at `path`, failing if one is there already. A new
/// file needs no integrity check, and nothing about it is remembered: its
/// first [`open`] checks it.
pub fn create(path: &Path, cache_bytes: usize) -> Result<Database> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?;
    builder(cache_bytes, false).create_with_backend(FileBackend::new(file)?)
}

/// The existing, non-empty file at `path`, opened for redb.
fn existing_file(path: &Path) -> Result<std::fs::File> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    if file.metadata()?.len() == 0 {
        return Err(StorageError::Corrupted(format!("{} is empty", path.display())).into());
    }
    Ok(file)
}

/// Run `read` on the closed file at `path` and close it again, so the
/// settings of a database that is not open can be read without a full
/// integrity check every time.
///
/// A file not verified in this process is opened as [`open`] would open it,
/// with the full check, and left verified once that passed and `read` (which
/// only reads) is done: this is the one writable open a probed file gets. A
/// verified file is opened read-only, which redb does without modifying it,
/// so repeated probes never write and stay verified.
///
/// The file's lock is held throughout, so an [`open`] or [`close_verified`]
/// of the same path waits for it; a file open elsewhere fails with
/// `DatabaseError::DatabaseAlreadyOpen`. Like [`open`], it never creates the
/// file.
pub fn peek<R>(
    path: &Path,
    cache_bytes: usize,
    read: impl FnOnce(&dyn ReadableDatabase) -> R,
) -> Result<R> {
    let flag = verified_flag(path);
    let mut verified = lock(&flag);
    if *verified {
        let file = OpenOptions::new().read(true).open(path)?;
        if file.metadata()?.len() == 0 {
            return Err(StorageError::Corrupted(format!("{} is empty", path.display())).into());
        }
        let db =
            builder(cache_bytes, false).open_read_only_with_backend(FileBackend::new(file)?)?;
        return Ok(read(&db));
    }
    let db =
        builder(cache_bytes, true).create_with_backend(FileBackend::new(existing_file(path)?)?)?;
    let result = read(&db);
    db.close()?;
    *verified = true;
    Ok(result)
}

/// [`open`] through the backend `wrap` makes of the file.
fn open_file<B: StorageBackend>(
    path: &Path,
    cache_bytes: usize,
    wrap: impl FnOnce(FileBackend) -> B,
) -> Result<Database> {
    // Held until the file is open: a close_verified of this path finishing
    // between the decision and the open would mark this session verified.
    let flag = verified_flag(path);
    let mut verified = lock(&flag);
    let check = !std::mem::replace(&mut *verified, false);
    let file = existing_file(path)?;
    let db = builder(cache_bytes, check).create_with_backend(wrap(FileBackend::new(file)?));
    drop(verified);
    db
}

/// Open a database over `backend`. With no path to remember a verification
/// by, the open always runs the full integrity check.
#[cfg(test)]
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
/// open, and the error is returned. Call it off the request path, with no
/// transaction open: the check reads the whole file.
pub fn close_verified(mut db: Database, path: &Path) -> Result<()> {
    let outcome = db.check_integrity();
    // Held from the release of the file until it is recorded: an open of
    // this path in between would be marked verified by the record below. A
    // close that fails leaves the file for its next open to recover, so it
    // is not verified either.
    let flag = verified_flag(path);
    let mut verified = lock(&flag);
    let closed = db.close();
    *verified = false;
    match outcome {
        Ok(true) => {
            closed?;
            *verified = true;
            Ok(())
        }
        Ok(false) => Err(StorageError::Corrupted(format!(
            "{} failed its integrity check and was repaired",
            path.display()
        ))
        .into()),
        Err(error) => Err(error),
    }
}

/// Begin a write transaction on `db` that commits in two phases, at redb's
/// default durability (`Immediate`). Every write transaction goes through
/// here: none calls `Database::begin_write` itself.
///
/// A one-phase commit writes its pages and the header naming them under a
/// single sync, so redb takes a newest commit whose pages fail their
/// checksums for one torn by a crash and silently rolls back to the commit
/// before it, losing an acknowledged write. A two-phase commit syncs its
/// pages before the header that makes them the newest commit, so damage to
/// them can only be damage, and the open or integrity check reports it as
/// `StorageError::Corrupted`. The cost is a second sync per commit.
pub(crate) fn begin_write(
    db: &Database,
) -> std::result::Result<WriteTransaction, TransactionError> {
    let mut txn = db.begin_write()?;
    txn.set_two_phase_commit(true);
    Ok(txn)
}

/// Whether the file at `path` would open without the full check.
#[cfg(test)]
pub(crate) fn is_verified(path: &Path) -> bool {
    *lock(&verified_flag(path))
}

/// Forget that the file at `path` was verified, because it has been replaced
/// (compaction swap, delete, recreate): its next open runs the check.
pub fn forget_verified(path: &Path) {
    *lock(&verified_flag(path)) = false;
}

#[cfg(test)]
mod tests {
    use super::powercut::PowerCut;
    use super::*;
    use redb::{ReadableDatabase, TableError};
    use std::io::ErrorKind;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;

    /// A temporary directory, removed when dropped.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(tag: &str) -> Scratch {
            let dir =
                std::env::temp_dir().join(format!("douchdb-kv-{tag}-{}", crate::util::uuid()));
            std::fs::create_dir_all(&dir).unwrap();
            let scratch = Scratch(dir);
            drop(create(&scratch.file(), CACHE).unwrap());
            scratch
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

    /// Whether another thread could change `path`'s verification right now.
    /// When it could, a racing open or close is run in place; when the flag
    /// is held, the race cannot happen there and the caller runs that step
    /// after instead.
    fn file_is_free(path: &Path) -> bool {
        verified_flag(path).try_lock().is_ok()
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
        let txn = begin_write(db).unwrap();
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
        let txn = begin_write(db).unwrap();
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

    /// Written only by the newest commit of a test: the pages that hold it
    /// are pages no earlier commit wrote.
    const NEWEST: &[u8] = b"<<douchdb-kv-newest-commit>>";

    /// Flip a byte inside every copy of `marker` in `bytes`: value pages of
    /// the committed tree, never the header.
    fn flip(bytes: &mut [u8], marker: &[u8]) {
        let hits: Vec<usize> = bytes
            .windows(marker.len())
            .enumerate()
            .filter(|(_, window)| *window == marker)
            .map(|(at, _)| at)
            .collect();
        assert!(!hits.is_empty(), "no value pages found");
        assert!(hits[0] >= 4096, "the header holds no values");
        for at in hits {
            bytes[at + 3] ^= 0x55;
        }
    }

    /// Damage every copy of [`MARKER`] in the file.
    fn damage(path: &Path) {
        let mut bytes = std::fs::read(path).unwrap();
        flip(&mut bytes, MARKER);
        std::fs::write(path, bytes).unwrap();
    }

    /// Commit an older value, then a newer one holding [`NEWEST`].
    fn commit_older_then_newest(db: &Database) {
        put(db, "older", b"1");
        put(db, "newest", &NEWEST.repeat(64));
    }

    /// Close `db` as a crash right after its last commit would, with that
    /// commit's pages then damaged: the file's bytes are taken while `db` is
    /// open and written back once it has closed, so the commit redb makes
    /// on a clean close is not on it.
    fn crash_with_newest_damaged(db: Database, path: &Path) {
        let mut bytes = std::fs::read(path).unwrap();
        drop(db);
        flip(&mut bytes, NEWEST);
        std::fs::write(path, bytes).unwrap();
        forget_verified(path);
    }

    /// An open of `path` must fail as corrupted; one that succeeds serves
    /// the older commit as if the newest had never been acknowledged.
    fn assert_fails_closed(path: &Path) {
        match open(path, CACHE) {
            Err(error) => assert!(is_corrupted(&error), "{error:?}"),
            Ok(db) => panic!(
                "opened with its newest commit damaged: older = {:?}, newest = {:?}",
                get(&db, "older"),
                get(&db, "newest").map(|value| value.len())
            ),
        }
    }

    fn is_corrupted(error: &DatabaseError) -> bool {
        matches!(error, DatabaseError::Storage(StorageError::Corrupted(_)))
    }

    fn verified(path: &Path) -> bool {
        is_verified(path)
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

    /// redb takes damage to a one-phase commit for a torn write and rolls
    /// back to the commit before it, losing an acknowledged write without a
    /// word. Every commit is two-phase, so the damage is reported instead.
    #[test]
    fn damage_to_the_newest_commit_is_corruption_not_rollback() {
        let scratch = Scratch::new("newest");
        let path = scratch.file();
        let db = open(&path, CACHE).unwrap();
        commit_older_then_newest(&db);
        crash_with_newest_damaged(db, &path);
        assert_fails_closed(&path);
        assert_fails_closed(&path);
    }

    /// The same damage while the file is open: the close-time check reports
    /// it and leaves the file unverified, and the next open fails closed
    /// rather than serving the commit before it.
    #[test]
    fn the_close_time_check_reports_damage_to_the_newest_commit() {
        use std::os::unix::fs::FileExt;
        let scratch = Scratch::new("newest-close");
        let path = scratch.file();
        let db = open(&path, CACHE).unwrap();
        commit_older_then_newest(&db);
        let before = std::fs::read(&path).unwrap();
        let mut after = before.clone();
        flip(&mut after, NEWEST);
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        for (at, (old, new)) in before.iter().zip(&after).enumerate() {
            if old != new {
                file.write_all_at(&[*new], at as u64).unwrap();
            }
        }
        drop(file);
        let error = close_verified(db, &path).expect_err("a damaged file verified");
        assert!(is_corrupted(&error), "{error:?}");
        assert!(!verified(&path));
        assert_fails_closed(&path);
    }

    /// A two-phase commit syncs its pages, then the header that makes them
    /// the newest commit. Power lost between the two leaves the pages on the
    /// disk but the previous commit in force, and the commit unacknowledged.
    #[test]
    fn a_power_cut_between_the_two_syncs_keeps_the_old_state_unacknowledged() {
        let (backend, switch) = PowerCut::new();
        let db = open_with(backend, CACHE).unwrap();
        put(&db, "older", b"1");
        let before = switch.syncs();
        put(&db, "control", b"2");
        let per_commit = switch.syncs() - before;
        assert!(per_commit >= 2, "a commit synced {per_commit} times");

        switch.cut_at_sync(per_commit - 1);
        let txn = begin_write(&db).unwrap();
        txn.open_table(META)
            .unwrap()
            .insert("newest", seal(&NEWEST.repeat(64)).as_slice())
            .unwrap();
        txn.commit()
            .expect_err("a commit was acknowledged without its header on the disk");
        drop(db);

        let backend = switch.backend();
        let mut image = vec![0; usize::try_from(backend.len().unwrap()).unwrap()];
        backend.read(0, &mut image).unwrap();
        assert!(
            image.windows(NEWEST.len()).any(|window| window == NEWEST),
            "the power was cut before the commit's pages were synced"
        );
        let db = open_with(backend, CACHE).unwrap();
        assert_eq!(get(&db, "control").as_deref(), Some(&b"2"[..]));
        assert_eq!(get(&db, "newest"), None);
    }

    /// Every write transaction commits in two phases only if it begins in
    /// [`begin_write`]: nothing else in `src/` begins one, in method or path
    /// form, or changes how one commits. The helper's own body is the one
    /// exemption. The patterns are assembled at run time so that this test
    /// does not match itself.
    #[test]
    fn every_write_transaction_goes_through_kv_begin_write() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let own = root.join("kv.rs");
        let calls = ["begin_write", "try_begin_write"];
        let mut patterns: Vec<String> = calls
            .iter()
            .flat_map(|call| [format!(".{call}("), format!("Database::{call}(")])
            .collect();
        patterns.extend(["two_phase_commit", "durability"].map(|knob| format!("set_{knob}(")));
        let helper = format!("pub(crate) fn {}(", calls[0]);
        let mut dirs = vec![root];
        let (mut scanned, mut found, mut exempt) = (0, Vec::new(), 0);
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    dirs.push(path);
                } else if path.extension().is_some_and(|ext| ext == "rs") {
                    scanned += 1;
                    let text = std::fs::read_to_string(&path).unwrap();
                    let mut in_helper = false;
                    for (n, line) in text.lines().enumerate() {
                        if path == own && line.starts_with(&helper) {
                            in_helper = true;
                        }
                        if in_helper {
                            exempt += 1;
                            in_helper = line != "}";
                        } else if patterns
                            .iter()
                            .any(|pattern| line.contains(pattern.as_str()))
                        {
                            found.push(format!("{}:{}", path.display(), n + 1));
                        }
                    }
                }
            }
        }
        assert!(scanned > 10, "only {scanned} source files found");
        assert!(exempt > 0, "the helper was not found in {}", own.display());
        assert!(
            found.is_empty(),
            "write transactions begun or configured outside kv::begin_write: {found:#?}"
        );
    }

    #[test]
    fn a_first_peek_checks_a_file_damaged_while_closed() {
        let scratch = Scratch::new("peek-damaged");
        let path = scratch.file();
        let db = open(&path, CACHE).unwrap();
        fill(&db, 200);
        drop(db);
        damage(&path);
        let error = peek(&path, CACHE, |_| ()).expect_err("a damaged file was read");
        assert!(is_corrupted(&error), "{error:?}");
        assert!(!verified(&path));
    }

    /// A peek reads a closed file without costing a full check every time:
    /// once it has passed one, the file stays verified, and an open after
    /// it reads little.
    #[test]
    fn a_peek_leaves_a_checked_file_verified() {
        let scratch = Scratch::new("peek-verified");
        let path = scratch.file();
        let db = open(&path, CACHE).unwrap();
        fill(&db, 4000);
        drop(db);
        let size = std::fs::metadata(&path).unwrap().len();
        assert!(!verified(&path));

        let read = peek(&path, CACHE, |db| {
            let txn = db.begin_read().unwrap();
            let table = txn.open_table(META).unwrap();
            table.get("doc-000007").unwrap().is_some()
        })
        .unwrap();
        assert!(read);
        assert!(verified(&path), "the checked peek did not record it");
        // A peek of a verified file opens it read-only: not a byte changes.
        let before = std::fs::read(&path).unwrap();
        peek(&path, CACHE, |_| ()).unwrap();
        peek(&path, CACHE, |_| ()).unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "a verified peek wrote"
        );
        assert!(verified(&path), "a peek of a verified file forgot it");
        let reopened = bytes_read_by_open(&path);
        assert!(reopened < size / 50, "{reopened} of {size} bytes read");
    }

    /// Only `create` makes files: an open or a peek of a missing file fails
    /// and leaves none behind, and an empty file (which redb would take for
    /// a new one) is refused as it is.
    #[test]
    fn open_and_peek_never_create_a_file() {
        let scratch = Scratch::new("no-create");
        let missing = scratch.0.join("missing.redb");
        assert!(open(&missing, CACHE).is_err());
        assert!(peek(&missing, CACHE, |_| ()).is_err());
        assert!(!missing.exists());

        let empty = scratch.0.join("empty.redb");
        std::fs::write(&empty, b"").unwrap();
        let error = open(&empty, CACHE).expect_err("an empty file opened");
        assert!(is_corrupted(&error), "{error:?}");
        assert!(peek(&empty, CACHE, |_| ()).is_err());
        assert_eq!(std::fs::metadata(&empty).unwrap().len(), 0);

        let made = scratch.0.join("made.redb");
        drop(create(&made, CACHE).unwrap());
        assert!(create(&made, CACHE).is_err(), "create replaced a file");
        drop(open(&made, CACHE).unwrap());
    }

    /// A peek of a file open elsewhere in the process is refused by redb's
    /// lock, never a second session on it; the caller asks the open
    /// database instead.
    #[test]
    fn a_peek_of_an_open_file_is_refused() {
        let scratch = Scratch::new("peek-open");
        let path = scratch.file();
        let db = open(&path, CACHE).unwrap();
        put(&db, "k", b"v");
        let error = peek(&path, CACHE, |_| ()).expect_err("an open file was peeked");
        assert!(
            matches!(error, DatabaseError::DatabaseAlreadyOpen),
            "{error:?}"
        );
        assert_eq!(get(&db, "k"), Some(b"v".to_vec()));
        drop(db);
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
                if file_is_free(&racer_path) {
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
            if file_is_free(&path) {
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
    fn an_open_of_one_path_does_not_hold_up_another() {
        let (first, other) = (Scratch::new("hold-first"), Scratch::new("hold-other"));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let held = std::thread::spawn({
            let path = first.file();
            move || {
                open_file(&path, CACHE, |inner| {
                    // Inside the open's locked section until released.
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    inner
                })
                .map(drop)
            }
        });
        entered_rx.recv().unwrap();

        // Inline: with one lock for every file this blocks for good, and
        // the run's bound reports it.
        let path = other.file();
        let db = open(&path, CACHE).unwrap();
        close_verified(db, &path).unwrap();

        release_tx.send(()).unwrap();
        held.join().unwrap().unwrap();
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
        let txn = begin_write(&db).unwrap();
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
        let txn = begin_write(&db).unwrap();
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

//! A storage backend that loses power on command, for durability tests.
//!
//! It holds two images of the file: the durable one, which is what a disk
//! would still hold after a power cut, and the volatile one, which every
//! write and `set_len` changes. `sync_data` makes the volatile image durable;
//! [`PowerSwitch::cut`] throws the volatile image away. A cut also kills every
//! backend handed out before it, so whatever the dropped database does on its
//! way out (a clean-shutdown write, a sync) cannot reach the durable image;
//! [`PowerSwitch::backend`] hands out a live one to reopen over what survived.
//!
//! The disk also stands for the file's name: [`PowerSwitch::rename_over`]
//! puts another disk's durable image under it, and until
//! [`PowerSwitch::sync_directory`] a cut brings back the image the name held
//! before, as a power cut does to a rename whose directory was not synced.

use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, Default)]
struct Disk {
    durable: Vec<u8>,
    volatile: Vec<u8>,
    /// Bumped by every cut; a backend from an earlier generation is dead.
    generation: u64,
    /// Every later write and sync fails with this.
    failing: Option<ErrorKind>,
    /// Every later read fails with this.
    reads_failing: Option<ErrorKind>,
    /// The next read fails with this; later ones succeed.
    one_read_failing: Option<ErrorKind>,
    /// What befalls a later sync, once `after` more syncs have succeeded.
    sync_fault: Option<SyncFault>,
    /// `sync_data` succeeds without making anything durable.
    syncs_ignored: bool,
    /// `sync_data` calls that succeeded.
    syncs: usize,
    /// What the name held durably before a rename over it whose directory
    /// has not been synced since; a cut brings it back.
    displaced: Option<Vec<u8>>,
    /// Every later rename over the file fails with this.
    renames_failing: Option<ErrorKind>,
    /// Every later directory sync fails with this.
    directory_syncs_failing: Option<ErrorKind>,
}

#[derive(Debug, Clone, Copy)]
struct SyncFault {
    after: usize,
    kind: Fault,
}

#[derive(Debug, Clone, Copy)]
enum Fault {
    /// This sync and every later one fail with the error; writes still
    /// succeed.
    Fail(ErrorKind),
    /// This sync fails with the error; later ones succeed.
    FailOnce(ErrorKind),
    /// The power is cut at this sync, which fails.
    Cut,
}

impl Disk {
    fn cut(&mut self) {
        if let Some(before) = self.displaced.take() {
            self.durable = before;
        }
        self.volatile = self.durable.clone();
        self.generation += 1;
        self.sync_fault = None;
    }
}

/// The backend redb writes through.
#[derive(Debug)]
pub struct PowerCut {
    disk: Arc<Mutex<Disk>>,
    generation: u64,
}

/// The test's handle on the disk under a [`PowerCut`]. Clones share the disk.
#[derive(Clone)]
pub struct PowerSwitch {
    disk: Arc<Mutex<Disk>>,
}

impl PowerCut {
    pub fn new() -> (PowerCut, PowerSwitch) {
        let disk = Arc::new(Mutex::new(Disk::default()));
        let switch = PowerSwitch { disk };
        (switch.backend(), switch)
    }

    /// The disk, if this backend still has power.
    fn live(&self) -> Result<MutexGuard<'_, Disk>, Error> {
        let disk = self.disk.lock().unwrap();
        if disk.generation == self.generation {
            Ok(disk)
        } else {
            Err(Error::other("the power was cut"))
        }
    }

    /// The disk, if this backend may change it.
    fn writable(&self) -> Result<MutexGuard<'_, Disk>, Error> {
        let disk = self.live()?;
        match disk.failing {
            Some(kind) => Err(Error::from(kind)),
            None => Ok(disk),
        }
    }
}

impl PowerSwitch {
    /// Discard every write since the last `sync_data`, and kill every backend
    /// handed out so far.
    pub fn cut(&self) {
        self.disk.lock().unwrap().cut();
    }

    /// Make every later write and sync fail with `error` (`StorageFull` for a
    /// full disk).
    pub fn fail_writes(&self, error: ErrorKind) {
        self.disk.lock().unwrap().failing = Some(error);
    }

    /// Make every later read fail with `error`.
    pub fn fail_reads(&self, error: ErrorKind) {
        self.disk.lock().unwrap().reads_failing = Some(error);
    }

    /// Make the next read fail with `error`: a passing I/O error.
    pub fn fail_one_read(&self, error: ErrorKind) {
        self.disk.lock().unwrap().one_read_failing = Some(error);
    }

    /// Let reads succeed again.
    pub fn restore_reads(&self) {
        self.disk.lock().unwrap().reads_failing = None;
    }

    /// Let `after` more `sync_data` calls succeed, then make every later one
    /// fail with `error` while writes still succeed: what was written stays
    /// in the volatile image, as in a page cache, so a commit can report an
    /// error and yet be on the disk.
    pub fn fail_syncs_after(&self, after: usize, error: ErrorKind) {
        self.disk.lock().unwrap().sync_fault = Some(SyncFault {
            after,
            kind: Fault::Fail(error),
        });
    }

    /// As [`PowerSwitch::fail_syncs_after`], but only the one sync fails: a
    /// passing I/O error, so the reopen that recovers from it succeeds.
    pub fn fail_one_sync_after(&self, after: usize, error: ErrorKind) {
        self.disk.lock().unwrap().sync_fault = Some(SyncFault {
            after,
            kind: Fault::FailOnce(error),
        });
    }

    /// Let `after` more `sync_data` calls succeed, then cut the power at the
    /// next one, which fails: a power cut part way through a commit.
    pub fn cut_at_sync(&self, after: usize) {
        self.disk.lock().unwrap().sync_fault = Some(SyncFault {
            after,
            kind: Fault::Cut,
        });
    }

    /// Let writes and syncs succeed again: the disk has room once more.
    pub fn restore_writes(&self) {
        let mut disk = self.disk.lock().unwrap();
        disk.failing = None;
        disk.sync_fault = None;
    }

    /// Make every later `sync_data` a no-op that reports success: a build
    /// that forgot to sync.
    pub fn ignore_syncs(&self) {
        self.disk.lock().unwrap().syncs_ignored = true;
    }

    /// How many `sync_data` calls have succeeded so far.
    pub fn syncs(&self) -> usize {
        self.disk.lock().unwrap().syncs
    }

    /// Put what `other` holds durably in place of this disk's contents, as
    /// a rename over the file does: the compaction swap of a shard on a test
    /// disk. A cut undoes it until [`PowerSwitch::sync_directory`].
    pub fn rename_over(&self, other: &PowerSwitch) -> Result<(), Error> {
        let image = other.disk.lock().unwrap().durable.clone();
        let mut disk = self.disk.lock().unwrap();
        if let Some(kind) = disk.renames_failing {
            return Err(Error::from(kind));
        }
        let before = std::mem::replace(&mut disk.durable, image.clone());
        disk.displaced.get_or_insert(before);
        disk.volatile = image;
        Ok(())
    }

    /// Sync the file's directory: a rename over the file survives a cut.
    pub fn sync_directory(&self) -> Result<(), Error> {
        let mut disk = self.disk.lock().unwrap();
        if let Some(kind) = disk.directory_syncs_failing {
            return Err(Error::from(kind));
        }
        disk.displaced = None;
        Ok(())
    }

    /// Make every later rename over the file fail with `error`.
    pub fn fail_renames(&self, error: ErrorKind) {
        self.disk.lock().unwrap().renames_failing = Some(error);
    }

    /// Make every later directory sync fail with `error`.
    pub fn fail_directory_syncs(&self, error: ErrorKind) {
        self.disk.lock().unwrap().directory_syncs_failing = Some(error);
    }

    /// A live backend over the disk as it now stands, to reopen after a cut.
    pub fn backend(&self) -> PowerCut {
        let generation = self.disk.lock().unwrap().generation;
        PowerCut {
            disk: Arc::clone(&self.disk),
            generation,
        }
    }
}

impl redb::StorageBackend for PowerCut {
    fn len(&self) -> Result<u64, Error> {
        Ok(self.live()?.volatile.len() as u64)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), Error> {
        let mut disk = self.live()?;
        if let Some(kind) = disk.reads_failing.or_else(|| disk.one_read_failing.take()) {
            return Err(Error::from(kind));
        }
        let start = usize::try_from(offset).map_err(Error::other)?;
        let bytes = start
            .checked_add(out.len())
            .and_then(|end| disk.volatile.get(start..end))
            .ok_or_else(|| Error::from(ErrorKind::UnexpectedEof))?;
        out.copy_from_slice(bytes);
        Ok(())
    }

    fn set_len(&self, len: u64) -> Result<(), Error> {
        let len = usize::try_from(len).map_err(Error::other)?;
        self.writable()?.volatile.resize(len, 0);
        Ok(())
    }

    fn sync_data(&self) -> Result<(), Error> {
        let mut disk = self.writable()?;
        if let Some(fault) = &mut disk.sync_fault {
            if let Some(after) = fault.after.checked_sub(1) {
                fault.after = after;
            } else {
                match fault.kind {
                    Fault::Fail(kind) => return Err(Error::from(kind)),
                    Fault::FailOnce(kind) => {
                        disk.sync_fault = None;
                        return Err(Error::from(kind));
                    }
                    Fault::Cut => {
                        disk.cut();
                        return Err(Error::other("the power was cut"));
                    }
                }
            }
        }
        disk.syncs += 1;
        if !disk.syncs_ignored {
            disk.durable = disk.volatile.clone();
        }
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), Error> {
        let mut disk = self.writable()?;
        let start = usize::try_from(offset).map_err(Error::other)?;
        let end = start
            .checked_add(data.len())
            .ok_or_else(|| Error::other("write past the end"))?;
        if disk.volatile.len() < end {
            disk.volatile.resize(end, 0);
        }
        disk.volatile[start..end].copy_from_slice(data);
        Ok(())
    }
}

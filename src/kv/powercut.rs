//! A storage backend that loses power on command, for durability tests.
//!
//! It holds two images of the file: the durable one, which is what a disk
//! would still hold after a power cut, and the volatile one, which every
//! write and `set_len` changes. `sync_data` makes the volatile image durable;
//! [`PowerSwitch::cut`] throws the volatile image away. A cut also kills every
//! backend handed out before it, so whatever the dropped database does on its
//! way out (a clean-shutdown write, a sync) cannot reach the durable image;
//! [`PowerSwitch::backend`] hands out a live one to reopen over what survived.

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
    /// `sync_data` succeeds without making anything durable.
    syncs_ignored: bool,
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
        let mut disk = self.disk.lock().unwrap();
        disk.volatile = disk.durable.clone();
        disk.generation += 1;
    }

    /// Make every later write and sync fail with `error` (`StorageFull` for a
    /// full disk).
    pub fn fail_writes(&self, error: ErrorKind) {
        self.disk.lock().unwrap().failing = Some(error);
    }

    /// Let writes and syncs succeed again: the disk has room once more.
    pub fn restore_writes(&self) {
        self.disk.lock().unwrap().failing = None;
    }

    /// Make every later `sync_data` a no-op that reports success: a build
    /// that forgot to sync.
    pub fn ignore_syncs(&self) {
        self.disk.lock().unwrap().syncs_ignored = true;
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
        let disk = self.live()?;
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

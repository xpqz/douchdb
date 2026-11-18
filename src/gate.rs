//! The shard handle gate: reads off the shard actor, on read transactions of
//! their own, through leases the gate can drain before the file is reopened.
//!
//! Reads that do not need the actor's in-memory indexes run on their own
//! redb read transaction, concurrently with the actor's write group. A
//! [`ReadTransaction`](redb::ReadTransaction) keeps the file open (and redb's
//! exclusive lock held) for as long as it lives, so a reader never holds one
//! directly: [`Gate::read`] takes a lease, opens the transaction, runs the
//! caller's function on the blocking pool with a [`LeaseTxn`] borrowed for
//! that call, then drops the transaction and returns the lease.
//! [`Gate::reopen`] closes the gate (new reads wait), waits until every lease
//! is back, drops the old database, opens the new one and opens the gate.
//!
//! Only the shard's actor (`store::Shard`) opens transactions without a
//! lease, through [`Gate::begin_write_txn`], [`Gate::begin_read`] and
//! [`Gate::read_here`]: it is the only caller of [`Gate::reopen`] and holds
//! none of them when it calls it. Every other read of a shard file goes
//! through [`Gate::read`]. Below the gate, only this module and `kv` call
//! redb's own `begin_read` and `begin_write` on a shard file the gate holds.
//!
//! Nothing a reader is handed can outlive its lease: `LeaseTxn` is only ever
//! lent, is neither `Send` nor `Sync` nor `Clone`, and returns owned bytes or
//! lends them to a callback for the length of one call. These do not compile:
//!
//! Returning the transaction from the read (wrapped in a type that claims to
//! be `Send`, so that only its lifetime stops it):
//!
//! ```compile_fail
//! use douchdb::gate::{Gate, LeaseTxn};
//! struct Smuggled<'a, 'l>(&'a LeaseTxn<'l>);
//! unsafe impl Send for Smuggled<'_, '_> {}
//! async fn escape(gate: &Gate) {
//!     let _ = gate.read(|txn| Ok(Smuggled(txn))).await;
//! }
//! ```
//!
//! Keeping a value `scan` lent to its callback:
//!
//! ```compile_fail
//! use douchdb::gate::{Gate, Table};
//! async fn escape(gate: &Gate) {
//!     let _ = gate
//!         .read(|txn| {
//!             let mut kept: Option<&[u8]> = None;
//!             txn.scan(Table::Meta, .., false, |_, value| {
//!                 kept = Some(value);
//!                 Ok(true)
//!             })?;
//!             Ok(kept.map(<[u8]>::to_vec))
//!         })
//!         .await;
//! }
//! ```
//!
//! Handing the transaction to another thread:
//!
//! ```compile_fail
//! use douchdb::gate::{Gate, Table};
//! async fn escape(gate: &Gate) {
//!     let _ = gate
//!         .read(|txn| std::thread::scope(|s| s.spawn(move || txn.len(Table::Meta)).join().unwrap()))
//!         .await;
//! }
//! ```
//!
//! The same reads without the escape compile:
//!
//! ```no_run
//! use douchdb::gate::{Gate, Table};
//! async fn reads(gate: &Gate) {
//!     let _ = gate.read(|txn| txn.len(Table::Meta)).await;
//!     let _ = gate
//!         .read(|txn| {
//!             let mut kept: Option<Vec<u8>> = None;
//!             txn.scan(Table::Meta, .., false, |_, value| {
//!                 kept = Some(value.to_vec());
//!                 Ok(true)
//!             })?;
//!             Ok(kept)
//!         })
//!         .await;
//!     let _ = gate
//!         .read(|txn| {
//!             let len = txn.len(Table::Meta)?;
//!             std::thread::scope(|s| s.spawn(move || len).join().unwrap());
//!             Ok(len)
//!         })
//!         .await;
//! }
//! ```

use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use anyhow::{anyhow, Result};
use redb::{
    Database, Key, ReadTransaction, ReadableDatabase, ReadableTableMetadata, TableDefinition,
    WriteTransaction,
};
use tokio::sync::watch;

pub use crate::kv::Table;
use crate::kv::{
    self, BODIES, BY_SEQ, CHECKPOINTS, LOCAL, META, PURGED_REVS, PURGE_INFOS, PURGE_UUIDS, SETTINGS,
};

/// Keys as [`LeaseTxn`] takes and lends them: one byte form per key type of
/// the tables. A range follows the table's key order (text bytewise,
/// sequences numerically, pairs by first element then second), not the
/// order of these bytes.
pub mod key {
    use anyhow::{anyhow, Result};

    /// A text key (document, `_local`, checkpoint, setting or purge uuid
    /// id): its UTF-8 bytes.
    pub fn text(key: &str) -> Vec<u8> {
        key.as_bytes().to_vec()
    }

    /// A sequence key (`BySeq`, `PurgeInfos`): 8 bytes, big-endian.
    pub fn seq(seq: u64) -> Vec<u8> {
        seq.to_be_bytes().to_vec()
    }

    /// A pair key (`Bodies`, `PurgedRevs`: document id and revision): the
    /// id's length as 4 bytes big-endian, the id, then the revision.
    pub fn pair(id: &str, rev: &str) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + id.len() + rev.len());
        put_pair(id, rev, &mut out);
        out
    }

    pub(super) fn put_pair(id: &str, rev: &str, out: &mut Vec<u8>) {
        let len = u32::try_from(id.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(id.as_bytes());
        out.extend_from_slice(rev.as_bytes());
    }

    /// The text in a text key.
    pub fn split_text(key: &[u8]) -> Result<&str> {
        std::str::from_utf8(key).map_err(|_| anyhow!("a text key that is not UTF-8"))
    }

    /// The sequence in a sequence key.
    pub fn split_seq(key: &[u8]) -> Result<u64> {
        <[u8; 8]>::try_from(key)
            .map(u64::from_be_bytes)
            .map_err(|_| anyhow!("a sequence key of {} bytes", key.len()))
    }

    /// The id and revision in a pair key.
    pub fn split_pair(key: &[u8]) -> Result<(&str, &str)> {
        let malformed = || anyhow!("a malformed pair key");
        let (len, rest) = key.split_first_chunk::<4>().ok_or_else(malformed)?;
        let len = usize::try_from(u32::from_be_bytes(*len)).map_err(|_| malformed())?;
        if len > rest.len() {
            return Err(malformed());
        }
        let (id, rev) = rest.split_at(len);
        Ok((split_text(id)?, split_text(rev)?))
    }
}

/// Where the gate is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Reads take leases.
    Open,
    /// A reopen is under way: new reads wait for it.
    Closed,
    /// A reopen failed or panicked: reads fail until a later reopen
    /// succeeds.
    Failed,
    /// The file is closed for good: reads fail, and the gate never opens
    /// again.
    Shut,
}

struct State {
    /// `None` only while a reopen swaps files, or once shut.
    db: Option<Database>,
    leases: usize,
    /// Reads waiting at the closed gate, for tests that line one up.
    #[cfg(test)]
    waiting: usize,
}

struct Inner {
    state: Mutex<State>,
    /// Signalled when the last lease comes back.
    returned: Condvar,
    /// The phase, for reads waiting at a closed gate.
    phase: watch::Sender<Phase>,
    /// A read met an I/O failure (see [`kv::io_failure`]) since the last
    /// [`Gate::take_io_failure`]. One flag for the whole gate, not one per
    /// lease: whichever caller asks next takes it, also one whose own read
    /// succeeded, and one set by a read nobody asks after (the actor's own,
    /// compaction's, or a lease whose caller stopped waiting) waits for the
    /// next caller. Harmless: the handle that met the error fails the next
    /// read that reaches the disk too, and that sets the flag again.
    io_failed: AtomicBool,
    /// The count of reads waiting, for tests.
    #[cfg(test)]
    waiting: watch::Sender<usize>,
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Wait for every lease to come back, with the gate already closed.
    fn drain(&self) -> MutexGuard<'_, State> {
        let mut state = self.lock();
        while state.leases > 0 {
            state = self
                .returned
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        state
    }
}

/// One shard file's gate. Clones share it.
#[derive(Clone)]
pub struct Gate {
    inner: Arc<Inner>,
}

/// A read's claim on the file: while one is out, the file is not reopened.
/// Returned when dropped, so a read that panics returns it too.
struct Lease {
    inner: Arc<Inner>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self.inner.lock();
        state.leases -= 1;
        if state.leases == 0 {
            self.inner.returned.notify_all();
        }
    }
}

/// A read transaction lent to one [`Gate::read`] call. It never hands out
/// the transaction, a table, a range, an iterator or a guard: its methods
/// return owned bytes or lend them to a callback for one call. Each value is
/// checked against its checksum as it is read; a mismatch is an error, never
/// a miss.
pub struct LeaseTxn<'l> {
    txn: ReadTransaction,
    /// The gate's record of a read that met an I/O failure.
    io_failed: &'l AtomicBool,
    /// Tied to the lease the transaction was opened under.
    _lease: PhantomData<&'l Lease>,
    /// Neither `Send` nor `Sync`: the transaction stays on the thread the
    /// lease runs on.
    _here: PhantomData<*const ()>,
}

/// The value inside a stored one, or the error a checksum mismatch is.
fn unsealed(table: Table, stored: &[u8]) -> Result<&[u8]> {
    kv::unseal(stored).map_err(|corrupt| anyhow!("{table:?} table: {corrupt}"))
}

fn bound<'k, T>(
    bound: Bound<&'k Vec<u8>>,
    decode: impl Fn(&'k [u8]) -> Result<T>,
) -> Result<Bound<T>> {
    Ok(match bound {
        Bound::Included(key) => Bound::Included(decode(key)?),
        Bound::Excluded(key) => Bound::Excluded(decode(key)?),
        Bound::Unbounded => Bound::Unbounded,
    })
}

/// How many entries `table` holds in `txn`: the count redb keeps in the
/// table's header, so no page is read.
pub(crate) fn table_len(txn: &ReadTransaction, table: Table) -> Result<u64> {
    Ok(match table {
        Table::Meta => txn.open_table(META)?.len()?,
        Table::BySeq => txn.open_table(BY_SEQ)?.len()?,
        Table::Bodies => txn.open_table(BODIES)?.len()?,
        Table::Local => txn.open_table(LOCAL)?.len()?,
        Table::PurgeInfos => txn.open_table(PURGE_INFOS)?.len()?,
        Table::PurgedRevs => txn.open_table(PURGED_REVS)?.len()?,
        Table::PurgeUuids => txn.open_table(PURGE_UUIDS)?.len()?,
        Table::Checkpoints => txn.open_table(CHECKPOINTS)?.len()?,
        Table::Settings => txn.open_table(SETTINGS)?.len()?,
    })
}

impl LeaseTxn<'_> {
    /// The value under `key` in `table`, unsealed and checked.
    pub fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        noted(self.io_failed, self.get_any(table, key))
    }

    fn get_any(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match table {
            Table::Meta => self.get_in(META, table, key::split_text(key)?),
            Table::BySeq => self.get_in(BY_SEQ, table, key::split_seq(key)?),
            Table::Bodies => self.get_in(BODIES, table, key::split_pair(key)?),
            Table::Local => self.get_in(LOCAL, table, key::split_text(key)?),
            Table::PurgeInfos => self.get_in(PURGE_INFOS, table, key::split_seq(key)?),
            Table::PurgedRevs => self.get_in(PURGED_REVS, table, key::split_pair(key)?),
            Table::PurgeUuids => self.get_in(PURGE_UUIDS, table, key::split_text(key)?),
            Table::Checkpoints => self.get_in(CHECKPOINTS, table, key::split_text(key)?),
            Table::Settings => self.get_in(SETTINGS, table, key::split_text(key)?),
        }
    }

    /// Visit the entries of `table` in `range`, in key order or reversed,
    /// until `f` returns `false`. The key and the unsealed value are lent to
    /// `f` for that call only.
    pub fn scan(
        &self,
        table: Table,
        range: impl RangeBounds<Vec<u8>>,
        reverse: bool,
        f: impl FnMut(&[u8], &[u8]) -> Result<bool>,
    ) -> Result<()> {
        noted(self.io_failed, self.scan_any(table, range, reverse, f))
    }

    fn scan_any(
        &self,
        table: Table,
        range: impl RangeBounds<Vec<u8>>,
        reverse: bool,
        mut f: impl FnMut(&[u8], &[u8]) -> Result<bool>,
    ) -> Result<()> {
        let (from, to) = (range.start_bound(), range.end_bound());
        let text = |from, to| -> Result<_> {
            Ok((bound(from, key::split_text)?, bound(to, key::split_text)?))
        };
        let seq = |from, to| -> Result<_> {
            Ok((bound(from, key::split_seq)?, bound(to, key::split_seq)?))
        };
        let pair = |from, to| -> Result<_> {
            Ok((bound(from, key::split_pair)?, bound(to, key::split_pair)?))
        };
        let put_text = |id: &str, out: &mut Vec<u8>| out.extend_from_slice(id.as_bytes());
        let put_seq = |seq: u64, out: &mut Vec<u8>| out.extend_from_slice(&seq.to_be_bytes());
        let put_pair = |(id, rev): (&str, &str), out: &mut Vec<u8>| key::put_pair(id, rev, out);
        let f = &mut f;
        match table {
            Table::Meta => self.scan_in(META, table, text(from, to)?, reverse, put_text, f),
            Table::Local => self.scan_in(LOCAL, table, text(from, to)?, reverse, put_text, f),
            Table::PurgeUuids => {
                self.scan_in(PURGE_UUIDS, table, text(from, to)?, reverse, put_text, f)
            }
            Table::Checkpoints => {
                self.scan_in(CHECKPOINTS, table, text(from, to)?, reverse, put_text, f)
            }
            Table::Settings => self.scan_in(SETTINGS, table, text(from, to)?, reverse, put_text, f),
            Table::BySeq => self.scan_in(BY_SEQ, table, seq(from, to)?, reverse, put_seq, f),
            Table::PurgeInfos => {
                self.scan_in(PURGE_INFOS, table, seq(from, to)?, reverse, put_seq, f)
            }
            Table::Bodies => self.scan_in(BODIES, table, pair(from, to)?, reverse, put_pair, f),
            Table::PurgedRevs => {
                self.scan_in(PURGED_REVS, table, pair(from, to)?, reverse, put_pair, f)
            }
        }
    }

    /// How many entries `table` holds.
    pub fn len(&self, table: Table) -> Result<u64> {
        noted(self.io_failed, table_len(&self.txn, table))
    }

    /// A missing table is an error, never an empty answer: every shard file
    /// is created with all of them.
    fn get_in<'k, K: Key + 'static>(
        &self,
        def: TableDefinition<'static, K, &'static [u8]>,
        table: Table,
        key: K::SelfType<'k>,
    ) -> Result<Option<Vec<u8>>> {
        let opened = self.txn.open_table(def)?;
        let found = opened.get(key)?;
        match found {
            Some(stored) => Ok(Some(unsealed(table, stored.value())?.to_vec())),
            None => Ok(None),
        }
    }

    fn scan_in<'k, K: Key + 'static>(
        &self,
        def: TableDefinition<'static, K, &'static [u8]>,
        table: Table,
        range: (Bound<K::SelfType<'k>>, Bound<K::SelfType<'k>>),
        reverse: bool,
        encode: impl for<'v> Fn(K::SelfType<'v>, &mut Vec<u8>),
        f: &mut impl FnMut(&[u8], &[u8]) -> Result<bool>,
    ) -> Result<()> {
        let opened = self.txn.open_table(def)?;
        let entries = opened.range(range)?;
        let entries: Box<dyn Iterator<Item = _>> = if reverse {
            Box::new(entries.rev())
        } else {
            Box::new(entries)
        };
        let mut bytes = Vec::new();
        for entry in entries {
            let (key, stored) = entry?;
            let value = unsealed(table, stored.value())?;
            bytes.clear();
            encode(key.value(), &mut bytes);
            if !f(&bytes, value)? {
                break;
            }
        }
        Ok(())
    }
}

/// `result`, with an I/O failure in it recorded in `io_failed`.
fn noted<T>(io_failed: &AtomicBool, result: Result<T>) -> Result<T> {
    if result.as_ref().is_err_and(kv::io_failure) {
        io_failed.store(true, Ordering::SeqCst);
    }
    result
}

/// What a panic's payload says.
fn panic_reason(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

impl Gate {
    /// A gate, open, over `db`.
    pub fn new(db: Database) -> Gate {
        Gate {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    db: Some(db),
                    leases: 0,
                    #[cfg(test)]
                    waiting: 0,
                }),
                returned: Condvar::new(),
                phase: watch::Sender::new(Phase::Open),
                io_failed: AtomicBool::new(false),
                #[cfg(test)]
                waiting: watch::Sender::new(0),
            }),
        }
    }

    /// Run `f` on a read transaction of its own, on the blocking pool, under
    /// a lease: waits while a reopen is under way, fails once the gate is
    /// shut. The transaction is dropped, then the lease returned, when `f`
    /// is done, even if it panics or the caller stops waiting. A streamed
    /// read calls this once per page, never once per feed.
    pub async fn read<R, F>(&self, f: F) -> Result<R>
    where
        R: Send + 'static,
        F: for<'l> FnOnce(&LeaseTxn<'l>) -> Result<R> + Send + 'static,
    {
        let lease = self.lease().await?;
        let ran = tokio::task::spawn_blocking(move || {
            let lease = lease;
            let txn = {
                let state = lease.inner.lock();
                let db = state
                    .db
                    .as_ref()
                    .ok_or_else(|| anyhow!("the shard file is closed"))?;
                noted(
                    &lease.inner.io_failed,
                    db.begin_read().map_err(anyhow::Error::from),
                )?
            };
            let txn = LeaseTxn {
                txn,
                io_failed: &lease.inner.io_failed,
                _lease: PhantomData::<&Lease>,
                _here: PhantomData,
            };
            // `txn` is dropped before `lease`.
            f(&txn)
        })
        .await;
        match ran {
            Ok(result) => result,
            Err(error) if error.is_panic() => Err(anyhow!(
                "shard read panicked: {}",
                panic_reason(&*error.into_panic())
            )),
            Err(error) => Err(anyhow!("shard read did not run: {error}")),
        }
    }

    /// Take a lease once the gate is open.
    async fn lease(&self) -> Result<Lease> {
        let mut phase = self.inner.phase.subscribe();
        #[cfg(test)]
        let mut counted = false;
        let lease = loop {
            {
                let mut state = self.inner.lock();
                match *phase.borrow_and_update() {
                    Phase::Open => {
                        state.leases += 1;
                        #[cfg(test)]
                        if counted {
                            state.waiting -= 1;
                            self.inner.waiting.send_replace(state.waiting);
                        }
                        break Lease {
                            inner: Arc::clone(&self.inner),
                        };
                    }
                    Phase::Shut | Phase::Failed => anyhow::bail!("the shard file is closed"),
                    Phase::Closed =>
                    {
                        #[cfg(test)]
                        if !counted {
                            counted = true;
                            state.waiting += 1;
                            self.inner.waiting.send_replace(state.waiting);
                        }
                    }
                }
            }
            phase
                .changed()
                .await
                .map_err(|_| anyhow!("the shard file is closed"))?;
        };
        Ok(lease)
    }

    /// Close the gate, wait until every lease is back, drop the database,
    /// open the one `open` returns and open the gate on it. Reads asked for
    /// meanwhile wait, then run on the new database. If `open` fails or
    /// panics, every read fails until a later reopen succeeds (a panic is
    /// returned as an error). A gate shut for good stays shut: the reopen
    /// fails without calling `open`. Blocks: call it from the shard's own thread (or the
    /// blocking pool), holding no transaction.
    pub fn reopen(&self, open: impl FnOnce() -> Result<Database>) -> Result<()> {
        {
            let _state = self.inner.lock();
            if *self.inner.phase.borrow() == Phase::Shut {
                anyhow::bail!("the shard file is closed");
            }
            self.inner.phase.send_replace(Phase::Closed);
        }
        let opened = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let old = self.inner.drain().db.take();
            drop(old);
            open()
        }))
        .unwrap_or_else(|payload| {
            Err(anyhow!(
                "reopening the shard file panicked: {}",
                panic_reason(&*payload)
            ))
        });
        let mut state = self.inner.lock();
        match opened {
            // Shut meanwhile: the new database is dropped, not served.
            Ok(_) if *self.inner.phase.borrow() == Phase::Shut => {
                Err(anyhow!("the shard file is closed"))
            }
            Ok(db) => {
                state.db = Some(db);
                self.inner.phase.send_replace(Phase::Open);
                Ok(())
            }
            Err(error) => {
                if *self.inner.phase.borrow() != Phase::Shut {
                    self.inner.phase.send_replace(Phase::Failed);
                }
                Err(error)
            }
        }
    }

    /// Shut the gate for good and hand back the database once every lease
    /// is back, for the shard to close it. `None` when already shut.
    pub(crate) fn shut(&self) -> Option<Database> {
        self.set_phase(Phase::Shut);
        self.inner.drain().db.take()
    }

    fn set_phase(&self, phase: Phase) {
        // Under the state lock, so a read deciding by the phase sees one
        // consistent with the database it finds.
        let _state = self.inner.lock();
        self.inner.phase.send_replace(phase);
    }

    /// A write transaction, for the shard's actor. Like [`Gate::begin_read`],
    /// it takes no lease: the actor is the only caller of [`Gate::reopen`],
    /// and holds none of its own transactions when it does.
    pub(crate) fn begin_write_txn(&self) -> Result<WriteTransaction> {
        let state = self.inner.lock();
        let db = state
            .db
            .as_ref()
            .ok_or_else(|| anyhow!("the shard file is closed"))?;
        Ok(kv::begin_write(db)?)
    }

    /// A read transaction for the shard's actor, which reads the committed
    /// state alongside its in-memory indexes.
    pub(crate) fn begin_read(&self) -> Result<ReadTransaction> {
        let state = self.inner.lock();
        let db = state
            .db
            .as_ref()
            .ok_or_else(|| anyhow!("the shard file is closed"))?;
        Ok(db.begin_read()?)
    }

    /// Run `f` here, on a read transaction of the shard's actor, which
    /// needs no lease (see [`Gate::begin_read`]): the actor's own reads of
    /// the committed state, such as compaction's final round, go through
    /// the same [`LeaseTxn`] as reads off it.
    pub(crate) fn read_here<R>(&self, f: impl FnOnce(&LeaseTxn<'_>) -> Result<R>) -> Result<R> {
        let txn = LeaseTxn {
            txn: self.begin_read()?,
            io_failed: &self.inner.io_failed,
            _lease: PhantomData,
            _here: PhantomData,
        };
        f(&txn)
    }

    /// Whether the gate is shut for good.
    pub(crate) fn is_shut(&self) -> bool {
        *self.inner.phase.borrow() == Phase::Shut
    }

    /// Whether a read has met an I/O failure since this was last asked;
    /// asking clears it.
    pub(crate) fn take_io_failure(&self) -> bool {
        self.inner.io_failed.swap(false, Ordering::SeqCst)
    }

    /// Wait until the gate is no longer open.
    #[cfg(test)]
    pub(crate) async fn closed(&self) {
        let mut phase = self.inner.phase.subscribe();
        let _ = phase.wait_for(|phase| *phase != Phase::Open).await;
    }

    /// Wait until `count` reads are waiting at the closed gate.
    #[cfg(test)]
    pub(crate) async fn waiting_reads(&self, count: usize) {
        let mut waiting = self.inner.waiting.subscribe();
        let _ = waiting.wait_for(|waiting| *waiting >= count).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    const CACHE: usize = 1 << 20;

    /// A temporary directory, removed when dropped.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(tag: &str) -> Scratch {
            let dir =
                std::env::temp_dir().join(format!("douchdb-gate-{tag}-{}", crate::util::uuid()));
            std::fs::create_dir_all(&dir).unwrap();
            Scratch(dir)
        }

        /// The file `name` in the directory, created empty if it is not
        /// there yet.
        fn file(&self, name: &str) -> PathBuf {
            let path = self.0.join(name);
            if !path.exists() {
                drop(kv::create(&path, CACHE).unwrap());
            }
            path
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            if let Ok(entries) = std::fs::read_dir(&self.0) {
                for entry in entries.flatten() {
                    kv::forget_verified(&entry.path());
                }
            }
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A database at `path` whose META table maps `key` to `value`.
    fn database(path: &Path, key: &str, value: &str) -> Database {
        let db = kv::open(path, CACHE).unwrap();
        let txn = kv::begin_write(&db).unwrap();
        txn.open_table(META)
            .unwrap()
            .insert(key, kv::seal(value.as_bytes()).as_slice())
            .unwrap();
        drop(txn.open_table(SETTINGS).unwrap());
        txn.commit().unwrap();
        db
    }

    async fn value_of(gate: &Gate, key: &str) -> Option<String> {
        let key = key::text(key);
        gate.read(move |txn| {
            Ok(txn
                .get(Table::Meta, &key)?
                .map(|value| String::from_utf8(value).unwrap()))
        })
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reopen_succeeds_after_many_reads() {
        let scratch = Scratch::new("many");
        let path = scratch.file("shard-0000.redb");
        let gate = Gate::new(database(&path, "a", "1"));
        for _ in 0..10_000 {
            assert_eq!(value_of(&gate, "a").await.as_deref(), Some("1"));
        }
        // Any handle a read leaked would keep the file's exclusive lock, and
        // this open of the same file would fail.
        let reopen_path = path.clone();
        let gate2 = gate.clone();
        tokio::task::spawn_blocking(move || gate2.reopen(|| Ok(kv::open(&reopen_path, CACHE)?)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value_of(&gate, "a").await.as_deref(), Some("1"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reopen_waits_for_an_outstanding_lease() {
        let scratch = Scratch::new("outstanding");
        let path = scratch.file("shard-0000.redb");
        let gate = Gate::new(database(&path, "a", "1"));

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let returned = Arc::new(AtomicBool::new(false));
        let held = tokio::spawn({
            let gate = gate.clone();
            let returned = Arc::clone(&returned);
            async move {
                gate.read(move |txn| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    let len = txn.len(Table::Meta)?;
                    returned.store(true, Ordering::SeqCst);
                    Ok(len)
                })
                .await
            }
        });
        entered_rx.await.unwrap();

        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let reopening = std::thread::spawn({
            let gate = gate.clone();
            let returned = Arc::clone(&returned);
            let path = path.clone();
            move || {
                gate.reopen(|| {
                    assert!(
                        returned.load(Ordering::SeqCst),
                        "reopened while a lease was out"
                    );
                    Ok(kv::open(&path, CACHE)?)
                })
                .unwrap();
                done_tx.send(()).unwrap();
            }
        });
        // The reopen has closed the gate and is waiting for the lease.
        gate.closed().await;
        assert!(!returned.load(Ordering::SeqCst));

        release_tx.send(()).unwrap();
        done_rx.await.unwrap();
        reopening.join().unwrap();
        assert_eq!(held.await.unwrap().unwrap(), 1);
        assert_eq!(value_of(&gate, "a").await.as_deref(), Some("1"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reads_requested_during_reopen_run_on_the_new_handle() {
        let scratch = Scratch::new("during");
        let old = scratch.file("shard-0000.redb");
        let new = scratch.file("shard-0001.redb");
        drop(database(&new, "where", "new"));
        let gate = Gate::new(database(&old, "where", "old"));
        assert_eq!(value_of(&gate, "where").await.as_deref(), Some("old"));

        let (opening_tx, opening_rx) = tokio::sync::oneshot::channel::<()>();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let reopening = std::thread::spawn({
            let gate = gate.clone();
            move || {
                gate.reopen(|| {
                    opening_tx.send(()).unwrap();
                    go_rx.recv().unwrap();
                    Ok(kv::open(&new, CACHE)?)
                })
            }
        });
        opening_rx.await.unwrap();
        let read = tokio::spawn({
            let gate = gate.clone();
            async move { value_of(&gate, "where").await }
        });
        // The read is queued at the closed gate before the new file opens.
        gate.waiting_reads(1).await;
        go_tx.send(()).unwrap();
        reopening.join().unwrap().unwrap();
        assert_eq!(read.await.unwrap().as_deref(), Some("new"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reads_run_concurrently_with_a_long_write_group() {
        let scratch = Scratch::new("concurrent");
        let path = scratch.file("shard-0000.redb");
        let gate = Gate::new(database(&path, "a", "1"));

        // A write group in progress: a write transaction with an uncommitted
        // change, held open on another thread until every read is done.
        let (written_tx, written_rx) = tokio::sync::oneshot::channel::<()>();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel::<()>();
        let writer = std::thread::spawn({
            let gate = gate.clone();
            move || {
                let txn = gate.begin_write_txn().unwrap();
                txn.open_table(META)
                    .unwrap()
                    .insert("a", kv::seal(b"2").as_slice())
                    .unwrap();
                written_tx.send(()).unwrap();
                finish_rx.recv().unwrap();
                txn.commit().unwrap();
            }
        });
        written_rx.await.unwrap();

        let reads = (0..8).map(|_| value_of(&gate, "a"));
        for value in futures::future::join_all(reads).await {
            assert_eq!(
                value.as_deref(),
                Some("1"),
                "a read saw an uncommitted write"
            );
        }
        finish_tx.send(()).unwrap();
        writer.join().unwrap();
        assert_eq!(value_of(&gate, "a").await.as_deref(), Some("2"));
    }

    #[tokio::test]
    async fn a_value_failing_its_checksum_is_an_error_not_a_miss() {
        let scratch = Scratch::new("checksum");
        let path = scratch.file("shard-0000.redb");
        let db = database(&path, "good", "1");
        let txn = kv::begin_write(&db).unwrap();
        txn.open_table(META)
            .unwrap()
            .insert("bad", b"not sealed".as_slice())
            .unwrap();
        txn.commit().unwrap();
        let gate = Gate::new(db);

        let bad = key::text("bad");
        let error = gate
            .read(move |txn| txn.get(Table::Meta, &bad))
            .await
            .expect_err("a damaged value read as present or missing");
        assert!(format!("{error:#}").contains("checksum"), "{error:#}");

        let error = gate
            .read(|txn| {
                let mut seen = 0;
                txn.scan(Table::Meta, .., false, |_, _| {
                    seen += 1;
                    Ok(true)
                })?;
                Ok(seen)
            })
            .await
            .expect_err("a scan skipped a damaged value");
        assert!(format!("{error:#}").contains("checksum"), "{error:#}");
        // Damage is not an I/O failure: nothing asks for the file to reopen.
        assert!(!gate.take_io_failure(), "a checksum error noted as I/O");
    }

    /// An I/O error met opening a table (redb's `TableError`, which hides
    /// its cause) is noted as one.
    #[tokio::test]
    async fn an_io_error_opening_a_table_is_noted() {
        let (_, switch) = kv::powercut::PowerCut::new();
        let db = kv::open_with(switch.backend(), CACHE).unwrap();
        let txn = kv::begin_write(&db).unwrap();
        drop(txn.open_table(META).unwrap());
        // Enough tables that finding one reads a page below the root.
        let names: Vec<String> = (0..1000).map(|n| format!("table-{n:04}")).collect();
        for name in &names {
            let def: TableDefinition<&str, &[u8]> = TableDefinition::new(name);
            drop(txn.open_table(def).unwrap());
        }
        txn.commit().unwrap();
        drop(db);
        // No page cache: every read reaches the disk.
        let gate = Gate::new(kv::open_with(switch.backend(), 0).unwrap());

        let failing = switch.clone();
        let error = gate
            .read(move |txn| {
                // The transaction has begun; opening the table reads a page.
                failing.fail_reads(std::io::ErrorKind::Other);
                txn.len(Table::Meta)
            })
            .await
            .expect_err("a read of a failing disk succeeded");
        assert!(
            error.chain().any(|cause| cause.is::<redb::TableError>()),
            "{error:#}"
        );
        assert!(gate.take_io_failure(), "the I/O error was not noted");
    }

    #[tokio::test]
    async fn scans_follow_key_order_both_ways_and_stop_when_asked() {
        let scratch = Scratch::new("scan");
        let path = scratch.file("shard-0000.redb");
        let db = kv::open(&path, CACHE).unwrap();
        let txn = kv::begin_write(&db).unwrap();
        {
            let mut bodies = txn.open_table(kv::BODIES).unwrap();
            for (id, rev) in [("a", "1-x"), ("a", "2-y"), ("ab", "1-z"), ("b", "1-w")] {
                bodies
                    .insert((id, rev), kv::seal(rev.as_bytes()).as_slice())
                    .unwrap();
            }
            let mut by_seq = txn.open_table(kv::BY_SEQ).unwrap();
            for seq in [1u64, 2, 256, 300] {
                by_seq
                    .insert(seq, kv::seal(seq.to_string().as_bytes()).as_slice())
                    .unwrap();
            }
        }
        txn.commit().unwrap();
        let gate = Gate::new(db);

        let pairs = |range: (Bound<Vec<u8>>, Bound<Vec<u8>>), reverse: bool| {
            let gate = gate.clone();
            async move {
                gate.read(move |txn| {
                    let mut out = Vec::new();
                    txn.scan(Table::Bodies, range, reverse, |key, value| {
                        let (id, rev) = key::split_pair(key)?;
                        assert_eq!(value, rev.as_bytes());
                        out.push(format!("{id}/{rev}"));
                        Ok(true)
                    })?;
                    Ok(out)
                })
                .await
                .unwrap()
            }
        };
        // Pairs sort by id, then revision; a range of one id is exact.
        let a = (
            Bound::Included(key::pair("a", "")),
            Bound::Excluded(key::pair("a\0", "")),
        );
        assert_eq!(pairs(a.clone(), false).await, ["a/1-x", "a/2-y"]);
        assert_eq!(pairs(a, true).await, ["a/2-y", "a/1-x"]);
        let all = (Bound::Unbounded, Bound::Unbounded);
        assert_eq!(
            pairs(all, false).await,
            ["a/1-x", "a/2-y", "ab/1-z", "b/1-w"]
        );

        // Sequences sort numerically, and `false` stops the scan.
        let seqs = gate
            .read(|txn| {
                let mut out = Vec::new();
                txn.scan(Table::BySeq, key::seq(2).., false, |key, _| {
                    out.push(key::split_seq(key)?);
                    Ok(out.len() < 2)
                })?;
                Ok((out, txn.len(Table::BySeq)?))
            })
            .await
            .unwrap();
        assert_eq!(seqs, (vec![2, 256], 4));
    }

    #[tokio::test]
    async fn a_malformed_key_is_an_error_not_a_panic() {
        let scratch = Scratch::new("malformed");
        let path = scratch.file("shard-0000.redb");
        let gate = Gate::new(database(&path, "a", "1"));
        for (table, key) in [
            (Table::Meta, vec![0xff, 0xfe]),
            (Table::BySeq, vec![1, 2, 3]),
            (Table::Bodies, vec![0, 0, 0, 9, b'a']),
        ] {
            let result = gate.read(move |txn| txn.get(table, &key)).await;
            assert!(result.is_err(), "{table:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_panicking_open_fails_waiting_reads() {
        let scratch = Scratch::new("panic");
        let path = scratch.file("shard-0000.redb");
        let gate = Gate::new(database(&path, "a", "1"));

        let (opening_tx, opening_rx) = tokio::sync::oneshot::channel::<()>();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let reopening = std::thread::spawn({
            let gate = gate.clone();
            move || {
                gate.reopen(|| {
                    opening_tx.send(()).unwrap();
                    go_rx.recv().unwrap();
                    panic!("open blew up");
                })
            }
        });
        opening_rx.await.unwrap();
        let waiting = tokio::spawn({
            let gate = gate.clone();
            async move { gate.read(|txn| txn.len(Table::Meta)).await }
        });
        gate.waiting_reads(1).await;
        go_tx.send(()).unwrap();

        let error = reopening
            .join()
            .expect("the panic was caught")
            .expect_err("a panicking open reopened the gate");
        assert!(format!("{error:#}").contains("open blew up"), "{error:#}");
        let error = waiting
            .await
            .unwrap()
            .expect_err("a waiting read ran after a panicking open");
        assert!(format!("{error:#}").contains("closed"), "{error:#}");
        let error = gate
            .read(|txn| txn.len(Table::Meta))
            .await
            .expect_err("a read ran after a panicking open");
        assert!(format!("{error:#}").contains("closed"), "{error:#}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_gate_shut_during_a_reopen_stays_shut() {
        let scratch = Scratch::new("shut-during");
        let path = scratch.file("shard-0000.redb");
        let gate = Gate::new(database(&path, "a", "1"));

        let (opening_tx, opening_rx) = tokio::sync::oneshot::channel::<()>();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let reopening = std::thread::spawn({
            let gate = gate.clone();
            move || {
                gate.reopen(|| {
                    opening_tx.send(()).unwrap();
                    go_rx.recv().unwrap();
                    Ok(kv::open(&path, CACHE)?)
                })
            }
        });
        opening_rx.await.unwrap();
        // The old database is already gone; the new one is never served.
        assert!(gate.shut().is_none());
        go_tx.send(()).unwrap();
        reopening
            .join()
            .unwrap()
            .expect_err("a reopen opened a gate shut meanwhile");
        assert!(gate.read(|txn| txn.len(Table::Meta)).await.is_err());
        assert!(gate.shut().is_none());
    }

    #[tokio::test]
    async fn a_shut_gate_is_not_reopened() {
        let scratch = Scratch::new("shut-reopen");
        let path = scratch.file("shard-0000.redb");
        let gate = Gate::new(database(&path, "a", "1"));
        drop(gate.shut().expect("the open database"));

        let called = Arc::new(AtomicBool::new(false));
        let reopening = gate.clone();
        let flag = Arc::clone(&called);
        tokio::task::spawn_blocking(move || {
            reopening.reopen(|| {
                flag.store(true, Ordering::SeqCst);
                Ok(kv::open(&path, CACHE)?)
            })
        })
        .await
        .unwrap()
        .expect_err("a shut gate reopened");
        assert!(
            !called.load(Ordering::SeqCst),
            "opened a file for a shut gate"
        );
        assert!(gate.read(|txn| txn.len(Table::Meta)).await.is_err());
        assert!(gate.shut().is_none());
    }

    #[tokio::test]
    async fn a_gate_whose_reopen_failed_opens_on_the_next_reopen() {
        let scratch = Scratch::new("retry");
        let path = scratch.file("shard-0000.redb");
        let gate = Gate::new(database(&path, "a", "1"));
        let failed = gate.clone();
        tokio::task::spawn_blocking(move || failed.reopen(|| anyhow::bail!("disk on fire")))
            .await
            .unwrap()
            .expect_err("a failed open reopened the gate");
        assert!(gate.read(|txn| txn.len(Table::Meta)).await.is_err());

        let retry = gate.clone();
        tokio::task::spawn_blocking(move || retry.reopen(|| Ok(kv::open(&path, CACHE)?)))
            .await
            .unwrap()
            .expect("the retry did not reopen the gate");
        assert_eq!(value_of(&gate, "a").await.as_deref(), Some("1"));
    }

    #[tokio::test]
    async fn reads_fail_once_the_gate_is_shut_or_a_reopen_failed() {
        let scratch = Scratch::new("shut");
        let path = scratch.file("shard-0000.redb");
        let gate = Gate::new(database(&path, "a", "1"));
        let failed = gate.clone();
        tokio::task::spawn_blocking(move || failed.reopen(|| anyhow::bail!("disk on fire")))
            .await
            .unwrap()
            .expect_err("a failed open reopened the gate");
        assert!(gate.read(|txn| txn.len(Table::Meta)).await.is_err());

        let gate = Gate::new(database(&scratch.file("shard-0001.redb"), "a", "1"));
        let db = gate.shut().expect("the open database");
        drop(db);
        assert!(gate.read(|txn| txn.len(Table::Meta)).await.is_err());
        assert!(gate.shut().is_none());
    }
}

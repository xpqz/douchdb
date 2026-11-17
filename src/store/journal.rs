//! The keys a running compaction must catch up on.
//!
//! While a compaction copies a snapshot of the shard, the actor records,
//! after each commit succeeds and before anyone hears back, every key the
//! transaction inserted, updated or removed, in every table. An aborted
//! transaction records nothing. The compaction takes the keys recorded so
//! far and re-reads each from the live file. A commit that rewrites the
//! shard wholesale (clearing it), or a recovery that reopened the file,
//! invalidates the journal instead: the compaction starts again.

use crate::gate::key;
pub use crate::kv::Table;
use std::collections::BTreeSet;
use std::sync::{Mutex, MutexGuard};

/// A key of one table, in the byte form [`crate::gate::LeaseTxn::get`]
/// takes.
pub type JournalKey = (Table, Vec<u8>);

enum State {
    Stopped,
    Recording(BTreeSet<JournalKey>),
    Invalidated,
}

/// What [`Journal::take`] found.
#[cfg_attr(test, derive(Debug, PartialEq))]
pub enum Taken {
    /// The keys recorded since the last take.
    Batch(BTreeSet<JournalKey>),
    /// The shard was rewritten or reopened since the journal started:
    /// what was copied cannot be caught up, only copied again.
    Invalidated,
    /// No compaction is recording.
    Stopped,
}

pub struct Journal(Mutex<State>);

impl Default for Journal {
    fn default() -> Journal {
        Journal(Mutex::new(State::Stopped))
    }
}

impl Journal {
    fn state(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    /// Record from now on, starting empty.
    pub fn start(&self) {
        *self.state() = State::Recording(BTreeSet::new());
    }

    /// Record the keys a commit changed. Called on the actor after the
    /// commit succeeded and before any of its callers hears back; does
    /// nothing unless recording.
    pub fn record(&self, keys: impl IntoIterator<Item = JournalKey>) {
        if let State::Recording(set) = &mut *self.state() {
            set.extend(keys);
        }
    }

    /// The keys recorded since the last take, swapped out under the lock:
    /// a key recorded concurrently lands in this batch or the next.
    pub fn take(&self) -> Taken {
        match &mut *self.state() {
            State::Recording(set) => Taken::Batch(std::mem::take(set)),
            State::Invalidated => Taken::Invalidated,
            State::Stopped => Taken::Stopped,
        }
    }

    /// The shard was rewritten or reopened: a recording journal can no
    /// longer catch up, and says so until it is started again or stopped.
    /// A stopped one stays stopped.
    pub fn invalidate(&self) {
        let mut state = self.state();
        if !matches!(*state, State::Stopped) {
            *state = State::Invalidated;
        }
    }

    pub fn is_recording(&self) -> bool {
        matches!(*self.state(), State::Recording(_))
    }

    pub fn stop(&self) {
        *self.state() = State::Stopped;
    }
}

/// A table key's byte form for the journal.
pub trait KeyBytes {
    fn key_bytes(&self) -> Vec<u8>;
}

impl KeyBytes for &str {
    fn key_bytes(&self) -> Vec<u8> {
        key::text(self)
    }
}

impl KeyBytes for u64 {
    fn key_bytes(&self) -> Vec<u8> {
        key::seq(*self)
    }
}

impl KeyBytes for (&str, &str) {
    fn key_bytes(&self) -> Vec<u8> {
        key::pair(self.0, self.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(taken: Taken) -> BTreeSet<JournalKey> {
        match taken {
            Taken::Batch(keys) => keys,
            other => panic!("expected a batch, got {other:?}"),
        }
    }

    #[test]
    fn concurrent_records_and_takes_lose_no_key() {
        const KEYS: u64 = 100_000;
        let journal = Journal::default();
        journal.start();
        let done = std::sync::atomic::AtomicBool::new(false);
        let taken = std::thread::scope(|s| {
            s.spawn(|| {
                for n in 0..KEYS {
                    journal.record([(Table::BySeq, key::seq(n))]);
                }
                done.store(true, std::sync::atomic::Ordering::SeqCst);
            });
            let taker = s.spawn(|| {
                let mut union = BTreeSet::new();
                let mut batches = 0;
                loop {
                    // Read before taking: once the recorder is done, this
                    // take is the last one that can find anything.
                    let finished = done.load(std::sync::atomic::Ordering::SeqCst);
                    let keys = batch(journal.take());
                    batches += usize::from(!keys.is_empty());
                    for key in keys {
                        assert!(union.insert(key), "a key was taken twice");
                    }
                    if finished {
                        return (union, batches);
                    }
                }
            });
            taker.join().unwrap()
        });
        let (union, batches) = taken;
        let recorded: BTreeSet<JournalKey> =
            (0..KEYS).map(|n| (Table::BySeq, key::seq(n))).collect();
        assert_eq!(union, recorded);
        assert!(batches >= 1);
        assert_eq!(batch(journal.take()), BTreeSet::new());
    }

    #[test]
    fn invalidate_is_reported_by_the_next_take() {
        let journal = Journal::default();
        assert_eq!(journal.take(), Taken::Stopped);
        // Nothing is recording: there is nothing to invalidate.
        journal.invalidate();
        assert_eq!(journal.take(), Taken::Stopped);

        journal.start();
        journal.record([(Table::Meta, key::text("a"))]);
        journal.invalidate();
        assert!(!journal.is_recording());
        assert_eq!(journal.take(), Taken::Invalidated);
        // Until started again, it stays invalidated and records nothing.
        journal.record([(Table::Meta, key::text("b"))]);
        assert_eq!(journal.take(), Taken::Invalidated);

        journal.start();
        assert!(journal.is_recording());
        journal.record([(Table::Meta, key::text("c"))]);
        assert_eq!(
            batch(journal.take()),
            BTreeSet::from([(Table::Meta, key::text("c"))])
        );
        journal.stop();
        assert_eq!(journal.take(), Taken::Stopped);
    }

    use crate::store::tests::write_doc;
    use crate::store::{PurgeInfo, Shard};
    use std::collections::BTreeMap;

    const TABLES: [Table; 9] = [
        Table::Meta,
        Table::BySeq,
        Table::Bodies,
        Table::Local,
        Table::PurgeInfos,
        Table::PurgedRevs,
        Table::PurgeUuids,
        Table::Checkpoints,
        Table::Settings,
    ];

    fn shard() -> Shard {
        let (_, switch) = crate::kv::powercut::PowerCut::new();
        Shard::create_over_backend(&switch, 1000).unwrap()
    }

    /// Every entry of every table, as a lease reads them.
    async fn dump(shard: &Shard) -> BTreeMap<JournalKey, Vec<u8>> {
        shard
            .gate
            .read(|txn| {
                let mut out = BTreeMap::new();
                for table in TABLES {
                    txn.scan(table, .., false, |key, value| {
                        out.insert((table, key.to_vec()), value.to_vec());
                        Ok(true)
                    })?;
                }
                Ok(out)
            })
            .await
            .unwrap()
    }

    /// The keys whose value differs between `before` and `after`.
    fn changed(
        before: &BTreeMap<JournalKey, Vec<u8>>,
        after: &BTreeMap<JournalKey, Vec<u8>>,
    ) -> BTreeSet<JournalKey> {
        before
            .keys()
            .chain(after.keys())
            .filter(|key| before.get(*key) != after.get(*key))
            .cloned()
            .collect()
    }

    fn purge(n: u64) -> PurgeInfo {
        PurgeInfo {
            uuid: format!("purge-{n}"),
            id: format!("gone-{n}"),
            revs: vec![format!("1-{n:032x}")],
        }
    }

    #[test]
    fn an_aborted_transaction_records_nothing() {
        let mut shard = shard();
        shard.journal.start();
        let failed = shard.write(|txn| {
            write_doc(txn, "a", false, b"{}", 1)?;
            txn.put_local("_local/x", b"{}")?;
            Err::<(), _>(crate::error::ApiError::bad_request("after writing"))
        });
        assert!(failed.is_err());
        assert_eq!(batch(shard.journal.take()), BTreeSet::new());
        assert_eq!(shard.seq(), 0);
    }

    /// Every key a commit changes, in every table and however it was
    /// written (the group's own writes, the settings its commit writes, the
    /// body sweep), is recorded in the form a lease reads it by.
    #[tokio::test]
    async fn every_committed_change_is_recorded_under_the_key_a_lease_reads() {
        let mut shard = shard();
        shard
            .write(|txn| {
                write_doc(txn, "a", false, b"{}", 1)?;
                txn.put_local("_local/gone", b"{}")?;
                txn.put_checkpoint("ae3-gone", b"1")?;
                txn.set_purged_infos_limit(1)?;
                txn.record_purge(&purge(1), &[])?;
                Ok(())
            })
            .unwrap();
        shard.journal.start();
        let before = dump(&shard).await;
        shard
            .write(|txn| {
                write_doc(txn, "a", false, b"{\"v\":2}", 2)?;
                write_doc(txn, "b", true, b"{}", 3)?;
                txn.put_local("_local/x", b"{}")?;
                txn.remove_local("_local/gone")?;
                txn.put_checkpoint("ae3-x", b"2")?;
                txn.remove_checkpoint("ae3-gone")?;
                txn.put_setting("security", b"{}")?;
                txn.set_revs_limit(7)?;
                // Trims the first purge info.
                txn.record_purge(&purge(2), &[])?;
                Ok(())
            })
            .unwrap();
        // Drops the body of the first revision of "a".
        shard.compact().unwrap();
        let after = dump(&shard).await;
        let changed = changed(&before, &after);
        assert!(changed.contains(&(Table::Bodies, key::pair("a", &format!("1-{:032x}", 1)))));
        assert!(changed.contains(&(Table::PurgeInfos, key::seq(1))));
        assert_eq!(batch(shard.journal.take()), changed);
    }

    #[test]
    fn clearing_a_shard_invalidates_the_journal() {
        let mut shard = shard();
        shard.journal.start();
        shard
            .write(|txn| write_doc(txn, "a", false, b"{}", 1))
            .unwrap();
        shard.write(|txn| Ok(txn.clear()?)).unwrap();
        assert_eq!(shard.journal.take(), Taken::Invalidated);
    }
}

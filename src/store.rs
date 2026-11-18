//! Shard storage: one redb file per (database, shard), written only by the
//! shard's actor.
//!
//! The actor runs write jobs in groups. Every job of a group works in the
//! group's one redb write transaction ([`ShardTxn`]), so a later job sees an
//! earlier job's writes, and the group commits once, synced
//! (`Durability::Immediate`, redb's default), before any of its callers hears
//! back. Read jobs that need the in-memory indexes run on the actor between
//! groups, on a read transaction; every other read runs off the actor, on a
//! read transaction of its own leased from the shard's [`Gate`]
//! ([`ShardHandle::lease`]), alongside the groups.
//!
//! The in-memory indexes (each document's latest sequence, the live ids and
//! their ranks) describe the committed state only: a group's changes reach
//! them after its commit has succeeded, so a reader on the actor sees exactly
//! what is on disk.
//!
//! After an I/O error redb fails every later write on that handle. A shard
//! whose commit failed, or whose handle an I/O error left failing, reopens
//! its file through the gate and reloads its state from what is committed
//! ([`Shard::recover`]); if the file cannot be reopened the shard fails
//! closed ([`ShardState::Failed`]) and requests to it retry the reopen,
//! backing off ([`Checkup`]). A read that meets an I/O error has the actor
//! check its handle and recover the same way, so a shard that only serves
//! reads recovers too.

pub mod compact;
pub mod journal;
use journal::KeyBytes;
#[cfg(test)]
mod model_test;

use crate::error::ApiError;
use crate::gate::{self, Gate, LeaseTxn};
use crate::kv::{
    self, Table, BODIES, BY_SEQ, CHECKPOINTS, LOCAL, META, PURGED_REVS, PURGE_INFOS, PURGE_UUIDS,
    SETTINGS,
};
use crate::revtree::RevTree;
use anyhow::Result;
use kv::io_failure;
use redb::{
    Database, Key, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition,
    WriteTransaction,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};

/// Jobs one group commit takes at most.
const GROUP_JOBS: usize = 64;

/// Default for how many purge infos a shard keeps (CouchDB's
/// `purged_infos_limit`); `_purged_infos_limit` changes it per shard.
const DEFAULT_PURGED_INFOS_LIMIT: u64 = 1000;

/// Checkpoint ids starting with this describe this shard copy's own contents
/// (anti-entropy per shard), so clearing the shard drops them.
const SHARD_CHECKPOINT_PREFIX: &str = "ae3";

// Names in the settings table.
/// This copy's identity (see [`ShardTxn::uuid`]).
const UUID: &str = "uuid";
/// The shard's sequence: the last one a committed change took.
const SEQ: &str = "seq";
/// Documents whose winning revision is live.
const DOC_COUNT: &str = "doc_count";
/// Documents whose winning revision is deleted.
const DELETED_COUNT: &str = "doc_del_count";
/// The documents' external sizes ([`DocMeta::external_size`]) summed.
const EXTERNAL_SIZE: &str = "external_size";
const REVS_LIMIT: &str = "revs_limit";
const PURGED_INFOS_LIMIT: &str = "purged_infos_limit";
/// `purge_checkpoint/<node>`: the purge sequence replica `node` has
/// confirmed reading.
const PURGE_CHECKPOINT: &str = "purge_checkpoint/";

fn purge_checkpoint_name(node: usize) -> String {
    format!("{PURGE_CHECKPOINT}{node}")
}

/// One purge request for one document: what CouchDB stores as a purge info
/// (`{PurgeSeq, UUID, DocId, Revs}`) so replicas can learn of the purge.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PurgeInfo {
    pub uuid: String,
    pub id: String,
    pub revs: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DocMeta {
    pub tree: RevTree,
    pub seq: u64,
    /// The length of each leaf revision's stored body, as JSON: what
    /// CouchDB keeps as a leaf's external size. Kept for leaves only (see
    /// [`DocMeta::keep_leaf_sizes`]).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sizes: BTreeMap<String, u64>,
}

impl DocMeta {
    /// The document's external size, as CouchDB counts it: the bodies of
    /// all its leaf revisions, conflicts and tombstones included.
    pub fn external_size(&self) -> u64 {
        self.sizes
            .iter()
            .filter(|(rev, _)| self.tree.is_leaf(rev))
            .map(|(_, size)| *size)
            .sum()
    }

    /// Forget the sizes of revisions that are no longer leaves.
    pub fn keep_leaf_sizes(&mut self) {
        let tree = &self.tree;
        self.sizes.retain(|rev, _| tree.is_leaf(rev));
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LiveDoc {
    pub rev: String,
}

/// One partition's document counts and external size.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct PartitionCounts {
    pub docs: u64,
    pub deleted: u64,
    pub external: u64,
}

/// Counts plus an optional content digest for rebalance verification.
#[derive(Clone, Debug, Default)]
pub struct ShardSummary {
    pub docs: u64,
    pub deleted: u64,
    pub seq: u64,
    pub digest: Option<String>,
}

/// Whether a shard serves requests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShardState {
    Serving,
    /// Its file could not be reopened after a failed commit (see
    /// [`Shard::recover`]): every request to the shard answers 500 with this
    /// reason. A request also has the actor retry the reopen, at most once
    /// per backoff ([`Checkup`]), and a retry that succeeds serves again. The
    /// retry keeps the sequence the shard published, so it finds a file
    /// behind it. Such a file can never come back, nor can a failed
    /// compaction swap: the shard's gate is shut for good and it is never
    /// retried.
    Failed(String),
    /// Its database was deleted: every request answers `not_found`, as
    /// CouchDB answers for a database that is gone.
    Deleted,
}

/// Opens the shard's file: once when the shard starts and again whenever
/// it recovers.
type Opener = Box<dyn FnMut() -> Result<Database> + Send>;

/// How long a failed shard waits before its first retry; each failed retry
/// doubles the wait, up to [`MAX_RETRY_WAIT`].
const FIRST_RETRY_WAIT: Duration = Duration::from_secs(1);
const MAX_RETRY_WAIT: Duration = Duration::from_secs(60);

/// A shard's checks of itself, asked for off the actor ([`Message::Check`]):
/// after a read met an I/O error, or by a request to a failed shard or to
/// one with jobs waiting to run again.
struct Checkup {
    /// A check is queued and not yet handled; at most one is.
    queued: AtomicBool,
    /// Jobs wait for the shard to serve although it serves: their read met
    /// an I/O failure, and they run again when a request finds the wait
    /// over, paced as a failed shard's retries are.
    waiting: AtomicBool,
    retry: std::sync::Mutex<Retry>,
}

/// When a failed shard may retry reopening its file, or jobs waiting to run
/// again may run.
struct Retry {
    /// The wait after the shard first fails: production pacing, which tests
    /// set to zero.
    first: Duration,
    /// The wait after the next failed reopen.
    wait: Duration,
    /// Not before then.
    at: Instant,
}

impl Checkup {
    fn new() -> Checkup {
        Checkup {
            queued: AtomicBool::new(false),
            waiting: AtomicBool::new(false),
            retry: std::sync::Mutex::new(Retry {
                first: FIRST_RETRY_WAIT,
                wait: FIRST_RETRY_WAIT,
                at: Instant::now(),
            }),
        }
    }

    fn retry(&self) -> std::sync::MutexGuard<'_, Retry> {
        self.retry
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Whether a failed shard may retry at `now`.
    fn due(&self, now: Instant) -> bool {
        self.retry().at <= now
    }

    /// A reopen failed at `now`: wait before the next, longer each time.
    fn failed(&self, now: Instant) {
        let mut retry = self.retry();
        retry.at = now + retry.wait;
        retry.wait = (retry.wait * 2).min(MAX_RETRY_WAIT);
    }

    /// A reopen succeeded: the next failure waits the first wait again,
    /// unless jobs wait to run again, whose waits keep growing until they
    /// have run.
    fn served(&self) {
        if self.waiting.load(Ordering::SeqCst) {
            return;
        }
        let mut retry = self.retry();
        retry.wait = retry.first;
    }
}

/// Each document's latest sequence, indexed both ways: by id to update it
/// and to find design documents, and by sequence so a `_changes` read only
/// touches the rows after its cursor instead of every document in the shard.
#[derive(Default)]
pub struct LatestChanges {
    by_id: BTreeMap<String, u64>,
    by_seq: BTreeSet<(u64, String)>,
    /// How many documents have their latest sequence in each bucket of
    /// `1 << SEQ_BUCKET_BITS` sequences, so counting the documents on either
    /// side of a cursor (the `pending` field of `_changes`) does not walk them.
    buckets: Fenwick,
}

const SEQ_BUCKET_BITS: u32 = 8;

/// Where one partition's ids sit among the shard's live ids, kept current on
/// every commit so a partition's count and offsets are not recounted by
/// walking the partition.
struct PartitionRank {
    /// The partition's key range: `[first, last)`, as `name:` to `name;`.
    first: String,
    last: String,
    /// Live ids below `first`.
    below: usize,
    /// Live ids in the partition.
    count: usize,
}

/// Partitions whose ranks a shard remembers.
const PARTITION_RANKS: usize = 16;

impl LatestChanges {
    pub fn insert(&mut self, id: String, seq: u64) {
        if let Some(old) = self.by_id.insert(id.clone(), seq) {
            self.by_seq.remove(&(old, id.clone()));
            self.buckets.add(bucket_of(old), -1);
        }
        self.by_seq.insert((seq, id));
        self.buckets.add(bucket_of(seq), 1);
    }

    /// How many documents have their latest sequence at or below `seq`.
    fn count_through(&self, seq: u64) -> usize {
        let bucket = bucket_of(seq);
        let bucket_start = (bucket as u64) << SEQ_BUCKET_BITS;
        let below = self.buckets.prefix(bucket);
        let within = self
            .by_seq
            .range((bucket_start, String::new())..)
            .take_while(|(entry, _)| *entry <= seq)
            .count();
        below + within
    }

    /// How many documents `after(since)` yields.
    pub fn count_after(&self, since: u64) -> usize {
        self.by_seq.len() - self.count_through(since)
    }

    /// How many documents `before(before)` yields.
    pub fn count_before(&self, before: u64) -> usize {
        before
            .checked_sub(1)
            .map_or(0, |seq| self.count_through(seq))
    }

    /// Documents whose latest sequence is above `since`, in sequence order.
    pub fn after(&self, since: u64) -> impl DoubleEndedIterator<Item = (u64, &str)> {
        let start = since.checked_add(1).map(|seq| (seq, String::new()));
        start
            .into_iter()
            .flat_map(|start| self.by_seq.range(start..))
            .map(|(seq, id)| (*seq, id.as_str()))
    }

    /// Documents whose latest sequence is at or below `seq`, in sequence order.
    pub fn through(&self, seq: u64) -> impl DoubleEndedIterator<Item = (u64, &str)> {
        let end = seq.checked_add(1).map_or(Bound::Unbounded, |next| {
            Bound::Excluded((next, String::new()))
        });
        self.by_seq
            .range((Bound::Unbounded, end))
            .map(|(seq, id)| (*seq, id.as_str()))
    }

    /// Forget a document that no longer has a record.
    pub fn remove(&mut self, id: &str) {
        if let Some(old) = self.by_id.remove(id) {
            self.by_seq.remove(&(old, id.to_string()));
            self.buckets.add(bucket_of(old), -1);
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn seq_of(&self, id: &str) -> Option<u64> {
        self.by_id.get(id).copied()
    }
}

fn bucket_of(seq: u64) -> usize {
    (seq >> SEQ_BUCKET_BITS) as usize
}

/// A Fenwick (binary indexed) tree of counts that grows as higher indexes are
/// used.
#[derive(Default)]
struct Fenwick {
    tree: Vec<usize>,
}

impl Fenwick {
    fn add(&mut self, index: usize, delta: isize) {
        while self.tree.len() <= index {
            // Node `i` (1-based) sums the entries ending at `i`, as many as
            // its lowest set bit; a new node starts with whatever earlier adds
            // put in that range.
            let i = self.tree.len() + 1;
            let span = 1 << i.trailing_zeros();
            let sum = self.prefix(i - 1) - self.prefix(i - span);
            self.tree.push(sum);
        }
        let mut i = index + 1;
        while i <= self.tree.len() {
            self.tree[i - 1] = self.tree[i - 1].wrapping_add_signed(delta);
            i += 1 << i.trailing_zeros();
        }
    }

    /// Sum of the entries before `end`.
    fn prefix(&self, end: usize) -> usize {
        let mut i = end.min(self.tree.len());
        let mut sum = 0;
        while i > 0 {
            sum += self.tree[i - 1];
            i -= 1 << i.trailing_zeros();
        }
        sum
    }
}

/// What a document's metadata says about it: the winning revision, whether it
/// is live, and its external size ([`DocMeta::external_size`]).
pub(crate) type MetaState = (String, bool, u64);

pub(crate) fn meta_state(meta: &DocMeta) -> Option<MetaState> {
    meta.tree.winner().map(|winner| {
        let live = !meta.tree.is_deleted(&winner);
        (winner, live, meta.external_size())
    })
}

/// Which database and shard a shard actor serves, for log lines.
#[derive(Clone, Debug)]
pub struct ShardName {
    db: String,
    shard: String,
}

impl ShardName {
    fn of(file: &Path) -> ShardName {
        let name = |part: Option<&std::ffi::OsStr>| {
            part.map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default()
        };
        ShardName {
            db: name(file.parent().and_then(Path::file_name)),
            shard: name(file.file_stem()),
        }
    }

    /// Log and count a record a scan or listing is about to skip.
    pub fn report_corrupt(&self, id: &str, error: &dyn std::fmt::Display) {
        tracing::error!(
            db = %self.db,
            shard = %self.shard,
            doc_id = %id,
            error = %error,
            "skipping corrupt record"
        );
        crate::telemetry::record_corrupt_record_skipped();
    }

    /// Log and count a corrupt record that compaction leaves in place.
    fn report_compaction_skip(&self, id: &str, error: &dyn std::fmt::Display) {
        tracing::warn!(
            db = %self.db,
            shard = %self.shard,
            doc_id = %id,
            error = %error,
            "compaction is leaving a corrupt record in place; purge the document to repair it"
        );
        crate::telemetry::record_corrupt_record_skipped();
    }
}

#[cfg(test)]
thread_local! {
    static META_PARSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many metadata records this thread has decoded, for tests that pin the
/// cost of a write.
#[cfg(test)]
pub(crate) fn meta_parses() -> usize {
    META_PARSES.with(std::cell::Cell::get)
}

/// Decode a stored metadata record. A record that does not parse is
/// corruption: point reads and writes fail on it, and listings skip it
/// through `ShardName::report_corrupt`.
fn parse_meta(id: &str, value: &[u8]) -> std::result::Result<DocMeta, crate::error::CorruptRecord> {
    #[cfg(test)]
    META_PARSES.with(|count| count.set(count.get() + 1));
    serde_json::from_slice(value).map_err(|error| crate::error::CorruptRecord {
        id: id.to_string(),
        reason: error.to_string(),
    })
}

/// The value inside a stored one. A checksum mismatch is an error that fails
/// the request (never a miss, and never a record a listing skips).
fn unsealed(table: Table, stored: &[u8]) -> Result<&[u8]> {
    kv::unseal(stored).map_err(|corrupt| anyhow::anyhow!("{table:?} table: {corrupt}"))
}

fn decode_u64(table: Table, value: &[u8]) -> Result<u64> {
    <[u8; 8]>::try_from(value)
        .map(u64::from_be_bytes)
        .map_err(|_| anyhow::anyhow!("{table:?} table: a number of {} bytes", value.len()))
}

/// The purge sequence a shard's handle publishes while it cannot know its
/// own: after a commit that may have recorded a purge or cleared the shard
/// failed, until a reopen publishes what the file holds. No shard records
/// this many purges, so no index cursor that holds it is ever current.
pub(crate) const UNKNOWN_PURGE_SEQ: u64 = u64::MAX;

/// The shard's scalar state, as stored in its settings table, and its purge
/// sequence, which the purge infos table holds (the key of its newest info).
#[derive(Clone, Debug, PartialEq)]
struct Settings {
    uuid: String,
    seq: u64,
    docs: u64,
    deleted: u64,
    /// See [`EXTERNAL_SIZE`].
    external: u64,
    revs_limit: u64,
    purged_infos_limit: u64,
    purge_seq: u64,
}

/// Give a new file every table, a uuid, its sequence and counters in one
/// transaction, so a read never finds a table missing and a file with a uuid
/// always has the rest. A file that has them already is left alone.
fn init_tables(db: &Database) -> Result<()> {
    let txn = kv::begin_write(db)?;
    let fresh = {
        drop(txn.open_table(META)?);
        drop(txn.open_table(BY_SEQ)?);
        drop(txn.open_table(BODIES)?);
        drop(txn.open_table(LOCAL)?);
        drop(txn.open_table(PURGE_INFOS)?);
        drop(txn.open_table(PURGED_REVS)?);
        drop(txn.open_table(PURGE_UUIDS)?);
        drop(txn.open_table(CHECKPOINTS)?);
        let mut settings = txn.open_table(SETTINGS)?;
        // Only a new file has no settings at all; one that lost some is
        // damaged, and fails in `load_settings` rather than starting over.
        if settings.first()?.is_none() {
            settings.insert(UUID, kv::seal(crate::util::uuid().as_bytes()).as_slice())?;
            for counter in [SEQ, DOC_COUNT, DELETED_COUNT, EXTERNAL_SIZE] {
                settings.insert(counter, kv::seal(&0u64.to_be_bytes()).as_slice())?;
            }
            true
        } else {
            false
        }
    };
    if fresh {
        txn.commit()?;
    } else {
        txn.abort()?;
    }
    Ok(())
}

/// Create the shard file at `path` with every table and its settings, and
/// return it open. Fails if a file is there already.
pub fn create_file(path: &Path, cache_bytes: usize) -> Result<Database> {
    let db = kv::create(path, cache_bytes)?;
    init_tables(&db)?;
    Ok(db)
}

/// Store `rows` in the settings table of `db` in one transaction, committed
/// durably (redb's default, `Immediate`), for a file no shard has open.
pub fn put_settings(db: &Database, rows: &[(&str, Vec<u8>)]) -> Result<()> {
    let txn = kv::begin_write(db)?;
    {
        let mut settings = txn.open_table(SETTINGS)?;
        for (name, value) in rows {
            settings.insert(*name, kv::seal(value).as_slice())?;
        }
    }
    txn.commit()?;
    Ok(())
}

/// The committed settings whose names start with `prefix`, checked against
/// their checksums.
fn settings_in(txn: &ReadTransaction, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
    let table = txn.open_table(SETTINGS)?;
    let mut rows = Vec::new();
    for entry in table.range(prefix..)? {
        let (name, stored) = entry?;
        let name = name.value();
        if !name.starts_with(prefix) {
            break;
        }
        rows.push((
            name.to_string(),
            unsealed(Table::Settings, stored.value())?.to_vec(),
        ));
    }
    Ok(rows)
}

/// [`settings_in`] the file `db`, for one no shard has open.
pub fn read_settings(db: &dyn ReadableDatabase, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
    settings_in(&db.begin_read()?, prefix)
}

/// Fail where opening the shard file `db` would: [`check_shard`] for a
/// file no shard has open, so a peek finds every damage an open would.
pub fn check_file(db: &dyn ReadableDatabase, file: &Path) -> Result<()> {
    // The default revs limit only fills in an absent setting: any will do.
    check_shard(db, &ShardName::of(file), 1).map(drop)
}

/// What opening a shard reads, and the one check of it: every table there,
/// and the settings it was created with, each matching its checksum and
/// parsing. A file that lost or damaged any of them fails, never taken for
/// a new one and filled in again (that would give it a new uuid and zeroed
/// counters). Both the open and [`check_file`] go through here.
fn check_shard(
    db: &dyn ReadableDatabase,
    name: &ShardName,
    default_revs_limit: u64,
) -> Result<Settings> {
    let txn = db.begin_read()?;
    let missing = |table: &'static str| {
        move |error: redb::TableError| {
            anyhow::anyhow!(
                "corrupt shard {}/{}: no {table} table: {error}",
                name.db,
                name.shard
            )
        }
    };
    drop(txn.open_table(META).map_err(missing("meta"))?);
    drop(txn.open_table(BY_SEQ).map_err(missing("by_seq"))?);
    drop(txn.open_table(BODIES).map_err(missing("bodies"))?);
    drop(txn.open_table(LOCAL).map_err(missing("local"))?);
    drop(
        txn.open_table(PURGE_INFOS)
            .map_err(missing("purge_infos"))?,
    );
    drop(
        txn.open_table(PURGED_REVS)
            .map_err(missing("purged_revs"))?,
    );
    drop(
        txn.open_table(PURGE_UUIDS)
            .map_err(missing("purge_uuids"))?,
    );
    drop(
        txn.open_table(CHECKPOINTS)
            .map_err(missing("checkpoints"))?,
    );
    drop(txn.open_table(SETTINGS).map_err(missing("settings"))?);
    load_settings(&txn, name, default_revs_limit)
}

/// The settings committed in `db`; a shard without a `revs_limit` of its
/// own has `default_revs_limit`.
fn load_settings(
    txn: &ReadTransaction,
    name: &ShardName,
    default_revs_limit: u64,
) -> Result<Settings> {
    let table = txn.open_table(SETTINGS)?;
    let get = |setting: &str| -> Result<Option<Vec<u8>>> {
        match table.get(setting)? {
            Some(stored) => Ok(Some(unsealed(Table::Settings, stored.value())?.to_vec())),
            None => Ok(None),
        }
    };
    let required = |setting: &str| -> Result<Vec<u8>> {
        get(setting)?.ok_or_else(|| {
            anyhow::anyhow!("corrupt shard {}/{}: no {setting}", name.db, name.shard)
        })
    };
    let number = |bytes: Vec<u8>| decode_u64(Table::Settings, &bytes);
    Ok(Settings {
        uuid: String::from_utf8(required(UUID)?)
            .map_err(|_| anyhow::anyhow!("Settings table: the uuid is not UTF-8"))?,
        seq: number(required(SEQ)?)?,
        docs: number(required(DOC_COUNT)?)?,
        deleted: number(required(DELETED_COUNT)?)?,
        external: number(required(EXTERNAL_SIZE)?)?,
        revs_limit: get(REVS_LIMIT)?
            .map(number)
            .transpose()?
            .unwrap_or(default_revs_limit),
        purged_infos_limit: get(PURGED_INFOS_LIMIT)?
            .map(number)
            .transpose()?
            .unwrap_or(DEFAULT_PURGED_INFOS_LIMIT),
        purge_seq: txn
            .open_table(PURGE_INFOS)?
            .last()?
            .map_or(0, |(seq, _)| seq.value()),
    })
}

/// The answer to every request to the shard `name` while it is `state`
/// [`ShardState::Failed`].
fn failed(name: &ShardName, state: &ShardState) -> Option<ApiError> {
    match state {
        ShardState::Serving => None,
        ShardState::Failed(reason) => Some(ApiError::new(
            500,
            "internal_server_error",
            format!("shard {}/{} has failed: {reason}", name.db, name.shard),
        )),
        ShardState::Deleted => Some(ApiError::db_not_found()),
    }
}

/// A document's metadata record, with the state it replaces and the state it
/// leaves.
pub struct MetaWrite {
    pub id: String,
    pub bytes: Vec<u8>,
    pub old: Option<MetaState>,
    pub new: Option<MetaState>,
    pub seq: u64,
}

/// One document's change, for the in-memory indexes once its group commits.
struct DocChange {
    id: String,
    /// The document's state after the change; `None` when its record went.
    new: Option<MetaState>,
    /// The sequence its record carries; `None` when the record went.
    seq: Option<u64>,
}

/// One shard's file and what the actor keeps in memory about it. It lives on
/// the actor thread; jobs reach it through a [`ShardTxn`].
pub struct Shard {
    /// The file, shared with the readers that lease it off the actor.
    gate: Gate,
    /// Opens the file again when the shard recovers.
    open: Opener,
    /// Shared with the shard's handle, which answers for a failed shard
    /// without a round trip.
    state: Arc<std::sync::RwLock<ShardState>>,
    /// Shared with the shard's handle, which asks for checks.
    checkup: Arc<Checkup>,
    /// Read jobs waiting for a retry to bring the failed shard back, or,
    /// on a shard that serves, for their wait to be over before they run
    /// again ([`ShardHandle::when_serving`], [`Shard::wait_again`]);
    /// dropped if the actor stops first.
    parked: std::collections::VecDeque<Box<dyn ShardJob>>,
    /// The file, for the integrity check when the shard closes; `None` for a
    /// test backend.
    path: Option<PathBuf>,
    name: ShardName,
    /// As of the last commit.
    settings: Settings,
    /// The `revs_limit` of a shard that has none set.
    default_revs_limit: u64,
    /// `settings.uuid`, shared with the shard's handle once it has one.
    shared_uuid: Option<Arc<std::sync::RwLock<String>>>,
    /// `settings.purge_seq`, shared with the shard's handle.
    shared_purge_seq: Arc<AtomicU64>,
    /// `settings.external`, shared with the shard's handle.
    shared_external: Arc<AtomicU64>,
    /// Live document id -> winning revision, built lazily on the first
    /// `_all_docs` range query and then maintained in memory.
    live: Option<BTreeMap<String, LiveDoc>>,
    /// The last `live_rank` answer: how many live ids sort before this key.
    /// Paging `_all_docs` by `startkey` asks for nearby keys, so the next rank
    /// is found by counting only the ids between the two keys. Kept in step
    /// with `live` on every commit and dropped whenever `live` is.
    rank_cursor: Option<(String, usize)>,
    /// Index entries this shard has walked to answer reads: change rows read
    /// from the sequence index and ids counted to rank a key. Shared with the
    /// shard's handle, so a request's cost can be judged by how many entries
    /// it visited rather than by how long it took.
    scanned: Arc<AtomicU64>,
    /// Document bodies this shard has read from storage, shared with the
    /// shard's handle like `scanned`.
    body_reads: Arc<AtomicU64>,
    /// Ranks of the partitions `_all_docs` was recently asked about. Dropped
    /// whenever `live` is.
    partition_ranks: Vec<PartitionRank>,
    /// Document id -> latest sequence, built lazily for `_changes`. CouchDB
    /// returns one row per document at its latest change, not one row per
    /// historical sequence entry.
    latest: Option<LatestChanges>,
    /// The keys a running compaction must catch up on, shared with the
    /// shard's handle, which takes them off the actor.
    journal: Arc<journal::Journal>,
    /// Where compaction builds the next file, shared with the handle.
    target: Arc<dyn compact::Target>,
    /// Test seam: make the next group that has something to commit fail
    /// instead of committing.
    #[cfg(test)]
    fail_next_commit: bool,
    /// Test seam: writes to let through before one fails.
    #[cfg(test)]
    fail_write_after: Option<usize>,
    /// Test seam: the row the next compaction drops from its copy.
    #[cfg(test)]
    drop_from_copy: Option<(Table, Vec<u8>)>,
    /// Test seam: how many times a job or a commit has opened a table.
    #[cfg(test)]
    table_opens: AtomicU64,
    /// Test seam: run once the next reopen succeeds.
    #[cfg(test)]
    on_reopen: Option<Box<dyn FnOnce() + Send>>,
}

impl Shard {
    /// Open the existing shard file at `path` (see [`create_file`]).
    pub fn open(path: &Path, revs_limit: u64, cache_bytes: usize) -> Result<Shard> {
        let file = path.to_path_buf();
        Shard::over(
            Box::new(move || Ok(kv::open(&file, cache_bytes)?)),
            Arc::new(compact::FileTarget::new(path, cache_bytes)),
            Some(path.to_path_buf()),
            ShardName::of(path),
            revs_limit,
        )
    }

    /// A shard over the database `open` opens. A new file gets every table,
    /// a uuid, its sequence and counters in one transaction, so a read never
    /// finds a table missing and a file with a uuid always has the rest.
    fn over(
        mut open: Opener,
        target: Arc<dyn compact::Target>,
        path: Option<PathBuf>,
        name: ShardName,
        revs_limit: u64,
    ) -> Result<Shard> {
        let db = open()?;
        let settings = check_shard(&db, &name, revs_limit)?;
        let shared_purge_seq = Arc::new(AtomicU64::new(settings.purge_seq));
        let shared_external = Arc::new(AtomicU64::new(settings.external));
        Ok(Shard {
            gate: Gate::new(db),
            open,
            state: Arc::new(std::sync::RwLock::new(ShardState::Serving)),
            checkup: Arc::new(Checkup::new()),
            parked: Default::default(),
            path,
            name,
            settings,
            default_revs_limit: revs_limit,
            shared_uuid: None,
            shared_purge_seq,
            shared_external,
            live: None,
            rank_cursor: None,
            scanned: Arc::new(AtomicU64::new(0)),
            body_reads: Arc::new(AtomicU64::new(0)),
            partition_ranks: Vec::new(),
            latest: None,
            journal: Default::default(),
            target,
            #[cfg(test)]
            fail_next_commit: false,
            #[cfg(test)]
            fail_write_after: None,
            #[cfg(test)]
            drop_from_copy: None,
            #[cfg(test)]
            table_opens: AtomicU64::new(0),
            #[cfg(test)]
            on_reopen: None,
        })
    }

    /// Recover from a failed commit, or from an I/O error that left the
    /// handle failing every write: reopen the file through the gate (close
    /// it, wait for every lease, drop the handle, open the file again, which
    /// runs redb's recovery and the integrity check), reload the settings
    /// from what is committed, and drop the in-memory indexes, which rebuild
    /// from the committed tables on next use. The gate opens only once the
    /// settings have loaded, so no read sees the file half recovered. If the
    /// file cannot be opened, or it lost a change that was acknowledged
    /// (its sequence is behind the one published), the shard fails closed:
    /// [`ShardState::Failed`], until a retry, which comes through here too,
    /// reopens it. A file behind is never retried.
    ///
    /// Runs on the actor, between groups. Every recovery comes through here,
    /// so whatever must learn that the file was reopened (a running
    /// compaction's journal) is told here.
    fn recover(&mut self, cause: &str) {
        self.journal.invalidate();
        tracing::warn!(
            db = %self.name.db,
            shard = %self.name.shard,
            cause,
            "reopening the shard file"
        );
        self.reopen(|| Ok(()));
    }

    /// Reopen the file through the gate, running `prepare` once every lease
    /// is back and the old handle is closed, and reload the settings (see
    /// [`Shard::recover`]); fail closed if `prepare` fails, or the file
    /// cannot be opened or is behind what was committed. Whether the shard
    /// still serves.
    fn reopen(&mut self, prepare: impl FnOnce() -> Result<()>) -> bool {
        let Shard {
            gate,
            open,
            name,
            settings,
            default_revs_limit,
            ..
        } = self;
        let mut loaded = None;
        let mut behind = false;
        let reopened = gate.reopen(|| {
            prepare()?;
            let db = open()?;
            let found = check_shard(&db, name, *default_revs_limit)?;
            // A commit that reported an error may still have landed, so the
            // file may be ahead of what was published; never behind it.
            behind = found.seq < settings.seq;
            anyhow::ensure!(
                !behind,
                "the file's sequence {} is behind the committed {}",
                found.seq,
                settings.seq
            );
            loaded = Some(found);
            Ok(db)
        });
        self.release_indexes();
        // A file behind what was committed never comes back (no reopen moves
        // it forward): its gate is shut for good and the shard never
        // retries, as after a failed compaction swap.
        if behind {
            drop(self.gate.shut());
        }
        let reopened = reopened.and_then(|()| {
            loaded.ok_or_else(|| anyhow::anyhow!("the shard file reopened without its settings"))
        });
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        match reopened {
            Ok(found) => {
                if found.uuid != self.settings.uuid {
                    if let Some(shared) = &self.shared_uuid {
                        *shared.write().unwrap_or_else(|poison| poison.into_inner()) =
                            found.uuid.clone();
                    }
                }
                self.shared_purge_seq
                    .store(found.purge_seq, Ordering::SeqCst);
                self.shared_external.store(found.external, Ordering::SeqCst);
                self.settings = found;
                self.checkup.served();
                if matches!(*state, ShardState::Failed(_)) {
                    *state = ShardState::Serving;
                    tracing::warn!(
                        db = %self.name.db,
                        shard = %self.name.shard,
                        "the shard file reopened; the failed shard serves again"
                    );
                }
                #[cfg(test)]
                if let Some(hook) = self.on_reopen.take() {
                    hook();
                }
                true
            }
            Err(error) => {
                let reason = format!("{error:#}");
                tracing::error!(
                    db = %self.name.db,
                    shard = %self.name.shard,
                    error = %reason,
                    "the shard file could not be reopened; the shard fails closed"
                );
                // The gate stays closed to reads until a retry reopens it,
                // or for good if the file is behind.
                self.checkup.failed(Instant::now());
                // A deleted database stays deleted.
                if *state != ShardState::Deleted {
                    *state = ShardState::Failed(reason);
                }
                false
            }
        }
    }

    /// Whether the file's handle fails every write after an I/O error.
    fn poisoned(&self) -> bool {
        match self.gate.begin_write_txn() {
            Ok(txn) => {
                drop(txn);
                false
            }
            Err(error) => io_failure(&error),
        }
    }

    /// Whether the shard has failed (not merely been deleted).
    fn is_failed(&self) -> bool {
        matches!(
            *self
                .state
                .read()
                .unwrap_or_else(|poison| poison.into_inner()),
            ShardState::Failed(_)
        )
    }

    /// The answer to every request while the shard has failed.
    fn failure(&self) -> Option<ApiError> {
        failed(
            &self.name,
            &self
                .state
                .read()
                .unwrap_or_else(|poison| poison.into_inner()),
        )
    }

    /// A write transaction for a group; a handle that an I/O error left
    /// failing every write (a read met it) is reopened first.
    fn begin_group(&mut self) -> Result<WriteTransaction> {
        match self.gate.begin_write_txn() {
            Err(error) if io_failure(&error) => {
                self.recover(&format!("{error:#}"));
                self.gate.begin_write_txn()
            }
            begun => begun,
        }
    }

    /// The shard's committed position.
    pub fn seq(&self) -> u64 {
        self.settings.seq
    }

    #[cfg(test)]
    pub(crate) fn journal(&self) -> &journal::Journal {
        &self.journal
    }

    /// Whether the write being made is the one `inject_write_failure` chose.
    #[cfg(test)]
    fn injected_write_failure(&mut self) -> bool {
        match self.fail_write_after {
            Some(0) => {
                self.fail_write_after = None;
                true
            }
            Some(after) => {
                self.fail_write_after = Some(after - 1);
                false
            }
            None => false,
        }
    }

    /// A shard over a new test disk, whose first open makes the file as
    /// `create_file` makes one. Recoveries reopen what the disk then holds.
    #[cfg(test)]
    pub(crate) fn create_over_backend(
        switch: &kv::powercut::PowerSwitch,
        revs_limit: u64,
    ) -> Result<Shard> {
        let disk = switch.clone();
        let mut first = true;
        Shard::over_test_opener(
            Box::new(move || {
                let db = kv::open_with(disk.backend(), 1 << 20)?;
                if std::mem::take(&mut first) {
                    init_tables(&db)?;
                }
                Ok(db)
            }),
            switch,
            revs_limit,
        )
    }

    /// A shard over a test disk that already holds one, reopened over what
    /// the disk then holds, as `open` reopens a file: nothing is created.
    #[cfg(test)]
    pub(crate) fn over_backend(
        switch: &kv::powercut::PowerSwitch,
        revs_limit: u64,
    ) -> Result<Shard> {
        let disk = switch.clone();
        Shard::over_test_opener(
            Box::new(move || Ok(kv::open_with(disk.backend(), 1 << 20)?)),
            switch,
            revs_limit,
        )
    }

    /// A shard over the test databases `open` opens from `switch`'s disk,
    /// which compaction replaces.
    #[cfg(test)]
    fn over_test_opener(
        open: Opener,
        switch: &kv::powercut::PowerSwitch,
        revs_limit: u64,
    ) -> Result<Shard> {
        let name = ShardName {
            db: "test".to_string(),
            shard: "shard-0000".to_string(),
        };
        let target = Arc::new(compact::DiskTarget::new(switch.clone()));
        Shard::over(open, target, None, name, revs_limit)
    }

    /// Run `f` as a group of its own and answer.
    #[cfg(test)]
    pub(crate) fn write<R, F>(&mut self, f: F) -> std::result::Result<R, ApiError>
    where
        R: Send + 'static,
        F: FnOnce(&mut ShardTxn<'_>) -> std::result::Result<R, ApiError> + Send + 'static,
    {
        let (job, mut answer) = call_job(f);
        let (replies, error) = self.run_group(job, || None);
        for reply in replies {
            reply(error.as_ref());
        }
        answer.try_recv().expect("the job answered")
    }

    /// Run `hook` once the next reopen of the file succeeds, before
    /// anything else runs on the reopened file.
    #[cfg(test)]
    pub(crate) fn on_next_reopen(&mut self, hook: impl FnOnce() + Send + 'static) {
        self.on_reopen = Some(Box::new(hook));
    }

    /// Run `f` as a read job and answer.
    #[cfg(test)]
    pub(crate) fn read<R, F>(&mut self, f: F) -> std::result::Result<R, ApiError>
    where
        R: Send + 'static,
        F: FnOnce(&mut ShardTxn<'_>) -> std::result::Result<R, ApiError> + Send + 'static,
    {
        let (job, mut answer) = call_job(f);
        if let Some(reply) = self.run_read(job) {
            reply(None);
        }
        answer.try_recv().expect("the job answered")
    }

    /// Close the file. One opened from a path is closed after a full
    /// integrity check, so its next open in this process can skip one; a
    /// failure is logged and leaves the file to be checked on its next open.
    fn close(self) {
        let Shard {
            gate, path, name, ..
        } = self;
        // Waits for the readers' leases to come back.
        let (Some(db), Some(path)) = (gate.shut(), path) else {
            return;
        };
        if let Err(error) = kv::close_verified(db, &path) {
            tracing::error!(
                db = %name.db,
                shard = %name.shard,
                error = %error,
                "shard file failed its integrity check on close"
            );
        }
    }

    /// Close the file of a deleted database without checking it: it will
    /// never be opened again, so there is nothing to record.
    fn discard(self) {
        drop(self.gate.shut());
    }

    /// Share the handle's counters as this shard's scanned-entries and
    /// body-read counts.
    fn set_counters(&mut self, scanned: Arc<AtomicU64>, body_reads: Arc<AtomicU64>) {
        self.scanned = scanned;
        self.body_reads = body_reads;
    }

    /// Drop the lazily built indexes (live ids, latest sequences, the rank
    /// cursor and the partition ranks). They are rebuilt from the shard's
    /// committed records on next use, so an idle shard holds none of them.
    pub fn release_indexes(&mut self) {
        self.live = None;
        self.rank_cursor = None;
        self.partition_ranks.clear();
        self.latest = None;
    }

    /// Visit every committed metadata record. A record that does not decode
    /// is reported and skipped; a checksum mismatch fails the walk.
    fn committed_metas(&self, mut visit: impl FnMut(&str, DocMeta)) -> Result<()> {
        let txn = self.gate.begin_read()?;
        let table = txn.open_table(META)?;
        for entry in table.iter()? {
            let (id, stored) = entry?;
            let id = id.value();
            match parse_meta(id, unsealed(Table::Meta, stored.value())?) {
                Ok(meta) => visit(id, meta),
                Err(corrupt) => self.name.report_corrupt(&corrupt.id, &corrupt),
            }
        }
        Ok(())
    }

    /// Bring the in-memory indexes up to a committed change.
    fn apply(&mut self, change: DocChange) {
        let DocChange { id, new, seq } = change;
        if let Some(latest) = self.latest.as_mut() {
            match seq {
                Some(seq) => latest.insert(id.clone(), seq),
                None => latest.remove(&id),
            }
        }
        let Some(live) = self.live.as_mut() else {
            return;
        };
        let (was_live, is_live) = match new {
            Some((rev, true, _)) => (live.insert(id.clone(), LiveDoc { rev }).is_some(), true),
            _ => (live.remove(&id).is_some(), false),
        };
        if was_live == is_live {
            return;
        }
        let step = |n: &mut usize| *n = if is_live { *n + 1 } else { n.saturating_sub(1) };
        for known in &mut self.partition_ranks {
            if id < known.first {
                step(&mut known.below);
            } else if id < known.last {
                step(&mut known.count);
            }
        }
        if let Some((from, rank)) = self.rank_cursor.as_mut() {
            if id < *from {
                step(rank);
            }
        }
    }

    /// Run a group: `first`, then whatever `more` hands over, up to
    /// [`GROUP_JOBS`], in one write transaction, committed once. Returns each
    /// job's reply and the group's error, if it failed: a job that panicked
    /// or failed after writing aborts the group, as does a failed commit.
    /// Nothing in memory changes unless the commit succeeds.
    fn run_group(
        &mut self,
        first: Box<dyn ShardJob>,
        mut more: impl FnMut() -> Option<Box<dyn ShardJob>>,
    ) -> (Vec<Reply>, Option<ApiError>) {
        // Beginning the transaction counts as job time, as in the floor.
        let jobs_started = Instant::now();
        let txn = match self.begin_group() {
            Ok(txn) => txn,
            Err(error) => {
                first.fail(ApiError::internal(error));
                return (Vec::new(), None);
            }
        };
        let mut txn = ShardTxn::new(self, Txn::Write(Box::new(txn)));
        let mut replies = Vec::new();
        let mut next = Some(first);
        let mut aborted = None;
        while let Some(job) = next.take() {
            txn.begin_job();
            let (reply, abort) = job.run(&mut txn);
            replies.push(reply);
            if abort.is_some() {
                aborted = abort;
                break;
            }
            if replies.len() < GROUP_JOBS {
                next = more();
            }
        }
        let jobs_us = jobs_started.elapsed().as_micros() as u64;
        let commit_started = Instant::now();
        let mut commit_failed = false;
        let error = match aborted {
            Some(error) => {
                // Dropping the transaction aborts it. A panicking job may
                // have left an index half built.
                drop(txn);
                self.release_indexes();
                Some(error)
            }
            None => txn.commit().err().map(|error| {
                commit_failed = true;
                ApiError::new(
                    500,
                    "internal_server_error",
                    format!("shard commit failed: {error:#}"),
                )
            }),
        };
        crate::metrics::record_shard_group(jobs_us, commit_started.elapsed().as_micros() as u64);
        // Before anyone hears back, so a retry meets the recovered file.
        if let Some(error) = &error {
            if commit_failed || self.poisoned() {
                self.recover(&error.reason);
            }
        }
        (replies, error)
    }

    /// Run a read job on a read transaction of the committed state.
    fn run_read(&mut self, job: Box<dyn ShardJob>) -> Option<Reply> {
        let txn = match self.gate.begin_read() {
            Ok(txn) => txn,
            Err(error) => {
                // Recovered before the job answers, as below.
                let io = io_failure(&error);
                let recovered = io && self.poisoned();
                if recovered {
                    self.recover("an I/O error beginning a read job");
                }
                if io && job.waits() {
                    return self.wait_again(job, recovered);
                }
                job.fail(ApiError::internal(error));
                return None;
            }
        };
        let mut txn = ShardTxn::new(self, Txn::Read(txn));
        txn.begin_job();
        let (reply, abort) = job.run(&mut txn);
        let internal_error = txn.internal_error;
        let again = txn.again.take();
        drop(txn);
        if abort.is_some() {
            self.release_indexes();
        }
        // An I/O error may have left the handle failing every write:
        // recovered before the job answers, as after a failed group.
        let recovered = internal_error && self.poisoned();
        if recovered {
            self.recover("an I/O error in a read job");
        }
        match again {
            Some(job) => self.wait_again(job, recovered),
            None => Some(reply),
        }
    }

    /// A job waiting for the shard to serve ([`WhenServing`]) whose read met
    /// an I/O failure that failed the handle: parked again, ahead of every
    /// job parked after it. If the shard failed, it runs after the next
    /// retry; if the shard was reopened and serves, once the retry wait
    /// that starts now is over, as a failed shard waits. Otherwise (the
    /// handle did not fail) it is done with, unanswered.
    fn wait_again(&mut self, job: Box<dyn ShardJob>, recovered: bool) -> Option<Reply> {
        if self.is_failed() {
            self.parked.push_front(job);
        } else if recovered {
            self.parked.push_front(job);
            self.checkup.failed(Instant::now());
            self.checkup.waiting.store(true, Ordering::SeqCst);
        }
        None
    }

    /// Run the parked jobs, in the order they were parked, until one fails
    /// the shard or has to wait again; those not run wait behind it.
    fn run_parked(&mut self) {
        let mut jobs = std::mem::take(&mut self.parked);
        while let Some(job) = jobs.pop_front() {
            if let Some(reply) = self.run_read(job) {
                reply(None);
            }
            if self.is_failed() || !self.parked.is_empty() {
                break;
            }
        }
        self.parked.extend(jobs);
        let waiting = !self.parked.is_empty();
        self.checkup.waiting.store(waiting, Ordering::SeqCst);
        // Every job has run: a later failure waits the first wait again.
        if !waiting && !self.is_failed() {
            self.checkup.served();
        }
    }

    /// Start a compaction: record every later commit's keys, and create the
    /// empty file the copy goes into.
    fn begin_compaction(&mut self) -> Result<Database> {
        self.journal.start();
        self.target.create()
    }

    /// End a compaction, writes paused: catch the new file up with the
    /// keys the journal still holds, compare its settings and the length of
    /// every table but the bodies with the shard's, close it (it was
    /// checked in full before the catch-up, off the actor), rename it over
    /// the live file, reopen and stop the journal. `false` when the journal
    /// was invalidated (the shard was cleared or reopened since it started):
    /// the compaction must start again. A failure before the swap leaves
    /// the live file as it is. A swap that fails (the rename, the directory
    /// sync that makes it durable, or the reopen) fails the shard closed for
    /// good, with no retry, and nothing is renamed back: a shard never
    /// serves a file whose name may not survive a power cut.
    fn finish_compaction(&mut self, target: Database, hook: &compact::Hook) -> Result<bool> {
        hook(compact::Stage::Swap);
        let journal::Taken::Batch(keys) = self.journal.take() else {
            return Ok(false);
        };
        let mut keys: std::collections::VecDeque<_> = keys.into_iter().collect();
        while !keys.is_empty() {
            let changes = self
                .gate
                .read_here(|txn| compact::catch_up(txn, &self.name, &mut keys))?;
            compact::write(&target, changes)?;
        }
        let built = check_shard(&target, &self.name, self.default_revs_limit)?;
        anyhow::ensure!(
            built == self.settings,
            "the compacted file's settings {built:?} differ from the shard's {:?}",
            self.settings
        );
        #[cfg(test)]
        if let Some((table, key)) = self.drop_from_copy.take() {
            compact::write(&target, vec![compact::Change::Set(table, key, None)])?;
        }
        // redb checks no page checksum on an ordinary read, so a damaged
        // branch page could have hidden part of a table from the copy. Every
        // table must hold as many entries in both files; only bodies drop.
        let copy = target.begin_read()?;
        self.gate.read_here(|live| {
            for table in compact::TABLES {
                if table == Table::Bodies {
                    continue;
                }
                let (had, has) = (live.len(table)?, gate::table_len(&copy, table)?);
                anyhow::ensure!(
                    had == has,
                    "the compacted file's {table:?} table holds {has} entries, the shard's {had}"
                );
            }
            Ok(())
        })?;
        drop(copy);
        self.target.seal(target)?;
        // Nothing commits on the actor between the take and here; checked
        // again all the same, right before the swap.
        if !self.journal.is_recording() {
            return Ok(false);
        }
        hook(compact::Stage::Sealed);
        let target = Arc::clone(&self.target);
        let serving = self.reopen(|| {
            target.install()?;
            hook(compact::Stage::Installed);
            Ok(())
        });
        self.journal.stop();
        if !serving {
            // Never retried: the file now under the name may not keep it
            // through a power cut.
            drop(self.gate.shut());
        }
        anyhow::ensure!(
            serving,
            "the compacted shard file could not be put in place"
        );
        Ok(true)
    }

    /// Compact the shard here, on the actor, with nothing writing meanwhile:
    /// for tests that drive a shard without its actor.
    #[cfg(test)]
    pub fn compact(&mut self) -> Result<()> {
        let target = self.begin_compaction()?;
        let mut cursor = Some(compact::Cursor::default());
        while let Some(at) = cursor.take() {
            let (changes, next, _) = self
                .gate
                .read_here(|txn| compact::copy_page(txn, &self.name, at))?;
            compact::write(&target, changes)?;
            cursor = next;
        }
        let mut target = target;
        self.target.check(&mut target)?;
        let hook: compact::Hook = Arc::new(|_| {});
        anyhow::ensure!(self.finish_compaction(target, &hook)?, "restarted");
        Ok(())
    }
}

/// The transaction a job works in.
enum Txn {
    /// A group's: the job may write.
    Write(Box<WriteTransaction>),
    /// A read job's, of the committed state.
    Read(ReadTransaction),
}

/// Run `$body` with `$table` bound to `$def` opened in the job's
/// transaction, read-only or not.
macro_rules! with_table {
    ($txn:expr, $def:expr, |$table:ident| $body:expr) => {{
        #[cfg(test)]
        $txn.shard.table_opens.fetch_add(1, Ordering::Relaxed);
        match &$txn.txn {
            Txn::Write(txn) => {
                let $table = txn.open_table($def)?;
                let result = $body;
                result
            }
            Txn::Read(txn) => {
                let $table = txn.open_table($def)?;
                let result = $body;
                result
            }
        }
    }};
}

/// A shard as one job sees it: the group's write transaction (or a read
/// job's read transaction) plus the shard's in-memory state.
///
/// A write job reads and decodes every record it will change before its
/// first write. A job that fails after writing, or panics, aborts its whole
/// group; one that fails before writing fails alone.
pub struct ShardTxn<'s> {
    shard: &'s mut Shard,
    txn: Txn,
    /// The group's view of the settings, written with the group's commit.
    staged: Settings,
    /// Document changes for the in-memory indexes, applied after the commit.
    changes: Vec<DocChange>,
    /// The group cleared the shard or removed a corrupt record: the indexes
    /// are dropped after the commit and rebuilt on next use.
    reset_indexes: bool,
    /// The keys the group wrote, for the journal once it commits.
    journaled: Vec<journal::JournalKey>,
    /// The group cleared the shard: its commit invalidates the journal
    /// rather than recording keys.
    cleared: bool,
    /// The settings as the current job found them, restored if it fails
    /// before writing.
    job_start: Settings,
    job_wrote: bool,
    group_wrote: bool,
    /// The first write of the group that failed. Whatever the job that made
    /// it returns, the group fails: it may hold part of that job's writes.
    write_failed: Option<String>,
    /// The last job answered with an internal error (500).
    internal_error: bool,
    /// A job waiting for the shard to serve whose read met an I/O failure,
    /// to run again ([`Shard::wait_again`]).
    again: Option<Box<dyn ShardJob>>,
}

impl<'s> ShardTxn<'s> {
    fn new(shard: &'s mut Shard, txn: Txn) -> ShardTxn<'s> {
        let staged = shard.settings.clone();
        ShardTxn {
            job_start: staged.clone(),
            staged,
            shard,
            txn,
            changes: Vec::new(),
            reset_indexes: false,
            journaled: Vec::new(),
            cleared: false,
            job_wrote: false,
            group_wrote: false,
            write_failed: None,
            internal_error: false,
            again: None,
        }
    }

    fn begin_job(&mut self) {
        self.job_start = self.staged.clone();
        self.job_wrote = false;
    }

    /// Forget what a job that failed before writing did to the settings
    /// (a sequence it took).
    fn undo_job(&mut self) {
        self.staged = self.job_start.clone();
    }

    /// Commit the group, write its settings first, and bring the in-memory
    /// state up to it. A group that changed nothing is not committed.
    fn commit(self) -> Result<()> {
        let ShardTxn {
            shard,
            txn,
            staged,
            changes,
            reset_indexes,
            mut journaled,
            cleared,
            group_wrote,
            ..
        } = self;
        let Txn::Write(txn) = txn else {
            return Ok(());
        };
        if !group_wrote && staged == shard.settings {
            // Dropping it aborts it: no sync for a group of reads.
            return Ok(());
        }
        {
            #[cfg(test)]
            shard.table_opens.fetch_add(1, Ordering::Relaxed);
            let mut settings = txn.open_table(SETTINGS)?;
            if staged.uuid != shard.settings.uuid {
                settings.insert(UUID, kv::seal(staged.uuid.as_bytes()).as_slice())?;
                journaled.push((Table::Settings, UUID.key_bytes()));
            }
            for (name, new, old) in [
                (SEQ, staged.seq, shard.settings.seq),
                (DOC_COUNT, staged.docs, shard.settings.docs),
                (DELETED_COUNT, staged.deleted, shard.settings.deleted),
                (EXTERNAL_SIZE, staged.external, shard.settings.external),
            ] {
                if new != old {
                    settings.insert(name, kv::seal(&new.to_be_bytes()).as_slice())?;
                    journaled.push((Table::Settings, name.key_bytes()));
                }
            }
        }
        #[cfg(test)]
        if std::mem::take(&mut shard.fail_next_commit) {
            anyhow::bail!("injected commit failure");
        }
        if let Err(error) = txn.commit() {
            // A commit that reports an error may have landed, and the reopen
            // that follows may fail too, leaving no way to tell. If it may
            // hold a purge or a clear, the purge sequence is unknown until a
            // reopen publishes what the file holds: an index that read the
            // shard before must rebuild rather than serve what may be gone.
            // The value published meanwhile is one no purge sequence ever
            // reaches, so an index that records it can never take it for
            // current later (the staged value could come back with the next
            // real purge, over rows read before it).
            if staged.purge_seq != shard.settings.purge_seq || staged.uuid != shard.settings.uuid {
                shard
                    .shared_purge_seq
                    .store(UNKNOWN_PURGE_SEQ, Ordering::SeqCst);
            }
            return Err(error.into());
        }
        // Before any caller hears back, so a compaction catching up cannot
        // miss an acknowledged write.
        if cleared {
            shard.journal.invalidate();
        } else {
            shard.journal.record(journaled);
        }
        if staged.uuid != shard.settings.uuid {
            if let Some(shared) = &shard.shared_uuid {
                *shared.write().unwrap_or_else(|poison| poison.into_inner()) = staged.uuid.clone();
            }
        }
        // Before any caller hears back, so a purge that was acknowledged is
        // inside the purge sequence an index checks.
        shard
            .shared_purge_seq
            .store(staged.purge_seq, Ordering::SeqCst);
        shard
            .shared_external
            .store(staged.external, Ordering::SeqCst);
        shard.settings = staged;
        if reset_indexes {
            shard.release_indexes();
        } else {
            for change in changes {
                shard.apply(change);
            }
        }
        Ok(())
    }

    /// The value under `key` in `def`, checked against its checksum.
    fn get_value<'k, K: Key + 'static>(
        &self,
        def: TableDefinition<'static, K, &'static [u8]>,
        table: Table,
        key: K::SelfType<'k>,
    ) -> Result<Option<Vec<u8>>> {
        with_table!(self, def, |t| match t.get(key)? {
            Some(stored) => Ok(Some(unsealed(table, stored.value())?.to_vec())),
            None => Ok(None),
        })
    }

    /// Visit the entries of `def` between `from` and `to`, in key order or
    /// reversed, until `visit` returns `false`.
    fn scan<'k, K: Key + 'static>(
        &self,
        def: TableDefinition<'static, K, &'static [u8]>,
        table: Table,
        (from, to): (Bound<K::SelfType<'k>>, Bound<K::SelfType<'k>>),
        reverse: bool,
        mut visit: impl for<'v> FnMut(K::SelfType<'v>, &'v [u8]) -> Result<bool>,
    ) -> Result<()> {
        with_table!(self, def, |t| {
            let entries = t.range((from, to))?;
            let entries: Box<dyn Iterator<Item = _>> = if reverse {
                Box::new(entries.rev())
            } else {
                Box::new(entries)
            };
            for entry in entries {
                let (key, stored) = entry?;
                if !visit(key.value(), unsealed(table, stored.value())?)? {
                    break;
                }
            }
            Ok(())
        })
    }

    /// Run `body` on `def` opened for writing, marking the job as one that
    /// has written. A read job cannot write. A write that fails is recorded,
    /// so the group fails even if the job carries on as if it had not.
    fn write<K: Key + 'static, R>(
        &mut self,
        def: TableDefinition<'static, K, &'static [u8]>,
        body: impl FnOnce(&mut redb::Table<'_, K, &'static [u8]>) -> Result<R>,
    ) -> Result<R> {
        let Txn::Write(txn) = &self.txn else {
            anyhow::bail!("a read-only shard job tried to write");
        };
        self.job_wrote = true;
        self.group_wrote = true;
        #[cfg(test)]
        let injected = self.shard.injected_write_failure();
        #[cfg(not(test))]
        let injected = false;
        #[cfg(test)]
        self.shard.table_opens.fetch_add(1, Ordering::Relaxed);
        let result = if injected {
            Err(anyhow::anyhow!("injected write failure"))
        } else {
            txn.open_table(def)
                .map_err(anyhow::Error::from)
                .and_then(|mut table| body(&mut table))
        };
        if let Err(error) = &result {
            self.note_failed_write(error);
        }
        result
    }

    fn note_failed_write(&mut self, error: &anyhow::Error) {
        self.write_failed
            .get_or_insert_with(|| format!("{error:#}"));
    }

    fn put_value<'k, K: Key + 'static>(
        &mut self,
        def: TableDefinition<'static, K, &'static [u8]>,
        table: Table,
        key: K::SelfType<'k>,
        value: &[u8],
    ) -> Result<()>
    where
        K::SelfType<'k>: KeyBytes + Clone,
    {
        self.put_values(def, table, std::iter::once((key, value)))
    }

    /// Store each of `entries` in `def`, opened once, journaling every key.
    /// No entries is no write.
    fn put_values<'k, 'v, K: Key + 'static>(
        &mut self,
        def: TableDefinition<'static, K, &'static [u8]>,
        table: Table,
        entries: impl Iterator<Item = (K::SelfType<'k>, &'v [u8])> + Clone,
    ) -> Result<()>
    where
        K::SelfType<'k>: KeyBytes,
    {
        let journaled = self.journaled.len();
        self.journaled
            .extend(entries.clone().map(|(key, _)| (table, key.key_bytes())));
        if self.journaled.len() == journaled {
            return Ok(());
        }
        self.write(def, |t| {
            for (key, value) in entries {
                t.insert(key, kv::seal(value).as_slice())?;
            }
            Ok(())
        })
    }

    fn remove_value<'k, K: Key + 'static>(
        &mut self,
        def: TableDefinition<'static, K, &'static [u8]>,
        table: Table,
        key: K::SelfType<'k>,
    ) -> Result<()>
    where
        K::SelfType<'k>: KeyBytes + Clone,
    {
        self.remove_values(def, table, std::iter::once(key))
    }

    /// Remove each of `keys` from `def`, opened once, journaling every key.
    /// No keys is no write.
    fn remove_values<'k, K: Key + 'static>(
        &mut self,
        def: TableDefinition<'static, K, &'static [u8]>,
        table: Table,
        keys: impl Iterator<Item = K::SelfType<'k>> + Clone,
    ) -> Result<()>
    where
        K::SelfType<'k>: KeyBytes,
    {
        let journaled = self.journaled.len();
        self.journaled
            .extend(keys.clone().map(|key| (table, key.key_bytes())));
        if self.journaled.len() == journaled {
            return Ok(());
        }
        self.write(def, |t| {
            for key in keys {
                t.remove(key)?;
            }
            Ok(())
        })
    }

    /// Take the next sequence of this shard for a write. Sequences are
    /// shard-local, as in CouchDB: each shard counts its own changes. A
    /// sequence is committed with the group that took it, so one taken by a
    /// group that failed is taken again.
    pub fn next_seq(&mut self) -> u64 {
        self.staged.seq += 1;
        self.staged.seq
    }

    /// This copy's identity: created with the file, replaced when the shard
    /// is cleared, so a position is never applied to another copy.
    #[cfg(test)]
    pub fn uuid(&self) -> &str {
        &self.staged.uuid
    }

    /// Live and deleted document counts for this shard.
    pub fn counts(&self) -> (u64, u64) {
        (self.staged.docs, self.staged.deleted)
    }

    /// The documents' external sizes summed ([`DocMeta::external_size`]).
    pub fn external_size(&self) -> u64 {
        self.staged.external
    }

    pub fn revs_limit(&self) -> u64 {
        self.staged.revs_limit
    }

    pub fn purged_infos_limit(&self) -> u64 {
        self.staged.purged_infos_limit
    }

    /// Store `value` under the setting `name`, with this group's commit.
    pub fn put_setting(&mut self, name: &str, value: &[u8]) -> Result<()> {
        self.put_value(SETTINGS, Table::Settings, name, value)
    }

    /// Remove the setting `name`, with this group's commit.
    pub fn remove_setting(&mut self, name: &str) -> Result<()> {
        self.remove_value(SETTINGS, Table::Settings, name)
    }

    /// The setting `name`, if there is one.
    pub fn get_setting(&self, name: &str) -> Result<Option<Vec<u8>>> {
        self.get_value(SETTINGS, Table::Settings, name)
    }

    pub fn set_revs_limit(&mut self, limit: u64) -> Result<()> {
        let limit = limit.max(1);
        self.put_value(SETTINGS, Table::Settings, REVS_LIMIT, &limit.to_be_bytes())?;
        self.staged.revs_limit = limit;
        Ok(())
    }

    pub fn set_purged_infos_limit(&mut self, limit: u64) -> Result<()> {
        let limit = limit.max(1);
        self.put_value(
            SETTINGS,
            Table::Settings,
            PURGED_INFOS_LIMIT,
            &limit.to_be_bytes(),
        )?;
        self.staged.purged_infos_limit = limit;
        Ok(())
    }

    /// Where this shard lives, for logging skipped records.
    pub fn name(&self) -> &ShardName {
        &self.shard.name
    }

    /// Record `entries` index entries walked by a read.
    pub fn add_scanned(&self, entries: usize) {
        self.shard
            .scanned
            .fetch_add(entries as u64, Ordering::Relaxed);
    }

    pub fn get_meta(&self, docid: &str) -> Result<Option<DocMeta>> {
        match self.get_value(META, Table::Meta, docid)? {
            Some(bytes) => Ok(Some(parse_meta(docid, &bytes)?)),
            None => Ok(None),
        }
    }

    /// The metadata of each of `ids`, in order, read with one open of the
    /// table. A record that cannot be read or decoded fails the whole read.
    pub fn get_metas(&self, ids: &[&str]) -> Result<Vec<Option<DocMeta>>> {
        with_table!(self, META, |t| ids
            .iter()
            .map(|id| match t.get(*id)? {
                Some(stored) => Ok(Some(parse_meta(
                    id,
                    unsealed(Table::Meta, stored.value())?,
                )?)),
                None => Ok(None),
            })
            .collect())
    }

    pub fn get_body(&self, docid: &str, rev: &str) -> Result<Option<Vec<u8>>> {
        self.shard.body_reads.fetch_add(1, Ordering::Relaxed);
        self.get_value(BODIES, Table::Bodies, (docid, rev))
    }

    /// Write a document's metadata record, saying what the document's state
    /// was before (`old`, which must be the state of the record this
    /// transaction holds now) and is after (`new`), and the sequence it
    /// carries. The counters follow, and the in-memory indexes after commit.
    pub fn put_meta(
        &mut self,
        docid: &str,
        bytes: &[u8],
        old: Option<MetaState>,
        new: Option<MetaState>,
        seq: u64,
    ) -> Result<()> {
        self.put_value(META, Table::Meta, docid, bytes)?;
        self.count(old.as_ref(), new.as_ref());
        self.changes.push(DocChange {
            id: docid.to_string(),
            new,
            seq: Some(seq),
        });
        Ok(())
    }

    /// Write each of `metas`, as [`ShardTxn::put_meta`] does, with one open
    /// of the table.
    pub fn put_metas(&mut self, metas: Vec<MetaWrite>) -> Result<()> {
        let entries = metas.iter().map(|m| (m.id.as_str(), m.bytes.as_slice()));
        self.put_values(META, Table::Meta, entries)?;
        self.changes.reserve(metas.len());
        for meta in metas {
            self.count(meta.old.as_ref(), meta.new.as_ref());
            self.changes.push(DocChange {
                id: meta.id,
                new: meta.new,
                seq: Some(meta.seq),
            });
        }
        Ok(())
    }

    /// Remove a document's metadata record, whose state was `old`.
    pub fn remove_meta(&mut self, docid: &str, old: Option<MetaState>) -> Result<()> {
        self.remove_value(META, Table::Meta, docid)?;
        self.count(old.as_ref(), None);
        self.changes.push(DocChange {
            id: docid.to_string(),
            new: None,
            seq: None,
        });
        Ok(())
    }

    /// Move the counters from a document's old state to its new one.
    fn count(&mut self, old: Option<&MetaState>, new: Option<&MetaState>) {
        let staged = &mut self.staged;
        if let Some((_, live, size)) = old {
            if *live {
                staged.docs = staged.docs.saturating_sub(1);
            } else {
                staged.deleted = staged.deleted.saturating_sub(1);
            }
            staged.external = staged.external.saturating_sub(*size);
        }
        if let Some((_, live, size)) = new {
            if *live {
                staged.docs += 1;
            } else {
                staged.deleted += 1;
            }
            staged.external += size;
        }
    }

    /// Store each `(docid, rev, body)` with one open of the table.
    pub fn put_bodies(&mut self, bodies: &[(&str, &str, &[u8])]) -> Result<()> {
        let entries = bodies.iter().map(|&(id, rev, body)| ((id, rev), body));
        self.put_values(BODIES, Table::Bodies, entries)
    }

    pub fn remove_body(&mut self, docid: &str, rev: &str) -> Result<()> {
        self.remove_value(BODIES, Table::Bodies, (docid, rev))
    }

    /// Record that `docid` changed at `seq`.
    pub fn put_seq(&mut self, seq: u64, docid: &str) -> Result<()> {
        self.put_value(BY_SEQ, Table::BySeq, seq, docid.as_bytes())
    }

    /// Record each `(seq, docid)` change with one open of the table.
    pub fn put_seqs(&mut self, seqs: &[(u64, &str)]) -> Result<()> {
        let entries = seqs.iter().map(|&(seq, id)| (seq, id.as_bytes()));
        self.put_values(BY_SEQ, Table::BySeq, entries)
    }

    pub fn remove_seq(&mut self, seq: u64) -> Result<()> {
        self.remove_value(BY_SEQ, Table::BySeq, seq)
    }

    pub fn remove_seqs(&mut self, seqs: &[u64]) -> Result<()> {
        self.remove_values(BY_SEQ, Table::BySeq, seqs.iter().copied())
    }

    pub fn get_local(&self, id: &str) -> Result<Option<Vec<u8>>> {
        self.get_value(LOCAL, Table::Local, id)
    }

    pub fn put_local(&mut self, id: &str, doc: &[u8]) -> Result<()> {
        self.put_value(LOCAL, Table::Local, id, doc)
    }

    pub fn remove_local(&mut self, id: &str) -> Result<()> {
        self.remove_value(LOCAL, Table::Local, id)
    }

    /// Every local document of this shard, in id order.
    pub fn locals(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let mut out = Vec::new();
        self.scan(
            LOCAL,
            Table::Local,
            (Unbounded, Unbounded),
            false,
            |id, doc| {
                out.push((id.to_string(), doc.to_vec()));
                Ok(true)
            },
        )?;
        Ok(out)
    }

    /// A node-local anti-entropy checkpoint. Not a `_local` document: it is
    /// never listed, shared or moved with a shard.
    pub fn get_checkpoint(&self, id: &str) -> Result<Option<Vec<u8>>> {
        self.get_value(CHECKPOINTS, Table::Checkpoints, id)
    }

    pub fn put_checkpoint(&mut self, id: &str, value: &[u8]) -> Result<()> {
        self.put_value(CHECKPOINTS, Table::Checkpoints, id, value)
    }

    pub fn remove_checkpoint(&mut self, id: &str) -> Result<()> {
        self.remove_value(CHECKPOINTS, Table::Checkpoints, id)
    }

    /// The checkpoints whose ids start with `prefix`, in id order.
    pub fn checkpoints(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let mut out = Vec::new();
        let from = (Included(prefix), Unbounded);
        self.scan(CHECKPOINTS, Table::Checkpoints, from, false, |id, value| {
            if !id.starts_with(prefix) {
                return Ok(false);
            }
            out.push((id.to_string(), value.to_vec()));
            Ok(true)
        })?;
        Ok(out)
    }

    /// Content digest over document ids, winning revisions, deleted flags
    /// and conflict leaves. It deliberately excludes per-replica sequence
    /// numbers, which differ between replicas. Two replicas with identical
    /// replicated content produce the same digest.
    pub fn fingerprint(&self) -> Result<String> {
        let mut hasher = Sha256::new();
        let mut documents = 0u64;
        self.scan(
            META,
            Table::Meta,
            (Unbounded, Unbounded),
            false,
            |id, stored| {
                let meta = parse_meta(id, stored)?;
                hasher.update(b"doc");
                hasher.update(id.as_bytes());
                match meta_state(&meta) {
                    Some((winner, live, _)) => {
                        hasher.update(winner.as_bytes());
                        hasher.update([live as u8]);
                    }
                    None => hasher.update(b"no-winner"),
                }
                let mut leaves = meta.tree.leaves();
                leaves.sort();
                for leaf in leaves {
                    hasher.update(b"leaf");
                    hasher.update(leaf.as_bytes());
                    hasher.update([meta.tree.is_deleted(&leaf) as u8]);
                }
                documents += 1;
                Ok(true)
            },
        )?;
        hasher.update(documents.to_be_bytes());
        Ok(crate::util::hex(&hasher.finalize()))
    }

    pub fn summary(&self, with_digest: bool) -> Result<ShardSummary> {
        let (docs, deleted) = self.counts();
        Ok(ShardSummary {
            docs,
            deleted,
            seq: self.staged.seq,
            digest: if with_digest {
                Some(self.fingerprint()?)
            } else {
                None
            },
        })
    }

    /// Visit the metadata records whose ids lie in `[first, last)`, skipping
    /// (and reporting) any that do not decode.
    fn metas_in(
        &self,
        first: &str,
        last: &str,
        mut visit: impl FnMut(&str, DocMeta),
    ) -> Result<()> {
        let name = self.shard.name.clone();
        let range = (Included(first), Excluded(last));
        self.scan(META, Table::Meta, range, false, |id, stored| {
            match parse_meta(id, stored) {
                Ok(meta) => visit(id, meta),
                Err(corrupt) => name.report_corrupt(&corrupt.id, &corrupt),
            }
            Ok(true)
        })
    }

    /// Live and deleted document counts for one partition, and its
    /// documents' external sizes summed. The scan stays inside the
    /// partition's id range.
    pub fn partition_counts(&self, partition: &str) -> Result<PartitionCounts> {
        let mut counts = PartitionCounts::default();
        let (first, last) = (format!("{partition}:"), format!("{partition};"));
        self.metas_in(&first, &last, |_, meta| {
            if let Some((_, live, size)) = meta_state(&meta) {
                if live {
                    counts.docs += 1;
                } else {
                    counts.deleted += 1;
                }
                counts.external += size;
            }
        })?;
        Ok(counts)
    }

    /// The ids of the design documents (deleted ones included), and those
    /// among them whose metadata is corrupt. A corrupt one's
    /// `validate_doc_update` cannot be loaded, so callers must fail instead of
    /// skipping it. A range scan of the `_design/` ids: it neither builds nor
    /// needs the id-to-sequence index.
    pub fn design_docs(&self) -> Result<(Vec<String>, Vec<String>)> {
        let name = self.shard.name.clone();
        let mut ids = Vec::new();
        let mut corrupt_design = Vec::new();
        // '0' follows '/', so this is every id starting with "_design/".
        let range = (Included("_design/"), Excluded("_design0"));
        self.scan(META, Table::Meta, range, false, |id, stored| {
            match parse_meta(id, stored) {
                Ok(_) => ids.push(id.to_string()),
                Err(corrupt) => {
                    name.report_corrupt(&corrupt.id, &corrupt);
                    corrupt_design.push(corrupt.id);
                }
            }
            Ok(true)
        })?;
        Ok((ids, corrupt_design))
    }

    /// Sorted live document ids and their winning revisions, as committed.
    /// The index is built on first use and maintained after each commit.
    pub fn live_docs(&mut self) -> Result<&BTreeMap<String, LiveDoc>> {
        if self.shard.live.is_none() {
            let mut live = BTreeMap::new();
            self.shard.committed_metas(|id, meta| {
                if let Some((rev, true, _)) = meta_state(&meta) {
                    live.insert(id.to_string(), LiveDoc { rev });
                }
            })?;
            self.shard.live = Some(live);
            self.shard.rank_cursor = None;
            self.shard.partition_ranks.clear();
        }
        Ok(self.shard.live.get_or_insert_with(BTreeMap::new))
    }

    /// How many live documents have an id in `[first, last)`.
    pub fn live_range_count(&mut self, first: &str, last: &str) -> Result<usize> {
        let count = self
            .live_docs()?
            .range::<str, _>((Bound::Included(first), Bound::Excluded(last)))
            .count();
        self.add_scanned(count);
        Ok(count)
    }

    /// How many live documents have an id below `key`. Counts from the
    /// previous call's key when there is one, so walking through the keys in
    /// either direction costs the distance moved, not the position.
    pub fn live_rank(&mut self, key: &str) -> Result<usize> {
        use std::ops::Bound::{Excluded, Unbounded};
        self.live_docs()?;
        let shard = &mut *self.shard;
        let (Some(live), cursor) = (shard.live.as_ref(), shard.rank_cursor.take()) else {
            return Ok(0);
        };
        let (rank, walked) = match &cursor {
            Some((from, rank)) if key == from.as_str() => (*rank, 0),
            Some((from, rank)) if key > from.as_str() => {
                let walked = live
                    .range::<str, _>((Excluded(from.as_str()), Excluded(key)))
                    .count();
                (
                    rank + walked + usize::from(live.contains_key(from.as_str())),
                    walked,
                )
            }
            Some((from, rank)) => {
                let walked = live
                    .range::<str, _>((Excluded(key), Excluded(from.as_str())))
                    .count();
                (
                    rank.saturating_sub(walked + usize::from(live.contains_key(key))),
                    walked,
                )
            }
            None => {
                let walked = live.range::<str, _>((Unbounded, Excluded(key))).count();
                (walked, walked)
            }
        };
        shard.scanned.fetch_add(walked as u64, Ordering::Relaxed);
        shard.rank_cursor = Some((key.to_string(), rank));
        Ok(rank)
    }

    /// Live document count for one partition.
    pub fn partition_live_count(&mut self, partition: &str) -> Result<u64> {
        Ok(self.partition_rank(partition)?.1 as u64)
    }

    /// How many live ids of `partition` come before `start` in the query's
    /// direction: the offset of a partitioned `_all_docs` reply. `start` is
    /// ranked from the previous rank, so paging by `startkey` costs the
    /// distance moved, not the partition's size.
    pub fn partition_offset(
        &mut self,
        partition: &str,
        start: &str,
        descending: bool,
    ) -> Result<usize> {
        let (below, count) = self.partition_rank(partition)?;
        let rank = self.live_rank(start)?;
        let on_start = usize::from(
            self.shard
                .live
                .as_ref()
                .is_some_and(|live| live.contains_key(start)),
        );
        Ok(if descending {
            // Ids after `start`: all but those up to and including it.
            let through = (rank + on_start).saturating_sub(below).min(count);
            count - through
        } else {
            rank.saturating_sub(below).min(count)
        })
    }

    /// `(live ids below the partition, live ids in it)`, found once by
    /// walking the partition and then kept current by commits.
    fn partition_rank(&mut self, partition: &str) -> Result<(usize, usize)> {
        let first = format!("{partition}:");
        if let Some(known) = self
            .shard
            .partition_ranks
            .iter()
            .find(|known| known.first == first)
        {
            return Ok((known.below, known.count));
        }
        let last = format!("{partition};");
        let below = self.live_rank(&first)?;
        let count = self
            .live_docs()?
            .range::<str, _>((
                Bound::Included(first.as_str()),
                Bound::Excluded(last.as_str()),
            ))
            .count();
        self.add_scanned(count);
        let ranks = &mut self.shard.partition_ranks;
        if ranks.len() >= PARTITION_RANKS {
            ranks.remove(0);
        }
        ranks.push(PartitionRank {
            first,
            last,
            below,
            count,
        });
        Ok((below, count))
    }

    /// Each document's latest committed sequence, built lazily from
    /// metadata. Used by `_changes`, which returns one row per document.
    pub fn latest_changes(&mut self) -> Result<&LatestChanges> {
        if self.shard.latest.is_none() {
            let mut latest = LatestChanges::default();
            self.shard
                .committed_metas(|id, meta| latest.insert(id.to_string(), meta.seq))?;
            self.shard.latest = Some(latest);
        }
        Ok(self.shard.latest.get_or_insert_with(LatestChanges::default))
    }

    pub fn pending_changes(&mut self, after: u64, descending: bool) -> Result<usize> {
        let latest = self.latest_changes()?;
        Ok(if descending {
            latest.count_before(after)
        } else {
            latest.count_after(after)
        })
    }

    /// See [`Shard::release_indexes`].
    pub fn release_indexes(&mut self) {
        self.shard.release_indexes();
    }

    #[cfg(test)]
    pub fn latest_built(&self) -> bool {
        self.shard.latest.is_some()
    }

    /// Entries held by the live and latest-changes indexes, for judging their
    /// memory by count.
    #[cfg(test)]
    pub fn index_entries(&self) -> usize {
        self.shard.live.as_ref().map_or(0, BTreeMap::len)
            + self.shard.latest.as_ref().map_or(0, LatestChanges::len)
    }

    /// The `_local` document `id` as last committed, whatever this
    /// transaction has written since.
    #[cfg(test)]
    pub(crate) fn committed_local(&self, id: &str) -> Option<Vec<u8>> {
        let txn = self.shard.gate.begin_read().unwrap();
        let table = txn.open_table(LOCAL).unwrap();
        let stored = table.get(id).unwrap()?;
        Some(kv::unseal(stored.value()).unwrap().to_vec())
    }

    /// Every row of the by_seq table, in sequence order.
    #[cfg(test)]
    pub(crate) fn by_seq(&self) -> Result<Vec<(u64, String)>> {
        let mut out = Vec::new();
        self.scan(
            BY_SEQ,
            Table::BySeq,
            (Unbounded, Unbounded),
            false,
            |seq, id| {
                out.push((seq, String::from_utf8(id.to_vec())?));
                Ok(true)
            },
        )?;
        Ok(out)
    }

    /// How many tables this shard's jobs and commits have opened.
    #[cfg(test)]
    pub(crate) fn table_opens(&self) -> u64 {
        self.shard.table_opens.load(Ordering::Relaxed)
    }

    /// Make the group's commit fail, as a write error would.
    #[cfg(test)]
    pub(crate) fn inject_commit_failure(&mut self) {
        self.shard.fail_next_commit = true;
    }

    /// Make the write after the next `after` writes fail, as an I/O error
    /// would.
    #[cfg(test)]
    pub(crate) fn inject_write_failure(&mut self, after: usize) {
        self.shard.fail_write_after = Some(after);
    }

    /// Store `value` under `key` in `def` exactly as given (sealed, so its
    /// checksum holds), bypassing the counters and indexes, so tests can
    /// plant records that do not decode. Journaled like any write.
    #[cfg(test)]
    pub(crate) fn put_raw<'k, K: Key + 'static>(
        &mut self,
        def: TableDefinition<'static, K, &'static [u8]>,
        table: Table,
        key: K::SelfType<'k>,
        value: &[u8],
    ) where
        K::SelfType<'k>: KeyBytes + Clone,
    {
        self.put_value(def, table, key, value).unwrap();
    }

    /// Store `value` under `key` in `def` without its seal, as damage on
    /// disk would leave it: reading it fails its checksum.
    #[cfg(test)]
    pub(crate) fn put_unsealed<'k, K: Key + 'static>(
        &mut self,
        def: TableDefinition<'static, K, &'static [u8]>,
        key: K::SelfType<'k>,
        value: &[u8],
    ) {
        let Txn::Write(raw) = &self.txn else {
            unreachable!("a write job runs on a write transaction")
        };
        raw.open_table(def).unwrap().insert(key, value).unwrap();
    }

    /// Sequence of the newest purge info of this shard: how many have been
    /// recorded. Trimming never removes the newest one.
    pub fn purge_seq(&self) -> Result<u64> {
        with_table!(self, PURGE_INFOS, |t| Ok(t
            .last()?
            .map_or(0, |(seq, _)| seq.value())))
    }

    /// The purge sequence replica `node` last confirmed reading from this
    /// shard; 0 when it never reported.
    fn purge_checkpoint(&self, node: usize) -> Result<u64> {
        self.get_value(SETTINGS, Table::Settings, &purge_checkpoint_name(node))?
            .map_or(Ok(0), |bytes| decode_u64(Table::Settings, &bytes))
    }

    /// Remember that replica `node` has confirmed purge infos up to `seq`.
    pub fn report_purge_checkpoint(&mut self, node: usize, seq: u64) -> Result<()> {
        if self.purge_checkpoint(node)? == seq {
            return Ok(());
        }
        self.put_value(
            SETTINGS,
            Table::Settings,
            &purge_checkpoint_name(node),
            &seq.to_be_bytes(),
        )
    }

    /// The purge infos at sequences `from..=through`, in order.
    fn purge_infos(&self, from: u64, through: u64) -> Result<Vec<(u64, PurgeInfo)>> {
        let mut infos = Vec::new();
        let range = (Included(from), Included(through));
        self.scan(
            PURGE_INFOS,
            Table::PurgeInfos,
            range,
            false,
            |seq, value| {
                let info = serde_json::from_slice(value)
                    .map_err(|error| anyhow::anyhow!("an unreadable purge info: {error}"))?;
                infos.push((seq, info));
                Ok(true)
            },
        )?;
        Ok(infos)
    }

    /// Record `info` under the next purge sequence, and drop the infos that
    /// are past the shard's `purged_infos_limit` and that every replica in
    /// `peers` has confirmed (CouchDB never trims past the lowest purge
    /// checkpoint of a peer). A peer that never reported holds trimming back.
    pub fn record_purge(&mut self, info: &PurgeInfo, peers: &[usize]) -> Result<()> {
        let seq = self.purge_seq()?.saturating_add(1);
        let mut floor = seq.saturating_sub(self.staged.purged_infos_limit);
        for peer in peers {
            floor = floor.min(self.purge_checkpoint(*peer)?);
        }
        let old = if floor == 0 {
            Vec::new()
        } else {
            self.purge_infos(1, floor)?
        };
        // A later info may list the same revision again: only the entries
        // that still name a trimmed info go.
        let mut stale_revs = Vec::new();
        for (old_seq, gone) in &old {
            for rev in &gone.revs {
                let named = self.get_value(
                    PURGED_REVS,
                    Table::PurgedRevs,
                    (gone.id.as_str(), rev.as_str()),
                )?;
                if named.as_deref() == Some(old_seq.to_be_bytes().as_slice()) {
                    stale_revs.push((gone.id.clone(), rev.clone()));
                }
            }
        }

        for (old_seq, gone) in &old {
            self.remove_value(PURGE_INFOS, Table::PurgeInfos, *old_seq)?;
            self.remove_value(PURGE_UUIDS, Table::PurgeUuids, gone.uuid.as_str())?;
        }
        for (id, rev) in &stale_revs {
            self.remove_value(PURGED_REVS, Table::PurgedRevs, (id.as_str(), rev.as_str()))?;
        }
        let seq_bytes = seq.to_be_bytes();
        self.put_value(
            PURGE_INFOS,
            Table::PurgeInfos,
            seq,
            &serde_json::to_vec(info)?,
        )?;
        self.put_value(
            PURGE_UUIDS,
            Table::PurgeUuids,
            info.uuid.as_str(),
            &seq_bytes,
        )?;
        for rev in &info.revs {
            self.put_value(
                PURGED_REVS,
                Table::PurgedRevs,
                (info.id.as_str(), rev.as_str()),
                &seq_bytes,
            )?;
        }
        self.staged.purge_seq = seq;
        Ok(())
    }

    /// The purge infos after sequence `since`, with the shard's purge sequence
    /// read in the same transaction.
    pub fn purged_since(&self, since: u64) -> Result<(u64, Vec<PurgeInfo>)> {
        let seq = self.purge_seq()?;
        let infos = match since.checked_add(1) {
            Some(from) if from <= seq => self.purge_infos(from, seq)?,
            _ => Vec::new(),
        };
        Ok((seq, infos.into_iter().map(|(_, info)| info).collect()))
    }

    /// Whether the purge info `uuid` was already recorded here.
    pub fn has_purge(&self, uuid: &str) -> Result<bool> {
        Ok(self
            .get_value(PURGE_UUIDS, Table::PurgeUuids, uuid)?
            .is_some())
    }

    /// Remove a document whose metadata record cannot be decoded: the record,
    /// its sequence entries and its revision bodies, recorded as the purge
    /// `info`. The counters are recounted, since the record's old state is
    /// unknown.
    pub fn purge_corrupt(&mut self, id: &str, info: &PurgeInfo, peers: &[usize]) -> Result<()> {
        let mut revs = Vec::new();
        // Every body of `id` and nothing else: ids sort before their
        // extensions, and "\0" is the least extension.
        let after_id = format!("{id}\0");
        let range = (Included((id, "")), Excluded((after_id.as_str(), "")));
        self.scan(BODIES, Table::Bodies, range, false, |(_, rev), _| {
            revs.push(rev.to_string());
            Ok(true)
        })?;
        let mut seqs = Vec::new();
        self.scan(
            BY_SEQ,
            Table::BySeq,
            (Unbounded, Unbounded),
            false,
            |seq, owner| {
                if owner == id.as_bytes() {
                    seqs.push(seq);
                }
                Ok(true)
            },
        )?;
        self.record_purge(info, peers)?;
        for rev in &revs {
            self.remove_body(id, rev)?;
        }
        for seq in seqs {
            self.remove_seq(seq)?;
        }
        self.remove_value(META, Table::Meta, id)?;

        let (mut docs, mut deleted, mut external) = (0u64, 0u64, 0u64);
        self.scan(
            META,
            Table::Meta,
            (Unbounded, Unbounded),
            false,
            |id, stored| {
                if let Ok(meta) = parse_meta(id, stored) {
                    if let Some((_, live, size)) = meta_state(&meta) {
                        if live {
                            docs += 1;
                        } else {
                            deleted += 1;
                        }
                        external += size;
                    }
                }
                Ok(true)
            },
        )?;
        self.staged.docs = docs;
        self.staged.deleted = deleted;
        self.staged.external = external;
        self.reset_indexes = true;
        Ok(())
    }

    /// Drop all document data from this local shard, keeping its sequence,
    /// its revs and purged-infos limits and the node-local checkpoints. Used
    /// after a shard has moved elsewhere and anti-entropy has seeded the new
    /// replicas. What is left is a new copy: it gets a new uuid, and the
    /// anti-entropy checkpoints that described the old copy's contents go
    /// with it.
    pub fn clear(&mut self) -> Result<()> {
        self.write(SETTINGS, |settings| {
            settings.retain(|name, _| !name.starts_with(PURGE_CHECKPOINT))?;
            Ok(())
        })?;
        let Txn::Write(txn) = &self.txn else {
            anyhow::bail!("a read-only shard job tried to write");
        };
        if let Err(error) = Self::clear_tables(txn) {
            self.note_failed_write(&error);
            return Err(error);
        }
        self.staged.uuid = crate::util::uuid();
        self.staged.docs = 0;
        self.staged.deleted = 0;
        self.staged.external = 0;
        // The purge infos went with the rest.
        self.staged.purge_seq = 0;
        self.reset_indexes = true;
        self.cleared = true;
        Ok(())
    }

    /// Empty every table of a shard's contents, keeping the node-local
    /// checkpoints.
    fn clear_tables(txn: &WriteTransaction) -> Result<()> {
        // Deleted and created again empty, so read transactions still find
        // every table.
        txn.delete_table(META)?;
        drop(txn.open_table(META)?);
        txn.delete_table(BY_SEQ)?;
        drop(txn.open_table(BY_SEQ)?);
        txn.delete_table(BODIES)?;
        drop(txn.open_table(BODIES)?);
        txn.delete_table(LOCAL)?;
        drop(txn.open_table(LOCAL)?);
        txn.delete_table(PURGE_INFOS)?;
        drop(txn.open_table(PURGE_INFOS)?);
        txn.delete_table(PURGED_REVS)?;
        drop(txn.open_table(PURGED_REVS)?);
        txn.delete_table(PURGE_UUIDS)?;
        drop(txn.open_table(PURGE_UUIDS)?);
        txn.open_table(CHECKPOINTS)?
            .retain(|id, _| !id.starts_with(SHARD_CHECKPOINT_PREFIX))?;
        Ok(())
    }
}

/// What reading a document needs, on the actor (a job's [`ShardTxn`]) or
/// off it (a [`ShardView`] on a lease).
pub trait DocRead {
    fn get_meta(&self, docid: &str) -> Result<Option<DocMeta>>;
    fn get_body(&self, docid: &str, rev: &str) -> Result<Option<Vec<u8>>>;
    fn revs_limit(&self) -> Result<u64>;
    /// Where this shard lives, for logging skipped records.
    fn name(&self) -> &ShardName;
}

impl DocRead for ShardTxn<'_> {
    fn get_meta(&self, docid: &str) -> Result<Option<DocMeta>> {
        ShardTxn::get_meta(self, docid)
    }

    fn get_body(&self, docid: &str, rev: &str) -> Result<Option<Vec<u8>>> {
        ShardTxn::get_body(self, docid, rev)
    }

    fn revs_limit(&self) -> Result<u64> {
        Ok(ShardTxn::revs_limit(self))
    }

    fn name(&self) -> &ShardName {
        ShardTxn::name(self)
    }
}

/// What a read off the actor knows of its shard besides the file.
struct ReadContext {
    name: ShardName,
    /// Shared with the shard and its handle (see [`Shard`]).
    body_reads: Arc<AtomicU64>,
    default_revs_limit: u64,
}

/// A shard as a read off the actor sees it: the committed state as of its
/// lease's transaction. It has none of the actor's in-memory indexes, so
/// reads that need them (`_changes` `pending`, `_all_docs` `offset` and
/// `total_rows`, partition counts) stay actor jobs.
pub struct ShardView<'a, 'l> {
    txn: &'a LeaseTxn<'l>,
    shard: &'a ReadContext,
}

impl DocRead for ShardView<'_, '_> {
    fn get_meta(&self, docid: &str) -> Result<Option<DocMeta>> {
        match self.txn.get(Table::Meta, &gate::key::text(docid))? {
            Some(bytes) => Ok(Some(parse_meta(docid, &bytes)?)),
            None => Ok(None),
        }
    }

    fn get_body(&self, docid: &str, rev: &str) -> Result<Option<Vec<u8>>> {
        self.shard.body_reads.fetch_add(1, Ordering::Relaxed);
        self.txn.get(Table::Bodies, &gate::key::pair(docid, rev))
    }

    fn revs_limit(&self) -> Result<u64> {
        match self
            .txn
            .get(Table::Settings, &gate::key::text(REVS_LIMIT))?
        {
            Some(bytes) => decode_u64(Table::Settings, &bytes),
            None => Ok(self.shard.default_revs_limit),
        }
    }

    fn name(&self) -> &ShardName {
        &self.shard.name
    }
}

impl ShardView<'_, '_> {
    /// The revisions of `id` that a recorded purge info lists.
    pub fn purged_revs(&self, id: &str, revs: &[String]) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for rev in revs {
            if self
                .txn
                .get(Table::PurgedRevs, &gate::key::pair(id, rev))?
                .is_some()
            {
                out.push(rev.clone());
            }
        }
        Ok(out)
    }

    /// Up to `limit` live documents (id and winning revision) whose ids
    /// `keep` accepts, in id order, after `after` when it is given, read from
    /// the metadata records. A record that does not decode is reported and
    /// skipped; a checksum mismatch fails the read.
    pub fn live_docs_after(
        &self,
        after: Option<&str>,
        limit: usize,
        keep: impl Fn(&str) -> bool,
    ) -> Result<Vec<(String, String)>> {
        let mut out = Vec::new();
        if limit == 0 {
            return Ok(out);
        }
        let from = match after {
            Some(after) => Excluded(gate::key::text(after)),
            None => Unbounded,
        };
        self.txn
            .scan(Table::Meta, (from, Unbounded), false, |id, stored| {
                let id = gate::key::split_text(id)?;
                if !keep(id) {
                    return Ok(true);
                }
                match parse_meta(id, stored) {
                    Ok(meta) => {
                        if let Some((rev, true, _)) = meta_state(&meta) {
                            out.push((id.to_string(), rev));
                        }
                    }
                    Err(corrupt) => self.shard.name.report_corrupt(&corrupt.id, &corrupt),
                }
                Ok(out.len() < limit)
            })?;
        Ok(out)
    }
}

/// A job's reply, sent once its group has committed or failed (`Some`).
type Reply = Box<dyn FnOnce(Option<&ApiError>) + Send>;

/// A job for the shard actor.
trait ShardJob: Send {
    /// Run in `txn`. Returns the reply to send once the group is decided, and
    /// the error that aborts the group, if this job does.
    fn run(self: Box<Self>, txn: &mut ShardTxn<'_>) -> (Reply, Option<ApiError>);
    /// Answer without running: no transaction could be opened.
    fn fail(self: Box<Self>, error: ApiError);
    /// Whether it waits for the shard to serve ([`Waiting`]), and so waits
    /// again when its read meets an I/O failure, instead of answering.
    fn waits(&self) -> bool {
        false
    }
}

/// A read job that waits for the shard to serve ([`WhenServing::run`]).
/// Nobody hears its answer. A read that meets an I/O failure hands the job
/// back ([`ShardTxn::again`]) to wait again ([`Shard::wait_again`]), so it
/// may run more than once; any other error, or a panic, ends it.
struct Waiting(Box<WaitingRead>);

/// A [`Waiting`] job's read, with what it does with the value it read.
type WaitingRead = dyn FnMut(&mut ShardTxn<'_>) -> Result<()> + Send;

impl ShardJob for Waiting {
    fn run(mut self: Box<Self>, txn: &mut ShardTxn<'_>) -> (Reply, Option<ApiError>) {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.0)(txn)));
        // An error has the shard check its handle, as a 500 from any job.
        txn.internal_error = !matches!(outcome, Ok(Ok(())));
        let abort = match outcome {
            Ok(Err(error)) if io_failure(&error) => {
                txn.again = Some(self);
                None
            }
            Ok(_) => None,
            // It may have left an index half built.
            Err(payload) => Some(panicked(payload)),
        };
        (Box::new(|_| {}), abort)
    }

    fn fail(self: Box<Self>, _: ApiError) {}

    fn waits(&self) -> bool {
        true
    }
}

/// A caller's job and where its answer goes.
struct Call<F, R> {
    f: F,
    reply: oneshot::Sender<std::result::Result<R, ApiError>>,
}

/// What a panic in a job turns into.
fn panicked(payload: Box<dyn std::any::Any + Send>) -> ApiError {
    let reason = payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string());
    ApiError::new(
        500,
        "internal_server_error",
        format!("shard job panicked: {reason}"),
    )
}

impl<F, R> ShardJob for Call<F, R>
where
    F: FnOnce(&mut ShardTxn<'_>) -> std::result::Result<R, ApiError> + Send,
    R: Send + 'static,
{
    fn run(self: Box<Self>, txn: &mut ShardTxn<'_>) -> (Reply, Option<ApiError>) {
        let Call { f, reply } = *self;
        // A panic must fail only this job's group, never kill the shard
        // actor (every later call would then answer "shard task is gone").
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(txn)));
        let (result, abort) = match outcome {
            Ok(Ok(_)) if txn.write_failed.is_some() => {
                // The job carried on past a failed write; part of its writes
                // may be in the transaction.
                let reason = txn.write_failed.clone().unwrap_or_default();
                let error = ApiError::new(
                    500,
                    "internal_server_error",
                    format!("a shard write failed: {reason}"),
                );
                let abort = ApiError::new(500, error.error, error.reason.clone());
                (Err(error), Some(abort))
            }
            Ok(Ok(value)) => (Ok(value), None),
            Ok(Err(error)) if txn.job_wrote => {
                let abort = ApiError::new(
                    500,
                    "internal_server_error",
                    format!("a job in the same group commit failed: {}", error.reason),
                );
                (Err(error), Some(abort))
            }
            Ok(Err(error)) => {
                txn.undo_job();
                (Err(error), None)
            }
            Err(payload) => {
                let error = panicked(payload);
                let abort = ApiError::new(error.status, error.error, error.reason.clone());
                (Err(error), Some(abort))
            }
        };
        txn.internal_error = matches!(&result, Err(error) if error.status == 500);
        let reply: Reply = Box::new(move |group| {
            // A job's own error is its answer; a job that succeeded fails
            // with its group.
            let result = match (result, group) {
                (Ok(_), Some(group)) => Err(ApiError::new(
                    group.status,
                    group.error,
                    group.reason.clone(),
                )),
                (result, _) => result,
            };
            let _ = reply.send(result);
        });
        (reply, abort)
    }

    fn fail(self: Box<Self>, error: ApiError) {
        let _ = self.reply.send(Err(error));
    }
}

/// A job and the receiver of its answer.
fn call_job<R, F>(
    f: F,
) -> (
    Box<dyn ShardJob>,
    oneshot::Receiver<std::result::Result<R, ApiError>>,
)
where
    R: Send + 'static,
    F: FnOnce(&mut ShardTxn<'_>) -> std::result::Result<R, ApiError> + Send + 'static,
{
    let (reply, answer) = oneshot::channel();
    (Box::new(Call { f, reply }), answer)
}

/// What the shard actor is asked to do.
enum Message {
    /// A job that may write: it joins a group.
    Write(Box<dyn ShardJob>),
    /// A job that only reads, on the committed state.
    Read(Box<dyn ShardJob>),
    /// A job that has the shard to itself between groups and opens its own
    /// transactions; handed the error instead when the shard has failed.
    Alone(AloneJob),
    /// Stop: the database is deleted. The file is closed unchecked, and the
    /// sender hears once it is.
    Close(oneshot::Sender<()>),
    /// Check the shard ([`Checkup`]): reopen a handle an I/O error left
    /// failing, retry the reopen of a failed shard once its wait is over,
    /// or run the jobs waiting to run again once theirs is.
    Check,
    /// A read job run as soon as the shard serves: now, or parked while it
    /// has failed or other such jobs wait (see [`ShardHandle::when_serving`]).
    WhenServing(Box<dyn ShardJob>),
}

/// A job run alone: handed the shard, or the error to answer with. It
/// returns its answer, sent once the actor has checked what its commits
/// left behind.
type AloneJob =
    Box<dyn FnOnce(std::result::Result<&mut Shard, ApiError>) -> Box<dyn FnOnce() + Send> + Send>;

impl Message {
    /// Answer with `error` without running.
    fn fail(self, error: ApiError) {
        match self {
            Message::Write(job) | Message::Read(job) | Message::WhenServing(job) => job.fail(error),
            Message::Alone(job) => job(Err(error))(),
            Message::Close(done) => drop(done),
            Message::Check => {}
        }
    }
}

/// Every shard is owned by exactly one dedicated OS thread, its only writer:
/// its calls are synchronous file I/O that must not run on Tokio workers.
/// Dropping the handle closes the queue, lets the thread finish the queued
/// jobs, and waits for it to close the file (after a full integrity check,
/// unless `skip_close_check` was called), so redb's file lock is released
/// before `drop` returns.
pub struct ShardHandle {
    tx: Option<mpsc::Sender<Message>>,
    /// Set by `discard` and `skip_close_check`: the actor closes the file
    /// without the close-time check and records nothing.
    discard: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    /// The shard's committed position: its sequence as of the last commit.
    seq_tracker: Arc<AtomicU64>,
    /// The shard copy's uuid, which `clear` replaces.
    uuid: Arc<std::sync::RwLock<String>>,
    /// The shard's purge sequence as of the last commit, shared with the
    /// shard.
    purge_seq: Arc<AtomicU64>,
    /// Shared with the shard.
    state: Arc<std::sync::RwLock<ShardState>>,
    /// Shared with the shard.
    checkup: Arc<Checkup>,
    /// The file, for reads off the actor.
    gate: Gate,
    read_context: Arc<ReadContext>,
    /// Shard round trips since it opened: jobs submitted to its actor and
    /// reads on leases. Each one is a cross-thread hop, so the count is the
    /// shard round-trip cost of a request.
    jobs: AtomicU64,
    /// Index entries the shard has walked, shared with the shard.
    scanned: Arc<AtomicU64>,
    /// Document bodies the shard has read, shared with the shard.
    body_reads: Arc<AtomicU64>,
    /// Shared with the shard: a compaction takes it off the actor.
    journal: Arc<journal::Journal>,
    /// Shared with the shard: a compaction that stops removes its file.
    target: Arc<dyn compact::Target>,
    /// The running compaction's progress.
    task: std::sync::Mutex<Option<compact::Task>>,
    /// The shard's documents' external sizes summed as of its last
    /// commit, shared with the shard.
    external: Arc<AtomicU64>,
    /// The size of the tables other than the bodies, as last measured.
    index: Arc<IndexSize>,
    /// The shard file's length when it was last read.
    file_len: AtomicU64,
}

/// Measurements of shard sizes ([`ShardHandle::index_bytes`]) running on
/// this node: at most one at a time, whatever the shard or database.
static MEASURING: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

/// Entries one lease of a size measurement reads at most.
const MEASURE_PAGE: usize = 1000;

/// What [`ShardHandle::index_bytes`] last measured, and when.
#[derive(Default)]
struct IndexSize {
    bytes: AtomicU64,
    /// Whether `bytes` holds a measurement.
    measured: AtomicBool,
    /// The shard's committed position and documents (live and deleted)
    /// when `bytes` was measured, or when the shard opened.
    seq: AtomicU64,
    docs: AtomicU64,
}

impl IndexSize {
    /// Whether the shard, now at `seq`, has taken as many changes since the
    /// last measurement (or its open) as it held documents then.
    fn due(&self, seq: u64) -> bool {
        seq.saturating_sub(self.seq.load(Ordering::SeqCst))
            >= self.docs.load(Ordering::SeqCst).max(1)
    }

    /// Start a measurement of the shard on `runtime` if one is due and no
    /// other runs on the node; otherwise do nothing, and a later commit
    /// tries again.
    fn measure_if_due(self: &Arc<Self>, seq: u64, gate: &Gate, runtime: Option<&Handle>) {
        if !self.due(seq) {
            return;
        }
        let (Some(runtime), Ok(permit)) = (runtime, MEASURING.try_acquire()) else {
            return;
        };
        let (index, gate) = (Arc::clone(self), gate.clone());
        runtime.spawn(async move {
            index.measure(&gate, seq).await;
            drop(permit);
        });
    }

    /// Measure the tables of the file under `gate` other than the bodies:
    /// their keys and values as stored, read a page per lease, so a
    /// reopen, a close or a compaction's swap waits for one page at most.
    /// A measurement that fails keeps the last one, and is not tried again
    /// until the shard has taken as many changes again.
    async fn measure(&self, gate: &Gate, seq: u64) {
        match measure_tables(gate).await {
            Ok((bytes, docs)) => {
                self.bytes.store(bytes, Ordering::SeqCst);
                self.docs.store(docs, Ordering::SeqCst);
                self.measured.store(true, Ordering::SeqCst);
            }
            Err(error) => tracing::debug!(error = %error, "shard size not measured"),
        }
        self.seq.store(seq, Ordering::SeqCst);
    }
}

/// The bytes of the keys and values of every table but the bodies, and the
/// documents (live and deleted) the shard holds, read a page per lease.
async fn measure_tables(gate: &Gate) -> Result<(u64, u64)> {
    let mut bytes = 0u64;
    for table in compact::TABLES {
        if table == Table::Bodies {
            continue;
        }
        let mut after: Option<Vec<u8>> = None;
        loop {
            let from = after.take();
            let (page, last) = gate
                .read(move |txn| {
                    let lower = from.map_or(Unbounded, Excluded);
                    let (mut bytes, mut entries, mut last) = (0u64, 0usize, None);
                    txn.scan(table, (lower, Unbounded), false, |key, value| {
                        bytes += (key.len() + value.len()) as u64;
                        entries += 1;
                        if entries == MEASURE_PAGE {
                            last = Some(key.to_vec());
                            return Ok(false);
                        }
                        Ok(true)
                    })?;
                    Ok((bytes, last))
                })
                .await?;
            bytes += page;
            match last {
                Some(key) => after = Some(key),
                None => break,
            }
        }
    }
    let docs = gate
        .read(|txn| {
            let mut docs = 0;
            for counter in [DOC_COUNT, DELETED_COUNT] {
                if let Some(count) = txn.get(Table::Settings, &gate::key::text(counter))? {
                    docs += decode_u64(Table::Settings, &count)?;
                }
            }
            Ok(docs)
        })
        .await?;
    Ok((bytes, docs))
}

impl ShardHandle {
    /// Open the shard file at `path` on a new actor thread. `committed` is
    /// pinged after every commit that advanced this shard's sequence, so a
    /// feed can wait for a commit instead of polling.
    pub fn spawn(
        path: impl AsRef<Path>,
        revs_limit: u64,
        cache_bytes: usize,
        committed: Arc<tokio::sync::watch::Sender<()>>,
    ) -> Result<ShardHandle> {
        let path = path.as_ref().to_path_buf();
        let name = format!(
            "shard-{}",
            path.file_stem().and_then(|n| n.to_str()).unwrap_or("?")
        );
        ShardHandle::spawn_with(name, committed, move || {
            Shard::open(&path, revs_limit, cache_bytes)
        })
    }

    /// A shard over a test disk that already holds one, on a new actor
    /// thread.
    #[cfg(test)]
    pub(crate) fn over_backend(switch: &kv::powercut::PowerSwitch) -> Result<ShardHandle> {
        let switch = switch.clone();
        ShardHandle::spawn_with("shard-test".to_string(), Default::default(), move || {
            Shard::over_backend(&switch, 1000)
        })
    }

    /// Run a shard that `open` opens on a new actor thread named `name`.
    fn spawn_with(
        name: String,
        committed: Arc<tokio::sync::watch::Sender<()>>,
        open: impl FnOnce() -> Result<Shard> + Send + 'static,
    ) -> Result<ShardHandle> {
        let seq_tracker = Arc::new(AtomicU64::new(0));
        let discard = Arc::new(AtomicBool::new(false));
        let discarded = discard.clone();
        let (tx, mut rx) = mpsc::channel::<Message>(256);
        type Ready = (
            u64,
            Arc<std::sync::RwLock<String>>,
            Arc<AtomicU64>,
            Arc<std::sync::RwLock<ShardState>>,
            Arc<Checkup>,
            Gate,
            ReadContext,
            Arc<journal::Journal>,
            Arc<dyn compact::Target>,
            Arc<AtomicU64>,
        );
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<Ready>>();
        let tracker = seq_tracker.clone();
        let scanned = Arc::new(AtomicU64::new(0));
        let shard_scanned = scanned.clone();
        let body_reads = Arc::new(AtomicU64::new(0));
        let shard_body_reads = body_reads.clone();
        let index = Arc::new(IndexSize::default());
        let shard_index = Arc::clone(&index);
        // Where measurements of the shard's size run: the runtime that opens
        // it, if any.
        let runtime = Handle::try_current().ok();
        let thread = std::thread::Builder::new().name(name).spawn(move || {
            // Opened on this thread, so the file never crosses threads.
            let mut shard = match open() {
                Ok(shard) => shard,
                Err(e) => {
                    let _ = ready_tx.send(Err(e));
                    return;
                }
            };
            shard.set_counters(shard_scanned, shard_body_reads);
            shard_index.seq.store(shard.seq(), Ordering::SeqCst);
            shard_index.docs.store(
                shard.settings.docs + shard.settings.deleted,
                Ordering::SeqCst,
            );
            let uuid = Arc::new(std::sync::RwLock::new(shard.settings.uuid.clone()));
            shard.shared_uuid = Some(uuid.clone());
            let context = ReadContext {
                name: shard.name.clone(),
                body_reads: Arc::clone(&shard.body_reads),
                default_revs_limit: shard.default_revs_limit,
            };
            let ready = (
                shard.seq(),
                uuid,
                Arc::clone(&shard.shared_purge_seq),
                Arc::clone(&shard.state),
                Arc::clone(&shard.checkup),
                shard.gate.clone(),
                context,
                Arc::clone(&shard.journal),
                Arc::clone(&shard.target),
                Arc::clone(&shard.shared_external),
            );
            if ready_tx.send(Ok(ready)).is_err() {
                return;
            }
            // Published before any caller hears back, so a write that was
            // acknowledged is inside the committed position.
            let publish = |seq: u64| {
                if tracker.swap(seq, Ordering::SeqCst) != seq {
                    committed.send_replace(());
                }
            };
            let mut next = None;
            let mut deleted = None;
            loop {
                let message = match next.take() {
                    Some(message) => message,
                    None => match rx.blocking_recv() {
                        Some(message) => message,
                        None => break,
                    },
                };
                // A failed shard answers for itself, but for a close, a check
                // (how it retries) and a job waiting for it to serve again.
                if let Some(error) = shard.failure() {
                    match message {
                        Message::Close(_) | Message::Check => {}
                        Message::WhenServing(job) if shard.is_failed() => {
                            shard.parked.push_back(job);
                            continue;
                        }
                        message => {
                            message.fail(error);
                            continue;
                        }
                    }
                }
                match message {
                    Message::Close(done) => {
                        deleted = Some(done);
                        break;
                    }
                    // Group commit: run a job, then every write job already
                    // queued, commit once (synced), and only then answer. A
                    // queued read ends the group and runs after its commit.
                    // A failed commit is recovered from before the answers.
                    Message::Write(job) => {
                        let (replies, error) = shard.run_group(job, || match rx.try_recv() {
                            Ok(Message::Write(job)) => Some(job),
                            Ok(other) => {
                                next = Some(other);
                                None
                            }
                            Err(_) => None,
                        });
                        publish(shard.seq());
                        shard_index.measure_if_due(shard.seq(), &shard.gate, runtime.as_ref());
                        for reply in replies {
                            reply(error.as_ref());
                        }
                    }
                    // Behind the jobs that wait again, in order.
                    Message::WhenServing(job) if !shard.parked.is_empty() => {
                        shard.parked.push_back(job);
                    }
                    Message::Read(job) | Message::WhenServing(job) => {
                        if let Some(reply) = shard.run_read(job) {
                            reply(None);
                        }
                    }
                    Message::Check => {
                        shard.checkup.queued.store(false, Ordering::SeqCst);
                        let state = shard
                            .state
                            .read()
                            .unwrap_or_else(|poison| poison.into_inner())
                            .clone();
                        match state {
                            // A gate shut for good (a failed compaction
                            // swap, a file behind) is never reopened.
                            ShardState::Failed(_)
                                if !shard.gate.is_shut() && shard.checkup.due(Instant::now()) =>
                            {
                                shard.recover("a retry of a failed shard");
                                // Before any job queued since; a job that
                                // fails the shard again leaves the rest for
                                // the next retry.
                                if !shard.is_failed() {
                                    shard.run_parked();
                                }
                            }
                            ShardState::Serving if shard.poisoned() => {
                                shard.recover("an I/O error in a read");
                            }
                            // Jobs whose read met an I/O failure, their
                            // wait over: the handle was reopened then.
                            ShardState::Serving
                                if !shard.parked.is_empty()
                                    && shard.checkup.due(Instant::now()) =>
                            {
                                shard.run_parked();
                            }
                            _ => {}
                        }
                        // A commit that failed yet landed is found on reopen.
                        publish(shard.seq());
                    }
                    Message::Alone(job) => {
                        let answer = job(Ok(&mut shard));
                        // Its own commits may have failed:
                        // recovered before it answers, so whoever hears
                        // back meets the recovered file.
                        if shard.poisoned() {
                            shard.recover("an I/O error in a job run alone");
                        }
                        answer();
                    }
                }
            }
            match deleted {
                Some(done) => {
                    shard.discard();
                    let _ = done.send(());
                }
                None if discarded.load(Ordering::SeqCst) => shard.discard(),
                None => shard.close(),
            }
        })?;
        let (
            initial_seq,
            uuid,
            purge_seq,
            state,
            checkup,
            gate,
            read_context,
            journal,
            target,
            external,
        ) = ready_rx
            .recv()
            .map_err(|_| anyhow::anyhow!("shard thread exited before opening its database"))??;
        seq_tracker.store(initial_seq, Ordering::SeqCst);
        Ok(ShardHandle {
            tx: Some(tx),
            discard,
            thread: Some(thread),
            seq_tracker,
            uuid,
            purge_seq,
            state,
            checkup,
            gate,
            read_context: Arc::new(read_context),
            jobs: AtomicU64::new(0),
            scanned,
            body_reads,
            journal,
            target,
            task: Default::default(),
            external,
            index,
            file_len: AtomicU64::new(0),
        })
    }

    /// The bytes the keys and values of the shard's tables other than the
    /// bodies take, as last measured; `None` before the first measurement.
    /// Reading every entry, a measurement never runs for a read: the shard
    /// starts one after a commit once it has taken as many changes since
    /// the last (or since it opened) as it held documents then, so their
    /// cost follows the writes, and a compaction asks for one when it is
    /// done ([`ShardHandle::measure_index`]).
    pub fn index_bytes(&self) -> Option<u64> {
        self.index
            .measured
            .load(Ordering::SeqCst)
            .then(|| self.index.bytes.load(Ordering::SeqCst))
    }

    /// The shard file's length: `read` when it could be read, else the
    /// length last read (a compaction's rename can hide the file for a
    /// moment).
    pub fn file_len(&self, read: Option<u64>) -> u64 {
        match read {
            Some(len) => {
                self.file_len.store(len, Ordering::SeqCst);
                len
            }
            None => self.file_len.load(Ordering::SeqCst),
        }
    }

    /// The shard's documents' external sizes summed
    /// ([`DocMeta::external_size`]), as of its last commit.
    pub fn external_size(&self) -> u64 {
        self.external.load(Ordering::SeqCst)
    }

    /// Measure [`ShardHandle::index_bytes`] now, as a compaction does once
    /// it has replaced the file, under the node's one measurement permit,
    /// waiting for it if another measurement runs.
    pub async fn measure_index(&self) {
        let Ok(_permit) = MEASURING.acquire().await else {
            return;
        };
        self.index.measure(&self.gate, self.committed_seq()).await;
    }

    /// Index entries the shard has walked to answer reads so far.
    pub fn scanned(&self) -> u64 {
        self.scanned.load(Ordering::Relaxed)
    }

    /// Document bodies the shard has read from storage so far.
    pub fn body_reads(&self) -> u64 {
        self.body_reads.load(Ordering::Relaxed)
    }

    /// Shard round trips so far: actor jobs and lease reads.
    pub fn jobs(&self) -> u64 {
        self.jobs.load(Ordering::Relaxed)
    }

    /// The shard's committed position.
    pub fn committed_seq(&self) -> u64 {
        self.seq_tracker.load(Ordering::SeqCst)
    }

    /// The shard's purge sequence as of its last commit: how many purge
    /// infos it has recorded. A purge leaves no change row, so an index
    /// learns of one by this moving. While a failed commit's outcome is
    /// unknown it is [`UNKNOWN_PURGE_SEQ`], which no index may treat as a
    /// count: it is for index cursors only, never for clients.
    pub(crate) fn purge_seq(&self) -> u64 {
        self.purge_seq.load(Ordering::SeqCst)
    }

    /// The shard copy's uuid.
    pub fn uuid(&self) -> String {
        self.uuid
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    /// Whether the shard serves requests.
    pub fn state(&self) -> ShardState {
        self.state
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    /// The answer to every request while the shard has failed. A request
    /// to a failed shard has it retry, and one to a shard with jobs waiting
    /// to run again has them run, once the wait since the last try is over;
    /// a shard shut for good is never retried.
    fn failure(&self) -> Option<ApiError> {
        let state = self.state();
        let retry = match state {
            ShardState::Failed(_) => !self.gate.is_shut(),
            ShardState::Serving => self.checkup.waiting.load(Ordering::SeqCst),
            ShardState::Deleted => false,
        };
        if retry && self.checkup.due(Instant::now()) {
            self.check();
        }
        failed(&self.read_context.name, &state)
    }

    /// Have the actor check the shard, unless a check is already queued.
    fn check(&self) {
        if self.checkup.queued.swap(true, Ordering::SeqCst) {
            return;
        }
        let queued = self
            .tx
            .as_ref()
            .is_some_and(|tx| tx.try_send(Message::Check).is_ok());
        if !queued {
            self.checkup.queued.store(false, Ordering::SeqCst);
        }
    }

    /// Have a failed shard wait `wait` before its next retry, doubling from
    /// there: production waits too long for a test.
    #[cfg(test)]
    pub(crate) fn set_retry_wait(&self, wait: Duration) {
        let mut retry = self.checkup.retry();
        retry.first = wait;
        retry.wait = wait;
        retry.at = Instant::now();
    }

    /// A read job queued straight on the actor, past the handle's own
    /// answer for a failed shard: it answers once the actor has handled
    /// everything queued before it.
    #[cfg(test)]
    pub(crate) fn barrier(&self) -> oneshot::Receiver<std::result::Result<(), ApiError>> {
        let (job, answer) = call_job(|_| Ok(()));
        let queued = self.tx.as_ref().unwrap().try_send(Message::Read(job));
        assert!(queued.is_ok(), "the actor's queue is full");
        answer
    }

    /// Jobs waiting in this shard's queue, for tests that line jobs up into
    /// one group.
    #[cfg(test)]
    pub(crate) fn queued(&self) -> usize {
        self.tx
            .as_ref()
            .map_or(0, |tx| tx.max_capacity() - tx.capacity())
    }

    /// Queue `message` and wait for its `answer`. The future borrows
    /// nothing from the handle, so it can be moved into a task of its own.
    fn send<R: Send + 'static>(
        &self,
        message: Message,
        answer: oneshot::Receiver<std::result::Result<R, ApiError>>,
    ) -> impl std::future::Future<Output = std::result::Result<R, ApiError>> + Send + 'static {
        self.jobs.fetch_add(1, Ordering::Relaxed);
        let queue = match self.failure() {
            Some(error) => Err(error),
            None => self
                .tx
                .clone()
                .ok_or_else(|| ApiError::new(500, "internal_server_error", "shard task is gone")),
        };
        async move {
            queue?
                .send(message)
                .await
                .map_err(|_| ApiError::new(500, "internal_server_error", "shard task is gone"))?;
            answer.await.map_err(|_| {
                ApiError::new(500, "internal_server_error", "shard task dropped reply")
            })?
        }
    }

    /// Run a job that may write, in the next group commit; it answers once
    /// the group has committed.
    pub fn call<R, F>(
        &self,
        f: F,
    ) -> impl std::future::Future<Output = std::result::Result<R, ApiError>> + Send + 'static
    where
        R: Send + 'static,
        F: FnOnce(&mut ShardTxn<'_>) -> std::result::Result<R, ApiError> + Send + 'static,
    {
        let (job, answer) = call_job(f);
        self.send(Message::Write(job), answer)
    }

    /// Run a job that only reads, on the committed state.
    pub fn read<R, F>(
        &self,
        f: F,
    ) -> impl std::future::Future<Output = std::result::Result<R, ApiError>> + Send + 'static
    where
        R: Send + 'static,
        F: FnOnce(&mut ShardTxn<'_>) -> std::result::Result<R, ApiError> + Send + 'static,
    {
        let (job, answer) = call_job(f);
        self.send(Message::Read(job), answer)
    }

    /// A way to run a read job once the shard serves (see
    /// [`WhenServing::run`]). It holds a sender to the actor until it runs,
    /// so it must not outlive the request it serves.
    pub fn when_serving(&self) -> WhenServing {
        WhenServing {
            tx: self.tx.clone(),
        }
    }

    /// Close the shard without the close-time integrity check, for a
    /// database whose open failed: the file is left as it is and its
    /// verification as the open left it (consumed, never recorded), so a
    /// damaged database costs no full read of its healthy shards per
    /// request. Waits until the file is closed.
    pub fn discard(self) {
        self.skip_close_check();
        drop(self);
    }

    /// Close the file without the close-time integrity check once the last
    /// handle goes, for a process that is exiting: the check reads the
    /// whole file, and what it records dies with the process.
    pub fn skip_close_check(&self) {
        self.discard.store(true, Ordering::SeqCst);
    }

    /// Stop the shard because its database is deleted, and wait until its
    /// file is closed. From then on every request answers `not_found`, also
    /// through handles cloned before, and the file is never closed again, so
    /// nothing can record it as verified after the database is gone.
    pub async fn close_deleted(&self) {
        *self
            .state
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = ShardState::Deleted;
        let (done, closed) = oneshot::channel();
        if let Some(tx) = &self.tx {
            if tx.send(Message::Close(done)).await.is_ok() {
                // A dropped sender means the actor is gone, file closed.
                let _ = closed.await;
            }
        }
    }

    /// Run `f` off the actor, on a read transaction of its own leased from
    /// the shard's gate, alongside the actor's write groups: for reads that
    /// need none of the actor's in-memory indexes. It sees the committed
    /// state as of its start. A streamed read takes one lease per page.
    pub async fn lease<R, F>(&self, f: F) -> std::result::Result<R, ApiError>
    where
        R: Send + 'static,
        F: FnOnce(&ShardView<'_, '_>) -> std::result::Result<R, ApiError> + Send + 'static,
    {
        self.jobs.fetch_add(1, Ordering::Relaxed);
        if let Some(error) = self.failure() {
            return Err(error);
        }
        let shard = Arc::clone(&self.read_context);
        let read = self
            .gate
            .read(move |txn| Ok(f(&ShardView { txn, shard: &shard })))
            .await;
        // Once the lease is back: the check may reopen the file, which
        // waits for every lease.
        if self.gate.take_io_failure() {
            self.check();
        }
        read.map_err(ApiError::from)?
    }

    /// The running compaction's progress, if one is running.
    pub fn compaction(&self) -> Option<compact::Task> {
        self.task
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    /// Run a job that has the shard to itself between groups, such as the
    /// start and the swap of a compaction.
    pub async fn alone<R, F>(&self, f: F) -> std::result::Result<R, ApiError>
    where
        R: Send + 'static,
        F: FnOnce(&mut Shard) -> std::result::Result<R, ApiError> + Send + 'static,
    {
        let (reply, answer) = oneshot::channel();
        let job = Box::new(move |shard: std::result::Result<&mut Shard, ApiError>| {
            let result = shard.and_then(|shard| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(shard)))
                    .unwrap_or_else(|payload| Err(panicked(payload)))
            });
            Box::new(move || {
                let _ = reply.send(result);
            }) as Box<dyn FnOnce() + Send>
        });
        self.send(Message::Alone(job), answer).await
    }
}

/// See [`ShardHandle::when_serving`].
pub struct WhenServing {
    tx: Option<mpsc::Sender<Message>>,
}

impl WhenServing {
    /// Queue `read` to run on the actor as a read job, and `then` on what
    /// it read: next, if the shard serves; while it has failed, right after
    /// a retry brings it back, before any job queued later. A `read` that
    /// meets an I/O failure is run again, still before any job queued
    /// this way later (other jobs do not wait for it): after the next retry
    /// if it failed the shard, or, if the shard reopened and serves, once
    /// the retry wait is over and a request arrives, as a failed shard is
    /// retried, the wait growing each time. Nobody hears its answer, and
    /// it never runs if the shard is deleted, closes, or fails for good
    /// first. Waits only for the queue.
    pub async fn run<T, F, G>(self, mut read: F, then: G)
    where
        F: FnMut(&mut ShardTxn<'_>) -> Result<T> + Send + 'static,
        G: FnOnce(T) + Send + 'static,
    {
        let mut then = Some(then);
        let job = Waiting(Box::new(move |txn| {
            let value = read(txn)?;
            if let Some(then) = then.take() {
                then(value);
            }
            Ok(())
        }));
        if let Some(tx) = self.tx {
            let _ = tx.send(Message::WhenServing(Box::new(job))).await;
        }
    }
}

impl Drop for ShardHandle {
    fn drop(&mut self) {
        self.tx = None;
        if let Some(thread) = self.thread.take() {
            // A job that owns the last reference to its own database would be
            // dropped on the actor thread; joining ourselves would deadlock.
            if thread.thread().id() != std::thread::current().id() {
                let _ = thread.join();
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::kv::powercut::PowerCut;
    use crate::rev::Rev;

    pub(crate) const CACHE: usize = 1 << 20;

    /// A new shard file in a fresh directory of its own.
    pub(crate) fn temp_file(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("douchdb-{tag}-{}", crate::util::uuid()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("shard-0000.redb");
        drop(create_file(&file, CACHE).unwrap());
        file
    }

    fn remove(file: &Path) {
        kv::forget_verified(file);
        let _ = std::fs::remove_dir_all(file.parent().unwrap());
    }

    pub(crate) fn open(file: &Path) -> Shard {
        Shard::open(file, 1000, CACHE).unwrap()
    }

    fn meta(deleted: bool, seq: u64) -> DocMeta {
        let rev = Rev::parse("1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut tree = RevTree::default();
        tree.merge_path(&[rev], deleted);
        DocMeta {
            tree,
            seq,
            ..DocMeta::default()
        }
    }

    /// Write `meta` as `id`'s record, reading the record it replaces first.
    pub(crate) fn put_doc_meta(
        txn: &mut ShardTxn<'_>,
        id: &str,
        meta: &DocMeta,
    ) -> std::result::Result<(), ApiError> {
        let old = txn.get_meta(id)?.as_ref().and_then(meta_state);
        let bytes = serde_json::to_vec(meta).unwrap();
        txn.put_meta(id, &bytes, old, meta_state(meta), meta.seq)?;
        Ok(())
    }

    /// Write a new revision of `id` on top of its winner (or create it),
    /// with its body, sequence entry and metadata, as a document write does:
    /// everything is read before the first write. The new revision.
    pub(crate) fn write_doc(
        txn: &mut ShardTxn<'_>,
        id: &str,
        deleted: bool,
        body: &[u8],
        n: u64,
    ) -> std::result::Result<String, ApiError> {
        let stored = txn.get_meta(id)?;
        let old = stored.as_ref().and_then(meta_state);
        let mut meta = stored.unwrap_or_default();
        let mut path: Vec<Rev> = match meta.tree.winner() {
            Some(winner) => meta
                .tree
                .path(&winner)
                .unwrap()
                .iter()
                .filter_map(|rev| Rev::parse(rev))
                .collect(),
            None => Vec::new(),
        };
        let pos = path.last().map_or(0, |rev| rev.pos) + 1;
        let rev = format!("{pos}-{n:032x}");
        path.push(Rev::parse(&rev).unwrap());
        meta.tree.merge_path(&path, deleted);
        meta.sizes.insert(rev.clone(), body.len() as u64);
        meta.keep_leaf_sizes();
        let old_seq = meta.seq;
        let seq = txn.next_seq();
        meta.seq = seq;
        txn.put_bodies(&[(id, &rev, body)])?;
        txn.put_seq(seq, id)?;
        if old_seq != 0 {
            txn.remove_seq(old_seq)?;
        }
        let bytes = serde_json::to_vec(&meta).unwrap();
        txn.put_meta(id, &bytes, old, meta_state(&meta), seq)?;
        Ok(rev)
    }

    fn live_ids(shard: &mut Shard) -> Vec<String> {
        shard
            .read(|txn| Ok(txn.live_docs()?.keys().cloned().collect()))
            .unwrap()
    }

    fn counts(shard: &mut Shard) -> (u64, u64) {
        shard.read(|txn| Ok(txn.counts())).unwrap()
    }

    fn body_keys(shard: &mut Shard) -> Vec<(String, String)> {
        shard
            .read(|txn| {
                let mut keys = Vec::new();
                let all = (Unbounded, Unbounded);
                txn.scan(BODIES, Table::Bodies, all, false, |(id, rev), _| {
                    keys.push((id.to_string(), rev.to_string()));
                    Ok(true)
                })?;
                Ok(keys)
            })
            .unwrap()
    }

    /// A failed shard first retries after a second, then waits twice as
    /// long after each failed retry, up to a minute; serving again starts
    /// the waits over.
    #[test]
    fn retry_waits_double_up_to_a_minute_and_restart_once_served() {
        let secs = Duration::from_secs;
        let checkup = Checkup::new();
        let t0 = Instant::now();
        checkup.failed(t0);
        assert!(!checkup.due(t0 + Duration::from_millis(999)));
        assert!(checkup.due(t0 + secs(1)));
        let mut waits = Vec::new();
        let mut now = t0;
        for _ in 0..8 {
            now += secs(3600);
            checkup.failed(now);
            waits.push((checkup.retry().at - now).as_secs());
        }
        assert_eq!(waits, [2, 4, 8, 16, 32, 60, 60, 60]);

        checkup.served();
        now += secs(3600);
        checkup.failed(now);
        assert_eq!(checkup.retry().at - now, secs(1));
    }

    #[test]
    fn compaction_keeps_leaf_bodies_whose_revision_contains_a_slash() {
        let file = temp_file("sweep-slash");
        let mut shard = open(&file);
        let tree = |edges: &[(&str, Option<&str>)]| {
            let edges: BTreeMap<&str, Option<&str>> = edges.iter().copied().collect();
            let tree: RevTree =
                serde_json::from_value(serde_json::json!({"edges": edges, "deleted": []})).unwrap();
            DocMeta {
                tree,
                seq: 1,
                ..DocMeta::default()
            }
        };
        shard
            .write(move |txn| {
                // The only leaf of "doc" has a '/' in its revision id.
                put_doc_meta(txn, "doc", &tree(&[("1-x/y", None)]))?;
                txn.put_bodies(&[("doc", "1-x/y", b"{}")])?;
                // "p" has a stale parent and a leaf, both with '/'.
                put_doc_meta(
                    txn,
                    "p",
                    &tree(&[("1-p/q", None), ("2-r/s", Some("1-p/q"))]),
                )?;
                txn.put_bodies(&[("p", "1-p/q", b"{}"), ("p", "2-r/s", b"{}")])?;
                // Under a joined key, a/1-b/c read as "a" with leaf "1-b/c"
                // or as "a/1-b" with rev "c"; each is a leaf of its own.
                put_doc_meta(txn, "a", &tree(&[("1-b/c", None)]))?;
                put_doc_meta(txn, "a/1-b", &tree(&[("2-z", None)]))?;
                txn.put_bodies(&[("a", "1-b/c", b"{}"), ("a/1-b", "2-z", b"{}")])?;
                Ok(())
            })
            .unwrap();

        shard.compact().unwrap();

        let mut expected: Vec<(String, String)> = [
            ("doc", "1-x/y"),
            ("p", "2-r/s"),
            ("a", "1-b/c"),
            ("a/1-b", "2-z"),
        ]
        .iter()
        .map(|(id, rev)| (id.to_string(), rev.to_string()))
        .collect();
        expected.sort();
        assert_eq!(body_keys(&mut shard), expected);
        drop(shard);
        remove(&file);
    }

    #[test]
    fn compaction_sweeps_stale_bodies_in_several_batches() {
        let file = temp_file("sweep");
        let mut shard = open(&file);
        let old = Rev::parse("1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let new = Rev::parse("2-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        let mut tree = RevTree::default();
        tree.merge_path(&[old.clone(), new.clone()], false);
        let doc_meta = DocMeta {
            tree,
            seq: 1,
            ..DocMeta::default()
        };
        // Ids with '/' sort between and around each other's keys.
        let ids = ["a", "a/b", "a/b/c", "a0", "b", "b/", "c", "d/e", "e", "f"];
        let (old_rev, new_rev) = (old.to_string(), new.to_string());
        shard
            .write(move |txn| {
                for id in ids {
                    put_doc_meta(txn, id, &doc_meta)?;
                    txn.put_bodies(&[(id, &old_rev, b"{}"), (id, &new_rev, b"{}")])?;
                }
                txn.put_bodies(&[("ghost/x", &old_rev, b"{}")])?;
                Ok(())
            })
            .unwrap();

        shard.compact().unwrap();

        let mut expected: Vec<(String, String)> = ids
            .iter()
            .map(|id| (id.to_string(), new.to_string()))
            .collect();
        expected.sort();
        assert_eq!(body_keys(&mut shard), expected, "only leaf bodies survive");
        drop(shard);
        remove(&file);
    }

    #[test]
    fn counters_track_live_and_deleted_documents_across_reopen() {
        let file = temp_file("counts");
        let mut shard = open(&file);
        assert_eq!(counts(&mut shard), (0, 0));

        shard
            .write(|txn| {
                put_doc_meta(txn, "a", &meta(false, 1))?;
                put_doc_meta(txn, "b", &meta(false, 2))?;
                put_doc_meta(txn, "c", &meta(true, 3))
            })
            .unwrap();
        assert_eq!(counts(&mut shard), (2, 1));

        // Turn a live doc into a deleted one, and purge another entirely.
        shard
            .write(|txn| {
                put_doc_meta(txn, "a", &meta(true, 4))?;
                let old = txn.get_meta("b")?.as_ref().and_then(meta_state);
                Ok(txn.remove_meta("b", old)?)
            })
            .unwrap();
        assert_eq!(counts(&mut shard), (0, 2));

        drop(shard);
        let mut shard = open(&file);
        assert_eq!(counts(&mut shard), (0, 2), "counters survive reopen");
        drop(shard);
        remove(&file);
    }

    /// A shard file that lost a table, or every setting, is damaged: it
    /// used to be filled in again on open (a new uuid, zeroed counters),
    /// now the open fails and the file is left as it is.
    #[test]
    fn a_shard_missing_a_table_or_its_settings_fails_to_open() {
        for damage in ["bodies", "settings"] {
            let file = temp_file("no-table");
            {
                let db = kv::open(&file, CACHE).unwrap();
                let txn = kv::begin_write(&db).unwrap();
                if damage == "bodies" {
                    txn.delete_table(BODIES).unwrap();
                } else {
                    txn.open_table(SETTINGS)
                        .unwrap()
                        .retain(|_, _| false)
                        .unwrap();
                }
                txn.commit().unwrap();
            }
            let error = Shard::open(&file, 1000, CACHE)
                .err()
                .expect("must refuse to open");
            assert!(
                error.to_string().contains("corrupt shard"),
                "{damage}: {error}"
            );
            let db = kv::open(&file, CACHE).unwrap();
            let txn = db.begin_read().unwrap();
            if damage == "bodies" {
                assert!(txn.open_table(BODIES).is_err(), "the table was recreated");
            } else {
                let settings = txn.open_table(SETTINGS).unwrap();
                assert!(
                    settings.first().unwrap().is_none(),
                    "settings were refilled"
                );
            }
            drop(txn);
            drop(db);
            remove(&file);
        }
    }

    /// A shard whose newest commit is damaged fails to open, so every
    /// request to it fails closed, rather than serving the commit before it
    /// as if the newest had never been acknowledged.
    #[test]
    fn damage_to_the_newest_commit_is_corruption_not_rollback() {
        const NEWEST: &str = "<<douchdb-shard-newest-commit>>";
        let file = temp_file("newest");
        let mut shard = open(&file);
        shard
            .write(|txn| write_doc(txn, "older", false, b"{}", 1))
            .unwrap();
        let body = format!("{{\"m\":\"{}\"}}", NEWEST.repeat(64));
        shard
            .write(move |txn| write_doc(txn, "newest", false, body.as_bytes(), 2))
            .unwrap();
        // The file as a crash right after that commit leaves it (a clean
        // close commits again), then damaged where only that commit wrote.
        let mut bytes = std::fs::read(&file).unwrap();
        drop(shard);
        let hits: Vec<usize> = bytes
            .windows(NEWEST.len())
            .enumerate()
            .filter(|(_, window)| *window == NEWEST.as_bytes())
            .map(|(at, _)| at)
            .collect();
        assert!(!hits.is_empty(), "the newest commit's body was not found");
        for at in hits {
            bytes[at + 3] ^= 0x55;
        }
        std::fs::write(&file, bytes).unwrap();
        kv::forget_verified(&file);

        for _ in 0..2 {
            match Shard::open(&file, 1000, CACHE) {
                Err(error) => assert!(
                    error.chain().any(|cause| matches!(
                        cause.downcast_ref::<redb::DatabaseError>(),
                        Some(redb::DatabaseError::Storage(redb::StorageError::Corrupted(
                            _
                        )))
                    )),
                    "{error:#}"
                ),
                Ok(mut shard) => panic!(
                    "opened with its newest commit damaged: newest = {:?}",
                    shard.read(|txn| Ok(txn.get_meta("newest")?)).unwrap()
                ),
            }
        }
        remove(&file);
    }

    /// Purging a document whose record cannot be decoded cannot take its
    /// external size off the counter, so the counter is recounted from the
    /// records that remain, as the document counts are.
    #[test]
    fn purging_an_undecodable_record_recounts_the_external_size() {
        let file = temp_file("purge-corrupt-sizes");
        let mut shard = open(&file);
        shard
            .write(|txn| {
                write_doc(txn, "a", false, b"{\"v\":1}", 1)?;
                write_doc(txn, "b", false, b"{\"v\":\"bbbb\"}", 2)?;
                write_doc(txn, "c", true, b"{}", 3)?;
                Ok(())
            })
            .unwrap();
        let external = |shard: &mut Shard| shard.read(|txn| Ok(txn.external_size())).unwrap();
        assert_eq!(external(&mut shard), 7 + 12 + 2);
        shard
            .write(|txn| {
                txn.put_raw(META, Table::Meta, "b", b"not a record");
                Ok(())
            })
            .unwrap();
        assert_eq!(
            external(&mut shard),
            7 + 12 + 2,
            "a raw write counts nothing"
        );
        let info = PurgeInfo {
            uuid: "u".to_string(),
            id: "b".to_string(),
            revs: Vec::new(),
        };
        shard
            .write(move |txn| Ok(txn.purge_corrupt("b", &info, &[])?))
            .unwrap();
        assert_eq!(external(&mut shard), 7 + 2);
        assert_eq!(counts(&mut shard), (1, 1));
        drop(shard);
        let mut shard = open(&file);
        assert_eq!(external(&mut shard), 7 + 2, "and as committed");
        drop(shard);
        remove(&file);
    }

    /// The settings a fresh file is created with are its state: one that
    /// lost any of them is damaged, not new, and must not open as if empty.
    #[test]
    fn a_shard_missing_a_setting_fails_to_open() {
        for setting in [UUID, SEQ, DOC_COUNT, DELETED_COUNT, EXTERNAL_SIZE] {
            let file = temp_file("no-setting");
            let mut shard = open(&file);
            shard
                .write(move |txn| Ok(txn.remove_value(SETTINGS, Table::Settings, setting)?))
                .unwrap();
            drop(shard);
            let error = Shard::open(&file, 1000, CACHE)
                .err()
                .expect("must refuse to open");
            assert!(error.to_string().contains("corrupt shard"), "{error}");
            remove(&file);
        }
    }

    #[test]
    fn a_failed_commit_leaves_the_state_on_disk() {
        let file = temp_file("commit-fail");
        let mut shard = open(&file);
        shard
            .write(|txn| {
                let seq = txn.next_seq();
                put_doc_meta(txn, "a", &meta(false, seq))
            })
            .unwrap();
        // Build the lazy indexes so the failure has to leave them alone too.
        assert_eq!(live_ids(&mut shard), vec!["a"]);
        let seq_of = |shard: &mut Shard, id: &'static str| {
            shard
                .read(move |txn| Ok(txn.latest_changes()?.seq_of(id)))
                .unwrap()
        };
        assert_eq!(seq_of(&mut shard, "a"), Some(1));

        let failed = shard.write(|txn| {
            txn.inject_commit_failure();
            let seq = txn.next_seq();
            put_doc_meta(txn, "b", &meta(false, seq))
        });
        assert!(failed.is_err());

        assert!(shard.read(|txn| Ok(txn.get_meta("b")?)).unwrap().is_none());
        assert_eq!(counts(&mut shard), (1, 0));
        // Nothing of the failed group is committed, its sequence included.
        assert_eq!(shard.seq(), 1);
        assert_eq!(live_ids(&mut shard), vec!["a"]);
        assert_eq!(seq_of(&mut shard, "b"), None);

        let seq = shard
            .write(|txn| {
                let seq = txn.next_seq();
                put_doc_meta(txn, "c", &meta(false, seq))?;
                Ok(seq)
            })
            .unwrap();
        assert_eq!(seq, 2);
        drop(shard);
        let mut shard = open(&file);
        assert_eq!(live_ids(&mut shard), vec!["a", "c"]);
        assert_eq!(counts(&mut shard), (2, 0));
        drop(shard);
        remove(&file);
    }

    #[test]
    fn shard_uuid_survives_reopen() {
        let file = temp_file("uuid-reopen");
        let mut shard = open(&file);
        let uuid = shard.read(|txn| Ok(txn.uuid().to_string())).unwrap();
        assert_eq!(uuid.len(), 32, "{uuid}");
        shard
            .write(|txn| {
                let seq = txn.next_seq();
                put_doc_meta(txn, "a", &meta(false, seq))
            })
            .unwrap();
        drop(shard);

        let mut shard = open(&file);
        assert_eq!(shard.read(|txn| Ok(txn.uuid().to_string())).unwrap(), uuid);
        let next = shard.write(|txn| Ok(txn.next_seq())).unwrap();
        assert_eq!(next, 2, "the sequence continues after reopen");
        // A cleared shard is a new copy: positions in the old one mean
        // nothing in it.
        shard.write(|txn| Ok(txn.clear()?)).unwrap();
        let cleared = shard.read(|txn| Ok(txn.uuid().to_string())).unwrap();
        assert_ne!(cleared, uuid);
        drop(shard);
        let mut shard = open(&file);
        assert_eq!(
            shard.read(|txn| Ok(txn.uuid().to_string())).unwrap(),
            cleared
        );
        drop(shard);
        remove(&file);
    }

    #[test]
    fn cleared_shard_reopens_and_keeps_its_sequence() {
        let file = temp_file("clear-reopen");
        let mut shard = open(&file);
        shard
            .write(|txn| {
                for _ in 0..5 {
                    txn.next_seq();
                }
                let seq = txn.next_seq();
                put_doc_meta(txn, "a", &meta(false, seq))?;
                let seq = txn.next_seq();
                put_doc_meta(txn, "b", &meta(true, seq))?;
                txn.put_local("l", b"{}")?;
                txn.put_checkpoint("ae3:peer", b"{}")?;
                txn.put_checkpoint("node-local", b"{}")?;
                txn.set_revs_limit(7)?;
                Ok(())
            })
            .unwrap();
        shard.write(|txn| Ok(txn.clear()?)).unwrap();
        assert_eq!(counts(&mut shard), (0, 0));
        drop(shard);

        let mut shard = open(&file);
        assert_eq!(shard.seq(), 7, "sequence numbers never go backwards");
        assert_eq!(counts(&mut shard), (0, 0));
        let left = shard
            .read(|txn| {
                Ok((
                    txn.get_meta("a")?.is_none(),
                    txn.get_local("l")?,
                    txn.checkpoints("")?,
                    txn.revs_limit(),
                ))
            })
            .unwrap();
        assert_eq!(
            left,
            (
                true,
                None,
                vec![("node-local".to_string(), b"{}".to_vec())],
                7
            ),
            "the copy's contents go; node-local checkpoints and limits stay"
        );
        drop(shard);
        remove(&file);
    }

    #[test]
    fn local_only_shard_reopens() {
        let file = temp_file("local-only");
        let mut shard = open(&file);
        shard.write(|txn| Ok(txn.put_local("x", b"{}")?)).unwrap();
        drop(shard);
        let mut shard = open(&file);
        assert_eq!(shard.seq(), 0);
        assert_eq!(
            shard
                .read(|txn| Ok(txn.get_local("x")?))
                .unwrap()
                .as_deref(),
            Some(&b"{}"[..])
        );
        drop(shard);
        remove(&file);
    }

    #[test]
    fn corrupt_metadata_is_skipped_by_scans_and_fails_closed_elsewhere() {
        let file = temp_file("corrupt-meta");
        let mut shard = open(&file);
        shard
            .write(|txn| {
                let seq = txn.next_seq();
                put_doc_meta(txn, "a", &meta(false, seq))?;
                txn.put_raw(META, Table::Meta, "p:bad", b"{not json");
                Ok(())
            })
            .unwrap();

        // Scans that serve many documents skip the record and count it.
        let before = crate::metrics::corrupt_records_skipped();
        assert_eq!(live_ids(&mut shard), ["a"]);
        let rows = shard
            .read(|txn| Ok(txn.latest_changes()?.through(u64::MAX).count()))
            .unwrap();
        assert_eq!(rows, 1);
        let partition = shard.read(|txn| Ok(txn.partition_counts("p")?)).unwrap();
        assert_eq!((partition.docs, partition.deleted), (0, 0));
        assert!(crate::metrics::corrupt_records_skipped() >= before + 3);
        // The anti-entropy fingerprint must not paper over it, and a point
        // read fails rather than answering "missing".
        assert!(shard.read(|txn| Ok(txn.fingerprint()?)).is_err());
        let read = shard.read(|txn| Ok(txn.get_meta("p:bad")?));
        assert!(read.is_err_and(|error| error.corrupt));
        drop(shard);
        remove(&file);
    }

    /// A value whose checksum fails is an error everywhere, listings
    /// included: it is never skipped as a corrupt record nor read as absent.
    #[test]
    fn a_value_failing_its_checksum_fails_reads_and_listings() {
        let file = temp_file("checksum");
        let mut shard = open(&file);
        shard
            .write(|txn| {
                let seq = txn.next_seq();
                put_doc_meta(txn, "a", &meta(false, seq))?;
                // Written without a seal: its last four bytes are no CRC.
                let Txn::Write(raw) = &txn.txn else {
                    unreachable!()
                };
                raw.open_table(META)
                    .unwrap()
                    .insert("bad", br#"{"tree":{},"seq":9}"#.as_slice())
                    .unwrap();
                Ok(())
            })
            .unwrap();
        let point = shard.read(|txn| Ok(txn.get_meta("bad")?)).unwrap_err();
        assert!(
            !point.corrupt,
            "a checksum failure is not a skippable record"
        );
        assert!(point.reason.contains("checksum"), "{}", point.reason);
        assert!(shard.read(|txn| Ok(txn.live_docs()?.len())).is_err());
        assert!(shard
            .read(|txn| Ok(txn.latest_changes()?.through(u64::MAX).count()))
            .is_err());
        drop(shard);
        remove(&file);
    }

    #[test]
    fn compaction_skips_a_corrupt_metadata_record_and_keeps_its_bodies() {
        let file = temp_file("corrupt-compact");
        let mut shard = open(&file);
        // A stale-looking body next to an unreadable metadata record: the
        // copy cannot know it is stale, so it must keep it and carry on.
        shard
            .write(|txn| {
                txn.put_raw(META, Table::Meta, "bad", b"{not json");
                txn.put_raw(BODIES, Table::Bodies, ("bad", "1-aaa"), b"{}");
                Ok(())
            })
            .unwrap();
        shard.compact().unwrap();
        let kept = shard.read(|txn| Ok(txn.get_body("bad", "1-aaa")?)).unwrap();
        assert!(kept.is_some());
        drop(shard);
        remove(&file);
    }

    #[test]
    fn live_index_tracks_documents_and_rebuilds_on_open() {
        let file = temp_file("live");
        let mut shard = open(&file);
        shard
            .write(|txn| {
                put_doc_meta(txn, "a", &meta(false, 1))?;
                put_doc_meta(txn, "b", &meta(true, 2))
            })
            .unwrap();
        assert_eq!(live_ids(&mut shard), ["a"]);

        // Once built, the index follows deletions and new writes.
        shard
            .write(|txn| {
                put_doc_meta(txn, "a", &meta(true, 3))?;
                put_doc_meta(txn, "c", &meta(false, 4))
            })
            .unwrap();
        assert_eq!(live_ids(&mut shard), ["c"]);
        drop(shard);

        // A fresh process rebuilds the index from metadata on first use.
        let mut shard = open(&file);
        assert_eq!(live_ids(&mut shard), ["c"]);
        assert_eq!(counts(&mut shard), (1, 2));

        shard
            .write(|txn| put_doc_meta(txn, "d", &meta(false, 5)))
            .unwrap();
        assert_eq!(live_ids(&mut shard), ["c", "d"]);
        drop(shard);
        remove(&file);
    }

    #[test]
    fn live_rank_follows_the_cursor_through_writes() {
        let file = temp_file("live-rank");
        let mut shard = open(&file);
        shard
            .write(|txn| {
                for (seq, id) in (1..).zip(["b", "d", "f", "h", "j"]) {
                    put_doc_meta(txn, id, &meta(false, seq))?;
                }
                Ok(())
            })
            .unwrap();
        let probes = ["a", "d", "e", "j", "z", "c", "f", "a", "h"];
        let check = |shard: &mut Shard| {
            for probe in probes {
                let expected = live_ids(shard)
                    .iter()
                    .filter(|id| id.as_str() < probe)
                    .count();
                let rank = shard.read(move |txn| Ok(txn.live_rank(probe)?)).unwrap();
                assert_eq!(rank, expected, "rank of {probe}");
            }
        };
        check(&mut shard);
        // Writes below, at and above the cursor key: insert, delete, update.
        for (seq, (id, deleted)) in (6..).zip([
            ("a", false),
            ("d", true),
            ("e", false),
            ("z", false),
            ("j", true),
        ]) {
            shard.read(|txn| Ok(txn.live_rank("f")?)).unwrap();
            shard
                .write(move |txn| put_doc_meta(txn, id, &meta(deleted, seq)))
                .unwrap();
            check(&mut shard);
        }
        drop(shard);
        remove(&file);
    }

    #[test]
    fn commit_keeps_the_latest_index_current() {
        let file = temp_file("latest-commit");
        let mut shard = open(&file);
        shard
            .write(|txn| {
                put_doc_meta(txn, "a", &meta(false, 1))?;
                put_doc_meta(txn, "b", &meta(false, 2))
            })
            .unwrap();
        let seq_a = shard
            .read(|txn| Ok(txn.latest_changes()?.seq_of("a")))
            .unwrap();
        assert_eq!(seq_a, Some(1));

        // An update, a new document and a purge, committed with the index
        // built: it follows them without a rebuild.
        shard
            .write(|txn| {
                put_doc_meta(txn, "a", &meta(false, 3))?;
                put_doc_meta(txn, "c", &meta(false, 4))?;
                let old = txn.get_meta("b")?.as_ref().and_then(meta_state);
                Ok(txn.remove_meta("b", old)?)
            })
            .unwrap();
        assert!(shard.latest.is_some(), "the index was dropped, not updated");
        let seen = shard
            .read(|txn| {
                let latest = txn.latest_changes()?;
                Ok((
                    latest.seq_of("a"),
                    latest.seq_of("b"),
                    latest.through(u64::MAX).count(),
                    latest.count_after(3),
                ))
            })
            .unwrap();
        assert_eq!(seen, (Some(3), None, 2, 1));
        drop(shard);
        remove(&file);
    }

    /// An idle shard holds no live or latest-changes entries, and the next
    /// query rebuilds them: counts, ranks, partition counts and `_changes`
    /// order come out as they did, and a write made while released shows.
    #[test]
    fn released_indexes_are_empty_and_rebuild_on_next_use() {
        let file = temp_file("release-indexes");
        let mut shard = open(&file);
        shard
            .write(|txn| {
                for (seq, id) in [(1, "a"), (2, "p:1"), (3, "p:2"), (4, "q:1"), (5, "z")] {
                    put_doc_meta(txn, id, &meta(false, seq))?;
                }
                put_doc_meta(txn, "p:3", &meta(true, 6))
            })
            .unwrap();

        type Observed = (
            Vec<String>,
            usize,
            crate::store::PartitionCounts,
            usize,
            usize,
            usize,
            Vec<(u64, String)>,
        );
        let observe = |shard: &mut Shard| -> Observed {
            shard
                .read(|txn| {
                    let changes: Vec<(u64, String)> = txn
                        .latest_changes()?
                        .after(1)
                        .map(|(seq, id)| (seq, id.to_string()))
                        .collect();
                    Ok((
                        txn.live_docs()?.keys().cloned().collect(),
                        txn.live_rank("q:")?,
                        txn.partition_counts("p")?,
                        txn.partition_offset("p", "p:2", true)?,
                        txn.pending_changes(3, false)?,
                        txn.pending_changes(3, true)?,
                        changes,
                    ))
                })
                .unwrap()
        };
        let entries = |shard: &mut Shard| shard.read(|txn| Ok(txn.index_entries())).unwrap();
        let before = observe(&mut shard);
        assert_eq!(before.1, 3);
        assert_eq!(entries(&mut shard), 5 + 6, "live plus latest");

        shard.release_indexes();
        assert_eq!(entries(&mut shard), 0);
        assert!(shard.rank_cursor.is_none() && shard.partition_ranks.is_empty());
        assert_eq!(observe(&mut shard), before, "rebuilt answers differ");
        assert_eq!(entries(&mut shard), 5 + 6);

        // A write between the release and the rebuild is not lost.
        shard.release_indexes();
        shard
            .write(|txn| {
                put_doc_meta(txn, "p:0", &meta(false, 7))?;
                put_doc_meta(txn, "z", &meta(true, 8))
            })
            .unwrap();
        assert_eq!(entries(&mut shard), 0, "a write rebuilt the indexes");
        assert_eq!(live_ids(&mut shard), ["a", "p:0", "p:1", "p:2", "q:1"]);
        let partition = shard.read(|txn| Ok(txn.partition_counts("p")?)).unwrap();
        assert_eq!((partition.docs, partition.deleted), (3, 1));
        assert_eq!(shard.read(|txn| Ok(txn.live_rank("q:")?)).unwrap(), 4);
        let latest = shard
            .read(|txn| {
                let latest = txn.latest_changes()?;
                Ok((
                    latest.seq_of("p:0"),
                    latest.seq_of("z"),
                    latest.through(u64::MAX).count(),
                ))
            })
            .unwrap();
        assert_eq!(latest, (Some(7), Some(8), 7));
        drop(shard);
        remove(&file);
    }

    #[test]
    fn latest_changes_counts_match_walking_the_index() {
        let mut latest = LatestChanges::default();
        // A fixed pseudo-random walk: new documents and updates that move a
        // document from an old bucket to a new one, with gaps between
        // sequences so many buckets stay empty.
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut seq = 0;
        for _ in 0..5000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            seq += 1 + state % 7;
            latest.insert(format!("doc{}", state % 1500), seq);
            if seq % 97 == 0 {
                for cursor in [0, 1, seq / 3, seq / 2, seq - 1, seq, seq + 1, u64::MAX] {
                    assert_eq!(latest.count_after(cursor), latest.after(cursor).count());
                    assert_eq!(
                        latest.count_before(cursor),
                        latest
                            .through(u64::MAX)
                            .take_while(|(seq, _)| *seq < cursor)
                            .count()
                    );
                }
            }
        }
        for cursor in 0..=seq + 1 {
            assert_eq!(latest.count_after(cursor), latest.after(cursor).count());
            assert_eq!(
                latest.count_before(cursor),
                latest
                    .through(u64::MAX)
                    .take_while(|(seq, _)| *seq < cursor)
                    .count()
            );
        }
    }

    #[test]
    fn latest_changes_index_moves_documents_to_their_newest_sequence() {
        let mut latest = LatestChanges::default();
        latest.insert("a".into(), 1);
        latest.insert("_design/v".into(), 2);
        latest.insert("b".into(), 3);
        latest.insert("a".into(), 4); // a is updated: its old sequence goes
        let after = |since| {
            latest
                .after(since)
                .map(|(seq, id)| (seq, id.to_string()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            after(0),
            [(2, "_design/v".into()), (3, "b".into()), (4, "a".into())]
        );
        assert_eq!(after(3), [(4, "a".to_string())]);
        assert!(after(4).is_empty());
        assert!(after(u64::MAX).is_empty());
        assert_eq!(
            latest
                .through(u64::MAX)
                .rev()
                .map(|(seq, _)| seq)
                .collect::<Vec<_>>(),
            [4, 3, 2]
        );
        assert_eq!(latest.seq_of("a"), Some(4));
    }

    /// A deterministic pseudo-random sequence (xorshift64).
    pub(crate) struct Rng(pub(crate) u64);

    impl Rng {
        pub(crate) fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        /// A number below `n`.
        pub(crate) fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Every acknowledged write is on disk when the power goes, however the
    /// writes were grouped and wherever the cut falls; a group in flight at
    /// the cut is wholly there or wholly gone.
    #[tokio::test]
    async fn acknowledged_writes_survive_a_power_cut() {
        let (_, switch) = PowerCut::new();
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        // Every acknowledged write: local id -> value.
        let mut acknowledged: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        let mut groups: Vec<Vec<String>> = Vec::new();
        // The shard file, made once; each round reopens it.
        drop(Shard::create_over_backend(&switch, 1000).unwrap());
        for round in 0..12u64 {
            let handle = Arc::new(ShardHandle::over_backend(&switch).unwrap());
            let writes = 20 + rng.below(40);
            let cut_after = rng.below(writes);
            let mut tasks = Vec::new();
            for i in 0..writes {
                let handle = handle.clone();
                // Each write is a group's worth: a document and two locals,
                // so a torn group would show as a part of one.
                let ids: Vec<String> = (0..3).map(|k| format!("r{round}-w{i}-{k}")).collect();
                groups.push(ids.clone());
                let value = format!("{round}/{i}").into_bytes();
                tasks.push(tokio::spawn(async move {
                    let stored = value.clone();
                    let result = handle
                        .call(move |txn| {
                            write_doc(txn, &ids[0], false, &stored, round * 1000 + i)?;
                            txn.put_local(&ids[1], &stored)?;
                            txn.put_local(&ids[2], &stored)?;
                            Ok(ids)
                        })
                        .await;
                    (result, value)
                }));
            }
            let mut done = 0;
            for task in tasks {
                let (result, value) = task.await.unwrap();
                if let Ok(ids) = result {
                    for id in ids {
                        acknowledged.insert(id, value.clone());
                    }
                }
                done += 1;
                if done == cut_after {
                    switch.cut();
                }
            }
            if cut_after == 0 {
                switch.cut();
            }
            drop(handle);

            let mut shard = Shard::over_backend(&switch, 1000).unwrap();
            let (docs, locals) = shard
                .read(|txn| {
                    let docs: BTreeSet<String> = txn.live_docs()?.keys().cloned().collect();
                    let locals: BTreeMap<String, Vec<u8>> = txn.locals()?.into_iter().collect();
                    Ok((docs, locals))
                })
                .unwrap();
            for (id, value) in &acknowledged {
                let present = docs.contains(id) || locals.get(id) == Some(value);
                assert!(present, "acknowledged write {id} lost in round {round}");
            }
            for ids in &groups {
                let parts = usize::from(docs.contains(&ids[0]))
                    + usize::from(locals.contains_key(&ids[1]))
                    + usize::from(locals.contains_key(&ids[2]));
                assert!(parts == 0 || parts == 3, "a torn group: {ids:?}");
            }
            assert_eq!(
                shard.read(|txn| Ok(txn.counts())).unwrap().0,
                docs.len() as u64,
                "counters agree with the documents"
            );
        }
        assert!(!acknowledged.is_empty());
    }
}

#[cfg(test)]
mod group_tests {
    use super::tests::{open, temp_file, write_doc};
    use super::*;

    type TestJob = Box<dyn FnOnce(&mut ShardTxn<'_>) -> std::result::Result<(), ApiError> + Send>;

    fn handle(tag: &str) -> (Arc<ShardHandle>, PathBuf) {
        let file = temp_file(tag);
        let handle = ShardHandle::spawn(&file, 1000, tests::CACHE, Default::default()).unwrap();
        (Arc::new(handle), file)
    }

    fn cleanup(handle: Arc<ShardHandle>, file: PathBuf) {
        drop(handle);
        kv::forget_verified(&file);
        let _ = std::fs::remove_dir_all(file.parent().unwrap());
    }

    /// Run `jobs` in one group, behind a first job that holds the actor until
    /// every one of them is queued. The first job's answer, then theirs.
    async fn grouped(
        handle: &Arc<ShardHandle>,
        jobs: Vec<TestJob>,
    ) -> (
        std::result::Result<(), ApiError>,
        Vec<std::result::Result<(), ApiError>>,
    ) {
        let (started, running) = oneshot::channel();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let first = tokio::spawn({
            let handle = handle.clone();
            async move {
                handle
                    .call(move |_| {
                        let _ = started.send(());
                        held.recv().map_err(ApiError::internal)
                    })
                    .await
            }
        });
        running.await.unwrap();
        let count = jobs.len();
        let tasks: Vec<_> = jobs
            .into_iter()
            .map(|job| {
                let handle = handle.clone();
                tokio::spawn(async move { handle.call(job).await })
            })
            .collect();
        while handle.queued() < count {
            tokio::task::yield_now().await;
        }
        release.send(()).unwrap();
        let first = first.await.unwrap();
        let mut answers = Vec::new();
        for task in tasks {
            answers.push(task.await.unwrap());
        }
        (first, answers)
    }

    fn local(id: &'static str) -> TestJob {
        Box::new(move |txn| Ok(txn.put_local(id, id.as_bytes())?))
    }

    async fn locals(handle: &ShardHandle) -> Vec<String> {
        handle
            .read(|txn| Ok(txn.locals()?.into_iter().map(|(id, _)| id).collect()))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_group_commits_atomically_and_later_jobs_see_earlier_writes() {
        let (handle, file) = handle("group-atomic");
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let record = seen.clone();
        let (first, answers) = grouped(
            &handle,
            vec![
                Box::new(|txn| {
                    write_doc(txn, "a", false, b"{}", 1)?;
                    Ok(txn.put_local("one", b"1")?)
                }),
                Box::new(move |txn| {
                    // The earlier job's writes, through the open transaction,
                    // none of them committed yet.
                    let mut seen = record.lock().unwrap();
                    seen.push(format!("{:?}", txn.get_local("one")?));
                    seen.push(format!("{:?}", txn.committed_local("one")));
                    seen.push(format!("{:?}", txn.get_meta("a")?.map(|meta| meta.seq)));
                    seen.push(format!("{}", txn.counts().0));
                    write_doc(txn, "b", false, b"{}", 2)?;
                    Ok(txn.put_local("two", b"2")?)
                }),
            ],
        )
        .await;
        first.unwrap();
        for answer in answers {
            answer.unwrap();
        }
        assert_eq!(
            *seen.lock().unwrap(),
            ["Some([49])", "None", "Some(1)", "1"],
            "a later job sees the earlier job's uncommitted writes"
        );
        assert_eq!(
            handle.committed_seq(),
            2,
            "sequences run on through the group"
        );
        assert_eq!(locals(&handle).await, ["one", "two"]);
        let (docs, rows) = handle
            .read(|txn| {
                let rows: Vec<(u64, String)> = txn
                    .latest_changes()?
                    .after(0)
                    .map(|(seq, id)| (seq, id.to_string()))
                    .collect();
                Ok((txn.counts(), rows))
            })
            .await
            .unwrap();
        assert_eq!(docs, (2, 0));
        assert_eq!(rows, [(1, "a".to_string()), (2, "b".to_string())]);
        cleanup(handle, file);
    }

    #[tokio::test]
    async fn a_panicking_job_fails_its_whole_group_and_writes_nothing() {
        let (handle, file) = handle("group-panic");
        handle
            .call(|txn| Ok(txn.put_local("before", b"")?))
            .await
            .unwrap();
        let (first, answers) = grouped(
            &handle,
            vec![
                local("a"),
                Box::new(|txn| {
                    write_doc(txn, "doc", false, b"{}", 1)?;
                    txn.put_local("b", b"")?;
                    panic!("boom in a grouped job");
                }),
                // Queued behind the panic: it runs in the next group.
                local("c"),
            ],
        )
        .await;
        for (who, answer) in [
            ("first", &first),
            ("a", &answers[0]),
            ("panicker", &answers[1]),
        ] {
            let error = answer.as_ref().unwrap_err();
            assert_eq!(error.status, 500, "{who}");
            assert!(
                error.reason.contains("boom in a grouped job"),
                "{who}: {}",
                error.reason
            );
        }
        answers[2].as_ref().unwrap();
        assert_eq!(locals(&handle).await, ["before", "c"]);
        assert_eq!(
            handle.committed_seq(),
            0,
            "the group's sequence is not committed"
        );
        let state = handle
            .read(|txn| Ok((txn.counts(), txn.get_meta("doc")?.is_none())))
            .await
            .unwrap();
        assert_eq!(state, ((0, 0), true));
        // The shard serves on.
        handle
            .call(|txn| Ok(txn.put_local("after", b"")?))
            .await
            .unwrap();
        cleanup(handle, file);
    }

    #[tokio::test]
    async fn a_job_error_before_writing_fails_only_that_job() {
        let (handle, file) = handle("group-early-error");
        let (first, answers) = grouped(
            &handle,
            vec![
                Box::new(|txn| {
                    write_doc(txn, "a", false, b"{}", 1)?;
                    Ok(())
                }),
                Box::new(|txn| {
                    // Validation fails after taking a sequence, before any
                    // write: the sequence goes back.
                    txn.next_seq();
                    txn.get_meta("a")?;
                    Err(ApiError::conflict())
                }),
                Box::new(|txn| {
                    write_doc(txn, "b", false, b"{}", 2)?;
                    Ok(())
                }),
            ],
        )
        .await;
        first.unwrap();
        answers[0].as_ref().unwrap();
        assert_eq!(answers[1].as_ref().unwrap_err().status, 409);
        answers[2].as_ref().unwrap();
        let seqs = handle
            .read(|txn| {
                Ok((
                    txn.get_meta("a")?.map(|meta| meta.seq),
                    txn.get_meta("b")?.map(|meta| meta.seq),
                ))
            })
            .await
            .unwrap();
        assert_eq!(seqs, (Some(1), Some(2)), "no gap where the failed job was");
        assert_eq!(handle.committed_seq(), 2);
        cleanup(handle, file);
    }

    #[tokio::test]
    async fn a_job_error_after_writing_fails_the_whole_group() {
        let (handle, file) = handle("group-late-error");
        let (first, answers) = grouped(
            &handle,
            vec![
                local("a"),
                Box::new(|txn| {
                    write_doc(txn, "doc", false, b"{}", 1)?;
                    txn.put_local("b", b"")?;
                    Err(ApiError::bad_request("failed after writing"))
                }),
                local("c"),
            ],
        )
        .await;
        let failing = answers[1].as_ref().unwrap_err();
        assert_eq!(failing.status, 400, "the failing job gets its own error");
        for (who, answer) in [("first", &first), ("a", &answers[0])] {
            let error = answer.as_ref().unwrap_err();
            assert_eq!(error.status, 500, "{who}");
            assert!(
                error.reason.contains("failed after writing"),
                "{who}: {}",
                error.reason
            );
        }
        answers[2].as_ref().unwrap();
        assert_eq!(locals(&handle).await, ["c"]);
        assert_eq!(handle.committed_seq(), 0);
        let doc = handle.read(|txn| Ok(txn.get_meta("doc")?)).await.unwrap();
        assert!(doc.is_none());
        cleanup(handle, file);
    }

    /// The handle's purge sequence follows what is committed: a recorded
    /// purge moves it, a failed group that recorded one does not, and a
    /// clear (which drops the purge infos) takes it back to 0.
    #[tokio::test]
    async fn the_handle_publishes_the_committed_purge_sequence() {
        let (handle, file) = handle("purge-seq");
        let info = |n: u64| PurgeInfo {
            uuid: format!("purge-{n}"),
            id: format!("gone-{n}"),
            revs: vec![format!("1-{n:032x}")],
        };
        assert_eq!(handle.purge_seq(), 0);
        handle
            .call(move |txn| Ok(txn.record_purge(&info(1), &[])?))
            .await
            .unwrap();
        assert_eq!(handle.purge_seq(), 1);
        let failed = handle
            .call(move |txn| -> Result<(), ApiError> {
                txn.record_purge(&info(2), &[])?;
                Err(ApiError::bad_request("failed after writing"))
            })
            .await;
        assert!(failed.is_err());
        assert_eq!(handle.purge_seq(), 1, "a failed group moved it");
        handle.call(|txn| Ok(txn.clear()?)).await.unwrap();
        assert_eq!(handle.purge_seq(), 0);
        let on_disk = handle.read(|txn| Ok(txn.purge_seq()?)).await.unwrap();
        assert_eq!(on_disk, 0);
        cleanup(handle, file);
    }

    /// A job that carries on past a failed write (as a batch write that turns
    /// a storage error into per-document results would) still fails its
    /// group: part of its writes may be in the transaction.
    #[tokio::test]
    async fn a_job_that_carries_on_past_a_failed_write_fails_the_whole_group() {
        let (handle, file) = handle("group-swallowed-write");
        let (first, answers) = grouped(
            &handle,
            vec![
                local("a"),
                Box::new(|txn| {
                    txn.inject_write_failure(1);
                    txn.put_local("b", b"")?;
                    let swallowed = txn.put_local("c", b"");
                    assert!(swallowed.is_err(), "the injected write failure");
                    Ok(())
                }),
                local("d"),
            ],
        )
        .await;
        for (who, answer) in [
            ("first", &first),
            ("a", &answers[0]),
            ("carrier", &answers[1]),
        ] {
            let error = answer.as_ref().unwrap_err();
            assert_eq!(error.status, 500, "{who}");
            assert!(
                error.reason.contains("injected write failure"),
                "{who}: {}",
                error.reason
            );
        }
        answers[2].as_ref().unwrap();
        assert_eq!(locals(&handle).await, ["d"]);
        cleanup(handle, file);
    }

    #[tokio::test]
    async fn a_panicking_job_fails_only_its_caller() {
        let (handle, file) = handle("panic");
        let result: std::result::Result<(), ApiError> =
            handle.call(|_| panic!("boom in job")).await;
        let error = result.unwrap_err();
        assert_eq!(error.status, 500);
        assert!(error.reason.contains("boom in job"), "{}", error.reason);
        handle.call(|_| Ok(())).await.unwrap();
        let read: std::result::Result<(), ApiError> = handle.read(|_| panic!("boom in read")).await;
        assert!(read.unwrap_err().reason.contains("boom in read"));
        handle.read(|_| Ok(())).await.unwrap();
        cleanup(handle, file);
    }

    /// Nothing of a failed job is committed, its sequence included: the
    /// next write takes the same number, which no reader ever saw.
    #[tokio::test]
    async fn a_failed_job_leaves_no_sequence_behind() {
        let (handle, file) = handle("seq-failed");
        let result: std::result::Result<(), ApiError> = handle
            .call(|txn| {
                txn.next_seq();
                panic!("after taking a sequence");
            })
            .await;
        assert!(result.is_err());
        let result: std::result::Result<(), ApiError> = handle
            .call(|txn| {
                txn.next_seq();
                Err(ApiError::bad_request("failed after taking a sequence"))
            })
            .await;
        assert!(result.is_err());
        assert_eq!(handle.committed_seq(), 0);
        let next = handle
            .call(|txn| write_doc(txn, "after", false, b"{}", 1).map(|_| txn.staged.seq))
            .await
            .unwrap();
        assert_eq!(next, 1);
        assert_eq!(handle.committed_seq(), 1);
        cleanup(handle, file);
    }

    #[tokio::test]
    async fn a_failed_commit_fails_the_caller_and_leaves_no_trace() {
        let (handle, file) = handle("commit-fail");
        handle
            .call(|txn| Ok(txn.put_local("one", b"v")?))
            .await
            .unwrap();
        handle
            .call(|txn| {
                txn.inject_commit_failure();
                Ok(())
            })
            .await
            .unwrap();
        assert!(handle
            .call(|txn| Ok(txn.put_local("two", b"v")?))
            .await
            .is_err());
        assert_eq!(locals(&handle).await, ["one"]);
        handle
            .call(|txn| Ok(txn.put_local("three", b"v")?))
            .await
            .unwrap();
        cleanup(handle, file);
    }

    #[tokio::test]
    async fn jobs_run_on_a_dedicated_thread_not_a_tokio_worker() {
        let (handle, file) = handle("thread");
        let (in_runtime, name) = handle
            .call(|_| {
                Ok((
                    tokio::runtime::Handle::try_current().is_ok(),
                    std::thread::current().name().map(str::to_string),
                ))
            })
            .await
            .unwrap();
        assert!(!in_runtime, "shard job ran inside a Tokio runtime");
        assert!(
            name.as_deref().is_some_and(|n| n.starts_with("shard-")),
            "unexpected thread name {name:?}"
        );
        cleanup(handle, file);
    }

    #[tokio::test]
    async fn dropping_the_handle_releases_the_file() {
        let (handle, file) = handle("drop");
        handle
            .call(|txn| Ok(txn.put_local("kept", b"v")?))
            .await
            .unwrap();
        let handle = Arc::into_inner(handle).unwrap();
        drop(handle);
        // No waiting: redb's lock on the file must already be released.
        let mut reopened = open(&file);
        let kept = reopened.read(|txn| Ok(txn.get_local("kept")?)).unwrap();
        assert_eq!(kept.as_deref(), Some(b"v".as_slice()));
        drop(reopened);
        kv::forget_verified(&file);
        let _ = std::fs::remove_dir_all(file.parent().unwrap());
    }

    /// A handle closed normally is checked and recorded verified; one
    /// discarded (a database whose open failed) is closed unchecked and
    /// records nothing.
    #[tokio::test]
    async fn a_discarded_handle_closes_its_file_unchecked() {
        let (handle, file) = handle("discard");
        let handle = Arc::into_inner(handle).unwrap();
        handle.discard();
        assert!(!kv::is_verified(&file), "a discarded shard was verified");
        let reopened = ShardHandle::spawn(&file, 1000, tests::CACHE, Default::default()).unwrap();
        drop(reopened);
        assert!(kv::is_verified(&file), "a normal close was not verified");
        kv::forget_verified(&file);
        let _ = std::fs::remove_dir_all(file.parent().unwrap());
    }

    #[tokio::test]
    async fn data_written_before_a_panic_survives_and_the_partial_job_is_discarded() {
        let (handle, file) = handle("partial");
        handle
            .call(|txn| Ok(txn.put_local("before", b"v")?))
            .await
            .unwrap();
        let result: std::result::Result<(), ApiError> = handle
            .call(|txn| {
                txn.put_local("partial", b"v")?;
                panic!("after partial write");
            })
            .await;
        assert!(result.is_err());
        assert_eq!(locals(&handle).await, ["before"]);
        cleanup(handle, file);
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::tests::{write_doc, CACHE};
    use super::*;
    use crate::kv::powercut::{PowerCut, PowerSwitch};
    use std::io::ErrorKind;
    use std::sync::atomic::AtomicBool;
    use tokio::sync::{mpsc::UnboundedReceiver, watch};

    /// A shard on its own actor over a test disk. Every open after the
    /// first (a recovery) reports on `opening` whether `held` was set, then
    /// waits for `go`.
    struct Rig {
        switch: PowerSwitch,
        handle: Arc<ShardHandle>,
        held: Arc<AtomicBool>,
        opening: UnboundedReceiver<bool>,
        go: std::sync::mpsc::Sender<()>,
        committed: Arc<watch::Sender<()>>,
    }

    /// A rig whose failed shard retries on the next request.
    fn rig() -> Rig {
        rig_retrying_after(Duration::ZERO)
    }

    /// A rig whose failed shard waits `wait` before its next retry.
    fn rig_retrying_after(wait: Duration) -> Rig {
        let (_, switch) = PowerCut::new();
        let held = Arc::new(AtomicBool::new(false));
        let (opening_tx, opening) = tokio::sync::mpsc::unbounded_channel();
        let (go, go_rx) = std::sync::mpsc::channel::<()>();
        let committed: Arc<watch::Sender<()>> = Default::default();
        let disk = switch.clone();
        let rig_switch = switch.clone();
        let lease_held = Arc::clone(&held);
        let mut first = true;
        let open: Opener = Box::new(move || {
            let creating = std::mem::take(&mut first);
            if !creating {
                let _ = opening_tx.send(lease_held.load(Ordering::SeqCst));
                go_rx.recv()?;
            }
            let db = kv::open_with(disk.backend(), CACHE)?;
            // Only the first open makes the file, as `create_file` would; a
            // recovery reopens what the disk holds.
            if creating {
                init_tables(&db)?;
            }
            Ok(db)
        });
        let handle = ShardHandle::spawn_with(
            "shard-recovery".to_string(),
            Arc::clone(&committed),
            move || Shard::over_test_opener(open, &rig_switch, 1000),
        )
        .unwrap();
        handle.set_retry_wait(wait);
        Rig {
            switch,
            handle: Arc::new(handle),
            held,
            opening,
            go,
            committed,
        }
    }

    /// Write a new document `id`; its sequence.
    async fn put(handle: &ShardHandle, id: &str, n: u64) -> std::result::Result<u64, ApiError> {
        let id = id.to_string();
        handle
            .call(move |txn| {
                write_doc(txn, &id, false, b"{}", n)?;
                Ok(txn.staged.seq)
            })
            .await
    }

    /// A write on a full disk, started.
    fn put_on_full_disk(
        rig: &Rig,
        id: &'static str,
    ) -> tokio::task::JoinHandle<std::result::Result<u64, ApiError>> {
        rig.switch.fail_writes(ErrorKind::StorageFull);
        let handle = Arc::clone(&rig.handle);
        tokio::spawn(async move { put(&handle, id, 99).await })
    }

    /// Wait until the shard reopens its file, failing if `write` is answered
    /// first: the answer to a failed commit comes after the recovery.
    async fn reopening<T: std::fmt::Debug>(
        rig: &mut Rig,
        write: &mut tokio::task::JoinHandle<std::result::Result<T, ApiError>>,
    ) -> bool {
        tokio::select! {
            held = rig.opening.recv() => held.unwrap(),
            answer = write => panic!("answered without reopening the file: {answer:?}"),
        }
    }

    /// The sequence index in a lease of its own: (sequence, id) pairs.
    async fn by_seq(handle: &ShardHandle) -> Vec<(u64, String)> {
        handle
            .lease(|s| {
                let mut rows = Vec::new();
                s.txn.scan(Table::BySeq, .., false, |key, id| {
                    rows.push((gate::key::split_seq(key)?, String::from_utf8(id.to_vec())?));
                    Ok(true)
                })?;
                Ok(rows)
            })
            .await
            .unwrap()
    }

    /// The shard's external size as its actor holds it and as its handle
    /// publishes it.
    async fn external(handle: &ShardHandle) -> (u64, u64) {
        let held = handle.read(|txn| Ok(txn.external_size())).await.unwrap();
        (held, handle.external_size())
    }

    fn rows(ids: &[(u64, &str)]) -> Vec<(u64, String)> {
        ids.iter().map(|(seq, id)| (*seq, id.to_string())).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_commit_reopens_the_shard_and_keeps_acknowledged_writes() {
        let mut rig = rig();
        assert_eq!(put(&rig.handle, "a", 1).await.unwrap(), 1);
        assert_eq!(put(&rig.handle, "b", 2).await.unwrap(), 2);

        let mut failing = put_on_full_disk(&rig, "lost");
        reopening(&mut rig, &mut failing).await;
        rig.switch.restore_writes();
        rig.go.send(()).unwrap();
        let error = failing
            .await
            .unwrap()
            .expect_err("a write to a full disk was acknowledged");
        assert_eq!(error.status, 500, "{error:?}");

        assert_eq!(rig.handle.state(), ShardState::Serving);
        assert_eq!(rig.handle.committed_seq(), 2);
        assert_eq!(put(&rig.handle, "c", 3).await.unwrap(), 3);
        assert_eq!(
            by_seq(&rig.handle).await,
            rows(&[(1, "a"), (2, "b"), (3, "c")])
        );
        let counts = rig.handle.read(|txn| Ok(txn.counts())).await.unwrap();
        assert_eq!(counts, (3, 0));

        // And on disk.
        let Rig { switch, handle, .. } = rig;
        drop(Arc::into_inner(handle).unwrap());
        let mut shard = Shard::over_backend(&switch, 1000).unwrap();
        let live: Vec<String> = shard
            .read(|txn| Ok(txn.live_docs()?.keys().cloned().collect()))
            .unwrap();
        assert_eq!(live, ["a", "b", "c"]);
        assert_eq!(shard.seq(), 3);
    }

    /// A shard whose file cannot be reopened fails closed; requests to it
    /// retry the reopen, which brings it back once the fault has cleared.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_shard_that_cannot_reopen_fails_closed_until_a_retry_reopens_it() {
        let mut rig = rig();
        put(&rig.handle, "a", 1).await.unwrap();

        let mut failing = put_on_full_disk(&rig, "lost");
        reopening(&mut rig, &mut failing).await;
        // A job queued behind the recovery.
        let queued = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move { handle.read(|txn| Ok(txn.counts())).await }
        });
        while rig.handle.queued() < 1 {
            tokio::task::yield_now().await;
        }
        // The disk is still full, so the file cannot be opened.
        rig.go.send(()).unwrap();
        assert_eq!(failing.await.unwrap().unwrap_err().status, 500);
        let error = queued.await.unwrap().unwrap_err();
        assert!(error.reason.contains("has failed"), "{error:?}");
        assert!(
            matches!(rig.handle.state(), ShardState::Failed(_)),
            "{:?}",
            rig.handle.state()
        );

        // A request answers 500 and has the shard retry; the disk is still
        // full, so the retry fails too.
        refused(rig.handle.lease(|s| Ok(s.get_meta("a")?.is_some())).await);
        let mut checked = barrier(&rig.handle);
        reopening(&mut rig, &mut checked).await;
        rig.go.send(()).unwrap();
        refused(checked.await.unwrap());
        assert!(
            matches!(rig.handle.state(), ShardState::Failed(_)),
            "{:?}",
            rig.handle.state()
        );

        // Room on the disk again: the next request still answers 500, and
        // its retry brings the shard back.
        rig.switch.restore_writes();
        refused(put(&rig.handle, "b", 2).await);
        let mut checked = barrier(&rig.handle);
        reopening(&mut rig, &mut checked).await;
        rig.go.send(()).unwrap();
        checked.await.unwrap().unwrap();
        assert_eq!(rig.handle.state(), ShardState::Serving);
        assert_eq!(rig.handle.committed_seq(), 1);
        assert_eq!(put(&rig.handle, "b", 2).await.unwrap(), 2);
        assert_eq!(
            rig.handle.read(|txn| Ok(txn.counts())).await.unwrap(),
            (2, 0)
        );
        assert!(rig
            .handle
            .lease(|s| Ok(s.get_meta("a")?.is_some()))
            .await
            .unwrap());
        assert_eq!(rig.handle.alone(|shard| Ok(shard.seq())).await.unwrap(), 2);

        let Rig { switch, handle, .. } = rig;
        drop(Arc::into_inner(handle).unwrap());
        let mut shard = Shard::over_backend(&switch, 1000).unwrap();
        let live: Vec<String> = shard
            .read(|txn| Ok(txn.live_docs()?.keys().cloned().collect()))
            .unwrap();
        assert_eq!(live, ["a", "b"]);
    }

    /// A commit can report an error and still be on the disk (here, its
    /// writes reached the disk and only the sync failed). The recovered
    /// shard serves exactly what the file holds: the landed write, its
    /// sequence and counters, and the published position advanced to it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_commit_that_landed_is_served_after_recovery() {
        let mut rig = rig();
        put(&rig.handle, "a", 1).await.unwrap();
        let mut commits = rig.committed.subscribe();
        commits.mark_unchanged();

        // The commit's pages sync; the sync of the header that makes them
        // the newest commit fails, after the header was written.
        rig.switch.fail_syncs_after(1, ErrorKind::Other);
        let mut failing = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move { put(&handle, "landed", 2).await }
        });
        reopening(&mut rig, &mut failing).await;
        rig.switch.restore_writes();
        rig.go.send(()).unwrap();
        let error = failing.await.unwrap().unwrap_err();
        assert!(error.reason.starts_with("shard commit failed"), "{error:?}");

        assert_eq!(rig.handle.state(), ShardState::Serving);
        assert_eq!(
            rig.handle.committed_seq(),
            2,
            "the landed commit is published"
        );
        assert!(commits.has_changed().unwrap(), "feeds were not woken");
        assert_eq!(by_seq(&rig.handle).await, rows(&[(1, "a"), (2, "landed")]));
        let (counts, latest) = rig
            .handle
            .read(|txn| {
                let latest: Vec<(u64, String)> = txn
                    .latest_changes()?
                    .after(0)
                    .map(|(seq, id)| (seq, id.to_string()))
                    .collect();
                Ok((txn.counts(), latest))
            })
            .await
            .unwrap();
        assert_eq!(counts, (2, 0));
        assert_eq!(latest, rows(&[(1, "a"), (2, "landed")]));
        // Each document's body is `{}`: the landed one's counts, in the
        // file and in what the handle publishes.
        assert_eq!(external(&rig.handle).await, (4, 4));
        assert_eq!(put(&rig.handle, "c", 3).await.unwrap(), 3);
        assert_eq!(rig.handle.committed_seq(), 3);
        assert_eq!(external(&rig.handle).await, (6, 6));
    }

    /// A purge whose commit reports an error and still lands is in the
    /// purge sequence the recovered shard publishes, so an index that read
    /// the shard before it rebuilds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_purge_commit_that_landed_moves_the_published_purge_sequence() {
        let mut rig = rig();
        put(&rig.handle, "a", 1).await.unwrap();
        assert_eq!(rig.handle.purge_seq(), 0);

        // As above: the header that makes the commit the newest is written,
        // and its sync fails.
        rig.switch.fail_syncs_after(1, ErrorKind::Other);
        let mut failing = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move {
                handle
                    .call(|txn| {
                        let info = PurgeInfo {
                            uuid: "landed".to_string(),
                            id: "gone".to_string(),
                            revs: vec![format!("1-{:032x}", 1)],
                        };
                        Ok(txn.record_purge(&info, &[])?)
                    })
                    .await
            }
        });
        reopening(&mut rig, &mut failing).await;
        rig.switch.restore_writes();
        rig.go.send(()).unwrap();
        let error = failing.await.unwrap().unwrap_err();
        assert!(error.reason.starts_with("shard commit failed"), "{error:?}");

        assert_eq!(rig.handle.state(), ShardState::Serving);
        assert_eq!(rig.handle.purge_seq(), 1, "the landed purge is published");
    }

    /// A purge whose commit reports an error may have landed. If the reopen
    /// after it fails too, the shard cannot tell, so it publishes the purge
    /// sequence the commit would have reached: an index that read the shard
    /// before it rebuilds (and meets the failed shard) rather than serve
    /// the purged document.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_purge_commit_whose_reopen_fails_publishes_the_purge() {
        let mut rig = rig();
        put(&rig.handle, "a", 1).await.unwrap();

        rig.switch.fail_syncs_after(1, ErrorKind::Other);
        let mut failing = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move {
                handle
                    .call(|txn| {
                        let info = PurgeInfo {
                            uuid: "landed".to_string(),
                            id: "gone".to_string(),
                            revs: vec![format!("1-{:032x}", 1)],
                        };
                        Ok(txn.record_purge(&info, &[])?)
                    })
                    .await
            }
        });
        reopening(&mut rig, &mut failing).await;
        // The file cannot be read, so the reopen fails.
        rig.switch.fail_reads(ErrorKind::Other);
        rig.go.send(()).unwrap();
        let error = failing.await.unwrap().unwrap_err();
        assert!(error.reason.starts_with("shard commit failed"), "{error:?}");

        assert!(matches!(rig.handle.state(), ShardState::Failed(_)));
        assert_eq!(
            rig.handle.purge_seq(),
            UNKNOWN_PURGE_SEQ,
            "the purge that may have landed is not published as unknown"
        );
    }

    /// A purge sequence published while a failed commit's reopen runs must
    /// never be one the shard reaches later. Here the purge's commit fails
    /// without landing and the reopen succeeds: an index that took the
    /// published value during the reopen (as a rebuild started then would)
    /// must not find it current again after the next real purge, or it would
    /// serve rows read before that purge.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_purge_sequence_published_while_a_failed_commit_reopens_is_never_reached() {
        let mut rig = rig();
        put(&rig.handle, "a", 1).await.unwrap();
        let info = |uuid: &str| PurgeInfo {
            uuid: uuid.to_string(),
            id: "gone".to_string(),
            revs: vec![format!("1-{:032x}", 1)],
        };

        rig.switch.fail_writes(ErrorKind::StorageFull);
        let mut failing = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            let info = info("lost");
            async move {
                handle
                    .call(move |txn| Ok(txn.record_purge(&info, &[])?))
                    .await
            }
        });
        reopening(&mut rig, &mut failing).await;
        let during = rig.handle.purge_seq();
        assert_eq!(
            during, UNKNOWN_PURGE_SEQ,
            "the reopen window published {during}"
        );
        rig.switch.restore_writes();
        rig.go.send(()).unwrap();
        failing.await.unwrap().unwrap_err();
        assert_eq!(rig.handle.state(), ShardState::Serving);
        assert_eq!(rig.handle.purge_seq(), 0, "the lost purge is published");

        let real = info("real");
        rig.handle
            .call(move |txn| Ok(txn.record_purge(&real, &[])?))
            .await
            .unwrap();
        assert_eq!(rig.handle.purge_seq(), 1);
        assert_ne!(
            during,
            rig.handle.purge_seq(),
            "a value published during the reopen came back with a real purge"
        );
    }

    /// The same for a failed clear: it stages a new uuid and a purge
    /// sequence of 0, while the published uuid stays the old one. Publishing
    /// that 0 would pair the old uuid with a value an index built before any
    /// purge recorded.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_clear_never_publishes_a_purge_sequence_of_the_old_copy() {
        let mut rig = rig();
        put(&rig.handle, "a", 1).await.unwrap();
        let before = rig.handle.purge_seq();
        assert_eq!(before, 0);

        rig.switch.fail_writes(ErrorKind::StorageFull);
        let mut failing = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move { handle.call(|txn| Ok(txn.clear()?)).await }
        });
        reopening(&mut rig, &mut failing).await;
        let during = rig.handle.purge_seq();
        assert_eq!(
            during, UNKNOWN_PURGE_SEQ,
            "the reopen window published {during}"
        );
        rig.switch.restore_writes();
        rig.go.send(()).unwrap();
        failing.await.unwrap().unwrap_err();
        assert_ne!(
            during, before,
            "an index built before the clear still looks current during the reopen"
        );
    }

    /// A commit whose first sync, of its pages, fails never lands: the
    /// header that would make it the newest commit is not written. The
    /// recovered shard serves the commit before it, and so does the disk
    /// after a power cut.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_commit_whose_page_sync_fails_is_not_served_after_recovery() {
        let mut rig = rig();
        put(&rig.handle, "a", 1).await.unwrap();

        rig.switch.fail_syncs_after(0, ErrorKind::Other);
        let mut failing = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move { put(&handle, "lost", 2).await }
        });
        reopening(&mut rig, &mut failing).await;
        rig.switch.restore_writes();
        rig.go.send(()).unwrap();
        let error = failing.await.unwrap().unwrap_err();
        assert!(error.reason.starts_with("shard commit failed"), "{error:?}");

        assert_eq!(rig.handle.state(), ShardState::Serving);
        assert_eq!(rig.handle.committed_seq(), 1);
        assert_eq!(by_seq(&rig.handle).await, rows(&[(1, "a")]));
        let lost = rig
            .handle
            .lease(|s| Ok(s.get_meta("lost")?.is_some()))
            .await
            .unwrap();
        assert!(!lost, "the failed commit is served");
        let counts = rig.handle.read(|txn| Ok(txn.counts())).await.unwrap();
        assert_eq!(counts, (1, 0));
        // Each document's body is `{}`: the lost one's does not count.
        assert_eq!(external(&rig.handle).await, (2, 2));
        assert_eq!(put(&rig.handle, "c", 2).await.unwrap(), 2);
        assert_eq!(external(&rig.handle).await, (4, 4));

        let Rig { switch, handle, .. } = rig;
        drop(Arc::into_inner(handle).unwrap());
        switch.cut();
        let mut shard = Shard::over_backend(&switch, 1000).unwrap();
        let live: Vec<String> = shard
            .read(|txn| Ok(txn.live_docs()?.keys().cloned().collect()))
            .unwrap();
        assert_eq!(live, ["a", "c"]);
        assert_eq!(shard.seq(), 2);
    }

    /// The published position never goes back: a file that comes back
    /// without changes that were acknowledged (here, a disk that ignored
    /// syncs and then lost power) fails the shard instead, for good: no
    /// request retries it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reopened_file_missing_acknowledged_changes_fails_closed() {
        let mut rig = rig();
        put(&rig.handle, "a", 1).await.unwrap();
        rig.switch.ignore_syncs();
        put(&rig.handle, "b", 2).await.unwrap();
        rig.switch.cut();

        let mut failing = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move { put(&handle, "c", 3).await }
        });
        reopening(&mut rig, &mut failing).await;
        rig.go.send(()).unwrap();
        assert_eq!(failing.await.unwrap().unwrap_err().status, 500);
        match rig.handle.state() {
            ShardState::Failed(reason) => assert!(reason.contains("behind"), "{reason}"),
            state => panic!("served a file that lost acknowledged changes: {state:?}"),
        }
        assert_eq!(rig.handle.committed_seq(), 2);
        assert!(rig.handle.gate.is_shut(), "a file behind can be retried");

        // No reopen could move the file forward, so however many requests
        // arrive, none retries it, although no wait holds them back. Opens
        // are let through, so a retry shows as a count, not a hang.
        for _ in 0..8 {
            rig.go.send(()).unwrap();
        }
        for _ in 0..4 {
            refused(rig.handle.lease(|s| Ok(s.get_meta("a")?.is_some())).await);
            refused(rig.handle.read(|txn| Ok(txn.counts())).await);
            refused(put(&rig.handle, "d", 4).await);
            refused(barrier(&rig.handle).await.unwrap());
        }
        assert!(
            rig.opening.try_recv().is_err(),
            "a file behind was reopened"
        );
        match rig.handle.state() {
            ShardState::Failed(reason) => assert!(reason.contains("behind"), "{reason}"),
            state => panic!("served a file that lost acknowledged changes: {state:?}"),
        }
        assert_eq!(rig.handle.committed_seq(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovery_waits_for_a_reader_holding_a_lease() {
        let mut rig = rig();
        put(&rig.handle, "a", 1).await.unwrap();

        let (entered_tx, entered_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let reader = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            let held = Arc::clone(&rig.held);
            async move {
                handle
                    .lease(move |s| {
                        held.store(true, Ordering::SeqCst);
                        let found = s.get_meta("a")?.is_some();
                        entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        held.store(false, Ordering::SeqCst);
                        Ok(found)
                    })
                    .await
            }
        });
        entered_rx.await.unwrap();

        let mut failing = put_on_full_disk(&rig, "lost");
        // The recovery has closed the gate and waits for the lease.
        rig.handle.gate.closed().await;
        release_tx.send(()).unwrap();
        assert!(
            !reopening(&mut rig, &mut failing).await,
            "reopened while a lease was out"
        );
        assert!(reader.await.unwrap().unwrap());

        // A read asked for during the reopen waits, then runs on the new file.
        let waiting = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move { handle.lease(|s| Ok(s.get_meta("a")?.is_some())).await }
        });
        rig.handle.gate.waiting_reads(1).await;
        rig.switch.restore_writes();
        rig.go.send(()).unwrap();
        assert_eq!(failing.await.unwrap().unwrap_err().status, 500);
        assert!(waiting.await.unwrap().unwrap());
        assert_eq!(rig.handle.state(), ShardState::Serving);
        assert_eq!(put(&rig.handle, "b", 2).await.unwrap(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_io_error_inside_a_group_reopens_the_shard() {
        let mut rig = rig();
        put(&rig.handle, "a", 1).await.unwrap();

        rig.switch.fail_writes(ErrorKind::StorageFull);
        // Too big for the page cache: its pages reach the disk, and fail,
        // before the group gets to commit.
        let mut failing = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move {
                handle
                    .call(|txn| write_doc(txn, "big", false, &vec![b'x'; 8 << 20], 2))
                    .await
            }
        });
        reopening(&mut rig, &mut failing).await;
        rig.switch.restore_writes();
        rig.go.send(()).unwrap();
        let error = failing.await.unwrap().unwrap_err();
        assert_eq!(error.status, 500, "{error:?}");
        assert!(
            !error.reason.starts_with("shard commit failed"),
            "the write did not fail before the commit: {error:?}"
        );
        assert_eq!(rig.handle.state(), ShardState::Serving);
        assert_eq!(put(&rig.handle, "b", 2).await.unwrap(), 2);
        assert_eq!(by_seq(&rig.handle).await, rows(&[(1, "a"), (2, "b")]));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handle_left_failing_by_a_job_run_alone_is_reopened() {
        let mut rig = rig();
        put(&rig.handle, "a", 1).await.unwrap();

        rig.switch.fail_writes(ErrorKind::StorageFull);
        let mut failed = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move {
                handle
                    .alone(|shard| {
                        let commit = || -> Result<()> {
                            let txn = shard.gate.begin_write_txn()?;
                            txn.open_table(LOCAL)?
                                .insert("x", kv::seal(b"x").as_slice())?;
                            txn.commit()?;
                            Ok(())
                        };
                        commit().map_err(ApiError::from)
                    })
                    .await
            }
        });
        // The actor reopens the file before the job answers, so neither its
        // caller nor the next job meets the failing handle.
        reopening(&mut rig, &mut failed).await;
        rig.switch.restore_writes();
        rig.go.send(()).unwrap();
        assert!(failed.await.unwrap().is_err());
        assert_eq!(
            rig.handle.read(|txn| Ok(txn.counts())).await.unwrap(),
            (1, 0)
        );
        assert_eq!(put(&rig.handle, "b", 2).await.unwrap(), 2);
        assert_eq!(rig.handle.state(), ShardState::Serving);
    }

    /// A read job queued straight on the actor, past the handle's own
    /// answer for a failed shard: it answers once the actor has handled
    /// everything queued before it.
    fn barrier(handle: &ShardHandle) -> tokio::task::JoinHandle<std::result::Result<(), ApiError>> {
        let answer = handle.barrier();
        tokio::spawn(async move { answer.await.unwrap() })
    }

    /// A request to a shard that has failed.
    fn refused<T: std::fmt::Debug>(result: std::result::Result<T, ApiError>) {
        let error = result.expect_err("a failed shard answered");
        assert_eq!(error.status, 500, "{error:?}");
    }

    /// Write `big`, a document too big for the page cache, so reading its
    /// body reaches the disk.
    async fn put_big(handle: &ShardHandle, n: u64) {
        handle
            .call(move |txn| write_doc(txn, "big", false, &vec![b'x'; 4 * CACHE], n))
            .await
            .unwrap();
    }

    /// The bytes of every stored body, read in a lease of its own.
    async fn body_bytes(handle: &ShardHandle) -> std::result::Result<usize, ApiError> {
        handle
            .lease(|s| {
                let mut bytes = 0;
                s.txn.scan(Table::Bodies, .., false, |_, body| {
                    bytes += body.len();
                    Ok(true)
                })?;
                Ok(bytes)
            })
            .await
    }

    /// A read that meets an I/O error leaves the handle failing; the shard
    /// reopens it although nothing writes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_read_that_meets_an_io_error_reopens_the_shard_without_a_write() {
        let mut rig = rig();
        put(&rig.handle, "a", 1).await.unwrap();
        put_big(&rig.handle, 2).await;
        let bytes = body_bytes(&rig.handle).await.unwrap();

        rig.switch.fail_reads(ErrorKind::Other);
        refused(body_bytes(&rig.handle).await);
        let mut checked = barrier(&rig.handle);
        reopening(&mut rig, &mut checked).await;
        rig.switch.restore_reads();
        rig.go.send(()).unwrap();
        checked.await.unwrap().unwrap();

        assert_eq!(rig.handle.state(), ShardState::Serving);
        assert_eq!(body_bytes(&rig.handle).await.unwrap(), bytes);
        assert_eq!(rig.handle.committed_seq(), 2);
    }

    /// Reads that fail while a check is queued queue no other: the shard
    /// reopens its file once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn many_failing_reads_reopen_the_shard_once() {
        let mut rig = rig();
        put(&rig.handle, "a", 1).await.unwrap();
        put_big(&rig.handle, 2).await;

        // The actor is held on a job, so whatever the reads queue waits.
        let (entered_tx, entered) = oneshot::channel::<()>();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let holding = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move {
                handle
                    .read(move |_| {
                        entered_tx.send(()).unwrap();
                        held.recv().unwrap();
                        Ok(())
                    })
                    .await
            }
        });
        entered.await.unwrap();

        rig.switch.fail_reads(ErrorKind::Other);
        for _ in 0..8 {
            refused(body_bytes(&rig.handle).await);
        }
        assert_eq!(rig.handle.queued(), 1, "not one check queued");

        // Two opens allowed, so a second reopen shows as a count, not a hang.
        rig.switch.restore_reads();
        rig.go.send(()).unwrap();
        rig.go.send(()).unwrap();
        let checked = barrier(&rig.handle);
        release.send(()).unwrap();
        holding.await.unwrap().unwrap();
        checked.await.unwrap().unwrap();
        assert!(rig.opening.try_recv().is_ok(), "the file was not reopened");
        assert!(
            rig.opening.try_recv().is_err(),
            "the file was reopened twice"
        );
        assert_eq!(rig.handle.state(), ShardState::Serving);
        assert!(body_bytes(&rig.handle).await.is_ok());
    }

    /// A read job on the actor that meets an I/O error has the shard reopen
    /// its file before it answers, although nothing writes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_read_job_that_meets_an_io_error_reopens_the_shard() {
        let mut rig = rig();
        put(&rig.handle, "a", 1).await.unwrap();
        put_big(&rig.handle, 2).await;

        rig.switch.fail_reads(ErrorKind::Other);
        let mut failing = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move {
                handle
                    .read(|txn| {
                        let mut bytes = 0;
                        let all = (Unbounded, Unbounded);
                        txn.scan(BODIES, Table::Bodies, all, false, |_, body| {
                            bytes += body.len();
                            Ok(true)
                        })?;
                        Ok(bytes)
                    })
                    .await
            }
        });
        reopening(&mut rig, &mut failing).await;
        rig.switch.restore_reads();
        rig.go.send(()).unwrap();
        refused(failing.await.unwrap());
        assert_eq!(rig.handle.state(), ShardState::Serving);
        assert!(body_bytes(&rig.handle).await.is_ok());
    }

    /// Jobs parked for a failed shard run once a retry brings it back; if
    /// one of them fails it again (its read meets an I/O error and the
    /// reopen fails), it and the jobs not yet run wait for the next retry,
    /// in the order they were parked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn parked_jobs_left_when_the_shard_fails_again_wait_for_the_next_retry() {
        let mut rig = rig();
        put(&rig.handle, "a", 1).await.unwrap();
        put_big(&rig.handle, 2).await;
        let mut failing = put_on_full_disk(&rig, "lost");
        reopening(&mut rig, &mut failing).await;
        rig.go.send(()).unwrap();
        refused(failing.await.unwrap());
        rig.switch.restore_writes();

        // The first job's first read meets the fault it sets.
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut fault = Some(rig.switch.clone());
        let log = Arc::clone(&order);
        rig.handle
            .when_serving()
            .run(
                move |txn| {
                    log.lock().unwrap().push("first read");
                    if let Some(switch) = fault.take() {
                        switch.fail_reads(ErrorKind::Other);
                    }
                    let all = (Unbounded, Unbounded);
                    txn.scan(BODIES, Table::Bodies, all, false, |_, _| Ok(true))
                },
                |_| {},
            )
            .await;
        let (ran_tx, mut ran) = oneshot::channel::<()>();
        let log = Arc::clone(&order);
        rig.handle
            .when_serving()
            .run(
                |_| Ok(()),
                move |()| {
                    log.lock().unwrap().push("second");
                    ran_tx.send(()).unwrap();
                },
            )
            .await;

        // The retry succeeds; the first job's read fails, and so does the
        // reopen after it, while reads still fail.
        refused(rig.handle.lease(|s| Ok(s.get_meta("a")?.is_some())).await);
        let mut checked = barrier(&rig.handle);
        reopening(&mut rig, &mut checked).await;
        rig.go.send(()).unwrap();
        reopening(&mut rig, &mut checked).await;
        rig.go.send(()).unwrap();
        refused(checked.await.unwrap());
        assert!(matches!(rig.handle.state(), ShardState::Failed(_)));
        assert!(
            matches!(ran.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "the job left was run on the failed shard, or dropped"
        );

        rig.switch.restore_reads();
        refused(rig.handle.lease(|s| Ok(s.get_meta("a")?.is_some())).await);
        let mut checked = barrier(&rig.handle);
        reopening(&mut rig, &mut checked).await;
        rig.go.send(()).unwrap();
        checked.await.unwrap().unwrap();
        assert_eq!(rig.handle.state(), ShardState::Serving);
        ran.await.expect("the parked job was dropped");
        assert_eq!(
            *order.lock().unwrap(),
            ["first read", "first read", "second"],
            "the job whose read failed was not run again first"
        );
    }

    /// A parked job whose read meets an I/O failure on every run, while
    /// every reopen succeeds, runs (and so has the file reopened) at most
    /// once per retry wait, not over and over: until the wait is over the
    /// shard serves requests and reopens nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_job_whose_read_always_fails_reopens_the_shard_once_per_wait() {
        let mut rig = rig_retrying_after(Duration::from_secs(3600));
        put(&rig.handle, "a", 1).await.unwrap();
        put_big(&rig.handle, 2).await;
        let mut failing = put_on_full_disk(&rig, "lost");
        reopening(&mut rig, &mut failing).await;
        rig.go.send(()).unwrap();
        refused(failing.await.unwrap());
        rig.switch.restore_writes();

        // Every run of its read meets a passing fault.
        let runs = Arc::new(AtomicU64::new(0));
        let (switch, counted) = (rig.switch.clone(), Arc::clone(&runs));
        rig.handle
            .when_serving()
            .run(
                move |txn| {
                    counted.fetch_add(1, Ordering::SeqCst);
                    switch.fail_one_read(ErrorKind::Other);
                    let all = (Unbounded, Unbounded);
                    txn.scan(BODIES, Table::Bodies, all, false, |_, _| Ok(true))
                },
                |_| {},
            )
            .await;

        // Opens are let through, so any reopen shows as a count, not a hang.
        for _ in 0..64 {
            rig.go.send(()).unwrap();
        }
        let mut reopens = 0;
        for round in 1..=3 {
            // The wait is over: a request has the shard run the job (the
            // first time, retry the failed shard first), and the job's
            // failed read has the file reopened.
            // The nudge reads nothing from the disk, so it cannot take the
            // job's passing fault.
            rig.handle.checkup.retry().at = Instant::now();
            let _ = rig.handle.read(|txn| Ok(txn.counts())).await;
            barrier(&rig.handle).await.unwrap().unwrap();
            assert_eq!(rig.handle.state(), ShardState::Serving);
            assert_eq!(runs.load(Ordering::SeqCst), round);
            while rig.opening.try_recv().is_ok() {
                reopens += 1;
            }
            // Each run's failed read reopens the file once; the first
            // round's retry of the failed shard was one more.
            assert_eq!(reopens, round + 1, "not one reopen per run");

            // The wait is not over: requests are served and run nothing.
            for _ in 0..4 {
                let found = rig.handle.lease(|s| Ok(s.get_meta("a")?.is_some()));
                assert!(found.await.unwrap());
                rig.handle.read(|txn| Ok(txn.counts())).await.unwrap();
            }
            barrier(&rig.handle).await.unwrap().unwrap();
            assert!(
                rig.opening.try_recv().is_err(),
                "reopened before the wait was over"
            );
            assert_eq!(runs.load(Ordering::SeqCst), round);
        }
    }

    /// A parked job whose read keeps meeting an I/O failure waits longer
    /// each time, as a failed shard does (doubling, up to a minute),
    /// although each reopen after its failed read succeeds; once it has
    /// run, the waits start over.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_job_whose_read_keeps_failing_waits_longer_each_time() {
        let mut rig = rig_retrying_after(Duration::from_secs(1));
        put(&rig.handle, "a", 1).await.unwrap();
        put_big(&rig.handle, 2).await;
        let mut failing = put_on_full_disk(&rig, "lost");
        reopening(&mut rig, &mut failing).await;
        rig.go.send(()).unwrap();
        refused(failing.await.unwrap());
        rig.switch.restore_writes();

        let faulty = Arc::new(AtomicBool::new(true));
        let runs = Arc::new(AtomicU64::new(0));
        let (switch, fault, counted) = (rig.switch.clone(), Arc::clone(&faulty), Arc::clone(&runs));
        rig.handle
            .when_serving()
            .run(
                move |txn| {
                    counted.fetch_add(1, Ordering::SeqCst);
                    if fault.load(Ordering::SeqCst) {
                        switch.fail_one_read(ErrorKind::Other);
                    }
                    let all = (Unbounded, Unbounded);
                    txn.scan(BODIES, Table::Bodies, all, false, |_, _| Ok(true))
                },
                |_| {},
            )
            .await;
        for _ in 0..64 {
            rig.go.send(()).unwrap();
        }

        // Each round, the wait is made over by hand and a request (one that
        // reads nothing from the disk) has the actor's own due check run
        // the job. Its read fails, the file is reopened, and the job waits
        // again: the wait it now waits is the one in force before the round.
        let secs = Duration::from_secs;
        for (round, wait) in [1, 2, 4, 8, 16, 32, 60, 60].into_iter().enumerate() {
            rig.handle.checkup.retry().at = Instant::now();
            let before = Instant::now();
            let _ = rig.handle.read(|txn| Ok(txn.counts())).await;
            barrier(&rig.handle).await.unwrap().unwrap();
            let after = Instant::now();
            assert_eq!(runs.load(Ordering::SeqCst), round as u64 + 1);
            assert_eq!(rig.handle.state(), ShardState::Serving);
            let at = rig.handle.checkup.retry().at;
            assert!(
                before + secs(wait) <= at && at <= after + secs(wait),
                "round {}: not a wait of {wait} s",
                round + 1
            );
        }

        // The fault clears: the job runs, and the waits start over.
        faulty.store(false, Ordering::SeqCst);
        rig.handle.checkup.retry().at = Instant::now();
        rig.handle.read(|txn| Ok(txn.counts())).await.unwrap();
        barrier(&rig.handle).await.unwrap().unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), 9);
        assert!(!rig.handle.checkup.waiting.load(Ordering::SeqCst));
        assert_eq!(rig.handle.checkup.retry().wait, secs(1));
    }

    /// A shard that failed for an hour's wait: requests answer 500 and
    /// retry nothing.
    async fn failed_for_an_hour() -> Rig {
        let mut rig = rig_retrying_after(Duration::from_secs(3600));
        put(&rig.handle, "a", 1).await.unwrap();
        let mut failing = put_on_full_disk(&rig, "lost");
        reopening(&mut rig, &mut failing).await;
        rig.go.send(()).unwrap();
        refused(failing.await.unwrap());
        assert!(matches!(rig.handle.state(), ShardState::Failed(_)));
        rig.switch.restore_writes();
        rig
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_shard_answers_500_and_retries_only_once_its_wait_is_over() {
        let mut rig = failed_for_an_hour().await;
        refused(put(&rig.handle, "b", 2).await);
        refused(rig.handle.read(|txn| Ok(txn.counts())).await);
        refused(rig.handle.lease(|s| Ok(s.get_meta("a")?.is_some())).await);
        refused(rig.handle.alone(|shard| Ok(shard.seq())).await);
        assert_eq!(rig.handle.queued(), 0, "a check was queued");
        refused(barrier(&rig.handle).await.unwrap());
        assert!(rig.opening.try_recv().is_err(), "a retry ran early");

        // The wait is over: the next request retries, once.
        rig.handle.checkup.retry().at = Instant::now();
        rig.go.send(()).unwrap();
        refused(rig.handle.lease(|s| Ok(s.get_meta("a")?.is_some())).await);
        barrier(&rig.handle).await.unwrap().unwrap();
        assert!(rig.opening.try_recv().is_ok(), "the shard did not retry");
        assert!(rig.opening.try_recv().is_err(), "the shard retried twice");
        assert_eq!(rig.handle.state(), ShardState::Serving);
    }

    /// The actor keeps the wait too: a check that reaches it early retries
    /// nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_check_before_the_wait_is_over_retries_nothing() {
        let mut rig = failed_for_an_hour().await;
        let queued = rig.handle.tx.as_ref().unwrap().try_send(Message::Check);
        assert!(queued.is_ok());
        refused(barrier(&rig.handle).await.unwrap());
        assert!(rig.opening.try_recv().is_err(), "a retry ran early");
        assert!(matches!(rig.handle.state(), ShardState::Failed(_)));
    }

    #[test]
    fn failed_commit_recovery_invalidates_a_recording_journal() {
        let (_, switch) = PowerCut::new();
        let mut shard = Shard::create_over_backend(&switch, 1000).unwrap();
        shard.journal.start();
        let failed = shard.write(|txn| {
            write_doc(txn, "a", false, b"{}", 1)?;
            txn.inject_commit_failure();
            Ok(())
        });
        assert!(failed.is_err());
        assert_eq!(*shard.state.read().unwrap(), ShardState::Serving);
        assert_eq!(shard.journal.take(), journal::Taken::Invalidated);
    }

    /// A test disk that lost its settings is reopened as the damaged file
    /// it is, not initialised again as a new shard: only the first open of a
    /// test disk creates its tables.
    #[test]
    fn a_test_disk_that_lost_its_settings_does_not_reopen_as_new() {
        let (_, switch) = PowerCut::new();
        let shard = Shard::create_over_backend(&switch, 1000).unwrap();
        let txn = shard.gate.begin_write_txn().unwrap();
        txn.open_table(SETTINGS)
            .unwrap()
            .retain(|_, _| false)
            .unwrap();
        txn.commit().unwrap();
        drop(shard);
        assert!(
            Shard::over_backend(&switch, 1000).is_err(),
            "a shard file without settings opened"
        );
    }

    #[test]
    fn a_handle_left_failing_is_reopened_before_the_next_group() {
        let (_, switch) = PowerCut::new();
        let mut shard = Shard::create_over_backend(&switch, 1000).unwrap();
        shard
            .write(|txn| write_doc(txn, "a", false, b"{}", 1))
            .unwrap();
        // A commit outside any group fails, and the handle with it.
        switch.fail_writes(ErrorKind::StorageFull);
        let txn = shard.gate.begin_write_txn().unwrap();
        txn.open_table(LOCAL)
            .unwrap()
            .insert("x", kv::seal(b"x").as_slice())
            .unwrap();
        assert!(txn.commit().is_err());
        switch.restore_writes();
        assert!(shard.poisoned());

        shard
            .write(|txn| write_doc(txn, "b", false, b"{}", 2))
            .unwrap();
        assert!(!shard.poisoned());
        assert_eq!(shard.seq(), 2);
        assert_eq!(*shard.state.read().unwrap(), ShardState::Serving);
    }

    /// A continuous feed reading the sequence index a page (one lease) at a
    /// time up to the published committed position, open across a recovery:
    /// it does not hold up the reopen, and gets every acknowledged change
    /// once, in order, and nothing of the failed group.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_continuous_feed_survives_recovery_without_skipping() {
        let mut rig = rig();
        for n in 1..=5 {
            put(&rig.handle, &format!("d{n}"), n).await.unwrap();
        }
        let (entered_tx, entered_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let feed = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            let mut commits = rig.committed.subscribe();
            let mut hold = Some((entered_tx, release_rx));
            async move {
                let mut seen: Vec<(u64, String)> = Vec::new();
                let (mut cursor, mut published) = (0, 0);
                while seen.len() < 10 {
                    let bound = handle.committed_seq();
                    assert!(bound >= published, "the committed position went back");
                    published = bound;
                    if cursor == bound {
                        commits.changed().await.unwrap();
                        continue;
                    }
                    // The page after d4 keeps its lease until released.
                    let held = if cursor == 4 { hold.take() } else { None };
                    let page = handle
                        .lease(move |s| {
                            let mut page = Vec::new();
                            let range = gate::key::seq(cursor + 1)..=gate::key::seq(bound);
                            s.txn.scan(Table::BySeq, range, false, |key, id| {
                                let id = String::from_utf8(id.to_vec())?;
                                page.push((gate::key::split_seq(key)?, id));
                                Ok(page.len() < 2)
                            })?;
                            if let Some((entered, release)) = held {
                                entered.send(()).unwrap();
                                release.recv().unwrap();
                            }
                            Ok(page)
                        })
                        .await
                        .unwrap();
                    assert!(!page.is_empty(), "nothing after {cursor} up to {bound}");
                    for (seq, id) in page {
                        assert!(
                            seq > cursor && seq <= bound,
                            "{seq} outside ({cursor}, {bound}]"
                        );
                        cursor = seq;
                        seen.push((seq, id));
                    }
                }
                seen
            }
        });
        entered_rx.await.unwrap();

        // The commit fails while the feed holds a page's lease; the reopen
        // waits for that page only, not for the feed.
        let mut failing = put_on_full_disk(&rig, "lost");
        rig.handle.gate.closed().await;
        release_tx.send(()).unwrap();
        reopening(&mut rig, &mut failing).await;
        rig.switch.restore_writes();
        rig.go.send(()).unwrap();
        assert_eq!(failing.await.unwrap().unwrap_err().status, 500);
        assert!(!feed.is_finished());

        for n in 6..=10 {
            assert_eq!(put(&rig.handle, &format!("d{n}"), n).await.unwrap(), n);
        }
        let expected: Vec<(u64, String)> = (1..=10).map(|n| (n, format!("d{n}"))).collect();
        assert_eq!(feed.await.unwrap(), expected);
    }
}

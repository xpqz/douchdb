//! Online shard compaction, as CouchDB's compactor does it: the shard keeps
//! serving throughout.
//!
//! 1. On the actor, the journal starts recording every committed key and an
//!    empty file, `shard-NNNN.compact.redb`, is created.
//! 2. Off the actor, the live data is copied into it a page (one lease) at a
//!    time: every document's metadata with only its leaf bodies (CouchDB
//!    drops the others), and every other table whole, the shard's sequence
//!    and uuid included. Pages read different snapshots, which is safe: a
//!    key that changes after its page was read is in the journal.
//! 3. Off the actor, the copied file is checked in full (`kv::check`), so
//!    the shard keeps acknowledging writes meanwhile.
//! 4. Catch-up rounds, until one is small, take the journal's keys and
//!    re-read each from a fresh lease: present is an upsert, absent a
//!    delete. A document named by a metadata or body key is reconciled as a
//!    whole: its metadata copied or deleted, and its bodies in the new file
//!    made exactly its leaf bodies, so a body that stopped being a leaf
//!    goes even though its own key was never written.
//! 5. On the actor, writes paused: the last round, a comparison of the new
//!    file's settings with the shard's, and the swap through the gate
//!    (close, drain, rename, directory sync, reopen).
//!
//! A clear or a recovery invalidates the journal; the compaction then throws
//! its file away and starts again. Until the rename the live file is
//! untouched, and a leftover `.compact.redb` is removed on start.

use super::journal::{JournalKey, Taken};
use super::{parse_meta, ShardHandle, ShardName};
use crate::error::ApiError;
use crate::gate::{key, LeaseTxn};
use crate::kv::{
    self, Table, BODIES, BY_SEQ, CHECKPOINTS, LOCAL, META, PURGED_REVS, PURGE_INFOS, PURGE_UUIDS,
    SETTINGS,
};
use anyhow::Result;
use redb::{Database, Key, ReadableTable, TableDefinition, WriteTransaction};
use std::collections::{BTreeSet, VecDeque};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError};

/// Every table, in the order the copy walks them. Bodies are copied with
/// their documents' metadata, not on their own.
const TABLES: [Table; 9] = [
    Table::Meta,
    Table::Bodies,
    Table::BySeq,
    Table::Local,
    Table::PurgeInfos,
    Table::PurgedRevs,
    Table::PurgeUuids,
    Table::Checkpoints,
    Table::Settings,
];

/// Entries one page of the copy, or of a catch-up round, reads at most.
#[cfg(not(test))]
const PAGE_ENTRIES: usize = 1000;
#[cfg(test)]
const PAGE_ENTRIES: usize = 2;

/// Bytes after which a page of the copy ends early.
const PAGE_BYTES: usize = 4 << 20;

/// Where a compaction has got to, for a hook to stop at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stage {
    /// A run (the first, or one after a restart) is about to start.
    Start,
    /// A page of the copy is in the new file.
    Page,
    /// Off the actor, the copied file's integrity check is about to run.
    Check,
    /// A catch-up round off the actor is in the new file.
    Round,
    /// On the actor, writes paused, before the final round.
    Swap,
    /// The caught-up file is closed and about to be renamed over the live
    /// one.
    Sealed,
    /// The rename is done and durable; the shard has yet to reopen.
    Installed,
}

/// A compaction's progress, as `_active_tasks` reports it.
#[derive(Clone, Debug)]
pub struct Task {
    pub phase: &'static str,
    /// Unix seconds.
    pub started_on: u64,
    /// Unix seconds.
    pub updated_on: u64,
    pub changes_done: u64,
    pub total_changes: u64,
}

/// Called at each [`Stage`]; tests stop there.
pub(crate) type Hook = Arc<dyn Fn(Stage) + Send + Sync>;

/// Where a compaction builds a shard's next file, and how that file takes
/// the live one's place.
pub(crate) trait Target: Send + Sync {
    /// A new, empty shard file, replacing any leftover.
    fn create(&self) -> Result<Database>;
    /// Check the copied file in full, off the actor, before the catch-up.
    fn check(&self, db: &mut Database) -> Result<()>;
    /// Close the caught-up file, verified if [`Target::check`] passed.
    fn seal(&self, db: Database) -> Result<()>;
    /// Put the sealed file in place of the live one, durably. Called with
    /// the live file closed.
    fn install(&self) -> Result<()>;
    /// Throw the file away.
    fn remove(&self);
}

/// `shard-NNNN.compact.redb` beside the shard file.
pub(crate) struct FileTarget {
    live: PathBuf,
    file: PathBuf,
    cache_bytes: usize,
    /// The check the current file passed.
    checked: std::sync::Mutex<Option<kv::Checked>>,
}

impl FileTarget {
    pub(crate) fn new(live: &Path, cache_bytes: usize) -> FileTarget {
        FileTarget {
            live: live.to_path_buf(),
            file: live.with_extension("compact.redb"),
            cache_bytes,
            checked: Default::default(),
        }
    }
}

impl Target for FileTarget {
    fn create(&self) -> Result<Database> {
        self.remove();
        super::create_file(&self.file, self.cache_bytes)
    }

    fn check(&self, db: &mut Database) -> Result<()> {
        let checked = kv::check(db, &self.file)?;
        *self.checked.lock().unwrap_or_else(PoisonError::into_inner) = Some(checked);
        Ok(())
    }

    fn seal(&self, db: Database) -> Result<()> {
        let checked = self
            .checked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or_else(|| anyhow::anyhow!("the compaction file was not checked"))?;
        Ok(kv::close_checked(db, checked)?)
    }

    fn install(&self) -> Result<()> {
        Ok(kv::rename_verified(&self.file, &self.live)?)
    }

    fn remove(&self) {
        *self.checked.lock().unwrap_or_else(PoisonError::into_inner) = None;
        match std::fs::remove_file(&self.file) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => tracing::warn!(
                file = %self.file.display(),
                %error,
                "could not remove a compaction file"
            ),
            _ => {}
        }
        kv::forget_verified(&self.file);
    }
}

/// A second test disk, whose image replaces the shard's disk on install.
#[cfg(test)]
pub(crate) struct DiskTarget {
    live: kv::powercut::PowerSwitch,
    next: std::sync::Mutex<Option<kv::powercut::PowerSwitch>>,
}

#[cfg(test)]
impl DiskTarget {
    pub(crate) fn new(live: kv::powercut::PowerSwitch) -> DiskTarget {
        DiskTarget {
            live,
            next: Default::default(),
        }
    }
}

#[cfg(test)]
impl Target for DiskTarget {
    fn create(&self) -> Result<Database> {
        let (backend, switch) = kv::powercut::PowerCut::new();
        let db = kv::open_with(backend, 1 << 20)?;
        super::init_tables(&db)?;
        *self.next.lock().unwrap() = Some(switch);
        Ok(db)
    }

    fn check(&self, db: &mut Database) -> Result<()> {
        anyhow::ensure!(db.check_integrity()?, "the compaction file was repaired");
        Ok(())
    }

    fn seal(&self, db: Database) -> Result<()> {
        drop(db);
        Ok(())
    }

    fn install(&self) -> Result<()> {
        let next = self.next.lock().unwrap().take();
        self.live
            .replace_with(&next.ok_or_else(|| anyhow::anyhow!("nothing to install"))?);
        Ok(())
    }

    fn remove(&self) {
        *self.next.lock().unwrap() = None;
    }
}

/// What to write into the new file.
pub(crate) enum Change {
    /// A key of a table other than `Meta` and `Bodies`: its value, or
    /// `None` to delete it.
    Set(Table, Vec<u8>, Option<Vec<u8>>),
    /// A document: its metadata (`None` when it is gone) and every body it
    /// keeps; any other body of it goes.
    Doc {
        id: String,
        meta: Option<Vec<u8>>,
        bodies: Vec<(String, Vec<u8>)>,
    },
}

/// Where the copy goes on: an index into [`TABLES`] and the last key copied
/// from that table (`None` at its start).
pub(crate) type Cursor = (usize, Option<Vec<u8>>);

/// The document `id` whose metadata is `meta`, with its leaf bodies. A
/// record that passes its checksum but does not decode cannot say which
/// bodies are leaves: it keeps them all, and is reported.
fn doc(txn: &LeaseTxn<'_>, name: &ShardName, id: &str, meta: Option<Vec<u8>>) -> Result<Change> {
    let mut bodies = Vec::new();
    if let Some(bytes) = &meta {
        match parse_meta(id, bytes) {
            Ok(meta) => {
                for rev in meta.tree.leaves() {
                    if let Some(body) = txn.get(Table::Bodies, &key::pair(id, &rev))? {
                        bodies.push((rev, body));
                    }
                }
            }
            Err(corrupt) => {
                name.report_compaction_skip(id, &corrupt);
                // Every body of `id` and nothing else: "\0" is the least
                // extension of an id.
                let range = key::pair(id, "")..key::pair(&format!("{id}\0"), "");
                txn.scan(Table::Bodies, range, false, |key, body| {
                    bodies.push((key::split_pair(key)?.1.to_string(), body.to_vec()));
                    Ok(true)
                })?;
            }
        }
    }
    Ok(Change::Doc {
        id: id.to_string(),
        meta,
        bodies,
    })
}

/// What one lease reads and one transaction writes: at most
/// [`PAGE_ENTRIES`] changes, ending early once they reach [`PAGE_BYTES`],
/// bodies included.
#[derive(Default)]
struct Page(Vec<Change>, usize);

impl Page {
    fn full(&self) -> bool {
        self.0.len() >= PAGE_ENTRIES || self.1 >= PAGE_BYTES
    }

    fn push(&mut self, change: Change) {
        self.1 += match &change {
            Change::Set(_, key, value) => key.len() + value.as_ref().map_or(0, Vec::len),
            Change::Doc { id, meta, bodies } => {
                id.len()
                    + meta.as_ref().map_or(0, Vec::len)
                    + bodies
                        .iter()
                        .map(|(rev, body)| rev.len() + body.len())
                        .sum::<usize>()
            }
        };
        self.0.push(change);
    }
}

/// One page of the copy from `at`: what to write, where the next page
/// starts (`None` when the copy is done) and how many documents it holds.
pub(crate) fn copy_page(
    txn: &LeaseTxn<'_>,
    name: &ShardName,
    at: Cursor,
) -> Result<(Vec<Change>, Option<Cursor>, u64)> {
    let (mut index, mut after) = at;
    let (mut page, mut docs) = (Page::default(), 0);
    while let Some(&table) = TABLES.get(index) {
        let from = match after.take() {
            Some(key) => Bound::Excluded(key),
            None => Bound::Unbounded,
        };
        let mut full = false;
        if table != Table::Bodies {
            txn.scan(table, (from, Bound::Unbounded), false, |key, value| {
                if page.full() {
                    full = true;
                    return Ok(false);
                }
                page.push(if table == Table::Meta {
                    docs += 1;
                    doc(txn, name, key::split_text(key)?, Some(value.to_vec()))?
                } else {
                    Change::Set(table, key.to_vec(), Some(value.to_vec()))
                });
                after = Some(key.to_vec());
                Ok(true)
            })?;
        }
        if full {
            return Ok((page.0, Some((index, after)), docs));
        }
        (index, after) = (index + 1, None);
    }
    Ok((page.0, None, docs))
}

/// What to write for the keys at the front of `keys`, read as they now
/// stand, a page's worth: the keys it covers are taken off `keys`.
pub(crate) fn catch_up(
    txn: &LeaseTxn<'_>,
    name: &ShardName,
    keys: &mut VecDeque<JournalKey>,
) -> Result<Vec<Change>> {
    let (mut page, mut ids) = (Page::default(), BTreeSet::new());
    while !page.full() {
        let Some((table, key)) = keys.pop_front() else {
            break;
        };
        let id = match table {
            Table::Meta => key::split_text(&key)?,
            Table::Bodies => key::split_pair(&key)?.0,
            _ => {
                let value = txn.get(table, &key)?;
                page.push(Change::Set(table, key, value));
                continue;
            }
        };
        if ids.insert(id.to_string()) {
            let meta = txn.get(Table::Meta, &key::text(id))?;
            page.push(doc(txn, name, id, meta)?);
        }
    }
    Ok(page.0)
}

/// Write `changes` into the new file in one transaction.
pub(crate) fn write(target: &Database, changes: Vec<Change>) -> Result<()> {
    let txn = kv::begin_write(target)?;
    for change in changes {
        match change {
            Change::Set(table, key, value) => set(&txn, table, &key, value.as_deref())?,
            Change::Doc { id, meta, bodies } => {
                set(&txn, Table::Meta, &key::text(&id), meta.as_deref())?;
                let mut table = txn.open_table(BODIES)?;
                let after = format!("{id}\0");
                let mut stale = Vec::new();
                let range = (
                    Bound::Included((id.as_str(), "")),
                    Bound::Excluded((after.as_str(), "")),
                );
                for entry in table.range::<(&str, &str)>(range)? {
                    let (key, _) = entry?;
                    let rev = key.value().1;
                    if !bodies.iter().any(|(kept, _)| kept == rev) {
                        stale.push(rev.to_string());
                    }
                }
                for rev in stale {
                    table.remove((id.as_str(), rev.as_str()))?;
                }
                for (rev, body) in &bodies {
                    table.insert((id.as_str(), rev.as_str()), kv::seal(body).as_slice())?;
                }
            }
        }
    }
    txn.commit()?;
    Ok(())
}

/// Put `value` under `key` in `table`, or delete the key when `None`.
fn set(txn: &WriteTransaction, table: Table, key: &[u8], value: Option<&[u8]>) -> Result<()> {
    match table {
        Table::Meta => set_in(txn, META, key::split_text(key)?, value),
        Table::BySeq => set_in(txn, BY_SEQ, key::split_seq(key)?, value),
        Table::Bodies => set_in(txn, BODIES, key::split_pair(key)?, value),
        Table::Local => set_in(txn, LOCAL, key::split_text(key)?, value),
        Table::PurgeInfos => set_in(txn, PURGE_INFOS, key::split_seq(key)?, value),
        Table::PurgedRevs => set_in(txn, PURGED_REVS, key::split_pair(key)?, value),
        Table::PurgeUuids => set_in(txn, PURGE_UUIDS, key::split_text(key)?, value),
        Table::Checkpoints => set_in(txn, CHECKPOINTS, key::split_text(key)?, value),
        Table::Settings => set_in(txn, SETTINGS, key::split_text(key)?, value),
    }
}

fn set_in<'k, K: Key + 'static>(
    txn: &WriteTransaction,
    def: TableDefinition<'static, K, &'static [u8]>,
    key: K::SelfType<'k>,
    value: Option<&[u8]>,
) -> Result<()> {
    let mut table = txn.open_table(def)?;
    match value {
        Some(value) => drop(table.insert(key, kv::seal(value).as_slice())?),
        None => drop(table.remove(key)?),
    }
    Ok(())
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

fn report(handle: &ShardHandle, task: Option<Task>) {
    *handle.task.lock().unwrap_or_else(PoisonError::into_inner) = task;
}

/// Compact the shard online: see the module documentation. Answers once
/// the new file is in place and the shard serves it, or with the error
/// that stopped the compaction, which then leaves the live file as it was.
pub async fn compact_shard(handle: &ShardHandle) -> Result<()> {
    compact(handle, Arc::new(|_| {})).await
}

/// [`compact_shard`], calling `hook` at each [`Stage`].
pub(crate) async fn compact(handle: &ShardHandle, hook: Hook) -> Result<()> {
    let result = run(handle, &hook).await;
    // Whatever happened, nothing records any longer.
    handle.journal.stop();
    if result.is_err() {
        // The file goes on the actor, never from here: a shard whose actor
        // is gone (its database deleted, and perhaps created again under
        // the same name, with a compaction file of its own at this path)
        // leaves it to the tombstone's removal or the start-up sweep.
        let _ = handle
            .alone(|shard| {
                shard.target.remove();
                Ok(())
            })
            .await;
    }
    report(handle, None);
    result
}

async fn run(handle: &ShardHandle, hook: &Hook) -> Result<()> {
    let name = handle.read_context.name.clone();
    let started_on = now();
    'run: loop {
        hook(Stage::Start);
        let target = handle
            .alone(|shard| shard.begin_compaction().map_err(ApiError::from))
            .await?;
        let target = Arc::new(target);
        let total_changes = handle.gate.read(|txn| txn.len(Table::Meta)).await?;
        let mut task = Task {
            phase: "document_copy",
            started_on,
            updated_on: now(),
            changes_done: 0,
            total_changes,
        };
        report(handle, Some(task.clone()));

        let mut cursor = Some(Cursor::default());
        while let Some(at) = cursor.take() {
            let (target, name) = (Arc::clone(&target), name.clone());
            let (next, docs) = handle
                .gate
                .read(move |txn| {
                    let (changes, next, docs) = copy_page(txn, &name, at)?;
                    write(&target, changes)?;
                    Ok((next, docs))
                })
                .await?;
            task.changes_done += docs;
            task.updated_on = now();
            report(handle, Some(task.clone()));
            cursor = next;
            hook(Stage::Page);
        }

        // The full check, off the actor, so the shard keeps serving writes:
        // what the catch-up writes after it is trusted as any two-phase
        // commit is, until the file's next close-time check.
        let mut target = Arc::into_inner(target)
            .ok_or_else(|| anyhow::anyhow!("the compaction file is still in use"))?;
        let (checker, check_hook) = (Arc::clone(&handle.target), Arc::clone(hook));
        let target = tokio::task::spawn_blocking(move || {
            check_hook(Stage::Check);
            checker.check(&mut target).map(|()| target)
        })
        .await??;
        let target = Arc::new(target);

        // Until a round is small, as CouchDB's compactor retries until it
        // has caught up; the last round runs on the actor.
        loop {
            let Taken::Batch(keys) = handle.journal.take() else {
                continue 'run;
            };
            let small = keys.len() <= PAGE_ENTRIES;
            let mut keys: VecDeque<JournalKey> = keys.into_iter().collect();
            while !keys.is_empty() {
                let (target, name) = (Arc::clone(&target), name.clone());
                keys = handle
                    .gate
                    .read(move |txn| {
                        write(&target, catch_up(txn, &name, &mut keys)?)?;
                        Ok(keys)
                    })
                    .await?;
            }
            hook(Stage::Round);
            if small {
                break;
            }
        }

        let target = Arc::into_inner(target)
            .ok_or_else(|| anyhow::anyhow!("the compaction file is still in use"))?;
        let hook = Arc::clone(hook);
        let swapped = handle
            .alone(move |shard| {
                shard
                    .finish_compaction(target, &hook)
                    .map_err(ApiError::from)
            })
            .await?;
        if swapped {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ApiError;
    use crate::gate::key;
    use crate::store::tests::{temp_file, write_doc, CACHE};
    use crate::store::{meta_state, DocMeta, PurgeInfo, ShardTxn};
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::PathBuf;
    use std::sync::Mutex;
    use tokio::sync::oneshot;

    type Op = Arc<dyn Fn(&mut ShardTxn<'_>) -> std::result::Result<(), ApiError> + Send + Sync>;
    /// Every entry of every table: (table, key bytes) -> value.
    type Dump = BTreeMap<(Table, Vec<u8>), Vec<u8>>;

    fn op(
        f: impl Fn(&mut ShardTxn<'_>) -> std::result::Result<(), ApiError> + Send + Sync + 'static,
    ) -> Op {
        Arc::new(f)
    }

    /// A shard file on its own actor.
    struct Rig {
        handle: Arc<ShardHandle>,
        file: PathBuf,
    }

    impl Rig {
        fn new(tag: &str) -> Rig {
            let file = temp_file(tag);
            let handle = ShardHandle::spawn(&file, 1000, CACHE, Default::default()).unwrap();
            Rig {
                handle: Arc::new(handle),
                file,
            }
        }

        async fn run(&self, op: &Op) -> std::result::Result<(), ApiError> {
            let op = Arc::clone(op);
            self.handle.call(move |txn| op(txn)).await
        }

        fn close(self) {
            let Rig { handle, file } = self;
            drop(handle);
            crate::kv::forget_verified(&file);
            let _ = std::fs::remove_dir_all(file.parent().unwrap());
        }
    }

    async fn dump(handle: &ShardHandle) -> Dump {
        handle
            .gate
            .read(|txn| {
                let mut out = Dump::new();
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

    /// `dump` as compaction should leave it: only leaf bodies, and the uuid
    /// left out (a twin shard has its own).
    fn compacted(mut dump: Dump) -> Dump {
        let mut leaves = BTreeSet::new();
        for ((table, id), value) in &dump {
            if *table == Table::Meta {
                let meta: DocMeta = serde_json::from_slice(value).unwrap();
                let id = key::split_text(id).unwrap().to_string();
                for leaf in meta.tree.leaves() {
                    leaves.insert((id.clone(), leaf));
                }
            }
        }
        dump.retain(|(table, key), _| {
            *table != Table::Bodies || {
                let (id, rev) = key::split_pair(key).unwrap();
                leaves.contains(&(id.to_string(), rev.to_string()))
            }
        });
        without_uuid(dump)
    }

    fn without_uuid(mut dump: Dump) -> Dump {
        dump.remove(&(Table::Settings, key::text("uuid")));
        dump
    }

    fn body_keys(dump: &Dump) -> Vec<(String, String)> {
        dump.keys()
            .filter(|(table, _)| *table == Table::Bodies)
            .map(|(_, key)| {
                let (id, rev) = key::split_pair(key).unwrap();
                (id.to_string(), rev.to_string())
            })
            .collect()
    }

    /// A compaction stopped at a stage until the test lets it go on.
    struct Pause {
        reached: oneshot::Receiver<()>,
        resume: std::sync::mpsc::Sender<()>,
    }

    type Armed = Mutex<Option<(oneshot::Sender<()>, std::sync::mpsc::Receiver<()>)>>;

    /// A hook that stops the first time each of `stages` is reached, and
    /// the stages it saw, in order.
    fn pauses(stages: &[Stage]) -> (Hook, Vec<Pause>, Arc<Mutex<Vec<Stage>>>) {
        let mut armed: Vec<(Stage, Armed)> = Vec::new();
        let mut handles = Vec::new();
        for stage in stages {
            let (reached_tx, reached) = oneshot::channel();
            let (resume, resume_rx) = std::sync::mpsc::channel();
            armed.push((*stage, Mutex::new(Some((reached_tx, resume_rx)))));
            handles.push(Pause { reached, resume });
        }
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        let hook: Hook = Arc::new(move |stage| {
            log.lock().unwrap().push(stage);
            let stop = armed
                .iter()
                .find(|(at, _)| *at == stage)
                .and_then(|(_, armed)| armed.lock().unwrap().take());
            if let Some((reached, resume)) = stop {
                let _ = reached.send(());
                let _ = resume.recv();
            }
        });
        (hook, handles, seen)
    }

    /// Write `ids` (one revision each) and give `d0` a second revision, so
    /// it has a body compaction drops.
    fn setup() -> Op {
        op(|txn| {
            for (n, id) in ["d0", "d1", "d2", "d3", "d4", "d5"].iter().enumerate() {
                write_doc(
                    txn,
                    id,
                    false,
                    format!("{{\"n\":{n}}}").as_bytes(),
                    n as u64,
                )?;
            }
            write_doc(txn, "d0", false, b"{\"n\":10}", 10)?;
            txn.put_local("l0", b"{}")?;
            txn.put_checkpoint("c0", b"{}")?;
            Ok(())
        })
    }

    /// Purge every leaf of `id`, as `_purge` does.
    fn purge(txn: &mut ShardTxn<'_>, id: &str, uuid: &str) -> std::result::Result<(), ApiError> {
        let meta = txn.get_meta(id)?.expect("the document is there");
        let old = meta_state(&meta);
        let mut tree = meta.tree.clone();
        let leaves = tree.leaves();
        let mut dead = Vec::new();
        for leaf in &leaves {
            dead.extend(tree.purge_leaf(leaf));
        }
        let info = PurgeInfo {
            uuid: uuid.to_string(),
            id: id.to_string(),
            revs: leaves,
        };
        txn.record_purge(&info, &[])?;
        for rev in dead {
            txn.remove_body(id, &rev)?;
        }
        txn.remove_seq(meta.seq)?;
        txn.remove_meta(id, old)?;
        Ok(())
    }

    /// The stages a mutation can land at: during the copy (after its first
    /// page), between catch-up rounds, and queued behind the swap.
    const STAGES: [Stage; 3] = [Stage::Page, Stage::Round, Stage::Swap];

    /// Run `setup` on a shard and a twin, compact the shard while `change`
    /// lands at `stage`, and apply `change` to the twin plainly. The
    /// compacted shard's tables, and the twin's as compaction should leave
    /// them: a change that lands before the swap is compacted with the
    /// rest; one queued behind it lands on the compacted file and keeps the
    /// bodies it supersedes until the next compaction.
    async fn compact_while(stage: Stage, setup: &Op, change: &Op) -> (Dump, Dump) {
        let (shard, twin) = (Rig::new("compact"), Rig::new("compact-twin"));
        for rig in [&shard, &twin] {
            rig.run(setup).await.unwrap();
        }
        let expected = if stage == Stage::Swap {
            compact_shard(&twin.handle).await.unwrap();
            twin.run(change).await.unwrap();
            without_uuid(dump(&twin.handle).await)
        } else {
            twin.run(change).await.unwrap();
            compacted(dump(&twin.handle).await)
        };
        let (hook, mut stops, _) = pauses(&[stage]);
        let Pause { reached, resume } = stops.remove(0);
        let compaction = tokio::spawn({
            let handle = Arc::clone(&shard.handle);
            async move { compact(&handle, hook).await }
        });
        reached.await.expect("the compaction reached the stage");
        if stage == Stage::Swap {
            // Writes are paused: the change queues behind the swap.
            let write = tokio::spawn({
                let change = Arc::clone(change);
                shard.handle.call(move |txn| change(txn))
            });
            while shard.handle.queued() == 0 {
                tokio::task::yield_now().await;
            }
            resume.send(()).unwrap();
            compaction.await.unwrap().unwrap();
            write.await.unwrap().unwrap();
        } else {
            shard.run(change).await.unwrap();
            resume.send(()).unwrap();
            compaction.await.unwrap().unwrap();
        }
        let found = (without_uuid(dump(&shard.handle).await), expected);
        shard.close();
        twin.close();
        found
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compaction_keeps_concurrent_updates_and_tombstones() {
        let change = op(|txn| {
            write_doc(txn, "d1", false, b"{\"v\":2}", 100)?;
            write_doc(txn, "d4", false, b"{\"v\":2}", 101)?;
            write_doc(txn, "d2", true, b"{}", 102)?;
            write_doc(txn, "d5", true, b"{}", 103)?;
            write_doc(txn, "new", false, b"{}", 104)?;
            Ok(())
        });
        for stage in STAGES {
            let (found, expected) = compact_while(stage, &setup(), &change).await;
            assert_eq!(found, expected, "{stage:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_document_purged_during_compaction_does_not_return() {
        let change = op(|txn| {
            purge(txn, "d0", "p0")?;
            purge(txn, "d4", "p4")
        });
        for stage in STAGES {
            let (found, expected) = compact_while(stage, &setup(), &change).await;
            assert!(
                !found.contains_key(&(Table::Meta, key::text("d0"))),
                "{stage:?}"
            );
            assert!(
                !body_keys(&found).iter().any(|(id, _)| id == "d0"),
                "{stage:?}"
            );
            assert_eq!(found, expected, "{stage:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_docs_settings_security_and_checkpoints_changed_during_compaction_survive() {
        let change = op(|txn| {
            txn.put_local("l1", b"{\"v\":1}")?;
            txn.remove_local("l0")?;
            txn.put_setting("db/security", b"{\"admins\":{}}")?;
            txn.set_revs_limit(7)?;
            txn.put_checkpoint("c1", b"{\"seq\":1}")?;
            txn.remove_checkpoint("c0")?;
            Ok(())
        });
        for stage in STAGES {
            let (found, expected) = compact_while(stage, &setup(), &change).await;
            assert!(
                found.contains_key(&(Table::Local, key::text("l1"))),
                "{stage:?}"
            );
            assert!(
                !found.contains_key(&(Table::Local, key::text("l0"))),
                "{stage:?}"
            );
            assert_eq!(found, expected, "{stage:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn purge_info_trimming_during_compaction_is_kept() {
        let base = setup();
        let setup = op(move |txn| {
            base(txn)?;
            txn.set_purged_infos_limit(1)?;
            purge(txn, "d3", "p3")
        });
        // Records a second purge, which trims the first.
        let change = op(|txn| purge(txn, "d1", "p1"));
        for stage in STAGES {
            let (found, expected) = compact_while(stage, &setup, &change).await;
            assert!(
                !found.contains_key(&(Table::PurgeUuids, key::text("p3"))),
                "{stage:?}"
            );
            assert_eq!(found, expected, "{stage:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bodies_that_became_non_leaf_during_compaction_are_removed_including_already_copied_ones(
    ) {
        // d0's and d1's bodies are in the first page of the copy; the
        // update journals the new body and the metadata, never the old
        // body's key.
        let change = op(|txn| {
            write_doc(txn, "d0", false, b"{\"v\":3}", 200)?;
            write_doc(txn, "d1", false, b"{\"v\":3}", 201)?;
            Ok(())
        });
        for stage in [Stage::Page, Stage::Round] {
            let (found, expected) = compact_while(stage, &setup(), &change).await;
            let bodies = body_keys(&found);
            for id in ["d0", "d1"] {
                let revs: Vec<&str> = bodies
                    .iter()
                    .filter(|(owner, _)| owner == id)
                    .map(|(_, rev)| rev.as_str())
                    .collect();
                assert_eq!(revs.len(), 1, "{stage:?} {id}: {revs:?}");
                assert!(revs[0].starts_with(if id == "d0" { "3-" } else { "2-" }));
            }
            assert_eq!(found, expected, "{stage:?}");
        }
        // Behind the swap, the update lands on the compacted file.
        let (found, expected) = compact_while(Stage::Swap, &setup(), &change).await;
        assert_eq!(found, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compaction_restarts_after_failed_commit_recovery() {
        let (shard, twin) = (
            Rig::new("compact-restart"),
            Rig::new("compact-restart-twin"),
        );
        let landed = op(|txn| write_doc(txn, "after", false, b"{}", 300).map(drop));
        for rig in [&shard, &twin] {
            rig.run(&setup()).await.unwrap();
        }
        twin.run(&landed).await.unwrap();
        let (hook, mut stops, seen) = pauses(&[Stage::Page]);
        let Pause { reached, resume } = stops.remove(0);
        let compaction = tokio::spawn({
            let handle = Arc::clone(&shard.handle);
            async move { compact(&handle, hook).await }
        });
        reached.await.unwrap();
        let failed = shard
            .run(&op(|txn| {
                write_doc(txn, "lost", false, b"{}", 299)?;
                txn.inject_commit_failure();
                Ok(())
            }))
            .await;
        assert_eq!(failed.unwrap_err().status, 500);
        shard.run(&landed).await.unwrap();
        resume.send(()).unwrap();
        compaction.await.unwrap().unwrap();

        let starts = seen
            .lock()
            .unwrap()
            .iter()
            .filter(|s| **s == Stage::Start)
            .count();
        assert_eq!(starts, 2, "the compaction started again");
        let found = dump(&shard.handle).await;
        assert_eq!(
            without_uuid(found.clone()),
            compacted(dump(&twin.handle).await)
        );
        compact_shard(&shard.handle).await.unwrap();
        assert_eq!(
            dump(&shard.handle).await,
            found,
            "a fresh compaction agrees"
        );
        shard.close();
        twin.close();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn non_leaf_bodies_are_dropped() {
        let rig = Rig::new("compact-leaves");
        rig.run(&op(|txn| {
            write_doc(txn, "a", false, b"{}", 1)?;
            write_doc(txn, "a", false, b"{}", 2)?;
            write_doc(txn, "a", false, b"{}", 3)?;
            write_doc(txn, "b", false, b"{}", 4)?;
            write_doc(txn, "b", true, b"{}", 5)?;
            Ok(())
        }))
        .await
        .unwrap();
        let before = dump(&rig.handle).await;
        assert_eq!(body_keys(&before).len(), 5);
        let uuid = rig.handle.uuid();
        let inode = std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&rig.file).unwrap());
        compact_shard(&rig.handle).await.unwrap();
        let after = dump(&rig.handle).await;
        let expected = vec![
            ("a".to_string(), format!("3-{:032x}", 3)),
            ("b".to_string(), format!("2-{:032x}", 5)),
        ];
        assert_eq!(body_keys(&after), expected);
        assert_eq!(without_uuid(after.clone()), compacted(before));
        assert_eq!(rig.handle.uuid(), uuid);
        let replaced = std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&rig.file).unwrap());
        assert_ne!(replaced, inode, "the shard file was rewritten");
        assert!(!rig.file.with_extension("compact.redb").exists());
        rig.close();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn progress_is_reported_while_compacting() {
        let rig = Rig::new("compact-progress");
        rig.run(&setup()).await.unwrap();
        assert!(rig.handle.compaction().is_none());
        let (hook, mut stops, _) = pauses(&[Stage::Page]);
        let Pause { reached, resume } = stops.remove(0);
        let compaction = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move { compact(&handle, hook).await }
        });
        reached.await.unwrap();
        let task = rig.handle.compaction().expect("a task while compacting");
        assert_eq!(task.phase, "document_copy");
        assert_eq!(task.total_changes, 6);
        assert_eq!(task.changes_done, 2);
        assert!(task.started_on > 0 && task.updated_on >= task.started_on);
        resume.send(()).unwrap();
        compaction.await.unwrap().unwrap();
        assert!(rig.handle.compaction().is_none());
        rig.close();
    }

    fn config(q: u32) -> crate::db::DbConfig {
        crate::db::DbConfig {
            q,
            revs_limit: 1000,
            cache_bytes: CACHE,
            shard_map: crate::cluster::ShardMap::single(q),
            partitioned: false,
        }
    }

    /// A database with `q` shards, for tests that go through `Db`.
    fn db(tag: &str, q: u32) -> (Arc<crate::db::Db>, PathBuf) {
        let root = std::env::temp_dir().join(format!("douchdb-{tag}-{}", crate::util::uuid()));
        let db = crate::db::Db::open_or_create(&root, tag, config(q)).unwrap();
        (Arc::new(db), root)
    }

    /// A view queried while its database compacts, and afterwards, keeps
    /// its index: the shard's uuid, which its cursor names, is unchanged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn uuid_is_unchanged_so_indexes_do_not_rebuild() {
        use serde_json::json;
        let (db, root) = db("compactview", 1);
        let docs = vec![
            json!({"_id": "a", "n": 1}),
            json!({"_id": "b", "n": 2}),
            json!({"_id": "c", "n": 3}),
            json!({"_id": "_design/v", "views": {"v": {"map": "function(doc){ emit(doc._id, doc.n); }"}}}),
        ];
        db.put_docs(docs, true).await.unwrap();
        let query = crate::view::ViewQuery::default();
        let rows = crate::view::query(&db, "_design/v", "v", &query)
            .await
            .unwrap();
        let file = crate::db::shard_file(&db.path, 0);
        let inode = std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&file).unwrap());
        let (uuid, reads) = (db.shard_handle(0).uuid(), db.shard_handle(0).body_reads());

        let (hook, mut stops, _) = pauses(&[Stage::Page]);
        let Pause { reached, resume } = stops.remove(0);
        let compaction = tokio::spawn({
            let db = Arc::clone(&db);
            async move { compact(db.shard_handle(0), hook).await }
        });
        reached.await.unwrap();
        let during = crate::view::query(&db, "_design/v", "v", &query)
            .await
            .unwrap();
        assert_eq!(during, rows);
        resume.send(()).unwrap();
        compaction.await.unwrap().unwrap();

        let replaced = std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&file).unwrap());
        assert_ne!(replaced, inode, "the shard file was rewritten");
        assert_eq!(db.shard_handle(0).uuid(), uuid);
        let after = crate::view::query(&db, "_design/v", "v", &query)
            .await
            .unwrap();
        assert_eq!(after, rows);
        assert_eq!(
            db.shard_handle(0).body_reads(),
            reads,
            "the view read no document: its index was kept"
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// `_active_tasks` lists a running compaction per shard, named by its
    /// hash range as CouchDB names shard files, and nothing once it is done.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_tasks_list_a_running_compaction_as_couchdb_does() {
        use serde_json::json;
        let (db, root) = db("compacttasks", 2);
        let ids: Vec<String> = (0..100)
            .map(|n| format!("doc{n}"))
            .filter(|id| db.shard_index(id) == 1)
            .take(5)
            .collect();
        let docs = ids.iter().map(|id| json!({"_id": id})).collect();
        db.put_docs(docs, true).await.unwrap();
        assert_eq!(db.active_tasks("node1"), Vec::<serde_json::Value>::new());

        let (hook, mut stops, _) = pauses(&[Stage::Page]);
        let Pause { reached, resume } = stops.remove(0);
        let compaction = tokio::spawn({
            let db = Arc::clone(&db);
            async move { compact(db.shard_handle(1), hook).await }
        });
        reached.await.unwrap();
        let tasks = db.active_tasks("node1");
        assert_eq!(tasks.len(), 1, "{tasks:?}");
        let task = &tasks[0];
        assert_eq!(task["type"], "database_compaction");
        assert_eq!(task["database"], "shards/80000000-ffffffff/compacttasks");
        assert_eq!(task["node"], "node1");
        assert_eq!(task["phase"], "document_copy");
        assert_eq!(task["changes_done"], 2);
        assert_eq!(task["total_changes"], 5);
        assert_eq!(task["progress"], 40);
        let started = task["started_on"].as_u64().unwrap();
        assert!(started > 0 && task["updated_on"].as_u64().unwrap() >= started);
        assert_eq!(task.as_object().unwrap().len(), 9, "{task}");
        resume.send(()).unwrap();
        compaction.await.unwrap().unwrap();
        assert!(db.active_tasks("node1").is_empty());
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A continuous feed through `Db::changes`, as `_changes?feed=continuous`
    /// drives it (actor reads for the pending count, leases for the rows),
    /// open across a compaction: writes land during the copy, between
    /// catch-up rounds and behind the swap, and afterwards.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_continuous_feed_streams_across_the_swap_without_skip_or_repeat() {
        use serde_json::json;
        const TOTAL: usize = 12;
        let (db, root) = db("compactfeed", 1);
        let put = |id: String| {
            let db = Arc::clone(&db);
            async move { db.put_docs(vec![json!({"_id": id})], true).await.unwrap() }
        };
        for n in 0..6 {
            put(format!("d{n:02}")).await;
        }
        let feed = tokio::spawn({
            let db = Arc::clone(&db);
            async move {
                let mut wake = db.subscribe_commits();
                let mut opts = crate::db::ChangesOpts {
                    since: crate::db::ShardSince(vec![0]),
                    ..Default::default()
                };
                let mut seen: Vec<String> = Vec::new();
                while seen.len() < TOTAL {
                    wake.borrow_and_update();
                    let list = db.changes(0, &opts, true).await.unwrap();
                    seen.extend(list.results.iter().map(|row| row.id.clone()));
                    let since = crate::since::parse(Some(&list.last_seq)).unwrap();
                    opts.since = db.shard_since(0, &since);
                    if list.results.is_empty() {
                        wake.changed().await.unwrap();
                    }
                }
                seen
            }
        });

        let (hook, stops, _) = pauses(&[Stage::Page, Stage::Round, Stage::Swap]);
        let compaction = tokio::spawn({
            let db = Arc::clone(&db);
            async move { compact(db.shard_handle(0), hook).await }
        });
        let mut stops = stops.into_iter();
        for id in ["d06", "d07"] {
            let Pause { reached, resume } = stops.next().unwrap();
            reached.await.unwrap();
            put(id.to_string()).await;
            resume.send(()).unwrap();
        }
        let Pause { reached, resume } = stops.next().unwrap();
        reached.await.unwrap();
        let queued = tokio::spawn(put("d08".to_string()));
        while db.shard_handle(0).queued() == 0 {
            tokio::task::yield_now().await;
        }
        resume.send(()).unwrap();
        compaction.await.unwrap().unwrap();
        queued.await.unwrap();
        for n in 9..TOTAL {
            put(format!("d{n:02}")).await;
        }
        let expected: Vec<String> = (0..TOTAL).map(|n| format!("d{n:02}")).collect();
        assert_eq!(feed.await.unwrap(), expected);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// The new file's full integrity check runs off the actor: the shard
    /// acknowledges writes while it is held.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_integrity_check_runs_while_the_shard_acknowledges_writes() {
        let rig = Rig::new("compact-check");
        rig.run(&setup()).await.unwrap();
        let (hook, mut stops, _) = pauses(&[Stage::Check]);
        let Pause { reached, resume } = stops.remove(0);
        let compaction = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move { compact(&handle, hook).await }
        });
        reached.await.expect("the check ran");
        rig.run(&op(|txn| {
            write_doc(txn, "during", false, b"{}", 50).map(drop)
        }))
        .await
        .unwrap();
        resume.send(()).unwrap();
        compaction.await.unwrap().unwrap();
        assert!(dump(&rig.handle)
            .await
            .contains_key(&(Table::Meta, key::text("during"))));
        rig.close();
    }

    /// A compaction of a database that was deleted and created again under
    /// the same name meanwhile fails, and leaves the new database's own
    /// compaction file alone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stale_compaction_never_touches_a_recreated_shards_file() {
        let Rig { handle: old, file } = Rig::new("compact-stale");
        let (old_rig, compacting) = (
            Rig {
                handle: Arc::clone(&old),
                file: file.clone(),
            },
            file.with_extension("compact.redb"),
        );
        old_rig.run(&setup()).await.unwrap();
        drop(old_rig);
        let (hook, mut stops, _) = pauses(&[Stage::Page]);
        let Pause { reached, resume } = stops.remove(0);
        let stale = tokio::spawn({
            let handle = Arc::clone(&old);
            async move { compact(&handle, hook).await }
        });
        reached.await.unwrap();

        old.close_deleted().await;
        let dir = file.parent().unwrap().to_path_buf();
        let tombstone = dir.with_extension("deleted");
        std::fs::rename(&dir, &tombstone).unwrap();
        std::fs::create_dir(&dir).unwrap();
        crate::kv::forget_verified(&file);
        drop(crate::store::create_file(&file, CACHE).unwrap());
        let new = Rig {
            handle: Arc::new(ShardHandle::spawn(&file, 1000, CACHE, Default::default()).unwrap()),
            file: file.clone(),
        };
        new.run(&setup()).await.unwrap();
        let (hook, mut stops, _) = pauses(&[Stage::Page]);
        let fresh_stop = stops.remove(0);
        let fresh = tokio::spawn({
            let handle = Arc::clone(&new.handle);
            async move { compact(&handle, hook).await }
        });
        fresh_stop.reached.await.unwrap();
        assert!(compacting.exists());

        resume.send(()).unwrap();
        assert!(stale.await.unwrap().is_err(), "the stale compaction failed");
        assert!(
            compacting.exists(),
            "the stale compaction removed the new one's file"
        );
        fresh_stop.resume.send(()).unwrap();
        fresh.await.unwrap().unwrap();
        drop(old);
        let _ = std::fs::remove_dir_all(tombstone);
        new.close();
    }

    /// A failed commit after the last round off the actor invalidates the
    /// journal: the final round finds it so and the compaction starts again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_invalidated_journal_at_the_final_round_restarts_the_compaction() {
        let rig = Rig::new("compact-final-invalid");
        rig.run(&setup()).await.unwrap();
        let (hook, mut stops, seen) = pauses(&[Stage::Round]);
        let Pause { reached, resume } = stops.remove(0);
        let compaction = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move { compact(&handle, hook).await }
        });
        // Nothing was written during the copy: this round is the last.
        reached.await.unwrap();
        let failed = rig
            .run(&op(|txn| {
                write_doc(txn, "lost", false, b"{}", 60)?;
                txn.inject_commit_failure();
                Ok(())
            }))
            .await;
        assert_eq!(failed.unwrap_err().status, 500);
        resume.send(()).unwrap();
        compaction.await.unwrap().unwrap();
        let seen = seen.lock().unwrap().clone();
        let swap = seen.iter().position(|stage| *stage == Stage::Swap).unwrap();
        assert_eq!(seen[swap + 1], Stage::Start, "{seen:?}");
        assert_eq!(seen.iter().filter(|s| **s == Stage::Start).count(), 2);
        rig.close();
    }

    /// The swap closes the gate and waits for a reader holding a lease; the
    /// reader finishes on the old file, and reads after it on the new one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_swap_waits_for_a_reader_holding_a_lease() {
        let rig = Rig::new("compact-lease");
        rig.run(&setup()).await.unwrap();
        let (hook, mut stops, _) = pauses(&[Stage::Swap]);
        let Pause { reached, resume } = stops.remove(0);
        let compaction = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move { compact(&handle, hook).await }
        });
        reached.await.unwrap();
        let (entered_tx, entered) = oneshot::channel();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let gate = rig.handle.gate.clone();
        let reader = tokio::spawn(async move {
            gate.read(move |txn| {
                entered_tx.send(()).unwrap();
                held.recv().unwrap();
                txn.len(Table::Meta)
            })
            .await
        });
        entered.await.unwrap();
        resume.send(()).unwrap();
        rig.handle.gate.closed().await;
        assert!(
            !compaction.is_finished(),
            "the swap did not wait for the lease"
        );
        release.send(()).unwrap();
        assert_eq!(reader.await.unwrap().unwrap(), 6);
        compaction.await.unwrap().unwrap();
        assert_eq!(body_keys(&dump(&rig.handle).await).len(), 6);
        rig.close();
    }

    /// Copy the directory `from` to `to` with its files as they now stand,
    /// open ones included: what a process killed now leaves behind.
    fn copy_dir(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap().flatten() {
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }

    /// A crash at any step before the rename leaves the live file as it
    /// was, with every write acknowledged until then; on the next start the
    /// half-built file is removed, the database opens, and it compacts.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_crash_before_the_rename_leaves_the_live_file_and_removes_the_leftover() {
        const TAG: &str = "compactcrash";
        for stage in [
            Stage::Page,
            Stage::Check,
            Stage::Round,
            Stage::Swap,
            Stage::Sealed,
        ] {
            let (db, root) = db(TAG, 1);
            let setup = setup();
            db.shard_handle(0)
                .call(move |txn| setup(txn))
                .await
                .unwrap();
            let (hook, mut stops, _) = pauses(&[stage]);
            let Pause { reached, resume } = stops.remove(0);
            let compaction = tokio::spawn({
                let db = Arc::clone(&db);
                async move { compact(db.shard_handle(0), hook).await }
            });
            reached.await.unwrap();
            if !matches!(stage, Stage::Swap | Stage::Sealed) {
                // Off the actor, so the shard acknowledges a write meanwhile.
                db.shard_handle(0)
                    .call(|txn| write_doc(txn, "during", false, b"{}", 50).map(drop))
                    .await
                    .unwrap();
            }
            let expected = dump(db.shard_handle(0)).await;
            let crashed = root.with_extension("crashed");
            copy_dir(&root, &crashed);
            resume.send(()).unwrap();
            compaction.await.unwrap().unwrap();
            drop(db);
            let _ = std::fs::remove_dir_all(&root);

            let dir = crate::db::db_path(&crashed, TAG);
            let leftover = crate::db::shard_file(&dir, 0).with_extension("compact.redb");
            assert!(
                leftover.exists(),
                "{stage:?}: no compaction file to crash with"
            );
            crate::state::recover_root(&crashed).unwrap();
            assert!(!leftover.exists(), "{stage:?}: the leftover was kept");
            let db = crate::db::Db::open(&crashed, TAG, config(1)).unwrap();
            assert_eq!(dump(db.shard_handle(0)).await, expected, "{stage:?}");
            compact_shard(db.shard_handle(0)).await.unwrap();
            assert_eq!(
                without_uuid(dump(db.shard_handle(0)).await),
                compacted(expected),
                "{stage:?}"
            );
            assert!(!leftover.exists());
            drop(db);
            let _ = std::fs::remove_dir_all(&crashed);
        }
    }

    /// A power cut once the new file is in place keeps it whole, with the
    /// writes that landed during the copy and between rounds: the swap
    /// installs only what the new file made durable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_power_cut_after_the_rename_keeps_the_compacted_file_complete() {
        let (_, switch) = crate::kv::powercut::PowerCut::new();
        drop(crate::store::Shard::create_over_backend(&switch, 1000).unwrap());
        let shard = Arc::new(ShardHandle::over_backend(&switch).unwrap());
        let twin = Rig::new("compact-cut-twin");
        let changes = [
            op(|txn| {
                write_doc(txn, "d1", false, b"{\"v\":2}", 100)?;
                write_doc(txn, "d2", true, b"{}", 101)?;
                Ok(())
            }),
            op(|txn| {
                write_doc(txn, "d1", false, b"{\"v\":3}", 102)?;
                write_doc(txn, "new", false, b"{}", 103)?;
                txn.put_local("l0", b"{\"v\":2}")?;
                Ok(())
            }),
        ];
        for op in [&setup()].into_iter().chain(&changes) {
            twin.run(op).await.unwrap();
        }
        let expected = compacted(dump(&twin.handle).await);
        twin.close();
        let setup = setup();
        shard.call(move |txn| setup(txn)).await.unwrap();

        let (hook, stops, _) = pauses(&[Stage::Page, Stage::Round]);
        let compaction = tokio::spawn({
            let shard = Arc::clone(&shard);
            async move { compact(&shard, hook).await }
        });
        for (Pause { reached, resume }, change) in stops.into_iter().zip(changes) {
            reached.await.unwrap();
            shard.call(move |txn| change(txn)).await.unwrap();
            resume.send(()).unwrap();
        }
        compaction.await.unwrap().unwrap();
        switch.cut();
        drop(Arc::into_inner(shard).unwrap());

        let shard = ShardHandle::over_backend(&switch).unwrap();
        assert_eq!(without_uuid(dump(&shard).await), expected);
    }

    /// The new file's check is trusted only by the process that ran it:
    /// once the file is renamed, this process opens it without the check,
    /// but one that never saw it (modelled by a copy at a path this
    /// process never checked) runs the full check on its first open, and
    /// so refuses a damaged body.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_power_cut_after_the_rename_leaves_the_check_to_the_next_process() {
        const MARKER: &str = "<<douchdb-compacted-body>>";
        let rig = Rig::new("compact-trust");
        rig.run(&setup()).await.unwrap();
        let body = format!("{{\"m\":\"{}\"}}", MARKER.repeat(64));
        rig.run(&op(move |txn| {
            write_doc(txn, "marked", false, body.as_bytes(), 20).map(drop)
        }))
        .await
        .unwrap();
        let (hook, mut stops, _) = pauses(&[Stage::Installed]);
        let Pause { reached, resume } = stops.remove(0);
        let compaction = tokio::spawn({
            let handle = Arc::clone(&rig.handle);
            async move { compact(&handle, hook).await }
        });
        reached.await.unwrap();
        assert!(kv::is_verified(&rig.file), "the swap carried the check");
        let next = temp_file("compact-trust-next");
        let mut bytes = std::fs::read(&rig.file).unwrap();
        resume.send(()).unwrap();
        compaction.await.unwrap().unwrap();

        let hits: Vec<usize> = bytes
            .windows(MARKER.len())
            .enumerate()
            .filter(|(_, window)| *window == MARKER.as_bytes())
            .map(|(at, _)| at)
            .collect();
        assert!(!hits.is_empty(), "the body was not found");
        for at in hits {
            bytes[at + 3] ^= 0x55;
        }
        std::fs::write(&next, bytes).unwrap();
        let error = crate::store::Shard::open(&next, 1000, CACHE)
            .err()
            .expect("a damaged compacted file opened unchecked");
        assert!(
            error.chain().any(|cause| matches!(
                cause.downcast_ref::<redb::DatabaseError>(),
                Some(redb::DatabaseError::Storage(redb::StorageError::Corrupted(
                    _
                )))
            )),
            "{error:#}"
        );
        let _ = std::fs::remove_dir_all(next.parent().unwrap());
        rig.close();
    }
}

//! Database and document semantics: revisions, conflicts, changes feeds.
//!
//! All writes for one request are staged, validated, and then written into
//! the shard's group commit (one synced redb transaction for every request
//! queued together), so a `_bulk_docs` of a thousand documents costs one
//! `fsync`, not a thousand, and is acknowledged only once it is durable.

use crate::cluster::{ShardMap, ShardMask};
pub use crate::error::{ApiError, DbResult};
use crate::prepare::{prepare, Prepared};
use crate::rev::{new_revid_doc, Rev};
use crate::store::{
    meta_state, DocMeta, DocRead, LiveDoc, MetaState, MetaWrite, PurgeInfo, ShardHandle, ShardTxn,
};
use crate::util::uuid;
use md5::{Digest, Md5};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

pub use crate::rev::META_FIELDS;

/// Names of the database's own settings, kept in the settings table of
/// shard 0 beside that shard's own.
const DB_SETTINGS: &str = "db/";
/// A write with more documents than this prepares them on the blocking pool.
const PREPARE_INLINE_MAX: usize = 1000;
/// How many writes prepared their documents on the blocking pool.
#[cfg(test)]
static BLOCKING_PREPARES: AtomicU64 = AtomicU64::new(0);
/// [`DbRecord`].
const INFO_SETTING: &str = "db/info";
/// The active [`ShardMap`].
const SHARD_MAP_SETTING: &str = "db/shard_map";
/// `{"version": .., "security": ..}`.
const SECURITY_SETTING: &str = "db/security";
/// A [`ShardMap`] this node has not had acknowledged; absent when none is.
pub(crate) const PENDING_SHARD_MAP_SETTING: &str = "db/pending_shard_map";

/// Page cache for reading a closed database's settings: a few pages.
const PEEK_CACHE: usize = 1 << 20;

/// What a database is, fixed when it is created.
#[derive(serde::Serialize, serde::Deserialize)]
struct DbRecord {
    q: u32,
    uuid: String,
    instance_start_time: String,
    partitioned: bool,
}

/// What a new database is created with.
pub struct DbSettings {
    pub q: u32,
    pub partitioned: bool,
    pub shard_map: ShardMap,
}

impl DbSettings {
    /// The settings `config` asks for. A configured placement for another
    /// shard count is replaced by one that keeps every shard here.
    pub fn of(config: &DbConfig) -> DbSettings {
        let q = config.q.max(1);
        DbSettings {
            q,
            partitioned: config.partitioned,
            shard_map: placement_for(config, q),
        }
    }

    /// The rows shard 0 of a new database is given, committed last when it
    /// is created: what it is, where its shards live, and an empty security
    /// object. Every one is required when the database opens.
    pub(crate) fn rows(&self) -> DbResult<Vec<(&'static str, Vec<u8>)>> {
        let info = DbRecord {
            q: self.q,
            uuid: uuid(),
            instance_start_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros()
                .to_string(),
            partitioned: self.partitioned,
        };
        Ok(vec![
            (INFO_SETTING, to_json(&info)?),
            (SHARD_MAP_SETTING, to_json(&self.shard_map)?),
            (SECURITY_SETTING, security_json(0, &json!({}))?),
        ])
    }
}

/// The configured placement of a database with `q` shards: the one
/// `config` carries when it is for `q` shards, else every shard here.
fn placement_for(config: &DbConfig, q: u32) -> ShardMap {
    if config.shard_map.q == q {
        config.shard_map.clone()
    } else {
        ShardMap::single(q)
    }
}

fn to_json<T: serde::Serialize>(value: &T) -> DbResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| ApiError::internal(error.to_string()))
}

fn security_json(version: u64, security: &Value) -> DbResult<Vec<u8>> {
    to_json(&json!({"version": version, "security": security}))
}

/// A database's settings as shard 0 stores them.
struct Stored {
    info: DbRecord,
    shard_map: ShardMap,
    security: (u64, Value),
    pending_shard_map: Option<ShardMap>,
}

impl Stored {
    /// Parse the `db/` settings of the database at `path`. Every one but the
    /// pending map is required: a database is created with all of them, so
    /// one missing or unreadable is damage and an error, never a default
    /// (an empty security object would make the database public).
    fn parse(path: &Path, rows: Vec<(String, Vec<u8>)>) -> DbResult<Stored> {
        let mut rows: HashMap<String, Vec<u8>> = rows.into_iter().collect();
        let mut take = |name: &str| rows.remove(name);
        fn parse<T: serde::de::DeserializeOwned>(
            path: &Path,
            name: &str,
            bytes: Option<Vec<u8>>,
        ) -> DbResult<T> {
            let bytes = bytes.ok_or_else(|| setting_error(path, name, "missing"))?;
            serde_json::from_slice(&bytes).map_err(|error| setting_error(path, name, error))
        }
        let info: DbRecord = parse(path, INFO_SETTING, take(INFO_SETTING))?;
        if info.q == 0 {
            return Err(setting_error(path, INFO_SETTING, "no shards"));
        }
        let shard_map: ShardMap = parse(path, SHARD_MAP_SETTING, take(SHARD_MAP_SETTING))?;
        if !shard_map.is_valid(info.q, shard_map.node_count) {
            return Err(setting_error(
                path,
                SHARD_MAP_SETTING,
                "not a valid shard map",
            ));
        }
        let stored: Value = parse(path, SECURITY_SETTING, take(SECURITY_SETTING))?;
        let security = match (
            stored.get("version").and_then(Value::as_u64),
            stored.get("security"),
        ) {
            (Some(version), Some(security)) => (version, security.clone()),
            _ => {
                return Err(setting_error(
                    path,
                    SECURITY_SETTING,
                    "expected an object with `version` and `security`",
                ))
            }
        };
        let pending_shard_map = match take(PENDING_SHARD_MAP_SETTING) {
            Some(bytes) => Some(parse(path, PENDING_SHARD_MAP_SETTING, Some(bytes))?),
            None => None,
        };
        Ok(Stored {
            info,
            shard_map,
            security,
            pending_shard_map,
        })
    }
}

/// One staged write of a document batch, applied once the whole batch is
/// staged.
enum Op {
    /// A revision's body.
    Body {
        id: String,
        rev: String,
        body: Vec<u8>,
    },
    /// The document's change at this sequence.
    Seq { seq: u64, id: String },
    /// The document's previous change, superseded.
    Unseq(u64),
}

/// Staged writes for one atomic commit.
type Ops = Vec<Op>;

/// Shard slot for a document id. Kept free so cluster code can map a view row
/// back to its owning shard without an open database handle.
pub fn shard_index(docid: &str, q: u32) -> usize {
    if q <= 1 {
        return 0;
    }
    let digest = Md5::digest(docid.as_bytes());
    let n = u32::from_be_bytes(digest[..4].try_into().unwrap());
    (n % q) as usize
}

/// Split a partitioned document id into `(partition, docid)`.
pub fn partition_of(docid: &str) -> Option<(&str, &str)> {
    let (partition, rest) = docid.split_once(':')?;
    if partition.is_empty() || rest.is_empty() || partition.starts_with('_') {
        return None;
    }
    Some((partition, rest))
}

/// Document ids in a partitioned database hash by their partition, so every
/// document in a partition lands on the same shard. Design/local documents are
/// global and keep hashing by their full id.
pub fn shard_index_for(docid: &str, q: u32, partitioned: bool) -> usize {
    if partitioned && !docid.starts_with('_') {
        if let Some((partition, _)) = partition_of(docid) {
            return shard_index(&format!("{partition}:foo"), q);
        }
    }
    shard_index(docid, q)
}

/// One position per shard of the database, by shard index: how far a feed
/// or an index has read each shard's own sequence. A missing entry reads as 0.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShardSince(pub Vec<u64>);

impl ShardSince {
    pub fn get(&self, shard: usize) -> u64 {
        self.0.get(shard).copied().unwrap_or(0)
    }

    fn set(&mut self, shard: usize, seq: u64) {
        if self.0.len() <= shard {
            self.0.resize(shard + 1, 0);
        }
        self.0[shard] = seq;
    }
}

/// How far an index has read one shard: the shard copy's uuid, its sequence,
/// and its purge sequence (CouchDB's index `purge_seq`).
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardPosition {
    pub uuid: String,
    pub seq: u64,
    pub purge_seq: u64,
}

/// How far an index (view or search) has read each shard, by shard index.
/// A cursor stored in another shape fails to parse, and its index is
/// rebuilt.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexCursor(pub Vec<ShardPosition>);

impl IndexCursor {
    /// The positions alone, for a changes read.
    pub fn since(&self) -> ShardSince {
        ShardSince(self.0.iter().map(|position| position.seq).collect())
    }

    /// Whether two cursors are over the same shard copies (same uuids, in
    /// order) with nothing purged from either since: rows read up to one
    /// can be carried up to the other by the changes in between. A purge
    /// sequence a shard published while it could not know its own
    /// ([`crate::store::UNKNOWN_PURGE_SEQ`]) matches nothing, itself
    /// included: rows read then may hold a purged document.
    pub fn same_copies_unpurged(&self, other: &IndexCursor) -> bool {
        self.0.len() == other.0.len()
            && self.0.iter().zip(&other.0).all(|(left, right)| {
                left.uuid == right.uuid
                    && left.purge_seq == right.purge_seq
                    && left.purge_seq != crate::store::UNKNOWN_PURGE_SEQ
            })
    }

    /// Whether an index that has read up to this cursor is up to date with
    /// the shards at `current`, and can be served as it is.
    pub fn is_current(&self, current: &IndexCursor) -> bool {
        self.same_copies_unpurged(current) && self == current
    }
}

/// Whether an index that has read up to `cursor` can be brought up to date
/// incrementally: it read these very shard copies (same uuids), none of
/// them is behind it, and none has purged anything since (a purge leaves no
/// change row to follow). Otherwise it is rebuilt from 0.
pub fn cursor_valid(cursor: &IndexCursor, db: &Db) -> bool {
    let current = db.index_cursor();
    cursor.same_copies_unpurged(&current)
        && cursor
            .0
            .iter()
            .zip(&current.0)
            .all(|(recorded, now)| recorded.seq <= now.seq)
}

/// Whether the shard at `index` is selected by an `owned` mask: every shard
/// when there is no mask, and never one the mask does not cover.
fn shard_owned(owned: Option<&ShardMask>, index: usize) -> bool {
    owned.is_none_or(|owned| owned.covers(index))
}

/// CouchDB partition-name rules: non-empty, no colon and not underscore-leading.
pub fn validate_partition(partition: &str) -> DbResult<()> {
    if partition.is_empty() {
        return Err(ApiError::new(
            400,
            "illegal_partition",
            "Partition must not be empty",
        ));
    }
    if partition.starts_with('_') {
        return Err(ApiError::new(
            400,
            "illegal_partition",
            "Partition must not start with an underscore",
        ));
    }
    if partition.contains(':') {
        return Err(ApiError::new(
            400,
            "illegal_partition",
            "Partition must not contain a colon",
        ));
    }
    Ok(())
}

/// Log a database setting that cannot be used and return the 500 for it.
/// The client sees the setting's name only; the log carries the path.
fn setting_error(path: &Path, setting: &str, error: impl std::fmt::Display) -> ApiError {
    tracing::error!(db = %path.display(), setting, %error, "database setting is unusable");
    ApiError::new(500, "internal_server_error", format!("{setting}: {error}"))
}

/// Log a shard file that cannot be opened or read and return the 500 for
/// it: the database fails closed, and the file is left as it is.
fn shard_error(file: &Path, error: impl std::fmt::Display) -> ApiError {
    tracing::error!(file = %file.display(), %error, "shard file is unusable");
    ApiError::new(
        500,
        "internal_server_error",
        format!("{}: {error}", file_name(file)),
    )
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Numbers the temporary files of this process.
static TEMPS: AtomicU64 = AtomicU64::new(0);

/// Write `bytes` to a new temporary file next to `path` and fsync it. A
/// crash before the caller publishes it leaves only the temporary file. A
/// name already taken (a leftover of an earlier process that had this pid,
/// as a container's pid 1 always does) is skipped for the next one, never
/// written over or removed.
fn write_temp(path: &Path, bytes: &[u8], private: bool) -> std::io::Result<PathBuf> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    #[cfg(not(unix))]
    let _ = private;
    let (temp, mut file) = loop {
        let temp = path.with_file_name(format!(
            ".{}.{}.{}.tmp",
            file_name(path),
            std::process::id(),
            TEMPS.fetch_add(1, Ordering::Relaxed)
        ));
        match options.open(&temp) {
            Ok(file) => break (temp, file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };
    match file.write_all(bytes).and_then(|()| file.sync_all()) {
        Ok(()) => Ok(temp),
        Err(error) => {
            let _ = std::fs::remove_file(&temp);
            Err(error)
        }
    }
}

/// Make the renames, links and new entries in directory `dir` durable.
pub(crate) fn sync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// Make a rename or link in the directory of `path` durable.
fn sync_parent(path: &Path) -> std::io::Result<()> {
    sync_dir(
        path.parent()
            .filter(|dir| !dir.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )
}

/// Replace `path` with `bytes` so that a reader, or a restart after a crash,
/// sees the old contents or the new ones, never a partial file: write a
/// temporary file in the same directory, fsync it, rename it over the target,
/// then fsync the directory.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let temp = write_temp(path, bytes, false)?;
    if let Err(error) = std::fs::rename(&temp, path) {
        let _ = std::fs::remove_file(&temp);
        return Err(error);
    }
    sync_parent(path)
}

/// Create `path` with `bytes`, readable by the owner only, failing with
/// `AlreadyExists` if it is there already. The file appears whole or not at
/// all: it is written to a temporary file and hard-linked into place.
pub(crate) fn create_private_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let temp = write_temp(path, bytes, true)?;
    let linked = std::fs::hard_link(&temp, path);
    let _ = std::fs::remove_file(&temp);
    linked?;
    sync_parent(path)
}

/// JSON text with object keys sorted, for ordering values independently of
/// the key order each node happened to receive.
pub(crate) fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            let body: Vec<String> = entries
                .into_iter()
                .map(|(key, value)| {
                    format!("{}:{}", Value::String(key.clone()), canonical_json(value))
                })
                .collect();
            format!("{{{}}}", body.join(","))
        }
        Value::Array(items) => {
            let body: Vec<String> = items.iter().map(canonical_json).collect();
            format!("[{}]", body.join(","))
        }
        other => other.to_string(),
    }
}

/// The placement metadata of a database that is not open.
pub struct ClosedMeta {
    pub q: u32,
    pub partitioned: bool,
    pub shard_map: ShardMap,
    pub pending_shard_map: bool,
}

/// A rebalance target and the id of the coordinator that announced it.
#[derive(PartialEq)]
struct Migration {
    map: ShardMap,
    id: String,
}

/// What a database caches from its design documents.
#[derive(Default)]
struct DesignCache {
    /// Cached `validate_doc_update` sources. Invalidated when a design
    /// document is written.
    validation: std::sync::RwLock<Option<Vec<String>>>,
    /// Bumped on every design-document write, so a validator load that raced
    /// with one is not cached.
    generation: AtomicU64,
    /// Design documents already read, with the committed sequence of their
    /// shard at the time (see `Db::design_doc`).
    docs: std::sync::Mutex<HashMap<String, (u64, Arc<Value>)>>,
    /// Bumped by the operations that change documents without moving a
    /// shard's sequence, so a read that raced with one is not cached.
    epoch: AtomicU64,
}

impl DesignCache {
    /// Everything cached is stale: bump the generation, drop the validation
    /// functions and every cached design document. Run after anything that
    /// may have changed a design document.
    fn invalidate(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        *self
            .validation
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = None;
        self.epoch.fetch_add(1, Ordering::SeqCst);
        self.docs
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clear();
    }
}

pub struct Db {
    pub name: String,
    pub path: PathBuf,
    pub q: u32,
    pub partitioned: bool,
    pub uuid: String,
    pub instance_start_time: String,
    /// The database's settings in memory, as shard 0 stores them. Each is
    /// shared with the task that stores a new value and installs it once
    /// it is durable (see `store_setting`).
    shard_map: Arc<std::sync::RwLock<ShardMap>>,
    security: Arc<std::sync::RwLock<(u64, Value)>>,
    pending_shard_map: Arc<std::sync::RwLock<Option<ShardMap>>>,
    /// Held from reading a setting to installing its replacement, so two
    /// updates on this node never interleave.
    setting_writes: Arc<tokio::sync::Mutex<()>>,
    /// Set when `open` put the configured placement in place of a map
    /// stored for another cluster size, until `store_replaced_shard_map`
    /// stores it.
    map_replaced: AtomicBool,
    /// Visible as `compact_running` in `GET /db`, and keeps the database
    /// open while a compaction runs.
    compact_running: AtomicBool,
    /// Target placement while a rebalance is being verified. Reads and writes
    /// continue to use the active `shard_map`; anti-entropy uses this map to
    /// seed the target replicas.
    migration_map: std::sync::RwLock<Option<Migration>>,
    /// Held shared by every cluster write for its full duration, including
    /// detached replica fan-out, and taken exclusively to drain them when a
    /// rebalance freezes the database.
    write_gate: Arc<tokio::sync::RwLock<()>>,
    /// Unix-millisecond deadline for the cluster-wide write freeze, or zero
    /// when writes are allowed. A deadline lets peers recover if the
    /// coordinating node disappears mid-rebalance.
    rebalance_until: AtomicU64,
    shards: Vec<ShardHandle>,
    /// Pinged by the shard actors after a flushed batch that committed a new
    /// sequence; feeds wait on it instead of polling.
    commits: Arc<tokio::sync::watch::Sender<()>>,
    /// What is cached from design documents. Shared with the task that
    /// awaits a write which may change one, and invalidates it once the
    /// write is done (see `invalidating`).
    design: Arc<DesignCache>,
    /// Set when the database is deleted; see `mark_deleted`.
    deleted: AtomicBool,
    /// Index rows view queries have looked at since the database opened.
    view_rows_visited: Arc<AtomicU64>,
}

/// Documents an index build reads at a time.
pub(crate) const MAP_DOCS_PAGE: usize = 1000;

/// Where a paged `map_docs_page` read has got to.
#[derive(Default)]
pub struct MapCursor {
    shard: usize,
    after: Option<String>,
}

#[derive(Clone)]
pub struct DbConfig {
    pub q: u32,
    pub revs_limit: u64,
    /// Page cache of each shard file (`--cache-mb`).
    pub cache_bytes: usize,
    pub shard_map: ShardMap,
    pub partitioned: bool,
}

/// Design documents `Db::design_doc` keeps.
const DESIGN_CACHE_LIMIT: usize = 64;

/// Upper bound for a cluster-wide rebalance write freeze. If the coordinator
/// disappears, peers resume writes after this window instead of staying frozen.
/// Default for `--rebalance-freeze-ms`.
pub const DEFAULT_REBALANCE_FREEZE_MS: u64 = 420_000;

static REBALANCE_FREEZE_MS: AtomicU64 = AtomicU64::new(DEFAULT_REBALANCE_FREEZE_MS);

/// Set the freeze bound (`--rebalance-freeze-ms`). Every freeze on this node,
/// whether it started the rebalance or accepted a peer's announcement, uses it.
pub fn set_rebalance_freeze_ms(ms: u64) {
    REBALANCE_FREEZE_MS.store(ms, Ordering::SeqCst);
}

fn now_millis() -> u64 {
    static BASE: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    BASE.get_or_init(std::time::Instant::now)
        .elapsed()
        .as_millis() as u64
}

/// The directory name of database `name` under the data root. The mapping is
/// flat and injective: the only character of a database name that is not
/// safe in a single path component is '/', which becomes `%2F`. '%' is not a
/// legal database-name character, so `dir_name_to_db_name` reverses it and two
/// names never share a directory. Names are lowercase, so a case-insensitive
/// filesystem cannot fold two of them together; '+', '$', '(' and ')' are
/// ordinary filename characters on every supported platform.
pub fn db_dir_name(name: &str) -> String {
    name.replace('/', "%2F")
}

/// The file of shard `index` of the database in `dir`. Built only here,
/// from the data root, so a file has one spelling everywhere, which is what
/// `kv` remembers verification by.
pub(crate) fn shard_file(dir: &Path, index: u32) -> PathBuf {
    dir.join(format!("shard-{index:04}.redb"))
}

/// Whether `name` is a shard file's name, `shard-NNNN.redb`.
fn is_shard_file_name(name: &str) -> bool {
    name.strip_prefix("shard-")
        .and_then(|rest| rest.strip_suffix(".redb"))
        .is_some_and(|digits| digits.len() == 4 && digits.bytes().all(|b| b.is_ascii_digit()))
}

/// Whether `dir` holds a database: a directory with at least one shard
/// file. A database that lost some of its shards (shard 0 included) is
/// still one, and fails closed when opened. A directory that merely has a
/// legal name (`lost+found` at the root of a volume) is not. One that lost
/// every shard file is not either: it is neither listed nor found, and a
/// create of its name answers 412, since publishing a new database cannot
/// replace a directory that is not empty.
pub(crate) fn is_db_dir(dir: &Path) -> bool {
    std::fs::read_dir(dir).is_ok_and(|entries| {
        entries.flatten().any(|entry| {
            entry.file_name().to_str().is_some_and(is_shard_file_name)
                && entry.file_type().is_ok_and(|kind| kind.is_file())
        })
    })
}

/// Peek shard `index` of the database in `dir` (see `kv::peek`): what
/// `read` returns, or a 500 naming the file when it is missing, damaged or
/// open elsewhere.
fn peek_shard<R>(
    dir: &Path,
    index: u32,
    read: impl FnOnce(&dyn redb::ReadableDatabase) -> anyhow::Result<R>,
) -> DbResult<R> {
    let file = shard_file(dir, index);
    match crate::kv::peek(&file, PEEK_CACHE, read) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(shard_error(&file, format!("{error:#}"))),
        Err(error) => Err(shard_error(&file, error)),
    }
}

/// The data-root directory of database `name`.
pub fn db_path(root: &Path, name: &str) -> PathBuf {
    root.join(db_dir_name(name))
}

/// The database a data-root directory name stands for, or `None` when the
/// name is not one `db_dir_name` produces.
pub fn dir_name_to_db_name(dir: &str) -> Option<String> {
    let name = dir.replace("%2F", "/");
    (!name.contains('%')).then_some(name)
}

/// A running compaction of a database, from [`Db::claim_compaction`]: the
/// database reports `compact_running` until this is dropped.
pub struct CompactionClaim<'a>(&'a AtomicBool);

impl Drop for CompactionClaim<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl Db {
    /// Open the database `name`, which must exist (see
    /// `state::create_db_staged`). Every shard file is opened as it is: one
    /// that is missing or cannot be opened fails the open with a 500 and is
    /// left untouched, never replaced, so damage is not mistaken for a new
    /// database. Blocking: shard files are checked on their first open.
    pub fn open(root: &Path, name: &str, config: DbConfig) -> DbResult<Db> {
        if !Db::exists(root, name) {
            return Err(ApiError::db_not_found());
        }
        let path = db_path(root, name);
        // A shard file renamed in place (a compaction swap whose directory
        // sync failed) is made durable before any write is acknowledged.
        sync_dir(&path).map_err(|error| {
            ApiError::internal(format!("cannot sync {}: {error}", path.display()))
        })?;
        // Every file is checked with a peek before any is opened for
        // writing: a damaged database fails here, its healthy files opened
        // read-only once verified, so a request to it neither writes to
        // them nor reads them in full again.
        let rows = peek_shard(&path, 0, |db| {
            crate::store::check_file(db, &shard_file(&path, 0))?;
            crate::store::read_settings(db, DB_SETTINGS)
        })?;
        let stored = Stored::parse(&path, rows)?;
        let q = stored.info.q;
        for index in 1..q {
            peek_shard(&path, index, |db| {
                crate::store::check_file(db, &shard_file(&path, index))
            })?;
        }

        let commits = Arc::new(tokio::sync::watch::Sender::new(()));
        let mut shards = Vec::new();
        for index in 0..q {
            let file = shard_file(&path, index);
            match ShardHandle::spawn(
                &file,
                config.revs_limit,
                config.cache_bytes,
                commits.clone(),
            ) {
                Ok(shard) => shards.push(shard),
                Err(error) => {
                    // Closed unchecked, as a damaged database's healthy
                    // shards are not to be read in full on every request.
                    for shard in shards {
                        shard.discard();
                    }
                    return Err(shard_error(&file, format!("{error:#}")));
                }
            }
        }

        // A map stored for a different cluster size gives way to the
        // configured placement (see `store_replaced_shard_map`).
        let map_replaced = stored.shard_map.node_count != config.shard_map.node_count;
        let shard_map = if map_replaced {
            tracing::warn!(
                db = %path.display(),
                "shard map is for a different cluster size; using the configured placement"
            );
            placement_for(&config, q)
        } else {
            stored.shard_map
        };

        Ok(Db {
            name: name.to_string(),
            path,
            q,
            partitioned: stored.info.partitioned,
            uuid: stored.info.uuid,
            instance_start_time: stored.info.instance_start_time,
            shard_map: Arc::new(std::sync::RwLock::new(shard_map)),
            security: Arc::new(std::sync::RwLock::new(stored.security)),
            pending_shard_map: Arc::new(std::sync::RwLock::new(stored.pending_shard_map)),
            setting_writes: Arc::default(),
            map_replaced: AtomicBool::new(map_replaced),
            compact_running: AtomicBool::new(false),
            migration_map: std::sync::RwLock::new(None),
            write_gate: Arc::new(tokio::sync::RwLock::new(())),
            rebalance_until: AtomicU64::new(0),
            shards,
            commits,
            design: Arc::default(),
            deleted: AtomicBool::new(false),
            view_rows_visited: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Open the database, creating it first (staged, as a client's create
    /// does) when it does not exist.
    #[cfg(test)]
    pub fn open_or_create(root: &Path, name: &str, config: DbConfig) -> DbResult<Db> {
        if !Db::exists(root, name) {
            crate::state::create_db_staged(root, name, &DbSettings::of(&config))?;
        }
        Db::open(root, name, config)
    }

    /// Whether database `name` exists: see [`is_db_dir`]. Whether its files
    /// can be opened is for the open to find out.
    pub fn exists(root: &Path, name: &str) -> bool {
        is_db_dir(&db_path(root, name))
    }

    /// The settings of a database that is not open, read from shard 0
    /// through `kv::peek`, which checks the file only when an open would
    /// and leaves it as verified as it found it. `None` when the file is
    /// open: the database is being opened or closed, so ask it instead.
    /// Blocking.
    fn read_stored(root: &Path, name: &str) -> DbResult<Option<Stored>> {
        if !Db::exists(root, name) {
            return Err(ApiError::db_not_found());
        }
        let path = db_path(root, name);
        let file = shard_file(&path, 0);
        match crate::kv::peek(&file, PEEK_CACHE, |db| {
            crate::store::read_settings(db, DB_SETTINGS)
        }) {
            Ok(Ok(rows)) => Stored::parse(&path, rows).map(Some),
            Ok(Err(error)) => Err(shard_error(&file, format!("{error:#}"))),
            Err(redb::DatabaseError::DatabaseAlreadyOpen) => Ok(None),
            // Deleted meanwhile.
            Err(_) if !path.is_dir() => Err(ApiError::db_not_found()),
            Err(error) => Err(shard_error(&file, error)),
        }
    }

    /// The security object of a database that is not open; `None` when it
    /// is open after all (see `read_stored`). Blocking.
    pub fn read_security(root: &Path, name: &str) -> DbResult<Option<Value>> {
        Ok(Db::read_stored(root, name)?.map(|stored| stored.security.1))
    }

    /// What `open` would settle on for placement, read without opening the
    /// database. `None` when only an open can say: the database is open
    /// (see `read_stored`), or its map is for another cluster size, which
    /// `open` replaces. Blocking.
    pub fn read_closed_meta(
        root: &Path,
        name: &str,
        node_count: usize,
    ) -> DbResult<Option<ClosedMeta>> {
        let Some(stored) = Db::read_stored(root, name)? else {
            return Ok(None);
        };
        if stored.shard_map.node_count != node_count {
            return Ok(None);
        }
        Ok(Some(ClosedMeta {
            q: stored.info.q,
            partitioned: stored.info.partitioned,
            shard_map: stored.shard_map,
            pending_shard_map: stored.pending_shard_map.is_some(),
        }))
    }

    /// Drop every shard's lazily built indexes so an idle database holds none
    /// of them; the next query rebuilds what it needs from the shards' records.
    pub async fn release_indexes(&self) {
        for shard in &self.shards {
            let _ = shard
                .read(|shard| {
                    shard.release_indexes();
                    Ok(())
                })
                .await;
        }
    }

    /// Close the shard files without the close-time integrity check when the
    /// last handle to this database goes: the process is exiting.
    pub fn skip_close_checks(&self) {
        for shard in &self.shards {
            shard.skip_close_check();
        }
    }

    /// One shard's actor, for tests that run jobs on it directly.
    #[cfg(test)]
    pub(crate) fn shard_handle(&self, index: usize) -> &ShardHandle {
        &self.shards[index]
    }

    /// Entries the shards hold in their live and latest-changes indexes.
    #[cfg(test)]
    pub async fn index_entries(&self) -> usize {
        let mut total = 0;
        for shard in &self.shards {
            total += shard
                .read(|shard| Ok(shard.index_entries()))
                .await
                .unwrap_or(0);
        }
        total
    }

    /// Whether the database can be closed and reopened without losing
    /// anything: no rebalance is freezing it, no compaction is running, and no
    /// cluster write holds its gate. What it keeps in memory beyond that is
    /// rebuilt from disk.
    pub fn quiescent(&self) -> bool {
        !self.rebalance_in_progress()
            && !self.compact_running.load(Ordering::SeqCst)
            && self.write_gate.try_write().is_ok()
    }

    /// Record that the database is being deleted. A request that still holds
    /// this handle then gets `not_found` from `check_live` instead of using
    /// the index directories, which a gate registered by path cannot ensure:
    /// it is dropped with the database, and the handle would take a fresh one.
    pub fn mark_deleted(&self) {
        self.deleted.store(true, Ordering::SeqCst);
    }

    /// `not_found` once the database has been deleted, as CouchDB answers a
    /// request for a database that is gone (`couch_server`).
    pub fn check_live(&self) -> DbResult<()> {
        if self.deleted.load(Ordering::SeqCst) {
            Err(ApiError::db_not_found())
        } else {
            Ok(())
        }
    }

    /// Stop every shard of a database being deleted and wait until their
    /// files are closed (see `ShardHandle::close_deleted`). Handles cloned
    /// before get `not_found` from then on.
    pub async fn close_deleted(&self) {
        self.mark_deleted();
        for shard in &self.shards {
            shard.close_deleted().await;
        }
    }

    pub fn shard_index(&self, docid: &str) -> usize {
        shard_index_for(docid, self.q, self.partitioned)
    }

    pub fn validate_doc_id(&self, id: &str) -> DbResult<()> {
        if id.starts_with("_design/") || id.starts_with("_local/") {
            return Ok(());
        }
        if self.partitioned {
            let Some((partition, rest)) = id.split_once(':') else {
                return Err(ApiError::new(
                    400,
                    "illegal_docid",
                    "Doc id must be of form partition:id",
                ));
            };
            validate_partition(partition)?;
            if rest.is_empty() {
                return Err(ApiError::new(
                    400,
                    "illegal_docid",
                    "Document id must not be empty",
                ));
            }
            if rest.starts_with('_') {
                return Err(ApiError::new(
                    400,
                    "illegal_docid",
                    "Only reserved document ids may start with underscore.",
                ));
            }
            return Ok(());
        }
        if id.is_empty() {
            return Err(ApiError::new(
                400,
                "illegal_docid",
                "Document id must not be empty",
            ));
        }
        if id.starts_with('_') {
            return Err(ApiError::new(
                400,
                "illegal_docid",
                "Only reserved document ids may start with underscore.",
            ));
        }
        Ok(())
    }

    /// Shard holding a partition. Used for partition metadata and scoped reads.
    pub fn partition_shard(&self, partition: &str) -> usize {
        shard_index(&format!("{partition}:foo"), self.q)
    }

    pub async fn partition_counts(
        &self,
        partition: &str,
    ) -> DbResult<crate::store::PartitionCounts> {
        let shard = &self.shards[self.partition_shard(partition)];
        let partition = partition.to_string();
        shard
            .read(move |s| s.partition_counts(&partition).map_err(ApiError::from))
            .await
    }

    pub fn security(&self) -> Value {
        self.security_versioned().1
    }

    /// The security object with its cluster version. The version lives beside
    /// the object so `GET /db/_security` shows only what CouchDB shows.
    pub fn security_versioned(&self) -> (u64, Value) {
        self.security
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    /// All `validate_doc_update` functions from live design documents.
    ///
    /// Loading is cached until a design document is written. The scan is a
    /// range over the shards' `_design/` metadata keys, so it reads neither
    /// ordinary documents nor the changes index.
    ///
    /// This is the validator set of every write, clustered or not: a write
    /// makes no peer call to find validators, as CouchDB's `ddoc_cache`
    /// serves them without reading every design document per write. Design
    /// documents are written to every node, so the local set is complete
    /// unless this node missed such a write. Anti-entropy (`--sync-secs`)
    /// pulls every design document from every peer, on startup and then on
    /// each pass, and a pull that applies one drops this cache; that is the
    /// bound on how long a node that missed the write validates without it.
    /// A corrupt or unreadable local design document is an error, never a
    /// skipped validator.
    pub async fn validation_functions(&self) -> DbResult<Vec<String>> {
        if let Some(functions) = self
            .design
            .validation
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
        {
            return Ok(functions);
        }

        let generation = self.design_generation();
        let mut ids = Vec::new();
        for shard in &self.shards {
            let (mut found, corrupt) = shard.read(|s| Ok(s.design_docs()?)).await?;
            if let Some(id) = corrupt.first() {
                // Skipping it would let writes through unvalidated.
                return Err(ApiError::corrupt(format!(
                    "corrupt design document {id}: its validate_doc_update cannot be loaded"
                )));
            }
            ids.append(&mut found);
        }
        ids.sort();
        ids.dedup();

        let mut functions = Vec::new();
        for id in ids {
            let shard = &self.shards[self.shard_index(&id)];
            let id_for_shard = id.clone();
            let doc = shard
                .read(move |s| {
                    let Some(meta) = s.get_meta(&id_for_shard)? else {
                        return Ok(None);
                    };
                    let Some(winner) = meta.tree.winner() else {
                        return Ok(None);
                    };
                    if meta.tree.is_deleted(&winner) {
                        return Ok(None);
                    }
                    read_doc_rev(s, &id_for_shard, &meta, &winner, &DocOpts::default()).map(Some)
                })
                .await?;
            if let Some(doc) = doc {
                if let Some(source) = doc.get("validate_doc_update").and_then(Value::as_str) {
                    functions.push(source.to_string());
                }
            }
        }

        let mut cached = self
            .design
            .validation
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        if self.design_generation() == generation {
            *cached = Some(functions.clone());
        }
        Ok(functions)
    }

    /// The winning revision of design document `id`, as `get_doc` returns it.
    ///
    /// A copy read earlier is served without a shard job while its shard has
    /// committed nothing since: any write to the document moves the shard's
    /// committed sequence, so the copy cannot be stale.
    pub(crate) async fn design_doc(&self, id: &str) -> DbResult<Arc<Value>> {
        let index = self.shard_index(id);
        let committed = self.shards[index].committed_seq();
        {
            let cached = self
                .design
                .docs
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if let Some((_, doc)) = cached.get(id).filter(|(seq, _)| *seq == committed) {
                return Ok(doc.clone());
            }
        }
        let epoch = self.design.epoch.load(Ordering::SeqCst);
        let doc = Arc::new(self.get_doc(id, &DocOpts::default()).await?);
        // A write committed while the read ran may or may not be in it; the
        // copy is cached only when the shard did not move.
        if self.shards[index].committed_seq() == committed {
            let mut cached = self
                .design
                .docs
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if self.design.epoch.load(Ordering::SeqCst) == epoch {
                // Few enough design documents are read per database that a
                // full cache is cleared rather than evicted from.
                if cached.len() >= DESIGN_CACHE_LIMIT {
                    cached.clear();
                }
                cached.insert(id.to_string(), (committed, doc.clone()));
            }
        }
        Ok(doc)
    }

    /// Await `commit` in a task of its own, then invalidate the design
    /// caches. A caller that stops waiting (a client that disconnects drops
    /// its request) cannot leave them stale: the commit still lands on the
    /// shard's actor, and the invalidation follows it.
    async fn invalidating<T: Send + 'static>(
        &self,
        commit: impl std::future::Future<Output = T> + Send + 'static,
    ) -> DbResult<T> {
        let design = Arc::clone(&self.design);
        tokio::spawn(async move {
            let done = commit.await;
            design.invalidate();
            done
        })
        .await
        .map_err(|error| ApiError::internal(format!("invalidating the design caches: {error}")))
    }

    /// Generation of this node's design documents.
    pub(crate) fn design_generation(&self) -> u64 {
        self.design.generation.load(Ordering::SeqCst)
    }

    /// Store database setting `name` (removing it for `None`) in shard 0,
    /// then `install` it in memory. Both run in a task of their own, so a
    /// caller that stops waiting cannot leave memory behind what is on disk;
    /// `writing`, the guard of `setting_writes`, is released only once the
    /// value is installed.
    ///
    /// A write that fails may still have landed (a commit can report an
    /// error and be on the disk), so the setting is then read back from
    /// shard 0 once it has recovered, and installed if it is what was
    /// written; the caller still hears the error. Every other setting is as
    /// it was: `writing` admits no other write. If it cannot be read back
    /// (shard 0 has failed), memory keeps what clients were last told, and
    /// the read back is parked on shard 0's actor: it runs, and installs
    /// the setting if the write landed, as soon as a retry brings the shard
    /// back, before any later settings write, so installs keep the order of
    /// the writes. A read back that meets an I/O failure is read again, as
    /// a failed shard is retried, before any later parked read back; if the
    /// shard serves meanwhile, later settings writes may run first, and it
    /// installs only if shard 0 still holds what it wrote, so it never
    /// undoes them. The caller hears its error at once and `writing` is
    /// not held meanwhile. A shard that never comes back never runs it.
    async fn store_setting(
        &self,
        writing: tokio::sync::OwnedMutexGuard<()>,
        name: &'static str,
        value: Option<Vec<u8>>,
        install: impl FnOnce() + Send + 'static,
    ) -> DbResult<()> {
        let attempted = value.clone();
        let write = self.shards[0].call(move |txn| {
            match &value {
                Some(value) => txn.put_setting(name, value),
                None => txn.remove_setting(name),
            }
            .map_err(ApiError::from)
        });
        // Made here, as the task cannot borrow the shard, so it counts as a
        // shard job even when the write succeeds and it is never sent. An
        // actor job, so it runs after the recovery.
        let read_back = self.shards[0].read(move |txn| Ok(txn.get_setting(name)?));
        let later = self.shards[0].when_serving();
        let path = self.path.clone();
        tokio::spawn(async move {
            let _writing = writing;
            let Err(error) = write.await else {
                install();
                return Ok(());
            };
            match read_back.await {
                Ok(stored) if stored == attempted => install(),
                Ok(_) => {}
                Err(read) => {
                    tracing::warn!(
                        db = %path.display(),
                        setting = name,
                        error = %read.reason,
                        "the setting cannot be read back after a failed write; it is read back once the shard serves again"
                    );
                    later
                        .run(
                            move |txn| txn.get_setting(name),
                            move |stored| {
                                if stored == attempted {
                                    install();
                                }
                            },
                        )
                        .await;
                }
            }
            Err(error)
        })
        .await
        .map_err(|error| ApiError::internal(format!("storing {name}: {error}")))?
    }

    async fn lock_settings(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.setting_writes.clone().lock_owned().await
    }

    /// Replace the security object with a client-supplied one, one version
    /// newer than the current one. Returns the version it was stored at.
    pub async fn set_security(&self, value: Value) -> DbResult<u64> {
        // Pick the version under the lock: two concurrent writes on this node
        // must not share a version, or the tiebreak could drop an acknowledged
        // one.
        let writing = self.lock_settings().await;
        let version = self.security_versioned().0.saturating_add(1);
        self.store_security(writing, version, value).await?;
        Ok(version)
    }

    /// Store a security object at `version` unless the current one is newer.
    /// Equal versions are ordered by the canonical form of the object so that
    /// concurrent updates settle on the same winner on every node. Returns
    /// whether the object is now the stored one.
    pub async fn set_security_versioned(&self, version: u64, value: Value) -> DbResult<bool> {
        let writing = self.lock_settings().await;
        let current = self.security_versioned();
        if version < current.0 {
            return Ok(false);
        }
        if version == current.0 {
            match canonical_json(&value).cmp(&canonical_json(&current.1)) {
                std::cmp::Ordering::Equal => return Ok(true),
                std::cmp::Ordering::Less => return Ok(false),
                std::cmp::Ordering::Greater => {}
            }
        }
        self.store_security(writing, version, value).await?;
        Ok(true)
    }

    /// Persist and install a security object; the caller holds the lock.
    async fn store_security(
        &self,
        writing: tokio::sync::OwnedMutexGuard<()>,
        version: u64,
        value: Value,
    ) -> DbResult<()> {
        let bytes = security_json(version, &value)?;
        let security = self.security.clone();
        self.store_setting(writing, SECURITY_SETTING, Some(bytes), move || {
            *security
                .write()
                .unwrap_or_else(|poison| poison.into_inner()) = (version, value);
        })
        .await
    }

    pub fn shard_map(&self) -> ShardMap {
        self.shard_map
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    /// Copies of each shard, from the current shard map.
    pub fn copies(&self) -> usize {
        self.shard_map
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .n()
    }

    /// CouchDB's `cluster` block in database info: shards, copies, and the
    /// default write and read quorums.
    fn cluster_info(&self) -> ClusterInfo {
        let n = self.copies();
        ClusterInfo {
            q: self.q,
            n,
            w: n / 2 + 1,
            r: n / 2 + 1,
        }
    }

    pub async fn set_shard_map(&self, shard_map: ShardMap) -> DbResult<bool> {
        if !shard_map.is_valid(self.q, shard_map.node_count) {
            return Err(ApiError::bad_request("invalid shard map"));
        }
        // Hold the lock across compare, persist and install, so two calls on
        // this node cannot interleave and leave the stored and the in-memory
        // map disagreeing.
        let writing = self.lock_settings().await;
        let current = self.shard_map();
        if shard_map.version < current.version {
            return Ok(false);
        }
        if shard_map.version == current.version {
            // Concurrent updates can share a version. Order the placements so
            // every node keeps the same one regardless of arrival order.
            match shard_map.replicas.cmp(&current.replicas) {
                std::cmp::Ordering::Equal => return Ok(true),
                std::cmp::Ordering::Less => return Ok(false),
                std::cmp::Ordering::Greater => {}
            }
        }
        self.store_shard_map(writing, shard_map).await?;
        Ok(true)
    }

    /// Store a shard map placed by a client, one version newer than the
    /// current one, and return it with that version. The version is picked
    /// under the lock, as `set_security` does, so two concurrent updates on
    /// this node never share one.
    pub async fn set_next_shard_map(&self, mut shard_map: ShardMap) -> DbResult<ShardMap> {
        if !shard_map.is_valid(self.q, shard_map.node_count) {
            return Err(ApiError::bad_request("invalid shard map"));
        }
        let writing = self.lock_settings().await;
        shard_map.version = self.shard_map().version.saturating_add(1);
        self.store_shard_map(writing, shard_map.clone()).await?;
        Ok(shard_map)
    }

    /// Store the placement `open` put in place of a map stored for another
    /// cluster size, so later opens and reads of the closed database find
    /// it. Nothing to do when the stored map was kept; a failure leaves it
    /// to be stored on the next call.
    pub async fn store_replaced_shard_map(&self) -> DbResult<()> {
        if !self.map_replaced.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        let writing = self.lock_settings().await;
        let stored = self.store_shard_map(writing, self.shard_map()).await;
        if stored.is_err() {
            self.map_replaced.store(true, Ordering::SeqCst);
        }
        stored
    }

    /// Persist and install a shard map; the caller holds the lock.
    async fn store_shard_map(
        &self,
        writing: tokio::sync::OwnedMutexGuard<()>,
        shard_map: ShardMap,
    ) -> DbResult<()> {
        let bytes = to_json(&shard_map)?;
        let current = self.shard_map.clone();
        self.store_setting(writing, SHARD_MAP_SETTING, Some(bytes), move || {
            *current.write().unwrap_or_else(|poison| poison.into_inner()) = shard_map;
        })
        .await
    }

    /// The migration target while a rebalance freeze is running. A map left
    /// behind after the freeze expired (a coordinator that vanished) is stale
    /// and must not steer placement or anti-entropy.
    fn migration_target(&self) -> Option<ShardMap> {
        if !self.rebalance_in_progress() {
            return None;
        }
        self.migration_map
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .map(|migration| migration.map.clone())
    }

    /// Placement targeted by an in-progress rebalance. Falls back to the
    /// active map outside a migration.
    pub fn replication_map(&self) -> ShardMap {
        self.migration_target().unwrap_or_else(|| self.shard_map())
    }

    /// Whether `node` replicates the shard that contains `docid` under the
    /// migration target if one is active, otherwise under the active map.
    pub fn is_replica_of(&self, docid: &str, node: usize) -> bool {
        // Design documents are replicated to every node so each can evaluate
        // views and search over its local shards; this matches normal write
        // placement and must hold for migration targets too.
        if docid.starts_with("_design/") {
            return true;
        }
        let shard = self.shard_index(docid) as u32;
        self.replication_map().replicas_of(shard).contains(&node)
    }

    pub fn set_migration_map(&self, map: ShardMap, id: String) -> DbResult<()> {
        if !map.is_valid(self.q, map.node_count) {
            return Err(ApiError::bad_request("invalid migration shard map"));
        }
        *self
            .migration_map
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(Migration { map, id });
        Ok(())
    }

    /// Accept a migration announced by a coordinator and freeze writes. A
    /// different migration while one is in progress is refused, so two
    /// coordinators cannot run at once and one cannot unfreeze the other;
    /// repeating the same one is harmless.
    pub fn begin_migration(&self, map: ShardMap, id: String) -> DbResult<()> {
        if !map.is_valid(self.q, map.node_count) {
            return Err(ApiError::bad_request("invalid migration shard map"));
        }
        let mut current = self
            .migration_map
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        let incoming = Migration { map, id };
        if self.rebalance_in_progress() && current.as_ref() != Some(&incoming) {
            return Err(ApiError::new(
                409,
                "rebalance_in_progress",
                "another migration is already in progress for this database",
            ));
        }
        *current = Some(incoming);
        self.set_rebalance_in_progress(true);
        Ok(())
    }

    /// Shared guard held by a cluster write until it, and every replica write
    /// it started, has finished.
    pub async fn write_guard(&self) -> tokio::sync::OwnedRwLockReadGuard<()> {
        self.write_gate.clone().read_owned().await
    }

    /// Wait for every write holding the gate to finish. Returns false if they
    /// did not within `timeout`. Call after the freeze is set: writes that
    /// start later see the freeze and are refused.
    pub async fn drain_writes(&self, timeout: std::time::Duration) -> bool {
        tokio::time::timeout(timeout, self.write_gate.write())
            .await
            .is_ok()
    }

    /// Clear the migration on behalf of the coordinator that announced it with
    /// `id`. Returns false, leaving the migration and its freeze in place, when
    /// another coordinator's migration is in progress. Nothing to clear and an
    /// expired freeze (the coordinator vanished) are cleared by any caller.
    pub fn clear_migration_if(&self, id: &str) -> bool {
        let mut current = self
            .migration_map
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        let Some(migration) = current.as_ref() else {
            return true;
        };
        if self.rebalance_in_progress() && migration.id != id {
            return false;
        }
        *current = None;
        self.set_rebalance_in_progress(false);
        true
    }

    pub fn clear_migration_map(&self) {
        *self
            .migration_map
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = None;
    }

    pub fn rebalance_in_progress(&self) -> bool {
        let until = self.rebalance_until.load(Ordering::SeqCst);
        until != 0 && now_millis() < until
    }

    /// Claim the single rebalance slot for this database and freeze writes for
    /// a bounded verification window.
    pub fn try_start_rebalance(&self) -> bool {
        let deadline = now_millis().saturating_add(REBALANCE_FREEZE_MS.load(Ordering::SeqCst));
        loop {
            let current = self.rebalance_until.load(Ordering::SeqCst);
            if current != 0 && now_millis() < current {
                return false;
            }
            if self
                .rebalance_until
                .compare_exchange(current, deadline, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }

    pub fn set_rebalance_in_progress(&self, in_progress: bool) {
        self.rebalance_until.store(
            if in_progress {
                now_millis().saturating_add(REBALANCE_FREEZE_MS.load(Ordering::SeqCst))
            } else {
                0
            },
            Ordering::SeqCst,
        );
    }

    /// Shard-actor jobs submitted for this database since it opened.
    pub fn shard_jobs(&self) -> u64 {
        self.shards.iter().map(ShardHandle::jobs).sum()
    }

    /// Index entries this database's shards have walked to answer reads since
    /// it opened: the work a request did, whatever the time it took.
    pub fn shard_scanned(&self) -> u64 {
        self.shards.iter().map(ShardHandle::scanned).sum()
    }

    /// View index rows this database's queries have looked at since it opened:
    /// the work a view query did, whatever the time it took.
    pub fn view_rows_visited(&self) -> u64 {
        self.view_rows_visited.load(Ordering::Relaxed)
    }

    pub(crate) fn view_rows_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.view_rows_visited)
    }

    /// Document bodies this database's shards have read from storage since it
    /// opened.
    pub fn shard_body_reads(&self) -> u64 {
        self.shards.iter().map(ShardHandle::body_reads).sum()
    }

    /// Every shard's uuid and committed position, by shard index. A position
    /// is committed once its batch is on disk; every acknowledged write is
    /// inside it.
    pub fn shard_seqs(&self) -> Vec<(String, u64)> {
        self.shards
            .iter()
            .map(|shard| (shard.uuid(), shard.committed_seq()))
            .collect()
    }

    /// Every shard's uuid, committed position and purge sequence, by shard
    /// index: what an index records as read when it reads the shards now.
    /// The purge sequences are taken before an index reads anything, so a
    /// purge that lands while it reads leaves the recorded one behind, and
    /// the next use rebuilds.
    pub fn index_cursor(&self) -> IndexCursor {
        IndexCursor(
            self.shards
                .iter()
                .map(|shard| ShardPosition {
                    purge_seq: shard.purge_seq(),
                    uuid: shard.uuid(),
                    seq: shard.committed_seq(),
                })
                .collect(),
        )
    }

    /// Where a feed starts on each shard of this node, for a `since` that
    /// was parsed already. A token position counts only for the copy it
    /// names (this node, the same shard, the same uuid); any other shard
    /// starts from 0, so a cleared or recreated copy is read again in full
    /// rather than skipped.
    pub(crate) fn shard_since(&self, me: usize, since: &crate::since::Since) -> ShardSince {
        use crate::since::Since;
        match since {
            Since::Zero => ShardSince(vec![0; self.shards.len()]),
            Since::Now => ShardSince(self.shard_seqs().into_iter().map(|(_, seq)| seq).collect()),
            Since::Token(copies) => ShardSince(
                self.shards
                    .iter()
                    .enumerate()
                    .map(|(index, shard)| {
                        let uuid = shard.uuid();
                        copies
                            .iter()
                            .find(|copy| {
                                copy.node == me && copy.shard == index && copy.uuid == uuid
                            })
                            .map_or(0, |copy| copy.seq)
                    })
                    .collect(),
            ),
        }
    }

    /// This node's copies of `shards` at `positions`, as an opaque token.
    pub(crate) fn local_token(
        &self,
        me: usize,
        shards: &[usize],
        positions: &ShardSince,
    ) -> String {
        let copies: Vec<crate::since::CopyPos> = shards
            .iter()
            .map(|&shard| crate::since::CopyPos {
                node: me,
                shard,
                seq: positions.get(shard),
                uuid: self.shards[shard].uuid(),
            })
            .collect();
        crate::since::encode(&copies)
    }

    /// The shard map this node has not had acknowledged, if any.
    pub fn pending_shard_map(&self) -> Option<ShardMap> {
        self.pending_shard_map
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    pub async fn set_pending_shard_map(&self, map: &ShardMap) -> DbResult<()> {
        self.store_pending_shard_map(Some(map.clone())).await
    }

    /// Remove the pending shard map, durably. Nothing pending is fine.
    pub async fn clear_pending_shard_map(&self) -> DbResult<()> {
        self.store_pending_shard_map(None).await
    }

    async fn store_pending_shard_map(&self, map: Option<ShardMap>) -> DbResult<()> {
        // A clear writes even with nothing pending in memory: a pending map
        // whose write failed yet landed may be on disk.
        let writing = self.lock_settings().await;
        let bytes = map.as_ref().map(to_json).transpose()?;
        let pending = self.pending_shard_map.clone();
        self.store_setting(writing, PENDING_SHARD_MAP_SETTING, bytes, move || {
            *pending.write().unwrap_or_else(|poison| poison.into_inner()) = map;
        })
        .await
    }

    pub fn replicas_of_shard(&self, shard: u32) -> Vec<usize> {
        self.shard_map
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .replicas_of(shard)
            .to_vec()
    }

    pub fn owner_of_shard(&self, shard: u32) -> usize {
        self.shard_map
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .owner_of(shard)
    }

    pub fn replicas_of_doc(&self, docid: &str) -> Vec<usize> {
        if docid.starts_with("_design/") {
            let node_count = self
                .shard_map
                .read()
                .unwrap_or_else(|poison| poison.into_inner())
                .node_count;
            return (0..node_count).collect();
        }
        self.replicas_of_shard(self.shard_index(docid) as u32)
    }

    pub async fn clear_shards(&self, shards: &[usize]) -> DbResult<()> {
        let clears: Vec<_> = shards
            .iter()
            .filter_map(|&index| self.shards.get(index))
            .map(|shard| shard.call(|s| s.clear().map_err(ApiError::from)))
            .collect();
        // One shard after another, stopping at the first that fails; a
        // cleared shard may have held design documents.
        self.invalidating(async move {
            for clear in clears {
                clear.await?;
            }
            Ok(())
        })
        .await?
    }

    pub async fn shard_counts(&self) -> DbResult<Vec<(u64, u64)>> {
        let mut out = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            out.push(shard.read(|s| Ok(s.counts())).await?);
        }
        Ok(out)
    }

    pub async fn shard_states(
        &self,
        with_digest: bool,
    ) -> DbResult<Vec<crate::store::ShardSummary>> {
        let mut out = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            out.push(
                shard
                    .read(move |s| s.summary(with_digest).map_err(ApiError::from))
                    .await?,
            );
        }
        Ok(out)
    }

    fn shard(&self, docid: &str) -> &ShardHandle {
        &self.shards[self.shard_index(docid)]
    }

    /// A receiver that changes whenever a write commits on this node. Mark it
    /// seen (`borrow_and_update`) before reading, so a commit that lands
    /// during the read is not missed.
    pub fn subscribe_commits(&self) -> tokio::sync::watch::Receiver<()> {
        self.commits.subscribe()
    }

    /// Owner and replica list for a document, computed with one shard-map
    /// read and one shard hash. Write routing needs both for every document.
    pub fn placement_of_doc(&self, docid: &str) -> (usize, Vec<usize>) {
        let shard = self.shard_index(docid) as u32;
        let map = self
            .shard_map
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        let owner = map.owner_of(shard);
        if docid.starts_with("_design/") {
            return (owner, (0..map.node_count).collect());
        }
        (owner, map.replicas_of(shard).to_vec())
    }

    pub async fn info(&self) -> DbResult<DbInfo> {
        // Snapshot the flag before queueing on the shard actors: a
        // compaction's swap holds its shard's actor, so reading the flag
        // afterwards could report the post-compaction state instead.
        let compact_running = self.compact_running.load(Ordering::SeqCst);
        let shard_seqs = self.shard_seqs();
        let update_seq = shard_seqs
            .iter()
            .fold(0u64, |sum, (_, seq)| sum.saturating_add(*seq));
        let mut doc_count = 0;
        let mut deleted_count = 0;
        let mut purge_seq = 0u64;
        let mut shard_purge_seqs = Vec::with_capacity(self.shards.len());
        let mut sizes = DbSizes::default();
        let counts = futures::future::join_all(
            self.shards
                .iter()
                .map(|shard| shard.read(|s| Ok((s.counts(), s.purge_seq()?, s.external_size())))),
        )
        .await;
        for (index, (counted, shard)) in counts.into_iter().zip(&self.shards).enumerate() {
            let ((docs, deleted), purged, external) = counted?;
            doc_count += docs;
            deleted_count += deleted;
            purge_seq = purge_seq.saturating_add(purged);
            shard_purge_seqs.push(purged);
            sizes.add(self.shard_sizes(index, shard, external));
        }
        Ok(DbInfo {
            db_name: self.name.clone(),
            doc_count,
            doc_del_count: deleted_count,
            update_seq,
            purge_seq,
            compact_running,
            // Deprecated; CouchDB 3's clustered `GET /db` no longer sends
            // them, but douchdb keeps them, equal to `file` and `active`.
            disk_size: sizes.file,
            data_size: sizes.active,
            sizes,
            instance_start_time: self.instance_start_time.clone(),
            disk_format_version: 8,
            committed_update_seq: update_seq,
            uuid: self.uuid.clone(),
            cluster: self.cluster_info(),
            props: self.partitioned.then_some(DbProps { partitioned: true }),
            shard_seqs,
            shard_purge_seqs,
        })
    }

    // ----- documents -------------------------------------------------------

    pub async fn put_docs<D>(&self, docs: Vec<D>, new_edits: bool) -> DbResult<Vec<WriteResult>>
    where
        D: std::borrow::Borrow<Value> + Send + 'static,
    {
        if docs
            .iter()
            .any(|doc| doc.borrow().get("_attachments").is_some())
        {
            return Err(attachments_unsupported());
        }
        let touches_design = docs.iter().any(|doc| {
            doc.borrow()
                .get("_id")
                .and_then(Value::as_str)
                .map(|id| id.starts_with("_design/"))
                .unwrap_or(false)
        });
        let mut out: Vec<Option<WriteResult>> = (0..docs.len()).map(|_| None).collect();
        let mut grouped: Vec<Vec<(usize, Prepared)>> = (0..self.q).map(|_| Vec::new()).collect();
        let mut ids: Vec<Option<String>> = vec![None; docs.len()];

        let mut locals: Vec<(usize, Value)> = Vec::new();
        let mut pending: Vec<(usize, String, D)> = Vec::new();
        for (i, doc) in docs.into_iter().enumerate() {
            let value = doc.borrow();
            // `_local/` documents have no revision tree and no sequence; they
            // are applied to the shard of their id further down.
            if value
                .get("_id")
                .and_then(Value::as_str)
                .is_some_and(|id| id.starts_with("_local/"))
            {
                locals.push((i, value.clone()));
                continue;
            }
            match doc_id(value) {
                Ok(id) => match self.validate_doc_id(&id) {
                    Ok(()) => pending.push((i, id, doc)),
                    Err(e) => out[i] = Some(WriteResult::error(Some(Value::String(id)), e)),
                },
                Err(e) => out[i] = Some(WriteResult::error(None, e)),
            }
        }
        // Splitting, serialising and hashing happen here, not on the shard's
        // writer; a large batch uses the blocking pool.
        let prepare_all = move |pending: Vec<(usize, String, D)>| {
            pending
                .into_iter()
                .map(|(i, id, doc)| (i, id.clone(), prepare(doc.borrow(), id, new_edits)))
                .collect::<Vec<_>>()
        };
        let prepared = if pending.len() > PREPARE_INLINE_MAX {
            #[cfg(test)]
            BLOCKING_PREPARES.fetch_add(1, Ordering::SeqCst);
            tokio::task::spawn_blocking(move || prepare_all(pending))
                .await
                .map_err(ApiError::internal)?
        } else {
            prepare_all(pending)
        };
        for (i, id, result) in prepared {
            match result {
                Ok(doc) => {
                    let shard = self.shard_index(&doc.id);
                    ids[i] = Some(doc.id.clone());
                    grouped[shard].push((i, doc));
                }
                Err(e) => out[i] = Some(WriteResult::error(Some(Value::String(id)), e)),
            }
        }

        // A design document's functions compile here, outside any shard job,
        // so the compile never holds a shard's writer.
        let mut compile_errors = if touches_design {
            compile_design_docs(&self.shards, &grouped, new_edits).await?
        } else {
            HashMap::new()
        };
        // Every shard's batch is sent at once; each commits on its own.
        let calls: Vec<_> = grouped
            .into_iter()
            .enumerate()
            .filter(|(_, batch)| !batch.is_empty())
            .map(|(shard_idx, batch)| {
                let verdicts: HashMap<usize, ApiError> = batch
                    .iter()
                    .filter_map(|(i, _)| compile_errors.remove(i).map(|error| (*i, error)))
                    .collect();
                self.shards[shard_idx].call(move |s| Ok(put_many(s, batch, new_edits, verdicts)))
            })
            .collect();
        // A batch with a design document drops the design caches once every
        // shard has answered, whether or not one failed: the others may have
        // committed design documents.
        let answers = if touches_design {
            self.invalidating(futures::future::join_all(calls)).await?
        } else {
            futures::future::join_all(calls).await
        };
        // The lowest shard's error is held back until every shard's results
        // are in.
        let mut shard_error = None;
        for answer in answers {
            let results = match answer {
                Ok(results) => results,
                Err(error) => {
                    shard_error = shard_error.or(Some(error));
                    continue;
                }
            };
            for (i, result) in results {
                out[i] = Some(match result {
                    Ok(ok) => WriteResult::Ok(ok),
                    Err(e) => WriteResult::error(
                        Some(ids[i].clone().map_or(Value::Null, Value::String)),
                        e,
                    ),
                });
            }
        }

        for (slot, result) in self.put_local_docs(locals).await {
            out[slot] = Some(result);
        }

        if let Some(error) = shard_error {
            return Err(error);
        }
        Ok(out
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    WriteResult::error(None, ApiError::internal("no result for the document"))
                })
            })
            .collect())
    }

    pub async fn get_doc(&self, id: &str, opts: &DocOpts) -> DbResult<Value> {
        let id = id.to_string();
        let opts = opts.clone();
        self.shard(id.as_str())
            .lease(move |s| read_doc(s, &id, &opts))
            .await
    }

    /// Read several documents with one round trip per shard, the shards in
    /// parallel. Each document has its own result, in the order of `ids`; the
    /// outer error is a shard that could not answer at all.
    pub(crate) async fn get_docs(
        &self,
        ids: &[String],
        opts: &DocOpts,
    ) -> DbResult<Vec<DbResult<Value>>> {
        let mut grouped: Vec<Vec<(usize, String)>> = (0..self.q).map(|_| Vec::new()).collect();
        for (slot, id) in ids.iter().enumerate() {
            grouped[self.shard_index(id)].push((slot, id.clone()));
        }
        let read = futures::future::join_all(
            grouped
                .into_iter()
                .enumerate()
                .filter(|(_, batch)| !batch.is_empty())
                .map(|(index, batch)| {
                    let opts = opts.clone();
                    self.shards[index].lease(move |s| {
                        Ok(batch
                            .into_iter()
                            .map(|(slot, id)| (slot, read_doc(s, &id, &opts)))
                            .collect::<Vec<_>>())
                    })
                }),
        )
        .await;
        let mut out: Vec<Option<DbResult<Value>>> = ids.iter().map(|_| None).collect();
        for batch in read {
            for (slot, result) in batch? {
                out[slot] = Some(result);
            }
        }
        Ok(out
            .into_iter()
            .map(|result| result.unwrap_or_else(|| Err(ApiError::not_found("missing"))))
            .collect())
    }

    /// The current body of each of `ids` that names a document, with one job
    /// per shard and nothing else (`all_docs` with `keys` also counts the
    /// shard's documents, a second job). A deleted document maps to `null`;
    /// an id that is missing, or that names a design or local document, is
    /// left out.
    pub(crate) async fn docs_by_ids(&self, ids: &[String]) -> DbResult<HashMap<String, Value>> {
        let mut grouped: Vec<Vec<String>> = (0..self.q).map(|_| Vec::new()).collect();
        for id in ids {
            if id.starts_with("_design/") || id.starts_with("_local/") {
                continue;
            }
            grouped[self.shard_index(id)].push(id.clone());
        }
        let opts = AllDocsOpts {
            include_docs: true,
            ..Default::default()
        };
        let fetched = futures::future::join_all(
            grouped
                .into_iter()
                .enumerate()
                .filter(|(_, batch)| !batch.is_empty())
                .map(|(index, batch)| {
                    let opts = opts.clone();
                    self.shards[index].lease(move |s| {
                        batch
                            .into_iter()
                            .filter_map(|id| match all_docs_key_row(s, &id, &opts) {
                                Ok(Some(row)) => row.doc.map(|doc| Ok((id, doc))),
                                Ok(None) => None,
                                Err(error) => Some(Err(error)),
                            })
                            .collect::<DbResult<Vec<_>>>()
                    })
                }),
        )
        .await;
        let mut docs = HashMap::new();
        for batch in fetched {
            docs.extend(batch?);
        }
        Ok(docs)
    }

    pub async fn all_docs(&self, opts: &AllDocsOpts) -> DbResult<DocList> {
        self.all_docs_filtered(opts, None).await
    }

    /// Like `all_docs`, but only shards whose `owned` entry is true. Cluster
    /// coordinators use this to fetch each document from exactly one primary
    /// instead of merging the same replica from every node.
    pub async fn all_docs_owned(&self, opts: &AllDocsOpts, owned: &ShardMask) -> DbResult<DocList> {
        self.all_docs_filtered(opts, Some(owned)).await
    }

    /// `opts` with a partition query's range narrowed to the partition's key
    /// range, and the one shard that holds the partition.
    fn partition_range(&self, opts: &AllDocsOpts) -> (AllDocsOpts, Option<usize>) {
        let mut opts = opts.clone();
        let partition_shard = opts
            .partition
            .as_ref()
            .map(|partition| self.partition_shard(partition));
        if let Some(partition) = &opts.partition {
            // CouchDB intersects the requested range with the partition's
            // [partition:, partition;) key range.
            let first = format!("{partition}:");
            let last = format!("{partition};");
            if opts.descending {
                opts.startkey = Some(match &opts.startkey {
                    Some(start) => start.min(&last).clone(),
                    None => last.clone(),
                });
                opts.endkey = Some(match &opts.endkey {
                    Some(end) => end.max(&first).clone(),
                    None => first.clone(),
                });
            } else {
                opts.startkey = Some(match &opts.startkey {
                    Some(start) => start.max(&first).clone(),
                    None => first.clone(),
                });
                opts.endkey = Some(match &opts.endkey {
                    Some(end) => end.min(&last).clone(),
                    None => last.clone(),
                });
            }
        }
        (opts, partition_shard)
    }

    async fn all_docs_filtered(
        &self,
        opts: &AllDocsOpts,
        owned: Option<&ShardMask>,
    ) -> DbResult<DocList> {
        if let Some(keys) = &opts.keys {
            return self.all_docs_by_keys(opts, keys, owned).await;
        }

        // A shard only needs to return enough rows to satisfy the global
        // `skip + limit`: any row in the global window is also in its shard's
        // first `skip + limit` rows.
        let take = opts.skip.saturating_add(opts.limit);
        let (opts, partition_shard) = self.partition_range(opts);
        let mut rows = Vec::new();
        let mut total_rows = 0u64;
        let mut before_start = 0usize;
        let collected = futures::future::join_all(
            self.shards
                .iter()
                .enumerate()
                .filter(|(index, _)| {
                    partition_shard.is_none_or(|partition| partition == *index)
                        && shard_owned(owned, *index)
                })
                .map(|(_, shard)| {
                    let opts = opts.clone();
                    shard.read(move |s| {
                        let docs = all_docs_live_total(s, &opts)?;
                        let (rows, offset) = collect_all_docs(s, &opts, take)?;
                        Ok((rows, offset, docs))
                    })
                }),
        )
        .await;
        for collected in collected {
            let (mut shard_rows, shard_offset, shard_docs) = collected?;
            rows.append(&mut shard_rows);
            before_start += shard_offset;
            total_rows += shard_docs;
        }
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        if opts.descending {
            rows.reverse();
        }
        let selected: Vec<_> = rows.into_iter().skip(opts.skip).take(opts.limit).collect();
        let offset = (before_start + opts.skip).min(total_rows as usize);
        finish_all_docs(selected, total_rows as usize, offset)
    }

    /// The `total_rows` and `offset` of an `_all_docs` query and a stream of
    /// its rows, for a query without `keys`. The rows are merged by id from a
    /// page at a time of each shard, so a listing of any size is never held
    /// whole. Unlike `all_docs`, the pages are read as the stream is polled,
    /// not from one snapshot, and a failure after the first page ends the
    /// stream with an error.
    pub(crate) async fn all_docs_stream(
        db: Arc<Db>,
        opts: &AllDocsOpts,
    ) -> DbResult<(
        AllDocsHead,
        impl futures::Stream<Item = DbResult<DocListRow>>,
    )> {
        const PAGE: usize = 256;

        struct Cursor {
            index: usize,
            after: Option<String>,
            buffer: std::collections::VecDeque<AllDocsRow>,
            /// How many of the front rows of `buffer` have their documents.
            /// Rows read while a skip is pending are only ids: they are
            /// likely to be skipped, so their documents are read once the
            /// rows that will be returned are known.
            loaded: usize,
            done: bool,
        }

        let (opts, partition_shard) = db.partition_range(opts);
        let mut total_rows = 0u64;
        let mut before_start = 0usize;
        let mut cursors = Vec::new();
        for (index, shard) in db.shards.iter().enumerate() {
            if partition_shard.is_some_and(|partition| partition != index) {
                continue;
            }
            let shard_opts = opts.clone();
            let (before, docs) = shard
                .read(move |s| {
                    let docs = all_docs_live_total(s, &shard_opts)?;
                    Ok((all_docs_before_start(s, &shard_opts)?, docs))
                })
                .await?;
            before_start += before;
            total_rows += docs;
            cursors.push(Cursor {
                index,
                after: None,
                buffer: std::collections::VecDeque::new(),
                loaded: 0,
                done: false,
            });
        }
        let head = AllDocsHead {
            total_rows: total_rows as usize,
            offset: before_start
                .saturating_add(opts.skip)
                .min(total_rows as usize),
        };

        let rows = async_stream::stream! {
            let mut skip = opts.skip;
            let mut remaining = opts.limit;
            while remaining > 0 {
                let wanted = skip.saturating_add(remaining).min(PAGE);
                // Rows that will be skipped are only stepped over.
                let load = skip == 0;
                let hungry: Vec<usize> = (0..cursors.len())
                    .filter(|slot| cursors[*slot].buffer.is_empty() && !cursors[*slot].done)
                    .collect();
                if !hungry.is_empty() {
                    let fetches = hungry.iter().map(|slot| {
                        let (index, after) = (cursors[*slot].index, cursors[*slot].after.clone());
                        let shard_opts = opts.clone();
                        let shard = &db.shards[index];
                        async move {
                            shard
                                .read(move |s| {
                                    let scanned =
                                        scan_all_docs(s, &shard_opts, after.as_deref(), wanted)?;
                                    let last = scanned.last().map(|row| row.id.clone());
                                    let exhausted = scanned.len() < wanted;
                                    let rows = if load {
                                        load_all_docs(s, scanned, &shard_opts)?
                                    } else {
                                        scanned
                                    };
                                    Ok((rows, last, exhausted))
                                })
                                .await
                        }
                    });
                    let pages = futures::future::join_all(fetches).await;
                    for (slot, page) in hungry.into_iter().zip(pages) {
                        match page {
                            Ok((rows, last, exhausted)) => {
                                let cursor = &mut cursors[slot];
                                cursor.buffer.extend(rows);
                                cursor.loaded = if load { cursor.buffer.len() } else { 0 };
                                if last.is_some() {
                                    cursor.after = last;
                                }
                                cursor.done = exhausted;
                            }
                            Err(error) => {
                                yield Err(error);
                                return;
                            }
                        }
                    }
                    continue;
                }
                if opts.include_docs && skip == 0 {
                    // The rows the merge is certain to return next, as far as
                    // the buffers reach: read exactly their documents.
                    let mut taken = vec![0usize; cursors.len()];
                    for _ in 0..remaining {
                        let mut best: Option<usize> = None;
                        let mut blocked = false;
                        for (slot, cursor) in cursors.iter().enumerate() {
                            let Some(row) = cursor.buffer.get(taken[slot]) else {
                                blocked |= !cursor.done;
                                continue;
                            };
                            let better = match best.and_then(|best| cursors[best].buffer.get(taken[best])) {
                                None => true,
                                Some(current) if opts.descending => row.id > current.id,
                                Some(current) => row.id < current.id,
                            };
                            if better {
                                best = Some(slot);
                            }
                        }
                        let Some(best) = best.filter(|_| !blocked) else {
                            break;
                        };
                        taken[best] += 1;
                    }
                    let loads: Vec<usize> = (0..cursors.len())
                        .filter(|slot| taken[*slot] > cursors[*slot].loaded)
                        .collect();
                    if !loads.is_empty() {
                        let mut rests = Vec::with_capacity(loads.len());
                        let mut fetches = Vec::with_capacity(loads.len());
                        for slot in &loads {
                            let cursor = &mut cursors[*slot];
                            // `buffer` keeps the loaded rows; the ones to load
                            // and the ones still bare are taken off its end.
                            let mut wanted = cursor.buffer.split_off(cursor.loaded);
                            rests.push(wanted.split_off(taken[*slot] - cursor.loaded));
                            let shard_opts = opts.clone();
                            fetches.push(db.shards[cursor.index].read(move |s| {
                                load_all_docs(s, wanted.into_iter().collect(), &shard_opts)
                            }));
                        }
                        let results = futures::future::join_all(fetches).await;
                        for ((slot, rest), result) in loads.into_iter().zip(rests).zip(results) {
                            let cursor = &mut cursors[slot];
                            match result {
                                Ok(rows) => {
                                    cursor.loaded += rows.len();
                                    cursor.buffer.extend(rows);
                                    cursor.buffer.extend(rest);
                                }
                                Err(error) => {
                                    yield Err(error);
                                    return;
                                }
                            }
                        }
                        continue;
                    }
                }
                let mut best: Option<usize> = None;
                for (slot, cursor) in cursors.iter().enumerate() {
                    let Some(front) = cursor.buffer.front() else {
                        continue;
                    };
                    let better = match best.and_then(|best| cursors[best].buffer.front()) {
                        None => true,
                        Some(current) if opts.descending => front.id > current.id,
                        Some(current) => front.id < current.id,
                    };
                    if better {
                        best = Some(slot);
                    }
                }
                let Some(slot) = best else {
                    break;
                };
                let cursor = &mut cursors[slot];
                let Some(row) = cursor.buffer.pop_front() else {
                    break;
                };
                cursor.loaded = cursor.loaded.saturating_sub(1);
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                remaining -= 1;
                yield Ok(all_docs_row(row));
            }
        };
        Ok((head, rows))
    }

    /// Every page `map_docs_page` would return, for tests to compare with.
    #[cfg(test)]
    pub async fn map_docs(&self, owned: Option<&ShardMask>) -> DbResult<Vec<(String, String)>> {
        let mut docs = Vec::new();
        let mut cursor = MapCursor::default();
        loop {
            let mut page = self
                .map_docs_page(owned, &mut cursor, MAP_DOCS_PAGE)
                .await?;
            if page.is_empty() {
                return Ok(docs);
            }
            docs.append(&mut page);
        }
    }

    /// The next at most `limit` documents for a view or search build, in shard
    /// and id order, and none once they are all read. A caller that maps each
    /// page before asking for the next holds one page of bodies, not every
    /// body in the database. Each is JSON text with `_id` and `_rev` already
    /// spliced into the stored body, which avoids parsing every body into a
    /// `Value` and serializing it again just to hand it to QuickJS. Cluster
    /// nodes pass their owned shards so replicas do not pay to index
    /// documents they never serve.
    pub async fn map_docs_page(
        &self,
        owned: Option<&ShardMask>,
        cursor: &mut MapCursor,
        limit: usize,
    ) -> DbResult<Vec<(String, String)>> {
        let shards = self.selected_shards(owned);
        while let Some(&index) = shards.get(cursor.shard) {
            let after = cursor.after.take();
            let (docs, last) = self.shards[index]
                .lease(move |s| {
                    // One page of live documents from the metadata records
                    // of the lease's snapshot, then their bodies.
                    let metas = s.live_docs_after(after.as_deref(), limit, |id| {
                        !id.starts_with("_design/") && !id.starts_with("_local/")
                    })?;
                    let last = (metas.len() == limit)
                        .then(|| metas.last().map(|(id, _)| id.clone()))
                        .flatten();

                    let mut docs = Vec::with_capacity(metas.len());
                    for (id, rev) in metas {
                        if let Some(body) = s.get_body(&id, &rev)? {
                            // The body is spliced into the map input as
                            // text, so check it decodes here or one bad
                            // body would fail the whole build.
                            let decoded = serde_json::from_slice::<serde::de::IgnoredAny>(&body)
                                .map_err(|error| {
                                    ApiError::corrupt(format!(
                                        "corrupt body for {id} {rev}: {error}"
                                    ))
                                });
                            if skip_corrupt(s, &id, decoded)?.is_some() {
                                docs.push((id.clone(), embed_doc_meta(&id, &rev, &body)));
                            }
                            continue;
                        }
                        // The body is gone: take the full read path, which
                        // reports it the way every other read does.
                        let doc_opts = DocOpts {
                            rev: Some(rev),
                            ..Default::default()
                        };
                        let doc = read_doc(s, &id, &doc_opts);
                        if let Some(doc) = skip_corrupt(s, &id, doc)? {
                            docs.push((
                                id,
                                serde_json::to_string(&doc).map_err(ApiError::internal)?,
                            ));
                        }
                    }
                    Ok((docs, last))
                })
                .await?;
            match last {
                Some(last) => cursor.after = Some(last),
                None => cursor.shard += 1,
            }
            if !docs.is_empty() {
                return Ok(docs);
            }
        }
        Ok(Vec::new())
    }

    /// The `total_rows` of a `keys` query.
    async fn keys_total_rows(
        &self,
        opts: &AllDocsOpts,
        owned: Option<&ShardMask>,
    ) -> DbResult<u64> {
        let mut total_rows = 0u64;
        if let Some(partition) = &opts.partition {
            let shard = self.partition_shard(partition);
            if !shard_owned(owned, shard) {
                total_rows = 0;
            } else {
                let partition = partition.clone();
                total_rows = self.shards[shard]
                    .read(move |s| s.partition_live_count(&partition).map_err(ApiError::from))
                    .await?;
            }
        } else {
            let counts =
                futures::future::join_all(self.selected_shards(owned).into_iter().map(|index| {
                    let opts = opts.clone();
                    self.shards[index].read(move |s| all_docs_live_total(s, &opts))
                }))
                .await;
            for count in counts {
                total_rows += count?;
            }
        }
        Ok(total_rows)
    }

    async fn all_docs_by_keys(
        &self,
        opts: &AllDocsOpts,
        keys: &[Value],
        owned: Option<&ShardMask>,
    ) -> DbResult<DocList> {
        let total_rows = self.keys_total_rows(opts, owned).await?;
        let page = keys_page(opts, keys);
        Ok(DocList {
            total_rows: Some(total_rows),
            offset: None,
            rows: self.keys_page_rows(opts, &page, owned).await?,
            update_seq: None,
        })
    }

    /// Like `all_docs` with `keys`, but the rows come back as a stream, a
    /// chunk of keys at a time. The `total_rows` leads the reply.
    pub(crate) async fn all_docs_keys_stream(
        db: Arc<Db>,
        opts: &AllDocsOpts,
    ) -> DbResult<(u64, impl futures::Stream<Item = DbResult<DocListRow>>)> {
        const CHUNK: usize = 256;
        let keys = opts.keys.clone().unwrap_or_default();
        let total_rows = db.keys_total_rows(opts, None).await?;
        let page = keys_page(opts, &keys);
        let opts = opts.clone();
        let rows = async_stream::stream! {
            for chunk in page.chunks(CHUNK) {
                match db.keys_page_rows(&opts, chunk, None).await {
                    Ok(rows) => {
                        for row in rows {
                            yield Ok(row);
                        }
                    }
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                }
            }
        };
        Ok((total_rows, rows))
    }

    /// The rows for `page`, a run of the query's keys in the order they are
    /// answered: one per key, a `not_found` row for a key with no document.
    async fn keys_page_rows(
        &self,
        opts: &AllDocsOpts,
        page: &[Value],
        owned: Option<&ShardMask>,
    ) -> DbResult<Vec<DocListRow>> {
        let mut grouped: Vec<Vec<(usize, String)>> = (0..self.q).map(|_| Vec::new()).collect();
        let mut included = vec![false; page.len()];
        if opts.partition.is_some() {
            included.fill(true);
        }
        for (slot, key) in page.iter().enumerate() {
            // CouchDB answers a key that is not a string with a `not_found`
            // row; it names no document on any shard.
            let Some(key) = key.as_str() else {
                included[slot] = true;
                continue;
            };
            if let Some(partition) = &opts.partition {
                if !key.starts_with(&format!("{partition}:")) {
                    continue;
                }
            }
            let shard = self.shard_index(key);
            if !shard_owned(owned, shard) {
                continue;
            }
            included[slot] = true;
            grouped[shard].push((slot, key.to_string()));
        }

        let mut results: Vec<Option<DocListRow>> = (0..page.len()).map(|_| None).collect();
        let fetched = futures::future::join_all(
            grouped
                .into_iter()
                .enumerate()
                .filter(|(_, batch)| !batch.is_empty())
                .map(|(index, batch)| {
                    let opts = opts.clone();
                    self.shards[index].read(move |s| {
                        let rows = batch
                            .into_iter()
                            .map(|(slot, key)| {
                                all_docs_key_row(s, &key, &opts).map(|row| (slot, row))
                            })
                            .collect::<DbResult<Vec<_>>>()?;
                        Ok(rows)
                    })
                }),
        )
        .await;
        for rows in fetched {
            for (slot, row) in rows? {
                results[slot] =
                    Some(row.unwrap_or_else(|| DocListRow::not_found(page[slot].clone())));
            }
        }

        Ok(results
            .into_iter()
            .enumerate()
            .filter(|(slot, _)| included[*slot])
            .map(|(slot, row)| row.unwrap_or_else(|| DocListRow::not_found(page[slot].clone())))
            .collect())
    }

    /// The changes rows, each shard's position after them, and how many
    /// changes remain after those positions.
    pub(crate) async fn changes_rows(
        &self,
        opts: &ChangesOpts,
    ) -> DbResult<(Vec<ChangeRow>, ShardSince, usize)> {
        let (rows, last, reached) = self.read_changes(opts).await?;
        let pending = if opts.descending {
            // A shard the read never reached has everything up to its
            // start still ahead of it, its newest change included.
            let mut before = last.clone();
            for (shard, reached) in reached.iter().enumerate() {
                if !reached {
                    before.set(shard, last.get(shard).saturating_add(1));
                }
            }
            self.changes_pending(&before, true, opts.owned.as_ref())
                .await?
        } else {
            self.changes_pending(&last, false, opts.owned.as_ref())
                .await?
        };
        Ok((rows, last, pending))
    }

    /// `changes_rows` without `pending`, which costs a further shard job per
    /// shard. For callers that only consume rows, such as index updates.
    ///
    /// Each shard's rows come in that shard's own sequence order; there is
    /// no order across shards (as in CouchDB). The shards are read a page at
    /// a time in turn, and a limited read stops as soon as it has enough. A
    /// filtered read's page is 64 to 1024 rows; an unfiltered read's is the
    /// rows still wanted, so without a limit each shard is read whole in one
    /// job. Every shard is read at most up to its position in `until`, or
    /// else its committed position when the read starts.
    ///
    /// The positions returned say, per selected shard, how far the rows
    /// cover it: the last row returned when the limit cut its rows short,
    /// else the last document scanned, matching or not
    /// (`couch_changes:changes_enumerator`), and the shard's bound when it
    /// was read to the end going up. Shards not read keep their `since`.
    pub(crate) async fn changes_rows_only(
        &self,
        opts: &ChangesOpts,
    ) -> DbResult<(Vec<ChangeRow>, ShardSince)> {
        let (rows, last, _) = self.read_changes(opts).await?;
        Ok((rows, last))
    }

    /// `changes_rows_only`, also saying which shards the read reached.
    async fn read_changes(
        &self,
        opts: &ChangesOpts,
    ) -> DbResult<(Vec<ChangeRow>, ShardSince, Vec<bool>)> {
        let bound = self.changes_bound(opts);
        let selected = self.selected_shards(opts.owned.as_ref());
        // A descending read starts each shard at its bound, so a shard it
        // never reaches still has everything below that ahead of it.
        let mut last = ShardSince(
            (0..self.shards.len())
                .map(|shard| {
                    if opts.descending && selected.contains(&shard) {
                        bound.get(shard)
                    } else {
                        opts.since.get(shard)
                    }
                })
                .collect(),
        );
        let limit = opts.limit.unwrap_or(usize::MAX);
        let mut reached = vec![false; self.shards.len()];
        let mut rows: Vec<ChangeRow> = Vec::new();
        // Shards still to read, each with where its next page starts: after
        // this sequence going up, at or below it going down.
        let mut open: Vec<(usize, u64)> = selected
            .into_iter()
            .map(|index| {
                let start = if opts.descending {
                    bound.get(index)
                } else {
                    opts.since.get(index)
                };
                (index, start)
            })
            .collect();
        while rows.len() < limit && !open.is_empty() {
            let mut still_open = Vec::with_capacity(open.len());
            for (index, cursor) in open {
                let want = limit - rows.len();
                if want == 0 {
                    break;
                }
                let page = if opts.filter.is_some() {
                    want.clamp(64, 1024)
                } else {
                    want
                };
                let shard_opts = opts.clone();
                let until = bound.get(index);
                let read = self.shards[index]
                    .read(move |s| collect_changes(s, &shard_opts, index, cursor, until, page))
                    .await?;
                reached[index] = true;
                let mut matched = match &opts.filter {
                    Some(filter) => apply_change_filter(read.rows, filter).await?,
                    None => read.rows,
                };
                if matched.len() > want {
                    // Cut short by the limit: the shard is covered up to the
                    // last row returned.
                    matched.truncate(want);
                    last.set(index, matched.last().map_or(cursor, |row| row.seq));
                    rows.append(&mut matched);
                    continue;
                }
                rows.append(&mut matched);
                if let Some(scanned) = read.scanned {
                    last.set(index, scanned);
                }
                if read.exhausted {
                    if !opts.descending {
                        last.set(index, until.max(opts.since.get(index)));
                    }
                    continue;
                }
                let next = match read.scanned {
                    Some(scanned) if opts.descending => scanned.checked_sub(1),
                    Some(scanned) => Some(scanned),
                    None => None,
                };
                if let Some(next) = next {
                    still_open.push((index, next));
                }
            }
            open = still_open;
        }
        Ok((rows, last, reached))
    }

    /// The bound of each shard for a read: `opts.until`, else the committed
    /// positions now.
    fn changes_bound(&self, opts: &ChangesOpts) -> ShardSince {
        opts.until.clone().unwrap_or_else(|| {
            ShardSince(self.shards.iter().map(ShardHandle::committed_seq).collect())
        })
    }

    /// Indexes of the shards in `owned`, or every shard.
    fn selected_shards(&self, owned: Option<&ShardMask>) -> Vec<usize> {
        (0..self.shards.len())
            .filter(|index| shard_owned(owned, *index))
            .collect()
    }

    pub(crate) async fn changes_pending(
        &self,
        after: &ShardSince,
        descending: bool,
        owned: Option<&ShardMask>,
    ) -> DbResult<usize> {
        futures::future::join_all(self.selected_shards(owned).into_iter().map(|index| {
            let after = after.get(index);
            self.shards[index]
                .read(move |s| s.pending_changes(after, descending).map_err(ApiError::from))
        }))
        .await
        .into_iter()
        .sum::<DbResult<usize>>()
    }

    /// The changes feed of this node's copies of the selected shards, with
    /// every position as an opaque token naming node `me`. With `full_rows`
    /// each row's `seq` holds every selected shard's position as of that row
    /// (what a client resumes from); without, only the row's own shard (what
    /// a coordinator merging nodes needs).
    pub async fn changes(
        &self,
        me: usize,
        opts: &ChangesOpts,
        full_rows: bool,
    ) -> DbResult<ChangesList> {
        // A descending read starts each selected shard at its bound, so the
        // rows' positions start there too (CouchDB starts each reverse worker
        // at its shard's update_seq). The bound is fixed here so the read and
        // the row positions share it.
        let bound = opts.descending.then(|| self.changes_bound(opts));
        let bounded = bound.clone().map(|bound| ChangesOpts {
            until: Some(bound),
            ..opts.clone()
        });
        let opts = bounded.as_ref().unwrap_or(opts);
        let (rows, last, pending) = self.changes_rows(opts).await?;
        let selected = self.selected_shards(opts.owned.as_ref());
        let mut progress = ShardSince(
            (0..self.shards.len())
                .map(|shard| match &bound {
                    Some(bound) if selected.contains(&shard) => bound.get(shard),
                    _ => opts.since.get(shard),
                })
                .collect(),
        );
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let doc = match row.doc {
                Some(doc) => Some(serde_json::from_str::<Value>(&doc).map_err(ApiError::internal)?),
                None => None,
            };
            progress.set(row.shard, row.seq);
            let seq = if full_rows {
                self.local_token(me, &selected, &progress)
            } else {
                self.local_token(me, &[row.shard], &progress)
            };
            results.push(ChangeEntry {
                seq,
                id: row.id,
                changes: row
                    .changes
                    .into_iter()
                    .map(|(rev, _)| ChangeRev { rev })
                    .collect(),
                deleted: row.deleted,
                doc,
            });
        }
        Ok(ChangesList {
            results,
            last_seq: self.local_token(me, &selected, &last),
            pending: pending as u64,
        })
    }

    /// Stream changes rows shard by shard, a page at a time, so a full feed
    /// does not materialize the whole response. After each shard it yields
    /// that shard's position, the bound it was read to.
    pub(crate) fn changes_stream_arc(
        db: Arc<Db>,
        opts: ChangesOpts,
    ) -> impl futures::Stream<Item = DbResult<ChangesItem>> + 'static {
        async_stream::stream! {
            const PAGE: usize = 1024;
            if opts.limit == Some(0) {
                return;
            }
            if opts.descending {
                yield Err(ApiError::new(
                    500,
                    "internal_server_error",
                    "streaming changes do not support descending order",
                ));
                return;
            }
            // One snapshot for the whole stream, taken before any shard read.
            let bound = db.changes_bound(&opts);
            let mut emitted = 0usize;
            for index in db.selected_shards(opts.owned.as_ref()) {
                let mut cursor = opts.since.get(index);
                let until = bound.get(index);
                loop {
                    // A shard never needs more rows than the request can
                    // still return. A filter may discard rows, so it reads
                    // at least a small page.
                    let left = opts.limit.map_or(usize::MAX, |limit| limit - emitted);
                    let page = match &opts.filter {
                        Some(_) => left.clamp(64, PAGE),
                        None => left.min(PAGE),
                    };
                    let shard_opts = opts.clone();
                    let read = match db.shards[index]
                        .read(move |shard| collect_changes(shard, &shard_opts, index, cursor, until, page))
                        .await
                    {
                        Ok(read) => read,
                        Err(error) => {
                            yield Err(error);
                            return;
                        }
                    };
                    let rows = match &opts.filter {
                        Some(filter) => match apply_change_filter(read.rows, filter).await {
                            Ok(rows) => rows,
                            Err(error) => {
                                yield Err(error);
                                return;
                            }
                        },
                        None => read.rows,
                    };
                    for row in rows {
                        emitted += 1;
                        yield Ok(ChangesItem::Row(row));
                        if opts.limit.is_some_and(|limit| emitted >= limit) {
                            return;
                        }
                    }
                    match read.scanned.filter(|_| !read.exhausted) {
                        Some(scanned) => cursor = scanned,
                        None => {
                            yield Ok(ChangesItem::Scanned(index, until.max(opts.since.get(index))));
                            break;
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn change_row_json(row: &ChangeRow, seq: &str) -> String {
        fn push_json_string(out: &mut String, value: &str) {
            if value
                .bytes()
                .all(|byte| byte >= 0x20 && byte != b'"' && byte != b'\\')
            {
                out.push('"');
                out.push_str(value);
                out.push('"');
            } else {
                out.push_str(&serde_json::to_string(value).unwrap_or_default());
            }
        }

        let mut out = String::with_capacity(128);
        out.push_str("{\"seq\":");
        out.push_str(seq);
        out.push_str(",\"id\":");
        push_json_string(&mut out, &row.id);
        out.push_str(",\"changes\":[");
        for (change_index, (rev, _)) in row.changes.iter().enumerate() {
            if change_index > 0 {
                out.push(',');
            }
            out.push_str("{\"rev\":");
            push_json_string(&mut out, rev);
            out.push('}');
        }
        out.push(']');
        if row.deleted {
            out.push_str(",\"deleted\":true");
        }
        if let Some(doc) = &row.doc {
            out.push_str(",\"doc\":");
            out.push_str(doc);
        }
        out.push('}');
        out
    }

    pub async fn revs_diff(&self, body: &Value) -> DbResult<RevsDiff> {
        let Some(map) = body.as_object() else {
            return Err(ApiError::bad_request("Request body must be a JSON object"));
        };
        // One round trip per shard instead of one per document.
        let mut ids = Vec::new();
        let mut grouped: Vec<Vec<(usize, String, Vec<String>)>> =
            (0..self.q).map(|_| Vec::new()).collect();
        for (id, revs) in map {
            let Some(list) = revs.as_array() else {
                continue;
            };
            let wanted: Vec<String> = list
                .iter()
                .filter_map(|r| r.as_str().map(str::to_string))
                .collect();
            grouped[self.shard_index(id)].push((ids.len(), id.clone(), wanted));
            ids.push(id);
        }
        let diffs = futures::future::join_all(
            grouped
                .into_iter()
                .enumerate()
                .filter(|(_, batch)| !batch.is_empty())
                .map(|(index, batch)| {
                    self.shards[index].lease(move |s| {
                        batch
                            .into_iter()
                            .map(|(slot, id, wanted)| {
                                revs_diff_one(s, &id, &wanted).map(|diff| (slot, diff))
                            })
                            .collect::<DbResult<Vec<_>>>()
                    })
                }),
        )
        .await;
        let mut found: Vec<Option<(Vec<String>, Vec<String>)>> = ids.iter().map(|_| None).collect();
        for batch in diffs {
            for (slot, diff) in batch? {
                found[slot] = Some(diff);
            }
        }
        let mut out = Vec::new();
        for (id, diff) in ids.into_iter().zip(found) {
            let Some((missing, possible_ancestors)) = diff else {
                continue;
            };
            if !missing.is_empty() {
                out.push((
                    id.clone(),
                    RevsDiffEntry {
                        missing,
                        possible_ancestors,
                    },
                ));
            }
        }
        Ok(RevsDiff(out))
    }

    pub async fn bulk_get(
        &self,
        body: &Value,
        revs: bool,
        latest: bool,
    ) -> DbResult<BulkGetResults> {
        let Some(reqs) = body.get("docs").and_then(Value::as_array) else {
            return Err(ApiError::bad_request("Missing JSON list of 'docs'"));
        };
        Ok(BulkGetResults {
            results: self.bulk_get_chunk(reqs, revs, latest).await?,
        })
    }

    /// Like `bulk_get`, but the results come back as a stream, a chunk of
    /// requests at a time, so a large batch never sits in memory whole. The
    /// results are in request order.
    pub async fn bulk_get_stream(
        db: Arc<Db>,
        body: Value,
        revs: bool,
        latest: bool,
    ) -> DbResult<impl futures::Stream<Item = DbResult<Option<BulkGetResult>>>> {
        const CHUNK: usize = 256;
        let Some(reqs) = body.get("docs").and_then(Value::as_array).cloned() else {
            return Err(ApiError::bad_request("Missing JSON list of 'docs'"));
        };
        Ok(async_stream::stream! {
            for chunk in reqs.chunks(CHUNK) {
                match db.bulk_get_chunk(chunk, revs, latest).await {
                    Ok(results) => {
                        for result in results {
                            yield Ok(result);
                        }
                    }
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                }
            }
        })
    }

    async fn bulk_get_chunk(
        &self,
        reqs: &[Value],
        revs: bool,
        latest: bool,
    ) -> DbResult<Vec<Option<BulkGetResult>>> {
        // One shard-actor round trip per shard instead of one per document.
        // `_bulk_get` is on the hot replication path, so the channel hops are
        // the difference between a batch and a request storm.
        let mut grouped: Vec<Vec<(usize, String, DocOpts)>> =
            (0..self.q).map(|_| Vec::new()).collect();
        for (slot, req) in reqs.iter().enumerate() {
            let id = req
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let rev = req.get("rev").and_then(Value::as_str).map(str::to_string);
            let opts = DocOpts {
                rev,
                revs: req.get("revs").and_then(Value::as_bool).unwrap_or(revs),
                latest: req.get("latest").and_then(Value::as_bool).unwrap_or(latest),
                include_deleted: true,
                ..Default::default()
            };
            grouped[self.shard_index(&id)].push((slot, id, opts));
        }

        let mut out: Vec<Option<BulkGetResult>> = (0..reqs.len()).map(|_| None).collect();
        let read = futures::future::join_all(
            grouped
                .into_iter()
                .enumerate()
                .filter(|(_, batch)| !batch.is_empty())
                .map(|(index, batch)| {
                    self.shards[index].lease(move |shard| {
                        let mut out = Vec::with_capacity(batch.len());
                        for (slot, id, opts) in batch {
                            let rev = opts
                                .rev
                                .as_deref()
                                .map_or_else(|| "undefined".into(), canonical_rev);
                            let result = read_doc(shard, &id, &opts);
                            out.push((slot, id, rev, result));
                        }
                        Ok(out)
                    })
                }),
        )
        .await;
        for results in read {
            for (slot, id, rev, result) in results? {
                let doc = match result {
                    Ok(doc) => BulkGetDoc::Doc(doc),
                    Err(error) => BulkGetDoc::Error(BulkGetError {
                        id: id.clone(),
                        rev,
                        error: error.error.to_string(),
                        reason: error.reason,
                    }),
                };
                out[slot] = Some(BulkGetResult {
                    id,
                    docs: vec![doc],
                });
            }
        }
        Ok(out)
    }

    // ----- local documents -------------------------------------------------

    /// The shard that holds `_local/{id}`: the same hash as any document, so
    /// a local document lives on the replicas of the shard its id falls in.
    fn local_shard(&self, id: &str) -> &ShardHandle {
        &self.shards[self.shard_index(&format!("_local/{id}"))]
    }

    pub async fn local_get(&self, id: &str) -> DbResult<Option<Value>> {
        let id = id.to_string();
        self.local_shard(&id)
            .read(move |s| read_local(s, &id))
            .await
    }

    pub async fn local_put(&self, id: &str, body: Value) -> DbResult<WriteOk> {
        let id = id.to_string();
        self.local_shard(&id)
            .call(move |s| write_local(s, &id, body))
            .await
    }

    pub async fn local_delete(&self, id: &str) -> DbResult<WriteOk> {
        let id = id.to_string();
        self.local_shard(&id)
            .call(move |s| delete_local(s, &id))
            .await
    }

    /// Apply the `_local/` documents of a `_bulk_docs` batch, each on the
    /// shard of its id. A delete of a missing document is a success at rev
    /// 0-0, unlike `DELETE /db/_local/id`, as in CouchDB.
    async fn put_local_docs(&self, docs: Vec<(usize, Value)>) -> Vec<(usize, WriteResult)> {
        let mut out = Vec::with_capacity(docs.len());
        for (slot, doc) in docs {
            let id = doc
                .get("_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let name = id.strip_prefix("_local/").unwrap_or(&id).to_string();
            let deleted = doc
                .get("_deleted")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let result = if deleted {
                match self.local_delete(&name).await {
                    Err(error) if error.status == 404 => {
                        Ok(WriteOk::new(id.clone(), "0-0".to_string()))
                    }
                    other => other,
                }
            } else {
                self.local_put(&name, doc).await
            };
            out.push((
                slot,
                match result {
                    Ok(ok) => WriteResult::Ok(ok),
                    Err(error) => WriteResult::error(Some(Value::String(id)), error),
                },
            ));
        }
        out
    }

    /// Store `_local/` documents exactly as given, `_rev` included, unless
    /// this node already holds a newer one. A rebalance uses it to hand a
    /// shard's local documents to a new replica; a normal write would number
    /// the copy one higher.
    pub async fn install_local_docs(&self, docs: Vec<Value>) -> Vec<WriteResult> {
        let mut out = Vec::with_capacity(docs.len());
        for doc in docs {
            let id = doc
                .get("_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let name = id.strip_prefix("_local/").unwrap_or(&id).to_string();
            let result = self
                .local_shard(&name)
                .call(move |s| install_local(s, &name, doc))
                .await;
            out.push(match result {
                Ok(ok) => WriteResult::Ok(ok),
                Err(error) => WriteResult::error(Some(Value::String(id)), error),
            });
        }
        out
    }

    /// `_local_docs`: the local documents in id order, as `_all_docs` rows,
    /// honouring `keys`, the key range, `descending`, `skip` and `limit`.
    /// CouchDB reports neither `total_rows` nor `offset` for them.
    pub async fn local_docs(&self, opts: &AllDocsOpts) -> DbResult<DocList> {
        self.local_docs_filtered(opts, None).await
    }

    /// Like `local_docs`, but only the shards whose `owned` entry is true.
    /// A cluster coordinator asks each node for one copy of every shard.
    pub async fn local_docs_owned(
        &self,
        opts: &AllDocsOpts,
        owned: &ShardMask,
    ) -> DbResult<DocList> {
        self.local_docs_filtered(opts, Some(owned)).await
    }

    async fn local_docs_filtered(
        &self,
        opts: &AllDocsOpts,
        owned: Option<&ShardMask>,
    ) -> DbResult<DocList> {
        let listed = futures::future::join_all(
            self.shards
                .iter()
                .enumerate()
                .filter(|(index, _)| shard_owned(owned, *index))
                .map(|(_, shard)| shard.read(list_local)),
        )
        .await;
        let mut docs: Vec<(String, Value)> = Vec::new();
        for shard_docs in listed {
            docs.extend(shard_docs?);
        }
        docs.sort_by(|a, b| a.0.cmp(&b.0));
        let rows = local_doc_rows(docs, opts);
        Ok(DocList {
            total_rows: None,
            offset: None,
            rows,
            update_seq: None,
        })
    }

    /// A node-local record that is not a document: the replication
    /// checkpoints of anti-entropy. They belong to this node and one peer,
    /// so they are neither shared with the cluster nor listed by
    /// `_local_docs`, and they live on the first shard whatever the
    /// placement.
    pub async fn checkpoint_get(&self, id: &str) -> DbResult<Option<Value>> {
        let id = id.to_string();
        self.shards[0]
            .read(move |s| match s.get_checkpoint(&id)? {
                Some(bytes) => serde_json::from_slice::<Value>(&bytes)
                    .map(Some)
                    .map_err(|error| {
                        ApiError::new(
                            500,
                            "internal_server_error",
                            format!("corrupt checkpoint: {error}"),
                        )
                    }),
                None => Ok(None),
            })
            .await
    }

    pub async fn checkpoint_put(&self, id: &str, value: &Value) -> DbResult<()> {
        let id = id.to_string();
        let value = serde_json::to_vec(value).map_err(ApiError::internal)?;
        self.shards[0]
            .call(move |s| Ok(s.put_checkpoint(&id, &value)?))
            .await
    }

    /// The checkpoints kept in shard `shard`'s own copy whose ids start with
    /// `prefix`. Unlike [`Db::checkpoint_get`] they describe that copy's
    /// contents, so clearing the shard drops them with it.
    pub async fn shard_checkpoints(
        &self,
        shard: usize,
        prefix: &str,
    ) -> DbResult<Vec<(String, Value)>> {
        let prefix = prefix.to_string();
        let found = self.shards[shard]
            .read(move |s| Ok(s.checkpoints(&prefix)?))
            .await?;
        found
            .into_iter()
            .map(|(id, value)| {
                serde_json::from_slice::<Value>(&value)
                    .map(|value| (id, value))
                    .map_err(|error| {
                        ApiError::new(
                            500,
                            "internal_server_error",
                            format!("corrupt checkpoint: {error}"),
                        )
                    })
            })
            .collect()
    }

    /// Store a checkpoint in shard `shard`'s own copy, dropping the `stale`
    /// ones it replaces in the same commit.
    pub async fn put_shard_checkpoint(
        &self,
        shard: usize,
        id: &str,
        value: &Value,
        stale: Vec<String>,
    ) -> DbResult<()> {
        let id = id.to_string();
        let value = serde_json::to_vec(value).map_err(ApiError::internal)?;
        self.shards[shard]
            .call(move |s| {
                for old in stale {
                    s.remove_checkpoint(&old)?;
                }
                s.put_checkpoint(&id, &value)?;
                Ok(())
            })
            .await
    }

    // ----- maintenance -----------------------------------------------------

    /// The sizes of shard `index`, whose documents' external sizes sum to
    /// `external`. The file's length is read from its metadata; one that
    /// cannot be read (a compaction's rename can hide the file for a
    /// moment) is the length last read.
    fn shard_sizes(&self, index: usize, shard: &ShardHandle, external: u64) -> DbSizes {
        let read = std::fs::metadata(shard_file(&self.path, index as u32))
            .ok()
            .map(|metadata| metadata.len());
        DbSizes::of_shard(shard.file_len(read), external, shard.index_bytes())
    }

    /// Measure each shard's tables other than the bodies, one shard after
    /// another under the node's one measurement permit: what a compaction
    /// asks for once it is done, its slot released.
    pub async fn measure_sizes(&self) {
        for shard in &self.shards {
            shard.measure_index().await;
        }
    }

    /// Each shard's sizes as of its last commit, without a round trip to
    /// its actor.
    pub fn published_shard_sizes(&self) -> Vec<DbSizes> {
        self.shards
            .iter()
            .enumerate()
            .map(|(index, shard)| self.shard_sizes(index, shard, shard.external_size()))
            .collect()
    }

    /// Each shard's purge sequence, by shard index: the one its handle
    /// publishes, read from the file instead while that is unknown (after a
    /// failed commit, until a reopen), so a client never sees the
    /// placeholder.
    pub async fn shard_purge_seqs(&self) -> DbResult<Vec<u64>> {
        futures::future::join_all(self.shards.iter().map(|shard| async move {
            match shard.purge_seq() {
                crate::store::UNKNOWN_PURGE_SEQ => shard.read(|s| Ok(s.purge_seq()?)).await,
                published => Ok(published),
            }
        }))
        .await
        .into_iter()
        .collect()
    }

    /// The replicas of `shard` other than node `me`: the peers whose purge
    /// checkpoints hold back trimming of the shard's purge infos.
    fn purge_peers(&self, shard: usize, me: usize) -> Vec<usize> {
        let shard = u32::try_from(shard).unwrap_or(u32::MAX);
        self.replication_map()
            .replicas_of(shard)
            .iter()
            .copied()
            .filter(|node| *node != me)
            .collect()
    }

    /// Per shard: its purge sequence and the purge infos after `since[shard]`
    /// (a missing entry reads as 0). `reader` is the replica asking, if any;
    /// it has confirmed everything up to `since`, so the shard may trim up to
    /// there once the other replicas have too.
    pub async fn purged_infos(
        &self,
        since: &[u64],
        reader: Option<usize>,
    ) -> DbResult<Vec<(u64, Vec<PurgeInfo>)>> {
        let reads = self.shards.iter().enumerate().map(|(index, shard)| {
            let after = since.get(index).copied().unwrap_or(0);
            shard.call(move |s| {
                // Read before writing, so a failed read fails only this job.
                let purged = s.purged_since(after)?;
                if let Some(node) = reader {
                    s.report_purge_checkpoint(node, after)?;
                }
                Ok(purged)
            })
        });
        futures::future::join_all(reads).await.into_iter().collect()
    }

    /// Record purge infos another copy of `shard` made and purge what they
    /// list. Infos already recorded here are skipped.
    pub async fn apply_purge_infos(
        &self,
        shard: usize,
        infos: Vec<PurgeInfo>,
        me: usize,
    ) -> DbResult<()> {
        let Some(handle) = self.shards.get(shard) else {
            return Ok(());
        };
        let peers = self.purge_peers(shard, me);
        let purge = handle.call(move |s| {
            // Everything is read before the first write: which infos are
            // new here, and the documents they name.
            let mut fresh = Vec::new();
            for info in infos {
                if !s.has_purge(&info.uuid)? {
                    fresh.push(info);
                }
            }
            let mut targets = purge_targets(s, fresh.iter().map(|info| info.id.as_str()))?;
            let mut applied = std::collections::HashSet::new();
            for info in &fresh {
                // An info listed twice is applied once.
                if !applied.insert(info.uuid.as_str()) {
                    continue;
                }
                purge_one(
                    s,
                    &info.id,
                    &info.revs,
                    PurgeSource::Replayed(&info.uuid),
                    &peers,
                    &mut targets,
                )?;
            }
            Ok(())
        });
        // A purge can remove a design document without taking a sequence,
        // and a failed commit can still land.
        self.invalidating(purge).await?
    }

    /// Apply a read repair to this copy: the given leaves as replicated
    /// changes, except those this copy's purge infos list. A purged revision
    /// stays purged, as anti-entropy keeps it (`fabric_rpc:read_repair_filter`).
    pub async fn put_read_repair(&self, docs: Vec<Value>) -> DbResult<Vec<WriteResult>> {
        let pair = |doc: &Value| {
            Some((
                doc.get("_id")?.as_str()?.to_string(),
                doc.get("_rev")?.as_str()?.to_string(),
            ))
        };
        let pairs: std::collections::BTreeSet<(String, String)> =
            docs.iter().filter_map(pair).collect();
        let purged = self.purged_among(&pairs).await?;
        let docs: Vec<Value> = docs
            .into_iter()
            .filter(|doc| pair(doc).is_none_or(|pair| !purged.contains(&pair)))
            .collect();
        if docs.is_empty() {
            return Ok(Vec::new());
        }
        self.put_docs(docs, false).await
    }

    /// The `(id, rev)` pairs among `pairs` that a purge info lists.
    pub async fn purged_among(
        &self,
        pairs: &std::collections::BTreeSet<(String, String)>,
    ) -> DbResult<std::collections::BTreeSet<(String, String)>> {
        let mut grouped: Vec<std::collections::BTreeMap<&str, Vec<String>>> =
            (0..self.q).map(|_| Default::default()).collect();
        for (id, rev) in pairs {
            grouped[self.shard_index(id)]
                .entry(id)
                .or_default()
                .push(rev.clone());
        }
        let mut found = std::collections::BTreeSet::new();
        for (index, docs) in grouped.into_iter().enumerate() {
            if docs.is_empty() {
                continue;
            }
            let docs: Vec<(String, Vec<String>)> = docs
                .into_iter()
                .map(|(id, revs)| (id.to_string(), revs))
                .collect();
            let hits = self.shards[index]
                .lease(move |s| {
                    let mut hits = Vec::new();
                    for (id, revs) in docs {
                        for rev in s.purged_revs(&id, &revs)? {
                            hits.push((id.clone(), rev));
                        }
                    }
                    Ok(hits)
                })
                .await?;
            found.extend(hits);
        }
        Ok(found)
    }

    /// Purge on this node, which is replica `me`, recording each document's purge under the uuid in
    /// `uuids` when it has one. The coordinator of a cluster purge picks them,
    /// so every copy of a shard holds the same purge info.
    pub async fn purge(
        &self,
        body: &Value,
        me: usize,
        uuids: &std::collections::BTreeMap<String, String>,
    ) -> DbResult<Value> {
        let Some(map) = body.as_object() else {
            return Err(ApiError::bad_request("Request body must be a JSON object"));
        };
        // One round trip per shard, the shards in parallel, instead of one
        // per document.
        let mut ids = Vec::new();
        let mut grouped: Vec<Vec<PurgeJob>> = (0..self.q).map(|_| Vec::new()).collect();
        for (id, revs) in map {
            let Some(list) = revs.as_array() else {
                continue;
            };
            let requested: Vec<String> = list
                .iter()
                .filter_map(|r| r.as_str().map(str::to_string))
                .collect();
            grouped[self.shard_index(id)].push((
                ids.len(),
                id.clone(),
                requested,
                uuids.get(id).cloned(),
            ));
            ids.push(id);
        }
        let mut calls = Vec::new();
        for (index, batch) in grouped.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let peers = self.purge_peers(index, me);
            calls.push(self.shards[index].call(move |s| {
                let mut targets = purge_targets(s, batch.iter().map(|(_, id, _, _)| id.as_str()))?;
                batch
                    .into_iter()
                    .map(|(slot, id, requested, assigned)| {
                        let source = assigned
                            .as_deref()
                            .map_or(PurgeSource::Fresh, PurgeSource::Assigned);
                        purge_one(s, &id, &requested, source, &peers, &mut targets)
                            .map(|removed| (slot, removed))
                    })
                    .collect::<DbResult<Vec<_>>>()
            }));
        }
        // A purge can remove a design document without taking a sequence,
        // and a failed commit can still land. The shards answer one after
        // another.
        let done = self
            .invalidating(async move {
                let mut done = Vec::new();
                for call in calls {
                    done.push(call.await);
                }
                done
            })
            .await?;
        let mut removed: Vec<Vec<String>> = ids.iter().map(|_| Vec::new()).collect();
        for batch in done {
            for (slot, revs) in batch? {
                removed[slot] = revs;
            }
        }
        let mut purged = Map::new();
        for (id, revs) in ids.into_iter().zip(removed) {
            purged.insert(id.clone(), json!(revs));
        }
        // CouchDB always answers `purge_seq: null` here (chttpd_db `_purge`).
        Ok(json!({ "purge_seq": Value::Null, "purged": purged }))
    }

    /// Compact every shard online, one after another (see
    /// `store::compact`). A compaction already running on this database is
    /// left to finish, as CouchDB leaves one: this returns at once.
    #[cfg(test)]
    pub async fn compact(&self) -> DbResult<()> {
        match self.claim_compaction() {
            Some(claim) => self.run_compaction(claim).await,
            None => Ok(()),
        }
    }

    /// Whether a compaction of this database is running.
    pub fn compacting(&self) -> bool {
        self.compact_running.load(Ordering::SeqCst)
    }

    /// Mark a compaction as running, unless one already is. Until the claim
    /// is dropped, however the compaction ends (a panic included), `GET /db`
    /// reports it and the database stays open.
    pub fn claim_compaction(&self) -> Option<CompactionClaim<'_>> {
        self.compact_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
            .then_some(CompactionClaim(&self.compact_running))
    }

    /// Run the compaction `claim` claimed.
    pub async fn run_compaction(&self, claim: CompactionClaim<'_>) -> DbResult<()> {
        for shard in &self.shards {
            crate::store::compact::compact_shard(shard).await?;
        }
        drop(claim);
        Ok(())
    }

    /// The `_active_tasks` entries of this database's running compaction on
    /// node `node`: one per shard being compacted, as CouchDB reports a
    /// `database_compaction` task per shard file.
    pub fn active_tasks(&self, node: &str) -> Vec<Value> {
        let q = u64::from(self.q);
        let mut tasks = Vec::new();
        for (index, shard) in self.shards.iter().enumerate() {
            let Some(task) = shard.compaction() else {
                continue;
            };
            // The shard's hash range, named as CouchDB names shard files.
            let index = index as u64;
            let (low, high) = ((index << 32) / q, ((index + 1) << 32) / q - 1);
            let progress = (task.changes_done * 100)
                .checked_div(task.total_changes)
                .unwrap_or(0)
                .min(100);
            tasks.push(json!({
                "type": "database_compaction",
                "database": format!("shards/{low:08x}-{high:08x}/{}", self.name),
                "node": node,
                "phase": task.phase,
                "progress": progress,
                "changes_done": task.changes_done,
                "total_changes": task.total_changes,
                "started_on": task.started_on,
                "updated_on": task.updated_on,
            }));
        }
        tasks
    }

    /// Every acknowledged write is already synced (each group commit is), so
    /// there is nothing left to flush.
    pub async fn ensure_full_commit(&self) -> DbResult<Value> {
        Ok(json!({ "ok": true, "instance_start_time": self.instance_start_time }))
    }

    pub async fn get_revs_limit(&self) -> DbResult<u64> {
        self.shards[0].read(|s| Ok(s.revs_limit())).await
    }

    pub async fn get_purged_infos_limit(&self) -> DbResult<u64> {
        self.shards[0].read(|s| Ok(s.purged_infos_limit())).await
    }

    pub async fn set_purged_infos_limit(&self, limit: u64) -> DbResult<()> {
        for shard in &self.shards {
            shard
                .call(move |s| s.set_purged_infos_limit(limit).map_err(ApiError::from))
                .await?;
        }
        Ok(())
    }

    pub async fn set_revs_limit(&self, limit: u64) -> DbResult<()> {
        for shard in &self.shards {
            shard
                .call(move |s| s.set_revs_limit(limit).map_err(ApiError::from))
                .await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// document helpers

/// The rejection for a write that carries attachments. CouchDB has no such
/// error; its attachment failures are `bad_request`, so clients that branch on
/// the error name see a familiar one.
pub fn attachments_unsupported() -> ApiError {
    ApiError::bad_request("douchdb does not support attachments")
}

/// The requested revisions of one document the shard does not have, and the
/// leaves that could be their ancestors.
fn revs_diff_one(
    s: &impl DocRead,
    id: &str,
    wanted: &[String],
) -> DbResult<(Vec<String>, Vec<String>)> {
    let tree = s.get_meta(id)?.map(|m| m.tree).unwrap_or_default();
    let mut missing = Vec::new();
    let mut ancestors: Vec<String> = Vec::new();
    for rev in wanted {
        if !tree.contains(rev) {
            missing.push(rev.clone());
            if let Some(parsed) = Rev::parse(rev) {
                for leaf in tree.leaves() {
                    if Rev::parse(&leaf)
                        .map(|l| l.pos < parsed.pos)
                        .unwrap_or(false)
                    {
                        ancestors.push(leaf);
                    }
                }
            }
        }
    }
    ancestors.sort();
    ancestors.dedup();
    ancestors.truncate(10);
    Ok((missing, ancestors))
}

/// The body a coordinator sends a peer's `_purge`: each document's revisions
/// as `{"uuid": .., "revs": [..]}`, so the peer records the coordinator's uuid.
pub fn with_purge_uuids(body: &Value, uuids: &std::collections::BTreeMap<String, String>) -> Value {
    let mut out = Map::new();
    for (id, revs) in body.as_object().into_iter().flatten() {
        out.insert(
            id.clone(),
            match uuids.get(id) {
                Some(uuid) => json!({"uuid": uuid, "revs": revs}),
                None => revs.clone(),
            },
        );
    }
    Value::Object(out)
}

/// The inverse of `with_purge_uuids`, for a request from a peer: the plain
/// `{id: [revs]}` body and the uuids it carried.
pub fn split_purge_uuids(body: &Value) -> (Value, std::collections::BTreeMap<String, String>) {
    let mut plain = Map::new();
    let mut uuids = std::collections::BTreeMap::new();
    for (id, entry) in body.as_object().into_iter().flatten() {
        match entry.as_object() {
            Some(carried) => {
                if let Some(uuid) = carried.get("uuid").and_then(Value::as_str) {
                    uuids.insert(id.clone(), uuid.to_string());
                }
                let revs = carried.get("revs").cloned().unwrap_or(Value::Null);
                plain.insert(id.clone(), revs);
            }
            None => {
                plain.insert(id.clone(), entry.clone());
            }
        }
    }
    (Value::Object(plain), uuids)
}

/// One document's purge on a shard: its slot in the request, id, requested
/// revisions and the uuid the coordinator assigned.
type PurgeJob = (usize, String, Vec<String>, Option<String>);

/// Where a purge came from, which decides the uuid of its purge info.
#[derive(Clone, Copy)]
enum PurgeSource<'a> {
    /// A client purge with no uuid given: a new one is made.
    Fresh,
    /// A client purge the coordinator gave this uuid.
    Assigned(&'a str),
    /// A purge info another copy recorded.
    Replayed(&'a str),
}

impl<'a> PurgeSource<'a> {
    fn uuid(self) -> Option<&'a str> {
        match self {
            PurgeSource::Fresh => None,
            PurgeSource::Assigned(uuid) | PurgeSource::Replayed(uuid) => Some(uuid),
        }
    }
}

/// A document as a purge job read it before its first write.
enum PurgeTarget {
    /// No metadata record.
    Missing,
    /// A record that reads but does not decode: the document goes whole.
    Undecodable,
    /// The document's metadata.
    Doc(DocMeta),
}

/// The documents a purge job will change, keyed by id: as stored when the
/// job starts, then as the job's own purges leave them.
type PurgeTargets = HashMap<String, PurgeTarget>;

/// Read and decode the metadata of every document in `ids`, before the
/// purge job writes anything (a job validates before it writes): a read that
/// fails, such as a value failing its checksum, fails the job while it has
/// written nothing, so the other jobs of its group still commit.
fn purge_targets<'a>(
    s: &ShardTxn,
    ids: impl IntoIterator<Item = &'a str>,
) -> DbResult<PurgeTargets> {
    let mut targets = HashMap::new();
    for id in ids {
        if targets.contains_key(id) {
            continue;
        }
        let target = match s.get_meta(id) {
            Ok(Some(meta)) => PurgeTarget::Doc(meta),
            Ok(None) => PurgeTarget::Missing,
            Err(error)
                if error
                    .downcast_ref::<crate::error::CorruptRecord>()
                    .is_some() =>
            {
                PurgeTarget::Undecodable
            }
            Err(error) => return Err(error.into()),
        };
        targets.insert(id.to_string(), target);
    }
    Ok(targets)
}

/// Purge the requested leaf revisions of one document; the revisions removed.
/// The document comes from `targets`, which [`purge_targets`] filled before
/// the job's first write, and goes back into it as the purge leaves it, for
/// a later purge of the same document in the job.
///
/// The purge is recorded as a purge info in the same transaction. A fresh
/// purge records the revisions it removed, under a new uuid, and nothing when
/// it removed none. A `replayed` uuid is a purge another copy of the shard
/// already recorded: it is recorded here as received, even when this copy
/// holds none of the revisions, so they cannot be copied back in. An
/// `Assigned` uuid is a fresh purge whose uuid the coordinator chose, so every
/// copy records the same one (fabric_doc_purge.erl).
fn purge_one(
    s: &mut ShardTxn,
    id: &str,
    requested: &[String],
    source: PurgeSource<'_>,
    peers: &[usize],
    targets: &mut PurgeTargets,
) -> DbResult<Vec<String>> {
    let target = targets.remove(id).ok_or_else(|| {
        ApiError::internal(format!("purge of {id} was not read before the job wrote"))
    })?;
    let (removed, after) = purge_target(s, id, requested, source, peers, target)?;
    targets.insert(id.to_string(), after);
    Ok(removed)
}

/// [`purge_one`] of the document as `target` holds it: the revisions removed
/// and the document as the purge leaves it.
fn purge_target(
    s: &mut ShardTxn,
    id: &str,
    requested: &[String],
    source: PurgeSource<'_>,
    peers: &[usize],
    target: PurgeTarget,
) -> DbResult<(Vec<String>, PurgeTarget)> {
    let mut meta = match target {
        PurgeTarget::Doc(meta) => meta,
        PurgeTarget::Undecodable => {
            // The revisions cannot be checked against unreadable metadata, so
            // the document goes whole; this is the repair path for it.
            let info = PurgeInfo {
                uuid: source.uuid().map_or_else(uuid, str::to_string),
                id: id.to_string(),
                revs: requested.to_vec(),
            };
            s.purge_corrupt(id, &info, peers)?;
            return Ok((requested.to_vec(), PurgeTarget::Missing));
        }
        PurgeTarget::Missing => {
            if let PurgeSource::Replayed(uuid) = source {
                let info = PurgeInfo {
                    uuid: uuid.to_string(),
                    id: id.to_string(),
                    revs: requested.to_vec(),
                };
                s.record_purge(&info, peers)?;
            }
            return Ok((Vec::new(), PurgeTarget::Missing));
        }
    };
    let old_state = meta_state(&meta);
    let mut removed = Vec::new();
    let mut dead_bodies = Vec::new();
    // Like CouchDB, purge leaves only: each takes its unshared
    // ancestors with it, and a listed non-leaf revision is
    // ignored and not reported.
    let leaves: std::collections::BTreeSet<String> = meta.tree.leaves().into_iter().collect();
    for rev in requested.iter().filter(|rev| leaves.contains(*rev)) {
        let gone = meta.tree.purge_leaf(rev);
        if gone.is_empty() {
            continue;
        }
        dead_bodies.extend(gone);
        removed.push(rev.clone());
    }
    // The purge info is recorded before this document's own writes: it
    // reads the shard's purge state. Earlier documents in the job may
    // already have written (#74).
    if matches!(source, PurgeSource::Replayed(_)) || !removed.is_empty() {
        let info = match source {
            PurgeSource::Replayed(uuid) => PurgeInfo {
                uuid: uuid.to_string(),
                id: id.to_string(),
                revs: requested.to_vec(),
            },
            PurgeSource::Assigned(_) | PurgeSource::Fresh => PurgeInfo {
                uuid: source.uuid().map_or_else(uuid, str::to_string),
                id: id.to_string(),
                revs: removed.clone(),
            },
        };
        s.record_purge(&info, peers)?;
    }
    if removed.is_empty() {
        return Ok((removed, PurgeTarget::Doc(meta)));
    }
    for dead in &dead_bodies {
        s.remove_body(id, dead)?;
    }
    if meta.tree.is_empty() {
        if meta.seq != 0 {
            s.remove_seq(meta.seq)?;
        }
        s.remove_meta(id, old_state)?;
        return Ok((removed, PurgeTarget::Missing));
    }
    meta.keep_leaf_sizes();
    let old_seq = meta.seq;
    // Purging the winner changes what a normal read sees,
    // so it is a change as far as `_changes` is concerned.
    let seq = s.next_seq();
    meta.seq = seq;
    s.remove_seq(old_seq)?;
    s.put_seq(seq, id)?;
    let bytes = meta_bytes(&meta).map_err(ApiError::internal)?;
    s.put_meta(id, &bytes, old_state, meta_state(&meta), seq)?;
    Ok((removed, PurgeTarget::Doc(meta)))
}

/// CouchDB rejects unknown `_`-prefixed members outright (`doc_validation`),
/// whether they arrive through PUT, `_bulk_docs` or `new_edits=false`.
pub fn validate_doc(doc: &Value) -> DbResult<()> {
    let Some(obj) = doc.as_object() else {
        return Err(ApiError::bad_request("Document must be a JSON object"));
    };
    if obj.contains_key("_attachments") {
        return Err(attachments_unsupported());
    }
    for key in obj.keys() {
        if key.starts_with('_')
            && !META_FIELDS.contains(&key.as_str())
            && !key.starts_with("_replication_")
        {
            return Err(ApiError::new(
                400,
                "doc_validation",
                format!("Bad special document member: {key}"),
            ));
        }
    }
    if let Some(revisions) = obj.get("_revisions") {
        validate_revisions(revisions)?;
    }
    // CouchDB (couch_doc.erl) accepts only a boolean `_deleted`.
    if obj.get("_deleted").is_some_and(|value| !value.is_boolean()) {
        return Err(ApiError::new(
            400,
            "doc_validation",
            "Bad special document member: _deleted",
        ));
    }
    Ok(())
}

/// What CouchDB checks when a design document is saved
/// (`couch_mrview:validate/2`): wrong member types are `invalid_design_doc`,
/// and every map and reduce function of a JavaScript design document must
/// compile (`compilation_error`), not fail later at query time. Nothing else
/// is compiled at save time. It runs after the revision check, on every write
/// path including replication. `obj` is the document without its `_`
/// metadata members.
///
/// This is only the structural half, which is cheap. The compile runs in
/// [`compile_design_docs`] before the write reaches its shard, and `Stage`
/// raises its verdict right after this check passes.
fn validate_design_doc(id: &str, deleted: bool, obj: &Map<String, Value>) -> DbResult<()> {
    let invalid = |reason: String| ApiError::new(400, "invalid_design_doc", reason);
    if !id.starts_with("_design/") || deleted {
        return Ok(());
    }
    let wrong = |field: &str, kind: &str| invalid(format!("`{field}` field must have {kind} type"));
    for field in ["language", "validate_doc_update"] {
        if obj.get(field).is_some_and(|value| !value.is_string()) {
            return Err(wrong(field, "string"));
        }
    }
    if obj.get("options").is_some_and(|value| !value.is_object()) {
        return Err(wrong("options", "object"));
    }
    if obj
        .get("rewrites")
        .is_some_and(|value| !value.is_array() && !value.is_string())
    {
        return Err(wrong("rewrites", "array or string"));
    }
    for field in ["filters", "updates", "lists", "shows"] {
        match obj.get(field) {
            None => {}
            Some(Value::Object(members)) => {
                for (name, value) in members {
                    if !value.is_string() && !value.is_object() {
                        return Err(wrong(&format!("{field}.{name}"), "string or object"));
                    }
                }
            }
            Some(_) => return Err(wrong(field, "object")),
        }
    }
    match obj.get("views") {
        None => {}
        Some(Value::Object(views)) => {
            for (name, view) in views {
                let Value::Object(view) = view else {
                    return Err(wrong(&format!("views.{name}"), "object"));
                };
                if !view.contains_key("map") {
                    return Err(invalid(format!("View `{name}` must contain map function")));
                }
                for part in ["map", "reduce"] {
                    if view.get(part).is_some_and(|value| !value.is_string()) {
                        return Err(wrong(&format!("views.{name}.{part}"), "string"));
                    }
                }
                if let Some(Value::String(reduce)) = view.get("reduce") {
                    crate::viewfn::check_reduce_name(reduce).map_err(invalid)?;
                }
            }
        }
        Some(_) => return Err(wrong("views", "object")),
    }
    Ok(())
}

/// Compile the design documents among `grouped` (per shard: index, document),
/// keeping the compile off the shard writer. Returns the compile error of each
/// document that has one. A document that fails the structural check is
/// skipped: that error wins, so its compile would be wasted. With
/// `new_edits=false`, a document whose revision the shard already stores is
/// skipped too, as `Stage::stage_replicated` never validates it.
async fn compile_design_docs(
    shards: &[crate::store::ShardHandle],
    grouped: &[Vec<(usize, Prepared)>],
    new_edits: bool,
) -> DbResult<HashMap<usize, ApiError>> {
    let mut pending: Vec<(usize, usize, Map<String, Value>)> = Vec::new();
    // For `new_edits=false`: each candidate's id and revision fields.
    let mut revs: HashMap<usize, (String, Option<String>, Option<Value>)> = HashMap::new();
    for (shard_idx, batch) in grouped.iter().enumerate() {
        for (i, doc) in batch {
            let Some(members) = &doc.members else {
                continue;
            };
            if compile_needed(members) && validate_design_doc(&doc.id, doc.deleted, members).is_ok()
            {
                pending.push((*i, shard_idx, members.clone()));
                if !new_edits {
                    revs.insert(*i, (doc.id.clone(), doc.rev.clone(), doc.revisions.clone()));
                }
            }
        }
    }
    if !new_edits {
        for (shard_idx, shard) in shards.iter().enumerate() {
            let candidates: Vec<_> = pending
                .iter()
                .filter(|(_, owner, _)| *owner == shard_idx)
                .filter_map(|(i, _, _)| revs.remove(i).map(|fields| (*i, fields)))
                .collect();
            if candidates.is_empty() {
                continue;
            }
            let stored = shard
                .read(move |shard| {
                    Ok(candidates
                        .into_iter()
                        .filter(|(_, (id, rev, revisions))| {
                            replicated_is_stored(shard, id, rev.clone(), revisions.as_ref())
                        })
                        .map(|(i, _)| i)
                        .collect::<Vec<usize>>())
                })
                .await?;
            pending.retain(|(i, _, _)| !stored.contains(i));
        }
    }
    if pending.is_empty() {
        return Ok(HashMap::new());
    }
    tokio::task::spawn_blocking(move || {
        pending
            .into_iter()
            .filter_map(|(i, _, doc)| compile_views(&doc).err().map(|error| (i, error)))
            .collect()
    })
    .await
    .map_err(ApiError::internal)
}

/// Whether the document has anything for [`compile_views`] to compile.
fn compile_needed(obj: &Map<String, Value>) -> bool {
    matches!(obj.get("views"), Some(Value::Object(views)) if !views.is_empty())
}

/// Whether `Stage::stage_replicated` would find this replicated revision
/// already stored, and so never validate it. Anything malformed is "not
/// stored": the stage reports its own error for it.
fn replicated_is_stored(
    shard: &mut ShardTxn,
    id: &str,
    rev: Option<String>,
    revisions: Option<&Value>,
) -> bool {
    let Ok((rev, _)) = revisions_path(rev, revisions) else {
        return false;
    };
    matches!(shard.get_meta(id), Ok(Some(meta)) if meta.tree.contains(&rev))
}

/// Every compile of a view function with the name of the thread that ran it,
/// for the tests that check where the compile happens.
#[cfg(test)]
static COMPILE_LOG: std::sync::Mutex<Vec<(String, Option<String>)>> =
    std::sync::Mutex::new(Vec::new());

/// CouchDB's `try_compile`. A design document in another language is saved
/// unchecked: there is no query server to compile it.
fn compile_views(obj: &Map<String, Value>) -> DbResult<()> {
    use crate::viewfn::{check_compiles, ViewFunction};
    if obj
        .get("language")
        .and_then(Value::as_str)
        .is_some_and(|language| language != "javascript")
    {
        return Ok(());
    }
    let Some(Value::Object(views)) = obj.get("views") else {
        return Ok(());
    };
    let limits = crate::js::ViewLimits::default();
    for (name, view) in views {
        let functions = [(ViewFunction::Map, "map"), (ViewFunction::Reduce, "reduce")];
        for (function, member) in functions {
            let Some(Value::String(source)) = view.get(member) else {
                continue;
            };
            if member == "reduce" && source.starts_with('_') {
                continue;
            }
            #[cfg(test)]
            COMPILE_LOG
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push((
                    source.clone(),
                    std::thread::current().name().map(str::to_string),
                ));
            if let Err(message) = check_compiles(function, source, limits) {
                return Err(ApiError::new(
                    400,
                    "compilation_error",
                    format!(
                        "Compilation of the {member} function in the '{name}' view failed: {message} ({source})"
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn doc_id(doc: &Value) -> DbResult<String> {
    match doc.get("_id") {
        Some(Value::String(id)) if !id.is_empty() => {
            if id.starts_with('_') && !id.starts_with("_design/") {
                return Err(ApiError::new(
                    400,
                    "illegal_docid",
                    "Only reserved document ids may start with underscore.",
                ));
            }
            Ok(id.clone())
        }
        Some(_) => Err(ApiError::new(
            400,
            "illegal_docid",
            "Document id must be a string",
        )),
        None => Ok(uuid()),
    }
}

/// The decoded body members of a stored or prepared body.
pub(crate) fn body_members(body: &[u8]) -> DbResult<Map<String, Value>> {
    serde_json::from_slice(body).map_err(ApiError::internal)
}

/// A stored revision tree whose parent links loop, reported as corruption.
fn corrupt_tree(id: &str, error: crate::revtree::CyclicTree) -> ApiError {
    ApiError::corrupt(format!("corrupt revision tree for {id}: {error}"))
}

fn put_many(
    shard: &mut ShardTxn,
    docs: Vec<(usize, Prepared)>,
    new_edits: bool,
    mut compile_errors: HashMap<usize, ApiError>,
) -> Vec<(usize, DbResult<WriteOk>)> {
    let mut ops: Ops = Vec::with_capacity(docs.len() * 2);
    let mut results = Vec::with_capacity(docs.len());
    // One slot per distinct id, in id order; `slot_of[k]` is the `k`th
    // document's.
    let mut by_id: Vec<usize> = (0..docs.len()).collect();
    by_id.sort_unstable_by(|&a, &b| docs[a].1.id.cmp(&docs[b].1.id));
    let mut ids: Vec<&str> = Vec::with_capacity(docs.len());
    let mut slot_of = vec![0; docs.len()];
    for k in by_id {
        let id = docs[k].1.id.as_str();
        if ids.last() != Some(&id) {
            ids.push(id);
        }
        slot_of[k] = ids.len() - 1;
    }
    // Every document's stored metadata, read in one pass before staging. If
    // a record cannot be read, each document reads its own instead, so the
    // failure is only theirs.
    let mut slots: Vec<Slot> = match shard.get_metas(&ids) {
        Ok(metas) => metas
            .into_iter()
            .map(|meta| Slot {
                meta: meta.unwrap_or_default(),
                read: true,
                origin: None,
            })
            .collect(),
        Err(_) => ids.iter().map(|_| Slot::default()).collect(),
    };
    // The staged slots with their ids, in the order they were first staged.
    let mut staged: Vec<(usize, String)> = Vec::with_capacity(slots.len());

    for ((i, doc), k) in docs.into_iter().zip(slot_of) {
        let mark = ops.len();
        let slot = &mut slots[k];
        let was_staged = slot.origin.is_some();
        let mut ctx = Stage {
            shard,
            ops: &mut ops,
        };
        match ctx.stage(doc, new_edits, compile_errors.remove(&i), slot) {
            Ok(value) => {
                if !was_staged && slot.origin.is_some() {
                    staged.push((k, value.id.clone()));
                }
                results.push((i, Ok(value)));
            }
            Err(e) => {
                // Staging changes a slot only after its last fallible step.
                debug_assert_eq!(slot.origin.is_some(), was_staged, "failed document staged");
                ops.truncate(mark);
                results.push((i, Err(e)));
            }
        }
    }

    // CouchDB merges all paths in a bulk request before applying revs_limit.
    // Prune once here so a later branch cannot resurrect a previously pruned
    // ancestor as an orphan root.
    let limit = shard.revs_limit();
    // Each document's record is written once, here, in its final form.
    let mut metas: Vec<MetaWrite> = Vec::with_capacity(staged.len());
    for (k, id) in staged {
        let Slot {
            mut meta, origin, ..
        } = std::mem::take(&mut slots[k]);
        // The batch's bodies and sequence numbers are already staged, so a
        // tree that cannot be pruned fails the whole batch rather than being
        // written back as if it were valid.
        if let Err(e) = meta.tree.prune(limit) {
            let message = corrupt_tree(&id, e).reason;
            return results
                .into_iter()
                .map(|(i, _)| (i, Err(ApiError::corrupt(message.clone()))))
                .collect();
        }
        meta.keep_leaf_sizes();
        if let Ok(bytes) = meta_bytes(&meta) {
            metas.push(MetaWrite {
                id,
                bytes,
                old: origin.flatten(),
                new: meta_state(&meta),
                seq: meta.seq,
            });
        }
    }

    if let Err(e) = commit_ops(shard, ops, metas) {
        let message = e.to_string();
        return results
            .into_iter()
            .map(|(i, _)| {
                (
                    i,
                    Err(ApiError::new(500, "internal_server_error", message.clone())),
                )
            })
            .collect();
    }
    results
}

/// One document id of a batch being staged.
#[derive(Default)]
struct Slot {
    /// The document's metadata, empty when it has none: as stored until a
    /// document of the batch is staged, then as staged.
    meta: DocMeta,
    /// Whether `meta` was read; false until then when the one-pass read
    /// failed.
    read: bool,
    /// Set when the first document is staged: what the record held before
    /// the batch.
    origin: Option<Option<MetaState>>,
}

impl Slot {
    /// The metadata to change for a document being staged; everything that
    /// can fail must come first.
    fn staging(&mut self) -> &mut DocMeta {
        if self.origin.is_none() {
            self.origin = Some(meta_state(&self.meta));
        }
        &mut self.meta
    }
}

struct Stage<'a, 's> {
    shard: &'a mut ShardTxn<'s>,
    ops: &'a mut Ops,
}

impl Stage<'_, '_> {
    fn next_seq(&mut self) -> u64 {
        self.shard.next_seq()
    }

    /// `compile_error` is the design document's compile verdict, computed
    /// before the shard job; it is raised where the structural check is.
    fn stage(
        &mut self,
        doc: Prepared,
        new_edits: bool,
        compile_error: Option<ApiError>,
        slot: &mut Slot,
    ) -> DbResult<WriteOk> {
        if !slot.read {
            slot.meta = self.shard.get_meta(&doc.id)?.unwrap_or_default();
            slot.read = true;
        }
        if !new_edits {
            return self.stage_replicated(doc, slot, compile_error);
        }
        let Prepared {
            id,
            rev: client_rev,
            deleted,
            body,
            members,
            new_rev,
            ..
        } = doc;
        let tree = &slot.meta.tree;

        let parent = match &client_rev {
            Some(rev) => {
                if Rev::parse(rev).is_none() {
                    return Err(ApiError::bad_request("Invalid rev format"));
                }
                if !tree.contains(rev) || !tree.is_leaf(rev) {
                    return Err(ApiError::conflict());
                }
                if tree.is_deleted(rev) {
                    return Err(ApiError::conflict());
                }
                Some(rev.clone())
            }
            None => match tree.winner() {
                Some(winner) if !tree.is_deleted(&winner) => return Err(ApiError::conflict()),
                other => other,
            },
        };

        // CouchDB validates a design document only once its revision is known
        // to be the right one: a conflict wins over a malformed member.
        if let Some(members) = &members {
            validate_design_doc(&id, deleted, members)?;
            if let Some(error) = compile_error {
                return Err(error);
            }
        }

        let parent_path = match parent.as_ref() {
            Some(p) => tree.path(p).map_err(|e| corrupt_tree(&id, e))?,
            None => Vec::new(),
        };
        let parent = parent.as_deref().and_then(Rev::parse);
        let pos = parent.as_ref().map_or(0, |p| p.pos).checked_add(1);

        // The prepared revision is for the client's parent. Without `_rev`
        // over a deleted winner the parent is the winner, and the revision is
        // computed here, from the body.
        let rev = match (new_rev, &client_rev, &parent) {
            (Some(rev), Some(_), _) | (Some(rev), None, None) => rev,
            _ => {
                let members = match members {
                    Some(members) => members,
                    None => body_members(&body)?,
                };
                let hash = new_revid_doc(deleted, parent.as_ref(), &members);
                // No revision follows one at the last position.
                let pos = pos.ok_or_else(|| ApiError::bad_request("Invalid rev format"))?;
                let leaf = Rev {
                    pos,
                    id: hash.to_vec(),
                    hex: true,
                };
                leaf.to_string()
            }
        };

        let meta = slot.staging();
        if parent_path.is_empty() {
            meta.tree.merge_root(rev.clone(), deleted);
        } else {
            let mut path: Vec<Rev> = parent_path.iter().filter_map(|r| Rev::parse(r)).collect();
            path.extend(Rev::parse(&rev));
            meta.tree.merge_path(&path, deleted);
        }
        meta.sizes.insert(rev.clone(), body.len() as u64);
        let old_seq = meta.seq;
        let seq = self.next_seq();
        meta.seq = seq;

        self.ops.push(Op::Body {
            id: id.clone(),
            rev: rev.clone(),
            body,
        });
        self.ops.push(Op::Seq {
            seq,
            id: id.clone(),
        });
        if old_seq != 0 {
            self.ops.push(Op::Unseq(old_seq));
        }

        Ok(WriteOk::new(id, rev))
    }

    fn stage_replicated(
        &mut self,
        doc: Prepared,
        slot: &mut Slot,
        compile_error: Option<ApiError>,
    ) -> DbResult<WriteOk> {
        let Prepared {
            id,
            rev,
            deleted,
            revisions,
            body,
            members,
            ..
        } = doc;
        let (rev, path) = revisions_path(rev, revisions.as_ref())?;
        let known = slot.meta.tree.contains(&rev);
        // A revision that is only an ancestor on a stored path has no body
        // yet; the replicated write supplies it.
        if known && self.shard.get_body(&id, &rev)?.is_some() {
            return Ok(WriteOk::new(id, rev));
        }
        // A replicated design document is validated too (couch_db:
        // prep_and_validate_replicated_updates), unless it is already stored.
        if !known {
            if let Some(members) = &members {
                validate_design_doc(&id, deleted, members)?;
                if let Some(error) = compile_error {
                    return Err(error);
                }
            }
        }

        let size = body.len() as u64;
        self.ops.push(Op::Body {
            id: id.clone(),
            rev: rev.clone(),
            body,
        });
        if !known {
            let meta = slot.staging();
            meta.tree.merge_path(&path, deleted);
            meta.sizes.insert(rev.clone(), size);
            let old_seq = meta.seq;
            let seq = self.next_seq();
            meta.seq = seq;
            self.ops.push(Op::Seq {
                seq,
                id: id.clone(),
            });
            if old_seq != 0 {
                self.ops.push(Op::Unseq(old_seq));
            }
        }

        Ok(WriteOk::new(id, rev))
    }
}

/// Check a document's `_revisions` the way `couch_doc:transfer_fields/3` does,
/// which runs for every document whatever `new_edits` says: `start` must be an
/// integer, `ids` an array, then each id goes through `parse_revid/2`. Returns
/// the validated start and ids.
fn validate_revisions(revisions: &Value) -> DbResult<(i64, Vec<&str>)> {
    let doc_validation = |reason: &str| ApiError::new(400, "doc_validation", reason);
    // couch_doc.erl:302 (`_revisions` clause); a non-object value reaches the
    // unknown special member clause at couch_doc.erl:378 instead.
    let Some(props) = revisions.as_object() else {
        return Err(doc_validation("Bad special document member: _revisions"));
    };
    // couch_doc.erl:307
    let Some(start) = props.get("start").and_then(Value::as_i64) else {
        return Err(doc_validation("_revisions.start isn't an integer."));
    };
    // couch_doc.erl:309
    let Some(ids) = props.get("ids").and_then(Value::as_array) else {
        return Err(doc_validation("_revisions.ids isn't a array."));
    };
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        match id.as_str() {
            // parse_revid/2 (couch_doc.erl:191): past position 0 a 32-byte id
            // must be hex; `binary:decode_hex` failing is couch_doc.erl:321.
            Some(id) if start != 0 && id.len() == 32 => {
                if !id.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err(doc_validation("RevId isn't a valid hexadecimal"));
                }
                out.push(id);
            }
            // couch_doc.erl:195
            Some(id) if start >= 0 => out.push(id),
            // No parse_revid/2 clause matches: couch_doc.erl:319.
            _ => return Err(doc_validation("RevId isn't a string")),
        }
    }
    Ok((start, out))
}

/// Reconstruct the root-to-leaf path of a replicated document and the revision
/// it writes. Like CouchDB (`couch_doc:transfer_fields/3`), `_revisions`
/// defines the revision when present and non-empty, even without or against
/// `_rev`; otherwise `_rev` alone does.
fn revisions_path(rev: Option<String>, revisions: Option<&Value>) -> DbResult<(String, Vec<Rev>)> {
    let mut path = Vec::new();
    let mut named_ids = false;
    if let Some(revs) = revisions {
        let (start, ids) = validate_revisions(revs)?;
        named_ids = !ids.is_empty();
        for (i, id) in ids.into_iter().enumerate() {
            let Some(pos) = u64::try_from(start)
                .ok()
                .and_then(|start| start.checked_sub(i as u64))
            else {
                break;
            };
            // Deliberate departure: CouchDB stores revisions at position 0
            // (`_revisions` with `start: 0`), but nothing generates them, so
            // the path stops before position 0 and `Rev::parse` refuses it.
            if pos == 0 {
                break;
            }
            path.push(
                Rev::parse(&format!("{pos}-{id}"))
                    .ok_or_else(|| ApiError::bad_request("Invalid revision id"))?,
            );
        }
        path.reverse();
        if let Some(leaf) = path.last() {
            return Ok((format!("{}-{}", leaf.pos, leaf.id_string()), path));
        }
    }
    let rev = match rev {
        Some(rev) => rev,
        // Ids that all sit at position 0 or below name no revision.
        None if named_ids => return Err(ApiError::bad_request("Invalid rev format")),
        None => {
            return Err(ApiError::bad_request(
                "When `new_edits: false`, the document needs `_rev` or `_revisions` specified",
            ))
        }
    };
    let leaf = Rev::parse(&rev).ok_or_else(|| ApiError::bad_request("Invalid rev format"))?;
    Ok((rev, vec![leaf]))
}

#[cfg(test)]
thread_local! {
    static META_SERIALISES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A document's metadata record as stored.
fn meta_bytes(meta: &DocMeta) -> serde_json::Result<Vec<u8>> {
    #[cfg(test)]
    META_SERIALISES.with(|count| count.set(count.get() + 1));
    serde_json::to_vec(meta)
}

/// Write a staged batch's operations and its documents' metadata records,
/// opening each table once. Everything was read and validated while staging,
/// so this only writes.
fn commit_ops(shard: &mut ShardTxn, ops: Ops, metas: Vec<MetaWrite>) -> anyhow::Result<()> {
    let (mut bodies, mut seqs, mut unseqs) = (Vec::new(), Vec::new(), Vec::new());
    for op in &ops {
        match op {
            Op::Body { id, rev, body } => bodies.push((id.as_str(), rev.as_str(), body.as_slice())),
            Op::Seq { seq, id } => seqs.push((*seq, id.as_str())),
            Op::Unseq(seq) => unseqs.push(*seq),
        }
    }
    shard.put_bodies(&bodies)?;
    // A superseded sequence is one from before the batch or one an earlier
    // document of the batch took, never a later one's: adding them all, then
    // removing, leaves by_seq as writing them in order did.
    shard.put_seqs(&seqs)?;
    shard.remove_seqs(&unseqs)?;
    shard.put_metas(metas)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// reads

#[derive(Clone, Debug, Default)]
pub struct DocOpts {
    pub rev: Option<String>,
    pub revs: bool,
    pub revs_info: bool,
    pub local_seq: bool,
    pub conflicts: bool,
    pub deleted_conflicts: bool,
    pub open_revs: Option<Vec<String>>,
    /// Internal quorum reads need the winning tombstone instead of a 404 so
    /// deletion can participate in revision comparison.
    pub include_deleted: bool,
    pub latest: bool,
}

#[derive(Clone, Debug)]
pub struct AllDocsOpts {
    pub partition: Option<String>,
    pub include_docs: bool,
    pub conflicts: bool,
    pub keys: Option<Vec<Value>>,
    pub startkey: Option<String>,
    pub endkey: Option<String>,
    pub descending: bool,
    pub inclusive_end: bool,
    pub limit: usize,
    pub skip: usize,
    /// When set, `total_rows` counts only the live ids in `[first, last)`
    /// instead of every live document (`_design_docs`).
    pub count_range: Option<(String, String)>,
}

impl Default for AllDocsOpts {
    fn default() -> Self {
        AllDocsOpts {
            partition: None,
            include_docs: false,
            conflicts: false,
            keys: None,
            startkey: None,
            endkey: None,
            descending: false,
            inclusive_end: true,
            limit: usize::MAX,
            skip: 0,
            count_range: None,
        }
    }
}

/// The counts an `_all_docs` reply leads with.
pub(crate) struct AllDocsHead {
    pub total_rows: usize,
    pub offset: usize,
}

#[derive(Clone, Debug, Default)]
pub struct ChangesOpts {
    /// Each shard's position to read after.
    pub since: ShardSince,
    pub limit: Option<usize>,
    pub include_docs: bool,
    pub style_all_docs: bool,
    pub descending: bool,
    pub doc_ids: Option<Vec<String>>,
    pub filter: Option<ChangeFilter>,
    pub conflicts: bool,
    /// Internal cluster filter: only scan shards selected by this mask.
    pub owned: Option<ShardMask>,
    /// Internal: each shard's position to read up to, taken by the caller
    /// (an index update reads up to where the shards were when it started).
    /// Without it a read stops at the committed positions when it starts.
    pub until: Option<ShardSince>,
}

/// Server-side `_changes` filters. `_doc_ids` stays in `doc_ids` because it is
/// an id-only filter that does not need to load document bodies.
#[derive(Clone, Debug)]
pub enum ChangeFilter {
    Design,
    Js { source: String, query: String },
}

impl ChangeFilter {
    fn needs_doc(&self) -> bool {
        matches!(self, ChangeFilter::Js { .. })
    }
}

/// What a changes stream yields: the rows, and after each shard that was read
/// to its end rather than stopped by the limit, that shard and the position
/// it was scanned to. A filter may have discarded rows after the last one
/// yielded, and CouchDB's feed position (`couch_changes:changes_enumerator`)
/// covers them too.
#[derive(Debug)]
pub(crate) enum ChangesItem {
    Row(ChangeRow),
    Scanned(usize, u64),
}

/// The `_revs_diff` answer: the documents that miss revisions, in the order
/// they were asked for, serialized as one JSON object keyed by document id.
#[derive(Debug, Default)]
pub struct RevsDiff(pub Vec<(String, RevsDiffEntry)>);

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RevsDiffEntry {
    pub missing: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub possible_ancestors: Vec<String>,
}

impl serde::Serialize for RevsDiff {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (id, entry) in &self.0 {
            map.serialize_entry(id, entry)?;
        }
        map.end()
    }
}

/// The `_bulk_get` answer: one result per request, in request order.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct BulkGetResults {
    pub results: Vec<Option<BulkGetResult>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct BulkGetResult {
    pub id: String,
    pub docs: Vec<BulkGetDoc>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum BulkGetDoc {
    #[serde(rename = "ok")]
    Doc(Value),
    #[serde(rename = "error")]
    Error(BulkGetError),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct BulkGetError {
    pub id: String,
    pub rev: String,
    pub error: String,
    pub reason: String,
}

/// The result of one document write.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WriteOk {
    pub ok: bool,
    pub id: String,
    pub rev: String,
}

impl WriteOk {
    pub fn new(id: String, rev: String) -> WriteOk {
        WriteOk { ok: true, id, rev }
    }
}

/// A failed document write. `id` is absent when the document had none that
/// could be read; a `_bulk_docs` validation failure reports the `_id` as sent,
/// whatever its type.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WriteError {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_value"
    )]
    pub id: Option<Value>,
    pub error: String,
    pub reason: String,
}

/// Read a field that may be `null` as `Some(Value::Null)`, so that it
/// serializes back as `null` instead of vanishing.
pub(crate) fn present_value<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Value>, D::Error> {
    <Value as serde::Deserialize>::deserialize(deserializer).map(Some)
}

/// One entry of a `_bulk_docs` answer: serialized as the bare object, `ok`
/// results and error results alike.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(untagged)]
pub enum WriteResult {
    Ok(WriteOk),
    Error(WriteError),
}

impl WriteResult {
    pub fn error(id: Option<Value>, error: ApiError) -> WriteResult {
        WriteResult::Error(WriteError {
            id,
            error: error.error.to_string(),
            reason: error.reason,
        })
    }

    /// The wire form, for tests that read fields by name.
    #[cfg(test)]
    pub(crate) fn json(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, WriteResult::Ok(_))
    }
}

impl std::fmt::Display for WriteResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(text) => f.write_str(&text),
            Err(_) => f.write_str("<unprintable>"),
        }
    }
}

#[derive(serde::Deserialize)]
struct FlatWriteResult {
    ok: Option<bool>,
    #[serde(default, deserialize_with = "present_value")]
    id: Option<Value>,
    rev: Option<String>,
    error: Option<String>,
    reason: Option<String>,
}

impl<'de> serde::Deserialize<'de> for WriteResult {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let flat = FlatWriteResult::deserialize(deserializer)?;
        match (flat.error, flat.reason) {
            (Some(error), reason) => Ok(WriteResult::Error(WriteError {
                id: flat.id,
                error,
                reason: reason.unwrap_or_default(),
            })),
            (None, _) => match (flat.ok, flat.id, flat.rev) {
                (Some(ok), Some(Value::String(id)), Some(rev)) => {
                    Ok(WriteResult::Ok(WriteOk { ok, id, rev }))
                }
                _ => Err(D::Error::custom("not a document write result")),
            },
        }
    }
}

/// The `_changes` answer of one pass.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ChangesList {
    pub results: Vec<ChangeEntry>,
    /// An opaque token (`crate::since`).
    pub last_seq: String,
    #[serde(default)]
    pub pending: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ChangeEntry {
    /// An opaque token (`crate::since`).
    pub seq: String,
    pub id: String,
    pub changes: Vec<ChangeRev>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deleted: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_value"
    )]
    pub doc: Option<Value>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ChangeRev {
    pub rev: String,
}

/// An `_all_docs`-shaped listing (`_all_docs`, `_design_docs`, `_local_docs`).
/// `_local_docs` and keyed reads report no `offset` and `_local_docs` no
/// `total_rows`: absent here means `null` on the wire.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct DocList {
    pub total_rows: Option<u64>,
    pub offset: Option<u64>,
    pub rows: Vec<DocListRow>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_value"
    )]
    pub update_seq: Option<Value>,
}

impl DocList {
    /// The wire form, for tests that read fields by name.
    #[cfg(test)]
    pub(crate) fn json(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// One row of a `DocList`: a document (`id`, `key`, `value`, maybe `doc`) or
/// the `error` row for a key that names none (`key`, `error`).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DocListRow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub key: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<RowValue>,
    /// `Some(Value::Null)` is the `doc: null` of a deleted document.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_value"
    )]
    pub doc: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RowValue {
    pub rev: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deleted: bool,
}

impl DocListRow {
    fn found(id: &str, rev: String, doc: Option<Value>) -> DocListRow {
        DocListRow {
            id: Some(id.to_string()),
            key: Value::String(id.to_string()),
            value: Some(RowValue {
                rev,
                deleted: false,
            }),
            doc,
            error: None,
        }
    }

    pub fn not_found(key: Value) -> DocListRow {
        DocListRow {
            id: None,
            key,
            value: None,
            doc: None,
            error: Some("not_found".to_string()),
        }
    }
}

/// The body of `GET /{db}`. Fields serialize in CouchDB's order.
#[derive(Debug, serde::Serialize)]
pub struct DbInfo {
    pub db_name: String,
    pub doc_count: u64,
    pub doc_del_count: u64,
    pub update_seq: u64,
    pub purge_seq: u64,
    pub compact_running: bool,
    pub disk_size: u64,
    pub data_size: u64,
    pub sizes: DbSizes,
    pub instance_start_time: String,
    pub disk_format_version: u64,
    pub committed_update_seq: u64,
    pub uuid: String,
    pub cluster: ClusterInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub props: Option<DbProps>,
    /// Each shard's uuid and committed position; `update_seq` is their sum.
    #[serde(skip)]
    pub shard_seqs: Vec<(String, u64)>,
    /// Each shard's purge sequence; `purge_seq` is their sum.
    #[serde(skip)]
    pub shard_purge_seqs: Vec<u64>,
}

impl DbInfo {
    /// Report `sizes` (and the deprecated `disk_size` and `data_size`
    /// that repeat two of them).
    pub(crate) fn set_sizes(&mut self, sizes: DbSizes) {
        self.sizes = sizes;
        self.disk_size = sizes.file;
        self.data_size = sizes.active;
    }

    /// The `update_seq` and `purge_seq` of this node's copies as opaque
    /// tokens naming node `me`, as a standalone node reports them.
    pub(crate) fn local_tokens(&self, me: usize) -> (String, String) {
        let copies = |seq: &dyn Fn(usize) -> u64| -> Vec<crate::since::CopyPos> {
            self.shard_seqs
                .iter()
                .enumerate()
                .map(|(shard, (uuid, _))| crate::since::CopyPos {
                    node: me,
                    shard,
                    seq: seq(shard),
                    uuid: uuid.clone(),
                })
                .collect()
        };
        (
            crate::since::encode(&copies(&|shard| self.shard_seqs[shard].1)),
            crate::since::encode(&copies(&|shard| {
                self.shard_purge_seqs.get(shard).copied().unwrap_or(0)
            })),
        )
    }
}

/// `sizes` in `GET /db`, in bytes. `file` is the shard files' length on
/// disk. `external` is CouchDB's: the JSON bodies of every document's leaf
/// revisions (conflicts and tombstones included), counted exactly as each
/// write commits. `active` is what compaction would keep: `external` plus
/// the keys and values of the tables other than the bodies as last
/// measured ([`ShardHandle::index_bytes`]), at most `file` but never less
/// than `external`; `external` alone until a first measurement.
#[derive(Clone, Copy, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DbSizes {
    pub active: u64,
    pub external: u64,
    pub file: u64,
}

impl DbSizes {
    /// The sizes of a shard whose file is `file` bytes long.
    fn of_shard(file: u64, external: u64, index: Option<u64>) -> DbSizes {
        DbSizes {
            active: external
                .saturating_add(index.unwrap_or(0))
                .min(file)
                .max(external),
            external,
            file,
        }
    }

    pub fn add(&mut self, other: DbSizes) {
        self.active = self.active.saturating_add(other.active);
        self.external = self.external.saturating_add(other.external);
        self.file = self.file.saturating_add(other.file);
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ClusterInfo {
    pub q: u32,
    pub n: usize,
    pub w: usize,
    pub r: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct DbProps {
    pub partitioned: bool,
}

#[derive(Debug)]
pub(crate) struct ChangeRow {
    pub(crate) id: String,
    /// The shard the row comes from, and its sequence in that shard.
    pub(crate) shard: usize,
    pub(crate) seq: u64,
    pub(crate) changes: Vec<(String, bool)>,
    pub(crate) deleted: bool,
    /// Full document JSON with `_id`/`_rev` already inserted. Keeping this as
    /// text lets the normal changes feed embed the stored body without parsing
    /// and re-serializing it.
    pub(crate) doc: Option<String>,
}

/// A revision as CouchDB stores and renders it: `couch_doc:parse_rev/1` decodes
/// a 32-character id as hex, so upper-case digits name the same revision as
/// their lower-case form. Anything that does not parse is left for the caller
/// to reject or report missing.
fn canonical_rev(rev: &str) -> String {
    Rev::parse(rev).map_or_else(|| rev.to_string(), |parsed| parsed.to_string())
}

/// In a listing, a document whose stored record cannot be decoded is skipped,
/// logged and counted so one bad record does not fail the whole request.
/// Only a decode failure (`ApiError::corrupt`) is skipped: a storage error or
/// a value failing its checksum, or any other failure, still fails the
/// request.
fn skip_corrupt<T>(shard: &impl DocRead, id: &str, result: DbResult<T>) -> DbResult<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.corrupt => {
            shard.name().report_corrupt(id, &error);
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn read_doc(shard: &impl DocRead, id: &str, opts: &DocOpts) -> DbResult<Value> {
    let meta = shard
        .get_meta(id)?
        .ok_or_else(|| ApiError::not_found("missing"))?;

    if let Some(open_revs) = &opts.open_revs {
        let mut wanted: Vec<String> = if open_revs.iter().any(|r| r == "all") {
            meta.tree.leaves()
        } else {
            open_revs.iter().map(|r| canonical_rev(r)).collect()
        };
        if !open_revs.iter().any(|r| r == "all")
            && open_revs.iter().any(|rev| Rev::parse(rev).is_none())
        {
            return Err(ApiError::bad_request("Invalid rev format"));
        }
        if opts.latest {
            let mut seen = std::collections::HashSet::new();
            wanted = wanted
                .into_iter()
                .flat_map(|rev| {
                    let leaves = meta.tree.leaves_from(&rev);
                    if leaves.is_empty() {
                        vec![rev]
                    } else {
                        leaves
                    }
                })
                .filter(|leaf| seen.insert(leaf.clone()))
                .collect();
        }
        let mut docs = Vec::new();
        for rev in wanted {
            match read_doc_rev(shard, id, &meta, &rev, opts) {
                Ok(doc) => docs.push(json!({"ok": doc})),
                // Only a revision that is not stored is "missing"; a read or
                // decode failure must not be reported as one.
                Err(e) if e.status == 404 => docs.push(json!({"missing": rev, "error": e.error})),
                Err(e) => return Err(e),
            }
        }
        return Ok(Value::Array(docs));
    }

    let rev = match &opts.rev {
        Some(r) => {
            let r = canonical_rev(r);
            if opts.latest {
                meta.tree
                    .winner_among(meta.tree.leaves_from(&r))
                    .unwrap_or(r)
            } else {
                r
            }
        }
        None => {
            let winner = meta
                .tree
                .winner()
                .ok_or_else(|| ApiError::not_found("missing"))?;
            if meta.tree.is_deleted(&winner) && !opts.include_deleted {
                return Err(ApiError::not_found("deleted"));
            }
            winner
        }
    };
    match read_doc_rev(shard, id, &meta, &rev, opts) {
        Ok(doc) => Ok(doc),
        // A tombstone may have no stored body. Quorum reads still need the
        // revision and deleted flag for winner selection.
        Err(error) if opts.include_deleted && meta.tree.is_deleted(&rev) && error.status == 404 => {
            Ok(json!({"_id": id, "_rev": rev, "_deleted": true}))
        }
        Err(error) => Err(error),
    }
}

fn read_doc_rev(
    shard: &impl DocRead,
    id: &str,
    meta: &DocMeta,
    rev: &str,
    opts: &DocOpts,
) -> DbResult<Value> {
    if !meta.tree.contains(rev) {
        return Err(ApiError::not_found("missing"));
    }
    let body = shard
        .get_body(id, rev)?
        .ok_or_else(|| ApiError::not_found("missing"))?;
    let mut doc: Value = serde_json::from_slice(&body)
        .map_err(|error| ApiError::corrupt(format!("corrupt body for {id} {rev}: {error}")))?;
    let Value::Object(body) = &mut doc else {
        return Err(ApiError::corrupt(format!(
            "corrupt body for {id} {rev}: not an object"
        )));
    };

    // CouchDB serialises `_id` and `_rev` first, then the document body in the
    // order the client wrote it. Body order is load-bearing: it feeds the rev
    // hash, so never route this through a sorted map.
    let mut map = Map::new();
    map.insert("_id".into(), json!(id));
    map.insert("_rev".into(), json!(rev));
    for (key, value) in body.iter() {
        map.insert(key.clone(), value.clone());
    }

    if meta.tree.is_deleted(rev) {
        map.insert("_deleted".into(), json!(true));
    }
    if opts.revs {
        let mut path = meta.tree.path(rev).map_err(|e| corrupt_tree(id, e))?;
        let limit = shard.revs_limit()?.max(1) as usize;
        if path.len() > limit {
            path.drain(0..path.len() - limit);
        }
        let start = Rev::parse(rev).map(|r| r.pos).unwrap_or(0);
        let ids: Vec<String> = path
            .iter()
            .rev()
            .filter_map(|r| Rev::parse(r).map(|p| p.id_string()))
            .collect();
        map.insert("_revisions".into(), json!({ "start": start, "ids": ids }));
    }
    if opts.local_seq {
        map.insert("_local_seq".into(), json!(meta.seq));
    }
    if opts.revs_info {
        let mut path = meta.tree.path(rev).map_err(|e| corrupt_tree(id, e))?;
        let limit = shard.revs_limit()?.max(1) as usize;
        if path.len() > limit {
            path.drain(0..path.len() - limit);
        }
        // A body compaction has dropped reads as "missing", as in CouchDB.
        let mut info: Vec<Value> = Vec::with_capacity(path.len());
        for r in path.iter().rev() {
            let status = if meta.tree.is_deleted(r) {
                "deleted"
            } else if shard.get_body(id, r)?.is_some() {
                "available"
            } else {
                "missing"
            };
            info.push(json!({ "rev": r, "status": status }));
        }
        map.insert("_revs_info".into(), Value::Array(info));
    }
    if opts.conflicts {
        if let Some(winner) = meta.tree.winner() {
            let conflicts = meta.tree.conflicts(&winner);
            if !conflicts.is_empty() {
                map.insert("_conflicts".into(), json!(conflicts));
            }
        }
    }
    if opts.deleted_conflicts {
        if let Some(winner) = meta.tree.winner() {
            let conflicts = meta.tree.deleted_conflicts(&winner);
            if !conflicts.is_empty() {
                map.insert("_deleted_conflicts".into(), json!(conflicts));
            }
        }
    }
    Ok(Value::Object(map))
}

/// Scan one shard for the live rows in the requested key range, stopping once
/// `take` rows have been collected. Also returns how many live rows precede
/// `startkey`, which is the shard's contribution to `offset`.
/// How many live ids sort before the query's start key, in the query's
/// direction: the `offset` of an `_all_docs` reply, before `skip`.
/// The keys a query answers, in the order it answers them: reversed for
/// `descending`, then past `skip` and up to `limit`.
fn keys_page(opts: &AllDocsOpts, keys: &[Value]) -> Vec<Value> {
    let ordered: Box<dyn Iterator<Item = &Value> + '_> = if opts.descending {
        Box::new(keys.iter().rev())
    } else {
        Box::new(keys.iter())
    };
    ordered.skip(opts.skip).take(opts.limit).cloned().collect()
}

/// The `total_rows` of one shard: its live documents, or those of the query's
/// partition or `count_range`.
fn all_docs_live_total(shard: &mut ShardTxn, opts: &AllDocsOpts) -> DbResult<u64> {
    Ok(match (&opts.partition, &opts.count_range) {
        (Some(partition), _) => shard.partition_live_count(partition)?,
        (None, Some((first, last))) => shard.live_range_count(first, last)? as u64,
        (None, None) => shard.counts().0,
    })
}

fn all_docs_before_start(shard: &mut ShardTxn, opts: &AllDocsOpts) -> DbResult<usize> {
    let Some(start) = opts.startkey.as_deref() else {
        return Ok(0);
    };
    // The rank comes from the shard's cursor, so paging by `startkey` does not
    // recount every id before it on each request.
    if let Some(partition) = &opts.partition {
        return Ok(shard.partition_offset(partition, start, opts.descending)?);
    }
    let rank = shard.live_rank(start)?;
    let live = shard.live_docs()?;
    Ok(if opts.descending {
        live.len()
            .saturating_sub(rank + usize::from(live.contains_key(start)))
    } else {
        rank
    })
}

/// Up to `take` live ids of the query's range in the query's direction,
/// starting after `after` when it is given. `after` must be an id this scan
/// already returned (or the start of the range), so a later page continues
/// where the last one stopped.
fn scan_all_docs(
    shard: &mut ShardTxn,
    opts: &AllDocsOpts,
    after: Option<&str>,
    take: usize,
) -> DbResult<Vec<AllDocsRow>> {
    let start = opts.startkey.as_deref();
    let end = opts.endkey.as_deref();
    let empty_range = match (start, end) {
        (Some(start), Some(end)) if opts.descending => start < end,
        (Some(start), Some(end)) => start > end,
        _ => false,
    };
    if empty_range {
        return Ok(Vec::new());
    }
    let live = shard.live_docs()?;
    let end_bound = match end {
        Some(end) if opts.inclusive_end => Bound::Included(end),
        Some(end) => Bound::Excluded(end),
        None => Bound::Unbounded,
    };
    let row = |(id, doc): (&String, &LiveDoc)| AllDocsRow {
        id: id.clone(),
        rev: doc.rev.clone(),
        doc: None,
    };
    let rows = if opts.descending {
        let low = end_bound;
        let high = match (after, start) {
            (Some(after), _) => Bound::Excluded(after),
            (None, Some(start)) => Bound::Included(start),
            (None, None) => Bound::Unbounded,
        };
        live.range::<str, _>((low, high))
            .rev()
            .take(take)
            .map(row)
            .collect()
    } else {
        let low = match (after, start) {
            (Some(after), _) => Bound::Excluded(after),
            (None, Some(start)) => Bound::Included(start),
            (None, None) => Bound::Unbounded,
        };
        let high = end_bound;
        live.range::<str, _>((low, high))
            .take(take)
            .map(row)
            .collect()
    };
    Ok(rows)
}

/// Attach each row's document when the query asks for them, leaving out any
/// whose record is corrupt.
fn load_all_docs(
    shard: &mut ShardTxn,
    rows: Vec<AllDocsRow>,
    opts: &AllDocsOpts,
) -> DbResult<Vec<AllDocsRow>> {
    if !opts.include_docs {
        return Ok(rows);
    }
    let mut readable = Vec::with_capacity(rows.len());
    for mut row in rows {
        let doc_opts = DocOpts {
            rev: Some(row.rev.clone()),
            conflicts: opts.conflicts,
            ..Default::default()
        };
        let doc = read_doc(shard, &row.id, &doc_opts);
        if let Some(doc) = skip_corrupt(shard, &row.id, doc)? {
            row.doc = Some(doc);
            readable.push(row);
        }
    }
    Ok(readable)
}

fn collect_all_docs(
    shard: &mut ShardTxn,
    opts: &AllDocsOpts,
    take: usize,
) -> DbResult<(Vec<AllDocsRow>, usize)> {
    let before_start = all_docs_before_start(shard, opts)?;
    let rows = scan_all_docs(shard, opts, None, take)?;
    Ok((load_all_docs(shard, rows, opts)?, before_start))
}

/// Put `_id` and `_rev` in front of an already-serialized document body.
/// Bodies are stored as compact JSON objects, so no parse/encode is needed.
fn embed_doc_meta(id: &str, rev: &str, body: &[u8]) -> String {
    let id = serde_json::to_string(id).unwrap_or_else(|_| "\"\"".to_string());
    let rev = serde_json::to_string(rev).unwrap_or_else(|_| "\"\"".to_string());
    let Ok(body) = std::str::from_utf8(body) else {
        return format!("{{\"_id\":{id},\"_rev\":{rev}}}");
    };
    let rest = body.strip_prefix('{').unwrap_or(body);
    if rest.trim_start().starts_with('}') {
        format!("{{\"_id\":{id},\"_rev\":{rev}}}")
    } else {
        format!("{{\"_id\":{id},\"_rev\":{rev},{rest}")
    }
}

fn all_docs_key_row(
    shard: &impl DocRead,
    key: &str,
    opts: &AllDocsOpts,
) -> DbResult<Option<DocListRow>> {
    let Some(meta) = shard.get_meta(key)? else {
        return Ok(None);
    };
    let Some(winner) = meta.tree.winner() else {
        return Ok(None);
    };
    let deleted = meta.tree.is_deleted(&winner);
    let mut row = DocListRow::found(key, winner.clone(), None);
    if let Some(value) = &mut row.value {
        value.deleted = deleted;
    }
    if opts.include_docs {
        row.doc = Some(if deleted {
            Value::Null
        } else {
            let doc_opts = DocOpts {
                rev: Some(winner),
                conflicts: opts.conflicts,
                ..Default::default()
            };
            read_doc(shard, key, &doc_opts)?
        });
    }
    Ok(Some(row))
}

fn corrupt_local(id: &str, error: impl std::fmt::Display) -> ApiError {
    ApiError::new(
        500,
        "internal_server_error",
        format!("corrupt local document {id}: {error}"),
    )
}

/// The revision number of a `0-N` local revision.
fn local_rev_number(doc: &Value) -> Option<u64> {
    doc.get("_rev")?
        .as_str()?
        .strip_prefix("0-")?
        .parse::<u64>()
        .ok()
}

fn read_local(s: &mut ShardTxn, id: &str) -> DbResult<Option<Value>> {
    match s.get_local(id)? {
        Some(bytes) => serde_json::from_slice::<Value>(&bytes)
            .map(Some)
            .map_err(|error| corrupt_local(id, error)),
        None => Ok(None),
    }
}

fn write_local(s: &mut ShardTxn, id: &str, mut body: Value) -> DbResult<WriteOk> {
    // CouchDB (couch_db_updater:update_local_docs/2) numbers a local write
    // from the supplied `_rev`, absent meaning 0, and never conflicts: the
    // stored revision is not consulted.
    let supplied = match body.get("_rev") {
        None => 0,
        Some(rev) => rev
            .as_str()
            .and_then(|r| r.strip_prefix("0-"))
            .and_then(|n| n.parse::<u64>().ok())
            .ok_or_else(|| ApiError::bad_request("Invalid rev format"))?,
    };
    let next = supplied.saturating_add(1);
    let rev = format!("0-{next}");
    if let Some(obj) = body.as_object_mut() {
        obj.insert("_id".into(), json!(format!("_local/{id}")));
        obj.insert("_rev".into(), json!(rev));
    }
    s.put_local(id, &serde_json::to_vec(&body).map_err(ApiError::internal)?)?;
    Ok(WriteOk::new(format!("_local/{id}"), rev))
}

fn delete_local(s: &mut ShardTxn, id: &str) -> DbResult<WriteOk> {
    if s.get_local(id)?.is_none() {
        return Err(ApiError::not_found("missing"));
    }
    s.remove_local(id)?;
    Ok(WriteOk::new(format!("_local/{id}"), "0-0".to_string()))
}

/// Keep a copy of a local document as it is, unless a newer one is stored.
fn install_local(s: &mut ShardTxn, id: &str, doc: Value) -> DbResult<WriteOk> {
    let Some(incoming) = local_rev_number(&doc) else {
        return Err(ApiError::bad_request("Invalid rev format"));
    };
    let held = read_local(s, id)?.as_ref().and_then(local_rev_number);
    let rev = doc
        .get("_rev")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if held.is_none_or(|held| held < incoming) {
        s.put_local(id, &serde_json::to_vec(&doc).map_err(ApiError::internal)?)?;
    }
    Ok(WriteOk::new(format!("_local/{id}"), rev))
}

/// Every local document of one shard, with its `_local/` id.
fn list_local(s: &mut ShardTxn) -> DbResult<Vec<(String, Value)>> {
    s.locals()?
        .into_iter()
        .map(|(id, doc)| match serde_json::from_slice::<Value>(&doc) {
            Ok(doc) => Ok((format!("_local/{id}"), doc)),
            Err(error) => Err(corrupt_local(&id, error)),
        })
        .collect()
}

/// Select and render `_local_docs` rows from every local document, which
/// arrive in id order.
fn local_doc_rows(docs: Vec<(String, Value)>, opts: &AllDocsOpts) -> Vec<DocListRow> {
    let render = |id: &str, doc: &Value| {
        let rev = doc
            .get("_rev")
            .and_then(Value::as_str)
            .unwrap_or("0-1")
            .to_string();
        DocListRow::found(id, rev, opts.include_docs.then(|| doc.clone()))
    };
    let rows: Vec<DocListRow> = if let Some(keys) = &opts.keys {
        let mut keys: Vec<&str> = keys
            .iter()
            .filter_map(Value::as_str)
            .filter(|key| key.starts_with("_local/"))
            .collect();
        if opts.descending {
            keys.reverse();
        }
        keys.into_iter()
            .map(|key| match docs.iter().find(|(id, _)| id == key) {
                Some((id, doc)) => render(id, doc),
                None => DocListRow::not_found(Value::String(key.to_string())),
            })
            .collect()
    } else {
        let in_range = |id: &str| {
            let (low, high) = if opts.descending {
                (opts.endkey.as_deref(), opts.startkey.as_deref())
            } else {
                (opts.startkey.as_deref(), opts.endkey.as_deref())
            };
            let (low_inclusive, high_inclusive) = if opts.descending {
                (opts.inclusive_end, true)
            } else {
                (true, opts.inclusive_end)
            };
            low.is_none_or(|low| id > low || (low_inclusive && id == low))
                && high.is_none_or(|high| id < high || (high_inclusive && id == high))
        };
        let mut selected: Vec<&(String, Value)> =
            docs.iter().filter(|(id, _)| in_range(id)).collect();
        if opts.descending {
            selected.reverse();
        }
        selected
            .into_iter()
            .map(|(id, doc)| render(id, doc))
            .collect()
    };
    rows.into_iter().skip(opts.skip).take(opts.limit).collect()
}

/// One `_all_docs` row before it is rendered as JSON.
struct AllDocsRow {
    id: String,
    rev: String,
    doc: Option<Value>,
}

fn all_docs_row(AllDocsRow { id, rev, doc }: AllDocsRow) -> DocListRow {
    DocListRow {
        key: Value::String(id.clone()),
        id: Some(id),
        value: Some(RowValue {
            rev,
            deleted: false,
        }),
        doc,
        error: None,
    }
}

fn finish_all_docs(rows: Vec<AllDocsRow>, total: usize, offset: usize) -> DbResult<DocList> {
    Ok(DocList {
        total_rows: Some(total as u64),
        offset: Some(offset as u64),
        rows: rows.into_iter().map(all_docs_row).collect(),
        update_seq: None,
    })
}

/// Apply a `_changes` filter to one batch of rows. A JavaScript filter runs
/// the whole batch in one go on the blocking pool, as view maps do.
async fn apply_change_filter(
    rows: Vec<ChangeRow>,
    filter: &ChangeFilter,
) -> DbResult<Vec<ChangeRow>> {
    match filter {
        ChangeFilter::Design => Ok(rows
            .into_iter()
            .filter(|row| row.id.starts_with("_design/"))
            .collect()),
        ChangeFilter::Js { source, query } => {
            let (source, query) = (source.clone(), query.clone());
            crate::view::blocking(move || {
                let docs: Vec<(String, String)> = rows
                    .iter()
                    .filter_map(|row| row.doc.clone().map(|doc| (row.id.clone(), doc)))
                    .collect();
                if docs.is_empty() {
                    return Ok(Vec::new());
                }
                let matched = crate::viewfn::run_filter_json(
                    &source,
                    &query,
                    &docs,
                    crate::js::ViewLimits::default(),
                )
                .map_err(ApiError::from)?;
                Ok(rows
                    .into_iter()
                    .filter(|row| matched.contains(&row.id))
                    .collect())
            })
            .await
        }
    }
}

/// One page of a shard's changes.
struct ChangesPage {
    rows: Vec<ChangeRow>,
    /// The sequence of the last document examined, row or not.
    scanned: Option<u64>,
    /// Whether the page reached the end of the range rather than its size.
    exhausted: bool,
}

/// Up to `limit` change rows of shard `index`. Going up, the rows after
/// `cursor` up to `until`; going down, the rows at or below `cursor`, so a
/// caller pages back by lowering it.
fn collect_changes(
    shard: &mut ShardTxn,
    opts: &ChangesOpts,
    index: usize,
    cursor: u64,
    until: u64,
    limit: usize,
) -> DbResult<ChangesPage> {
    if limit == 0 {
        return Ok(ChangesPage {
            rows: Vec::new(),
            scanned: None,
            exhausted: false,
        });
    }

    // CouchDB returns one row per document at its latest sequence, not one
    // row per historical sequence entry. `latest_changes` is maintained on
    // writes and built lazily from metadata.
    // Read only the rows after the cursor, already in sequence order. A
    // descending feed walks back from the newest change.
    let latest = shard.latest_changes()?;
    let mut candidates: Vec<(u64, String)> = match &opts.doc_ids {
        Some(ids) => {
            let mut selected: Vec<(u64, String)> = ids
                .iter()
                .filter_map(|id| latest.seq_of(id).map(|seq| (seq, id.clone())))
                .filter(|(seq, _)| {
                    if opts.descending {
                        *seq <= cursor
                    } else {
                        *seq > cursor && *seq <= until
                    }
                })
                .collect();
            selected.sort();
            selected.dedup();
            if opts.descending {
                selected.reverse();
            }
            selected.truncate(limit.saturating_add(1));
            selected
        }
        None if opts.descending => latest
            .through(cursor)
            .rev()
            .take(limit.saturating_add(1))
            .map(|(seq, id)| (seq, id.to_string()))
            .collect(),
        None => latest
            .after(cursor)
            .take_while(|(seq, _)| *seq <= until)
            .take(limit.saturating_add(1))
            .map(|(seq, id)| (seq, id.to_string()))
            .collect(),
    };
    // One look past the page tells whether the range goes on.
    let exhausted = candidates.len() <= limit;
    candidates.truncate(limit);
    let scanned = candidates.last().map(|(seq, _)| *seq);

    shard.add_scanned(candidates.len());
    let mut rows = Vec::with_capacity(candidates.len());
    for (seq, id) in candidates {
        let meta = shard.get_meta(&id).map_err(ApiError::from);
        let Some(Some(meta)) = skip_corrupt(shard, &id, meta)? else {
            continue;
        };
        if meta.seq != seq {
            continue;
        }
        let Some(winner) = meta.tree.winner() else {
            continue;
        };
        let deleted = meta.tree.is_deleted(&winner);
        let changes: Vec<(String, bool)> = if opts.style_all_docs {
            // CouchDB lists the winner first, then the remaining leaves by
            // descending position/id, regardless of whether the winner is
            // live or deleted.
            let mut leaves = meta
                .tree
                .leaves()
                .into_iter()
                .filter(|leaf| leaf != &winner)
                .collect::<Vec<_>>();
            leaves.sort_by(|a, b| {
                let pa = Rev::parse(a).map(|r| (r.pos, r.id)).unwrap_or_default();
                let pb = Rev::parse(b).map(|r| (r.pos, r.id)).unwrap_or_default();
                pb.cmp(&pa)
            });
            std::iter::once(winner.clone())
                .chain(leaves)
                .map(|leaf| {
                    let leaf_deleted = meta.tree.is_deleted(&leaf);
                    (leaf, leaf_deleted)
                })
                .collect()
        } else {
            vec![(winner.clone(), deleted)]
        };

        let needs_doc = opts.include_docs
            || opts
                .filter
                .as_ref()
                .map(ChangeFilter::needs_doc)
                .unwrap_or(false);
        let doc = if needs_doc {
            let read = read_doc_rev(
                shard,
                &id,
                &meta,
                &winner,
                &DocOpts {
                    rev: Some(winner.clone()),
                    include_deleted: true,
                    conflicts: opts.conflicts,
                    ..Default::default()
                },
            );
            match read {
                Ok(doc) => Some(serde_json::to_string(&doc).map_err(ApiError::internal)?),
                // A tombstone may have no stored body.
                Err(error) if deleted && error.status == 404 => None,
                Err(error) if error.status == 404 => {
                    return Err(ApiError::new(
                        500,
                        "internal_server_error",
                        format!("document {id} has no stored body"),
                    ))
                }
                // An undecodable body drops the row, as in `_all_docs`.
                Err(error) => {
                    skip_corrupt(shard, &id, Err::<(), _>(error))?;
                    continue;
                }
            }
        } else {
            None
        };
        rows.push(ChangeRow {
            id,
            shard: index,
            seq,
            changes,
            deleted,
            doc,
        });
    }
    Ok(ChangesPage {
        rows,
        scanned,
        exhausted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Metadata decodes and encodes for one `put_many` call.
    fn write_costs(shard: &mut crate::store::Shard, docs: Vec<Value>) -> (usize, usize) {
        let parses = crate::store::meta_parses();
        let serialises = META_SERIALISES.with(std::cell::Cell::get);
        let docs = docs
            .iter()
            .map(|doc| prepare(doc, doc_id(doc).unwrap(), true).unwrap())
            .enumerate()
            .collect();
        let results = shard
            .write(move |s| Ok(put_many(s, docs, true, HashMap::new())))
            .unwrap();
        for (_, result) in results {
            result.unwrap();
        }
        (
            crate::store::meta_parses() - parses,
            META_SERIALISES.with(std::cell::Cell::get) - serialises,
        )
    }

    /// A write reads the document's metadata once (to stage it) and writes it
    /// once. Staging, pruning and the commit's counter bookkeeping used to
    /// each decode or encode it again: five decodes and two encodes per update.
    #[test]
    fn a_write_decodes_the_metadata_once_and_encodes_it_once() {
        let file = crate::store::tests::temp_file("write-cost");
        let mut shard = crate::store::tests::open(&file);

        // A new document has no stored metadata to decode.
        let costs = write_costs(&mut shard, vec![json!({"_id": "a", "n": 1})]);
        assert_eq!(costs, (0, 1), "create");

        let rev = shard
            .read(|s| Ok(s.get_meta("a")?.unwrap().tree.winner().unwrap()))
            .unwrap();
        let update = vec![json!({"_id": "a", "_rev": rev, "n": 2})];
        assert_eq!(write_costs(&mut shard, update), (1, 1), "update");

        // Each document in a batch costs the same.
        let batch = vec![json!({"_id": "b"}), json!({"_id": "c"})];
        assert_eq!(write_costs(&mut shard, batch), (0, 2), "batch");
        drop(shard);
        crate::kv::forget_verified(&file);
        let _ = std::fs::remove_dir_all(file.parent().unwrap());
    }

    /// A bulk job opens each table once: the metadata to read it, then the
    /// bodies, by_seq and metadata to write, and the commit the settings.
    #[test]
    fn a_bulk_job_opens_each_table_once() {
        let file = crate::store::tests::temp_file("table-opens");
        let mut shard = crate::store::tests::open(&file);
        let opens = |shard: &mut crate::store::Shard| shard.write(|s| Ok(s.table_opens())).unwrap();
        let before = opens(&mut shard);
        let docs = (0..500)
            .map(|n| {
                let doc = json!({"_id": format!("doc{n:03}"), "n": n});
                prepare(&doc, doc_id(&doc).unwrap(), true).unwrap()
            })
            .enumerate()
            .collect();
        let results = shard
            .write(move |s| Ok(put_many(s, docs, true, HashMap::new())))
            .unwrap();
        assert!(results.iter().all(|(_, result)| result.is_ok()));
        let opened = opens(&mut shard) - before;
        assert!(opened <= 5, "{opened} table opens");
        drop(shard);
        crate::kv::forget_verified(&file);
        let _ = std::fs::remove_dir_all(file.parent().unwrap());
    }

    /// An edit over a revision at the last position has no next position:
    /// the document fails and nothing of it is stored.
    #[test]
    fn an_edit_past_the_last_position_fails_its_document() {
        let file = crate::store::tests::temp_file("last-position");
        let mut shard = crate::store::tests::open(&file);
        let last = format!("{}-a", u64::MAX);
        let put = |shard: &mut crate::store::Shard, doc: Value, new_edits: bool| {
            let job = vec![(0, prepare(&doc, doc_id(&doc).unwrap(), new_edits).unwrap())];
            let mut out = shard
                .write(move |s| Ok(put_many(s, job, new_edits, HashMap::new())))
                .unwrap();
            out.pop().unwrap().1
        };
        put(&mut shard, json!({"_id": "x", "_rev": last}), false).unwrap();
        let before = shard
            .read(|s| {
                Ok((
                    serde_json::to_string(&s.get_meta("x")?).unwrap(),
                    s.by_seq()?,
                ))
            })
            .unwrap();
        for deleted in [false, true] {
            let edit = json!({"_id": "x", "_rev": last, "_deleted": deleted});
            let error = put(&mut shard, edit, true).unwrap_err();
            assert_eq!(
                (error.status, error.reason.as_str()),
                (400, "Invalid rev format")
            );
        }
        let after = shard
            .read(|s| {
                Ok((
                    serde_json::to_string(&s.get_meta("x")?).unwrap(),
                    s.by_seq()?,
                ))
            })
            .unwrap();
        assert_eq!(after, before);
        drop(shard);
        crate::kv::forget_verified(&file);
        let _ = std::fs::remove_dir_all(file.parent().unwrap());
    }

    /// A batch stores what its documents store written one per batch: the
    /// same results, metadata records, sequences and counts, whatever repeats
    /// an id, fails, or replicates a revision the batch or the shard holds.
    #[test]
    fn a_batch_stores_what_its_documents_store_one_at_a_time() {
        let hex = |n: u8| format!("{n:02x}").repeat(16);
        let batches: [(bool, Vec<Value>); 3] = [
            (true, vec![json!({"_id": "a"}), json!({"_id": "c", "n": 1})]),
            (
                true,
                vec![
                    json!({"_id": "b", "_deleted": true}),
                    json!({"_id": "a"}),
                    json!({"_id": "a", "_rev": "1-967a00dff5e02add41819138abb3284d", "v": 2}),
                    json!({"_id": "a", "_rev": "1-967a00dff5e02add41819138abb3284d"}),
                    json!({"_id": "b"}),
                    json!({"_id": "c", "_rev": "bad"}),
                    json!({"_id": "d", "_deleted": true}),
                    json!({"_id": "d"}),
                ],
            ),
            (
                false,
                vec![
                    json!({"_id": "a", "_revisions": {"start": 3, "ids": [hex(3), hex(2), hex(1)]}}),
                    json!({"_id": "a", "_revisions": {"start": 3, "ids": [hex(3), hex(2), hex(1)]}}),
                    json!({"_id": "c", "_rev": "1-967a00dff5e02add41819138abb3284d"}),
                    json!({"_id": "e", "_revisions": {"start": 2, "ids": [hex(5), hex(4)]}, "_deleted": true}),
                    json!({"_id": "e", "_rev": format!("2-{}", hex(6))}),
                    json!({"_id": "f"}),
                ],
            ),
        ];
        let state = |by_batch: bool| {
            let file = crate::store::tests::temp_file("batch-vs-single");
            let mut shard = crate::store::tests::open(&file);
            let mut results = Vec::new();
            for (new_edits, docs) in &batches {
                let docs: Vec<Prepared> = docs
                    .iter()
                    .map(|doc| prepare(doc, doc_id(doc).unwrap(), *new_edits).unwrap())
                    .collect();
                let jobs: Vec<Vec<Prepared>> = if by_batch {
                    vec![docs]
                } else {
                    docs.into_iter().map(|doc| vec![doc]).collect()
                };
                for job in jobs {
                    let new_edits = *new_edits;
                    let job = job.into_iter().enumerate().collect();
                    let out = shard
                        .write(move |s| Ok(put_many(s, job, new_edits, HashMap::new())))
                        .unwrap();
                    results.extend(out.into_iter().map(|(_, result)| match result {
                        Ok(ok) => json!({"id": ok.id, "rev": ok.rev}),
                        Err(e) => json!({"error": e.error, "reason": e.reason}),
                    }));
                }
            }
            let stored = shard
                .read(|s| {
                    let metas = ["a", "b", "c", "d", "e", "f"].map(|id| {
                        s.get_meta(id)
                            .unwrap()
                            .map(|m| serde_json::to_string(&m).unwrap())
                    });
                    Ok((metas, s.by_seq()?, s.counts()))
                })
                .unwrap();
            drop(shard);
            crate::kv::forget_verified(&file);
            let _ = std::fs::remove_dir_all(file.parent().unwrap());
            (results, stored)
        };
        let single = state(false);
        assert_eq!(state(true), single);
        // The cases ran as meant: a's first revision is the one the batch
        // names, and the replicated revision of `a` was not stored twice.
        assert_eq!(single.0[0]["rev"], "1-967a00dff5e02add41819138abb3284d");
        assert!(single.1 .0[0]
            .as_ref()
            .unwrap()
            .contains(&format!("3-{}", hex(3))));
    }

    /// A bulk job journals every key its commit writes, as writing each key
    /// on its own did: each document's metadata and body, each sequence it
    /// adds or supersedes (one added earlier in the same job included), and
    /// the settings the commit changes.
    #[test]
    fn batched_writes_are_journaled_like_single_ones() {
        use crate::store::journal::{JournalKey, KeyBytes, Table, Taken};
        let file = crate::store::tests::temp_file("bulk-journal");
        let mut shard = crate::store::tests::open(&file);
        let put = |shard: &mut crate::store::Shard, docs: &[Value], new_edits| {
            let docs = docs
                .iter()
                .map(|doc| prepare(doc, doc_id(doc).unwrap(), new_edits).unwrap())
                .enumerate()
                .collect();
            let results = shard
                .write(move |s| Ok(put_many(s, docs, new_edits, HashMap::new())))
                .unwrap();
            results
                .into_iter()
                .map(|(_, result)| result.unwrap())
                .collect::<Vec<_>>()
        };
        let first = put(
            &mut shard,
            &[json!({"_id": "a"}), json!({"_id": "b"})],
            true,
        );
        let seq_of = |shard: &mut crate::store::Shard, id: &'static str| {
            shard
                .read(move |s| Ok(s.get_meta(id)?.unwrap().seq))
                .unwrap()
        };
        // The sequences the job supersedes, from before it.
        let mut expected: std::collections::BTreeSet<JournalKey> = ["a", "b"]
            .into_iter()
            .map(|id| (Table::BySeq, seq_of(&mut shard, id).key_bytes()))
            .collect();
        let start = shard.seq();
        shard.journal().start();

        let edited = [
            json!({"_id": "a", "_rev": first[0].rev, "n": 2}),
            json!({"_id": "b", "_rev": first[1].rev, "_deleted": true}),
            json!({"_id": "c"}),
        ];
        let mut written = put(&mut shard, &edited, true);
        // "e" twice in one job: the second supersedes the first's sequence.
        let (e1, e2) = (
            "1-11111111111111111111111111111111",
            "2-22222222222222222222222222222222",
        );
        let replicated = [
            json!({"_id": "e", "_rev": e1}),
            json!({"_id": "e", "_rev": e2,
                   "_revisions": {"start": 2, "ids": [&e2[2..], &e1[2..]]}}),
        ];
        put(&mut shard, &replicated, false);
        written.push(WriteOk {
            ok: true,
            id: "e".into(),
            rev: e1.into(),
        });
        written.push(WriteOk {
            ok: true,
            id: "e".into(),
            rev: e2.into(),
        });

        let end = shard.seq();
        assert_eq!(end, start + 5);
        for doc in &written {
            expected.insert((Table::Meta, doc.id.as_str().key_bytes()));
            expected.insert((
                Table::Bodies,
                (doc.id.as_str(), doc.rev.as_str()).key_bytes(),
            ));
        }
        for seq in start + 1..=end {
            expected.insert((Table::BySeq, seq.key_bytes()));
        }
        for name in ["seq", "doc_count", "doc_del_count", "external_size"] {
            expected.insert((Table::Settings, name.key_bytes()));
        }
        assert_eq!(shard.journal().take(), Taken::Batch(expected));

        // One by_seq row per document, at its latest sequence.
        let by_seq = shard.read(|s| Ok(s.by_seq()?)).unwrap();
        let mut latest = Vec::new();
        for id in ["a", "b", "c", "e"] {
            latest.push((seq_of(&mut shard, id), id.to_string()));
        }
        latest.sort();
        assert_eq!(by_seq, latest);
        drop(shard);
        crate::kv::forget_verified(&file);
        let _ = std::fs::remove_dir_all(file.parent().unwrap());
    }

    #[test]
    fn an_owned_mask_selects_only_the_shards_it_covers() {
        assert!(shard_owned(None, 7));
        let mask = ShardMask::from_iter([true, false, true]);
        assert!(shard_owned(Some(&mask), 0));
        assert!(!shard_owned(Some(&mask), 1));
        assert!(shard_owned(Some(&mask), 2));
        // A shard beyond the mask is not owned.
        assert!(!shard_owned(Some(&mask), 3));
    }

    #[test]
    fn db_dir_names_are_flat_and_reversible() {
        for name in ["a", "a/b", "a/b/c", "a+b$(c)-d_e", "a/b+c", "ab"] {
            let dir = db_dir_name(name);
            assert!(!dir.contains('/'), "{dir}");
            assert_eq!(dir_name_to_db_name(&dir).as_deref(), Some(name));
        }
        assert_ne!(db_dir_name("a/b"), db_dir_name("a"));
        assert_eq!(dir_name_to_db_name("a%2fb"), None);
        assert_eq!(dir_name_to_db_name("a%b"), None);
    }

    #[test]
    fn uuids_do_not_repeat_in_a_tight_loop() {
        let ids: std::collections::HashSet<String> = (0..100_000).map(|_| uuid()).collect();
        assert_eq!(ids.len(), 100_000);
        assert!(
            ids.iter()
                .all(|id| id.len() == 32
                    && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
        );
    }

    fn doc_id_for_shard(db: &Db, shard: usize) -> String {
        (0..10_000)
            .map(|i| format!("shard{shard}-{i}"))
            .find(|id| db.shard_index(id) == shard)
            .expect("document id for shard")
    }

    async fn single_shard_db(name: &str) -> (Db, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!("douchdb-{name}-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            name,
            DbConfig {
                q: 1,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        (db, root)
    }

    async fn stored_bodies(db: &Db, revs: Vec<String>) -> Vec<bool> {
        db.shards[0]
            .call(move |shard| {
                let mut out = Vec::new();
                for rev in &revs {
                    out.push(shard.get_body("a", rev)?.is_some());
                }
                Ok(out)
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn compaction_drops_bodies_of_revisions_stemmed_by_revs_limit() {
        let root = std::env::temp_dir().join(format!("douchdb-stem-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "stem",
            DbConfig {
                q: 1,
                revs_limit: 2,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        let mut revs = Vec::new();
        for n in 0..4 {
            let mut doc = json!({"_id": "a", "n": n});
            if let Some(rev) = revs.last() {
                doc["_rev"] = json!(rev);
            }
            let put = db.put_docs(vec![doc], true).await.unwrap();
            revs.push(put[0].json()["rev"].as_str().unwrap().to_string());
        }
        assert_eq!(stored_bodies(&db, revs.clone()).await, [true; 4]);
        db.compact().await.unwrap();
        assert_eq!(
            stored_bodies(&db, revs).await,
            [false, false, false, true],
            "stemmed and superseded bodies go, the leaf stays"
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Check `db`'s sizes after `step`: `external` is the stored bodies of
    /// every leaf revision of `ids`, by length summed; `file` is the shard
    /// file's length; `active` lies between them. Returns `external`.
    async fn check_sizes(db: &Db, ids: &[&str], step: &str) -> u64 {
        let info = db.info().await.unwrap();
        let ids: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
        let expected = db.shards[0]
            .call(move |shard| {
                let mut total = 0;
                for id in &ids {
                    let Some(meta) = shard.get_meta(id)? else {
                        continue;
                    };
                    for leaf in meta.tree.leaves() {
                        total += shard
                            .get_body(id, &leaf)?
                            .map_or(0, |body| body.len() as u64);
                    }
                }
                Ok(total)
            })
            .await
            .unwrap();
        assert_eq!(info.sizes.external, expected, "{step}");
        let file = std::fs::metadata(shard_file(&db.path, 0)).unwrap().len();
        assert_eq!(info.sizes.file, file, "{step}");
        assert!(info.sizes.active >= info.sizes.external, "{step}");
        assert!(info.sizes.active <= info.sizes.file, "{step}");
        info.sizes.external
    }

    /// `sizes.external` is CouchDB's: every leaf revision's body (conflicts
    /// and tombstones included) counted once, whatever superseded bodies
    /// the file still holds, through updates, conflicts, deletes, purges,
    /// stemming, a compaction and a reopen.
    #[tokio::test]
    async fn sizes_follow_the_leaf_bodies_and_the_file() {
        let (db, root) = single_shard_db("sizes").await;
        let ids = ["a", "b", "c"];
        let rev = |written: &[WriteResult]| written[0].json()["rev"].as_str().unwrap().to_string();
        assert_eq!(check_sizes(&db, &ids, "empty").await, 0);

        let a1 = rev(&db
            .put_docs(vec![json!({"_id": "a", "v": "x".repeat(300)})], true)
            .await
            .unwrap());
        let b1 = rev(&db
            .put_docs(vec![json!({"_id": "b", "v": 1})], true)
            .await
            .unwrap());
        let created = check_sizes(&db, &ids, "created").await;
        assert!(created > 300);
        let a2 = rev(&db
            .put_docs(vec![json!({"_id": "a", "_rev": a1, "v": "y"})], true)
            .await
            .unwrap());
        assert!(
            check_sizes(&db, &ids, "updated").await < created,
            "the superseded body no longer counts"
        );

        // A conflicting branch adds its leaf.
        let a1_id = a1.split_once('-').unwrap().1.to_string();
        let branch = "ffffffffffffffffffffffffffffffff";
        let conflict = db
            .put_docs(
                vec![json!({"_id": "a", "_rev": format!("2-{branch}"),
                    "_revisions": {"start": 2, "ids": [branch, a1_id]},
                    "w": "z".repeat(100)})],
                false,
            )
            .await
            .unwrap();
        assert!(
            conflict[0].json()["error"].is_null(),
            "{}",
            conflict[0].json()
        );
        let conflicted = check_sizes(&db, &ids, "conflict").await;

        // A delete leaves a tombstone leaf; a purge takes leaves away.
        let b2 = rev(&db
            .put_docs(
                vec![json!({"_id": "b", "_rev": b1, "_deleted": true})],
                true,
            )
            .await
            .unwrap());
        check_sizes(&db, &ids, "deleted").await;
        let none = Default::default();
        db.purge(&json!({"b": [b2], "a": [a2]}), 0, &none)
            .await
            .unwrap();
        assert!(check_sizes(&db, &ids, "purged").await < conflicted);

        // A document whose old revisions are stemmed counts its leaf.
        db.set_revs_limit(1).await.unwrap();
        let c1 = rev(&db
            .put_docs(vec![json!({"_id": "c", "v": 1})], true)
            .await
            .unwrap());
        db.put_docs(vec![json!({"_id": "c", "_rev": c1, "v": 22})], true)
            .await
            .unwrap();
        let before = check_sizes(&db, &ids, "stemmed").await;

        db.compact().await.unwrap();
        assert_eq!(check_sizes(&db, &ids, "compacted").await, before);
        db.measure_sizes().await;
        let info = db.info().await.unwrap();
        assert!(
            info.sizes.active > info.sizes.external,
            "the other tables are measured once asked: {info:?}"
        );
        drop(db);
        let db = Db::open(
            &root,
            "sizes",
            DbConfig {
                q: 1,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        assert_eq!(check_sizes(&db, &ids, "reopened").await, before);
        for _ in 0..3 {
            let info = db.info().await.unwrap();
            assert_eq!(
                info.sizes.active, info.sizes.external,
                "a reopened shard is measured only once it has taken writes, never for a read: {info:?}"
            );
        }
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn local_docs_spread_over_shards_and_checkpoints_stay_out_of_the_listing() {
        let root =
            std::env::temp_dir().join(format!("douchdb-local-shards-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "local-shards",
            DbConfig {
                q: 4,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: ShardMap::single(4),
                partitioned: false,
            },
        )
        .unwrap();
        let ids: Vec<String> = (0..16).map(|i| format!("doc{i:02}")).collect();
        for id in &ids {
            db.local_put(id, json!({"v": id})).await.unwrap();
        }
        let holding: std::collections::BTreeSet<usize> = ids
            .iter()
            .map(|id| db.shard_index(&format!("_local/{id}")))
            .collect();
        assert!(holding.len() > 1, "the ids should not share one shard");

        db.checkpoint_put("ae2:peer", &json!({"seq": 3}))
            .await
            .unwrap();
        assert_eq!(
            db.checkpoint_get("ae2:peer").await.unwrap(),
            Some(json!({"seq": 3}))
        );
        assert_eq!(db.local_get("ae2:peer").await.unwrap(), None);

        let listing = db.local_docs(&AllDocsOpts::default()).await.unwrap().json();
        let listed: Vec<&str> = listing["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect();
        let expected: Vec<String> = ids.iter().map(|id| format!("_local/{id}")).collect();
        assert_eq!(listed, expected);

        // A copy keeps its revision and never replaces a newer one.
        let copy = json!({"_id": "_local/doc00", "_rev": "0-5", "v": "copy"});
        db.install_local_docs(vec![copy]).await;
        assert_eq!(db.local_get("doc00").await.unwrap().unwrap()["_rev"], "0-5");
        let stale = json!({"_id": "_local/doc00", "_rev": "0-2", "v": "stale"});
        db.install_local_docs(vec![stale]).await;
        assert_eq!(db.local_get("doc00").await.unwrap().unwrap()["v"], "copy");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// `_local_docs` rows serialize as `_all_docs` rows do, with null
    /// `total_rows` and `offset` and `not_found` rows for absent keys.
    #[tokio::test]
    async fn local_docs_serialize_in_wire_order() {
        let (db, root) = single_shard_db("local-wire").await;
        db.local_put("a", json!({"x": 1})).await.unwrap();
        db.local_put("b", json!({"y": [true, null]})).await.unwrap();
        let text = |listing: DbResult<_>| -> String {
            serde_json::to_string(&serde_json::to_value::<&_>(&listing.unwrap()).unwrap()).unwrap()
        };
        let plain = text(db.local_docs(&AllDocsOpts::default()).await);
        assert_eq!(
            plain,
            r#"{"total_rows":null,"offset":null,"rows":[{"id":"_local/a","key":"_local/a","value":{"rev":"0-1"}},{"id":"_local/b","key":"_local/b","value":{"rev":"0-1"}}]}"#
        );
        let keyed = text(
            db.local_docs(&AllDocsOpts {
                include_docs: true,
                keys: Some(vec![
                    json!("_local/b"),
                    json!("_local/zz"),
                    json!("_local/a"),
                ]),
                ..Default::default()
            })
            .await,
        );
        assert_eq!(
            keyed,
            r#"{"total_rows":null,"offset":null,"rows":[{"id":"_local/b","key":"_local/b","value":{"rev":"0-1"},"doc":{"y":[true,null],"_id":"_local/b","_rev":"0-1"}},{"key":"_local/zz","error":"not_found"},{"id":"_local/a","key":"_local/a","value":{"rev":"0-1"},"doc":{"x":1,"_id":"_local/a","_rev":"0-1"}}]}"#
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// `_bulk_get` answers one entry per request in request order: the
    /// document under `ok`, or the error with the revision that was asked for.
    #[tokio::test]
    async fn bulk_get_serializes_in_wire_order() {
        let (db, root) = single_shard_db("bulk-get-wire").await;
        let written = db
            .put_docs(vec![json!({"_id": "a", "n": 1})], true)
            .await
            .unwrap();
        let rev = written[0].json()["rev"].as_str().unwrap().to_string();
        let body = json!({"docs": [
            {"id": "a", "rev": rev},
            {"id": "nope"},
            {"id": "a", "rev": "1-00000000000000000000000000000000"},
            {"id": "a", "rev": "garbage"},
        ]});
        let got = db.bulk_get(&body, false, false).await.unwrap();
        let text = serde_json::to_string(&serde_json::to_value(&got).unwrap()).unwrap();
        assert_eq!(
            text,
            format!(
                r#"{{"results":[{{"id":"a","docs":[{{"ok":{{"_id":"a","_rev":"{rev}","n":1}}}}]}},{{"id":"nope","docs":[{{"error":{{"id":"nope","rev":"undefined","error":"not_found","reason":"missing"}}}}]}},{{"id":"a","docs":[{{"error":{{"id":"a","rev":"1-00000000000000000000000000000000","error":"not_found","reason":"missing"}}}}]}},{{"id":"a","docs":[{{"error":{{"id":"a","rev":"garbage","error":"not_found","reason":"missing"}}}}]}}]}}"#
            )
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// `put_docs` answers one entry per document in request order: `ok`
    /// entries, errors with or without the document id, `_local/` writes.
    #[tokio::test]
    async fn put_docs_serialize_in_wire_order() {
        let (db, root) = single_shard_db("put-wire").await;
        let text = |results: DbResult<_>| -> String {
            serde_json::to_string(&serde_json::to_value::<&_>(&results.unwrap()).unwrap()).unwrap()
        };
        let first = text(
            db.put_docs(
                vec![
                    json!({"_id": "a"}),
                    json!({"_id": "_bad"}),
                    json!({"_id": 5}),
                    json!({"_id": "_local/l"}),
                    json!({"_id": "_local/l", "_deleted": true}),
                    json!({"_id": "_local/gone", "_deleted": true}),
                ],
                true,
            )
            .await,
        );
        assert_eq!(
            first,
            r#"[{"ok":true,"id":"a","rev":"1-967a00dff5e02add41819138abb3284d"},{"error":"illegal_docid","reason":"Only reserved document ids may start with underscore."},{"error":"illegal_docid","reason":"Document id must be a string"},{"ok":true,"id":"_local/l","rev":"0-1"},{"ok":true,"id":"_local/l","rev":"0-0"},{"ok":true,"id":"_local/gone","rev":"0-0"}]"#
        );
        let second = text(
            db.put_docs(
                vec![
                    json!({"_id": "a"}),
                    json!({"_id": "_local/x", "_rev": "bad"}),
                ],
                true,
            )
            .await,
        );
        assert_eq!(
            second,
            r#"[{"id":"a","error":"conflict","reason":"Document update conflict."},{"id":"_local/x","error":"bad_request","reason":"Invalid rev format"}]"#
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// The changes feed serializes with `seq`, `id`, `changes`, then the
    /// optional `deleted` and `doc`, followed by `last_seq` and `pending`.
    #[tokio::test]
    async fn changes_serialize_in_wire_order() {
        let (db, root) = single_shard_db("changes-wire").await;
        let written = db
            .put_docs(
                vec![json!({"_id": "a", "n": 1}), json!({"_id": "b", "n": 2})],
                true,
            )
            .await
            .unwrap();
        let rev_b = written[1].json()["rev"].as_str().unwrap().to_string();
        db.put_docs(
            vec![json!({"_id": "b", "_rev": rev_b, "_deleted": true})],
            true,
        )
        .await
        .unwrap();
        let text = |changes: DbResult<_>| -> String {
            serde_json::to_string(&serde_json::to_value::<&_>(&changes.unwrap()).unwrap()).unwrap()
        };
        let token = |seq: u64| db.local_token(0, &[0], &ShardSince(vec![seq]));
        let plain = text(db.changes(0, &ChangesOpts::default(), true).await);
        assert_eq!(
            plain,
            format!(
                r#"{{"results":[{{"seq":"{}","id":"a","changes":[{{"rev":"1-731bef401491606a3b246ed178e697c1"}}]}},{{"seq":"{}","id":"b","changes":[{{"rev":"2-6b4cac76d0f72a9035d0e6b20c710a24"}}],"deleted":true}}],"last_seq":"{}","pending":0}}"#,
                token(1),
                token(3),
                token(3)
            )
        );
        let docs = text(
            db.changes(
                0,
                &ChangesOpts {
                    include_docs: true,
                    limit: Some(1),
                    ..Default::default()
                },
                true,
            )
            .await,
        );
        assert_eq!(
            docs,
            format!(
                r#"{{"results":[{{"seq":"{}","id":"a","changes":[{{"rev":"1-731bef401491606a3b246ed178e697c1"}}],"doc":{{"_id":"a","_rev":"1-731bef401491606a3b246ed178e697c1","n":1}}}}],"last_seq":"{}","pending":1}}"#,
                token(1),
                token(1)
            )
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A `_changes` filter runs user JavaScript, which must happen on the
    /// blocking pool; the JavaScript entry point asserts it is not inside an
    /// async task. Covers the ascending and descending paths.
    #[tokio::test]
    async fn changes_filters_run_javascript_off_the_async_workers() {
        let (db, root) = single_shard_db("changes-filter-blocking").await;
        db.put_docs(
            (0..5)
                .map(|n| json!({"_id": format!("d{n}"), "n": n}))
                .collect(),
            true,
        )
        .await
        .unwrap();
        let filter = ChangeFilter::Js {
            source: "function (doc, req) { return doc.n % 2 === 0; }".to_string(),
            query: "{}".to_string(),
        };
        let ids = |list: ChangesList| -> Vec<String> {
            list.results.into_iter().map(|row| row.id).collect()
        };
        let ascending = db
            .changes(
                0,
                &ChangesOpts {
                    filter: Some(filter.clone()),
                    include_docs: true,
                    ..Default::default()
                },
                true,
            )
            .await
            .unwrap();
        assert_eq!(ids(ascending), ["d0", "d2", "d4"]);
        let descending = db
            .changes(
                0,
                &ChangesOpts {
                    filter: Some(filter.clone()),
                    include_docs: true,
                    descending: true,
                    ..Default::default()
                },
                true,
            )
            .await
            .unwrap();
        assert_eq!(ids(descending), ["d4", "d2", "d0"]);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// `_all_docs` rows serialize as `id`, `key`, `value`, then `doc`; a keyed
    /// read answers `not_found` rows and `doc: null` for deleted documents.
    #[tokio::test]
    async fn all_docs_serialize_in_wire_order() {
        let (db, root) = single_shard_db("all-docs-wire").await;
        db.put_docs(
            vec![json!({"_id": "a", "n": 1}), json!({"_id": "b", "n": 2})],
            true,
        )
        .await
        .unwrap();
        let gone = db.get_doc("b", &DocOpts::default()).await.unwrap()["_rev"]
            .as_str()
            .unwrap()
            .to_string();
        db.put_docs(
            vec![json!({"_id": "b", "_rev": gone, "_deleted": true})],
            true,
        )
        .await
        .unwrap();
        let text = |listing: DbResult<_>| -> String {
            serde_json::to_string(&serde_json::to_value::<&_>(&listing.unwrap()).unwrap()).unwrap()
        };
        let plain = text(db.all_docs(&AllDocsOpts::default()).await);
        assert_eq!(
            plain,
            r#"{"total_rows":1,"offset":0,"rows":[{"id":"a","key":"a","value":{"rev":"1-731bef401491606a3b246ed178e697c1"}}]}"#
        );
        let with_docs = text(
            db.all_docs(&AllDocsOpts {
                include_docs: true,
                skip: 0,
                limit: 1,
                ..Default::default()
            })
            .await,
        );
        assert_eq!(
            with_docs,
            r#"{"total_rows":1,"offset":0,"rows":[{"id":"a","key":"a","value":{"rev":"1-731bef401491606a3b246ed178e697c1"},"doc":{"_id":"a","_rev":"1-731bef401491606a3b246ed178e697c1","n":1}}]}"#
        );
        let keyed = text(
            db.all_docs(&AllDocsOpts {
                include_docs: true,
                keys: Some(vec![json!("b"), json!("zz"), json!(7), json!("a")]),
                ..Default::default()
            })
            .await,
        );
        assert_eq!(
            keyed,
            r#"{"total_rows":1,"offset":null,"rows":[{"id":"b","key":"b","value":{"rev":"2-6b4cac76d0f72a9035d0e6b20c710a24","deleted":true},"doc":null},{"key":"zz","error":"not_found"},{"key":7,"error":"not_found"},{"id":"a","key":"a","value":{"rev":"1-731bef401491606a3b246ed178e697c1"},"doc":{"_id":"a","_rev":"1-731bef401491606a3b246ed178e697c1","n":1}}]}"#
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn a_corrupt_local_document_is_an_error_not_missing() {
        let (db, root) = single_shard_db("local-corrupt").await;
        db.shards[0]
            .call(|shard| {
                shard.put_raw(
                    crate::kv::LOCAL,
                    crate::kv::Table::Local,
                    "bad",
                    b"{not json",
                );
                Ok(())
            })
            .await
            .unwrap();
        let error = db.local_get("bad").await.unwrap_err();
        assert_eq!(error.status, 500, "{error}");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    fn row_ids(value: &Value, rows: &str) -> Vec<String> {
        value[rows]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap().to_string())
            .collect()
    }

    #[tokio::test]
    async fn a_corrupt_design_document_fails_writes_closed() {
        // Every write loads the validators first (`validate::validate_docs`),
        // and an error there is the write's 500.
        let (db, root) = single_shard_db("ddoc-ok").await;
        db.put_docs(vec![json!({"_id": "ok1", "n": 1})], true)
            .await
            .unwrap();
        assert!(db.validation_functions().await.unwrap().is_empty());
        drop(db);
        let _ = std::fs::remove_dir_all(root);

        // A design document that cannot be decoded must not be skipped:
        // its validate_doc_update would silently stop running.
        let (db, root) = single_shard_db("ddoc-corrupt").await;
        db.shards[0]
            .call(|shard| {
                shard.put_raw(
                    crate::kv::META,
                    crate::kv::Table::Meta,
                    "_design/v",
                    b"{not json",
                );
                Ok(())
            })
            .await
            .unwrap();
        let error = db.validation_functions().await.unwrap_err();
        assert_eq!(error.status, 500, "{error}");
        assert!(error.reason.contains("_design/v"), "{error}");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Finding the design documents used to build every shard's
    /// id-to-sequence index, which then stays in memory for the database's life.
    #[tokio::test]
    async fn loading_validators_does_not_build_the_changes_index() {
        let (db, root) = single_shard_db("ddoc-scan").await;
        db.put_docs(
            vec![
                json!({"_id": "_design/v", "validate_doc_update": "function(){}"}),
                json!({"_id": "plain", "n": 1}),
            ],
            true,
        )
        .await
        .unwrap();
        db.shards[0]
            .call(|shard| {
                shard.release_indexes();
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(db.validation_functions().await.unwrap().len(), 1);
        let built = db.shards[0]
            .call(|shard| Ok(shard.latest_built()))
            .await
            .unwrap();
        assert!(!built, "the validator scan built the changes index");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// The streamed `_all_docs` reply, assembled in the shape of `all_docs`.
    async fn streamed_all_docs(db: &Arc<Db>, opts: &AllDocsOpts) -> DocList {
        use futures::StreamExt;
        let (head, rows) = Db::all_docs_stream(db.clone(), opts).await.unwrap();
        let rows: Vec<DocListRow> = Box::pin(rows)
            .map(|row| row.unwrap())
            .collect::<Vec<_>>()
            .await;
        DocList {
            total_rows: Some(head.total_rows as u64),
            offset: Some(head.offset as u64),
            rows,
            update_seq: None,
        }
    }

    #[tokio::test]
    async fn streamed_all_docs_match_the_buffered_reply() {
        let root = std::env::temp_dir().join(format!("douchdb-stream-{}", crate::util::uuid()));
        let db = Arc::new(
            Db::open_or_create(
                &root,
                "stream",
                DbConfig {
                    q: 3,
                    revs_limit: 100,
                    cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                    shard_map: ShardMap::balanced(3, 1, 1),
                    partitioned: false,
                },
            )
            .unwrap(),
        );
        // More documents per shard than one page, so the merge refills.
        let docs: Vec<Value> = (0..900)
            .map(|n| json!({"_id": format!("doc{n:04}"), "n": n, "text": "x".repeat(n % 7)}))
            .collect();
        let put = db.put_docs(docs, true).await.unwrap();
        // One corrupt body, which include_docs skips.
        let bad_id = "doc0450";
        let bad_rev = put[450].json()["rev"].as_str().unwrap().to_string();
        let shard = db.shard_index(bad_id);
        db.shards[shard]
            .call(move |s| {
                s.put_raw(
                    crate::kv::BODIES,
                    crate::kv::Table::Bodies,
                    (bad_id, bad_rev.as_str()),
                    b"{not json",
                );
                Ok(())
            })
            .await
            .unwrap();
        // A deleted document does not count.
        let gone = put[10].json()["rev"].as_str().unwrap().to_string();
        db.put_docs(
            vec![json!({"_id": "doc0010", "_rev": gone, "_deleted": true})],
            true,
        )
        .await
        .unwrap();

        let base = AllDocsOpts::default;
        let shapes: Vec<(&str, AllDocsOpts)> = vec![
            ("all", base()),
            (
                "include_docs",
                AllDocsOpts {
                    include_docs: true,
                    ..base()
                },
            ),
            (
                "descending",
                AllDocsOpts {
                    descending: true,
                    ..base()
                },
            ),
            (
                "descending include_docs",
                AllDocsOpts {
                    descending: true,
                    include_docs: true,
                    ..base()
                },
            ),
            (
                "range",
                AllDocsOpts {
                    startkey: Some("doc0100".into()),
                    endkey: Some("doc0600".into()),
                    ..base()
                },
            ),
            (
                "range exclusive end",
                AllDocsOpts {
                    startkey: Some("doc0100".into()),
                    endkey: Some("doc0600".into()),
                    inclusive_end: false,
                    ..base()
                },
            ),
            (
                "descending range",
                AllDocsOpts {
                    startkey: Some("doc0600".into()),
                    endkey: Some("doc0100".into()),
                    descending: true,
                    include_docs: true,
                    ..base()
                },
            ),
            (
                "inverted range",
                AllDocsOpts {
                    startkey: Some("doc0600".into()),
                    endkey: Some("doc0100".into()),
                    ..base()
                },
            ),
            (
                "start between ids",
                AllDocsOpts {
                    startkey: Some("doc03505".into()),
                    ..base()
                },
            ),
            (
                "skip and limit",
                AllDocsOpts {
                    skip: 301,
                    limit: 400,
                    ..base()
                },
            ),
            (
                "skip over unread documents, then read some",
                AllDocsOpts {
                    skip: 301,
                    limit: 400,
                    include_docs: true,
                    ..base()
                },
            ),
            (
                "descending skip over unread documents",
                AllDocsOpts {
                    skip: 300,
                    limit: 300,
                    include_docs: true,
                    descending: true,
                    ..base()
                },
            ),
            (
                "skip and limit with docs",
                AllDocsOpts {
                    skip: 7,
                    limit: 5,
                    include_docs: true,
                    descending: true,
                    ..base()
                },
            ),
            ("limit zero", AllDocsOpts { limit: 0, ..base() }),
            (
                "skip past the end",
                AllDocsOpts {
                    skip: 5000,
                    ..base()
                },
            ),
            (
                "one row",
                AllDocsOpts {
                    startkey: Some("doc0777".into()),
                    endkey: Some("doc0777".into()),
                    include_docs: true,
                    ..base()
                },
            ),
            (
                "no rows",
                AllDocsOpts {
                    startkey: Some("zzz".into()),
                    ..base()
                },
            ),
        ];
        for (name, opts) in shapes {
            let buffered = db.all_docs(&opts).await.unwrap();
            let streamed = streamed_all_docs(&db, &opts).await;
            assert_eq!(
                serde_json::to_string(&streamed).unwrap(),
                serde_json::to_string(&buffered).unwrap(),
                "{name}"
            );
        }

        let empty_root =
            std::env::temp_dir().join(format!("douchdb-stream-empty-{}", crate::util::uuid()));
        let empty = Arc::new(
            Db::open_or_create(
                &empty_root,
                "empty",
                DbConfig {
                    q: 3,
                    revs_limit: 100,
                    cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                    shard_map: ShardMap::balanced(3, 1, 1),
                    partitioned: false,
                },
            )
            .unwrap(),
        );
        let opts = AllDocsOpts::default();
        assert_eq!(
            streamed_all_docs(&empty, &opts).await.json(),
            empty.all_docs(&opts).await.unwrap().json()
        );
        drop(db);
        drop(empty);
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(empty_root);
    }

    #[tokio::test]
    async fn listings_skip_corrupt_records_and_point_reads_fail() {
        let (db, root) = single_shard_db("listing-corrupt").await;
        // Plant the corrupt metadata before anything builds the shard's
        // indexes, so the first scan meets it.
        db.shards[0]
            .call(|shard| {
                shard.put_raw(
                    crate::kv::META,
                    crate::kv::Table::Meta,
                    "bad-meta",
                    b"{not json",
                );
                Ok(())
            })
            .await
            .unwrap();
        let put = db
            .put_docs(
                vec![
                    json!({"_id": "a", "n": 1}),
                    json!({"_id": "bad-body", "n": 2}),
                    json!({"_id": "c", "n": 3}),
                ],
                true,
            )
            .await
            .unwrap();
        let bad_rev = put[1].json()["rev"].as_str().unwrap().to_string();
        let corrupt_rev = bad_rev.clone();
        db.shards[0]
            .call(move |shard| {
                shard.put_raw(
                    crate::kv::BODIES,
                    crate::kv::Table::Bodies,
                    ("bad-body", corrupt_rev.as_str()),
                    b"{not json",
                );
                Ok(())
            })
            .await
            .unwrap();

        let before = crate::metrics::corrupt_records_skipped();
        let all = db.all_docs(&AllDocsOpts::default()).await.unwrap();
        assert_eq!(row_ids(&all.json(), "rows"), ["a", "bad-body", "c"]);
        assert!(crate::metrics::corrupt_records_skipped() > before);

        let before = crate::metrics::corrupt_records_skipped();
        let all = db
            .all_docs(&AllDocsOpts {
                include_docs: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(row_ids(&all.json(), "rows"), ["a", "c"]);
        assert!(
            crate::metrics::corrupt_records_skipped() > before,
            "the skipped body was not counted"
        );

        let changes = db.changes(0, &ChangesOpts::default(), true).await.unwrap();
        assert_eq!(
            row_ids(&serde_json::to_value(&changes).unwrap(), "results"),
            ["a", "bad-body", "c"]
        );
        let before = crate::metrics::corrupt_records_skipped();
        let changes = db
            .changes(
                0,
                &ChangesOpts {
                    include_docs: true,
                    ..Default::default()
                },
                true,
            )
            .await
            .unwrap();
        assert_eq!(
            row_ids(&serde_json::to_value(&changes).unwrap(), "results"),
            ["a", "c"]
        );
        assert!(crate::metrics::corrupt_records_skipped() > before);

        // View and search builds index every readable document.
        let docs = db.map_docs(None).await.unwrap();
        let ids: Vec<&str> = docs.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, ["a", "c"]);

        // A point read of either document still fails.
        for id in ["bad-meta", "bad-body"] {
            let error = db.get_doc(id, &DocOpts::default()).await.unwrap_err();
            assert_eq!(error.status, 500, "{id}: {error}");
        }
        let open_revs = DocOpts {
            open_revs: Some(vec![bad_rev.clone()]),
            ..Default::default()
        };
        let error = db.get_doc("bad-body", &open_revs).await.unwrap_err();
        assert_eq!(error.status, 500, "{error}");
        let rev = DocOpts {
            rev: Some(bad_rev),
            ..Default::default()
        };
        let error = db.get_doc("bad-body", &rev).await.unwrap_err();
        assert_eq!(error.status, 500, "{error}");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn a_failed_shard_write_still_invalidates_the_validator_cache() {
        let root = std::env::temp_dir().join(format!("douchdb-valfail-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "valfail",
            DbConfig {
                q: 2,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: ShardMap::balanced(2, 1, 1),
                partitioned: false,
            },
        )
        .unwrap();
        let design_id = (0..10_000)
            .map(|i| format!("_design/d{i}"))
            .find(|id| db.shard_index(id) == 0)
            .expect("design id on shard 0");
        let plain_id = doc_id_for_shard(&db, 1);

        // Warm the cache with the empty validator set.
        assert!(db.validation_functions().await.unwrap().is_empty());
        let generation = db.design_generation();

        db.shards[1]
            .call(|shard| {
                shard.inject_commit_failure();
                Ok(())
            })
            .await
            .unwrap();
        let result = db
            .put_docs(
                vec![
                    json!({"_id": design_id, "validate_doc_update": "function(){}"}),
                    json!({"_id": plain_id}),
                ],
                true,
            )
            .await;
        assert!(result.is_err(), "the second shard's write fails");

        // Shard 0 committed the design document before shard 1 failed.
        assert_ne!(db.design_generation(), generation);
        assert_eq!(db.validation_functions().await.unwrap().len(), 1);
        drop(db);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A database of one shard, moved onto a test disk holding the same
    /// settings, so its commits can be made to fail.
    async fn db_on_test_disk(
        tag: &str,
    ) -> (Db, std::path::PathBuf, crate::kv::powercut::PowerSwitch) {
        use crate::store::{Shard, ShardHandle};
        let root = std::env::temp_dir().join(format!("douchdb-{tag}-{}", crate::util::uuid()));
        let mut db = Db::open_or_create(
            &root,
            tag,
            DbConfig {
                q: 1,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        let rows = stored_rows(&db).await;
        let (_, switch) = crate::kv::powercut::PowerCut::new();
        drop(Shard::create_over_backend(&switch, 1000).unwrap());
        let disk = ShardHandle::over_backend(&switch).unwrap();
        disk.call(move |s| {
            for (name, value) in &rows {
                s.put_setting(name, value)?;
            }
            Ok(())
        })
        .await
        .unwrap();
        db.shards[0] = disk;
        (db, root, switch)
    }

    /// The `db/` settings shard 0 holds, read through its actor.
    async fn stored_rows(db: &Db) -> Vec<(String, Vec<u8>)> {
        db.shards[0]
            .read(|s| {
                let mut rows = Vec::new();
                for name in [
                    INFO_SETTING,
                    SHARD_MAP_SETTING,
                    SECURITY_SETTING,
                    PENDING_SHARD_MAP_SETTING,
                ] {
                    if let Some(value) = s.get_setting(name)? {
                        rows.push((name.to_string(), value));
                    }
                }
                Ok(rows)
            })
            .await
            .unwrap()
    }

    /// A settings write whose commit reports an error yet lands (its pages
    /// and header reach the disk; only the sync of the header fails) is
    /// still an error to the client, but memory holds what the recovered
    /// shard holds, so a later versioned write orders against it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_settings_write_that_landed_is_what_memory_holds() {
        let (db, root, switch) = db_on_test_disk("landed").await;
        let old = json!({"members": {"names": ["m"]}});
        let landed = json!({"members": {"names": ["z"]}});
        let version = db.set_security(old.clone()).await.unwrap();
        assert_eq!(db.security_versioned(), (version, old));

        // The commit's pages sync; the sync of its header fails, once.
        switch.fail_one_sync_after(1, std::io::ErrorKind::Other);
        let error = db.set_security(landed.clone()).await.unwrap_err();
        assert!(error.reason.starts_with("shard commit failed"), "{error:?}");

        let on_disk = Stored::parse(&root, stored_rows(&db).await)
            .unwrap()
            .security;
        assert_eq!(on_disk, (version + 1, landed.clone()), "the write landed");
        assert_eq!(db.security_versioned(), on_disk);

        // Ordered against the landed object: an equal version with a lesser
        // object loses. Against the old one it would be newer, and stored.
        let lesser = json!({"members": {"names": ["a"]}});
        assert!(!db
            .set_security_versioned(version + 1, lesser)
            .await
            .unwrap());
        assert_eq!(db.security_versioned(), (version + 1, landed));
        drop(db);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A shard map whose write fails yet lands is the one in memory, here
    /// one for another cluster size than the map it replaces.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_shard_map_write_that_landed_is_what_memory_holds() {
        let (db, root, switch) = db_on_test_disk("landedmap").await;
        assert_eq!(db.shard_map().node_count, 1);

        switch.fail_one_sync_after(1, std::io::ErrorKind::Other);
        let error = db
            .set_next_shard_map(ShardMap::balanced(1, 2, 1))
            .await
            .unwrap_err();
        assert!(error.reason.starts_with("shard commit failed"), "{error:?}");

        let on_disk = Stored::parse(&root, stored_rows(&db).await)
            .unwrap()
            .shard_map;
        assert_eq!(on_disk.node_count, 2, "the write landed");
        assert_eq!(db.shard_map(), on_disk);
        drop(db);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A failed settings write whose shard cannot recover leaves memory with
    /// what clients were last told while nothing can be read back. Once a
    /// retry brings the shard back, memory holds what the disk holds: here
    /// the write, which landed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_settings_write_is_read_back_once_its_shard_serves_again() {
        let (db, root, switch) = db_on_test_disk("unread").await;
        let old = json!({"members": {"names": ["m"]}});
        let landed = json!({"members": {"names": ["z"]}});
        let version = db.set_security(old.clone()).await.unwrap();

        // Every sync from the header's on fails, so the reopen fails too.
        switch.fail_syncs_after(1, std::io::ErrorKind::Other);
        let error = db.set_security(landed.clone()).await.unwrap_err();
        assert!(error.reason.starts_with("shard commit failed"), "{error:?}");
        assert!(
            matches!(db.shards[0].state(), crate::store::ShardState::Failed(_)),
            "{:?}",
            db.shards[0].state()
        );
        assert_eq!(db.security_versioned(), (version, old));

        // The fault clears and a request has shard 0 retry.
        switch.restore_writes();
        db.shards[0].set_retry_wait(std::time::Duration::ZERO);
        assert!(db.shards[0].read(|_| Ok(())).await.is_err());
        db.shards[0].barrier().await.unwrap().unwrap();
        assert_eq!(db.shards[0].state(), crate::store::ShardState::Serving);
        let on_disk = Stored::parse(&root, stored_rows(&db).await)
            .unwrap()
            .security;
        assert_eq!(on_disk, (version + 1, landed), "the write landed");
        assert_eq!(db.security_versioned(), on_disk);
        drop(db);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Whether shard 0 holds a pending shard map, read through its actor.
    async fn pending_map_on_disk(db: &Db) -> bool {
        stored_rows(db)
            .await
            .iter()
            .any(|(name, _)| name == PENDING_SHARD_MAP_SETTING)
    }

    /// A clear with no pending map in memory still removes one on disk.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clearing_the_pending_shard_map_removes_one_memory_does_not_hold() {
        let (db, root, _switch) = db_on_test_disk("clearondisk").await;
        let map = to_json(&ShardMap::single(1)).unwrap();
        db.shards[0]
            .call(move |s| Ok(s.put_setting(PENDING_SHARD_MAP_SETTING, &map)?))
            .await
            .unwrap();
        assert!(db.pending_shard_map().is_none());
        assert!(pending_map_on_disk(&db).await);

        db.clear_pending_shard_map().await.unwrap();
        assert!(db.pending_shard_map().is_none());
        assert!(!pending_map_on_disk(&db).await, "the clear left it on disk");
        drop(db);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A pending map whose write fails yet lands while shard 0 fails is in
    /// neither memory nor reach until the shard serves again. A clear
    /// meanwhile cannot reach the disk, so it is an error, not an ok over a
    /// map that the parked read back then installs; once the shard serves,
    /// a clear removes it from memory and disk.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clearing_a_pending_shard_map_that_landed_while_shard_0_failed() {
        let (db, root, switch) = db_on_test_disk("clearlanded").await;
        // Every sync from the header's on fails, so the reopen fails too.
        switch.fail_syncs_after(1, std::io::ErrorKind::Other);
        let error = db
            .set_pending_shard_map(&ShardMap::single(1))
            .await
            .unwrap_err();
        assert!(error.reason.starts_with("shard commit failed"), "{error:?}");
        assert!(
            matches!(db.shards[0].state(), crate::store::ShardState::Failed(_)),
            "{:?}",
            db.shards[0].state()
        );
        assert!(db.pending_shard_map().is_none());

        assert!(
            db.clear_pending_shard_map().await.is_err(),
            "a clear that wrote nothing answered ok"
        );

        // The fault clears and a request has shard 0 retry: the landed map
        // is read back.
        switch.restore_writes();
        db.shards[0].set_retry_wait(std::time::Duration::ZERO);
        assert!(db.shards[0].read(|_| Ok(())).await.is_err());
        db.shards[0].barrier().await.unwrap().unwrap();
        assert_eq!(db.shards[0].state(), crate::store::ShardState::Serving);
        assert!(pending_map_on_disk(&db).await, "the write landed");
        assert_eq!(db.pending_shard_map(), Some(ShardMap::single(1)));

        db.clear_pending_shard_map().await.unwrap();
        assert!(db.pending_shard_map().is_none());
        assert!(!pending_map_on_disk(&db).await);
        drop(db);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A settings write that lands on shard 0 while it fails, its read back
    /// parked, and `fault` armed on the disk once the next retry has
    /// reopened the file, before the read back runs. The objects are larger
    /// than the page cache, so reading one back reaches the disk. Returns
    /// the database, its root, the disk, and the version and object before
    /// the write and the object written.
    async fn landed_while_failed_then_fault(
        tag: &str,
        fault: impl FnOnce(&crate::kv::powercut::PowerSwitch) + Send + 'static,
    ) -> (
        Db,
        std::path::PathBuf,
        crate::kv::powercut::PowerSwitch,
        u64,
        Value,
        Value,
    ) {
        let (db, root, switch) = db_on_test_disk(tag).await;
        let old = json!({"members": {"names": ["m".repeat(4 << 20)]}});
        let landed = json!({"members": {"names": ["z".repeat(4 << 20)]}});
        let before = switch.syncs();
        let version = db.set_security(old.clone()).await.unwrap();
        // The commit's syncs; the last writes its header.
        let syncs = switch.syncs() - before;

        let disk = switch.clone();
        db.shards[0]
            .alone(move |shard| {
                shard.on_next_reopen(move || fault(&disk));
                Ok(())
            })
            .await
            .unwrap();
        switch.fail_syncs_after(syncs - 1, std::io::ErrorKind::Other);
        assert!(db.set_security(landed.clone()).await.is_err());
        assert!(
            matches!(db.shards[0].state(), crate::store::ShardState::Failed(_)),
            "{:?}",
            db.shards[0].state()
        );
        switch.restore_writes();
        db.shards[0].set_retry_wait(std::time::Duration::ZERO);
        (db, root, switch, version, old, landed)
    }

    /// A parked read back whose own read meets an I/O failure, failing
    /// shard 0 again, is read again after the next retry, so memory still
    /// comes to hold the write that landed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_parked_read_back_whose_read_fails_the_shard_again_is_read_again() {
        let (db, root, switch, version, old, landed) =
            landed_while_failed_then_fault("rereadback", |disk| {
                disk.fail_reads(std::io::ErrorKind::Other)
            })
            .await;

        // The retry reopens the file; the read back's read fails, and so
        // does the reopen after it.
        assert!(db.shards[0].read(|_| Ok(())).await.is_err());
        assert!(db.shards[0].barrier().await.unwrap().is_err());
        assert!(
            matches!(db.shards[0].state(), crate::store::ShardState::Failed(_)),
            "{:?}",
            db.shards[0].state()
        );
        assert_eq!(db.security_versioned(), (version, old));

        // The next retry reads it back.
        switch.restore_reads();
        assert!(db.shards[0].read(|_| Ok(())).await.is_err());
        db.shards[0].barrier().await.unwrap().unwrap();
        assert_eq!(db.shards[0].state(), crate::store::ShardState::Serving);
        let on_disk = Stored::parse(&root, stored_rows(&db).await)
            .unwrap()
            .security;
        assert_eq!(on_disk, (version + 1, landed), "the write landed");
        assert_eq!(db.security_versioned(), on_disk);
        drop(db);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A parked read back whose own read meets a passing I/O failure: the
    /// shard reopens and serves, and the read back runs again once the
    /// retry wait is over and a request arrives, so memory comes to hold
    /// the write that landed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_parked_read_back_whose_read_meets_a_passing_fault_is_read_again() {
        let (db, root, _switch, version, old, landed) =
            landed_while_failed_then_fault("passingreadback", |disk| {
                disk.fail_one_read(std::io::ErrorKind::Other)
            })
            .await;

        // The retry reopens the file; the read back's read fails, and the
        // reopen after it succeeds.
        assert!(db.shards[0].read(|_| Ok(())).await.is_err());
        db.shards[0].barrier().await.unwrap().unwrap();
        assert_eq!(db.shards[0].state(), crate::store::ShardState::Serving);
        assert_eq!(db.security_versioned(), (version, old));

        // The wait (none here) is over: a request has the read back run.
        db.shards[0].read(|_| Ok(())).await.unwrap();
        db.shards[0].barrier().await.unwrap().unwrap();
        let on_disk = Stored::parse(&root, stored_rows(&db).await)
            .unwrap()
            .security;
        assert_eq!(on_disk, (version + 1, landed), "the write landed");
        assert_eq!(db.security_versioned(), on_disk);
        drop(db);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Shard 0 failed to recover after a settings write that never reached
    /// the disk: once a retry brings it back, memory still holds what it
    /// held, which is what the disk holds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_settings_write_that_did_not_land_leaves_memory_once_its_shard_serves() {
        let (db, root, switch) = db_on_test_disk("unlanded").await;
        let old = json!({"members": {"names": ["m"]}});
        let version = db.set_security(old.clone()).await.unwrap();

        // A full disk: the write fails, and so does the reopen after it.
        switch.fail_writes(std::io::ErrorKind::StorageFull);
        assert!(db
            .set_security(json!({"members": {"names": ["z"]}}))
            .await
            .is_err());
        assert!(
            matches!(db.shards[0].state(), crate::store::ShardState::Failed(_)),
            "{:?}",
            db.shards[0].state()
        );

        switch.restore_writes();
        db.shards[0].set_retry_wait(std::time::Duration::ZERO);
        assert!(db.shards[0].read(|_| Ok(())).await.is_err());
        db.shards[0].barrier().await.unwrap().unwrap();
        assert_eq!(db.shards[0].state(), crate::store::ShardState::Serving);
        let on_disk = Stored::parse(&root, stored_rows(&db).await)
            .unwrap()
            .security;
        assert_eq!(on_disk, (version, old));
        assert_eq!(db.security_versioned(), on_disk);
        drop(db);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Two settings writes fail while shard 0 cannot recover: the first
    /// landed, the second was refused. Once shard 0 serves again, memory
    /// holds the one that landed, never the refused one, and a setting
    /// written after that is not overwritten.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn parked_settings_read_backs_install_in_write_order() {
        let (db, root, switch) = db_on_test_disk("parkedorder").await;
        let old = json!({"members": {"names": ["m"]}});
        let landed = json!({"members": {"names": ["x"]}});
        let version = db.set_security(old.clone()).await.unwrap();

        switch.fail_syncs_after(1, std::io::ErrorKind::Other);
        assert!(db.set_security(landed.clone()).await.is_err());
        assert!(
            matches!(db.shards[0].state(), crate::store::ShardState::Failed(_)),
            "{:?}",
            db.shards[0].state()
        );
        assert!(db
            .set_security(json!({"members": {"names": ["y"]}}))
            .await
            .is_err());
        assert_eq!(db.security_versioned(), (version, old));

        switch.restore_writes();
        db.shards[0].set_retry_wait(std::time::Duration::ZERO);
        assert!(db.shards[0].read(|_| Ok(())).await.is_err());
        db.shards[0].barrier().await.unwrap().unwrap();
        assert_eq!(db.shards[0].state(), crate::store::ShardState::Serving);
        let on_disk = Stored::parse(&root, stored_rows(&db).await)
            .unwrap()
            .security;
        assert_eq!(on_disk, (version + 1, landed));
        assert_eq!(db.security_versioned(), on_disk);

        let later = json!({"members": {"names": ["w"]}});
        let written = db.set_security(later.clone()).await.unwrap();
        db.shards[0].barrier().await.unwrap().unwrap();
        let on_disk = Stored::parse(&root, stored_rows(&db).await)
            .unwrap()
            .security;
        assert_eq!(on_disk, (written, later));
        assert_eq!(db.security_versioned(), on_disk);
        drop(db);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A request's shards commit together: shard 1 commits while shard 0's
    /// writer is held.
    #[tokio::test]
    async fn shards_of_one_request_commit_concurrently() {
        let (db, root) = two_shard_db("concurrent");
        let db = Arc::new(db);
        let (started_tx, started) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let holding = tokio::spawn({
            let db = Arc::clone(&db);
            async move {
                db.shards[0]
                    .alone(move |_| {
                        let _ = started_tx.send(());
                        let _ = held.recv();
                        Ok(())
                    })
                    .await
            }
        });
        started.await.unwrap();

        let ids = [doc_id_for_shard(&db, 0), doc_id_for_shard(&db, 1)];
        let request = tokio::spawn({
            let db = Arc::clone(&db);
            let docs = ids.iter().map(|id| json!({"_id": id})).collect::<Vec<_>>();
            async move { db.put_docs(docs, true).await }
        });
        while db.shards[1].committed_seq() == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(db.shards[0].committed_seq(), 0, "shard 0 is held");

        release.send(()).unwrap();
        holding.await.unwrap().unwrap();
        let out = request.await.unwrap().unwrap();
        for (result, id) in out.iter().zip(&ids) {
            let result = result.json();
            assert_eq!(result["ok"], true, "{result}");
            assert_eq!(result["id"], id.as_str(), "{result}");
        }
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Every shard of a request answers before the request does: a failing
    /// shard 0 fails the request with its own error, shard 1's documents
    /// still commit, and the validator cache is dropped.
    #[tokio::test]
    async fn one_failing_shard_fails_the_request_after_both_answer() {
        let (db, root) = two_shard_db("onefails");
        let design_id = (0..10_000)
            .map(|i| format!("_design/d{i}"))
            .find(|id| db.shard_index(id) == 1)
            .expect("design id on shard 1");
        let failing_id = doc_id_for_shard(&db, 0);
        let plain_id = doc_id_for_shard(&db, 1);
        assert!(db.validation_functions().await.unwrap().is_empty());
        let generation = db.design_generation();

        db.shards[0]
            .call(|shard| {
                shard.inject_commit_failure();
                Ok(())
            })
            .await
            .unwrap();
        let error = db
            .put_docs(
                vec![
                    json!({"_id": failing_id}),
                    json!({"_id": design_id, "validate_doc_update": "function(){}"}),
                    json!({"_id": plain_id}),
                ],
                true,
            )
            .await
            .unwrap_err();
        assert_eq!(error.status, 500, "{error:?}");
        assert_eq!(
            error.reason, "shard commit failed: injected commit failure",
            "{error:?}"
        );

        let opts = DocOpts::default();
        assert!(db.get_doc(&failing_id, &opts).await.is_err());
        db.get_doc(&plain_id, &opts).await.unwrap();
        db.get_doc(&design_id, &opts).await.unwrap();
        assert_ne!(db.design_generation(), generation);
        assert_eq!(db.validation_functions().await.unwrap().len(), 1);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn security_versions_order_updates_and_survive_reopen() {
        let root = std::env::temp_dir().join(format!("douchdb-secver-{}", crate::util::uuid()));
        let config = || DbConfig {
            q: 1,
            revs_limit: 100,
            cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
            shard_map: ShardMap::balanced(1, 1, 1),
            partitioned: false,
        };
        let db = Db::open_or_create(&root, "sec", config()).unwrap();
        assert_eq!(db.security_versioned(), (0, json!({})));

        assert_eq!(db.set_security(json!({"a": 1})).await.unwrap(), 1);
        assert!(db.set_security_versioned(3, json!({"a": 3})).await.unwrap());
        // Older versions are rejected; equal ones are ordered by canonical form.
        assert!(!db.set_security_versioned(2, json!({"a": 2})).await.unwrap());
        assert!(!db.set_security_versioned(3, json!({"a": 2})).await.unwrap());
        assert!(db.set_security_versioned(3, json!({"a": 4})).await.unwrap());
        assert!(db.set_security_versioned(3, json!({"a": 4})).await.unwrap());
        assert_eq!(db.security(), json!({"a": 4}));
        drop(db);

        let db = Db::open(&root, "sec", config()).unwrap();
        assert_eq!(db.security_versioned(), (3, json!({"a": 4})));
        drop(db);
        let _ = std::fs::remove_dir_all(&root);
    }

    fn meta_config() -> DbConfig {
        DbConfig {
            q: 1,
            revs_limit: 100,
            cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
            shard_map: ShardMap::balanced(1, 1, 1),
            partitioned: false,
        }
    }

    /// Every database setting but the pending map is written when the
    /// database is created, so one that is missing or does not parse is
    /// damage: the open fails with a 500 naming it, never falls back to a
    /// default (an empty security object would make the database public),
    /// and the file is left as it is.
    #[test]
    fn a_missing_or_unusable_database_setting_fails_open_with_a_server_error() {
        let broken: [(&str, Option<&[u8]>); 7] = [
            (INFO_SETTING, None),
            (INFO_SETTING, Some(b"{ not json")),
            (SHARD_MAP_SETTING, None),
            (
                SHARD_MAP_SETTING,
                Some(br#"{"q":1,"node_count":1,"replicas":[]}"#),
            ),
            (SECURITY_SETTING, None),
            (SECURITY_SETTING, Some(br#"{"version":1}"#)),
            (PENDING_SHARD_MAP_SETTING, Some(b"{ not json")),
        ];
        for (setting, value) in broken {
            let root =
                std::env::temp_dir().join(format!("douchdb-badsetting-{}", crate::util::uuid()));
            drop(Db::open_or_create(&root, "meta", meta_config()).unwrap());
            let file = shard_file(&db_path(&root, "meta"), 0);
            let db = crate::kv::open(&file, 1 << 20).unwrap();
            let txn = crate::kv::begin_write(&db).unwrap();
            {
                let mut table = txn.open_table(crate::kv::SETTINGS).unwrap();
                match value {
                    Some(bytes) => {
                        table
                            .insert(setting, crate::kv::seal(bytes).as_slice())
                            .unwrap();
                    }
                    None => {
                        table.remove(setting).unwrap();
                    }
                }
            }
            txn.commit().unwrap();
            drop(db);

            let error = match Db::open(&root, "meta", meta_config()) {
                Ok(_) => panic!("{setting} = {value:?} opened"),
                Err(error) => error,
            };
            assert_eq!(error.status, 500, "{setting}: {}", error.reason);
            assert!(
                error.reason.contains(setting),
                "{setting}: {}",
                error.reason
            );
            let closed = Db::read_security(&root, "meta").map(|_| ());
            assert_eq!(closed.unwrap_err().status, 500, "{setting} read closed");
            // Left as it was for the operator, not repaired or defaulted.
            let db = crate::kv::open(&file, 1 << 20).unwrap();
            let left = crate::store::read_settings(&db, setting).unwrap();
            let left = left.into_iter().find(|(name, _)| name == setting);
            assert_eq!(left.map(|(_, bytes)| bytes).as_deref(), value, "{setting}");
            drop(db);
            crate::kv::forget_verified(&file);
            std::fs::remove_dir_all(root).ok();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_equal_version_shard_maps_leave_disk_and_memory_in_step() {
        let root = std::env::temp_dir().join(format!("douchdb-mapraced-{}", crate::util::uuid()));
        let config = || DbConfig {
            q: 2,
            revs_limit: 100,
            cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
            shard_map: ShardMap::balanced(2, 3, 1),
            partitioned: false,
        };
        for round in 0..40 {
            let name = format!("race{round}");
            let db = Arc::new(Db::open_or_create(&root, &name, config()).unwrap());
            let barrier = Arc::new(tokio::sync::Barrier::new(3));
            let racers: Vec<_> = (0..3)
                .map(|node| {
                    let (db, barrier) = (db.clone(), barrier.clone());
                    tokio::spawn(async move {
                        barrier.wait().await;
                        db.set_shard_map(ShardMap {
                            q: 2,
                            node_count: 3,
                            replicas: vec![vec![node], vec![node]],
                            version: 9,
                        })
                        .await
                        .unwrap();
                    })
                })
                .collect();
            for racer in racers {
                racer.await.unwrap();
            }
            // The tiebreak winner is the greatest placement, and what is
            // stored is what is in memory.
            let in_memory = db.shard_map();
            assert_eq!(in_memory.replicas, vec![vec![2], vec![2]], "round {round}");
            drop(db);
            let stored = Db::read_closed_meta(&root, &name, 3).unwrap().unwrap();
            assert_eq!(stored.shard_map, in_memory, "round {round}");
        }
        std::fs::remove_dir_all(root).ok();
    }

    fn names_in(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    /// Temporary files an earlier process with this pid left (a container
    /// runs the server as pid 1 every time) under the next names do not
    /// make a write fail, and are neither written over nor removed.
    #[test]
    fn write_atomic_skips_temporary_names_already_taken() {
        let dir = std::env::temp_dir().join(format!("douchdb-taken-{}", crate::util::uuid()));
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("meta.json");
        let next = TEMPS.load(Ordering::Relaxed);
        let taken: Vec<String> = (next..next + 64)
            .map(|n| format!(".meta.json.{}.{n}.tmp", std::process::id()))
            .collect();
        for name in &taken {
            std::fs::write(dir.join(name), b"leftover").unwrap();
        }
        write_atomic(&target, b"new").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"new");
        for name in &taken {
            assert_eq!(
                std::fs::read(dir.join(name)).unwrap(),
                b"leftover",
                "{name}"
            );
        }
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn write_atomic_replaces_whole_and_leaves_no_temporary_file() {
        let dir = std::env::temp_dir().join(format!("douchdb-atomic-{}", crate::util::uuid()));
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("meta.json");
        write_atomic(&target, b"first").unwrap();
        write_atomic(&target, b"second").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"second");
        assert_eq!(names_in(&dir), ["meta.json"]);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn the_pending_shard_map_is_stored_and_cleared_durably() {
        let root = std::env::temp_dir().join(format!("douchdb-pending-{}", crate::util::uuid()));
        let config = || DbConfig {
            q: 1,
            revs_limit: 100,
            cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
            shard_map: ShardMap::single(1),
            partitioned: false,
        };
        let db = Db::open_or_create(&root, "pending", config()).unwrap();
        // Nothing pending is not an error.
        db.clear_pending_shard_map().await.unwrap();
        assert!(db.pending_shard_map().is_none());

        db.set_pending_shard_map(&ShardMap::single(1))
            .await
            .unwrap();
        assert_eq!(db.pending_shard_map(), Some(ShardMap::single(1)));
        drop(db);
        let closed = Db::read_closed_meta(&root, "pending", 1).unwrap().unwrap();
        assert!(closed.pending_shard_map);

        let db = Db::open(&root, "pending", config()).unwrap();
        assert_eq!(db.pending_shard_map(), Some(ShardMap::single(1)));
        db.clear_pending_shard_map().await.unwrap();
        assert!(db.pending_shard_map().is_none());
        drop(db);
        let closed = Db::read_closed_meta(&root, "pending", 1).unwrap().unwrap();
        assert!(!closed.pending_shard_map);
        std::fs::remove_dir_all(root).ok();
    }

    /// A shard of a database whose shard 0 is cleared (it moved elsewhere)
    /// keeps the database's own settings, which live beside its own.
    #[tokio::test]
    async fn clearing_shard_0_keeps_the_database_settings() {
        let root = std::env::temp_dir().join(format!("douchdb-clear0-{}", crate::util::uuid()));
        let db = Db::open_or_create(&root, "clear0", meta_config()).unwrap();
        db.set_security(json!({"members": {"names": ["a"]}}))
            .await
            .unwrap();
        db.set_pending_shard_map(&ShardMap::balanced(1, 1, 1))
            .await
            .unwrap();
        db.clear_shards(&[0]).await.unwrap();
        drop(db);
        let db = Db::open(&root, "clear0", meta_config()).unwrap();
        assert_eq!(db.security(), json!({"members": {"names": ["a"]}}));
        assert!(db.pending_shard_map().is_some());
        drop(db);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn failed_atomic_write_shows_no_partial_file() {
        let dir = std::env::temp_dir().join(format!("douchdb-atomicfail-{}", crate::util::uuid()));
        std::fs::create_dir_all(&dir).unwrap();

        // A directory that is not there.
        assert!(write_atomic(&dir.join("missing").join("meta.json"), b"x").is_err());

        // The rename fails: the target is a directory. The old target is
        // untouched and the temporary file is cleaned up.
        let target = dir.join("meta.json");
        std::fs::create_dir(&target).unwrap();
        assert!(write_atomic(&target, b"x").is_err());
        assert_eq!(names_in(&dir), ["meta.json"]);
        assert!(target.is_dir());
        std::fs::remove_dir(&target).unwrap();

        // The directory cannot be written to; an existing file keeps its
        // contents.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            write_atomic(&target, b"old").unwrap();
            let locked = std::fs::Permissions::from_mode(0o555);
            std::fs::set_permissions(&dir, locked).unwrap();
            let failed = write_atomic(&target, b"new");
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
            // Running as root ignores the mode; the write then simply succeeds.
            if failed.is_err() {
                assert_eq!(std::fs::read(&target).unwrap(), b"old");
                assert_eq!(names_in(&dir), ["meta.json"]);
            }
        }
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn create_private_atomic_is_exclusive_and_owner_only() {
        let dir = std::env::temp_dir().join(format!("douchdb-private-{}", crate::util::uuid()));
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("secret");
        create_private_atomic(&target, b"one").unwrap();
        let error = create_private_atomic(&target, b"two").unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&target).unwrap(), b"one");
        assert_eq!(names_in(&dir), ["secret"]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&target).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_client_shard_maps_get_distinct_versions() {
        let root = std::env::temp_dir().join(format!("douchdb-nextmap-{}", crate::util::uuid()));
        let db = Arc::new(
            Db::open_or_create(
                &root,
                "next",
                DbConfig {
                    q: 2,
                    revs_limit: 100,
                    cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                    shard_map: ShardMap::balanced(2, 3, 1),
                    partitioned: false,
                },
            )
            .unwrap(),
        );
        let start = db.shard_map().version;
        let racers: Vec<_> = (0..8)
            .map(|node| {
                let db = db.clone();
                tokio::spawn(async move {
                    db.set_next_shard_map(ShardMap {
                        q: 2,
                        node_count: 3,
                        replicas: vec![vec![node % 3], vec![node % 3]],
                        version: 0,
                    })
                    .await
                    .unwrap()
                    .version
                })
            })
            .collect();
        let mut versions = Vec::new();
        for racer in racers {
            versions.push(racer.await.unwrap());
        }
        versions.sort();
        assert_eq!(versions, (start + 1..=start + 8).collect::<Vec<_>>());
        assert_eq!(db.shard_map().version, start + 8);
        drop(db);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn equal_version_shard_maps_converge_on_one_winner() {
        let root = std::env::temp_dir().join(format!("douchdb-tiebreak-{}", crate::util::uuid()));
        let open = |name: &str| {
            Db::open_or_create(
                &root,
                name,
                DbConfig {
                    q: 2,
                    revs_limit: 100,
                    cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                    shard_map: ShardMap::balanced(2, 2, 1),
                    partitioned: false,
                },
            )
            .unwrap()
        };
        let map = |replicas: Vec<Vec<usize>>| ShardMap {
            q: 2,
            node_count: 2,
            replicas,
            version: 1,
        };
        let a = map(vec![vec![0], vec![0]]);
        let b = map(vec![vec![1], vec![1]]);

        let first = open("tie-a");
        assert!(first.set_shard_map(a.clone()).await.unwrap());
        let first_accepts_b = first.set_shard_map(b.clone()).await.unwrap();
        let second = open("tie-b");
        assert!(second.set_shard_map(b.clone()).await.unwrap());
        let second_accepts_a = second.set_shard_map(a.clone()).await.unwrap();

        // Exactly one of the two competing maps wins on both replicas.
        assert_ne!(first_accepts_b, second_accepts_a);
        assert_eq!(first.shard_map().replicas, second.shard_map().replicas);
        drop((first, second));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn expired_migration_map_does_not_affect_placement() {
        let root = std::env::temp_dir().join(format!("douchdb-stale-mig-{}", crate::util::uuid()));
        let active = ShardMap {
            q: 2,
            node_count: 2,
            replicas: vec![vec![0], vec![0]],
            version: 1,
        };
        let target = ShardMap {
            replicas: vec![vec![1], vec![1]],
            ..active.clone()
        };
        let db = Db::open_or_create(
            &root,
            "stale-mig",
            DbConfig {
                q: 2,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: active.clone(),
                partitioned: false,
            },
        )
        .unwrap();
        db.set_migration_map(target.clone(), "a".into()).unwrap();
        db.set_rebalance_in_progress(true);
        assert!(db.is_replica_of("doc", 1) && !db.is_replica_of("doc", 0));
        assert_eq!(db.replication_map().replicas, target.replicas);

        // The freeze window ends without the coordinator clearing the map.
        // The clock counts from its first reading, so "expired" (an end of 1)
        // only holds once it has reached 1.
        while now_millis() < 1 {
            std::thread::yield_now();
        }
        db.rebalance_until.store(1, Ordering::SeqCst);
        assert!(!db.rebalance_in_progress());
        assert!(db.is_replica_of("doc", 0) && !db.is_replica_of("doc", 1));
        assert_eq!(db.replication_map().replicas, active.replicas);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn migration_clears_only_for_its_own_id_unless_expired() {
        let root = std::env::temp_dir().join(format!("douchdb-mig-id-{}", crate::util::uuid()));
        let active = ShardMap {
            q: 2,
            node_count: 2,
            replicas: vec![vec![0], vec![0]],
            version: 1,
        };
        let target = ShardMap {
            replicas: vec![vec![1], vec![1]],
            ..active.clone()
        };
        let db = Db::open_or_create(
            &root,
            "mig-id",
            DbConfig {
                q: 2,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: active,
                partitioned: false,
            },
        )
        .unwrap();
        // Nothing to clear is fine.
        assert!(db.clear_migration_if("a"));
        db.begin_migration(target.clone(), "a".into()).unwrap();
        assert!(!db.clear_migration_if("b"));
        assert!(db.rebalance_in_progress());
        assert!(db.clear_migration_if("a"));
        assert!(!db.rebalance_in_progress());
        // An expired freeze no longer protects the stale map.
        db.begin_migration(target, "a".into()).unwrap();
        while now_millis() < 1 {
            std::thread::yield_now();
        }
        db.rebalance_until.store(1, Ordering::SeqCst);
        assert!(db.clear_migration_if("b"));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn design_documents_replicate_to_every_node() {
        let root = std::env::temp_dir().join(format!("douchdb-placement-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "placement",
            DbConfig {
                q: 4,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: ShardMap::balanced(4, 3, 1),
                partitioned: false,
            },
        )
        .unwrap();

        assert_eq!(db.replicas_of_doc("_design/foo"), vec![0, 1, 2]);
        assert!((0..3).all(|node| db.is_replica_of("_design/foo", node)));

        let replicas = db.replicas_of_doc("ordinary");
        assert_eq!(replicas.len(), 1);
        assert_eq!(
            replicas,
            db.replicas_of_shard(db.shard_index("ordinary") as u32)
        );
        assert_eq!(
            (0..3)
                .filter(|node| db.is_replica_of("ordinary", *node))
                .count(),
            1
        );

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn changes_limit_reports_pending_documents() {
        let root = std::env::temp_dir().join(format!("douchdb-pending-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "pending",
            DbConfig {
                q: 1,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        let docs: Vec<Value> = (0..5)
            .map(|i| json!({"_id": format!("p{i}"), "n": i}))
            .collect();
        db.put_docs(docs, true).await.unwrap();

        let opts = ChangesOpts {
            since: ShardSince::default(),
            limit: Some(2),
            include_docs: false,
            style_all_docs: false,
            descending: false,
            doc_ids: None,
            filter: None,
            conflicts: false,
            owned: None,
            until: None,
        };
        let (rows, last_seq, pending) = db.changes_rows(&opts).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(last_seq.get(0), 2);
        assert_eq!(pending, 3, "three documents remain after the page");

        let (rows, last_seq, pending) = db
            .changes_rows(&ChangesOpts {
                descending: true,
                ..opts
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(last_seq.get(0), 4);
        assert_eq!(pending, 3, "three older documents remain descending");

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    fn scan_db(root: &std::path::Path, name: &str, q: u32, partitioned: bool) -> Db {
        Db::open_or_create(
            root,
            name,
            DbConfig {
                q,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: ShardMap::single(q),
                partitioned,
            },
        )
        .unwrap()
    }

    /// A view build reads its documents a page at a time: no page holds more
    /// than the limit, however many documents the shards hold, and the pages
    /// together are exactly what `map_docs` returns.
    #[tokio::test]
    async fn map_docs_pages_are_bounded_and_cover_every_document() {
        let root = std::env::temp_dir().join(format!("douchdb-mappage-{}", crate::util::uuid()));
        let db = scan_db(&root, "mappage", 3, false);
        let mut docs: Vec<Value> = (0..250).map(|i| json!({"_id": format!("d{i}")})).collect();
        docs.push(json!({"_id": "_design/skipped"}));
        db.put_docs(docs, true).await.unwrap();

        let mut cursor = MapCursor::default();
        let mut sizes = Vec::new();
        let mut paged = Vec::new();
        loop {
            let page = db.map_docs_page(None, &mut cursor, 40).await.unwrap();
            if page.is_empty() {
                break;
            }
            sizes.push(page.len());
            paged.extend(page);
        }
        assert!(sizes.iter().all(|&size| size <= 40), "{sizes:?}");
        assert!(sizes.len() >= 250 / 40, "{sizes:?}");
        let mut all = db.map_docs(None).await.unwrap();
        paged.sort();
        all.sort();
        assert_eq!(paged.len(), 250);
        assert_eq!(paged, all);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    fn descending_design_changes(limit: Option<usize>) -> ChangesOpts {
        ChangesOpts {
            since: ShardSince::default(),
            limit,
            include_docs: false,
            style_all_docs: false,
            descending: true,
            doc_ids: None,
            filter: Some(ChangeFilter::Design),
            conflicts: false,
            owned: None,
            until: None,
        }
    }

    /// A descending filtered feed reports the lowest sequence it scanned when
    /// it runs out of documents, the last row's when it stops at its limit,
    /// and counts the documents older than that as pending.
    #[tokio::test]
    async fn descending_filtered_changes_report_last_seq_and_pending() {
        let root = std::env::temp_dir().join(format!("douchdb-descf-{}", crate::util::uuid()));
        let db = scan_db(&root, "descf", 1, false);
        // Sequences 1..=30; the design documents are 5, 12 and 25.
        let docs: Vec<Value> = (1..=30)
            .map(|i| match i {
                5 | 12 | 25 => json!({"_id": format!("_design/d{i}")}),
                _ => json!({"_id": format!("doc{i:02}")}),
            })
            .collect();
        for doc in docs {
            db.put_docs(vec![doc], true).await.unwrap();
        }

        let (rows, last_seq, pending) = db
            .changes_rows(&descending_design_changes(Some(1)))
            .await
            .unwrap();
        assert_eq!(rows.iter().map(|r| r.seq).collect::<Vec<_>>(), [25]);
        assert_eq!((last_seq.get(0), pending), (25, 24));

        let (rows, last_seq, pending) = db
            .changes_rows(&descending_design_changes(Some(2)))
            .await
            .unwrap();
        assert_eq!(rows.iter().map(|r| r.seq).collect::<Vec<_>>(), [25, 12]);
        assert_eq!((last_seq.get(0), pending), (12, 11));

        // Fewer matches than the limit: the whole feed was scanned.
        let (rows, last_seq, pending) = db
            .changes_rows(&descending_design_changes(Some(10)))
            .await
            .unwrap();
        assert_eq!(rows.iter().map(|r| r.seq).collect::<Vec<_>>(), [25, 12, 5]);
        assert_eq!((last_seq.get(0), pending), (1, 0));

        let (rows, last_seq, pending) = db
            .changes_rows(&descending_design_changes(None))
            .await
            .unwrap();
        assert_eq!(rows.iter().map(|r| r.seq).collect::<Vec<_>>(), [25, 12, 5]);
        assert_eq!((last_seq.get(0), pending), (1, 0));

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// The same over two shards, read newest first one shard after the
    /// other: each shard's position is the lowest sequence scanned on it, or
    /// its last row's when the limit cut its rows short, or its bound when
    /// the read never reached it; `pending` counts what lies below each.
    #[tokio::test]
    async fn descending_filtered_changes_report_each_shards_position_and_pending() {
        let root = std::env::temp_dir().join(format!("douchdb-descf2-{}", crate::util::uuid()));
        let db = scan_db(&root, "descf2", 2, false);
        let on_shard = |shard: usize, prefix: &str| {
            (0..10_000)
                .map(|i| format!("{prefix}{i}"))
                .find(|id| db.shard_index(id) == shard)
                .expect("id for shard")
        };
        // Shard 0: plain 1, design 2, plain 3, plain 4.
        // Shard 1: design 1, plain 2, design 3, plain 4.
        let shard0 = [
            on_shard(0, "p0-"),
            on_shard(0, "_design/a"),
            on_shard(0, "p1-"),
            on_shard(0, "p2-"),
        ];
        let shard1 = [
            on_shard(1, "_design/c"),
            on_shard(1, "q0-"),
            on_shard(1, "_design/b"),
            on_shard(1, "q1-"),
        ];
        for id in shard0.iter().chain(&shard1) {
            db.put_docs(vec![json!({"_id": id})], true).await.unwrap();
        }
        assert_eq!(
            db.shard_seqs()
                .iter()
                .map(|(_, seq)| *seq)
                .collect::<Vec<_>>(),
            [4, 4]
        );
        let read = |limit| {
            let db = &db;
            async move {
                let (rows, last, pending) = db
                    .changes_rows(&descending_design_changes(limit))
                    .await
                    .unwrap();
                let rows: Vec<(usize, u64)> = rows.iter().map(|r| (r.shard, r.seq)).collect();
                (rows, (last.get(0), last.get(1)), pending)
            }
        };

        // Shard 0 is scanned to its start; shard 1 is never reached, so its
        // four changes are all still ahead.
        assert_eq!(read(Some(1)).await, (vec![(0, 2)], (1, 4), 4));
        // Shard 1's rows are cut by the limit after its newest design
        // document: the two changes below it are still ahead.
        assert_eq!(read(Some(2)).await, (vec![(0, 2), (1, 3)], (1, 3), 2));
        // Fewer matches than the limit: both shards were scanned.
        assert_eq!(
            read(Some(10)).await,
            (vec![(0, 2), (1, 3), (1, 1)], (1, 1), 0)
        );
        assert_eq!(read(None).await, (vec![(0, 2), (1, 3), (1, 1)], (1, 1), 0));

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A descending feed starts every shard at its current position, so a
    /// row's token puts the shards not yet read there, not at `since`: it
    /// agrees with `last_seq`, and resuming from it replays nothing (CouchDB
    /// starts each reverse worker at its shard's update_seq).
    #[tokio::test]
    async fn descending_row_tokens_start_unread_shards_at_their_bound() {
        let root = std::env::temp_dir().join(format!("douchdb-descrow-{}", crate::util::uuid()));
        let db = scan_db(&root, "descrow", 2, false);
        for i in 0..10 {
            db.put_docs(vec![json!({"_id": format!("doc{i}")})], true)
                .await
                .unwrap();
        }
        let positions = |token: &str| {
            db.shard_since(
                0,
                &crate::since::Since::Token(crate::since::decode(token).unwrap()),
            )
        };
        let opts = ChangesOpts {
            descending: true,
            limit: Some(1),
            filter: None,
            ..descending_design_changes(Some(1))
        };
        let list = db.changes(0, &opts, true).await.unwrap();
        assert_eq!(list.results.len(), 1);
        let row = positions(&list.results[0].seq);
        let last = positions(&list.last_seq);
        let current = db.changes_bound(&opts);
        assert!((0..2).all(|shard| current.get(shard) > 0), "{current:?}");
        assert_eq!(row, last, "the row's token and last_seq disagree");

        let (replayed, _) = db
            .changes_rows_only(&ChangesOpts {
                since: row,
                descending: false,
                limit: None,
                ..opts
            })
            .await
            .unwrap();
        assert!(
            replayed.is_empty(),
            "resuming from the row replayed {:?}",
            replayed.iter().map(|r| &r.id).collect::<Vec<_>>()
        );

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// The newest changes are read first, so a descending filtered feed with a
    /// small limit visits about a page of the sequence index however many
    /// documents the database holds.
    #[tokio::test]
    async fn descending_filtered_changes_visit_entries_independent_of_size() {
        let mut visited = Vec::new();
        for n in [300usize, 3000] {
            let root = std::env::temp_dir().join(format!("douchdb-descv-{}", crate::util::uuid()));
            let db = scan_db(&root, "descv", 2, false);
            let docs: Vec<Value> = (0..n).map(|i| json!({"_id": format!("d{i}")})).collect();
            db.put_docs(docs, true).await.unwrap();
            db.put_docs(vec![json!({"_id": "_design/newest"})], true)
                .await
                .unwrap();
            // Build the sequence index first: only the read is measured.
            db.changes_rows(&descending_design_changes(Some(1)))
                .await
                .unwrap();

            let before = db.shard_scanned();
            let (rows, _, _) = db
                .changes_rows(&descending_design_changes(Some(1)))
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            visited.push(db.shard_scanned() - before);
            drop(db);
            let _ = std::fs::remove_dir_all(root);
        }
        // One page per shard at most.
        assert!(
            visited.iter().all(|&v| v <= 128),
            "entries visited for n=300 and n=3000: {visited:?}"
        );
    }

    /// Paging a partition's `_all_docs` by `startkey` ranks each start from the
    /// previous one, so the offset costs the distance between the two, not the
    /// partition's size, and the partition's count is not recounted.
    #[tokio::test]
    async fn partitioned_all_docs_offset_visits_entries_independent_of_size() {
        let mut visited = Vec::new();
        for n in [500usize, 5000] {
            let root = std::env::temp_dir().join(format!("douchdb-partv-{}", crate::util::uuid()));
            let db = scan_db(&root, "partv", 1, true);
            let mut docs: Vec<Value> = (0..n)
                .map(|i| json!({"_id": format!("p:{i:05}")}))
                .collect();
            docs.extend((0..10).map(|i| json!({"_id": format!("o:{i}")})));
            docs.extend((0..10).map(|i| json!({"_id": format!("q:{i}")})));
            db.put_docs(docs, true).await.unwrap();
            let query = |start: usize, descending: bool| AllDocsOpts {
                partition: Some("p".into()),
                startkey: Some(format!("p:{start:05}")),
                descending,
                limit: 1,
                ..Default::default()
            };
            // Warm the cursor deep in the partition.
            db.all_docs(&query(n - 100, false)).await.unwrap();

            let before = db.shard_scanned();
            let list = db.all_docs(&query(n - 95, false)).await.unwrap();
            assert_eq!(
                (list.total_rows, list.offset),
                (Some(n as u64), Some(n as u64 - 95))
            );
            let list = db.all_docs(&query(n - 90, true)).await.unwrap();
            assert_eq!((list.total_rows, list.offset), (Some(n as u64), Some(89)));
            visited.push(db.shard_scanned() - before);

            // Writes before the cursor keep later offsets exact.
            db.put_docs(vec![json!({"_id": "p:00000a"})], true)
                .await
                .unwrap();
            let rev = db.get_doc("p:00001", &DocOpts::default()).await.unwrap();
            db.put_docs(
                vec![json!({"_id": "p:00001", "_rev": rev["_rev"], "_deleted": true})],
                true,
            )
            .await
            .unwrap();
            let list = db.all_docs(&query(n - 95, false)).await.unwrap();
            assert_eq!(
                (list.total_rows, list.offset),
                (Some(n as u64), Some(n as u64 - 95))
            );
            drop(db);
            let _ = std::fs::remove_dir_all(root);
        }
        assert!(
            visited.iter().all(|&v| v <= 50),
            "entries visited for n=500 and n=5000: {visited:?}"
        );
    }

    /// The `GET /db` body is the serialized `info()` result: its field order
    /// and the optional `props` are part of the wire format.
    #[tokio::test]
    async fn info_serializes_in_wire_order() {
        for partitioned in [false, true] {
            let q = 2u32;
            let root =
                std::env::temp_dir().join(format!("douchdb-info-wire-{}", crate::util::uuid()));
            let db = Db::open_or_create(
                &root,
                "infowire",
                DbConfig {
                    q,
                    revs_limit: 100,
                    cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                    shard_map: ShardMap::single(q),
                    partitioned,
                },
            )
            .unwrap();
            let info = serde_json::to_value(db.info().await.unwrap()).unwrap();
            let mut keys: Vec<&str> = info
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            let mut expected = vec![
                "db_name",
                "doc_count",
                "doc_del_count",
                "update_seq",
                "purge_seq",
                "compact_running",
                "disk_size",
                "data_size",
                "sizes",
                "instance_start_time",
                "disk_format_version",
                "committed_update_seq",
                "uuid",
                "cluster",
            ];
            if partitioned {
                expected.push("props");
            }
            assert_eq!(keys, expected);
            keys.clear();
            let file = info["sizes"]["file"].as_u64().unwrap();
            assert!(file > 0, "{info}");
            assert_eq!(info["sizes"]["external"], json!(0));
            assert_eq!(info["disk_size"], json!(file));
            assert_eq!(info["data_size"], info["sizes"]["active"]);
            assert_eq!(info["cluster"], json!({"q": 2, "n": 1, "w": 1, "r": 1}));
            assert_eq!(info["doc_count"], json!(0));
            assert_eq!(info["compact_running"], json!(false));
            if partitioned {
                assert_eq!(info["props"], json!({"partitioned": true}));
            }
            drop(db);
            let _ = std::fs::remove_dir_all(root);
        }
    }

    /// Document reads (`get_doc`, `get_docs`, `_bulk_get`, `_revs_diff`, the
    /// view and anti-entropy fetches) run on leases off the shard actor, so
    /// they answer while a write group holds it, from the committed state.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn document_reads_answer_while_a_write_group_holds_the_actor() {
        let (db, root) = single_shard_db("leased-reads").await;
        db.set_revs_limit(2).await.unwrap();
        let mut rev = String::new();
        for n in 0..3 {
            let mut doc = json!({"_id": "a", "n": n});
            if !rev.is_empty() {
                doc["_rev"] = json!(rev);
            }
            let written = db.put_docs(vec![doc], true).await.unwrap();
            rev = written[0].json()["rev"].as_str().unwrap().to_string();
        }
        db.put_docs(vec![json!({"_id": "b"})], true).await.unwrap();

        // A job that holds the actor, and with it the next group, until the
        // reads are done.
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let held = db.shard_handle(0).call(move |shard| {
            shard.put_local("held", b"{}")?;
            entered_tx.send(()).unwrap();
            release_rx.recv().map_err(ApiError::internal)?;
            Ok(())
        });
        let reads = async {
            entered_rx.await.unwrap();
            let opts = DocOpts {
                revs: true,
                ..Default::default()
            };
            let doc = db.get_doc("a", &opts).await.unwrap();
            assert_eq!(doc["n"], json!(2));
            assert_eq!(
                doc["_revisions"]["ids"].as_array().unwrap().len(),
                2,
                "revs_limit applies off the actor"
            );
            let docs = db
                .get_docs(&["a".to_string(), "zz".to_string()], &DocOpts::default())
                .await
                .unwrap();
            assert!(docs[0].is_ok() && docs[1].is_err());
            let by_id = db.docs_by_ids(&["b".to_string()]).await.unwrap();
            assert!(by_id.contains_key("b"));
            let diff = db
                .revs_diff(&json!({"a": [rev.clone(), "9-ffff"]}))
                .await
                .unwrap();
            assert_eq!(diff.0.len(), 1);
            assert_eq!(diff.0[0].1.missing, vec!["9-ffff".to_string()]);
            let got = db
                .bulk_get(
                    &json!({"docs": [{"id": "a", "rev": rev.clone()}]}),
                    false,
                    false,
                )
                .await
                .unwrap();
            assert_eq!(got.results.len(), 1);
            let pairs = [("a".to_string(), rev.clone())].into_iter().collect();
            assert!(db.purged_among(&pairs).await.unwrap().is_empty());
            let page = db
                .map_docs_page(None, &mut MapCursor::default(), 10)
                .await
                .unwrap();
            let ids: Vec<&str> = page.iter().map(|(id, _)| id.as_str()).collect();
            assert_eq!(ids, ["a", "b"]);
            release_tx.send(()).unwrap();
        };
        let (held, ()) = tokio::join!(held, reads);
        held.unwrap();
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// `_revs_diff` answers in request order, leaves `possible_ancestors` out
    /// when there are none, and omits documents that miss nothing.
    #[tokio::test]
    async fn revs_diff_serializes_in_request_order() {
        let q = 2u32;
        let root = std::env::temp_dir().join(format!("douchdb-revs-wire-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "revswire",
            DbConfig {
                q,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: ShardMap::single(q),
                partitioned: false,
            },
        )
        .unwrap();
        let written = db
            .put_docs(vec![json!({"_id": "known"}), json!({"_id": "same"})], true)
            .await
            .unwrap();
        let rev = written[0].json()["rev"].as_str().unwrap().to_string();
        let same_rev = written[1].json()["rev"].as_str().unwrap().to_string();
        let body: Value = serde_json::from_str(&format!(
            r#"{{"zed":["1-aaaa"],"known":["{rev}","2-ffff"],"same":["{same_rev}"],"not-a-list":"x","alpha":["1-bbbb","1-cccc"]}}"#
        ))
        .unwrap();
        let diff = db.revs_diff(&body).await.unwrap();
        let text = serde_json::to_string(&serde_json::to_value(&diff).unwrap()).unwrap();
        assert_eq!(
            text,
            format!(
                r#"{{"zed":{{"missing":["1-aaaa"]}},"known":{{"missing":["2-ffff"],"possible_ancestors":["{rev}"]}},"alpha":{{"missing":["1-bbbb","1-cccc"]}}}}"#
            )
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Requests that touch many documents fan out one call per shard; the
    /// answers must still come back per document, in the order asked.
    #[tokio::test]
    async fn per_document_answers_survive_the_per_shard_fan_out() {
        let q = 4u32;
        let root = std::env::temp_dir().join(format!("douchdb-fan-out-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "fanout",
            DbConfig {
                q,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: ShardMap::single(q),
                partitioned: false,
            },
        )
        .unwrap();
        let ids: Vec<String> = (0..12).map(|n| format!("doc-{n:02}")).collect();
        let shards: std::collections::HashSet<usize> =
            ids.iter().map(|id| db.shard_index(id)).collect();
        assert!(shards.len() > 1, "the ids must spread over shards");
        let docs = ids.iter().map(|id| json!({"_id": id, "n": id})).collect();
        let written = db.put_docs(docs, true).await.unwrap();
        assert!(written.iter().all(|row| row.is_ok()));
        let info = db.info().await.unwrap();
        assert_eq!(info.doc_count, 12);
        assert_eq!(info.update_seq, 12);

        let got = db.get_docs(&ids, &DocOpts::default()).await.unwrap();
        for (id, doc) in ids.iter().zip(&got) {
            assert_eq!(doc.as_ref().unwrap()["n"], json!(id));
        }
        let mut asked = ids.clone();
        asked.push("absent".to_string());
        let got = db.get_docs(&asked, &DocOpts::default()).await.unwrap();
        assert_eq!(got.last().unwrap().as_ref().unwrap_err().status, 404);

        // Every document is missing one revision it was not told about, and
        // lists the leaf it has as a possible ancestor.
        let mut diff = Map::new();
        for id in &ids {
            let rev =
                written[ids.iter().position(|other| other == id).unwrap()].json()["rev"].clone();
            diff.insert(id.clone(), json!([rev, "2-ffff"]));
        }
        let diff = db.revs_diff(&Value::Object(diff)).await.unwrap();
        assert_eq!(diff.0.len(), 12);
        for (position, id) in ids.iter().enumerate() {
            let entry = &diff.0.iter().find(|(row, _)| row == id).unwrap().1;
            assert_eq!(entry.missing, ["2-ffff"]);
            assert_eq!(
                json!(entry.possible_ancestors),
                json!([written[position].json()["rev"]])
            );
        }

        // Purging by document answers for each document, and only for the
        // revisions it removed.
        let mut purge = Map::new();
        for (position, id) in ids.iter().enumerate().take(6) {
            purge.insert(
                id.clone(),
                json!([written[position].json()["rev"], "9-nope"]),
            );
        }
        let purged = db
            .purge(&Value::Object(purge), 0, &Default::default())
            .await
            .unwrap();
        for (position, id) in ids.iter().enumerate().take(6) {
            assert_eq!(
                purged["purged"][id],
                json!([written[position].json()["rev"]])
            );
        }
        assert_eq!(db.info().await.unwrap().doc_count, 6);

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A storage error part way through writing a batch fails the whole
    /// batch, though `put_many` turns it into per-document results: none of
    /// its documents is readable, its sequences are not committed, and the
    /// shard takes the batch afterwards.
    #[tokio::test]
    async fn a_write_failing_inside_a_batch_fails_the_batch_and_leaves_none_of_it() {
        let (db, root) = single_shard_db("batch-write-fail").await;
        db.put_docs(vec![json!({"_id": "kept"})], true)
            .await
            .unwrap();
        let before = db.shard_seqs()[0].1;
        // The bodies and sequence rows get through; the metadata does not.
        db.shards[0]
            .call(|s| {
                s.inject_write_failure(2);
                Ok(())
            })
            .await
            .unwrap();
        let batch = vec![
            json!({"_id": "x"}),
            json!({"_id": "y"}),
            json!({"_id": "z"}),
        ];
        let failed = db.put_docs(batch.clone(), true).await;
        assert!(failed.is_err(), "{failed:?}");
        for id in ["x", "y", "z"] {
            let read = db.get_doc(id, &DocOpts::default()).await;
            assert_eq!(read.unwrap_err().status, 404, "{id}");
        }
        let all = db.all_docs(&AllDocsOpts::default()).await.unwrap();
        assert_eq!(all.rows.len(), 1, "{all:?}");
        assert_eq!(db.info().await.unwrap().doc_count, 1);
        assert_eq!(db.shard_seqs()[0].1, before, "no sequence was committed");

        let written = db.put_docs(batch, true).await.unwrap();
        assert!(written.iter().all(WriteResult::is_ok), "{written:?}");
        assert_eq!(db.info().await.unwrap().doc_count, 4);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn failed_shard_commit_leaves_no_phantom_documents_or_stalled_changes() {
        let q = 2u32;
        let root = std::env::temp_dir().join(format!("douchdb-flushfail-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            "flushfail",
            DbConfig {
                q,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: ShardMap::single(q),
                partitioned: false,
            },
        )
        .unwrap();
        let good = doc_id_for_shard(&db, 0);
        let lost = (0..10_000)
            .map(|i| format!("lost-{i}"))
            .find(|id| db.shard_index(id) == 0)
            .expect("second document id for shard 0");
        db.put_docs(vec![json!({"_id": good})], true).await.unwrap();
        // Build the lazy indexes so the failure has to repair them.
        let all = db.all_docs(&AllDocsOpts::default()).await.unwrap();
        assert_eq!(all.total_rows, Some(1));

        db.shards[0]
            .call(|s| {
                s.inject_commit_failure();
                Ok(())
            })
            .await
            .unwrap();
        let failed = db.put_docs(vec![json!({"_id": lost})], true).await;
        assert!(failed.is_err(), "{failed:?}");

        assert!(db.get_doc(&lost, &DocOpts::default()).await.is_err());
        let all = db.all_docs(&AllDocsOpts::default()).await.unwrap();
        assert_eq!(all.rows.len(), 1, "{all:?}");
        assert_eq!(db.info().await.unwrap().doc_count, 1);

        // Nothing of the lost write is committed, its sequence included;
        // the feed is not held back.
        let before = db.shard_seqs()[0].1;
        let opts = ChangesOpts {
            since: ShardSince::default(),
            limit: None,
            include_docs: false,
            style_all_docs: false,
            descending: false,
            doc_ids: None,
            filter: None,
            conflicts: false,
            owned: None,
            until: None,
        };
        let (rows, _, pending) = db.changes_rows(&opts).await.unwrap();
        assert_eq!(
            rows.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            [good.as_str()]
        );
        assert_eq!(pending, 0);

        // The shard accepts the write again.
        let results = db.put_docs(vec![json!({"_id": lost})], true).await.unwrap();
        assert!(results[0].is_ok(), "{results:?}");
        assert_eq!(db.info().await.unwrap().doc_count, 2);
        let (rows, last_seq, _) = db.changes_rows(&opts).await.unwrap();
        assert_eq!(rows.len(), 2);
        let after = db.shard_seqs()[0].1;
        assert_eq!(last_seq.get(0), after);
        // The retried write takes the sequence the failed one never
        // committed, which no reader saw.
        assert_eq!(after, before + 1, "{before} -> {after}");

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    fn two_shard_db(tag: &str) -> (Db, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!("douchdb-{tag}-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            tag,
            DbConfig {
                q: 2,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: ShardMap::single(2),
                partitioned: false,
            },
        )
        .unwrap();
        (db, root)
    }

    fn ids_on_shard(db: &Db, shard: usize, tag: &str, count: usize) -> Vec<String> {
        (0..100_000)
            .map(|i| format!("{tag}{i}"))
            .filter(|id| db.shard_index(id) == shard)
            .take(count)
            .collect()
    }

    #[tokio::test]
    async fn shard_sequences_are_independent() {
        let (db, root) = two_shard_db("shardseqs");
        for id in ids_on_shard(&db, 0, "zero", 3) {
            db.put_docs(vec![json!({"_id": id})], true).await.unwrap();
        }
        for id in ids_on_shard(&db, 1, "one", 1) {
            db.put_docs(vec![json!({"_id": id})], true).await.unwrap();
        }
        let seqs: Vec<u64> = db.shard_seqs().into_iter().map(|(_, seq)| seq).collect();
        assert_eq!(seqs, vec![3, 1]);
        let uuids: Vec<String> = db.shard_seqs().into_iter().map(|(uuid, _)| uuid).collect();
        assert!(uuids.iter().all(|uuid| !uuid.is_empty()), "{uuids:?}");
        assert_ne!(uuids[0], uuids[1]);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// An index update reads each shard only up to the position that shard
    /// had when the update started, and records exactly that position.
    #[tokio::test]
    async fn incremental_update_is_bounded_by_each_shards_start_position() {
        let (db, root) = two_shard_db("bounded");
        let zero = ids_on_shard(&db, 0, "z", 4);
        let one = ids_on_shard(&db, 1, "o", 4);
        for id in zero.iter().take(2).chain(one.iter().take(1)) {
            db.put_docs(vec![json!({"_id": id})], true).await.unwrap();
        }
        let bound = ShardSince(db.shard_seqs().into_iter().map(|(_, seq)| seq).collect());
        for id in zero.iter().skip(2).chain(one.iter().skip(1)) {
            db.put_docs(vec![json!({"_id": id})], true).await.unwrap();
        }
        let (rows, last) = db
            .changes_rows_only(&ChangesOpts {
                until: Some(bound.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut got: Vec<&str> = rows.iter().map(|row| row.id.as_str()).collect();
        got.sort();
        let mut want: Vec<&str> = zero
            .iter()
            .take(2)
            .chain(one.iter().take(1))
            .map(String::as_str)
            .collect();
        want.sort();
        assert_eq!(got, want);
        assert_eq!(last, bound);
        // Resuming from there reads the rest, shard by shard.
        let (rows, last) = db
            .changes_rows_only(&ChangesOpts {
                since: last,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 5, "{rows:?}");
        assert_eq!(last, ShardSince(vec![4, 4]));
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Rows come back in each shard's own order; a limit stops part-way
    /// through and the reported positions resume exactly there.
    #[tokio::test]
    async fn limited_changes_resume_from_per_shard_positions() {
        let (db, root) = two_shard_db("perlimit");
        let mut written = Vec::new();
        for (zero, one) in ids_on_shard(&db, 0, "z", 5)
            .into_iter()
            .zip(ids_on_shard(&db, 1, "o", 5))
        {
            db.put_docs(vec![json!({"_id": zero}), json!({"_id": one})], true)
                .await
                .unwrap();
            written.extend([zero, one]);
        }
        let mut since = ShardSince::default();
        let mut seen = Vec::new();
        loop {
            let (rows, last, _) = db
                .changes_rows(&ChangesOpts {
                    since: since.clone(),
                    limit: Some(3),
                    ..Default::default()
                })
                .await
                .unwrap();
            if rows.is_empty() {
                break;
            }
            for shard in 0..2 {
                let seqs: Vec<u64> = rows
                    .iter()
                    .filter(|row| row.shard == shard)
                    .map(|row| row.seq)
                    .collect();
                assert!(seqs.windows(2).all(|pair| pair[0] < pair[1]), "{rows:?}");
            }
            seen.extend(rows.into_iter().map(|row| row.id));
            since = last;
        }
        seen.sort();
        written.sort();
        assert_eq!(seen, written);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn shard_placement_is_deterministic_and_in_range(
            docid in ".{0,80}",
            q in 1u32..64,
        ) {
            let a = shard_index(&docid, q);
            let b = shard_index(&docid, q);
            prop_assert_eq!(a, b);
            prop_assert!(a < q as usize);
        }

        #[test]
        fn partitioned_documents_stay_in_their_partition_shard(
            partition in "[A-Za-z0-9][A-Za-z0-9_-]{0,29}",
            doc in "[A-Za-z0-9][A-Za-z0-9_-]{0,29}",
            q in 2u32..64,
        ) {
            let first = format!("{partition}:{doc}");
            let second = format!("{partition}:other-{doc}");
            prop_assert_eq!(
                shard_index_for(&first, q, true),
                shard_index_for(&second, q, true)
            );
        }
    }

    fn purge_test_db(name: &str, shard_map: ShardMap) -> (Db, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!("douchdb-{name}-{}", crate::util::uuid()));
        let db = Db::open_or_create(
            &root,
            name,
            DbConfig {
                q: 1,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map,
                partitioned: false,
            },
        )
        .unwrap();
        (db, root)
    }

    /// The threads a given map function was compiled on.
    fn compiled_on(source: &str) -> Vec<Option<String>> {
        COMPILE_LOG
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .filter(|(compiled, _)| compiled == source)
            .map(|(_, thread)| thread.clone())
            .collect()
    }

    fn design_doc_with_map(map: &str) -> Value {
        json!({"_id": "_design/d", "views": {"v": {"map": map}}})
    }

    /// Saving a design document must not hold its shard's writer while the
    /// functions compile: the check runs before the shard job, never on the
    /// shard thread, and once per save.
    #[tokio::test]
    async fn design_doc_compile_runs_outside_the_shard_thread() {
        let (db, root) = purge_test_db("compile-outside", ShardMap::single(1));
        let map = "function(doc){ /* outside-shard */ emit(doc._id, 1); }";
        let put = db
            .put_docs(vec![design_doc_with_map(map)], true)
            .await
            .unwrap();
        assert!(put[0].is_ok(), "{put:?}");
        let threads = compiled_on(map);
        assert_eq!(threads.len(), 1, "{threads:?}");
        assert!(
            threads
                .iter()
                .flatten()
                .all(|name| !name.starts_with("shard-")),
            "compiled on the shard thread: {threads:?}"
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A revision conflict still wins over a compile error, and a replicated
    /// design document that is already stored is not compiled again.
    #[tokio::test]
    async fn compile_verdict_keeps_conflict_first_and_skips_stored_docs() {
        let (db, root) = purge_test_db("compile-order", ShardMap::single(1));
        let good = "function(doc){ /* order-good */ emit(1, 1); }";
        let put = db
            .put_docs(vec![design_doc_with_map(good)], true)
            .await
            .unwrap();
        assert!(put[0].is_ok(), "{put:?}");

        let bad = "function(doc){ /* order-bad */ emit(";
        let conflict = db
            .put_docs(vec![design_doc_with_map(bad)], true)
            .await
            .unwrap();
        assert_eq!(conflict[0].json()["error"], "conflict", "{conflict:?}");

        let replicated_map = "function(doc){ /* order-replicated */ emit(1, 1); }";
        let hash = "b".repeat(32);
        let replicated = json!({
            "_id": "_design/r",
            "_rev": format!("1-{hash}"),
            "_revisions": {"start": 1, "ids": [hash]},
            "views": {"v": {"map": replicated_map}},
        });
        for _ in 0..2 {
            let out = db.put_docs(vec![replicated.clone()], false).await.unwrap();
            assert_eq!(out[0].json()["ok"], true, "{out:?}");
        }
        assert_eq!(compiled_on(replicated_map).len(), 1);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A purge info another copy recorded is applied once: replaying it
    /// neither counts again nor purges a revision that was added back since.
    #[tokio::test]
    async fn a_purge_info_is_applied_once() {
        let (db, root) = purge_test_db("purge-once", ShardMap::single(1));
        let hash = "a".repeat(32);
        let rev = format!("1-{hash}");
        let doc = json!({"_id": "a", "_rev": rev, "_revisions": {"start": 1, "ids": [hash]}});
        db.put_docs(vec![doc.clone()], false).await.unwrap();
        let info = PurgeInfo {
            uuid: "u1".to_string(),
            id: "a".to_string(),
            revs: vec![rev.clone()],
        };

        db.apply_purge_infos(0, vec![info.clone()], 0)
            .await
            .unwrap();
        assert_eq!(db.shard_purge_seqs().await.unwrap().iter().sum::<u64>(), 1);
        assert!(db.get_doc("a", &DocOpts::default()).await.is_err());

        db.put_docs(vec![doc], false).await.unwrap();
        db.apply_purge_infos(0, vec![info], 0).await.unwrap();
        assert_eq!(db.shard_purge_seqs().await.unwrap().iter().sum::<u64>(), 1);
        assert!(db.get_doc("a", &DocOpts::default()).await.is_ok());

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A cursor that recorded the purge sequence a shard publishes while it
    /// cannot know its own is never current, not even against a shard that
    /// still publishes it: the rows it covers may hold a purged document.
    #[test]
    fn a_cursor_holding_the_unknown_purge_sequence_is_never_current() {
        let cursor = |purge_seq| {
            IndexCursor(vec![ShardPosition {
                uuid: "u".to_string(),
                seq: 3,
                purge_seq,
            }])
        };
        let unknown = cursor(crate::store::UNKNOWN_PURGE_SEQ);
        assert!(!unknown.same_copies_unpurged(&unknown.clone()));
        assert!(!unknown.is_current(&unknown.clone()));
        assert!(cursor(2).same_copies_unpurged(&cursor(2)));
        assert!(cursor(2).is_current(&cursor(2)));
        assert!(!cursor(2).is_current(&cursor(3)));
    }

    /// The ids view `_design/d/v` of `db` serves.
    async fn view_ids(db: &Db) -> Vec<String> {
        let query = crate::view::ViewQuery {
            limit: usize::MAX,
            inclusive_end: true,
            ..Default::default()
        };
        let rows = crate::view::query(db, "_design/d", "v", &query)
            .await
            .unwrap();
        rows["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap().to_string())
            .collect()
    }

    /// The ids search index `_design/d/i` of `db` finds for `name:fig`.
    async fn search_ids(db: &Db) -> Vec<String> {
        let request = crate::search::SearchRequest {
            query: "name:fig".to_string(),
            ..Default::default()
        };
        let limits = crate::view::ViewQuery::default().limits;
        let found = crate::search::query(db, "_design/d", "i", &request, limits)
            .await
            .unwrap();
        let mut ids: Vec<String> = found["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        ids
    }

    /// A purge that lands on the shard while nothing drops the indexes (a
    /// crash before the invalidation, here a purge that never asks for one):
    /// once the database is opened again from scratch, the view and the
    /// search index see that the shard's purge sequence moved past the one
    /// they recorded, and no longer serve the purged document.
    #[tokio::test]
    async fn indexes_drop_a_purged_document_after_a_restart() {
        let _cap = crate::index_cache::CapGuard::new(crate::index_cache::DEFAULT_MAX_OPEN).await;
        let root = std::env::temp_dir().join(format!("douchdb-purge-restart-{}", uuid()));
        let open = || {
            Db::open_or_create(
                &root,
                "purgerestart",
                DbConfig {
                    q: 1,
                    revs_limit: 100,
                    cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                    shard_map: ShardMap::single(1),
                    partitioned: false,
                },
            )
            .unwrap()
        };
        let db = open();
        let written = db
            .put_docs(
                vec![
                    json!({"_id": "_design/d",
                           "views": {"v": {"map": "function(doc) { emit(doc._id, null); }"}},
                           "indexes": {"i": {"index": "function(doc) { index('name', doc.name); }"}}}),
                    json!({"_id": "a", "name": "fig"}),
                    json!({"_id": "b", "name": "fig"}),
                ],
                true,
            )
            .await
            .unwrap();
        let rev = written[1].json()["rev"].as_str().unwrap().to_string();
        assert_eq!(view_ids(&db).await, ["a", "b"]);
        assert_eq!(search_ids(&db).await, ["a", "b"]);

        db.purge(&json!({ "a": [rev] }), 0, &Default::default())
            .await
            .unwrap();
        // Closed as a crash leaves it: the index files stay as they were.
        let path = db.path.clone();
        drop(db);
        crate::view::forget_db(&path).await;
        crate::search::forget_db(&path).await;

        let db = open();
        assert_eq!(
            view_ids(&db).await,
            ["b"],
            "the view serves a purged document"
        );
        assert_eq!(
            search_ids(&db).await,
            ["b"],
            "the search index serves a purged document"
        );
        let path = db.path.clone();
        drop(db);
        crate::view::forget_db(&path).await;
        crate::search::forget_db(&path).await;
        let _ = std::fs::remove_dir_all(root);
    }

    /// Write `id` normally, then replace its metadata record with garbage, as
    /// a damaged shard would hold it. The revision the write produced.
    async fn put_then_corrupt_meta(db: &Db, id: &str) -> String {
        let written = db.put_docs(vec![json!({"_id": id})], true).await.unwrap();
        let rev = written[0].json()["rev"].as_str().unwrap().to_string();
        let id = id.to_string();
        db.shards[0]
            .call(move |s| {
                s.put_raw(
                    crate::kv::META,
                    crate::kv::Table::Meta,
                    id.as_str(),
                    b"{not json",
                );
                Ok(())
            })
            .await
            .unwrap();
        rev
    }

    /// In a batch, a document whose metadata cannot be decoded, or fails its
    /// checksum, fails alone, each time it appears; the rest are written.
    #[tokio::test]
    async fn an_unreadable_record_fails_only_its_document_in_a_batch() {
        let (db, root) = single_shard_db("batch-corrupt").await;
        let rev = put_then_corrupt_meta(&db, "bad").await;
        db.shards[0]
            .call(|s| {
                s.put_unsealed(crate::kv::META, "crc", br#"{"tree":{},"seq":9}"#);
                Ok(s.put_local("setup", b"1")?)
            })
            .await
            .unwrap();
        let written = db
            .put_docs(
                vec![
                    json!({"_id": "x"}),
                    json!({"_id": "bad", "_rev": rev}),
                    json!({"_id": "crc"}),
                    json!({"_id": "y"}),
                    json!({"_id": "bad"}),
                ],
                true,
            )
            .await
            .unwrap();
        let answers: Vec<Value> = written.iter().map(WriteResult::json).collect();
        assert!(written[0].is_ok() && written[3].is_ok(), "{answers:?}");
        let corrupt = "corrupt metadata for bad: key must be a string at line 1 column 2; its \
                       revision tree cannot be validated, so the document cannot be changed: \
                       purge it with POST /{db}/_purge to repair it";
        for at in [1, 4] {
            assert_eq!(
                answers[at],
                json!({"id": "bad", "error": "internal_server_error", "reason": corrupt}),
                "{answers:?}"
            );
        }
        assert_eq!(answers[2]["id"], "crc", "{answers:?}");
        assert_eq!(answers[2]["error"], "internal_server_error", "{answers:?}");
        assert!(
            answers[2]["reason"]
                .as_str()
                .unwrap()
                .starts_with("Meta table: "),
            "{answers:?}"
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    async fn body_exists(db: &Db, id: &str, rev: &str) -> bool {
        let (id, rev) = (id.to_string(), rev.to_string());
        db.shards[0]
            .call(move |s| Ok(s.get_body(&id, &rev)?.is_some()))
            .await
            .unwrap()
    }

    /// A document whose metadata is corrupt cannot be overwritten, but
    /// `_purge` removes it outright, whatever revisions the request lists,
    /// and records the purge; the database is usable afterwards.
    #[tokio::test]
    async fn purge_removes_a_document_with_corrupt_metadata() {
        let (db, root) = purge_test_db("purge-corrupt", ShardMap::single(1));
        db.put_docs(vec![json!({"_id": "good"})], true)
            .await
            .unwrap();
        let rev = put_then_corrupt_meta(&db, "bad").await;

        let written = db
            .put_docs(vec![json!({"_id": "bad", "_rev": rev})], true)
            .await
            .unwrap();
        let reason = written[0].json()["reason"]
            .as_str()
            .unwrap_or("")
            .to_string();
        assert!(reason.contains("_purge"), "{reason}");

        let listed = "9-ffffffffffffffffffffffffffffffff";
        let purged = db
            .purge(&json!({"bad": [listed]}), 0, &Default::default())
            .await
            .unwrap();
        assert_eq!(purged["purged"]["bad"], json!([listed]));
        assert_eq!(db.shard_purge_seqs().await.unwrap().iter().sum::<u64>(), 1);
        assert!(!body_exists(&db, "bad", &rev).await);
        assert!(db.get_doc("bad", &DocOpts::default()).await.is_err());
        let all = db.all_docs(&AllDocsOpts::default()).await.unwrap();
        assert_eq!(row_ids(&all.json(), "rows"), ["good"]);
        let changes = db.changes(0, &ChangesOpts::default(), true).await.unwrap();
        assert_eq!(
            row_ids(&serde_json::to_value(&changes).unwrap(), "results"),
            ["good"]
        );
        assert_eq!(db.info().await.unwrap().doc_count, 1);

        // The id is free again and the database compacts.
        db.put_docs(vec![json!({"_id": "bad", "n": 1})], true)
            .await
            .unwrap();
        db.compact().await.unwrap();
        assert_eq!(db.info().await.unwrap().doc_count, 2);

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A compaction that panics must not leave its database compacting: it
    /// would report `compact_running` and never close again.
    #[tokio::test]
    async fn a_panicking_compaction_releases_its_claim() {
        let (db, root) = purge_test_db("compact-panic", ShardMap::single(1));
        let db = Arc::new(db);
        let held = Arc::clone(&db);
        let panicked = tokio::spawn(async move {
            let _claim = held.claim_compaction().expect("nothing else compacts");
            panic!("the compaction panicked");
        })
        .await;
        assert!(panicked.is_err());
        assert!(db.quiescent());
        assert!(!db.info().await.unwrap().compact_running);

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// One corrupt record must not stop compaction of the rest of the shard:
    /// the record and its bodies stay, the other documents are compacted.
    #[tokio::test]
    async fn compaction_skips_a_corrupt_record_and_compacts_the_rest() {
        let (db, root) = purge_test_db("compact-corrupt", ShardMap::single(1));
        let first = db
            .put_docs(vec![json!({"_id": "good"})], true)
            .await
            .unwrap();
        let first = first[0].json()["rev"].as_str().unwrap().to_string();
        db.put_docs(vec![json!({"_id": "good", "_rev": first, "n": 2})], true)
            .await
            .unwrap();
        let bad_rev = put_then_corrupt_meta(&db, "bad").await;
        assert!(body_exists(&db, "good", &first).await);

        let before = crate::metrics::corrupt_records_skipped();
        db.compact().await.unwrap();
        assert!(crate::metrics::corrupt_records_skipped() > before);
        assert!(!body_exists(&db, "good", &first).await);
        assert!(body_exists(&db, "bad", &bad_rev).await);

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A purge job reads and checks every document's metadata before its
    /// first write: a later document whose metadata fails its checksum fails
    /// that purge alone, and the other jobs of its group commit.
    #[tokio::test]
    async fn a_purge_reads_every_document_before_it_writes() {
        let (db, root) = purge_test_db("purge-reads-first", ShardMap::single(1));
        let written = db.put_docs(vec![json!({"_id": "a"})], true).await.unwrap();
        let rev = written[0].json()["rev"].as_str().unwrap().to_string();
        db.shards[0]
            .call(|s| {
                s.put_unsealed(crate::kv::META, "z", br#"{"tree":{},"seq":9}"#);
                // A job that wrote nothing through the shard commits nothing.
                Ok(s.put_local("setup", b"1")?)
            })
            .await
            .unwrap();

        // Hold the actor until the purge and a bystander are queued behind
        // it, so the three run as one group, the purge before the bystander.
        let (started_tx, started) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let blocker = db.shards[0].call(move |_| {
            let _ = started_tx.send(());
            held.recv().map_err(ApiError::internal)
        });
        // "a" comes first and loses its only leaf; "z" cannot be read.
        let body = json!({"a": [rev], "z": ["1-ffffffffffffffffffffffffffffffff"]});
        let uuids = std::collections::BTreeMap::new();
        let purge = db.purge(&body, 0, &uuids);
        let bystander = db.shards[0].call(|s| Ok(s.put_local("bystander", b"1")?));
        let releaser = async {
            started.await.unwrap();
            while db.shards[0].queued() < 2 {
                tokio::task::yield_now().await;
            }
            release.send(()).unwrap();
        };
        let (blocked, purged, bystood, ()) = tokio::join!(blocker, purge, bystander, releaser);

        let error = purged.expect_err("a purge over a damaged record succeeded");
        assert!(error.reason.contains("checksum"), "{}", error.reason);
        bystood.expect("the purge's failure aborted another job's write");
        blocked.unwrap();
        let local = db.shards[0]
            .read(|s| Ok(s.get_local("bystander")?))
            .await
            .unwrap();
        assert_eq!(local.as_deref(), Some(&b"1"[..]));
        // The failed purge wrote nothing: "a" is still there.
        assert!(db.get_doc("a", &DocOpts::default()).await.is_ok());
        assert_eq!(db.shard_purge_seqs().await.unwrap(), [0]);

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// The purge sequences `GET /db` reports are the shards' published
    /// ones, which match their tables; while a shard publishes the unknown
    /// placeholder, its table answers instead, never the placeholder.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shard_purge_seqs_never_report_the_unknown_placeholder() {
        let (db, root, switch) = db_on_test_disk("purgeseqs").await;
        let table = |db: &Db| db.shards[0].read(|s| Ok(s.purge_seq()?));
        let written = db
            .put_docs(vec![json!({"_id": "a"}), json!({"_id": "b"})], true)
            .await
            .unwrap();
        let revs: Vec<String> = written
            .iter()
            .map(|w| w.json()["rev"].as_str().unwrap().to_string())
            .collect();
        let body = |id: &str, rev: &str| json!({ id: [rev] });
        let none = Default::default();
        db.purge(&body("a", &revs[0]), 0, &none).await.unwrap();
        assert_eq!(table(&db).await.unwrap(), 1);
        assert_eq!(db.shard_purge_seqs().await.unwrap(), [1]);

        // A full disk: the purge fails, and so does the reopen after it, so
        // the shard publishes the placeholder.
        switch.fail_writes(std::io::ErrorKind::StorageFull);
        assert!(db.purge(&body("b", &revs[1]), 0, &none).await.is_err());
        assert_eq!(db.shards[0].purge_seq(), crate::store::UNKNOWN_PURGE_SEQ);
        assert!(
            db.shard_purge_seqs().await.is_err(),
            "a failed shard of unknown purge sequence answered"
        );

        switch.restore_writes();
        db.shards[0].set_retry_wait(std::time::Duration::ZERO);
        assert!(db.shards[0].read(|_| Ok(())).await.is_err());
        db.shards[0].barrier().await.unwrap().unwrap();
        assert_eq!(db.shards[0].state(), crate::store::ShardState::Serving);
        assert_eq!(
            db.shard_purge_seqs().await.unwrap(),
            [table(&db).await.unwrap()]
        );
        drop(db);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Hold shard 0's actor with a job that waits for the returned sender;
    /// returns once the job runs, with the job's answer.
    async fn hold_shard_0(
        db: &Db,
    ) -> (
        std::sync::mpsc::Sender<()>,
        tokio::task::JoinHandle<DbResult<()>>,
    ) {
        let (started_tx, started) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let blocker = tokio::spawn(db.shards[0].call(move |_| {
            let _ = started_tx.send(());
            held.recv().map_err(ApiError::internal)
        }));
        started.await.unwrap();
        (release, blocker)
    }

    /// Drop `request` once its shard job is queued behind the held shard 0,
    /// then let the job run and wait until the task the request left behind,
    /// if any, is done with the design caches.
    async fn drop_after_send<T: Send + 'static>(
        db: &Db,
        request: impl std::future::Future<Output = T> + Send + 'static,
    ) {
        let (release, blocker) = hold_shard_0(db).await;
        let request = tokio::spawn(request);
        while db.shards[0].queued() < 1 {
            tokio::task::yield_now().await;
        }
        request.abort();
        assert!(request.await.is_err_and(|error| error.is_cancelled()));
        release.send(()).unwrap();
        blocker.await.unwrap().unwrap();
        db.shards[0].barrier().await.unwrap().unwrap();
        while Arc::strong_count(&db.design) > 1 {
            tokio::task::yield_now().await;
        }
    }

    /// A design-document write whose request is dropped after its shard job
    /// was sent still commits, and the caches then serve the new validator
    /// and the new design document.
    #[tokio::test]
    async fn a_dropped_design_write_invalidates_the_design_caches_once_it_commits() {
        let (db, root) = purge_test_db("drop-ddoc-write", ShardMap::single(1));
        let db = Arc::new(db);
        let id = "_design/v";
        let old = "function(newDoc) { var old = 1; }";
        let new = "function(newDoc) { var new_ = 2; }";
        let written = db
            .put_docs(vec![json!({"_id": id, "validate_doc_update": old})], true)
            .await
            .unwrap();
        let rev = written[0].json()["rev"].as_str().unwrap().to_string();
        assert_eq!(db.validation_functions().await.unwrap(), [old]);
        assert_eq!(db.design_doc(id).await.unwrap()["validate_doc_update"], old);

        let writer = Arc::clone(&db);
        let update = json!({"_id": id, "_rev": rev, "validate_doc_update": new});
        drop_after_send(
            &db,
            async move { writer.put_docs(vec![update], true).await },
        )
        .await;

        assert_eq!(db.validation_functions().await.unwrap(), [new]);
        assert_eq!(db.design_doc(id).await.unwrap()["validate_doc_update"], new);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A purge of a design document whose request is dropped after its shard
    /// job was sent still lands, and the caches then hold neither its
    /// validator nor the document.
    #[tokio::test]
    async fn a_dropped_design_purge_invalidates_the_design_caches_once_it_lands() {
        let (db, root) = purge_test_db("drop-ddoc-purge", ShardMap::single(1));
        let db = Arc::new(db);
        let id = "_design/v";
        let source = "function(newDoc) { var old = 1; }";
        let written = db
            .put_docs(
                vec![json!({"_id": id, "validate_doc_update": source})],
                true,
            )
            .await
            .unwrap();
        let rev = written[0].json()["rev"].as_str().unwrap().to_string();
        assert_eq!(db.validation_functions().await.unwrap(), [source]);
        db.design_doc(id).await.unwrap();

        let purger = Arc::clone(&db);
        let body = json!({ id: [rev] });
        drop_after_send(&db, async move {
            purger.purge(&body, 0, &Default::default()).await
        })
        .await;

        assert!(db.validation_functions().await.unwrap().is_empty());
        let error = db.design_doc(id).await.unwrap_err();
        assert_eq!(error.status, 404, "{error:?}");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A shard cleanup whose request is dropped after its clear was sent
    /// still lands, and the caches then hold neither the validator nor the
    /// design document it removed.
    #[tokio::test]
    async fn a_dropped_shard_clear_invalidates_the_design_caches_once_it_lands() {
        let (db, root) = purge_test_db("drop-ddoc-clear", ShardMap::single(1));
        let db = Arc::new(db);
        let id = "_design/v";
        let source = "function(newDoc) { var old = 1; }";
        db.put_docs(
            vec![json!({"_id": id, "validate_doc_update": source})],
            true,
        )
        .await
        .unwrap();
        assert_eq!(db.validation_functions().await.unwrap(), [source]);
        db.design_doc(id).await.unwrap();

        let clearer = Arc::clone(&db);
        drop_after_send(&db, async move { clearer.clear_shards(&[0]).await }).await;

        assert!(db.validation_functions().await.unwrap().is_empty());
        let error = db.design_doc(id).await.unwrap_err();
        assert_eq!(error.status, 404, "{error:?}");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Purge `n` fresh documents on node 0; the `(id, rev)` pairs purged.
    async fn purge_docs_on(db: &Db, n: usize) -> std::collections::BTreeSet<(String, String)> {
        let mut revs = std::collections::BTreeSet::new();
        for _ in 0..n {
            let id = uuid();
            let written = db.put_docs(vec![json!({"_id": id})], true).await.unwrap();
            let rev = written[0].json()["rev"].as_str().unwrap().to_string();
            db.purge(
                &json!({ id.clone(): [rev.clone()] }),
                0,
                &Default::default(),
            )
            .await
            .unwrap();
            revs.insert((id, rev));
        }
        revs
    }

    /// A shard keeps only the newest `purged_infos_limit` purge infos, and
    /// forgets the revisions of the ones it drops.
    #[tokio::test]
    async fn old_purge_infos_are_trimmed_at_the_limit() {
        let (db, root) = purge_test_db("purge-trim", ShardMap::single(1));
        db.set_purged_infos_limit(3).await.unwrap();
        assert_eq!(db.get_purged_infos_limit().await.unwrap(), 3);
        let revs = purge_docs_on(&db, 5).await;
        let (purge_seq, infos) = db.purged_infos(&[0], None).await.unwrap().remove(0);
        assert_eq!(purge_seq, 5);
        assert_eq!(infos.len(), 3);
        let kept: std::collections::BTreeSet<(String, String)> = infos
            .into_iter()
            .map(|info| (info.id, info.revs[0].clone()))
            .collect();
        assert_eq!(db.purged_among(&revs).await.unwrap(), kept);

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A database holding a design document with a validator, the validation
    /// and design caches warm; returns the design document's revision.
    async fn db_with_warm_design_caches(name: &str) -> (Db, std::path::PathBuf, String) {
        let (db, root) = purge_test_db(name, ShardMap::single(1));
        let written = db
            .put_docs(
                vec![json!({
                    "_id": "_design/v",
                    "validate_doc_update": "function(){ throw({forbidden: 'no'}); }",
                })],
                true,
            )
            .await
            .unwrap();
        let rev = written[0].json()["rev"].as_str().unwrap().to_string();
        assert_eq!(db.validation_functions().await.unwrap().len(), 1);
        db.design_doc("_design/v").await.unwrap();
        (db, root, rev)
    }

    async fn assert_design_caches_forgotten(db: &Db) {
        assert!(
            db.validation_functions().await.unwrap().is_empty(),
            "the purged validator no longer runs"
        );
        assert!(
            db.design_doc("_design/v").await.is_err(),
            "the purged design document is no longer served"
        );
    }

    #[tokio::test]
    async fn a_local_purge_of_a_design_document_forgets_its_caches() {
        let (db, root, rev) = db_with_warm_design_caches("purge-design-local").await;
        db.purge(&json!({ "_design/v": [rev] }), 0, &Default::default())
            .await
            .unwrap();
        assert_design_caches_forgotten(&db).await;
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn a_replicated_purge_of_a_design_document_forgets_its_caches() {
        let (db, root, rev) = db_with_warm_design_caches("purge-design-repl").await;
        let info = PurgeInfo {
            uuid: uuid(),
            id: "_design/v".to_string(),
            revs: vec![rev],
        };
        db.apply_purge_infos(0, vec![info], 0).await.unwrap();
        assert_design_caches_forgotten(&db).await;
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Infos a replica has not confirmed are never trimmed, whatever the
    /// limit; once every other replica has confirmed them they can go.
    #[tokio::test]
    async fn purge_infos_are_kept_until_the_peers_confirm_them() {
        let (db, root) = purge_test_db("purge-hold", ShardMap::balanced(1, 2, 2));
        db.set_purged_infos_limit(2).await.unwrap();
        purge_docs_on(&db, 5).await;
        let infos = db.purged_infos(&[0], None).await.unwrap().remove(0).1;
        assert_eq!(infos.len(), 5, "node 1 never reported");

        // Node 1 confirms the first four; they stay until the next purge.
        db.purged_infos(&[4], Some(1)).await.unwrap();
        assert_eq!(db.purged_infos(&[0], None).await.unwrap()[0].1.len(), 5);
        purge_docs_on(&db, 1).await;
        let infos = db.purged_infos(&[0], None).await.unwrap().remove(0).1;
        assert_eq!(infos.len(), 2, "below the limit and confirmed: trimmed");

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Replay one golden case on a fresh database with `q` shards: each
    /// request's responses, then the stored body of every revision named by
    /// a request or a response, as text (`None` when not stored).
    async fn replay_golden(case: &Value, q: u32) -> (Vec<Value>, Vec<Value>) {
        let name = case["name"].as_str().unwrap();
        let root = std::env::temp_dir().join(format!("douchdb-golden-{name}-{}", uuid()));
        let db = Db::open_or_create(
            &root,
            name,
            DbConfig {
                q,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: ShardMap::single(q),
                partitioned: false,
            },
        )
        .unwrap();
        let mut responses = Vec::new();
        let mut named: Vec<(String, String)> = Vec::new();
        for request in case["requests"].as_array().unwrap() {
            let docs = request["docs"].as_array().unwrap().clone();
            for doc in &docs {
                if let (Some(id), Some(rev)) = (doc["_id"].as_str(), doc["_rev"].as_str()) {
                    named.push((id.to_string(), rev.to_string()));
                }
            }
            let new_edits = request["new_edits"].as_bool().unwrap();
            let out = db.put_docs(docs, new_edits).await.unwrap();
            let out: Vec<Value> = out.iter().map(WriteResult::json).collect();
            for result in &out {
                if let (Some(id), Some(rev)) = (result["id"].as_str(), result["rev"].as_str()) {
                    named.push((id.to_string(), rev.to_string()));
                }
            }
            responses.push(Value::Array(out));
        }
        let mut stored = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (id, rev) in named {
            if !seen.insert((id.clone(), rev.clone())) {
                continue;
            }
            let shard = db.shard_index(&id);
            let (doc, at) = (id.clone(), rev.clone());
            let body = db.shards[shard]
                .call(move |s| Ok(s.get_body(&doc, &at)?))
                .await
                .unwrap()
                .map(|bytes| String::from_utf8(bytes).unwrap());
            stored.push(json!({"id": id, "rev": rev, "body": body}));
        }
        drop(db);
        let _ = std::fs::remove_dir_all(root);
        (responses, stored)
    }

    /// `_bulk_docs` answers and stores exactly what main did when the
    /// fixture was captured: revision ids, responses, error precedence and
    /// the stored body bytes, at q=1 and q=2.
    #[tokio::test]
    async fn bulk_write_matches_golden() {
        let cases: Vec<Value> =
            serde_json::from_str(include_str!("../tests/golden/bulk_write.json")).unwrap();
        assert_eq!(cases.len(), 10);
        for case in &cases {
            for q in [1, 2] {
                let (responses, stored) = replay_golden(case, q).await;
                let name = &case["name"];
                assert_eq!(
                    Value::Array(responses),
                    case["responses"],
                    "{name} responses, q={q}"
                );
                // As text, so the comparison is of the stored bytes.
                assert_eq!(
                    serde_json::to_string(&stored).unwrap(),
                    serde_json::to_string(&case["stored_bodies"]).unwrap(),
                    "{name} stored bodies, q={q}"
                );
            }
        }
    }

    /// A batch large enough to be prepared on the blocking pool keeps each
    /// result in its document's slot, a document that fails to prepare
    /// included, and a document without `_id` is stored under the id its
    /// result names.
    #[tokio::test]
    async fn large_batches_keep_their_order() {
        let root = std::env::temp_dir().join(format!("douchdb-large-batch-{}", uuid()));
        let db = Db::open_or_create(
            &root,
            "large",
            DbConfig {
                q: 2,
                revs_limit: 100,
                cache_bytes: crate::state::DEFAULT_CACHE_MB << 20,
                shard_map: ShardMap::single(2),
                partitioned: false,
            },
        )
        .unwrap();
        let count = PREPARE_INLINE_MAX + 2;
        let mut docs: Vec<Value> = (0..count)
            .map(|i| json!({"_id": format!("d{i:05}"), "n": i}))
            .collect();
        docs[7] = json!({"_id": "bad", "_rev": 5});
        docs[8] = json!({"n": 8});
        let blocking = BLOCKING_PREPARES.load(Ordering::SeqCst);
        let out = db.put_docs(docs, true).await.unwrap();
        assert!(BLOCKING_PREPARES.load(Ordering::SeqCst) > blocking);
        assert_eq!(out.len(), count);
        for (i, result) in out.iter().enumerate() {
            let result = serde_json::to_value(result).unwrap();
            match i {
                7 => {
                    assert_eq!(result["id"], "bad", "{result}");
                    assert_eq!(result["reason"], "Invalid rev format", "{result}");
                }
                8 => {
                    let id = result["id"].as_str().unwrap();
                    let doc = db.get_doc(id, &DocOpts::default()).await.unwrap();
                    assert_eq!(doc["n"], 8);
                }
                _ => assert_eq!(result["id"], format!("d{i:05}"), "{result}"),
            }
        }
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A write counts its shard group, the time spent in its jobs and its
    /// commit, and the commit's two syncs (two-phase). The counters are
    /// process-wide, so the test reruns itself alone in a child process:
    /// no other test's commits land between the snapshots.
    #[tokio::test]
    async fn shard_metrics_count_groups_and_syncs() {
        const CHILD: &str = "DOUCHDB_SHARD_METRICS_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "db::tests::shard_metrics_count_groups_and_syncs",
                    "--test-threads=1",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success() && stdout.contains("1 passed"),
                "{stdout}{}",
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        let (db, root) = single_shard_db("shard-metrics").await;
        // The first commit grows the new file, and redb syncs a growth
        // once more; the second is a plain two-phase commit.
        db.put_docs(vec![json!({"_id": "a", "n": 1})], true)
            .await
            .unwrap();
        let before = crate::metrics::snapshot()["shard"].clone();
        db.put_docs(vec![json!({"_id": "b", "n": 1})], true)
            .await
            .unwrap();
        let after = crate::metrics::snapshot()["shard"].clone();
        let delta = |name: &str| after[name].as_u64().unwrap() - before[name].as_u64().unwrap();
        assert!(delta("groups") >= 1, "{before} {after}");
        assert_eq!(delta("syncs"), 2 * delta("groups"), "{before} {after}");
        assert!(delta("jobs_us") > 0, "{before} {after}");
        assert!(delta("commit_us") > 0, "{before} {after}");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }
}

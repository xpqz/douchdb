//! Database and document semantics: revisions, conflicts, changes feeds.
//!
//! All writes for one request are staged as an ordered list of (key, value)
//! operations and applied as a single LevelDB `WriteBatch`, so a `_bulk_docs`
//! of a thousand documents costs one OS flush, not a thousand. rusty-leveldb
//! does not fsync, so this is process-crash durability, not power-loss
//! durability.

use crate::cluster::{ShardMap, ShardMask};
pub use crate::error::{ApiError, DbResult};
use crate::rev::{new_revid, Attachment, Rev};
use crate::store::{
    blob_prefix, body_key, local_key, meta_key, meta_state, seq_key, AttMeta, DocMeta, LiveDoc,
    MetaState, ShardHandle, Tx,
};
use crate::util::{hex, uuid};
use base64::Engine;
use md5::{Digest, Md5};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Fields CouchDB strips before hashing a document body.
pub const META_FIELDS: [&str; 9] = [
    "_id",
    "_rev",
    "_revisions",
    "_deleted",
    "_attachments",
    "_conflicts",
    "_deleted_conflicts",
    "_revs_info",
    "_local_seq",
];

#[derive(serde::Serialize, serde::Deserialize)]
struct DbFile {
    q: u32,
    uuid: String,
    instance_start_time: String,
    #[serde(default)]
    partitioned: bool,
}

/// Staged operations for one atomic commit.
type Ops = Vec<(Vec<u8>, Option<Vec<u8>>)>;

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

/// Take the next global sequence for a write staged on one shard. The shard
/// is marked in flight (`reserved > committed`) *before* the counter moves,
/// so a changes snapshot that sees the new counter also sees the shard as
/// in flight. Committed sequences never exceed the counter, so `counter + 1`
/// is enough to mark it.
fn allocate_seq(counter: &AtomicU64, reserved: &AtomicU64) -> u64 {
    reserved.fetch_max(counter.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    let seq = counter.fetch_add(1, Ordering::SeqCst) + 1;
    reserved.fetch_max(seq, Ordering::SeqCst);
    seq
}

/// Pure form of the changes watermark rule: only shards with an allocated but
/// uncommitted sequence constrain the cursor, and the cursor never goes below
/// `since`.
pub(crate) fn changes_watermark_from(
    committed: &[u64],
    reserved: &[u64],
    since: u64,
    owned: Option<&ShardMask>,
) -> u64 {
    let mut safe = u64::MAX;
    for index in 0..committed.len().min(reserved.len()) {
        if !shard_owned(owned, index) {
            continue;
        }
        if reserved[index] > committed[index] {
            safe = safe.min(committed[index].max(since));
        }
    }
    safe
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

/// Log a metadata file that cannot be used and return the 500 for it. The
/// client sees the file name only; the log carries the full path.
fn metadata_error(path: &Path, error: impl std::fmt::Display) -> ApiError {
    tracing::error!(file = %path.display(), %error, "database metadata is unusable");
    ApiError::new(
        500,
        "internal_server_error",
        format!("{}: {error}", file_name(path)),
    )
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Read and parse one metadata file. `None` means it does not exist; one that
/// cannot be read or parsed is a 500, never a default.
fn read_metadata<T: serde::de::DeserializeOwned>(path: &Path) -> DbResult<Option<T>> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| metadata_error(path, error)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(metadata_error(path, error)),
    }
}

fn clear_error(path: &Path, error: &dyn std::fmt::Display) -> ApiError {
    ApiError::new(
        500,
        "internal_server_error",
        format!("{}: {error}", file_name(path)),
    )
}

/// Serialize `value` and replace `path` with it atomically; see
/// [`write_atomic`].
fn write_json_atomic<T: serde::Serialize>(path: &Path, value: &T) -> DbResult<()> {
    let write_error = |error: &dyn std::fmt::Display| {
        tracing::error!(file = %path.display(), %error, "cannot write database metadata");
        ApiError::new(
            500,
            "internal_server_error",
            format!("{}: {error}", file_name(path)),
        )
    };
    let bytes = serde_json::to_vec(value).map_err(|error| write_error(&error))?;
    write_atomic(path, &bytes).map_err(|error| write_error(&error))
}

/// Temporary files of writes in flight in this process. Cleanup of stale
/// temporary files must not remove one of these.
static LIVE_TEMPS: std::sync::Mutex<Option<std::collections::HashSet<PathBuf>>> =
    std::sync::Mutex::new(None);

fn live_temps() -> std::sync::MutexGuard<'static, Option<std::collections::HashSet<PathBuf>>> {
    LIVE_TEMPS
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

/// A temporary file being written; it counts as in flight until dropped,
/// which is after the caller has renamed or removed it.
struct LiveTemp(PathBuf);

impl LiveTemp {
    fn register(path: PathBuf) -> LiveTemp {
        live_temps()
            .get_or_insert_with(Default::default)
            .insert(path.clone());
        LiveTemp(path)
    }
}

impl Drop for LiveTemp {
    fn drop(&mut self) {
        if let Some(set) = live_temps().as_mut() {
            set.remove(&self.0);
        }
    }
}

/// Whether `name` is one `write_temp` produces: `.<name>.<pid>.<n>.tmp`.
fn is_temp_name(name: &str) -> bool {
    let Some(stem) = name.strip_prefix('.').and_then(|n| n.strip_suffix(".tmp")) else {
        return false;
    };
    let mut parts = stem.rsplitn(3, '.');
    let numeric = |part: Option<&str>| {
        part.is_some_and(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
    };
    let (counter, pid, target) = (parts.next(), parts.next(), parts.next());
    numeric(counter) && numeric(pid) && target.is_some_and(|t| !t.is_empty())
}

/// Remove the temporary files of interrupted writes from `dir` (not its
/// subdirectories). Only names matching [`is_temp_name`] go, and never one a
/// write in this process is still using, so this is safe while the directory
/// is in use; whether the pid in a name is alive is not considered. A file
/// that cannot be removed is logged, not an error.
pub(crate) fn remove_stale_temps(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        if !name.to_str().is_some_and(is_temp_name) {
            continue;
        }
        if !entry.file_type().is_ok_and(|kind| kind.is_file()) {
            continue;
        }
        let path = entry.path();
        // Hold the registry while removing: a write registers its file before
        // creating it, so a file that is not registered here is not in use.
        let live = live_temps();
        if live.as_ref().is_some_and(|set| set.contains(&path)) {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => tracing::info!(file = %path.display(), "removed stale temporary file"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(file = %path.display(), %error, "cannot remove stale temporary file")
            }
        }
    }
}

/// Write `bytes` to a temporary file next to `path` and fsync it. A crash
/// before the caller publishes it leaves only the temporary file behind.
fn write_temp(path: &Path, bytes: &[u8], private: bool) -> std::io::Result<LiveTemp> {
    use std::io::Write;
    static TEMPS: AtomicU64 = AtomicU64::new(0);
    let temp = LiveTemp::register(path.with_file_name(format!(
        ".{}.{}.{}.tmp",
        file_name(path),
        std::process::id(),
        TEMPS.fetch_add(1, Ordering::Relaxed)
    )));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    #[cfg(not(unix))]
    let _ = private;
    let written = options.open(&temp.0).and_then(|mut file| {
        file.write_all(bytes)?;
        file.sync_all()
    });
    match written {
        Ok(()) => Ok(temp),
        Err(error) => {
            let _ = std::fs::remove_file(&temp.0);
            Err(error)
        }
    }
}

/// Make a rename or link in the directory of `path` durable.
fn sync_parent(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let dir = path
            .parent()
            .filter(|dir| !dir.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

/// Replace `path` with `bytes` so that a reader, or a restart after a crash,
/// sees the old contents or the new ones, never a partial file: write a
/// temporary file in the same directory, fsync it, rename it over the target,
/// then fsync the directory.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let temp = write_temp(path, bytes, false)?;
    if let Err(error) = std::fs::rename(&temp.0, path) {
        let _ = std::fs::remove_file(&temp.0);
        return Err(error);
    }
    sync_parent(path)
}

/// Create `path` with `bytes`, readable by the owner only, failing with
/// `AlreadyExists` if it is there already. The file appears whole or not at
/// all: it is written to a temporary file and hard-linked into place.
pub(crate) fn create_private_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let temp = write_temp(path, bytes, true)?;
    let linked = std::fs::hard_link(&temp.0, path);
    let _ = std::fs::remove_file(&temp.0);
    linked?;
    sync_parent(path)
}

/// Read the stored security object. Databases written before security was
/// versioned keep a bare object in `security.json`; that is version 0. Neither
/// file existing is an empty object; a file that is there but unusable is an
/// error, since falling back to `{}` would make the database public.
fn load_security(path: &Path) -> DbResult<(u64, Value)> {
    let versioned = path.join("security-versioned.json");
    if let Some(stored) = read_metadata::<Value>(&versioned)? {
        return match (
            stored.get("version").and_then(Value::as_u64),
            stored.get("security"),
        ) {
            (Some(version), Some(security)) => Ok((version, security.clone())),
            _ => Err(metadata_error(
                &versioned,
                "expected an object with `version` and `security`",
            )),
        };
    }
    let legacy = read_metadata::<Value>(&path.join("security.json"))?;
    Ok((0, legacy.unwrap_or_else(|| json!({}))))
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

/// A rebalance target and the id of the coordinator that announced it. The id
/// is absent when an older coordinator announced it.
#[derive(PartialEq)]
struct Migration {
    map: ShardMap,
    id: Option<String>,
}

pub struct Db {
    pub name: String,
    pub path: PathBuf,
    pub q: u32,
    pub partitioned: bool,
    pub uuid: String,
    pub instance_start_time: String,
    shard_map: std::sync::RwLock<ShardMap>,
    security: std::sync::RwLock<(u64, Value)>,
    /// Visible as `compact_running` in `GET /db`; the shard actor still owns
    /// the actual compaction and blocks its shard while it runs.
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
    /// Last committed sequence per shard, shared with the shard actors.
    committed_seq: Vec<Arc<AtomicU64>>,
    /// Highest sequence allocated but not necessarily committed per shard.
    reserved_seq: Vec<Arc<AtomicU64>>,
    /// One global sequence counter for the database. CouchDB's `since` is a
    /// database-wide cursor, so shard-local counters cannot be used directly.
    seq: Arc<AtomicU64>,
    /// Pinged by the shard actors after a flushed batch that committed a new
    /// sequence; feeds wait on it instead of polling.
    commits: Arc<tokio::sync::watch::Sender<()>>,
    /// Cached `validate_doc_update` sources. Invalidated when a design
    /// document is written.
    validation: std::sync::RwLock<Option<Vec<String>>>,
    /// Bumped on every design-document write, so a validator load that raced
    /// with one is not cached.
    design_generation: AtomicU64,
    /// Set when the database is deleted; see `mark_deleted`.
    deleted: AtomicBool,
}

#[derive(Clone)]
pub struct DbConfig {
    pub q: u32,
    pub revs_limit: u64,
    pub shard_map: ShardMap,
    pub partitioned: bool,
}

/// Upper bound for a cluster-wide rebalance write freeze. If the coordinator
/// disappears, peers resume writes after this window instead of staying frozen.
/// Default for `--rebalance-freeze-ms`.
pub const DEFAULT_REBALANCE_FREEZE_MS: u64 = 420_000;

static REBALANCE_FREEZE_MS: AtomicU64 = AtomicU64::new(DEFAULT_REBALANCE_FREEZE_MS);

/// Set the freeze bound (`--rebalance-freeze-ms`). Every freeze on this node,
/// whether it started the rebalance or accepted a peer's announcement, uses it.
pub fn set_rebalance_freeze_ms(ms: u64) {
    REBALANCE_FREEZE_MS.store(ms.max(1), Ordering::SeqCst);
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

/// Whether `segment` could be one '/'-separated part of a database name.
fn is_db_name_segment(segment: &str) -> bool {
    !segment.is_empty()
        && !segment.starts_with('_')
        && segment.bytes().all(|b| {
            b.is_ascii_lowercase()
                || b.is_ascii_digit()
                || matches!(b, b'_' | b'-' | b'$' | b'+' | b'(' | b')')
        })
}

fn collect_nested_dbs(dir: &Path, rel: &[String], found: &mut Vec<(Vec<String>, PathBuf)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(segment) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if !is_db_name_segment(&segment) || !path.is_dir() {
            continue;
        }
        let mut parts = rel.to_vec();
        parts.push(segment);
        if parts.len() > 1 && path.join("db.json").is_file() {
            found.push((parts.clone(), path.clone()));
        }
        collect_nested_dbs(&path, &parts, found);
    }
}

/// Move databases that an older version stored as nested directories
/// (`<root>/a/b`) to their flat directory (`<root>/a%2Fb`). Deepest first, so
/// moving `a/b` never carries `a/b/c` along unmoved. Every target is checked
/// before anything moves, and an existing target is never overwritten: the
/// error names both directories and the data root is left as it was. Returns
/// the moves made as `(from, to)`.
pub fn migrate_nested_dbs(root: &Path) -> Result<Vec<(PathBuf, PathBuf)>, String> {
    let mut found = Vec::new();
    collect_nested_dbs(root, &[], &mut found);
    found.sort_by(|a, b| b.0.len().cmp(&a.0.len()).then_with(|| a.0.cmp(&b.0)));
    let mut plan = Vec::new();
    for (parts, from) in found {
        let to = db_path(root, &parts.join("/"));
        if to.exists() {
            return Err(format!(
                "cannot migrate database {}: {} is already there; move one of {} or {} away",
                parts.join("/"),
                to.display(),
                from.display(),
                to.display()
            ));
        }
        plan.push((from, to));
    }
    let mut moved = Vec::new();
    for (from, to) in plan {
        std::fs::rename(&from, &to).map_err(|e| {
            format!(
                "cannot move database directory {} to {}: {e}",
                from.display(),
                to.display()
            )
        })?;
        tracing::info!(from = %from.display(), to = %to.display(), "migrated nested database directory");
        // Drop the parent directories the move left empty; one that still
        // holds anything (a database, another file) is not ours to remove.
        let mut parent = from.parent();
        while let Some(dir) = parent.filter(|dir| *dir != root) {
            if std::fs::remove_dir(dir).is_err() {
                break;
            }
            parent = dir.parent();
        }
        moved.push((from, to));
    }
    Ok(moved)
}

impl Db {
    pub fn open(root: &Path, name: &str, config: DbConfig) -> DbResult<Db> {
        let path = db_path(root, name);
        remove_stale_temps(&path);
        if let Ok(indexes) = std::fs::read_dir(path.join("_search_indexes")) {
            for index in indexes.flatten() {
                remove_stale_temps(&index.path());
            }
        }
        let db_file = path.join("db.json");
        let file: DbFile = match read_metadata(&db_file)? {
            Some(file) => file,
            None => {
                std::fs::create_dir_all(&path)
                    .map_err(|e| ApiError::new(500, "internal_server_error", e.to_string()))?;
                let file = DbFile {
                    q: config.q.max(1),
                    uuid: uuid(),
                    instance_start_time: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_micros()
                        .to_string(),
                    partitioned: config.partitioned,
                };
                write_json_atomic(&db_file, &file)?;
                file
            }
        };

        let map_path = path.join("shard-map.json");
        let stored_map: Option<ShardMap> = read_metadata(&map_path)?;
        if let Some(map) = &stored_map {
            if !map.is_valid(file.q, map.node_count) {
                return Err(metadata_error(&map_path, "not a valid shard map"));
            }
        }
        let shard_map = match stored_map {
            Some(map) if map.node_count == config.shard_map.node_count => map,
            stored => {
                // A legacy database has no shard map yet, and one stored for a
                // different cluster size no longer applies: start from the
                // configured placement and persist it.
                if stored.is_some() {
                    tracing::warn!(
                        file = %map_path.display(),
                        "shard map is for a different cluster size; replacing it"
                    );
                }
                let map = if config.shard_map.q == file.q {
                    config.shard_map.clone()
                } else {
                    ShardMap::single(file.q)
                };
                write_json_atomic(&map_path, &map)?;
                map
            }
        };

        let mut shards = Vec::new();
        let mut committed_seq = Vec::with_capacity(file.q as usize);
        let mut reserved_seq = Vec::with_capacity(file.q as usize);
        let commits = Arc::new(tokio::sync::watch::Sender::new(()));
        for i in 0..file.q {
            let shard = ShardHandle::spawn(
                path.join(format!("shard-{i:04}")),
                config.revs_limit,
                commits.clone(),
            )?;
            committed_seq.push(shard.seq_tracker());
            reserved_seq.push(shard.reserved_seq());
            shards.push(shard);
        }

        let seq = shards
            .iter()
            .map(ShardHandle::initial_seq)
            .max()
            .unwrap_or(0);
        let security = load_security(&path)?;

        Ok(Db {
            name: name.to_string(),
            path,
            q: file.q.max(1),
            partitioned: file.partitioned,
            uuid: file.uuid,
            instance_start_time: file.instance_start_time,
            shard_map: std::sync::RwLock::new(shard_map),
            security: std::sync::RwLock::new(security),
            compact_running: AtomicBool::new(false),
            migration_map: std::sync::RwLock::new(None),
            write_gate: Arc::new(tokio::sync::RwLock::new(())),
            rebalance_until: AtomicU64::new(0),
            shards,
            committed_seq,
            reserved_seq,
            seq: Arc::new(AtomicU64::new(seq)),
            commits,
            validation: std::sync::RwLock::new(None),
            design_generation: AtomicU64::new(0),
            deleted: AtomicBool::new(false),
        })
    }

    /// Open a database that is already on disk. Unlike `open`, a missing
    /// database is an error and nothing is created, so a request that races a
    /// drop cannot bring the database back.
    pub fn open_existing(root: &Path, name: &str, config: DbConfig) -> DbResult<Db> {
        if !Db::exists(root, name) {
            return Err(ApiError::db_not_found());
        }
        Db::open(root, name, config)
    }

    pub fn exists(root: &Path, name: &str) -> bool {
        db_path(root, name).join("db.json").is_file()
    }

    /// The security object of a database that need not be open: what its
    /// directory holds, which is what an open one holds in memory too, since
    /// it is written before it is installed.
    pub fn read_security(root: &Path, name: &str) -> DbResult<Value> {
        if !Db::exists(root, name) {
            return Err(ApiError::db_not_found());
        }
        load_security(&db_path(root, name)).map(|(_, security)| security)
    }

    /// What `open` would settle on for placement, read from the directory
    /// without opening the database. `None` when `open` would have to rewrite
    /// the shard map (none stored, or one for a different cluster size), so
    /// the caller must open the database instead.
    pub fn read_closed_meta(
        root: &Path,
        name: &str,
        node_count: usize,
    ) -> DbResult<Option<ClosedMeta>> {
        let path = db_path(root, name);
        let Some(file) = read_metadata::<DbFile>(&path.join("db.json"))? else {
            return Err(ApiError::db_not_found());
        };
        let map_path = path.join("shard-map.json");
        let Some(shard_map) = read_metadata::<ShardMap>(&map_path)? else {
            return Ok(None);
        };
        if shard_map.node_count != node_count || !shard_map.is_valid(file.q, shard_map.node_count) {
            return Ok(None);
        }
        Ok(Some(ClosedMeta {
            q: file.q.max(1),
            partitioned: file.partitioned,
            shard_map,
            pending_shard_map: Db::has_pending_shard_map(root, name)?,
        }))
    }

    /// Whether a shard map this node has not acknowledged is waiting on disk.
    pub fn has_pending_shard_map(root: &Path, name: &str) -> DbResult<bool> {
        Ok(
            read_metadata::<ShardMap>(&db_path(root, name).join("pending-shard-map.json"))?
                .is_some(),
        )
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

    pub fn destroy(root: &Path, name: &str) -> DbResult<()> {
        match std::fs::remove_dir_all(db_path(root, name)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(ApiError::db_not_found())
            }
            Err(error) => Err(metadata_error(&db_path(root, name), error)),
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

    pub async fn partition_counts(&self, partition: &str) -> DbResult<(u64, u64)> {
        let shard = &self.shards[self.partition_shard(partition)];
        let partition = partition.to_string();
        shard
            .call(move |s| s.partition_counts(&partition).map_err(ApiError::from))
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
            let (mut found, corrupt) = shard.call(|s| Ok(s.design_docs()?)).await?;
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
                .call(move |s| {
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
            .validation
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        if self.design_generation() == generation {
            *cached = Some(functions.clone());
        }
        Ok(functions)
    }

    /// Generation of this node's design documents.
    pub(crate) fn design_generation(&self) -> u64 {
        self.design_generation.load(Ordering::SeqCst)
    }

    /// Replace the security object with a client-supplied one, one version
    /// newer than the current one. Returns the version it was stored at.
    pub fn set_security(&self, value: Value) -> DbResult<u64> {
        // Pick the version under the lock: two concurrent writes on this node
        // must not share a version, or the tiebreak could drop an acknowledged
        // one.
        let mut current = self
            .security
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        let version = current.0.saturating_add(1);
        self.store_security(&mut current, version, value)?;
        Ok(version)
    }

    /// Store a security object at `version` unless the current one is newer.
    /// Equal versions are ordered by the canonical form of the object so that
    /// concurrent updates settle on the same winner on every node. Returns
    /// whether the object is now the stored one.
    pub fn set_security_versioned(&self, version: u64, value: Value) -> DbResult<bool> {
        let mut current = self
            .security
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
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
        self.store_security(&mut current, version, value)?;
        Ok(true)
    }

    /// Persist and install a security object; the caller holds the lock.
    fn store_security(
        &self,
        current: &mut (u64, Value),
        version: u64,
        value: Value,
    ) -> DbResult<()> {
        write_json_atomic(
            &self.path.join("security-versioned.json"),
            &json!({"version": version, "security": value}),
        )?;
        *current = (version, value);
        Ok(())
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
    fn cluster_info(&self) -> Value {
        let n = self.copies();
        json!({"q": self.q, "n": n, "w": n / 2 + 1, "r": n / 2 + 1})
    }

    pub fn set_shard_map(&self, shard_map: ShardMap) -> DbResult<bool> {
        if !shard_map.is_valid(self.q, shard_map.node_count) {
            return Err(ApiError::bad_request("invalid shard map"));
        }
        // Hold the lock across compare, persist and swap, so two calls on
        // this node cannot interleave and leave the file and the in-memory
        // map disagreeing.
        let mut current = self
            .shard_map
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
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
        self.store_shard_map(&mut current, shard_map)?;
        Ok(true)
    }

    /// Store a shard map placed by a client, one version newer than the
    /// current one, and return it with that version. The version is picked
    /// under the lock, as `set_security` does, so two concurrent updates on
    /// this node never share one.
    pub fn set_next_shard_map(&self, mut shard_map: ShardMap) -> DbResult<ShardMap> {
        if !shard_map.is_valid(self.q, shard_map.node_count) {
            return Err(ApiError::bad_request("invalid shard map"));
        }
        let mut current = self
            .shard_map
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        shard_map.version = current.version.saturating_add(1);
        self.store_shard_map(&mut current, shard_map.clone())?;
        Ok(shard_map)
    }

    /// Persist and install a shard map; the caller holds the lock.
    fn store_shard_map(&self, current: &mut ShardMap, shard_map: ShardMap) -> DbResult<()> {
        write_json_atomic(&self.path.join("shard-map.json"), &shard_map)?;
        *current = shard_map;
        Ok(())
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

    pub fn set_migration_map(&self, map: ShardMap, id: Option<String>) -> DbResult<()> {
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
    pub fn begin_migration(&self, map: ShardMap, id: Option<String>) -> DbResult<()> {
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
    /// another coordinator's migration is in progress. Nothing to clear, an
    /// expired freeze (the coordinator vanished) and a migration announced
    /// without an id (an older coordinator) are all cleared by any caller.
    pub fn clear_migration_if(&self, id: Option<&str>) -> bool {
        let mut current = self
            .migration_map
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        let Some(migration) = current.as_ref() else {
            return true;
        };
        if self.rebalance_in_progress() && migration.id.is_some() && migration.id.as_deref() != id {
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

    /// Highest sequence that is safe for a changes cursor to publish, taken
    /// as a snapshot **before** reading any shard.
    ///
    /// Sequence numbers are allocated while a shard job is staging, but only
    /// become visible when that job commits. Different shard actors run
    /// concurrently, so another shard may publish a higher sequence first. A
    /// numeric `last_seq` must not advance past a shard's committed sequence
    /// while that shard has an allocated-but-uncommitted sequence; otherwise
    /// the next request skips the later-visible row.
    ///
    /// Every sequence up to the bound is committed when the snapshot is
    /// taken, so a read that starts afterwards sees all of them. The bound is
    /// also capped at the global counter: anything allocated after the
    /// snapshot is higher and is left for the next request. (Computing this
    /// after the read is unsafe: a shard read before its commit, but
    /// committed by the time of the check, no longer holds the cursor back.)
    pub fn changes_watermark(&self, since: u64, owned: Option<&ShardMask>) -> u64 {
        // Load the counter first: an allocation it includes has already marked
        // its shard in flight (see `Stage::next_seq`).
        let allocated = self.seq.load(Ordering::SeqCst);
        let committed: Vec<u64> = self
            .committed_seq
            .iter()
            .map(|seq| seq.load(Ordering::SeqCst))
            .collect();
        let reserved: Vec<u64> = self
            .reserved_seq
            .iter()
            .map(|seq| seq.load(Ordering::SeqCst))
            .collect();
        changes_watermark_from(&committed, &reserved, since, owned).min(allocated)
    }

    /// The shard map this node has not acknowledged yet. `None` when there is
    /// none; one that cannot be read or parsed is an error.
    pub fn pending_shard_map(&self) -> DbResult<Option<ShardMap>> {
        read_metadata(&self.path.join("pending-shard-map.json"))
    }

    pub fn set_pending_shard_map(&self, map: &ShardMap) -> DbResult<()> {
        write_json_atomic(&self.path.join("pending-shard-map.json"), map)
    }

    /// Remove the pending shard map and make the removal durable. Already
    /// gone is fine; any other failure is an error, since the map would
    /// otherwise reappear or be retried as if still pending.
    pub fn clear_pending_shard_map(&self) -> DbResult<()> {
        let path = self.path.join("pending-shard-map.json");
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(clear_error(&path, &error)),
        }
        sync_parent(&path).map_err(|error| clear_error(&path, &error))
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
        for &index in shards {
            let Some(shard) = self.shards.get(index) else {
                continue;
            };
            shard.call(|s| s.clear().map_err(ApiError::from)).await?;
        }
        Ok(())
    }

    pub async fn shard_counts(&self) -> DbResult<Vec<(u64, u64)>> {
        let mut out = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            out.push(shard.call(|s| Ok(s.counts())).await?);
        }
        Ok(out)
    }

    pub async fn shard_states(&self, with_digest: bool) -> DbResult<Vec<crate::store::ShardState>> {
        let mut out = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            out.push(
                shard
                    .call(move |s| s.state(with_digest).map_err(ApiError::from))
                    .await?,
            );
        }
        Ok(out)
    }

    fn shard(&self, docid: &str) -> &ShardHandle {
        &self.shards[self.shard_index(docid)]
    }

    /// The update sequence a client may be shown: the changes watermark, so
    /// every write at or below it is committed. The allocated counter can be
    /// ahead of it (a write in flight has already taken its sequence); a
    /// client that resumed `_changes` from the counter would skip that write
    /// once it commits.
    pub fn update_seq(&self) -> u64 {
        self.changes_watermark(0, None)
    }

    /// A receiver that changes whenever a write commits on this node. Mark it
    /// seen (`borrow_and_update`) before reading, so a commit that lands
    /// during the read is not missed.
    pub fn subscribe_commits(&self) -> tokio::sync::watch::Receiver<()> {
        self.commits.subscribe()
    }

    /// Allocate a sequence on `shard` as a write still in flight would: the
    /// counter has moved but the shard has not committed it. Any later job on
    /// that shard counts it as committed, as for a write that never finished,
    /// so a test must not touch the shard again before the code under test.
    #[cfg(test)]
    pub(crate) fn start_in_flight_write(&self, shard: usize) -> u64 {
        allocate_seq(&self.seq, &self.reserved_seq[shard])
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

    pub async fn info(&self) -> DbResult<Value> {
        // Snapshot the flag before queueing on the shard actors. A compaction
        // holds its shard's actor while it runs, so reading the flag afterwards
        // would block and report the post-compaction state instead.
        let compact_running = self.compact_running.load(Ordering::SeqCst);
        // Taken before reading the shards: the highest committed sequence of
        // any one shard can sit above another shard's uncommitted write.
        let update_seq = self.update_seq();
        let mut doc_count = 0;
        let mut deleted_count = 0;
        let counts = futures::future::join_all(
            self.shards
                .iter()
                .map(|shard| shard.call(|s| Ok(s.counts()))),
        )
        .await;
        for counted in counts {
            let (docs, deleted) = counted?;
            doc_count += docs;
            deleted_count += deleted;
        }
        let mut info = json!({
            "db_name": self.name,
            "doc_count": doc_count,
            "doc_del_count": deleted_count,
            "update_seq": update_seq,
            "purge_seq": 0,
            "compact_running": compact_running,
            "disk_size": 0,
            "data_size": 0,
            "sizes": { "active": 0, "external": 0, "file": 0 },
            "instance_start_time": self.instance_start_time,
            "disk_format_version": 8,
            "committed_update_seq": update_seq,
            "uuid": self.uuid,
            "cluster": self.cluster_info(),
        });
        if self.partitioned {
            info["props"] = json!({"partitioned": true});
        }
        Ok(info)
    }

    // ----- documents -------------------------------------------------------

    pub async fn put_docs(&self, docs: Vec<Value>, new_edits: bool) -> DbResult<Vec<Value>> {
        if docs.iter().any(|doc| doc.get("_attachments").is_some()) {
            return Err(attachments_unsupported());
        }
        let touches_design = docs.iter().any(|doc| {
            doc.get("_id")
                .and_then(Value::as_str)
                .map(|id| id.starts_with("_design/"))
                .unwrap_or(false)
        });
        let mut out: Vec<Option<Value>> = vec![None; docs.len()];
        let mut grouped: Vec<Vec<(usize, Value)>> = (0..self.q).map(|_| Vec::new()).collect();
        let mut ids: Vec<Option<String>> = vec![None; docs.len()];

        for (i, doc) in docs.into_iter().enumerate() {
            match doc_id(&doc) {
                Ok(id) => match self.validate_doc_id(&id) {
                    Ok(()) => {
                        ids[i] = Some(id.clone());
                        grouped[self.shard_index(&id)].push((i, doc));
                    }
                    Err(e) => {
                        out[i] = Some(json!({
                            "id": id, "error": e.error, "reason": e.reason
                        }))
                    }
                },
                Err(e) => out[i] = Some(json!({"error": e.error, "reason": e.reason})),
            }
        }

        // A shard error is held back until the validator cache is dropped:
        // earlier shards may already have committed design documents.
        // The shards are written one after another: each document takes the
        // next global sequence when its shard stages it, so the order the
        // shards run in is the order of `_changes` for this batch.
        let mut shard_error = None;
        for (shard_idx, batch) in grouped.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let seq = self.seq.clone();
            let reserved = self.reserved_seq[shard_idx].clone();
            let results = match self.shards[shard_idx]
                .call(move |s| Ok(put_many(s, batch, new_edits, &seq, &reserved)))
                .await
            {
                Ok(results) => results,
                Err(error) => {
                    shard_error = Some(error);
                    break;
                }
            };
            for (i, result) in results {
                out[i] = Some(match result {
                    Ok(doc) => doc,
                    Err(e) => json!({
                        "id": ids[i], "error": e.error, "reason": e.reason
                    }),
                });
            }
        }

        if touches_design {
            self.design_generation.fetch_add(1, Ordering::SeqCst);
            *self
                .validation
                .write()
                .unwrap_or_else(|poison| poison.into_inner()) = None;
        }

        if let Some(error) = shard_error {
            return Err(error);
        }
        Ok(out.into_iter().map(|v| v.unwrap_or(Value::Null)).collect())
    }

    pub async fn get_doc(&self, id: &str, opts: &DocOpts) -> DbResult<Value> {
        let id = id.to_string();
        let opts = opts.clone();
        self.shard(id.as_str())
            .call(move |s| read_doc(s, &id, &opts))
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
                    self.shards[index].call(move |s| {
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

    pub async fn all_docs(&self, opts: &AllDocsOpts) -> DbResult<Value> {
        self.all_docs_filtered(opts, None).await
    }

    /// Like `all_docs`, but only shards whose `owned` entry is true. Cluster
    /// coordinators use this to fetch each document from exactly one primary
    /// instead of merging the same replica from every node.
    pub async fn all_docs_owned(&self, opts: &AllDocsOpts, owned: &ShardMask) -> DbResult<Value> {
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
    ) -> DbResult<Value> {
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
                    shard.call(move |s| {
                        let docs = match &opts.partition {
                            Some(partition) => s.partition_live_count(partition)?,
                            None => s.counts().0,
                        };
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
    ) -> DbResult<(AllDocsHead, impl futures::Stream<Item = DbResult<Value>>)> {
        const PAGE: usize = 256;

        struct Cursor {
            index: usize,
            after: Option<String>,
            buffer: std::collections::VecDeque<AllDocsRow>,
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
                .call(move |s| {
                    let docs = match &shard_opts.partition {
                        Some(partition) => s.partition_live_count(partition)?,
                        None => s.counts().0,
                    };
                    Ok((all_docs_before_start(s, &shard_opts)?, docs))
                })
                .await?;
            before_start += before;
            total_rows += docs;
            cursors.push(Cursor {
                index,
                after: None,
                buffer: std::collections::VecDeque::new(),
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
                                .call(move |s| {
                                    let scanned =
                                        scan_all_docs(s, &shard_opts, after.as_deref(), wanted)?;
                                    let last = scanned.last().map(|row| row.id.clone());
                                    let exhausted = scanned.len() < wanted;
                                    Ok((load_all_docs(s, scanned, &shard_opts)?, last, exhausted))
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
                let Some(row) = best.and_then(|slot| cursors[slot].buffer.pop_front()) else {
                    break;
                };
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                remaining -= 1;
                yield Ok(all_docs_row_json(row));
            }
        };
        Ok((head, rows))
    }

    /// Documents for a view build, as JSON text with `_id` and `_rev` already
    /// spliced into the stored body. This avoids parsing every body into a
    /// `Value` and serializing it again just to hand it to QuickJS.
    /// View documents from selected shards. Cluster nodes pass their owned
    /// shards so replicas do not pay to index documents they never serve.
    pub async fn map_docs(&self, owned: Option<&ShardMask>) -> DbResult<Vec<(String, String)>> {
        let mut docs = Vec::new();
        for index in self.selected_shards(owned) {
            let mut shard_docs = self.shards[index]
                .call(|s| {
                    // Build (or reuse) the live map, then read only bodies.
                    // This avoids decoding every full DocMeta record again for
                    // each cold view build.
                    let metas: Vec<(String, String, bool)> = s
                        .live_docs()?
                        .iter()
                        .filter(|(id, _)| !id.starts_with("_design/") && !id.starts_with("_local/"))
                        .map(|(id, doc)| (id.clone(), doc.rev.clone(), doc.has_attachments))
                        .collect();

                    let mut docs = Vec::with_capacity(metas.len());
                    for (id, rev, has_attachments) in metas {
                        if !has_attachments {
                            if let Some(body) = s.get_body(&id, &rev)? {
                                // The body is spliced into the map input as
                                // text, so check it decodes here or one bad
                                // body would fail the whole build.
                                let decoded = serde_json::from_slice::<serde::de::IgnoredAny>(
                                    &body,
                                )
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
                        }
                        // Attachments are rare; fall back to the full document
                        // shape so map functions still see `_attachments`.
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
                    Ok(docs)
                })
                .await?;
            docs.append(&mut shard_docs);
        }
        Ok(docs)
    }

    async fn all_docs_by_keys(
        &self,
        opts: &AllDocsOpts,
        keys: &[Value],
        owned: Option<&ShardMask>,
    ) -> DbResult<Value> {
        let mut total_rows = 0u64;
        if let Some(partition) = &opts.partition {
            let shard = self.partition_shard(partition);
            if !shard_owned(owned, shard) {
                total_rows = 0;
            } else {
                let partition = partition.clone();
                total_rows = self.shards[shard]
                    .call(move |s| s.partition_live_count(&partition).map_err(ApiError::from))
                    .await?;
            }
        } else {
            let counts = futures::future::join_all(
                self.selected_shards(owned)
                    .into_iter()
                    .map(|index| self.shards[index].call(|s| Ok(s.counts().0))),
            )
            .await;
            for count in counts {
                total_rows += count?;
            }
        }

        let ordered_keys: Vec<Value> = if opts.descending {
            keys.iter().rev().cloned().collect()
        } else {
            keys.to_vec()
        };
        let page: Vec<Value> = ordered_keys
            .iter()
            .skip(opts.skip)
            .take(opts.limit)
            .cloned()
            .collect();
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

        let mut results: Vec<Option<Value>> = vec![None; page.len()];
        let fetched = futures::future::join_all(
            grouped
                .into_iter()
                .enumerate()
                .filter(|(_, batch)| !batch.is_empty())
                .map(|(index, batch)| {
                    let opts = opts.clone();
                    self.shards[index].call(move |s| {
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
                    Some(row.unwrap_or_else(|| json!({"key": page[slot], "error": "not_found"})));
            }
        }

        Ok(json!({
            "total_rows": total_rows,
            "offset": Value::Null,
            "rows": results
                .into_iter()
                .enumerate()
                .filter(|(slot, _)| included[*slot])
                .map(|(slot, row)| {
                    row.unwrap_or_else(|| json!({"key": page[slot], "error": "not_found"}))
                })
                .collect::<Vec<_>>(),
        }))
    }

    pub(crate) async fn changes_rows(
        &self,
        opts: &ChangesOpts,
    ) -> DbResult<(Vec<ChangeRow>, u64, usize)> {
        let watermark = opts
            .until
            .unwrap_or_else(|| self.changes_watermark(opts.since, opts.owned.as_ref()));
        let selected = self.selected_shards(opts.owned.as_ref());
        let until = if opts.descending {
            u64::MAX
        } else {
            watermark.min(self.changes_cutoff(&selected, opts).await?)
        };
        let mut rows;
        let scanned_last;
        match &opts.filter {
            Some(filter) if !opts.descending => {
                (rows, scanned_last) = self
                    .filtered_changes(opts, filter, &selected, until)
                    .await?;
            }
            _ => {
                rows = Vec::new();
                for index in selected {
                    let shard_opts = opts.clone();
                    let mut shard_rows = self.shards[index]
                        .call(move |s| collect_changes(s, &shard_opts, until))
                        .await?;
                    rows.append(&mut shard_rows);
                }
                rows.sort_by_key(|r| r.seq);
                // Rows above the watermark are withheld; the next request
                // re-reads them rather than skipping a later-visible lower
                // sequence.
                if !opts.descending {
                    rows.retain(|row| row.seq <= watermark);
                }
                if opts.descending {
                    rows.reverse();
                }
                // A descending filter is evaluated after merging shard rows
                // so every shard is scanned: a shard-local limit would
                // otherwise drop matching rows behind non-matching documents.
                scanned_last = rows.last().map(|row| row.seq);
                if let Some(filter) = &opts.filter {
                    rows = apply_change_filter(rows, filter)?;
                }
            }
        }
        if let Some(limit) = opts.limit {
            rows.truncate(limit);
        }
        // CouchDB's feed position is the last document scanned, matching or
        // not (`couch_changes:changes_enumerator`), except that a feed which
        // stopped at its limit ends at its last row.
        let at_limit = opts
            .limit
            .is_some_and(|limit| limit > 0 && rows.len() >= limit);
        let last_seq = if at_limit {
            rows.last().map(|r| r.seq)
        } else {
            scanned_last.or(rows.last().map(|r| r.seq))
        }
        .unwrap_or(opts.since);
        let pending = self
            .changes_pending(last_seq, opts.descending, opts.owned.as_ref())
            .await?;
        Ok((rows, last_seq, pending))
    }

    /// An ascending filtered read: scans each shard a page at a time and merges
    /// the pages in sequence order, filtering as it goes, until `limit` rows
    /// match or every shard is exhausted. No single shard job loads more than
    /// a page of rows (with bodies, for a JS filter). Returns the matching
    /// rows and the last sequence scanned.
    async fn filtered_changes(
        &self,
        opts: &ChangesOpts,
        filter: &ChangeFilter,
        selected: &[usize],
        until: u64,
    ) -> DbResult<(Vec<ChangeRow>, Option<u64>)> {
        use std::collections::VecDeque;

        // `limit=0` keeps the single full pass: its `last_seq` is the last
        // row scanned.
        let page = match opts.limit {
            Some(0) => usize::MAX,
            Some(limit) => limit.clamp(64, 1024),
            None => 1024,
        };
        let mut cursors = vec![opts.since; selected.len()];
        let mut done = vec![false; selected.len()];
        let mut buffers: Vec<VecDeque<ChangeRow>> =
            (0..selected.len()).map(|_| VecDeque::new()).collect();
        let mut matched: Vec<ChangeRow> = Vec::new();
        let mut scanned_last = None;
        loop {
            for (slot, &index) in selected.iter().enumerate() {
                if !buffers[slot].is_empty() || done[slot] {
                    continue;
                }
                let mut shard_opts = opts.clone();
                shard_opts.since = cursors[slot];
                shard_opts.limit = Some(page);
                let rows = self.shards[index]
                    .call(move |s| collect_changes(s, &shard_opts, until))
                    .await?;
                match rows.last() {
                    // A page can come back short when unreadable records are
                    // skipped, so only an empty one ends the shard.
                    None => done[slot] = true,
                    Some(last) => cursors[slot] = last.seq,
                }
                buffers[slot].extend(rows);
            }
            // Everything up to the lowest buffered tail is complete: no shard
            // can still produce a row below it.
            let Some(cutoff) = buffers
                .iter()
                .zip(&done)
                .filter(|(_, done)| !**done)
                .filter_map(|(buffer, _)| buffer.back().map(|row| row.seq))
                .min()
            else {
                break;
            };
            let mut batch = Vec::new();
            for buffer in &mut buffers {
                while buffer.front().is_some_and(|row| row.seq <= cutoff) {
                    batch.extend(buffer.pop_front());
                }
            }
            batch.sort_by_key(|row| row.seq);
            scanned_last = batch.last().map(|row| row.seq).or(scanned_last);
            matched.extend(apply_change_filter(batch, filter)?);
            if opts
                .limit
                .is_some_and(|limit| limit > 0 && matched.len() >= limit)
            {
                break;
            }
        }
        Ok((matched, scanned_last))
    }

    /// Indexes of the shards in `owned`, or every shard.
    fn selected_shards(&self, owned: Option<&ShardMask>) -> Vec<usize> {
        (0..self.shards.len())
            .filter(|index| shard_owned(owned, *index))
            .collect()
    }

    /// The highest sequence an ascending, limited, unfiltered changes read can
    /// return: the `limit`-th lowest latest sequence after the cursor across
    /// `shards`, read from their indexes alone. Each shard then reads metadata
    /// only up to it rather than for `limit` rows of its own. Bounding by
    /// sequence rather than by a per-shard count keeps the merged rows gapless
    /// when a document moves between the two reads.
    async fn changes_cutoff(&self, shards: &[usize], opts: &ChangesOpts) -> DbResult<u64> {
        let Some(limit) = opts.limit else {
            return Ok(u64::MAX);
        };
        if shards.len() < 2 || opts.descending || opts.filter.is_some() || opts.doc_ids.is_some() {
            return Ok(u64::MAX);
        }
        let mut seqs = Vec::new();
        for &index in shards {
            let since = opts.since;
            let mut found = self.shards[index]
                .call(move |s| {
                    Ok(s.latest_changes()?
                        .after(since)
                        .take(limit)
                        .map(|(seq, _)| seq)
                        .collect::<Vec<u64>>())
                })
                .await?;
            seqs.append(&mut found);
        }
        if seqs.len() <= limit {
            return Ok(u64::MAX);
        }
        let (_, cutoff, _) = seqs.select_nth_unstable(limit - 1);
        Ok(*cutoff)
    }

    pub(crate) async fn changes_pending(
        &self,
        after: u64,
        descending: bool,
        owned: Option<&ShardMask>,
    ) -> DbResult<usize> {
        futures::future::join_all(self.selected_shards(owned).into_iter().map(|index| {
            self.shards[index]
                .call(move |s| s.pending_changes(after, descending).map_err(ApiError::from))
        }))
        .await
        .into_iter()
        .sum::<DbResult<usize>>()
    }

    pub async fn changes(&self, opts: &ChangesOpts) -> DbResult<Value> {
        let (rows, last_seq, pending) = self.changes_rows(opts).await?;
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let mut entry = json!({
                "seq": row.seq,
                "id": row.id,
                "changes": row.changes.iter().map(|(rev, _)| json!({"rev": rev})).collect::<Vec<_>>(),
            });
            if row.deleted {
                entry["deleted"] = json!(true);
            }
            if let Some(doc) = row.doc {
                entry["doc"] = serde_json::from_str::<Value>(&doc).map_err(ApiError::internal)?;
            }
            results.push(entry);
        }
        Ok(json!({ "results": results, "last_seq": last_seq, "pending": pending }))
    }

    /// Stream changes rows in global sequence order by merging a page at a
    /// time from each selected shard. The caller serializes rows as they
    /// arrive, so a full feed does not materialize the whole response.
    pub(crate) fn changes_stream_arc(
        db: Arc<Db>,
        opts: ChangesOpts,
    ) -> impl futures::Stream<Item = DbResult<ChangesItem>> + 'static {
        async_stream::stream! {
            use std::collections::VecDeque;

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
            let selected = db.selected_shards(opts.owned.as_ref());
            let mut cursors = vec![opts.since; selected.len()];
            let mut done = vec![false; selected.len()];
            let mut buffers: Vec<VecDeque<ChangeRow>> =
                (0..selected.len()).map(|_| VecDeque::new()).collect();
            let mut emitted = 0usize;
            // One snapshot for the whole stream, taken before any shard read.
            // The stream ends at the watermark; the client resumes from the
            // last row it received.
            let watermark = db.changes_watermark(opts.since, opts.owned.as_ref());
            let watermark = match db.changes_cutoff(&selected, &opts).await {
                Ok(cutoff) => watermark.min(cutoff),
                Err(error) => {
                    yield Err(error);
                    return;
                }
            };

            loop {
                let mut best: Option<usize> = None;
                for slot in 0..selected.len() {
                    while buffers[slot].is_empty() && !done[slot] {
                        let index = selected[slot];
                        let mut shard_opts = opts.clone();
                        shard_opts.since = cursors[slot];
                        shard_opts.descending = false;
                        // A shard never needs more rows than the request can
                        // still return. A filter may discard rows, so it reads
                        // at least a small page and refills until a row
                        // survives or the shard is exhausted.
                        let wanted = match (opts.limit, &opts.filter) {
                            (Some(limit), None) => limit.saturating_sub(emitted).max(1),
                            (Some(limit), Some(_)) => {
                                limit.saturating_sub(emitted).clamp(64, PAGE)
                            }
                            _ => PAGE,
                        };
                        let requested = PAGE.min(wanted);
                        shard_opts.limit = Some(requested);
                        match db.shards[index]
                            .call(move |shard| collect_changes(shard, &shard_opts, watermark))
                            .await
                        {
                            Ok(mut rows) => {
                                if rows.is_empty() {
                                    done[slot] = true;
                                } else {
                                    cursors[slot] =
                                        rows.last().map(|row| row.seq).unwrap_or(cursors[slot]);
                                    if opts.filter.is_none() && rows.len() < requested {
                                        // The shard has nothing more at or
                                        // below the watermark.
                                        done[slot] = true;
                                    }
                                    if let Some(filter) = &opts.filter {
                                        match apply_change_filter(rows, filter) {
                                            Ok(filtered) => rows = filtered,
                                            Err(error) => {
                                                yield Err(error);
                                                return;
                                            }
                                        }
                                    }
                                    buffers[slot].extend(rows);
                                }
                            }
                            Err(error) => {
                                yield Err(error);
                                return;
                            }
                        }
                    }
                    if let Some(front) = buffers[slot].front() {
                        if best
                            .map(|current| front.seq < buffers[current].front().map(|row| row.seq).unwrap_or(u64::MAX))
                            .unwrap_or(true)
                        {
                            best = Some(slot);
                        }
                    }
                }
                let Some(slot) = best else {
                    // Every shard is exhausted: report how far the scan got.
                    let scanned = cursors.iter().copied().max().unwrap_or(opts.since);
                    yield Ok(ChangesItem::Scanned(scanned));
                    break;
                };
                let row = buffers[slot].pop_front().expect("buffered row");
                emitted += 1;
                yield Ok(ChangesItem::Row(row));
                if opts.limit.map(|limit| emitted >= limit).unwrap_or(false) {
                    break;
                }
            }
        }
    }

    pub(crate) fn change_row_json(row: &ChangeRow) -> String {
        use std::fmt::Write as _;

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
        let _ = write!(out, "{}", row.seq);
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

    pub async fn revs_diff(&self, body: &Value) -> DbResult<Value> {
        let Some(map) = body.as_object() else {
            return Err(ApiError::bad_request("Request body must be a JSON object"));
        };
        // One round trip per shard, the shards in parallel, instead of one
        // per document.
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
                    self.shards[index].call(move |s| {
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
        let mut out = Map::new();
        for (id, diff) in ids.into_iter().zip(found) {
            let Some((missing, ancestors)) = diff else {
                continue;
            };
            if !missing.is_empty() {
                let mut entry = json!({ "missing": missing });
                if !ancestors.is_empty() {
                    entry["possible_ancestors"] = json!(ancestors);
                }
                out.insert(id.clone(), entry);
            }
        }
        Ok(Value::Object(out))
    }

    pub async fn bulk_get(
        &self,
        body: &Value,
        attachments: bool,
        revs: bool,
        latest: bool,
    ) -> DbResult<Value> {
        let Some(reqs) = body.get("docs").and_then(Value::as_array) else {
            return Err(ApiError::bad_request("Missing JSON list of 'docs'"));
        };

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
                attachments: req
                    .get("attachments")
                    .and_then(Value::as_bool)
                    .unwrap_or(attachments),
                latest: req.get("latest").and_then(Value::as_bool).unwrap_or(latest),
                include_deleted: true,
                ..Default::default()
            };
            grouped[self.shard_index(&id)].push((slot, id, opts));
        }

        let mut out: Vec<Option<Value>> = vec![None; reqs.len()];
        let read = futures::future::join_all(
            grouped
                .into_iter()
                .enumerate()
                .filter(|(_, batch)| !batch.is_empty())
                .map(|(index, batch)| {
                    self.shards[index].call(move |shard| {
                        let mut out = Vec::with_capacity(batch.len());
                        for (slot, id, opts) in batch {
                            let rev = opts.rev.clone().unwrap_or_else(|| "undefined".into());
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
                out[slot] = Some(match result {
                    Ok(doc) => json!({ "id": id, "docs": [{"ok": doc}] }),
                    Err(error) => json!({
                        "id": id,
                        "docs": [{"error": {
                            "id": id,
                            "rev": rev,
                            "error": error.error,
                            "reason": error.reason,
                        }}]
                    }),
                });
            }
        }
        Ok(json!({
            "results": out.into_iter().map(|value| value.unwrap_or(Value::Null)).collect::<Vec<_>>()
        }))
    }

    // ----- local documents -------------------------------------------------

    pub async fn local_get(&self, id: &str) -> DbResult<Option<Value>> {
        let id = id.to_string();
        self.shards[0]
            .call(move |s| match s.get_checked(&local_key(&id))? {
                Some(bytes) => serde_json::from_slice::<Value>(&bytes)
                    .map(Some)
                    .map_err(|error| {
                        ApiError::new(
                            500,
                            "internal_server_error",
                            format!("corrupt local document {id}: {error}"),
                        )
                    }),
                None => Ok(None),
            })
            .await
    }

    pub async fn local_put(&self, id: &str, mut body: Value) -> DbResult<Value> {
        let id = id.to_string();
        self.shards[0]
            .call(move |s| {
                let key = local_key(&id);
                // CouchDB (couch_db_updater:update_local_docs/2) numbers a
                // local write from the supplied `_rev`, absent meaning 0, and
                // never conflicts: the stored revision is not consulted.
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
                let mut tx = Tx::new();
                tx.put_json(&key, &body)?;
                s.commit(tx)?;
                Ok(json!({"ok": true, "id": format!("_local/{id}"), "rev": rev}))
            })
            .await
    }

    pub async fn local_delete(&self, id: &str) -> DbResult<Value> {
        let id = id.to_string();
        self.shards[0]
            .call(move |s| {
                if s.get_checked(&local_key(&id))?.is_none() {
                    return Err(ApiError::not_found("missing"));
                }
                let mut tx = Tx::new();
                tx.delete(&local_key(&id));
                s.commit(tx)?;
                Ok(json!({"ok": true, "id": format!("_local/{id}"), "rev": "0-0"}))
            })
            .await
    }

    /// `_local_docs`: the local documents in id order, as `_all_docs` rows,
    /// honouring `keys`, the key range, `descending`, `skip` and `limit`.
    /// CouchDB reports neither `total_rows` nor `offset` for them.
    pub async fn local_docs(&self, opts: &AllDocsOpts) -> DbResult<Value> {
        let opts = opts.clone();
        let rows = self.shards[0]
            .call(move |s| {
                let mut docs: Vec<(String, Value)> = Vec::new();
                let mut corrupt = None;
                s.scan_range_while(b"l/", None, false, |key, value| {
                    let Some(id) = key
                        .strip_prefix(b"l/")
                        .and_then(|id| std::str::from_utf8(id).ok())
                    else {
                        return true;
                    };
                    match serde_json::from_slice::<Value>(value) {
                        Ok(doc) => docs.push((format!("_local/{id}"), doc)),
                        Err(error) => {
                            corrupt = Some(format!("corrupt local document {id}: {error}"));
                            return false;
                        }
                    }
                    true
                })?;
                if let Some(reason) = corrupt {
                    return Err(ApiError::new(500, "internal_server_error", reason));
                }
                Ok(local_doc_rows(docs, &opts))
            })
            .await?;
        Ok(json!({"total_rows": Value::Null, "offset": Value::Null, "rows": rows}))
    }

    // ----- maintenance -----------------------------------------------------

    pub async fn purge(&self, body: &Value) -> DbResult<Value> {
        let Some(map) = body.as_object() else {
            return Err(ApiError::bad_request("Request body must be a JSON object"));
        };
        // One round trip per shard, the shards in parallel, instead of one
        // per document.
        let mut ids = Vec::new();
        let mut grouped: Vec<Vec<(usize, String, Vec<String>)>> =
            (0..self.q).map(|_| Vec::new()).collect();
        for (id, revs) in map {
            let Some(list) = revs.as_array() else {
                continue;
            };
            let requested: Vec<String> = list
                .iter()
                .filter_map(|r| r.as_str().map(str::to_string))
                .collect();
            grouped[self.shard_index(id)].push((ids.len(), id.clone(), requested));
            ids.push(id);
        }
        // Purging a document that survives takes a new sequence, so the
        // shards run one after another to keep that order deterministic.
        let mut done = Vec::new();
        for (index, batch) in grouped.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let seq_counter = self.seq.clone();
            let reserved = self.reserved_seq[index].clone();
            done.push(
                self.shards[index]
                    .call(move |s| {
                        batch
                            .into_iter()
                            .map(|(slot, id, requested)| {
                                purge_one(s, &id, &requested, &seq_counter, &reserved)
                                    .map(|removed| (slot, removed))
                            })
                            .collect::<DbResult<Vec<_>>>()
                    })
                    .await,
            );
        }
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
        Ok(json!({ "purge_seq": 0, "purged": purged }))
    }

    pub async fn compact(&self) -> DbResult<()> {
        self.compact_running.store(true, Ordering::SeqCst);
        let result = async {
            for shard in &self.shards {
                shard.call(|s| s.compact().map_err(ApiError::from)).await?;
            }
            Ok(())
        }
        .await;
        self.compact_running.store(false, Ordering::SeqCst);
        result
    }

    pub async fn ensure_full_commit(&self) -> DbResult<Value> {
        for shard in &self.shards {
            shard.call(|s| s.flush().map_err(ApiError::from)).await?;
        }
        Ok(json!({ "ok": true, "instance_start_time": self.instance_start_time }))
    }

    pub async fn get_revs_limit(&self) -> DbResult<u64> {
        self.shards[0].call(|s| Ok(s.revs_limit())).await
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
    s: &mut crate::store::Shard,
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

/// Purge the requested leaf revisions of one document; the revisions removed.
fn purge_one(
    s: &mut crate::store::Shard,
    id: &str,
    requested: &[String],
    seq_counter: &AtomicU64,
    reserved: &AtomicU64,
) -> DbResult<Vec<String>> {
    let Some(mut meta) = s.get_meta(id)? else {
        return Ok(Vec::new());
    };
    let mut tx = Tx::new();
    let mut removed = Vec::new();
    // Like CouchDB, purge leaves only: each takes its unshared
    // ancestors with it, and a listed non-leaf revision is
    // ignored and not reported.
    let leaves: std::collections::BTreeSet<String> = meta.tree.leaves().into_iter().collect();
    for rev in requested.iter().filter(|rev| leaves.contains(*rev)) {
        let gone = meta.tree.purge_leaf(rev);
        if gone.is_empty() {
            continue;
        }
        for dead in &gone {
            tx.delete(&body_key(id, dead));
            meta.atts.remove(dead);
        }
        removed.push(rev.clone());
    }
    if !removed.is_empty() {
        if meta.tree.is_empty() {
            tx.delete(&meta_key(id));
            if meta.seq != 0 {
                tx.delete(&seq_key(meta.seq));
            }
            s.commit(tx)?;
        } else {
            let old_seq = meta.seq;
            // Purging the winner changes what a normal read sees,
            // so it is a change as far as `_changes` is concerned.
            let seq = allocate_seq(seq_counter, reserved);
            meta.seq = seq;
            tx.delete(&seq_key(old_seq));
            tx.put(&seq_key(seq), id.as_bytes());
            tx.put_json(&meta_key(id), &meta)?;
            s.commit_at(tx, Some(seq))?;
        }
    } else {
        s.commit(tx)?;
    }
    if !removed.is_empty() {
        s.invalidate_latest();
    }
    Ok(removed)
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

/// The structural checks CouchDB applies to a design document before storing
/// it (couch_mrview_util and the design-doc validator): wrong member types are
/// `invalid_design_doc`, not a stored document that fails later.
pub fn validate_design_doc(doc: &Value) -> DbResult<()> {
    let invalid = |reason: String| ApiError::new(400, "invalid_design_doc", reason);
    let Some(obj) = doc.as_object() else {
        return Ok(());
    };
    let is_design = matches!(obj.get("_id"), Some(Value::String(id)) if id.starts_with("_design/"));
    if !is_design || obj.get("_deleted") == Some(&Value::Bool(true)) {
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
                    if reduce.starts_with('_')
                        && !["_sum", "_count", "_stats", "_approx_count_distinct"]
                            .contains(&reduce.as_str())
                    {
                        return Err(invalid(format!(
                            "`{reduce}` is not a supported reduce function."
                        )));
                    }
                }
            }
        }
        Some(_) => return Err(wrong("views", "object")),
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

/// A client document split into its metadata fields and its body.
struct SplitDoc {
    id: String,
    rev: Option<String>,
    deleted: bool,
    revisions: Option<Value>,
    body: Map<String, Value>,
}

fn split_doc(doc: Value) -> DbResult<SplitDoc> {
    /// `_id`/`_rev`/... are document metadata, not body. `_conflicts` and
    /// friends are read-only decorations a client may echo back.
    const META: [&str; 9] = META_FIELDS;

    validate_doc(&doc)?;
    let Value::Object(obj) = doc else {
        return Err(ApiError::bad_request("Document must be a JSON object"));
    };

    let mut id = None;
    let mut rev = None;
    let mut deleted = false;
    let mut revisions = None;
    let mut body = Map::new();

    // One ordered pass. `serde_json::Map::remove` is a `swap_remove`, which
    // would silently reorder the body and change the revision hash.
    for (key, value) in obj {
        match key.as_str() {
            "_id" => match value {
                Value::String(s) => id = Some(s),
                _ => {
                    return Err(ApiError::new(
                        400,
                        "illegal_docid",
                        "Document id must be a string",
                    ))
                }
            },
            "_rev" => match value {
                Value::String(s) => rev = Some(s),
                _ => return Err(ApiError::bad_request("Invalid rev format")),
            },
            "_deleted" => deleted = matches!(value, Value::Bool(true)),
            "_revisions" => revisions = Some(value),
            k if META.contains(&k) => {}
            _ => {
                body.insert(key, value);
            }
        }
    }

    let id = id.unwrap_or_else(uuid);
    if id.is_empty() {
        return Err(ApiError::new(
            400,
            "illegal_docid",
            "Document id must not be empty",
        ));
    }
    Ok(SplitDoc {
        id,
        rev,
        deleted,
        revisions,
        body,
    })
}

/// A stored revision tree whose parent links loop, reported as corruption.
fn corrupt_tree(id: &str, error: crate::revtree::CyclicTree) -> ApiError {
    ApiError::corrupt(format!("corrupt revision tree for {id}: {error}"))
}

fn put_many(
    shard: &mut crate::store::Shard,
    docs: Vec<(usize, Value)>,
    new_edits: bool,
    seq: &Arc<AtomicU64>,
    reserved: &Arc<AtomicU64>,
) -> Vec<(usize, DbResult<Value>)> {
    let mut ops: Ops = Vec::new();
    let mut staged: HashMap<String, DocMeta> = HashMap::new();
    // What each staged document's record held in the shard before this batch.
    let mut origin: HashMap<String, Option<MetaState>> = HashMap::new();
    let mut results = Vec::new();

    for (i, doc) in docs {
        let mark = ops.len();
        let mut ctx = Stage {
            shard,
            ops: &mut ops,
            staged: &mut staged,
            origin: &mut origin,
            seq,
            reserved,
        };
        match ctx.stage(doc, new_edits) {
            Ok(value) => results.push((i, Ok(value))),
            Err(e) => {
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
    for (id, meta) in staged.iter_mut() {
        // The batch's bodies and sequence numbers are already staged, so a
        // tree that cannot be pruned fails the whole batch rather than being
        // written back as if it were valid.
        if let Err(e) = meta.tree.prune(limit) {
            let message = corrupt_tree(id, e).reason;
            shard.invalidate_latest();
            return results
                .into_iter()
                .map(|(i, _)| (i, Err(ApiError::corrupt(message.clone()))))
                .collect();
        }
        meta.atts.retain(|rev, _| meta.tree.contains(rev));
        if let Ok(bytes) = meta_bytes(meta) {
            let old = origin.get(id).cloned().flatten();
            metas.push(MetaWrite {
                key: meta_key(id),
                bytes,
                old,
                new: meta_state(meta),
            });
        }
    }

    if let Err(e) = commit_ops(shard, ops, metas) {
        shard.invalidate_latest();
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
    let ok_ids: std::collections::HashSet<String> = results
        .iter()
        .filter_map(|(_, result)| {
            result
                .as_ref()
                .ok()
                .and_then(|value| value.get("id"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect();
    shard.update_latest(&staged, &ok_ids);
    results
}

struct Stage<'a> {
    shard: &'a mut crate::store::Shard,
    ops: &'a mut Ops,
    staged: &'a mut HashMap<String, DocMeta>,
    origin: &'a mut HashMap<String, Option<MetaState>>,
    seq: &'a Arc<AtomicU64>,
    reserved: &'a Arc<AtomicU64>,
}

impl Stage<'_> {
    fn next_seq(&self) -> u64 {
        allocate_seq(self.seq, self.reserved)
    }

    fn meta(&mut self, id: &str) -> DbResult<Option<DocMeta>> {
        if let Some(m) = self.staged.get(id) {
            return Ok(Some(m.clone()));
        }
        let meta = self.shard.get_meta(id)?;
        self.origin
            .entry(id.to_string())
            .or_insert_with(|| meta.as_ref().and_then(meta_state));
        Ok(meta)
    }

    /// Record the document's new metadata. `put_many` writes it, once, after
    /// the whole batch is staged and pruned.
    fn flush_meta(&mut self, id: &str, meta: &DocMeta) {
        self.staged.insert(id.to_string(), meta.clone());
    }

    fn stage(&mut self, doc: Value, new_edits: bool) -> DbResult<Value> {
        let doc = split_doc(doc)?;
        let mut meta = self.meta(&doc.id)?.unwrap_or_default();
        if !new_edits {
            return self.stage_replicated(doc, meta);
        }
        let SplitDoc {
            id,
            rev: parent_rev,
            deleted,
            revisions: _,
            body,
        } = doc;
        let parent_rev = parent_rev.map(|rev| canonical_rev(&rev));
        let body = Value::Object(body);

        let parent = match parent_rev {
            Some(rev) => {
                if Rev::parse(&rev).is_none() {
                    return Err(ApiError::bad_request("Invalid rev format"));
                }
                if !meta.tree.contains(&rev) || !meta.tree.is_leaf(&rev) {
                    return Err(ApiError::conflict());
                }
                if meta.tree.is_deleted(&rev) {
                    return Err(ApiError::conflict());
                }
                Some(rev)
            }
            None => match meta.tree.winner() {
                Some(winner) if !meta.tree.is_deleted(&winner) => return Err(ApiError::conflict()),
                other => other,
            },
        };

        let parent_path = match parent.as_ref() {
            Some(p) => meta.tree.path(p).map_err(|e| corrupt_tree(&id, e))?,
            None => Vec::new(),
        };
        let pos = parent
            .as_ref()
            .and_then(|p| Rev::parse(p).map(|r| r.pos))
            .unwrap_or(0)
            + 1;

        let inherited = parent
            .as_ref()
            .and_then(|p| meta.atts.get(p).cloned())
            .unwrap_or_default();
        // Uploads are rejected, so a new revision only carries forward the
        // attachments that older data already holds.
        let atts = inherited;

        let hashed = atts
            .iter()
            .map(|(name, a)| {
                Ok(Attachment {
                    name: name.clone(),
                    content_type: a.content_type.clone(),
                    md5: hex_to_16(&a.digest)?,
                })
            })
            .collect::<DbResult<Vec<Attachment>>>()?;
        let rev_id = hex(&new_revid(
            deleted,
            parent.as_ref().and_then(|p| Rev::parse(p)).as_ref(),
            &body,
            &hashed,
        ));
        let rev = format!("{pos}-{rev_id}");

        let mut path: Vec<Rev> = parent_path.iter().filter_map(|r| Rev::parse(r)).collect();
        path.push(Rev::parse(&rev).expect("valid rev"));

        meta.tree.merge_path(&path, deleted);
        let old_seq = meta.seq;
        let seq = self.next_seq();
        meta.seq = seq;
        meta.atts.insert(rev.clone(), atts);

        self.ops.push((
            body_key(&id, &rev),
            Some(serde_json::to_vec(&body).map_err(ApiError::internal)?),
        ));
        self.ops.push((seq_key(seq), Some(id.as_bytes().to_vec())));
        if old_seq != 0 {
            self.ops.push((seq_key(old_seq), None));
        }
        self.flush_meta(&id, &meta);

        Ok(json!({"ok": true, "id": id, "rev": rev}))
    }

    fn stage_replicated(&mut self, doc: SplitDoc, mut meta: DocMeta) -> DbResult<Value> {
        let SplitDoc {
            id,
            rev,
            deleted,
            revisions,
            body,
        } = doc;
        let body = Value::Object(body);
        let (rev, path) = revisions_path(rev, revisions.as_ref())?;
        let known = meta.tree.contains(&rev);
        if known && meta.atts.contains_key(&rev) {
            return Ok(json!({"ok": true, "id": id, "rev": rev}));
        }

        let parent = meta
            .tree
            .parent(&rev)
            .and_then(|p| meta.atts.get(p).cloned())
            .unwrap_or_default();
        let atts = parent;

        let changed = !known;
        if changed {
            meta.tree.merge_path(&path, deleted);
        }

        self.ops.push((
            body_key(&id, &rev),
            Some(serde_json::to_vec(&body).map_err(ApiError::internal)?),
        ));
        if changed {
            let old_seq = meta.seq;
            let seq = self.next_seq();
            meta.seq = seq;
            meta.atts.insert(rev.clone(), atts);
            self.ops.push((seq_key(seq), Some(id.as_bytes().to_vec())));
            if old_seq != 0 {
                self.ops.push((seq_key(old_seq), None));
            }
            self.flush_meta(&id, &meta);
        }

        Ok(json!({"ok": true, "id": id, "rev": rev}))
    }
}

/// Reconstruct the root-to-leaf path of a replicated document and the revision
/// it writes. Like CouchDB (`couch_doc:transfer_fields/3`), `_revisions`
/// defines the revision when present and non-empty, even without or against
/// `_rev`; otherwise `_rev` alone does.
fn revisions_path(rev: Option<String>, revisions: Option<&Value>) -> DbResult<(String, Vec<Rev>)> {
    let mut path = Vec::new();
    if let Some(revs) = revisions {
        let Some(start) = revs.get("start").and_then(Value::as_u64) else {
            return Err(ApiError::bad_request("_revisions.start is not an integer"));
        };
        let Some(ids) = revs.get("ids").and_then(Value::as_array) else {
            return Err(ApiError::bad_request("_revisions.ids is not an array"));
        };
        for (i, id) in ids.iter().enumerate() {
            let Some(id) = id.as_str() else {
                return Err(ApiError::bad_request(
                    "_revisions.ids contains a non-string",
                ));
            };
            let pos = start
                .checked_sub(i as u64)
                .ok_or_else(|| ApiError::bad_request("_revisions is malformed"))?;
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
    let rev = rev.ok_or_else(|| {
        ApiError::bad_request(
            "When `new_edits: false`, the document needs `_rev` or `_revisions` specified",
        )
    })?;
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

/// A document's metadata record, with the state it replaces and the state it
/// leaves.
struct MetaWrite {
    key: Vec<u8>,
    bytes: Vec<u8>,
    old: Option<MetaState>,
    new: Option<MetaState>,
}

/// Commit a batch's operations and its documents' metadata records.
fn commit_ops(
    shard: &mut crate::store::Shard,
    ops: Ops,
    metas: Vec<MetaWrite>,
) -> anyhow::Result<()> {
    let mut last_seq = None;
    let mut tx = Tx::new();
    for (key, value) in ops {
        if key.starts_with(b"s/") {
            if let Some(seq) = key[2..10].try_into().ok().map(u64::from_be_bytes) {
                // A document update emits the new sequence key and then
                // deletes the old one. Keep the highest, not whichever key
                // happens to be last in the operation list.
                last_seq = Some(last_seq.map_or(seq, |current: u64| current.max(seq)));
            }
        }
        match value {
            Some(v) => tx.put(&key, &v),
            None => tx.delete(&key),
        }
    }
    for meta in metas {
        tx.put_meta(&meta.key, &meta.bytes, meta.old, meta.new);
    }
    shard.commit_at(tx, last_seq)
}

/// The raw bytes of a stored hex digest. The digest comes from disk, so one
/// that is not well-formed hex is reported as corruption rather than guessed at.
fn hex_bytes(hex: &str) -> DbResult<Vec<u8>> {
    let corrupt = || ApiError::corrupt(format!("corrupt attachment digest {hex:?}"));
    if !hex.len().is_multiple_of(2) {
        return Err(corrupt());
    }
    hex.as_bytes()
        .chunks(2)
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or_else(corrupt)
        })
        .collect()
}

fn hex_to_16(hex: &str) -> DbResult<[u8; 16]> {
    hex_bytes(hex)?
        .try_into()
        .map_err(|_| ApiError::corrupt(format!("corrupt attachment digest {hex:?}")))
}

/// CouchDB writes attachment digests as `md5-<base64 of the raw digest>`, not
/// hex. The wire format is not cosmetic: PouchDB uses it to decide whether an
/// attachment has to be transferred.
pub fn digest_to_wire(hex: &str) -> DbResult<String> {
    Ok(format!(
        "md5-{}",
        base64::engine::general_purpose::STANDARD.encode(hex_bytes(hex)?)
    ))
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
    pub attachments: bool,
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
    pub attachments: bool,
    pub keys: Option<Vec<Value>>,
    pub startkey: Option<String>,
    pub endkey: Option<String>,
    pub descending: bool,
    pub inclusive_end: bool,
    pub limit: usize,
    pub skip: usize,
}

impl Default for AllDocsOpts {
    fn default() -> Self {
        AllDocsOpts {
            partition: None,
            include_docs: false,
            conflicts: false,
            attachments: false,
            keys: None,
            startkey: None,
            endkey: None,
            descending: false,
            inclusive_end: true,
            limit: usize::MAX,
            skip: 0,
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
    pub since: u64,
    pub limit: Option<usize>,
    pub include_docs: bool,
    pub style_all_docs: bool,
    pub descending: bool,
    pub doc_ids: Option<Vec<String>>,
    pub filter: Option<ChangeFilter>,
    pub conflicts: bool,
    /// Internal cluster filter: only scan shards selected by this mask.
    pub owned: Option<ShardMask>,
    /// Internal: a watermark the caller already read with `changes_watermark`.
    /// Rows above it are withheld, and the feed does not take a newer one, so
    /// a caller that records it as its position has seen every row up to it.
    pub until: Option<u64>,
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

/// What a changes stream yields: the rows, then, when the scan ran to the end
/// rather than stopping at the limit, the last sequence it scanned. A filter
/// may have discarded rows after the last one yielded, and CouchDB's feed
/// position (`couch_changes:changes_enumerator`) covers them too.
#[derive(Debug)]
pub(crate) enum ChangesItem {
    Row(ChangeRow),
    Scanned(u64),
}

#[derive(Debug)]
pub(crate) struct ChangeRow {
    pub(crate) id: String,
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
/// Only a decode failure (`ApiError::corrupt`) is skipped: an error from
/// LevelDB itself, or any other failure, still fails the request.
fn skip_corrupt<T>(
    shard: &crate::store::Shard,
    id: &str,
    result: DbResult<T>,
) -> DbResult<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.corrupt => {
            shard.name().report_corrupt(id, &error);
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn read_doc(shard: &mut crate::store::Shard, id: &str, opts: &DocOpts) -> DbResult<Value> {
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
    shard: &mut crate::store::Shard,
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
    if let Some(atts) = meta.atts.get(rev) {
        if !atts.is_empty() {
            let mut out = Map::new();
            for (name, att) in atts {
                out.insert(name.clone(), attachment_json(shard, att, opts.attachments)?);
            }
            map.insert("_attachments".into(), Value::Object(out));
        }
    }
    if opts.revs {
        let mut path = meta.tree.path(rev).map_err(|e| corrupt_tree(id, e))?;
        let limit = shard.revs_limit().max(1) as usize;
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
        let limit = shard.revs_limit().max(1) as usize;
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

fn attachment_json(
    shard: &mut crate::store::Shard,
    att: &AttMeta,
    with_data: bool,
) -> DbResult<Value> {
    let mut out = json!({
        "content_type": att.content_type,
        "revpos": att.revpos,
        "digest": digest_to_wire(&att.digest)?,
        "length": att.length,
    });
    if with_data {
        let mut data = Vec::new();
        let mut chunk = 0u32;
        while let Some(part) = shard.get_checked(&blob_prefix(&att.digest, chunk))? {
            data.extend_from_slice(&part);
            chunk += 1;
        }
        // A missing chunk ends the loop like the last one does, so the length
        // recorded with the attachment is what tells them apart. Never serve
        // a truncated body under the original digest.
        if data.len() as u64 != att.length {
            return Err(ApiError::new(
                500,
                "internal_server_error",
                format!(
                    "attachment {} is incomplete: {} of {} bytes stored",
                    att.digest,
                    data.len(),
                    att.length
                ),
            ));
        }
        out["data"] = json!(base64::engine::general_purpose::STANDARD.encode(&data));
    } else if let Some(obj) = out.as_object_mut() {
        obj.insert("stub".into(), json!(true));
    }
    Ok(out)
}

/// Scan one shard for the live rows in the requested key range, stopping once
/// `take` rows have been collected. Also returns how many live rows precede
/// `startkey`, which is the shard's contribution to `offset`.
/// How many live ids sort before the query's start key, in the query's
/// direction: the `offset` of an `_all_docs` reply, before `skip`.
fn all_docs_before_start(shard: &mut crate::store::Shard, opts: &AllDocsOpts) -> DbResult<usize> {
    let Some(start) = opts.startkey.as_deref() else {
        return Ok(0);
    };
    // Without a partition the rank comes from the shard's cursor, so paging by
    // `startkey` does not recount every id before it on each request.
    let ranked = match &opts.partition {
        None => Some(shard.live_rank(start)?),
        Some(_) => None,
    };
    let live = shard.live_docs()?;
    let prefix = opts.partition.as_ref().map(|p| format!("{p}:"));
    let in_partition = |id: &str| prefix.as_deref().map(|p| id.starts_with(p)).unwrap_or(true);
    Ok(match (opts.descending, ranked) {
        (false, None) => live
            .range::<str, _>((Bound::Unbounded, Bound::Excluded(start)))
            .filter(|(id, _)| in_partition(id))
            .count(),
        (true, None) => live
            .range::<str, _>((Bound::Excluded(start), Bound::Unbounded))
            .filter(|(id, _)| in_partition(id))
            .count(),
        (false, Some(rank)) => rank,
        (true, Some(rank)) => live
            .len()
            .saturating_sub(rank + usize::from(live.contains_key(start))),
    })
}

/// Up to `take` live ids of the query's range in the query's direction,
/// starting after `after` when it is given. `after` must be an id this scan
/// already returned (or the start of the range), so a later page continues
/// where the last one stopped.
fn scan_all_docs(
    shard: &mut crate::store::Shard,
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
    shard: &mut crate::store::Shard,
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
            attachments: opts.attachments,
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
    shard: &mut crate::store::Shard,
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
    shard: &mut crate::store::Shard,
    key: &str,
    opts: &AllDocsOpts,
) -> DbResult<Option<Value>> {
    let Some(meta) = shard.get_meta(key)? else {
        return Ok(None);
    };
    let Some(winner) = meta.tree.winner() else {
        return Ok(None);
    };
    let deleted = meta.tree.is_deleted(&winner);
    let mut row = json!({"id": key, "key": key, "value": {"rev": winner.clone()}});
    if deleted {
        row["value"]["deleted"] = json!(true);
    }
    if opts.include_docs {
        if deleted {
            row["doc"] = Value::Null;
        } else {
            let doc_opts = DocOpts {
                rev: Some(winner),
                conflicts: opts.conflicts,
                attachments: opts.attachments,
                ..Default::default()
            };
            row["doc"] = read_doc(shard, key, &doc_opts)?;
        }
    }
    Ok(Some(row))
}

/// Select and render `_local_docs` rows from every local document, which
/// arrive in id order.
fn local_doc_rows(docs: Vec<(String, Value)>, opts: &AllDocsOpts) -> Vec<Value> {
    let render = |id: &str, doc: &Value| {
        let rev = doc.get("_rev").cloned().unwrap_or_else(|| json!("0-1"));
        let mut row = json!({"id": id, "key": id, "value": {"rev": rev}});
        if opts.include_docs {
            row["doc"] = doc.clone();
        }
        row
    };
    let rows: Vec<Value> = if let Some(keys) = &opts.keys {
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
                None => json!({"key": key, "error": "not_found"}),
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

fn all_docs_row_json(AllDocsRow { id, rev, doc }: AllDocsRow) -> Value {
    let mut row = json!({"id": id, "key": id, "value": {"rev": rev}});
    if let Some(doc) = doc {
        row["doc"] = doc;
    }
    row
}

fn finish_all_docs(rows: Vec<AllDocsRow>, total: usize, offset: usize) -> DbResult<Value> {
    let results: Vec<Value> = rows.into_iter().map(all_docs_row_json).collect();
    Ok(json!({ "total_rows": total, "offset": offset, "rows": results }))
}

fn apply_change_filter(rows: Vec<ChangeRow>, filter: &ChangeFilter) -> DbResult<Vec<ChangeRow>> {
    match filter {
        ChangeFilter::Design => Ok(rows
            .into_iter()
            .filter(|row| row.id.starts_with("_design/"))
            .collect()),
        ChangeFilter::Js { source, query } => {
            let docs: Vec<(String, String)> = rows
                .iter()
                .filter_map(|row| row.doc.clone().map(|doc| (row.id.clone(), doc)))
                .collect();
            if docs.is_empty() {
                return Ok(Vec::new());
            }
            let matched = crate::view::run_filter_json(
                source,
                query,
                &docs,
                crate::view::ViewLimits::default(),
            )
            .map_err(ApiError::from)?;
            Ok(rows
                .into_iter()
                .filter(|row| matched.contains(&row.id))
                .collect())
        }
    }
}

/// Change rows from one shard. An ascending read stops at sequence `until`.
fn collect_changes(
    shard: &mut crate::store::Shard,
    opts: &ChangesOpts,
    until: u64,
) -> DbResult<Vec<ChangeRow>> {
    // A descending feed has no cursor to resume a filtered scan from, so it
    // still reads every row; ascending callers page by `since` and `limit`.
    let limit = if opts.filter.is_some() && opts.descending {
        usize::MAX
    } else {
        opts.limit.unwrap_or(usize::MAX)
    };
    if limit == 0 {
        return Ok(Vec::new());
    }

    // CouchDB returns one row per document at its latest sequence, not one
    // row per historical sequence entry. `latest_changes` is maintained on
    // writes and built lazily from metadata.
    // Read only the rows after the cursor, already in sequence order. A
    // descending feed walks back from the newest change.
    let latest = shard.latest_changes()?;
    let candidates: Vec<(u64, String)> = match &opts.doc_ids {
        Some(ids) => {
            let mut selected: Vec<(u64, String)> = ids
                .iter()
                .filter_map(|id| latest.seq_of(id).map(|seq| (seq, id.clone())))
                .filter(|(seq, _)| opts.descending || (*seq > opts.since && *seq <= until))
                .collect();
            selected.sort();
            selected.dedup();
            if opts.descending {
                selected.reverse();
            }
            selected.truncate(limit);
            selected
        }
        None if opts.descending => latest
            .all()
            .rev()
            .take(limit)
            .map(|(seq, id)| (seq, id.to_string()))
            .collect(),
        None => latest
            .after(opts.since)
            .take_while(|(seq, _)| *seq <= until)
            .take(limit)
            .map(|(seq, id)| (seq, id.to_string()))
            .collect(),
    };

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
            seq,
            changes,
            deleted,
            doc,
        });
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Metadata decodes and encodes for one `put_many` call.
    fn write_costs(shard: &mut crate::store::Shard, docs: Vec<Value>) -> (usize, usize) {
        let parses = crate::store::meta_parses();
        let serialises = META_SERIALISES.with(std::cell::Cell::get);
        let seq = Arc::new(AtomicU64::new(shard.seq()));
        let reserved = Arc::new(AtomicU64::new(0));
        let docs = docs.into_iter().enumerate().collect();
        for (_, result) in put_many(shard, docs, true, &seq, &reserved) {
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
        let dir = std::env::temp_dir().join(format!("douchdb-write-cost-{}", uuid()));
        let mut shard = crate::store::Shard::open(&dir, 1000).unwrap();

        // A new document has no stored metadata to decode.
        let costs = write_costs(&mut shard, vec![json!({"_id": "a", "n": 1})]);
        assert_eq!(costs, (0, 1), "create");

        let rev = shard.get_meta("a").unwrap().unwrap().tree.winner().unwrap();
        let update = vec![json!({"_id": "a", "_rev": rev, "n": 2})];
        assert_eq!(write_costs(&mut shard, update), (1, 1), "update");

        // Each document in a batch costs the same.
        let batch = vec![json!({"_id": "b"}), json!({"_id": "c"})];
        assert_eq!(write_costs(&mut shard, batch), (0, 2), "batch");
        let _ = std::fs::remove_dir_all(&dir);
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
        // The watermark ignores shards the mask excludes.
        assert_eq!(
            changes_watermark_from(&[5, 1, 9], &[5, 4, 9], 0, Some(&mask)),
            u64::MAX
        );
        assert_eq!(changes_watermark_from(&[5, 1, 9], &[5, 4, 9], 0, None), 1);
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
    fn nested_db_migration_never_overwrites() {
        let root = std::env::temp_dir().join(format!("douchdb-nest-{}", uuid()));
        for dir in ["a/b", "a%2Fb"] {
            std::fs::create_dir_all(root.join(dir)).unwrap();
            std::fs::write(root.join(dir).join("db.json"), b"{}").unwrap();
        }
        let err = migrate_nested_dbs(&root).unwrap_err();
        assert!(err.contains("already there"), "{err}");
        assert!(root.join("a/b/db.json").is_file());
        assert!(root.join("a%2Fb/db.json").is_file());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn nested_db_migration_moves_deepest_first_and_prunes_empty_parents() {
        let root = std::env::temp_dir().join(format!("douchdb-nest-ok-{}", uuid()));
        for dir in ["a/b/c", "a/b", "x/y", "top"] {
            std::fs::create_dir_all(root.join(dir)).unwrap();
            std::fs::write(root.join(dir).join("db.json"), b"{}").unwrap();
        }
        let moved = migrate_nested_dbs(&root).unwrap();
        assert_eq!(moved.len(), 3);
        for dir in ["a%2Fb%2Fc", "a%2Fb", "x%2Fy", "top"] {
            assert!(root.join(dir).join("db.json").is_file(), "{dir}");
        }
        assert!(!root.join("a").exists());
        assert!(!root.join("x").exists());
        assert!(migrate_nested_dbs(&root).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&root);
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
        let db = Db::open(
            &root,
            name,
            DbConfig {
                q: 1,
                revs_limit: 100,
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
        let db = Db::open(
            &root,
            "stem",
            DbConfig {
                q: 1,
                revs_limit: 2,
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
            revs.push(put[0]["rev"].as_str().unwrap().to_string());
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

    #[tokio::test]
    async fn replicated_writes_do_not_keep_attachments_for_stemmed_revisions() {
        let root = std::env::temp_dir().join(format!("douchdb-stem-atts-{}", crate::util::uuid()));
        let db = Db::open(
            &root,
            "stematts",
            DbConfig {
                q: 1,
                revs_limit: 2,
                shard_map: ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        let ids: Vec<String> = (1..=5).map(|n| format!("{n:032x}")).collect();
        for n in 1..=5usize {
            let chain: Vec<&String> = ids[..n].iter().rev().collect();
            let rev = format!("{n}-{}", ids[n - 1]);
            let doc = json!({
                "_id": "a",
                "_rev": rev,
                "_revisions": {"start": n, "ids": chain},
            });
            db.put_docs(vec![doc], false).await.unwrap();
            // The public write path rejects attachments, so plant one on each
            // revision the way older data may carry them.
            db.shards[0]
                .call(move |shard| {
                    let mut meta = shard.get_meta("a")?.unwrap();
                    meta.atts.insert(
                        rev,
                        vec![(
                            "f".to_string(),
                            AttMeta {
                                content_type: "text/plain".into(),
                                digest: "5d41402abc4b2a76b9719d911017c592".into(),
                                length: 5,
                                revpos: n as u64,
                            },
                        )],
                    );
                    shard.put_raw(&meta_key("a"), &serde_json::to_vec(&meta).unwrap());
                    Ok(())
                })
                .await
                .unwrap();
        }
        let meta = db.shards[0]
            .call(|shard| Ok(shard.get_meta("a")?.unwrap()))
            .await
            .unwrap();
        assert_eq!(meta.tree.revs().count(), 2);
        let kept: Vec<&String> = meta.atts.keys().collect();
        let tree: Vec<&String> = meta.tree.revs().collect();
        assert_eq!(kept, tree, "atts only for revisions still in the tree");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn a_missing_attachment_chunk_fails_the_read() {
        let (db, root) = single_shard_db("att-chunk").await;
        // Uploads are rejected at the API, so plant the attachment directly.
        let put = db.put_docs(vec![json!({"_id": "a"})], true).await.unwrap();
        let rev = put[0]["rev"].as_str().unwrap().to_string();
        db.shards[0]
            .call(move |shard| {
                let mut meta = shard.get_meta("a")?.unwrap();
                meta.atts.insert(
                    rev,
                    vec![(
                        "f.txt".to_string(),
                        AttMeta {
                            content_type: "text/plain".into(),
                            digest: "5d41402abc4b2a76b9719d911017c592".into(),
                            length: 5,
                            revpos: 1,
                        },
                    )],
                );
                shard.put_raw(&meta_key("a"), &serde_json::to_vec(&meta).unwrap());
                shard.put_raw(
                    &blob_prefix("5d41402abc4b2a76b9719d911017c592", 0),
                    b"hello",
                );
                Ok(())
            })
            .await
            .unwrap();
        let opts = DocOpts {
            attachments: true,
            ..Default::default()
        };
        let doc = db.get_doc("a", &opts).await.unwrap();
        assert_eq!(doc["_attachments"]["f.txt"]["data"], "aGVsbG8=");

        db.shards[0]
            .call(|shard| {
                shard.delete_raw(&blob_prefix("5d41402abc4b2a76b9719d911017c592", 0));
                Ok(())
            })
            .await
            .unwrap();
        let error = db.get_doc("a", &opts).await.unwrap_err();
        assert_eq!(error.status, 500, "{error}");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn a_non_ascii_attachment_digest_is_corruption_not_a_panic() {
        let (db, root) = single_shard_db("att-digest").await;
        let put = db.put_docs(vec![json!({"_id": "a"})], true).await.unwrap();
        let rev = put[0]["rev"].as_str().unwrap().to_string();
        db.shards[0]
            .call(move |shard| {
                let mut meta = shard.get_meta("a")?.unwrap();
                meta.atts.insert(
                    rev,
                    vec![(
                        "f.txt".to_string(),
                        AttMeta {
                            content_type: "text/plain".into(),
                            // The second character is two bytes wide, so a
                            // byte-offset slice at 2 lands inside it.
                            digest: "a\u{e9}bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
                            length: 5,
                            revpos: 1,
                        },
                    )],
                );
                shard.put_raw(&meta_key("a"), &serde_json::to_vec(&meta).unwrap());
                Ok(())
            })
            .await
            .unwrap();
        let error = db.get_doc("a", &DocOpts::default()).await.unwrap_err();
        assert_eq!(error.status, 500, "{error}");
        assert!(error.corrupt, "{error}");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn a_corrupt_local_document_is_an_error_not_missing() {
        let (db, root) = single_shard_db("local-corrupt").await;
        db.shards[0]
            .call(|shard| {
                shard.put_raw(&local_key("bad"), b"{not json");
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
                shard.put_raw(&meta_key("_design/v"), b"{not json");
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
                shard.invalidate_latest();
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
    async fn streamed_all_docs(db: &Arc<Db>, opts: &AllDocsOpts) -> Value {
        use futures::StreamExt;
        let (head, rows) = Db::all_docs_stream(db.clone(), opts).await.unwrap();
        let rows: Vec<Value> = Box::pin(rows)
            .map(|row| row.unwrap())
            .collect::<Vec<_>>()
            .await;
        json!({"total_rows": head.total_rows, "offset": head.offset, "rows": rows})
    }

    #[tokio::test]
    async fn streamed_all_docs_match_the_buffered_reply() {
        let root = std::env::temp_dir().join(format!("douchdb-stream-{}", crate::util::uuid()));
        let db = Arc::new(
            Db::open(
                &root,
                "stream",
                DbConfig {
                    q: 3,
                    revs_limit: 100,
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
        let bad_rev = put[450]["rev"].as_str().unwrap().to_string();
        let shard = db.shard_index(bad_id);
        db.shards[shard]
            .call(move |s| {
                s.put_raw(&body_key(bad_id, &bad_rev), b"{not json");
                Ok(())
            })
            .await
            .unwrap();
        // A deleted document does not count.
        let gone = put[10]["rev"].as_str().unwrap();
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
            Db::open(
                &empty_root,
                "empty",
                DbConfig {
                    q: 3,
                    revs_limit: 100,
                    shard_map: ShardMap::balanced(3, 1, 1),
                    partitioned: false,
                },
            )
            .unwrap(),
        );
        let opts = AllDocsOpts::default();
        assert_eq!(
            streamed_all_docs(&empty, &opts).await,
            empty.all_docs(&opts).await.unwrap()
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
                shard.put_raw(&meta_key("bad-meta"), b"{not json");
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
        let bad_rev = put[1]["rev"].as_str().unwrap().to_string();
        let corrupt_rev = bad_rev.clone();
        db.shards[0]
            .call(move |shard| {
                shard.put_raw(&body_key("bad-body", &corrupt_rev), b"{not json");
                Ok(())
            })
            .await
            .unwrap();

        let before = crate::metrics::corrupt_records_skipped();
        let all = db.all_docs(&AllDocsOpts::default()).await.unwrap();
        assert_eq!(row_ids(&all, "rows"), ["a", "bad-body", "c"]);
        assert!(crate::metrics::corrupt_records_skipped() > before);

        let before = crate::metrics::corrupt_records_skipped();
        let all = db
            .all_docs(&AllDocsOpts {
                include_docs: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(row_ids(&all, "rows"), ["a", "c"]);
        assert!(
            crate::metrics::corrupt_records_skipped() > before,
            "the skipped body was not counted"
        );

        let changes = db.changes(&ChangesOpts::default()).await.unwrap();
        assert_eq!(row_ids(&changes, "results"), ["a", "bad-body", "c"]);
        let before = crate::metrics::corrupt_records_skipped();
        let changes = db
            .changes(&ChangesOpts {
                include_docs: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(row_ids(&changes, "results"), ["a", "c"]);
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
        let db = Db::open(
            &root,
            "valfail",
            DbConfig {
                q: 2,
                revs_limit: 100,
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
                shard.inject_flush_failure();
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

    #[test]
    fn security_versions_order_updates_and_survive_reopen() {
        let root = std::env::temp_dir().join(format!("douchdb-secver-{}", crate::util::uuid()));
        let config = || DbConfig {
            q: 1,
            revs_limit: 100,
            shard_map: ShardMap::balanced(1, 1, 1),
            partitioned: false,
        };
        let db = Db::open(&root, "sec", config()).unwrap();
        assert_eq!(db.security_versioned(), (0, json!({})));

        // A legacy bare object is version 0.
        std::fs::write(
            db.path.join("security.json"),
            br#"{"members":{"names":["old"]}}"#,
        )
        .unwrap();
        drop(db);
        let db = Db::open(&root, "sec", config()).unwrap();
        assert_eq!(
            db.security_versioned(),
            (0, json!({"members": {"names": ["old"]}}))
        );

        assert_eq!(db.set_security(json!({"a": 1})).unwrap(), 1);
        assert!(db.set_security_versioned(3, json!({"a": 3})).unwrap());
        // Older versions are rejected; equal ones are ordered by canonical form.
        assert!(!db.set_security_versioned(2, json!({"a": 2})).unwrap());
        assert!(!db.set_security_versioned(3, json!({"a": 2})).unwrap());
        assert!(db.set_security_versioned(3, json!({"a": 4})).unwrap());
        assert!(db.set_security_versioned(3, json!({"a": 4})).unwrap());
        assert_eq!(db.security(), json!({"a": 4}));
        drop(db);

        let db = Db::open(&root, "sec", config()).unwrap();
        assert_eq!(db.security_versioned(), (3, json!({"a": 4})));
        let _ = std::fs::remove_dir_all(&root);
    }

    fn meta_config() -> DbConfig {
        DbConfig {
            q: 1,
            revs_limit: 100,
            shard_map: ShardMap::balanced(1, 1, 1),
            partitioned: false,
        }
    }

    fn open_error(file: &str, contents: &[u8]) -> ApiError {
        let root = std::env::temp_dir().join(format!("douchdb-corrupt-{}", crate::util::uuid()));
        drop(Db::open(&root, "meta", meta_config()).unwrap());
        std::fs::write(db_path(&root, "meta").join(file), contents).unwrap();
        let error = match Db::open(&root, "meta", meta_config()) {
            Ok(_) => panic!("{file} with {contents:?} opened"),
            Err(error) => error,
        };
        // The corrupt file is left for the operator, not overwritten.
        assert_eq!(
            std::fs::read(db_path(&root, "meta").join(file)).unwrap(),
            contents
        );
        std::fs::remove_dir_all(root).ok();
        error
    }

    #[test]
    fn corrupt_metadata_fails_open_with_a_server_error() {
        for (file, contents) in [
            ("db.json", &b"{ not json"[..]),
            ("shard-map.json", b"{ not json"),
            ("shard-map.json", br#"{"q":1,"node_count":1,"replicas":[]}"#),
            ("security-versioned.json", b"{ not json"),
            ("security-versioned.json", br#"{"version":1}"#),
            ("security.json", b"{ not json"),
        ] {
            let error = open_error(file, contents);
            assert_eq!(error.status, 500, "{file}: {}", error.reason);
            assert!(error.reason.contains(file), "{file}: {}", error.reason);
        }
    }

    #[test]
    fn concurrent_equal_version_shard_maps_leave_file_and_memory_in_step() {
        let root = std::env::temp_dir().join(format!("douchdb-mapraced-{}", crate::util::uuid()));
        let config = || DbConfig {
            q: 2,
            revs_limit: 100,
            shard_map: ShardMap::balanced(2, 3, 1),
            partitioned: false,
        };
        for round in 0..40 {
            let db = Db::open(&root, &format!("race{round}"), config()).unwrap();
            let maps: Vec<ShardMap> = (0..3)
                .map(|node| ShardMap {
                    q: 2,
                    node_count: 3,
                    replicas: vec![vec![node], vec![node]],
                    version: 9,
                })
                .collect();
            let barrier = std::sync::Barrier::new(maps.len());
            std::thread::scope(|scope| {
                for map in &maps {
                    let (db, barrier) = (&db, &barrier);
                    scope.spawn(move || {
                        barrier.wait();
                        db.set_shard_map(map.clone()).unwrap();
                    });
                }
            });
            // The tiebreak winner is the greatest placement, and the file on
            // disk is the one in memory.
            let stored: ShardMap =
                serde_json::from_slice(&std::fs::read(db.path.join("shard-map.json")).unwrap())
                    .unwrap();
            assert_eq!(
                db.shard_map().replicas,
                vec![vec![2], vec![2]],
                "round {round}"
            );
            assert_eq!(stored, db.shard_map(), "round {round}");
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

    #[test]
    fn open_removes_stale_temporary_files_and_nothing_else() {
        let root = std::env::temp_dir().join(format!("douchdb-stale-{}", crate::util::uuid()));
        let config = || DbConfig {
            q: 1,
            revs_limit: 100,
            shard_map: ShardMap::single(1),
            partitioned: false,
        };
        let db = Db::open(&root, "stale", config()).unwrap();
        let dir = db.path.clone();
        drop(db);

        // Temporary files an interrupted write_atomic left behind, from
        // another process and from one that happened to share our pid.
        let foreign = format!(".db.json.{}.7.tmp", std::process::id().wrapping_add(1));
        let same_pid = format!(".shard-map.json.{}.0.tmp", std::process::id());
        let search = dir.join("_search_indexes").join("abc");
        std::fs::create_dir_all(&search).unwrap();
        let stale_search = search.join(".douchdb-search-meta.json.1.2.tmp");
        for stale in [
            dir.join(&foreign),
            dir.join(&same_pid),
            stale_search.clone(),
        ] {
            std::fs::write(stale, b"partial").unwrap();
        }
        // Names that merely resemble the pattern are left alone.
        let keep = [
            "db.json.1.2.tmp",
            ".db.json.x.2.tmp",
            ".db.json.1.tmp",
            ".db.json.1.2.tmp.bak",
            ".notes.tmp",
        ];
        for name in keep {
            std::fs::write(dir.join(name), b"keep").unwrap();
        }
        let before = names_in(&dir);

        let db = Db::open(&root, "stale", config()).unwrap();
        drop(db);

        let mut expected: Vec<String> = before
            .into_iter()
            .filter(|name| *name != foreign && *name != same_pid)
            .collect();
        expected.sort();
        assert_eq!(names_in(&dir), expected);
        assert!(!stale_search.exists());
        for name in keep {
            assert!(dir.join(name).exists(), "{name} was removed");
        }
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cleanup_spares_a_temporary_file_a_write_is_using() {
        let dir = std::env::temp_dir().join(format!("douchdb-live-{}", crate::util::uuid()));
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("meta.json");
        let live = write_temp(&target, b"partial", false).unwrap();
        let stale = dir.join(".meta.json.1.1.tmp");
        std::fs::write(&stale, b"old").unwrap();
        remove_stale_temps(&dir);
        assert!(live.0.exists(), "a temporary file in use was removed");
        assert!(!stale.exists());
        std::fs::remove_file(&live.0).unwrap();
        drop(live);
        remove_stale_temps(&dir);
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn an_unusable_pending_shard_map_is_an_error_not_none() {
        let root =
            std::env::temp_dir().join(format!("douchdb-pending-bad-{}", crate::util::uuid()));
        let db = Db::open(
            &root,
            "pending",
            DbConfig {
                q: 1,
                revs_limit: 100,
                shard_map: ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        let pending = db.path.join("pending-shard-map.json");
        let is_err = |text: String| text.starts_with("Err");

        // Missing is the legitimate "nothing pending".
        assert_eq!(format!("{:?}", db.pending_shard_map()), "Ok(None)");
        assert_eq!(
            format!("{:?}", Db::has_pending_shard_map(&root, "pending")),
            "Ok(false)"
        );

        // Content that does not parse.
        std::fs::write(&pending, b"{not json").unwrap();
        assert!(is_err(format!("{:?}", db.pending_shard_map())));
        assert!(is_err(format!(
            "{:?}",
            Db::has_pending_shard_map(&root, "pending")
        )));

        // A read error that is not NotFound: a directory in place of the file.
        std::fs::remove_file(&pending).unwrap();
        std::fs::create_dir(&pending).unwrap();
        assert!(is_err(format!("{:?}", db.pending_shard_map())));
        assert!(is_err(format!(
            "{:?}",
            Db::has_pending_shard_map(&root, "pending")
        )));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn destroy_answers_not_found_only_for_a_missing_database() {
        let root = std::env::temp_dir().join(format!("douchdb-destroy-{}", crate::util::uuid()));
        std::fs::create_dir_all(&root).unwrap();
        assert_eq!(Db::destroy(&root, "gone").unwrap_err().status, 404);

        // A path that exists but is not a directory cannot be removed as one.
        std::fs::write(db_path(&root, "stuck"), b"not a directory").unwrap();
        assert_eq!(Db::destroy(&root, "stuck").unwrap_err().status, 500);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn clear_pending_shard_map_reports_what_it_cannot_remove() {
        let root = std::env::temp_dir().join(format!("douchdb-pending-{}", crate::util::uuid()));
        let db = Db::open(
            &root,
            "pending",
            DbConfig {
                q: 1,
                revs_limit: 100,
                shard_map: ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        let pending = db.path.join("pending-shard-map.json");

        // Nothing pending is not an error.
        db.clear_pending_shard_map().unwrap();

        db.set_pending_shard_map(&ShardMap::single(1)).unwrap();
        assert!(db.pending_shard_map().unwrap().is_some());
        db.clear_pending_shard_map().unwrap();
        assert!(db.pending_shard_map().unwrap().is_none());

        // A path that cannot be unlinked is an error, not a silent success.
        std::fs::create_dir(&pending).unwrap();
        assert!(db.clear_pending_shard_map().is_err());
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

    #[test]
    fn concurrent_client_shard_maps_get_distinct_versions() {
        let root = std::env::temp_dir().join(format!("douchdb-nextmap-{}", crate::util::uuid()));
        let db = Db::open(
            &root,
            "next",
            DbConfig {
                q: 2,
                revs_limit: 100,
                shard_map: ShardMap::balanced(2, 3, 1),
                partitioned: false,
            },
        )
        .unwrap();
        let start = db.shard_map().version;
        let versions: Vec<u64> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|node| {
                    let db = &db;
                    scope.spawn(move || {
                        db.set_next_shard_map(ShardMap {
                            q: 2,
                            node_count: 3,
                            replicas: vec![vec![node % 3], vec![node % 3]],
                            version: 0,
                        })
                        .unwrap()
                        .version
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let mut sorted = versions.clone();
        sorted.sort();
        assert_eq!(sorted, (start + 1..=start + 8).collect::<Vec<_>>());
        assert_eq!(db.shard_map().version, start + 8);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn equal_version_shard_maps_converge_on_one_winner() {
        let root = std::env::temp_dir().join(format!("douchdb-tiebreak-{}", crate::util::uuid()));
        let open = |name: &str| {
            Db::open(
                &root,
                name,
                DbConfig {
                    q: 2,
                    revs_limit: 100,
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
        assert!(first.set_shard_map(a.clone()).unwrap());
        let first_accepts_b = first.set_shard_map(b.clone()).unwrap();
        let second = open("tie-b");
        assert!(second.set_shard_map(b.clone()).unwrap());
        let second_accepts_a = second.set_shard_map(a.clone()).unwrap();

        // Exactly one of the two competing maps wins on both replicas.
        assert_ne!(first_accepts_b, second_accepts_a);
        assert_eq!(first.shard_map().replicas, second.shard_map().replicas);
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
        let db = Db::open(
            &root,
            "stale-mig",
            DbConfig {
                q: 2,
                revs_limit: 100,
                shard_map: active.clone(),
                partitioned: false,
            },
        )
        .unwrap();
        db.set_migration_map(target.clone(), None).unwrap();
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
    fn migration_clears_only_for_its_own_id_unless_expired_or_unnamed() {
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
        let db = Db::open(
            &root,
            "mig-id",
            DbConfig {
                q: 2,
                revs_limit: 100,
                shard_map: active,
                partitioned: false,
            },
        )
        .unwrap();
        // Nothing to clear is fine.
        assert!(db.clear_migration_if(Some("a")));
        db.begin_migration(target.clone(), Some("a".into()))
            .unwrap();
        assert!(!db.clear_migration_if(Some("b")));
        assert!(!db.clear_migration_if(None));
        assert!(db.rebalance_in_progress());
        assert!(db.clear_migration_if(Some("a")));
        assert!(!db.rebalance_in_progress());
        // A migration from a coordinator without ids is cleared by anyone.
        db.begin_migration(target.clone(), None).unwrap();
        assert!(db.clear_migration_if(Some("b")));
        // An expired freeze no longer protects the stale map.
        db.begin_migration(target, Some("a".into())).unwrap();
        while now_millis() < 1 {
            std::thread::yield_now();
        }
        db.rebalance_until.store(1, Ordering::SeqCst);
        assert!(db.clear_migration_if(Some("b")));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn design_documents_replicate_to_every_node() {
        let root = std::env::temp_dir().join(format!("douchdb-placement-{}", crate::util::uuid()));
        let db = Db::open(
            &root,
            "placement",
            DbConfig {
                q: 4,
                revs_limit: 100,
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
        let db = Db::open(
            &root,
            "pending",
            DbConfig {
                q: 1,
                revs_limit: 100,
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
            since: 0,
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
        assert_eq!(last_seq, 2);
        assert_eq!(pending, 3, "three documents remain after the page");

        let (rows, last_seq, pending) = db
            .changes_rows(&ChangesOpts {
                descending: true,
                ..opts
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(last_seq, 4);
        assert_eq!(pending, 3, "three older documents remain descending");

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Requests that touch many documents fan out one call per shard; the
    /// answers must still come back per document, in the order asked.
    #[tokio::test]
    async fn per_document_answers_survive_the_per_shard_fan_out() {
        let q = 4u32;
        let root = std::env::temp_dir().join(format!("douchdb-fan-out-{}", crate::util::uuid()));
        let db = Db::open(
            &root,
            "fanout",
            DbConfig {
                q,
                revs_limit: 100,
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
        assert!(written.iter().all(|row| row["ok"] == json!(true)));
        let info = db.info().await.unwrap();
        assert_eq!(info["doc_count"], json!(12));
        assert_eq!(info["update_seq"], json!(12));

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
            let rev = written[ids.iter().position(|other| other == id).unwrap()]["rev"].clone();
            diff.insert(id.clone(), json!([rev, "2-ffff"]));
        }
        let diff = db.revs_diff(&Value::Object(diff)).await.unwrap();
        let rows = diff.as_object().unwrap();
        assert_eq!(rows.len(), 12);
        for (position, id) in ids.iter().enumerate() {
            assert_eq!(rows[id]["missing"], json!(["2-ffff"]));
            assert_eq!(
                rows[id]["possible_ancestors"],
                json!([written[position]["rev"]])
            );
        }

        // Purging by document answers for each document, and only for the
        // revisions it removed.
        let mut purge = Map::new();
        for (position, id) in ids.iter().enumerate().take(6) {
            purge.insert(id.clone(), json!([written[position]["rev"], "9-nope"]));
        }
        let purged = db.purge(&Value::Object(purge)).await.unwrap();
        for (position, id) in ids.iter().enumerate().take(6) {
            assert_eq!(purged["purged"][id], json!([written[position]["rev"]]));
        }
        assert_eq!(db.info().await.unwrap()["doc_count"], json!(6));

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn changes_cursor_stops_at_allocated_but_uncommitted_shard_sequence() {
        let q = 4u32;
        let root = std::env::temp_dir().join(format!("douchdb-watermark-{}", crate::util::uuid()));
        let db = Db::open(
            &root,
            "watermark",
            DbConfig {
                q,
                revs_limit: 100,
                shard_map: ShardMap::single(q),
                partitioned: false,
            },
        )
        .unwrap();

        let ids: Vec<String> = (0..q as usize)
            .map(|shard| doc_id_for_shard(&db, shard))
            .collect();
        let first: Vec<Value> = ids.iter().map(|id| json!({"_id": id})).collect();
        db.put_docs(first, true).await.unwrap();
        // The second write gets a higher global sequence on shard 0.
        let later = doc_id_for_shard(&db, 0) + "-later";
        db.put_docs(vec![json!({"_id": later})], true)
            .await
            .unwrap();

        // Both writes are committed: no shard has a pending allocation.
        for shard in 0..q as usize {
            assert_eq!(
                db.committed_seq[shard].load(Ordering::SeqCst),
                db.reserved_seq[shard].load(Ordering::SeqCst),
                "shard {shard} sequence bookkeeping"
            );
        }

        // Simulate shard 1 staging a write with a sequence that is not visible
        // yet. A cursor must not advance past shard 1's committed sequence.
        let lagging = 1usize;
        let committed = db.committed_seq[lagging].load(Ordering::SeqCst);
        db.reserved_seq[lagging].store(committed + 100, Ordering::SeqCst);
        let watermark = db.changes_watermark(0, None);
        assert_eq!(watermark, committed);

        let opts = ChangesOpts {
            since: 0,
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
        let (rows, last_seq, _pending) = db.changes_rows(&opts).await.unwrap();
        assert!(rows.iter().all(|row| row.seq <= committed), "{rows:?}");
        assert_eq!(last_seq, committed);

        // Once the simulated write commits, a resumed cursor sees the rows
        // that were withheld, including the later shard-0 write.
        db.reserved_seq[lagging].store(committed, Ordering::SeqCst);
        let resumed = ChangesOpts {
            since: last_seq,
            ..opts
        };
        let (rows, _, _) = db.changes_rows(&resumed).await.unwrap();
        assert!(rows.iter().any(|row| row.id == later));
        assert!(rows.iter().all(|row| row.seq > last_seq));

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn client_visible_update_seq_excludes_uncommitted_allocations() {
        let q = 2u32;
        let root = std::env::temp_dir().join(format!("douchdb-updseq-{}", crate::util::uuid()));
        let db = Db::open(
            &root,
            "updseq",
            DbConfig {
                q,
                revs_limit: 100,
                shard_map: ShardMap::single(q),
                partitioned: false,
            },
        )
        .unwrap();

        db.put_docs(vec![json!({"_id": doc_id_for_shard(&db, 0)})], true)
            .await
            .unwrap();
        // Shard 1 has allocated a sequence it has not committed, and shard 0
        // then commits a higher one.
        let held = db.start_in_flight_write(1);
        let later = doc_id_for_shard(&db, 0) + "-later";
        db.put_docs(vec![json!({"_id": later})], true)
            .await
            .unwrap();

        // Neither the `update_seq` reported by GET /db and `since=now` nor
        // the one in the database info may reach the held sequence, or a
        // feed resumed from it would skip that write when it commits.
        assert!(db.update_seq() < held, "update_seq {}", db.update_seq());
        let info = db.info().await.unwrap();
        let reported = info["update_seq"].as_u64().unwrap();
        assert!(reported < held, "info update_seq {reported}");

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn failed_shard_flush_leaves_no_phantom_documents_or_stalled_changes() {
        let q = 2u32;
        let root = std::env::temp_dir().join(format!("douchdb-flushfail-{}", crate::util::uuid()));
        let db = Db::open(
            &root,
            "flushfail",
            DbConfig {
                q,
                revs_limit: 100,
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
        assert_eq!(all["total_rows"], 1);

        db.shards[0]
            .call(|s| {
                s.inject_flush_failure();
                Ok(())
            })
            .await
            .unwrap();
        let failed = db.put_docs(vec![json!({"_id": lost})], true).await;
        assert!(failed.is_err(), "{failed:?}");

        assert!(db.get_doc(&lost, &DocOpts::default()).await.is_err());
        let all = db.all_docs(&AllDocsOpts::default()).await.unwrap();
        assert_eq!(all["rows"].as_array().unwrap().len(), 1, "{all}");
        assert_eq!(db.info().await.unwrap()["doc_count"], 1);

        // The changes cursor is not held back by the lost write.
        assert_eq!(db.changes_watermark(0, None), db.seq.load(Ordering::SeqCst));
        let opts = ChangesOpts {
            since: 0,
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
        assert!(results[0].get("error").is_none(), "{results:?}");
        assert_eq!(db.info().await.unwrap()["doc_count"], 2);
        let (rows, last_seq, _) = db.changes_rows(&opts).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(last_seq, db.seq.load(Ordering::SeqCst));

        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    use proptest::prelude::*;

    #[test]
    fn watermark_ignores_settled_and_unowned_shards() {
        // Nothing in flight: no bound.
        assert_eq!(changes_watermark_from(&[5, 9], &[5, 9], 0, None), u64::MAX);
        // Shard 1 is in flight at 9; shard 0 is settled.
        assert_eq!(changes_watermark_from(&[5, 9], &[5, 12], 0, None), 9);
        // The same in-flight shard is not ours, so it does not hold us back.
        assert_eq!(
            changes_watermark_from(
                &[5, 9],
                &[5, 12],
                0,
                Some(&ShardMask::from_iter([true, false]))
            ),
            u64::MAX
        );
        // Never below `since`.
        assert_eq!(changes_watermark_from(&[5, 9], &[5, 12], 20, None), 20);
    }

    proptest! {
        #[test]
        fn watermark_respects_every_in_flight_shard(
            committed in prop::collection::vec(0u64..1000, 1..8),
            reserved in prop::collection::vec(0u64..1000, 1..8),
            since in 0u64..1000,
            owned in prop::option::of(prop::collection::vec(any::<bool>(), 1..8)),
        ) {
            let mut reserved = reserved;
            // Make the two arrays the same shape and ensure reserved >= committed.
            reserved.resize(committed.len(), 0);
            for (r, c) in reserved.iter_mut().zip(&committed) {
                *r = (*r).max(*c);
            }
            let mask = owned.map(|mut m| {
                m.resize(committed.len(), false);
                ShardMask::from_iter(m)
            });
            let watermark = changes_watermark_from(
                &committed,
                &reserved,
                since,
                mask.as_ref(),
            );

            // The cursor may advance to the lowest committed sequence of any
            // owned shard that still has an allocated, uncommitted write, but
            // never below `since`. With nothing in flight it is unbounded.
            let expected = (0..committed.len())
                .filter(|&index| mask.as_ref().is_none_or(|m| m.covers(index)))
                .filter(|&index| reserved[index] > committed[index])
                .map(|index| committed[index].max(since))
                .min()
                .unwrap_or(u64::MAX);
            prop_assert_eq!(watermark, expected);
        }

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
}

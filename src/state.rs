//! Shared node state: the open databases, cluster handle, configuration and
//! the counters the HTTP layer and the background tasks both use.

use crate::auth::Principal;
use crate::cluster_http::ClusterCtx;
use crate::db::{db_path, shard_file, sync_dir, ApiError, Db, DbConfig, DbResult, DbSettings};
use crate::dbname::{illegal_db_name, valid_db_name};
use crate::health::FsHealthCache;
use axum::http::HeaderMap;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

/// How many events `_db_updates` remembers.
const DB_UPDATES_KEPT: usize = 5000;

struct DbEvent {
    seq: u64,
    db_name: String,
    kind: String,
}

pub struct DbUpdates {
    events: Mutex<DbEvents>,
    /// Signalled whenever an event is recorded, so a feed waits for it
    /// instead of polling.
    wake: tokio::sync::watch::Sender<()>,
}

impl Default for DbUpdates {
    fn default() -> Self {
        DbUpdates {
            events: Mutex::default(),
            wake: tokio::sync::watch::channel(()).0,
        }
    }
}

#[derive(Default)]
struct DbEvents {
    seq: u64,
    /// Oldest first, strictly increasing in `seq`.
    kept: VecDeque<DbEvent>,
}

impl DbUpdates {
    pub(crate) fn record(&self, name: &str, kind: &str) {
        let mut events = self
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        // The sequence is assigned under the lock so the deque stays ordered.
        events.seq += 1;
        let seq = events.seq;
        if events.kept.len() >= DB_UPDATES_KEPT {
            events.kept.pop_front();
        }
        events.kept.push_back(DbEvent {
            seq,
            db_name: name.to_string(),
            kind: kind.to_string(),
        });
        self.wake.send_replace(());
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<()> {
        self.wake.subscribe()
    }

    pub(crate) fn current(&self) -> u64 {
        self.events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .seq
    }

    /// The events after `after`, oldest first, and the current sequence.
    pub(crate) fn since(&self, after: u64) -> (Vec<Value>, u64) {
        let events = self
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let first = events.kept.partition_point(|event| event.seq <= after);
        let results = events
            .kept
            .iter()
            .skip(first)
            .map(|event| {
                json!({
                    "seq": event.seq.to_string(),
                    "db_name": event.db_name,
                    "type": event.kind,
                })
            })
            .collect();
        (results, events.seq)
    }
}

/// Work done by feeds, counted so tests and operators can tell an idle feed
/// that waits from one that polls. Reported by the local metrics.
#[derive(Default)]
pub struct FeedStats {
    /// Reads of the `_db_updates` event log by a feed.
    pub db_updates_polls: AtomicU64,
    /// Full `_changes` reads (filter resolution included) by a feed.
    pub changes_once: AtomicU64,
}

/// Default for `--max-open-dbs`. CouchDB's `max_dbs_open` is 500, but a
/// database here holds a thread and several file descriptors per shard.
pub const DEFAULT_MAX_OPEN_DBS: usize = 256;

/// Default for `--cache-mb`, the page cache of each shard file.
pub const DEFAULT_CACHE_MB: usize = 16;

/// An open database and when it was last asked for.
pub struct OpenDb {
    db: Arc<Db>,
    used: AtomicU64,
    /// The `used` stamp at which the indexes were last released, so an idle
    /// database is released once per idle spell and not on every sweep.
    released_at: AtomicU64,
}

/// Source of the "last used" stamps. Only their order matters.
static DB_CLOCK: AtomicU64 = AtomicU64::new(0);

impl OpenDb {
    fn new(db: Arc<Db>) -> Self {
        OpenDb {
            db,
            used: AtomicU64::new(DB_CLOCK.fetch_add(1, Ordering::Relaxed)),
            released_at: AtomicU64::new(u64::MAX),
        }
    }

    fn touch(&self) -> Arc<Db> {
        self.used
            .store(DB_CLOCK.fetch_add(1, Ordering::Relaxed), Ordering::Relaxed);
        self.db.clone()
    }
}

/// A database's placement and, when it is open, the handle that can also
/// report counts and sequences.
pub(crate) struct Placement {
    pub(crate) q: u32,
    pub(crate) partitioned: bool,
    pub(crate) shard_map: crate::cluster::ShardMap,
    pub(crate) pending_shard_map: bool,
    pub(crate) open: Option<Arc<Db>>,
}

impl Placement {
    fn of_open(db: Arc<Db>) -> Self {
        Placement {
            q: db.q,
            partitioned: db.partitioned,
            shard_map: db.shard_map(),
            pending_shard_map: db.pending_shard_map().is_some(),
            open: Some(db),
        }
    }
}

type LockMap = Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>;

/// One lock per database name, present only while someone holds or waits on
/// it. Every holder and waiter owns a clone of the entry's `Arc`, taken under
/// the map lock, so a guard that finds nobody else sharing the entry when it
/// lets go can remove it without stranding a waiter on a lock that a later
/// caller would replace with a fresh one.
#[derive(Default)]
pub struct NameLocks {
    map: Arc<LockMap>,
}

/// A name's lock, held or being waited for. Dropping it releases the lock and
/// removes the entry when no one else uses it, also when the wait is cancelled.
pub struct NameGuard {
    held: Option<tokio::sync::OwnedMutexGuard<()>>,
    lock: Option<Arc<tokio::sync::Mutex<()>>>,
    name: String,
    map: Arc<LockMap>,
}

impl NameLocks {
    fn guard(&self, name: &str) -> NameGuard {
        let lock = self
            .map
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .entry(name.to_string())
            .or_default()
            .clone();
        NameGuard {
            held: None,
            lock: Some(lock),
            name: name.to_string(),
            map: self.map.clone(),
        }
    }

    pub async fn lock(&self, name: &str) -> NameGuard {
        let mut guard = self.guard(name);
        if let Some(lock) = guard.lock.clone() {
            guard.held = Some(lock.lock_owned().await);
        }
        guard
    }

    /// The lock if it is free right now.
    pub fn try_lock(&self, name: &str) -> Option<NameGuard> {
        let mut guard = self.guard(name);
        let lock = guard.lock.clone()?;
        guard.held = Some(lock.try_lock_owned().ok()?);
        Some(guard)
    }

    /// Entries in the map.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.map
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .len()
    }
}

impl Drop for NameGuard {
    fn drop(&mut self) {
        self.held = None;
        self.lock = None;
        let mut map = self.map.lock().unwrap_or_else(|poison| poison.into_inner());
        // Under the map lock nobody can clone the entry, so a count of one
        // means only the map holds it.
        if map
            .get(&self.name)
            .is_some_and(|lock| Arc::strong_count(lock) == 1)
        {
            map.remove(&self.name);
        }
    }
}

pub struct AppState {
    pub root: PathBuf,
    /// The open databases, at most `config.max_open_dbs` once idle ones have
    /// been closed. A closed database is reopened from disk on its next use.
    pub dbs: RwLock<HashMap<String, OpenDb>>,
    /// One lock per database name. Open, create and drop of a name run one at
    /// a time, so a name is never opened twice (the second open would fail on
    /// a shard file's lock) and a drop cannot interleave with a create or open.
    pub db_locks: NameLocks,
    /// How many times a database has been opened from disk, creation
    /// included. A closed database that is asked for again counts once more.
    pub db_opens: AtomicU64,
    /// The database-use clock at the previous `release_idle_indexes` sweep.
    pub last_sweep: AtomicU64,
    /// How many times a request's principal was resolved from its headers.
    /// One per request is the goal; the local metrics report it so tests and
    /// operators can count.
    pub principal_resolutions: AtomicU64,
    pub config: Config,
    pub ctx: ClusterCtx,
    pub db_updates: DbUpdates,
    pub feed_stats: FeedStats,
    pub cluster_state: Arc<crate::cluster_state::ClusterState>,
    pub admission: Arc<crate::admission::Admission>,
    /// Bound concurrent `_compact` requests across databases on this node.
    pub compaction: Arc<tokio::sync::Semaphore>,
    /// Recent result of the health check's blocking filesystem probe.
    pub fs_health: FsHealthCache,
    /// Cancelled when the server starts shutting down. Streaming feeds end
    /// (with their trailer) and background tasks stop.
    pub shutdown: tokio_util::sync::CancellationToken,
}

#[derive(Clone)]
pub struct Config {
    pub q: u32,
    pub revs_limit: u64,
    pub sync_secs: u64,
    /// Additional peers probed for membership discovery.
    pub seeds: Vec<String>,
    /// The URL this node advertises in cluster management views.
    pub advertise: String,
    /// Empty disables CORS. `*` allows any origin (the request origin is
    /// echoed back, as CouchDB does); entries may be exact origins or `https://*.example.com`.
    pub cors_origins: Vec<String>,
    /// Send `Access-Control-Allow-Credentials: true` (CouchDB `[cors]
    /// credentials`, default false).
    pub cors_credentials: bool,
    /// Request headers a preflight may ask for, replacing CouchDB's default
    /// list when non-empty (CouchDB `[cors] headers`), lowercase.
    pub cors_headers: Vec<String>,
    /// Methods a preflight may ask for, replacing CouchDB's list when
    /// non-empty (CouchDB `[cors] methods`); case-sensitive.
    pub cors_methods: Vec<String>,
    /// Response headers listed in `Access-Control-Expose-Headers` after the
    /// simple ones, replacing CouchDB's own list when non-empty (CouchDB
    /// `[cors] exposed_headers`), lowercase.
    pub cors_exposed_headers: Vec<String>,
    /// `Access-Control-Max-Age` seconds (CouchDB `[cors] max_age`, default 600).
    pub cors_max_age: u64,
    pub view_timeout_ms: u64,
    pub view_memory_mb: usize,
    /// When true, a database with an empty `_security` object is admin-only.
    /// This is the default whenever authentication is configured.
    pub default_admin_only: bool,
    /// Databases kept open between requests; see `AppState::close_idle`.
    pub max_open_dbs: usize,
    /// Page cache of each shard file (`--cache-mb`).
    pub cache_bytes: usize,
    /// How often an idle standalone `_changes` feed re-checks when no commit
    /// wakes it; see [`LOCAL_FEED_RECHECK`].
    pub local_feed_recheck: std::time::Duration,
}

/// The production value of [`Config::local_feed_recheck`]. A standalone node
/// sees every commit through `Db::subscribe_commits`, so this only bounds how
/// long a security change or other out-of-band state can go unnoticed.
pub const LOCAL_FEED_RECHECK: std::time::Duration = std::time::Duration::from_secs(5);

// ---------------------------------------------------------------------------
// state

impl AppState {
    pub async fn db(&self, name: &str) -> DbResult<Arc<Db>> {
        self.open_db(name, true).await
    }

    /// Like `db`, for a sweep over every database (anti-entropy). A database
    /// opened here enters the cache as the least recently used, and a hit
    /// does not refresh it, so the sweep does not push the databases clients
    /// are using out of the cache. Call `release_cold` once the handle is
    /// dropped to close the ones that no longer fit.
    pub(crate) async fn db_cold(&self, name: &str) -> DbResult<Arc<Db>> {
        self.open_db(name, false).await
    }

    pub(crate) async fn release_cold(&self) {
        self.close_idle().await;
    }

    /// The database if it is open, without opening it and without counting
    /// as a use.
    pub(crate) async fn open_db_if_cached(&self, name: &str) -> Option<Arc<Db>> {
        self.dbs.read().await.get(name).map(|open| open.db.clone())
    }

    async fn open_db(&self, name: &str, hot: bool) -> DbResult<Arc<Db>> {
        if !valid_db_name(name) {
            return Err(ApiError::db_not_found());
        }
        if let Some(open) = self.dbs.read().await.get(name) {
            return Ok(if hot { open.touch() } else { open.db.clone() });
        }
        if !Db::exists(&self.root, name) {
            return Err(ApiError::db_not_found());
        }
        // Concurrent first requests for one name share a single open, and a
        // drop that won the race leaves nothing to open.
        let guard = self.db_locks.lock(name).await;
        if let Some(open) = self.dbs.read().await.get(name) {
            return Ok(if hot { open.touch() } else { open.db.clone() });
        }
        let config = DbConfig {
            q: self.config.q,
            revs_limit: self.config.revs_limit,
            cache_bytes: self.config.cache_bytes,
            shard_map: self.ctx.cluster.shard_map(name, self.config.q),
            partitioned: false,
        };
        let (root, owned) = (self.root.clone(), name.to_string());
        // The name's lock goes along, so the open finishes under it even if
        // this request stops waiting.
        let (opened, _guard) = blocking(move || (Db::open(&root, &owned, config), guard)).await?;
        let db = Arc::new(opened?);
        if let Err(error) = db.store_replaced_shard_map().await {
            tracing::warn!(db = name, error = %error.reason, "cannot store the replaced shard map");
        }
        self.db_opens.fetch_add(1, Ordering::Relaxed);
        let open = OpenDb::new(db.clone());
        if !hot {
            open.used.store(0, Ordering::Relaxed);
        }
        self.dbs.write().await.insert(name.to_string(), open);
        self.close_idle().await;
        Ok(db)
    }

    /// Placement of a database for the management views: from memory when it
    /// is open, from its directory when it is not, and by opening it only
    /// when the directory cannot say (no shard map yet, or one for another cluster size).
    pub(crate) async fn placement(&self, name: &str) -> DbResult<Placement> {
        if !valid_db_name(name) {
            return Err(ApiError::db_not_found());
        }
        if let Some(db) = self.open_db_if_cached(name).await {
            return Ok(Placement::of_open(db));
        }
        let nodes = self.ctx.cluster.nodes.len().max(1);
        let closed = self
            .read_closed(name, move |root, name| {
                Db::read_closed_meta(root, name, nodes)
            })
            .await?;
        match closed {
            Some(meta) => Ok(Placement {
                q: meta.q,
                partitioned: meta.partitioned,
                shard_map: meta.shard_map,
                pending_shard_map: meta.pending_shard_map,
                open: None,
            }),
            None => Ok(Placement::of_open(self.db(name).await?)),
        }
    }

    /// Close the least recently used databases until at most
    /// `max_open_dbs` are open. A database is closed only when nothing but the
    /// map holds it and it is quiescent (see `Db::quiescent`), and its name
    /// is not being opened, created or dropped, so a request in flight never
    /// loses its database and a rebalance is never forgotten.
    ///
    /// Dropping a `Db` joins its shard actor threads, which waits for their
    /// queued work, so the drop runs on the blocking pool and not on an async
    /// worker. The name's lock travels with it: a request that wants the
    /// database back waits for the drop, so it never meets a shard file's
    /// lock that is still held.
    async fn close_idle(&self) {
        self.close_over_cap().await;
        self.release_idle_indexes().await;
    }

    async fn close_over_cap(&self) {
        let cap = self.config.max_open_dbs.max(1);
        let mut closing = Vec::new();
        {
            let mut dbs = self.dbs.write().await;
            if dbs.len() <= cap {
                return;
            }
            let mut idle: Vec<(u64, String)> = dbs
                .iter()
                .filter(|(_, open)| Arc::strong_count(&open.db) == 1)
                .map(|(name, open)| (open.used.load(Ordering::Relaxed), name.clone()))
                .collect();
            idle.sort();
            for (_, name) in idle {
                if dbs.len() <= cap {
                    break;
                }
                // Holding the write lock, nobody can take a new reference
                // from the map, so the count cannot rise from here.
                let quiescent = dbs
                    .get(&name)
                    .is_some_and(|open| Arc::strong_count(&open.db) == 1 && open.db.quiescent());
                if !quiescent {
                    continue;
                }
                let Some(guard) = self.db_locks.try_lock(&name) else {
                    continue;
                };
                if let Some(open) = dbs.remove(&name) {
                    closing.push((open, guard));
                }
            }
        }
        if !closing.is_empty() {
            tokio::task::spawn_blocking(move || drop(closing));
        }
    }

    /// Drop the in-memory indexes of the open databases nobody has asked for
    /// since the previous sweep, so a database that stays open but idle does
    /// not keep every document id in memory. Sweeps happen whenever a
    /// database is opened or created; "idle" is counted in database uses
    /// between sweeps, not in time. The indexes rebuild on the next query.
    async fn release_idle_indexes(&self) {
        let now = DB_CLOCK.load(Ordering::Relaxed);
        let since = self.last_sweep.swap(now, Ordering::Relaxed);
        let idle: Vec<Arc<Db>> = {
            let dbs = self.dbs.read().await;
            dbs.values()
                .filter(|open| {
                    let used = open.used.load(Ordering::Relaxed);
                    used < since
                        && Arc::strong_count(&open.db) == 1
                        && open.released_at.swap(used, Ordering::Relaxed) != used
                })
                .map(|open| open.db.clone())
                .collect()
        };
        // Idle databases have nothing queued, so this does not hold up the
        // caller. It runs after any closing so the handles taken here do not
        // make a database look busy to `close_over_cap`.
        for db in idle {
            db.release_indexes().await;
        }
    }

    /// The `_security` object of a database, read from memory when it is
    /// open and from its shard 0 when it is not, so that asking who may see
    /// a database does not open it. `None` when there is no such database or
    /// its security cannot be read.
    pub async fn db_security(&self, name: &str) -> Option<Value> {
        if !valid_db_name(name) {
            return None;
        }
        if let Some(open) = self.dbs.read().await.get(name) {
            return Some(open.db.security());
        }
        match self.read_closed(name, Db::read_security).await {
            Ok(Some(security)) => Some(security),
            // Open after all: ask the open database.
            Ok(None) => {
                let security = self.db_cold(name).await.ok().map(|db| db.security());
                self.release_cold().await;
                security
            }
            Err(_) => None,
        }
    }

    /// `read` the settings of database `name` while it is closed, on the
    /// blocking pool. `read` answers `None` when the file is open (or only
    /// an open can say): then this waits for the name's lock, so an open or
    /// idle close in progress finishes, and reads again unless the database
    /// is now open. `None` means: ask the open database.
    async fn read_closed<T: Send + 'static>(
        &self,
        name: &str,
        read: impl Fn(&Path, &str) -> DbResult<Option<T>> + Clone + Send + 'static,
    ) -> DbResult<Option<T>> {
        let (root, owned) = (self.root.clone(), name.to_string());
        let first = read.clone();
        if let Some(found) = blocking(move || first(&root, &owned)).await?? {
            return Ok(Some(found));
        }
        let _guard = self.db_locks.lock(name).await;
        if self.open_db_if_cached(name).await.is_some() {
            return Ok(None);
        }
        let (root, owned) = (self.root.clone(), name.to_string());
        blocking(move || read(&root, &owned)).await?
    }

    /// Create a database with `q` shards and `n` copies of each; `None`
    /// takes the node's configured default.
    pub async fn create_db(
        &self,
        name: &str,
        partitioned: bool,
        q: Option<u32>,
        n: Option<usize>,
    ) -> DbResult<Arc<Db>> {
        if !valid_db_name(name) {
            return Err(illegal_db_name(name));
        }
        let guard = self.db_locks.lock(name).await;
        let path = db_path(&self.root, name);
        crate::view::forget_db(&path).await;
        crate::search::forget_db(&path).await;
        let config = DbConfig {
            q: q.unwrap_or(self.config.q),
            revs_limit: self.config.revs_limit,
            cache_bytes: self.config.cache_bytes,
            shard_map: self.ctx.cluster.shard_map_n(
                name,
                q.unwrap_or(self.config.q),
                n.unwrap_or(self.ctx.cluster.n),
            ),
            partitioned,
        };
        let (root, owned) = (self.root.clone(), name.to_string());
        let (created, _guard) = blocking(move || {
            let created = if Db::exists(&root, &owned) {
                Err(ApiError::new(
                    412,
                    "file_exists",
                    "The database could not be created, the file already exists.",
                ))
            } else {
                create_db_staged(&root, &owned, &DbSettings::of(&config))
                    .and_then(|()| Db::open(&root, &owned, config))
            };
            (created, guard)
        })
        .await?;
        let db = Arc::new(created?);
        self.db_opens.fetch_add(1, Ordering::Relaxed);
        self.dbs
            .write()
            .await
            .insert(name.to_string(), OpenDb::new(db.clone()));
        self.close_idle().await;
        self.db_updates.record(name, "created");
        Ok(db)
    }

    pub async fn drop_db(&self, name: &str) -> DbResult<()> {
        if !valid_db_name(name) {
            return Err(ApiError::db_not_found());
        }
        let _guard = self.db_locks.lock(name).await;
        // Closed before the gates are forgotten: a request holding the handle
        // can no longer take a fresh gate and reach the index directories,
        // and its shards answer `not_found`. Their files are closed before
        // they are renamed away, so no late close can record a file as
        // verified once a new one may take its name.
        let open = self.dbs.write().await.remove(name);
        if let Some(open) = open {
            open.db.close_deleted().await;
            tokio::task::spawn_blocking(move || drop(open));
        }
        let path = db_path(&self.root, name);
        crate::view::forget_db(&path).await;
        crate::search::forget_db(&path).await;
        let (root, owned) = (self.root.clone(), name.to_string());
        blocking(move || delete_db(&root, &owned)).await??;
        self.db_updates.record(name, "deleted");
        Ok(())
    }

    /// Names of the databases that are open now.
    pub(crate) async fn open_db_names(&self) -> std::collections::HashSet<String> {
        self.dbs.read().await.keys().cloned().collect()
    }

    /// Every database: each directory of the data root that stands for a
    /// legal database name and holds a shard file (see `db::is_db_dir`).
    /// Staging and tombstone directories cannot (a leading `.`), and a
    /// database whose files are damaged is still one.
    pub async fn db_names(&self) -> Vec<String> {
        let root = self.root.clone();
        blocking(move || {
            let mut names: Vec<String> = std::fs::read_dir(&root)
                .into_iter()
                .flatten()
                .flatten()
                .filter(|entry| crate::db::is_db_dir(&entry.path()))
                .filter_map(|entry| {
                    entry
                        .file_name()
                        .to_str()
                        .and_then(crate::db::dir_name_to_db_name)
                })
                .filter(|name| valid_db_name(name))
                .collect();
            names.sort();
            names
        })
        .await
        .unwrap_or_default()
    }

    /// The caller's principal. With no auth configured every request is a
    /// service admin, preserving the single-user default. Internal cluster
    /// requests without user credentials are service admins; peers are trusted
    /// by the shared cluster secret.
    pub fn principal(&self, headers: &HeaderMap) -> Principal {
        self.principal_resolutions.fetch_add(1, Ordering::Relaxed);
        let Some(auth) = &self.ctx.auth else {
            return Principal::service_admin();
        };
        let principal = auth.principal(headers);
        if principal.is_authenticated() {
            return principal;
        }
        if self.ctx.cluster.is_internal(headers) || self.ctx.cluster.is_internal_db(headers) {
            return Principal::service_admin();
        }
        principal
    }
}

/// Run blocking file work on the blocking pool, not on a runtime worker.
async fn blocking<R: Send + 'static>(work: impl FnOnce() -> R + Send + 'static) -> DbResult<R> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| ApiError::internal(format!("blocking task failed: {error}")))
}

/// Name prefix of the directory a database is built in before it is
/// published. No database name starts with a `.`.
const STAGING_PREFIX: &str = ".creating-";

/// Name prefix of the directory a deleted database is moved to before it is
/// removed.
const TOMBSTONE_PREFIX: &str = ".deleted-";

/// Page cache while a new database's files are written: they are empty.
const CREATE_CACHE: usize = 1 << 20;

/// A step of creating or deleting a database, after which a test can stop
/// as a crash would (see `reached`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    /// Create: the staging directory exists.
    Staged,
    /// Create: every shard file exists, empty.
    ShardFiles,
    /// Create: shard 0's settings are committed.
    Settings,
    /// Create: the staging directory is synced.
    StagingSynced,
    /// Create: the database has its name.
    Published,
    /// Create: the data root is synced.
    PublishSynced,
    /// Delete: the database has its tombstone's name.
    Tombstoned,
    /// Delete: the data root is synced.
    TombstoneSynced,
}

/// Where tests stop creates and deletes, by data root.
#[cfg(test)]
static STOPS: Mutex<Vec<(PathBuf, Step)>> = Mutex::new(Vec::new());

/// What a stop unwinds with.
#[cfg(test)]
struct Crashed;

/// `step` of a create or delete under `root` is done. A test that asked to
/// stop there gets an unwind, which skips every cleanup as a crash would.
fn reached(root: &Path, step: Step) {
    #[cfg(test)]
    if STOPS
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .iter()
        .any(|(at, stop)| at == root && *stop == step)
    {
        std::panic::resume_unwind(Box::new(Crashed));
    }
    #[cfg(not(test))]
    let _ = (root, step);
}

fn io_error(what: &Path, error: impl std::fmt::Display) -> ApiError {
    tracing::error!(path = %what.display(), %error, "database directory operation failed");
    ApiError::internal(format!(
        "{}: {error}",
        what.file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    ))
}

/// Create database `name` under `root` so that a crash at any point leaves
/// either no database or the whole one: build it in a staging directory
/// (every shard file, then shard 0's settings committed last), sync that
/// directory, rename it to the database's name and sync the root. Done
/// once this returns. Blocking. A failure removes the staging directory; a
/// crash leaves it for `recover_root`.
pub fn create_db_staged(root: &Path, name: &str, settings: &DbSettings) -> DbResult<()> {
    let rows = settings.rows()?;
    std::fs::create_dir_all(root).map_err(|error| io_error(root, error))?;
    let staging = root.join(format!("{STAGING_PREFIX}{}", crate::util::uuid()));
    std::fs::create_dir(&staging).map_err(|error| io_error(&staging, error))?;
    let built = build_staged(root, &staging, name, settings.q, &rows);
    if built.is_err() {
        let _ = std::fs::remove_dir_all(&staging);
    }
    built
}

fn build_staged(
    root: &Path,
    staging: &Path,
    name: &str,
    q: u32,
    rows: &[(&str, Vec<u8>)],
) -> DbResult<()> {
    reached(root, Step::Staged);
    let mut first = None;
    for index in 0..q {
        let file = shard_file(staging, index);
        let db = crate::store::create_file(&file, CREATE_CACHE)
            .map_err(|error| io_error(&file, format!("{error:#}")))?;
        if index == 0 {
            first = Some(db);
        }
    }
    reached(root, Step::ShardFiles);
    if let Some(first) = first {
        crate::store::put_settings(&first, rows)
            .map_err(|error| io_error(&shard_file(staging, 0), format!("{error:#}")))?;
    }
    reached(root, Step::Settings);
    sync_dir(staging).map_err(|error| io_error(staging, error))?;
    reached(root, Step::StagingSynced);
    let path = db_path(root, name);
    if let Err(error) = std::fs::rename(staging, &path) {
        return Err(if path.exists() {
            ApiError::new(
                412,
                "file_exists",
                "The database could not be created, the file already exists.",
            )
        } else {
            io_error(&path, error)
        });
    }
    reached(root, Step::Published);
    // New files under these names: their first open checks them, whatever
    // an earlier file of the name left recorded.
    for index in 0..q {
        crate::kv::forget_verified(&shard_file(&path, index));
    }
    sync_dir(root).map_err(|error| io_error(root, error))?;
    reached(root, Step::PublishSynced);
    Ok(())
}

/// Delete database `name` under `root`: rename it to a tombstone and sync
/// the root, which is the delete; the files are removed in the background,
/// or by `recover_root` after a crash. Blocking; its shards must be closed.
pub fn delete_db(root: &Path, name: &str) -> DbResult<()> {
    let path = db_path(root, name);
    if !crate::db::is_db_dir(&path) {
        return Err(ApiError::db_not_found());
    }
    // The shard files, named before they move: their names are forgotten
    // below whatever happens after the rename.
    let files: Vec<PathBuf> = std::fs::read_dir(&path)
        .map_err(|error| io_error(&path, error))?
        .flatten()
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "redb"))
        .map(|entry| path.join(entry.file_name()))
        .collect();
    let tombstone = root.join(format!("{TOMBSTONE_PREFIX}{}", crate::util::uuid()));
    match std::fs::rename(&path, &tombstone) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(ApiError::db_not_found())
        }
        Err(error) => return Err(io_error(&path, error)),
    }
    // A file created later under one of these names is a new one, which its
    // first open checks. Forgotten straight after the rename, so neither a
    // failed sync below nor a read of a closed file (`kv::peek`, which holds
    // the file's lock) that ran before can leave one recorded.
    for file in &files {
        crate::kv::forget_verified(file);
    }
    reached(root, Step::Tombstoned);
    sync_dir(root).map_err(|error| io_error(root, error))?;
    reached(root, Step::TombstoneSynced);
    tokio::task::spawn_blocking(move || {
        if let Err(error) = std::fs::remove_dir_all(&tombstone) {
            tracing::warn!(path = %tombstone.display(), %error, "cannot remove a deleted database");
        }
    });
    Ok(())
}

/// Finish what a crash interrupted, before the node serves: remove every
/// staging and tombstone directory under `root`, then sync it. Nothing
/// under a database's own name is touched, whatever state it is in.
pub fn recover_root(root: &Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let interrupted = entry.file_name().to_str().is_some_and(|name| {
            name.starts_with(STAGING_PREFIX) || name.starts_with(TOMBSTONE_PREFIX)
        });
        if interrupted && entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
            tracing::info!(path = %entry.path().display(), "removed an interrupted create or delete");
        }
    }
    sync_dir(root)
}

/// A node state over `root` for tests: one node, no peers, no auth.
#[cfg(test)]
pub(crate) fn test_state(root: PathBuf, max_open_dbs: usize) -> Arc<AppState> {
    Arc::new(AppState {
        root,
        dbs: Default::default(),
        db_locks: Default::default(),
        db_opens: Default::default(),
        last_sweep: Default::default(),
        principal_resolutions: Default::default(),
        fs_health: Default::default(),
        shutdown: Default::default(),
        config: Config {
            q: 1,
            revs_limit: 1000,
            sync_secs: 60,
            seeds: Vec::new(),
            advertise: String::new(),
            cors_origins: Vec::new(),
            cors_credentials: false,
            cors_headers: Vec::new(),
            cors_methods: Vec::new(),
            cors_exposed_headers: Vec::new(),
            cors_max_age: 600,
            view_timeout_ms: 1000,
            view_memory_mb: 1,
            default_admin_only: true,
            max_open_dbs,
            cache_bytes: DEFAULT_CACHE_MB << 20,
            // A test must not be answered by this fallback: with it out of
            // reach, a feed that does not wake for a commit hangs instead of
            // passing late.
            local_feed_recheck: std::time::Duration::from_secs(3600),
        },
        ctx: ClusterCtx {
            auth: None,
            cluster: crate::cluster::Cluster {
                nodes: Vec::new(),
                me: 0,
                n: 1,
                secret: String::new(),
            },
            client: reqwest::Client::new(),
            health: Default::default(),
        },
        db_updates: Default::default(),
        feed_stats: Default::default(),
        cluster_state: Arc::new(crate::cluster_state::ClusterState::new()),
        admission: Arc::new(crate::admission::Admission::new(Default::default())),
        compaction: Arc::new(tokio::sync::Semaphore::new(1)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::AllDocsOpts;

    /// Every database ever opened used to stay open, with its shard actors and
    /// stores, until it was deleted.
    #[tokio::test]
    async fn open_databases_stay_within_the_cap() {
        let root = std::env::temp_dir().join(format!("douchdb-dbcap-{}", crate::util::uuid()));
        let st = test_state(root.clone(), 4);
        for n in 0..12 {
            let name = format!("db{n}");
            st.create_db(&name, false, None, None).await.unwrap();
            st.db(&name).await.unwrap();
        }
        // Reopen the first ones: they were closed, and come back from disk.
        for n in 0..3 {
            let db = st.db(&format!("db{n}")).await.unwrap();
            db.put_docs(vec![json!({"_id": "a"})], true).await.unwrap();
        }
        let open = st.dbs.read().await.len();
        assert!(open <= 4, "{open} databases are open, cap is 4");
        drop(st);
        let _ = std::fs::remove_dir_all(root);
    }

    /// The per-name locks were never removed: one entry stayed for every
    /// distinct name that was created, opened or dropped.
    #[tokio::test]
    async fn name_locks_are_removed_once_unused() {
        let root = std::env::temp_dir().join(format!("douchdb-locks-{}", crate::util::uuid()));
        let st = test_state(root.clone(), 2);
        for n in 0..8 {
            let name = format!("db{n}");
            st.create_db(&name, false, None, None).await.unwrap();
            st.db(&name).await.unwrap();
        }
        for n in 0..4 {
            st.drop_db(&format!("db{n}")).await.unwrap();
        }
        // A drop of a name that never existed, and an open of one.
        let _ = st.drop_db("never").await;
        let _ = st.db("never").await;
        // Closing an idle database releases its lock on the blocking pool, so
        // wait for the map to drain rather than for a fixed time.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        loop {
            let left = st.db_locks.len();
            if left == 0 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{left} name locks are still in the map"
            );
            tokio::task::yield_now().await;
        }
        st.dbs.write().await.clear();
        let _ = std::fs::remove_dir_all(root);
    }

    /// A database a request holds, or one that is being rebalanced, stays
    /// open past the cap; idle ones go.
    #[tokio::test]
    async fn busy_databases_are_not_closed() {
        let root = std::env::temp_dir().join(format!("douchdb-dbbusy-{}", crate::util::uuid()));
        let st = test_state(root.clone(), 1);
        let held = st.create_db("held", false, None, None).await.unwrap();
        let moving = st.create_db("moving", false, None, None).await.unwrap();
        assert!(moving.try_start_rebalance());
        drop(moving);
        for n in 0..4 {
            st.create_db(&format!("idle{n}"), false, None, None)
                .await
                .unwrap();
        }
        let dbs = st.dbs.read().await;
        assert!(dbs.contains_key("held"), "a database in use was closed");
        assert!(
            dbs.contains_key("moving"),
            "a rebalancing database was closed"
        );
        assert_eq!(dbs.len(), 3, "the idle ones are closed down to the pinned");
        drop(dbs);
        // The held database still works.
        held.put_docs(vec![json!({"_id": "a"})], true)
            .await
            .unwrap();
        drop(held);
        st.dbs.write().await.clear();
        let _ = std::fs::remove_dir_all(root);
    }

    /// A database that stays open but is not asked for between two sweeps
    /// gives up its in-memory indexes; one in use keeps them, and the idle
    /// one rebuilds them on its next query.
    #[tokio::test]
    async fn idle_databases_release_their_indexes() {
        let root = std::env::temp_dir().join(format!("douchdb-dbrelease-{}", crate::util::uuid()));
        let st = test_state(root.clone(), 10);
        let docs =
            |n: usize| -> Vec<Value> { (0..n).map(|i| json!({"_id": format!("d{i}")})).collect() };
        // An `_all_docs` page from a startkey builds the live index, a
        // pending count the latest-changes index.
        let query = |db: Arc<Db>| async move {
            let page = AllDocsOpts {
                startkey: Some("d1".into()),
                ..AllDocsOpts::default()
            };
            assert_eq!(db.all_docs(&page).await.unwrap().rows.len(), 2);
            assert_eq!(
                db.changes_pending(&crate::db::ShardSince::default(), false, None)
                    .await
                    .unwrap(),
                3
            );
        };
        let build = |db: Arc<Db>| async move {
            db.put_docs(docs(3), true).await.unwrap();
            query(db.clone()).await;
            assert_eq!(db.index_entries().await, 6);
        };
        build(st.create_db("idle", false, None, None).await.unwrap()).await;
        build(st.create_db("busy", false, None, None).await.unwrap()).await;
        // Use "busy" again, then open another database: the sweep it runs
        // finds "idle" untouched since the last one and "busy" in use.
        drop(st.db("busy").await.unwrap());
        drop(st.create_db("other", false, None, None).await.unwrap());
        let idle = st.open_db_if_cached("idle").await.unwrap();
        assert_eq!(
            idle.index_entries().await,
            0,
            "the idle database kept its indexes"
        );
        assert_eq!(
            st.open_db_if_cached("busy")
                .await
                .unwrap()
                .index_entries()
                .await,
            6
        );
        // The next query rebuilds them.
        query(idle.clone()).await;
        assert_eq!(idle.index_entries().await, 6);
        drop(idle);
        st.dbs.write().await.clear();
        let _ = std::fs::remove_dir_all(root);
    }

    /// A request that cloned the database before it was deleted must not be
    /// able to touch the index directories afterwards, neither while the
    /// directory is removed nor in a database recreated under the same name:
    /// the deleted database's gate is gone, so a stale handle would otherwise
    /// take a fresh one. CouchDB answers 404 once the database is gone.
    #[tokio::test]
    async fn a_stale_handle_cannot_touch_the_indexes_of_a_deleted_database() {
        let root = std::env::temp_dir().join(format!("douchdb-stale-{}", crate::util::uuid()));
        let st = test_state(root.clone(), 4);
        let ddoc = json!({"_id": "_design/d",
            "views": {"v": {"map": "function(doc) { emit(doc.n, null); }"}},
            "indexes": {"s": {"index": "function(doc){ if (doc.n) index('n', doc.n); }"}}});
        let stale = st.create_db("stale", false, None, None).await.unwrap();
        stale
            .put_docs(vec![json!({"_id": "a", "n": 1}), ddoc], true)
            .await
            .unwrap();
        let view_query = crate::view::ViewQuery::default();
        let limits = view_query.limits;
        let request = crate::search::SearchRequest {
            query: "n:1".to_string(),
            ..Default::default()
        };
        crate::view::query(&stale, "_design/d", "v", &view_query)
            .await
            .unwrap();
        crate::search::query(&stale, "_design/d", "s", &request, limits)
            .await
            .unwrap();
        let path = stale.path.clone();
        assert!(path.join("_view_indexes").exists());
        assert!(path.join("_search_indexes").exists());

        // The request still holds its handle when the database goes away.
        st.drop_db("stale").await.unwrap();
        assert!(!path.exists());
        let error = crate::view::query(&stale, "_design/d", "v", &view_query)
            .await
            .expect_err("a view query ran on a deleted database");
        assert_eq!(error.status, 404, "{}", error.reason);
        let error = crate::search::query(&stale, "_design/d", "s", &request, limits)
            .await
            .expect_err("a search ran on a deleted database");
        assert_eq!(error.status, 404, "{}", error.reason);
        assert!(!path.exists(), "a stale query recreated the directory");

        // Nor can it reach the database created under the same name.
        let fresh = st.create_db("stale", false, None, None).await.unwrap();
        let error = crate::view::query(&stale, "_design/d", "v", &view_query)
            .await
            .expect_err("a stale view query ran on the recreated database");
        assert_eq!(error.status, 404, "{}", error.reason);
        let error = crate::search::query(&stale, "_design/d", "s", &request, limits)
            .await
            .expect_err("a stale search ran on the recreated database");
        assert_eq!(error.status, 404, "{}", error.reason);
        assert!(!path.join("_view_indexes").exists());
        assert!(!path.join("_search_indexes").exists());
        drop((stale, fresh));
        st.dbs.write().await.clear();
        let _ = std::fs::remove_dir_all(root);
    }

    /// A data root of its own, removed by the test.
    fn scratch_root(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("douchdb-{tag}-{}", crate::util::uuid()))
    }

    /// Stop creates and deletes under `root` after `step`, as a crash would.
    fn stop_at(root: &Path, step: Step) {
        STOPS.lock().unwrap().push((root.to_path_buf(), step));
    }

    fn clear_stops(root: &Path) {
        STOPS.lock().unwrap().retain(|(at, _)| at != root);
    }

    /// The names in `dir`, sorted.
    fn entries(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    /// What a restart does before the node serves: recovery, then a fresh
    /// state over the same root.
    fn restart(root: &Path) -> Arc<AppState> {
        recover_root(root).unwrap();
        test_state(root.to_path_buf(), 8)
    }

    /// A name that needs encoding as a directory, so staging and tombstone
    /// names are checked against an encoded one.
    const NAME: &str = "a/b";

    /// A crash at any step before the rename leaves a staging directory
    /// that recovery removes: the database never existed, and the name is
    /// free.
    #[tokio::test]
    async fn a_crash_during_create_leaves_no_database() {
        for step in [
            Step::Staged,
            Step::ShardFiles,
            Step::Settings,
            Step::StagingSynced,
        ] {
            let root = scratch_root("crash-create");
            let st = test_state(root.clone(), 8);
            stop_at(&root, step);
            let error = st
                .create_db(NAME, false, Some(2), None)
                .await
                .err()
                .expect("the create was not stopped");
            assert_eq!(error.status, 500, "{step:?}: {}", error.reason);
            clear_stops(&root);
            let left = entries(&root);
            assert_eq!(left.len(), 1, "{step:?}: {left:?}");
            assert!(left[0].starts_with(STAGING_PREFIX), "{step:?}: {left:?}");
            assert!(st.db_names().await.is_empty(), "{step:?}: listed");
            drop(st);

            let st = restart(&root);
            assert!(entries(&root).is_empty(), "{step:?}: {:?}", entries(&root));
            assert!(st.db_names().await.is_empty(), "{step:?}");
            assert_eq!(
                st.db(NAME).await.err().map(|e| e.status),
                Some(404),
                "{step:?}"
            );
            st.create_db(NAME, false, Some(2), None).await.unwrap();
            drop(st);
            let _ = std::fs::remove_dir_all(root);
        }
    }

    /// Once renamed into place the database exists, synced or not, and
    /// recovery keeps it whole.
    #[tokio::test]
    async fn a_crash_after_publish_keeps_the_database() {
        for step in [Step::Published, Step::PublishSynced] {
            let root = scratch_root("crash-publish");
            let st = test_state(root.clone(), 8);
            stop_at(&root, step);
            assert!(st.create_db(NAME, false, Some(2), None).await.is_err());
            clear_stops(&root);
            drop(st);

            let st = restart(&root);
            assert_eq!(entries(&root), ["a%2Fb"], "{step:?}");
            assert_eq!(st.db_names().await, [NAME], "{step:?}");
            let db = st.db(NAME).await.unwrap();
            assert_eq!(db.q, 2);
            assert_eq!(db.shard_map(), crate::cluster::ShardMap::single(2));
            assert_eq!(db.security_versioned(), (0, json!({})));
            db.put_docs(vec![json!({"_id": "x"})], true).await.unwrap();
            drop(db);
            drop(st);
            let _ = std::fs::remove_dir_all(root);
        }
    }

    /// The rename to the tombstone is the delete: a crash after it leaves a
    /// tombstone that recovery removes, and the name is free for a new,
    /// empty database.
    #[tokio::test]
    async fn a_crash_during_delete_completes_the_delete() {
        for step in [Step::Tombstoned, Step::TombstoneSynced] {
            let root = scratch_root("crash-delete");
            let st = test_state(root.clone(), 8);
            let db = st.create_db(NAME, false, Some(2), None).await.unwrap();
            db.put_docs(vec![json!({"_id": "x"})], true).await.unwrap();
            drop(db);
            stop_at(&root, step);
            assert!(st.drop_db(NAME).await.is_err(), "{step:?}: not stopped");
            clear_stops(&root);
            let left = entries(&root);
            assert_eq!(left.len(), 1, "{step:?}: {left:?}");
            assert!(left[0].starts_with(TOMBSTONE_PREFIX), "{step:?}: {left:?}");
            assert!(st.db_names().await.is_empty(), "{step:?}: listed");
            drop(st);

            let st = restart(&root);
            assert!(entries(&root).is_empty(), "{step:?}: {:?}", entries(&root));
            assert!(st.db_names().await.is_empty(), "{step:?}");
            assert_eq!(st.drop_db(NAME).await.unwrap_err().status, 404);
            let db = st.create_db(NAME, false, Some(2), None).await.unwrap();
            assert_eq!(db.info().await.unwrap().doc_count, 0, "{step:?}");
            drop(db);
            drop(st);
            let _ = std::fs::remove_dir_all(root);
        }
    }

    /// A database whose shard file is missing or damaged is damage, not an
    /// interrupted create: it stays listed, every request to it answers 500,
    /// and neither the requests nor recovery create, replace or remove any
    /// of its files. The damaged file is left byte for byte. A healthy shard
    /// 0 is opened to learn `q` (redb keeps bookkeeping in its header), so
    /// it is checked by putting the damaged file back: the database then
    /// opens with every document.
    #[tokio::test]
    async fn a_missing_shard_fails_closed_and_keeps_the_other_files() {
        let damage: [(&str, Option<&[u8]>); 5] = [
            ("shard-0001.redb", None),
            ("shard-0000.redb", None),
            ("shard-0001.redb", Some(&[0xab; 8192])),
            ("shard-0001.redb", Some(b"")),
            // A redb file whose shard lost its settings: damage only the
            // shard layer can see.
            ("shard-0001.redb", Some(b"settings")),
        ];
        for (file, replacement) in damage {
            let root = scratch_root("fail-closed");
            let st = test_state(root.clone(), 8);
            let db = st.create_db("m", false, Some(2), None).await.unwrap();
            let docs = (0..8).map(|i| json!({"_id": format!("d{i}")})).collect();
            db.put_docs(docs, true).await.unwrap();
            drop(db);
            drop(st);

            let dir = db_path(&root, "m");
            let original = std::fs::read(dir.join(file)).unwrap();
            match replacement {
                None => std::fs::remove_file(dir.join(file)).unwrap(),
                Some(b"settings") => {
                    let db = crate::kv::open(&dir.join(file), 1 << 20).unwrap();
                    let txn = db.begin_write().unwrap();
                    txn.open_table(crate::kv::SETTINGS)
                        .unwrap()
                        .retain(|_, _| false)
                        .unwrap();
                    txn.commit().unwrap();
                    drop(db);
                    crate::kv::forget_verified(&dir.join(file));
                }
                Some(bytes) => std::fs::write(dir.join(file), bytes).unwrap(),
            }
            let files = entries(&dir);
            let damaged = std::fs::read(dir.join(file)).ok();
            let others: Vec<(String, Vec<u8>)> = files
                .iter()
                .map(|name| (name.clone(), std::fs::read(dir.join(name)).unwrap()))
                .collect();

            let st = restart(&root);
            assert_eq!(st.db_names().await, ["m"], "{file}");
            let error = st.db("m").await.err().expect("a damaged database opened");
            assert_eq!(error.status, 500, "{file}: {}", error.reason);
            // Each request after the first opens the healthy files read-only
            // and closes none of them with a check: not a byte changes, and
            // the files stay verified.
            let first = entries(&dir)
                .into_iter()
                .map(|name| (name.clone(), std::fs::read(dir.join(&name)).unwrap()))
                .collect::<Vec<_>>();
            for _ in 0..2 {
                let error = st.db("m").await.err().expect("a damaged database opened");
                assert_eq!(error.status, 500, "{file}: {}", error.reason);
            }
            for (name, bytes) in &first {
                let now = std::fs::read(dir.join(name)).unwrap();
                assert!(now == *bytes, "{file}: a request wrote to {name}");
            }
            if file != "shard-0000.redb" {
                assert!(crate::kv::is_verified(&shard_file(&dir, 0)), "{file}");
            }
            // Placement lives in shard 0: readable while shard 0 is.
            let placement = st.placement("m").await.err().map(|error| error.status);
            let expected = (file == "shard-0000.redb").then_some(500);
            assert_eq!(placement, expected, "{file}: placement");
            drop(st);
            recover_root(&root).unwrap();
            assert_eq!(entries(&dir), files, "{file}: files created or removed");
            if replacement == Some(b"settings") {
                // A redb file is opened, once writable, to be checked; its
                // shard is not filled in again.
                let db = crate::kv::open(&dir.join(file), 1 << 20).unwrap();
                let txn = redb::ReadableDatabase::begin_read(&db).unwrap();
                let settings = txn.open_table(crate::kv::SETTINGS).unwrap();
                assert!(redb::ReadableTable::first(&settings).unwrap().is_none());
                drop((settings, txn, db));
            } else {
                assert_eq!(
                    std::fs::read(dir.join(file)).ok(),
                    damaged,
                    "{file}: the damaged file was changed"
                );
            }
            if file == "shard-0000.redb" {
                // Nothing healthy was opened: every file is as it was.
                for (name, bytes) in &others {
                    let now = std::fs::read(dir.join(name)).unwrap();
                    assert!(now == *bytes, "{file}: {name} was changed");
                }
            }

            // Repaired, the database opens with everything it held.
            std::fs::write(dir.join(file), &original).unwrap();
            let st = restart(&root);
            let db = st.db("m").await.unwrap();
            assert_eq!(db.info().await.unwrap().doc_count, 8, "{file}");
            drop(db);
            drop(st);
            for index in 0..2 {
                crate::kv::forget_verified(&shard_file(&dir, index));
            }
            let _ = std::fs::remove_dir_all(root);
        }
    }

    /// A map stored for another cluster size is replaced by the configured
    /// placement when the database opens, and stored, so reading the
    /// closed database later need not open it again.
    #[tokio::test]
    async fn a_map_for_another_cluster_size_is_replaced_and_stored() {
        let root = scratch_root("resize");
        let settings = DbSettings {
            q: 2,
            partitioned: false,
            shard_map: crate::cluster::ShardMap::balanced(2, 3, 1),
        };
        create_db_staged(&root, "r", &settings).unwrap();
        assert!(Db::read_closed_meta(&root, "r", 1).unwrap().is_none());

        let st = test_state(root.clone(), 8);
        let db = st.db("r").await.unwrap();
        assert_eq!(db.shard_map(), crate::cluster::ShardMap::single(2));
        drop(db);
        drop(st);
        let closed = Db::read_closed_meta(&root, "r", 1).unwrap();
        let closed = closed.expect("the replaced map was not stored");
        assert_eq!(closed.shard_map, crate::cluster::ShardMap::single(2));
        let _ = std::fs::remove_dir_all(root);
    }

    /// A request holding a database across its delete and the creation of
    /// a new one under the same name used to close the old shard files
    /// last, through `kv::close_verified`, which recorded the new file at
    /// that path as verified: its next open would skip the full check.
    #[tokio::test]
    async fn a_late_close_cannot_mark_a_recreated_file_verified() {
        let root = scratch_root("late-close");
        let st = test_state(root.clone(), 8);
        let stale = st.create_db("late", false, None, None).await.unwrap();
        stale
            .put_docs(vec![json!({"_id": "x"})], true)
            .await
            .unwrap();
        st.drop_db("late").await.unwrap();
        let fresh = st.create_db("late", false, None, None).await.unwrap();
        let file = shard_file(&db_path(&root, "late"), 0);
        assert!(!crate::kv::is_verified(&file));

        // The stale handle answers as for a database that is gone.
        let error = stale
            .get_doc("x", &crate::db::DocOpts::default())
            .await
            .unwrap_err();
        assert_eq!(error.status, 404, "{}", error.reason);
        // Its last reference goes while the new file is open.
        drop(stale);
        assert!(
            !crate::kv::is_verified(&file),
            "a closed, deleted file marked the new one verified"
        );
        drop(fresh);
        st.dbs.write().await.clear();
        let _ = std::fs::remove_dir_all(root);
    }

    /// The delete forgets the verification of its files straight after the
    /// rename, so a crash or a failed sync after it cannot leave a recreated
    /// file's first open unchecked; publishing a database forgets its new
    /// files' names too, whatever an earlier file there left recorded.
    #[tokio::test]
    async fn delete_and_create_forget_every_shard_file() {
        let root = scratch_root("forget");
        let st = test_state(root.clone(), 8);
        drop(st.create_db("f", false, Some(2), None).await.unwrap());
        drop(st);
        // Closed idle, both files verified.
        let files: Vec<PathBuf> = (0..2)
            .map(|i| shard_file(&db_path(&root, "f"), i))
            .collect();
        assert!(files.iter().all(|file| crate::kv::is_verified(file)));

        stop_at(&root, Step::Tombstoned);
        let st = test_state(root.clone(), 8);
        assert!(st.drop_db("f").await.is_err(), "not stopped");
        clear_stops(&root);
        for file in &files {
            assert!(
                !crate::kv::is_verified(file),
                "{} still verified",
                file.display()
            );
        }
        drop(st);

        // Removed by hand while verified; the new files must be checked.
        let st = restart(&root);
        drop(st.create_db("f", false, Some(2), None).await.unwrap());
        drop(st);
        assert!(files.iter().all(|file| crate::kv::is_verified(file)));
        std::fs::remove_dir_all(db_path(&root, "f")).unwrap();
        create_db_staged(
            &root,
            "f",
            &DbSettings {
                q: 2,
                partitioned: false,
                shard_map: crate::cluster::ShardMap::single(2),
            },
        )
        .unwrap();
        for file in &files {
            assert!(
                !crate::kv::is_verified(file),
                "{} still verified",
                file.display()
            );
        }
        let _ = std::fs::remove_dir_all(root);
    }

    /// A directory is a database when it holds a shard file. One that only
    /// has a legal name, like `lost+found` at the root of a volume, is not
    /// listed and does not exist; creating its name answers 412, since
    /// publishing cannot replace a directory that is not empty.
    #[tokio::test]
    async fn a_directory_without_shard_files_is_not_a_database() {
        let root = scratch_root("not-a-db");
        std::fs::create_dir_all(root.join("lost+found")).unwrap();
        std::fs::write(root.join("lost+found").join("#1234"), b"x").unwrap();
        let st = test_state(root.clone(), 8);
        assert!(st.db_names().await.is_empty());
        assert_eq!(st.db("lost+found").await.err().map(|e| e.status), Some(404));
        assert_eq!(st.drop_db("lost+found").await.unwrap_err().status, 404);
        let error = st.create_db("lost+found", false, None, None).await.err();
        assert_eq!(error.map(|e| e.status), Some(412));
        assert_eq!(
            entries(&root),
            ["lost+found"],
            "a staging directory was left"
        );
        assert_eq!(entries(&root.join("lost+found")), ["#1234"]);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Recovery removes staging and tombstone directories and nothing else
    /// in the root: databases, the health probe's scratch files, temporary
    /// files and anything else that only resembles them stay.
    #[test]
    fn recovery_removes_only_staging_and_tombstone_directories() {
        let root = scratch_root("recover");
        std::fs::create_dir_all(&root).unwrap();
        for dir in [".creating-1", ".deleted-2", "keep", "a%2Fb"] {
            std::fs::create_dir(root.join(dir)).unwrap();
            std::fs::write(root.join(dir).join("shard-0000.redb"), b"x").unwrap();
        }
        for file in [".creating-3", ".health-4", ".secret.1.2.tmp"] {
            std::fs::write(root.join(file), b"x").unwrap();
        }
        recover_root(&root).unwrap();
        assert_eq!(
            entries(&root),
            [
                ".creating-3",
                ".health-4",
                ".secret.1.2.tmp",
                "a%2Fb",
                "keep"
            ]
        );
        assert_eq!(entries(&root.join("keep")), ["shard-0000.redb"]);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn db_updates_keeps_the_newest_events_in_order() {
        let updates = DbUpdates::default();
        for i in 0..(DB_UPDATES_KEPT + 10) {
            updates.record(&format!("db{i}"), "updated");
        }
        let total = (DB_UPDATES_KEPT + 10) as u64;
        let (all, current) = updates.since(0);
        assert_eq!(current, total);
        assert_eq!(all.len(), DB_UPDATES_KEPT);
        assert_eq!(all[0]["seq"], "11");
        assert_eq!(all[0]["db_name"], "db10");
        let (tail, _) = updates.since(total - 2);
        let seqs: Vec<&str> = tail.iter().filter_map(|e| e["seq"].as_str()).collect();
        assert_eq!(seqs, [(total - 1).to_string(), total.to_string()]);
        assert!(updates.since(total).0.is_empty());
    }
}

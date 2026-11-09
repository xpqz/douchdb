//! Shared node state: the open databases, cluster handle, configuration and
//! the counters the HTTP layer and the background tasks both use.

use crate::auth::Principal;
use crate::cluster_http::ClusterCtx;
use crate::db::{ApiError, Db, DbConfig, DbResult};
use crate::dbname::{illegal_db_name, valid_db_name};
use crate::health::FsHealthCache;
use axum::http::HeaderMap;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
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

#[derive(Default)]
pub struct DbUpdates {
    events: Mutex<DbEvents>,
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

/// Default for `--max-open-dbs`. CouchDB's `max_dbs_open` is 500, but a
/// database here holds a thread and several file descriptors per shard.
pub const DEFAULT_MAX_OPEN_DBS: usize = 256;

/// An open database and when it was last asked for.
pub struct OpenDb {
    db: Arc<Db>,
    used: AtomicU64,
}

/// Source of the "last used" stamps. Only their order matters.
static DB_CLOCK: AtomicU64 = AtomicU64::new(0);

impl OpenDb {
    fn new(db: Arc<Db>) -> Self {
        OpenDb {
            db,
            used: AtomicU64::new(DB_CLOCK.fetch_add(1, Ordering::Relaxed)),
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
    fn of_open(db: Arc<Db>) -> DbResult<Self> {
        Ok(Placement {
            q: db.q,
            partitioned: db.partitioned,
            shard_map: db.shard_map(),
            pending_shard_map: db.pending_shard_map()?.is_some(),
            open: Some(db),
        })
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
    /// the LevelDB lock) and a drop cannot interleave with a create or open.
    pub db_locks: NameLocks,
    /// How many times a database has been opened from disk, creation
    /// included. A closed database that is asked for again counts once more.
    pub db_opens: AtomicU64,
    pub config: Config,
    pub ctx: ClusterCtx,
    pub db_updates: DbUpdates,
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
        let _guard = self.db_locks.lock(name).await;
        if let Some(open) = self.dbs.read().await.get(name) {
            return Ok(if hot { open.touch() } else { open.db.clone() });
        }
        let db = Arc::new(Db::open_existing(
            &self.root,
            name,
            DbConfig {
                q: self.config.q,
                revs_limit: self.config.revs_limit,
                shard_map: self.ctx.cluster.shard_map(name, self.config.q),
                partitioned: false,
            },
        )?);
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
            return Placement::of_open(db);
        }
        let root = self.root.clone();
        let owned = name.to_string();
        let nodes = self.ctx.cluster.nodes.len().max(1);
        let closed =
            tokio::task::spawn_blocking(move || Db::read_closed_meta(&root, &owned, nodes))
                .await
                .map_err(|e| ApiError::new(500, "internal_server_error", e.to_string()))??;
        match closed {
            Some(meta) => Ok(Placement {
                q: meta.q,
                partitioned: meta.partitioned,
                shard_map: meta.shard_map,
                pending_shard_map: meta.pending_shard_map,
                open: None,
            }),
            None => Placement::of_open(self.db(name).await?),
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
    /// database back waits for the drop, so it never meets a LevelDB lock
    /// that is still held.
    async fn close_idle(&self) {
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

    /// The `_security` object of a database, read from memory when it is
    /// open and from its directory when it is not, so that asking who may see
    /// a database does not open it. `None` when there is no such database or
    /// its security cannot be read.
    pub async fn db_security(&self, name: &str) -> Option<Value> {
        if !valid_db_name(name) {
            return None;
        }
        if let Some(open) = self.dbs.read().await.get(name) {
            return Some(open.db.security());
        }
        let root = self.root.clone();
        let name = name.to_string();
        tokio::task::spawn_blocking(move || Db::read_security(&root, &name).ok())
            .await
            .ok()
            .flatten()
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
        let _guard = self.db_locks.lock(name).await;
        let path = crate::db::db_path(&self.root, name);
        crate::view::forget_db(&path).await;
        crate::search::forget_db(&path).await;
        if Db::exists(&self.root, name) {
            return Err(ApiError::new(
                412,
                "file_exists",
                "The database could not be created, the file already exists.",
            ));
        }
        let db = Arc::new(Db::open(
            &self.root,
            name,
            DbConfig {
                q: q.unwrap_or(self.config.q),
                revs_limit: self.config.revs_limit,
                shard_map: self.ctx.cluster.shard_map_n(
                    name,
                    q.unwrap_or(self.config.q),
                    n.unwrap_or(self.ctx.cluster.n),
                ),
                partitioned,
            },
        )?);
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
        // Marked before the gates are forgotten: a request holding the handle
        // can no longer take a fresh gate and reach the index directories.
        if let Some(open) = self.dbs.write().await.remove(name) {
            open.db.mark_deleted();
        }
        let path = crate::db::db_path(&self.root, name);
        crate::view::forget_db(&path).await;
        crate::search::forget_db(&path).await;
        Db::destroy(&self.root, name)?;
        self.db_updates.record(name, "deleted");
        Ok(())
    }

    /// Names of the databases that are open now.
    pub(crate) async fn open_db_names(&self) -> std::collections::HashSet<String> {
        self.dbs.read().await.keys().cloned().collect()
    }

    pub async fn db_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.root) {
            for entry in entries.flatten() {
                if entry.path().join("db.json").is_file() {
                    if let Some(name) = entry
                        .file_name()
                        .to_str()
                        .and_then(crate::db::dir_name_to_db_name)
                    {
                        names.push(name);
                    }
                }
            }
        }
        names.sort();
        names
    }

    /// The caller's principal. With no auth configured every request is a
    /// service admin, preserving the single-user default. Internal cluster
    /// requests without user credentials are service admins; peers are trusted
    /// by the shared cluster secret.
    pub fn principal(&self, headers: &HeaderMap) -> Principal {
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

/// A node state over `root` for tests: one node, no peers, no auth.
#[cfg(test)]
pub(crate) fn test_state(root: PathBuf, max_open_dbs: usize) -> Arc<AppState> {
    Arc::new(AppState {
        root,
        dbs: Default::default(),
        db_locks: Default::default(),
        db_opens: Default::default(),
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
        cluster_state: Arc::new(crate::cluster_state::ClusterState::new()),
        admission: Arc::new(crate::admission::Admission::new(Default::default())),
        compaction: Arc::new(tokio::sync::Semaphore::new(1)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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

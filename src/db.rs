//! Database and document semantics: revisions, conflicts, changes feeds.
//!
//! All writes for one request are staged as an ordered list of (key, value)
//! operations and applied as a single LevelDB `WriteBatch`, so a `_bulk_docs`
//! of a thousand documents costs one OS flush, not a thousand. rusty-leveldb
//! does not fsync, so this is process-crash durability, not power-loss
//! durability.

use crate::cluster::ShardMap;
pub use crate::error::{ApiError, DbResult};
use crate::rev::{new_revid, Attachment, Rev};
use crate::store::{
    blob_prefix, body_key, local_key, meta_key, seq_key, AttMeta, DocMeta, ShardHandle, Tx,
    ATTEST_CHUNK,
};
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

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn uuid() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut buf = [0u8; 32];
    let mut seed = format!("{now}{:p}", &now as *const _);
    for (i, slot) in buf.iter_mut().enumerate() {
        *slot = seed.as_bytes()[i % seed.len()];
        seed.push(((i as u8) ^ 0x5a) as char);
    }
    hex(&Md5::digest(buf))
}

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

/// Pure form of the changes watermark rule: only shards with an allocated but
/// uncommitted sequence constrain the cursor, and the cursor never goes below
/// `since`.
pub(crate) fn changes_watermark_from(
    committed: &[u64],
    reserved: &[u64],
    since: u64,
    owned: Option<&[bool]>,
) -> u64 {
    let mut safe = u64::MAX;
    for index in 0..committed.len().min(reserved.len()) {
        if owned
            .map(|mask| !mask.get(index).copied().unwrap_or(false))
            .unwrap_or(false)
        {
            continue;
        }
        if reserved[index] > committed[index] {
            safe = safe.min(committed[index].max(since));
        }
    }
    safe
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

pub struct Db {
    pub name: String,
    pub path: PathBuf,
    pub q: u32,
    pub partitioned: bool,
    pub uuid: String,
    pub instance_start_time: String,
    shard_map: Arc<std::sync::RwLock<ShardMap>>,
    security: Arc<std::sync::RwLock<Value>>,
    /// Visible as `compact_running` in `GET /db`; the shard actor still owns
    /// the actual compaction and blocks its shard while it runs.
    compact_running: Arc<AtomicBool>,
    /// Target placement while a rebalance is being verified. Reads and writes
    /// continue to use the active `shard_map`; anti-entropy uses this map to
    /// seed the target replicas.
    migration_map: Arc<std::sync::RwLock<Option<ShardMap>>>,
    /// Unix-millisecond deadline for the cluster-wide write freeze, or zero
    /// when writes are allowed. A deadline lets peers recover if the
    /// coordinating node disappears mid-rebalance.
    rebalance_until: Arc<AtomicU64>,
    shards: Vec<ShardHandle>,
    /// Last committed sequence per shard, shared with the shard actors.
    committed_seq: Vec<Arc<AtomicU64>>,
    /// Highest sequence allocated but not necessarily committed per shard.
    reserved_seq: Vec<Arc<AtomicU64>>,
    /// One global sequence counter for the database. CouchDB's `since` is a
    /// database-wide cursor, so shard-local counters cannot be used directly.
    seq: Arc<AtomicU64>,
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
const REBALANCE_FREEZE_MS: u64 = 420_000;

fn now_millis() -> u64 {
    static BASE: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    BASE.get_or_init(std::time::Instant::now)
        .elapsed()
        .as_millis() as u64
}

impl Db {
    pub fn open(root: &Path, name: &str, config: DbConfig) -> DbResult<Db> {
        let path = root.join(name);
        let db_file = path.join("db.json");
        let file: DbFile = if let Ok(bytes) = std::fs::read(&db_file) {
            serde_json::from_slice(&bytes).map_err(|_| ApiError::not_found("bad db.json"))?
        } else {
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
            std::fs::write(&db_file, serde_json::to_vec(&file).unwrap()).ok();
            file
        };

        let map_path = path.join("shard-map.json");
        let shard_map = std::fs::read(&map_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<ShardMap>(&bytes).ok())
            .filter(|map| map.is_valid(file.q, config.shard_map.node_count))
            .unwrap_or_else(|| {
                let map = if config.shard_map.q == file.q {
                    config.shard_map.clone()
                } else {
                    ShardMap::single(file.q)
                };
                let _ = std::fs::write(&map_path, serde_json::to_vec(&map).unwrap());
                map
            });

        let mut shards = Vec::new();
        let mut committed_seq = Vec::with_capacity(file.q as usize);
        let mut reserved_seq = Vec::with_capacity(file.q as usize);
        for i in 0..file.q {
            let shard = ShardHandle::spawn(path.join(format!("shard-{i:04}")), config.revs_limit)?;
            committed_seq.push(shard.seq_tracker());
            reserved_seq.push(Arc::new(AtomicU64::new(shard.initial_seq())));
            shards.push(shard);
        }

        let seq = shards
            .iter()
            .map(ShardHandle::initial_seq)
            .max()
            .unwrap_or(0);
        let security = std::fs::read(path.join("security.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .unwrap_or_else(|| json!({}));

        Ok(Db {
            name: name.to_string(),
            path,
            q: file.q.max(1),
            partitioned: file.partitioned,
            uuid: file.uuid,
            instance_start_time: file.instance_start_time,
            shard_map: Arc::new(std::sync::RwLock::new(shard_map)),
            security: Arc::new(std::sync::RwLock::new(security)),
            compact_running: Arc::new(AtomicBool::new(false)),
            migration_map: Arc::new(std::sync::RwLock::new(None)),
            rebalance_until: Arc::new(AtomicU64::new(0)),
            shards,
            committed_seq,
            reserved_seq,
            seq: Arc::new(AtomicU64::new(seq)),
        })
    }

    pub fn exists(root: &Path, name: &str) -> bool {
        root.join(name).join("db.json").is_file()
    }

    pub fn destroy(root: &Path, name: &str) -> DbResult<()> {
        std::fs::remove_dir_all(root.join(name))
            .map_err(|_| ApiError::not_found(format!("Database does not exist: {name}")))
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
        self.security
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    pub fn set_security(&self, value: Value) -> DbResult<()> {
        let bytes = serde_json::to_vec(&value)
            .map_err(|error| ApiError::new(500, "internal_server_error", error.to_string()))?;
        std::fs::write(self.path.join("security.json"), bytes).map_err(|error| {
            ApiError::new(
                500,
                "internal_server_error",
                format!("security.json: {error}"),
            )
        })?;
        *self
            .security
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = value;
        Ok(())
    }

    pub fn shard_map(&self) -> ShardMap {
        self.shard_map
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    pub fn set_shard_map(&self, shard_map: ShardMap) -> DbResult<bool> {
        if !shard_map.is_valid(self.q, shard_map.node_count) {
            return Err(ApiError::bad_request("invalid shard map"));
        }
        let current = self.shard_map();
        if shard_map.version < current.version {
            return Ok(false);
        }
        if shard_map.version == current.version {
            if shard_map.replicas != current.replicas {
                return Ok(false);
            }
            return Ok(true);
        }
        std::fs::write(
            self.path.join("shard-map.json"),
            serde_json::to_vec(&shard_map)
                .map_err(|error| ApiError::new(500, "internal_server_error", error.to_string()))?,
        )
        .map_err(|error| ApiError::new(500, "internal_server_error", error.to_string()))?;
        *self
            .shard_map
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = shard_map;
        Ok(true)
    }

    /// Placement targeted by an in-progress rebalance. Falls back to the
    /// active map outside a migration.
    pub fn replication_map(&self) -> ShardMap {
        self.migration_map
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
            .unwrap_or_else(|| self.shard_map())
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
        let migration = self
            .migration_map
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(map) = migration.as_ref() {
            return map.replicas_of(shard).contains(&node);
        }
        drop(migration);
        self.shard_map
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .replicas_of(shard)
            .contains(&node)
    }

    pub fn set_migration_map(&self, map: ShardMap) -> DbResult<()> {
        if !map.is_valid(self.q, map.node_count) {
            return Err(ApiError::bad_request("invalid migration shard map"));
        }
        *self
            .migration_map
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(map);
        Ok(())
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
        let deadline = now_millis().saturating_add(REBALANCE_FREEZE_MS);
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
                now_millis().saturating_add(REBALANCE_FREEZE_MS)
            } else {
                0
            },
            Ordering::SeqCst,
        );
    }

    /// Highest sequence that is safe for a changes cursor to publish.
    ///
    /// Sequence numbers are allocated while a shard job is staging, but only
    /// become visible when that job commits. Different shard actors can run
    /// concurrently, so another shard may publish a higher sequence first. A
    /// numeric `last_seq` must not advance past a shard's committed sequence
    /// while that shard still has an allocated-but-uncommitted sequence;
    /// otherwise the next request skips the later-visible row.
    pub fn changes_watermark(&self, since: u64, owned: Option<&[bool]>) -> u64 {
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
        changes_watermark_from(&committed, &reserved, since, owned)
    }

    pub fn pending_shard_map(&self) -> Option<ShardMap> {
        std::fs::read(self.path.join("pending-shard-map.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
    }

    pub fn set_pending_shard_map(&self, map: &ShardMap) -> DbResult<()> {
        std::fs::write(
            self.path.join("pending-shard-map.json"),
            serde_json::to_vec(map)
                .map_err(|error| ApiError::new(500, "internal_server_error", error.to_string()))?,
        )
        .map_err(|error| ApiError::new(500, "internal_server_error", error.to_string()))?;
        Ok(())
    }

    pub fn clear_pending_shard_map(&self) {
        let _ = std::fs::remove_file(self.path.join("pending-shard-map.json"));
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

    /// Current committed sequence counter. This is the same database-wide
    /// counter used for `update_seq`, without asking every shard actor.
    pub fn update_seq(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
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
        let mut doc_count = 0;
        let mut deleted_count = 0;
        let mut update_seq = 0;
        for shard in &self.shards {
            let (docs, deleted, seq) = shard
                .call(|s| {
                    let (docs, deleted) = s.counts();
                    Ok((docs, deleted, s.seq()))
                })
                .await?;
            doc_count += docs;
            deleted_count += deleted;
            update_seq = update_seq.max(seq);
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
        });
        if self.partitioned {
            info["props"] = json!({"partitioned": true});
        }
        Ok(info)
    }

    // ----- documents -------------------------------------------------------

    pub async fn put_docs(&self, docs: Vec<Value>, new_edits: bool) -> DbResult<Vec<Value>> {
        if docs.iter().any(|doc| doc.get("_attachments").is_some()) {
            return Err(ApiError::new(
                400,
                "attachments_not_supported",
                "douchdb does not support attachments",
            ));
        }
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

        for (shard_idx, batch) in grouped.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let seq = self.seq.clone();
            let reserved = self.reserved_seq[shard_idx].clone();
            let results = self.shards[shard_idx]
                .call(move |s| Ok(put_many(s, batch, new_edits, &seq, &reserved)))
                .await?;
            for (i, result) in results {
                out[i] = Some(match result {
                    Ok(doc) => doc,
                    Err(e) => json!({
                        "id": ids[i], "error": e.error, "reason": e.reason
                    }),
                });
            }
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

    pub async fn all_docs(&self, opts: &AllDocsOpts) -> DbResult<Value> {
        self.all_docs_filtered(opts, None).await
    }

    /// Like `all_docs`, but only shards whose `owned` entry is true. Cluster
    /// coordinators use this to fetch each document from exactly one primary
    /// instead of merging the same replica from every node.
    pub async fn all_docs_owned(&self, opts: &AllDocsOpts, owned: &[bool]) -> DbResult<Value> {
        self.all_docs_filtered(opts, Some(owned)).await
    }

    async fn all_docs_filtered(
        &self,
        opts: &AllDocsOpts,
        owned: Option<&[bool]>,
    ) -> DbResult<Value> {
        if let Some(keys) = &opts.keys {
            return self.all_docs_by_keys(opts, keys, owned).await;
        }

        // A shard only needs to return enough rows to satisfy the global
        // `skip + limit`: any row in the global window is also in its shard's
        // first `skip + limit` rows.
        let take = opts.skip.saturating_add(opts.limit);
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

        let mut rows = Vec::new();
        let mut total_rows = 0u64;
        let mut before_start = 0usize;
        for (index, shard) in self.shards.iter().enumerate() {
            if partition_shard
                .map(|partition| partition != index)
                .unwrap_or(false)
            {
                continue;
            }
            if owned
                .map(|owned| !owned.get(index).copied().unwrap_or(false))
                .unwrap_or(false)
            {
                continue;
            }
            let opts = opts.clone();
            let (mut shard_rows, shard_offset, shard_docs) = shard
                .call(move |s| {
                    let docs = match &opts.partition {
                        Some(partition) => s.partition_live_count(partition)?,
                        None => s.counts().0,
                    };
                    let (rows, offset) = collect_all_docs(s, &opts, take)?;
                    Ok((rows, offset, docs))
                })
                .await?;
            rows.append(&mut shard_rows);
            before_start += shard_offset;
            total_rows += shard_docs;
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        if opts.descending {
            rows.reverse();
        }
        let selected: Vec<_> = rows.into_iter().skip(opts.skip).take(opts.limit).collect();
        let offset = (before_start + opts.skip).min(total_rows as usize);
        finish_all_docs(selected, total_rows as usize, offset)
    }

    /// Documents for a view build, as JSON text with `_id` and `_rev` already
    /// spliced into the stored body. This avoids parsing every body into a
    /// `Value` and serializing it again just to hand it to QuickJS.
    /// View documents from selected shards. Cluster nodes pass their owned
    /// shards so replicas do not pay to index documents they never serve.
    pub async fn map_docs(&self, owned: Option<&[bool]>) -> DbResult<Vec<(String, String)>> {
        let mut docs = Vec::new();
        for (index, shard) in self.shards.iter().enumerate() {
            if owned
                .map(|owned| !owned.get(index).copied().unwrap_or(false))
                .unwrap_or(false)
            {
                continue;
            }
            let mut shard_docs = shard
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
                                docs.push((id.clone(), embed_doc_meta(&id, &rev, &body)));
                                continue;
                            }
                        }
                        // Attachments are rare; fall back to the full document
                        // shape so map functions still see `_attachments`.
                        let doc_opts = DocOpts {
                            rev: Some(rev),
                            ..Default::default()
                        };
                        if let Ok(doc) = read_doc(s, &id, &doc_opts) {
                            if let Ok(text) = serde_json::to_string(&doc) {
                                docs.push((id, text));
                            }
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
        keys: &[String],
        owned: Option<&[bool]>,
    ) -> DbResult<Value> {
        let mut total_rows = 0u64;
        if let Some(partition) = &opts.partition {
            let shard = self.partition_shard(partition);
            if owned
                .map(|owned| !owned.get(shard).copied().unwrap_or(false))
                .unwrap_or(false)
            {
                total_rows = 0;
            } else {
                let partition = partition.clone();
                total_rows = self.shards[shard]
                    .call(move |s| s.partition_live_count(&partition).map_err(ApiError::from))
                    .await?;
            }
        } else {
            for (index, shard) in self.shards.iter().enumerate() {
                if owned
                    .map(|owned| !owned.get(index).copied().unwrap_or(false))
                    .unwrap_or(false)
                {
                    continue;
                }
                total_rows += shard.call(|s| Ok(s.counts().0)).await?;
            }
        }

        let ordered_keys: Vec<String> = if opts.descending {
            keys.iter().rev().cloned().collect()
        } else {
            keys.to_vec()
        };
        let page: Vec<String> = ordered_keys
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
            if let Some(partition) = &opts.partition {
                if !key.starts_with(&format!("{partition}:")) {
                    continue;
                }
            }
            let shard = self.shard_index(key);
            if owned
                .map(|owned| !owned.get(shard).copied().unwrap_or(false))
                .unwrap_or(false)
            {
                continue;
            }
            included[slot] = true;
            grouped[shard].push((slot, key.clone()));
        }

        let mut results: Vec<Option<Value>> = vec![None; page.len()];
        for (index, batch) in grouped.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let opts = opts.clone();
            let rows = self.shards[index]
                .call(move |s| {
                    let rows = batch
                        .into_iter()
                        .map(|(slot, key)| all_docs_key_row(s, &key, &opts).map(|row| (slot, row)))
                        .collect::<DbResult<Vec<_>>>()?;
                    Ok(rows)
                })
                .await?;
            for (slot, row) in rows {
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
        let mut rows = Vec::new();
        for (index, shard) in self.shards.iter().enumerate() {
            if opts
                .owned
                .as_ref()
                .map(|owned| !owned.get(index).copied().unwrap_or(false))
                .unwrap_or(false)
            {
                continue;
            }
            let shard_opts = opts.clone();
            let mut shard_rows = shard.call(move |s| collect_changes(s, &shard_opts)).await?;
            rows.append(&mut shard_rows);
        }
        rows.sort_by_key(|r| r.seq);
        // Recompute after collecting rows so an allocation that started while
        // the shards were being read also constrains this cursor. Rows above
        // the watermark are withheld; the next request re-reads them rather
        // than skipping a later-visible lower sequence.
        let watermark = self.changes_watermark(opts.since, opts.owned.as_deref());
        if !opts.descending && watermark != u64::MAX {
            rows.retain(|row| row.seq <= watermark);
        }
        if opts.descending {
            rows.reverse();
        }
        if let Some(limit) = opts.limit {
            rows.truncate(limit);
        }
        let last_seq = rows.last().map(|r| r.seq).unwrap_or(opts.since);
        let pending = self
            .changes_pending(last_seq, opts.descending, opts.owned.as_deref())
            .await?;
        Ok((rows, last_seq, pending))
    }

    pub(crate) async fn changes_pending(
        &self,
        after: u64,
        descending: bool,
        owned: Option<&[bool]>,
    ) -> DbResult<usize> {
        let mut pending = 0usize;
        for (index, shard) in self.shards.iter().enumerate() {
            if owned
                .map(|owned| !owned.get(index).copied().unwrap_or(false))
                .unwrap_or(false)
            {
                continue;
            }
            pending += shard
                .call(move |s| s.pending_changes(after, descending).map_err(ApiError::from))
                .await?;
        }
        Ok(pending)
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
                if let Ok(doc) = serde_json::from_str::<Value>(&doc) {
                    entry["doc"] = doc;
                }
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
    ) -> impl futures::Stream<Item = DbResult<ChangeRow>> + 'static {
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
            let selected: Vec<usize> = (0..db.shards.len())
                .filter(|index| {
                    opts.owned
                        .as_ref()
                        .map(|owned| owned.get(*index).copied().unwrap_or(false))
                        .unwrap_or(true)
                })
                .collect();
            let mut cursors = vec![opts.since; selected.len()];
            let mut done = vec![false; selected.len()];
            let mut buffers: Vec<VecDeque<ChangeRow>> =
                (0..selected.len()).map(|_| VecDeque::new()).collect();
            let mut emitted = 0usize;

            loop {
                let mut best: Option<usize> = None;
                for slot in 0..selected.len() {
                    if buffers[slot].is_empty() && !done[slot] {
                        let index = selected[slot];
                        let mut shard_opts = opts.clone();
                        shard_opts.since = cursors[slot];
                        shard_opts.descending = false;
                        shard_opts.limit = Some(PAGE);
                        match db.shards[index]
                            .call(move |shard| collect_changes(shard, &shard_opts))
                            .await
                        {
                            Ok(mut rows) => {
                                if rows.is_empty() {
                                    done[slot] = true;
                                } else {
                                    cursors[slot] =
                                        rows.last().map(|row| row.seq).unwrap_or(cursors[slot]);
                                    let watermark = db
                                        .changes_watermark(opts.since, opts.owned.as_deref());
                                    if watermark != u64::MAX {
                                        rows.retain(|row| row.seq <= watermark);
                                        done[slot] = true;
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
                    break;
                };
                let row = buffers[slot].pop_front().expect("buffered row");
                emitted += 1;
                yield Ok(row);
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
        let mut out = Map::new();
        for (id, revs) in map {
            let Some(list) = revs.as_array() else {
                continue;
            };
            let wanted: Vec<String> = list
                .iter()
                .filter_map(|r| r.as_str().map(str::to_string))
                .collect();
            let id2 = id.clone();
            let (missing, ancestors) = self
                .shard(id)
                .call(move |s| {
                    let tree = s.get_meta(&id2)?.map(|m| m.tree).unwrap_or_default();
                    let mut missing = Vec::new();
                    let mut ancestors: Vec<String> = Vec::new();
                    for rev in &wanted {
                        if !tree.edges.contains_key(rev) {
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
                })
                .await?;
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

    pub async fn bulk_get(&self, body: &Value, attachments: bool, revs: bool) -> DbResult<Value> {
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
                include_deleted: true,
                ..Default::default()
            };
            grouped[self.shard_index(&id)].push((slot, id, opts));
        }

        let mut out: Vec<Option<Value>> = vec![None; reqs.len()];
        for (index, batch) in grouped.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let results = self.shards[index]
                .call(move |shard| {
                    let mut out = Vec::with_capacity(batch.len());
                    for (slot, id, opts) in batch {
                        let rev = opts.rev.clone().unwrap_or_else(|| "undefined".into());
                        let result = read_doc(shard, &id, &opts);
                        out.push((slot, id, rev, result));
                    }
                    Ok(out)
                })
                .await?;
            for (slot, id, rev, result) in results {
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
            .call(move |s| {
                Ok(s.get(&local_key(&id))
                    .and_then(|b| serde_json::from_slice::<Value>(&b).ok()))
            })
            .await
    }

    pub async fn local_put(&self, id: &str, mut body: Value) -> DbResult<Value> {
        let id = id.to_string();
        self.shards[0]
            .call(move |s| {
                let key = local_key(&id);
                let existing = s
                    .get(&key)
                    .and_then(|b| serde_json::from_slice::<Value>(&b).ok());
                let next = existing
                    .as_ref()
                    .and_then(|v| v.get("_rev").and_then(Value::as_str))
                    .and_then(|r| r.strip_prefix("0-"))
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap_or(0)
                    + 1;
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
                let mut tx = Tx::new();
                tx.delete(&local_key(&id));
                s.commit(tx)?;
                Ok(json!({"ok": true, "id": format!("_local/{id}"), "rev": "0-0"}))
            })
            .await
    }

    // ----- maintenance -----------------------------------------------------

    pub async fn purge(&self, body: &Value) -> DbResult<Value> {
        let Some(map) = body.as_object() else {
            return Err(ApiError::bad_request("Request body must be a JSON object"));
        };
        let mut purged = Map::new();
        for (id, revs) in map {
            let Some(list) = revs.as_array() else {
                continue;
            };
            let requested: Vec<String> = list
                .iter()
                .filter_map(|r| r.as_str().map(str::to_string))
                .collect();
            let id2 = id.clone();
            let seq_counter = self.seq.clone();
            let reserved = self.reserved_seq[self.shard_index(id)].clone();
            let done = self
                .shard(id)
                .call(move |s| {
                    let Some(mut meta) = s.get_meta(&id2)? else {
                        return Ok(Vec::new());
                    };
                    let mut tx = Tx::new();
                    let mut removed = Vec::new();
                    for rev in &requested {
                        if !meta.tree.edges.contains_key(rev) {
                            continue;
                        }
                        tx.delete(&body_key(&id2, rev));
                        meta.tree.edges.remove(rev);
                        meta.tree.deleted.remove(rev);
                        meta.atts.remove(rev);
                        removed.push(rev.clone());
                    }
                    if !removed.is_empty() {
                        // Children of a purged revision become roots.
                        let live: std::collections::BTreeSet<String> =
                            meta.tree.edges.keys().cloned().collect();
                        for parent in meta.tree.edges.values_mut() {
                            if let Some(p) = parent {
                                if !live.contains(p) {
                                    *parent = None;
                                }
                            }
                        }
                        if meta.tree.edges.is_empty() {
                            tx.delete(&meta_key(&id2));
                            if meta.seq != 0 {
                                tx.delete(&seq_key(meta.seq));
                            }
                            s.commit(tx)?;
                        } else {
                            let old_seq = meta.seq;
                            // Purging the winner changes what a normal read sees,
                            // so it is a change as far as `_changes` is concerned.
                            let seq = seq_counter.fetch_add(1, Ordering::SeqCst) + 1;
                            reserved.fetch_max(seq, Ordering::SeqCst);
                            meta.seq = seq;
                            tx.delete(&seq_key(old_seq));
                            tx.put(&seq_key(seq), id2.as_bytes());
                            tx.put_json(&meta_key(&id2), &meta)?;
                            s.commit_at(tx, Some(seq))?;
                        }
                    } else {
                        s.commit(tx)?;
                    }
                    if !removed.is_empty() {
                        s.invalidate_latest();
                    }
                    Ok(removed)
                })
                .await?;
            purged.insert(id.clone(), json!(done));
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

/// CouchDB rejects unknown `_`-prefixed members outright (`doc_validation`),
/// whether they arrive through PUT, `_bulk_docs` or `new_edits=false`.
pub fn validate_doc(doc: &Value) -> DbResult<()> {
    let Some(obj) = doc.as_object() else {
        return Err(ApiError::bad_request("Document must be a JSON object"));
    };
    if obj.contains_key("_attachments") {
        return Err(ApiError::new(
            400,
            "attachments_not_supported",
            "douchdb does not support attachments",
        ));
    }
    for key in obj.keys() {
        if key.starts_with('_') && !META_FIELDS.contains(&key.as_str()) {
            return Err(ApiError::new(
                400,
                "doc_validation",
                format!("Bad special document member: {key}"),
            ));
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

fn split_doc(
    doc: Value,
) -> DbResult<(
    String,
    Option<String>,
    bool,
    Option<Value>,
    Option<Value>,
    Map<String, Value>,
)> {
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
    let mut attachments = None;
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
            "_attachments" => attachments = Some(value),
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
    Ok((id, rev, deleted, attachments, revisions, body))
}

fn decode_attachment(value: &Value) -> DbResult<Option<(AttMeta, Vec<u8>)>> {
    let Some(obj) = value.as_object() else {
        return Err(ApiError::bad_request("Attachment must be an object"));
    };
    let content_type = obj
        .get("content_type")
        .and_then(Value::as_str)
        .unwrap_or("application/octet-stream")
        .to_string();
    let Some(data) = obj.get("data").and_then(Value::as_str) else {
        if obj.get("follows").and_then(Value::as_bool).unwrap_or(false) {
            return Err(ApiError::bad_request(
                "multipart attachments are not supported",
            ));
        }
        return Ok(None);
    };
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|_| ApiError::bad_request("Invalid attachment data"))?;
    let digest = hex(&Md5::digest(&bytes));
    let meta = AttMeta {
        content_type,
        digest,
        length: bytes.len() as u64,
        revpos: 0,
    };
    Ok(Some((meta, bytes)))
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
    let mut results = Vec::new();

    for (i, doc) in docs {
        let mark = ops.len();
        let mut ctx = Stage {
            shard,
            ops: &mut ops,
            staged: &mut staged,
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
    for (id, meta) in staged.iter_mut() {
        meta.tree.prune(limit);
        if let Ok(bytes) = serde_json::to_vec(meta) {
            ops.push((meta_key(id), Some(bytes)));
        }
    }

    if let Err(e) = commit_ops(shard, ops) {
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
    seq: &'a Arc<AtomicU64>,
    reserved: &'a Arc<AtomicU64>,
}

impl Stage<'_> {
    fn next_seq(&self) -> u64 {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        self.reserved.fetch_max(seq, Ordering::SeqCst);
        seq
    }

    fn meta(&mut self, id: &str) -> DbResult<Option<DocMeta>> {
        if let Some(m) = self.staged.get(id) {
            return Ok(Some(m.clone()));
        }
        Ok(self.shard.get_meta(id)?)
    }

    fn flush_meta(&mut self, id: &str, meta: &DocMeta) -> DbResult<()> {
        let bytes = serde_json::to_vec(meta)?;
        self.ops.push((meta_key(id), Some(bytes)));
        self.staged.insert(id.to_string(), meta.clone());
        Ok(())
    }

    fn stage(&mut self, doc: Value, new_edits: bool) -> DbResult<Value> {
        let (id, parent_rev, deleted, attachments, revisions, body) = split_doc(doc)?;
        let mut meta = self.meta(&id)?.unwrap_or_default();
        let body = Value::Object(body);

        if !new_edits {
            return self.stage_replicated(
                id,
                meta,
                parent_rev,
                revisions,
                deleted,
                attachments,
                body,
            );
        }

        let parent = match parent_rev {
            Some(rev) => {
                if Rev::parse(&rev).is_none() {
                    return Err(ApiError::bad_request("Invalid rev format"));
                }
                if !meta.tree.edges.contains_key(&rev) || !meta.tree.is_leaf(&rev) {
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

        let parent_path = parent
            .as_ref()
            .map(|p| meta.tree.path(p))
            .unwrap_or_default();
        let pos = parent
            .as_ref()
            .and_then(|p| Rev::parse(p).map(|r| r.pos))
            .unwrap_or(0)
            + 1;

        let inherited = parent
            .as_ref()
            .and_then(|p| meta.atts.get(p).cloned())
            .unwrap_or_default();
        let fallback = all_atts(&meta);
        let atts = self.apply_attachments(&inherited, &fallback, attachments.as_ref(), pos)?;

        let hashed: Vec<Attachment> = atts
            .iter()
            .map(|(name, a)| Attachment {
                name: name.clone(),
                content_type: a.content_type.clone(),
                md5: hex_to_16(&a.digest),
            })
            .collect();
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
        meta.atts.retain(|r, _| meta.tree.edges.contains_key(r));
        meta.atts.insert(rev.clone(), atts);

        self.ops
            .push((body_key(&id, &rev), Some(serde_json::to_vec(&body)?)));
        self.ops.push((seq_key(seq), Some(id.as_bytes().to_vec())));
        if old_seq != 0 {
            self.ops.push((seq_key(old_seq), None));
        }
        self.flush_meta(&id, &meta)?;

        Ok(json!({"ok": true, "id": id, "rev": rev}))
    }

    fn stage_replicated(
        &mut self,
        id: String,
        mut meta: DocMeta,
        rev: Option<String>,
        revisions: Option<Value>,
        deleted: bool,
        attachments: Option<Value>,
        body: Value,
    ) -> DbResult<Value> {
        let rev = rev.ok_or_else(|| ApiError::bad_request("Document is missing _rev"))?;
        let leaf = Rev::parse(&rev).ok_or_else(|| ApiError::bad_request("Invalid rev format"))?;

        let known = meta.tree.edges.contains_key(&rev);
        if known && meta.atts.contains_key(&rev) && attachments.is_none() {
            return Ok(json!({"ok": true, "id": id, "rev": rev}));
        }

        let parent = meta
            .tree
            .edges
            .get(&rev)
            .cloned()
            .flatten()
            .and_then(|p| meta.atts.get(&p).cloned())
            .unwrap_or_default();
        let fallback = all_atts(&meta);
        let mut atts =
            self.apply_attachments(&parent, &fallback, attachments.as_ref(), leaf.pos)?;
        for (_, a) in atts.iter_mut() {
            if a.revpos == 0 {
                a.revpos = leaf.pos;
            }
        }

        let path = revisions_path(&leaf, revisions.as_ref())?;
        let changed = !known;
        if changed {
            meta.tree.merge_path(&path, deleted);
        }

        self.ops
            .push((body_key(&id, &rev), Some(serde_json::to_vec(&body)?)));
        if changed {
            let old_seq = meta.seq;
            let seq = self.next_seq();
            meta.seq = seq;
            meta.atts.insert(rev.clone(), atts);
            self.ops.push((seq_key(seq), Some(id.as_bytes().to_vec())));
            if old_seq != 0 {
                self.ops.push((seq_key(old_seq), None));
            }
            self.flush_meta(&id, &meta)?;
        }

        Ok(json!({"ok": true, "id": id, "rev": rev}))
    }

    fn apply_attachments(
        &mut self,
        inherited: &[(String, AttMeta)],
        fallback: &[(String, AttMeta)],
        submitted: Option<&Value>,
        pos: u64,
    ) -> DbResult<Vec<(String, AttMeta)>> {
        let Some(submitted) = submitted else {
            return Ok(inherited.to_vec());
        };
        let Some(obj) = submitted.as_object() else {
            return Err(ApiError::bad_request("_attachments must be an object"));
        };
        let mut out = Vec::new();
        for (name, value) in obj {
            if let Some((mut att, bytes)) = decode_attachment(value)? {
                att.revpos = pos;
                for (i, chunk) in bytes.chunks(ATTEST_CHUNK).enumerate() {
                    self.ops
                        .push((blob_prefix(&att.digest, i as u32), Some(chunk.to_vec())));
                }
                out.push((name.clone(), att));
                continue;
            }
            let digest = value
                .get("digest")
                .and_then(Value::as_str)
                .and_then(digest_from_wire);
            let found = inherited
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, a)| a.clone())
                .or_else(|| {
                    fallback
                        .iter()
                        .find(|(n, _)| n == name)
                        .map(|(_, a)| a.clone())
                })
                .or_else(|| {
                    digest.as_ref().and_then(|d| {
                        fallback
                            .iter()
                            .find(|(_, a)| &a.digest == d)
                            .map(|(_, a)| a.clone())
                    })
                })
                .ok_or_else(|| {
                    ApiError::new(
                        412,
                        "missing_stub",
                        format!("Invalid attachment stub in {name}"),
                    )
                })?;
            out.push((name.clone(), found));
        }
        Ok(out)
    }
}

/// Reconstruct the root-to-leaf path from a replicated document's `_revisions`.
fn revisions_path(leaf: &Rev, revisions: Option<&Value>) -> DbResult<Vec<Rev>> {
    let Some(revs) = revisions else {
        return Ok(vec![leaf.clone()]);
    };
    let Some(start) = revs.get("start").and_then(Value::as_u64) else {
        return Err(ApiError::bad_request("_revisions.start is not an integer"));
    };
    let Some(ids) = revs.get("ids").and_then(Value::as_array) else {
        return Err(ApiError::bad_request("_revisions.ids is not an array"));
    };
    let mut path = Vec::new();
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
    if path.is_empty() {
        path.push(leaf.clone());
    }
    Ok(path)
}

fn all_atts(meta: &DocMeta) -> Vec<(String, AttMeta)> {
    let mut out = Vec::new();
    for list in meta.atts.values() {
        for (name, att) in list {
            if !out.iter().any(|(n, _): &(String, AttMeta)| n == name) {
                out.push((name.clone(), att.clone()));
            }
        }
    }
    out
}

fn commit_ops(shard: &mut crate::store::Shard, ops: Ops) -> anyhow::Result<()> {
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
    shard.commit_at(tx, last_seq)
}

fn hex_to_16(hex: &str) -> [u8; 16] {
    let bytes = hex_bytes(hex);
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = bytes.get(i).copied().unwrap_or(0);
    }
    out
}

fn hex_bytes(hex: &str) -> Vec<u8> {
    let bytes = hex.as_bytes();
    (0..bytes.len() / 2)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap_or(0))
        .collect()
}

/// CouchDB writes attachment digests as `md5-<base64 of the raw digest>`, not
/// hex. The wire format is not cosmetic: PouchDB uses it to decide whether an
/// attachment has to be transferred.
pub fn digest_to_wire(hex: &str) -> String {
    format!(
        "md5-{}",
        base64::engine::general_purpose::STANDARD.encode(hex_bytes(hex))
    )
}

/// Parse `md5-<base64>` (and, leniently, a bare hex digest) back to hex.
pub fn digest_from_wire(value: &str) -> Option<String> {
    let value = value.strip_prefix("md5-").unwrap_or(value);
    if value.len() == 32 && value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Some(value.to_ascii_lowercase());
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(value)
        .ok()?;
    if bytes.len() != 16 {
        return None;
    }
    Some(hex(&bytes))
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
    #[allow(dead_code)]
    pub atts_since: Option<Vec<String>>,
    #[allow(dead_code)]
    pub latest: bool,
}

#[derive(Clone, Debug)]
pub struct AllDocsOpts {
    pub partition: Option<String>,
    pub include_docs: bool,
    pub conflicts: bool,
    pub attachments: bool,
    pub keys: Option<Vec<String>>,
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

#[derive(Clone, Debug, Default)]
pub struct ChangesOpts {
    pub since: u64,
    pub limit: Option<usize>,
    pub include_docs: bool,
    pub style_all_docs: bool,
    pub descending: bool,
    pub doc_ids: Option<Vec<String>>,
    /// Internal cluster filter: only scan shards selected by this mask.
    pub owned: Option<Vec<bool>>,
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

fn read_doc(shard: &mut crate::store::Shard, id: &str, opts: &DocOpts) -> DbResult<Value> {
    let meta = shard
        .get_meta(id)?
        .ok_or_else(|| ApiError::not_found("missing"))?;

    if let Some(open_revs) = &opts.open_revs {
        let wanted: Vec<String> = if open_revs.iter().any(|r| r == "all") {
            meta.tree.leaves()
        } else {
            open_revs.clone()
        };
        let mut docs = Vec::new();
        for rev in wanted {
            match read_doc_rev(shard, id, &meta, &rev, opts) {
                Ok(doc) => docs.push(json!({"ok": doc})),
                Err(e) => docs.push(json!({"missing": rev, "error": e.error})),
            }
        }
        return Ok(Value::Array(docs));
    }

    let rev = match &opts.rev {
        Some(r) => r.clone(),
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
    if !meta.tree.edges.contains_key(rev) {
        return Err(ApiError::not_found("missing"));
    }
    let body = shard
        .get_body(id, rev)?
        .ok_or_else(|| ApiError::not_found("missing"))?;
    let mut doc: Value = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(500, "internal_server_error", "corrupt body"))?;
    let Value::Object(body) = &mut doc else {
        return Err(ApiError::new(500, "internal_server_error", "corrupt body"));
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
        let mut path = meta.tree.path(rev);
        let limit = shard.revs_limit().max(1) as usize;
        if path.len() > limit {
            path.drain(0..path.len() - limit);
        }
        let start = Rev::parse(rev).map(|r| r.pos).unwrap_or(0);
        let ids: Vec<String> = path
            .iter()
            .rev()
            .filter_map(|r| Rev::parse(r).map(|p| hex(&p.id)))
            .collect();
        map.insert("_revisions".into(), json!({ "start": start, "ids": ids }));
    }
    if opts.local_seq {
        map.insert("_local_seq".into(), json!(meta.seq));
    }
    if opts.revs_info {
        let mut path = meta.tree.path(rev);
        let limit = shard.revs_limit().max(1) as usize;
        if path.len() > limit {
            path.drain(0..path.len() - limit);
        }
        let info: Vec<Value> = path
            .iter()
            .rev()
            .map(|r| {
                json!({
                    "rev": r,
                    "status": if meta.tree.is_deleted(r) { "deleted" } else { "available" }
                })
            })
            .collect();
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
        "digest": digest_to_wire(&att.digest),
        "length": att.length,
    });
    if with_data {
        let mut data = Vec::new();
        let mut chunk = 0u32;
        while let Some(part) = shard.get(&blob_prefix(&att.digest, chunk)) {
            data.extend_from_slice(&part);
            chunk += 1;
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
fn collect_all_docs(
    shard: &mut crate::store::Shard,
    opts: &AllDocsOpts,
    take: usize,
) -> DbResult<(
    Vec<(String, Option<String>, Option<Value>, Vec<String>, bool)>,
    usize,
)> {
    let start = opts.startkey.as_deref();
    let end = opts.endkey.as_deref();
    let mut rows = Vec::new();
    let before_start = {
        let live = shard.live_docs()?;
        let prefix = opts.partition.as_ref().map(|p| format!("{p}:"));
        let in_partition = |id: &str| prefix.as_deref().map(|p| id.starts_with(p)).unwrap_or(true);
        let before_start = match (start, opts.descending) {
            (Some(start), false) => live
                .range::<str, _>((Bound::Unbounded, Bound::Excluded(start)))
                .filter(|(id, _)| in_partition(id))
                .count(),
            (Some(start), true) => live
                .range::<str, _>((Bound::Excluded(start), Bound::Unbounded))
                .filter(|(id, _)| in_partition(id))
                .count(),
            (None, _) => 0,
        };

        let empty_range = match (start, end) {
            (Some(start), Some(end)) if opts.descending => start < end,
            (Some(start), Some(end)) => start > end,
            _ => false,
        };
        if !empty_range {
            if opts.descending {
                let range = match (start, end) {
                    (Some(start), Some(end)) => (
                        if opts.inclusive_end {
                            Bound::Included(end)
                        } else {
                            Bound::Excluded(end)
                        },
                        Bound::Included(start),
                    ),
                    (Some(start), None) => (Bound::Unbounded, Bound::Included(start)),
                    (None, Some(end)) => (
                        if opts.inclusive_end {
                            Bound::Included(end)
                        } else {
                            Bound::Excluded(end)
                        },
                        Bound::Unbounded,
                    ),
                    (None, None) => (Bound::Unbounded, Bound::Unbounded),
                };
                for (id, doc) in live.range::<str, _>(range).rev() {
                    if rows.len() >= take {
                        break;
                    }
                    rows.push((id.clone(), Some(doc.rev.clone()), None, Vec::new(), false));
                }
            } else {
                let range = match (start, end) {
                    (Some(start), Some(end)) => (
                        Bound::Included(start),
                        if opts.inclusive_end {
                            Bound::Included(end)
                        } else {
                            Bound::Excluded(end)
                        },
                    ),
                    (Some(start), None) => (Bound::Included(start), Bound::Unbounded),
                    (None, Some(end)) => (
                        Bound::Unbounded,
                        if opts.inclusive_end {
                            Bound::Included(end)
                        } else {
                            Bound::Excluded(end)
                        },
                    ),
                    (None, None) => (Bound::Unbounded, Bound::Unbounded),
                };
                for (id, doc) in live.range::<str, _>(range) {
                    if rows.len() >= take {
                        break;
                    }
                    rows.push((id.clone(), Some(doc.rev.clone()), None, Vec::new(), false));
                }
            }
        }
        before_start
    };

    if opts.include_docs || opts.conflicts {
        for (id, rev, doc, conflicts, _) in rows.iter_mut() {
            let Some(rev) = rev.as_ref() else {
                continue;
            };
            let meta = if opts.conflicts {
                shard.get_meta(id)?
            } else {
                None
            };
            if let Some(meta) = &meta {
                *conflicts = meta.tree.conflicts(rev);
            }
            if opts.include_docs {
                let doc_opts = DocOpts {
                    rev: Some(rev.clone()),
                    conflicts: opts.conflicts,
                    attachments: opts.attachments,
                    ..Default::default()
                };
                *doc = read_doc(shard, id, &doc_opts).ok();
            }
        }
    }

    Ok((rows, before_start))
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

fn finish_all_docs(
    rows: Vec<(String, Option<String>, Option<Value>, Vec<String>, bool)>,
    total: usize,
    offset: usize,
) -> DbResult<Value> {
    let mut results = Vec::new();
    for (id, rev, doc, _conflicts, _deleted) in rows {
        let mut row = json!({"id": id, "key": id, "value": {"rev": rev}});
        if let Some(doc) = doc {
            row["doc"] = doc;
        }
        results.push(row);
    }
    Ok(json!({ "total_rows": total, "offset": offset, "rows": results }))
}

fn collect_changes(
    shard: &mut crate::store::Shard,
    opts: &ChangesOpts,
) -> DbResult<Vec<ChangeRow>> {
    let limit = opts.limit.unwrap_or(usize::MAX);
    if limit == 0 {
        return Ok(Vec::new());
    }

    // CouchDB returns one row per document at its latest sequence, not one
    // row per historical sequence entry. `latest_changes` is maintained on
    // writes and built lazily from metadata.
    let mut candidates: Vec<(u64, String)> = shard
        .latest_changes()?
        .iter()
        .filter(|(_, seq)| opts.descending || **seq > opts.since)
        .map(|(id, seq)| (*seq, id.clone()))
        .collect();
    if let Some(ids) = &opts.doc_ids {
        candidates.retain(|(_, id)| ids.iter().any(|wanted| wanted == id));
    }
    candidates.sort_by_key(|(seq, _)| *seq);
    if opts.descending {
        candidates.reverse();
    }
    candidates.truncate(limit);

    let mut rows = Vec::with_capacity(candidates.len());
    for (seq, id) in candidates {
        let Some(meta) = shard.get_meta(&id)? else {
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
            meta.tree
                .leaves()
                .into_iter()
                .map(|leaf| {
                    let leaf_deleted = meta.tree.is_deleted(&leaf);
                    (leaf, leaf_deleted)
                })
                .collect()
        } else {
            vec![(winner.clone(), deleted)]
        };

        let doc = if opts.include_docs {
            read_doc_rev(
                shard,
                &id,
                &meta,
                &winner,
                &DocOpts {
                    rev: Some(winner.clone()),
                    include_deleted: true,
                    ..Default::default()
                },
            )
            .ok()
            .and_then(|doc| serde_json::to_string(&doc).ok())
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

    fn doc_id_for_shard(db: &Db, shard: usize) -> String {
        (0..10_000)
            .map(|i| format!("shard{shard}-{i}"))
            .find(|id| db.shard_index(id) == shard)
            .expect("document id for shard")
    }

    #[tokio::test]
    async fn changes_limit_reports_pending_documents() {
        let root = std::env::temp_dir().join(format!("douchdb-pending-{}", crate::db::uuid()));
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
            owned: None,
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

    #[tokio::test]
    async fn changes_cursor_stops_at_allocated_but_uncommitted_shard_sequence() {
        let q = 4u32;
        let root = std::env::temp_dir().join(format!("douchdb-watermark-{}", crate::db::uuid()));
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
            owned: None,
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

    use proptest::prelude::*;

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
            let mask = owned.as_ref().map(|m| {
                let mut m = m.clone();
                m.resize(committed.len(), false);
                m
            });
            let watermark = changes_watermark_from(
                &committed,
                &reserved,
                since,
                mask.as_deref(),
            );

            for index in 0..committed.len() {
                if mask.as_ref().is_some_and(|m| !m[index]) {
                    continue;
                }
                if reserved[index] > committed[index] {
                    prop_assert!(watermark <= committed[index].max(since));
                    prop_assert!(watermark >= since);
                }
            }
            if watermark != u64::MAX {
                prop_assert!(watermark >= since);
            }
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

//! Database and document semantics: revisions, conflicts, changes feeds.
//!
//! All writes for one request are staged as an ordered list of (key, value)
//! operations and applied as a single LevelDB `WriteBatch`, so a `_bulk_docs`
//! of a thousand documents costs one fsync, not a thousand.

use crate::cluster::ShardMap;
pub use crate::error::{ApiError, DbResult};
use crate::rev::{new_revid, Attachment, Rev};
use crate::store::{
    blob_prefix, body_key, local_key, meta_key, seq_key, seq_of_key, AttMeta, DocMeta, ShardHandle,
    Tx, ATTEST_CHUNK,
};
use base64::Engine;
use md5::{Digest, Md5};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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

pub struct Db {
    pub name: String,
    #[allow(dead_code)]
    pub path: PathBuf,
    pub q: u32,
    pub uuid: String,
    pub instance_start_time: String,
    shard_map: Arc<std::sync::RwLock<ShardMap>>,
    shards: Vec<ShardHandle>,
    /// One global sequence counter for the database. CouchDB's `since` is a
    /// database-wide cursor, so shard-local counters cannot be used directly.
    seq: Arc<AtomicU64>,
}

#[derive(Clone)]
pub struct DbConfig {
    pub q: u32,
    pub revs_limit: u64,
    pub shard_map: ShardMap,
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
        for i in 0..file.q {
            shards.push(ShardHandle::spawn(
                path.join(format!("shard-{i:04}")),
                config.revs_limit,
            )?);
        }

        let seq = shards
            .iter()
            .map(ShardHandle::initial_seq)
            .max()
            .unwrap_or(0);

        Ok(Db {
            name: name.to_string(),
            path,
            q: file.q.max(1),
            uuid: file.uuid,
            instance_start_time: file.instance_start_time,
            shard_map: Arc::new(std::sync::RwLock::new(shard_map)),
            shards,
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
        shard_index(docid, self.q)
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

    pub fn owner_of_doc(&self, docid: &str) -> usize {
        self.owner_of_shard(self.shard_index(docid) as u32)
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

    fn shard(&self, docid: &str) -> &ShardHandle {
        &self.shards[self.shard_index(docid)]
    }

    pub async fn info(&self) -> DbResult<Value> {
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
        Ok(json!({
            "db_name": self.name,
            "doc_count": doc_count,
            "doc_del_count": deleted_count,
            "update_seq": update_seq,
            "purge_seq": 0,
            "compact_running": false,
            "disk_size": 0,
            "data_size": 0,
            "sizes": { "active": 0, "external": 0, "file": 0 },
            "instance_start_time": self.instance_start_time,
            "disk_format_version": 8,
            "committed_update_seq": update_seq,
            "uuid": self.uuid,
        }))
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
                Ok(id) => {
                    ids[i] = Some(id.clone());
                    grouped[self.shard_index(&id)].push((i, doc));
                }
                Err(e) => out[i] = Some(json!({"error": e.error, "reason": e.reason})),
            }
        }

        for (shard_idx, batch) in grouped.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let seq = self.seq.clone();
            let results = self.shards[shard_idx]
                .call(move |s| Ok(put_many(s, batch, new_edits, &seq)))
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
        let mut rows = Vec::new();
        let mut total_rows = 0u64;
        let mut before_start = 0usize;
        for (index, shard) in self.shards.iter().enumerate() {
            if owned
                .map(|owned| !owned.get(index).copied().unwrap_or(false))
                .unwrap_or(false)
            {
                continue;
            }
            let opts = opts.clone();
            let (mut shard_rows, shard_offset, shard_docs) = shard
                .call(move |s| {
                    let (docs, _) = s.counts();
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
        finish_all_docs(selected, total_rows as usize, before_start + opts.skip)
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
                            if let Some(body) = s.get_body(&id, &rev) {
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
        for (index, shard) in self.shards.iter().enumerate() {
            if owned
                .map(|owned| !owned.get(index).copied().unwrap_or(false))
                .unwrap_or(false)
            {
                continue;
            }
            total_rows += shard.call(|s| Ok(s.counts().0)).await?;
        }

        let page: Vec<String> = keys
            .iter()
            .skip(opts.skip)
            .take(opts.limit)
            .cloned()
            .collect();
        let mut grouped: Vec<Vec<(usize, String)>> = (0..self.q).map(|_| Vec::new()).collect();
        let mut included = vec![false; page.len()];
        for (slot, key) in page.iter().enumerate() {
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
                        .map(|(slot, key)| (slot, all_docs_key_row(s, &key, &opts)))
                        .collect::<Vec<_>>();
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
                .map(|(_, row)| row.unwrap_or(Value::Null))
                .collect::<Vec<_>>(),
        }))
    }

    pub async fn changes(&self, opts: &ChangesOpts) -> DbResult<Value> {
        let mut rows = Vec::new();
        for shard in &self.shards {
            let shard_opts = opts.clone();
            let mut shard_rows = shard.call(move |s| collect_changes(s, &shard_opts)).await?;
            rows.append(&mut shard_rows);
        }
        rows.sort_by_key(|r| r.seq);
        if opts.descending {
            rows.reverse();
        }
        if let Some(limit) = opts.limit {
            rows.truncate(limit);
        }
        let last_seq = rows.last().map(|r| r.seq).unwrap_or(opts.since);

        let mut results = Vec::new();
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
                entry["doc"] = doc;
            }
            results.push(entry);
        }
        Ok(json!({ "results": results, "last_seq": last_seq, "pending": 0 }))
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
                    let tree = s.get_meta(&id2).map(|m| m.tree).unwrap_or_default();
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
                Ok(json!({"ok": true, "id": format!("_local/{id}")}))
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
            let done = self
                .shard(id)
                .call(move |s| {
                    let Some(mut meta) = s.get_meta(&id2) else {
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
                            meta.seq = seq;
                            tx.delete(&seq_key(old_seq));
                            tx.put(&seq_key(seq), id2.as_bytes());
                            tx.put_json(&meta_key(&id2), &meta)?;
                            s.commit_at(tx, Some(seq))?;
                        }
                    } else {
                        s.commit(tx)?;
                    }
                    Ok(removed)
                })
                .await?;
            purged.insert(id.clone(), json!(done));
        }
        Ok(json!({ "purge_seq": 0, "purged": purged }))
    }

    pub async fn compact(&self) -> DbResult<()> {
        for shard in &self.shards {
            shard.call(|s| s.compact().map_err(ApiError::from)).await?;
        }
        Ok(())
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
                return Err(ApiError::bad_request(
                    "Only reserved document ids may start with underscore.",
                ));
            }
            Ok(id.clone())
        }
        Some(_) => Err(ApiError::bad_request("Document id must be a string")),
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
                _ => return Err(ApiError::bad_request("Document id must be a string")),
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
        return Err(ApiError::bad_request("Document id must not be empty"));
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
        };
        match ctx.stage(doc, new_edits) {
            Ok(value) => results.push((i, Ok(value))),
            Err(e) => {
                ops.truncate(mark);
                results.push((i, Err(e)));
            }
        }
    }

    if let Err(e) = commit_ops(shard, ops) {
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

struct Stage<'a> {
    shard: &'a mut crate::store::Shard,
    ops: &'a mut Ops,
    staged: &'a mut HashMap<String, DocMeta>,
    seq: &'a Arc<AtomicU64>,
}

impl Stage<'_> {
    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn meta(&mut self, id: &str) -> Option<DocMeta> {
        if let Some(m) = self.staged.get(id) {
            return Some(m.clone());
        }
        self.shard.get_meta(id)
    }

    fn flush_meta(&mut self, id: &str, meta: &DocMeta) -> DbResult<()> {
        let bytes = serde_json::to_vec(meta)?;
        self.ops.push((meta_key(id), Some(bytes)));
        self.staged.insert(id.to_string(), meta.clone());
        Ok(())
    }

    fn stage(&mut self, doc: Value, new_edits: bool) -> DbResult<Value> {
        let (id, parent_rev, deleted, attachments, revisions, body) = split_doc(doc)?;
        let mut meta = self.meta(&id).unwrap_or_default();
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
        meta.tree.prune(self.shard.revs_limit());
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
            meta.tree.prune(self.shard.revs_limit());
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
            last_seq = key[2..10].try_into().ok().map(u64::from_be_bytes);
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
    pub conflicts: bool,
    pub deleted_conflicts: bool,
    pub attachments: bool,
    pub open_revs: Option<Vec<String>>,
    #[allow(dead_code)]
    pub atts_since: Option<Vec<String>>,
    #[allow(dead_code)]
    pub latest: bool,
}

#[derive(Clone, Debug)]
pub struct AllDocsOpts {
    pub include_docs: bool,
    pub conflicts: bool,
    pub attachments: bool,
    pub keys: Option<Vec<String>>,
    pub startkey: Option<String>,
    pub endkey: Option<String>,
    pub descending: bool,
    pub limit: usize,
    pub skip: usize,
}

impl Default for AllDocsOpts {
    fn default() -> Self {
        AllDocsOpts {
            include_docs: false,
            conflicts: false,
            attachments: false,
            keys: None,
            startkey: None,
            endkey: None,
            descending: false,
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
}

struct ChangeRow {
    id: String,
    seq: u64,
    changes: Vec<(String, bool)>,
    deleted: bool,
    doc: Option<Value>,
}

fn read_doc(shard: &mut crate::store::Shard, id: &str, opts: &DocOpts) -> DbResult<Value> {
    let meta = shard
        .get_meta(id)
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
            if meta.tree.is_deleted(&winner) {
                return Err(ApiError::not_found("deleted"));
            }
            winner
        }
    };
    read_doc_rev(shard, id, &meta, &rev, opts)
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
        .get_body(id, rev)
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
        let path = meta.tree.path(rev);
        let start = Rev::parse(rev).map(|r| r.pos).unwrap_or(0);
        let ids: Vec<String> = path
            .iter()
            .rev()
            .filter_map(|r| Rev::parse(r).map(|p| hex(&p.id)))
            .collect();
        map.insert("_revisions".into(), json!({ "start": start, "ids": ids }));
    }
    if opts.revs_info {
        let info: Vec<Value> = meta
            .tree
            .path(rev)
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
        let before_start = match (start, opts.descending) {
            (Some(start), false) => live
                .range::<str, _>((Bound::Unbounded, Bound::Excluded(start)))
                .count(),
            (Some(start), true) => live
                .range::<str, _>((Bound::Excluded(start), Bound::Unbounded))
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
                    (Some(start), Some(end)) => (Bound::Included(end), Bound::Included(start)),
                    (Some(start), None) => (Bound::Unbounded, Bound::Included(start)),
                    (None, Some(end)) => (Bound::Included(end), Bound::Unbounded),
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
                    (Some(start), Some(end)) => (Bound::Included(start), Bound::Included(end)),
                    (Some(start), None) => (Bound::Included(start), Bound::Unbounded),
                    (None, Some(end)) => (Bound::Unbounded, Bound::Included(end)),
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
                shard.get_meta(id)
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
) -> Option<Value> {
    let meta = shard.get_meta(key)?;
    let winner = meta.tree.winner()?;
    let deleted = meta.tree.is_deleted(&winner);
    let mut row = json!({"id": key, "key": key, "value": {"rev": winner.clone()}});
    if deleted {
        row["value"]["deleted"] = json!(true);
    }
    if opts.conflicts {
        let conflicts = meta.tree.conflicts(&winner);
        if !conflicts.is_empty() {
            row["value"]["_conflicts"] = json!(conflicts);
        }
    }
    if opts.include_docs {
        let doc_opts = DocOpts {
            rev: Some(winner),
            conflicts: opts.conflicts,
            attachments: opts.attachments,
            ..Default::default()
        };
        if let Ok(doc) = read_doc(shard, key, &doc_opts) {
            row["doc"] = doc;
        }
    }
    Some(row)
}

fn finish_all_docs(
    rows: Vec<(String, Option<String>, Option<Value>, Vec<String>, bool)>,
    total: usize,
    offset: usize,
) -> DbResult<Value> {
    let mut results = Vec::new();
    for (id, rev, doc, conflicts, _deleted) in rows {
        let mut row = json!({"id": id, "key": id, "value": {"rev": rev}});
        if !conflicts.is_empty() {
            row["value"]["_conflicts"] = json!(conflicts);
        }
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

    // Each sequence key maps to one document's current change. When there is
    // no filter and no descending merge, the shard only needs `limit`
    // sequence entries; `_doc_ids` filters may skip entries, so those still
    // scan the range before processing.
    let entry_cap = if opts.doc_ids.is_none() && !opts.descending {
        limit
    } else {
        usize::MAX
    };
    let mut entries = Vec::new();
    shard.scan_range_while(
        b"s/",
        Some(&seq_key(opts.since.saturating_add(1))),
        false,
        |key, value| {
            entries.push((key.to_vec(), value.to_vec()));
            entries.len() < entry_cap
        },
    )?;

    let mut rows = Vec::new();
    for (key, id_bytes) in entries {
        let Some(seq) = seq_of_key(&key) else {
            continue;
        };
        let Ok(id) = String::from_utf8(id_bytes) else {
            continue;
        };
        if let Some(ids) = &opts.doc_ids {
            if !ids.contains(&id) {
                continue;
            }
        }
        let Some(meta) = shard.get_meta(&id) else {
            continue;
        };
        if meta.seq != seq {
            continue;
        }
        let Some(winner) = meta.tree.winner() else {
            continue;
        };
        let deleted = meta.tree.is_deleted(&winner);
        let changes = if opts.style_all_docs {
            meta.tree
                .leaves()
                .into_iter()
                .map(|leaf| {
                    let deleted = meta.tree.is_deleted(&leaf);
                    (leaf, deleted)
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
                    ..Default::default()
                },
            )
            .ok()
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
        // Descending feeds are reversed after the merge, so the first `limit`
        // rows collected here are not the ones the response keeps.
        if !opts.descending && rows.len() >= limit {
            break;
        }
    }
    Ok(rows)
}

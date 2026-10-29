//! Database and document semantics: revisions, conflicts, changes feeds.
//!
//! All writes for one request are staged as an ordered list of (key, value)
//! operations and applied as a single LevelDB `WriteBatch`, so a `_bulk_docs`
//! of a thousand documents costs one fsync, not a thousand.

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

pub struct Db {
    pub name: String,
    #[allow(dead_code)]
    pub path: PathBuf,
    pub q: u32,
    pub uuid: String,
    pub instance_start_time: String,
    shards: Vec<ShardHandle>,
    /// One global sequence counter for the database. CouchDB's `since` is a
    /// database-wide cursor, so shard-local counters cannot be used directly.
    seq: Arc<AtomicU64>,
}

#[derive(Clone, Copy)]
pub struct DbConfig {
    pub q: u32,
    pub revs_limit: u64,
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
        if self.q <= 1 {
            return 0;
        }
        let digest = Md5::digest(docid.as_bytes());
        let n = u32::from_be_bytes(digest[..4].try_into().unwrap());
        (n % self.q) as usize
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
                    let mut docs = 0;
                    let mut deleted = 0;
                    for (_, bytes) in s.scan(b"m/")? {
                        if let Ok(meta) = serde_json::from_slice::<DocMeta>(&bytes) {
                            match meta.tree.winner() {
                                Some(w) if !meta.tree.is_deleted(&w) => docs += 1,
                                Some(_) => deleted += 1,
                                None => {}
                            }
                        }
                    }
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
        let mut rows = Vec::new();
        for shard in &self.shards {
            let opts = opts.clone();
            let mut shard_rows = shard.call(move |s| collect_all_docs(s, &opts)).await?;
            rows.append(&mut shard_rows);
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        finish_all_docs(rows, opts)
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
        let mut results = Vec::new();
        for req in reqs {
            let id = req
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let rev = req.get("rev").and_then(Value::as_str).map(str::to_string);
            let revs = req.get("revs").and_then(Value::as_bool).unwrap_or(revs);
            let opts = DocOpts {
                rev: rev.clone(),
                revs,
                attachments: req
                    .get("attachments")
                    .and_then(Value::as_bool)
                    .unwrap_or(attachments),
                ..Default::default()
            };
            let docs = match self.get_doc(&id, &opts).await {
                Ok(doc) => vec![json!({"ok": doc})],
                Err(e) => vec![json!({"error": {
                    "id": id,
                    "rev": rev.unwrap_or_else(|| "undefined".into()),
                    "error": e.error,
                    "reason": e.reason
                }})],
            };
            results.push(json!({ "id": id, "docs": docs }));
        }
        Ok(json!({ "results": results }))
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

    pub async fn attachment(
        &self,
        docid: &str,
        rev: Option<&str>,
        name: &str,
    ) -> DbResult<(AttMeta, Vec<u8>)> {
        let docid = docid.to_string();
        let rev = rev.map(str::to_string);
        let name = name.to_string();
        self.shard(&docid)
            .call(move |s| {
                let meta = s
                    .get_meta(&docid)
                    .ok_or_else(|| ApiError::not_found("missing"))?;
                let rev = match rev {
                    Some(r) => r,
                    None => meta
                        .tree
                        .winner()
                        .ok_or_else(|| ApiError::not_found("missing"))?,
                };
                let att = meta
                    .atts
                    .get(&rev)
                    .and_then(|list| list.iter().find(|(n, _)| n.as_str() == name))
                    .map(|(_, a)| a.clone())
                    .ok_or_else(|| ApiError::not_found("Document is missing attachment"))?;
                let mut data = Vec::new();
                let mut chunk = 0u32;
                while let Some(part) = s.get(&blob_prefix(&att.digest, chunk)) {
                    data.extend_from_slice(&part);
                    chunk += 1;
                }
                Ok((att, data))
            })
            .await
    }
}

// ---------------------------------------------------------------------------
// document helpers

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
            seq: &seq,
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
        if known && meta.atts.get(&rev).is_some() && attachments.is_none() {
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

fn collect_all_docs(
    shard: &mut crate::store::Shard,
    opts: &AllDocsOpts,
) -> DbResult<Vec<(String, Option<String>, Option<Value>, Vec<String>, bool)>> {
    let mut rows = Vec::new();
    for (key, bytes) in shard.scan(b"m/")? {
        let Some(id) = std::str::from_utf8(&key[2..]).ok() else {
            continue;
        };
        let Ok(meta) = serde_json::from_slice::<DocMeta>(&bytes) else {
            continue;
        };
        let Some(winner) = meta.tree.winner() else {
            continue;
        };
        let deleted = meta.tree.is_deleted(&winner);
        if deleted && opts.keys.is_none() {
            continue;
        }
        let conflicts = if opts.conflicts {
            meta.tree.conflicts(&winner)
        } else {
            Vec::new()
        };
        let doc = if opts.include_docs {
            let doc_opts = DocOpts {
                rev: Some(winner.clone()),
                conflicts: opts.conflicts,
                attachments: opts.attachments,
                ..Default::default()
            };
            read_doc_rev(shard, id, &meta, &winner, &doc_opts).ok()
        } else {
            None
        };
        rows.push((id.to_string(), Some(winner), doc, conflicts, deleted));
    }
    Ok(rows)
}

fn finish_all_docs(
    rows: Vec<(String, Option<String>, Option<Value>, Vec<String>, bool)>,
    opts: &AllDocsOpts,
) -> DbResult<Value> {
    let total = rows.iter().filter(|(_, _, _, _, deleted)| !deleted).count();
    let mut results = Vec::new();

    if let Some(keys) = &opts.keys {
        for key in keys {
            match rows.iter().find(|r| &r.0 == key) {
                Some((id, rev, doc, conflicts, deleted)) => {
                    let mut row = json!({"id": id, "key": id, "value": {"rev": rev}});
                    if *deleted {
                        row["value"]["deleted"] = json!(true);
                    }
                    if !conflicts.is_empty() {
                        row["value"]["_conflicts"] = json!(conflicts);
                    }
                    if let Some(doc) = doc {
                        row["doc"] = doc.clone();
                    }
                    results.push(row);
                }
                None => results.push(json!({"key": key, "error": "not_found"})),
            }
        }
        return Ok(json!({ "total_rows": total, "offset": Value::Null, "rows": results }));
    }

    // `offset` is the position of the first returned row in the full index,
    // not the number of rows this response happened to skip.
    let before_start = match (&opts.startkey, opts.descending) {
        (Some(start), false) => rows.iter().filter(|(id, _, _, _, _)| id < start).count(),
        (Some(start), true) => rows.iter().filter(|(id, _, _, _, _)| id > start).count(),
        (None, _) => 0,
    };
    let mut selected: Vec<_> = rows
        .into_iter()
        .filter(|(id, _, _, _, _)| {
            let after_start = opts.startkey.as_ref().map(|s| id >= s).unwrap_or(true);
            let before_end = opts.endkey.as_ref().map(|e| id <= e).unwrap_or(true);
            after_start && before_end
        })
        .collect();
    if opts.descending {
        selected.reverse();
    }
    let selected: Vec<_> = selected
        .into_iter()
        .skip(opts.skip)
        .take(opts.limit)
        .collect();

    for (id, rev, doc, conflicts, _deleted) in selected {
        let mut row = json!({"id": id, "key": id, "value": {"rev": rev}});
        if !conflicts.is_empty() {
            row["value"]["_conflicts"] = json!(conflicts);
        }
        if let Some(doc) = doc {
            row["doc"] = doc;
        }
        results.push(row);
    }
    Ok(json!({ "total_rows": total, "offset": before_start + opts.skip, "rows": results }))
}

fn collect_changes(
    shard: &mut crate::store::Shard,
    opts: &ChangesOpts,
) -> DbResult<Vec<ChangeRow>> {
    let entries = shard.scan_from(b"s/", &seq_key(opts.since.saturating_add(1)))?;
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
    }
    Ok(rows)
}

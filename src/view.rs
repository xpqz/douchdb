//! CouchDB map/reduce views, evaluated with an embedded QuickJS runtime.
//!
//! Each view has a persistent LevelDB index under `_view_indexes/`. A query
//! applies `_changes` since the last indexed sequence and only re-runs the map
//! function for documents that changed; a design-document change rebuilds the
//! index. The JS engine exposes the same `doc`/`emit` environment as CouchDB's
//! JavaScript view server.

use crate::db::{hex, AllDocsOpts, ApiError, ChangesOpts, Db, DbResult, DocOpts};
use base64::Engine as _;
use icu_collator::options::CollatorOptions;
use icu_collator::Collator;
use md5::{Digest, Md5};
use rusty_leveldb::{LdbIterator, Options as LdbOptions, WriteBatch, DB as Ldb};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

#[derive(Clone, Copy, Debug)]
pub struct ViewLimits {
    pub timeout: Duration,
    pub memory: usize,
}

impl Default for ViewLimits {
    fn default() -> Self {
        ViewLimits {
            timeout: Duration::from_secs(5),
            memory: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ViewQuery {
    pub key: Option<Value>,
    pub keys: Option<Vec<Value>>,
    pub startkey: Option<Value>,
    pub endkey: Option<Value>,
    pub startkey_docid: Option<String>,
    pub endkey_docid: Option<String>,
    pub descending: bool,
    pub inclusive_end: bool,
    pub skip: usize,
    pub limit: usize,
    pub reduce: Option<bool>,
    pub group: bool,
    pub group_level: usize,
    pub include_docs: bool,
    pub attachments: bool,
    pub update_seq: bool,
    pub limits: ViewLimits,
    /// Internal cluster filter: return only rows whose shard is owned by this
    /// node. The coordinator unions the owner-only responses, so each map row
    /// crosses the network once instead of once per replica.
    pub owner_shards: Option<Vec<bool>>,
    pub shard_count: u32,
    /// Set for a `_partition/{p}/_design/.../_view/...` query. Only rows
    /// belonging to that partition are returned.
    pub partition: Option<String>,
    /// Partitioned databases hash by partition, so owner-shard filtering must
    /// use the same placement function.
    pub partitioned: bool,
    /// Set by the cluster coordinator after summing owner-only map row counts.
    pub total_rows_override: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct ViewSpec {
    pub map: String,
    pub reduce: Option<String>,
    /// Design documents in a partitioned database are partitioned by default.
    /// `options.partitioned = false` makes the view global.
    pub partitioned: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row {
    pub id: String,
    pub key: Value,
    pub value: Value,
}

#[derive(Serialize, Deserialize)]
struct IndexMeta {
    last_seq: u64,
    design: String,
}

#[derive(Serialize, Deserialize)]
struct StoredRow {
    key: Value,
    value: Value,
}

#[derive(Serialize)]
struct StoredRowRef<'a> {
    key: &'a Value,
    value: &'a Value,
}

/// A persistent index for one (database, design document, view) tuple. Rows
/// are stored per document so a changed document can be re-mapped without
/// walking the whole database.
struct ViewIndex {
    db: Ldb,
    last_seq: u64,
    design: String,
    /// Sorted rows for the current index state. Warm queries reuse this
    /// instead of scanning and parsing every stored row on each request.
    cached: Option<Arc<Vec<Row>>>,
}

/// Rows are ordered by CouchDB key collation, then document id.
pub fn sort_rows(rows: &mut [Row]) {
    rows.sort_by(row_cmp);
}

fn row_cmp(a: &Row, b: &Row) -> Ordering {
    cmp_json(&a.key, &b.key).then_with(|| a.id.cmp(&b.id))
}

/// Merge two already-sorted row runs.
fn merge_sorted_rows(base: Vec<Row>, added: Vec<Row>) -> Vec<Row> {
    let mut out = Vec::with_capacity(base.len() + added.len());
    let mut base = base.into_iter().peekable();
    let mut added = added.into_iter().peekable();
    loop {
        match (base.peek(), added.peek()) {
            (Some(left), Some(right)) => {
                if row_cmp(left, right) == Ordering::Greater {
                    out.push(added.next().expect("peeked row"));
                } else {
                    out.push(base.next().expect("peeked row"));
                }
            }
            (Some(_), None) => {
                out.extend(base);
                break;
            }
            (None, Some(_)) => {
                out.extend(added);
                break;
            }
            (None, None) => break,
        }
    }
    out
}

type IndexMap = HashMap<PathBuf, Arc<AsyncMutex<ViewIndex>>>;

fn registry() -> &'static StdMutex<IndexMap> {
    static INDEXES: OnceLock<StdMutex<IndexMap>> = OnceLock::new();
    INDEXES.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn registry_lock() -> std::sync::MutexGuard<'static, IndexMap> {
    // A panic in an unrelated request must not poison every later view query.
    registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn index_path(db: &Db, ddoc_id: &str, view_name: &str) -> PathBuf {
    let digest = Md5::digest(format!("{ddoc_id}\0{view_name}").as_bytes());
    db.path.join("_view_indexes").join(hex(&digest))
}

fn index_handle(path: &Path) -> DbResult<Arc<AsyncMutex<ViewIndex>>> {
    let mut indexes = registry_lock();
    if let Some(handle) = indexes.get(path) {
        return Ok(handle.clone());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| ApiError::new(500, "view_index_error", error.to_string()))?;
    }
    let index =
        ViewIndex::open(path).map_err(|error| ApiError::new(500, "view_index_error", error))?;
    let handle = Arc::new(AsyncMutex::new(index));
    indexes.insert(path.to_path_buf(), handle.clone());
    Ok(handle)
}

/// Drop cached index handles when a database is deleted or recreated. The
/// LevelDB files live under the database directory.
pub fn forget_db(db_path: &Path) {
    let prefix = db_path.join("_view_indexes");
    registry_lock().retain(|path, _| !path.starts_with(&prefix));
}

impl ViewIndex {
    fn open(path: &Path) -> Result<Self, String> {
        let mut options = LdbOptions::default();
        options.create_if_missing = true;
        let mut db = Ldb::open(path, options).map_err(|error| error.to_string())?;
        let meta = db
            .get(b"m")
            .and_then(|bytes| serde_json::from_slice::<IndexMeta>(&bytes).ok());
        let (last_seq, design) = meta
            .map(|meta| (meta.last_seq, meta.design))
            .unwrap_or((0, String::new()));
        Ok(ViewIndex {
            db,
            last_seq,
            design,
            cached: None,
        })
    }

    fn load_rows(&mut self) -> Result<Vec<Row>, String> {
        // Keys are `r/<base64(docid)>/<ordinal>`; the id is recovered from the
        // key rather than duplicated in every stored row.
        let mut rows = Vec::new();
        for (key, bytes) in self.scan(b"r/")? {
            let Some(rest) = key.strip_prefix(b"r/") else {
                continue;
            };
            let Some(encoded) = rest.split(|byte| *byte == b'/').next() else {
                continue;
            };
            let Ok(id) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(encoded) else {
                continue;
            };
            let Ok(id) = String::from_utf8(id) else {
                continue;
            };
            let Ok(stored) = serde_json::from_slice::<StoredRow>(&bytes) else {
                continue;
            };
            rows.push(Row {
                id,
                key: stored.key,
                value: stored.value,
            });
        }
        Ok(rows)
    }

    /// Sorted rows for the current index state. The first query after a
    /// rebuild or incremental update pays for loading/sorting; later queries
    /// reuse the same immutable snapshot.
    fn sorted_rows(&mut self) -> Result<Arc<Vec<Row>>, String> {
        if let Some(rows) = &self.cached {
            return Ok(rows.clone());
        }
        let mut rows = self.load_rows()?;
        sort_rows(&mut rows);
        let rows = Arc::new(rows);
        self.cached = Some(rows.clone());
        Ok(rows)
    }

    fn scan(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let mut iterator = self.db.new_iter().map_err(|error| error.to_string())?;
        iterator.seek(prefix);
        let mut out = Vec::new();
        while let Some((key, value)) = iterator.current() {
            if !key.starts_with(prefix) {
                break;
            }
            out.push((key.to_vec(), value.to_vec()));
            if !iterator.advance() {
                break;
            }
        }
        Ok(out)
    }

    fn doc_prefix(id: &str) -> Vec<u8> {
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(id.as_bytes());
        format!("r/{encoded}/").into_bytes()
    }

    fn apply(
        &mut self,
        changed: Vec<String>,
        by_doc: HashMap<String, Vec<Row>>,
        last_seq: u64,
        design: &str,
    ) -> Result<(), String> {
        let mut batch = WriteBatch::default();
        for id in &changed {
            for (key, _) in self.scan(&Self::doc_prefix(id))? {
                batch.delete(&key);
            }
        }

        let mut added = Vec::new();
        for (id, rows) in &by_doc {
            let prefix = Self::doc_prefix(id);
            for (ordinal, row) in rows.iter().enumerate() {
                let key = format!("{}{ordinal:08}", String::from_utf8_lossy(&prefix));
                batch.put(
                    key.as_bytes(),
                    &serde_json::to_vec(&StoredRowRef {
                        key: &row.key,
                        value: &row.value,
                    })
                    .map_err(|error| error.to_string())?,
                );
                added.push(row.clone());
            }
        }

        // Update the in-memory snapshot incrementally: drop rows for changed
        // documents and merge in the new ones. Both runs are sorted, so this is
        // linear rather than a full reload.
        let next_cached = match self.cached.take() {
            Some(cached) => {
                let changed: HashSet<&str> = changed.iter().map(String::as_str).collect();
                // Reuse the cached allocation when no in-flight query still
                // holds the snapshot; otherwise clone it.
                let mut kept = Arc::try_unwrap(cached).unwrap_or_else(|cached| (*cached).clone());
                kept.retain(|row| !changed.contains(row.id.as_str()));
                sort_rows(&mut added);
                Some(Arc::new(merge_sorted_rows(kept, added)))
            }
            None => None,
        };

        self.last_seq = last_seq;
        self.design = design.to_string();
        batch.put(
            b"m",
            &serde_json::to_vec(&IndexMeta {
                last_seq,
                design: design.to_string(),
            })
            .map_err(|error| error.to_string())?,
        );
        self.db
            .write(batch, true)
            .map_err(|error| error.to_string())?;
        self.cached = next_cached;
        Ok(())
    }

    fn rebuild(&mut self, mut rows: Vec<Row>, last_seq: u64, design: &str) -> Result<(), String> {
        sort_rows(&mut rows);
        let mut batch = WriteBatch::default();
        for (key, _) in self.scan(b"r/")? {
            batch.delete(&key);
        }
        {
            let mut by_doc: HashMap<&str, Vec<&Row>> = HashMap::new();
            for row in &rows {
                by_doc.entry(row.id.as_str()).or_default().push(row);
            }
            for (id, doc_rows) in by_doc {
                let prefix = Self::doc_prefix(id);
                for (ordinal, row) in doc_rows.into_iter().enumerate() {
                    let key = format!("{}{ordinal:08}", String::from_utf8_lossy(&prefix));
                    batch.put(
                        key.as_bytes(),
                        &serde_json::to_vec(&StoredRowRef {
                            key: &row.key,
                            value: &row.value,
                        })
                        .map_err(|error| error.to_string())?,
                    );
                }
            }
        }
        self.last_seq = last_seq;
        self.design = design.to_string();
        batch.put(
            b"m",
            &serde_json::to_vec(&IndexMeta {
                last_seq,
                design: design.to_string(),
            })
            .map_err(|error| error.to_string())?,
        );
        self.db
            .write(batch, true)
            .map_err(|error| error.to_string())?;
        self.cached = Some(Arc::new(rows));
        Ok(())
    }
}

/// Run one view query against the local shards.
pub async fn query(db: &Db, ddoc_id: &str, view_name: &str, query: &ViewQuery) -> DbResult<Value> {
    let spec = load_spec(db, ddoc_id, view_name).await?;
    query_with_spec(db, ddoc_id, view_name, &spec, query).await
}

/// Same as `query`, for callers that already loaded the design document.
pub async fn query_with_spec(
    db: &Db,
    ddoc_id: &str,
    view_name: &str,
    spec: &ViewSpec,
    query: &ViewQuery,
) -> DbResult<Value> {
    validate_view_partition(db, spec, query.partition.as_deref())?;
    let rows = local_rows(
        db,
        ddoc_id,
        view_name,
        spec,
        query.limits,
        query.partition.as_deref(),
    )
    .await?;
    let reduce = query.reduce.unwrap_or(spec.reduce.is_some());
    let docs = if !reduce && (query.include_docs || query.attachments) {
        let ids = selected_row_ids(&rows, query);
        docs_by_ids(db, query.attachments, &ids).await?
    } else {
        HashMap::new()
    };
    finish(spec, rows, docs, query)
}

/// Partitioned design docs may only be queried through `_partition`, and
/// global design docs may only be queried globally.
pub fn validate_view_partition(db: &Db, spec: &ViewSpec, partition: Option<&str>) -> DbResult<()> {
    if !db.partitioned {
        if partition.is_some() {
            return Err(ApiError::bad_request(
                "`partition` parameter is not supported on this db",
            ));
        }
        return Ok(());
    }
    match (spec.partitioned, partition) {
        (true, None) => Err(ApiError::bad_request(
            "`partition` parameter is mandatory for queries to this view.",
        )),
        (true, Some(partition)) => crate::db::validate_partition(partition),
        (false, None) => Ok(()),
        (false, Some(_)) => Err(ApiError::bad_request(
            "`partition` parameter is not supported in this design doc",
        )),
    }
}

/// Local map rows, filtered to a partition when requested.
pub async fn local_rows(
    db: &Db,
    ddoc_id: &str,
    view_name: &str,
    spec: &ViewSpec,
    limits: ViewLimits,
    partition: Option<&str>,
) -> DbResult<Arc<Vec<Row>>> {
    let rows = ensure_rows(db, ddoc_id, view_name, spec, limits).await?;
    let rows = match partition {
        Some(partition) => {
            let prefix = format!("{partition}:");
            Arc::new(
                rows.iter()
                    .filter(|row| row.id.starts_with(&prefix))
                    .cloned()
                    .collect(),
            )
        }
        None => rows,
    };
    Ok(rows)
}

pub fn finish(
    spec: &ViewSpec,
    rows: Arc<Vec<Row>>,
    docs: HashMap<String, Value>,
    query: &ViewQuery,
) -> DbResult<Value> {
    render(spec, &rows, &docs, query)
}

async fn ensure_rows(
    db: &Db,
    ddoc_id: &str,
    view_name: &str,
    spec: &ViewSpec,
    limits: ViewLimits,
) -> DbResult<Arc<Vec<Row>>> {
    let current_seq = db.update_seq();
    let design = if NativeMap::parse(&spec.map).is_some() {
        format!(
            "{}|{}|native={NATIVE_MAP_VERSION}",
            spec.map,
            spec.reduce.as_deref().unwrap_or_default()
        )
    } else {
        format!(
            "{}|{}",
            spec.map,
            spec.reduce.as_deref().unwrap_or_default()
        )
    };
    let handle = index_handle(&index_path(db, ddoc_id, view_name))?;
    let mut index = handle.lock().await;

    if index.design != design || index.last_seq > current_seq {
        let rows = map_all(db, &spec.map, limits).await?;
        index
            .rebuild(rows, current_seq, &design)
            .map_err(|error| ApiError::new(500, "view_index_error", error))?;
        return index
            .sorted_rows()
            .map_err(|error| ApiError::new(500, "view_index_error", error));
    }

    if index.last_seq < current_seq {
        let changes = db
            .changes(&ChangesOpts {
                since: index.last_seq,
                include_docs: true,
                ..Default::default()
            })
            .await?;
        let mut changed = Vec::new();
        let mut docs = Vec::new();
        for row in changes["results"].as_array().cloned().unwrap_or_default() {
            let Some(id) = row.get("id").and_then(Value::as_str).map(str::to_string) else {
                continue;
            };
            changed.push(id.clone());
            if id.starts_with("_design/")
                || id.starts_with("_local/")
                || row.get("deleted").and_then(Value::as_bool).unwrap_or(false)
            {
                continue;
            }
            if let Some(doc) = row.get("doc").cloned() {
                docs.push((id, doc));
            }
        }
        let rows = run_map(&spec.map, &docs, limits).map_err(|reason| {
            ApiError::new(500, "view_error", format!("map function failed: {reason}"))
        })?;
        let mut by_doc: HashMap<String, Vec<Row>> = HashMap::new();
        for row in rows {
            by_doc.entry(row.id.clone()).or_default().push(row);
        }
        let last_seq = changes["last_seq"]
            .as_u64()
            .unwrap_or(current_seq)
            .max(current_seq);
        index
            .apply(changed, by_doc, last_seq, &design)
            .map_err(|error| ApiError::new(500, "view_index_error", error))?;
    }

    index
        .sorted_rows()
        .map_err(|error| ApiError::new(500, "view_index_error", error))
}

async fn map_all(db: &Db, map_source: &str, limits: ViewLimits) -> DbResult<Vec<Row>> {
    let docs = db.map_docs(None).await?;
    run_map_json(map_source, &docs, limits).map_err(|reason| {
        ApiError::new(500, "view_error", format!("map function failed: {reason}"))
    })
}

pub(crate) async fn docs_by_ids(
    db: &Db,
    attachments: bool,
    ids: &[String],
) -> DbResult<HashMap<String, Value>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let opts = AllDocsOpts {
        keys: Some(ids.to_vec()),
        include_docs: true,
        attachments,
        limit: usize::MAX,
        ..Default::default()
    };
    let all = db.all_docs(&opts).await?;
    let mut docs = HashMap::new();
    for row in all["rows"].as_array().cloned().unwrap_or_default() {
        let Some(id) = row.get("id").and_then(Value::as_str).map(str::to_string) else {
            continue;
        };
        if id.starts_with("_design/") || id.starts_with("_local/") {
            continue;
        }
        if let Some(doc) = row.get("doc").cloned() {
            docs.insert(id, doc);
        }
    }
    Ok(docs)
}

/// Drop every index for a database. Used by `_purge`, which can remove a
/// document without advancing the database sequence.
pub async fn invalidate(db: &Db) -> DbResult<()> {
    let directory = db.path.join("_view_indexes");
    registry_lock().retain(|path, _| !path.starts_with(&directory));
    if let Ok(entries) = std::fs::read_dir(&directory) {
        for entry in entries.flatten() {
            let path = entry.path();
            if entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
                let _ = std::fs::remove_dir_all(&path);
            }
        }
    }
    Ok(())
}

/// Remove index directories whose design document or view no longer exists.
pub async fn cleanup(db: &Db) -> DbResult<()> {
    let mut live = std::collections::HashSet::new();
    let all = db
        .all_docs(&AllDocsOpts {
            include_docs: true,
            startkey: Some("_design/".to_string()),
            endkey: Some(format!("_design/{}", char::MAX)),
            limit: usize::MAX,
            ..Default::default()
        })
        .await?;
    for row in all["rows"].as_array().cloned().unwrap_or_default() {
        let Some(ddoc) = row.get("doc") else {
            continue;
        };
        let Some(id) = ddoc.get("_id").and_then(Value::as_str) else {
            continue;
        };
        if let Some(views) = ddoc.get("views").and_then(Value::as_object) {
            for name in views.keys() {
                live.insert(index_path(db, id, name));
            }
        }
    }

    let directory = db.path.join("_view_indexes");
    if let Ok(entries) = std::fs::read_dir(&directory) {
        for entry in entries.flatten() {
            let path = entry.path();
            if entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) && !live.contains(&path)
            {
                let _ = std::fs::remove_dir_all(&path);
            }
        }
    }
    registry_lock().retain(|path, _| live.contains(path));
    Ok(())
}

pub async fn load_spec(db: &Db, ddoc_id: &str, view_name: &str) -> DbResult<ViewSpec> {
    let ddoc = db.get_doc(ddoc_id, &DocOpts::default()).await?;
    spec_from_doc(&ddoc, view_name)
}

pub fn spec_from_doc(ddoc: &Value, view_name: &str) -> DbResult<ViewSpec> {
    let language = ddoc
        .get("language")
        .and_then(Value::as_str)
        .unwrap_or("javascript");
    if language != "javascript" {
        return Err(ApiError::bad_request(format!(
            "View language must be javascript, not {language}"
        )));
    }
    let spec = ddoc
        .get("views")
        .and_then(Value::as_object)
        .and_then(|views| views.get(view_name))
        .ok_or_else(|| ApiError::not_found(format!("missing view {view_name}")))?;
    let map = spec
        .get("map")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::bad_request("View is missing a map function"))?
        .to_string();
    let reduce = spec
        .get("reduce")
        .and_then(Value::as_str)
        .map(str::to_string);
    let partitioned = ddoc
        .get("options")
        .and_then(Value::as_object)
        .and_then(|options| options.get("partitioned"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    Ok(ViewSpec {
        map,
        reduce,
        partitioned,
    })
}

const NATIVE_MAP_VERSION: u8 = 1;

/// A map function reduced to direct field/literal extraction. Anything that
/// does not match this deliberately tiny grammar falls back to QuickJS.
#[derive(Clone, Debug)]
struct NativeMap {
    key: EmitExpr,
    value: EmitExpr,
}

#[derive(Clone, Debug)]
enum EmitExpr {
    Field(String),
    Literal(Value),
}

fn raw_json_field<'a>(json: &'a str, field: &str) -> Result<Option<&'a str>, ()> {
    let bytes = json.as_bytes();
    let mut index = skip_ws(bytes, 0);
    if bytes.get(index) != Some(&b'{') {
        return Err(());
    }
    index += 1;
    let mut found = None;
    loop {
        index = skip_ws(bytes, index);
        match bytes.get(index) {
            Some(b'}') => return Ok(found),
            Some(b'"') => {
                let key_end = scan_json_string(bytes, index).ok_or(())?;
                let key: String = serde_json::from_str(&json[index..key_end]).map_err(|_| ())?;
                index = skip_ws(bytes, key_end);
                if bytes.get(index) != Some(&b':') {
                    return Err(());
                }
                index = skip_ws(bytes, index + 1);
                let value_start = index;
                let value_end = skip_json_value(bytes, index).ok_or(())?;
                if key == field {
                    found = Some(json[value_start..value_end].trim());
                }
                index = skip_ws(bytes, value_end);
                match bytes.get(index) {
                    Some(b',') => index += 1,
                    Some(b'}') => return Ok(found),
                    _ => return Err(()),
                }
            }
            _ => return Err(()),
        }
    }
}

fn skip_ws(bytes: &[u8], mut index: usize) -> usize {
    while matches!(bytes.get(index), Some(b' ' | b'\n' | b'\r' | b'\t')) {
        index += 1;
    }
    index
}

fn scan_json_string(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'"') {
        return None;
    }
    let mut index = start + 1;
    while let Some(byte) = bytes.get(index) {
        match byte {
            b'\\' => index += 2,
            b'"' => return Some(index + 1),
            _ => index += 1,
        }
    }
    None
}

fn skip_json_value(bytes: &[u8], start: usize) -> Option<usize> {
    match bytes.get(start)? {
        b'"' => scan_json_string(bytes, start),
        b'{' | b'[' => {
            let mut depth = 0usize;
            let mut index = start;
            while let Some(byte) = bytes.get(index) {
                match byte {
                    b'"' => {
                        index = scan_json_string(bytes, index)?;
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth = depth.saturating_sub(1);
                        if depth == 0 {
                            return Some(index + 1);
                        }
                    }
                    _ => {}
                }
                index += 1;
            }
            None
        }
        _ => {
            let mut index = start;
            while let Some(byte) = bytes.get(index) {
                if matches!(byte, b',' | b'}' | b']' | b' ' | b'\n' | b'\r' | b'\t') {
                    break;
                }
                index += 1;
            }
            Some(index)
        }
    }
}

impl NativeMap {
    fn parse(source: &str) -> Option<NativeMap> {
        let compact = compact_js(source)?;
        let body = function_body(&compact)?;
        let body = body.strip_suffix(';').unwrap_or(body);
        let inner = body.strip_prefix("emit(")?.strip_suffix(')')?;
        let args = split_top_level(inner, ',');
        let (key, value) = match args.as_slice() {
            [key] => (parse_emit_expr(key)?, EmitExpr::Literal(Value::Null)),
            [key, value] => (parse_emit_expr(key)?, parse_emit_expr(value)?),
            _ => return None,
        };
        Some(NativeMap { key, value })
    }

    fn eval(&self, expression: &EmitExpr, doc: &Value) -> Value {
        match expression {
            EmitExpr::Field(field) => doc.get(field).cloned().unwrap_or(Value::Null),
            EmitExpr::Literal(value) => value.clone(),
        }
    }

    fn row(&self, id: &str, doc: &Value) -> Row {
        Row {
            id: id.to_string(),
            key: self.eval(&self.key, doc),
            value: self.eval(&self.value, doc),
        }
    }

    fn run_values(&self, docs: &[(String, Value)]) -> Vec<Row> {
        docs.iter().map(|(id, doc)| self.row(id, doc)).collect()
    }

    fn eval_raw(&self, expression: &EmitExpr, raw: &str) -> Result<Value, ()> {
        match expression {
            EmitExpr::Literal(value) => Ok(value.clone()),
            EmitExpr::Field(field) => match raw_json_field(raw, field)? {
                Some(value) => serde_json::from_str(value).map_err(|_| ()),
                None => Ok(Value::Null),
            },
        }
    }

    fn run_json(&self, docs: &[(String, String)]) -> Result<Vec<Row>, String> {
        let mut rows = Vec::with_capacity(docs.len());
        for (id, raw) in docs {
            match (
                self.eval_raw(&self.key, raw),
                self.eval_raw(&self.value, raw),
            ) {
                (Ok(key), Ok(value)) => rows.push(Row {
                    id: id.clone(),
                    key,
                    value,
                }),
                _ => {
                    let doc: Value =
                        serde_json::from_str(raw).map_err(|error| format!("{id}: {error}"))?;
                    rows.push(self.row(id, &doc));
                }
            }
        }
        Ok(rows)
    }
}

/// Remove JavaScript whitespace but leave quoted strings untouched.
fn compact_js(source: &str) -> Option<String> {
    let mut out = String::with_capacity(source.len());
    let mut chars = source.chars().peekable();
    let mut quote = None;
    while let Some(ch) = chars.next() {
        if let Some(q) = quote {
            out.push(ch);
            if ch == '\\' {
                if let Some(next) = chars.next() {
                    out.push(next);
                }
            } else if ch == q {
                quote = None;
            }
        } else if ch == '"' || ch == '\'' {
            quote = Some(ch);
            out.push(ch);
        } else if !ch.is_whitespace() {
            out.push(ch);
        }
    }
    if quote.is_some() {
        None
    } else {
        Some(out)
    }
}

fn function_body(compact: &str) -> Option<&str> {
    if let Some(rest) = compact.strip_prefix("function(") {
        let (args, tail) = rest.split_once(')')?;
        if args != "doc" {
            return None;
        }
        let body = tail.strip_prefix('{')?.strip_suffix('}')?;
        return Some(body);
    }
    if let Some(rest) = compact.strip_prefix("(doc)=>") {
        return Some(
            rest.strip_prefix('{')
                .and_then(|body| body.strip_suffix('}'))
                .unwrap_or(rest),
        );
    }
    if let Some(rest) = compact.strip_prefix("doc=>") {
        return Some(
            rest.strip_prefix('{')
                .and_then(|body| body.strip_suffix('}'))
                .unwrap_or(rest),
        );
    }
    None
}

/// Split on a separator at bracket depth zero, respecting strings.
fn split_top_level(input: &str, separator: char) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in input.char_indices() {
        if let Some(q) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == q {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '\'' => quote = Some(ch),
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ if ch == separator && depth == 0 => {
                out.push(&input[start..index]);
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    out.push(&input[start..]);
    out
}

fn parse_emit_expr(raw: &str) -> Option<EmitExpr> {
    if let Some(field) = raw.strip_prefix("doc.") {
        if is_js_ident(field) && !is_object_prototype_field(field) {
            return Some(EmitExpr::Field(field.to_string()));
        }
        return None;
    }
    if let Some(key) = raw.strip_prefix("doc[") {
        let key = key.strip_suffix(']')?;
        let key: String = serde_json::from_str(key).ok()?;
        return Some(EmitExpr::Field(key));
    }
    serde_json::from_str(raw).ok().map(EmitExpr::Literal)
}

/// `doc.<name>` can resolve through `Object.prototype` when the document has
/// no such own property. Leave those names to QuickJS so a missing field does
/// not silently become `null` instead of a function value.
fn is_object_prototype_field(value: &str) -> bool {
    matches!(
        value,
        "__proto__"
            | "constructor"
            | "prototype"
            | "toString"
            | "valueOf"
            | "hasOwnProperty"
            | "isPrototypeOf"
            | "propertyIsEnumerable"
            | "toLocaleString"
    )
}

fn is_js_ident(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first == '$' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch == '$' || ch.is_ascii_alphanumeric())
}

fn engine(limits: ViewLimits) -> Result<rquickjs::Runtime, String> {
    crate::js::runtime(limits.timeout, limits.memory)
}

fn run_map(source: &str, docs: &[(String, Value)], limits: ViewLimits) -> Result<Vec<Row>, String> {
    if let Some(native) = NativeMap::parse(source) {
        return Ok(native.run_values(docs));
    }
    let mut serialized = Vec::with_capacity(docs.len());
    for (id, doc) in docs {
        serialized.push((
            id.clone(),
            serde_json::to_string(doc).map_err(|error| format!("{id}: {error}"))?,
        ));
    }
    run_map_json(source, &serialized, limits)
}

/// Run the map function over batches of already-serialized documents. Batching
/// amortizes the QuickJS call/JSON boundary; callers that already hold raw
/// bodies (cold index builds) also avoid a Rust parse/encode round trip.
pub(crate) fn run_map_json(
    source: &str,
    docs: &[(String, String)],
    limits: ViewLimits,
) -> Result<Vec<Row>, String> {
    if docs.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(native) = NativeMap::parse(source) {
        return native.run_json(docs);
    }
    const MAX_DOCS: usize = 256;
    const MAX_BYTES: usize = 4 * 1024 * 1024;

    if crate::js::process_enabled() {
        let mut rows = Vec::new();
        let mut start = 0;
        while start < docs.len() {
            let mut end = start;
            let mut bytes = 2;
            while end < docs.len()
                && end - start < MAX_DOCS
                && (end == start || bytes + docs[end].1.len() + 1 <= MAX_BYTES)
            {
                bytes += docs[end].1.len() + 1;
                end += 1;
            }
            let value = crate::js::process_request(
                "map",
                json!({"source": source, "docs": &docs[start..end]}),
                limits,
            )?;
            let chunk: Vec<Row> =
                serde_json::from_value(value).map_err(|error| error.to_string())?;
            rows.extend(chunk);
            start = end;
        }
        return Ok(rows);
    }

    let runtime = engine(limits)?;
    let context = rquickjs::Context::full(&runtime).map_err(|error| error.to_string())?;

    crate::js::guard("map function", || {
        context.with(|ctx| {
            let wrapped = format!(
                r#"
            (function () {{
              function sum(values) {{
                var total = 0;
                for (var i = 0; i < values.length; i++) total += values[i];
                return total;
              }}
              function log(message) {{ }}
              function isArray(value) {{ return Array.isArray(value); }}
              function toJSON(value) {{ return JSON.stringify(value); }}
              var __out = [];
              var emit = function (key, value) {{
                __out.push([
                  key === undefined ? null : key,
                  value === undefined ? null : value
                ]);
              }};
              var __map = ({source});
              if (typeof __map !== "function") {{
                throw new Error("map must be a function");
              }}
              return function (json) {{
                var docs = JSON.parse(json);
                var all = [];
                for (var i = 0; i < docs.length; i++) {{
                  __out = [];
                  __map(docs[i]);
                  all.push(__out);
                }}
                return JSON.stringify(all);
              }};
            }})()
            "#
            );
            let func: rquickjs::Function = ctx
                .eval(wrapped)
                .map_err(|error| format!("compile: {error}"))?;

            let mut rows = Vec::new();
            let mut start = 0;
            while start < docs.len() {
                let mut end = start;
                let mut bytes = 2;
                while end < docs.len()
                    && end - start < MAX_DOCS
                    && (end == start || bytes + docs[end].1.len() + 1 <= MAX_BYTES)
                {
                    bytes += docs[end].1.len() + 1;
                    end += 1;
                }

                let mut batch = String::with_capacity(bytes);
                batch.push('[');
                for (index, (_, doc)) in docs[start..end].iter().enumerate() {
                    if index > 0 {
                        batch.push(',');
                    }
                    batch.push_str(doc);
                }
                batch.push(']');

                let first_id = &docs[start].0;
                let output: String = func
                    .call((batch,))
                    .map_err(|error| format!("{first_id}: {error}"))?;
                crate::js::check_output("map function", &output)?;
                let emitted: Vec<Vec<(Value, Value)>> = serde_json::from_str(&output)
                    .map_err(|error| format!("{first_id}: {error}"))?;
                if emitted.len() != end - start {
                    return Err(format!(
                        "{first_id}: map returned {} results for {} documents",
                        emitted.len(),
                        end - start
                    ));
                }
                for (offset, pairs) in emitted.into_iter().enumerate() {
                    let id = &docs[start + offset].0;
                    for (key, value) in pairs {
                        rows.push(Row {
                            id: id.clone(),
                            key,
                            value,
                        });
                    }
                }
                start = end;
            }
            Ok(rows)
        })
    })
}

fn ordered_rows<'a>(rows: &'a [Row], query: &ViewQuery) -> (Vec<&'a Row>, usize) {
    let mut total_rows = query.total_rows_override.unwrap_or(rows.len());
    let mut ordered: Vec<&Row> = if query.descending {
        rows.iter().rev().collect()
    } else {
        rows.iter().collect()
    };

    if let Some(partition) = &query.partition {
        let prefix = format!("{partition}:");
        ordered.retain(|row| row.id.starts_with(&prefix));
        total_rows = ordered.len();
    }

    if let Some(owned) = &query.owner_shards {
        ordered.retain(|row| {
            let shard = crate::db::shard_index_for(&row.id, query.shard_count, query.partitioned);
            owned.get(shard).copied().unwrap_or(false)
        });
        total_rows = ordered.len();
    }

    if let Some(keys) = &query.keys {
        let mut selected = Vec::new();
        for key in keys {
            for row in &ordered {
                if cmp_json(&row.key, key) == Ordering::Equal {
                    selected.push(*row);
                }
            }
        }
        ordered = selected;
    } else if let Some(key) = &query.key {
        ordered.retain(|row| cmp_json(&row.key, key) == Ordering::Equal);
    }
    (ordered, total_rows)
}

fn selected_row_ids(rows: &[Row], query: &ViewQuery) -> Vec<String> {
    let (mut ordered, _) = ordered_rows(rows, query);
    ordered.retain(|row| in_range(row, query));
    let skip = query.skip.min(ordered.len());
    ordered
        .into_iter()
        .skip(skip)
        .take(query.limit)
        .map(|row| row.id.clone())
        .collect()
}

fn render(
    spec: &ViewSpec,
    rows: &[Row],
    docs: &HashMap<String, Value>,
    query: &ViewQuery,
) -> DbResult<Value> {
    let (mut ordered, total_rows) = ordered_rows(rows, query);
    let before_start = count_before_start(&ordered, query);
    ordered.retain(|row| in_range(row, query));
    let reduce = query.reduce.unwrap_or(spec.reduce.is_some());

    if reduce {
        let Some(reducer) = &spec.reduce else {
            return Err(ApiError::new(
                400,
                "query_parse_error",
                "Reduce is invalid for map-only views",
            ));
        };
        let groups = group_rows(ordered, query);
        let mut out = Vec::new();
        for (key, members) in groups {
            let value = match reducer.as_str() {
                "_count" => Ok(json!(members.len())),
                "_sum" => reduce_sum(members.iter().map(|row| &row.value)),
                "_stats" => reduce_stats(members.iter().map(|row| &row.value)),
                "_approx_count_distinct" => {
                    reduce_approx_count_distinct(members.iter().map(|row| &row.value))
                }
                _ => {
                    let keys: Vec<Value> =
                        members.iter().map(|row| json!([row.id, row.key])).collect();
                    let values: Vec<Value> = members.iter().map(|row| row.value.clone()).collect();
                    reduce_value(reducer, &keys, &values, query.limits)
                }
            };
            let value = match value {
                Ok(value) => value,
                Err(reason) => {
                    return Err(ApiError::new(
                        500,
                        "view_error",
                        format!("reduce function failed: {reason}"),
                    ))
                }
            };
            out.push(json!({"key": key, "value": value}));
        }
        return Ok(json!({"rows": out}));
    }

    let skip = query.skip.min(ordered.len());
    let take = query.limit;
    let selected = ordered.into_iter().skip(skip).take(take);
    let rows: Vec<Value> = selected
        .map(|row| {
            let mut entry = json!({
                "id": row.id.clone(),
                "key": row.key.clone(),
                "value": row.value.clone(),
            });
            if query.include_docs {
                if let Some(doc) = docs.get(&row.id) {
                    entry["doc"] = doc.clone();
                }
            }
            entry
        })
        .collect();
    Ok(json!({
        "total_rows": total_rows,
        "offset": before_start + skip,
        "rows": rows,
    }))
}

fn count_before_start(rows: &[&Row], query: &ViewQuery) -> usize {
    let Some(start) = &query.startkey else {
        return 0;
    };
    rows.iter()
        .filter(|row| match cmp_json(&row.key, start) {
            Ordering::Less => !query.descending,
            Ordering::Greater => query.descending,
            Ordering::Equal => match &query.startkey_docid {
                Some(id) if query.descending => row.id > *id,
                Some(id) => row.id < *id,
                None => false,
            },
        })
        .count()
}

fn in_range(row: &Row, query: &ViewQuery) -> bool {
    if let Some(start) = &query.startkey {
        match cmp_json(&row.key, start) {
            Ordering::Less if !query.descending => return false,
            Ordering::Greater if query.descending => return false,
            Ordering::Equal => {
                if let Some(id) = &query.startkey_docid {
                    if query.descending {
                        if row.id > *id {
                            return false;
                        }
                    } else if row.id < *id {
                        return false;
                    }
                }
            }
            _ => {}
        }
    }
    if let Some(end) = &query.endkey {
        match cmp_json(&row.key, end) {
            Ordering::Greater if !query.descending => return false,
            Ordering::Less if query.descending => return false,
            Ordering::Equal => {
                if !query.inclusive_end {
                    return false;
                }
                if let Some(id) = &query.endkey_docid {
                    if query.descending {
                        if row.id < *id {
                            return false;
                        }
                    } else if row.id > *id {
                        return false;
                    }
                }
            }
            _ => {}
        }
    }
    true
}

fn group_rows<'a>(rows: Vec<&'a Row>, query: &ViewQuery) -> Vec<(Value, Vec<&'a Row>)> {
    if !query.group && query.group_level == 0 {
        return vec![(Value::Null, rows)];
    }
    let mut groups: Vec<(Value, Vec<&'a Row>)> = Vec::new();
    for row in rows {
        let key = group_key(&row.key, query.group_level);
        match groups.last_mut() {
            Some((existing, members)) if cmp_json(existing, &key) == Ordering::Equal => {
                members.push(row);
            }
            _ => groups.push((key, vec![row])),
        }
    }
    groups
}

fn group_key(key: &Value, level: usize) -> Value {
    if level == 0 {
        return key.clone();
    }
    match key {
        Value::Array(items) => Value::Array(items.iter().take(level).cloned().collect()),
        other => other.clone(),
    }
}

fn reduce_value(
    source: &str,
    keys: &[Value],
    values: &[Value],
    limits: ViewLimits,
) -> Result<Value, String> {
    match source {
        "_count" => return Ok(json!(values.len())),
        "_sum" => return reduce_sum(values.iter()),
        "_stats" => return reduce_stats(values.iter()),
        "_approx_count_distinct" => return reduce_approx_count_distinct(values.iter()),
        _ => {}
    }

    // CouchDB reduces in bounded batches and then rereduces the partials.
    // Do the same for custom JavaScript reducers.
    const CHUNK: usize = 1000;
    if values.len() > CHUNK {
        let mut partials = Vec::new();
        for (offset, chunk) in values.chunks(CHUNK).enumerate() {
            let start = offset * CHUNK;
            let end = (start + chunk.len()).min(keys.len());
            let chunk_keys = if start < end { &keys[start..end] } else { &[] };
            partials.push(run_reduce(source, chunk_keys, chunk, false, limits)?);
        }
        return run_reduce(source, &[], &partials, true, limits);
    }
    run_reduce(source, keys, values, false, limits)
}

/// Combine partial custom-reduce values with the reducer's rereduce mode.
pub(crate) fn recombine(
    source: &str,
    values: &[Value],
    limits: ViewLimits,
) -> Result<Value, String> {
    run_reduce(source, &[], values, true, limits)
}

pub(crate) fn run_reduce(
    source: &str,
    keys: &[Value],
    values: &[Value],
    rereduce: bool,
    limits: ViewLimits,
) -> Result<Value, String> {
    if crate::js::process_enabled() {
        return crate::js::process_request(
            "reduce",
            json!({"source": source, "keys": keys, "values": values, "rereduce": rereduce}),
            limits,
        );
    }
    let runtime = engine(limits)?;
    let context = rquickjs::Context::full(&runtime).map_err(|error| error.to_string())?;
    crate::js::guard("reduce function", || {
        context.with(|ctx| {
            let wrapped = format!(
                r#"
            (function () {{
              function sum(values) {{
                var total = 0;
                for (var i = 0; i < values.length; i++) total += values[i];
                return total;
              }}
              function log(message) {{ }}
              function isArray(value) {{ return Array.isArray(value); }}
              function toJSON(value) {{ return JSON.stringify(value); }}
              var __reduce = ({source});
              if (typeof __reduce !== "function") {{
                throw new Error("reduce must be a function");
              }}
              return function (keys, values, rereduce) {{
                var result = __reduce(JSON.parse(keys), JSON.parse(values), rereduce);
                return result === undefined ? "null" : JSON.stringify(result);
              }};
            }})()
            "#
            );
            let func: rquickjs::Function = ctx
                .eval(wrapped)
                .map_err(|error| format!("compile: {error}"))?;
            // CouchDB passes null, not [], for keys during rereduce.
            let keys = if rereduce {
                "null".to_string()
            } else {
                serde_json::to_string(keys).map_err(|error| error.to_string())?
            };
            let values = serde_json::to_string(values).map_err(|error| error.to_string())?;
            let output: String = func
                .call((keys, values, rereduce))
                .map_err(|error| error.to_string())?;
            crate::js::check_output("reduce function", &output)?;
            serde_json::from_str(&output).map_err(|error| error.to_string())
        })
    })
}

fn reduce_sum<'a>(values: impl IntoIterator<Item = &'a Value>) -> Result<Value, String> {
    let mut sum = 0.0;
    for value in values {
        let number = value
            .as_f64()
            .ok_or_else(|| format!("_sum requires numbers, got {value}"))?;
        sum += number;
    }
    Ok(number_value(sum))
}

fn reduce_stats<'a>(values: impl IntoIterator<Item = &'a Value>) -> Result<Value, String> {
    let mut count = 0usize;
    let mut sum = 0.0;
    let mut sumsqr = 0.0;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for value in values {
        let number = value
            .as_f64()
            .ok_or_else(|| format!("_stats requires numbers, got {value}"))?;
        count += 1;
        sum += number;
        sumsqr += number * number;
        min = min.min(number);
        max = max.max(number);
    }
    if count == 0 {
        return Ok(json!({
            "sum": 0, "count": 0, "min": null, "max": null, "sumsqr": 0
        }));
    }
    Ok(json!({
        "sum": number_value(sum),
        "count": count,
        "min": number_value(min),
        "max": number_value(max),
        "sumsqr": number_value(sumsqr),
    }))
}

fn reduce_approx_count_distinct<'a>(
    values: impl IntoIterator<Item = &'a Value>,
) -> Result<Value, String> {
    let mut distinct = HashSet::new();
    for value in values {
        distinct.insert(serde_json::to_string(value).map_err(|error| error.to_string())?);
    }
    Ok(json!(distinct.len()))
}

pub(crate) fn number_value(value: f64) -> Value {
    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        json!(value as i64)
    } else {
        json!(value)
    }
}

/// CouchDB JSON collation: null < false < true < number < string < array <
/// object. Strings use ICU's default (tertiary) collation, as CouchDB does.
pub fn cmp_json(a: &Value, b: &Value) -> Ordering {
    fn rank(value: &Value) -> u8 {
        match value {
            Value::Null => 0,
            Value::Bool(false) => 1,
            Value::Bool(true) => 2,
            Value::Number(_) => 3,
            Value::String(_) => 4,
            Value::Array(_) => 5,
            Value::Object(_) => 6,
        }
    }

    let (ra, rb) = (rank(a), rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Number(a), Value::Number(b)) => {
            let (a, b) = (a.as_f64().unwrap_or(0.0), b.as_f64().unwrap_or(0.0));
            a.partial_cmp(&b).unwrap_or(Ordering::Equal)
        }
        (Value::String(a), Value::String(b)) => compare_strings(a, b),
        (Value::Array(a), Value::Array(b)) => {
            for (left, right) in a.iter().zip(b.iter()) {
                let order = cmp_json(left, right);
                if order != Ordering::Equal {
                    return order;
                }
            }
            a.len().cmp(&b.len())
        }
        (Value::Object(a), Value::Object(b)) => {
            // CouchDB object collation follows the stored member order, not
            // lexicographic key order.
            let a: Vec<_> = a.iter().collect();
            let b: Vec<_> = b.iter().collect();
            for (left, right) in a.iter().zip(b.iter()) {
                let order = left.0.cmp(right.0).then_with(|| cmp_json(left.1, right.1));
                if order != Ordering::Equal {
                    return order;
                }
            }
            a.len().cmp(&b.len())
        }
        _ => Ordering::Equal,
    }
}

fn compare_strings(a: &str, b: &str) -> Ordering {
    static COLLATOR: OnceLock<Option<icu_collator::CollatorBorrowed<'static>>> = OnceLock::new();
    let collator = COLLATOR
        .get_or_init(|| Collator::try_new(Default::default(), CollatorOptions::default()).ok());
    match collator {
        Some(collator) => collator.compare(a, b),
        None => a.cmp(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn couchdb_collation_orders_every_json_type() {
        let ordered = vec![
            Value::Null,
            json!(false),
            json!(true),
            json!(-1),
            json!(2.5),
            json!("a"),
            json!(["a"]),
            json!({"a": 1}),
        ];
        for pair in ordered.windows(2) {
            assert_eq!(cmp_json(&pair[0], &pair[1]), Ordering::Less);
        }
    }

    #[test]
    fn strings_use_icu_collation() {
        // Codepoint order would put "B" before "a"; ICU puts "a" first.
        assert_eq!(cmp_json(&json!("a"), &json!("B")), Ordering::Less);
        // Accented letters sort next to their base letter, not after "z".
        assert_eq!(cmp_json(&json!("z"), &json!("ä")), Ordering::Greater);
        assert_eq!(cmp_json(&json!("ä"), &json!("b")), Ordering::Less);
    }

    #[test]
    fn native_map_matches_javascript_for_fields_and_literals() {
        let docs = vec![
            (
                "a".to_string(),
                json!({"_id": "a", "city": "Berlin", "n": 2}),
            ),
            ("b".to_string(), json!({"_id": "b", "n": 3})),
        ];
        let native = run_map(
            "function(doc){ emit(doc.city, doc.n); }",
            &docs,
            ViewLimits::default(),
        )
        .unwrap();
        let javascript = run_map(
            "function(doc){ var k = doc.city; var v = doc.n; emit(k, v); }",
            &docs,
            ViewLimits::default(),
        )
        .unwrap();
        assert_eq!(native.len(), javascript.len());
        for (left, right) in native.iter().zip(javascript.iter()) {
            assert_eq!(left.id, right.id);
            assert_eq!(left.key, right.key);
            assert_eq!(left.value, right.value);
        }

        let constant = run_map(
            "function(doc){ emit(doc.city, 1); }",
            &docs,
            ViewLimits::default(),
        )
        .unwrap();
        assert_eq!(constant[0].value, json!(1));
        assert_eq!(constant[1].key, Value::Null);
    }

    #[test]
    fn native_raw_json_field_extraction_matches_parsed_documents() {
        let docs = vec![
            (
                "a".to_string(),
                r#"{"_id":"a","_rev":"1-x","city":"Ber,lin","n":2,"nested":{"x":[1,2]},"escaped":"a\"b"}"#
                    .to_string(),
            ),
            (
                "b".to_string(),
                r#"{"_id":"b","_rev":"1-y","n":3,"other":null}"#.to_string(),
            ),
        ];
        let native = NativeMap::parse("function(doc){ emit(doc.city, doc.n); }")
            .unwrap()
            .run_json(&docs)
            .unwrap();
        let javascript = run_map_json(
            "function(doc){ var k=doc.city; var v=doc.n; emit(k, v); }",
            &docs,
            ViewLimits::default(),
        )
        .unwrap();
        assert_eq!(native.len(), javascript.len());
        for (left, right) in native.iter().zip(javascript.iter()) {
            assert_eq!(left.id, right.id);
            assert_eq!(left.key, right.key);
            assert_eq!(left.value, right.value);
        }
        assert_eq!(native[0].key, json!("Ber,lin"));
        assert_eq!(native[0].value, json!(2));
        assert_eq!(native[1].key, Value::Null);
        assert_eq!(native[1].value, json!(3));
    }

    #[test]
    fn native_map_supports_brackets_and_arrow_functions() {
        let docs = vec![("a".to_string(), json!({"strange-key": "v"}))];
        let rows = run_map(
            "(doc) => { emit(doc[\"strange-key\"], doc[\"strange-key\"]); }",
            &docs,
            ViewLimits::default(),
        )
        .unwrap();
        assert_eq!(rows[0].key, json!("v"));
        assert_eq!(rows[0].value, json!("v"));
    }

    #[test]
    fn native_map_rejects_more_complex_functions() {
        assert!(NativeMap::parse("function(doc){ if (doc.x) emit(doc.x); }").is_none());
        assert!(NativeMap::parse("function(doc){ emit(doc.x); emit(doc.y); }").is_none());
        assert!(NativeMap::parse("function(doc){ for (var i=0;i<2;i++) emit(i); }").is_none());
        assert!(NativeMap::parse("function(other){ emit(other.x); }").is_none());
        assert!(NativeMap::parse("function(doc){ emit(doc.toString); }").is_none());
    }

    #[test]
    fn map_emits_keys_and_values() {
        let docs = vec![
            (
                "a".to_string(),
                json!({"_id": "a", "city": "Berlin", "n": 2}),
            ),
            (
                "b".to_string(),
                json!({"_id": "b", "city": "Berlin", "n": 3}),
            ),
        ];
        let rows = run_map(
            "function(doc){ emit(doc.city, doc.n); }",
            &docs,
            ViewLimits::default(),
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key, json!("Berlin"));
        assert_eq!(rows[1].value, json!(3));
    }

    #[test]
    fn map_batches_large_document_sets() {
        let docs: Vec<(String, Value)> = (0..600)
            .map(|i| (format!("d{i}"), json!({"_id": format!("d{i}"), "n": i})))
            .collect();
        let rows = run_map(
            "function(doc){ emit(doc.n, doc.n); }",
            &docs,
            ViewLimits::default(),
        )
        .unwrap();
        assert_eq!(rows.len(), 600);
        assert_eq!(rows[300].id, "d300");
        assert_eq!(rows[300].value, json!(300));
    }

    #[test]
    fn index_cache_tracks_incremental_updates() {
        let dir = std::env::temp_dir().join(format!(
            "douchdb-view-cache-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut index = ViewIndex::open(&dir).unwrap();
        index
            .rebuild(
                vec![
                    Row {
                        id: "a".into(),
                        key: json!("a"),
                        value: json!(1),
                    },
                    Row {
                        id: "b".into(),
                        key: json!("b"),
                        value: json!(2),
                    },
                ],
                1,
                "map|",
            )
            .unwrap();
        assert_eq!(index.sorted_rows().unwrap().len(), 2);

        // Move a to a later key and delete b; the cached snapshot must follow.
        let mut by_doc = HashMap::new();
        by_doc.insert(
            "a".to_string(),
            vec![Row {
                id: "a".into(),
                key: json!("z"),
                value: json!(3),
            }],
        );
        index
            .apply(vec!["a".to_string(), "b".to_string()], by_doc, 2, "map|")
            .unwrap();
        let rows = index.sorted_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "a");
        assert_eq!(rows[0].key, json!("z"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn custom_reduce_rereduces_large_groups() {
        let values: Vec<Value> = (0..2500).map(|i| json!(i)).collect();
        let value = reduce_value(
            "function(keys, values, rereduce){ return (rereduce ? 1000 : 0) + values.length; }",
            &[],
            &values,
            ViewLimits::default(),
        )
        .unwrap();
        // 1000 + 1000 + 500 in three batches, then rereduce over three partials.
        assert_eq!(value, json!(1003));
    }

    #[test]
    fn an_infinite_map_is_interrupted() {
        let docs = vec![("a".to_string(), json!({}))];
        let result = run_map(
            "function (doc) { while (true) {} }",
            &docs,
            ViewLimits {
                timeout: Duration::from_millis(50),
                memory: 16 * 1024 * 1024,
            },
        );
        assert!(result.is_err());
    }

    #[test]
    fn index_rows_survive_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "douchdb-view-index-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let rows = vec![
            Row {
                id: "a".into(),
                key: json!("Berlin"),
                value: json!(1),
            },
            Row {
                id: "b".into(),
                key: json!("Paris"),
                value: json!(2),
            },
        ];
        {
            let mut index = ViewIndex::open(&dir).unwrap();
            index.rebuild(rows, 7, "map|").unwrap();
        }
        {
            let mut index = ViewIndex::open(&dir).unwrap();
            assert_eq!(index.last_seq, 7);
            assert_eq!(index.design, "map|");
            let mut restored = index.load_rows().unwrap();
            restored.sort_by(|a, b| a.id.cmp(&b.id));
            assert_eq!(restored.len(), 2);
            assert_eq!(restored[0].id, "a");
            assert_eq!(restored[1].key, json!("Paris"));
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn builtin_reduces_work() {
        assert_eq!(
            reduce_value("_count", &[], &[json!(1), json!(2)], ViewLimits::default()).unwrap(),
            json!(2)
        );
        assert_eq!(
            reduce_value("_sum", &[], &[json!(1), json!(2.5)], ViewLimits::default()).unwrap(),
            json!(3.5)
        );
        let stats =
            reduce_value("_stats", &[], &[json!(1), json!(3)], ViewLimits::default()).unwrap();
        assert_eq!(stats["count"], 2);
        assert_eq!(stats["min"], 1);
        assert_eq!(stats["max"], 3);
        assert_eq!(
            reduce_value(
                "_approx_count_distinct",
                &[],
                &[json!(1), json!(2), json!(1)],
                ViewLimits::default()
            )
            .unwrap(),
            json!(2)
        );
    }
}

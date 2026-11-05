//! Tantivy-backed CouchDB search indexes.

use crate::db::{hex, ApiError, Db, DbResult};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;
use tantivy::collector::DocSetCollector;
use tantivy::query::{AllQuery, QueryParser};
use tantivy::schema::{
    Field, FieldType, OwnedValue, Schema, TantivyDocument, FAST, INDEXED, STORED, STRING, TEXT,
};
use tantivy::{Index, Term};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedRwLockReadGuard, RwLock};

const SEARCH_VERSION: u8 = 2;
const PAGE_LIMIT: usize = 2000;

#[derive(Clone, Debug)]
pub struct SearchSpec {
    pub source: String,
    pub analyzer: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SearchDoc {
    id: String,
    fields: Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SearchMeta {
    version: u8,
    last_seq: u64,
    design: String,
    #[serde(default)]
    ddoc: String,
    #[serde(default)]
    name: String,
    docs: Vec<SearchDoc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IndexedDoc {
    id: String,
    fields: Map<String, Value>,
}

#[derive(Clone, Debug)]
pub struct SearchRequest {
    pub query: String,
    pub limit: usize,
    pub bookmark: Option<String>,
    pub include_docs: bool,
    pub counts: Vec<String>,
    pub ranges: Map<String, Value>,
    pub group_field: Option<String>,
    pub drilldown: Vec<Vec<(String, Vec<Value>)>>,
    pub partition: Option<String>,
    /// Internal: only return documents whose shard is marked here. Cluster
    /// coordinators use this to split a search into disjoint owner sets.
    pub owner_shards: Option<Vec<bool>>,
    pub sort: Vec<(String, bool)>,
    pub highlight_fields: Vec<String>,
    pub highlight_pre: String,
    pub highlight_post: String,
    pub highlight_size: usize,
    pub highlight_number: usize,
    pub timeout_ms: u64,
}

impl Default for SearchRequest {
    fn default() -> Self {
        SearchRequest {
            query: "*:*".to_string(),
            limit: 25,
            bookmark: None,
            include_docs: false,
            counts: Vec::new(),
            ranges: Map::new(),
            group_field: None,
            drilldown: Vec::new(),
            partition: None,
            owner_shards: None,
            sort: Vec::new(),
            highlight_fields: Vec::new(),
            highlight_pre: "<em>".to_string(),
            highlight_post: "</em>".to_string(),
            highlight_size: 100,
            highlight_number: 1,
            timeout_ms: 30_000,
        }
    }
}

fn index_dir(db: &Db, ddoc: &str, index: &str) -> PathBuf {
    let digest = Md5::digest(format!("{ddoc}\0{index}").as_bytes());
    db.path.join("_search_indexes").join(hex(&digest))
}

fn design_signature(spec: &SearchSpec) -> String {
    let digest =
        Md5::digest(format!("{SEARCH_VERSION}\0{}\0{}", spec.analyzer, spec.source).as_bytes());
    hex(&digest)
}

pub async fn load_spec(db: &Db, ddoc_id: &str, index_name: &str) -> DbResult<SearchSpec> {
    let ddoc = db.get_doc(ddoc_id, &crate::db::DocOpts::default()).await?;
    let entry = ddoc
        .get("indexes")
        .and_then(Value::as_object)
        .and_then(|indexes| indexes.get(index_name))
        .ok_or_else(|| ApiError::not_found(format!("missing search index {index_name}")))?;
    let source = entry
        .get("index")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::bad_request("Search index is missing an index function"))?
        .to_string();
    let analyzer = entry
        .get("analyzer")
        .and_then(|analyzer| {
            analyzer
                .get("name")
                .and_then(Value::as_str)
                .or_else(|| analyzer.as_str())
        })
        .unwrap_or("standard")
        .to_string();
    Ok(SearchSpec { source, analyzer })
}

fn engine(limits: crate::view::ViewLimits) -> Result<rquickjs::Runtime, String> {
    crate::js::runtime(limits.timeout, limits.memory)
}

pub(crate) fn run_index_json(
    source: &str,
    docs: &[(String, String)],
    limits: crate::view::ViewLimits,
) -> Result<Vec<IndexedDoc>, String> {
    if docs.is_empty() {
        return Ok(Vec::new());
    }
    if crate::js::sandboxed() {
        let mut out = Vec::with_capacity(docs.len());
        for chunk in docs.chunks(2048) {
            let value = crate::js::sandbox_request(
                "index",
                json!({"source": source, "docs": chunk}),
                limits,
            )?;
            let mut chunk: Vec<IndexedDoc> =
                serde_json::from_value(value).map_err(|error| error.to_string())?;
            out.append(&mut chunk);
        }
        return Ok(out);
    }
    let runtime = engine(limits)?;
    let context = rquickjs::Context::full(&runtime).map_err(|error| error.to_string())?;
    crate::js::guard("index function", || {
        context.with(|ctx| {
            let wrapped = format!(
                r#"
            (function () {{
              var __out = [];
              function log(message) {{ }}
              var index = function (name, value, options) {{
                if (value === undefined || value === null) return;
                __out.push([String(name), value]);
              }};
              var __index = ({source});
              if (typeof __index !== "function") {{
                throw new Error("index must be a function");
              }}
              return function (json) {{
                var docs = JSON.parse(json);
                var all = [];
                for (var i = 0; i < docs.length; i++) {{
                  __out = [];
                  __index(docs[i]);
                  all.push([docs[i]._id, __out]);
                }}
                return JSON.stringify(all);
              }};
            }})()
            "#
            );
            let func: rquickjs::Function = ctx
                .eval(wrapped)
                .map_err(|error| format!("compile: {error}"))?;
            let mut out = Vec::with_capacity(docs.len());
            const BATCH: usize = 128;
            for chunk in docs.chunks(BATCH) {
                let mut batch = String::from("[");
                for (index, (_, raw)) in chunk.iter().enumerate() {
                    if index > 0 {
                        batch.push(',');
                    }
                    batch.push_str(raw);
                }
                batch.push(']');
                let output: String = func
                    .call((batch,))
                    .map_err(|error| format!("{}: {error}", chunk[0].0))?;
                crate::js::check_output("index function", &output)?;
                let emitted: Vec<(String, Vec<(String, Value)>)> =
                    serde_json::from_str(&output).map_err(|error| error.to_string())?;
                for (id, fields) in emitted {
                    let mut map = Map::new();
                    for (name, value) in fields {
                        map.insert(name, value);
                    }
                    out.push(IndexedDoc { id, fields: map });
                }
            }
            Ok(out)
        })
    })
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum FieldKind {
    Text,
    Number,
    Bool,
}

fn infer_kinds<'a>(
    docs: impl IntoIterator<Item = &'a Map<String, Value>>,
) -> BTreeMap<String, FieldKind> {
    let mut kinds: BTreeMap<String, FieldKind> = BTreeMap::new();
    for fields in docs {
        for (name, value) in fields {
            let kind = match value {
                Value::Number(_) => FieldKind::Number,
                Value::Bool(_) => FieldKind::Bool,
                _ => FieldKind::Text,
            };
            kinds
                .entry(name.clone())
                .and_modify(|existing| {
                    let current = *existing;
                    if !matches!(
                        (current, kind),
                        (FieldKind::Text, _)
                            | (_, FieldKind::Text)
                            | (FieldKind::Number, FieldKind::Number)
                            | (FieldKind::Bool, FieldKind::Bool)
                    ) {
                        *existing = FieldKind::Text;
                    }
                })
                .or_insert(kind);
        }
    }
    kinds
}

fn write_meta(path: &Path, meta: &SearchMeta) -> DbResult<()> {
    std::fs::write(
        path.join("douchdb-search-meta.json"),
        serde_json::to_vec(meta)
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?,
    )
    .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))
}

fn read_meta(path: &Path) -> Option<SearchMeta> {
    std::fs::read(path.join("douchdb-search-meta.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
}

async fn build_index(
    db: &Db,
    ddoc: &str,
    index_name: &str,
    spec: &SearchSpec,
    limits: crate::view::ViewLimits,
    gate: OwnedRwLockReadGuard<()>,
    index_guard: OwnedMutexGuard<()>,
) -> DbResult<(PathBuf, SearchMeta, OwnedRwLockReadGuard<()>)> {
    // Capture the sequence before reading the snapshot. A write that lands
    // while the index is being built then leaves the database ahead of the
    // recorded sequence, so the next query rebuilds instead of trusting a
    // snapshot that may have missed it.
    let last_seq = db.update_seq();
    let docs = db.map_docs(None).await?;
    let path = index_dir(db, ddoc, index_name);
    let source = spec.source.clone();
    // Everything but the documents is known before the build starts.
    let meta = SearchMeta {
        version: SEARCH_VERSION,
        last_seq,
        design: design_signature(spec),
        ddoc: ddoc.to_string(),
        name: index_name.to_string(),
        docs: Vec::new(),
    };
    // QuickJS/Tantivy work is CPU- and IO-bound. Run it on the blocking pool so
    // a search rebuild cannot occupy a Tokio async worker for its full duration.
    let (result, gate) = tokio::task::spawn_blocking(move || {
        // Keep both permits in the blocking task, not the request future. A
        // cancelled request must not let cleanup race a live rebuild. The read
        // permit is returned so a query can keep it for its whole lifetime.
        let _index_guard = index_guard;
        let result = build_index_files(path, docs, &source, meta, limits);
        (result, gate)
    })
    .await
    .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    let (path, meta) = result?;
    Ok((path, meta, gate))
}

/// Build the index at `path` and write `meta` with the indexed documents.
fn build_index_files(
    path: PathBuf,
    docs: Vec<(String, String)>,
    source: &str,
    mut meta: SearchMeta,
    limits: crate::view::ViewLimits,
) -> DbResult<(PathBuf, SearchMeta)> {
    let indexed = run_index_json(source, &docs, limits).map_err(|reason| {
        ApiError::new(
            500,
            "search_index_error",
            format!("index function failed: {reason}"),
        )
    })?;
    let kinds = infer_kinds(indexed.iter().map(|doc| &doc.fields));

    if path.exists() {
        std::fs::remove_dir_all(&path)
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    }
    std::fs::create_dir_all(&path)
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;

    let mut builder = Schema::builder();
    let id_field = builder.add_text_field("_id", STRING | STORED | FAST);
    let mut fields: HashMap<String, (Field, FieldKind)> = HashMap::new();
    for (name, kind) in &kinds {
        let field = match kind {
            FieldKind::Text => builder.add_text_field(name, TEXT),
            FieldKind::Number => builder.add_f64_field(name, INDEXED | STORED),
            FieldKind::Bool => builder.add_bool_field(name, INDEXED | STORED),
        };
        fields.insert(name.clone(), (field, *kind));
    }
    let schema = builder.build();
    let tantivy_index = Index::create_in_dir(&path, schema.clone())
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    let writer = tantivy_index
        .writer(50_000_000)
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;

    for doc in &indexed {
        writer
            .add_document(tantivy_doc(&doc.id, &doc.fields, id_field, &fields))
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    }
    finish_writer(writer)?;

    meta.docs = indexed
        .into_iter()
        .map(|doc| SearchDoc {
            id: doc.id,
            fields: doc.fields,
        })
        .collect();
    write_meta(&path, &meta)?;
    Ok((path, meta))
}

fn tantivy_doc(
    id: &str,
    values: &Map<String, Value>,
    id_field: Field,
    fields: &HashMap<String, (Field, FieldKind)>,
) -> TantivyDocument {
    let mut tantivy_doc = TantivyDocument::default();
    tantivy_doc.add_text(id_field, id);
    for (name, value) in values {
        let Some((field, kind)) = fields.get(name) else {
            continue;
        };
        match kind {
            FieldKind::Text => {
                let text = match value {
                    Value::String(text) => text.clone(),
                    other => serde_json::to_string(other).unwrap_or_default(),
                };
                tantivy_doc.add_text(*field, text);
            }
            FieldKind::Number => {
                if let Some(number) = value.as_f64() {
                    tantivy_doc.add_f64(*field, number);
                }
            }
            FieldKind::Bool => {
                if let Some(value) = value.as_bool() {
                    tantivy_doc.add_bool(*field, value);
                }
            }
        }
    }
    tantivy_doc
}

fn finish_writer(mut writer: tantivy::IndexWriter) -> DbResult<()> {
    writer
        .commit()
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    // Dropping the writer leaves its merge threads running, and they can still
    // create and delete segment files after the build gate is released. A
    // concurrent `_search_cleanup` then fails with "Directory not empty".
    writer
        .wait_merging_threads()
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))
}

/// The result of bringing an index up to date from its changes.
enum Update {
    Done(SearchMeta, OwnedRwLockReadGuard<()>),
    /// Field types inferred over the updated documents differ from the
    /// index's schema, which only a rebuild can change.
    Rebuild(OwnedRwLockReadGuard<()>, OwnedMutexGuard<()>),
}

/// Bring the index at `path` up to date with the changes since
/// `meta.last_seq`, re-indexing only the documents that changed.
async fn update_index(
    db: &Db,
    path: PathBuf,
    mut meta: SearchMeta,
    spec: &SearchSpec,
    limits: crate::view::ViewLimits,
    gate: OwnedRwLockReadGuard<()>,
    index_guard: OwnedMutexGuard<()>,
) -> DbResult<Update> {
    let changes = db
        .changes(&crate::db::ChangesOpts {
            since: meta.last_seq,
            include_docs: true,
            ..Default::default()
        })
        .await?;
    let mut changed = std::collections::HashSet::new();
    let mut live = Vec::new();
    for row in changes["results"].as_array().into_iter().flatten() {
        let Some(id) = row.get("id").and_then(Value::as_str) else {
            continue;
        };
        if id.starts_with("_design/") || id.starts_with("_local/") {
            continue;
        }
        changed.insert(id.to_string());
        if row.get("deleted").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        if let Some(doc) = row.get("doc") {
            live.push((id.to_string(), doc.to_string()));
        }
    }
    let last_seq = changes["last_seq"]
        .as_u64()
        .unwrap_or(meta.last_seq)
        .max(meta.last_seq);
    let source = spec.source.clone();
    let placement = (db.q, db.partitioned);
    // As in a rebuild, the permits travel with the blocking work so a
    // cancelled request cannot let cleanup or another update race it.
    tokio::task::spawn_blocking(move || {
        let indexed = run_index_json(&source, &live, limits).map_err(|reason| {
            ApiError::new(
                500,
                "search_index_error",
                format!("index function failed: {reason}"),
            )
        })?;
        meta.docs.retain(|doc| !changed.contains(&doc.id));
        meta.docs.extend(indexed.into_iter().map(|doc| SearchDoc {
            id: doc.id,
            fields: doc.fields,
        }));
        // A rebuild reads documents shard by shard in id order, and a field's
        // type can depend on which value comes first, so infer in that order.
        let (q, partitioned) = placement;
        meta.docs.sort_by_cached_key(|doc| {
            (
                crate::db::shard_index_for(&doc.id, q, partitioned),
                doc.id.clone(),
            )
        });
        let kinds = infer_kinds(meta.docs.iter().map(|doc| &doc.fields));
        let tantivy_index = Index::open_in_dir(&path)
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
        let schema = tantivy_index.schema();
        let mut fields: HashMap<String, (Field, FieldKind)> = HashMap::new();
        let mut id_field = None;
        for (field, entry) in schema.fields() {
            let kind = match entry.field_type() {
                FieldType::Str(_) if entry.name() == "_id" => {
                    id_field = Some(field);
                    continue;
                }
                FieldType::Str(_) => FieldKind::Text,
                FieldType::F64(_) => FieldKind::Number,
                FieldType::Bool(_) => FieldKind::Bool,
                _ => return Ok(Update::Rebuild(gate, index_guard)),
            };
            fields.insert(entry.name().to_string(), (field, kind));
        }
        let Some(id_field) = id_field else {
            return Ok(Update::Rebuild(gate, index_guard));
        };
        let schema_kinds: BTreeMap<String, FieldKind> = fields
            .iter()
            .map(|(name, (_, kind))| (name.clone(), *kind))
            .collect();
        if kinds != schema_kinds {
            return Ok(Update::Rebuild(gate, index_guard));
        }

        let writer: tantivy::IndexWriter = tantivy_index
            .writer(50_000_000)
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
        for id in &changed {
            writer.delete_term(Term::from_field_text(id_field, id));
        }
        for doc in meta.docs.iter().filter(|doc| changed.contains(&doc.id)) {
            writer
                .add_document(tantivy_doc(&doc.id, &doc.fields, id_field, &fields))
                .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
        }
        finish_writer(writer)?;

        meta.last_seq = last_seq;
        write_meta(&path, &meta)?;
        drop(index_guard);
        Ok(Update::Done(meta, gate))
    })
    .await
    .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?
}

fn build_gate(db: &Db) -> Arc<RwLock<()>> {
    static GATES: OnceLock<Mutex<HashMap<PathBuf, Arc<RwLock<()>>>>> = OnceLock::new();
    let gates = GATES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut gates = gates.lock().unwrap_or_else(|poison| poison.into_inner());
    gates
        .entry(db.path.clone())
        .or_insert_with(|| Arc::new(RwLock::new(())))
        .clone()
}

fn build_lock(path: &Path) -> Arc<AsyncMutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<AsyncMutex<()>>>>> = OnceLock::new();
    let locks = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks.lock().unwrap_or_else(|poison| poison.into_inner());
    locks
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(AsyncMutex::new(())))
        .clone()
}

async fn ensure_index(
    db: &Db,
    ddoc: &str,
    index_name: &str,
    spec: &SearchSpec,
    limits: crate::view::ViewLimits,
    gate: OwnedRwLockReadGuard<()>,
) -> DbResult<(PathBuf, SearchMeta, OwnedRwLockReadGuard<()>)> {
    let path = index_dir(db, ddoc, index_name);
    let signature = design_signature(spec);
    let lock = build_lock(&path);
    let index_guard = lock.lock_owned().await;
    // Re-check after acquiring the per-index lock: another request may have
    // completed the rebuild while this one waited.
    if let Some(meta) = read_meta(&path) {
        if meta.version == SEARCH_VERSION && meta.design == signature {
            let current = db.update_seq();
            if meta.last_seq == current {
                return Ok((path, meta, gate));
            }
            if meta.last_seq < current {
                let update =
                    update_index(db, path.clone(), meta, spec, limits, gate, index_guard).await?;
                return match update {
                    Update::Done(meta, gate) => Ok((path, meta, gate)),
                    Update::Rebuild(gate, index_guard) => {
                        build_index(db, ddoc, index_name, spec, limits, gate, index_guard).await
                    }
                };
            }
        }
    }
    build_index(db, ddoc, index_name, spec, limits, gate, index_guard).await
}

fn owned_value_to_json(value: OwnedValue) -> Value {
    match value {
        OwnedValue::Null => Value::Null,
        OwnedValue::Str(text) => Value::String(text),
        OwnedValue::U64(number) => json!(number),
        OwnedValue::I64(number) => json!(number),
        OwnedValue::F64(number) => crate::view::number_value(number),
        OwnedValue::Bool(value) => json!(value),
        OwnedValue::Array(values) => {
            Value::Array(values.into_iter().map(owned_value_to_json).collect())
        }
        OwnedValue::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(k, v)| (k, owned_value_to_json(v)))
                .collect(),
        ),
        OwnedValue::Bytes(bytes) => Value::String(hex(&bytes)),
        OwnedValue::Facet(facet) => Value::String(facet.to_string()),
        _ => Value::Null,
    }
}

fn field_key(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn matches_clause(doc: &SearchDoc, field: &str, values: &[Value]) -> bool {
    doc.fields
        .get(field)
        .map(|value| values.iter().any(|expected| expected == value))
        .unwrap_or(false)
}

fn matches_drilldown(doc: &SearchDoc, drilldown: &[Vec<(String, Vec<Value>)>]) -> bool {
    drilldown.iter().all(|clause| {
        clause
            .iter()
            .all(|(field, values)| matches_clause(doc, field, values))
    })
}

fn parse_number(text: &str) -> Option<f64> {
    match text.trim() {
        "Infinity" | "+Infinity" | "inf" => Some(f64::INFINITY),
        "-Infinity" | "-inf" => Some(f64::NEG_INFINITY),
        other => other.parse().ok(),
    }
}

fn value_in_range(value: &Value, range: &str) -> bool {
    let range = range.trim();
    if range.len() < 2 {
        return false;
    }
    let lower_inclusive = range.starts_with('[');
    let upper_inclusive = range.ends_with(']');
    let inner = &range[1..range.len() - 1];
    let Some((lower, upper)) = inner.split_once(" TO ") else {
        return false;
    };
    let compare = |left: &str, right: &str| -> std::cmp::Ordering {
        match (parse_number(left), parse_number(right)) {
            (Some(left), Some(right)) => left
                .partial_cmp(&right)
                .unwrap_or(std::cmp::Ordering::Equal),
            _ => left.cmp(right),
        }
    };
    let within_lower = if lower.trim() == "*" || lower.trim() == "-Infinity" {
        true
    } else {
        let order = match value {
            Value::Number(number) => compare(&number.to_string(), lower),
            Value::String(text) => compare(text, lower),
            _ => return false,
        };
        if lower_inclusive {
            order != std::cmp::Ordering::Less
        } else {
            order == std::cmp::Ordering::Greater
        }
    };
    let within_upper = if upper.trim() == "*" || upper.trim() == "Infinity" {
        true
    } else {
        let order = match value {
            Value::Number(number) => compare(&number.to_string(), upper),
            Value::String(text) => compare(text, upper),
            _ => return false,
        };
        if upper_inclusive {
            order != std::cmp::Ordering::Greater
        } else {
            order == std::cmp::Ordering::Less
        }
    };
    within_lower && within_upper
}

pub(crate) fn parse_bookmark(bookmark: Option<&str>) -> usize {
    bookmark
        .and_then(|bookmark| bookmark.strip_prefix("search:").or(Some(bookmark)))
        .and_then(|bookmark| bookmark.parse().ok())
        .unwrap_or(0)
}

pub(crate) fn parse_sort(value: &Value) -> Vec<(String, bool)> {
    let mut out = Vec::new();
    let items: Vec<&str> = match value {
        Value::String(text) => vec![text.as_str()],
        Value::Array(items) => items.iter().filter_map(Value::as_str).collect(),
        _ => return out,
    };
    for item in items {
        let mut item = item.trim();
        let descending = item.starts_with('-');
        if descending {
            item = &item[1..];
        }
        let name = item.split('<').next().unwrap_or(item).trim();
        if !name.is_empty() && name != "<score>" && name != "score" {
            out.push((name.to_string(), descending));
        }
    }
    out
}

fn query_terms(query: &str) -> Vec<String> {
    query
        .split_whitespace()
        .flat_map(|token| token.split(['(', ')']))
        .filter_map(|token| {
            let token = token.trim_matches(|ch: char| {
                matches!(
                    ch,
                    '+' | '-' | '"' | '~' | '*' | '?' | '[' | ']' | '{' | '}' | ':' | '^'
                )
            });
            if token.is_empty()
                || token == "TO"
                || token == "AND"
                || token == "OR"
                || token == "NOT"
            {
                return None;
            }
            let token = token.rsplit(':').next().unwrap_or(token);
            Some(token.to_ascii_lowercase())
        })
        .collect()
}

fn highlight_fragment(
    text: &str,
    terms: &[String],
    pre: &str,
    post: &str,
    size: usize,
    number: usize,
) -> Vec<String> {
    let lower = text.to_lowercase();
    let mut fragments = Vec::new();
    for term in terms {
        if term.is_empty() || fragments.len() >= number {
            continue;
        }
        let Some(index) = lower.find(term) else {
            continue;
        };
        if !text.is_char_boundary(index) || !text.is_char_boundary(index + term.len()) {
            continue;
        }
        let start = index.saturating_sub(size / 2);
        let end = (index + term.len() + size / 2).min(text.len());
        let start = text
            .char_indices()
            .map(|(i, _)| i)
            .find(|i| *i >= start)
            .unwrap_or(0);
        let end = text
            .char_indices()
            .map(|(i, _)| i)
            .find(|i| *i >= end)
            .unwrap_or(text.len());
        let mut fragment = String::new();
        fragment.push_str(&text[start..index]);
        fragment.push_str(pre);
        fragment.push_str(&text[index..index + term.len()]);
        fragment.push_str(post);
        fragment.push_str(&text[index + term.len()..end]);
        fragments.push(fragment);
    }
    fragments
}

fn sort_compare(
    left: &SearchDoc,
    right: &SearchDoc,
    sort: &[(String, bool)],
) -> std::cmp::Ordering {
    for (field, descending) in sort {
        let left_value = left.fields.get(field);
        let right_value = right.fields.get(field);
        let order = match (left_value, right_value) {
            (Some(left_value), Some(right_value)) => crate::view::cmp_json(left_value, right_value),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        };
        if order != std::cmp::Ordering::Equal {
            return if *descending { order.reverse() } else { order };
        }
    }
    left.id.cmp(&right.id)
}

fn empty_search_response(request: &SearchRequest) -> Value {
    let mut response = json!({
        "total_rows": 0,
        "bookmark": "search:0",
        "rows": [],
    });
    if !request.counts.is_empty() {
        let counts: Map<String, Value> = request
            .counts
            .iter()
            .map(|field| (field.clone(), json!({})))
            .collect();
        response["counts"] = Value::Object(counts);
    }
    if !request.ranges.is_empty() {
        let ranges: Map<String, Value> = request
            .ranges
            .keys()
            .map(|field| (field.clone(), json!({})))
            .collect();
        response["ranges"] = Value::Object(ranges);
    }
    if request.group_field.is_some() {
        response["groups"] = json!([]);
    }
    response
}

pub async fn query(
    db: &Db,
    ddoc: &str,
    index_name: &str,
    request: &SearchRequest,
    limits: crate::view::ViewLimits,
) -> DbResult<Value> {
    let started = Instant::now();
    if request.timeout_ms == 0 || started.elapsed().as_millis() as u64 > request.timeout_ms {
        return Err(ApiError::new(
            500,
            "timeout",
            "The request could not be processed in a reasonable amount of time.",
        ));
    }
    if request.partition.is_some() && request.limit > PAGE_LIMIT {
        return Err(ApiError::bad_request(
            "Limit is too large for a partitioned search.",
        ));
    }
    let spec = load_spec(db, ddoc, index_name).await?;
    // Hold the read permit for the whole query, not just the rebuild, so a
    // concurrent `_search_cleanup` cannot remove the index under an open
    // searcher.
    let gate = build_gate(db).read_owned().await;
    let (path, meta, _gate) = ensure_index(db, ddoc, index_name, &spec, limits, gate).await?;
    if started.elapsed().as_millis() as u64 > request.timeout_ms {
        return Err(ApiError::new(
            500,
            "timeout",
            "The request could not be processed in a reasonable amount of time.",
        ));
    }
    let tantivy_index = Index::open_in_dir(&path)
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    let schema = tantivy_index.schema();
    let id_field = schema
        .get_field("_id")
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    let reader = tantivy_index
        .reader()
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    let searcher = reader.searcher();

    let query_text = request.query.trim();
    let query: Box<dyn tantivy::query::Query> = if query_text == "*:*" || query_text.is_empty() {
        Box::new(AllQuery)
    } else {
        let default_fields: Vec<Field> = schema
            .fields()
            .filter(|(_, field)| field.name() != "_id")
            .map(|(field, _)| field)
            .collect();
        let parser = QueryParser::for_index(&tantivy_index, default_fields);
        match parser.parse_query(query_text) {
            Ok(query) => query,
            // A node may hold no local documents with an indexed field even
            // though another shard does. Treat an absent field as no local
            // matches so cluster fan-out can merge other shards' rows.
            Err(error) if error.to_string().contains("Field does not exist") => {
                return Ok(empty_search_response(request));
            }
            Err(error) => {
                return Err(ApiError::bad_request(format!(
                    "invalid search query: {error}"
                )))
            }
        }
    };
    let addresses = searcher
        .search(&*query, &DocSetCollector)
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;

    let by_id: HashMap<&str, &SearchDoc> =
        meta.docs.iter().map(|doc| (doc.id.as_str(), doc)).collect();
    let mut matched: Vec<SearchDoc> = Vec::new();
    for address in addresses {
        let document: TantivyDocument = searcher
            .doc(address)
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
        let Some(value) = document.get_first(id_field) else {
            continue;
        };
        let id = match owned_value_to_json(value.into()) {
            Value::String(id) => id,
            _ => continue,
        };
        let Some(doc) = by_id.get(id.as_str()) else {
            continue;
        };
        if let Some(partition) = &request.partition {
            if !doc.id.starts_with(&format!("{partition}:")) {
                continue;
            }
        }
        if let Some(owner_shards) = &request.owner_shards {
            if !owner_shards
                .get(db.shard_index(&doc.id))
                .copied()
                .unwrap_or(false)
            {
                continue;
            }
        }
        if !matches_drilldown(doc, &request.drilldown) {
            continue;
        }
        matched.push((*doc).clone());
    }
    if request.sort.is_empty() {
        matched.sort_by(|a, b| a.id.cmp(&b.id));
    } else {
        matched.sort_by(|a, b| sort_compare(a, b, &request.sort));
    }

    let total_rows = matched.len();
    let offset = parse_bookmark(request.bookmark.as_deref());
    let limit = if request.limit == 0 {
        0
    } else {
        request.limit.min(PAGE_LIMIT)
    };
    let page: Vec<&SearchDoc> = matched.iter().skip(offset).take(limit).collect();
    let docs = if request.include_docs {
        let ids: Vec<String> = page.iter().map(|doc| doc.id.clone()).collect();
        crate::view::docs_by_ids(db, false, &ids).await?
    } else {
        HashMap::new()
    };

    let terms = query_terms(&request.query);
    let mut rows = Vec::with_capacity(page.len());
    for doc in &page {
        let order: Vec<Value> = request
            .sort
            .iter()
            .map(|(field, _)| doc.fields.get(field).cloned().unwrap_or(Value::Null))
            .collect();
        let mut row = json!({
            "id": doc.id,
            "fields": doc.fields,
            "order": order,
        });
        if let Some(document) = docs.get(&doc.id) {
            row["doc"] = document.clone();
        }
        if !request.highlight_fields.is_empty() {
            let mut highlights = Map::new();
            for field in &request.highlight_fields {
                if let Some(Value::String(text)) = doc.fields.get(field) {
                    let fragments = highlight_fragment(
                        text,
                        &terms,
                        &request.highlight_pre,
                        &request.highlight_post,
                        request.highlight_size,
                        request.highlight_number,
                    );
                    if !fragments.is_empty() {
                        highlights.insert(field.clone(), json!(fragments));
                    }
                }
            }
            if !highlights.is_empty() {
                row["highlights"] = Value::Object(highlights);
            }
        }
        rows.push(row);
    }
    let next = offset.saturating_add(page.len());
    let mut response = json!({
        "total_rows": total_rows,
        "bookmark": format!("search:{next}"),
        "rows": rows,
    });

    if !request.counts.is_empty() {
        let mut counts = Map::new();
        for field in &request.counts {
            let mut values: BTreeMap<String, u64> = BTreeMap::new();
            for doc in &matched {
                if let Some(value) = doc.fields.get(field) {
                    *values.entry(field_key(value)).or_default() += 1;
                }
            }
            counts.insert(field.clone(), json!(values));
        }
        response["counts"] = Value::Object(counts);
    }

    if !request.ranges.is_empty() {
        let mut ranges = Map::new();
        for (field, definitions) in &request.ranges {
            let mut counts = Map::new();
            if let Some(definitions) = definitions.as_object() {
                for (label, range) in definitions {
                    let Some(range) = range.as_str() else {
                        continue;
                    };
                    let count = matched
                        .iter()
                        .filter(|doc| {
                            doc.fields
                                .get(field)
                                .map(|value| value_in_range(value, range))
                                .unwrap_or(false)
                        })
                        .count();
                    counts.insert(label.clone(), json!(count));
                }
            }
            ranges.insert(field.clone(), Value::Object(counts));
        }
        response["ranges"] = Value::Object(ranges);
    }

    if let Some(group_field) = &request.group_field {
        let mut groups: BTreeMap<String, Vec<&SearchDoc>> = BTreeMap::new();
        for doc in &matched {
            let key = doc
                .fields
                .get(group_field)
                .map(field_key)
                .unwrap_or_else(|| "null".to_string());
            groups.entry(key).or_default().push(doc);
        }
        let groups: Vec<Value> = groups
            .into_iter()
            .map(|(key, members)| {
                json!({
                    "by": key,
                    "total_rows": members.len(),
                    "rows": members
                        .iter()
                        .map(|doc| json!({"id": doc.id, "fields": doc.fields, "order": []}))
                        .collect::<Vec<_>>(),
                })
            })
            .collect();
        response["groups"] = json!(groups);
    }

    Ok(response)
}

pub async fn info(
    db: &Db,
    ddoc: &str,
    index_name: &str,
    limits: crate::view::ViewLimits,
) -> DbResult<Value> {
    let spec = load_spec(db, ddoc, index_name).await?;
    let gate = build_gate(db).read_owned().await;
    let (_path, meta, _gate) = ensure_index(db, ddoc, index_name, &spec, limits, gate).await?;
    let copies = db.replicas_of_shard(0).len().max(1);
    Ok(json!({
        "search_index": {
            "doc_count": meta.docs.len(),
            "signature": meta.design,
            "updates_pending": {
                "copies": copies,
                "copies_expected": copies,
                "maximum": 0,
                "minimum": 0,
                "preferred": copies,
                "total": 0,
            }
        }
    }))
}

pub async fn cleanup(db: &Db) -> DbResult<()> {
    // Wait for in-flight builds (including builds whose request was cancelled)
    // before removing their directories.
    let _gate = build_gate(db).write_owned().await;
    let directory = db.path.join("_search_indexes");
    if directory.exists() {
        std::fs::remove_dir_all(&directory)
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    }
    Ok(())
}

/// Entries for the `_index_info` endpoint.
pub fn all_indexes(db: &Db) -> Vec<Value> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(db.path.join("_search_indexes")) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(meta) = read_meta(&path) else {
            continue;
        };
        if meta.ddoc.is_empty() || meta.name.is_empty() {
            continue;
        }
        out.push(json!({
            "ddoc": meta.ddoc,
            "name": meta.name,
            "ok": true,
            "info": {
                "doc_count": meta.docs.len(),
                "signature": meta.design,
                "updates_pending": {
                    "copies": db.replicas_of_shard(0).len().max(1),
                    "copies_expected": db.replicas_of_shard(0).len().max(1),
                    "maximum": 0,
                    "minimum": 0,
                    "preferred": db.replicas_of_shard(0).len().max(1),
                    "total": 0,
                }
            }
        }));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn updated_indexes_match_a_rebuild_after_each_write() {
        let root = std::env::temp_dir().join(format!(
            "douchdb-search-update-{}-{}",
            std::process::id(),
            crate::db::uuid()
        ));
        let db = Db::open(
            &root,
            "searchupdate",
            crate::db::DbConfig {
                q: 2,
                revs_limit: 1000,
                shard_map: crate::cluster::ShardMap::single(2),
                partitioned: false,
            },
        )
        .unwrap();
        // Two design documents with the same index: `a` is kept up to date
        // across writes, `b` is rebuilt from scratch before each comparison.
        let index = "function(doc){ if (doc.name) index('name', doc.name); \
                     if (doc.n !== undefined) index('n', doc.n); \
                     if (doc.colour) index('colour', doc.colour); }";
        for ddoc in ["_design/a", "_design/b"] {
            db.put_docs(
                vec![json!({"_id": ddoc, "indexes": {"idx": {"index": index}}})],
                true,
            )
            .await
            .unwrap();
        }
        let request = |q: &str| SearchRequest {
            query: q.to_string(),
            limit: 200,
            sort: vec![("n".to_string(), false)],
            ..SearchRequest::default()
        };
        let limits = crate::view::ViewQuery::default().limits;
        let search = |ddoc: &'static str, q: &'static str| {
            let db = &db;
            async move { query(db, ddoc, "idx", &request(q), limits).await.unwrap() }
        };
        let marker = index_dir(&db, "_design/a", "idx").join("douchdb-test-marker");
        let mut revs: HashMap<String, String> = HashMap::new();

        let initial: Vec<Value> = (0..40)
            .map(|i| json!({"_id": format!("d{i:02}"), "name": format!("apple{}", i % 4), "n": i}))
            .collect();
        // Each step's documents, and whether it should force a rebuild.
        let steps: Vec<(Vec<Value>, bool)> = vec![
            (initial, true),
            // New documents and an updated value: incremental.
            (
                vec![
                    json!({"_id": "d40", "name": "pear", "n": 40}),
                    json!({"_id": "d03", "name": "pear", "n": 3}),
                ],
                false,
            ),
            // A delete: incremental.
            (vec![json!({"_id": "d05", "_deleted": true})], false),
            // A new field changes the schema.
            (
                vec![json!({"_id": "d06", "name": "fig", "n": 6, "colour": "red"})],
                true,
            ),
            // A string where `n` is numeric leaves it numeric: the first
            // value read decides.
            (
                vec![json!({"_id": "d08", "name": "fig", "n": "eight"})],
                false,
            ),
            // A boolean where `n` is numeric makes it a text field.
            (vec![json!({"_id": "d07", "name": "fig", "n": true})], true),
            // Deleting the only boolean makes `n` numeric again.
            (vec![json!({"_id": "d07", "_deleted": true})], true),
        ];
        for (step, (docs, rebuilds)) in steps.into_iter().enumerate() {
            let docs: Vec<Value> = docs
                .into_iter()
                .map(|mut doc| {
                    let id = doc["_id"].as_str().unwrap().to_string();
                    if let Some(rev) = revs.get(&id) {
                        doc["_rev"] = json!(rev);
                    }
                    doc
                })
                .collect();
            for result in db.put_docs(docs, true).await.unwrap() {
                let id = result["id"].as_str().unwrap().to_string();
                revs.insert(id, result["rev"].as_str().unwrap().to_string());
            }
            if step > 0 {
                std::fs::write(&marker, b"").unwrap();
            }
            std::fs::remove_dir_all(index_dir(&db, "_design/b", "idx")).ok();
            for q in [
                "*:*",
                "name:pear",
                "name:apple1",
                "name:apple3",
                "n:[3 TO 20]",
            ] {
                let updated = search("_design/a", q).await;
                let rebuilt = search("_design/b", q).await;
                assert_eq!(updated, rebuilt, "step {step} {q}");
            }
            if step > 0 {
                assert_eq!(!marker.exists(), rebuilds, "step {step} rebuilt");
            }
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn build_locks_are_per_index() {
        let first = build_lock(Path::new("/tmp/douchdb-search-lock-a"));
        let second = build_lock(Path::new("/tmp/douchdb-search-lock-b"));
        assert!(Arc::ptr_eq(
            &first,
            &build_lock(Path::new("/tmp/douchdb-search-lock-a"))
        ));
        assert!(!Arc::ptr_eq(&first, &second));
    }
}

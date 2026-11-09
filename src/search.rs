//! Tantivy-backed CouchDB search indexes.

use crate::db::{ApiError, Db, DbResult};
use crate::index_cache::{Gate, Permit, Registry};
use crate::js::JsError;
use crate::util::hex;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tantivy::collector::DocSetCollector;
use tantivy::directory::error::{
    DeleteError, LockError, OpenDirectoryError, OpenReadError, OpenWriteError,
};
use tantivy::directory::{
    AntiCallToken, Directory, DirectoryLock, FileHandle, Lock, MmapDirectory, TerminatingWrite,
    WatchCallback, WatchHandle, WritePtr,
};
use tantivy::query::{AllQuery, EnableScoring, QueryParser};
use tantivy::schema::{
    Field, FieldType, OwnedValue, Schema, TantivyDocument, FAST, INDEXED, STORED, STRING, TEXT,
};
use tantivy::{Index, IndexReader, IndexSettings, ReloadPolicy, Searcher, Term};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

const SEARCH_VERSION: u8 = 3;
const PAGE_LIMIT: usize = 2000;

#[derive(Clone, Debug)]
pub struct SearchSpec {
    pub source: String,
    pub analyzer: String,
}

/// A document as the index function emitted it.
#[derive(Clone, Debug)]
struct SearchDoc {
    id: String,
    /// The shard the document lives on, which orders a rebuild's documents.
    shard: usize,
    fields: Map<String, Value>,
}

impl SearchDoc {
    fn new(id: String, fields: Map<String, Value>, (q, partitioned): (u32, bool)) -> Self {
        SearchDoc {
            shard: crate::db::shard_index_for(&id, q, partitioned),
            id,
            fields,
        }
    }
}

/// An index's description. Only the identifying fields are written to the
/// metadata file; `last_seq` is the payload of the index's last commit and
/// each document's fields are stored with it, so both always match the
/// committed index.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SearchMeta {
    version: u8,
    #[serde(skip)]
    last_seq: u64,
    design: String,
    #[serde(default)]
    ddoc: String,
    #[serde(default)]
    name: String,
    /// The indexed documents by id. A document is shared, so an update
    /// copies pointers for the documents it leaves alone, not their fields.
    #[serde(skip)]
    docs: HashMap<String, Arc<SearchDoc>>,
    #[serde(skip)]
    kinds: KindStats,
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
    pub owner_shards: Option<crate::cluster::ShardMask>,
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

const INDEX_WRAPPER: &str = include_str!("../wasm/js/index.js");

fn engine(limits: crate::view::ViewLimits) -> Result<rquickjs::Runtime, JsError> {
    crate::js::runtime(limits.timeout, limits.memory)
}

pub(crate) fn run_index_json(
    source: &str,
    docs: &[(String, String)],
    limits: crate::view::ViewLimits,
) -> Result<Vec<IndexedDoc>, JsError> {
    if docs.is_empty() {
        return Ok(Vec::new());
    }
    if crate::js::sandboxed() {
        run_index_sandboxed(source, docs, limits)
    } else {
        run_index_in_process(source, docs, limits)
    }
}

/// Run the index function in the sandbox process, in chunks that keep each
/// request small.
fn run_index_sandboxed(
    source: &str,
    docs: &[(String, String)],
    limits: crate::view::ViewLimits,
) -> Result<Vec<IndexedDoc>, JsError> {
    let mut out = Vec::with_capacity(docs.len());
    for chunk in docs.chunks(2048) {
        let value =
            crate::js::sandbox_request("index", json!({"source": source, "docs": chunk}), limits)?;
        let mut chunk: Vec<IndexedDoc> =
            serde_json::from_value(value).map_err(|error| JsError::Runtime(error.to_string()))?;
        out.append(&mut chunk);
    }
    Ok(out)
}

/// Run the index function in a QuickJS runtime of this process.
fn run_index_in_process(
    source: &str,
    docs: &[(String, String)],
    limits: crate::view::ViewLimits,
) -> Result<Vec<IndexedDoc>, JsError> {
    let runtime = engine(limits)?;
    let context =
        rquickjs::Context::full(&runtime).map_err(|error| JsError::Runtime(error.to_string()))?;
    crate::js::guard("index function", || {
        context.with(|ctx| {
            let wrapped = INDEX_WRAPPER.replace(crate::viewfn::WRAPPER_SOURCE, source);
            let func: rquickjs::Function = ctx
                .eval(wrapped)
                .map_err(|error| JsError::Runtime(format!("compile: {error}")))?;
            let mut out = Vec::with_capacity(docs.len());
            const BATCH: usize = 128;
            for chunk in docs.chunks(BATCH) {
                out.extend(index_chunk(&func, chunk)?);
            }
            Ok(out)
        })
    })
}

/// Index one batch of documents with a compiled index function.
fn index_chunk(
    func: &rquickjs::Function<'_>,
    chunk: &[(String, String)],
) -> Result<Vec<IndexedDoc>, JsError> {
    let mut batch = String::from("[");
    for (index, (_, raw)) in chunk.iter().enumerate() {
        if index > 0 {
            batch.push(',');
        }
        batch.push_str(raw);
    }
    batch.push(']');
    let first = chunk.first().map(|(id, _)| id.as_str()).unwrap_or_default();
    let output: String = func
        .call((batch,))
        .map_err(|error| JsError::Runtime(format!("{first}: {error}")))?;
    crate::js::check_output("index function", &output)?;
    let emitted: Vec<Vec<(String, Value)>> =
        serde_json::from_str(&output).map_err(|error| JsError::Runtime(error.to_string()))?;
    if emitted.len() != chunk.len() {
        return Err(JsError::Runtime(format!(
            "{first}: index returned {} results for {} documents",
            emitted.len(),
            chunk.len()
        )));
    }
    // The id comes from the stored document, not from the object the index
    // function was handed.
    Ok(chunk
        .iter()
        .zip(emitted)
        .map(|((id, _), fields)| {
            let mut map = Map::new();
            for (name, value) in fields {
                map.insert(name, value);
            }
            IndexedDoc {
                id: id.clone(),
                fields: map,
            }
        })
        .collect())
}

/// The schema field holding each document's id.
const ID_FIELD: &str = "_id";
/// The stored-only schema field holding each document's indexed fields as
/// JSON, read back when the index is opened.
const FIELDS_FIELD: &str = "_fields";

#[derive(Clone, Copy, Debug, PartialEq)]
enum FieldKind {
    Text,
    Number,
    Bool,
}

/// The value kinds seen in each field, kept up to date as documents are
/// added and removed so an update does not look at the others.
///
/// A rebuild reads documents shard by shard in id order and a field's type
/// depends on which value comes first: a field is text if its first value is,
/// and also if both numbers and booleans were indexed into it. Other values
/// are text in a numeric or boolean field and are not indexed into it.
#[derive(Clone, Debug, Default)]
struct KindStats {
    fields: BTreeMap<String, FieldTally>,
}

#[derive(Clone, Debug, Default)]
struct FieldTally {
    numbers: usize,
    bools: usize,
    texts: usize,
    /// The kind of the field's first value, with the (shard, id) of the
    /// document it came from. `None` while that document is gone and the
    /// next one has not been looked up.
    first: Option<((usize, String), FieldKind)>,
}

impl FieldTally {
    fn total(&self) -> usize {
        self.numbers + self.bools + self.texts
    }

    fn count(&mut self, kind: FieldKind) -> &mut usize {
        match kind {
            FieldKind::Text => &mut self.texts,
            FieldKind::Number => &mut self.numbers,
            FieldKind::Bool => &mut self.bools,
        }
    }
}

fn value_kind(value: &Value) -> FieldKind {
    match value {
        Value::Number(_) => FieldKind::Number,
        Value::Bool(_) => FieldKind::Bool,
        _ => FieldKind::Text,
    }
}

/// The index's own fields cannot also be index fields.
fn is_index_field(name: &str) -> bool {
    name != ID_FIELD && name != FIELDS_FIELD
}

impl KindStats {
    fn add(&mut self, doc: &SearchDoc) {
        for (name, value) in doc.fields.iter().filter(|(name, _)| is_index_field(name)) {
            let kind = value_kind(value);
            let tally = self.fields.entry(name.clone()).or_default();
            *tally.count(kind) += 1;
            let earlier = tally
                .first
                .as_ref()
                .is_none_or(|(key, _)| (doc.shard, doc.id.as_str()) < (key.0, key.1.as_str()));
            if earlier {
                tally.first = Some(((doc.shard, doc.id.clone()), kind));
            }
        }
    }

    /// Take a document out. A field whose first value was in it needs
    /// `refresh` before more documents are added.
    fn remove(&mut self, doc: &SearchDoc) {
        for (name, value) in doc.fields.iter().filter(|(name, _)| is_index_field(name)) {
            let Some(tally) = self.fields.get_mut(name) else {
                continue;
            };
            let count = tally.count(value_kind(value));
            *count = count.saturating_sub(1);
            if tally.total() == 0 {
                self.fields.remove(name);
            } else if tally
                .first
                .as_ref()
                .is_some_and(|(key, _)| (key.0, key.1.as_str()) == (doc.shard, doc.id.as_str()))
            {
                tally.first = None;
            }
        }
    }

    /// Find the first value again of each field that lost it, among the
    /// documents that remain. That reads every document, but only when the
    /// document holding a field's first value is changed or deleted.
    fn refresh<'a>(&mut self, docs: impl Iterator<Item = &'a Arc<SearchDoc>>) {
        let lost: Vec<String> = self
            .fields
            .iter()
            .filter(|(_, tally)| tally.first.is_none())
            .map(|(name, _)| name.clone())
            .collect();
        if lost.is_empty() {
            return;
        }
        for doc in docs {
            for name in &lost {
                let Some(value) = doc.fields.get(name) else {
                    continue;
                };
                let Some(tally) = self.fields.get_mut(name) else {
                    continue;
                };
                let earlier = tally
                    .first
                    .as_ref()
                    .is_none_or(|(key, _)| (doc.shard, doc.id.as_str()) < (key.0, key.1.as_str()));
                if earlier {
                    tally.first = Some(((doc.shard, doc.id.clone()), value_kind(value)));
                }
            }
        }
    }

    fn kinds(&self) -> BTreeMap<String, FieldKind> {
        self.fields
            .iter()
            .filter_map(|(name, tally)| {
                let (_, first) = tally.first.as_ref()?;
                let kind = if *first == FieldKind::Text || (tally.numbers > 0 && tally.bools > 0) {
                    FieldKind::Text
                } else {
                    *first
                };
                Some((name.clone(), kind))
            })
            .collect()
    }
}

fn write_meta(path: &Path, meta: &SearchMeta) -> DbResult<()> {
    let bytes = serde_json::to_vec(meta)
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    crate::db::write_atomic(&path.join("douchdb-search-meta.json"), &bytes)
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
    gate: Permit,
    mut entry: OwnedMutexGuard<Option<Cached>>,
) -> DbResult<(Snapshot, Permit)> {
    // Capture the bound before reading the snapshot. A write that lands
    // while the index is being built then leaves the database ahead of the
    // recorded sequence, so the next query rebuilds instead of trusting a
    // snapshot that may have missed it. The bound is the changes watermark: a
    // write in flight has taken a sequence but is not readable yet.
    gate.check()?;
    let last_seq = db.changes_watermark(0, None);
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
        docs: HashMap::new(),
        kinds: KindStats::default(),
    };
    let placement = (db.q, db.partitioned);
    // QuickJS/Tantivy work is CPU- and IO-bound. Run it on the blocking pool so
    // a search rebuild cannot occupy a Tokio async worker for its full duration.
    let (result, gate) = tokio::task::spawn_blocking(move || {
        // Keep both permits in the blocking task, not the request future. A
        // cancelled request must not let cleanup race a live rebuild. The read
        // permit is returned so a query can keep it for its whole lifetime.
        // The old index's files are about to be removed, so forget it first.
        *entry = None;
        let result = build_index_files(path, docs, &source, meta, placement, limits, &gate)
            .and_then(|(index, meta)| Cached::open(index, meta))
            .map(|cached| {
                let snapshot = cached.snapshot.clone();
                *entry = Some(cached);
                snapshot
            });
        (result, gate)
    })
    .await
    .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    Ok((result?, gate))
}

/// Documents indexed between checks that the database still exists.
const INDEX_BATCH: usize = 1000;

/// The index function over `docs` a batch at a time, so a build notices that
/// its database was deleted between batches, not after the last document.
fn index_in_batches(
    source: &str,
    docs: &[(String, String)],
    limits: crate::view::ViewLimits,
    gate: &Permit,
) -> DbResult<Vec<IndexedDoc>> {
    let mut indexed = Vec::new();
    for batch in docs.chunks(INDEX_BATCH) {
        gate.check()?;
        indexed.extend(run_index_json(source, batch, limits).map_err(|reason| {
            ApiError::new(
                500,
                "search_index_error",
                format!("index function failed: {reason}"),
            )
        })?);
    }
    Ok(indexed)
}

/// Build the index at `path` and write `meta` with the indexed documents.
fn build_index_files(
    path: PathBuf,
    docs: Vec<(String, String)>,
    source: &str,
    mut meta: SearchMeta,
    placement: (u32, bool),
    limits: crate::view::ViewLimits,
    gate: &Permit,
) -> DbResult<(Index, SearchMeta)> {
    let indexed = index_in_batches(source, &docs, limits, gate)?;
    let indexed: Vec<Arc<SearchDoc>> = indexed
        .into_iter()
        .map(|doc| Arc::new(SearchDoc::new(doc.id, doc.fields, placement)))
        .collect();
    for doc in &indexed {
        meta.kinds.add(doc);
    }
    let kinds = meta.kinds.kinds();

    if path.exists() {
        std::fs::remove_dir_all(&path)
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    }
    std::fs::create_dir_all(&path)
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;

    let mut builder = Schema::builder();
    let id_field = builder.add_text_field(ID_FIELD, STRING | STORED | FAST);
    let fields_field = builder.add_text_field(FIELDS_FIELD, STORED);
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
    let tantivy_index = UnsyncedDirectory::open(&path)
        .map_err(tantivy::TantivyError::from)
        .and_then(|directory| Index::create(directory, schema.clone(), IndexSettings::default()))
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    let writer = tantivy_index
        .writer(50_000_000)
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;

    for (at, doc) in indexed.iter().enumerate() {
        if at.is_multiple_of(INDEX_BATCH) {
            gate.check()?;
        }
        writer
            .add_document(tantivy_doc(
                &doc.id,
                &doc.fields,
                (id_field, fields_field),
                &fields,
            ))
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    }
    gate.check()?;
    finish_writer(writer, meta.last_seq)?;

    meta.docs = indexed
        .into_iter()
        .map(|doc| (doc.id.clone(), doc))
        .collect();
    write_meta(&path, &meta)?;
    Ok((tantivy_index, meta))
}

/// A document for the index, with its id and its fields as JSON in the
/// schema's own two fields.
fn tantivy_doc(
    id: &str,
    values: &Map<String, Value>,
    (id_field, fields_field): (Field, Field),
    fields: &HashMap<String, (Field, FieldKind)>,
) -> TantivyDocument {
    let mut tantivy_doc = TantivyDocument::default();
    tantivy_doc.add_text(id_field, id);
    tantivy_doc.add_text(fields_field, Value::Object(values.clone()).to_string());
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

/// Commit with `last_seq` as the payload, so the sequence an index is up to
/// date with is saved with the documents it covers.
fn finish_writer(mut writer: tantivy::IndexWriter, last_seq: u64) -> DbResult<()> {
    let mut commit = writer
        .prepare_commit()
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    commit.set_payload(&last_seq.to_string());
    commit
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
    Done(Snapshot, Permit),
    /// Field types inferred over the updated documents differ from the
    /// index's schema, which only a rebuild can change.
    Rebuild(Permit, OwnedMutexGuard<Option<Cached>>),
}

/// The ids changed in `(since, last_seq]`, and the live documents among them
/// as `(id, JSON text)`, which is what the index function takes.
async fn indexable_changes(
    db: &Db,
    since: u64,
    last_seq: u64,
) -> DbResult<(std::collections::HashSet<String>, Vec<(String, String)>)> {
    let (rows, _, _) = db
        .changes_rows(&crate::db::ChangesOpts {
            since,
            include_docs: true,
            until: Some(last_seq),
            ..Default::default()
        })
        .await?;
    let mut changed = std::collections::HashSet::new();
    let mut live = Vec::new();
    for row in rows {
        if row.id.starts_with("_design/") || row.id.starts_with("_local/") {
            continue;
        }
        changed.insert(row.id.clone());
        if row.deleted {
            continue;
        }
        // The row already carries the document as JSON text.
        if let Some(doc) = row.doc {
            live.push((row.id, doc));
        }
    }
    Ok((changed, live))
}

/// An on-disk index's schema, read back: the two bookkeeping fields and the
/// indexed fields with their kinds.
struct SchemaLayout {
    id_field: Field,
    fields_field: Field,
    fields: HashMap<String, (Field, FieldKind)>,
}

impl SchemaLayout {
    /// None when the schema is not one this code wrote, so only a rebuild can
    /// bring the index up to date.
    fn read(schema: &Schema) -> Option<SchemaLayout> {
        let mut fields: HashMap<String, (Field, FieldKind)> = HashMap::new();
        let (mut id_field, mut fields_field) = (None, None);
        for (field, field_entry) in schema.fields() {
            let kind = match field_entry.field_type() {
                FieldType::Str(_) if field_entry.name() == ID_FIELD => {
                    id_field = Some(field);
                    continue;
                }
                FieldType::Str(_) if field_entry.name() == FIELDS_FIELD => {
                    fields_field = Some(field);
                    continue;
                }
                FieldType::Str(_) => FieldKind::Text,
                FieldType::F64(_) => FieldKind::Number,
                FieldType::Bool(_) => FieldKind::Bool,
                _ => return None,
            };
            fields.insert(field_entry.name().to_string(), (field, kind));
        }
        Some(SchemaLayout {
            id_field: id_field?,
            fields_field: fields_field?,
            fields,
        })
    }

    fn kinds(&self) -> BTreeMap<String, FieldKind> {
        self.fields
            .iter()
            .map(|(name, (_, kind))| (name.clone(), *kind))
            .collect()
    }
}

/// Delete the changed documents from the index, add the re-indexed ones and
/// commit with `last_seq` as the payload.
fn write_changes(
    index: &Index,
    layout: &SchemaLayout,
    changed: &std::collections::HashSet<String>,
    added: &[Arc<SearchDoc>],
    last_seq: u64,
    gate: &Permit,
) -> DbResult<()> {
    let writer: tantivy::IndexWriter = index
        .writer(50_000_000)
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    for id in changed {
        writer.delete_term(Term::from_field_text(layout.id_field, id));
    }
    for (at, doc) in added.iter().enumerate() {
        if at.is_multiple_of(INDEX_BATCH) {
            gate.check()?;
        }
        writer
            .add_document(tantivy_doc(
                &doc.id,
                &doc.fields,
                (layout.id_field, layout.fields_field),
                &layout.fields,
            ))
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    }
    finish_writer(writer, last_seq)
}

/// Bring the cached index up to date with the changes since its
/// `last_seq`, re-indexing only the documents that changed.
async fn update_index(
    db: &Db,
    spec: &SearchSpec,
    limits: crate::view::ViewLimits,
    gate: Permit,
    mut entry: OwnedMutexGuard<Option<Cached>>,
) -> DbResult<Update> {
    let since = entry
        .as_ref()
        .map_or(0, |cached| cached.snapshot.meta.last_seq);
    // Read the bound once and record it: every sequence up to it was
    // committed before it was read, so a gap left by a write that never
    // finished does not keep the index behind.
    let last_seq = db.changes_watermark(since, None);
    let (changed, live) = indexable_changes(db, since, last_seq).await?;
    let source = spec.source.clone();
    let placement = (db.q, db.partitioned);
    // As in a rebuild, the permits travel with the blocking work so a
    // cancelled request cannot let cleanup or another update race it.
    tokio::task::spawn_blocking(move || {
        // Taken out while it changes: if anything below fails, the next query
        // finds no cached index and reloads it from disk.
        gate.check()?;
        let Some(Cached { reader, snapshot }) = entry.take() else {
            return Ok(Update::Rebuild(gate, entry));
        };
        let tantivy_index = snapshot.index;
        // Free of copying unless a query still holds the previous snapshot.
        let mut meta = Arc::unwrap_or_clone(snapshot.meta);
        let indexed = index_in_batches(&source, &live, limits, &gate)?;
        for id in &changed {
            if let Some(old) = meta.docs.remove(id) {
                meta.kinds.remove(&old);
            }
        }
        meta.kinds.refresh(meta.docs.values());
        let mut added = Vec::with_capacity(indexed.len());
        for doc in indexed {
            let doc = Arc::new(SearchDoc::new(doc.id, doc.fields, placement));
            // Each changed document appears once, and was removed above.
            meta.docs.insert(doc.id.clone(), doc.clone());
            meta.kinds.add(&doc);
            added.push(doc);
        }
        let kinds = meta.kinds.kinds();
        let Some(layout) = SchemaLayout::read(&tantivy_index.schema()) else {
            return Ok(Update::Rebuild(gate, entry));
        };
        if kinds != layout.kinds() {
            return Ok(Update::Rebuild(gate, entry));
        }
        write_changes(&tantivy_index, &layout, &changed, &added, last_seq, &gate)?;
        meta.last_seq = last_seq;
        reader
            .reload()
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
        let snapshot = Snapshot::new(meta, tantivy_index, reader.searcher());
        *entry = Some(Cached {
            reader,
            snapshot: snapshot.clone(),
        });
        Ok(Update::Done(snapshot, gate))
    })
    .await
    .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?
}

/// An index's files, written without fsync. A search index is derived from
/// the database: one a crash damages fails its checksums when it is next
/// opened and is rebuilt, so a commit does not wait for the disk. (The
/// database's own storage does not sync either.)
#[derive(Clone, Debug)]
struct UnsyncedDirectory {
    inner: MmapDirectory,
    root: PathBuf,
}

impl UnsyncedDirectory {
    fn open(root: &Path) -> Result<Self, OpenDirectoryError> {
        Ok(UnsyncedDirectory {
            inner: MmapDirectory::open(root)?,
            root: root.to_path_buf(),
        })
    }
}

struct UnsyncedFile(std::fs::File);

impl std::io::Write for UnsyncedFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        std::io::Write::write(&mut self.0, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::Write::flush(&mut self.0)
    }
}

impl TerminatingWrite for UnsyncedFile {
    fn terminate_ref(&mut self, _: AntiCallToken) -> std::io::Result<()> {
        std::io::Write::flush(&mut self.0)
    }
}

impl Directory for UnsyncedDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.inner.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.inner.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.inner.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.root.join(path))
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    OpenWriteError::FileAlreadyExists(path.to_path_buf())
                } else {
                    OpenWriteError::wrap_io_error(error, path.to_path_buf())
                }
            })?;
        Ok(std::io::BufWriter::new(Box::new(UnsyncedFile(file))))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.inner.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        // Replace by rename, so a reader sees the old or the new file whole.
        // A crash can leave the temporary file behind; a rebuild or cleanup
        // removes the whole directory.
        static TEMPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let target = self.root.join(path);
        let temp = self.root.join(format!(
            ".{}.{}.tmp",
            std::process::id(),
            TEMPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::write(&temp, data)?;
        std::fs::rename(&temp, target)
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        self.inner.acquire_lock(lock)
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.inner.watch(watch_callback)
    }
}

fn build_gate(db: &Db) -> Arc<Gate> {
    gates().get(&db.path)
}

fn gates() -> &'static Registry<Gate> {
    static GATES: Registry<Gate> = Registry::new();
    &GATES
}

/// What a query reads from an index: replaced as a whole when the index
/// changes, so a query keeps a consistent view while an update runs.
#[derive(Clone)]
struct Snapshot {
    meta: Arc<SearchMeta>,
    index: Index,
    searcher: Searcher,
}

impl Snapshot {
    fn new(meta: SearchMeta, index: Index, searcher: Searcher) -> Self {
        Snapshot {
            meta: Arc::new(meta),
            index,
            searcher,
        }
    }
}

/// An index kept open between queries, so a query neither re-reads the
/// metadata file nor reopens the index.
struct Cached {
    reader: IndexReader,
    snapshot: Snapshot,
}

impl Cached {
    fn open(index: Index, meta: SearchMeta) -> DbResult<Self> {
        // Reloaded by hand after each commit, which also avoids the thread
        // that watches the index's meta file for an automatic reload.
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
        let snapshot = Snapshot::new(meta, index, reader.searcher());
        Ok(Cached { reader, snapshot })
    }

    /// Open an index from disk, or `None` if it has to be rebuilt.
    fn load(path: &Path, placement: (u32, bool)) -> Option<Self> {
        let mut meta = read_meta(path)?;
        let index = Index::open(UnsyncedDirectory::open(path).ok()?).ok()?;
        // Files a crash left incomplete fail their checksums. Every file of
        // the committed segments is checked, not only those `.managed.json`
        // lists: it is written before the commit and may have been lost.
        let metas = index.load_metas().ok()?;
        let directory = index.directory();
        for file in metas.list_segment_files() {
            if directory.exists(&file).ok()? && !directory.validate_checksum(&file).ok()? {
                return None;
            }
        }
        meta.last_seq = metas.payload?.parse().ok()?;
        let schema = index.schema();
        let id_field = schema.get_field(ID_FIELD).ok()?;
        let fields_field = schema.get_field(FIELDS_FIELD).ok()?;
        let reader: IndexReader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .ok()?;
        let searcher = reader.searcher();
        let text = |document: &TantivyDocument, field: Field| match document
            .get_first(field)
            .map(|value| owned_value_to_json(value.into()))
        {
            Some(Value::String(text)) => Some(text),
            _ => None,
        };
        for address in searcher.search(&AllQuery, &DocSetCollector).ok()? {
            let document: TantivyDocument = searcher.doc(address).ok()?;
            let fields = serde_json::from_str(&text(&document, fields_field)?).ok()?;
            let doc = Arc::new(SearchDoc::new(
                text(&document, id_field)?,
                fields,
                placement,
            ));
            meta.kinds.add(&doc);
            meta.docs.insert(doc.id.clone(), doc);
        }
        Cached::open(index, meta).ok()
    }
}

type Entry = Arc<AsyncMutex<Option<Cached>>>;

fn registry() -> &'static Registry<AsyncMutex<Option<Cached>>> {
    static ENTRIES: Registry<AsyncMutex<Option<Cached>>> = Registry::new();
    &ENTRIES
}

/// The per-index entry. Its lock serialises updates and rebuilds of the index.
/// The registry only evicts an entry nobody holds, so a query or update in
/// flight keeps its index; opening it is done under the entry's own lock, not
/// the registry's.
fn index_entry(path: &Path) -> Entry {
    registry().get(path)
}

/// Drop cached indexes when a database is deleted or recreated, and the
/// database's gate with them. Signals the gate, so a build or query in flight
/// fails with not_found, then takes its write side; see `view::forget_db`.
pub async fn forget_db(db_path: &Path) {
    let gate = gates().get(db_path);
    let _gate = gate.forget().await;
    drop_cached(db_path);
    gates().retain(|path| path != db_path);
}

/// Drop the cached indexes of a database. The caller holds its gate.
fn drop_cached(db_path: &Path) {
    let prefix = db_path.join("_search_indexes");
    registry().retain(|path| !path.starts_with(&prefix));
}

async fn ensure_index(
    db: &Db,
    ddoc: &str,
    index_name: &str,
    spec: &SearchSpec,
    limits: crate::view::ViewLimits,
    gate: Permit,
) -> DbResult<(Snapshot, Permit)> {
    let path = index_dir(db, ddoc, index_name);
    let signature = design_signature(spec);
    // A query queued behind a long build leaves when the database is deleted.
    let mut entry = gate.until(index_entry(&path).lock_owned()).await?;
    // Check after acquiring the per-index lock: another request may have
    // completed an update or rebuild while this one waited.
    if entry.is_none() {
        // Opening reads every segment file and stored document.
        let load = path.clone();
        let placement = (db.q, db.partitioned);
        *entry = tokio::task::spawn_blocking(move || Cached::load(&load, placement))
            .await
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    }
    if let Some(cached) = entry.as_ref() {
        let meta = &cached.snapshot.meta;
        if meta.version == SEARCH_VERSION && meta.design == signature {
            let current = db.changes_watermark(meta.last_seq, None);
            if meta.last_seq == current {
                return Ok((cached.snapshot.clone(), gate));
            }
            if meta.last_seq < current {
                return match update_index(db, spec, limits, gate, entry).await? {
                    Update::Done(snapshot, gate) => Ok((snapshot, gate)),
                    Update::Rebuild(gate, entry) => {
                        build_index(db, ddoc, index_name, spec, limits, gate, entry).await
                    }
                };
            }
        }
    }
    build_index(db, ddoc, index_name, spec, limits, gate, entry).await
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
    let (lower_inclusive, rest) = if let Some(rest) = range.strip_prefix('[') {
        (true, rest)
    } else if let Some(rest) = range.strip_prefix('{') {
        (false, rest)
    } else {
        return false;
    };
    let (upper_inclusive, inner) = if let Some(inner) = rest.strip_suffix(']') {
        (true, inner)
    } else if let Some(inner) = rest.strip_suffix('}') {
        (false, inner)
    } else {
        return false;
    };
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
    const PUNCTUATION: &[char] = &['+', '-', '"', '~', '*', '?', '[', ']', '{', '}', ':', '^'];
    query
        .split_whitespace()
        .flat_map(|token| token.split(['(', ')']))
        .filter_map(|token| {
            let token = token.trim_matches(PUNCTUATION);
            if token.is_empty()
                || token == "TO"
                || token == "AND"
                || token == "OR"
                || token == "NOT"
            {
                return None;
            }
            // A quoted phrase after a field name (field:"a b") keeps its
            // opening quote once the field is split off.
            let token = token.rsplit(':').next().unwrap_or(token);
            let token = token.trim_matches(PUNCTUATION);
            if token.is_empty() {
                return None;
            }
            // Same per-character lowercasing as highlight_fragment applies to the text.
            Some(token.chars().flat_map(char::to_lowercase).collect())
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
    // Lowercasing can change a character's length ("İ" grows, "ẞ" shrinks),
    // so each byte of the lowercase text remembers the span of the original
    // character it came from.
    let mut lower = String::with_capacity(text.len());
    let mut origin: Vec<(usize, usize)> = Vec::with_capacity(text.len());
    for (at, ch) in text.char_indices() {
        let span = (at, at + ch.len_utf8());
        for lowered in ch.to_lowercase() {
            lower.push(lowered);
            origin.extend(std::iter::repeat_n(span, lowered.len_utf8()));
        }
    }
    let mut fragments = Vec::new();
    for term in terms {
        if term.is_empty() || fragments.len() >= number {
            continue;
        }
        let Some(found) = lower.find(term) else {
            continue;
        };
        let (Some(first), Some(last)) = (
            origin.get(found),
            origin.get((found + term.len()).saturating_sub(1)),
        ) else {
            continue;
        };
        let (index, term_end) = (first.0, last.1);
        let start = index.saturating_sub(size / 2);
        let end = (term_end + size / 2).min(text.len());
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
        fragment.push_str(&text[index..term_end]);
        fragment.push_str(post);
        fragment.push_str(&text[term_end..end]);
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

fn timeout_error() -> ApiError {
    ApiError::new(
        500,
        "timeout",
        "The request could not be processed in a reasonable amount of time.",
    )
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
        return Err(timeout_error());
    }
    let deadline = started + Duration::from_millis(request.timeout_ms);
    if request.partition.is_some() && request.limit > PAGE_LIMIT {
        return Err(ApiError::bad_request(
            "Limit is too large for a partitioned search.",
        ));
    }
    let spec = load_spec(db, ddoc, index_name).await?;
    // Hold the read permit for the whole query, not just the rebuild, so a
    // concurrent `_search_cleanup` cannot remove the index under an open
    // searcher.
    let gate = build_gate(db).read().await?;
    db.check_live()?;
    let (snapshot, gate) = ensure_index(db, ddoc, index_name, &spec, limits, gate).await?;
    if started.elapsed().as_millis() as u64 > request.timeout_ms {
        return Err(timeout_error());
    }
    let schema = snapshot.index.schema();

    let Some(query) = parse_query(&snapshot, &schema, request.query.trim())? else {
        return Ok(empty_search_response(request));
    };
    // Scanning the hits is CPU-bound and can be long, so it runs on the
    // blocking pool, which keeps the permit as a rebuild does.
    let scan_request = request.clone();
    let (scanned, gate) = tokio::task::spawn_blocking(move || {
        let scanned = scan(&snapshot, &scan_request, &*query, deadline);
        (scanned, gate)
    })
    .await
    .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    // The scan is over and the snapshot with it: reading the documents for the
    // page does not use the index files, so a delete need not wait for it.
    drop(gate);
    let Scan {
        total_rows,
        offset,
        page,
        facets,
    } = scanned?;
    let rows = page_rows(db, request, &page).await?;
    let next = offset.saturating_add(page.len());
    let mut response = json!({
        "total_rows": total_rows,
        "bookmark": format!("search:{next}"),
        "rows": rows,
    });
    if let (Some(response), Value::Object(facets)) = (response.as_object_mut(), facets) {
        response.extend(facets);
    }
    Ok(response)
}

/// What a query found: the requested page of the matches, and the facets
/// computed over all of them.
struct Scan {
    total_rows: usize,
    offset: usize,
    page: Vec<Arc<SearchDoc>>,
    facets: Value,
}

fn scan(
    snapshot: &Snapshot,
    request: &SearchRequest,
    query: &dyn tantivy::query::Query,
    deadline: Instant,
) -> DbResult<Scan> {
    let matched = collect_matches(snapshot, request, query, deadline)?;
    let offset = parse_bookmark(request.bookmark.as_deref());
    let limit = if request.limit == 0 {
        0
    } else {
        request.limit.min(PAGE_LIMIT)
    };
    let page = matched.iter().skip(offset).take(limit).cloned().collect();
    let mut facets = json!({});
    add_facets(&mut facets, request, &matched);
    Ok(Scan {
        total_rows: matched.len(),
        offset,
        page,
        facets,
    })
}

/// Parse the query text. `None` means a field the query names does not exist
/// in this node's index, which is no local matches rather than an error.
fn parse_query(
    snapshot: &Snapshot,
    schema: &Schema,
    query_text: &str,
) -> DbResult<Option<Box<dyn tantivy::query::Query>>> {
    if query_text == "*:*" || query_text.is_empty() {
        return Ok(Some(Box::new(AllQuery)));
    }
    let default_fields: Vec<Field> = schema
        .fields()
        .filter(|(_, field)| field.name() != ID_FIELD && field.name() != FIELDS_FIELD)
        .map(|(field, _)| field)
        .collect();
    let parser = QueryParser::for_index(&snapshot.index, default_fields);
    match parser.parse_query(query_text) {
        Ok(query) => Ok(Some(query)),
        // A node may hold no local documents with an indexed field even
        // though another shard does. Treat an absent field as no local
        // matches so cluster fan-out can merge other shards' rows.
        Err(error) if error.to_string().contains("Field does not exist") => Ok(None),
        Err(error) => Err(ApiError::bad_request(format!(
            "invalid search query: {error}"
        ))),
    }
}

/// Run the query and keep the documents this request may see, sorted. Each
/// hit's id is read from the index's fast field, not from its stored fields,
/// and the deadline is checked as the hits are read.
fn collect_matches(
    snapshot: &Snapshot,
    request: &SearchRequest,
    query: &dyn tantivy::query::Query,
    deadline: Instant,
) -> DbResult<Vec<Arc<SearchDoc>>> {
    let index_error =
        |error: tantivy::TantivyError| ApiError::new(500, "search_index_error", error.to_string());
    let searcher = &snapshot.searcher;
    let weight = query
        .weight(EnableScoring::disabled_from_searcher(searcher))
        .map_err(index_error)?;
    let partition = request
        .partition
        .as_ref()
        .map(|partition| format!("{partition}:"));
    let keep = |doc: &SearchDoc| {
        partition
            .as_ref()
            .is_none_or(|partition| doc.id.starts_with(partition.as_str()))
            && request
                .owner_shards
                .as_ref()
                .is_none_or(|owner_shards| owner_shards.covers(doc.shard))
            && matches_drilldown(doc, &request.drilldown)
    };
    let mut matched: Vec<Arc<SearchDoc>> = Vec::new();
    for reader in searcher.segment_readers() {
        let Some(ids) = reader.fast_fields().str(ID_FIELD).map_err(index_error)? else {
            continue;
        };
        let alive = reader.alive_bitset();
        // The hits' id ordinals first, which is cheap, so the ids themselves
        // can be looked up together in one pass over the dictionary.
        let mut ords: Vec<u64> = Vec::new();
        let mut timed_out = false;
        weight
            .for_each_no_score(reader, &mut |docs| {
                if timed_out || Instant::now() > deadline {
                    timed_out = true;
                    return;
                }
                for &doc in docs {
                    if alive.is_some_and(|alive| !alive.is_alive(doc)) {
                        continue;
                    }
                    ords.extend(ids.ords().first(doc));
                }
            })
            .map_err(index_error)?;
        ords.sort_unstable();
        ords.dedup();
        let mut looked_up = 0_usize;
        ids.dictionary()
            .sorted_ords_to_term_cb(&ords, |term| {
                looked_up += 1;
                if timed_out || (looked_up.is_multiple_of(1024) && Instant::now() > deadline) {
                    timed_out = true;
                    return;
                }
                let doc = std::str::from_utf8(term)
                    .ok()
                    .and_then(|id| snapshot.meta.docs.get(id));
                if let Some(doc) = doc.filter(|doc| keep(doc)) {
                    matched.push(Arc::clone(doc));
                }
            })
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
        if timed_out {
            return Err(timeout_error());
        }
    }
    if request.sort.is_empty() {
        matched.sort_by(|a, b| a.id.cmp(&b.id));
    } else {
        matched.sort_by(|a, b| sort_compare(a, b, &request.sort));
    }
    if Instant::now() > deadline {
        return Err(timeout_error());
    }
    Ok(matched)
}

/// Build the result rows for one page, with documents and highlights.
async fn page_rows(
    db: &Db,
    request: &SearchRequest,
    page: &[Arc<SearchDoc>],
) -> DbResult<Vec<Value>> {
    let docs = if request.include_docs {
        let ids: Vec<String> = page.iter().map(|doc| doc.id.clone()).collect();
        crate::view::docs_by_ids(db, false, &ids).await?
    } else {
        HashMap::new()
    };

    let terms = query_terms(&request.query);
    let mut rows = Vec::with_capacity(page.len());
    for doc in page {
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
    Ok(rows)
}

/// Add `counts`, `ranges` and `groups`, computed over all matches.
fn add_facets(response: &mut Value, request: &SearchRequest, matched: &[Arc<SearchDoc>]) {
    if !request.counts.is_empty() {
        let mut counts = Map::new();
        for field in &request.counts {
            let mut values: BTreeMap<String, u64> = BTreeMap::new();
            for doc in matched {
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
        for doc in matched {
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
}

pub async fn info(
    db: &Db,
    ddoc: &str,
    index_name: &str,
    limits: crate::view::ViewLimits,
) -> DbResult<Value> {
    let spec = load_spec(db, ddoc, index_name).await?;
    let gate = build_gate(db).read().await?;
    db.check_live()?;
    let (snapshot, _gate) = ensure_index(db, ddoc, index_name, &spec, limits, gate).await?;
    let meta = &snapshot.meta;
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
    let _gate = build_gate(db).write().await?;
    db.check_live()?;
    drop_cached(&db.path);
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
        let Some(doc_count) = UnsyncedDirectory::open(&path)
            .ok()
            .and_then(|directory| Index::open(directory).ok())
            .and_then(|index| index.searchable_segment_metas().ok())
            .map(|segments| {
                segments
                    .iter()
                    .map(|segment| segment.num_docs())
                    .sum::<u32>()
            })
        else {
            continue;
        };
        out.push(json!({
            "ddoc": meta.ddoc,
            "name": meta.name,
            "ok": true,
            "info": {
                "doc_count": doc_count,
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

    #[test]
    fn highlights_follow_the_original_text_when_lowercasing_changes_its_length() {
        // "İ" (2 bytes) lowercases to "i" plus a combining dot (3 bytes), and
        // "ẞ" (3 bytes) to "ß" (2 bytes), so offsets found in the lowercased
        // text do not apply to the original.
        for (text, term, expected) in [
            ("İstanbul foo bar", "foo", "İstanbul <b>foo</b> bar"),
            ("ẞ foo bar", "foo", "ẞ <b>foo</b> bar"),
            ("İİİİ foo", "foo", "İİİİ <b>foo</b>"),
            ("Straße FOO", "foo", "Straße <b>FOO</b>"),
            ("İ", "i", "<b>İ</b>"),
        ] {
            let terms = vec![term.to_string()];
            let fragments = highlight_fragment(text, &terms, "<b>", "</b>", 100, 1);
            assert_eq!(fragments, vec![expected.to_string()], "{text}");
        }
        // Small fragments must still cut on character boundaries.
        let terms = vec!["foo".to_string()];
        let fragments = highlight_fragment("İİİİİİ foo İİİİİİ", &terms, "<b>", "</b>", 5, 1);
        assert_eq!(fragments.len(), 1);
        assert!(fragments[0].contains("<b>foo</b>"), "{fragments:?}");
    }

    #[test]
    fn query_terms_lowercase_like_the_highlighted_text() {
        assert_eq!(query_terms("Apple +BANANA"), ["apple", "banana"]);
        assert_eq!(query_terms("item:Ünï"), ["ünï"]);
        assert_eq!(query_terms("ẞ"), ["ß"]);
        // "İ" lowercases to "i" plus a combining dot, as in the highlighted text.
        assert_eq!(query_terms("İstanbul"), ["i\u{307}stanbul"]);
    }

    /// `str::to_lowercase` maps a word-final "Σ" to "ς", but tantivy's
    /// `LowerCaser` (the filter in the analyzer that indexes the text)
    /// deliberately lowercases per character, giving "σ". Query terms and
    /// highlighting must agree with the analyzer, not with the std function.
    #[test]
    fn final_sigma_lowercases_as_the_indexing_analyzer_does() {
        let mut analyzer = tantivy::tokenizer::TextAnalyzer::builder(
            tantivy::tokenizer::SimpleTokenizer::default(),
        )
        .filter(tantivy::tokenizer::LowerCaser)
        .build();
        let mut indexed = Vec::new();
        analyzer
            .token_stream("ΟΔΟΣ ΣΟΦΟΣ")
            .process(&mut |token| indexed.push(token.text.clone()));
        assert_eq!(indexed, ["οδοσ", "σοφοσ"]);
        // The std function would give a final sigma, which the analyzer never indexes.
        assert_eq!("ΟΔΟΣ".to_lowercase(), "οδος");
        assert_eq!(query_terms("ΟΔΟΣ"), indexed[..1]);

        let terms = query_terms("ΟΔΟΣ");
        let fragments = highlight_fragment("η ΟΔΟΣ και οδοσ", &terms, "<b>", "</b>", 100, 1);
        assert_eq!(fragments, ["η <b>ΟΔΟΣ</b> και οδοσ"]);
    }

    #[test]
    fn query_terms_strip_quotes_from_field_qualified_phrases() {
        assert_eq!(query_terms("item:\"CAFÉ\""), ["café"]);
        assert_eq!(query_terms("item:\"café ünï\""), ["café", "ünï"]);
        assert_eq!(query_terms("+item:\"Foo Bar\" baz"), ["foo", "bar", "baz"]);
        assert_eq!(query_terms("\"foo bar\""), ["foo", "bar"]);
    }

    #[test]
    fn highlights_find_upper_case_non_ascii_terms() {
        for (text, query, expected) in [
            ("ünï café", "Ünï", "<b>ünï</b> café"),
            ("x İSTANBUL y", "İstanbul", "x <b>İSTANBUL</b> y"),
            ("a ẞ b", "ẞ", "a <b>ẞ</b> b"),
            ("Mixed Case", "mIxEd", "<b>Mixed</b> Case"),
        ] {
            let terms = query_terms(query);
            let fragments = highlight_fragment(text, &terms, "<b>", "</b>", 100, 1);
            assert_eq!(fragments, vec![expected.to_string()], "{text}");
        }
    }

    #[test]
    fn value_in_range_ignores_multibyte_bracket_positions() {
        let value = serde_json::json!("a");
        assert!(!value_in_range(&value, "é"));
        assert!(!value_in_range(&value, "[é TO z]é"));
        assert!(!value_in_range(&value, "é[a TO z]"));
    }

    /// Every index ever searched used to stay open, with all its stored
    /// documents, until the database was deleted.
    #[tokio::test]
    async fn open_search_indexes_stay_within_the_cap() {
        let _cap = crate::index_cache::CapGuard::new(8).await;
        let root = std::env::temp_dir().join(format!("douchdb-search-cap-{}", crate::util::uuid()));
        let db = Db::open(
            &root,
            "searchcap",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        db.put_docs(vec![json!({"_id": "a", "name": "apple"})], true)
            .await
            .unwrap();
        let limits = crate::view::ViewQuery::default().limits;
        for n in 0..20 {
            let ddoc = format!("_design/s{n}");
            db.put_docs(
                vec![json!({"_id": ddoc, "indexes": {"idx": {
                    "index": "function(doc){ if (doc.name) index('name', doc.name); }"}}})],
                true,
            )
            .await
            .unwrap();
            let request = SearchRequest {
                query: "name:apple".to_string(),
                limit: 10,
                ..SearchRequest::default()
            };
            let result = query(&db, &ddoc, "idx", &request, limits).await.unwrap();
            assert_eq!(result["total_rows"], 1, "{result}");
        }
        let prefix = db.path.join("_search_indexes");
        let open = registry().count_under(&prefix);
        assert!(open <= 8, "{open} search indexes are open, cap is 8");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Deleting or recreating a database must wait for a search build or query
    /// that is using its indexes, and must leave no gate behind.
    #[tokio::test]
    async fn forget_db_waits_for_a_search_in_flight() {
        let root =
            std::env::temp_dir().join(format!("douchdb-search-forget-{}", crate::util::uuid()));
        let db = Db::open(
            &root,
            "searchforget",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        db.put_docs(
            vec![
                json!({"_id": "a", "name": "apple"}),
                json!({"_id": "_design/s", "indexes": {"idx": {
                    "index": "function(doc){ if (doc.name) index('name', doc.name); }"}}}),
            ],
            true,
        )
        .await
        .unwrap();
        let limits = crate::view::ViewQuery::default().limits;
        let request = SearchRequest {
            query: "name:apple".to_string(),
            limit: 10,
            ..SearchRequest::default()
        };
        query(&db, "_design/s", "idx", &request, limits)
            .await
            .unwrap();
        let prefix = db.path.join("_search_indexes");
        assert_eq!(registry().count_under(&prefix), 1);

        // A build or query holds the read side of the gate.
        let reader = build_gate(&db).read().await.unwrap();
        let path = db.path.clone();
        let forget = forget_db(&path);
        tokio::pin!(forget);
        // One poll is enough: the only thing forget_db awaits is the gate.
        let done = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(forget.as_mut(), cx).is_ready())
        })
        .await;
        assert!(!done, "forget_db finished while a build held the gate");
        assert_eq!(
            registry().count_under(&prefix),
            1,
            "index dropped under a reader"
        );

        drop(reader);
        forget.await;
        assert_eq!(registry().count_under(&prefix), 0);
        assert_eq!(
            gates().count_under(&db.path),
            0,
            "gate outlived its database"
        );
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Whether a query holds the read side of the database's gate.
    fn gate_is_held(db: &Db) -> bool {
        build_gate(db).is_held()
    }

    /// Deleting a database must not wait for a search build: the build is
    /// cancelled and its query fails with not_found, as CouchDB shuts the
    /// index processes down with the database. The build is held at the index
    /// entry's lock, where it waits with the gate's read permit; nothing here
    /// depends on timing, and the timeout only turns a hang into a failure.
    #[tokio::test]
    async fn forget_db_does_not_wait_for_a_search_build() {
        let root =
            std::env::temp_dir().join(format!("douchdb-search-cancel-{}", crate::util::uuid()));
        let config = || crate::db::DbConfig {
            q: 1,
            revs_limit: 1000,
            shard_map: crate::cluster::ShardMap::single(1),
            partitioned: false,
        };
        let ddoc = json!({"_id": "_design/s", "indexes": {"idx": {
            "index": "function(doc){ if (doc.name) index('name', doc.name); }"}}});
        let db = Arc::new(Db::open(&root, "searchcancel", config()).unwrap());
        db.put_docs(
            vec![json!({"_id": "a", "name": "apple"}), ddoc.clone()],
            true,
        )
        .await
        .unwrap();
        let limits = crate::view::ViewQuery::default().limits;
        let request = SearchRequest {
            query: "name:apple".to_string(),
            limit: 10,
            ..SearchRequest::default()
        };
        let held = index_entry(&index_dir(&db, "_design/s", "idx"))
            .lock_owned()
            .await;
        let build = {
            let (db, request) = (db.clone(), request.clone());
            tokio::spawn(async move { query(&db, "_design/s", "idx", &request, limits).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(120), async {
            while !gate_is_held(&db) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the build never took the gate");

        let path = db.path.clone();
        tokio::time::timeout(std::time::Duration::from_secs(120), forget_db(&path))
            .await
            .expect("forget_db waited for a search build in flight");
        let error = tokio::time::timeout(std::time::Duration::from_secs(120), build)
            .await
            .expect("the build did not end")
            .unwrap()
            .expect_err("the database was deleted under the query");
        assert_eq!(error.status, 404, "{}", error.reason);
        assert_eq!(error.error, "not_found");
        drop(held);
        assert_eq!(gates().count_under(&path), 0, "gate outlived its database");

        // A database recreated under the name has a fresh gate and serves.
        drop(db);
        std::fs::remove_dir_all(&root).unwrap();
        let db = Db::open(&root, "searchcancel", config()).unwrap();
        db.put_docs(vec![json!({"_id": "b", "name": "apple"}), ddoc], true)
            .await
            .unwrap();
        let result = query(&db, "_design/s", "idx", &request, limits)
            .await
            .unwrap();
        assert_eq!(result["total_rows"], 1, "{result}");
        assert_eq!(result["rows"][0]["id"], "b", "{result}");
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A build running when its database is deleted stops at its next batch
    /// check instead of indexing the rest of the documents.
    #[tokio::test]
    async fn a_build_stops_between_batches_once_the_database_is_deleted() {
        let path = std::env::temp_dir().join(format!("douchdb-batches-{}", crate::util::uuid()));
        let gate = gates().get(&path);
        let permit = gate.read().await.unwrap();
        let docs = vec![("a".to_string(), r#"{"_id":"a","name":"x"}"#.to_string())];
        let source = "function(doc){ index('name', doc.name); }";
        let limits = crate::view::ViewQuery::default().limits;
        assert_eq!(
            index_in_batches(source, &docs, limits, &permit)
                .unwrap()
                .len(),
            1
        );

        // The first poll signals the deletion; it then waits for the permit.
        let forget = gate.forget();
        tokio::pin!(forget);
        let done = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(forget.as_mut(), cx).is_ready())
        })
        .await;
        assert!(!done, "the delete finished under a permit");
        let error = index_in_batches(source, &docs, limits, &permit)
            .map(|indexed| indexed.len())
            .expect_err("the build went on after the delete");
        assert_eq!(error.status, 404);
        // Readers of the signalled gate are refused, and so are queued ones.
        assert_eq!(gate.read().await.err().map(|error| error.status), Some(404));
        drop(permit);
        drop(forget.await);
    }

    /// A small deterministic generator, so a failing sequence can be replayed.
    struct Lcg(u64);

    impl Lcg {
        fn below(&mut self, bound: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 33) % bound
        }
    }

    /// The kinds a rebuild infers: the documents' fields in (shard, id)
    /// order, and a field's kind decided as each value is met.
    fn folded_kinds(docs: &[Arc<SearchDoc>]) -> BTreeMap<String, FieldKind> {
        let mut ordered: Vec<&Arc<SearchDoc>> = docs.iter().collect();
        ordered.sort_by(|a, b| (a.shard, &a.id).cmp(&(b.shard, &b.id)));
        let mut kinds: BTreeMap<String, FieldKind> = BTreeMap::new();
        for doc in ordered {
            for (name, value) in &doc.fields {
                let kind = value_kind(value);
                match kinds.get_mut(name) {
                    None => {
                        kinds.insert(name.clone(), kind);
                    }
                    Some(existing) => {
                        if !matches!(
                            (*existing, kind),
                            (FieldKind::Text, _)
                                | (_, FieldKind::Text)
                                | (FieldKind::Number, FieldKind::Number)
                                | (FieldKind::Bool, FieldKind::Bool)
                        ) {
                            *existing = FieldKind::Text;
                        }
                    }
                }
            }
        }
        kinds
    }

    fn random_fields(rng: &mut Lcg) -> Map<String, Value> {
        let mut fields = Map::new();
        for name in ["a", "b", "c"] {
            match rng.below(6) {
                0 => fields.insert(name.to_string(), json!(rng.below(100))),
                1 => fields.insert(name.to_string(), json!(rng.below(2) == 0)),
                2 => fields.insert(name.to_string(), json!(format!("w{}", rng.below(5)))),
                _ => None,
            };
        }
        fields
    }

    #[test]
    fn kind_tallies_follow_documents_as_they_come_and_go() {
        let placement = (3, false);
        for seed in 0..200 {
            let mut rng = Lcg(seed);
            let mut docs: HashMap<String, Arc<SearchDoc>> = HashMap::new();
            let mut stats = KindStats::default();
            for _ in 0..60 {
                let id = format!("d{}", rng.below(12));
                if let Some(old) = docs.remove(&id) {
                    stats.remove(&old);
                }
                stats.refresh(docs.values());
                if rng.below(3) > 0 {
                    let doc = Arc::new(SearchDoc::new(
                        id.clone(),
                        random_fields(&mut rng),
                        placement,
                    ));
                    stats.add(&doc);
                    docs.insert(id, doc);
                }
                let live: Vec<Arc<SearchDoc>> = docs.values().cloned().collect();
                assert_eq!(stats.kinds(), folded_kinds(&live), "seed {seed}");
            }
        }
    }

    #[tokio::test]
    async fn updated_indexes_match_a_rebuild_over_random_writes() {
        let root = std::env::temp_dir().join(format!(
            "douchdb-search-random-{}-{}",
            std::process::id(),
            crate::util::uuid()
        ));
        let db = Db::open(
            &root,
            "searchrandom",
            crate::db::DbConfig {
                q: 3,
                revs_limit: 1000,
                shard_map: crate::cluster::ShardMap::single(3),
                partitioned: false,
            },
        )
        .unwrap();
        let index = "function(doc){ for (const f of ['a', 'b', 'c']) { \
                     if (doc[f] !== undefined) index(f, doc[f]); } }";
        for ddoc in ["_design/a", "_design/b"] {
            db.put_docs(
                vec![json!({"_id": ddoc, "indexes": {"idx": {"index": index}}})],
                true,
            )
            .await
            .unwrap();
        }
        let limits = crate::view::ViewQuery::default().limits;
        let mut revs: HashMap<String, String> = HashMap::new();
        let mut rng = Lcg(7);
        // A rebuild removes the marker; steps that keep it were incremental.
        let marker = index_dir(&db, "_design/a", "idx").join("douchdb-test-marker");
        let mut incremental = 0;
        for step in 0..60 {
            let mut batch: HashMap<String, Value> = HashMap::new();
            for _ in 0..1 + rng.below(4) {
                let id = format!("d{:02}", rng.below(25));
                let mut doc = if rng.below(4) == 0 {
                    json!({"_deleted": true})
                } else {
                    Value::Object(random_fields(&mut rng))
                };
                doc["_id"] = json!(id);
                if let Some(rev) = revs.get(&id) {
                    doc["_rev"] = json!(rev);
                }
                batch.insert(id, doc);
            }
            for result in db
                .put_docs(batch.into_values().collect(), true)
                .await
                .unwrap()
            {
                if let Some(rev) = result.json()["rev"].as_str() {
                    revs.insert(
                        result.json()["id"].as_str().unwrap().to_string(),
                        rev.to_string(),
                    );
                }
            }
            let rebuilt = index_dir(&db, "_design/b", "idx");
            registry().remove(&rebuilt);
            std::fs::remove_dir_all(rebuilt).ok();
            if step > 0 {
                std::fs::write(&marker, b"").unwrap();
            }
            if step % 5 == 4 {
                registry().remove(&index_dir(&db, "_design/a", "idx"));
            }
            for q in ["*:*", "a:[10 TO 60]", "b:w1", "c:true", "a:w2 OR b:3"] {
                for sort in [
                    vec![],
                    vec![("a".to_string(), true), ("b".to_string(), false)],
                ] {
                    let request = SearchRequest {
                        query: q.to_string(),
                        limit: 200,
                        sort,
                        counts: vec!["b".to_string()],
                        ..SearchRequest::default()
                    };
                    let updated = query(&db, "_design/a", "idx", &request, limits).await;
                    let rebuilt = query(&db, "_design/b", "idx", &request, limits).await;
                    match (updated, rebuilt) {
                        (Ok(updated), Ok(rebuilt)) => {
                            assert_eq!(updated, rebuilt, "step {step} {q}")
                        }
                        (Err(updated), Err(rebuilt)) => {
                            assert_eq!(updated.reason, rebuilt.reason, "step {step} {q}")
                        }
                        (updated, rebuilt) => {
                            panic!("step {step} {q}: {updated:?} against {rebuilt:?}")
                        }
                    }
                }
            }
            incremental += usize::from(marker.exists());
        }
        assert!(
            incremental >= 20,
            "only {incremental} steps were incremental"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    /// A deadline that has passed stops the scan of the first hits, so the
    /// query fails without having read the rest.
    #[tokio::test]
    async fn a_passed_deadline_fails_a_scan_in_progress() {
        let root = std::env::temp_dir().join(format!(
            "douchdb-search-passed-{}-{}",
            std::process::id(),
            crate::util::uuid()
        ));
        let db = Db::open(
            &root,
            "searchpassed",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        let mut docs = vec![json!({"_id": "_design/a", "indexes": {"idx": {
            "index": "function(doc){ if (doc.name) index('name', doc.name); }"}}})];
        docs.extend((0..50).map(|i| json!({"_id": format!("d{i:02}"), "name": "apple"})));
        db.put_docs(docs, true).await.unwrap();
        let limits = crate::view::ViewQuery::default().limits;
        let request = SearchRequest::default();
        query(&db, "_design/a", "idx", &request, limits)
            .await
            .unwrap();
        let spec = load_spec(&db, "_design/a", "idx").await.unwrap();
        let gate = build_gate(&db).read().await.unwrap();
        let (snapshot, _gate) = ensure_index(&db, "_design/a", "idx", &spec, limits, gate)
            .await
            .unwrap();
        let passed = Instant::now() - Duration::from_secs(1);
        let error = collect_matches(&snapshot, &request, &AllQuery, passed)
            .map(|matched| matched.len())
            .expect_err("the deadline passed before the scan began");
        assert_eq!(error.error, "timeout");
        let open = Instant::now() + Duration::from_secs(60);
        let matched = collect_matches(&snapshot, &request, &AllQuery, open).unwrap();
        assert_eq!(matched.len(), 50);
        let _ = std::fs::remove_dir_all(root);
    }

    /// The deadline used to be checked only before and after the index was
    /// made current, so a scan over a large result ran to the end.
    #[tokio::test]
    async fn a_query_deadline_stops_the_scan_of_a_large_result() {
        let root = std::env::temp_dir().join(format!(
            "douchdb-search-deadline-{}-{}",
            std::process::id(),
            crate::util::uuid()
        ));
        let db = Db::open(
            &root,
            "searchdeadline",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        let mut docs = vec![json!({"_id": "_design/a", "indexes": {"idx": {
            "index": "function(doc){ if (doc.name) index('name', doc.name); }"}}})];
        docs.extend((0..20_000).map(|i| json!({"_id": format!("d{i:05}"), "name": "apple"})));
        db.put_docs(docs, true).await.unwrap();
        let limits = crate::view::ViewQuery::default().limits;
        let request = |timeout_ms| SearchRequest {
            query: "*:*".to_string(),
            limit: 10,
            timeout_ms,
            ..SearchRequest::default()
        };
        // The first query builds the index; a later one finds it current.
        let built = query(&db, "_design/a", "idx", &request(60_000), limits)
            .await
            .unwrap();
        assert_eq!(built["total_rows"], 20_000);
        let error = query(&db, "_design/a", "idx", &request(1), limits)
            .await
            .expect_err("a one millisecond deadline cannot cover 20000 hits");
        assert_eq!(error.error, "timeout");
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn updated_indexes_match_a_rebuild_after_each_write() {
        let root = std::env::temp_dir().join(format!(
            "douchdb-search-update-{}-{}",
            std::process::id(),
            crate::util::uuid()
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
                let id = result.json()["id"].as_str().unwrap().to_string();
                revs.insert(id, result.json()["rev"].as_str().unwrap().to_string());
            }
            if step > 0 {
                std::fs::write(&marker, b"").unwrap();
            }
            let rebuilt = index_dir(&db, "_design/b", "idx");
            registry().remove(&rebuilt);
            std::fs::remove_dir_all(rebuilt).ok();
            // Every other step, `a` is read back from disk as after a
            // restart: its sequence and fields come from its last commit.
            if step % 2 == 1 {
                registry().remove(&index_dir(&db, "_design/a", "idx"));
            }
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

    #[tokio::test]
    async fn a_search_index_does_not_move_past_a_write_in_flight() {
        let root = std::env::temp_dir().join(format!(
            "douchdb-search-flight-{}-{}",
            std::process::id(),
            crate::util::uuid()
        ));
        let db = Db::open(
            &root,
            "searchflight",
            crate::db::DbConfig {
                q: 2,
                revs_limit: 1000,
                shard_map: crate::cluster::ShardMap::single(2),
                partitioned: false,
            },
        )
        .unwrap();
        let on_shard = |shard: usize, tag: &str| {
            (0..10_000)
                .map(|i| format!("{tag}{i}"))
                .find(|id| db.shard_index(id) == shard)
                .expect("document id for shard")
        };
        // Design documents on shard 0, so reading them never touches shard 1.
        let (inc, full) = (on_shard(0, "_design/inc"), on_shard(0, "_design/full"));
        let (a, c, e, d) = (
            on_shard(0, "a"),
            on_shard(0, "c"),
            on_shard(0, "e"),
            on_shard(1, "d"),
        );
        let index = "function(doc){ index('name', doc.name); }";
        for ddoc in [&inc, &full] {
            db.put_docs(
                vec![json!({"_id": ddoc, "indexes": {"idx": {"index": index}}})],
                true,
            )
            .await
            .unwrap();
        }
        let limits = crate::view::ViewQuery::default().limits;
        let search = |ddoc: &str| {
            let db = &db;
            let ddoc = ddoc.to_string();
            async move {
                let request = SearchRequest {
                    query: "name:fig".to_string(),
                    limit: 200,
                    ..SearchRequest::default()
                };
                query(db, &ddoc, "idx", &request, limits).await.unwrap()
            }
        };
        let last_seq = |ddoc: &str| {
            let entry = index_entry(&index_dir(&db, ddoc, "idx"));
            async move {
                let guard = entry.lock().await;
                guard.as_ref().map(|cached| cached.snapshot.meta.last_seq)
            }
        };
        let put = |id: &str| db.put_docs(vec![json!({"_id": id, "name": "fig"})], true);
        put(&a).await.unwrap();
        search(&inc).await;
        let built = last_seq(&inc).await.unwrap();

        // Shard 1 holds an allocated, uncommitted sequence while a later
        // write on shard 0 commits. An incremental update must stop short.
        let held = db.start_in_flight_write(1);
        put(&c).await.unwrap();
        search(&inc).await;
        let seq = last_seq(&inc).await.unwrap();
        assert!(
            (built..held).contains(&seq),
            "incremental update recorded last_seq {seq}, past the in-flight sequence {held}"
        );

        // So must a rebuild.
        let held = db.start_in_flight_write(1);
        put(&e).await.unwrap();
        search(&full).await;
        let seq = last_seq(&full).await.unwrap();
        assert!(
            seq < held,
            "rebuild recorded last_seq {seq}, past the in-flight sequence {held}"
        );

        // Once the shard commits, a later write on it reaches both indexes.
        put(&d).await.unwrap();
        for ddoc in [&inc, &full] {
            let result = search(ddoc).await;
            assert_eq!(result["total_rows"], 4, "{ddoc}");
        }
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn a_gap_in_the_sequence_does_not_keep_a_search_index_behind() {
        let root = std::env::temp_dir().join(format!(
            "douchdb-search-gap-{}-{}",
            std::process::id(),
            crate::util::uuid()
        ));
        let db = Db::open(
            &root,
            "searchgap",
            crate::db::DbConfig {
                q: 2,
                revs_limit: 1000,
                shard_map: crate::cluster::ShardMap::single(2),
                partitioned: false,
            },
        )
        .unwrap();
        let on_shard = |shard: usize, tag: &str| {
            (0..10_000)
                .map(|i| format!("{tag}{i}"))
                .find(|id| db.shard_index(id) == shard)
                .expect("document id for shard")
        };
        let ddoc = on_shard(0, "_design/gap");
        let (a, c) = (on_shard(0, "a"), on_shard(0, "c"));
        let index = "function(doc){ index('name', doc.name); }";
        db.put_docs(
            vec![json!({"_id": ddoc, "indexes": {"idx": {"index": index}}})],
            true,
        )
        .await
        .unwrap();
        let limits = crate::view::ViewQuery::default().limits;
        let search = || async {
            let request = SearchRequest {
                query: "name:fig".to_string(),
                limit: 200,
                ..SearchRequest::default()
            };
            query(&db, &ddoc, "idx", &request, limits).await.unwrap()
        };
        db.put_docs(vec![json!({"_id": a, "name": "fig"})], true)
            .await
            .unwrap();
        search().await;

        // A write commits, then a sequence above it is taken and abandoned,
        // as by a write whose flush failed. Any later job on the shard
        // counts the abandoned sequence as settled.
        db.put_docs(vec![json!({"_id": c, "name": "fig"})], true)
            .await
            .unwrap();
        let abandoned = db.start_in_flight_write(1);
        db.get_doc(&on_shard(1, "x"), &Default::default())
            .await
            .ok();
        assert_eq!(db.update_seq(), abandoned);

        let result = search().await;
        assert_eq!(result["total_rows"], 2);
        let entry = index_entry(&index_dir(&db, &ddoc, "idx"));
        let guard = entry.lock().await;
        let recorded = guard.as_ref().map(|cached| cached.snapshot.meta.last_seq);
        assert_eq!(
            recorded,
            Some(abandoned),
            "the index stays behind a sequence no write will commit"
        );
        drop(guard);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn a_damaged_index_is_rebuilt_when_opened() {
        let root = std::env::temp_dir().join(format!(
            "douchdb-search-damaged-{}-{}",
            std::process::id(),
            crate::util::uuid()
        ));
        let db = Db::open(
            &root,
            "searchdamaged",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        let docs: Vec<Value> = (0..20)
            .map(|i| json!({"_id": format!("d{i:02}"), "name": format!("apple{i}")}))
            .chain([json!({"_id": "_design/a", "indexes": {"idx": {"index":
                "function(doc){ if (doc.name) index('name', doc.name); }"}}})])
            .collect();
        db.put_docs(docs, true).await.unwrap();
        let limits = crate::view::ViewQuery::default().limits;
        let request = SearchRequest {
            query: "name:apple7".to_string(),
            ..SearchRequest::default()
        };
        let first = query(&db, "_design/a", "idx", &request, limits)
            .await
            .unwrap();
        assert_eq!(first["total_rows"], 1, "{first}");

        // Flip a byte in the term and postings files, as an interrupted write
        // might leave them, then open the index afresh.
        let path = index_dir(&db, "_design/a", "idx");
        registry().remove(&path);
        let marker = path.join("douchdb-test-marker");
        std::fs::write(&marker, b"").unwrap();
        // The file list a crash may also have lost.
        std::fs::remove_file(path.join(".managed.json")).unwrap();
        for entry in std::fs::read_dir(&path).unwrap().flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            // Not the document store, which opening the index reads anyway.
            if name.ends_with(".idx") || name.ends_with(".term") {
                let mut bytes = std::fs::read(entry.path()).unwrap();
                bytes[0] ^= 0xff;
                std::fs::write(entry.path(), bytes).unwrap();
            }
        }
        let again = query(&db, "_design/a", "idx", &request, limits)
            .await
            .unwrap();
        assert_eq!(again, first);
        assert!(!marker.exists(), "the damaged index was not rebuilt");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn index_entries_are_per_index() {
        let first = index_entry(Path::new("/tmp/douchdb-search-lock-a"));
        let second = index_entry(Path::new("/tmp/douchdb-search-lock-b"));
        assert!(Arc::ptr_eq(
            &first,
            &index_entry(Path::new("/tmp/douchdb-search-lock-a"))
        ));
        assert!(!Arc::ptr_eq(&first, &second));
    }
}

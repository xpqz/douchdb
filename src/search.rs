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
use tantivy::directory::error::{
    DeleteError, LockError, OpenDirectoryError, OpenReadError, OpenWriteError,
};
use tantivy::directory::{
    AntiCallToken, Directory, DirectoryLock, FileHandle, Lock, MmapDirectory, TerminatingWrite,
    WatchCallback, WatchHandle, WritePtr,
};
use tantivy::query::{AllQuery, QueryParser};
use tantivy::schema::{
    Field, FieldType, OwnedValue, Schema, TantivyDocument, FAST, INDEXED, STORED, STRING, TEXT,
};
use tantivy::{Index, IndexReader, IndexSettings, ReloadPolicy, Searcher, Term};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedRwLockReadGuard, RwLock};

const SEARCH_VERSION: u8 = 3;
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
    #[serde(skip)]
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

fn infer_kinds<'a>(
    docs: impl IntoIterator<Item = &'a Map<String, Value>>,
) -> BTreeMap<String, FieldKind> {
    let mut kinds: BTreeMap<String, FieldKind> = BTreeMap::new();
    for fields in docs {
        for (name, value) in fields {
            // The schema's own fields cannot also be index fields.
            if name == ID_FIELD || name == FIELDS_FIELD {
                continue;
            }
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
    mut entry: OwnedMutexGuard<Option<Cached>>,
) -> DbResult<(Snapshot, OwnedRwLockReadGuard<()>)> {
    // Capture the bound before reading the snapshot. A write that lands
    // while the index is being built then leaves the database ahead of the
    // recorded sequence, so the next query rebuilds instead of trusting a
    // snapshot that may have missed it. The bound is the changes watermark: a
    // write in flight has taken a sequence but is not readable yet.
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
        docs: Vec::new(),
    };
    // QuickJS/Tantivy work is CPU- and IO-bound. Run it on the blocking pool so
    // a search rebuild cannot occupy a Tokio async worker for its full duration.
    let (result, gate) = tokio::task::spawn_blocking(move || {
        // Keep both permits in the blocking task, not the request future. A
        // cancelled request must not let cleanup race a live rebuild. The read
        // permit is returned so a query can keep it for its whole lifetime.
        // The old index's files are about to be removed, so forget it first.
        *entry = None;
        let result = build_index_files(path, docs, &source, meta, limits)
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

/// Build the index at `path` and write `meta` with the indexed documents.
fn build_index_files(
    path: PathBuf,
    docs: Vec<(String, String)>,
    source: &str,
    mut meta: SearchMeta,
    limits: crate::view::ViewLimits,
) -> DbResult<(Index, SearchMeta)> {
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

    for doc in &indexed {
        writer
            .add_document(tantivy_doc(
                &doc.id,
                &doc.fields,
                (id_field, fields_field),
                &fields,
            ))
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    }
    finish_writer(writer, meta.last_seq)?;

    meta.docs = indexed
        .into_iter()
        .map(|doc| SearchDoc {
            id: doc.id,
            fields: doc.fields,
        })
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
    Done(Snapshot, OwnedRwLockReadGuard<()>),
    /// Field types inferred over the updated documents differ from the
    /// index's schema, which only a rebuild can change.
    Rebuild(OwnedRwLockReadGuard<()>, OwnedMutexGuard<Option<Cached>>),
}

/// Bring the cached index up to date with the changes since its
/// `last_seq`, re-indexing only the documents that changed.
async fn update_index(
    db: &Db,
    spec: &SearchSpec,
    limits: crate::view::ViewLimits,
    gate: OwnedRwLockReadGuard<()>,
    mut entry: OwnedMutexGuard<Option<Cached>>,
) -> DbResult<Update> {
    let since = entry
        .as_ref()
        .map_or(0, |cached| cached.snapshot.meta.last_seq);
    // Read the bound once and record it: every sequence up to it was
    // committed before it was read, so a gap left by a write that never
    // finished does not keep the index behind.
    let last_seq = db.changes_watermark(since, None);
    let changes = db
        .changes(&crate::db::ChangesOpts {
            since,
            include_docs: true,
            until: Some(last_seq),
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
    let source = spec.source.clone();
    let placement = (db.q, db.partitioned);
    // As in a rebuild, the permits travel with the blocking work so a
    // cancelled request cannot let cleanup or another update race it.
    tokio::task::spawn_blocking(move || {
        // Taken out while it changes: if anything below fails, the next query
        // finds no cached index and reloads it from disk.
        let Some(Cached { reader, snapshot }) = entry.take() else {
            return Ok(Update::Rebuild(gate, entry));
        };
        let tantivy_index = snapshot.index;
        let mut meta = Arc::unwrap_or_clone(snapshot.meta);
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
        let schema = tantivy_index.schema();
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
                _ => return Ok(Update::Rebuild(gate, entry)),
            };
            fields.insert(field_entry.name().to_string(), (field, kind));
        }
        let (Some(id_field), Some(fields_field)) = (id_field, fields_field) else {
            return Ok(Update::Rebuild(gate, entry));
        };
        let schema_kinds: BTreeMap<String, FieldKind> = fields
            .iter()
            .map(|(name, (_, kind))| (name.clone(), *kind))
            .collect();
        if kinds != schema_kinds {
            return Ok(Update::Rebuild(gate, entry));
        }

        let writer: tantivy::IndexWriter = tantivy_index
            .writer(50_000_000)
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
        for id in &changed {
            writer.delete_term(Term::from_field_text(id_field, id));
        }
        for doc in meta.docs.iter().filter(|doc| changed.contains(&doc.id)) {
            writer
                .add_document(tantivy_doc(
                    &doc.id,
                    &doc.fields,
                    (id_field, fields_field),
                    &fields,
                ))
                .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
        }
        finish_writer(writer, last_seq)?;
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

fn build_gate(db: &Db) -> Arc<RwLock<()>> {
    static GATES: OnceLock<Mutex<HashMap<PathBuf, Arc<RwLock<()>>>>> = OnceLock::new();
    let gates = GATES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut gates = gates.lock().unwrap_or_else(|poison| poison.into_inner());
    gates
        .entry(db.path.clone())
        .or_insert_with(|| Arc::new(RwLock::new(())))
        .clone()
}

/// What a query reads from an index: replaced as a whole when the index
/// changes, so a query keeps a consistent view while an update runs.
#[derive(Clone)]
struct Snapshot {
    meta: Arc<SearchMeta>,
    /// Each document's position in `meta.docs`.
    positions: Arc<HashMap<String, usize>>,
    index: Index,
    searcher: Searcher,
}

impl Snapshot {
    fn new(meta: SearchMeta, index: Index, searcher: Searcher) -> Self {
        let positions = meta
            .docs
            .iter()
            .enumerate()
            .map(|(position, doc)| (doc.id.clone(), position))
            .collect();
        Snapshot {
            meta: Arc::new(meta),
            positions: Arc::new(positions),
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
    fn load(path: &Path) -> Option<Self> {
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
            meta.docs.push(SearchDoc {
                id: text(&document, id_field)?,
                fields,
            });
        }
        Cached::open(index, meta).ok()
    }
}

type Entry = Arc<AsyncMutex<Option<Cached>>>;

fn registry() -> std::sync::MutexGuard<'static, HashMap<PathBuf, Entry>> {
    static ENTRIES: OnceLock<Mutex<HashMap<PathBuf, Entry>>> = OnceLock::new();
    ENTRIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

/// The per-index entry. Its lock serialises updates and rebuilds of the index.
fn index_entry(path: &Path) -> Entry {
    registry()
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
        .clone()
}

/// Drop cached indexes when a database is deleted or recreated, or its index
/// files are removed.
pub fn forget_db(db_path: &Path) {
    let prefix = db_path.join("_search_indexes");
    registry().retain(|path, _| !path.starts_with(&prefix));
}

async fn ensure_index(
    db: &Db,
    ddoc: &str,
    index_name: &str,
    spec: &SearchSpec,
    limits: crate::view::ViewLimits,
    gate: OwnedRwLockReadGuard<()>,
) -> DbResult<(Snapshot, OwnedRwLockReadGuard<()>)> {
    let path = index_dir(db, ddoc, index_name);
    let signature = design_signature(spec);
    let mut entry = index_entry(&path).lock_owned().await;
    // Check after acquiring the per-index lock: another request may have
    // completed an update or rebuild while this one waited.
    if entry.is_none() {
        // Opening reads every segment file and stored document.
        let load = path.clone();
        *entry = tokio::task::spawn_blocking(move || Cached::load(&load))
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
    let (snapshot, _gate) = ensure_index(db, ddoc, index_name, &spec, limits, gate).await?;
    if started.elapsed().as_millis() as u64 > request.timeout_ms {
        return Err(ApiError::new(
            500,
            "timeout",
            "The request could not be processed in a reasonable amount of time.",
        ));
    }
    let meta = &snapshot.meta;
    let searcher = &snapshot.searcher;
    let schema = snapshot.index.schema();
    let id_field = schema
        .get_field(ID_FIELD)
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;

    let query_text = request.query.trim();
    let query: Box<dyn tantivy::query::Query> = if query_text == "*:*" || query_text.is_empty() {
        Box::new(AllQuery)
    } else {
        let default_fields: Vec<Field> = schema
            .fields()
            .filter(|(_, field)| field.name() != ID_FIELD && field.name() != FIELDS_FIELD)
            .map(|(field, _)| field)
            .collect();
        let parser = QueryParser::for_index(&snapshot.index, default_fields);
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

    let mut matched: Vec<&SearchDoc> = Vec::new();
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
        let Some(doc) = snapshot.positions.get(&id).map(|&at| &meta.docs[at]) else {
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
        matched.push(doc);
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
    let page: Vec<&SearchDoc> = matched.iter().skip(offset).take(limit).copied().collect();
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
    let _gate = build_gate(db).write_owned().await;
    forget_db(&db.path);
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
            crate::db::uuid()
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
            crate::db::uuid()
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
            crate::db::uuid()
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

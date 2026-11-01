//! Tantivy-backed CouchDB search indexes.

use crate::db::{hex, ApiError, Db, DbResult};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tantivy::collector::DocSetCollector;
use tantivy::query::{AllQuery, QueryParser};
use tantivy::schema::{
    Field, OwnedValue, Schema, TantivyDocument, FAST, INDEXED, STORED, STRING, TEXT,
};
use tantivy::Index;

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
    if crate::js::process_enabled() {
        let mut out = Vec::with_capacity(docs.len());
        for chunk in docs.chunks(128) {
            let value = crate::js::process_request(
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

#[derive(Clone, Copy, Debug)]
enum FieldKind {
    Text,
    Number,
    Bool,
}

fn infer_kinds(docs: &[IndexedDoc]) -> BTreeMap<String, FieldKind> {
    let mut kinds: BTreeMap<String, FieldKind> = BTreeMap::new();
    for doc in docs {
        for (name, value) in &doc.fields {
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
) -> DbResult<(PathBuf, SearchMeta)> {
    // Capture the sequence before reading the snapshot. A write that lands
    // while the index is being built then leaves the database ahead of the
    // recorded sequence, so the next query rebuilds instead of trusting a
    // snapshot that may have missed it.
    let last_seq = db.update_seq();
    let docs = db.map_docs(None).await?;
    let indexed = run_index_json(&spec.source, &docs, limits).map_err(|reason| {
        ApiError::new(
            500,
            "search_index_error",
            format!("index function failed: {reason}"),
        )
    })?;
    let kinds = infer_kinds(&indexed);

    let path = index_dir(db, ddoc, index_name);
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
    let mut writer = tantivy_index
        .writer(50_000_000)
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;

    for doc in &indexed {
        let mut tantivy_doc = TantivyDocument::default();
        tantivy_doc.add_text(id_field, &doc.id);
        for (name, value) in &doc.fields {
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
        writer
            .add_document(tantivy_doc)
            .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;
    }
    writer
        .commit()
        .map_err(|error| ApiError::new(500, "search_index_error", error.to_string()))?;

    let meta = SearchMeta {
        version: SEARCH_VERSION,
        last_seq,
        design: design_signature(spec),
        ddoc: ddoc.to_string(),
        name: index_name.to_string(),
        docs: indexed
            .into_iter()
            .map(|doc| SearchDoc {
                id: doc.id,
                fields: doc.fields,
            })
            .collect(),
    };
    write_meta(&path, &meta)?;
    Ok((path, meta))
}

async fn ensure_index(
    db: &Db,
    ddoc: &str,
    index_name: &str,
    spec: &SearchSpec,
    limits: crate::view::ViewLimits,
) -> DbResult<(PathBuf, SearchMeta)> {
    let path = index_dir(db, ddoc, index_name);
    let signature = design_signature(spec);
    if let Some(meta) = read_meta(&path) {
        if meta.version == SEARCH_VERSION
            && meta.last_seq == db.update_seq()
            && meta.design == signature
        {
            return Ok((path, meta));
        }
    }
    build_index(db, ddoc, index_name, spec, limits).await
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
    let (path, meta) = ensure_index(db, ddoc, index_name, &spec, limits).await?;
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
        parser
            .parse_query(query_text)
            .map_err(|error| ApiError::bad_request(format!("invalid search query: {error}")))?
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
    let (_path, meta) = ensure_index(db, ddoc, index_name, &spec, limits).await?;
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

pub fn cleanup(db: &Db) -> DbResult<()> {
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

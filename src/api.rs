//! HTTP surface. The shape of every response here is dictated by CouchDB's
//! wire protocol, not by taste.

use crate::auth::Auth;
use crate::cluster::Cluster;
use crate::db::{AllDocsOpts, ApiError, ChangesOpts, Db, DbConfig, DbResult, DocOpts};
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::Engine;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct AppState {
    pub root: PathBuf,
    pub dbs: RwLock<HashMap<String, Arc<Db>>>,
    pub config: Config,
    pub auth: Option<Auth>,
    pub cluster: Cluster,
    pub client: reqwest::Client,
}

#[derive(Clone)]
pub struct Config {
    #[allow(dead_code)]
    pub q: u32,
    pub revs_limit: u64,
    pub admin: Option<(String, String)>,
    #[allow(dead_code)]
    pub nodes: Vec<String>,
    #[allow(dead_code)]
    pub me: usize,
    pub sync_secs: u64,
}

// ---------------------------------------------------------------------------
// small helpers

fn json(status: StatusCode, value: Value) -> Response {
    (status, Json(value)).into_response()
}

fn fail(e: ApiError) -> Response {
    json(
        StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        e.body(),
    )
}

fn ok() -> Response {
    json(StatusCode::OK, json!({"ok": true}))
}

fn query(uri: &Uri) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Some(raw) = uri.query() else { return out };
    for pair in raw.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        out.insert(percent_decode(k), percent_decode(v));
    }
    out
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    out.push(b);
                    i += 3;
                    continue;
                }
                out.push(b'%');
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn flag(q: &HashMap<String, String>, key: &str) -> bool {
    matches!(q.get(key).map(String::as_str), Some("true") | Some("1"))
}

fn num(q: &HashMap<String, String>, key: &str) -> Option<usize> {
    q.get(key).and_then(|v| v.parse().ok())
}

/// CouchDB carries keys as JSON values (`startkey="foo"`), so unquote them.
fn key_param(q: &HashMap<String, String>, key: &str) -> Option<String> {
    let raw = q.get(key)?;
    match serde_json::from_str::<Value>(raw) {
        Ok(Value::String(s)) => Some(s),
        Ok(Value::Number(n)) => Some(n.to_string()),
        _ => Some(raw.clone()),
    }
}

fn json_body(body: &[u8]) -> DbResult<Value> {
    if body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(body).map_err(|e| ApiError::bad_request(format!("invalid JSON: {e}")))
}

fn valid_db_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('_')
        && name.bytes().all(|b| {
            b.is_ascii_lowercase()
                || b.is_ascii_digit()
                || matches!(b, b'_' | b'-' | b'$' | b'+' | b'(' | b')')
        })
}

// ---------------------------------------------------------------------------
// state

impl AppState {
    pub async fn db(&self, name: &str) -> DbResult<Arc<Db>> {
        if let Some(db) = self.dbs.read().await.get(name) {
            return Ok(db.clone());
        }
        if !Db::exists(&self.root, name) {
            return Err(ApiError::not_found(format!(
                "Database does not exist: {name}"
            )));
        }
        let db = Arc::new(Db::open(
            &self.root,
            name,
            DbConfig {
                q: self.config.q,
                revs_limit: self.config.revs_limit,
            },
        )?);
        self.dbs.write().await.insert(name.to_string(), db.clone());
        Ok(db)
    }

    pub async fn create_db(&self, name: &str) -> DbResult<Arc<Db>> {
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
                q: self.config.q,
                revs_limit: self.config.revs_limit,
            },
        )?);
        self.dbs.write().await.insert(name.to_string(), db.clone());
        Ok(db)
    }

    pub async fn drop_db(&self, name: &str) -> DbResult<()> {
        self.dbs.write().await.remove(name);
        Db::destroy(&self.root, name)
    }

    pub async fn db_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.root) {
            for entry in entries.flatten() {
                if entry.path().join("db.json").is_file() {
                    if let Some(name) = entry.file_name().to_str() {
                        names.push(name.to_string());
                    }
                }
            }
        }
        names.sort();
        names
    }

    fn authorized(&self, headers: &HeaderMap) -> bool {
        let Some(auth) = &self.auth else { return true };
        if let Some(value) = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
        {
            if auth.check_basic(value) {
                return true;
            }
        }
        let cookies = headers.get(header::COOKIE).and_then(|v| v.to_str().ok());
        auth.check_cookies(cookies)
    }
}

fn unauthorized() -> Response {
    json(
        StatusCode::UNAUTHORIZED,
        json!({"error": "unauthorized", "reason": "You are not authorized to access this db."}),
    )
}

/// Request log, enabled for every node. Cheap, and indispensable when a client
/// disagrees with us about the protocol.
pub async fn log_requests(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let response = next.run(req).await;
    tracing::info!("{} {} -> {}", method, uri, response.status().as_u16());
    response
}

// ---------------------------------------------------------------------------
// top-level endpoints

pub async fn welcome() -> Response {
    json(
        StatusCode::OK,
        json!({
            "couchdb": "Welcome",
            "version": "3.3.3",
            "vendor": { "name": "douchdb" },
            "features": ["access-ready", "partitioned", "pluggable-storage-engines", "reshard", "scheduler"],
        }),
    )
}

pub async fn up() -> Response {
    json(StatusCode::OK, json!({"status": "ok"}))
}

pub async fn all_dbs(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !st.authorized(&headers) {
        return unauthorized();
    }
    json(StatusCode::OK, json!(st.db_names().await))
}

pub async fn uuids(State(_st): State<Arc<AppState>>, uri: Uri) -> Response {
    let count = num(&query(&uri), "count").unwrap_or(1).clamp(1, 1000);
    let ids: Vec<String> = (0..count).map(|_| crate::db::uuid()).collect();
    json(StatusCode::OK, json!({ "uuids": ids }))
}

pub async fn membership(State(st): State<Arc<AppState>>) -> Response {
    let nodes: Vec<String> = if st.cluster.nodes.is_empty() {
        vec!["nonode@localhost".to_string()]
    } else {
        st.cluster.nodes.clone()
    };
    json(
        StatusCode::OK,
        json!({ "all_nodes": nodes, "cluster_nodes": nodes }),
    )
}

pub async fn session_get(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let authenticated = st.authorized(&headers);
    let name = if authenticated {
        st.config
            .admin
            .as_ref()
            .map(|(u, _)| u.clone())
            .unwrap_or_else(|| "admin".into())
    } else {
        Value::Null.to_string()
    };
    let roles = if authenticated {
        json!(["_admin"])
    } else {
        json!([])
    };
    json(
        StatusCode::OK,
        json!({
            "ok": true,
            "userCtx": { "name": name, "roles": roles },
            "info": {
                "authenticated": authenticated,
                "authentication_db": "_users",
                "authentication_handlers": ["cookie", "default"]
            }
        }),
    )
}

pub async fn session_post(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let text = String::from_utf8_lossy(&body);
    let mut user = String::new();
    let mut pass = String::new();
    if let Ok(v) = serde_json::from_slice::<Value>(&body) {
        user = v
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        pass = v
            .get("password")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
    }
    if user.is_empty() {
        for pair in text.split('&') {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            match percent_decode(k).as_str() {
                "name" => user = percent_decode(v),
                "password" => pass = percent_decode(v),
                _ => {}
            }
        }
    }
    let valid = match &st.config.admin {
        Some((u, p)) => &user == u && &pass == p,
        None => true,
    };
    if !valid {
        return json(
            StatusCode::UNAUTHORIZED,
            json!({"error": "unauthorized", "reason": "Name or password is incorrect."}),
        );
    }
    let mut response = json(
        StatusCode::OK,
        json!({
            "ok": true,
            "name": user,
            "roles": ["_admin"],
        }),
    );
    if let Some(auth) = &st.auth {
        if let Ok(value) = auth.cookie_header().parse() {
            response.headers_mut().insert(header::SET_COOKIE, value);
        }
    }
    response
}

// ---------------------------------------------------------------------------
// database-level endpoints

pub async fn db_root(
    State(st): State<Arc<AppState>>,
    Path(name): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !st.authorized(&headers) {
        return unauthorized();
    }
    if !valid_db_name(&name) {
        return fail(ApiError::bad_request("Name must be lowercase and URL-safe"));
    }
    let q = query(&uri);

    match method {
        Method::PUT => match st.create_db(&name).await {
            Ok(_) => json(StatusCode::CREATED, json!({"ok": true})),
            Err(e) => fail(e),
        },
        Method::DELETE => match st.drop_db(&name).await {
            Ok(()) => ok(),
            Err(e) => fail(e),
        },
        Method::GET | Method::HEAD => match st.db(&name).await {
            Ok(db) => match db.info().await {
                Ok(info) => json(StatusCode::OK, info),
                Err(e) => fail(e),
            },
            Err(e) => fail(e),
        },
        Method::POST => {
            // Create a document with a server-generated id.
            let db = match st.db(&name).await {
                Ok(db) => db,
                Err(e) => return fail(e),
            };
            let Ok(mut doc) = json_body(&body) else {
                return fail(ApiError::bad_request("Document must be a JSON object"));
            };
            if doc.get("_id").is_none() {
                doc["_id"] = json!(crate::db::uuid());
            }
            let id = doc
                .get("_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            match db.put_docs(vec![doc], true).await {
                Ok(mut results) => {
                    let result = results.pop().unwrap_or(Value::Null);
                    if result.get("error").is_some() {
                        return fail(ApiError::conflict());
                    }
                    if flag(&q, "batch") {
                        return json(StatusCode::ACCEPTED, json!({"ok": true, "id": id}));
                    }
                    json(StatusCode::CREATED, result)
                }
                Err(e) => fail(e),
            }
        }
        _ => fail(ApiError::new(
            405,
            "method_not_allowed",
            "method not allowed",
        )),
    }
}

pub async fn db_rest(
    State(st): State<Arc<AppState>>,
    Path((name, rest)): Path<(String, String)>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !st.authorized(&headers) {
        return unauthorized();
    }
    if rest.is_empty() {
        // CouchDB accepts `GET /db/` and PouchDB uses it during setup.
        return db_root(State(st), Path(name), method, uri, headers, body).await;
    }
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(e) => return fail(e),
    };
    let q = query(&uri);

    match rest.as_str() {
        "_bulk_docs" => return bulk_docs(&db, &body).await,
        "_bulk_get" => {
            return match serde_json::from_slice::<Value>(&body) {
                Ok(v) => {
                    let revs =
                        flag(&q, "revs") || v.get("revs").and_then(Value::as_bool).unwrap_or(false);
                    match db.bulk_get(&v, flag(&q, "attachments"), revs).await {
                        Ok(v) => json(StatusCode::OK, v),
                        Err(e) => fail(e),
                    }
                }
                Err(_) => fail(ApiError::bad_request("invalid JSON")),
            }
        }
        "_all_docs" => return all_docs(&db, &q, &method, &body).await,
        "_changes" => return changes(&st, &db, &q, &method, &body).await,
        "_revs_diff" => {
            return match serde_json::from_slice::<Value>(&body) {
                Ok(v) => match db.revs_diff(&v).await {
                    Ok(diff) => json(StatusCode::OK, diff),
                    Err(e) => fail(e),
                },
                Err(_) => fail(ApiError::bad_request("invalid JSON")),
            }
        }
        "_purge" => {
            return match serde_json::from_slice::<Value>(&body) {
                Ok(v) => match db.purge(&v).await {
                    Ok(p) => json(StatusCode::OK, p),
                    Err(e) => fail(e),
                },
                Err(_) => fail(ApiError::bad_request("invalid JSON")),
            }
        }
        "_compact" => {
            if let Err(e) = db.compact().await {
                return fail(e);
            }
            return json(StatusCode::ACCEPTED, json!({"ok": true}));
        }
        "_ensure_full_commit" => {
            return match db.ensure_full_commit().await {
                Ok(v) => json(StatusCode::CREATED, v),
                Err(e) => fail(e),
            }
        }
        "_revs_limit" => {
            return match method {
                Method::GET => match db.get_revs_limit().await {
                    Ok(n) => json(StatusCode::OK, json!(n)),
                    Err(e) => fail(e),
                },
                Method::PUT => {
                    let limit = String::from_utf8_lossy(&body)
                        .trim()
                        .parse()
                        .unwrap_or(1000);
                    match db.set_revs_limit(limit).await {
                        Ok(()) => ok(),
                        Err(e) => fail(e),
                    }
                }
                _ => fail(ApiError::new(
                    405,
                    "method_not_allowed",
                    "method not allowed",
                )),
            }
        }
        "_security" => {
            return match method {
                Method::GET => json(StatusCode::OK, json!({})),
                Method::PUT => ok(),
                _ => fail(ApiError::new(
                    405,
                    "method_not_allowed",
                    "method not allowed",
                )),
            }
        }
        "_local" => return fail(ApiError::bad_request("missing local doc id")),
        _ => {}
    }

    if let Some(id) = rest.strip_prefix("_local/") {
        return local_doc(&db, id, &method, &body).await;
    }

    doc_request(&st, &db, &name, &rest, method, &q, &headers, body).await
}

async fn local_doc(db: &Db, id: &str, method: &Method, body: &[u8]) -> Response {
    match method.as_str() {
        "GET" => match db.local_get(id).await {
            Ok(Some(doc)) => json(StatusCode::OK, doc),
            Ok(None) => fail(ApiError::not_found("missing")),
            Err(e) => fail(e),
        },
        "PUT" => match serde_json::from_slice::<Value>(body) {
            Ok(doc) => match db.local_put(id, doc).await {
                Ok(v) => json(StatusCode::CREATED, v),
                Err(e) => fail(e),
            },
            Err(_) => fail(ApiError::bad_request("invalid JSON")),
        },
        "DELETE" => match db.local_delete(id).await {
            Ok(v) => json(StatusCode::OK, v),
            Err(e) => fail(e),
        },
        _ => fail(ApiError::new(
            405,
            "method_not_allowed",
            "method not allowed",
        )),
    }
}

async fn bulk_docs(db: &Db, body: &[u8]) -> Response {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return fail(ApiError::bad_request("invalid JSON"));
    };
    let new_edits = value
        .get("new_edits")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let Some(docs) = value.get("docs").and_then(Value::as_array).cloned() else {
        return fail(ApiError::bad_request("Missing JSON list of 'docs'"));
    };
    match db.put_docs(docs, new_edits).await {
        Ok(results) => {
            if new_edits {
                json(StatusCode::CREATED, Value::Array(results))
            } else {
                let errors: Vec<Value> = results
                    .into_iter()
                    .filter(|r| r.get("error").is_some())
                    .collect();
                json(StatusCode::CREATED, Value::Array(errors))
            }
        }
        Err(e) => fail(e),
    }
}

async fn all_docs(db: &Db, q: &HashMap<String, String>, method: &Method, body: &[u8]) -> Response {
    let mut opts = AllDocsOpts {
        include_docs: flag(q, "include_docs"),
        conflicts: flag(q, "conflicts"),
        attachments: flag(q, "attachments"),
        startkey: key_param(q, "startkey"),
        endkey: key_param(q, "endkey"),
        descending: flag(q, "descending"),
        limit: num(q, "limit").unwrap_or(usize::MAX),
        skip: num(q, "skip").unwrap_or(0),
        ..Default::default()
    };
    if *method == Method::POST {
        if let Ok(value) = serde_json::from_slice::<Value>(body) {
            if let Some(keys) = value.get("keys").and_then(Value::as_array) {
                opts.keys = Some(
                    keys.iter()
                        .filter_map(|k| k.as_str().map(str::to_string))
                        .collect(),
                );
            }
            if value
                .get("include_docs")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                opts.include_docs = true;
            }
        }
    }
    match db.all_docs(&opts).await {
        Ok(v) => json(StatusCode::OK, v),
        Err(e) => fail(e),
    }
}

async fn changes(
    st: &AppState,
    db: &Db,
    q: &HashMap<String, String>,
    method: &Method,
    body: &[u8],
) -> Response {
    let since = match q.get("since").map(String::as_str) {
        Some("now") => match db.info().await {
            Ok(info) => info["update_seq"].as_u64().unwrap_or(0),
            Err(e) => return fail(e),
        },
        Some(raw) => raw.trim_matches('"').parse().unwrap_or(0),
        None => 0,
    };
    let mut opts = ChangesOpts {
        since,
        limit: num(q, "limit"),
        include_docs: flag(q, "include_docs"),
        style_all_docs: q.get("style").map(String::as_str) == Some("all_docs"),
        descending: flag(q, "descending"),
        doc_ids: None,
    };
    if q.get("filter").map(String::as_str) == Some("_doc_ids") {
        if let Ok(value) = serde_json::from_slice::<Value>(body) {
            opts.doc_ids = value
                .get("doc_ids")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .or_else(|| {
                    q.get("doc_ids")
                        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
                });
        }
    }
    let _ = method;

    let feed = q.get("feed").map(String::as_str).unwrap_or("normal");
    if feed == "longpoll" || feed == "continuous" {
        let timeout = num(q, "timeout").unwrap_or(30_000) as u64;
        let started = std::time::Instant::now();
        loop {
            match db.changes(&opts).await {
                Ok(v)
                    if v["results"]
                        .as_array()
                        .map(|a| !a.is_empty())
                        .unwrap_or(false) =>
                {
                    return json(StatusCode::OK, v)
                }
                Ok(v) if started.elapsed().as_millis() as u64 >= timeout => {
                    return json(StatusCode::OK, v)
                }
                Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
                Err(e) => return fail(e),
            }
            let _ = st;
        }
    }

    match db.changes(&opts).await {
        Ok(v) => json(StatusCode::OK, v),
        Err(e) => fail(e),
    }
}

async fn doc_request(
    st: &AppState,
    db: &Db,
    dbname: &str,
    rest: &str,
    method: Method,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let (docid, attachment) = split_doc_path(rest);
    if docid.is_empty() {
        return fail(ApiError::bad_request("invalid document id"));
    }

    if let Some(peer) = st.cluster.owner_url(&docid, db.q) {
        return proxy(st, &method, dbname, rest, q, headers, body, peer).await;
    }

    let Some(attachment) = attachment else {
        return match method {
            Method::GET | Method::HEAD => get_doc(db, &docid, q).await,
            Method::PUT => put_doc(db, &docid, body, q).await,
            Method::DELETE => delete_doc(db, &docid, q).await,
            _ => fail(ApiError::new(
                405,
                "method_not_allowed",
                "method not allowed",
            )),
        };
    };

    match method {
        Method::GET => match db
            .attachment(&docid, q.get("rev").map(String::as_str), &attachment)
            .await
        {
            Ok((att, bytes)) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, att.content_type)],
                bytes,
            )
                .into_response(),
            Err(e) => fail(e),
        },
        Method::PUT => {
            let content_type = headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/octet-stream")
                .to_string();
            let encoded = base64::engine::general_purpose::STANDARD.encode(&body);
            // `PUT /db/doc/att` updates the document: the existing body and the
            // other attachments must be preserved, only this attachment changes.
            let mut doc = match q.get("rev") {
                Some(rev) => match db
                    .get_doc(
                        &docid,
                        &DocOpts {
                            rev: Some(rev.clone()),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(doc) => doc,
                    Err(e) => return fail(e),
                },
                None => json!({"_id": docid}),
            };
            let mut atts = doc
                .get("_attachments")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            atts.insert(
                attachment.clone(),
                json!({ "content_type": content_type, "data": encoded }),
            );
            doc["_attachments"] = Value::Object(atts);
            doc["_id"] = json!(docid);
            if let Some(rev) = q.get("rev") {
                doc["_rev"] = json!(rev);
            }
            match db.put_docs(vec![doc], true).await {
                Ok(mut results) => {
                    let result = results.pop().unwrap_or(Value::Null);
                    if result.get("error").is_some() {
                        return json(StatusCode::CONFLICT, result);
                    }
                    json(StatusCode::CREATED, result)
                }
                Err(e) => fail(e),
            }
        }
        Method::DELETE => {
            let Some(rev) = q.get("rev") else {
                return fail(ApiError::conflict());
            };
            let opts = DocOpts {
                rev: Some(rev.clone()),
                ..Default::default()
            };
            let Ok(mut doc) = db.get_doc(&docid, &opts).await else {
                return fail(ApiError::not_found("missing"));
            };
            if let Some(atts) = doc.get_mut("_attachments").and_then(Value::as_object_mut) {
                atts.remove(&attachment);
            }
            doc["_rev"] = json!(rev);
            match db.put_docs(vec![doc], true).await {
                Ok(mut results) => {
                    let result = results.pop().unwrap_or(Value::Null);
                    if result.get("error").is_some() {
                        return json(StatusCode::CONFLICT, result);
                    }
                    json(StatusCode::OK, result)
                }
                Err(e) => fail(e),
            }
        }
        _ => fail(ApiError::new(
            405,
            "method_not_allowed",
            "method not allowed",
        )),
    }
}

fn split_doc_path(rest: &str) -> (String, Option<String>) {
    if let Some(tail) = rest.strip_prefix("_design/") {
        return match tail.split_once('/') {
            Some((name, att)) => (format!("_design/{name}"), Some(att.to_string())),
            None => (format!("_design/{tail}"), None),
        };
    }
    match rest.split_once('/') {
        Some((id, att)) => (id.to_string(), Some(att.to_string())),
        None => (rest.to_string(), None),
    }
}

async fn get_doc(db: &Db, docid: &str, q: &HashMap<String, String>) -> Response {
    let opts = DocOpts {
        rev: q.get("rev").cloned(),
        revs: flag(q, "revs"),
        revs_info: flag(q, "revs_info"),
        conflicts: flag(q, "conflicts"),
        deleted_conflicts: flag(q, "deleted_conflicts"),
        attachments: flag(q, "attachments"),
        open_revs: q
            .get("open_revs")
            .map(|v| serde_json::from_str::<Vec<String>>(v).unwrap_or_else(|_| vec![v.clone()])),
        latest: flag(q, "latest"),
        ..Default::default()
    };
    match db.get_doc(docid, &opts).await {
        Ok(doc) => {
            let etag = doc
                .get("_rev")
                .and_then(Value::as_str)
                .map(|r| format!("\"{r}\""))
                .unwrap_or_default();
            let mut response = json(StatusCode::OK, doc);
            if !etag.is_empty() {
                if let Ok(value) = etag.parse() {
                    response.headers_mut().insert(header::ETAG, value);
                }
            }
            response
        }
        Err(e) => fail(e),
    }
}

async fn put_doc(db: &Db, docid: &str, body: Bytes, q: &HashMap<String, String>) -> Response {
    let mut doc = match serde_json::from_slice::<Value>(&body) {
        Ok(v) => v,
        Err(e) => return fail(ApiError::bad_request(format!("invalid JSON: {e}"))),
    };
    // The URL owns the id: CouchDB ignores a conflicting `_id` in the body.
    doc["_id"] = json!(docid);
    let new_edits = !q.contains_key("new_edits") || flag(q, "new_edits");
    if !new_edits && doc.get("_rev").is_none() {
        return fail(ApiError::bad_request("Document is missing _rev"));
    }
    match db.put_docs(vec![doc], new_edits).await {
        Ok(mut results) => {
            let mut result = results.pop().unwrap_or(Value::Null);
            if result.get("error").is_some() {
                // Single-document endpoints, unlike `_bulk_docs`, return no id.
                if let Some(obj) = result.as_object_mut() {
                    obj.remove("id");
                }
                let status = if result["error"] == "conflict" {
                    StatusCode::CONFLICT
                } else {
                    StatusCode::BAD_REQUEST
                };
                return json(status, result);
            }
            json(StatusCode::CREATED, result)
        }
        Err(e) => fail(e),
    }
}

async fn delete_doc(db: &Db, docid: &str, q: &HashMap<String, String>) -> Response {
    // A missing document is 404 even without a `rev`; only a live document
    // without a rev is a conflict.
    if db.get_doc(docid, &DocOpts::default()).await.is_err() {
        return fail(ApiError::not_found("missing"));
    }
    let Some(rev) = q.get("rev") else {
        return fail(ApiError::conflict());
    };
    let doc = json!({"_id": docid, "_rev": rev, "_deleted": true});
    match db.put_docs(vec![doc], true).await {
        Ok(mut results) => {
            let mut result = results.pop().unwrap_or(Value::Null);
            if result.get("error").is_some() {
                if let Some(obj) = result.as_object_mut() {
                    obj.remove("id");
                }
                return json(StatusCode::CONFLICT, result);
            }
            json(StatusCode::OK, result)
        }
        Err(e) => fail(e),
    }
}

// ---------------------------------------------------------------------------
// clustering

async fn proxy(
    st: &AppState,
    method: &Method,
    dbname: &str,
    rest: &str,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
    body: Bytes,
    peer: &str,
) -> Response {
    let mut url = format!("{peer}/{dbname}/{rest}");
    if !q.is_empty() {
        let encoded: Vec<String> = q
            .iter()
            .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
            .collect();
        url.push('?');
        url.push_str(&encoded.join("&"));
    }
    let mut request = st.client.request(
        reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap_or(reqwest::Method::GET),
        &url,
    );
    if let Some(value) = headers.get(header::AUTHORIZATION) {
        request = request.header("authorization", value);
    }
    if let Some(value) = headers.get(header::CONTENT_TYPE) {
        request = request.header("content-type", value);
    }
    if !body.is_empty() {
        request = request.body(body.to_vec());
    }
    match request.send().await {
        Ok(resp) => {
            let status =
                StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let content_type = resp
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/json")
                .to_string();
            let bytes = resp.bytes().await.unwrap_or_default().to_vec();
            (status, [(header::CONTENT_TYPE, content_type)], bytes).into_response()
        }
        Err(e) => fail(ApiError::new(502, "bad_gateway", e.to_string())),
    }
}

fn percent_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push_str("%20"),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// misc

pub async fn not_found() -> Response {
    json(
        StatusCode::NOT_FOUND,
        json!({"error": "not_found", "reason": "missing"}),
    )
}

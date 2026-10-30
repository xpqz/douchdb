//! HTTP surface. The shape of every response here is dictated by CouchDB's
//! wire protocol, not by taste.

use crate::auth::Auth;
use crate::cluster::Cluster;
use crate::db::{
    validate_doc, AllDocsOpts, ApiError, ChangesOpts, Db, DbConfig, DbResult, DocOpts,
};
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::Engine;
use serde_json::{json, Map, Value};
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
    #[allow(dead_code)]
    pub n: usize,
    pub revs_limit: u64,
    pub admin: Option<(String, String)>,
    #[allow(dead_code)]
    pub nodes: Vec<String>,
    #[allow(dead_code)]
    pub me: usize,
    pub sync_secs: u64,
    /// Empty disables CORS. `*` allows any origin (echoed back so credentials
    /// work); entries may be exact origins or `https://*.example.com`.
    pub cors_origins: Vec<String>,
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

/// True when a proxied request failed to reach the peer, rather than the peer
/// answering on behalf of the cluster.
fn unavailable(response: &Response) -> bool {
    matches!(
        response.status(),
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
    )
}

/// True for a request a peer already routed. Such requests are served from the
/// local node's shards: re-running the fan-out on the receiving side would send
/// the request straight back and loop.
fn is_forwarded(headers: &HeaderMap) -> bool {
    headers.get("x-douchdb-forwarded").is_some()
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

/// Opt-in CORS, matching CouchDB's `[cors]` configuration. The embedded
/// console is served from the same origin and needs none of this; it exists for
/// browser apps (PouchDB, Fauxton, a separate frontend) on another origin.
pub async fn cors(
    State(st): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if st.config.cors_origins.is_empty() {
        return next.run(req).await;
    }
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let allowed = origin
        .as_deref()
        .map(|origin| cors_origin_allowed(&st.config.cors_origins, origin))
        .unwrap_or(false);

    if req.method() == Method::OPTIONS {
        if !allowed {
            return fail(ApiError::new(
                403,
                "forbidden",
                "CORS origin is not allowed",
            ));
        }
        let mut response = StatusCode::NO_CONTENT.into_response();
        add_cors_headers(
            response.headers_mut(),
            origin.as_deref().unwrap_or_default(),
            Some(req.headers()),
        );
        return response;
    }

    let mut response = next.run(req).await;
    if allowed {
        add_cors_headers(
            response.headers_mut(),
            origin.as_deref().unwrap_or_default(),
            None,
        );
    }
    response
}

fn cors_origin_allowed(rules: &[String], origin: &str) -> bool {
    rules.iter().any(|rule| {
        if rule == "*" {
            return true;
        }
        let Some((rule_scheme, rule_host)) = rule.split_once("://") else {
            return false;
        };
        let Some((origin_scheme, origin_host)) = origin.split_once("://") else {
            return false;
        };
        if !rule_scheme.eq_ignore_ascii_case(origin_scheme) {
            return false;
        }
        match rule_host.strip_prefix("*.") {
            // CouchDB-style subdomain wildcard: https://*.example.com
            Some(suffix) => {
                let host = origin_host.to_ascii_lowercase();
                let suffix = suffix.to_ascii_lowercase();
                host.len() > suffix.len() + 1 && host.ends_with(&format!(".{suffix}"))
            }
            None => rule_host.eq_ignore_ascii_case(origin_host),
        }
    })
}

fn add_cors_headers(headers: &mut HeaderMap, origin: &str, preflight: Option<&HeaderMap>) {
    let put = |headers: &mut HeaderMap, name, value: &str| {
        if let Ok(value) = HeaderValue::from_str(value) {
            headers.insert(name, value);
        }
    };
    // Echo the origin rather than using `*`, so cookie credentials work even
    // when the rule is `*`.
    put(headers, header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    put(headers, header::ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
    put(
        headers,
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        "ETag, Location, Server, X-Couch-Request-ID, Content-Type, Content-Length",
    );
    headers.append(header::VARY, HeaderValue::from_static("Origin"));

    if let Some(request_headers) = preflight {
        put(
            headers,
            header::ACCESS_CONTROL_ALLOW_METHODS,
            "GET, PUT, POST, HEAD, DELETE, OPTIONS",
        );
        let requested = request_headers
            .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("accept, authorization, content-type, origin, referer, x-csrf-token");
        put(headers, header::ACCESS_CONTROL_ALLOW_HEADERS, requested);
        put(headers, header::ACCESS_CONTROL_MAX_AGE, "3600");
        headers.append(
            header::VARY,
            HeaderValue::from_static("Access-Control-Request-Method"),
        );
        headers.append(
            header::VARY,
            HeaderValue::from_static("Access-Control-Request-Headers"),
        );
    }
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
    let mut names = st.db_names().await;
    if st.cluster.is_clustered() && headers.get("x-douchdb-local-db").is_none() {
        for (i, peer) in st.cluster.nodes.iter().enumerate() {
            if i == st.cluster.me {
                continue;
            }
            let mut request = st
                .client
                .get(format!("{peer}/_all_dbs"))
                .header("x-douchdb-local-db", "1");
            if let Some(value) = headers.get(header::AUTHORIZATION) {
                request = request.header(header::AUTHORIZATION, value);
            }
            if let Ok(response) = request.send().await {
                if let Ok(Value::Array(remote)) = response.json::<Value>().await {
                    for name in remote {
                        if let Some(name) = name.as_str() {
                            if !names.iter().any(|n| n == name) {
                                names.push(name.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    names.sort();
    names.dedup();
    json(StatusCode::OK, json!(names))
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

/// CouchDB clears the session cookie with DELETE /_session. The cookie is
/// stateless, so expiring it on the client is all that is needed.
pub async fn session_delete() -> Response {
    let mut response = json(StatusCode::OK, json!({"ok": true}));
    response.headers_mut().insert(
        header::SET_COOKIE,
        "AuthSession=; Version=1; Path=/; Max-Age=0; HttpOnly"
            .parse()
            .expect("valid cookie"),
    );
    response
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

    let local_only = headers.get("x-douchdb-local-db").is_some();
    match method {
        Method::PUT => {
            if let Err(e) = st.create_db(&name).await {
                return fail(e);
            }
            if st.cluster.is_clustered() && !local_only {
                for peer in st.cluster.nodes.iter() {
                    if peer == &st.cluster.nodes[st.cluster.me] {
                        continue;
                    }
                    let mut request = st
                        .client
                        .put(format!("{peer}/{name}"))
                        .header("x-douchdb-local-db", "1");
                    if let Some(value) = headers.get(header::AUTHORIZATION) {
                        request = request.header(header::AUTHORIZATION, value);
                    }
                    let response = match request.send().await {
                        Ok(response) => response,
                        Err(e) => {
                            return fail(ApiError::new(502, "bad_gateway", format!("{peer}: {e}")))
                        }
                    };
                    // 412 means another node already had it, which is fine.
                    if !response.status().is_success()
                        && response.status() != StatusCode::PRECONDITION_FAILED
                    {
                        let status = response.status().as_u16();
                        let text = response.text().await.unwrap_or_default();
                        return fail(ApiError::new(status, "db_create_failed", text));
                    }
                }
            }
            json(StatusCode::CREATED, json!({"ok": true}))
        }
        Method::DELETE => {
            let local = st.drop_db(&name).await;
            if st.cluster.is_clustered() && !local_only {
                for (i, peer) in st.cluster.nodes.iter().enumerate() {
                    if i == st.cluster.me {
                        continue;
                    }
                    let mut request = st
                        .client
                        .delete(format!("{peer}/{name}"))
                        .header("x-douchdb-local-db", "1");
                    if let Some(value) = headers.get(header::AUTHORIZATION) {
                        request = request.header(header::AUTHORIZATION, value);
                    }
                    let _ = request.send().await;
                }
            }
            match local {
                Ok(()) => ok(),
                Err(e) => fail(e),
            }
        }
        Method::GET | Method::HEAD => match st.db(&name).await {
            Ok(db) => match db.info().await {
                Ok(mut info) => {
                    // In a cluster the update_seq is the same version vector
                    // the changes feed returns, so clients that use it as
                    // `since` do not skip another node's changes.
                    if st.cluster.is_clustered() && !is_forwarded(&headers) {
                        if let Ok(vector) = cluster_update_vector(&st, &db, &name, &headers).await {
                            let seq = encode_since(&vector);
                            info["update_seq"] = json!(seq);
                            info["committed_update_seq"] = json!(seq);
                        }
                    }
                    json(StatusCode::OK, info)
                }
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
            if let Err(e) = validate_doc(&doc) {
                return fail(e);
            }
            if doc.get("_id").is_none() {
                doc["_id"] = json!(crate::db::uuid());
            }
            let id = doc
                .get("_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            // The id only exists after the body is parsed, so the shard's
            // replicas are known here and a generated document can still be
            // routed. Try each replica so a dead primary does not lose it.
            let replicas = st.cluster.replicas_of_doc(&id, db.q);
            if st.cluster.is_clustered() && !replicas.contains(&st.cluster.me) {
                let forwarded = serde_json::to_vec(&doc).unwrap_or_default();
                let mut last = None;
                for node in replicas {
                    let peer = st.cluster.nodes[node].clone();
                    let response = proxy(
                        &st,
                        &Method::POST,
                        &name,
                        "",
                        &q,
                        &headers,
                        Bytes::from(forwarded.clone()),
                        &peer,
                    )
                    .await;
                    if !unavailable(&response) {
                        return response;
                    }
                    last = Some(response);
                }
                return last.unwrap_or_else(|| {
                    fail(ApiError::new(
                        502,
                        "bad_gateway",
                        "no replica in the shard set is reachable",
                    ))
                });
            }
            match commit_docs(
                &st,
                &db,
                &name,
                vec![doc],
                true,
                num(&q, "w").unwrap_or(0),
                &headers,
            )
            .await
            {
                Ok((mut results, quorum)) => {
                    let result = results.pop().unwrap_or(Value::Null);
                    if result.get("error").is_some() {
                        return fail(ApiError::conflict());
                    }
                    if flag(&q, "batch") {
                        return json(StatusCode::ACCEPTED, json!({"ok": true, "id": id}));
                    }
                    json(
                        if quorum {
                            StatusCode::CREATED
                        } else {
                            StatusCode::ACCEPTED
                        },
                        result,
                    )
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
        "_bulk_docs" => return bulk_docs(&st, &db, &name, &q, &body, &headers).await,
        "_bulk_get" => return bulk_get(&st, &db, &name, &q, &body, &headers).await,
        "_all_docs" => return all_docs(&st, &db, &name, &q, &method, &body, &headers).await,
        "_changes" => return changes(&st, &db, &name, &q, &method, &body, &headers).await,
        "_revs_diff" => return revs_diff(&st, &db, &name, &body, &headers).await,
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

    // `Path` percent-decodes the wildcard, which would turn an encoded slash
    // in a document id (`a%2Fb`) into a path separator (`a`, attachment `b`).
    // Keep the raw remainder for document routing and decode each segment only
    // after splitting.
    let raw_rest = uri
        .path()
        .strip_prefix(&format!("/{name}/"))
        .unwrap_or(rest.as_str())
        .to_string();

    if let Some(id) = raw_rest.strip_prefix("_local/") {
        return local_doc(&db, &percent_decode(id), &method, &body).await;
    }

    doc_request(&st, &db, &name, &raw_rest, method, &q, &headers, body).await
}

async fn local_doc(db: &Db, id: &str, method: &Method, body: &[u8]) -> Response {
    match method.as_str() {
        "GET" => match db.local_get(id).await {
            Ok(Some(doc)) => json(StatusCode::OK, doc),
            Ok(None) => fail(ApiError::not_found("missing")),
            Err(e) => fail(e),
        },
        "PUT" => match serde_json::from_slice::<Value>(body) {
            Ok(doc) => {
                if let Err(e) = validate_doc(&doc) {
                    return fail(e);
                }
                match db.local_put(id, doc).await {
                    Ok(v) => json(StatusCode::CREATED, v),
                    Err(e) => fail(e),
                }
            }
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

async fn bulk_docs(
    st: &AppState,
    db: &Db,
    dbname: &str,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
) -> Response {
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
    for doc in &docs {
        if let Err(e) = validate_doc(doc) {
            return fail(e);
        }
    }

    if !st.cluster.is_clustered() {
        return match db.put_docs(docs, new_edits).await {
            Ok(results) => bulk_response(new_edits, results),
            Err(e) => fail(e),
        };
    }
    // A forwarded batch has already been placed by its coordinator; write it
    // to this node's shards and let the coordinator count the replica ack.
    if is_forwarded(headers) {
        return match db.put_docs(docs, new_edits).await {
            Ok(results) => bulk_response(new_edits, results),
            Err(e) => fail(e),
        };
    }

    let w = num(q, "w").unwrap_or(0);
    match commit_docs(st, db, dbname, docs, new_edits, w, headers).await {
        Ok((results, quorum)) => {
            let mut response = bulk_response(new_edits, results);
            if !quorum {
                // CouchDB answers 202 when the write reached fewer than the
                // requested number of replicas; anti-entropy repairs the rest.
                *response.status_mut() = StatusCode::ACCEPTED;
            }
            response
        }
        Err(e) => fail(e),
    }
}

/// Place documents on their replica sets.
///
/// `new_edits=true` writes to each document's primary (the coordinating
/// replica), then copies the resulting full revision to the other replicas with
/// `new_edits=false`, so every replica ends with byte-identical history.
/// `new_edits=false` (the replication path) sends the given revision to every
/// replica directly. The bool is whether the default write quorum was met.
async fn commit_docs(
    st: &AppState,
    db: &Db,
    dbname: &str,
    mut docs: Vec<Value>,
    new_edits: bool,
    w: usize,
    headers: &HeaderMap,
) -> DbResult<(Vec<Value>, bool)> {
    if !st.cluster.is_clustered() {
        return Ok((db.put_docs(docs, new_edits).await?, true));
    }

    // Routing needs ids, so generate them before grouping. `Db::put_docs`
    // would otherwise assign them per replica and the copies would diverge.
    for doc in &mut docs {
        if doc.get("_id").is_none() {
            doc["_id"] = json!(crate::db::uuid());
        }
    }
    let ids: Vec<String> = docs
        .iter()
        .map(|doc| {
            doc.get("_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        })
        .collect();
    let primaries: Vec<usize> = ids
        .iter()
        .map(|id| st.cluster.owner_of_doc(id, db.q))
        .collect();
    let replicas: Vec<Vec<usize>> = ids
        .iter()
        .map(|id| st.cluster.replicas_of_doc(id, db.q))
        .collect();
    let replication = st.cluster.replication_factor();
    // CouchDB defaults to a write quorum and lets `?w=` override it.
    let quorum = if w == 0 {
        replication / 2 + 1
    } else {
        w.min(replication)
    };
    let total = docs.len();
    let mut out = vec![Value::Null; total];
    let mut acks = vec![0usize; total];

    if new_edits {
        // Write each document to the first live replica, then copy the result
        // to the rest of its replica set. A dead primary falls through to the
        // next replica instead of failing the request.
        let mut written_at: Vec<Option<usize>> = vec![None; total];
        let mut pending: Vec<(usize, Value)> = docs.into_iter().enumerate().collect();
        let mut attempt = vec![0usize; total];
        while !pending.is_empty() {
            let mut groups: HashMap<usize, Vec<(usize, Value)>> = HashMap::new();
            for (slot, doc) in pending.drain(..) {
                let list = &replicas[slot];
                let node = list[attempt[slot].min(list.len() - 1)];
                groups.entry(node).or_default().push((slot, doc));
            }
            let mut retry = Vec::new();
            for (node, group) in groups {
                let slots: Vec<usize> = group.iter().map(|(slot, _)| *slot).collect();
                let subdocs: Vec<Value> = group.iter().map(|(_, doc)| doc.clone()).collect();
                match write_local_or_forward(st, db, dbname, node, subdocs, true, headers).await {
                    Ok(results) => {
                        for (slot, result) in slots.into_iter().zip(results) {
                            if result.get("error").is_none() {
                                acks[slot] = 1;
                                written_at[slot] = Some(node);
                            }
                            out[slot] = result;
                        }
                    }
                    Err(_) => {
                        for (slot, doc) in group {
                            attempt[slot] += 1;
                            if attempt[slot] < replicas[slot].len() {
                                retry.push((slot, doc));
                            } else {
                                out[slot] = json!({
                                    "id": ids[slot],
                                    "error": "replica_unavailable",
                                    "reason": "no replica in the shard set is reachable",
                                });
                            }
                        }
                    }
                }
            }
            pending = retry;
        }

        // Copy each successful revision to the rest of its replica set.
        let mut batches: HashMap<usize, Vec<(usize, Value)>> = HashMap::new();
        for slot in 0..total {
            let Some(source) = written_at[slot] else {
                continue;
            };
            let Some(rev) = out[slot].get("rev").and_then(Value::as_str) else {
                continue;
            };
            for node in replicas[slot]
                .iter()
                .copied()
                .filter(|node| *node != source)
            {
                let full = fetch_rev(st, db, dbname, source, &ids[slot], rev, headers).await?;
                batches.entry(node).or_default().push((slot, full));
            }
        }
        for (node, batch) in batches {
            let slots: Vec<usize> = batch.iter().map(|(slot, _)| *slot).collect();
            let docs: Vec<Value> = batch.into_iter().map(|(_, doc)| doc).collect();
            if write_local_or_forward(st, db, dbname, node, docs, false, headers)
                .await
                .is_ok()
            {
                for slot in slots {
                    acks[slot] += 1;
                }
            }
        }
        let met = acks.iter().all(|ack| *ack >= quorum);
        Ok((out, met))
    } else {
        // Replication writes carry their own history and must reach every
        // replica, including ones that are not the primary. A down replica is
        // not fatal: anti-entropy will heal it when it returns.
        let mut groups: HashMap<usize, Vec<(usize, Value)>> = HashMap::new();
        for (slot, doc) in docs.into_iter().enumerate() {
            for node in &replicas[slot] {
                groups.entry(*node).or_default().push((slot, doc.clone()));
            }
        }
        let mut reached = vec![false; total];
        for (node, group) in groups {
            let slots: Vec<usize> = group.iter().map(|(slot, _)| *slot).collect();
            let subdocs: Vec<Value> = group.iter().map(|(_, doc)| doc.clone()).collect();
            match write_local_or_forward(st, db, dbname, node, subdocs, false, headers).await {
                Ok(results) => {
                    for (slot, result) in slots.into_iter().zip(results) {
                        reached[slot] = true;
                        // Prefer the primary's verdict.
                        if node == primaries[slot] || out[slot].is_null() {
                            out[slot] = result;
                        }
                    }
                }
                Err(_) => {
                    for (slot, _) in group {
                        if out[slot].is_null() {
                            out[slot] = json!({
                                "id": ids[slot],
                                "error": "replica_unavailable",
                                "reason": "no replica in the shard set is reachable",
                            });
                        }
                    }
                }
            }
        }
        let met = reached.iter().all(|ok| *ok);
        Ok((out, met))
    }
}

async fn write_local_or_forward(
    st: &AppState,
    db: &Db,
    dbname: &str,
    node: usize,
    docs: Vec<Value>,
    new_edits: bool,
    headers: &HeaderMap,
) -> DbResult<Vec<Value>> {
    if node == st.cluster.me {
        return db.put_docs(docs, new_edits).await;
    }
    let peer = st.cluster.nodes[node].clone();
    forward_bulk(st, &peer, dbname, docs, new_edits, headers).await
}

/// Read one revision with its history and attachment bytes, for copying to
/// another replica. `attachments=true` matters: a stub would not carry the
/// chunk data the secondary needs to store the same bytes.
async fn fetch_rev(
    st: &AppState,
    db: &Db,
    dbname: &str,
    node: usize,
    id: &str,
    rev: &str,
    headers: &HeaderMap,
) -> DbResult<Value> {
    let opts = DocOpts {
        rev: Some(rev.to_string()),
        revs: true,
        attachments: true,
        ..Default::default()
    };
    if node == st.cluster.me {
        return db.get_doc(id, &opts).await;
    }
    let peer = &st.cluster.nodes[node];
    let url = format!("{peer}/{dbname}/{}", percent_encode(id));
    let mut request =
        st.client
            .get(url)
            .query(&[("rev", rev), ("revs", "true"), ("attachments", "true")]);
    if let Some(value) = headers.get(header::AUTHORIZATION) {
        request = request.header(header::AUTHORIZATION, value);
    }
    let response = request
        .send()
        .await
        .map_err(|e| ApiError::new(502, "bad_gateway", format!("{peer}: {e}")))?;
    if !response.status().is_success() {
        let status = response.status().as_u16();
        let text = response.text().await.unwrap_or_default();
        return Err(ApiError::new(status, "replica_fetch_failed", text));
    }
    response
        .json::<Value>()
        .await
        .map_err(|e| ApiError::new(502, "bad_gateway", format!("{peer}: {e}")))
}

fn bulk_response(new_edits: bool, results: Vec<Value>) -> Response {
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

async fn forward_bulk(
    st: &AppState,
    peer: &str,
    dbname: &str,
    docs: Vec<Value>,
    new_edits: bool,
    headers: &HeaderMap,
) -> DbResult<Vec<Value>> {
    let mut request = st
        .client
        .post(format!("{peer}/{dbname}/_bulk_docs"))
        .header("x-douchdb-forwarded", "1")
        .json(&json!({"docs": docs, "new_edits": new_edits}));
    if let Some(value) = headers.get(header::AUTHORIZATION) {
        request = request.header(header::AUTHORIZATION, value);
    }
    let response = request
        .send()
        .await
        .map_err(|e| ApiError::new(502, "bad_gateway", format!("{peer}: {e}")))?;
    if !response.status().is_success() {
        let status = response.status().as_u16();
        let text = response.text().await.unwrap_or_default();
        return Err(ApiError::new(status, "bulk_docs_failed", text));
    }
    match response.json::<Value>().await {
        Ok(Value::Array(values)) => Ok(values),
        Ok(_) => Err(ApiError::new(
            502,
            "bad_gateway",
            format!("{peer}: unexpected _bulk_docs response"),
        )),
        Err(e) => Err(ApiError::new(502, "bad_gateway", format!("{peer}: {e}"))),
    }
}

async fn bulk_get(
    st: &AppState,
    db: &Db,
    dbname: &str,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
) -> Response {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return fail(ApiError::bad_request("invalid JSON"));
    };
    let attachments = flag(q, "attachments");
    let revs = flag(q, "revs") || value.get("revs").and_then(Value::as_bool).unwrap_or(false);
    if !st.cluster.is_clustered() || is_forwarded(headers) {
        return match db.bulk_get(&value, attachments, revs).await {
            Ok(v) => json(StatusCode::OK, v),
            Err(e) => fail(e),
        };
    }
    let Some(reqs) = value.get("docs").and_then(Value::as_array).cloned() else {
        return fail(ApiError::bad_request("Missing JSON list of 'docs'"));
    };
    let mut groups: Vec<Vec<(usize, Value)>> =
        (0..st.cluster.nodes.len()).map(|_| Vec::new()).collect();
    for (i, req) in reqs.into_iter().enumerate() {
        let id = req.get("id").and_then(Value::as_str).unwrap_or_default();
        groups[st.cluster.owner_of_doc(id, db.q)].push((i, req));
    }
    let mut out = vec![Value::Null; groups.iter().map(Vec::len).sum()];
    for (owner, group) in groups.into_iter().enumerate() {
        if group.is_empty() {
            continue;
        }
        let indices: Vec<usize> = group.iter().map(|(i, _)| *i).collect();
        let subdocs: Vec<Value> = group.into_iter().map(|(_, req)| req).collect();
        let sub = json!({"docs": subdocs});
        let result = if owner == st.cluster.me {
            match db.bulk_get(&sub, attachments, revs).await {
                Ok(v) => v,
                Err(e) => return fail(e),
            }
        } else {
            let peer = st.cluster.nodes[owner].clone();
            let mut query = format!("revs={revs}");
            if attachments {
                query.push_str("&attachments=true");
            }
            match forward_json(st, &peer, dbname, "_bulk_get", &query, &sub, headers).await {
                Ok(v) => v,
                Err(e) => return fail(e),
            }
        };
        let rows = result["results"].as_array().cloned().unwrap_or_default();
        for (slot, row) in indices.into_iter().zip(rows) {
            out[slot] = row;
        }
    }
    json(StatusCode::OK, json!({"results": out}))
}

async fn revs_diff(
    st: &AppState,
    db: &Db,
    dbname: &str,
    body: &[u8],
    headers: &HeaderMap,
) -> Response {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return fail(ApiError::bad_request("invalid JSON"));
    };
    if !st.cluster.is_clustered() || is_forwarded(headers) {
        return match db.revs_diff(&value).await {
            Ok(v) => json(StatusCode::OK, v),
            Err(e) => fail(e),
        };
    }
    let Some(map) = value.as_object() else {
        return fail(ApiError::bad_request("Request body must be a JSON object"));
    };
    let mut groups: Vec<Map<String, Value>> =
        (0..st.cluster.nodes.len()).map(|_| Map::new()).collect();
    for (id, revs) in map {
        groups[st.cluster.owner_of_doc(id, db.q)].insert(id.clone(), revs.clone());
    }
    let mut out = Map::new();
    for (owner, group) in groups.into_iter().enumerate() {
        if group.is_empty() {
            continue;
        }
        let sub = Value::Object(group);
        let result = if owner == st.cluster.me {
            match db.revs_diff(&sub).await {
                Ok(v) => v,
                Err(e) => return fail(e),
            }
        } else {
            let peer = st.cluster.nodes[owner].clone();
            match forward_json(st, &peer, dbname, "_revs_diff", "", &sub, headers).await {
                Ok(v) => v,
                Err(e) => return fail(e),
            }
        };
        if let Some(obj) = result.as_object() {
            for (id, entry) in obj {
                out.insert(id.clone(), entry.clone());
            }
        }
    }
    json(StatusCode::OK, Value::Object(out))
}

async fn forward_json(
    st: &AppState,
    peer: &str,
    dbname: &str,
    endpoint: &str,
    query: &str,
    body: &Value,
    headers: &HeaderMap,
) -> DbResult<Value> {
    let mut url = format!("{peer}/{dbname}/{endpoint}");
    if !query.is_empty() {
        url.push('?');
        url.push_str(query);
    }
    let mut request = st
        .client
        .post(url)
        .header("x-douchdb-forwarded", "1")
        .json(body);
    if let Some(value) = headers.get(header::AUTHORIZATION) {
        request = request.header(header::AUTHORIZATION, value);
    }
    let response = request
        .send()
        .await
        .map_err(|e| ApiError::new(502, "bad_gateway", format!("{peer}: {e}")))?;
    if !response.status().is_success() {
        let status = response.status().as_u16();
        let text = response.text().await.unwrap_or_default();
        return Err(ApiError::new(status, "request_failed", text));
    }
    response
        .json::<Value>()
        .await
        .map_err(|e| ApiError::new(502, "bad_gateway", format!("{peer}: {e}")))
}

async fn all_docs(
    st: &AppState,
    db: &Db,
    dbname: &str,
    q: &HashMap<String, String>,
    method: &Method,
    body: &[u8],
    headers: &HeaderMap,
) -> Response {
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
    let result = if st.cluster.is_clustered() && !is_forwarded(headers) {
        cluster_all_docs(st, db, dbname, &opts, headers).await
    } else {
        db.all_docs(&opts).await
    };
    match result {
        Ok(v) => json(StatusCode::OK, v),
        Err(e) => fail(e),
    }
}

async fn cluster_all_docs(
    st: &AppState,
    db: &Db,
    dbname: &str,
    opts: &AllDocsOpts,
    headers: &HeaderMap,
) -> DbResult<Value> {
    let mut full = opts.clone();
    full.skip = 0;
    full.limit = usize::MAX;
    full.startkey = None;
    full.endkey = None;
    full.descending = false;

    // Count every live doc once. Nodes may hold replicated copies, so union by
    // id and prefer the row from the shard's owner.
    let mut count_opts = full.clone();
    count_opts.keys = None;
    count_opts.include_docs = false;
    count_opts.conflicts = false;
    count_opts.attachments = false;
    let mut live: HashMap<String, (usize, Value)> = HashMap::new();
    // Range mode needs the rows themselves (with include_docs/conflicts when
    // asked for); keys mode only needs the count here and fetches the rows
    // below.
    let scan_opts = if opts.keys.is_some() {
        &count_opts
    } else {
        &full
    };
    for node in 0..st.cluster.nodes.len() {
        let response = fetch_all_docs(st, db, dbname, node, scan_opts, headers).await?;
        for row in response["rows"].as_array().cloned().unwrap_or_default() {
            merge_all_docs_row(st, db, &mut live, node, row);
        }
    }
    let total_rows = live.len();

    if let Some(keys) = &opts.keys {
        // A keys query must also surface deleted docs, which a range scan
        // omits. Ask each node for exactly those keys.
        let mut key_opts = opts.clone();
        key_opts.skip = 0;
        key_opts.limit = usize::MAX;
        key_opts.startkey = None;
        key_opts.endkey = None;
        key_opts.descending = false;
        let mut found: HashMap<String, (usize, Value)> = HashMap::new();
        let mut errors: HashMap<String, Value> = HashMap::new();
        for node in 0..st.cluster.nodes.len() {
            let response = fetch_all_docs(st, db, dbname, node, &key_opts, headers).await?;
            for row in response["rows"].as_array().cloned().unwrap_or_default() {
                match row.get("id").and_then(Value::as_str) {
                    Some(_) => merge_all_docs_row(st, db, &mut found, node, row),
                    None => {
                        if let Some(key) = row.get("key").and_then(Value::as_str) {
                            errors.entry(key.to_string()).or_insert(row);
                        }
                    }
                }
            }
        }
        let mut rows = Vec::new();
        for key in keys.iter().skip(opts.skip).take(opts.limit) {
            if let Some((_, row)) = found.remove(key) {
                rows.push(row);
            } else if let Some(row) = errors.remove(key) {
                rows.push(row);
            } else {
                rows.push(json!({"key": key, "error": "not_found"}));
            }
        }
        return Ok(json!({"total_rows": total_rows, "offset": Value::Null, "rows": rows}));
    }

    let mut rows: Vec<(String, Value)> = live.into_iter().map(|(id, (_, row))| (id, row)).collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let before_start = match (&opts.startkey, opts.descending) {
        (Some(start), false) => rows.iter().filter(|(id, _)| id < start).count(),
        (Some(start), true) => rows.iter().filter(|(id, _)| id > start).count(),
        (None, _) => 0,
    };
    rows.retain(|(id, _)| {
        let after_start = opts.startkey.as_ref().map(|s| id >= s).unwrap_or(true);
        let before_end = opts.endkey.as_ref().map(|e| id <= e).unwrap_or(true);
        after_start && before_end
    });
    if opts.descending {
        rows.reverse();
    }
    let rows: Vec<Value> = rows
        .into_iter()
        .skip(opts.skip)
        .take(opts.limit)
        .map(|(_, row)| row)
        .collect();
    Ok(json!({"total_rows": total_rows, "offset": before_start + opts.skip, "rows": rows}))
}

fn merge_all_docs_row(
    st: &AppState,
    db: &Db,
    rows: &mut HashMap<String, (usize, Value)>,
    node: usize,
    row: Value,
) {
    let Some(id) = row.get("id").and_then(Value::as_str) else {
        return;
    };
    let owner = st.cluster.owner_of_doc(id, db.q);
    match rows.get(id) {
        Some((existing, _)) if *existing == owner => {}
        Some(_) if owner == node => {
            rows.insert(id.to_string(), (node, row));
        }
        None => {
            rows.insert(id.to_string(), (node, row));
        }
        _ => {}
    }
}

async fn fetch_all_docs(
    st: &AppState,
    db: &Db,
    dbname: &str,
    node: usize,
    opts: &AllDocsOpts,
    headers: &HeaderMap,
) -> DbResult<Value> {
    if node == st.cluster.me {
        return db.all_docs(opts).await;
    }
    let peer = &st.cluster.nodes[node];
    let mut query = Vec::new();
    if opts.include_docs {
        query.push("include_docs=true");
    }
    if opts.conflicts {
        query.push("conflicts=true");
    }
    if opts.attachments {
        query.push("attachments=true");
    }
    let mut url = format!("{peer}/{dbname}/_all_docs");
    if !query.is_empty() {
        url.push('?');
        url.push_str(&query.join("&"));
    }
    let body = match &opts.keys {
        Some(keys) => json!({"keys": keys, "include_docs": opts.include_docs}),
        None => json!({}),
    };
    let mut request = st
        .client
        .post(url)
        .header("x-douchdb-forwarded", "1")
        .json(&body);
    if let Some(value) = headers.get(header::AUTHORIZATION) {
        request = request.header(header::AUTHORIZATION, value);
    }
    let response = request
        .send()
        .await
        .map_err(|e| ApiError::new(502, "bad_gateway", e.to_string()))?;
    if !response.status().is_success() {
        let status = response.status().as_u16();
        let text = response.text().await.unwrap_or_default();
        return Err(ApiError::new(status, "all_docs_failed", text));
    }
    response
        .json::<Value>()
        .await
        .map_err(|e| ApiError::new(502, "bad_gateway", e.to_string()))
}

async fn changes(
    st: &AppState,
    db: &Db,
    dbname: &str,
    q: &HashMap<String, String>,
    method: &Method,
    body: &[u8],
    headers: &HeaderMap,
) -> Response {
    let _ = method;
    let local_only = is_forwarded(headers);
    let feed = q.get("feed").map(String::as_str).unwrap_or("normal");
    let deadline = num(q, "timeout").unwrap_or(30_000) as u64;

    if feed == "longpoll" || feed == "continuous" {
        let started = std::time::Instant::now();
        loop {
            match changes_once(st, db, dbname, q, body, headers, local_only).await {
                Ok(v)
                    if v["results"]
                        .as_array()
                        .map(|a| !a.is_empty())
                        .unwrap_or(false) =>
                {
                    return json(StatusCode::OK, v)
                }
                Ok(v) if started.elapsed().as_millis() as u64 >= deadline => {
                    return json(StatusCode::OK, v)
                }
                Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
                Err(e) => return fail(e),
            }
        }
    }

    match changes_once(st, db, dbname, q, body, headers, local_only).await {
        Ok(v) => json(StatusCode::OK, v),
        Err(e) => fail(e),
    }
}

async fn changes_once(
    st: &AppState,
    db: &Db,
    dbname: &str,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
    local_only: bool,
) -> DbResult<Value> {
    if st.cluster.is_clustered() && !local_only && !flag(q, "descending") {
        return cluster_changes(st, db, dbname, q, body, headers).await;
    }

    let since = if q.get("since").map(String::as_str) == Some("now") {
        db.info().await?["update_seq"].as_u64().unwrap_or(0)
    } else {
        since_component(q.get("since").map(String::as_str), st.cluster.me)
    };
    let opts = ChangesOpts {
        since,
        limit: num(q, "limit"),
        include_docs: flag(q, "include_docs"),
        style_all_docs: q.get("style").map(String::as_str) == Some("all_docs"),
        descending: flag(q, "descending"),
        doc_ids: doc_ids_from(q, body),
    };
    db.changes(&opts).await
}

fn doc_ids_from(q: &HashMap<String, String>, body: &[u8]) -> Option<Vec<String>> {
    if q.get("filter").map(String::as_str) != Some("_doc_ids") {
        return None;
    }
    if let Ok(value) = serde_json::from_slice::<Value>(body) {
        if let Some(ids) = value.get("doc_ids").and_then(Value::as_array) {
            return Some(
                ids.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect(),
            );
        }
    }
    q.get("doc_ids")
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
}

/// A cluster `since` is a base64url JSON object of `{node index: update_seq}`.
/// Clients treat it as opaque and hand it back verbatim, so no version of
/// PouchDB needs to understand it.
fn encode_since(vector: &[u64]) -> String {
    let map: Map<String, Value> = vector
        .iter()
        .enumerate()
        .map(|(i, seq)| (i.to_string(), json!(seq)))
        .collect();
    let bytes = serde_json::to_vec(&map).unwrap_or_else(|_| b"{}".to_vec());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn decode_since(raw: &str) -> Option<Vec<u64>> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw.trim_matches('"'))
        .ok()?;
    let value: Value = serde_json::from_slice(&bytes).ok()?;
    let mut out = Vec::new();
    for (key, seq) in value.as_object()? {
        let index: usize = key.parse().ok()?;
        if out.len() <= index {
            out.resize(index + 1, 0);
        }
        out[index] = seq.as_u64()?;
    }
    Some(out)
}

fn since_component(raw: Option<&str>, index: usize) -> u64 {
    let Some(raw) = raw else { return 0 };
    let raw = raw.trim_matches('"');
    if let Ok(seq) = raw.parse::<u64>() {
        return seq;
    }
    decode_since(raw)
        .and_then(|vector| vector.get(index).copied())
        .unwrap_or(0)
}

async fn cluster_changes(
    st: &AppState,
    db: &Db,
    dbname: &str,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
) -> DbResult<Value> {
    let n = st.cluster.nodes.len();
    let now = q.get("since").map(String::as_str) == Some("now");
    let mut since = if now {
        cluster_update_vector(st, db, dbname, headers).await?
    } else if let Some(raw) = q.get("since").map(String::as_str) {
        match decode_since(raw) {
            Some(mut vector) => {
                vector.resize(n.max(vector.len()), 0);
                vector
            }
            None => vec![since_component(Some(raw), st.cluster.me); n],
        }
    } else {
        vec![0; n]
    };
    since.resize(n, 0);

    let include_docs = flag(q, "include_docs");
    let style_all_docs = q.get("style").map(String::as_str) == Some("all_docs");
    let doc_ids = doc_ids_from(q, body);
    let limit = num(q, "limit");
    let mut progress = since.clone();
    let mut rows = Vec::new();

    for node in 0..n {
        let remaining = limit.map(|l| l.saturating_sub(rows.len()));
        if remaining == Some(0) {
            break;
        }
        let response = if node == st.cluster.me {
            db.changes(&ChangesOpts {
                since: since[node],
                limit: remaining,
                include_docs,
                style_all_docs,
                descending: false,
                doc_ids: doc_ids.clone(),
            })
            .await?
        } else {
            let peer = &st.cluster.nodes[node];
            let mut url = format!("{peer}/{dbname}/_changes?since={}", since[node]);
            if let Some(limit) = remaining {
                url.push_str(&format!("&limit={limit}"));
            }
            if include_docs {
                url.push_str("&include_docs=true");
            }
            if style_all_docs {
                url.push_str("&style=all_docs");
            }
            let mut request = st.client.get(url).header("x-douchdb-forwarded", "1");
            if let Some(value) = headers.get(header::AUTHORIZATION) {
                request = request.header(header::AUTHORIZATION, value);
            }
            let request = if let Some(ids) = &doc_ids {
                request
                    .query(&[("filter", "_doc_ids")])
                    .json(&json!({"doc_ids": ids}))
            } else {
                request
            };
            let response = request
                .send()
                .await
                .map_err(|e| ApiError::new(502, "bad_gateway", e.to_string()))?;
            if !response.status().is_success() {
                let status = response.status().as_u16();
                let text = response.text().await.unwrap_or_default();
                return Err(ApiError::new(status, "changes_failed", text));
            }
            response
                .json::<Value>()
                .await
                .map_err(|e| ApiError::new(502, "bad_gateway", e.to_string()))?
        };
        for mut row in response["results"].as_array().cloned().unwrap_or_default() {
            let shard_seq = row["seq"].as_u64().unwrap_or(0);
            progress[node] = shard_seq;
            row["seq"] = json!(encode_since(&progress));
            rows.push(row);
        }
    }

    // With N replicas the same revision arrives from more than one node. Keep
    // one row per document (the one furthest along the version vector) so
    // clients fed by any coordinator see each change once.
    let mut by_id: HashMap<String, usize> = HashMap::new();
    let mut deduped: Vec<Value> = Vec::new();
    for row in rows {
        let Some(id) = row.get("id").and_then(Value::as_str).map(str::to_string) else {
            deduped.push(row);
            continue;
        };
        let score = row
            .get("seq")
            .and_then(Value::as_str)
            .and_then(decode_since)
            .map(|vector| vector.iter().sum())
            .unwrap_or(0);
        match by_id.get(&id) {
            Some(&index) => {
                let existing = deduped[index]
                    .get("seq")
                    .and_then(Value::as_str)
                    .and_then(decode_since)
                    .map(|vector| vector.iter().sum())
                    .unwrap_or(0);
                if score > existing {
                    deduped[index] = row;
                }
            }
            None => {
                by_id.insert(id, deduped.len());
                deduped.push(row);
            }
        }
    }

    Ok(json!({
        "results": deduped,
        "last_seq": encode_since(&progress),
        "pending": 0,
    }))
}

/// Current per-node update_seq vector. Used for `since=now` and for the
/// `update_seq` reported in `GET /db`.
async fn cluster_update_vector(
    st: &AppState,
    db: &Db,
    dbname: &str,
    headers: &HeaderMap,
) -> DbResult<Vec<u64>> {
    let mut out = vec![0; st.cluster.nodes.len()];
    for (node, slot) in out.iter_mut().enumerate() {
        if node == st.cluster.me {
            *slot = db.info().await?["update_seq"].as_u64().unwrap_or(0);
            continue;
        }
        let peer = &st.cluster.nodes[node];
        let mut request = st
            .client
            .get(format!("{peer}/{dbname}"))
            .header("x-douchdb-forwarded", "1");
        if let Some(value) = headers.get(header::AUTHORIZATION) {
            request = request.header(header::AUTHORIZATION, value);
        }
        if let Ok(response) = request.send().await {
            if response.status().is_success() {
                if let Ok(info) = response.json::<Value>().await {
                    *slot = info["update_seq"].as_u64().unwrap_or(0);
                }
            }
        }
    }
    Ok(out)
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

    let read_quorum = num(q, "r").unwrap_or(1);
    if attachment.is_none()
        && (method == Method::GET || method == Method::HEAD)
        && st.cluster.is_clustered()
        && read_quorum > 1
    {
        return quorum_read(st, db, dbname, &docid, q, headers, read_quorum).await;
    }

    // Serve locally if this node holds a replica; otherwise try every replica
    // in preference order so a dead primary does not break reads or writes.
    let replicas = st.cluster.replicas_of_doc(&docid, db.q);
    if st.cluster.is_clustered() && !replicas.contains(&st.cluster.me) {
        let mut last = None;
        for node in replicas {
            let peer = st.cluster.nodes[node].clone();
            let response = proxy(st, &method, dbname, rest, q, headers, body.clone(), &peer).await;
            if !unavailable(&response) {
                return response;
            }
            last = Some(response);
        }
        return last.unwrap_or_else(|| {
            fail(ApiError::new(
                502,
                "bad_gateway",
                "no replica in the shard set is reachable",
            ))
        });
    }

    let Some(attachment) = attachment else {
        return match method {
            Method::GET | Method::HEAD => get_doc(db, &docid, q).await,
            Method::PUT => put_doc(st, db, dbname, &docid, body, q, headers).await,
            Method::DELETE => delete_doc(st, db, dbname, &docid, q, headers).await,
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
            match commit_docs(
                st,
                db,
                dbname,
                vec![doc],
                true,
                num(q, "w").unwrap_or(0),
                headers,
            )
            .await
            {
                Ok((mut results, quorum)) => {
                    let result = results.pop().unwrap_or(Value::Null);
                    if result.get("error").is_some() {
                        return json(StatusCode::CONFLICT, result);
                    }
                    let status = if quorum {
                        StatusCode::CREATED
                    } else {
                        StatusCode::ACCEPTED
                    };
                    json(status, result)
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
            let result = match commit_docs(
                st,
                db,
                dbname,
                vec![doc],
                true,
                num(q, "w").unwrap_or(0),
                headers,
            )
            .await
            {
                Ok((mut results, quorum)) => (results.pop().unwrap_or(Value::Null), quorum),
                Err(e) => return fail(e),
            };
            let (result, quorum) = result;
            if result.get("error").is_some() {
                return json(StatusCode::CONFLICT, result);
            }
            json(
                if quorum {
                    StatusCode::OK
                } else {
                    StatusCode::ACCEPTED
                },
                result,
            )
        }
        _ => fail(ApiError::new(
            405,
            "method_not_allowed",
            "method not allowed",
        )),
    }
}

fn split_doc_path(rest: &str) -> (String, Option<String>) {
    // Split on literal slashes first, then decode each segment. That keeps
    // `a%2Fb` as one document id while `doc/att` is still an attachment path.
    let segments: Vec<String> = rest.split('/').map(percent_decode).collect();
    if segments.is_empty() {
        return (String::new(), None);
    }
    if segments[0] == "_design" && segments.len() > 1 {
        return (format!("_design/{}", segments[1]), segments.get(2).cloned());
    }
    (segments[0].clone(), segments.get(1).cloned())
}

fn doc_opts(q: &HashMap<String, String>) -> DocOpts {
    DocOpts {
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
    }
}

fn doc_response(doc: Value) -> Response {
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

async fn get_doc(db: &Db, docid: &str, q: &HashMap<String, String>) -> Response {
    match db.get_doc(docid, &doc_opts(q)).await {
        Ok(doc) => doc_response(doc),
        Err(e) => fail(e),
    }
}

/// Compare two document versions with CouchDB's winner rule: live beats
/// deleted, then higher position, then greater revision id.
fn better_rev(candidate: &Value, current: &Value) -> bool {
    let key = |doc: &Value| {
        let deleted = doc
            .get("_deleted")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let rev = doc.get("_rev").and_then(Value::as_str).unwrap_or_default();
        let parsed = crate::rev::Rev::parse(rev);
        let pos = parsed.as_ref().map(|r| r.pos).unwrap_or(0);
        let id = parsed.map(|r| r.id).unwrap_or_default();
        (deleted, pos, id)
    };
    let a = key(candidate);
    let b = key(current);
    (!a.0, a.1, a.2) > (!b.0, b.1, b.2)
}

/// `?r=N` reads from several replicas and returns the winning revision.
async fn quorum_read(
    st: &AppState,
    db: &Db,
    dbname: &str,
    docid: &str,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
    r: usize,
) -> Response {
    let mut best: Option<Value> = None;
    // Strip `r` from the hop so the replica answers locally instead of
    // starting another quorum read and bouncing the request back.
    let mut inner = q.clone();
    inner.remove("r");
    for node in st.cluster.replicas_of_doc(docid, db.q).into_iter().take(r) {
        let doc = if node == st.cluster.me {
            db.get_doc(docid, &doc_opts(q)).await.ok()
        } else {
            let peer = st.cluster.nodes[node].clone();
            let response = proxy(
                st,
                &Method::GET,
                dbname,
                docid,
                &inner,
                headers,
                Bytes::new(),
                &peer,
            )
            .await;
            if response.status().is_success() {
                axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            } else {
                None
            }
        };
        if let Some(doc) = doc {
            if best.as_ref().map(|b| better_rev(&doc, b)).unwrap_or(true) {
                best = Some(doc);
            }
        }
    }
    match best {
        Some(doc) => doc_response(doc),
        None => fail(ApiError::not_found("missing")),
    }
}

async fn put_doc(
    st: &AppState,
    db: &Db,
    dbname: &str,
    docid: &str,
    body: Bytes,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Response {
    let mut doc = match serde_json::from_slice::<Value>(&body) {
        Ok(v) => v,
        Err(e) => return fail(ApiError::bad_request(format!("invalid JSON: {e}"))),
    };
    if let Err(e) = validate_doc(&doc) {
        return fail(e);
    }
    // The URL owns the id: CouchDB ignores a conflicting `_id` in the body.
    doc["_id"] = json!(docid);
    let new_edits = !q.contains_key("new_edits") || flag(q, "new_edits");
    if !new_edits && doc.get("_rev").is_none() {
        return fail(ApiError::bad_request("Document is missing _rev"));
    }
    let tombstone = doc
        .get("_deleted")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    match commit_docs(
        st,
        db,
        dbname,
        vec![doc],
        new_edits,
        num(q, "w").unwrap_or(0),
        headers,
    )
    .await
    {
        Ok((mut results, quorum)) => {
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
            // CouchDB answers 200, not 201, when a PUT lands as a tombstone.
            let status = if tombstone {
                StatusCode::OK
            } else if quorum {
                StatusCode::CREATED
            } else {
                StatusCode::ACCEPTED
            };
            json(status, result)
        }
        Err(e) => fail(e),
    }
}

async fn delete_doc(
    st: &AppState,
    db: &Db,
    dbname: &str,
    docid: &str,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Response {
    // A missing document is 404 even without a `rev`; only a live document
    // without a rev is a conflict. A tombstone reports reason "deleted".
    if let Err(e) = db.get_doc(docid, &DocOpts::default()).await {
        return fail(e);
    }
    let Some(rev) = q.get("rev") else {
        return fail(ApiError::conflict());
    };
    let doc = json!({"_id": docid, "_rev": rev, "_deleted": true});
    match commit_docs(
        st,
        db,
        dbname,
        vec![doc],
        true,
        num(q, "w").unwrap_or(0),
        headers,
    )
    .await
    {
        Ok((mut results, quorum)) => {
            let mut result = results.pop().unwrap_or(Value::Null);
            if result.get("error").is_some() {
                if let Some(obj) = result.as_object_mut() {
                    obj.remove("id");
                }
                return json(StatusCode::CONFLICT, result);
            }
            json(
                if quorum {
                    StatusCode::OK
                } else {
                    StatusCode::ACCEPTED
                },
                result,
            )
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

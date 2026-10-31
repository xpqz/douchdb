//! HTTP surface. The shape of every response here is dictated by CouchDB's
//! wire protocol, not by taste.

use crate::auth::Auth;
use crate::cluster::{Cluster, ShardMap};
use crate::db::{
    validate_doc, AllDocsOpts, ApiError, ChangesOpts, Db, DbConfig, DbResult, DocOpts,
};
use crate::fabric::*;
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::StreamExt;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

#[derive(Default)]
pub struct DbUpdates {
    seq: AtomicU64,
    events: Mutex<Vec<Value>>,
}

impl DbUpdates {
    pub(crate) fn record(&self, name: &str, kind: &str) {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let mut events = self
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        events.push(json!({
            "seq": seq.to_string(),
            "db_name": name,
            "type": kind,
        }));
        if events.len() > 5000 {
            let excess = events.len() - 5000;
            events.drain(0..excess);
        }
    }

    fn current(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }

    fn since(&self, after: u64) -> (Vec<Value>, u64) {
        let events = self
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let results = events
            .iter()
            .filter(|event| {
                event["seq"]
                    .as_str()
                    .and_then(|seq| seq.parse::<u64>().ok())
                    .map(|seq| seq > after)
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        (results, self.current())
    }
}

pub struct AppState {
    pub root: PathBuf,
    pub dbs: RwLock<HashMap<String, Arc<Db>>>,
    pub config: Config,
    pub auth: Option<Auth>,
    pub cluster: Cluster,
    pub client: reqwest::Client,
    pub db_updates: DbUpdates,
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
    pub view_timeout_ms: u64,
    pub view_memory_mb: usize,
}

// ---------------------------------------------------------------------------
// small helpers

fn json(status: StatusCode, value: Value) -> Response {
    (status, Json(value)).into_response()
}

pub(crate) fn fail(e: ApiError) -> Response {
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

pub(crate) fn flag(q: &HashMap<String, String>, key: &str) -> bool {
    matches!(q.get(key).map(String::as_str), Some("true") | Some("1"))
}

pub(crate) fn num(q: &HashMap<String, String>, key: &str) -> Option<usize> {
    q.get(key).and_then(|v| v.parse().ok())
}

fn parse_shard_mask(raw: &str, q: u32) -> Option<Vec<bool>> {
    let mut mask = vec![false; q as usize];
    for part in raw.split(',').filter(|part| !part.is_empty()) {
        let shard: usize = part.parse().ok()?;
        if shard >= mask.len() {
            return None;
        }
        mask[shard] = true;
    }
    Some(mask)
}

/// True when a proxied request failed to reach the peer, rather than the peer
/// answering on behalf of the cluster.

/// True for a request a peer already routed. Such requests are served from the
/// local node's shards: re-running the fan-out on the receiving side would send
/// the request straight back and loop.
fn is_forwarded(st: &AppState, headers: &HeaderMap) -> bool {
    st.cluster.is_internal(headers)
}

/// Internal cluster calls must keep the caller's credentials. Basic auth and
/// the `_session` cookie both arrive as plain headers; the peer is trusted, so
/// forward whichever one the client used.
pub(crate) fn forward_client_auth(
    request: reqwest::RequestBuilder,
    headers: &HeaderMap,
) -> reqwest::RequestBuilder {
    let request = match headers.get(header::AUTHORIZATION) {
        Some(value) => request.header(header::AUTHORIZATION, value),
        None => request,
    };
    match headers.get(header::COOKIE) {
        Some(value) => request.header(header::COOKIE, value),
        None => request,
    }
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
                shard_map: self.cluster.shard_map(self.config.q),
                partitioned: false,
            },
        )?);
        self.dbs.write().await.insert(name.to_string(), db.clone());
        Ok(db)
    }

    pub async fn create_db(&self, name: &str, partitioned: bool) -> DbResult<Arc<Db>> {
        crate::view::forget_db(&self.root.join(name));
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
                shard_map: self.cluster.shard_map(self.config.q),
                partitioned,
            },
        )?);
        self.dbs.write().await.insert(name.to_string(), db.clone());
        self.db_updates.record(name, "created");
        Ok(db)
    }

    pub async fn drop_db(&self, name: &str) -> DbResult<()> {
        self.dbs.write().await.remove(name);
        crate::view::forget_db(&self.root.join(name));
        Db::destroy(&self.root, name)?;
        self.db_updates.record(name, "deleted");
        Ok(())
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
/// Baseline response hardening. TLS is an operator concern; these headers still
/// stop MIME sniffing and framing when the console is served directly.
pub async fn request_id(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let id = crate::db::uuid();
    let mut response = next.run(req).await;
    if let Ok(value) = HeaderValue::from_str(&id) {
        response.headers_mut().insert("x-couch-request-id", value);
    }
    response
}

pub async fn head_only(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let head = req.method() == Method::HEAD;
    let mut response = next.run(req).await;
    if head {
        *response.body_mut() = Body::empty();
    }
    response
}

pub async fn security_headers(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let mut response = next.run(req).await;
    let headers = response.headers_mut();
    headers
        .entry(header::X_CONTENT_TYPE_OPTIONS)
        .or_insert(HeaderValue::from_static("nosniff"));
    headers
        .entry(header::X_FRAME_OPTIONS)
        .or_insert(HeaderValue::from_static("DENY"));
    headers
        .entry(header::REFERRER_POLICY)
        .or_insert(HeaderValue::from_static("no-referrer"));
    response
}

pub async fn log_requests(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    use tracing::Instrument;

    let method = req.method().clone();
    let uri = req.uri().clone();
    let started = std::time::Instant::now();
    let span = tracing::info_span!(
        "http.request",
        "http.method" = %method,
        "http.url" = %uri
    );
    let response = next.run(req).instrument(span).await;
    let status = response.status().as_u16();
    crate::telemetry::record_http(
        method.as_str(),
        status,
        started.elapsed().as_secs_f64() * 1000.0,
    );
    tracing::info!("{} {} -> {}", method, uri, status);
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
            "features": ["access-ready", "partitioned", "pluggable-storage-engines", "reshard", "scheduler", "search"],
        }),
    )
}

pub async fn up() -> Response {
    json(StatusCode::OK, json!({"status": "ok"}))
}

pub async fn all_dbs(State(st): State<Arc<AppState>>, headers: HeaderMap, uri: Uri) -> Response {
    if !st.authorized(&headers) {
        return unauthorized();
    }
    let q = query(&uri);
    let mut names = st.db_names().await;
    if st.cluster.is_clustered() && !st.cluster.is_internal_db(&headers) {
        for (i, peer) in st.cluster.nodes.iter().enumerate() {
            if i == st.cluster.me {
                continue;
            }
            let request = st
                .client
                .get(format!("{peer}/_all_dbs"))
                .header("x-douchdb-local-db", "1")
                .header("x-douchdb-secret", st.cluster.secret.as_str());
            let request = forward_client_auth(request, &headers);
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

    let descending = flag(&q, "descending");
    if descending {
        names.reverse();
    }
    if let Some(start) = key_param(&q, "startkey").or_else(|| key_param(&q, "start_key")) {
        names.retain(|name| {
            if descending {
                name <= &start
            } else {
                name >= &start
            }
        });
    }
    if let Some(end) = key_param(&q, "endkey").or_else(|| key_param(&q, "end_key")) {
        names.retain(|name| {
            if descending {
                name >= &end
            } else {
                name <= &end
            }
        });
    }
    let skip = num(&q, "skip").unwrap_or(0);
    let limit = num(&q, "limit").unwrap_or(usize::MAX);
    names = names.into_iter().skip(skip).take(limit).collect();
    json(StatusCode::OK, json!(names))
}

pub async fn uuids(State(_st): State<Arc<AppState>>, uri: Uri) -> Response {
    let count = num(&query(&uri), "count").unwrap_or(1).clamp(1, 1000);
    let ids: Vec<String> = (0..count).map(|_| crate::db::uuid()).collect();
    json(StatusCode::OK, json!({ "uuids": ids }))
}

/// `_db_updates` is an in-memory global feed of database lifecycle and write
/// events. It is intentionally best-effort: a restart starts a new sequence.
pub async fn db_updates(State(st): State<Arc<AppState>>, headers: HeaderMap, uri: Uri) -> Response {
    if !st.authorized(&headers) {
        return unauthorized();
    }
    let q = query(&uri);
    let feed = q.get("feed").map(String::as_str).unwrap_or("normal");
    let timeout = num(&q, "timeout").unwrap_or(60_000).min(300_000) as u64;
    let heartbeat = num(&q, "heartbeat")
        .filter(|value| *value > 0)
        .map(|value| std::time::Duration::from_millis(value as u64));
    let limit = num(&q, "limit");
    let mut since = match q.get("since").map(String::as_str) {
        Some("now") => st.db_updates.current(),
        Some(raw) => raw.trim_matches('"').parse::<u64>().unwrap_or(0),
        None => 0,
    };

    if feed == "continuous" {
        let stream = async_stream::stream! {
            let started = std::time::Instant::now();
            let mut sent = 0usize;
            let mut last_heartbeat = std::time::Instant::now();
            loop {
                if limit.map(|limit| sent >= limit).unwrap_or(false) {
                    break;
                }
                let (events, _) = st.db_updates.since(since);
                for event in events {
                    if limit.map(|limit| sent >= limit).unwrap_or(false) {
                        break;
                    }
                    if let Some(seq) = event["seq"].as_str().and_then(|seq| seq.parse().ok()) {
                        since = seq;
                    }
                    let mut line = serde_json::to_vec(&event).unwrap_or_default();
                    line.push(b'\n');
                    yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                    sent += 1;
                }
                if started.elapsed().as_millis() as u64 >= timeout {
                    break;
                }
                if let Some(heartbeat) = heartbeat {
                    if last_heartbeat.elapsed() >= heartbeat {
                        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"\n"));
                        last_heartbeat = std::time::Instant::now();
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        };
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from_stream(stream))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    let started = std::time::Instant::now();
    loop {
        let (events, current) = st.db_updates.since(since);
        if !events.is_empty() || feed == "normal" || started.elapsed().as_millis() as u64 >= timeout
        {
            let selected: Vec<Value> = events
                .into_iter()
                .take(limit.unwrap_or(usize::MAX))
                .collect();
            let last_seq = selected
                .last()
                .and_then(|event| event["seq"].as_str())
                .map(str::to_string)
                .unwrap_or_else(|| current.to_string());
            return json(
                StatusCode::OK,
                json!({"results": selected, "last_seq": last_seq}),
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

pub async fn dbs_info(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Response {
    if !st.authorized(&headers) {
        return unauthorized();
    }
    let q = query(&uri);
    let keys = if method == Method::POST {
        serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|value| value.get("keys").and_then(Value::as_array).cloned())
    } else {
        q.get("keys")
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            .and_then(|value| value.as_array().cloned())
    };
    let Some(keys) = keys else {
        return fail(ApiError::bad_request("`keys` must be a JSON array"));
    };
    let mut results = Vec::new();
    for key in keys {
        let Some(name) = key.as_str() else {
            results.push(json!({"key": key, "error": "bad_request"}));
            continue;
        };
        match st.db(name).await {
            Ok(db) => match db.info().await {
                Ok(info) => results.push(json!({"key": name, "info": info})),
                Err(error) => results.push(json!({
                    "key": name,
                    "error": error.error,
                    "reason": error.reason,
                })),
            },
            Err(error) => results.push(json!({
                "key": name,
                "error": error.error,
                "reason": error.reason,
            })),
        }
    }
    json(StatusCode::OK, Value::Array(results))
}

pub async fn membership(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !st.authorized(&headers) {
        return unauthorized();
    }
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

/// mem3-style shard-map inspection and movement endpoint.
pub async fn dbs_metadata(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !st.authorized(&headers) {
        return unauthorized();
    }
    let mut out = Map::new();
    for name in st.db_names().await {
        if let Ok(db) = st.db(&name).await {
            out.insert(name, json!(db.shard_map()));
        }
    }
    json(StatusCode::OK, Value::Object(out))
}

pub async fn db_metadata(
    State(st): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !st.authorized(&headers) {
        return unauthorized();
    }
    match st.db(&name).await {
        Ok(db) => json(StatusCode::OK, json!(db.shard_map())),
        Err(error) => fail(error),
    }
}

pub async fn db_shard_counts(
    State(st): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !st.authorized(&headers) {
        return unauthorized();
    }
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(error) => return fail(error),
    };
    match db.shard_counts().await {
        Ok(counts) => json(
            StatusCode::OK,
            json!({
                "shards": counts
                    .into_iter()
                    .enumerate()
                    .map(|(shard, (docs, deleted))| {
                        json!({"shard": shard, "docs": docs, "deleted": deleted})
                    })
                    .collect::<Vec<_>>()
            }),
        ),
        Err(error) => fail(error),
    }
}

pub async fn db_metadata_put(
    State(st): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !st.authorized(&headers) {
        return unauthorized();
    }
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(error) => return fail(error),
    };
    let map: ShardMap = match serde_json::from_slice(&body) {
        Ok(map) => map,
        Err(_) => return fail(ApiError::bad_request("invalid shard map")),
    };
    match apply_shard_map(&st, &db, &name, map, &headers).await {
        Ok((map, pending)) => {
            let mut value = json!(map);
            value["pending"] = json!(pending);
            json(StatusCode::OK, value)
        }
        Err(error) => fail(error),
    }
}

async fn apply_shard_map(
    st: &AppState,
    db: &Arc<Db>,
    name: &str,
    mut map: ShardMap,
    headers: &HeaderMap,
) -> DbResult<(ShardMap, bool)> {
    if map.q != db.q || map.node_count != st.cluster.nodes.len().max(1) {
        return Err(ApiError::bad_request(
            "shard map does not match database q or cluster node count",
        ));
    }
    let internal = is_forwarded(st, headers);
    if !internal {
        map.version = db.shard_map().version.saturating_add(1);
    }
    db.set_shard_map(map.clone())?;
    if internal {
        db.clear_pending_shard_map();
        return Ok((map, false));
    }

    let payload = serde_json::to_vec(&map)
        .map_err(|error| ApiError::new(500, "internal_server_error", error.to_string()))?;
    let peers: Vec<String> = st
        .cluster
        .nodes
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != st.cluster.me)
        .map(|(_, peer)| peer.clone())
        .collect();
    let responses = futures::future::join_all(peers.into_iter().map(|peer| {
        let payload = payload.clone();
        async move {
            let request = st
                .client
                .put(format!("{peer}/_node/_local/_dbs/{name}"))
                .header("x-douchdb-forwarded", "1")
                .header("x-douchdb-secret", st.cluster.secret.as_str())
                .header(header::CONTENT_TYPE, "application/json")
                .body(payload);
            let request = forward_client_auth(request, headers);
            request
                .send()
                .await
                .map(|response| response.status().is_success())
                .unwrap_or(false)
        }
    }))
    .await;
    let mut acknowledgements = 1usize; // local write
    for acknowledged in responses {
        if acknowledged {
            acknowledgements += 1;
        }
    }
    let quorum = st.cluster.nodes.len().max(1) / 2 + 1;
    let pending = acknowledgements < quorum;
    if pending {
        db.set_pending_shard_map(&map)?;
    } else {
        db.clear_pending_shard_map();
    }
    crate::telemetry::record_shard_map_update(pending);
    Ok((map, pending))
}

async fn fetch_shard_docs(
    st: &AppState,
    db: &Arc<Db>,
    node: usize,
    headers: &HeaderMap,
) -> Option<Vec<u64>> {
    if node == st.cluster.me {
        return db
            .shard_counts()
            .await
            .ok()
            .map(|counts| counts.into_iter().map(|(docs, _)| docs).collect());
    }
    let request = st
        .client
        .get(format!(
            "{}/_node/_local/_dbs/{}/shards",
            st.cluster.nodes[node], db.name
        ))
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", st.cluster.secret.as_str());
    let request = forward_client_auth(request, headers);
    let value = request.send().await.ok()?.json::<Value>().await.ok()?;
    let mut out = Vec::new();
    for row in value["shards"].as_array()? {
        out.push(row.get("docs").and_then(Value::as_u64).unwrap_or(0));
    }
    Some(out)
}

async fn wait_for_rebalance(
    st: &AppState,
    db: &Arc<Db>,
    map: &ShardMap,
    required: &[u64],
    headers: &HeaderMap,
    timeout_ms: u64,
) -> DbResult<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    loop {
        let counts = futures::future::join_all(
            (0..st.cluster.nodes.len()).map(|node| fetch_shard_docs(st, db, node, headers)),
        )
        .await;
        let ready = (0..db.q).all(|shard| {
            let required = required.get(shard as usize).copied().unwrap_or(0);
            map.replicas_of(shard).iter().all(|node| {
                counts
                    .get(*node)
                    .and_then(|count| count.as_ref())
                    .and_then(|count| count.get(shard as usize))
                    .copied()
                    .map(|docs| docs >= required)
                    .unwrap_or(false)
            })
        });
        if ready {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(ApiError::new(
                504,
                "rebalance_timeout",
                "new replicas did not report the shard data before the timeout",
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn cleanup_all_replicas(
    st: &AppState,
    db: &Arc<Db>,
    headers: &HeaderMap,
) -> DbResult<Vec<Value>> {
    let responses = futures::future::join_all(st.cluster.nodes.iter().map(|peer| {
        let request = st
            .client
            .post(format!("{peer}/_node/_local/_dbs/{}/cleanup", db.name))
            .header("x-douchdb-forwarded", "1")
            .header("x-douchdb-secret", st.cluster.secret.as_str());
        let request = forward_client_auth(request, headers);
        async move {
            let response = request
                .send()
                .await
                .map_err(|error| ApiError::new(502, "bad_gateway", format!("{peer}: {error}")))?;
            if !response.status().is_success() {
                return Err(ApiError::new(
                    response.status().as_u16(),
                    "cleanup_failed",
                    response.text().await.unwrap_or_default(),
                ));
            }
            response
                .json::<Value>()
                .await
                .map_err(|error| ApiError::new(502, "bad_gateway", error.to_string()))
        }
    }))
    .await;
    let mut out = Vec::new();
    for response in responses {
        out.push(response?);
    }
    Ok(out)
}

pub async fn db_rebalance(
    State(st): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if !st.authorized(&headers) {
        return unauthorized();
    }
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(error) => return fail(error),
    };
    let q = query(&uri);
    let cleanup = q
        .get("cleanup")
        .map(|value| value != "false")
        .unwrap_or(true);
    let timeout_ms = num(&q, "timeout_ms").unwrap_or(30_000).min(300_000) as u64;

    crate::telemetry::record_rebalance_started();
    let old_map = db.shard_map();
    let counts = futures::future::join_all(
        (0..st.cluster.nodes.len()).map(|node| fetch_shard_docs(&st, &db, node, &headers)),
    )
    .await;
    let mut required = Vec::new();
    for shard in 0..db.q {
        let mut best = None;
        for node in old_map.replicas_of(shard) {
            if let Some(docs) = counts
                .get(*node)
                .and_then(|count| count.as_ref())
                .and_then(|count| count.get(shard as usize))
            {
                best = Some(best.map_or(*docs, |current: u64| current.max(*docs)));
            }
        }
        let Some(best) = best else {
            return fail(ApiError::new(
                502,
                "rebalance_failed",
                "no reachable old replica for shard",
            ));
        };
        required.push(best);
    }

    let map = ShardMap::balanced(
        db.q,
        st.cluster.nodes.len().max(1),
        st.cluster.replication_factor(),
    );
    let (map, pending) = match apply_shard_map(&st, &db, &name, map, &headers).await {
        Ok(result) => result,
        Err(error) => return fail(error),
    };
    let mut value = json!(map.clone());
    value["pending"] = json!(pending);
    if !cleanup || pending {
        return json(StatusCode::OK, value);
    }

    if let Err(error) = wait_for_rebalance(&st, &db, &map, &required, &headers, timeout_ms).await {
        if error.status == 504 {
            crate::telemetry::record_rebalance_timeout();
        }
        return fail(error);
    }
    match cleanup_all_replicas(&st, &db, &headers).await {
        Ok(cleaned) => {
            let cleared = cleaned
                .iter()
                .filter_map(|response| response["cleared"].as_array())
                .map(|shards| shards.len() as u64)
                .sum();
            crate::telemetry::record_rebalance_completed(cleared);
            value["cleaned"] = json!(cleaned);
            json(StatusCode::OK, value)
        }
        Err(error) => fail(error),
    }
}

pub async fn db_cleanup(
    State(st): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !st.authorized(&headers) {
        return unauthorized();
    }
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(error) => return fail(error),
    };
    let map = db.shard_map();
    let mut cleared = Vec::new();
    for shard in 0..db.q {
        if !map.replicas_of(shard).contains(&st.cluster.me) {
            cleared.push(shard as usize);
        }
    }
    match db.clear_shards(&cleared).await {
        Ok(()) => json(StatusCode::OK, json!({"cleared": cleared})),
        Err(error) => fail(error),
    }
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
        "AuthSession=; Version=1; Path=/; Max-Age=0; HttpOnly; SameSite=Strict"
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

    let local_only = st.cluster.is_internal_db(&headers);
    match method {
        Method::PUT => {
            let partitioned = flag(&q, "partitioned");
            if let Err(e) = st.create_db(&name, partitioned).await {
                return fail(e);
            }
            if st.cluster.is_clustered() && !local_only {
                for peer in st.cluster.nodes.iter() {
                    if peer == &st.cluster.nodes[st.cluster.me] {
                        continue;
                    }
                    let mut url = format!("{peer}/{name}");
                    if partitioned {
                        url.push_str("?partitioned=true");
                    }
                    let request = st
                        .client
                        .put(url)
                        .header("x-douchdb-local-db", "1")
                        .header("x-douchdb-secret", st.cluster.secret.as_str());
                    let request = forward_client_auth(request, &headers);
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
            let mut response = json(StatusCode::CREATED, json!({"ok": true}));
            if let Ok(value) = format!("/{name}").parse() {
                response.headers_mut().insert(header::LOCATION, value);
            }
            response
        }
        Method::DELETE => {
            let local = st.drop_db(&name).await;
            if st.cluster.is_clustered() && !local_only {
                for (i, peer) in st.cluster.nodes.iter().enumerate() {
                    if i == st.cluster.me {
                        continue;
                    }
                    let request = st
                        .client
                        .delete(format!("{peer}/{name}"))
                        .header("x-douchdb-local-db", "1")
                        .header("x-douchdb-secret", st.cluster.secret.as_str());
                    let request = forward_client_auth(request, &headers);
                    let _ = request.send().await;
                }
            }
            match local {
                Ok(()) => ok(),
                Err(e) => fail(e),
            }
        }
        Method::GET | Method::HEAD => match st.db(&name).await {
            Ok(db) => {
                if is_forwarded(&st, &headers)
                    && headers
                        .get("x-douchdb-update-seq")
                        .and_then(|value| value.to_str().ok())
                        == Some("1")
                {
                    return json(StatusCode::OK, json!({"update_seq": db.update_seq()}));
                }
                match db.info().await {
                    Ok(mut info) => {
                        // In a cluster the update_seq is the same version vector
                        // the changes feed returns, so clients that use it as
                        // `since` do not skip another node's changes.
                        if st.cluster.is_clustered() && !is_forwarded(&st, &headers) {
                            if let Ok(vector) =
                                cluster_update_vector(&st, &db, &name, &headers).await
                            {
                                let seq = encode_since(&vector);
                                info["update_seq"] = json!(seq);
                                info["committed_update_seq"] = json!(seq);
                            }
                        }
                        json(StatusCode::OK, info)
                    }
                    Err(e) => fail(e),
                }
            }
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
            let id = match doc.get("_id") {
                Some(Value::String(id)) if !id.is_empty() => id.clone(),
                Some(Value::String(_)) => {
                    return fail(ApiError::new(
                        400,
                        "illegal_docid",
                        "Document id must not be empty",
                    ))
                }
                Some(_) => {
                    return fail(ApiError::new(
                        400,
                        "illegal_docid",
                        "Document id must be a string",
                    ))
                }
                None => unreachable!("id generated above"),
            };
            if let Err(e) = db.validate_doc_id(&id) {
                return fail(e);
            }
            // The id only exists after the body is parsed, so the shard's
            // replicas are known here and a generated document can still be
            // routed. Try each replica so a dead primary does not lose it.
            let replicas = replicas_of_doc(&db, &id);
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

async fn partition_info(
    st: &AppState,
    db: &Db,
    name: &str,
    partition: &str,
    method: &Method,
    headers: &HeaderMap,
) -> Response {
    if *method != Method::GET && *method != Method::HEAD {
        return fail(ApiError::new(
            405,
            "method_not_allowed",
            "method not allowed",
        ));
    }
    if st.cluster.is_clustered() && !is_forwarded(st, headers) {
        let replicas = db.replicas_of_shard(db.partition_shard(partition) as u32);
        if !replicas.contains(&st.cluster.me) {
            if let Some(node) = replicas.first() {
                let url = format!(
                    "{}/{name}/_partition/{}",
                    st.cluster.nodes[*node],
                    percent_encode(partition)
                );
                let request = st
                    .client
                    .get(url)
                    .header("x-douchdb-forwarded", "1")
                    .header("x-douchdb-secret", st.cluster.secret.as_str());
                let request = forward_client_auth(request, headers);
                return match request.send().await {
                    Ok(response) => {
                        let status = response.status();
                        let value = response.json::<Value>().await.unwrap_or_else(
                            |error| json!({"error": "bad_gateway", "reason": error.to_string()}),
                        );
                        json(status, value)
                    }
                    Err(error) => fail(ApiError::new(
                        502,
                        "bad_gateway",
                        format!("{}: {error}", st.cluster.nodes[*node]),
                    )),
                };
            }
        }
    }
    match db.partition_counts(partition).await {
        Ok((docs, deleted)) => json(
            StatusCode::OK,
            json!({
                "db_name": name,
                "doc_count": docs,
                "doc_del_count": deleted,
                "partition": partition,
                "sizes": { "active": 0, "external": 0 },
            }),
        ),
        Err(error) => fail(error),
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
        "_all_docs" => return all_docs(&st, &db, &name, &q, &method, &body, &headers, None).await,
        "_changes" => {
            return changes(st.clone(), db.clone(), &name, &q, &method, &body, &headers).await
        }
        "_revs_diff" => return revs_diff(&st, &db, &name, &body, &headers).await,
        "_purge" => {
            return match serde_json::from_slice::<Value>(&body) {
                Ok(v) => match db.purge(&v).await {
                    Ok(p) => {
                        // A fully purged document is removed from `_changes`
                        // entirely, so the view indexes cannot be updated
                        // incrementally; rebuild them on the next query.
                        if let Err(e) = crate::view::invalidate(&db).await {
                            return fail(e);
                        }
                        json(StatusCode::OK, p)
                    }
                    Err(e) => fail(e),
                },
                Err(_) => fail(ApiError::bad_request("invalid JSON")),
            };
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
        "_view_cleanup" => {
            return match crate::view::cleanup(&db).await {
                Ok(()) => json(StatusCode::ACCEPTED, json!({"ok": true})),
                Err(error) => fail(error),
            }
        }
        "_search_cleanup" => {
            return match crate::search::cleanup(&db) {
                Ok(()) => json(StatusCode::ACCEPTED, json!({"ok": true})),
                Err(error) => fail(error),
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

    if let Some((ddoc, index, info)) = search_path(&rest) {
        return search_request(
            &st, &db, &name, &ddoc, &index, &method, &q, &uri, &body, &headers, None, info,
        )
        .await;
    }

    if let Some(tail) = rest.strip_prefix("_partition/") {
        let (partition, suffix) = match tail.find("/_") {
            Some(index) => (&tail[..index], &tail[index + 1..]),
            None => (tail, ""),
        };
        if !db.partitioned {
            return fail(ApiError::bad_request("database is not partitioned"));
        }
        if let Err(error) = crate::db::validate_partition(partition) {
            return fail(error);
        }
        if let Some(requested) = q.get("partition") {
            if requested != partition {
                return fail(ApiError::bad_request(
                    "Conflicting value for `partition` in query string",
                ));
            }
        }
        return match suffix {
            "" => partition_info(&st, &db, &name, partition, &method, &headers).await,
            "_all_docs" => {
                all_docs(
                    &st,
                    &db,
                    &name,
                    &q,
                    &method,
                    &body,
                    &headers,
                    Some(partition.to_string()),
                )
                .await
            }
            _ => match search_path(suffix) {
                Some((ddoc, index, info)) => {
                    search_request(
                        &st,
                        &db,
                        &name,
                        &ddoc,
                        &index,
                        &method,
                        &q,
                        &uri,
                        &body,
                        &headers,
                        Some(partition.to_string()),
                        info,
                    )
                    .await
                }
                None => match view_path(suffix) {
                    Some((ddoc, view)) => {
                        view_request(
                            &st,
                            &db,
                            &name,
                            &ddoc,
                            &view,
                            &method,
                            &q,
                            &body,
                            &headers,
                            Some(partition.to_string()),
                        )
                        .await
                    }
                    None => fail(ApiError::not_found("missing partition endpoint")),
                },
            },
        };
    }

    if let Some((ddoc, view)) = view_path(&rest) {
        return view_request(
            &st, &db, &name, &ddoc, &view, &method, &q, &body, &headers, None,
        )
        .await;
    }

    doc_request(&st, &db, &name, &raw_rest, method, &q, &headers, body).await
}

/// `_design/{ddoc}/_view/{view}` (the rest has already been percent-decoded).
fn search_path(rest: &str) -> Option<(String, String, bool)> {
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() == 4
        && parts[0] == "_design"
        && (parts[2] == "_search" || parts[2] == "_search_info")
        && !parts[1].is_empty()
        && !parts[3].is_empty()
    {
        Some((
            format!("_design/{}", parts[1]),
            parts[3].to_string(),
            parts[2] == "_search_info",
        ))
    } else {
        None
    }
}

fn view_path(rest: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() == 4
        && parts[0] == "_design"
        && parts[2] == "_view"
        && !parts[1].is_empty()
        && !parts[3].is_empty()
    {
        Some((format!("_design/{}", parts[1]), parts[3].to_string()))
    } else {
        None
    }
}

fn view_json_param(q: &HashMap<String, String>, name: &str) -> DbResult<Option<Value>> {
    match q.get(name) {
        Some(raw) => match serde_json::from_str::<Value>(raw) {
            Ok(value) => Ok(Some(value)),
            // CouchDB wants JSON-encoded keys, but tolerating a bare string is
            // friendlier to hand-written URLs.
            Err(_) => Ok(Some(Value::String(raw.clone()))),
        },
        None => Ok(None),
    }
}

fn query_values(uri: &Uri, name: &str) -> Vec<String> {
    uri.query()
        .unwrap_or_default()
        .split('&')
        .filter_map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            if percent_decode(key) == name {
                Some(percent_decode(value))
            } else {
                None
            }
        })
        .collect()
}

fn drilldown_clause(value: &Value) -> Option<(String, Vec<Value>)> {
    let values = value.as_array()?;
    let field = values.first()?.as_str()?.to_string();
    Some((field, values.get(1..).unwrap_or_default().to_vec()))
}

fn parse_drilldown_value(value: &Value) -> Vec<Vec<(String, Vec<Value>)>> {
    let Some(values) = value.as_array() else {
        return Vec::new();
    };
    if values.first().map(Value::is_array).unwrap_or(false) {
        values
            .iter()
            .filter_map(|value| drilldown_clause(value).map(|clause| vec![clause]))
            .collect()
    } else {
        drilldown_clause(value)
            .map(|clause| vec![clause])
            .into_iter()
            .collect()
    }
}

async fn search_request(
    st: &AppState,
    db: &Db,
    dbname: &str,
    ddoc: &str,
    index_name: &str,
    method: &Method,
    q: &HashMap<String, String>,
    uri: &Uri,
    body: &[u8],
    headers: &HeaderMap,
    partition: Option<String>,
    info: bool,
) -> Response {
    let _ = (dbname, headers);
    if *method != Method::GET && *method != Method::POST && *method != Method::HEAD {
        return fail(ApiError::new(
            405,
            "method_not_allowed",
            "method not allowed",
        ));
    }
    let limits = crate::view::ViewLimits {
        timeout: std::time::Duration::from_millis(st.config.view_timeout_ms),
        memory: st.config.view_memory_mb.saturating_mul(1024 * 1024),
    };
    if info {
        return match crate::search::info(db, ddoc, index_name, limits).await {
            Ok(value) => json(StatusCode::OK, value),
            Err(error) => fail(error),
        };
    }

    let mut request = crate::search::SearchRequest {
        partition,
        ..Default::default()
    };
    let mut supplied = false;
    if *method == Method::POST {
        if let Ok(value) = serde_json::from_slice::<Value>(body) {
            let value = match value {
                Value::String(text) => serde_json::from_str::<Value>(&text).unwrap_or(Value::Null),
                other => other,
            };
            if let Some(object) = value.as_object() {
                supplied = true;
                if let Some(query) = object.get("q").or_else(|| object.get("query")) {
                    request.query = query.as_str().unwrap_or("*:*").to_string();
                }
                if let Some(limit) = object.get("limit").and_then(Value::as_u64) {
                    request.limit = limit as usize;
                }
                if let Some(bookmark) = object.get("bookmark").and_then(Value::as_str) {
                    request.bookmark = Some(bookmark.to_string());
                }
                if let Some(include_docs) = object.get("include_docs").and_then(Value::as_bool) {
                    request.include_docs = include_docs;
                }
                if let Some(counts) = object.get("counts").and_then(Value::as_array) {
                    request.counts = counts
                        .iter()
                        .filter_map(|field| field.as_str().map(str::to_string))
                        .collect();
                }
                if let Some(ranges) = object.get("ranges").and_then(Value::as_object) {
                    request.ranges = ranges.clone();
                }
                if let Some(field) = object.get("group_field").and_then(Value::as_str) {
                    request.group_field = Some(field.to_string());
                }
                if let Some(drilldown) = object.get("drilldown") {
                    request.drilldown = parse_drilldown_value(drilldown);
                }
            }
        }
    }
    if !supplied {
        request.query = q
            .get("q")
            .or_else(|| q.get("query"))
            .cloned()
            .unwrap_or_else(|| "*:*".to_string());
        if let Some(limit) = num(q, "limit") {
            request.limit = limit;
        }
        request.bookmark = q.get("bookmark").cloned();
        request.include_docs = flag(q, "include_docs");
        if let Some(raw) = q.get("counts") {
            if let Ok(Value::Array(counts)) = serde_json::from_str::<Value>(raw) {
                request.counts = counts
                    .into_iter()
                    .filter_map(|field| field.as_str().map(str::to_string))
                    .collect();
            }
        }
        if let Some(raw) = q.get("ranges") {
            if let Ok(Value::Object(ranges)) = serde_json::from_str::<Value>(raw) {
                request.ranges = ranges;
            }
        }
        request.group_field = q.get("group_field").cloned();
        for raw in query_values(uri, "drilldown") {
            if let Ok(value) = serde_json::from_str::<Value>(&raw) {
                request.drilldown.extend(parse_drilldown_value(&value));
            }
        }
    }

    match crate::search::query(db, ddoc, index_name, &request, limits).await {
        Ok(value) => json(StatusCode::OK, value),
        Err(error) => fail(error),
    }
}

async fn view_request(
    st: &AppState,
    db: &Db,
    dbname: &str,
    ddoc: &str,
    view_name: &str,
    method: &Method,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
    partition: Option<String>,
) -> Response {
    if *method != Method::GET && *method != Method::POST && *method != Method::HEAD {
        return fail(ApiError::new(
            405,
            "method_not_allowed",
            "method not allowed",
        ));
    }

    let build = || -> DbResult<crate::view::ViewQuery> {
        let mut keys = match view_json_param(q, "keys")? {
            Some(Value::Array(keys)) => Some(keys),
            Some(_) => return Err(ApiError::bad_request("`keys` must be a JSON array")),
            None => None,
        };
        if keys.is_none() && *method == Method::POST {
            if let Ok(value) = serde_json::from_slice::<Value>(body) {
                if let Some(array) = value.get("keys").and_then(Value::as_array) {
                    keys = Some(array.clone());
                }
            }
        }
        Ok(crate::view::ViewQuery {
            key: view_json_param(q, "key")?,
            keys,
            startkey: view_json_param(q, "startkey")?.or(view_json_param(q, "start_key")?),
            endkey: view_json_param(q, "endkey")?.or(view_json_param(q, "end_key")?),
            startkey_docid: q
                .get("startkey_docid")
                .or_else(|| q.get("start_key_docid"))
                .cloned(),
            endkey_docid: q
                .get("endkey_docid")
                .or_else(|| q.get("end_key_doc_id"))
                .cloned(),
            descending: flag(q, "descending"),
            inclusive_end: q
                .get("inclusive_end")
                .map(|value| value != "false")
                .unwrap_or(true),
            skip: num(q, "skip").unwrap_or(0),
            limit: num(q, "limit").unwrap_or(usize::MAX),
            reduce: q.get("reduce").map(|_| flag(q, "reduce")),
            group: flag(q, "group"),
            group_level: num(q, "group_level").unwrap_or(0),
            include_docs: flag(q, "include_docs"),
            attachments: flag(q, "attachments"),
            update_seq: flag(q, "update_seq"),
            limits: crate::view::ViewLimits {
                timeout: std::time::Duration::from_millis(st.config.view_timeout_ms),
                memory: st.config.view_memory_mb.saturating_mul(1024 * 1024),
            },
            owner_shards: None,
            shard_count: db.q,
            partition: partition.clone().or_else(|| q.get("partition").cloned()),
            partitioned: db.partitioned,
            total_rows_override: None,
        })
    };

    let mut query = match build() {
        Ok(query) => query,
        Err(error) => return fail(error),
    };
    if is_forwarded(st, headers) {
        if let Some(mask) = q.get("shards").and_then(|raw| parse_shard_mask(raw, db.q)) {
            query.owner_shards = Some(mask);
        } else if flag(q, "owner_only") {
            query.owner_shards = Some(
                (0..db.q)
                    .map(|shard| owner_of_shard(db, shard) == st.cluster.me)
                    .collect(),
            );
        }
    }

    // A global `_count`/`_sum`/`_stats` with no grouping can be reduced
    // independently on each owner and combined, instead of shipping every map
    // row to the coordinator.
    let combine_reduce = if st.cluster.is_clustered() && !is_forwarded(st, headers) {
        match crate::view::load_spec(db, ddoc, view_name).await {
            Ok(spec) => {
                let reduce = query.reduce.unwrap_or(spec.reduce.is_some());
                if reduce
                    && query.key.is_none()
                    && query.keys.is_none()
                    && query.startkey.is_none()
                    && query.endkey.is_none()
                    && !query.descending
                    && spec
                        .reduce
                        .as_deref()
                        .map(|reducer| reducer != "_approx_count_distinct")
                        .unwrap_or(false)
                {
                    spec.reduce
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    } else {
        None
    };

    let result = if let Some(reducer) = combine_reduce.as_deref() {
        cluster_reduce(st, db, dbname, ddoc, view_name, &query, reducer, headers).await
    } else if st.cluster.is_clustered() && !is_forwarded(st, headers) {
        match cluster_view_rows(st, db, dbname, ddoc, view_name, &query, headers).await {
            Ok((spec, rows, docs, total_rows)) => {
                if let Some(total_rows) = total_rows {
                    query.total_rows_override = Some(total_rows);
                }
                crate::view::finish(&spec, rows, docs, &query)
            }
            Err(error) => Err(error),
        }
    } else {
        crate::view::query(db, ddoc, view_name, &query).await
    };

    match result {
        Ok(mut value) => {
            if query.update_seq {
                if let Ok(info) = db.info().await {
                    value["update_seq"] = info["update_seq"].clone();
                }
            }
            json(StatusCode::OK, value)
        }
        Err(error) => fail(error),
    }
}

/// Views are sharded with the documents. Each node returns only the map rows
/// for shards it owns, so the coordinator unions complete rows without replica
/// duplicates. Fan-out is concurrent, and map-only queries push `skip + limit`
/// down to every node while summing their full owned-row counts.

/// Degraded-mode view read: an owner-only fetch failed, so query every
/// reachable node for all local replicas and merge duplicates by row identity.

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
    let new_edits = match value.get("new_edits") {
        Some(Value::Bool(new_edits)) => *new_edits,
        Some(_) => {
            return fail(ApiError::bad_request(
                "`new_edits` parameter must be a boolean.",
            ))
        }
        None => true,
    };
    let Some(docs) = value.get("docs").and_then(Value::as_array).cloned() else {
        return fail(ApiError::bad_request("Missing JSON list of 'docs'"));
    };
    for doc in &docs {
        if let Err(e) = validate_doc(doc) {
            return fail(e);
        }
        if !db.partitioned {
            match doc.get("_id") {
                Some(Value::String(id))
                    if id.starts_with('_')
                        && !id.starts_with("_design/")
                        && !id.starts_with("_local/") =>
                {
                    return fail(ApiError::new(
                        400,
                        "illegal_docid",
                        "Only reserved document ids may start with underscore.",
                    ))
                }
                Some(Value::String(_)) | None => {}
                Some(_) => {
                    return fail(ApiError::new(
                        400,
                        "illegal_docid",
                        "Document id must be a string",
                    ))
                }
            }
        }
    }
    if value
        .get("all_or_nothing")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        let results: Vec<Value> = docs
            .iter()
            .map(|_| {
                json!({
                    "error": "not_implemented",
                    "reason": "all_or_nothing is not supported"
                })
            })
            .collect();
        return json(StatusCode::EXPECTATION_FAILED, Value::Array(results));
    }

    if !st.cluster.is_clustered() {
        return match db.put_docs(docs, new_edits).await {
            Ok(results) => bulk_response(new_edits, results),
            Err(e) => fail(e),
        };
    }
    // A forwarded batch has already been placed by its coordinator; write it
    // to this node's shards and let the coordinator count the replica ack.
    if is_forwarded(st, headers) {
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

fn bulk_response(new_edits: bool, results: Vec<Value>) -> Response {
    if new_edits {
        json(StatusCode::CREATED, Value::Array(results))
    } else {
        let errors: Vec<Value> = results
            .into_iter()
            .filter(|result| result.get("error").is_some())
            .collect();
        json(StatusCode::CREATED, Value::Array(errors))
    }
}

/// Place documents on their replica sets.
///
/// `new_edits=true` writes to each document's primary (the coordinating
/// replica), then copies the resulting full revision to the other replicas with
/// `new_edits=false`, so every replica ends with byte-identical history.
/// `new_edits=false` (the replication path) sends the given revision to every
/// replica directly. The bool is whether the default write quorum was met.

/// Replica candidates for a bulk read, with the coordinator's local replica
/// first when it holds one. The remaining replicas are ordered by the shard
/// map so a failed request can retry elsewhere.
fn bulk_read_candidates(db: &Db, id: &str, me: usize) -> Vec<usize> {
    let (owner, mut replicas) = db.placement_of_doc(id);
    if replicas.is_empty() {
        return vec![owner];
    }
    if replicas.contains(&me) {
        replicas.retain(|node| *node != me);
        replicas.insert(0, me);
    }
    replicas
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
    if !st.cluster.is_clustered() || is_forwarded(st, headers) {
        return match db.bulk_get(&value, attachments, revs).await {
            Ok(v) => json(StatusCode::OK, v),
            Err(e) => fail(e),
        };
    }
    let Some(reqs) = value.get("docs").and_then(Value::as_array).cloned() else {
        return fail(ApiError::bad_request("Missing JSON list of 'docs'"));
    };
    let mut candidates: Vec<Vec<usize>> = reqs
        .iter()
        .map(|req| {
            let id = req.get("id").and_then(Value::as_str).unwrap_or_default();
            bulk_read_candidates(db, id, st.cluster.me)
        })
        .collect();
    let mut out = vec![Value::Null; reqs.len()];
    let mut pending: Vec<usize> = (0..reqs.len()).collect();
    while !pending.is_empty() {
        let mut groups: HashMap<usize, Vec<usize>> = HashMap::new();
        for slot in pending.drain(..) {
            groups.entry(candidates[slot][0]).or_default().push(slot);
        }
        let requests = groups.into_iter().map(|(node, slots)| {
            let subdocs: Vec<Value> = slots.iter().map(|slot| reqs[*slot].clone()).collect();
            async move {
                let sub = json!({"docs": subdocs});
                let result = if node == st.cluster.me {
                    db.bulk_get(&sub, attachments, revs).await
                } else {
                    let peer = st.cluster.nodes[node].clone();
                    let mut query = format!("revs={revs}");
                    if attachments {
                        query.push_str("&attachments=true");
                    }
                    forward_json(st, &peer, dbname, "_bulk_get", &query, &sub, headers).await
                };
                (slots, result)
            }
        });
        for (slots, result) in futures::future::join_all(requests).await {
            match result {
                Ok(value) => {
                    let rows = value["results"].as_array().cloned().unwrap_or_default();
                    for (slot, row) in slots.into_iter().zip(rows) {
                        out[slot] = row;
                    }
                }
                Err(error) => {
                    for slot in slots {
                        if candidates[slot].len() > 1 {
                            candidates[slot].remove(0);
                            pending.push(slot);
                        } else {
                            return fail(error);
                        }
                    }
                }
            }
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
    if !st.cluster.is_clustered() || is_forwarded(st, headers) {
        return match db.revs_diff(&value).await {
            Ok(v) => json(StatusCode::OK, v),
            Err(e) => fail(e),
        };
    }
    let Some(map) = value.as_object() else {
        return fail(ApiError::bad_request("Request body must be a JSON object"));
    };
    let entries: Vec<(String, Value)> = map
        .iter()
        .map(|(id, revs)| (id.clone(), revs.clone()))
        .collect();
    let mut candidates: Vec<Vec<usize>> = entries
        .iter()
        .map(|(id, _)| bulk_read_candidates(db, id, st.cluster.me))
        .collect();
    let mut out = Map::new();
    let mut pending: Vec<usize> = (0..entries.len()).collect();
    while !pending.is_empty() {
        let mut groups: HashMap<usize, Vec<usize>> = HashMap::new();
        for slot in pending.drain(..) {
            groups.entry(candidates[slot][0]).or_default().push(slot);
        }
        let requests = groups.into_iter().map(|(node, slots)| {
            let sub = Value::Object(
                slots
                    .iter()
                    .map(|slot| (entries[*slot].0.clone(), entries[*slot].1.clone()))
                    .collect(),
            );
            async move {
                let result = if node == st.cluster.me {
                    db.revs_diff(&sub).await
                } else {
                    let peer = st.cluster.nodes[node].clone();
                    forward_json(st, &peer, dbname, "_revs_diff", "", &sub, headers).await
                };
                (slots, result)
            }
        });
        for (slots, result) in futures::future::join_all(requests).await {
            match result {
                Ok(value) => {
                    if let Some(obj) = value.as_object() {
                        for (id, entry) in obj {
                            out.insert(id.clone(), entry.clone());
                        }
                    }
                }
                Err(error) => {
                    for slot in slots {
                        if candidates[slot].len() > 1 {
                            candidates[slot].remove(0);
                            pending.push(slot);
                        } else {
                            return fail(error);
                        }
                    }
                }
            }
        }
    }
    json(StatusCode::OK, Value::Object(out))
}

async fn all_docs(
    st: &AppState,
    db: &Db,
    dbname: &str,
    q: &HashMap<String, String>,
    method: &Method,
    body: &[u8],
    headers: &HeaderMap,
    partition: Option<String>,
) -> Response {
    let partition = partition.or_else(|| q.get("partition").cloned());
    if let Some(partition) = &partition {
        if !db.partitioned {
            return fail(ApiError::bad_request(
                "`partition` parameter is not supported on this db",
            ));
        }
        if let Err(error) = crate::db::validate_partition(partition) {
            return fail(error);
        }
    }
    let query_limit = q.contains_key("limit");
    let query_skip = q.contains_key("skip");
    let mut opts = AllDocsOpts {
        partition: partition.clone(),
        include_docs: flag(q, "include_docs"),
        conflicts: flag(q, "conflicts"),
        attachments: flag(q, "attachments"),
        startkey: key_param(q, "startkey").or_else(|| key_param(q, "start_key")),
        endkey: key_param(q, "endkey").or_else(|| key_param(q, "end_key")),
        descending: flag(q, "descending"),
        limit: num(q, "limit").unwrap_or(usize::MAX),
        skip: num(q, "skip").unwrap_or(0),
        ..Default::default()
    };
    if let Some(key) = key_param(q, "key") {
        opts.keys = Some(vec![key]);
    } else if let Some(raw) = q.get("keys") {
        if let Ok(Value::Array(keys)) = serde_json::from_str::<Value>(raw) {
            opts.keys = Some(
                keys.into_iter()
                    .filter_map(|key| key.as_str().map(str::to_string))
                    .collect(),
            );
        }
    }
    if *method == Method::POST {
        if let Ok(value) = serde_json::from_slice::<Value>(body) {
            if opts.keys.is_none() {
                if let Some(keys_value) = value.get("keys") {
                    let Some(keys) = keys_value.as_array() else {
                        return fail(ApiError::bad_request("`keys` must be an array."));
                    };
                    opts.keys = Some(
                        keys.iter()
                            .filter_map(|k| k.as_str().map(str::to_string))
                            .collect(),
                    );
                }
            }
            if value
                .get("include_docs")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                opts.include_docs = true;
            }
            if value
                .get("descending")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                opts.descending = true;
            }
            if !query_limit {
                if let Some(limit) = value.get("limit").and_then(Value::as_u64) {
                    opts.limit = limit as usize;
                }
            }
            if !query_skip {
                if let Some(skip) = value.get("skip").and_then(Value::as_u64) {
                    opts.skip = skip as usize;
                }
            }
        }
    }
    if opts.keys.is_some() && (opts.startkey.is_some() || opts.endkey.is_some()) {
        return fail(ApiError::bad_request(
            "`keys` is incompatible with `startkey` and `endkey`",
        ));
    }
    let shard_mask = if is_forwarded(st, headers) {
        q.get("shards").and_then(|raw| parse_shard_mask(raw, db.q))
    } else {
        None
    };
    let result = if st.cluster.is_clustered() && !is_forwarded(st, headers) {
        cluster_all_docs(st, db, dbname, &opts, headers).await
    } else if let Some(mask) = shard_mask {
        db.all_docs_owned(&opts, &mask).await
    } else if flag(q, "owner_only") && is_forwarded(st, headers) {
        let owned: Vec<bool> = (0..db.q)
            .map(|shard| owner_of_shard(db, shard) == st.cluster.me)
            .collect();
        db.all_docs_owned(&opts, &owned).await
    } else {
        db.all_docs(&opts).await
    };
    match result {
        Ok(v) => json(StatusCode::OK, v),
        Err(e) => fail(e),
    }
}

/// A primary/owner was unavailable. Read every live replica from every
/// reachable node, deduplicate by id, and answer the original query from the
/// union. This is the degraded path; the owner-only path stays the fast path.

async fn changes(
    st: Arc<AppState>,
    db: Arc<Db>,
    dbname: &str,
    q: &HashMap<String, String>,
    method: &Method,
    body: &[u8],
    headers: &HeaderMap,
) -> Response {
    let _ = method;
    let local_only = is_forwarded(&st, headers);
    let feed = q.get("feed").map(String::as_str).unwrap_or("normal");
    // Cap longpoll/continuous connections so a client cannot pin a worker for
    // an arbitrary amount of time.
    let deadline = num(q, "timeout").unwrap_or(60_000).min(300_000) as u64;
    let heartbeat = num(q, "heartbeat")
        .filter(|value| *value > 0)
        .map(|value| std::time::Duration::from_millis(value as u64));

    let eventsource = feed == "eventsource";
    if feed == "continuous" || eventsource {
        let dbname = dbname.to_string();
        let mut query = q.clone();
        let body = Bytes::copy_from_slice(body);
        let headers = headers.clone();
        let limit = num(q, "limit");
        let stream = async_stream::stream! {
            let started = std::time::Instant::now();
            let mut sent = 0usize;
            let mut last_heartbeat = std::time::Instant::now();
            loop {
                if limit.map(|limit| sent >= limit).unwrap_or(false) {
                    break;
                }
                match changes_once(&st, &db, &dbname, &query, &body, &headers, local_only).await {
                    Ok(value) => {
                        let rows = value["results"].as_array().cloned().unwrap_or_default();
                        for row in rows {
                            if limit.map(|limit| sent >= limit).unwrap_or(false) {
                                break;
                            }
                            let json = serde_json::to_vec(&row).unwrap_or_default();
                            let line = if eventsource {
                                [b"data: ".as_slice(), json.as_slice(), b"\n\n"].concat()
                            } else {
                                [json.as_slice(), b"\n"].concat()
                            };
                            yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                            sent += 1;
                        }
                        if let Some(seq) = value.get("last_seq") {
                            let since = seq
                                .as_str()
                                .map(str::to_string)
                                .unwrap_or_else(|| seq.to_string());
                            query.insert("since".to_string(), since);
                        }
                    }
                    Err(error) => {
                        let json = serde_json::to_vec(&json!({
                            "error": error.error,
                            "reason": error.reason,
                        }))
                        .unwrap_or_default();
                        let line = if eventsource {
                            [b"data: ".as_slice(), json.as_slice(), b"\n\n"].concat()
                        } else {
                            [json.as_slice(), b"\n"].concat()
                        };
                        yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                        break;
                    }
                }
                if started.elapsed().as_millis() as u64 >= deadline {
                    break;
                }
                if let Some(heartbeat) = heartbeat {
                    if last_heartbeat.elapsed() >= heartbeat {
                        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(if eventsource {
                            b": heartbeat\n\n"
                        } else {
                            b"\n"
                        }));
                        last_heartbeat = std::time::Instant::now();
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        };
        return Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                if eventsource {
                    "text/event-stream"
                } else {
                    "application/json"
                },
            )
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from_stream(stream))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    if feed == "longpoll" {
        if let Some(heartbeat) = heartbeat {
            let dbname = dbname.to_string();
            let query = q.clone();
            let body = Bytes::copy_from_slice(body);
            let headers = headers.clone();
            let stream = async_stream::stream! {
                let started = std::time::Instant::now();
                let mut last_heartbeat = std::time::Instant::now();
                loop {
                    match changes_once(&st, &db, &dbname, &query, &body, &headers, local_only).await {
                        Ok(value)
                            if value["results"]
                                .as_array()
                                .map(|rows| !rows.is_empty())
                                .unwrap_or(false)
                                || started.elapsed().as_millis() as u64 >= deadline =>
                        {
                            let mut line = serde_json::to_vec(&value).unwrap_or_default();
                            line.push(b'\n');
                            yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                            break;
                        }
                        Ok(_) => {}
                        Err(error) => {
                            let mut line = serde_json::to_vec(&json!({
                                "error": error.error,
                                "reason": error.reason,
                            }))
                            .unwrap_or_default();
                            line.push(b'\n');
                            yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                            break;
                        }
                    }
                    if last_heartbeat.elapsed() >= heartbeat {
                        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"\n"));
                        last_heartbeat = std::time::Instant::now();
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            };
            return Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::CACHE_CONTROL, "no-cache")
                .body(Body::from_stream(stream))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
        }
        let started = std::time::Instant::now();
        loop {
            match changes_once(&st, &db, dbname, q, body, headers, local_only).await {
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

    if feed == "normal" {
        if let Some(response) = changes_normal_raw(&st, &db, q, body, headers, local_only).await {
            return response;
        }
    }

    match changes_once(&st, &db, dbname, q, body, headers, local_only).await {
        Ok(v) => json(StatusCode::OK, v),
        Err(e) => fail(e),
    }
}

async fn changes_normal_raw(
    st: &AppState,
    db: &Arc<Db>,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
    local_only: bool,
) -> Option<Response> {
    if flag(q, "descending") {
        return None;
    }
    let mut local_mask = None;
    if st.cluster.is_clustered() && !local_only {
        let masks = crate::fabric::preferred_shard_masks(st, db);
        let active: Vec<usize> = masks
            .iter()
            .enumerate()
            .filter(|(_, mask)| mask.iter().any(|owned| *owned))
            .map(|(node, _)| node)
            .collect();
        if active.len() != 1 || active[0] != st.cluster.me {
            return None;
        }
        local_mask = Some(masks[st.cluster.me].clone());
    }
    let (mut opts, mut progress) = changes_options(st, db, q, body, headers, local_only)
        .await
        .ok()?;
    let rewrite = local_mask.is_some();
    if let Some(mask) = local_mask {
        opts.owned = Some(mask);
    }
    let me = st.cluster.me;
    let initial_seq = progress.get(me).copied().unwrap_or(0);
    let mut rows = Box::pin(Db::changes_stream_arc(db.clone(), opts));
    let stream = async_stream::stream! {
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"{\"results\":["));
        let mut buffer = String::with_capacity(64 * 1024);
        let mut first = true;
        let mut last_seq = initial_seq;
        while let Some(item) = rows.next().await {
            let row = match item {
                Ok(row) => row,
                Err(error) => {
                    yield Err(std::io::Error::other(format!("{}: {}", error.error, error.reason)));
                    return;
                }
            };
            last_seq = row.seq;
            if rewrite {
                progress[me] = row.seq;
            }
            if !first {
                buffer.push(',');
            }
            first = false;
            buffer.push_str(&Db::change_row_json(&row));
            if buffer.len() >= 64 * 1024 {
                yield Ok(Bytes::from(std::mem::take(&mut buffer)));
            }
        }
        let last_seq_json = if rewrite {
            crate::fabric::encode_since(&progress)
        } else {
            last_seq.to_string()
        };
        buffer.push_str("],\"last_seq\":");
        if rewrite {
            buffer.push('"');
            buffer.push_str(&last_seq_json);
            buffer.push('"');
        } else {
            buffer.push_str(&last_seq_json);
        }
        buffer.push_str(",\"pending\":0}");
        if !buffer.is_empty() {
            yield Ok(Bytes::from(buffer));
        }
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from_stream(stream))
        .ok()
}

async fn changes_options(
    st: &AppState,
    db: &Db,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
    local_only: bool,
) -> DbResult<(ChangesOpts, Vec<u64>)> {
    let n = st.cluster.nodes.len().max(1);
    let raw_since = q.get("since").map(String::as_str);
    let mut progress = if raw_since == Some("now") {
        if st.cluster.is_clustered() && !local_only {
            cluster_update_vector(st, db, &db.name, headers).await?
        } else {
            let mut vector = vec![0; n];
            vector[st.cluster.me] = db.update_seq();
            vector
        }
    } else if let Some(raw) = raw_since {
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
    progress.resize(n, 0);
    let since = progress.get(st.cluster.me).copied().unwrap_or(0);
    let owned = if is_forwarded(st, headers) {
        q.get("shards").and_then(|raw| parse_shard_mask(raw, db.q))
    } else {
        None
    };
    let opts = ChangesOpts {
        since,
        limit: num(q, "limit"),
        include_docs: flag(q, "include_docs"),
        style_all_docs: q.get("style").map(String::as_str) == Some("all_docs"),
        descending: flag(q, "descending"),
        doc_ids: doc_ids_from(q, body),
        owned,
    };
    Ok((opts, progress))
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

    let (opts, _) = changes_options(st, db, q, body, headers, local_only).await?;
    db.changes(&opts).await
}

pub(crate) fn doc_ids_from(q: &HashMap<String, String>, body: &[u8]) -> Option<Vec<String>> {
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

/// Current per-node update_seq vector. Used for `since=now` and for the
/// `update_seq` reported in `GET /db`.

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

    let default_read_quorum = if st.cluster.is_clustered() {
        st.cluster.replication_factor() / 2 + 1
    } else {
        1
    };
    let read_quorum = num(q, "r").unwrap_or(default_read_quorum);
    if attachment.is_none()
        && (method == Method::GET || method == Method::HEAD)
        && st.cluster.is_clustered()
        && read_quorum > 1
        && !is_forwarded(st, headers)
    {
        return quorum_read(st, db, dbname, &docid, q, headers, read_quorum).await;
    }

    // Serve locally if this node holds a replica; otherwise try every replica
    // in preference order so a dead primary does not break reads or writes.
    let replicas = replicas_of_doc(db, &docid);
    if st.cluster.is_clustered() && !replicas.contains(&st.cluster.me) {
        let mut forward_q = q.clone();
        // The peer is a replica; answer locally instead of starting another
        // quorum read and bouncing the request back.
        forward_q.insert("r".to_string(), "1".to_string());
        let mut last = None;
        for node in replicas {
            let peer = st.cluster.nodes[node].clone();
            let response = proxy(
                st,
                &method,
                dbname,
                rest,
                &forward_q,
                headers,
                body.clone(),
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

    if attachment.is_some() {
        return fail(ApiError::new(
            400,
            "attachments_not_supported",
            "douchdb does not support attachments",
        ));
    }

    match method {
        Method::GET | Method::HEAD => get_doc(db, &docid, q, headers).await,
        Method::PUT => put_doc(st, db, dbname, &docid, body, q, headers).await,
        Method::DELETE => delete_doc(st, db, dbname, &docid, q, headers).await,
        _ if method.as_str() == "COPY" => copy_doc(st, db, dbname, &docid, q, headers).await,
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
        let attachment = segments
            .get(2..)
            .map(|parts| parts.join("/"))
            .filter(|name| !name.is_empty());
        return (format!("_design/{}", segments[1]), attachment);
    }
    let attachment = segments
        .get(1..)
        .map(|parts| parts.join("/"))
        .filter(|name| !name.is_empty());
    (segments[0].clone(), attachment)
}

pub(crate) fn doc_opts(q: &HashMap<String, String>) -> DocOpts {
    DocOpts {
        rev: q.get("rev").cloned(),
        revs: flag(q, "revs"),
        revs_info: flag(q, "revs_info"),
        local_seq: flag(q, "local_seq"),
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

pub(crate) fn doc_response(doc: Value) -> Response {
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

async fn get_doc(
    db: &Db,
    docid: &str,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Response {
    match db.get_doc(docid, &doc_opts(q)).await {
        Ok(doc) => {
            let etag = doc
                .get("_rev")
                .and_then(Value::as_str)
                .map(|rev| format!("\"{rev}\""))
                .unwrap_or_default();
            if !etag.is_empty() && etag_matches(headers, header::IF_NONE_MATCH, &etag) {
                let mut response = StatusCode::NOT_MODIFIED.into_response();
                if let Ok(value) = etag.parse() {
                    response.headers_mut().insert(header::ETAG, value);
                }
                return response;
            }
            doc_response(doc)
        }
        Err(e) => fail(e),
    }
}

fn etag_matches(headers: &HeaderMap, name: header::HeaderName, etag: &str) -> bool {
    headers.get_all(name).iter().any(|value| {
        value
            .to_str()
            .ok()
            .unwrap_or_default()
            .split(',')
            .any(|candidate| {
                let candidate = candidate.trim();
                candidate == "*" || candidate == etag || candidate.strip_prefix("W/") == Some(etag)
            })
    })
}

async fn check_if_match(db: &Db, docid: &str, headers: &HeaderMap) -> Option<Response> {
    if headers.get(header::IF_MATCH).is_none() {
        return None;
    }
    let current = db
        .get_doc(docid, &DocOpts::default())
        .await
        .ok()
        .and_then(|doc| {
            doc.get("_rev")
                .and_then(Value::as_str)
                .map(|rev| format!("\"{rev}\""))
        });
    match current {
        Some(etag) if etag_matches(headers, header::IF_MATCH, &etag) => None,
        _ => Some(fail(ApiError::new(
            412,
            "precondition_failed",
            "If-Match does not match the current revision",
        ))),
    }
}

/// Compare two document versions with CouchDB's winner rule: live beats
/// deleted, then higher position, then greater revision id.

/// `?r=N` reads from several replicas and returns the winning revision.

async fn copy_doc(
    st: &AppState,
    db: &Db,
    dbname: &str,
    docid: &str,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Response {
    let Some(raw) = headers
        .get("destination")
        .and_then(|value| value.to_str().ok())
    else {
        return fail(ApiError::bad_request("Destination header is required"));
    };
    let (raw, destination_rev) = match raw.split_once('?') {
        Some((destination, query)) => {
            let rev = query
                .split('&')
                .find_map(|pair| pair.strip_prefix("rev=").map(percent_decode));
            (destination, rev)
        }
        None => (raw, None),
    };
    let destination = percent_decode(raw);
    if destination.is_empty() {
        return fail(ApiError::bad_request("Destination must not be empty"));
    }

    let source = match db
        .get_doc(
            docid,
            &DocOpts {
                rev: q.get("rev").cloned(),
                ..Default::default()
            },
        )
        .await
    {
        Ok(doc) => doc,
        Err(error) => return fail(error),
    };
    if source.get("_attachments").is_some() {
        return fail(ApiError::bad_request(
            "Attachments are not supported by douchdb",
        ));
    }
    let mut copy = source;
    if let Some(object) = copy.as_object_mut() {
        object.remove("_id");
        object.remove("_rev");
        object.remove("_revisions");
        object.remove("_conflicts");
        object.remove("_deleted_conflicts");
        object.remove("_deleted");
    }
    copy["_id"] = json!(destination);
    if let Some(rev) = destination_rev {
        copy["_rev"] = json!(rev);
    }
    match commit_docs(
        st,
        db,
        dbname,
        vec![copy],
        true,
        num(q, "w").unwrap_or(0),
        headers,
    )
    .await
    {
        Ok((mut results, _)) => {
            let result = results.pop().unwrap_or(Value::Null);
            if result.get("error").is_some() {
                json(StatusCode::CONFLICT, result)
            } else {
                json(StatusCode::CREATED, result)
            }
        }
        Err(error) => fail(error),
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
    if let Err(error) = db.validate_doc_id(docid) {
        return fail(error);
    }
    let body_rev = doc.get("_rev").and_then(Value::as_str).map(str::to_string);
    if let Some(query_rev) = q.get("rev") {
        match &body_rev {
            Some(body_rev) if body_rev != query_rev => {
                return fail(ApiError::bad_request(
                    "Document rev from request body and query string have different values",
                ))
            }
            None => doc["_rev"] = json!(query_rev),
            _ => {}
        }
    }
    if headers.get(header::IF_MATCH).is_some() {
        if let Some(body_rev) = &body_rev {
            let etag = format!("\"{body_rev}\"");
            if !etag_matches(headers, header::IF_MATCH, &etag) {
                return fail(ApiError::bad_request(
                    "Document rev and etag have different values",
                ));
            }
        }
    }
    let new_edits = !q.contains_key("new_edits") || flag(q, "new_edits");
    if !new_edits && doc.get("_rev").is_none() {
        return fail(ApiError::bad_request("Document is missing _rev"));
    }
    if let Some(response) = check_if_match(db, docid, headers).await {
        return response;
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
            let etag = result
                .get("rev")
                .or_else(|| result.get("_rev"))
                .and_then(Value::as_str)
                .map(|rev| format!("\"{rev}\""));
            let mut response = json(status, result);
            if let Some(etag) = etag {
                if let Ok(value) = etag.parse() {
                    response.headers_mut().insert(header::ETAG, value);
                }
            }
            response
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
    if let Err(error) = db.validate_doc_id(docid) {
        return fail(error);
    }
    // A missing document is 404 even without a `rev`; only a live document
    // without a rev is a conflict. A tombstone reports reason "deleted".
    if let Err(e) = db.get_doc(docid, &DocOpts::default()).await {
        return fail(e);
    }
    if let Some(response) = check_if_match(db, docid, headers).await {
        return response;
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

pub(crate) fn percent_encode(s: &str) -> String {
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

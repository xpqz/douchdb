//! HTTP surface. The shape of every response here is dictated by CouchDB's
//! wire protocol, not by taste.

use crate::auth::{Auth, Principal};
use crate::cluster::{Cluster, ShardMap};
use crate::db::{
    validate_doc, AllDocsOpts, ApiError, ChangesOpts, Db, DbConfig, DbResult, DocOpts,
};
use crate::fabric::*;
pub use crate::observability::{health_public, health_status, metrics_status};
use crate::store::ShardState;
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
    pub cluster_state: Arc<crate::cluster_state::ClusterState>,
    pub admission: Arc<crate::admission::Admission>,
    /// Bound concurrent `_compact` requests across databases on this node.
    pub compaction: Arc<tokio::sync::Semaphore>,
}

#[derive(Clone)]
pub struct Config {
    #[allow(dead_code)]
    pub q: u32,
    #[allow(dead_code)]
    pub n: usize,
    pub revs_limit: u64,
    #[allow(dead_code)]
    pub nodes: Vec<String>,
    #[allow(dead_code)]
    pub me: usize,
    pub sync_secs: u64,
    /// Additional peers probed for membership discovery.
    pub seeds: Vec<String>,
    /// The URL this node advertises in cluster management views.
    pub advertise: String,
    /// Empty disables CORS. `*` allows any origin (echoed back so credentials
    /// work); entries may be exact origins or `https://*.example.com`.
    pub cors_origins: Vec<String>,
    pub view_timeout_ms: u64,
    pub view_memory_mb: usize,
    /// When true, a database with an empty `_security` object is admin-only.
    /// This is the default whenever authentication is configured.
    pub default_admin_only: bool,
}

// ---------------------------------------------------------------------------
// small helpers

pub(crate) fn json(status: StatusCode, value: Value) -> Response {
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

pub(crate) fn query(uri: &Uri) -> HashMap<String, String> {
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
    crate::json::from_slice(body).map_err(|e| ApiError::bad_request(format!("invalid JSON: {e}")))
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
        if !valid_db_name(name) {
            return Err(ApiError::bad_request("invalid database name"));
        }
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
        if !valid_db_name(name) {
            return Err(ApiError::bad_request("invalid database name"));
        }
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
        if !valid_db_name(name) {
            return Err(ApiError::bad_request("invalid database name"));
        }
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

    /// The caller's principal. With no auth configured every request is a
    /// service admin, preserving the single-user default. Internal cluster
    /// requests without user credentials are service admins; peers are trusted
    /// by the shared cluster secret.
    pub fn principal(&self, headers: &HeaderMap) -> Principal {
        let Some(auth) = &self.auth else {
            return Principal::service_admin();
        };
        let principal = auth.principal(headers);
        if principal.is_authenticated() {
            return principal;
        }
        if self.cluster.is_internal(headers) || self.cluster.is_internal_db(headers) {
            return Principal::service_admin();
        }
        principal
    }

    fn authorized(&self, headers: &HeaderMap) -> bool {
        self.principal(headers).is_authenticated()
    }
}

fn forbidden() -> Response {
    json(
        StatusCode::FORBIDDEN,
        json!({"error": "forbidden", "reason": "You are not authorized to access this db."}),
    )
}

pub(crate) fn access_denied(principal: &Principal) -> Response {
    if principal.is_authenticated() {
        forbidden()
    } else {
        unauthorized()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DbAccess {
    Member,
    Admin,
}

fn security_matches(principal: &Principal, section: Option<&Value>) -> bool {
    let Some(section) = section.and_then(Value::as_object) else {
        return false;
    };
    if let (Some(name), Some(names)) = (
        principal.name.as_deref(),
        section.get("names").and_then(Value::as_array),
    ) {
        if names
            .iter()
            .any(|candidate| candidate.as_str() == Some(name))
        {
            return true;
        }
    }
    if let Some(roles) = section.get("roles").and_then(Value::as_array) {
        if roles.iter().any(|role| {
            role.as_str()
                .map(|role| principal.has_role(role))
                .unwrap_or(false)
        }) {
            return true;
        }
    }
    false
}

fn security_list_empty(security: &Value, key: &str) -> bool {
    let Some(section) = security.get(key).and_then(Value::as_object) else {
        return true;
    };
    ["names", "roles"].iter().all(|field| {
        section
            .get(*field)
            .and_then(Value::as_array)
            .map(Vec::is_empty)
            .unwrap_or(true)
    })
}

fn db_access(st: &AppState, principal: &Principal, db: &Db) -> Option<DbAccess> {
    if principal.admin {
        return Some(DbAccess::Admin);
    }
    let security = db.security();
    if security_matches(principal, security.get("admins")) {
        return Some(DbAccess::Admin);
    }
    if security_matches(principal, security.get("members")) {
        return Some(DbAccess::Member);
    }
    // A database is public only when the whole security object is empty and
    // the operator chose `open`. This is deliberately stricter than CouchDB's
    // "empty members means public" rule: setting admins without members must
    // not accidentally expose the database to every authenticated user.
    if security_list_empty(&security, "members") {
        if security_list_empty(&security, "admins") && !st.config.default_admin_only {
            Some(DbAccess::Member)
        } else {
            None
        }
    } else {
        None
    }
}

fn require_db_access(
    st: &AppState,
    principal: &Principal,
    db: &Db,
    need_admin: bool,
) -> Option<Response> {
    match db_access(st, principal, db) {
        Some(DbAccess::Admin) => None,
        Some(DbAccess::Member) if !need_admin => None,
        Some(DbAccess::Member) => Some(forbidden()),
        None => Some(access_denied(principal)),
    }
}

fn still_allowed(st: &AppState, db: &Db, headers: &HeaderMap) -> bool {
    let principal = st.principal(headers);
    db_access(st, &principal, db).is_some()
}

fn requires_db_admin(rest: &str, method: &Method) -> bool {
    if rest == "_security" {
        return *method == Method::PUT || *method == Method::POST;
    }
    if rest == "_compact"
        || rest == "_view_cleanup"
        || rest == "_search_cleanup"
        || rest == "_purge"
        || rest == "_sync_shards"
    {
        return true;
    }
    if rest == "_revs_limit" {
        return *method == Method::PUT || *method == Method::POST;
    }
    // A design document write is `_design/{ddoc}`; queries under `_view/` and
    // `_search/` remain member operations.
    if rest.starts_with("_design/")
        && !rest.contains("/_view/")
        && !rest.contains("/_search/")
        && !rest.contains("/_show/")
        && !rest.contains("/_list/")
        && !rest.contains("/_update/")
    {
        return method.as_str() == "COPY"
            || matches!(
                *method,
                Method::PUT | Method::POST | Method::DELETE | Method::PATCH
            );
    }
    false
}

fn is_write_request(method: &Method, rest: &str) -> bool {
    if method.as_str() == "COPY" || matches!(*method, Method::PUT | Method::DELETE) {
        return true;
    }
    if *method != Method::POST {
        return false;
    }
    // POST is also used for read-only query shapes.
    if rest == "_all_docs" || rest == "_bulk_get" || rest == "_revs_diff" {
        return false;
    }
    if rest.starts_with("_design/")
        && (rest.contains("/_view/")
            || rest.contains("/_search/")
            || rest.contains("/_search_info/"))
    {
        return false;
    }
    !matches!(rest, "_search_info" | "_index_info")
}

fn reject_during_rebalance(
    st: &AppState,
    db: &Db,
    method: &Method,
    rest: &str,
    headers: &HeaderMap,
) -> Option<Response> {
    if db.rebalance_in_progress() && !is_forwarded(st, headers) && is_write_request(method, rest) {
        Some(fail(ApiError::new(
            503,
            "rebalance_in_progress",
            "writes are paused while this database changes shard placement",
        )))
    } else {
        None
    }
}

fn validate_security(value: &Value) -> DbResult<()> {
    let object = value
        .as_object()
        .ok_or_else(|| ApiError::bad_request("security object must be an object"))?;
    for key in ["admins", "members"] {
        let Some(section) = object.get(key) else {
            continue;
        };
        let section = section
            .as_object()
            .ok_or_else(|| ApiError::bad_request(format!("`{key}` must be an object")))?;
        for field in ["names", "roles"] {
            let Some(list) = section.get(field) else {
                continue;
            };
            let list = list.as_array().ok_or_else(|| {
                ApiError::bad_request(format!("`{key}.{field}` must be an array"))
            })?;
            if !list.iter().all(Value::is_string) {
                return Err(ApiError::bad_request(format!(
                    "`{key}.{field}` must contain strings"
                )));
            }
        }
    }
    Ok(())
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
    let path = uri.path().to_string();
    let started = std::time::Instant::now();
    crate::metrics::request_started();
    let span = tracing::info_span!(
        "http.request",
        "http.method" = %method,
        "http.url" = %uri
    );
    let response = next.run(req).instrument(span).await;
    let status = response.status().as_u16();
    let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
    crate::metrics::record_http(method.as_str(), status, duration_ms, &path);
    crate::telemetry::record_http(method.as_str(), status, duration_ms);
    tracing::info!("{} {} -> {}", method, uri, status);
    response
}

/// Per-principal admission. This is the fairness layer: no single user can fill
/// the global in-flight pool, and view/search work uses separate heavy permits.
pub async fn admit(
    State(st): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = req.uri().path();
    if crate::admission::Admission::exempt(path) {
        return next.run(req).await;
    }
    let principal = st.principal(req.headers());
    let lane = crate::admission::Admission::classify(path);
    match st.admission.try_admit(&principal, lane) {
        Ok(_permit) => next.run(req).await,
        Err(denied) => {
            let mut response = json(
                StatusCode::TOO_MANY_REQUESTS,
                json!({
                    "error": "too_many_requests",
                    "reason": denied.reason(),
                }),
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            response
        }
    }
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

    let principal = st.principal(&headers);
    if !principal.admin {
        let mut visible = Vec::new();
        for name in names {
            if let Ok(db) = st.db(&name).await {
                if db_access(&st, &principal, &db).is_some() {
                    visible.push(name);
                }
            }
        }
        names = visible;
    }

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
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
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
                if limit.map(|limit| sent >= limit).unwrap_or(false)
                    || !st.principal(&headers).admin
                {
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
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
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
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
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

/// Cluster overview for the management console: membership, discovery probes,
/// per-database shard maps and the background replicator snapshot.
pub async fn cluster_status(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
    }
    let membership: Vec<String> = if st.cluster.nodes.is_empty() {
        vec!["nonode@localhost".to_string()]
    } else {
        st.cluster.nodes.clone()
    };
    let mut databases = Vec::new();
    for name in st.db_names().await {
        let Ok(db) = st.db(&name).await else {
            continue;
        };
        let map = db.shard_map();
        let counts = db.shard_counts().await.unwrap_or_default();
        let shards: Vec<Value> = (0..map.q)
            .map(|shard| {
                let replicas = map.replicas_of(shard).to_vec();
                let (docs, deleted) = counts.get(shard as usize).copied().unwrap_or((0, 0));
                json!({
                    "shard": shard,
                    "replicas": replicas,
                    "owner": map.owner_of(shard),
                    "local": replicas.contains(&st.cluster.me),
                    "docs": docs,
                    "deleted": deleted,
                })
            })
            .collect();
        databases.push(json!({
            "name": name,
            "q": map.q,
            "node_count": map.node_count,
            "version": map.version,
            "shards": shards,
        }));
    }
    json(
        StatusCode::OK,
        json!({
            "local": {
                "url": st.config.advertise,
                "index": st.cluster.me,
                "clustered": st.cluster.is_clustered(),
                "q": st.config.q,
                "n": st.cluster.replication_factor(),
                "sync_secs": st.config.sync_secs,
            },
            "membership": {
                "all_nodes": membership.clone(),
                "cluster_nodes": membership,
            },
            "discovery": st.cluster_state.discovery_json(&st.cluster.nodes, &st.config.seeds),
            "replication": st.cluster_state.replication_json(),
            "databases": databases,
        }),
    )
}

/// Probe peers and reported memberships now. Returns the previous snapshot and
/// starts the pass in the background so a dead seed cannot block the request.
pub async fn cluster_discover(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
    }
    if !st.cluster_state.discovery_running() {
        tokio::spawn(crate::discovery::discover_once(st.clone()));
    }
    json(
        StatusCode::ACCEPTED,
        json!({
            "ok": true,
            "running": st.cluster_state.discovery_running(),
            "discovery": st.cluster_state.discovery_json(&st.cluster.nodes, &st.config.seeds),
        }),
    )
}

pub async fn replication_status(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
    }
    json(StatusCode::OK, st.cluster_state.replication_json())
}

/// Trigger one anti-entropy pass. Background replication already runs on its
/// timer; this is the "sync now" control for operators.
pub async fn replication_run(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
    }
    let db = query(&uri).get("db").cloned();
    if !st.cluster_state.replication_running() {
        tokio::spawn(crate::replicate::sync_once(st.clone(), db));
    }
    json(
        StatusCode::ACCEPTED,
        json!({"ok": true, "running": true, "replication": st.cluster_state.replication_json()}),
    )
}

pub async fn replication_pause(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
    }
    st.cluster_state.set_replication_paused(true);
    json(
        StatusCode::OK,
        json!({"ok": true, "replication": st.cluster_state.replication_json()}),
    )
}

pub async fn replication_resume(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
    }
    st.cluster_state.set_replication_paused(false);
    json(
        StatusCode::OK,
        json!({"ok": true, "replication": st.cluster_state.replication_json()}),
    )
}

/// mem3-style shard-map inspection and movement endpoint.
pub async fn dbs_metadata(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
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
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
    }
    match st.db(&name).await {
        Ok(db) => json(StatusCode::OK, json!(db.shard_map())),
        Err(error) => fail(error),
    }
}

async fn send_security(
    st: &AppState,
    peer: &str,
    name: &str,
    value: &Value,
    headers: &HeaderMap,
) -> Result<(), String> {
    let request = st
        .client
        .put(format!("{peer}/_node/_local/_dbs/{name}/security"))
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", st.cluster.secret.as_str())
        .json(value);
    let request = forward_client_auth(request, headers);
    match request.send().await {
        Ok(response) if response.status().is_success() => Ok(()),
        Ok(response) => Err(format!("{peer}: HTTP {}", response.status())),
        Err(error) => Err(format!("{peer}: {error}")),
    }
}

/// Apply a security object locally and, in a cluster, to every peer before
/// reporting success. A partial fan-out is rolled back so a reachable peer does
/// not keep a newer security object than the coordinator.
async fn set_security_everywhere(
    st: &AppState,
    db: &Db,
    name: &str,
    value: Value,
    headers: &HeaderMap,
) -> DbResult<()> {
    if !st.cluster.is_clustered() || is_forwarded(st, headers) {
        return db.set_security(value);
    }
    let old = db.security();
    db.set_security(value.clone())?;
    let peers: Vec<String> = st
        .cluster
        .nodes
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != st.cluster.me)
        .map(|(_, peer)| peer.clone())
        .collect();
    let mut failures = Vec::new();
    for peer in &peers {
        if let Err(error) = send_security(st, peer, name, &value, headers).await {
            failures.push(error);
        }
    }
    if failures.is_empty() {
        return Ok(());
    }
    let rollback_error = db.set_security(old.clone()).err();
    for peer in &peers {
        if let Err(error) = send_security(st, peer, name, &old, headers).await {
            tracing::warn!("security rollback to {peer} failed: {error}");
        }
    }
    let reason = failures.join("; ");
    if let Some(error) = rollback_error {
        return Err(ApiError::new(
            500,
            "security_rollback_failed",
            format!("{reason}; local rollback failed: {}", error.reason),
        ));
    }
    Err(ApiError::new(
        502,
        "security_propagation_failed",
        format!("security change was not applied everywhere: {reason}"),
    ))
}

/// Internal/replication-side security update. The public `PUT /db/_security`
/// handler fans out here after checking database-admin access.
/// Internal target-placement update used while a rebalance is being verified.
pub async fn db_migration_put(
    State(st): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !st.cluster.is_internal(&headers) {
        let principal = st.principal(&headers);
        return access_denied(&principal);
    }
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(error) => return fail(error),
    };
    let map: ShardMap = match serde_json::from_slice(&body) {
        Ok(map) => map,
        Err(_) => return fail(ApiError::bad_request("invalid migration shard map")),
    };
    if map.q != db.q || map.node_count != st.cluster.nodes.len().max(1) {
        return fail(ApiError::bad_request(
            "migration shard map does not match database q or cluster node count",
        ));
    }
    match db.set_migration_map(map) {
        Ok(()) => {
            // Freeze client writes on this peer for the verification window.
            db.set_rebalance_in_progress(true);
            ok()
        }
        Err(error) => fail(error),
    }
}

pub async fn db_migration_delete(
    State(st): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !st.cluster.is_internal(&headers) {
        let principal = st.principal(&headers);
        return access_denied(&principal);
    }
    match st.db(&name).await {
        Ok(db) => {
            db.clear_migration_map();
            db.set_rebalance_in_progress(false);
            ok()
        }
        Err(error) => fail(error),
    }
}

pub async fn db_security_put(
    State(st): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !st.cluster.is_internal(&headers) {
        let principal = st.principal(&headers);
        return access_denied(&principal);
    }
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(error) => return fail(error),
    };
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return fail(ApiError::bad_request("invalid security object")),
    };
    if let Err(error) = validate_security(&value) {
        return fail(error);
    }
    match db.set_security(value) {
        Ok(()) => ok(),
        Err(error) => fail(error),
    }
}

pub async fn db_shard_counts(
    State(st): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
    }
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(error) => return fail(error),
    };
    let with_digest = flag(&query(&uri), "digest");
    match db.shard_states(with_digest).await {
        Ok(states) => json(
            StatusCode::OK,
            json!({
                "shards": states
                    .into_iter()
                    .enumerate()
                    .map(|(shard, state)| {
                        json!({
                            "shard": shard,
                            "docs": state.docs,
                            "deleted": state.deleted,
                            "seq": state.seq,
                            "digest": state.digest,
                        })
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
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
    }
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(error) => return fail(error),
    };
    // A migration owns placement changes until it activates or rolls back.
    // Internal activation/rollback requests carry the forwarded marker.
    if db.rebalance_in_progress() && !is_forwarded(&st, &headers) {
        return fail(ApiError::new(
            409,
            "rebalance_in_progress",
            "shard placement cannot change while a rebalance is running",
        ));
    }
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

async fn fetch_shard_states(
    st: &AppState,
    db: &Arc<Db>,
    node: usize,
    headers: &HeaderMap,
    with_digest: bool,
) -> Option<Vec<ShardState>> {
    if node == st.cluster.me {
        return db.shard_states(with_digest).await.ok();
    }
    let suffix = if with_digest { "?digest=1" } else { "" };
    let request = st
        .client
        .get(format!(
            "{}/_node/_local/_dbs/{}/shards{suffix}",
            st.cluster.nodes[node], db.name
        ))
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", st.cluster.secret.as_str());
    let request = forward_client_auth(request, headers);
    let value = request.send().await.ok()?.json::<Value>().await.ok()?;
    let mut out = Vec::new();
    for row in value["shards"].as_array()? {
        out.push(ShardState {
            docs: row.get("docs").and_then(Value::as_u64).unwrap_or(0),
            deleted: row.get("deleted").and_then(Value::as_u64).unwrap_or(0),
            seq: row.get("seq").and_then(Value::as_u64).unwrap_or(0),
            digest: row
                .get("digest")
                .and_then(Value::as_str)
                .map(str::to_string),
        });
    }
    Some(out)
}

fn same_shard_content(left: &ShardState, right: &ShardState) -> bool {
    left.docs == right.docs && left.deleted == right.deleted && left.digest == right.digest
}

/// Per-shard source digest, or `None` while old replicas disagree, are
/// unreachable, or did not produce a digest.
fn source_required(
    states: &[Option<Vec<ShardState>>],
    old_map: &ShardMap,
    q: u32,
    require_digest: bool,
) -> Option<Vec<ShardState>> {
    let mut required = Vec::with_capacity(q as usize);
    for shard in 0..q {
        let mut chosen: Option<&ShardState> = None;
        for node in old_map.replicas_of(shard) {
            // Every old owner must answer; otherwise a replica that is merely
            // unreachable could be dropped while it still holds newer writes.
            let Some(state) = states
                .get(*node)
                .and_then(|state| state.as_ref())
                .and_then(|state| state.get(shard as usize))
            else {
                return None;
            };
            match chosen {
                Some(previous) if !same_shard_content(previous, state) => return None,
                Some(_) => {}
                None => chosen = Some(state),
            }
        }
        let state = chosen?;
        if require_digest && state.digest.is_none() {
            return None;
        }
        required.push(state.clone());
    }
    Some(required)
}

/// Wait for the frozen old owners to settle on one digest per shard. Writes
/// accepted just before the freeze may still be finishing, so brief
/// disagreement is retried rather than reported as corruption.
async fn stable_source_states(
    st: &AppState,
    db: &Arc<Db>,
    old_map: &ShardMap,
    headers: &HeaderMap,
    grace: std::time::Duration,
) -> DbResult<Vec<ShardState>> {
    let deadline = std::time::Instant::now() + grace;
    loop {
        let states = futures::future::join_all(
            (0..st.cluster.nodes.len()).map(|node| fetch_shard_states(st, db, node, headers, true)),
        )
        .await;
        if let Some(required) = source_required(&states, old_map, db.q, true) {
            return Ok(required);
        }
        if std::time::Instant::now() >= deadline {
            return Err(ApiError::new(
                502,
                "rebalance_failed",
                "old replicas did not agree on a stable content digest",
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn wait_for_rebalance(
    st: &AppState,
    db: &Arc<Db>,
    old_map: &ShardMap,
    map: &ShardMap,
    headers: &HeaderMap,
    timeout_ms: u64,
) -> DbResult<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    loop {
        // Recompute the source state each round. Once writes are frozen the
        // source stops moving, so this converges on the latest acknowledged
        // state rather than a snapshot taken before an in-flight write landed.
        // Counts are cheap and prune most retry rounds; compare digests only
        // after every target has caught up on the source counts.
        let counts = futures::future::join_all(
            (0..st.cluster.nodes.len())
                .map(|node| fetch_shard_states(st, db, node, headers, false)),
        )
        .await;
        let counts_ready = source_required(&counts, old_map, db.q, false)
            .map(|required| {
                (0..db.q).all(|shard| {
                    let Some(expected) = required.get(shard as usize) else {
                        return false;
                    };
                    map.replicas_of(shard).iter().all(|node| {
                        counts
                            .get(*node)
                            .and_then(|state| state.as_ref())
                            .and_then(|state| state.get(shard as usize))
                            .map(|state| {
                                state.docs >= expected.docs && state.deleted == expected.deleted
                            })
                            .unwrap_or(false)
                    })
                })
            })
            .unwrap_or(false);
        if counts_ready {
            let digests = futures::future::join_all(
                (0..st.cluster.nodes.len())
                    .map(|node| fetch_shard_states(st, db, node, headers, true)),
            )
            .await;
            let ready = source_required(&digests, old_map, db.q, true)
                .map(|required| {
                    (0..db.q).all(|shard| {
                        let Some(expected) = required.get(shard as usize) else {
                            return false;
                        };
                        map.replicas_of(shard).iter().all(|node| {
                            digests
                                .get(*node)
                                .and_then(|state| state.as_ref())
                                .and_then(|state| state.get(shard as usize))
                                .map(|state| {
                                    state.docs >= expected.docs
                                        && state.deleted == expected.deleted
                                        && state.digest == expected.digest
                                })
                                .unwrap_or(false)
                        })
                    })
                })
                .unwrap_or(false);
            if ready {
                return Ok(());
            }
        }

        if std::time::Instant::now() >= deadline {
            return Err(ApiError::new(
                504,
                "rebalance_timeout",
                "new replicas did not match the old shard content before the timeout",
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
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

async fn send_migration(
    st: &AppState,
    peer: &str,
    name: &str,
    map: Option<&ShardMap>,
    headers: &HeaderMap,
) -> Result<(), String> {
    let url = format!("{peer}/_node/_local/_dbs/{name}/migration");
    let request = match map {
        Some(map) => st
            .client
            .put(url)
            .header("x-douchdb-forwarded", "1")
            .header("x-douchdb-secret", st.cluster.secret.as_str())
            .json(map),
        None => st
            .client
            .delete(url)
            .header("x-douchdb-forwarded", "1")
            .header("x-douchdb-secret", st.cluster.secret.as_str()),
    };
    let request = forward_client_auth(request, headers);
    match request.send().await {
        Ok(response) if response.status().is_success() => Ok(()),
        Ok(response) => Err(format!("{peer}: HTTP {}", response.status())),
        Err(error) => Err(format!("{peer}: {error}")),
    }
}

async fn set_migration_everywhere(
    st: &AppState,
    db: &Db,
    name: &str,
    map: &ShardMap,
    headers: &HeaderMap,
) -> DbResult<()> {
    db.set_migration_map(map.clone())?;
    let peers: Vec<String> = st.cluster.peers().cloned().collect();
    let mut failures = Vec::new();
    for peer in &peers {
        if let Err(error) = send_migration(st, peer, name, Some(map), headers).await {
            failures.push(error);
        }
    }
    if failures.is_empty() {
        return Ok(());
    }
    db.clear_migration_map();
    for peer in &peers {
        let _ = send_migration(st, peer, name, None, headers).await;
    }
    Err(ApiError::new(
        502,
        "rebalance_failed",
        format!(
            "migration map was not accepted everywhere: {}",
            failures.join("; ")
        ),
    ))
}

async fn clear_migration_everywhere(st: &AppState, db: &Db, name: &str, headers: &HeaderMap) {
    db.clear_migration_map();
    for peer in st.cluster.peers() {
        if let Err(error) = send_migration(st, peer, name, None, headers).await {
            tracing::warn!("migration map clear on {peer} failed: {error}");
        }
    }
}

async fn activate_map_everywhere(
    st: &AppState,
    db: &Db,
    name: &str,
    map: &ShardMap,
    headers: &HeaderMap,
) -> DbResult<()> {
    let old = db.shard_map();
    db.set_shard_map(map.clone())?;
    let peers: Vec<String> = st.cluster.peers().cloned().collect();
    let mut failures = Vec::new();
    for peer in &peers {
        let request = st
            .client
            .put(format!("{peer}/_node/_local/_dbs/{name}"))
            .header("x-douchdb-forwarded", "1")
            .header("x-douchdb-secret", st.cluster.secret.as_str())
            .json(map);
        let request = forward_client_auth(request, headers);
        match request.send().await {
            Ok(response) if response.status().is_success() => {}
            Ok(response) => failures.push(format!("{peer}: HTTP {}", response.status())),
            Err(error) => failures.push(format!("{peer}: {error}")),
        }
    }
    if failures.is_empty() {
        return Ok(());
    }

    // Roll back with a strictly newer version so peers that already accepted
    // the target map also accept the old placement again.
    let mut rollback = old;
    rollback.version = map.version.saturating_add(1);
    let _ = db.set_shard_map(rollback.clone());
    for peer in &peers {
        let request = st
            .client
            .put(format!("{peer}/_node/_local/_dbs/{name}"))
            .header("x-douchdb-forwarded", "1")
            .header("x-douchdb-secret", st.cluster.secret.as_str())
            .json(&rollback);
        let request = forward_client_auth(request, headers);
        if let Err(error) = request.send().await {
            tracing::warn!("rebalance rollback on {peer} failed: {error}");
        }
    }
    Err(ApiError::new(
        502,
        "rebalance_failed",
        format!(
            "active map was not accepted everywhere: {}",
            failures.join("; ")
        ),
    ))
}

pub async fn db_rebalance(
    State(st): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
    }
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(error) => return fail(error),
    };
    let q = query(&uri);
    let cleanup = flag(&q, "cleanup");
    let timeout_ms = num(&q, "timeout_ms").unwrap_or(30_000).min(300_000) as u64;

    // Freeze client writes before reading the source digests. Reads continue to
    // use the old active map; anti-entropy is internal and remains allowed.
    if !db.try_start_rebalance() {
        return fail(ApiError::new(
            409,
            "rebalance_in_progress",
            "another rebalance is already running for this database",
        ));
    }
    crate::telemetry::record_rebalance_started();
    let old_map = db.shard_map();
    let mut map = ShardMap::balanced(
        db.q,
        st.cluster.nodes.len().max(1),
        st.cluster.replication_factor(),
    );
    map.version = old_map.version.saturating_add(1);
    // Broadcast the migration map first: it freezes writes on every peer and
    // lets anti-entropy seed the target replicas while reads stay on the old
    // active map.
    if let Err(error) = set_migration_everywhere(&st, &db, &name, &map, &headers).await {
        db.set_rebalance_in_progress(false);
        return fail(error);
    }

    if let Err(error) = stable_source_states(
        &st,
        &db,
        &old_map,
        &headers,
        // Allow slow digest scans plus any in-flight write to settle.
        std::time::Duration::from_millis(10_000),
    )
    .await
    {
        clear_migration_everywhere(&st, &db, &name, &headers).await;
        db.set_rebalance_in_progress(false);
        return fail(error);
    }

    if let Err(error) = wait_for_rebalance(&st, &db, &old_map, &map, &headers, timeout_ms).await {
        // The active map never changed, so reads continued on the old owners
        // throughout the migration attempt.
        clear_migration_everywhere(&st, &db, &name, &headers).await;
        db.set_rebalance_in_progress(false);
        if error.status == 504 {
            crate::telemetry::record_rebalance_timeout();
        }
        return fail(error);
    }

    if let Err(error) = activate_map_everywhere(&st, &db, &name, &map, &headers).await {
        clear_migration_everywhere(&st, &db, &name, &headers).await;
        db.set_rebalance_in_progress(false);
        return fail(error);
    }
    clear_migration_everywhere(&st, &db, &name, &headers).await;
    db.set_rebalance_in_progress(false);

    let mut value = json!(map.clone());
    value["pending"] = json!(false);
    if !cleanup {
        return json(StatusCode::OK, value);
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
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
    }
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(error) => return fail(error),
    };
    if db.rebalance_in_progress() {
        return fail(ApiError::new(
            409,
            "rebalance_in_progress",
            "cleanup is not allowed while this database changes shard placement",
        ));
    }
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
    let principal = st.principal(&headers);
    let had_credentials =
        headers.contains_key(header::AUTHORIZATION) || headers.contains_key(header::COOKIE);
    if had_credentials && !principal.is_authenticated() {
        return unauthorized();
    }
    let authenticated = principal.is_authenticated();
    let name = principal.name.clone().unwrap_or_else(|| {
        if authenticated {
            "admin".into()
        } else {
            Value::Null.to_string()
        }
    });
    json(
        StatusCode::OK,
        json!({
            "ok": true,
            "userCtx": { "name": name, "roles": principal.roles },
            "info": {
                "authenticated": authenticated,
                "authentication_db": "_users",
                "authentication_handlers": ["cookie", "default", "bearer"]
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
    let principal = match &st.auth {
        Some(auth) => auth.authenticate(&user, &pass),
        None => Some(Principal {
            name: Some(if user.is_empty() {
                "admin".into()
            } else {
                user.clone()
            }),
            roles: vec!["_admin".to_string()],
            admin: true,
        }),
    };
    let Some(principal) = principal else {
        return json(
            StatusCode::UNAUTHORIZED,
            json!({"error": "unauthorized", "reason": "Name or password is incorrect."}),
        );
    };
    let name = principal.name.clone().unwrap_or_else(|| user.clone());
    let mut response = json(
        StatusCode::OK,
        json!({
            "ok": true,
            "name": name,
            "roles": principal.roles,
        }),
    );
    if let Some(auth) = &st.auth {
        let cookie_name = principal.name.as_deref().unwrap_or(&user);
        if let Ok(value) = auth.cookie_header_for(cookie_name).parse() {
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
    if !valid_db_name(&name) {
        return fail(ApiError::bad_request("Name must be lowercase and URL-safe"));
    }
    let q = query(&uri);
    let principal = st.principal(&headers);

    let local_only = st.cluster.is_internal_db(&headers);
    match method {
        Method::PUT => {
            if !principal.admin {
                return access_denied(&principal);
            }
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
            if !principal.admin {
                return access_denied(&principal);
            }
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
                if let Some(response) = require_db_access(&st, &principal, &db, false) {
                    return response;
                }
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
            if let Some(response) = require_db_access(&st, &principal, &db, false) {
                return response;
            }
            if let Some(response) = reject_during_rebalance(&st, &db, &Method::POST, "", &headers) {
                return response;
            }
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
    if rest.is_empty() {
        // CouchDB accepts `GET /db/` and PouchDB uses it during setup.
        return db_root(State(st), Path(name), method, uri, headers, body).await;
    }
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(e) => return fail(e),
    };
    let principal = st.principal(&headers);
    if let Some(response) =
        require_db_access(&st, &principal, &db, requires_db_admin(&rest, &method))
    {
        return response;
    }
    if let Some(response) = reject_during_rebalance(&st, &db, &method, &rest, &headers) {
        return response;
    }
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
            let Ok(_permit) = st.compaction.try_acquire() else {
                return fail(ApiError::new(
                    409,
                    "compaction_in_progress",
                    "another compaction is already running on this node",
                ));
            };
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
                Method::GET => json(StatusCode::OK, db.security()),
                Method::PUT => {
                    let value: Value = match serde_json::from_slice(&body) {
                        Ok(value) => value,
                        Err(_) => return fail(ApiError::bad_request("invalid security object")),
                    };
                    if let Err(error) = validate_security(&value) {
                        return fail(error);
                    }
                    match set_security_everywhere(&st, &db, &name, value, &headers).await {
                        Ok(()) => ok(),
                        Err(error) => fail(error),
                    }
                }
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
            return match crate::search::cleanup(&db).await {
                Ok(()) => json(StatusCode::ACCEPTED, json!({"ok": true})),
                Err(error) => fail(error),
            }
        }
        "_index_info" => {
            let search = crate::search::all_indexes(&db);
            let total_rows = search.len();
            return json(
                StatusCode::OK,
                json!({
                    "total_rows": total_rows,
                    "bookmark": Value::Null,
                    "view_indexes": [],
                    "search_indexes": search,
                    "nouveau_indexes": [],
                }),
            );
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

fn string_list(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) => text
            .split(',')
            .map(str::trim)
            .filter(|field| !field.is_empty())
            .map(str::to_string)
            .collect(),
        Value::Array(values) => values
            .iter()
            .filter_map(|value| value.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    }
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
                if let Some(sort) = object.get("sort") {
                    request.sort = crate::search::parse_sort(sort);
                }
                if let Some(fields) = object.get("highlight_fields") {
                    request.highlight_fields = string_list(fields);
                }
                if let Some(value) = object.get("highlight_pre_tag").and_then(Value::as_str) {
                    request.highlight_pre = value.to_string();
                }
                if let Some(value) = object.get("highlight_post_tag").and_then(Value::as_str) {
                    request.highlight_post = value.to_string();
                }
                if let Some(value) = object.get("highlight_size").and_then(Value::as_u64) {
                    request.highlight_size = value as usize;
                }
                if let Some(value) = object.get("highlight_number").and_then(Value::as_u64) {
                    request.highlight_number = value as usize;
                }
                if let Some(value) = object.get("timeout").and_then(Value::as_u64) {
                    request.timeout_ms = value;
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
        if let Some(raw) = q.get("sort") {
            let value = serde_json::from_str::<Value>(raw).unwrap_or(Value::String(raw.clone()));
            request.sort = crate::search::parse_sort(&value);
        }
        if let Some(raw) = q.get("highlight_fields") {
            let value =
                serde_json::from_str::<Value>(raw).unwrap_or_else(|_| Value::String(raw.clone()));
            request.highlight_fields = string_list(&value);
        }
        if let Some(value) = q
            .get("highlight_pre_tag")
            .or_else(|| q.get("highlight_pre"))
        {
            request.highlight_pre = value.clone();
        }
        if let Some(value) = q
            .get("highlight_post_tag")
            .or_else(|| q.get("highlight_post"))
        {
            request.highlight_post = value.clone();
        }
        if let Some(value) = num(q, "highlight_size") {
            request.highlight_size = value;
        }
        if let Some(value) = num(q, "highlight_number") {
            request.highlight_number = value;
        }
        if let Some(value) = num(q, "timeout") {
            request.timeout_ms = value as u64;
        }
    }

    let started = std::time::Instant::now();
    let result = if is_forwarded(st, headers) {
        if let Some(mask) = q.get("shards").and_then(|raw| parse_shard_mask(raw, db.q)) {
            request.owner_shards = Some(mask);
        }
        crate::search::query(db, ddoc, index_name, &request, limits).await
    } else if st.cluster.is_clustered() {
        cluster_search(st, db, dbname, ddoc, index_name, &request, limits, headers).await
    } else {
        crate::search::query(db, ddoc, index_name, &request, limits).await
    };
    if !is_forwarded(st, headers) {
        crate::metrics::record_engine(
            "search",
            started.elapsed().as_secs_f64() * 1000.0,
            result.is_ok(),
        );
    }
    if !still_allowed(st, db, headers) {
        return access_denied(&st.principal(headers));
    }
    match result {
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
        let group_level = num(q, "group_level");
        if group_level.is_some_and(|level| level > 0)
            && q.get("group").is_some_and(|_| !flag(q, "group"))
        {
            return Err(ApiError::new(
                400,
                "query_parse_error",
                "Can't specify group=false and group_level>0 at the same time",
            ));
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
            // CouchDB: an explicit group_level wins over group. With
            // group_level=0 the result is ungrouped even if group=true.
            group: group_level.is_none() && flag(q, "group"),
            group_level: group_level.unwrap_or(0),
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

    let started = std::time::Instant::now();
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
    if !is_forwarded(st, headers) {
        crate::metrics::record_engine(
            "views",
            started.elapsed().as_secs_f64() * 1000.0,
            result.is_ok(),
        );
    }
    if !still_allowed(st, db, headers) {
        return access_denied(&st.principal(headers));
    }

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
        "PUT" => match crate::json::from_slice(body) {
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
    let Ok(value) = crate::json::from_slice(body) else {
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
    // `_bulk_docs` is a member operation for ordinary documents, but design
    // documents are database-admin data. Check the batch, not just the route.
    let writes_design = docs.iter().any(|doc| {
        doc.get("_id")
            .and_then(Value::as_str)
            .map(|id| id.starts_with("_design/"))
            .unwrap_or(false)
    });
    if writes_design {
        let principal = st.principal(headers);
        if let Some(response) = require_db_access(st, &principal, db, true) {
            return response;
        }
    }
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
    if !still_allowed(st, db, headers) {
        return access_denied(&st.principal(headers));
    }
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
                if limit.map(|limit| sent >= limit).unwrap_or(false)
                    || !still_allowed(&st, &db, &headers)
                {
                    break;
                }
                match changes_once(&st, &db, &dbname, &query, &body, &headers, local_only).await {
                    Ok(value) => {
                        if !still_allowed(&st, &db, &headers) {
                            break;
                        }
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
                    if !still_allowed(&st, &db, &headers) {
                        break;
                    }
                    match changes_once(&st, &db, &dbname, &query, &body, &headers, local_only).await {
                        Ok(value)
                            if (value["results"]
                                .as_array()
                                .map(|rows| !rows.is_empty())
                                .unwrap_or(false)
                                || started.elapsed().as_millis() as u64 >= deadline)
                                && still_allowed(&st, &db, &headers) =>
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
            if !still_allowed(&st, &db, headers) {
                return access_denied(&st.principal(headers));
            }
            match changes_once(&st, &db, dbname, q, body, headers, local_only).await {
                Ok(v)
                    if v["results"]
                        .as_array()
                        .map(|a| !a.is_empty())
                        .unwrap_or(false) =>
                {
                    if !still_allowed(&st, &db, headers) {
                        return access_denied(&st.principal(headers));
                    }
                    return json(StatusCode::OK, v);
                }
                Ok(v) if started.elapsed().as_millis() as u64 >= deadline => {
                    if !still_allowed(&st, &db, headers) {
                        return access_denied(&st.principal(headers));
                    }
                    return json(StatusCode::OK, v);
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
    // `revs_info=false` must not downgrade the requested consistency level.
    let revision_shaped = q.contains_key("open_revs") || flag(q, "revs_info");
    if attachment.is_none()
        && (method == Method::GET || method == Method::HEAD)
        && st.cluster.is_clustered()
        && read_quorum > 1
        && !is_forwarded(st, headers)
    {
        if revision_shaped {
            // Revision-shaped responses are served locally, but the requested
            // quorum still has to be reachable.
            if let Some(response) =
                quorum_reachable(st, db, dbname, &docid, q, headers, read_quorum).await
            {
                return response;
            }
        } else {
            return quorum_read(st, db, dbname, &docid, q, headers, read_quorum).await;
        }
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
        Method::GET | Method::HEAD => {
            let include_deleted =
                is_forwarded(st, headers) && headers.contains_key("x-douchdb-quorum-read");
            get_doc(db, &docid, q, headers, include_deleted).await
        }
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
    include_deleted: bool,
) -> Response {
    let mut opts = doc_opts(q);
    opts.include_deleted = include_deleted;
    match db.get_doc(docid, &opts).await {
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

pub(crate) fn etag_matches(headers: &HeaderMap, name: header::HeaderName, etag: &str) -> bool {
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
    if destination.starts_with("_design/") {
        let principal = st.principal(headers);
        if let Some(response) = require_db_access(st, &principal, db, true) {
            return response;
        }
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
    let mut doc = match crate::json::from_slice(&body) {
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

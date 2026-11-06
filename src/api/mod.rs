//! HTTP surface. The shape of every response here is dictated by CouchDB's
//! wire protocol, not by taste.

use crate::auth::{Auth, Principal};
use crate::cluster::{Cluster, ShardMap};
use crate::db::{
    validate_doc, AllDocsOpts, ApiError, ChangeFilter, ChangesOpts, Db, DbConfig, DbResult, DocOpts,
};
use crate::fabric::*;
pub use crate::observability::{health_public, health_status, metrics_status};
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
    /// One lock per database name. Open, create and drop of a name run one at
    /// a time, so a name is never opened twice (the second open would fail on
    /// the LevelDB lock) and a drop cannot interleave with a create or open.
    pub db_locks: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    pub config: Config,
    pub auth: Option<Auth>,
    pub cluster: Cluster,
    pub client: reqwest::Client,
    pub db_updates: DbUpdates,
    pub cluster_state: Arc<crate::cluster_state::ClusterState>,
    pub admission: Arc<crate::admission::Admission>,
    /// Bound concurrent `_compact` requests across databases on this node.
    pub compaction: Arc<tokio::sync::Semaphore>,
    /// Recent result of the health check's blocking filesystem probe.
    pub fs_health: crate::observability::FsHealthCache,
}

#[derive(Clone)]
pub struct Config {
    pub q: u32,
    pub revs_limit: u64,
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

/// A request against one database, as `db_rest` hands it to a handler.
#[derive(Clone, Copy)]
pub(crate) struct DbRequest<'a> {
    pub st: &'a AppState,
    pub db: &'a Db,
    pub name: &'a str,
    pub method: &'a Method,
    pub uri: &'a Uri,
    pub q: &'a HashMap<String, String>,
    pub headers: &'a HeaderMap,
    pub body: &'a Bytes,
}

// ---------------------------------------------------------------------------
// small helpers

pub(crate) fn json(status: StatusCode, value: Value) -> Response {
    (status, Json(value)).into_response()
}

pub(crate) fn fail(e: ApiError) -> Response {
    if e.status >= 500 {
        tracing::error!(
            status = e.status,
            "request failed: {}: {}",
            e.error,
            e.reason
        );
    }
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

/// CouchDB answers a request with the wrong method with 405, an `Allow`
/// header and `Only GET,POST allowed`; the caller passes the allowed methods in
/// CouchDB's order.
pub(crate) fn method_not_allowed(allowed: &[&str]) -> Response {
    let list = allowed.join(",");
    let mut response = json(
        StatusCode::METHOD_NOT_ALLOWED,
        json!({
            "error": "method_not_allowed",
            "reason": format!("Only {list} allowed"),
        }),
    );
    if let Ok(value) = HeaderValue::from_str(&list) {
        response.headers_mut().insert(header::ALLOW, value);
    }
    response
}

/// `Some(405 response)` unless `method` is one of `allowed`.
pub(crate) fn require_method(method: &Method, allowed: &[&str]) -> Option<Response> {
    if allowed.contains(&method.as_str()) {
        None
    } else {
        Some(method_not_allowed(allowed))
    }
}

/// The largest `limit` CouchDB accepts.
const MAX_LIMIT: u64 = 576_460_752_303_423_487;

/// A `limit` or `skip` query parameter. CouchDB rejects a value that is not a
/// non-negative integer with `query_parse_error`; ignoring it would turn a
/// typo into an unbounded query.
pub(crate) fn uint_param(q: &HashMap<String, String>, key: &str) -> DbResult<Option<usize>> {
    let Some(raw) = q.get(key) else {
        return Ok(None);
    };
    let invalid = |kind: &str| {
        let shown = if raw.is_empty() {
            "[]".to_string()
        } else {
            format!("\"{raw}\"")
        };
        ApiError::new(
            400,
            "query_parse_error",
            format!("Invalid value for {kind}: {shown}"),
        )
    };
    match raw.parse::<i128>() {
        Ok(n) if n < 0 => Err(invalid("positive integer")),
        Ok(n) if key == "limit" && n > i128::from(MAX_LIMIT) => Err(ApiError::new(
            400,
            "query_parse_error",
            format!("Limit is too large, must not exceed {MAX_LIMIT}"),
        )),
        Ok(n) => Ok(Some(usize::try_from(n).unwrap_or(usize::MAX))),
        Err(_) => Err(invalid("integer")),
    }
}

/// A `limit` or `skip` member of a JSON request body, with CouchDB's errors.
pub(crate) fn uint_member(body: &Map<String, Value>, key: &str) -> DbResult<Option<usize>> {
    let Some(value) = body.get(key) else {
        return Ok(None);
    };
    if let Some(n) = value.as_u64() {
        return Ok(Some(usize::try_from(n).unwrap_or(usize::MAX)));
    }
    let (kind, shown) = match value {
        Value::Number(n) if n.is_i64() => ("positive integer", n.to_string()),
        Value::String(s) => ("integer", format!("<<\"{s}\">>")),
        other => ("integer", other.to_string()),
    };
    Err(ApiError::new(
        400,
        "query_parse_error",
        format!("Invalid value for {kind}: {shown}"),
    ))
}

/// A POST body that must be a JSON object. An empty body is an empty object.
pub(crate) fn body_object(body: &[u8]) -> DbResult<Map<String, Value>> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(Map::new());
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(object)) => Ok(object),
        Ok(_) => Err(ApiError::bad_request("Request body must be a JSON object")),
        Err(_) => Err(ApiError::bad_request("invalid UTF-8 JSON")),
    }
}

/// The HTTP status for a failed per-document write result (`{"error": ...}`).
pub(crate) fn doc_error_status(result: &Value) -> StatusCode {
    match result.get("error").and_then(Value::as_str) {
        Some("conflict") => StatusCode::CONFLICT,
        Some("forbidden") => StatusCode::FORBIDDEN,
        Some("unauthorized") => StatusCode::UNAUTHORIZED,
        Some("replica_unavailable" | "internal_server_error" | "unknown_error") => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
        _ => StatusCode::BAD_REQUEST,
    }
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

/// True for a request a peer already routed. Such requests are served from the
/// local node's shards: re-running the fan-out on the receiving side would send
/// the request straight back and loop.
pub(crate) fn is_forwarded(st: &AppState, headers: &HeaderMap) -> bool {
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

/// A peer URL with every path segment percent-encoded. Database names may hold
/// `/` (CouchDB clients send `a%2Fb`) and document ids may hold `?`, `#` or
/// `%`, so a segment is never interpolated raw: the receiving node decodes
/// each one exactly once.
pub(crate) fn peer_url(peer: &str, segments: &[&str]) -> String {
    let mut url = peer.to_string();
    for segment in segments {
        url.push('/');
        url.push_str(&percent_encode(segment));
    }
    url
}

/// An internal cluster request: encoded URL plus the forwarded marker and the
/// cluster secret that make the peer serve it from its local shards.
pub(crate) fn internal_request_raw(
    client: &reqwest::Client,
    secret: &str,
    method: reqwest::Method,
    peer: &str,
    segments: &[&str],
) -> reqwest::RequestBuilder {
    client
        .request(method, peer_url(peer, segments))
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", secret)
}

/// `internal_request_raw` plus credentials: the calling client's own when the request
/// was made on its behalf (`Some(headers)`), otherwise this node's.
pub(crate) fn internal_request(
    st: &AppState,
    method: reqwest::Method,
    peer: &str,
    segments: &[&str],
    client_headers: Option<&HeaderMap>,
) -> reqwest::RequestBuilder {
    let request = internal_request_raw(&st.client, &st.cluster.secret, method, peer, segments);
    match (client_headers, &st.auth) {
        (Some(headers), _) => forward_client_auth(request, headers),
        (None, Some(auth)) => request.basic_auth(&auth.user, Some(&auth.pass)),
        (None, None) => request,
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
        && !name.starts_with('/')
        && !name.ends_with('/')
        && !name.contains("//")
        && name.bytes().all(|b| {
            b.is_ascii_lowercase()
                || b.is_ascii_digit()
                || matches!(b, b'_' | b'-' | b'$' | b'+' | b'(' | b')' | b'/')
        })
}

// ---------------------------------------------------------------------------
// state

impl AppState {
    fn name_lock(&self, name: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self
            .db_locks
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        locks.entry(name.to_string()).or_default().clone()
    }

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
        // Concurrent first requests for one name share a single open, and a
        // drop that won the race leaves nothing to open.
        let lock = self.name_lock(name);
        let _guard = lock.lock().await;
        if let Some(db) = self.dbs.read().await.get(name) {
            return Ok(db.clone());
        }
        let db = Arc::new(Db::open_existing(
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

    /// Create a database with `q` shards and `n` copies of each; `None`
    /// takes the node's configured default.
    pub async fn create_db(
        &self,
        name: &str,
        partitioned: bool,
        q: Option<u32>,
        n: Option<usize>,
    ) -> DbResult<Arc<Db>> {
        if !valid_db_name(name) {
            return Err(ApiError::bad_request("invalid database name"));
        }
        let lock = self.name_lock(name);
        let _guard = lock.lock().await;
        crate::view::forget_db(&self.root.join(name));
        crate::search::forget_db(&self.root.join(name));
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
                q: q.unwrap_or(self.config.q),
                revs_limit: self.config.revs_limit,
                shard_map: self
                    .cluster
                    .shard_map_n(q.unwrap_or(self.config.q), n.unwrap_or(self.cluster.n)),
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
        let lock = self.name_lock(name);
        let _guard = lock.lock().await;
        self.dbs.write().await.remove(name);
        crate::view::forget_db(&self.root.join(name));
        crate::search::forget_db(&self.root.join(name));
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

mod access;
mod bulk;
mod changes;
mod database;
mod documents;
mod middleware;
mod node;
mod root;
mod session;
mod views;

pub(crate) use access::*;
use bulk::*;
pub(crate) use changes::*;
pub(crate) use database::*;
pub(crate) use documents::*;
pub(crate) use middleware::*;
pub(crate) use node::*;
pub(crate) use root::*;
pub(crate) use session::*;
use views::*;

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

pub async fn not_found() -> Response {
    json(
        StatusCode::NOT_FOUND,
        json!({"error": "not_found", "reason": "missing"}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn percent_decode_round_trips_encoded_bytes(bytes in prop::collection::vec(any::<u8>(), 0..200)) {
            let encoded: String = bytes.iter().map(|byte| format!("%{byte:02X}")).collect();
            let decoded = percent_decode(&encoded);
            prop_assert_eq!(decoded, String::from_utf8_lossy(&bytes));
        }

        #[test]
        fn percent_decode_never_panics(text in ".{0,300}") {
            let _ = percent_decode(&text);
        }

        #[test]
        fn shard_mask_round_trips_valid_sets(q in 1u32..64, selected in prop::collection::vec(any::<bool>(), 1..64)) {
            let mut selected = selected;
            selected.resize(q as usize, false);
            let raw = selected
                .iter()
                .enumerate()
                .filter(|&(_, on)| *on)
                .map(|(index, _)| index.to_string())
                .collect::<Vec<_>>()
                .join(",");
            let mask = parse_shard_mask(&raw, q).unwrap_or_else(|| panic!("failed to parse {raw}"));
            prop_assert_eq!(mask.len(), q as usize);
            prop_assert_eq!(mask, selected);
        }

        #[test]
        fn shard_mask_never_panics(raw in ".{0,120}", q in 0u32..64) {
            let _ = parse_shard_mask(&raw, q);
        }
    }

    #[test]
    fn percent_decode_handles_plus_and_bad_escapes() {
        assert_eq!(percent_decode("a+b"), "a b");
        assert_eq!(percent_decode("%zz"), "%zz");
        assert_eq!(percent_decode("%2"), "%2");
        assert_eq!(percent_decode("%C3%A9"), "é");
    }

    #[test]
    fn peer_url_encodes_every_segment() {
        assert_eq!(
            peer_url("http://n1:5984", &["a/b", "x?y#z%", "_design", "p+q"]),
            "http://n1:5984/a%2Fb/x%3Fy%23z%25/_design/p%2Bq"
        );
        assert_eq!(peer_url("http://n1:5984", &[]), "http://n1:5984");
    }

    #[test]
    fn internal_requests_carry_the_marker_and_secret() {
        let client = reqwest::Client::new();
        let request = internal_request_raw(
            &client,
            "s3cret",
            reqwest::Method::GET,
            "http://n1",
            &["a/b", "doc"],
        )
        .build()
        .unwrap();
        assert_eq!(request.url().path(), "/a%2Fb/doc");
        assert_eq!(request.headers()["x-douchdb-forwarded"], "1");
        assert_eq!(request.headers()["x-douchdb-secret"], "s3cret");
    }

    #[test]
    fn forward_client_auth_copies_credentials_and_nothing_else() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic Ym9iOnB3"),
        );
        headers.insert(header::COOKIE, HeaderValue::from_static("AuthSession=abc"));
        headers.insert("x-douchdb-secret", HeaderValue::from_static("forged"));
        let request = forward_client_auth(reqwest::Client::new().get("http://peer/db"), &headers)
            .build()
            .unwrap();
        assert_eq!(request.headers()["authorization"], "Basic Ym9iOnB3");
        assert_eq!(request.headers()["cookie"], "AuthSession=abc");
        assert!(request.headers().get("x-douchdb-secret").is_none());

        let bare = forward_client_auth(
            reqwest::Client::new().get("http://peer/db"),
            &HeaderMap::new(),
        )
        .build()
        .unwrap();
        assert!(bare.headers().is_empty());
    }
}

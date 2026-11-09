//! HTTP surface. The shape of every response here is dictated by CouchDB's
//! wire protocol, not by taste.

use crate::auth::Principal;
use crate::cluster::{ShardMap, ShardMask};
use crate::cluster_http::{forward_client_auth, internal_request, peer_url, send_peer};
use crate::db::{
    attachments_unsupported, validate_doc, AllDocsOpts, ApiError, ChangesOpts, Db, DbResult,
    DocOpts,
};
use crate::dbname::{illegal_db_name, valid_db_name};
use crate::fabric::*;
pub(crate) use crate::params::{
    doc_ids_from, doc_opts, flag, json_body, json_or_string_param, json_param, lenient_json_param,
    num, resolve_changes_filter,
};
pub use crate::state::{AppState, Config, DbUpdates};
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::StreamExt;
pub use observability::{health_public, health_status, metrics_status};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// A request against one database, as `db_rest` hands it to a handler.
#[derive(Clone, Copy)]
pub(crate) struct DbRequest<'a> {
    pub st: &'a AppState,
    pub db: &'a Arc<Db>,
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

/// A streaming 200 response for a feed: not cached, one chunk per stream item.
pub(crate) fn feed_response(
    content_type: &'static str,
    stream: impl futures::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Paces the keep-alive lines of a feed: `due` is true once per interval, and
/// never when the client asked for no heartbeat.
pub(crate) struct Heartbeat {
    every: Option<std::time::Duration>,
    last: std::time::Instant,
}

impl Heartbeat {
    pub(crate) fn new(every: Option<std::time::Duration>) -> Self {
        Self {
            every,
            last: std::time::Instant::now(),
        }
    }

    /// Time until the next heartbeat is due; `None` when there is none.
    pub(crate) fn next_in(&self) -> Option<std::time::Duration> {
        self.every
            .map(|every| every.saturating_sub(self.last.elapsed()))
    }

    pub(crate) fn due(&mut self) -> bool {
        let due = self.every.is_some_and(|every| self.last.elapsed() >= every);
        if due {
            self.last = std::time::Instant::now();
        }
        due
    }
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

fn parse_shard_mask(raw: &str, q: u32) -> Option<ShardMask> {
    let mut mask = ShardMask::none(q);
    for part in raw.split(',').filter(|part| !part.is_empty()) {
        let shard: usize = part.parse().ok()?;
        if shard >= mask.len() {
            return None;
        }
        mask.select(shard);
    }
    Some(mask)
}

/// The `update_seq` a query reports with `update_seq=true`. In a cluster it is
/// the opaque vector `GET /db` reports, so it works as a `_changes` since;
/// otherwise it is this node's sequence, as `GET /db` reports it.
async fn query_update_seq(
    st: &AppState,
    db: &Arc<Db>,
    name: &str,
    headers: &HeaderMap,
) -> DbResult<Value> {
    if st.ctx.cluster.is_clustered() && !st.ctx.cluster.is_internal(headers) {
        let (seq, _) = cluster_seq_tokens(ClusterRead {
            ctx: &st.ctx,
            db,
            dbname: name,
            headers,
        })
        .await?;
        return Ok(json!(seq));
    }
    Ok(json!(db.update_seq()))
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

/// Like `key_param`, but a value that is not JSON is a 400 as in CouchDB
/// (`key=abc` must be written `key="abc"`).
pub(crate) fn json_key_param(q: &HashMap<String, String>, key: &str) -> DbResult<Option<String>> {
    let Some(raw) = q.get(key) else {
        return Ok(None);
    };
    match serde_json::from_str::<Value>(raw) {
        Ok(Value::String(s)) => Ok(Some(s)),
        Ok(Value::Number(n)) => Ok(Some(n.to_string())),
        Ok(_) => Ok(Some(raw.clone())),
        Err(_) => Err(ApiError::bad_request("invalid UTF-8 JSON")),
    }
}

/// CouchDB accepts `true` and `false` (in any case) for its boolean query
/// parameters and answers 400 `query_parse_error` for anything else.
pub(crate) fn validate_bool_params(q: &HashMap<String, String>, names: &[&str]) -> DbResult<()> {
    for name in names {
        if let Some(raw) = q.get(*name) {
            if !raw.eq_ignore_ascii_case("true") && !raw.eq_ignore_ascii_case("false") {
                return Err(ApiError::new(
                    400,
                    "query_parse_error",
                    format!("Invalid boolean parameter: \"{raw}\""),
                ));
            }
        }
    }
    Ok(())
}

/// A database name taken from the URL path. The segment is decoded the way
/// CouchDB (mochiweb) decodes every path segment, `+` as a space and then
/// `%XX` escapes, so `a+b` is the name "a b" and only `a%2Bb` is "a+b". axum's
/// `Path` decodes the escapes first and leaves `+` alone, which cannot tell the
/// two apart, so the segment is read from the raw URI path: the first one, or
/// the one after `/_node/_local/_dbs/` on the node endpoints.
pub(crate) struct DbName(pub String);

impl<S: Send + Sync> axum::extract::FromRequestParts<S> for DbName {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let mut segments = parts.uri.path().trim_start_matches('/').split('/');
        let raw = match segments.next() {
            Some("_node") => segments.nth(2),
            first => first,
        };
        raw.map(|segment| DbName(percent_decode(segment)))
            .ok_or_else(|| fail(ApiError::bad_request("missing database name")))
    }
}

mod access;
mod bulk;
mod changes;
mod database;
mod documents;
mod forward;
mod middleware;
mod node;
mod observability;
mod root;
mod session;
mod views;

pub(crate) use access::*;
use bulk::*;
use changes::*;
pub(crate) use database::*;
pub(crate) use documents::*;
pub(crate) use forward::*;
pub(crate) use middleware::*;
pub(crate) use node::*;
pub(crate) use root::*;
pub(crate) use session::*;
use views::*;

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
            prop_assert_eq!(mask, ShardMask::from_iter(selected));
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
}

//! HTTP-shaped halves of cluster reads: replaying a request against the peer
//! that owns a document, and `?r=N` quorum reads answered as responses.

use super::{doc_response, etag_matches, fail};
use crate::cluster_http::{forward_client_auth, peer_url, percent_encode, send_peer, ClusterCtx};
use crate::db::{ApiError, Db};
use crate::fabric::{reconcile_leaves, replica_docs};
use crate::params::flag;
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{json, Value};
use std::collections::HashMap;

/// A client request to replay against a peer that owns the document.
pub(crate) struct ForwardRequest<'a> {
    pub method: &'a Method,
    pub dbname: &'a str,
    /// The path after `/{dbname}/`.
    pub rest: &'a str,
    pub q: &'a HashMap<String, String>,
    pub headers: &'a HeaderMap,
    pub body: &'a [u8],
}

pub(crate) async fn proxy(ctx: &ClusterCtx, request: &ForwardRequest<'_>, peer: &str) -> Response {
    let ForwardRequest {
        method,
        dbname,
        rest,
        q,
        headers,
        body,
    } = *request;
    // `rest` is the caller's still-encoded remainder; only the name is encoded.
    let mut url = format!("{}/{rest}", peer_url(peer, &[dbname]));
    if !q.is_empty() {
        let encoded: Vec<String> = q
            .iter()
            .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
            .collect();
        url.push('?');
        url.push_str(&encoded.join("&"));
    }
    let request = ctx.client.request(
        reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap_or(reqwest::Method::GET),
        &url,
    );
    let request = forward_client_auth(request, headers);
    let mut request = match headers.get(header::CONTENT_TYPE) {
        Some(value) => request.header("content-type", value),
        None => request,
    };
    for name in ["destination", "if-match", "if-none-match"] {
        if let Some(value) = headers.get(name) {
            request = request.header(name, value);
        }
    }
    if !body.is_empty() {
        request = request.body(body.to_vec());
    }
    match send_peer(request).await {
        Ok(resp) => {
            let status =
                StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let content_type = resp
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/json")
                .to_string();
            let etag = resp.headers().get(header::ETAG).cloned();
            let location = resp.headers().get(header::LOCATION).cloned();
            let bytes = resp.bytes().await.unwrap_or_default().to_vec();
            let mut response =
                (status, [(header::CONTENT_TYPE, content_type)], bytes).into_response();
            if let Some(value) = etag {
                response.headers_mut().insert(header::ETAG, value);
            }
            if let Some(value) = location {
                response.headers_mut().insert(header::LOCATION, value);
            }
            response
        }
        Err(e) => fail(ApiError::new(502, "bad_gateway", e.to_string())),
    }
}

/// True when a proxied request failed to reach the peer, rather than the peer
/// answering on behalf of the cluster.
pub(crate) fn unavailable(response: &Response) -> bool {
    matches!(
        response.status(),
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
    )
}

/// `?r=N` reads from several replicas and returns the winning revision.
pub(crate) async fn quorum_read(
    ctx: &ClusterCtx,
    db: &Db,
    dbname: &str,
    docid: &str,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
    r: usize,
) -> Response {
    // Without an explicit `rev`, ask every replica for *all* leaves with their
    // revision histories. A conflict branch that never wins locally can still
    // be the correct global winner, so top-level winners alone are not enough.
    let requested_rev = q.get("rev").cloned();
    let open_all = requested_rev.is_none();
    let (reachable, docs) = replica_docs(ctx, db, dbname, docid, q, headers, open_all).await;
    let r = r.min(db.replicas_of_doc(docid).len()).max(1);
    if let Some(error) = read_quorum_failure(reachable, r, q) {
        return fail(error);
    }
    if docs.is_empty() {
        return fail(ApiError::not_found("missing"));
    }

    let Some(reconciled) = reconcile_leaves(&docs, requested_rev.as_deref()) else {
        return fail(ApiError::not_found("missing"));
    };
    let winner_rev = reconciled
        .winner
        .get("_rev")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let mut winner = reconciled.winner;
    let conflicts = reconciled.conflicts;
    let deleted_conflicts = reconciled.deleted_conflicts;
    let history = reconciled.history;

    if let Some(object) = winner.as_object_mut() {
        if flag(q, "conflicts") {
            object.insert("_conflicts".to_string(), json!(conflicts));
        } else {
            object.remove("_conflicts");
        }
        if flag(q, "deleted_conflicts") {
            object.insert("_deleted_conflicts".to_string(), json!(deleted_conflicts));
        } else {
            object.remove("_deleted_conflicts");
        }
        if flag(q, "revs") {
            // Return a deterministic history: the longest one supplied for
            // this revision, not whichever replica answered first.
            if let Some(start) = history
                .first()
                .and_then(|rev| crate::rev::Rev::parse(rev))
                .map(|rev| rev.pos)
            {
                let ids: Vec<String> = history
                    .iter()
                    .filter_map(|rev| rev.split_once('-').map(|(_, id)| id.to_string()))
                    .collect();
                object.insert(
                    "_revisions".to_string(),
                    json!({"start": start, "ids": ids}),
                );
            }
        } else {
            object.remove("_revisions");
        }
    }

    let explicitly_requested = requested_rev.is_some();
    let deleted = winner
        .get("_deleted")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if deleted && !explicitly_requested {
        return fail(ApiError::not_found("deleted"));
    }
    let etag = format!("\"{winner_rev}\"");
    if etag_matches(headers, header::IF_NONE_MATCH, &etag) {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        if let Ok(value) = etag.parse() {
            response.headers_mut().insert(header::ETAG, value);
        }
        return response;
    }
    doc_response(winner)
}

/// Count reachable replicas for revision-shaped responses (`open_revs`,
/// `revs_info`) without silently downgrading the requested `r`.
pub(crate) async fn quorum_reachable(
    ctx: &ClusterCtx,
    db: &Db,
    dbname: &str,
    docid: &str,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
    r: usize,
) -> Option<Response> {
    let (reachable, _) = replica_docs(ctx, db, dbname, docid, q, headers, false).await;
    let r = r.min(db.replicas_of_doc(docid).len()).max(1);
    read_quorum_failure(reachable, r, q).map(fail)
}

/// The error for a read that reached fewer than `r` replicas. When some
/// replica answered, CouchDB's `fabric_doc_open` would still serve the best
/// document it received; douchdb deliberately keeps the read strict (the
/// linearizability checks depend on it) and answers 503 `quorum_not_met`.
/// When none answered, the error is fabric's own.
fn read_quorum_failure(
    reachable: usize,
    r: usize,
    q: &HashMap<String, String>,
) -> Option<ApiError> {
    if reachable >= r {
        return None;
    }
    crate::telemetry::record_quorum_failure();
    Some(if reachable == 0 {
        no_replica_answered(reads_revisions(q))
    } else {
        ApiError::new(
            503,
            "quorum_not_met",
            format!("read quorum {r} not met: {reachable} replica(s) answered"),
        )
    })
}

/// True when chttpd serves the read through `fabric:open_revs`: an
/// `open_revs` query or an explicit `rev` (`chttpd_db:couch_doc_open`).
fn reads_revisions(q: &HashMap<String, String>) -> bool {
    q.contains_key("open_revs") || q.contains_key("rev")
}

/// The error for a read that no replica answered. A plain read or
/// `revs_info` goes through `fabric:open_doc`, which answers
/// `{error, read_failure}`; chttpd's `error_info` has no clause for it, so
/// `maybe_handle_error` renders 500 `error`/`read_failure`. A read of
/// specific revisions goes through `fabric:open_revs`, whose
/// `format_reply` answers `all_workers_died` when no worker replied, and
/// `error_info(all_workers_died)` is 503 `service unavailable`
/// (chttpd.erl 3.5.0).
fn no_replica_answered(open_revs: bool) -> ApiError {
    if open_revs {
        ApiError::new(
            503,
            "service unavailable",
            "Nodes are unable to service this request due to overloading or maintenance mode.",
        )
    } else {
        ApiError::new(500, "error", "read_failure")
    }
}

//! Cluster coordination: shard-map routing, replica selection, quorum
//! reads/writes and internal node calls.

use crate::api::{
    doc_ids_from, doc_opts, doc_response, etag_matches, fail, flag, forward_client_auth, num,
    percent_encode, AppState,
};
use crate::db::{AllDocsOpts, ApiError, ChangesOpts, Db, DbResult, DocOpts};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

// Replica-set selection is deliberately centralized here so the shard map is
// the single source of placement truth for routing, quorum and anti-entropy.
pub(crate) fn replicas_of_doc(db: &Db, docid: &str) -> Vec<usize> {
    db.replicas_of_doc(docid)
}

pub(crate) fn owner_of_shard(db: &Db, shard: u32) -> usize {
    db.owner_of_shard(shard)
}

/// Assign each shard to exactly one node for a fan-out read. Prefer the
/// coordinator's local replica when it holds one: for n=3 on three nodes every
/// node holds every shard, so the whole query can be answered locally instead
/// of issuing one HTTP request per owner. Shards without a local replica stay
/// with their primary owner and can still fall back to other replicas.
pub(crate) fn preferred_shard_masks(st: &AppState, db: &Db) -> Vec<Vec<bool>> {
    let node_count = st.cluster.nodes.len().max(1);
    let mut masks = vec![vec![false; db.q as usize]; node_count];
    let map = db.shard_map();
    for shard in 0..db.q {
        let replicas = map.replicas_of(shard);
        let target = if replicas.contains(&st.cluster.me) {
            st.cluster.me
        } else {
            replicas.first().copied().unwrap_or(0)
        };
        if let Some(mask) = masks.get_mut(target) {
            mask[shard as usize] = true;
        }
    }
    masks
}

pub(crate) fn is_replica_of(db: &Db, docid: &str, me: usize) -> bool {
    db.is_replica_of(docid, me)
}

/// Reconcile versioned shard-map metadata with peers. Newer maps are pulled;
/// older peers receive the local or pending map. This runs from anti-entropy,
/// so a metadata update that missed a peer is repaired after that peer returns.
pub(crate) async fn reconcile_shard_map(st: &AppState, db: &Arc<Db>) {
    if !st.cluster.is_clustered() {
        return;
    }
    let local = db.shard_map();
    let mut desired = db
        .pending_shard_map()
        .filter(|pending| pending.version >= local.version)
        .unwrap_or_else(|| local.clone());

    let maps = join_all(st.cluster.peers().map(|peer| {
        let url = format!("{peer}/_node/_local/_dbs/{}", db.name);
        async move {
            let request = st
                .client
                .get(url)
                .header("x-douchdb-forwarded", "1")
                .header("x-douchdb-secret", st.cluster.secret.as_str());
            let request = if let Some(auth) = &st.auth {
                request.basic_auth(&auth.user, Some(&auth.pass))
            } else {
                request
            };
            request
                .send()
                .await
                .ok()?
                .json::<crate::cluster::ShardMap>()
                .await
                .ok()
        }
    }))
    .await;

    for map in maps.into_iter().flatten() {
        if map.version > desired.version && db.set_shard_map(map.clone()).unwrap_or(false) {
            desired = map;
        }
    }

    let payload = match serde_json::to_vec(&desired) {
        Ok(payload) => payload,
        Err(_) => return,
    };
    let peers: Vec<String> = st
        .cluster
        .nodes
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != st.cluster.me)
        .map(|(_, peer)| peer.clone())
        .collect();
    let responses = join_all(peers.into_iter().map(|peer| {
        let payload = payload.clone();
        async move {
            let request = st
                .client
                .put(format!("{peer}/_node/_local/_dbs/{}", db.name))
                .header("x-douchdb-forwarded", "1")
                .header("x-douchdb-secret", st.cluster.secret.as_str())
                .header("content-type", "application/json")
                .body(payload);
            let request = if let Some(auth) = &st.auth {
                request.basic_auth(&auth.user, Some(&auth.pass))
            } else {
                request
            };
            request
                .send()
                .await
                .map(|response| response.status().is_success())
                .unwrap_or(false)
        }
    }))
    .await;
    let acknowledgements = 1 + responses.into_iter().filter(|ok| *ok).count();
    if acknowledgements > st.cluster.nodes.len().max(1) / 2 {
        db.clear_pending_shard_map();
    }
}

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

pub(crate) async fn proxy(st: &AppState, request: &ForwardRequest<'_>, peer: &str) -> Response {
    let ForwardRequest {
        method,
        dbname,
        rest,
        q,
        headers,
        body,
    } = *request;
    let mut url = format!("{peer}/{dbname}/{rest}");
    if !q.is_empty() {
        let encoded: Vec<String> = q
            .iter()
            .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
            .collect();
        url.push('?');
        url.push_str(&encoded.join("&"));
    }
    let request = st.client.request(
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
    st: &AppState,
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
    let (reachable, docs) = replica_docs(st, db, dbname, docid, q, headers, open_all).await;
    let r = r.min(replicas_of_doc(db, docid).len()).max(1);
    if reachable < r {
        crate::telemetry::record_quorum_failure();
        return fail(ApiError::new(
            503,
            "quorum_not_met",
            format!("read quorum {r} not met: {reachable} replica(s) answered"),
        ));
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
    st: &AppState,
    db: &Db,
    dbname: &str,
    docid: &str,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
    r: usize,
) -> Option<Response> {
    let (reachable, _) = replica_docs(st, db, dbname, docid, q, headers, false).await;
    let r = r.min(replicas_of_doc(db, docid).len()).max(1);
    if reachable < r {
        crate::telemetry::record_quorum_failure();
        Some(fail(ApiError::new(
            503,
            "quorum_not_met",
            format!("read quorum {r} not met: {reachable} replica(s) answered"),
        )))
    } else {
        None
    }
}

/// Ask every replica for a document. With `open_all`, each peer returns all
/// leaves (`open_revs=all`) and their histories; otherwise the single winner
/// (or the client's explicit `rev`) is returned. Explicit not-found answers
/// count as reachable, but a transport or server failure does not.
async fn replica_docs(
    st: &AppState,
    db: &Db,
    dbname: &str,
    docid: &str,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
    open_all: bool,
) -> (usize, Vec<Value>) {
    let mut inner = q.clone();
    inner.insert("r".to_string(), "1".to_string());
    inner.insert("revs".to_string(), "true".to_string());
    if open_all {
        inner.insert("open_revs".to_string(), "all".to_string());
        inner.remove("rev");
        inner.remove("revs_info");
    } else {
        inner.remove("open_revs");
    }

    let mut opts = doc_opts(&inner);
    opts.revs = true;
    opts.include_deleted = true;
    if open_all {
        opts.open_revs = Some(vec!["all".to_string()]);
    }

    let mut replicas = replicas_of_doc(db, docid);
    if let Some(index) = replicas.iter().position(|node| *node == st.cluster.me) {
        let local = replicas.remove(index);
        replicas.insert(0, local);
    }
    let responses = join_all(replicas.into_iter().map(|node| {
        let inner = inner.clone();
        let opts = opts.clone();
        async move {
            if node == st.cluster.me {
                crate::telemetry::record_local_read();
                return match db.get_doc(docid, &opts).await {
                    Ok(doc) => (true, Some(doc)),
                    Err(error) => (error.status == 404, None),
                };
            }
            crate::telemetry::record_remote_read();
            let peer = st.cluster.nodes[node].clone();
            let request = st
                .client
                .get(format!("{peer}/{dbname}/{}", percent_encode(docid)))
                .header("x-douchdb-forwarded", "1")
                .header("x-douchdb-secret", st.cluster.secret.as_str())
                .header("x-douchdb-quorum-read", "1")
                .query(&inner);
            let request = forward_client_auth(request, headers);
            match request.send().await {
                Ok(response) if response.status().is_success() => {
                    match response.json::<Value>().await {
                        Ok(value) => (true, Some(value)),
                        Err(_) => (false, None),
                    }
                }
                Ok(response) if response.status() == StatusCode::NOT_FOUND => (true, None),
                Ok(_) => (false, None),
                Err(_) => (false, None),
            }
        }
    }))
    .await;

    let reachable = responses.iter().filter(|(reachable, _)| *reachable).count();
    let mut docs = Vec::new();
    for (_, value) in responses {
        match value {
            Some(Value::Array(entries)) => {
                for entry in entries {
                    if let Some(ok) = entry.get("ok") {
                        docs.push(ok.clone());
                    }
                }
            }
            Some(value) => docs.push(value),
            None => {}
        }
    }
    (reachable, docs)
}

struct Reconciled {
    winner: Value,
    conflicts: Vec<String>,
    deleted_conflicts: Vec<String>,
    history: Vec<String>,
}

/// Pure merged-tree reconciliation. `docs` may contain duplicate revision ids
/// with different retained history lengths; ancestry is merged from every
/// response before bodies are deduplicated, and the longest history wins
/// deterministically. The result must not depend on input order.
fn reconcile_leaves(docs: &[Value], requested_rev: Option<&str>) -> Option<Reconciled> {
    let mut bodies: BTreeMap<String, Value> = BTreeMap::new();
    let mut histories: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut leaves: BTreeMap<String, bool> = BTreeMap::new();
    for doc in docs {
        let Some(rev) = doc.get("_rev").and_then(Value::as_str) else {
            continue;
        };
        let history = revision_history(doc);
        histories
            .entry(rev.to_string())
            .and_modify(|current| {
                if history.len() > current.len()
                    || (history.len() == current.len() && history > *current)
                {
                    *current = history.clone();
                }
            })
            .or_insert(history);
        leaves
            .entry(rev.to_string())
            .or_insert_with(|| doc.get("_deleted").and_then(Value::as_bool) == Some(true));
        bodies.entry(rev.to_string()).or_insert_with(|| doc.clone());
    }
    if bodies.is_empty() {
        return None;
    }

    let ancestor_revs: BTreeSet<String> = histories
        .values()
        .flat_map(|history| history.iter().skip(1).cloned())
        .collect();
    let winner_rev = match requested_rev {
        Some(rev) if leaves.contains_key(rev) => rev.to_string(),
        Some(_) => return None,
        None => leaves
            .iter()
            .filter(|(rev, _)| !ancestor_revs.contains(*rev))
            .max_by_key(|(rev, deleted)| {
                let parsed = crate::rev::Rev::parse(rev);
                let pos = parsed.as_ref().map(|rev| rev.pos).unwrap_or(0);
                let id = parsed.map(|rev| rev.id);
                (!**deleted, pos, id)
            })
            .map(|(rev, _)| rev.clone())?,
    };
    let winner = bodies.get(&winner_rev)?.clone();
    let mut conflicts = Vec::new();
    let mut deleted_conflicts = Vec::new();
    for (rev, deleted) in &leaves {
        if *rev == winner_rev || ancestor_revs.contains(rev) {
            continue;
        }
        if *deleted {
            deleted_conflicts.push(rev.clone());
        } else {
            conflicts.push(rev.clone());
        }
    }
    Some(Reconciled {
        winner,
        conflicts,
        deleted_conflicts,
        history: histories.get(&winner_rev).cloned().unwrap_or_default(),
    })
}

/// Revision history from a `revs=true` document response, newest first.
fn revision_history(doc: &Value) -> Vec<String> {
    let Some(revisions) = doc.get("_revisions") else {
        return Vec::new();
    };
    let Some(start) = revisions.get("start").and_then(Value::as_u64) else {
        return Vec::new();
    };
    let Some(ids) = revisions.get("ids").and_then(Value::as_array) else {
        return Vec::new();
    };
    ids.iter()
        .enumerate()
        .filter_map(|(index, id)| {
            id.as_str()
                .map(|id| format!("{}-{id}", start.saturating_sub(index as u64)))
        })
        .collect()
}

#[derive(serde::Serialize)]
struct BulkBody<'a, T: serde::Serialize> {
    docs: &'a [T],
    new_edits: bool,
}

/// `new_edits=true` writes to each document's primary (the coordinating
/// replica), then copies the resulting full revision to the other replicas with
/// `new_edits=false`, so every replica ends with byte-identical history.
/// `new_edits=false` (the replication path) sends the given revision to every
/// replica directly. The bool is whether the default write quorum was met.
pub(crate) async fn commit_docs(
    st: &AppState,
    db: &Db,
    dbname: &str,
    mut docs: Vec<Value>,
    new_edits: bool,
    w: usize,
    headers: &HeaderMap,
) -> DbResult<(Vec<Value>, bool)> {
    if !st.cluster.is_clustered() {
        let results = db.put_docs(docs, new_edits).await?;
        if results.iter().any(|result| result.get("ok").is_some()) {
            st.db_updates.record(dbname, "updated");
        }
        return Ok((results, true));
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
    let (primaries, replicas): (Vec<usize>, Vec<Vec<usize>>) =
        ids.iter().map(|id| db.placement_of_doc(id)).unzip();
    let replication = db.copies();
    // CouchDB defaults to a write quorum and lets `?w=` override it.
    let quorum = if w == 0 {
        replication / 2 + 1
    } else {
        w.min(replication)
    };
    let total = docs.len();
    let docs: Vec<Arc<Value>> = docs.into_iter().map(Arc::new).collect();
    let mut out = vec![Value::Null; total];
    let mut acks = vec![0usize; total];

    if new_edits {
        // CouchDB-style: send the original document to every replica in its
        // shard set and wait for `w` acknowledgements. Revision ids are
        // deterministic, so no primary re-fetch/copy pass is needed.
        let db_handle = st.db(dbname).await?;
        let mut groups: HashMap<usize, Vec<(usize, Arc<Value>)>> = HashMap::new();
        for (slot, doc) in docs.into_iter().enumerate() {
            for node in &replicas[slot] {
                groups.entry(*node).or_default().push((slot, doc.clone()));
            }
        }

        crate::telemetry::record_fanout(groups.len() as u64);
        let mut successes: Vec<Option<Value>> = vec![None; total];
        let mut failures: Vec<Option<Value>> = vec![None; total];
        // Design documents go to every node, and any node may coordinate a
        // query against them, so their write waits for every replica to answer
        // rather than only the quorum.
        let mut unanswered: Vec<usize> = (0..total)
            .map(|slot| {
                if ids[slot].starts_with("_design/") {
                    replicas[slot].len()
                } else {
                    0
                }
            })
            .collect();
        let mut pending = FuturesUnordered::new();
        for (node, group) in groups {
            let slots: Vec<usize> = group.iter().map(|(slot, _)| *slot).collect();
            if node == st.cluster.me {
                let subdocs: Vec<Value> =
                    group.into_iter().map(|(_, doc)| (*doc).clone()).collect();
                let db = db_handle.clone();
                pending.push(tokio::spawn(async move {
                    (node, slots, db.put_docs(subdocs, true).await)
                }));
            } else {
                let subdocs: Vec<Arc<Value>> = group.into_iter().map(|(_, doc)| doc).collect();
                let client = st.client.clone();
                let secret = st.cluster.secret.clone();
                let peer = st.cluster.nodes[node].clone();
                let dbname = dbname.to_string();
                let headers = headers.clone();
                pending.push(tokio::spawn(async move {
                    (
                        node,
                        slots,
                        forward_bulk_owned(client, secret, peer, dbname, subdocs, true, headers)
                            .await,
                    )
                }));
            }
        }

        // Return as soon as every document has `w` acknowledgements. The
        // remaining replica writes keep running as detached tasks, so a slow
        // or unreachable replica no longer sets write tail latency.
        while let Some(joined) = pending.next().await {
            let Ok((node, slots, response)) = joined else {
                continue;
            };
            for slot in &slots {
                unanswered[*slot] = unanswered[*slot].saturating_sub(1);
            }
            match response {
                Ok(results) => {
                    for (slot, result) in slots.into_iter().zip(results) {
                        if result.get("error").is_none() {
                            acks[slot] += 1;
                            if successes[slot].is_none() || node == primaries[slot] {
                                successes[slot] = Some(result);
                            }
                        } else if failures[slot].is_none() || node == primaries[slot] {
                            failures[slot] = Some(result);
                        }
                    }
                }
                Err(_) => {
                    for slot in slots {
                        if failures[slot].is_none() {
                            failures[slot] = Some(json!({
                                "id": ids[slot],
                                "error": "replica_unavailable",
                                "reason": "no replica in the shard set is reachable",
                            }));
                        }
                    }
                }
            }
            if acks.iter().all(|ack| *ack >= quorum) && unanswered.iter().all(|left| *left == 0) {
                break;
            }
        }

        for slot in 0..total {
            out[slot] = successes[slot]
                .take()
                .or_else(|| failures[slot].take())
                .unwrap_or_else(|| {
                    json!({
                        "id": ids[slot],
                        "error": "replica_unavailable",
                        "reason": "no replica in the shard set is reachable",
                    })
                });
        }
        let met = acks.iter().all(|ack| *ack >= quorum);
        if !met {
            crate::telemetry::record_quorum_failure();
        }
        if out.iter().any(|result| result.get("ok").is_some()) {
            st.db_updates.record(dbname, "updated");
        }
        Ok((out, met))
    } else {
        // Replication writes carry their own history and must reach every
        // replica, including ones that are not the primary. A down replica is
        // not fatal: anti-entropy will heal it when it returns.
        let mut groups: HashMap<usize, Vec<(usize, Arc<Value>)>> = HashMap::new();
        for (slot, doc) in docs.into_iter().enumerate() {
            for node in &replicas[slot] {
                groups.entry(*node).or_default().push((slot, doc.clone()));
            }
        }
        crate::telemetry::record_fanout(groups.len() as u64);
        let mut reached = vec![false; total];
        let mut responses = Vec::new();
        if let Some(group) = groups.remove(&st.cluster.me) {
            let slots: Vec<usize> = group.iter().map(|(slot, _)| *slot).collect();
            let subdocs: Vec<Value> = group.into_iter().map(|(_, doc)| (*doc).clone()).collect();
            responses.push((st.cluster.me, slots, db.put_docs(subdocs, false).await));
        }
        let remote = futures::future::join_all(groups.into_iter().map(|(node, group)| {
            let slots: Vec<usize> = group.iter().map(|(slot, _)| *slot).collect();
            let subdocs: Vec<Arc<Value>> = group.into_iter().map(|(_, doc)| doc).collect();
            let client = st.client.clone();
            let secret = st.cluster.secret.clone();
            let peer = st.cluster.nodes[node].clone();
            let dbname = dbname.to_string();
            let headers = headers.clone();
            async move {
                (
                    node,
                    slots,
                    forward_bulk_owned(client, secret, peer, dbname, subdocs, false, headers).await,
                )
            }
        }))
        .await;
        responses.extend(remote);
        for (node, slots, response) in responses {
            match response {
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
                    for slot in slots {
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
        if !met {
            crate::telemetry::record_quorum_failure();
        }
        if out.iter().any(|result| result.get("ok").is_some()) {
            st.db_updates.record(dbname, "updated");
        }
        Ok((out, met))
    }
}

async fn forward_bulk_owned<T>(
    client: reqwest::Client,
    secret: String,
    peer: String,
    dbname: String,
    docs: Vec<T>,
    new_edits: bool,
    headers: HeaderMap,
) -> DbResult<Vec<Value>>
where
    T: serde::Serialize,
{
    let request = client
        .post(format!("{peer}/{dbname}/_bulk_docs"))
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", secret)
        .json(&BulkBody {
            docs: &docs,
            new_edits,
        });
    let request = forward_client_auth(request, &headers);
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

pub(crate) async fn forward_json(
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
    let request = st
        .client
        .post(url)
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", st.cluster.secret.as_str())
        .json(body);
    let request = forward_client_auth(request, headers);
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

pub(crate) fn replicas_of_shard(db: &Db, shard: u32) -> Vec<usize> {
    db.replicas_of_shard(shard)
}

fn fallback_shard_masks(
    db: &Db,
    owned_by_node: &[Vec<bool>],
    failed: &[usize],
    alive: &[usize],
) -> DbResult<Vec<(usize, Vec<bool>)>> {
    let mut groups: HashMap<usize, Vec<bool>> = HashMap::new();
    for &node in failed {
        for shard in 0..db.q {
            if !owned_by_node
                .get(node)
                .and_then(|owned| owned.get(shard as usize))
                .copied()
                .unwrap_or(false)
            {
                continue;
            }
            let target = replicas_of_shard(db, shard)
                .into_iter()
                .find(|candidate| alive.contains(candidate))
                .ok_or_else(|| {
                    ApiError::new(502, "no_replica", "no live replica for failed shard")
                })?;
            groups
                .entry(target)
                .or_insert_with(|| vec![false; db.q as usize])[shard as usize] = true;
        }
    }
    Ok(groups.into_iter().collect())
}

/// What every cluster read needs: the database to read, and the client's
/// headers to forward to peers.
#[derive(Clone, Copy)]
pub(crate) struct ClusterRead<'a> {
    pub st: &'a AppState,
    pub db: &'a Db,
    pub dbname: &'a str,
    pub headers: &'a HeaderMap,
}

/// A view or search index: a design document and the name inside it.
#[derive(Clone, Copy)]
pub(crate) struct IndexRef<'a> {
    pub ddoc: &'a str,
    pub name: &'a str,
}

/// The `_changes` options sent unchanged to every node.
#[derive(Clone, Copy)]
pub(crate) struct ChangesFetch<'a> {
    pub include_docs: bool,
    pub style_all_docs: bool,
    pub doc_ids: Option<&'a [String]>,
    pub filter: Option<&'a crate::db::ChangeFilter>,
    pub q: &'a HashMap<String, String>,
}

/// Which round of a [`scatter`] a request belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Round {
    /// The node's own preferred shards.
    Owner,
    /// Shards reassigned from a node that failed in the owner round.
    Fallback,
}

/// Fan a read out to every node with at least one shard in `owned_by_node`,
/// then send the shards of each node that failed to a live replica. Returns
/// `(node, round, value)` for every request, owner results first. A failed
/// fallback request fails the read; `what` names the read in the 502 returned
/// when no request succeeds.
async fn scatter<T, F, Fut>(
    read: ClusterRead<'_>,
    owned_by_node: &[Vec<bool>],
    what: &str,
    fetch: F,
) -> DbResult<Vec<(usize, Round, T)>>
where
    F: Fn(usize, Vec<bool>, Round) -> Fut,
    Fut: std::future::Future<Output = DbResult<T>>,
{
    let ClusterRead { st, db, .. } = read;
    let requests: Vec<(usize, Vec<bool>)> = owned_by_node
        .iter()
        .cloned()
        .enumerate()
        .filter(|(_, mask)| mask.iter().any(|owned| *owned))
        .collect();
    crate::telemetry::record_fanout(requests.len() as u64);
    let responses = join_all(requests.into_iter().map(|(node, mask)| {
        let response = fetch(node, mask, Round::Owner);
        async move { (node, response.await) }
    }))
    .await;

    let mut values = Vec::new();
    let mut failed = Vec::new();
    for (node, response) in responses {
        match response {
            Ok(value) => values.push((node, Round::Owner, value)),
            Err(error) => {
                tracing::warn!(
                    "{what} read from node {node} failed: {} {}",
                    error.status,
                    error.reason
                );
                failed.push(node);
            }
        }
    }
    if !failed.is_empty() {
        crate::telemetry::record_degraded_read();
        let alive: Vec<usize> = (0..st.cluster.nodes.len())
            .filter(|node| !failed.contains(node))
            .collect();
        let groups = fallback_shard_masks(db, owned_by_node, &failed, &alive)?;
        let fallbacks = join_all(groups.into_iter().map(|(node, mask)| {
            let response = fetch(node, mask, Round::Fallback);
            async move { (node, response.await) }
        }))
        .await;
        for (node, response) in fallbacks {
            values.push((node, Round::Fallback, response?));
        }
    }
    if values.is_empty() {
        return Err(ApiError::new(
            502,
            "bad_gateway",
            format!("no {what} replica is reachable"),
        ));
    }
    Ok(values)
}

pub(crate) async fn cluster_all_docs(read: ClusterRead<'_>, opts: &AllDocsOpts) -> DbResult<Value> {
    let ClusterRead { st, db, .. } = read;
    let owned_by_node = preferred_shard_masks(st, db);

    if let Some(keys) = &opts.keys {
        let mut key_opts = opts.clone();
        key_opts.skip = 0;
        key_opts.limit = usize::MAX;
        key_opts.startkey = None;
        key_opts.endkey = None;
        key_opts.descending = false;

        let key_opts = &key_opts;
        let values = scatter(
            read,
            &owned_by_node,
            "all_docs",
            |node, mask, _| async move { fetch_all_docs(read, node, key_opts, Some(&mask)).await },
        )
        .await?;

        let mut total_rows = 0u64;
        let mut found: HashMap<String, Value> = HashMap::new();
        let mut errors: HashMap<String, Value> = HashMap::new();
        for (_, _, response) in values {
            total_rows += response["total_rows"].as_u64().unwrap_or(0);
            for row in response["rows"].as_array().cloned().unwrap_or_default() {
                match row.get("id").and_then(Value::as_str) {
                    Some(id) => {
                        found.insert(id.to_string(), row);
                    }
                    None => {
                        if let Some(key) = row.get("key").and_then(Value::as_str) {
                            errors.entry(key.to_string()).or_insert(row);
                        }
                    }
                }
            }
        }

        let ordered_keys: Vec<&String> = if opts.descending {
            keys.iter().rev().collect()
        } else {
            keys.iter().collect()
        };
        let mut rows = Vec::new();
        for key in ordered_keys.into_iter().skip(opts.skip).take(opts.limit) {
            if let Some(row) = found.get(key).cloned() {
                rows.push(row);
            } else if let Some(row) = errors.get(key).cloned() {
                rows.push(row);
            } else {
                rows.push(json!({"key": key, "error": "not_found"}));
            }
        }
        return Ok(json!({"total_rows": total_rows, "offset": Value::Null, "rows": rows}));
    }

    let mut window = opts.clone();
    window.keys = None;
    let take = opts.skip.saturating_add(opts.limit);
    window.skip = 0;
    window.limit = take;

    let window = &window;
    let values = scatter(
        read,
        &owned_by_node,
        "all_docs",
        |node, mask, _| async move { fetch_all_docs(read, node, window, Some(&mask)).await },
    )
    .await?;

    let mut total_rows = 0u64;
    let mut before_start = 0usize;
    let mut rows: Vec<(String, Value)> = Vec::new();
    for (_, _, response) in values {
        total_rows += response["total_rows"].as_u64().unwrap_or(0);
        before_start += response["offset"].as_u64().unwrap_or(0) as usize;
        for row in response["rows"].as_array().cloned().unwrap_or_default() {
            if let Some(id) = row.get("id").and_then(Value::as_str) {
                rows.push((id.to_string(), row));
            }
        }
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
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

pub(crate) async fn fetch_all_docs(
    read: ClusterRead<'_>,
    node: usize,
    opts: &AllDocsOpts,
    shards: Option<&[bool]>,
) -> DbResult<Value> {
    let ClusterRead {
        st,
        db,
        dbname,
        headers,
    } = read;
    if node == st.cluster.me {
        if let Some(shards) = shards {
            return db.all_docs_owned(opts, shards).await;
        }
        return db.all_docs(opts).await;
    }
    let peer = &st.cluster.nodes[node];
    let mut query = Vec::new();
    if let Some(shards) = shards {
        let selected: Vec<String> = shards
            .iter()
            .enumerate()
            .filter(|(_, owned)| **owned)
            .map(|(shard, _)| shard.to_string())
            .collect();
        query.push(format!("shards={}", selected.join(",")));
    }
    if let Some(partition) = &opts.partition {
        query.push(format!("partition={}", percent_encode(partition)));
    }
    if opts.include_docs {
        query.push("include_docs=true".to_string());
    }
    if opts.conflicts {
        query.push("conflicts=true".to_string());
    }
    if opts.attachments {
        query.push("attachments=true".to_string());
    }
    if opts.descending {
        query.push("descending=true".to_string());
    }
    if !opts.inclusive_end {
        query.push("inclusive_end=false".to_string());
    }
    if opts.skip > 0 {
        query.push(format!("skip={}", opts.skip));
    }
    if opts.limit != usize::MAX {
        query.push(format!("limit={}", opts.limit));
    }
    if let Some(start) = &opts.startkey {
        let encoded = serde_json::to_string(start).unwrap_or_default();
        query.push(format!("startkey={}", percent_encode(&encoded)));
    }
    if let Some(end) = &opts.endkey {
        let encoded = serde_json::to_string(end).unwrap_or_default();
        query.push(format!("endkey={}", percent_encode(&encoded)));
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
    let request = st
        .client
        .post(url)
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", st.cluster.secret.as_str())
        .json(&body);
    let request = forward_client_auth(request, headers);
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

/// One owner's part of a cluster view query.
#[derive(Default)]
pub(crate) struct ViewBatch {
    rows: Arc<Vec<crate::view::Row>>,
    docs: HashMap<String, Value>,
    total_rows: usize,
    offset: usize,
}

/// Merged map rows for a cluster view query, with the counts CouchDB reports.
pub(crate) struct ClusterViewRows {
    pub spec: crate::view::ViewSpec,
    pub rows: Arc<Vec<crate::view::Row>>,
    pub docs: HashMap<String, Value>,
    pub total_rows: usize,
    pub offset: usize,
}

async fn fetch_view_mask(
    read: ClusterRead<'_>,
    view: IndexRef<'_>,
    query: &crate::view::ViewQuery,
    spec: &crate::view::ViewSpec,
    limit: Option<usize>,
    node: usize,
    mask: &[bool],
) -> DbResult<ViewBatch> {
    let ClusterRead {
        st,
        db,
        dbname,
        headers,
    } = read;
    let IndexRef {
        ddoc,
        name: view_name,
    } = view;
    if !mask.iter().any(|owned| *owned) {
        return Ok(ViewBatch::default());
    }
    // Owners apply the key range and return at most `limit` rows from their
    // shards; the coordinator merges and pages the union.
    let mut local = query.clone();
    local.skip = 0;
    local.limit = limit.unwrap_or(usize::MAX);
    local.owner_shards = Some(mask.to_vec());
    if node == st.cluster.me {
        let selection = crate::view::local_select(db, ddoc, view_name, spec, &local, false).await?;
        let docs = if query.include_docs || query.attachments {
            let ids = selection
                .rows
                .iter()
                .map(|row| row.id.clone())
                .collect::<Vec<_>>();
            crate::view::docs_by_ids(db, query.attachments, &ids).await?
        } else {
            HashMap::new()
        };
        return Ok(ViewBatch {
            rows: Arc::new(selection.rows),
            docs,
            total_rows: selection.total_rows,
            offset: selection.offset,
        });
    }

    let peer = &st.cluster.nodes[node];
    let url = format!(
        "{peer}/{dbname}/{}/_view/{}",
        percent_encode(ddoc),
        percent_encode(view_name)
    );
    let selected: Vec<String> = mask
        .iter()
        .enumerate()
        .filter(|(_, owned)| **owned)
        .map(|(shard, _)| shard.to_string())
        .collect();
    let mut params = vec![
        ("reduce", "false".to_string()),
        ("shards", selected.join(",")),
        (
            "include_docs",
            if query.include_docs { "true" } else { "false" }.to_string(),
        ),
        (
            "attachments",
            if query.attachments { "true" } else { "false" }.to_string(),
        ),
    ];
    push_range_params(&mut params, query);
    if let Some(limit) = limit.filter(|limit| *limit != usize::MAX) {
        params.push(("limit", limit.to_string()));
    }
    let mut request = st
        .client
        .get(url)
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", st.cluster.secret.as_str())
        .query(&params);
    request = forward_client_auth(request, headers);
    let response = request
        .send()
        .await
        .map_err(|error| ApiError::new(502, "bad_gateway", error.to_string()))?;
    if !response.status().is_success() {
        return Err(ApiError::new(
            response.status().as_u16(),
            "view_failed",
            response.text().await.unwrap_or_default(),
        ));
    }
    let value = response
        .json::<Value>()
        .await
        .map_err(|error| ApiError::new(502, "bad_gateway", error.to_string()))?;
    Ok(decode_view_batch(value))
}

/// The key-range parameters an owner applies to its own shards.
fn push_range_params(params: &mut Vec<(&'static str, String)>, query: &crate::view::ViewQuery) {
    if query.descending {
        params.push(("descending", "true".to_string()));
    }
    let encode = |value: &Value| serde_json::to_string(value).unwrap_or_default();
    if let Some(key) = &query.key {
        params.push(("key", encode(key)));
    }
    if let Some(keys) = &query.keys {
        // An owner only needs each key once: the coordinator restores the
        // requested order and repetitions when it renders the merged result.
        params.push((
            "keys",
            encode(&Value::Array(crate::view::unique_keys(keys))),
        ));
    }
    if let Some(start) = &query.startkey {
        params.push(("startkey", encode(start)));
    }
    if let Some(end) = &query.endkey {
        params.push(("endkey", encode(end)));
    }
    if let Some(id) = &query.startkey_docid {
        params.push(("startkey_docid", id.clone()));
    }
    if let Some(id) = &query.endkey_docid {
        params.push(("endkey_docid", id.clone()));
    }
    if !query.inclusive_end {
        params.push(("inclusive_end", "false".to_string()));
    }
    if let Some(partition) = &query.partition {
        params.push(("partition", partition.clone()));
    }
}

pub(crate) async fn cluster_view_rows(
    read: ClusterRead<'_>,
    view: IndexRef<'_>,
    query: &crate::view::ViewQuery,
) -> DbResult<ClusterViewRows> {
    let ClusterRead {
        st,
        db,
        dbname,
        headers,
    } = read;
    let IndexRef {
        ddoc,
        name: view_name,
    } = view;
    let ddoc_doc = match db.get_doc(ddoc, &DocOpts::default()).await {
        Ok(doc) => doc,
        Err(error) if error.status == 404 => {
            fetch_ddoc_from_replica(st, db, dbname, ddoc, headers).await?
        }
        Err(error) => return Err(error),
    };
    let spec = crate::view::spec_from_doc(&ddoc_doc, view_name)?;
    crate::view::validate_view_partition(db, &spec, query.partition.as_deref())?;
    let reduce = query.reduce.unwrap_or(spec.reduce.is_some());
    // Owners apply the key range themselves. A reduce needs every row in it;
    // a map query takes only the global window from each owner because the
    // owner sets are disjoint.
    let can_limit = !reduce;
    let take = query.skip.saturating_add(query.limit);

    let owned_by_node = preferred_shard_masks(st, db);
    let limit = can_limit.then_some(take);

    let spec_ref = &spec;
    let values = scatter(read, &owned_by_node, "view", |node, mask, _| async move {
        fetch_view_mask(read, view, query, spec_ref, limit, node, &mask).await
    })
    .await?;

    let mut counts: HashMap<String, (usize, crate::view::Row)> = HashMap::new();
    let mut docs: HashMap<String, Value> = HashMap::new();
    let mut total_rows = 0usize;
    let mut offset = 0usize;
    for (_, _, batch) in values {
        let ViewBatch {
            rows,
            docs: node_docs,
            total_rows: node_total,
            offset: node_offset,
        } = batch;
        total_rows += node_total;
        offset += node_offset;
        docs.extend(node_docs);
        let mut local: HashMap<String, (usize, crate::view::Row)> = HashMap::new();
        for row in rows.iter() {
            let key = row_key(row);
            local
                .entry(key)
                .and_modify(|(count, _)| *count += 1)
                .or_insert((1, row.clone()));
        }
        for (key, (count, row)) in local {
            match counts.get_mut(&key) {
                Some((existing, _)) => *existing = (*existing).max(count),
                None => {
                    counts.insert(key, (count, row));
                }
            }
        }
    }

    let mut rows = Vec::new();
    for (_, (count, row)) in counts {
        for _ in 0..count {
            rows.push(row.clone());
        }
    }
    crate::view::sort_rows(&mut rows);
    Ok(ClusterViewRows {
        spec,
        rows: Arc::new(rows),
        docs,
        total_rows,
        offset,
    })
}

/// The reduce each owner runs on its shards: the requested key range without
/// paging, which applies to the merged groups. For `keys`, owners return one
/// partial per distinct exact key, so the coordinator can rebuild the
/// requested order, repeats and group levels.
fn owner_reduce_query(query: &crate::view::ViewQuery, mask: &[bool]) -> crate::view::ViewQuery {
    let mut owner = query.clone();
    owner.skip = 0;
    owner.limit = usize::MAX;
    owner.reduce = Some(true);
    owner.owner_shards = Some(mask.to_vec());
    if let Some(keys) = &query.keys {
        owner.keys = Some(crate::view::unique_keys(keys));
        owner.group = true;
        owner.group_level = 0;
        owner.descending = false;
    }
    owner
}

async fn fetch_reduce_mask(
    read: ClusterRead<'_>,
    view: IndexRef<'_>,
    query: &crate::view::ViewQuery,
    node: usize,
    mask: &[bool],
) -> DbResult<Value> {
    let ClusterRead {
        st,
        db,
        dbname,
        headers,
    } = read;
    let IndexRef {
        ddoc,
        name: view_name,
    } = view;
    let owner = owner_reduce_query(query, mask);
    if node == st.cluster.me {
        return crate::view::query(db, ddoc, view_name, &owner).await;
    }

    let peer = &st.cluster.nodes[node];
    let url = format!(
        "{peer}/{dbname}/{}/_view/{}",
        percent_encode(ddoc),
        percent_encode(view_name)
    );
    let selected: Vec<String> = mask
        .iter()
        .enumerate()
        .filter(|(_, owned)| **owned)
        .map(|(shard, _)| shard.to_string())
        .collect();
    let mut params = vec![
        ("reduce", "true".to_string()),
        ("shards", selected.join(",")),
    ];
    if owner.group {
        params.push(("group", "true".to_string()));
    }
    if owner.group_level > 0 {
        params.push(("group_level", owner.group_level.to_string()));
    }
    push_range_params(&mut params, &owner);
    let request = st
        .client
        .get(url)
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", st.cluster.secret.as_str())
        .query(&params);
    let request = forward_client_auth(request, headers);
    let response = request
        .send()
        .await
        .map_err(|error| ApiError::new(502, "bad_gateway", error.to_string()))?;
    if !response.status().is_success() {
        return Err(ApiError::new(
            response.status().as_u16(),
            "view_failed",
            response.text().await.unwrap_or_default(),
        ));
    }
    response
        .json::<Value>()
        .await
        .map_err(|error| ApiError::new(502, "bad_gateway", error.to_string()))
}

/// Reduce on every owner and combine the partials, so only one value per
/// group crosses the network.
pub(crate) async fn cluster_reduce(
    read: ClusterRead<'_>,
    view: IndexRef<'_>,
    query: &crate::view::ViewQuery,
    reducer: &str,
) -> DbResult<Value> {
    let ClusterRead { st, db, .. } = read;
    let IndexRef {
        ddoc,
        name: view_name,
    } = view;
    let ddoc_doc = db.get_doc(ddoc, &DocOpts::default()).await?;
    let spec = crate::view::spec_from_doc(&ddoc_doc, view_name)?;
    crate::view::validate_view_partition(db, &spec, query.partition.as_deref())?;
    crate::view::check_query(&spec, query)?;
    let owned_by_node = preferred_shard_masks(st, db);
    let values = scatter(read, &owned_by_node, "view", |node, mask, _| async move {
        fetch_reduce_mask(read, view, query, node, &mask).await
    })
    .await?;

    let mut partials = crate::view::Partials::new();
    for (_, _, value) in values {
        for row in value["rows"].as_array().cloned().unwrap_or_default() {
            let key = row.get("key").cloned().unwrap_or(Value::Null);
            let value = row.get("value").cloned().unwrap_or(Value::Null);
            crate::view::add_partial(&mut partials, key, value);
        }
    }
    let mut js = crate::view::JsReducer::new(reducer, query.limits);
    crate::view::assemble_reduce(partials, query, &mut js).map_err(|reason| {
        ApiError::new(
            500,
            "view_error",
            format!("reduce function failed: {reason}"),
        )
    })
}

pub(crate) fn decode_view_batch(value: Value) -> ViewBatch {
    let total_rows = value["total_rows"].as_u64().unwrap_or(0) as usize;
    let offset = value["offset"].as_u64().unwrap_or(0) as usize;
    let mut rows = Vec::new();
    let mut docs = HashMap::new();
    for row in value["rows"].as_array().cloned().unwrap_or_default() {
        let Some(id) = row.get("id").and_then(Value::as_str).map(str::to_string) else {
            continue;
        };
        if let Some(doc) = row.get("doc").cloned() {
            docs.insert(id.clone(), doc);
        }
        rows.push(crate::view::Row {
            id,
            key: row.get("key").cloned().unwrap_or(Value::Null),
            value: row.get("value").cloned().unwrap_or(Value::Null),
        });
    }
    ViewBatch {
        rows: Arc::new(rows),
        docs,
        total_rows,
        offset,
    }
}

pub(crate) fn row_key(row: &crate::view::Row) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}",
        row.id,
        serde_json::to_string(&row.key).unwrap_or_default(),
        serde_json::to_string(&row.value).unwrap_or_default()
    )
}

pub(crate) async fn fetch_ddoc_from_replica(
    st: &AppState,
    db: &Db,
    dbname: &str,
    ddoc: &str,
    headers: &HeaderMap,
) -> DbResult<Value> {
    for node in replicas_of_doc(db, ddoc) {
        if node == st.cluster.me {
            continue;
        }
        let peer = st.cluster.nodes[node].clone();
        let mut request = st
            .client
            .get(format!("{peer}/{dbname}/{}", percent_encode(ddoc)));
        request = forward_client_auth(request, headers);
        if let Ok(response) = request.send().await {
            if response.status().is_success() {
                if let Ok(value) = response.json::<Value>().await {
                    return Ok(value);
                }
            }
        }
    }
    Err(ApiError::not_found(format!("missing {ddoc}")))
}

fn last_seq_for_node(value: &Value, node: usize, nodes: usize) -> Option<u64> {
    let seq = value.get("last_seq")?;
    if let Some(seq) = seq.as_u64() {
        return Some(seq);
    }
    let decoded = decode_since(seq.as_str()?, nodes)?;
    decoded.get(node).copied()
}

pub(crate) async fn cluster_changes(
    read: ClusterRead<'_>,
    q: &HashMap<String, String>,
    body: &[u8],
) -> DbResult<Value> {
    let ClusterRead { st, db, .. } = read;
    let n = st.cluster.nodes.len();
    let now = q.get("since").map(String::as_str) == Some("now");
    let mut since = if now {
        cluster_update_vector(read).await?
    } else if let Some(raw) = q.get("since").map(String::as_str) {
        match decode_since(raw, n) {
            Some(mut vector) => {
                vector.resize(n.max(vector.len()), 0);
                vector
            }
            None => vec![since_component(Some(raw), st.cluster.me, n); n],
        }
    } else {
        vec![0; n]
    };
    since.resize(n, 0);
    let owned_by_node = preferred_shard_masks(st, db);

    let doc_ids = doc_ids_from(q, body);
    let change_filter = crate::api::resolve_changes_filter(db, q).await?;
    let changes = ChangesFetch {
        include_docs: flag(q, "include_docs"),
        style_all_docs: q.get("style").map(String::as_str) == Some("all_docs"),
        doc_ids: doc_ids.as_deref(),
        filter: change_filter.as_ref(),
        q,
    };
    let limit = num(q, "limit");

    if let Some(limit) = limit {
        if limit == 0 {
            return Ok(json!({
                "results": [],
                "last_seq": encode_since(&since),
                "pending": 0,
            }));
        }

        let node_limit = if change_filter.is_some() {
            None
        } else {
            Some(limit)
        };
        let since_ref = &since;
        let responses = scatter(
            read,
            &owned_by_node,
            "changes",
            |node, mask, round| async move {
                // The failed owner's sequence is not meaningful for the survivor's
                // reassigned shards, so a fallback starts at 0; preferred masks are
                // disjoint, so ids do not overlap.
                let since = match round {
                    Round::Owner => since_ref[node],
                    Round::Fallback => 0,
                };
                fetch_node_changes(read, changes, node, &mask, since, node_limit).await
            },
        )
        .await?;
        let mut node_rows: Vec<std::collections::VecDeque<Value>> =
            (0..n).map(|_| std::collections::VecDeque::new()).collect();
        let mut node_last: Vec<Option<u64>> = vec![None; n];
        let mut pending = 0;
        for (node, round, mut value) in responses {
            pending += node_pending(&value);
            if let Some(seq) = last_seq_for_node(&value, node, n) {
                node_last[node] = Some(match (round, node_last[node]) {
                    (Round::Fallback, Some(previous)) => previous.min(seq),
                    _ => seq,
                });
            }
            if let Some(Value::Array(rows)) = value.get_mut("results").map(std::mem::take) {
                node_rows[node].extend(rows);
            }
        }

        for rows in &mut node_rows {
            rows.make_contiguous()
                .sort_by_key(|row| row["seq"].as_u64().unwrap_or(0));
        }
        let mut progress = since.clone();
        let mut out = Vec::new();
        loop {
            let mut progressed = false;
            for node in 0..n {
                let Some(mut row) = node_rows[node].pop_front() else {
                    continue;
                };
                progressed = true;
                let shard_seq = row["seq"].as_u64().unwrap_or(0);
                let safe = node_last[node]
                    .map(|seq| shard_seq.min(seq))
                    .unwrap_or(shard_seq);
                progress[node] = progress[node].max(safe);
                row["seq"] = json!(encode_since(&progress));
                out.push(row);
                if out.len() >= limit {
                    break;
                }
            }
            if out.len() >= limit || !progressed {
                break;
            }
        }
        // Rows fetched but not returned are still to come.
        pending += node_rows.iter().map(|rows| rows.len() as u64).sum::<u64>();

        return Ok(json!({
            "results": out,
            "last_seq": encode_since(&progress),
            "pending": pending,
        }));
    }

    let since_ref = &since;
    let responses = scatter(
        read,
        &owned_by_node,
        "changes",
        |node, mask, round| async move {
            // Reassigned shards have no useful checkpoint on the survivor.
            let since = match round {
                Round::Owner => since_ref[node],
                Round::Fallback => 0,
            };
            fetch_node_changes(read, changes, node, &mask, since, None).await
        },
    )
    .await?;
    let mut rows = Vec::new();
    let mut progress = since.clone();
    let mut pending = 0;
    for (node, _, value) in responses {
        pending += node_pending(&value);
        absorb_node_changes(value, node, &mut progress, &mut rows);
    }

    // Shard masks are disjoint, so each document's change is produced by
    // exactly one node. The old all-node fan-out needed a per-row dedup pass;
    // local-first masks make that redundant.
    Ok(json!({
        "results": rows,
        "last_seq": encode_since(&progress),
        "pending": pending,
    }))
}

/// Changes a node had left after the rows it returned, across the shards it
/// read for this request.
fn node_pending(response: &Value) -> u64 {
    response.get("pending").and_then(Value::as_u64).unwrap_or(0)
}

pub(crate) async fn fetch_node_changes(
    read: ClusterRead<'_>,
    changes: ChangesFetch<'_>,
    node: usize,
    mask: &[bool],
    since: u64,
    limit: Option<usize>,
) -> DbResult<Value> {
    let ClusterRead {
        st,
        db,
        dbname,
        headers,
    } = read;
    let ChangesFetch {
        include_docs,
        style_all_docs,
        doc_ids,
        filter,
        q,
    } = changes;
    if node == st.cluster.me {
        return db
            .changes(&ChangesOpts {
                since,
                limit,
                include_docs,
                style_all_docs,
                descending: false,
                doc_ids: doc_ids.map(|ids| ids.to_vec()),
                filter: filter.cloned(),
                conflicts: crate::api::flag(q, "conflicts"),
                owned: Some(mask.to_vec()),
            })
            .await;
    }

    let peer = &st.cluster.nodes[node];
    let selected: Vec<String> = mask
        .iter()
        .enumerate()
        .filter(|(_, owned)| **owned)
        .map(|(shard, _)| shard.to_string())
        .collect();
    let mut url = format!(
        "{peer}/{dbname}/_changes?since={since}&shards={}",
        selected.join(",")
    );
    if let Some(limit) = limit {
        url.push_str(&format!("&limit={limit}"));
    }
    if include_docs {
        url.push_str("&include_docs=true");
    }
    if style_all_docs {
        url.push_str("&style=all_docs");
    }
    if let Some(filter) = q.get("filter") {
        url.push_str("&filter=");
        url.push_str(filter);
    }
    if let Some(view) = q.get("view") {
        url.push_str("&view=");
        url.push_str(view);
    }
    if crate::api::flag(q, "conflicts") {
        url.push_str("&conflicts=true");
    }
    let request = st
        .client
        .get(url)
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", st.cluster.secret.as_str());
    let request = forward_client_auth(request, headers);
    let request = if let Some(ids) = doc_ids {
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
        .map_err(|e| ApiError::new(502, "bad_gateway", e.to_string()))
}

pub(crate) fn absorb_node_changes(
    mut response: Value,
    node: usize,
    progress: &mut [u64],
    rows: &mut Vec<Value>,
) {
    let node_last = last_seq_for_node(&response, node, progress.len());
    let Some(Value::Array(values)) = response.get_mut("results").map(std::mem::take) else {
        return;
    };
    for mut row in values {
        let shard_seq = row["seq"].as_u64().unwrap_or(0);
        let safe = node_last.map(|seq| shard_seq.min(seq)).unwrap_or(shard_seq);
        progress[node] = progress[node].max(safe);
        row["seq"] = json!(encode_since(progress));
        rows.push(row);
    }
}

/// Current per-node update_seq vector. Used for `since=now` and for the
/// `update_seq` reported in `GET /db`.
pub(crate) async fn cluster_update_vector(read: ClusterRead<'_>) -> DbResult<Vec<u64>> {
    let ClusterRead {
        st,
        db,
        dbname,
        headers,
    } = read;
    let out = futures::future::join_all((0..st.cluster.nodes.len()).map(|node| async move {
        if node == st.cluster.me {
            return Ok(db.update_seq());
        }
        let peer = &st.cluster.nodes[node];
        let request = st
            .client
            .get(format!("{peer}/{dbname}"))
            .header("x-douchdb-forwarded", "1")
            .header("x-douchdb-update-seq", "1")
            .header("x-douchdb-secret", st.cluster.secret.as_str());
        let request = forward_client_auth(request, headers);
        let seq = match request.send().await {
            Ok(response) if response.status().is_success() => response
                .json::<Value>()
                .await
                .ok()
                .and_then(|info| info["update_seq"].as_u64())
                .unwrap_or(0),
            _ => 0,
        };
        Ok(seq)
    }))
    .await
    .into_iter()
    .collect::<DbResult<Vec<u64>>>()?;
    Ok(out)
}

pub(crate) fn encode_since(vector: &[u64]) -> String {
    use std::fmt::Write as _;

    // This runs once per change row in a cluster feed. The old form was
    // base64(JSON) and allocated a map plus a base64 buffer for every row.
    // `v1:` is still an opaque token to clients; `decode_since` accepts both.
    let mut out = String::with_capacity(3 + vector.len() * 12);
    out.push_str("v1:");
    for (index, seq) in vector.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        let _ = write!(out, "{seq}");
    }
    out
}

/// Decodes a since token for a cluster of `nodes` nodes. A legacy token naming
/// a node index outside the cluster is invalid (`None`) rather than allocated.
pub(crate) fn decode_since(raw: &str, nodes: usize) -> Option<Vec<u64>> {
    let raw = raw.trim_matches('"');
    if let Some(rest) = raw.strip_prefix("v1:") {
        let mut out = Vec::new();
        for part in rest.split(',') {
            out.push(part.parse().ok()?);
        }
        return Some(out);
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .ok()?;
    let value: Value = serde_json::from_slice(&bytes).ok()?;
    let mut out = Vec::new();
    for (key, seq) in value.as_object()? {
        let index: usize = key.parse().ok()?;
        if index >= nodes {
            return None;
        }
        if out.len() <= index {
            out.resize(index + 1, 0);
        }
        out[index] = seq.as_u64()?;
    }
    Some(out)
}

pub(crate) fn since_component(raw: Option<&str>, index: usize, nodes: usize) -> u64 {
    let Some(raw) = raw else { return 0 };
    let raw = raw.trim_matches('"');
    if let Ok(seq) = raw.parse::<u64>() {
        return seq;
    }
    decode_since(raw, nodes)
        .and_then(|vector| vector.get(index).copied())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Search fan-out

fn search_row_compare(left: &Value, right: &Value, sort: &[(String, bool)]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if !sort.is_empty() {
        let left_order = left.get("order").and_then(Value::as_array);
        let right_order = right.get("order").and_then(Value::as_array);
        for (index, (_, descending)) in sort.iter().enumerate() {
            let null = Value::Null;
            let left_value = left_order
                .and_then(|values| values.get(index))
                .unwrap_or(&null);
            let right_value = right_order
                .and_then(|values| values.get(index))
                .unwrap_or(&null);
            let order = crate::view::cmp_json(left_value, right_value);
            if order != Ordering::Equal {
                return if *descending { order.reverse() } else { order };
            }
        }
    }
    left.get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .cmp(right.get("id").and_then(Value::as_str).unwrap_or_default())
}

/// Search results merged across nodes. Masks are disjoint, so totals and
/// facet counts add up; rows are deduplicated by document id.
#[derive(Default)]
struct SearchBatch {
    total_rows: usize,
    rows: HashMap<String, Value>,
    counts: HashMap<String, HashMap<String, u64>>,
    ranges: HashMap<String, HashMap<String, u64>>,
    groups: BTreeMap<String, (usize, Vec<Value>)>,
}

fn merge_search_batch(batch: &mut SearchBatch, value: Value) {
    batch.total_rows += value.get("total_rows").and_then(Value::as_u64).unwrap_or(0) as usize;
    if let Some(rows) = value.get("rows").and_then(Value::as_array) {
        for row in rows {
            if let Some(id) = row.get("id").and_then(Value::as_str) {
                batch
                    .rows
                    .entry(id.to_string())
                    .or_insert_with(|| row.clone());
            }
        }
    }
    if let Some(counts) = value.get("counts").and_then(Value::as_object) {
        for (field, values) in counts {
            let entry = batch.counts.entry(field.clone()).or_default();
            if let Some(values) = values.as_object() {
                for (key, count) in values {
                    *entry.entry(key.clone()).or_default() += count.as_u64().unwrap_or(0);
                }
            }
        }
    }
    if let Some(ranges) = value.get("ranges").and_then(Value::as_object) {
        for (field, values) in ranges {
            let entry = batch.ranges.entry(field.clone()).or_default();
            if let Some(values) = values.as_object() {
                for (key, count) in values {
                    *entry.entry(key.clone()).or_default() += count.as_u64().unwrap_or(0);
                }
            }
        }
    }
    if let Some(groups) = value.get("groups").and_then(Value::as_array) {
        for group in groups {
            let by = group
                .get("by")
                .and_then(Value::as_str)
                .unwrap_or("null")
                .to_string();
            let entry = batch.groups.entry(by).or_insert_with(|| (0, Vec::new()));
            entry.0 += group.get("total_rows").and_then(Value::as_u64).unwrap_or(0) as usize;
            if let Some(rows) = group.get("rows").and_then(Value::as_array) {
                entry.1.extend(rows.iter().cloned());
            }
        }
    }
}

/// Union per-node search results. Each node answers for a disjoint set of
/// shards, so total counts add up while rows are deduplicated by document id.
pub(crate) async fn cluster_search(
    read: ClusterRead<'_>,
    index: IndexRef<'_>,
    request: &crate::search::SearchRequest,
    limits: crate::view::ViewLimits,
) -> DbResult<Value> {
    let ClusterRead { st, db, .. } = read;
    let owned_by_node = preferred_shard_masks(st, db);
    let offset = crate::search::parse_bookmark(request.bookmark.as_deref());
    let window = offset.saturating_add(request.limit);
    let values = scatter(read, &owned_by_node, "search", |node, mask, _| async move {
        fetch_search_mask(read, index, request, limits, window, node, &mask).await
    })
    .await?;
    let mut batch = SearchBatch::default();
    for (_, _, value) in values {
        merge_search_batch(&mut batch, value);
    }

    let SearchBatch {
        total_rows,
        rows,
        counts,
        ranges,
        groups,
    } = batch;
    let mut rows: Vec<Value> = rows.into_values().collect();
    rows.sort_by(|left, right| search_row_compare(left, right, &request.sort));
    let page: Vec<Value> = rows.into_iter().skip(offset).take(request.limit).collect();

    let mut response = json!({
        "total_rows": total_rows,
        "bookmark": format!("search:{}", offset.saturating_add(page.len())),
        "rows": page,
    });
    if !request.counts.is_empty() {
        response["counts"] = json!(counts);
    }
    if !request.ranges.is_empty() {
        response["ranges"] = json!(ranges);
    }
    if request.group_field.is_some() {
        let groups: Vec<Value> = groups
            .into_iter()
            .map(|(by, (total_rows, rows))| {
                json!({"by": by, "total_rows": total_rows, "rows": rows})
            })
            .collect();
        response["groups"] = json!(groups);
    }
    Ok(response)
}

async fn fetch_search_mask(
    read: ClusterRead<'_>,
    index: IndexRef<'_>,
    request: &crate::search::SearchRequest,
    limits: crate::view::ViewLimits,
    window: usize,
    node: usize,
    mask: &[bool],
) -> DbResult<Value> {
    let ClusterRead {
        st,
        db,
        dbname,
        headers,
    } = read;
    let IndexRef {
        ddoc,
        name: index_name,
    } = index;
    if !mask.iter().any(|owned| *owned) {
        return Ok(json!({"total_rows": 0, "rows": [], "bookmark": "search:0"}));
    }
    let mut local = request.clone();
    local.owner_shards = Some(mask.to_vec());
    local.bookmark = None;
    local.limit = window;
    if node == st.cluster.me {
        return crate::search::query(db, ddoc, index_name, &local, limits).await;
    }

    let peer = &st.cluster.nodes[node];
    let url = format!(
        "{peer}/{dbname}/{}/_search/{}",
        percent_encode(ddoc),
        percent_encode(index_name)
    );
    let selected: Vec<String> = mask
        .iter()
        .enumerate()
        .filter(|(_, owned)| **owned)
        .map(|(shard, _)| shard.to_string())
        .collect();
    let mut params: Vec<(&str, String)> = vec![
        ("q", request.query.clone()),
        ("limit", window.to_string()),
        ("timeout", request.timeout_ms.to_string()),
        ("shards", selected.join(",")),
    ];
    if request.include_docs {
        params.push(("include_docs", "true".to_string()));
    }
    if let Some(partition) = &request.partition {
        params.push(("partition", partition.clone()));
    }
    if !request.counts.is_empty() {
        params.push((
            "counts",
            serde_json::to_string(&request.counts).unwrap_or_default(),
        ));
    }
    if !request.ranges.is_empty() {
        params.push((
            "ranges",
            serde_json::to_string(&request.ranges).unwrap_or_default(),
        ));
    }
    if let Some(group_field) = &request.group_field {
        params.push(("group_field", group_field.clone()));
    }
    for clause in &request.drilldown {
        let mut flat = Vec::with_capacity(clause.len() + 1);
        for (field, values) in clause {
            flat.push(Value::String(field.clone()));
            flat.extend(values.iter().cloned());
        }
        params.push((
            "drilldown",
            serde_json::to_string(&flat).unwrap_or_default(),
        ));
    }
    if !request.sort.is_empty() {
        let sort: Vec<String> = request
            .sort
            .iter()
            .map(|(field, descending)| {
                if *descending {
                    format!("-{field}")
                } else {
                    field.clone()
                }
            })
            .collect();
        params.push(("sort", serde_json::to_string(&sort).unwrap_or_default()));
    }
    if !request.highlight_fields.is_empty() {
        params.push((
            "highlight_fields",
            serde_json::to_string(&request.highlight_fields).unwrap_or_default(),
        ));
        params.push(("highlight_pre_tag", request.highlight_pre.clone()));
        params.push(("highlight_post_tag", request.highlight_post.clone()));
        params.push(("highlight_size", request.highlight_size.to_string()));
        params.push(("highlight_number", request.highlight_number.to_string()));
    }

    let mut outgoing = st
        .client
        .get(url)
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", st.cluster.secret.as_str())
        .query(&params);
    outgoing = forward_client_auth(outgoing, headers);
    let response = outgoing
        .send()
        .await
        .map_err(|error| ApiError::new(502, "bad_gateway", error.to_string()))?;
    if !response.status().is_success() {
        return Err(ApiError::new(
            response.status().as_u16(),
            "search_failed",
            response.text().await.unwrap_or_default(),
        ));
    }
    response
        .json::<Value>()
        .await
        .map_err(|error| ApiError::new(502, "bad_gateway", error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decode_since_rejects_out_of_range_node_index() {
        let enc = |text: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(text);
        assert_eq!(decode_since(&enc(br#"{"99999999999":1}"#), 2), None);
        assert_eq!(decode_since(&enc(br#"{"1":7}"#), 2), Some(vec![0, 7]));
    }

    fn doc(rev: &str, start: u64, ids: &[&str], deleted: bool) -> Value {
        json!({
            "_id": "x",
            "_rev": rev,
            "_deleted": deleted,
            "_revisions": {"start": start, "ids": ids}
        })
    }

    #[test]
    fn reconcile_merges_ancestry_before_deduplicating_bodies() {
        let live = doc("2-b", 2, &["b", "0"], false);
        let short = doc("3-c", 3, &["c"], true);
        let full = doc("3-c", 3, &["c", "b", "0"], true);

        for docs in [
            vec![short.clone(), full.clone(), live.clone()],
            vec![full.clone(), live.clone(), short.clone()],
            vec![live.clone(), short.clone(), full.clone()],
        ] {
            let merged = reconcile_leaves(&docs, None).expect("merged tree");
            assert_eq!(merged.winner["_rev"], "3-c");
            assert_eq!(merged.winner["_deleted"], true);
            assert!(merged.conflicts.is_empty(), "{:?}", merged.conflicts);
            assert_eq!(merged.history, ["3-c", "2-b", "1-0"]);
        }
    }

    #[test]
    fn reconcile_promotes_a_live_conflict_over_a_descendant_tombstone() {
        let docs = [
            doc("2-a", 2, &["a", "0"], false),
            doc("2-b", 2, &["b", "0"], false),
            doc("3-c", 3, &["c", "b", "0"], true),
        ];
        let merged = reconcile_leaves(&docs, None).expect("merged tree");
        assert_eq!(merged.winner["_rev"], "2-a");
        assert!(merged.conflicts.is_empty());
        assert_eq!(merged.deleted_conflicts, ["3-c"]);
    }

    #[test]
    fn reconcile_exact_revision_uses_the_longest_history() {
        let short = doc("3-c", 3, &["c"], true);
        let full = doc("3-c", 3, &["c", "b", "0"], true);
        for docs in [[short.clone(), full.clone()], [full, short]] {
            let merged = reconcile_leaves(&docs, Some("3-c")).expect("exact rev");
            assert_eq!(merged.winner["_rev"], "3-c");
            assert_eq!(merged.history, ["3-c", "2-b", "1-0"]);
        }
    }
}

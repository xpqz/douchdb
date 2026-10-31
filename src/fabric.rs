//! Cluster coordination: shard-map routing, replica selection, quorum
//! reads/writes and internal node calls.

use crate::api::{
    doc_ids_from, doc_opts, doc_response, fail, flag, forward_client_auth, num, percent_encode,
    AppState,
};
use crate::db::{AllDocsOpts, ApiError, ChangesOpts, Db, DbResult, DocOpts};
use axum::body::Bytes;
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
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
    db.replicas_of_doc(docid).contains(&me)
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
        if map.version > desired.version {
            if db.set_shard_map(map.clone()).unwrap_or(false) {
                desired = map;
            }
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
    if acknowledgements >= st.cluster.nodes.len().max(1) / 2 + 1 {
        db.clear_pending_shard_map();
    }
}

pub(crate) async fn proxy(
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
    let request = st.client.request(
        reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap_or(reqwest::Method::GET),
        &url,
    );
    let request = forward_client_auth(request, &headers);
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

pub(crate) fn unavailable(response: &Response) -> bool {
    matches!(
        response.status(),
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
    )
}

pub(crate) async fn quorum_read(
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
    inner.insert("r".to_string(), "1".to_string());
    let mut replicas = replicas_of_doc(db, docid);
    if let Some(index) = replicas.iter().position(|node| *node == st.cluster.me) {
        let local = replicas.remove(index);
        replicas.insert(0, local);
    }
    let targets: Vec<usize> = replicas.into_iter().take(r).collect();
    let docs = futures::future::join_all(targets.into_iter().map(|node| {
        let inner = inner.clone();
        async move {
            if node == st.cluster.me {
                crate::telemetry::record_local_read();
                return db.get_doc(docid, &doc_opts(q)).await.ok();
            }
            crate::telemetry::record_remote_read();
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
        }
    }))
    .await;
    let mut found = 0usize;
    for doc in docs.into_iter().flatten() {
        found += 1;
        if best.as_ref().map(|b| better_rev(&doc, b)).unwrap_or(true) {
            best = Some(doc);
        }
    }
    if found < r {
        crate::telemetry::record_quorum_failure();
    }
    match best {
        Some(doc) => doc_response(doc),
        None => fail(ApiError::not_found("missing")),
    }
}

pub(crate) fn better_rev(candidate: &Value, current: &Value) -> bool {
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

#[derive(serde::Serialize)]
struct BulkBody<'a, T: serde::Serialize> {
    docs: &'a [T],
    new_edits: bool,
}

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
    let replication = st.cluster.replication_factor();
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
            if acks.iter().all(|ack| *ack >= quorum) {
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
    let request = forward_client_auth(request, &headers);
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

pub(crate) fn fallback_shard_masks(
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

pub(crate) async fn cluster_all_docs(
    st: &AppState,
    db: &Db,
    dbname: &str,
    opts: &AllDocsOpts,
    headers: &HeaderMap,
) -> DbResult<Value> {
    let node_count = st.cluster.nodes.len();
    let owned_by_node = preferred_shard_masks(st, db);
    let requests: Vec<(usize, Vec<bool>)> = owned_by_node
        .iter()
        .cloned()
        .enumerate()
        .filter(|(_, mask)| mask.iter().any(|owned| *owned))
        .collect();
    crate::telemetry::record_fanout(requests.len() as u64);

    if let Some(keys) = &opts.keys {
        let mut key_opts = opts.clone();
        key_opts.skip = 0;
        key_opts.limit = usize::MAX;
        key_opts.startkey = None;
        key_opts.endkey = None;
        key_opts.descending = false;

        let responses = futures::future::join_all(requests.iter().map(|(node, mask)| {
            let node = *node;
            let mask = mask.clone();
            let key_opts = key_opts.clone();
            async move {
                (
                    node,
                    fetch_all_docs(st, db, dbname, node, &key_opts, headers, Some(&mask)).await,
                )
            }
        }))
        .await;
        let mut values = Vec::new();
        let mut failed = Vec::new();
        for (node, response) in responses {
            match response {
                Ok(value) => values.push(value),
                Err(_) => failed.push(node),
            }
        }
        if !failed.is_empty() {
            crate::telemetry::record_degraded_read();
            let alive: Vec<usize> = (0..node_count).filter(|n| !failed.contains(n)).collect();
            let groups = fallback_shard_masks(db, &owned_by_node, &failed, &alive)?;
            let fallbacks = futures::future::join_all(groups.into_iter().map(|(node, mask)| {
                let key_opts = key_opts.clone();
                async move {
                    fetch_all_docs(st, db, dbname, node, &key_opts, headers, Some(&mask)).await
                }
            }))
            .await;
            for response in fallbacks {
                values.push(response?);
            }
        }
        if values.is_empty() {
            return Err(ApiError::new(
                502,
                "bad_gateway",
                "no all_docs replica is reachable",
            ));
        }

        let mut total_rows = 0u64;
        let mut found: HashMap<String, Value> = HashMap::new();
        let mut errors: HashMap<String, Value> = HashMap::new();
        for response in values {
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

        let mut rows = Vec::new();
        for key in keys.iter().skip(opts.skip).take(opts.limit) {
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

    let responses = futures::future::join_all(requests.iter().map(|(node, mask)| {
        let node = *node;
        let mask = mask.clone();
        let window = window.clone();
        async move {
            (
                node,
                fetch_all_docs(st, db, dbname, node, &window, headers, Some(&mask)).await,
            )
        }
    }))
    .await;
    let mut values = Vec::new();
    let mut failed = Vec::new();
    for (node, response) in responses {
        match response {
            Ok(value) => values.push(value),
            Err(_) => failed.push(node),
        }
    }
    if !failed.is_empty() {
        crate::telemetry::record_degraded_read();
        let alive: Vec<usize> = (0..node_count).filter(|n| !failed.contains(n)).collect();
        let groups = fallback_shard_masks(db, &owned_by_node, &failed, &alive)?;
        let fallbacks =
            futures::future::join_all(
                groups.into_iter().map(|(node, mask)| {
                    let window = window.clone();
                    async move {
                        fetch_all_docs(st, db, dbname, node, &window, headers, Some(&mask)).await
                    }
                }),
            )
            .await;
        for response in fallbacks {
            values.push(response?);
        }
    }
    if values.is_empty() {
        return Err(ApiError::new(
            502,
            "bad_gateway",
            "no all_docs replica is reachable",
        ));
    }

    let mut total_rows = 0u64;
    let mut before_start = 0usize;
    let mut rows: Vec<(String, Value)> = Vec::new();
    for response in values {
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
    st: &AppState,
    db: &Db,
    dbname: &str,
    node: usize,
    opts: &AllDocsOpts,
    headers: &HeaderMap,
    shards: Option<&[bool]>,
) -> DbResult<Value> {
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
    let request = forward_client_auth(request, &headers);
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

async fn fetch_view_mask(
    st: &AppState,
    db: &Db,
    dbname: &str,
    ddoc: &str,
    view_name: &str,
    query: &crate::view::ViewQuery,
    headers: &HeaderMap,
    node: usize,
    mask: &[bool],
    limit: Option<usize>,
    spec: &crate::view::ViewSpec,
) -> DbResult<(
    usize,
    Arc<Vec<crate::view::Row>>,
    HashMap<String, Value>,
    usize,
)> {
    if !mask.iter().any(|owned| *owned) {
        return Ok((node, Arc::new(Vec::new()), HashMap::new(), 0));
    }
    if node == st.cluster.me {
        let rows = crate::view::local_rows(
            db,
            ddoc,
            view_name,
            spec,
            query.limits,
            query.partition.as_deref(),
        )
        .await?;
        let mut rows: Vec<crate::view::Row> = rows
            .iter()
            .filter(|row| mask.get(db.shard_index(&row.id)).copied().unwrap_or(false))
            .cloned()
            .collect();
        let total_rows = rows.len();
        if let Some(limit) = limit {
            if query.descending {
                let skip = rows.len().saturating_sub(limit);
                rows = rows.split_off(skip);
            } else {
                rows.truncate(limit);
            }
        }
        let node_docs = if query.include_docs || query.attachments {
            let ids = rows.iter().map(|row| row.id.clone()).collect::<Vec<_>>();
            crate::view::docs_by_ids(db, query.attachments, &ids).await?
        } else {
            HashMap::new()
        };
        return Ok((node, Arc::new(rows), node_docs, total_rows));
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
    if query.descending {
        params.push(("descending", "true".to_string()));
    }
    if let Some(partition) = &query.partition {
        params.push(("partition", partition.clone()));
    }
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
    let (rows, node_docs, total_rows) = decode_view_batch(value);
    Ok((node, rows, node_docs, total_rows))
}

pub(crate) async fn cluster_view_rows(
    st: &AppState,
    db: &Db,
    dbname: &str,
    ddoc: &str,
    view_name: &str,
    query: &crate::view::ViewQuery,
    headers: &HeaderMap,
) -> DbResult<(
    crate::view::ViewSpec,
    Arc<Vec<crate::view::Row>>,
    HashMap<String, Value>,
    Option<usize>,
)> {
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
    // A reduce needs the whole range. A map query can take only the global
    // window from each owner because the owner sets are disjoint.
    let can_limit = !reduce
        && query.startkey.is_none()
        && query.endkey.is_none()
        && query.key.is_none()
        && query.keys.is_none();
    let take = query.skip.saturating_add(query.limit);

    let node_count = st.cluster.nodes.len();
    let owned_by_node = preferred_shard_masks(st, db);
    let active = owned_by_node
        .iter()
        .filter(|mask| mask.iter().any(|owned| *owned))
        .count();
    crate::telemetry::record_fanout(active as u64);
    let limit = can_limit.then_some(take);

    let batches = futures::future::join_all((0..node_count).map(|node| {
        let mask = owned_by_node[node].clone();
        let spec = spec.clone();
        async move {
            fetch_view_mask(
                st, db, dbname, ddoc, view_name, query, headers, node, &mask, limit, &spec,
            )
            .await
        }
    }))
    .await;

    let mut values = Vec::new();
    let mut failed = Vec::new();
    for (node, batch) in batches.into_iter().enumerate() {
        match batch {
            Ok(batch) => values.push(batch),
            Err(_) => failed.push(node),
        }
    }
    if !failed.is_empty() {
        crate::telemetry::record_degraded_read();
        let alive: Vec<usize> = (0..node_count).filter(|n| !failed.contains(n)).collect();
        let groups = fallback_shard_masks(db, &owned_by_node, &failed, &alive)?;
        let fallbacks = futures::future::join_all(groups.into_iter().map(|(node, mask)| {
            let spec = spec.clone();
            async move {
                fetch_view_mask(
                    st, db, dbname, ddoc, view_name, query, headers, node, &mask, limit, &spec,
                )
                .await
            }
        }))
        .await;
        for fallback in fallbacks {
            values.push(fallback?);
        }
    }
    if values.is_empty() {
        return Err(ApiError::new(
            502,
            "bad_gateway",
            "no view replica is reachable",
        ));
    }

    let mut counts: HashMap<String, (usize, crate::view::Row)> = HashMap::new();
    let mut docs: HashMap<String, Value> = HashMap::new();
    let mut total_rows = 0usize;
    for (_, rows, node_docs, node_total) in values {
        total_rows += node_total;
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
    Ok((spec, Arc::new(rows), docs, Some(total_rows)))
}

async fn fetch_reduce_mask(
    st: &AppState,
    db: &Db,
    dbname: &str,
    ddoc: &str,
    view_name: &str,
    query: &crate::view::ViewQuery,
    headers: &HeaderMap,
    node: usize,
    mask: &[bool],
) -> DbResult<(usize, Value)> {
    if node == st.cluster.me {
        let mut local = query.clone();
        local.owner_shards = Some(mask.to_vec());
        local.reduce = Some(true);
        let value = crate::view::query(db, ddoc, view_name, &local).await?;
        return Ok((node, value));
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
    if query.group {
        params.push(("group", "true".to_string()));
    }
    if query.group_level > 0 {
        params.push(("group_level", query.group_level.to_string()));
    }
    if let Some(partition) = &query.partition {
        params.push(("partition", partition.clone()));
    }
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
    let value = response
        .json::<Value>()
        .await
        .map_err(|error| ApiError::new(502, "bad_gateway", error.to_string()))?;
    Ok((node, value))
}

pub(crate) async fn cluster_reduce(
    st: &AppState,
    db: &Db,
    dbname: &str,
    ddoc: &str,
    view_name: &str,
    query: &crate::view::ViewQuery,
    reducer: &str,
    headers: &HeaderMap,
) -> DbResult<Value> {
    let ddoc_doc = db.get_doc(ddoc, &DocOpts::default()).await?;
    let spec = crate::view::spec_from_doc(&ddoc_doc, view_name)?;
    crate::view::validate_view_partition(db, &spec, query.partition.as_deref())?;
    let node_count = st.cluster.nodes.len();
    let owned_by_node = preferred_shard_masks(st, db);
    let active = owned_by_node
        .iter()
        .filter(|mask| mask.iter().any(|owned| *owned))
        .count();
    crate::telemetry::record_fanout(active as u64);

    let batches = futures::future::join_all((0..node_count).map(|node| {
        let mask = owned_by_node[node].clone();
        async move {
            if !mask.iter().any(|owned| *owned) {
                return Ok(None);
            }
            fetch_reduce_mask(st, db, dbname, ddoc, view_name, query, headers, node, &mask)
                .await
                .map(Some)
        }
    }))
    .await;

    let mut values = Vec::new();
    let mut failed = Vec::new();
    for (node, batch) in batches.into_iter().enumerate() {
        match batch {
            Ok(Some(value)) => values.push(value),
            Ok(None) => {}
            Err(_) => failed.push(node),
        }
    }
    if !failed.is_empty() {
        crate::telemetry::record_degraded_read();
        let alive: Vec<usize> = (0..node_count)
            .filter(|node| !failed.contains(node))
            .collect();
        let groups = fallback_shard_masks(db, &owned_by_node, &failed, &alive)?;
        let fallbacks =
            futures::future::join_all(groups.into_iter().map(|(node, mask)| async move {
                fetch_reduce_mask(st, db, dbname, ddoc, view_name, query, headers, node, &mask)
                    .await
            }))
            .await;
        for fallback in fallbacks {
            values.push(fallback?);
        }
    }
    if values.is_empty() {
        return Err(ApiError::new(
            502,
            "bad_gateway",
            "no view replica is reachable",
        ));
    }

    let grouped = query.group || query.group_level > 0;
    let mut partials = Vec::new();
    let mut groups: HashMap<String, (Value, Value)> = HashMap::new();
    for (_, value) in values {
        if grouped {
            for row in value["rows"].as_array().cloned().unwrap_or_default() {
                let key = row.get("key").cloned().unwrap_or(Value::Null);
                let value = row.get("value").cloned().unwrap_or(Value::Null);
                let hash = serde_json::to_string(&key).unwrap_or_default();
                match groups.get_mut(&hash) {
                    Some((_, existing)) => {
                        let combined = combine_reduce_values(
                            reducer,
                            &[existing.clone(), value],
                            query.limits,
                        )?;
                        *existing = combined;
                    }
                    None => {
                        groups.insert(hash, (key, value));
                    }
                }
            }
        } else {
            partials.push(
                value["rows"]
                    .get(0)
                    .and_then(|row| row.get("value"))
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }
    }

    if grouped {
        let mut rows: Vec<(Value, Value)> = groups.into_values().collect();
        rows.sort_by(|a, b| crate::view::cmp_json(&a.0, &b.0));
        return Ok(json!({
            "rows": rows
                .into_iter()
                .map(|(key, value)| json!({"key": key, "value": value}))
                .collect::<Vec<_>>()
        }));
    }

    let value = if partials.len() == 1 {
        partials.pop().unwrap_or(Value::Null)
    } else {
        combine_reduce_values(reducer, &partials, query.limits)?
    };
    Ok(json!({"rows": [{"key": Value::Null, "value": value}]}))
}

pub(crate) fn combine_reduce_values(
    reducer: &str,
    partials: &[Value],
    limits: crate::view::ViewLimits,
) -> DbResult<Value> {
    match reducer {
        "_count" => Ok(json!(partials
            .iter()
            .map(|value| value.as_u64().unwrap_or(0))
            .sum::<u64>())),
        "_sum" => {
            let mut sum = 0.0;
            for value in partials {
                sum += value.as_f64().ok_or_else(|| {
                    ApiError::new(
                        500,
                        "view_error",
                        format!("_sum requires numbers, got {value}"),
                    )
                })?;
            }
            Ok(crate::view::number_value(sum))
        }
        "_stats" => {
            let mut count = 0u64;
            let mut sum = 0.0;
            let mut sumsqr = 0.0;
            let mut min: Option<f64> = None;
            let mut max: Option<f64> = None;
            for value in partials {
                count += value.get("count").and_then(Value::as_u64).unwrap_or(0);
                sum += value.get("sum").and_then(Value::as_f64).unwrap_or(0.0);
                sumsqr += value.get("sumsqr").and_then(Value::as_f64).unwrap_or(0.0);
                if let Some(number) = value.get("min").and_then(Value::as_f64) {
                    min = Some(min.map_or(number, |current| current.min(number)));
                }
                if let Some(number) = value.get("max").and_then(Value::as_f64) {
                    max = Some(max.map_or(number, |current| current.max(number)));
                }
            }
            if count == 0 {
                return Ok(json!({
                    "sum": 0, "count": 0, "min": null, "max": null, "sumsqr": 0
                }));
            }
            Ok(json!({
                "sum": crate::view::number_value(sum),
                "count": count,
                "min": crate::view::number_value(min.unwrap_or(0.0)),
                "max": crate::view::number_value(max.unwrap_or(0.0)),
                "sumsqr": crate::view::number_value(sumsqr),
            }))
        }
        _ => crate::view::recombine(reducer, partials, limits).map_err(|reason| {
            ApiError::new(
                500,
                "view_error",
                format!("reduce function failed: {reason}"),
            )
        }),
    }
}

pub(crate) fn decode_view_batch(
    value: Value,
) -> (Arc<Vec<crate::view::Row>>, HashMap<String, Value>, usize) {
    let total_rows = value["total_rows"].as_u64().unwrap_or(0) as usize;
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
    (Arc::new(rows), docs, total_rows)
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

pub(crate) async fn cluster_changes(
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
    let owned_by_node = preferred_shard_masks(st, db);
    let requests: Vec<(usize, Vec<bool>)> = owned_by_node
        .iter()
        .cloned()
        .enumerate()
        .filter(|(_, mask)| mask.iter().any(|owned| *owned))
        .collect();
    crate::telemetry::record_fanout(requests.len() as u64);

    let include_docs = flag(q, "include_docs");
    let style_all_docs = q.get("style").map(String::as_str) == Some("all_docs");
    let doc_ids = doc_ids_from(q, body);
    let limit = num(q, "limit");

    if let Some(limit) = limit {
        if limit == 0 {
            return Ok(json!({
                "results": [],
                "last_seq": encode_since(&since),
                "pending": 0,
            }));
        }

        let responses = futures::future::join_all(requests.iter().map(|(node, mask)| {
            let node = *node;
            let mask = mask.clone();
            let since = since[node];
            let doc_ids = doc_ids.clone();
            async move {
                (
                    node,
                    fetch_node_changes(
                        st,
                        db,
                        dbname,
                        node,
                        since,
                        Some(limit),
                        include_docs,
                        style_all_docs,
                        doc_ids.as_deref(),
                        headers,
                        &mask,
                    )
                    .await,
                )
            }
        }))
        .await;
        let mut node_rows: Vec<std::collections::VecDeque<Value>> =
            (0..n).map(|_| std::collections::VecDeque::new()).collect();
        let mut failed = Vec::new();
        let mut succeeded = false;
        for (node, response) in responses {
            match response {
                Ok(mut value) => {
                    succeeded = true;
                    if let Some(Value::Array(rows)) = value.get_mut("results").map(std::mem::take) {
                        node_rows[node].extend(rows);
                    }
                }
                Err(_) => failed.push(node),
            }
        }
        if !failed.is_empty() {
            crate::telemetry::record_degraded_read();
            let alive: Vec<usize> = (0..n).filter(|node| !failed.contains(node)).collect();
            let groups = fallback_shard_masks(db, &owned_by_node, &failed, &alive)?;
            let fallbacks = futures::future::join_all(groups.into_iter().map(|(node, mask)| {
                let since = since[node];
                let doc_ids = doc_ids.clone();
                async move {
                    (
                        node,
                        fetch_node_changes(
                            st,
                            db,
                            dbname,
                            node,
                            since,
                            Some(limit),
                            include_docs,
                            style_all_docs,
                            doc_ids.as_deref(),
                            headers,
                            &mask,
                        )
                        .await,
                    )
                }
            }))
            .await;
            for (node, response) in fallbacks {
                let mut value = response?;
                succeeded = true;
                if let Some(Value::Array(rows)) = value.get_mut("results").map(std::mem::take) {
                    node_rows[node].extend(rows);
                }
            }
        }
        if !succeeded {
            return Err(ApiError::new(
                502,
                "bad_gateway",
                "no changes replica is reachable",
            ));
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
                progress[node] = progress[node].max(shard_seq);
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

        return Ok(json!({
            "results": out,
            "last_seq": encode_since(&progress),
            "pending": 0,
        }));
    }

    let responses = futures::future::join_all(requests.iter().map(|(node, mask)| {
        let node = *node;
        let mask = mask.clone();
        let since = since[node];
        let doc_ids = doc_ids.clone();
        async move {
            (
                node,
                fetch_node_changes(
                    st,
                    db,
                    dbname,
                    node,
                    since,
                    None,
                    include_docs,
                    style_all_docs,
                    doc_ids.as_deref(),
                    headers,
                    &mask,
                )
                .await,
            )
        }
    }))
    .await;
    let mut succeeded = false;
    let mut failed = Vec::new();
    let mut rows = Vec::new();
    let mut progress = since.clone();
    for (node, response) in responses {
        match response {
            Ok(value) => {
                succeeded = true;
                absorb_node_changes(value, node, &mut progress, &mut rows);
            }
            Err(_) => failed.push(node),
        }
    }
    if !failed.is_empty() {
        crate::telemetry::record_degraded_read();
        let alive: Vec<usize> = (0..n).filter(|node| !failed.contains(node)).collect();
        let groups = fallback_shard_masks(db, &owned_by_node, &failed, &alive)?;
        let fallbacks = futures::future::join_all(groups.into_iter().map(|(node, mask)| {
            let since = since[node];
            let doc_ids = doc_ids.clone();
            async move {
                (
                    node,
                    fetch_node_changes(
                        st,
                        db,
                        dbname,
                        node,
                        since,
                        None,
                        include_docs,
                        style_all_docs,
                        doc_ids.as_deref(),
                        headers,
                        &mask,
                    )
                    .await,
                )
            }
        }))
        .await;
        for (node, response) in fallbacks {
            let value = response?;
            succeeded = true;
            absorb_node_changes(value, node, &mut progress, &mut rows);
        }
    }
    if !succeeded {
        return Err(ApiError::new(
            502,
            "bad_gateway",
            "no changes replica is reachable",
        ));
    }

    // Shard masks are disjoint, so each document's change is produced by
    // exactly one node. The old all-node fan-out needed a per-row dedup pass;
    // local-first masks make that redundant.
    Ok(json!({
        "results": rows,
        "last_seq": encode_since(&progress),
        "pending": 0,
    }))
}

pub(crate) async fn fetch_node_changes(
    st: &AppState,
    db: &Db,
    dbname: &str,
    node: usize,
    since: u64,
    limit: Option<usize>,
    include_docs: bool,
    style_all_docs: bool,
    doc_ids: Option<&[String]>,
    headers: &HeaderMap,
    mask: &[bool],
) -> DbResult<Value> {
    if node == st.cluster.me {
        return db
            .changes(&ChangesOpts {
                since,
                limit,
                include_docs,
                style_all_docs,
                descending: false,
                doc_ids: doc_ids.map(|ids| ids.to_vec()),
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
    let Some(Value::Array(values)) = response.get_mut("results").map(std::mem::take) else {
        return;
    };
    for mut row in values {
        let shard_seq = row["seq"].as_u64().unwrap_or(0);
        progress[node] = progress[node].max(shard_seq);
        row["seq"] = json!(encode_since(progress));
        rows.push(row);
    }
}

pub(crate) async fn cluster_update_vector(
    st: &AppState,
    db: &Db,
    dbname: &str,
    headers: &HeaderMap,
) -> DbResult<Vec<u64>> {
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

pub(crate) fn decode_since(raw: &str) -> Option<Vec<u64>> {
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
        if out.len() <= index {
            out.resize(index + 1, 0);
        }
        out[index] = seq.as_u64()?;
    }
    Some(out)
}

pub(crate) fn since_component(raw: Option<&str>, index: usize) -> u64 {
    let Some(raw) = raw else { return 0 };
    let raw = raw.trim_matches('"');
    if let Ok(seq) = raw.parse::<u64>() {
        return seq;
    }
    decode_since(raw)
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

fn merge_search_batch(
    batch: &mut (
        usize,
        usize,
        HashMap<String, Value>,
        HashMap<String, HashMap<String, u64>>,
        HashMap<String, HashMap<String, u64>>,
        std::collections::BTreeMap<String, (usize, Vec<Value>)>,
    ),
    value: Value,
) {
    batch.1 += value.get("total_rows").and_then(Value::as_u64).unwrap_or(0) as usize;
    if let Some(rows) = value.get("rows").and_then(Value::as_array) {
        for row in rows {
            if let Some(id) = row.get("id").and_then(Value::as_str) {
                batch.2.entry(id.to_string()).or_insert_with(|| row.clone());
            }
        }
    }
    if let Some(counts) = value.get("counts").and_then(Value::as_object) {
        for (field, values) in counts {
            let entry = batch.3.entry(field.clone()).or_default();
            if let Some(values) = values.as_object() {
                for (key, count) in values {
                    *entry.entry(key.clone()).or_default() += count.as_u64().unwrap_or(0);
                }
            }
        }
    }
    if let Some(ranges) = value.get("ranges").and_then(Value::as_object) {
        for (field, values) in ranges {
            let entry = batch.4.entry(field.clone()).or_default();
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
            let entry = batch.5.entry(by).or_insert_with(|| (0, Vec::new()));
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
    st: &AppState,
    db: &Db,
    dbname: &str,
    ddoc: &str,
    index_name: &str,
    request: &crate::search::SearchRequest,
    limits: crate::view::ViewLimits,
    headers: &HeaderMap,
) -> DbResult<Value> {
    use futures::future::join_all;

    let node_count = st.cluster.nodes.len();
    let built = preferred_shard_masks(st, db);
    let offset = crate::search::parse_bookmark(request.bookmark.as_deref());
    let window = offset.saturating_add(request.limit);
    let active = built
        .iter()
        .filter(|mask| mask.iter().any(|owned| *owned))
        .count();
    crate::telemetry::record_fanout(active as u64);

    let mut masks = built.clone();
    let mut failed: Vec<usize> = Vec::new();
    let mut batch = (
        0usize,
        0usize,
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        std::collections::BTreeMap::new(),
    );

    let batches = join_all((0..node_count).map(|node| {
        let mask = masks[node].clone();
        async move {
            fetch_search_mask(
                st, db, dbname, ddoc, index_name, request, limits, headers, node, &mask, window,
            )
            .await
        }
    }))
    .await;

    for (node, result) in batches.into_iter().enumerate() {
        match result {
            Ok(value) => merge_search_batch(&mut batch, value),
            Err(_) => failed.push(node),
        }
    }

    if !failed.is_empty() {
        crate::telemetry::record_degraded_read();
        let alive: Vec<usize> = (0..node_count)
            .filter(|node| !failed.contains(node))
            .collect();
        if alive.is_empty() {
            return Err(ApiError::new(
                502,
                "bad_gateway",
                "no search replica is reachable",
            ));
        }
        let groups = fallback_shard_masks(db, &built, &failed, &alive)?;
        for node in &failed {
            masks[*node] = vec![false; db.q as usize];
        }
        for (node, extra) in groups {
            for shard in 0..extra.len() {
                masks[node][shard] |= extra[shard];
            }
        }

        batch = (
            0,
            0,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            std::collections::BTreeMap::new(),
        );
        let batches = join_all((0..node_count).map(|node| {
            let mask = masks[node].clone();
            async move {
                fetch_search_mask(
                    st, db, dbname, ddoc, index_name, request, limits, headers, node, &mask, window,
                )
                .await
            }
        }))
        .await;
        let mut any = false;
        for result in batches {
            if let Ok(value) = result {
                any = true;
                merge_search_batch(&mut batch, value);
            }
        }
        if !any {
            return Err(ApiError::new(
                502,
                "bad_gateway",
                "no search replica is reachable",
            ));
        }
    }

    let (_, total_rows, rows, counts, ranges, groups) = batch;
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
    st: &AppState,
    db: &Db,
    dbname: &str,
    ddoc: &str,
    index_name: &str,
    request: &crate::search::SearchRequest,
    limits: crate::view::ViewLimits,
    headers: &HeaderMap,
    node: usize,
    mask: &[bool],
    window: usize,
) -> DbResult<Value> {
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

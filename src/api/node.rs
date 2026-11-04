//! Node and cluster management endpoints: `/_membership` and
//! `/_node/_local/...`.

use super::*;

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

/// Internal/replication-side security update. The public `PUT /db/_security`
/// handler fans out here after checking database-admin access.
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
    match crate::cluster_ops::apply_shard_map(&st, &db, &name, map, &headers).await {
        Ok((map, pending)) => {
            let mut value = json!(map);
            value["pending"] = json!(pending);
            json(StatusCode::OK, value)
        }
        Err(error) => fail(error),
    }
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

    match crate::cluster_ops::rebalance(&st, &db, &name, &headers, timeout_ms, cleanup).await {
        Ok(value) => json(StatusCode::OK, value),
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

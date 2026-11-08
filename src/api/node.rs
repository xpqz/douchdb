//! Node and cluster management endpoints: `/_membership` and
//! `/_node/_local/...`.

use super::*;

pub async fn membership(State(st): State<Arc<AppState>>, _: AdminOnly) -> Response {
    let nodes: Vec<String> = if st.ctx.cluster.nodes.is_empty() {
        vec!["nonode@localhost".to_string()]
    } else {
        st.ctx.cluster.nodes.clone()
    };
    json(
        StatusCode::OK,
        json!({ "all_nodes": nodes, "cluster_nodes": nodes }),
    )
}

/// Cluster overview for the management console: membership, discovery probes,
/// per-database shard maps and the background replicator snapshot.
pub async fn cluster_status(State(st): State<Arc<AppState>>, _: AdminOnly) -> Response {
    let membership: Vec<String> = if st.ctx.cluster.nodes.is_empty() {
        vec!["nonode@localhost".to_string()]
    } else {
        st.ctx.cluster.nodes.clone()
    };
    let mut databases = Vec::new();
    for name in st.db_names().await {
        let Ok(placed) = st.placement(&name).await else {
            continue;
        };
        let map = placed.shard_map;
        // Counts only for an open database; a closed one is not opened here.
        let counts = match &placed.open {
            Some(db) => db.shard_counts().await.ok(),
            None => None,
        };
        let shards: Vec<Value> = (0..map.q)
            .map(|shard| {
                let replicas = map.replicas_of(shard).to_vec();
                let count = counts
                    .as_ref()
                    .and_then(|counts| counts.get(shard as usize).copied());
                json!({
                    "shard": shard,
                    "replicas": replicas,
                    "owner": map.owner_of(shard),
                    "local": replicas.contains(&st.ctx.cluster.me),
                    "docs": count.map(|(docs, _)| docs),
                    "deleted": count.map(|(_, deleted)| deleted),
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
                "index": st.ctx.cluster.me,
                "clustered": st.ctx.cluster.is_clustered(),
                "q": st.config.q,
                "n": st.ctx.cluster.replication_factor(),
                "sync_secs": st.config.sync_secs,
            },
            "membership": {
                "all_nodes": membership.clone(),
                "cluster_nodes": membership,
            },
            "discovery": st.cluster_state.discovery_json(&st.ctx.cluster.nodes, &st.config.seeds),
            "replication": st.cluster_state.replication_json(),
            "databases": databases,
        }),
    )
}

/// Probe peers and reported memberships now. Returns the previous snapshot and
/// starts the pass in the background so a dead seed cannot block the request.
pub async fn cluster_discover(State(st): State<Arc<AppState>>, _: AdminOnly) -> Response {
    if !st.cluster_state.discovery_running() {
        tokio::spawn(crate::discovery::discover_once(st.clone()));
    }
    json(
        StatusCode::ACCEPTED,
        json!({
            "ok": true,
            "running": st.cluster_state.discovery_running(),
            "discovery": st.cluster_state.discovery_json(&st.ctx.cluster.nodes, &st.config.seeds),
        }),
    )
}

pub async fn replication_status(State(st): State<Arc<AppState>>, _: AdminOnly) -> Response {
    json(StatusCode::OK, st.cluster_state.replication_json())
}

/// Trigger one anti-entropy pass. Background replication already runs on its
/// timer; this is the "sync now" control for operators.
pub async fn replication_run(State(st): State<Arc<AppState>>, uri: Uri, _: AdminOnly) -> Response {
    let db = query(&uri).get("db").cloned();
    if !st.cluster_state.replication_running() {
        tokio::spawn(crate::replicate::sync_once(st.clone(), db));
    }
    json(
        StatusCode::ACCEPTED,
        json!({"ok": true, "running": true, "replication": st.cluster_state.replication_json()}),
    )
}

pub async fn replication_pause(State(st): State<Arc<AppState>>, _: AdminOnly) -> Response {
    st.cluster_state.set_replication_paused(true);
    json(
        StatusCode::OK,
        json!({"ok": true, "replication": st.cluster_state.replication_json()}),
    )
}

pub async fn replication_resume(State(st): State<Arc<AppState>>, _: AdminOnly) -> Response {
    st.cluster_state.set_replication_paused(false);
    json(
        StatusCode::OK,
        json!({"ok": true, "replication": st.cluster_state.replication_json()}),
    )
}

/// mem3-style shard-map inspection and movement endpoint.
pub async fn dbs_metadata(State(st): State<Arc<AppState>>, _: AdminOnly) -> Response {
    let mut out = Map::new();
    for name in st.db_names().await {
        if let Ok(placed) = st.placement(&name).await {
            out.insert(name, json!(placed.shard_map));
        }
    }
    json(StatusCode::OK, Value::Object(out))
}

pub async fn db_metadata(
    State(st): State<Arc<AppState>>,
    DbName(name): DbName,
    _: AdminOnly,
) -> Response {
    match st.placement(&name).await {
        Ok(placed) => json(StatusCode::OK, json!(placed.shard_map)),
        Err(error) => fail(error),
    }
}

/// Internal target-placement update used while a rebalance is being verified.
pub async fn db_migration_put(
    State(st): State<Arc<AppState>>,
    DbName(name): DbName,
    axum::extract::Query(query): axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !st.ctx.cluster.is_internal(&headers) {
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
    if map.q != db.q || map.node_count != st.ctx.cluster.nodes.len().max(1) {
        return fail(ApiError::bad_request(
            "migration shard map does not match database q or cluster node count",
        ));
    }
    // Freezes client writes on this peer for the verification window.
    if let Err(error) = db.begin_migration(map, query.get("id").cloned()) {
        return fail(error);
    }
    // Writes accepted before the freeze may still be fanning out to replicas.
    // Do not report the freeze until they have landed.
    if !db.drain_writes(crate::cluster_ops::write_drain()).await {
        return fail(ApiError::new(
            503,
            "rebalance_in_progress",
            "in-flight writes did not finish before the freeze",
        ));
    }
    ok()
}

/// Clears the migration announced with the same `id`. A different id, or none
/// while the migration has one, is a 409 and leaves the freeze in place, so a
/// stale or rival coordinator cannot unfreeze another's rebalance. Clearing
/// when there is nothing to clear succeeds.
pub async fn db_migration_delete(
    State(st): State<Arc<AppState>>,
    DbName(name): DbName,
    axum::extract::Query(query): axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if !st.ctx.cluster.is_internal(&headers) {
        let principal = st.principal(&headers);
        return access_denied(&principal);
    }
    match st.db(&name).await {
        Ok(db) => {
            if db.clear_migration_if(query.get("id").map(String::as_str)) {
                ok()
            } else {
                fail(ApiError::new(
                    409,
                    "rebalance_in_progress",
                    "the migration in progress belongs to another coordinator",
                ))
            }
        }
        Err(error) => fail(error),
    }
}

/// Internal security inspection for anti-entropy: the object and its version.
pub async fn db_security_get(
    State(st): State<Arc<AppState>>,
    DbName(name): DbName,
    headers: HeaderMap,
) -> Response {
    if !st.ctx.cluster.is_internal(&headers) {
        let principal = st.principal(&headers);
        return access_denied(&principal);
    }
    match st.db(&name).await {
        Ok(db) => {
            let (version, security) = db.security_versioned();
            json(
                StatusCode::OK,
                json!({"version": version, "security": security}),
            )
        }
        Err(error) => fail(error),
    }
}

/// Internal/replication-side security update. The public `PUT /db/_security`
/// handler fans out here after checking database-admin access. A version older
/// than the stored one is rejected with 409.
pub async fn db_security_put(
    State(st): State<Arc<AppState>>,
    DbName(name): DbName,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !st.ctx.cluster.is_internal(&headers) {
        let principal = st.principal(&headers);
        return access_denied(&principal);
    }
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(error) => return fail(error),
    };
    let envelope: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return fail(ApiError::bad_request("invalid security object")),
    };
    let (Some(version), Some(value)) = (
        envelope.get("version").and_then(Value::as_u64),
        envelope.get("security"),
    ) else {
        return fail(ApiError::bad_request("invalid security object"));
    };
    if let Err(error) = validate_security(value) {
        return fail(error);
    }
    match db.set_security_versioned(version, value.clone()) {
        Ok(true) => ok(),
        Ok(false) => fail(ApiError::new(
            409,
            "conflict",
            "security object was rejected in favour of a newer or competing one",
        )),
        Err(error) => fail(error),
    }
}

pub async fn db_shard_counts(
    State(st): State<Arc<AppState>>,
    DbName(name): DbName,
    uri: Uri,
    _: AdminOnly,
) -> Response {
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
    DbName(name): DbName,
    headers: HeaderMap,
    _: AdminOnly,
    body: Bytes,
) -> Response {
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
    match crate::cluster_ops::apply_shard_map(&st.ctx, &db, &name, map, &headers).await {
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
    DbName(name): DbName,
    headers: HeaderMap,
    uri: Uri,
    _: AdminOnly,
) -> Response {
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(error) => return fail(error),
    };
    let q = query(&uri);
    let cleanup = flag(&q, "cleanup");
    let timeout_ms = num(&q, "timeout_ms").unwrap_or(30_000).min(300_000) as u64;

    match crate::cluster_ops::rebalance(&st.ctx, &db, &name, &headers, timeout_ms, cleanup).await {
        Ok(value) => json(StatusCode::OK, value),
        Err(error) => fail(error),
    }
}

pub async fn db_cleanup(
    State(st): State<Arc<AppState>>,
    DbName(name): DbName,
    _: AdminOnly,
) -> Response {
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
        if !map.replicas_of(shard).contains(&st.ctx.cluster.me) {
            cleared.push(shard as usize);
        }
    }
    match db.clear_shards(&cleared).await {
        Ok(()) => json(StatusCode::OK, json!({"cleared": cleared})),
        Err(error) => fail(error),
    }
}

/// Internal batched metadata exchange for anti-entropy: compares the sender's
/// summary of shard-map and security versions with what this node holds and
/// returns the entries that differ, and applies the entries the sender pushes
/// (through the same setters as the per-database PUTs, so version and
/// tiebreak rules are the same).
pub async fn metadata_sync(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    use crate::fabric::{Accepted, MetadataSync, MetadataSyncReply};
    if !st.ctx.cluster.is_internal(&headers) {
        let principal = st.principal(&headers);
        return access_denied(&principal);
    }
    let request: MetadataSync = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return fail(ApiError::bad_request("invalid metadata sync request")),
    };
    let mut reply = MetadataSyncReply::default();
    (reply.differs, reply.missing) =
        crate::fabric::metadata_differences(&st, &request.summary).await;
    for (name, entries) in request.push {
        let Ok(db) = st.db_cold(&name).await else {
            continue;
        };
        let mut accepted = Accepted::default();
        if let Some(map) = entries.shard_map {
            accepted.shard_map =
                crate::cluster_ops::apply_shard_map(&st.ctx, &db, &name, map, &headers)
                    .await
                    .is_ok();
        }
        if let Some(entry) = entries.security {
            accepted.security = validate_security(&entry.security).is_ok()
                && db
                    .set_security_versioned(entry.version, entry.security)
                    .unwrap_or(false);
        }
        reply.accepted.insert(name, accepted);
        drop(db);
        st.release_cold().await;
    }
    json(StatusCode::OK, json!(reply))
}

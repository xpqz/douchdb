//! Cluster-wide metadata changes coordinated from one node: shard-map
//! updates, rebalance and security-object propagation.

use crate::api::{internal_request, is_forwarded, AppState};
use crate::cluster::ShardMap;
use crate::db::{ApiError, Db, DbResult};
use crate::store::ShardState;
use axum::http::{header, HeaderMap, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;

async fn send_security(
    st: &AppState,
    peer: &str,
    name: &str,
    value: &Value,
    headers: &HeaderMap,
) -> Result<(), String> {
    let request = internal_request(
        st,
        reqwest::Method::PUT,
        peer,
        &["_node", "_local", "_dbs", name, "security"],
        Some(headers),
    )
    .json(value);
    match request.send().await {
        Ok(response) if response.status().is_success() => Ok(()),
        Ok(response) => Err(format!("{peer}: HTTP {}", response.status())),
        Err(error) => Err(format!("{peer}: {error}")),
    }
}

/// Apply a security object locally and, in a cluster, to every peer before
/// reporting success. A partial fan-out is rolled back so a reachable peer does
/// not keep a newer security object than the coordinator.
pub(crate) async fn set_security_everywhere(
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

fn shard_map_rejected() -> ApiError {
    ApiError::new(
        409,
        "conflict",
        "shard map was rejected in favour of a newer or competing map",
    )
}

pub(crate) async fn apply_shard_map(
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
    if !db.set_shard_map(map.clone())? {
        return Err(shard_map_rejected());
    }
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
            let request = internal_request(
                st,
                reqwest::Method::PUT,
                &peer,
                &["_node", "_local", "_dbs", name],
                Some(headers),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .body(payload);
            request.send().await.ok().map(|response| response.status())
        }
    }))
    .await;
    let mut acknowledgements = 1usize; // local write
    let mut rejected = false;
    for status in responses.into_iter().flatten() {
        if status.is_success() {
            acknowledgements += 1;
        } else if status == StatusCode::CONFLICT {
            rejected = true;
        }
    }
    if rejected {
        // A peer holds a competing map. Leave the pending marker alone;
        // anti-entropy converges every node on the tiebreak winner.
        return Err(shard_map_rejected());
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
    let request = internal_request(
        st,
        reqwest::Method::GET,
        &st.cluster.nodes[node],
        &["_node", "_local", "_dbs", &db.name, "shards"],
        Some(headers),
    );
    let request = if with_digest {
        request.query(&[("digest", "1")])
    } else {
        request
    };
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
            let state = states
                .get(*node)
                .and_then(|state| state.as_ref())
                .and_then(|state| state.get(shard as usize))?;
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
        let request = internal_request(
            st,
            reqwest::Method::POST,
            peer,
            &["_node", "_local", "_dbs", &db.name, "cleanup"],
            Some(headers),
        );
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

/// Time to wait for in-flight writes to finish once writes are frozen.
pub(crate) const WRITE_DRAIN: std::time::Duration = std::time::Duration::from_secs(15);

/// A peer's refusal of a migration. `conflict` marks a different migration
/// already owning the peer, which must be left alone.
struct MigrationRefused {
    conflict: bool,
    message: String,
}

async fn send_migration(
    st: &AppState,
    peer: &str,
    name: &str,
    map: Option<&ShardMap>,
    headers: &HeaderMap,
) -> Result<(), MigrationRefused> {
    let segments = ["_node", "_local", "_dbs", name, "migration"];
    let request = match map {
        Some(map) => {
            internal_request(st, reqwest::Method::PUT, peer, &segments, Some(headers)).json(map)
        }
        None => internal_request(st, reqwest::Method::DELETE, peer, &segments, Some(headers)),
    };
    match request.send().await {
        Ok(response) if response.status().is_success() => Ok(()),
        Ok(response) => Err(MigrationRefused {
            conflict: response.status() == reqwest::StatusCode::CONFLICT,
            message: format!("{peer}: HTTP {}", response.status()),
        }),
        Err(error) => Err(MigrationRefused {
            conflict: false,
            message: format!("{peer}: {error}"),
        }),
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
    // Peers that refused because another migration owns them keep that
    // migration; clearing them would unfreeze the other coordinator.
    let mut foreign = Vec::new();
    if !db.drain_writes(WRITE_DRAIN).await {
        failures.push("local writes did not finish before the freeze".to_string());
    }
    if failures.is_empty() {
        for peer in &peers {
            if let Err(refused) = send_migration(st, peer, name, Some(map), headers).await {
                if refused.conflict {
                    foreign.push(peer.clone());
                }
                failures.push(refused.message);
            }
        }
    }
    if failures.is_empty() {
        return Ok(());
    }
    db.clear_migration_map();
    for peer in peers.iter().filter(|peer| !foreign.contains(peer)) {
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
            tracing::warn!("migration map clear on {}", error.message);
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
        let request = internal_request(
            st,
            reqwest::Method::PUT,
            peer,
            &["_node", "_local", "_dbs", name],
            Some(headers),
        )
        .json(map);
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
        let request = internal_request(
            st,
            reqwest::Method::PUT,
            peer,
            &["_node", "_local", "_dbs", name],
            Some(headers),
        )
        .json(&rollback);
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

/// Move a database to the balanced placement for the current cluster. Client
/// writes are frozen on every node while the target replicas catch up, and
/// reads stay on the old map until the new one is activated. With `cleanup`,
/// every node then clears the shards it no longer owns. Returns the new map
/// with `pending` and, after a cleanup, `cleaned`.
pub(crate) async fn rebalance(
    st: &AppState,
    db: &Arc<Db>,
    name: &str,
    headers: &HeaderMap,
    timeout_ms: u64,
    cleanup: bool,
) -> DbResult<Value> {
    // Freeze client writes before reading the source digests. Reads continue to
    // use the old active map; anti-entropy is internal and remains allowed.
    if !db.try_start_rebalance() {
        return Err(ApiError::new(
            409,
            "rebalance_in_progress",
            "another rebalance is already running for this database",
        ));
    }
    crate::telemetry::record_rebalance_started();
    let old_map = db.shard_map();
    let mut map = ShardMap::balanced(db.q, st.cluster.nodes.len().max(1), old_map.n());
    map.version = old_map.version.saturating_add(1);
    // Broadcast the migration map first: it freezes writes on every peer and
    // lets anti-entropy seed the target replicas while reads stay on the old
    // active map.
    if let Err(error) = set_migration_everywhere(st, db, name, &map, headers).await {
        db.set_rebalance_in_progress(false);
        return Err(error);
    }

    if let Err(error) = stable_source_states(
        st,
        db,
        &old_map,
        headers,
        // Allow slow digest scans plus any in-flight write to settle.
        std::time::Duration::from_millis(10_000),
    )
    .await
    {
        clear_migration_everywhere(st, db, name, headers).await;
        db.set_rebalance_in_progress(false);
        return Err(error);
    }

    if let Err(error) = wait_for_rebalance(st, db, &old_map, &map, headers, timeout_ms).await {
        // The active map never changed, so reads continued on the old owners
        // throughout the migration attempt.
        clear_migration_everywhere(st, db, name, headers).await;
        db.set_rebalance_in_progress(false);
        if error.status == 504 {
            crate::telemetry::record_rebalance_timeout();
        }
        return Err(error);
    }

    if let Err(error) = activate_map_everywhere(st, db, name, &map, headers).await {
        clear_migration_everywhere(st, db, name, headers).await;
        db.set_rebalance_in_progress(false);
        return Err(error);
    }
    clear_migration_everywhere(st, db, name, headers).await;
    db.set_rebalance_in_progress(false);

    let mut value = json!(map.clone());
    value["pending"] = json!(false);
    if !cleanup {
        return Ok(value);
    }

    let cleaned = cleanup_all_replicas(st, db, headers).await?;
    let cleared = cleaned
        .iter()
        .filter_map(|response| response["cleared"].as_array())
        .map(|shards| shards.len() as u64)
        .sum();
    crate::telemetry::record_rebalance_completed(cleared);
    value["cleaned"] = json!(cleaned);
    Ok(value)
}

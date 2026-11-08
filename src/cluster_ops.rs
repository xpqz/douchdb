//! Cluster-wide metadata changes coordinated from one node: shard-map
//! updates, rebalance and security-object propagation.

use crate::cluster::ShardMap;
use crate::cluster_http::{internal_request, ClusterCtx};
use crate::db::{ApiError, Db, DbResult};
use crate::store::ShardState;
use axum::http::{header, HeaderMap, StatusCode};
use futures::future::join_all;
use serde_json::{json, Value};
use std::sync::Arc;

/// Apply a security object locally and push it to every peer.
///
/// The object is versioned, so the local write is kept even when peers are
/// unreachable: anti-entropy (`fabric::reconcile_security`) delivers it later
/// and peers never move back to an older version. Returns whether a majority
/// of the cluster, this node included, holds the object now.
pub(crate) async fn set_security_everywhere(
    ctx: &ClusterCtx,
    db: &Db,
    name: &str,
    value: Value,
    headers: &HeaderMap,
) -> DbResult<bool> {
    let version = db.set_security(value.clone())?;
    if !ctx.cluster.is_clustered() || ctx.cluster.is_internal(headers) {
        return Ok(true);
    }
    let envelope = json!({"version": version, "security": value});
    let responses = join_all(ctx.cluster.peers().map(|peer| {
        let request = internal_request(
            ctx,
            reqwest::Method::PUT,
            peer,
            &["_node", "_local", "_dbs", name, "security"],
            Some(headers),
        )
        .json(&envelope);
        async move {
            request
                .send()
                .await
                .map(|response| response.status().is_success())
                .unwrap_or(false)
        }
    }))
    .await;
    let acknowledgements = 1 + responses.into_iter().filter(|ok| *ok).count();
    Ok(acknowledgements > ctx.cluster.nodes.len().max(1) / 2)
}

fn shard_map_rejected() -> ApiError {
    ApiError::new(
        409,
        "conflict",
        "shard map was rejected in favour of a newer or competing map",
    )
}

pub(crate) async fn apply_shard_map(
    ctx: &ClusterCtx,
    db: &Arc<Db>,
    name: &str,
    mut map: ShardMap,
    headers: &HeaderMap,
) -> DbResult<(ShardMap, bool)> {
    if map.q != db.q || map.node_count != ctx.cluster.nodes.len().max(1) {
        return Err(ApiError::bad_request(
            "shard map does not match database q or cluster node count",
        ));
    }
    let internal = ctx.cluster.is_internal(headers);
    if internal {
        if !db.set_shard_map(map.clone())? {
            return Err(shard_map_rejected());
        }
    } else {
        map = db.set_next_shard_map(map)?;
    }
    if internal {
        db.clear_pending_shard_map()?;
        return Ok((map, false));
    }

    let payload = serde_json::to_vec(&map)
        .map_err(|error| ApiError::new(500, "internal_server_error", error.to_string()))?;
    let peers: Vec<String> = ctx
        .cluster
        .nodes
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != ctx.cluster.me)
        .map(|(_, peer)| peer.clone())
        .collect();
    let responses = futures::future::join_all(peers.into_iter().map(|peer| {
        let payload = payload.clone();
        async move {
            let request = internal_request(
                ctx,
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
    let quorum = ctx.cluster.nodes.len().max(1) / 2 + 1;
    let pending = acknowledgements < quorum;
    if pending {
        db.set_pending_shard_map(&map)?;
    } else {
        db.clear_pending_shard_map()?;
    }
    crate::telemetry::record_shard_map_update(pending);
    Ok((map, pending))
}

async fn fetch_shard_states(
    ctx: &ClusterCtx,
    db: &Arc<Db>,
    node: usize,
    headers: &HeaderMap,
    with_digest: bool,
) -> Option<Vec<ShardState>> {
    if node == ctx.cluster.me {
        return db.shard_states(with_digest).await.ok();
    }
    let request = internal_request(
        ctx,
        reqwest::Method::GET,
        &ctx.cluster.nodes[node],
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

/// Every node that holds a replica of any shard in any of `maps`.
fn owners_of(maps: &[&ShardMap]) -> Vec<usize> {
    let owners: std::collections::BTreeSet<usize> = maps
        .iter()
        .flat_map(|map| map.replicas.iter().flatten().copied())
        .collect();
    owners.into_iter().collect()
}

/// Shard states from `nodes`, indexed by node. Nodes outside `nodes` are not
/// asked and read as unreachable, which `source_required` never looks at.
async fn fetch_states(
    ctx: &ClusterCtx,
    db: &Arc<Db>,
    nodes: &[usize],
    headers: &HeaderMap,
    with_digest: bool,
) -> Vec<Option<Vec<ShardState>>> {
    let fetched = futures::future::join_all(
        nodes
            .iter()
            .map(|node| fetch_shard_states(ctx, db, *node, headers, with_digest)),
    )
    .await;
    let mut states = vec![None; ctx.cluster.nodes.len().max(1)];
    for (node, state) in nodes.iter().zip(fetched) {
        if let Some(slot) = states.get_mut(*node) {
            *slot = state;
        }
    }
    states
}

/// A poll interval that doubles from `first` to `cap` milliseconds, so a long
/// wait does not scan every shard on every node ten times a second.
struct Backoff {
    next: u64,
    cap: u64,
}

impl Backoff {
    fn new(first: u64, cap: u64) -> Backoff {
        Backoff { next: first, cap }
    }

    fn next(&mut self) -> std::time::Duration {
        let current = self.next;
        self.next = (current * 2).min(self.cap);
        std::time::Duration::from_millis(current)
    }
}

fn remaining(deadline: std::time::Instant) -> std::time::Duration {
    deadline.saturating_duration_since(std::time::Instant::now())
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
    ctx: &ClusterCtx,
    db: &Arc<Db>,
    old_map: &ShardMap,
    headers: &HeaderMap,
    grace: std::time::Duration,
) -> DbResult<Vec<ShardState>> {
    let deadline = std::time::Instant::now() + grace;
    let owners = owners_of(&[old_map]);
    let mut delay = Backoff::new(100, 1_000);
    loop {
        let states = fetch_states(ctx, db, &owners, headers, true).await;
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
        tokio::time::sleep(delay.next().min(remaining(deadline))).await;
    }
}

async fn wait_for_rebalance(
    ctx: &ClusterCtx,
    db: &Arc<Db>,
    old_map: &ShardMap,
    map: &ShardMap,
    headers: &HeaderMap,
    timeout_ms: u64,
) -> DbResult<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    let owners = owners_of(&[old_map, map]);
    let mut delay = Backoff::new(250, 2_000);
    // Per node and shard, the sequence the last failed digest comparison saw.
    // A digest scans every document of the shard, and a shard whose sequence
    // has not moved has the same digest, so it is not asked again.
    let mut compared: Option<Vec<Option<Vec<u64>>>> = None;
    loop {
        // Recompute the source state each round. Once writes are frozen the
        // source stops moving, so this converges on the latest acknowledged
        // state rather than a snapshot taken before an in-flight write landed.
        // Counts are cheap and prune most retry rounds; compare digests only
        // after every target has caught up on the source counts.
        let counts = fetch_states(ctx, db, &owners, headers, false).await;
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
        let seqs = counts
            .iter()
            .map(|state| {
                state
                    .as_ref()
                    .map(|shards| shards.iter().map(|shard| shard.seq).collect())
            })
            .collect::<Vec<_>>();
        if counts_ready && compared.as_ref() != Some(&seqs) {
            let digests = fetch_states(ctx, db, &owners, headers, true).await;
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
            // An unreachable node says nothing about the content, so only a
            // comparison that heard from every owner is remembered.
            if owners
                .iter()
                .all(|node| digests.get(*node).is_some_and(Option::is_some))
            {
                compared = Some(seqs);
            }
        }

        if std::time::Instant::now() >= deadline {
            return Err(ApiError::new(
                504,
                "rebalance_timeout",
                "new replicas did not match the old shard content before the timeout",
            ));
        }
        tokio::time::sleep(delay.next().min(remaining(deadline))).await;
    }
}

async fn cleanup_all_replicas(
    ctx: &ClusterCtx,
    db: &Arc<Db>,
    headers: &HeaderMap,
) -> DbResult<Vec<Value>> {
    let responses = futures::future::join_all(ctx.cluster.nodes.iter().map(|peer| {
        let request = internal_request(
            ctx,
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
    ctx: &ClusterCtx,
    peer: &str,
    name: &str,
    map: Option<&ShardMap>,
    headers: &HeaderMap,
) -> Result<(), MigrationRefused> {
    let segments = ["_node", "_local", "_dbs", name, "migration"];
    let request = match map {
        Some(map) => {
            internal_request(ctx, reqwest::Method::PUT, peer, &segments, Some(headers)).json(map)
        }
        None => internal_request(ctx, reqwest::Method::DELETE, peer, &segments, Some(headers)),
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
    ctx: &ClusterCtx,
    db: &Db,
    name: &str,
    map: &ShardMap,
    headers: &HeaderMap,
) -> DbResult<()> {
    db.set_migration_map(map.clone())?;
    let peers: Vec<String> = ctx.cluster.peers().cloned().collect();
    let mut failures = Vec::new();
    // Peers that refused because another migration owns them keep that
    // migration; clearing them would unfreeze the other coordinator.
    let mut foreign = Vec::new();
    if !db.drain_writes(WRITE_DRAIN).await {
        failures.push("local writes did not finish before the freeze".to_string());
    }
    if failures.is_empty() {
        for peer in &peers {
            if let Err(refused) = send_migration(ctx, peer, name, Some(map), headers).await {
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
        let _ = send_migration(ctx, peer, name, None, headers).await;
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

async fn clear_migration_everywhere(ctx: &ClusterCtx, db: &Db, name: &str, headers: &HeaderMap) {
    db.clear_migration_map();
    for peer in ctx.cluster.peers() {
        if let Err(error) = send_migration(ctx, peer, name, None, headers).await {
            tracing::warn!("migration map clear on {}", error.message);
        }
    }
}

async fn activate_map_everywhere(
    ctx: &ClusterCtx,
    db: &Db,
    name: &str,
    map: &ShardMap,
    headers: &HeaderMap,
) -> DbResult<()> {
    let old = db.shard_map();
    db.set_shard_map(map.clone())?;
    let peers: Vec<String> = ctx.cluster.peers().cloned().collect();
    let mut failures = Vec::new();
    for peer in &peers {
        let request = internal_request(
            ctx,
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
            ctx,
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

/// The freeze, copy and activation steps of a rebalance to `map`. The caller
/// holds the rebalance flag; every path out of here releases it.
async fn migrate(
    ctx: &ClusterCtx,
    db: &Arc<Db>,
    name: &str,
    old_map: &ShardMap,
    map: &ShardMap,
    headers: &HeaderMap,
    timeout_ms: u64,
) -> DbResult<Value> {
    // Broadcast the migration map first: it freezes writes on every peer and
    // lets anti-entropy seed the target replicas while reads stay on the old
    // active map.
    if let Err(error) = set_migration_everywhere(ctx, db, name, map, headers).await {
        db.set_rebalance_in_progress(false);
        return Err(error);
    }

    if let Err(error) = stable_source_states(
        ctx,
        db,
        old_map,
        headers,
        // Allow slow digest scans plus any in-flight write to settle.
        std::time::Duration::from_millis(10_000),
    )
    .await
    {
        clear_migration_everywhere(ctx, db, name, headers).await;
        db.set_rebalance_in_progress(false);
        return Err(error);
    }

    if let Err(error) = wait_for_rebalance(ctx, db, old_map, map, headers, timeout_ms).await {
        // The active map never changed, so reads continued on the old owners
        // throughout the migration attempt.
        clear_migration_everywhere(ctx, db, name, headers).await;
        db.set_rebalance_in_progress(false);
        if error.status == 504 {
            crate::telemetry::record_rebalance_timeout();
        }
        return Err(error);
    }

    if let Err(error) = activate_map_everywhere(ctx, db, name, map, headers).await {
        clear_migration_everywhere(ctx, db, name, headers).await;
        db.set_rebalance_in_progress(false);
        return Err(error);
    }
    clear_migration_everywhere(ctx, db, name, headers).await;
    db.set_rebalance_in_progress(false);

    let mut value = json!(map);
    value["pending"] = json!(false);
    Ok(value)
}

/// Move a database to the placement creation would give it on the current
/// cluster; a database already there is left alone. Client
/// writes are frozen on every node while the target replicas catch up, and
/// reads stay on the old map until the new one is activated. With `cleanup`,
/// every node then clears the shards it no longer owns. Returns the new map
/// with `pending` and, after a cleanup, `cleaned`.
pub(crate) async fn rebalance(
    ctx: &ClusterCtx,
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
    // The placement creation uses, so a cluster that has not changed since the
    // database was made is already where it should be.
    let map = ctx.cluster.shard_map_n(db.q, old_map.n());
    let value = if map.q == old_map.q
        && map.node_count == old_map.node_count
        && map.replicas == old_map.replicas
    {
        // Nothing to move: do not freeze writes or bump the version.
        db.set_rebalance_in_progress(false);
        let mut value = json!(old_map);
        value["pending"] = json!(false);
        value
    } else {
        let map = ShardMap {
            version: old_map.version.saturating_add(1),
            ..map
        };
        migrate(ctx, db, name, &old_map, &map, headers, timeout_ms).await?
    };
    if !cleanup {
        return Ok(value);
    }
    let mut value = value;
    let cleaned = cleanup_all_replicas(ctx, db, headers).await?;
    let cleared = cleaned
        .iter()
        .filter_map(|response| response["cleared"].as_array())
        .map(|shards| shards.len() as u64)
        .sum();
    crate::telemetry::record_rebalance_completed(cleared);
    value["cleaned"] = json!(cleaned);
    Ok(value)
}

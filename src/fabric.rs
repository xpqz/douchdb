//! Cluster coordination: shard-map routing, replica selection, quorum
//! reads/writes and internal node calls.

use crate::cluster::ShardMask;
use crate::cluster_http::{
    forward_client_auth, in_request_scope, internal_request, internal_request_raw, send_peer,
    ClusterCtx,
};
use crate::db::{AllDocsOpts, ApiError, ChangesOpts, Db, DbResult, DocOpts};
use crate::params::{doc_ids_from, doc_opts, flag, num, resolve_changes_filter};
use crate::state::AppState;
use axum::http::{HeaderMap, StatusCode};
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// Assign each shard to exactly one node for a fan-out read. Prefer the
/// coordinator's local replica when it holds one: for n=3 on three nodes every
/// node holds every shard, so the whole query can be answered locally instead
/// of issuing one HTTP request per owner. Shards without a local replica stay
/// with their primary owner and can still fall back to other replicas.
pub(crate) fn preferred_shard_masks(ctx: &ClusterCtx, db: &Db) -> Vec<ShardMask> {
    let node_count = ctx.cluster.nodes.len().max(1);
    let mut masks = vec![ShardMask::none(db.q); node_count];
    let map = db.shard_map();
    for shard in 0..db.q {
        let replicas = map.replicas_of(shard);
        let target = if replicas.contains(&ctx.cluster.me) {
            ctx.cluster.me
        } else {
            replicas.first().copied().unwrap_or(0)
        };
        if let Some(mask) = masks.get_mut(target) {
            mask.select(shard as usize);
        }
    }
    masks
}

pub(crate) fn is_replica_of(db: &Db, docid: &str, me: usize) -> bool {
    db.is_replica_of(docid, me)
}

/// Whether a map reported by a peer may replace ours.
///
/// The node count must match this process's cluster size, or routed requests
/// would index past the node list. Equal versions are eligible so that the
/// deterministic tiebreak in `Db::set_shard_map` can settle concurrent updates.
fn adoptable_peer_map(map: &crate::cluster::ShardMap, version: u64, cluster_size: usize) -> bool {
    map.node_count == cluster_size.max(1) && map.version >= version
}

/// Whether `(version, security)` supersedes `(current_version, current)`:
/// newer wins, equal versions fall back to the canonical-form tiebreak that
/// `Db::set_security_versioned` applies.
fn security_supersedes(
    version: u64,
    security: &serde_json::Value,
    current_version: u64,
    current: &serde_json::Value,
) -> bool {
    version > current_version
        || (version == current_version
            && crate::db::canonical_json(security) > crate::db::canonical_json(current))
}

// ---- Batched metadata anti-entropy -----------------------------------------
//
// One exchange per peer covers every database of a pass. The sender posts a
// summary, `{db: (shard map version + placement fingerprint, security version
// + object fingerprint)}`; the peer answers with the entries it holds that
// differ, so a converged cluster costs one request per peer however many
// databases there are. Anti-entropy then applies what came back with the
// setters `set_shard_map` and `set_security_versioned`, which own the version
// and tiebreak rules of the shard map and security reconciliation. Entries the peer is behind
// on are pushed in one more request to that peer, only when there are any.

/// Databases per exchange, to bound the size of one request body. A pass over
/// more databases than this makes one exchange per peer per batch.
const METADATA_BATCH: usize = 1000;

/// A metadata version and a fingerprint of its content.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Stamp {
    version: u64,
    fingerprint: String,
}

fn fingerprint(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn shard_map_stamp(map: &crate::cluster::ShardMap) -> Stamp {
    let content = serde_json::to_vec(&(map.q, map.node_count, &map.replicas)).unwrap_or_default();
    Stamp {
        version: map.version,
        fingerprint: fingerprint(&content),
    }
}

pub(crate) fn security_stamp(version: u64, security: &Value) -> Stamp {
    Stamp {
        version,
        fingerprint: fingerprint(crate::db::canonical_json(security).as_bytes()),
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct DbSummary {
    shard_map: Stamp,
    security: Stamp,
}

/// A security object with its cluster version, as stored.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct SecurityEntry {
    pub version: u64,
    pub security: Value,
}

/// Metadata of one database: what a peer holds that differs from a summary,
/// or what is pushed to a peer that is behind.
#[derive(Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct DbEntries {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_map: Option<crate::cluster::ShardMap>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security: Option<SecurityEntry>,
}

/// Which pushed entries the receiver stored.
#[derive(Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct Accepted {
    pub shard_map: bool,
    pub security: bool,
}

/// Body of `POST /_node/_local/_metadata_sync`: a summary to compare with,
/// and entries to apply. Either may be empty.
#[derive(Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct MetadataSync {
    #[serde(default)]
    pub summary: BTreeMap<String, DbSummary>,
    #[serde(default)]
    pub push: BTreeMap<String, DbEntries>,
}

/// Answer to [`MetadataSync`]: the receiver's own entries that differ from the
/// summary, the databases it does not have, and what became of the pushes.
#[derive(Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct MetadataSyncReply {
    #[serde(default)]
    pub differs: BTreeMap<String, DbEntries>,
    #[serde(default)]
    pub missing: BTreeSet<String>,
    #[serde(default)]
    pub accepted: BTreeMap<String, Accepted>,
}

/// The shard map anti-entropy spreads for `db`: the pending one if it is at
/// least as new as the stored one, else the stored one.
fn desired_shard_map(db: &Db) -> DbResult<crate::cluster::ShardMap> {
    let local = db.shard_map();
    Ok(db
        .pending_shard_map()?
        .filter(|pending| pending.version >= local.version)
        .unwrap_or(local))
}

/// The receiver's side of the exchange: compare `summary` with what the local
/// databases hold. Returns the entries that differ and the databases that do
/// not exist here.
pub(crate) async fn metadata_differences(
    state: &AppState,
    summary: &BTreeMap<String, DbSummary>,
) -> (BTreeMap<String, DbEntries>, BTreeSet<String>) {
    let mut differs = BTreeMap::new();
    let mut missing = BTreeSet::new();
    for (name, theirs) in summary {
        let Ok(db) = state.db_cold(name).await else {
            missing.insert(name.clone());
            continue;
        };
        let mut entries = DbEntries::default();
        let map = db.shard_map();
        if shard_map_stamp(&map) != theirs.shard_map {
            entries.shard_map = Some(map);
        }
        let (version, security) = db.security_versioned();
        if security_stamp(version, &security) != theirs.security {
            entries.security = Some(SecurityEntry { version, security });
        }
        if entries.shard_map.is_some() || entries.security.is_some() {
            differs.insert(name.clone(), entries);
        }
        drop(db);
        state.release_cold().await;
    }
    (differs, missing)
}

/// How a peer took part in the exchange.
enum PeerReply {
    Answered(MetadataSyncReply),
    /// Unreachable or failed; tried again on the next pass.
    Unavailable,
}

async fn metadata_exchange(ctx: &ClusterCtx, peer: &str, body: &MetadataSync) -> PeerReply {
    let request = internal_request(
        ctx,
        reqwest::Method::POST,
        peer,
        &["_node", "_local", "_metadata_sync"],
        None,
    )
    .json(body);
    let Ok(response) = send_peer(request).await else {
        return PeerReply::Unavailable;
    };
    if !response.status().is_success() {
        return PeerReply::Unavailable;
    }
    match response.json::<MetadataSyncReply>().await {
        Ok(reply) => PeerReply::Answered(reply),
        Err(_) => PeerReply::Unavailable,
    }
}

/// Reconcile shard maps and security objects of `names` with every peer: one
/// exchange per peer per batch of databases, plus one push to each peer that
/// is behind on something.
pub(crate) async fn sync_metadata(state: &Arc<AppState>, names: &[String]) {
    if !state.ctx.cluster.is_clustered() {
        return;
    }
    for batch in names.chunks(METADATA_BATCH) {
        sync_metadata_batch(state, batch).await;
    }
}

async fn sync_metadata_batch(state: &Arc<AppState>, names: &[String]) {
    let ctx = &state.ctx;
    let peers: Vec<String> = ctx.cluster.peers().cloned().collect();

    let mut sent = MetadataSync::default();
    for name in names {
        let Ok(db) = state.db_cold(name).await else {
            continue;
        };
        let (version, security) = db.security_versioned();
        let desired = match desired_shard_map(&db) {
            Ok(desired) => desired,
            Err(error) => {
                tracing::warn!(db = %db.name, error = %error.reason, "cannot read the pending shard map");
                continue;
            }
        };
        sent.summary.insert(
            name.clone(),
            DbSummary {
                shard_map: shard_map_stamp(&desired),
                security: security_stamp(version, &security),
            },
        );
        drop(db);
        state.release_cold().await;
    }
    if sent.summary.is_empty() {
        return;
    }

    let replies = crate::cluster_http::in_one_round(join_all(
        peers
            .iter()
            .map(|peer| async { (peer.clone(), metadata_exchange(ctx, peer, &sent).await) }),
    ))
    .await;
    let mut answered = Vec::new();
    for (peer, reply) in replies {
        match reply {
            PeerReply::Answered(reply) => answered.push((peer, reply)),
            PeerReply::Unavailable => {}
        }
    }

    // Adopt what the peers hold, and work out who is behind.
    let mut pushes: BTreeMap<String, MetadataSync> = BTreeMap::new();
    let mut pending: BTreeMap<String, usize> = BTreeMap::new();
    for (name, mine) in &sent.summary {
        if answered.is_empty() {
            break;
        }
        let Ok(db) = state.db_cold(name).await else {
            continue;
        };
        let holders: Vec<(&String, &MetadataSyncReply)> = answered
            .iter()
            .filter(|(_, reply)| !reply.missing.contains(name))
            .map(|(peer, reply)| (peer, reply))
            .collect();

        let mut desired = match desired_shard_map(&db) {
            Ok(desired) => desired,
            Err(error) => {
                tracing::warn!(db = %db.name, error = %error.reason, "cannot read the pending shard map");
                continue;
            }
        };
        for (_, reply) in &holders {
            let Some(map) = reply
                .differs
                .get(name)
                .and_then(|entries| entries.shard_map.as_ref())
            else {
                continue;
            };
            if adoptable_peer_map(map, desired.version, ctx.cluster.nodes.len())
                && db.set_shard_map(map.clone()).unwrap_or(false)
            {
                if db
                    .pending_shard_map()
                    .ok()
                    .flatten()
                    .is_some_and(|pending| pending.version <= map.version)
                {
                    // The adopted map supersedes (or beat the tiebreak against) ours.
                    if let Err(error) = db.clear_pending_shard_map() {
                        tracing::warn!(db = %db.name, %error, "cannot clear the pending shard map");
                    }
                }
                desired = map.clone();
            }
        }
        let desired_stamp = shard_map_stamp(&desired);
        let mut current = 1usize;
        for (peer, reply) in &holders {
            // A peer that sent no map holds the one we summarised.
            let held = reply
                .differs
                .get(name)
                .and_then(|entries| entries.shard_map.as_ref())
                .map(shard_map_stamp)
                .unwrap_or_else(|| mine.shard_map.clone());
            if held == desired_stamp {
                current += 1;
            } else {
                pushes
                    .entry((*peer).clone())
                    .or_default()
                    .push
                    .entry(name.clone())
                    .or_default()
                    .shard_map = Some(desired.clone());
            }
        }
        if !matches!(db.pending_shard_map(), Ok(None)) {
            pending.insert(name.clone(), current);
        }

        let (mut version, mut security) = db.security_versioned();
        for (_, reply) in &holders {
            let Some(entry) = reply
                .differs
                .get(name)
                .and_then(|entries| entries.security.as_ref())
            else {
                continue;
            };
            if security_supersedes(entry.version, &entry.security, version, &security)
                && db
                    .set_security_versioned(entry.version, entry.security.clone())
                    .unwrap_or(false)
            {
                version = entry.version;
                security = entry.security.clone();
            }
        }
        let security_now = security_stamp(version, &security);
        for (peer, reply) in &holders {
            let held = reply
                .differs
                .get(name)
                .and_then(|entries| entries.security.as_ref())
                .map(|entry| security_stamp(entry.version, &entry.security))
                .unwrap_or_else(|| mine.security.clone());
            if held != security_now {
                pushes
                    .entry((*peer).clone())
                    .or_default()
                    .push
                    .entry(name.clone())
                    .or_default()
                    .security = Some(SecurityEntry {
                    version,
                    security: security.clone(),
                });
            }
        }
        drop(db);
        state.release_cold().await;
    }

    // Push to the peers that are behind; each one that stores a pending map
    // counts as an acknowledgement of it.
    let pushed =
        crate::cluster_http::in_one_round(join_all(pushes.iter().map(|(peer, body)| async move {
            let request = internal_request(
                ctx,
                reqwest::Method::POST,
                peer,
                &["_node", "_local", "_metadata_sync"],
                None,
            )
            .json(body);
            let reply = match send_peer(request).await {
                Ok(response) if response.status().is_success() => {
                    response.json::<MetadataSyncReply>().await.ok()
                }
                _ => None,
            };
            (peer.clone(), reply)
        })))
        .await;
    for (_, reply) in &pushed {
        for (name, accepted) in reply.iter().flat_map(|reply| reply.accepted.iter()) {
            if accepted.shard_map {
                if let Some(count) = pending.get_mut(name) {
                    *count += 1;
                }
            }
        }
    }
    let quorum_floor = ctx.cluster.nodes.len().max(1) / 2;
    for (name, acknowledgements) in &pending {
        if *acknowledgements <= quorum_floor {
            continue;
        }
        let Ok(db) = state.db_cold(name).await else {
            continue;
        };
        if let Err(error) = db.clear_pending_shard_map() {
            tracing::warn!(db = %db.name, %error, "cannot clear the pending shard map");
        }
        drop(db);
        state.release_cold().await;
    }
}

/// Ask every replica for a document. With `open_all`, each peer returns all
/// leaves (`open_revs=all`) and their histories; otherwise the single winner
/// (or the client's explicit `rev`) is returned. Explicit not-found answers
/// count as reachable, but a transport or server failure does not.
pub(crate) async fn replica_docs(
    ctx: &ClusterCtx,
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

    let mut replicas = db.replicas_of_doc(docid);
    if let Some(index) = replicas.iter().position(|node| *node == ctx.cluster.me) {
        let local = replicas.remove(index);
        replicas.insert(0, local);
    }
    let responses = crate::cluster_http::in_one_round(join_all(replicas.into_iter().map(|node| {
        let inner = inner.clone();
        let opts = opts.clone();
        async move {
            if node == ctx.cluster.me {
                crate::telemetry::record_local_read();
                return match db.get_doc(docid, &opts).await {
                    Ok(doc) => (true, Some(doc)),
                    Err(error) => (error.status == 404, None),
                };
            }
            crate::telemetry::record_remote_read();
            let peer = ctx.cluster.nodes[node].clone();
            let request = internal_request(
                ctx,
                reqwest::Method::GET,
                &peer,
                &[dbname, docid],
                Some(headers),
            )
            .header("x-douchdb-quorum-read", "1")
            .query(&inner);
            match send_peer(request).await {
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
    })))
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

pub(crate) struct Reconciled {
    pub(crate) winner: Value,
    pub(crate) conflicts: Vec<String>,
    pub(crate) deleted_conflicts: Vec<String>,
    pub(crate) history: Vec<String>,
}

/// Pure merged-tree reconciliation. `docs` may contain duplicate revision ids
/// with different retained history lengths; ancestry is merged from every
/// response before bodies are deduplicated, and the longest history wins
/// deterministically. The result must not depend on input order.
pub(crate) fn reconcile_leaves(docs: &[Value], requested_rev: Option<&str>) -> Option<Reconciled> {
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

/// `new_edits=true` sends each document to every replica in its shard set and
/// returns once `w` of them acknowledge (CouchDB-style; revision ids are
/// deterministic, so no primary re-fetch/copy pass is needed).
/// `new_edits=false` (the replication path) sends the given revision to every
/// replica directly. The bool is whether the default write quorum was met.
pub(crate) async fn commit_docs(
    st: &AppState,
    db: &Arc<Db>,
    dbname: &str,
    mut docs: Vec<Value>,
    new_edits: bool,
    w: usize,
    headers: &HeaderMap,
) -> DbResult<(Vec<Value>, bool)> {
    let ctx = &st.ctx;
    if !ctx.cluster.is_clustered() {
        let results = db.put_docs(docs, new_edits).await?;
        if results.iter().any(|result| result.get("ok").is_some()) {
            st.db_updates.record(dbname, "updated");
        }
        return Ok((results, true));
    }

    // Hold the write gate until this write and its detached replica writes
    // finish, so a rebalance freeze can drain them. Checking the freeze again
    // under the gate closes the window between the request-entry check and
    // acquiring it.
    let gate = Arc::new(db.write_guard().await);
    if db.rebalance_in_progress() && !ctx.cluster.is_internal(headers) {
        return Err(ApiError::new(
            503,
            "rebalance_in_progress",
            "writes are paused while this database changes shard placement",
        ));
    }

    let ids = assign_ids(&mut docs);
    let (primaries, replicas): (Vec<usize>, Vec<Vec<usize>>) =
        ids.iter().map(|id| db.placement_of_doc(id)).unzip();
    let docs: Vec<Arc<Value>> = docs.into_iter().map(Arc::new).collect();
    let groups = group_by_node(&docs, &replicas);
    crate::telemetry::record_fanout(groups.len() as u64);

    let (out, met) = if new_edits {
        let tally = QuorumTally::new(&ids, &primaries, &replicas, write_quorum(w, db.copies()));
        let pending = spawn_quorum_writes(st, db, dbname, groups, headers, &gate).await;
        collect_quorum(pending, tally).await
    } else {
        let responses = replicate_to_all(st, db, dbname, groups, headers).await;
        merge_replicated(&ids, &primaries, responses)
    };
    if !met {
        crate::telemetry::record_quorum_failure();
    }
    if out.iter().any(|result| result.get("ok").is_some()) {
        st.db_updates.record(dbname, "updated");
    }
    Ok((out, met))
}

/// What `_purge` reports for one document (`fabric_doc_purge:format_resps`).
pub(crate) enum PurgeOutcome {
    /// At least `w` copies answered and they agree on the revisions purged.
    Ok(Vec<String>),
    /// Some copy purged, but fewer than `w` did or the copies disagree.
    Accepted(Vec<String>),
    /// No copy purged: the first failure.
    Error(ApiError),
}

/// `fabric:purge_docs`: send each document's purge to every copy of its
/// shard and judge each document by the write quorum. The coordinator's own
/// copy is purged in-process and the others through the forwarded `_purge`
/// call, which only applies to the receiving node. A copy that is down or
/// fails counts against the quorum but does not stop the others.
pub(crate) async fn purge_docs(
    st: &AppState,
    db: &Db,
    dbname: &str,
    request: &serde_json::Map<String, Value>,
    w: usize,
    headers: &HeaderMap,
) -> Vec<(String, PurgeOutcome)> {
    let ctx = &st.ctx;
    let mut groups: HashMap<usize, serde_json::Map<String, Value>> = HashMap::new();
    let mut copies: Vec<usize> = Vec::new();
    for (id, revs) in request {
        let (_, replicas) = db.placement_of_doc(id);
        copies.push(replicas.len());
        for node in replicas {
            groups
                .entry(node)
                .or_default()
                .insert(id.clone(), revs.clone());
        }
    }
    crate::telemetry::record_fanout(groups.len() as u64);

    let replies = crate::cluster_http::in_one_round(join_all(groups.into_iter().map(
        |(node, sub)| async move {
            let body = Value::Object(sub);
            let reply = if node == ctx.cluster.me {
                db.purge(&body, ctx.cluster.me).await
            } else {
                match ctx.cluster.nodes.get(node) {
                    Some(peer) => {
                        forward_json(ctx, peer, dbname, "_purge", "", &body, headers).await
                    }
                    None => Err(nodedown()),
                }
            };
            (body, reply)
        },
    )))
    .await;

    let mut tallies: HashMap<&str, (Vec<Vec<String>>, Option<ApiError>)> = HashMap::new();
    for (body, reply) in &replies {
        let ids = body.as_object().into_iter().flat_map(|sub| sub.keys());
        for id in ids {
            let (oks, failure) = tallies.entry(id.as_str()).or_default();
            match reply {
                Ok(reply) => match purged_revs(reply, id) {
                    Some(revs) => oks.push(revs),
                    None => {
                        failure.get_or_insert_with(|| {
                            ApiError::internal("replica sent a malformed _purge reply")
                        });
                    }
                },
                Err(e) => {
                    failure
                        .get_or_insert_with(|| ApiError::new(e.status, e.error, e.reason.clone()));
                }
            }
        }
    }

    request
        .keys()
        .zip(copies)
        .map(|(id, copies)| {
            let (oks, failure) = tallies.remove(id.as_str()).unwrap_or_default();
            let outcome = if oks.is_empty() {
                PurgeOutcome::Error(
                    failure.unwrap_or_else(|| ApiError::internal("no replica answered the purge")),
                )
            } else {
                let all: BTreeSet<&String> = oks.iter().flatten().collect();
                let all = all.into_iter().cloned().collect();
                let agree = oks.windows(2).all(|pair| pair[0] == pair[1]);
                if oks.len() >= write_quorum(w, copies) && agree {
                    PurgeOutcome::Ok(all)
                } else {
                    PurgeOutcome::Accepted(all)
                }
            };
            (id.clone(), outcome)
        })
        .collect()
}

/// The sorted revisions one copy reports purged for `id`.
fn purged_revs(reply: &Value, id: &str) -> Option<Vec<String>> {
    let mut revs: Vec<String> = reply
        .get("purged")?
        .get(id)?
        .as_array()?
        .iter()
        .map(|rev| rev.as_str().map(str::to_string))
        .collect::<Option<_>>()?;
    revs.sort();
    Some(revs)
}

/// Routing needs ids, so generate them before grouping. `Db::put_docs` would
/// otherwise assign them per replica and the copies would diverge.
fn assign_ids(docs: &mut [Value]) -> Vec<String> {
    for doc in docs.iter_mut() {
        if doc.get("_id").is_none() {
            doc["_id"] = json!(crate::util::uuid());
        }
    }
    docs.iter()
        .map(|doc| {
            doc.get("_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        })
        .collect()
}

/// CouchDB defaults to a write quorum and lets `?w=` override it.
fn write_quorum(w: usize, copies: usize) -> usize {
    if w == 0 {
        copies / 2 + 1
    } else {
        w.min(copies)
    }
}

type SlotDocs = Vec<(usize, Arc<Value>)>;
/// One node's answer: the node, the document slots it was sent, and the
/// per-document results or the failure to reach it.
type NodeReply = (usize, Vec<usize>, DbResult<Vec<Value>>);

/// The documents each node is to receive, with each document's slot in the
/// request.
fn group_by_node(docs: &[Arc<Value>], replicas: &[Vec<usize>]) -> HashMap<usize, SlotDocs> {
    let mut groups: HashMap<usize, SlotDocs> = HashMap::new();
    for (slot, doc) in docs.iter().enumerate() {
        for node in replicas.get(slot).into_iter().flatten() {
            groups.entry(*node).or_default().push((slot, doc.clone()));
        }
    }
    groups
}

fn replica_unavailable(id: &str) -> Value {
    json!({
        "id": id,
        "error": "replica_unavailable",
        "reason": "no replica in the shard set is reachable",
    })
}

/// Start one detached write per node. Each task holds the write gate, so a
/// slow or unreachable replica keeps running after the request returns. Each
/// also holds a detached-write slot, so a slow peer makes later writes wait
/// rather than accumulate tasks without limit.
async fn spawn_quorum_writes(
    st: &AppState,
    db: &Arc<Db>,
    dbname: &str,
    groups: HashMap<usize, SlotDocs>,
    headers: &HeaderMap,
    gate: &Arc<impl Send + Sync + 'static>,
) -> FuturesUnordered<tokio::task::JoinHandle<NodeReply>> {
    let ctx = &st.ctx;
    let pending = FuturesUnordered::new();
    // One round for all the replica writes, however far apart they start or
    // finish; each remote task keeps it open until its call is done.
    let round = crate::cluster_http::peer_round();
    for (node, group) in groups {
        let slots: Vec<usize> = group.iter().map(|(slot, _)| *slot).collect();
        let slot = st.admission.acquire_detached().await;
        if node == ctx.cluster.me {
            let subdocs: Vec<Value> = group.into_iter().map(|(_, doc)| (*doc).clone()).collect();
            let db = db.clone();
            let gate = gate.clone();
            pending.push(spawn_node_write(node, slots, async move {
                let (_gate, _slot) = (gate, slot);
                db.put_docs(subdocs, true).await
            }));
        } else {
            let subdocs: Vec<Arc<Value>> = group.into_iter().map(|(_, doc)| doc).collect();
            let client = ctx.client.clone();
            let secret = ctx.cluster.secret.clone();
            let peer = ctx.cluster.nodes[node].clone();
            let dbname = dbname.to_string();
            let headers = headers.clone();
            let gate = gate.clone();
            let round = round.clone();
            pending.push(spawn_node_write(node, slots, async move {
                let (_gate, _slot, _round) = (gate, slot, round);
                forward_bulk_owned(client, secret, peer, dbname, subdocs, true, headers).await
            }));
        }
    }
    pending
}

/// Run one node's write as a detached task that reports which node and slots
/// it answered for. A panic is caught and reported as that node's failure: the
/// join handle would otherwise lose the node and slots, and the tally would
/// keep waiting for a reply that is never coming.
fn spawn_node_write(
    node: usize,
    slots: Vec<usize>,
    write: impl std::future::Future<Output = DbResult<Vec<Value>>> + Send + 'static,
) -> tokio::task::JoinHandle<NodeReply> {
    use futures::FutureExt;
    // The task keeps counting its peer calls against the client request.
    let write = in_request_scope(write);
    tokio::spawn(async move {
        let result = std::panic::AssertUnwindSafe(write)
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(ApiError::internal("replica write task panicked")));
        (node, slots, result)
    })
}

/// Return as soon as the tally is settled. The remaining replica writes keep
/// running as detached tasks, so a slow or unreachable replica no longer sets
/// write tail latency.
async fn collect_quorum(
    mut pending: FuturesUnordered<tokio::task::JoinHandle<NodeReply>>,
    mut tally: QuorumTally<'_>,
) -> (Vec<Value>, bool) {
    while let Some(joined) = pending.next().await {
        // Panics are caught in the task, so only a cancelled task (runtime
        // shutdown) fails to join.
        let Ok((node, slots, response)) = joined else {
            continue;
        };
        tally.record(node, slots, response.ok());
        if tally.settled() {
            break;
        }
    }
    let met = tally.met();
    (tally.finish(), met)
}

/// Pure bookkeeping for a quorum write: per-document acknowledgement counts
/// and the result to report for each document.
struct QuorumTally<'a> {
    ids: &'a [String],
    primaries: &'a [usize],
    quorum: usize,
    acks: Vec<usize>,
    successes: Vec<Option<Value>>,
    failures: Vec<Option<Value>>,
    /// Replies still owed per document. Design documents go to every node, and
    /// any node may coordinate a query against them, so their write waits for
    /// every replica to answer rather than only the quorum.
    unanswered: Vec<usize>,
}

impl<'a> QuorumTally<'a> {
    fn new(
        ids: &'a [String],
        primaries: &'a [usize],
        replicas: &[Vec<usize>],
        quorum: usize,
    ) -> Self {
        let total = ids.len();
        let unanswered = ids
            .iter()
            .enumerate()
            .map(|(slot, id)| {
                if id.starts_with("_design/") {
                    replicas.get(slot).map_or(0, Vec::len)
                } else {
                    0
                }
            })
            .collect();
        QuorumTally {
            ids,
            primaries,
            quorum,
            acks: vec![0; total],
            successes: vec![None; total],
            failures: vec![None; total],
            unanswered,
        }
    }

    /// Record one node's reply for the given slots; `None` means the node
    /// could not be reached.
    fn record(&mut self, node: usize, slots: Vec<usize>, response: Option<Vec<Value>>) {
        for slot in &slots {
            if let Some(left) = self.unanswered.get_mut(*slot) {
                *left = left.saturating_sub(1);
            }
        }
        match response {
            Some(results) => {
                for (slot, result) in slots.into_iter().zip(results) {
                    let is_primary = self.primaries.get(slot) == Some(&node);
                    let (Some(ack), Some(success), Some(failure)) = (
                        self.acks.get_mut(slot),
                        self.successes.get_mut(slot),
                        self.failures.get_mut(slot),
                    ) else {
                        continue;
                    };
                    if result.get("error").is_none() {
                        *ack += 1;
                        if success.is_none() || is_primary {
                            *success = Some(result);
                        }
                    } else if failure.is_none() || is_primary {
                        *failure = Some(result);
                    }
                }
            }
            None => {
                for slot in slots {
                    if let (Some(failure), Some(id)) =
                        (self.failures.get_mut(slot), self.ids.get(slot))
                    {
                        if failure.is_none() {
                            *failure = Some(replica_unavailable(id));
                        }
                    }
                }
            }
        }
    }

    /// Every document has its quorum and no design document is still owed a
    /// reply.
    fn settled(&self) -> bool {
        self.met() && self.unanswered.iter().all(|left| *left == 0)
    }

    /// Every document has at least `quorum` acknowledgements.
    fn met(&self) -> bool {
        self.acks.iter().all(|ack| *ack >= self.quorum)
    }

    /// The result per document: a success if any replica acknowledged (the
    /// primary's if it did), else a failure (the primary's if it reported one),
    /// else `replica_unavailable`.
    fn finish(self) -> Vec<Value> {
        let ids = self.ids;
        self.successes
            .into_iter()
            .zip(self.failures)
            .enumerate()
            .map(|(slot, (success, failure))| {
                success.or(failure).unwrap_or_else(|| {
                    replica_unavailable(ids.get(slot).map_or("", String::as_str))
                })
            })
            .collect()
    }
}

/// Replication writes carry their own history and must reach every replica,
/// including ones that are not the primary. A down replica is not fatal:
/// anti-entropy will heal it when it returns.
async fn replicate_to_all(
    st: &AppState,
    db: &Db,
    dbname: &str,
    mut groups: HashMap<usize, SlotDocs>,
    headers: &HeaderMap,
) -> Vec<NodeReply> {
    let ctx = &st.ctx;
    let mut responses = Vec::new();
    if let Some(group) = groups.remove(&ctx.cluster.me) {
        let slots: Vec<usize> = group.iter().map(|(slot, _)| *slot).collect();
        let subdocs: Vec<Value> = group.into_iter().map(|(_, doc)| (*doc).clone()).collect();
        responses.push((ctx.cluster.me, slots, db.put_docs(subdocs, false).await));
    }
    let remote = crate::cluster_http::in_one_round(futures::future::join_all(
        groups.into_iter().map(|(node, group)| {
            let slots: Vec<usize> = group.iter().map(|(slot, _)| *slot).collect();
            let subdocs: Vec<Arc<Value>> = group.into_iter().map(|(_, doc)| doc).collect();
            let client = ctx.client.clone();
            let secret = ctx.cluster.secret.clone();
            let peer = ctx.cluster.nodes[node].clone();
            let dbname = dbname.to_string();
            let headers = headers.clone();
            async move {
                (
                    node,
                    slots,
                    forward_bulk_owned(client, secret, peer, dbname, subdocs, false, headers).await,
                )
            }
        }),
    ))
    .await;
    responses.extend(remote);
    responses
}

/// Merge the replies of a replication write: the primary's verdict wins, and
/// the write is complete only if every document reached some replica.
fn merge_replicated(
    ids: &[String],
    primaries: &[usize],
    responses: Vec<NodeReply>,
) -> (Vec<Value>, bool) {
    let total = ids.len();
    let mut out = vec![Value::Null; total];
    let mut reached = vec![false; total];
    for (node, slots, response) in responses {
        match response {
            Ok(results) => {
                // A replica that took a `new_edits=false` write answers with
                // the failures only, so the reply can be shorter than the
                // batch. Answering at all is the acknowledgement; the failures
                // are matched to their documents by id.
                let full = results.len() == slots.len();
                for (position, slot) in slots.iter().enumerate() {
                    let (Some(reached), Some(out)) = (reached.get_mut(*slot), out.get_mut(*slot))
                    else {
                        continue;
                    };
                    *reached = true;
                    let result = if full {
                        results.get(position)
                    } else {
                        results.iter().find(|result| {
                            result.get("id").and_then(Value::as_str)
                                == ids.get(*slot).map(String::as_str)
                        })
                    };
                    if let Some(result) = result {
                        if primaries.get(*slot) == Some(&node) || out.is_null() {
                            *out = result.clone();
                        }
                    }
                }
            }
            Err(_) => {
                for slot in slots {
                    if let (Some(out), Some(id)) = (out.get_mut(slot), ids.get(slot)) {
                        if out.is_null() {
                            *out = replica_unavailable(id);
                        }
                    }
                }
            }
        }
    }
    let met = reached.iter().all(|ok| *ok);
    (out, met)
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
    let request = internal_request_raw(
        &client,
        &secret,
        reqwest::Method::POST,
        &peer,
        &[&dbname, "_bulk_docs"],
    )
    .json(&BulkBody {
        docs: &docs,
        new_edits,
    });
    let request = forward_client_auth(request, &headers);
    let response = send_peer(request)
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
    ctx: &ClusterCtx,
    peer: &str,
    dbname: &str,
    endpoint: &str,
    query: &str,
    body: &Value,
    headers: &HeaderMap,
) -> DbResult<Value> {
    let params: Vec<(&str, &str)> = query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .collect();
    let request = internal_request(
        ctx,
        reqwest::Method::POST,
        peer,
        &[dbname, endpoint],
        Some(headers),
    )
    .query(&params)
    .json(body);
    let response = send_peer(request)
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

/// CouchDB's `{error, {nodedown, <<"progress not possible">>}}`, which chttpd
/// maps to 412 (`error_info({error, {nodedown, Reason}})`).
pub(crate) fn nodedown() -> ApiError {
    ApiError::new(412, "nodedown", "progress not possible")
}

/// True when a peer request failed in transport (connect, timeout, reset, or
/// no usable response), rather than because the peer answered with an error.
/// Every such failure is built as 502 `bad_gateway`; a peer's own error
/// response keeps its status under another name (`request_failed`,
/// `all_docs_failed`, ...), so a 503 or 504 the peer produced is an answer,
/// not a node that is down.
pub(crate) fn peer_unreachable(error: &ApiError) -> bool {
    error.status == 502 && error.error == "bad_gateway"
}

/// Reassign the shards of failed nodes to live replicas. A shard with no live
/// copy is CouchDB's `{error, {nodedown, <<"progress not possible">>}}`
/// (fabric_streams, fabric_view:check_down_shards), which chttpd maps to 412.
fn fallback_shard_masks(
    db: &Db,
    owned_by_node: &[ShardMask],
    failed: &[usize],
    alive: &[usize],
) -> DbResult<Vec<(usize, ShardMask)>> {
    let mut groups: HashMap<usize, ShardMask> = HashMap::new();
    for &node in failed {
        for shard in 0..db.q {
            if !owned_by_node
                .get(node)
                .is_some_and(|owned| owned.covers(shard as usize))
            {
                continue;
            }
            let target = db
                .replicas_of_shard(shard)
                .into_iter()
                .find(|candidate| alive.contains(candidate))
                .ok_or_else(nodedown)?;
            groups
                .entry(target)
                .or_insert_with(|| ShardMask::none(db.q))
                .select(shard as usize);
        }
    }
    Ok(groups.into_iter().collect())
}

/// What every cluster read needs: the database to read, and the client's
/// headers to forward to peers.
#[derive(Clone, Copy)]
pub(crate) struct ClusterRead<'a> {
    pub ctx: &'a ClusterCtx,
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
///
/// A peer that discovery found unreachable is not called: its shards go to a
/// replica in the first round, together with the other requests. If that
/// leaves some shard without a live copy the mark is distrusted (it may be
/// stale) and the node is tried like any other.
async fn scatter<T, F, Fut>(
    read: ClusterRead<'_>,
    owned_by_node: &[ShardMask],
    what: &str,
    fetch: F,
) -> DbResult<Vec<(usize, Round, T)>>
where
    F: Fn(usize, ShardMask, Round) -> Fut,
    Fut: std::future::Future<Output = DbResult<T>>,
{
    scatter_nodes(read, owned_by_node, what, false, &[], fetch).await
}

/// [`scatter`], optionally also asking the nodes that own no shard (with an
/// empty mask), so that one round both reads the shards and learns which
/// nodes answer. Nodes in `avoid` are routed around like nodes known to be
/// down, as long as every shard keeps a live copy without them.
async fn scatter_nodes<T, F, Fut>(
    read: ClusterRead<'_>,
    owned_by_node: &[ShardMask],
    what: &str,
    include_idle: bool,
    avoid: &[usize],
    fetch: F,
) -> DbResult<Vec<(usize, Round, T)>>
where
    F: Fn(usize, ShardMask, Round) -> Fut,
    Fut: std::future::Future<Output = DbResult<T>>,
{
    let ClusterRead { ctx, db, .. } = read;
    let nodes = ctx.cluster.nodes.len();

    let known_down: Vec<usize> = (0..nodes)
        .filter(|&node| node != ctx.cluster.me && ctx.health.is_down(node))
        .collect();
    let mut excluded: Vec<usize> = Vec::new();
    let mut rerouted: Vec<(usize, ShardMask, Round)> = Vec::new();
    let mut avoided = known_down.clone();
    avoided.extend(
        avoid
            .iter()
            .copied()
            .filter(|node| *node != ctx.cluster.me && !known_down.contains(node)),
    );
    for skipped in [avoided, known_down] {
        if skipped.is_empty() {
            continue;
        }
        let alive: Vec<usize> = (0..nodes).filter(|node| !skipped.contains(node)).collect();
        if let Ok(groups) = fallback_shard_masks(db, owned_by_node, &skipped, &alive) {
            if !groups.is_empty() {
                crate::telemetry::record_degraded_read();
            }
            rerouted = groups
                .into_iter()
                .map(|(node, mask)| (node, mask, Round::Fallback))
                .collect();
            excluded = skipped;
            break;
        }
    }
    let mut requests: Vec<(usize, ShardMask, Round)> = owned_by_node
        .iter()
        .cloned()
        .enumerate()
        .filter(|(node, mask)| !excluded.contains(node) && (include_idle || !mask.is_empty()))
        .map(|(node, mask)| (node, mask, Round::Owner))
        .collect();
    requests.extend(rerouted);
    crate::telemetry::record_fanout(requests.len() as u64);

    let mut values = Vec::new();
    // The nodes that were asked and failed, as against `excluded`, which also
    // holds nodes only skipped because discovery marked them down.
    let mut failed_nodes: Vec<usize> = Vec::new();
    // Only the first dispatch may fail over: a failed fallback fails the read.
    let mut first = true;
    loop {
        let responses = crate::cluster_http::in_one_round(join_all(requests.into_iter().map(
            |(node, mask, round)| {
                let response = fetch(node, mask.clone(), round);
                async move { (node, mask, round, response.await) }
            },
        )))
        .await;

        let mut lost = vec![ShardMask::none(db.q); nodes];
        let mut failed: Vec<usize> = Vec::new();
        for (node, mask, round, response) in responses {
            match response {
                Ok(value) => values.push((node, round, value)),
                Err(error) if first => {
                    tracing::warn!(
                        "{what} read from node {node} failed: {} {}",
                        error.status,
                        error.reason
                    );
                    if !failed.contains(&node) {
                        failed.push(node);
                    }
                    if let Some(slot) = lost.get_mut(node) {
                        for shard in mask.shards() {
                            slot.select(shard);
                        }
                    }
                }
                // The substitute node failed too: no copy of these shards
                // answered, which is nodedown, not the transport error.
                Err(error) if peer_unreachable(&error) => return Err(nodedown()),
                Err(error) => return Err(error),
            }
        }
        if failed.is_empty() {
            break;
        }
        first = false;
        if lost.iter().any(|mask| !mask.is_empty()) {
            crate::telemetry::record_degraded_read();
        }
        excluded.extend(failed.iter().copied());
        // Prefer a node nothing says is down. A node skipped only because
        // discovery marked it down may be wrongly marked (the mark lasts until
        // the next pass), so when the unmarked nodes hold no copy it is asked
        // after all; only a node that actually failed is out for good.
        failed_nodes.extend(failed.iter().copied());
        let alive: Vec<usize> = (0..nodes).filter(|node| !excluded.contains(node)).collect();
        let groups = match fallback_shard_masks(db, &lost, &failed, &alive) {
            Err(_) => {
                let alive: Vec<usize> = (0..nodes)
                    .filter(|node| !failed_nodes.contains(node))
                    .collect();
                fallback_shard_masks(db, &lost, &failed, &alive)?
            }
            groups => groups?,
        };
        if groups.is_empty() {
            break;
        }
        requests = groups
            .into_iter()
            .map(|(node, mask)| (node, mask, Round::Fallback))
            .collect();
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
    let ClusterRead { ctx, db, .. } = read;
    let owned_by_node = preferred_shard_masks(ctx, db);

    if let Some(keys) = &opts.keys {
        let mut key_opts = opts.clone();
        // Only string keys name a document; the rest answer `not_found` here.
        key_opts.keys = Some(keys.iter().filter(|key| key.is_string()).cloned().collect());
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
        for (_, _, mut response) in values {
            total_rows += response["total_rows"].as_u64().unwrap_or(0);
            for row in crate::json::take_array(&mut response, "rows").unwrap_or_default() {
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

        let ordered_keys: Vec<&Value> = if opts.descending {
            keys.iter().rev().collect()
        } else {
            keys.iter().collect()
        };
        let mut rows = Vec::new();
        for key in ordered_keys.into_iter().skip(opts.skip).take(opts.limit) {
            let row = key
                .as_str()
                .and_then(|name| found.get(name).or_else(|| errors.get(name)));
            rows.push(
                row.cloned()
                    .unwrap_or_else(|| json!({"key": key, "error": "not_found"})),
            );
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
    for (_, _, mut response) in values {
        total_rows += response["total_rows"].as_u64().unwrap_or(0);
        before_start += response["offset"].as_u64().unwrap_or(0) as usize;
        for row in crate::json::take_array(&mut response, "rows").unwrap_or_default() {
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
    shards: Option<&ShardMask>,
) -> DbResult<Value> {
    let ClusterRead { ctx, db, .. } = read;
    if node == ctx.cluster.me {
        if let Some(shards) = shards {
            return db.all_docs_owned(opts, shards).await;
        }
        return db.all_docs(opts).await;
    }
    fetch_remote_docs(read, node, "_all_docs", opts, shards).await
}

/// An `_all_docs`-shaped query (`endpoint` names which) answered by a peer
/// from its own shards.
async fn fetch_remote_docs(
    read: ClusterRead<'_>,
    node: usize,
    endpoint: &str,
    opts: &AllDocsOpts,
    shards: Option<&ShardMask>,
) -> DbResult<Value> {
    let ClusterRead {
        ctx,
        dbname,
        headers,
        ..
    } = read;
    let peer = &ctx.cluster.nodes[node];
    let mut query: Vec<(&str, String)> = Vec::new();
    if let Some(shards) = shards {
        query.push(("shards", shards.query_value()));
    }
    if let Some(partition) = &opts.partition {
        query.push(("partition", partition.clone()));
    }
    if opts.include_docs {
        query.push(("include_docs", "true".to_string()));
    }
    if opts.conflicts {
        query.push(("conflicts", "true".to_string()));
    }
    if opts.attachments {
        query.push(("attachments", "true".to_string()));
    }
    if opts.descending {
        query.push(("descending", "true".to_string()));
    }
    if !opts.inclusive_end {
        query.push(("inclusive_end", "false".to_string()));
    }
    if opts.skip > 0 {
        query.push(("skip", opts.skip.to_string()));
    }
    if opts.limit != usize::MAX {
        query.push(("limit", opts.limit.to_string()));
    }
    if let Some(start) = &opts.startkey {
        query.push(("startkey", serde_json::to_string(start).unwrap_or_default()));
    }
    if let Some(end) = &opts.endkey {
        query.push(("endkey", serde_json::to_string(end).unwrap_or_default()));
    }
    let body = match &opts.keys {
        Some(keys) => json!({"keys": keys, "include_docs": opts.include_docs}),
        None => json!({}),
    };
    let request = internal_request(
        ctx,
        reqwest::Method::POST,
        peer,
        &[dbname, endpoint],
        Some(headers),
    )
    .query(&query)
    .json(&body);
    let response = send_peer(request)
        .await
        .map_err(|e| ApiError::new(502, "bad_gateway", e.to_string()))?;
    if !response.status().is_success() {
        let status = response.status().as_u16();
        let text = response.text().await.unwrap_or_default();
        let error = if endpoint == "_all_docs" {
            "all_docs_failed"
        } else {
            "local_docs_failed"
        };
        return Err(ApiError::new(status, error, text));
    }
    response
        .json::<Value>()
        .await
        .map_err(|e| ApiError::new(502, "bad_gateway", e.to_string()))
}

/// Hand the `_local` documents of every shard to the replicas the shard gains
/// in a rebalance from `old_map` to `map`. They take no sequence, so the
/// `_changes`-driven anti-entropy that seeds the other documents never
/// carries them, and it must not: a local delete leaves no tombstone, so a
/// pull would bring a deleted document back.
///
/// Writes are frozen, so each old copy is read once, the newest revision of
/// each document wins, and the copy is installed with its revision as is. A
/// failure leaves the active map untouched; the caller abandons the
/// migration.
pub(crate) async fn copy_local_docs(
    read: ClusterRead<'_>,
    old_map: &crate::cluster::ShardMap,
    map: &crate::cluster::ShardMap,
) -> DbResult<()> {
    let ClusterRead {
        ctx,
        db,
        dbname,
        headers,
    } = read;
    let mut gained: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut sources: HashMap<usize, ShardMask> = HashMap::new();
    for shard in 0..db.q {
        let old = old_map.replicas_of(shard);
        let new: Vec<usize> = map
            .replicas_of(shard)
            .iter()
            .copied()
            .filter(|node| !old.contains(node))
            .collect();
        if new.is_empty() {
            continue;
        }
        gained.insert(shard as usize, new);
        for node in old {
            sources
                .entry(*node)
                .or_insert_with(|| ShardMask::none(db.q))
                .select(shard as usize);
        }
    }
    if gained.is_empty() {
        return Ok(());
    }

    let opts = AllDocsOpts {
        include_docs: true,
        ..Default::default()
    };
    let opts = &opts;
    let listings = in_request_scope(crate::cluster_http::in_one_round(join_all(
        sources.iter().map(|(node, mask)| async move {
            if *node == ctx.cluster.me {
                db.local_docs_owned(opts, mask).await
            } else {
                fetch_remote_docs(read, *node, "_local_docs", opts, Some(mask)).await
            }
        }),
    )))
    .await;
    let mut newest: BTreeMap<String, Value> = BTreeMap::new();
    for listing in listings {
        let mut listing = listing?;
        for mut row in crate::json::take_array(&mut listing, "rows").unwrap_or_default() {
            let Some(doc) = row.get_mut("doc").map(Value::take) else {
                continue;
            };
            let Some(id) = doc.get("_id").and_then(Value::as_str).map(str::to_string) else {
                continue;
            };
            let current = newest.get(&id).map_or(0, local_rev_number);
            if !newest.contains_key(&id) || local_rev_number(&doc) > current {
                newest.insert(id, doc);
            }
        }
    }

    let mut per_node: HashMap<usize, Vec<Value>> = HashMap::new();
    for (id, doc) in newest {
        let Some(nodes) = gained.get(&db.shard_index(&id)) else {
            continue;
        };
        for node in nodes {
            per_node.entry(*node).or_default().push(doc.clone());
        }
    }
    let installs = in_request_scope(crate::cluster_http::in_one_round(join_all(
        per_node.into_iter().map(|(node, docs)| async move {
            if node == ctx.cluster.me {
                return Ok(db.install_local_docs(docs).await);
            }
            forward_bulk_owned(
                ctx.client.clone(),
                ctx.cluster.secret.clone(),
                ctx.cluster.nodes[node].clone(),
                dbname.to_string(),
                docs,
                false,
                headers.clone(),
            )
            .await
        }),
    )))
    .await;
    for results in installs {
        if let Some(failed) = results?.iter().find(|result| result.get("error").is_some()) {
            return Err(ApiError::new(
                502,
                "rebalance_failed",
                format!("a replica refused a _local document: {failed}"),
            ));
        }
    }
    Ok(())
}

/// The number of a `0-N` local revision, 0 for anything else.
fn local_rev_number(doc: &Value) -> u64 {
    doc.get("_rev")
        .and_then(Value::as_str)
        .and_then(|rev| rev.strip_prefix("0-"))
        .and_then(|number| number.parse().ok())
        .unwrap_or(0)
}

/// `GET /db/_local/{id}` in a cluster. Like `fabric_doc_open`, every copy of
/// the document's shard is asked and the highest revision wins; local
/// documents are not read-repaired. A copy that cannot be reached does not
/// fail the read while another answered: the read-quorum shortfall that
/// `fabric_doc_open` also lets through for them. `None` means the copies that
/// answered do not have it.
pub(crate) async fn read_local_doc(read: ClusterRead<'_>, id: &str) -> DbResult<Option<Value>> {
    let ClusterRead {
        ctx,
        db,
        dbname,
        headers,
    } = read;
    let mut replicas = db.replicas_of_doc(&format!("_local/{id}"));
    if let Some(index) = replicas.iter().position(|node| *node == ctx.cluster.me) {
        let local = replicas.remove(index);
        replicas.insert(0, local);
    }
    let responses =
        crate::cluster_http::in_one_round(join_all(replicas.into_iter().map(|node| async move {
            if node == ctx.cluster.me {
                crate::telemetry::record_local_read();
                return db.local_get(id).await.ok();
            }
            crate::telemetry::record_remote_read();
            let peer = &ctx.cluster.nodes[node];
            let request = internal_request(
                ctx,
                reqwest::Method::GET,
                peer,
                &[dbname, "_local", id],
                Some(headers),
            );
            match send_peer(request).await {
                Ok(response) if response.status().is_success() => {
                    response.json::<Value>().await.ok().map(Some)
                }
                Ok(response) if response.status() == StatusCode::NOT_FOUND => Some(None),
                Ok(_) | Err(_) => None,
            }
        })))
        .await;
    let answered = responses.iter().flatten().count();
    let newest = responses
        .into_iter()
        .flatten()
        .flatten()
        .max_by_key(local_rev_number);
    match newest {
        Some(doc) => Ok(Some(doc)),
        None if answered > 0 => Ok(None),
        None => Err(ApiError::new(500, "error", "read_failure")),
    }
}

/// `_local_docs` across the cluster, as `_all_docs` is read: one copy of each
/// shard, chosen by `preferred_shard_masks`, so a document is listed once.
/// Like CouchDB (`fabric_view_all_docs` with the `_local` namespace) it
/// reports no `total_rows` or `offset`.
pub(crate) async fn cluster_local_docs(
    read: ClusterRead<'_>,
    opts: &AllDocsOpts,
) -> DbResult<Value> {
    let ClusterRead { ctx, db, .. } = read;
    let owned_by_node = preferred_shard_masks(ctx, db);

    if let Some(keys) = &opts.keys {
        // Only keys in the `_local/` namespace are looked up at all.
        let keys: Vec<Value> = keys
            .iter()
            .filter(|key| key.as_str().is_some_and(|key| key.starts_with("_local/")))
            .cloned()
            .collect();
        let mut key_opts = opts.clone();
        key_opts.keys = Some(keys.clone());
        key_opts.skip = 0;
        key_opts.limit = usize::MAX;
        key_opts.startkey = None;
        key_opts.endkey = None;
        key_opts.descending = false;

        let key_opts = &key_opts;
        let values = scatter(
            read,
            &owned_by_node,
            "local_docs",
            |node, mask, _| async move { fetch_local_docs(read, node, key_opts, &mask).await },
        )
        .await?;
        let mut found: HashMap<String, Value> = HashMap::new();
        for (_, _, mut response) in values {
            for row in crate::json::take_array(&mut response, "rows").unwrap_or_default() {
                if let Some(id) = row.get("id").and_then(Value::as_str) {
                    found.insert(id.to_string(), row);
                }
            }
        }
        let ordered: Vec<&Value> = if opts.descending {
            keys.iter().rev().collect()
        } else {
            keys.iter().collect()
        };
        let rows: Vec<Value> = ordered
            .into_iter()
            .skip(opts.skip)
            .take(opts.limit)
            .map(|key| {
                key.as_str()
                    .and_then(|name| found.get(name).cloned())
                    .unwrap_or_else(|| json!({"key": key, "error": "not_found"}))
            })
            .collect();
        return Ok(json!({"total_rows": Value::Null, "offset": Value::Null, "rows": rows}));
    }

    let mut window = opts.clone();
    window.skip = 0;
    window.limit = opts.skip.saturating_add(opts.limit);
    let window = &window;
    let values = scatter(
        read,
        &owned_by_node,
        "local_docs",
        |node, mask, _| async move { fetch_local_docs(read, node, window, &mask).await },
    )
    .await?;
    let mut by_id: BTreeMap<String, Value> = BTreeMap::new();
    for (_, _, mut response) in values {
        for row in crate::json::take_array(&mut response, "rows").unwrap_or_default() {
            if let Some(id) = row.get("id").and_then(Value::as_str) {
                by_id.entry(id.to_string()).or_insert(row);
            }
        }
    }
    let mut rows: Vec<Value> = by_id.into_values().collect();
    if opts.descending {
        rows.reverse();
    }
    let rows: Vec<Value> = rows.into_iter().skip(opts.skip).take(opts.limit).collect();
    Ok(json!({"total_rows": Value::Null, "offset": Value::Null, "rows": rows}))
}

async fn fetch_local_docs(
    read: ClusterRead<'_>,
    node: usize,
    opts: &AllDocsOpts,
    shards: &ShardMask,
) -> DbResult<Value> {
    if node == read.ctx.cluster.me {
        return read.db.local_docs_owned(opts, shards).await;
    }
    fetch_remote_docs(read, node, "_local_docs", opts, Some(shards)).await
}

/// One owner's part of a cluster view query.
#[derive(Default)]
pub(crate) struct ViewBatch {
    rows: Arc<Vec<crate::viewfn::Row>>,
    docs: HashMap<String, Value>,
    total_rows: usize,
    offset: usize,
}

/// Merged map rows for a cluster view query, with the counts CouchDB reports.
pub(crate) struct ClusterViewRows {
    pub spec: crate::view::ViewSpec,
    pub rows: Arc<Vec<crate::viewfn::Row>>,
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
    mask: &ShardMask,
) -> DbResult<ViewBatch> {
    let ClusterRead {
        ctx,
        db,
        dbname,
        headers,
    } = read;
    let IndexRef {
        ddoc,
        name: view_name,
    } = view;
    if mask.is_empty() {
        return Ok(ViewBatch::default());
    }
    // Owners apply the key range and return at most `limit` rows from their
    // shards; the coordinator merges and pages the union.
    let mut local = query.clone();
    local.skip = 0;
    local.limit = limit.unwrap_or(usize::MAX);
    local.owner_shards = Some(mask.clone());
    if node == ctx.cluster.me {
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

    let peer = &ctx.cluster.nodes[node];
    let selected = mask.query_value();
    let mut params = vec![
        ("reduce", "false".to_string()),
        ("shards", selected),
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
    let request = internal_request(
        ctx,
        reqwest::Method::GET,
        peer,
        &[dbname, ddoc, "_view", view_name],
        Some(headers),
    )
    .query(&params);
    let response = send_peer(request)
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
    let ClusterRead { ctx, db, .. } = read;
    let IndexRef {
        ddoc,
        name: view_name,
    } = view;
    let ddoc_doc = design_doc(read, ddoc).await?;
    let spec = crate::view::spec_from_doc(&ddoc_doc, view_name)?;
    crate::view::validate_view_partition(db, &spec, query.partition.as_deref())?;
    let reduce = query.reduce.unwrap_or(spec.reduce.is_some());
    // Owners apply the key range themselves. A reduce needs every row in it;
    // a map query takes only the global window from each owner because the
    // owner sets are disjoint.
    let can_limit = !reduce;
    let take = query.skip.saturating_add(query.limit);

    let owned_by_node = preferred_shard_masks(ctx, db);
    let limit = can_limit.then_some(take);

    let spec_ref = &spec;
    let values = scatter(read, &owned_by_node, "view", |node, mask, _| async move {
        fetch_view_mask(read, view, query, spec_ref, limit, node, &mask).await
    })
    .await?;

    let mut counts: HashMap<String, (usize, crate::viewfn::Row)> = HashMap::new();
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
        let mut local: HashMap<String, (usize, crate::viewfn::Row)> = HashMap::new();
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
fn owner_reduce_query(query: &crate::view::ViewQuery, mask: &ShardMask) -> crate::view::ViewQuery {
    let mut owner = query.clone();
    owner.skip = 0;
    owner.limit = usize::MAX;
    owner.reduce = Some(true);
    owner.owner_shards = Some(mask.clone());
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
    mask: &ShardMask,
) -> DbResult<Value> {
    let ClusterRead {
        ctx,
        db,
        dbname,
        headers,
    } = read;
    let IndexRef {
        ddoc,
        name: view_name,
    } = view;
    let owner = owner_reduce_query(query, mask);
    if node == ctx.cluster.me {
        return crate::view::query(db, ddoc, view_name, &owner).await;
    }

    let peer = &ctx.cluster.nodes[node];
    let selected = mask.query_value();
    let mut params = vec![("reduce", "true".to_string()), ("shards", selected)];
    if owner.group {
        params.push(("group", "true".to_string()));
    }
    if owner.group_level > 0 {
        params.push(("group_level", owner.group_level.to_string()));
    }
    push_range_params(&mut params, &owner);
    let request = internal_request(
        ctx,
        reqwest::Method::GET,
        peer,
        &[dbname, ddoc, "_view", view_name],
        Some(headers),
    )
    .query(&params);
    let response = send_peer(request)
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
    let ClusterRead { ctx, db, .. } = read;
    let IndexRef {
        ddoc,
        name: view_name,
    } = view;
    let ddoc_doc = design_doc(read, ddoc).await?;
    let spec = crate::view::spec_from_doc(&ddoc_doc, view_name)?;
    crate::view::validate_view_partition(db, &spec, query.partition.as_deref())?;
    crate::view::check_query(&spec, query)?;
    let owned_by_node = preferred_shard_masks(ctx, db);
    let values = scatter(read, &owned_by_node, "view", |node, mask, _| async move {
        fetch_reduce_mask(read, view, query, node, &mask).await
    })
    .await?;

    let mut partials = crate::view::Partials::new();
    for (_, _, mut value) in values {
        for mut row in crate::json::take_array(&mut value, "rows").unwrap_or_default() {
            let key = row.get_mut("key").map(Value::take).unwrap_or(Value::Null);
            let value = row.get_mut("value").map(Value::take).unwrap_or(Value::Null);
            crate::view::add_partial(&mut partials, key, value);
        }
    }
    let reducer = reducer.to_string();
    let query = query.clone();
    crate::view::blocking(move || {
        let mut js = crate::view::JsReducer::new(&reducer, query.limits);
        crate::view::assemble_reduce(partials, &query, &mut js)
            .map_err(|reason| crate::view::reduce_failure(&reason))
    })
    .await
}

pub(crate) fn decode_view_batch(mut value: Value) -> ViewBatch {
    let total_rows = value["total_rows"].as_u64().unwrap_or(0) as usize;
    let offset = value["offset"].as_u64().unwrap_or(0) as usize;
    let mut rows = Vec::new();
    let mut docs = HashMap::new();
    for mut row in crate::json::take_array(&mut value, "rows").unwrap_or_default() {
        let Some(id) = row.get("id").and_then(Value::as_str).map(str::to_string) else {
            continue;
        };
        if let Some(doc) = row.get_mut("doc").map(Value::take) {
            docs.insert(id.clone(), doc);
        }
        rows.push(crate::viewfn::Row {
            id,
            key: row.get_mut("key").map(Value::take).unwrap_or(Value::Null),
            value: row.get_mut("value").map(Value::take).unwrap_or(Value::Null),
        });
    }
    ViewBatch {
        rows: Arc::new(rows),
        docs,
        total_rows,
        offset,
    }
}

pub(crate) fn row_key(row: &crate::viewfn::Row) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}",
        row.id,
        serde_json::to_string(&row.key).unwrap_or_default(),
        serde_json::to_string(&row.value).unwrap_or_default()
    )
}

/// The design document from this node, or from a replica when the shard that
/// owns it lives elsewhere.
async fn design_doc(read: ClusterRead<'_>, ddoc: &str) -> DbResult<Value> {
    let ClusterRead {
        ctx,
        db,
        dbname,
        headers,
    } = read;
    match db.get_doc(ddoc, &DocOpts::default()).await {
        Err(error) if error.status == 404 => {
            fetch_ddoc_from_replica(ctx, db, dbname, ddoc, headers).await
        }
        other => other,
    }
}

pub(crate) async fn fetch_ddoc_from_replica(
    ctx: &ClusterCtx,
    db: &Db,
    dbname: &str,
    ddoc: &str,
    headers: &HeaderMap,
) -> DbResult<Value> {
    for node in db.replicas_of_doc(ddoc) {
        if node == ctx.cluster.me {
            continue;
        }
        let peer = ctx.cluster.nodes[node].clone();
        let request = internal_request(
            ctx,
            reqwest::Method::GET,
            &peer,
            &[dbname, ddoc],
            Some(headers),
        );
        if let Ok(response) = send_peer(request).await {
            if response.status().is_success() {
                if let Ok(value) = response.json::<Value>().await {
                    return Ok(value);
                }
            }
        }
    }
    Err(ApiError::not_found(format!("missing {ddoc}")))
}

fn last_seq_for_node(value: &Value, node: usize) -> Option<u64> {
    let seq = value.get("last_seq")?;
    if let Some(seq) = seq.as_u64() {
        return Some(seq);
    }
    let decoded = decode_since(seq.as_str()?)?;
    decoded.get(node).copied()
}

/// Where a feed stands on shards read from a replica because their preferred
/// owner failed: `node` served `shards` and had returned changes up to its own
/// `seq`. The owner's vector slot is in another node's sequence space, so it
/// cannot carry this progress.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FallbackMark {
    node: usize,
    shards: Vec<usize>,
    seq: u64,
}

/// A vector slot for a node whose sequence was not known when the token was
/// made because it did not answer. The node's own sequence at that moment
/// cannot be learned afterwards, so the slot is not a position: a feed reads
/// the node's shards from another replica for as long as it can, and only
/// replays the node from its start when there is no alternative (a feed may
/// replay, never skip).
pub(crate) const UNKNOWN_SEQ: u64 = u64::MAX;

/// One stream of changes with its own sequence space: a node reading its
/// preferred shards, or a node reading shards reassigned from a failed owner.
struct ChangeLane {
    node: usize,
    /// Index into the marks of a fallback lane; `None` for an owner lane.
    mark: Option<usize>,
    rows: std::collections::VecDeque<Value>,
    last: Option<u64>,
}

pub(crate) async fn cluster_changes(
    read: ClusterRead<'_>,
    q: &HashMap<String, String>,
    body: &[u8],
) -> DbResult<Value> {
    let ClusterRead { ctx, db, .. } = read;
    let n = ctx.cluster.nodes.len();
    let raw_since = q.get("since").map(String::as_str);
    let limit = num(q, "limit");
    // `since=now` is resolved by the nodes themselves, in the same round that
    // reads their changes: each answers for its own sequence space, so no
    // separate round has to learn the vector first. `limit=0` reads nothing,
    // so only the vector is needed.
    let now = raw_since == Some("now") && limit != Some(0);
    let (mut since, marks) = if now {
        (vec![0; n], Vec::new())
    } else if raw_since == Some("now") {
        cluster_update_vector(read).await?
    } else if let Some(raw) = raw_since {
        let vector = match decode_since(raw) {
            Some(vector) => vector,
            None => vec![since_component(Some(raw), ctx.cluster.me); n],
        };
        (vector, decode_fallbacks(raw, n))
    } else {
        (vec![0; n], Vec::new())
    };
    since.resize(n, 0);
    let unknown: Vec<usize> = (0..n).filter(|&node| since[node] == UNKNOWN_SEQ).collect();
    let owned_by_node = preferred_shard_masks(ctx, db);

    let doc_ids = doc_ids_from(q, body);
    let change_filter = resolve_changes_filter(db, q).await?;
    let changes = ChangesFetch {
        include_docs: flag(q, "include_docs"),
        style_all_docs: q.get("style").map(String::as_str) == Some("all_docs"),
        doc_ids: doc_ids.as_deref(),
        filter: change_filter.as_ref(),
        q,
    };
    if limit == Some(0) {
        return Ok(json!({
            "results": [],
            "last_seq": encode_token(&since, &marks),
            "pending": 0,
        }));
    }

    let node_limit = if change_filter.is_some() { None } else { limit };
    let since_ref = &since;
    let marks_ref = &marks;
    let responses = scatter_nodes(
        read,
        &owned_by_node,
        "changes",
        now,
        &unknown,
        |node, mask, round| async move {
            let shards = mask.shards();
            if now {
                if mask.is_empty() {
                    // Owns no shard: only its sequence is wanted, for its
                    // slot in the vector.
                    let (seq, _) = fetch_node_seqs(read, node).await?;
                    let empty = json!({"results": [], "last_seq": seq, "pending": 0});
                    return Ok((shards, None, empty));
                }
                let start = ChangesStart::Now;
                let value =
                    fetch_node_changes(read, changes, node, &mask, start, node_limit).await?;
                return Ok((shards, None, value));
            }
            // Shards reassigned from a failed owner resume from the mark this
            // node left for exactly this set of shards, else from the start;
            // preferred masks are disjoint, so ids do not overlap. A node
            // whose position was unknown when the token was made is read
            // from the start too: a feed may replay, never skip.
            let since = match round {
                Round::Owner => Some(since_ref[node]).filter(|seq| *seq != UNKNOWN_SEQ),
                Round::Fallback => marks_ref
                    .iter()
                    .find(|mark| mark.node == node && mark.shards == shards)
                    .map(|mark| mark.seq),
            }
            .unwrap_or(0);
            let start = ChangesStart::At(since);
            let value = fetch_node_changes(read, changes, node, &mask, start, node_limit).await?;
            Ok((shards, Some(since), value))
        },
    )
    .await?;

    let mut progress = since.clone();
    let mut answered = vec![false; n];
    let mut fallbacks: Vec<FallbackMark> = Vec::new();
    let mut lanes: Vec<ChangeLane> = Vec::new();
    let mut pending = 0;
    for (node, round, (shards, lane_since, mut value)) in responses {
        pending += node_pending(&value);
        let mut rows: Vec<Value> = match value.get_mut("results").map(std::mem::take) {
            Some(Value::Array(rows)) => rows,
            _ => Vec::new(),
        };
        rows.sort_by_key(|row| row["seq"].as_u64().unwrap_or(0));
        let last = last_seq_for_node(&value, node);
        // A node that resolved `since=now` itself started just before its
        // first row, or where it reports having stopped when there is none.
        let lane_since = lane_since.unwrap_or_else(|| match rows.first() {
            Some(row) => row["seq"].as_u64().unwrap_or(0).saturating_sub(1),
            None => last.unwrap_or(0),
        });
        let mark = match round {
            Round::Owner => {
                if let Some(slot) = progress.get_mut(node) {
                    *slot = if *slot == UNKNOWN_SEQ {
                        lane_since
                    } else {
                        (*slot).max(lane_since)
                    };
                }
                if let Some(seen) = answered.get_mut(node) {
                    *seen = true;
                }
                None
            }
            Round::Fallback => {
                fallbacks.push(FallbackMark {
                    node,
                    shards,
                    seq: lane_since,
                });
                Some(fallbacks.len() - 1)
            }
        };
        lanes.push(ChangeLane {
            node,
            mark,
            rows: rows.into(),
            last,
        });
    }

    if now {
        // A node that gave no answer has no position in this vector.
        for (slot, _) in progress
            .iter_mut()
            .zip(&answered)
            .filter(|(_, seen)| !**seen)
        {
            *slot = UNKNOWN_SEQ;
        }
    }

    let limit = limit.unwrap_or(usize::MAX);
    let mut out = Vec::new();
    loop {
        let mut progressed = false;
        for lane in &mut lanes {
            let Some(mut row) = lane.rows.pop_front() else {
                continue;
            };
            progressed = true;
            let shard_seq = row["seq"].as_u64().unwrap_or(0);
            let safe = lane.last.map_or(shard_seq, |seq| shard_seq.min(seq));
            match lane.mark {
                None => {
                    if let Some(slot) = progress.get_mut(lane.node) {
                        *slot = (*slot).max(safe);
                    }
                }
                Some(mark) => {
                    if let Some(mark) = fallbacks.get_mut(mark) {
                        mark.seq = mark.seq.max(safe);
                    }
                }
            }
            row["seq"] = json!(encode_token(&progress, &fallbacks));
            out.push(row);
            if out.len() >= limit {
                break;
            }
        }
        if out.len() >= limit || !progressed {
            break;
        }
    }
    // A lane whose rows were all returned has also scanned past them: a filter
    // may have discarded documents after its last row, or all of them, and
    // CouchDB's feed position covers every document scanned
    // (`couch_changes:changes_enumerator`).
    for lane in lanes.iter().filter(|lane| lane.rows.is_empty()) {
        let Some(last) = lane.last else {
            continue;
        };
        match lane.mark {
            None => {
                if let Some(slot) = progress.get_mut(lane.node) {
                    *slot = (*slot).max(last);
                }
            }
            Some(mark) => {
                if let Some(mark) = fallbacks.get_mut(mark) {
                    mark.seq = mark.seq.max(last);
                }
            }
        }
    }
    // Rows fetched but not returned are still to come.
    pending += lanes.iter().map(|lane| lane.rows.len() as u64).sum::<u64>();

    // Shard masks are disjoint, so each document's change is produced by
    // exactly one lane.
    Ok(json!({
        "results": out,
        "last_seq": encode_token(&progress, &fallbacks),
        "pending": pending,
    }))
}

/// Changes a node had left after the rows it returned, across the shards it
/// read for this request.
fn node_pending(response: &Value) -> u64 {
    response.get("pending").and_then(Value::as_u64).unwrap_or(0)
}

/// Where a node starts reading changes.
#[derive(Clone, Copy)]
pub(crate) enum ChangesStart {
    At(u64),
    /// The node's current sequence, as it is when the node handles the call.
    Now,
}

pub(crate) async fn fetch_node_changes(
    read: ClusterRead<'_>,
    changes: ChangesFetch<'_>,
    node: usize,
    mask: &ShardMask,
    start: ChangesStart,
    limit: Option<usize>,
) -> DbResult<Value> {
    let ClusterRead {
        ctx,
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
    if node == ctx.cluster.me {
        let since = match start {
            ChangesStart::At(since) => since,
            ChangesStart::Now => db.update_seq(),
        };
        return db
            .changes(&ChangesOpts {
                since,
                limit,
                include_docs,
                style_all_docs,
                descending: false,
                doc_ids: doc_ids.map(|ids| ids.to_vec()),
                filter: filter.cloned(),
                conflicts: flag(q, "conflicts"),
                owned: Some(mask.clone()),
                until: None,
            })
            .await;
    }

    let peer = &ctx.cluster.nodes[node];
    let selected = mask.query_value();
    // A peer reads `since=now` against its own sequence, with or without this
    // change: the forwarded form of the request has always accepted it.
    let since = match start {
        ChangesStart::At(since) => since.to_string(),
        ChangesStart::Now => "now".to_string(),
    };
    let mut params: Vec<(&str, String)> = vec![("since", since), ("shards", selected)];
    if let Some(limit) = limit {
        params.push(("limit", limit.to_string()));
    }
    if include_docs {
        params.push(("include_docs", "true".to_string()));
    }
    if style_all_docs {
        params.push(("style", "all_docs".to_string()));
    }
    if let Some(filter) = q.get("filter") {
        params.push(("filter", filter.clone()));
    }
    if let Some(view) = q.get("view") {
        params.push(("view", view.clone()));
    }
    if flag(q, "conflicts") {
        params.push(("conflicts", "true".to_string()));
    }
    // A filter function reads its own arguments from `req.query`, so the peer
    // must see every argument the client sent. reqwest percent-encodes them.
    const OWN_PARAMS: [&str; 14] = [
        "since",
        "limit",
        "shards",
        "feed",
        "timeout",
        "heartbeat",
        "descending",
        "doc_ids",
        "style",
        "include_docs",
        "filter",
        "view",
        "conflicts",
        "last-event-id",
    ];
    let mut extra: Vec<(&str, String)> = q
        .iter()
        .filter(|(key, _)| !OWN_PARAMS.contains(&key.as_str()))
        .map(|(key, value)| (key.as_str(), value.clone()))
        .collect();
    extra.sort();
    params.extend(extra);
    let request = internal_request(
        ctx,
        reqwest::Method::GET,
        peer,
        &[dbname, "_changes"],
        Some(headers),
    )
    .query(&params);
    let request = if let Some(ids) = doc_ids {
        request
            .query(&[("filter", "_doc_ids")])
            .json(&json!({"doc_ids": ids}))
    } else {
        request
    };
    let response = send_peer(request)
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

/// One node's current update sequence and purge sequence: its own, or what
/// the peer reports in the same answer.
async fn fetch_node_seqs(read: ClusterRead<'_>, node: usize) -> DbResult<(u64, u64)> {
    let ClusterRead {
        ctx,
        db,
        dbname,
        headers,
    } = read;
    if node == ctx.cluster.me {
        return Ok((db.update_seq(), db.purge_seq().await?));
    }
    let peer = &ctx.cluster.nodes[node];
    let request = internal_request(ctx, reqwest::Method::GET, peer, &[dbname], Some(headers))
        .header("x-douchdb-update-seq", "1");
    let bad = |detail: String| ApiError::new(502, "bad_gateway", detail);
    let response = send_peer(request).await.map_err(|e| bad(e.to_string()))?;
    if !response.status().is_success() {
        return Err(bad(format!(
            "{peer}: update_seq returned {}",
            response.status()
        )));
    }
    let body = response
        .json::<Value>()
        .await
        .map_err(|e| bad(e.to_string()))?;
    let update_seq = body["update_seq"]
        .as_u64()
        .ok_or_else(|| bad(format!("{peer}: no update_seq")))?;
    let purge_seq = body["purge_seq"]
        .as_u64()
        .ok_or_else(|| bad(format!("{peer}: no purge_seq")))?;
    Ok((update_seq, purge_seq))
}

/// Current per-node update_seq vector, plus fallback marks for the shards of
/// any node that did not answer. Used for `since=now` and for the `update_seq`
/// reported in `GET /db`.
///
/// Like CouchDB's `fabric_db_info`, an unreachable node is fine while another
/// replica holds each of its shards; the feed then reads those shards from the
/// replica, so the mark starts at that replica's current sequence. When some
/// shard has no live copy the answer is 412 `nodedown` / `progress not
/// possible`. The unreachable node's own slot is [`UNKNOWN_SEQ`], so after it
/// returns its changes are neither replayed nor skipped wholesale.
pub(crate) async fn cluster_update_vector(
    read: ClusterRead<'_>,
) -> DbResult<(Vec<u64>, Vec<FallbackMark>)> {
    let (vector, marks, _) = cluster_seq_vectors(read).await?;
    Ok((vector, marks))
}

/// [`cluster_update_vector`] plus the per-node purge sequence vector, which
/// the same peer answers carry; a node that did not answer counts as 0.
pub(crate) async fn cluster_seq_vectors(
    read: ClusterRead<'_>,
) -> DbResult<(Vec<u64>, Vec<FallbackMark>, Vec<u64>)> {
    let ClusterRead { ctx, db, .. } = read;
    let nodes = ctx.cluster.nodes.len();
    // Every node is asked, including one discovery marked down: the mark may
    // be stale, and a node that is up must report its own position. Skipping
    // it would put 0 in its slot of the vectors, though the shards it owns are
    // readable from a replica.
    let mut seqs: Vec<Option<u64>> = vec![None; nodes];
    let mut purge_seqs: Vec<u64> = vec![0; nodes];
    let answers = crate::cluster_http::in_one_round(futures::future::join_all(
        (0..nodes).map(|node| async move { fetch_node_seqs(read, node).await.ok() }),
    ))
    .await;
    for (node, answer) in answers.into_iter().enumerate() {
        seqs[node] = answer.map(|(seq, _)| seq);
        purge_seqs[node] = answer.map_or(0, |(_, purged)| purged);
    }
    let failed: Vec<usize> = (0..nodes).filter(|node| seqs[*node].is_none()).collect();
    let alive: Vec<usize> = (0..nodes).filter(|node| seqs[*node].is_some()).collect();
    let vector: Vec<u64> = seqs.iter().map(|seq| seq.unwrap_or(0)).collect();
    if failed.is_empty() {
        return Ok((vector, Vec::new(), purge_seqs));
    }
    let groups = fallback_shard_masks(db, &preferred_shard_masks(ctx, db), &failed, &alive)?;
    let marks = groups
        .into_iter()
        .map(|(node, mask)| FallbackMark {
            node,
            shards: mask.shards(),
            seq: vector.get(node).copied().unwrap_or(0),
        })
        .collect();
    let vector = vector
        .into_iter()
        .zip(&seqs)
        .map(|(seq, answer)| answer.map_or(UNKNOWN_SEQ, |_| seq))
        .collect();
    Ok((vector, marks, purge_seqs))
}

/// The `update_seq` and `purge_seq` that `GET /db` reports in a cluster, and
/// that a query reports with `update_seq=true`: the opaque per-node vectors,
/// one fan-out to the peers. A node that did not answer has no position to
/// report; 0 makes a feed resumed from here replay it, never skip it.
pub(crate) async fn cluster_seq_tokens(read: ClusterRead<'_>) -> DbResult<(String, String)> {
    let (vector, marks, purge_vector) = cluster_seq_vectors(read).await?;
    let vector: Vec<u64> = vector
        .into_iter()
        .map(|seq| if seq == UNKNOWN_SEQ { 0 } else { seq })
        .collect();
    Ok((
        encode_token(&vector, &marks),
        encode_token(&purge_vector, &[]),
    ))
}

pub(crate) fn encode_since(vector: &[u64]) -> String {
    encode_token(vector, &[])
}

/// A since token: the per-node vector, then `~node:shard.shard:seq` for each
/// fallback mark.
pub(crate) fn encode_token(vector: &[u64], marks: &[FallbackMark]) -> String {
    use std::fmt::Write as _;

    // This runs once per change row in a cluster feed. The old form was
    // base64(JSON) and allocated a map plus a base64 buffer for every row.
    // `v1:` is an opaque token to clients.
    let mut out = String::with_capacity(3 + vector.len() * 12);
    out.push_str("v1:");
    for (index, seq) in vector.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        let _ = write!(out, "{seq}");
    }
    for mark in marks {
        let _ = write!(out, "~{}:", mark.node);
        for (index, shard) in mark.shards.iter().enumerate() {
            if index > 0 {
                out.push('.');
            }
            let _ = write!(out, "{shard}");
        }
        let _ = write!(out, ":{}", mark.seq);
    }
    out
}

/// The fallback marks of a `v1:` since token; malformed or out-of-cluster
/// marks are dropped, which only makes that fallback start from the beginning.
pub(crate) fn decode_fallbacks(raw: &str, nodes: usize) -> Vec<FallbackMark> {
    let Some(rest) = raw.trim_matches('"').strip_prefix("v1:") else {
        return Vec::new();
    };
    rest.split('~')
        .skip(1)
        .filter_map(|part| {
            let mut fields = part.split(':');
            let node: usize = fields.next()?.parse().ok()?;
            let shards: Option<Vec<usize>> = fields
                .next()?
                .split('.')
                .map(|shard| shard.parse().ok())
                .collect();
            let seq: u64 = fields.next()?.parse().ok()?;
            (node < nodes && fields.next().is_none()).then_some(FallbackMark {
                node,
                shards: shards?,
                seq,
            })
        })
        .collect()
}

/// Decodes a `v1:` since token into its per-node sequence vector. Anything
/// else is not one of ours (`None`).
pub(crate) fn decode_since(raw: &str) -> Option<Vec<u64>> {
    let rest = raw.trim_matches('"').strip_prefix("v1:")?;
    let rest = rest.split('~').next().unwrap_or(rest);
    rest.split(',').map(|part| part.parse().ok()).collect()
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
    let ClusterRead { ctx, db, .. } = read;
    let owned_by_node = preferred_shard_masks(ctx, db);
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
    mask: &ShardMask,
) -> DbResult<Value> {
    let ClusterRead {
        ctx,
        db,
        dbname,
        headers,
    } = read;
    let IndexRef {
        ddoc,
        name: index_name,
    } = index;
    if mask.is_empty() {
        return Ok(json!({"total_rows": 0, "rows": [], "bookmark": "search:0"}));
    }
    let mut local = request.clone();
    local.owner_shards = Some(mask.clone());
    local.bookmark = None;
    local.limit = window;
    if node == ctx.cluster.me {
        return crate::search::query(db, ddoc, index_name, &local, limits).await;
    }

    let peer = &ctx.cluster.nodes[node];
    let selected = mask.query_value();
    let mut params: Vec<(&str, String)> = vec![
        ("q", request.query.clone()),
        ("limit", window.to_string()),
        ("timeout", request.timeout_ms.to_string()),
        ("shards", selected),
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

    let outgoing = internal_request(
        ctx,
        reqwest::Method::GET,
        peer,
        &[dbname, ddoc, "_search", index_name],
        Some(headers),
    )
    .query(&params);
    let response = send_peer(outgoing)
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

    /// A one-shot HTTP peer that answers every request with `status`.
    fn peer_answering(status: u16) -> String {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let peer = format!("http://{}", listener.local_addr().expect("addr"));
        std::thread::spawn(move || {
            while let Ok((mut socket, _)) = listener.accept() {
                let mut seen = Vec::new();
                let mut chunk = [0u8; 4096];
                // Read the request head and body before answering.
                while let Ok(read) = socket.read(&mut chunk) {
                    if read == 0 {
                        break;
                    }
                    seen.extend_from_slice(&chunk[..read]);
                    let text = String::from_utf8_lossy(&seen).to_ascii_lowercase();
                    let Some(head_end) = text.find("\r\n\r\n") else {
                        continue;
                    };
                    let length = text
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if seen.len() >= head_end + 4 + length {
                        break;
                    }
                }
                let reply = format!(
                    "HTTP/1.1 {status} X\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{{}}"
                );
                let _ = socket.write_all(reply.as_bytes());
            }
        });
        peer
    }

    async fn forward_to(peer: &str) -> ApiError {
        let ctx = ClusterCtx {
            cluster: crate::cluster::Cluster {
                nodes: vec![peer.to_string()],
                me: 0,
                n: 1,
                secret: "test-secret".to_string(),
            },
            client: reqwest::Client::new(),
            auth: None,
            health: Default::default(),
        };
        forward_json(
            &ctx,
            peer,
            "db",
            "_all_docs",
            "",
            &json!({}),
            &HeaderMap::new(),
        )
        .await
        .expect_err("the peer does not answer successfully")
    }

    /// A peer that answers 503 or 504 itself is reachable: its own error is
    /// not a transport failure, so callers must not treat it as nodedown.
    #[tokio::test]
    async fn a_peer_answering_5xx_itself_is_not_unreachable() {
        for status in [502, 503, 504] {
            let error = forward_to(&peer_answering(status)).await;
            assert_eq!(error.status, status);
            assert!(
                !peer_unreachable(&error),
                "a peer that answered {status} was classified as unreachable"
            );
        }
    }

    /// A refused connection is the genuine transport failure.
    #[tokio::test]
    async fn a_refused_connection_is_unreachable() {
        let closed = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            format!("http://{}", listener.local_addr().expect("addr"))
        };
        let error = forward_to(&closed).await;
        assert!(
            peer_unreachable(&error),
            "{} {}",
            error.status,
            error.reason
        );
    }

    /// A fan-out task that panics must still count as that node's reply.
    /// Slot 0 is a design document on nodes 0 and 1, so it is owed a reply
    /// from each; slot 1 sits on nodes 0 and 2 and node 2 never answers. Node 0
    /// acknowledges both, node 1 panics. Counting the panic settles the write
    /// (slot 1 already has its quorum); skipping it leaves slot 0 owed a reply
    /// and the request waits on node 2 forever.
    #[tokio::test]
    async fn panicked_fan_out_task_counts_as_answered() {
        let ids = vec!["_design/a".to_string(), "plain".to_string()];
        let primaries = vec![0, 0];
        let replicas = vec![vec![0, 1], vec![0, 2]];
        let tally = QuorumTally::new(&ids, &primaries, &replicas, 1);
        let ack = |id: &str| json!({ "id": id, "ok": true, "rev": "1-a" });

        let pending = FuturesUnordered::new();
        pending.push(spawn_node_write(0, vec![0, 1], {
            let results = vec![ack("_design/a"), ack("plain")];
            async move { Ok(results) }
        }));
        pending.push(spawn_node_write(1, vec![0], async {
            panic!("replica task exploded")
        }));
        pending.push(spawn_node_write(2, vec![1], async {
            std::future::pending::<DbResult<Vec<Value>>>().await
        }));

        let settled = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            collect_quorum(pending, tally),
        )
        .await
        .expect("a panicked task left the request waiting for a replica that never answers");
        let (out, met) = settled;
        assert!(met);
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|result| result.get("ok").is_some()));
    }

    /// A peer that accepts a write and never answers must not accumulate
    /// detached tasks without limit: the writes past the bound wait for a
    /// slot instead of piling up behind the stuck peer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn detached_replica_writes_are_bounded() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let hung = Arc::new(AtomicUsize::new(0));
        // A deep backlog, so the listener does not cap what the writer opens.
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(1024).unwrap();
        let peer = format!("http://{}", listener.local_addr().unwrap());
        let accepted = hung.clone();
        let stuck = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                accepted.fetch_add(1, Ordering::SeqCst);
                held.push(socket);
            }
        });

        let root = std::env::temp_dir().join(format!("douchdb-detached-{}", crate::util::uuid()));
        let mut st = crate::state::test_state(root.clone(), 4);
        Arc::get_mut(&mut st).expect("sole owner").ctx.cluster = crate::cluster::Cluster {
            nodes: vec!["http://127.0.0.1:1".to_string(), peer],
            me: 0,
            n: 2,
            secret: "test-secret".to_string(),
        };
        let db = st
            .create_db("bounded", false, Some(1), Some(2))
            .await
            .unwrap();

        let writes: Vec<_> = (0..400)
            .map(|i| {
                let st = st.clone();
                let db = db.clone();
                tokio::spawn(async move {
                    commit_docs(
                        &st,
                        &db,
                        "bounded",
                        vec![json!({"_id": format!("d{i}")})],
                        true,
                        1,
                        &HeaderMap::new(),
                    )
                    .await
                })
            })
            .collect();
        // Wait until the count stops moving: local commits are paced by the
        // disk, so the peer sees the writes over several seconds.
        let mut in_flight = 0;
        let mut stable = 0;
        while stable < 15 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let now = hung.load(Ordering::SeqCst);
            stable = if now > 0 && now == in_flight {
                stable + 1
            } else {
                0
            };
            in_flight = now;
        }
        for write in &writes {
            write.abort();
        }
        stuck.abort();
        drop(writes);
        drop(db);
        drop(st);
        let _ = std::fs::remove_dir_all(root);
        assert!(
            in_flight < 200,
            "{in_flight} replica writes are in flight to a peer that never answers"
        );
    }

    #[test]
    fn peer_map_with_foreign_node_count_is_not_adoptable() {
        let mut foreign = crate::cluster::ShardMap::balanced(4, 4, 1);
        foreign.version = 5;
        assert!(!adoptable_peer_map(&foreign, 1, 3));
        let mut ours = crate::cluster::ShardMap::balanced(4, 3, 1);
        ours.version = 5;
        assert!(adoptable_peer_map(&ours, 1, 3));
        assert!(adoptable_peer_map(&ours, 5, 3));
        assert!(!adoptable_peer_map(&ours, 6, 3));
    }

    #[test]
    fn since_token_round_trips_fallback_marks() {
        let marks = vec![FallbackMark {
            node: 1,
            shards: vec![0, 3],
            seq: 42,
        }];
        let token = encode_token(&[5, 0, 7], &marks);
        assert_eq!(token, "v1:5,0,7~1:0.3:42");
        assert_eq!(decode_since(&token), Some(vec![5, 0, 7]));
        assert_eq!(decode_fallbacks(&token, 3), marks);
        assert!(decode_fallbacks(&token, 1).is_empty());
        assert!(decode_fallbacks("v1:5,0,7~x:1:2", 3).is_empty());
        assert!(decode_fallbacks("7", 3).is_empty());
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

    mod quorum_tally {
        use super::*;
        use proptest::prelude::*;

        /// What a node answers for one document.
        #[derive(Clone, Copy, Debug)]
        enum Verdict {
            Ack,
            Reject,
        }

        /// A cluster of `copies` nodes, every document replicated on all of
        /// them with node 0 as primary. `nodes[n]` is `None` for an
        /// unreachable node, else its verdict per document.
        type Scenario = (usize, Vec<Option<Vec<Verdict>>>, Vec<bool>, Vec<usize>);

        fn scenario() -> impl Strategy<Value = Scenario> {
            (1usize..5, 1usize..5).prop_flat_map(|(docs, copies)| {
                let verdict = prop_oneof![Just(Verdict::Ack), Just(Verdict::Reject)];
                let node = prop::option::of(prop::collection::vec(verdict, docs));
                (
                    Just(docs),
                    prop::collection::vec(node, copies),
                    prop::collection::vec(any::<bool>(), docs),
                    Just((0..copies).collect::<Vec<_>>()).prop_shuffle(),
                )
            })
        }

        fn reply(node: usize, slot: usize, verdict: Verdict) -> Value {
            match verdict {
                Verdict::Ack => json!({"ok": true, "id": format!("d{slot}"), "from": node}),
                Verdict::Reject => {
                    json!({"id": format!("d{slot}"), "error": "conflict", "from": node})
                }
            }
        }

        proptest! {
            #[test]
            fn tally_agrees_with_a_direct_count(
                (docs, nodes, design, order) in scenario(),
                quorum in 1usize..5,
            ) {
                let copies = nodes.len();
                let quorum = quorum.min(copies);
                let ids: Vec<String> = (0..docs)
                    .map(|slot| {
                        if design[slot] { format!("_design/d{slot}") } else { format!("d{slot}") }
                    })
                    .collect();
                let primaries = vec![0usize; docs];
                let replicas = vec![(0..copies).collect::<Vec<_>>(); docs];
                let mut tally = QuorumTally::new(&ids, &primaries, &replicas, quorum);
                let slots: Vec<usize> = (0..docs).collect();
                let mut answered = 0;
                let mut settled_early = false;
                for node in &order {
                    let response = nodes[*node].as_ref().map(|verdicts| {
                        verdicts
                            .iter()
                            .enumerate()
                            .map(|(slot, v)| reply(*node, slot, *v))
                            .collect::<Vec<_>>()
                    });
                    tally.record(*node, slots.clone(), response);
                    answered += 1;
                    if tally.settled() {
                        // Settled means the quorum is met and every design
                        // document has heard from all of its replicas.
                        prop_assert!(tally.met());
                        if design.iter().any(|d| *d) {
                            prop_assert_eq!(answered, copies);
                        }
                        settled_early = true;
                        break;
                    }
                }
                let acks: Vec<usize> = (0..docs)
                    .map(|slot| {
                        let upto = if settled_early { answered } else { copies };
                        order[..upto]
                            .iter()
                            .filter(|n| matches!(&nodes[**n], Some(v) if matches!(v[slot], Verdict::Ack)))
                            .count()
                    })
                    .collect();
                prop_assert_eq!(tally.met(), acks.iter().all(|a| *a >= quorum));
                let met = tally.met();
                for (slot, result) in tally.finish().into_iter().enumerate() {
                    if acks[slot] > 0 {
                        // Any acknowledgement beats any failure, and the
                        // primary's acknowledgement beats the others'.
                        prop_assert!(result.get("error").is_none());
                        let primary_acked = matches!(&nodes[0], Some(v) if matches!(v[slot], Verdict::Ack))
                            && order[..if settled_early { answered } else { copies }].contains(&0);
                        if primary_acked {
                            prop_assert_eq!(&result["from"], &json!(0));
                        }
                    } else {
                        prop_assert!(result.get("error").is_some());
                        prop_assert!(!met);
                    }
                }
            }

            #[test]
            fn a_document_nobody_answered_is_replica_unavailable(docs in 1usize..5) {
                let ids: Vec<String> = (0..docs).map(|slot| format!("d{slot}")).collect();
                let primaries = vec![0usize; docs];
                let replicas = vec![vec![0usize, 1]; docs];
                let mut tally = QuorumTally::new(&ids, &primaries, &replicas, 1);
                prop_assert!(!tally.met());
                tally.record(0, (0..docs).collect(), None);
                tally.record(1, (0..docs).collect(), None);
                prop_assert!(!tally.settled());
                for (slot, result) in tally.finish().into_iter().enumerate() {
                    prop_assert_eq!(&result["error"], &json!("replica_unavailable"));
                    prop_assert_eq!(&result["id"], &json!(format!("d{slot}")));
                }
            }

            #[test]
            fn write_quorum_is_a_majority_unless_overridden(copies in 1usize..8, w in 1usize..12) {
                prop_assert_eq!(write_quorum(0, copies), copies / 2 + 1);
                prop_assert_eq!(write_quorum(w, copies), w.min(copies));
            }
        }
    }
}

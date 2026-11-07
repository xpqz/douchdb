//! Cluster coordination: shard-map routing, replica selection, quorum
//! reads/writes and internal node calls.

use crate::cluster_http::{
    forward_client_auth, internal_request, internal_request_raw, ClusterCtx,
};
use crate::db::{AllDocsOpts, ApiError, ChangesOpts, Db, DbResult, DocOpts};
use crate::params::{doc_ids_from, doc_opts, flag, num, resolve_changes_filter};
use crate::state::AppState;
use axum::http::{HeaderMap, StatusCode};
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
pub(crate) fn preferred_shard_masks(ctx: &ClusterCtx, db: &Db) -> Vec<Vec<bool>> {
    let node_count = ctx.cluster.nodes.len().max(1);
    let mut masks = vec![vec![false; db.q as usize]; node_count];
    let map = db.shard_map();
    for shard in 0..db.q {
        let replicas = map.replicas_of(shard);
        let target = if replicas.contains(&ctx.cluster.me) {
            ctx.cluster.me
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

/// Whether a map reported by a peer may replace ours.
///
/// The node count must match this process's cluster size, or routed requests
/// would index past the node list. Equal versions are eligible so that the
/// deterministic tiebreak in `Db::set_shard_map` can settle concurrent updates.
fn adoptable_peer_map(map: &crate::cluster::ShardMap, version: u64, cluster_size: usize) -> bool {
    map.node_count == cluster_size.max(1) && map.version >= version
}

/// Reconcile versioned shard-map metadata with peers. Newer maps are pulled;
/// older peers receive the local or pending map. This runs from anti-entropy,
/// so a metadata update that missed a peer is repaired after that peer returns.
pub(crate) async fn reconcile_shard_map(ctx: &ClusterCtx, db: &Arc<Db>) {
    if !ctx.cluster.is_clustered() {
        return;
    }
    let local = db.shard_map();
    let mut desired = db
        .pending_shard_map()
        .filter(|pending| pending.version >= local.version)
        .unwrap_or_else(|| local.clone());

    let maps = join_all(ctx.cluster.peers().map(|peer| async move {
        let request = internal_request(
            ctx,
            reqwest::Method::GET,
            peer,
            &["_node", "_local", "_dbs", &db.name],
            None,
        );
        request
            .send()
            .await
            .ok()?
            .json::<crate::cluster::ShardMap>()
            .await
            .ok()
    }))
    .await;

    for map in maps.into_iter().flatten() {
        if adoptable_peer_map(&map, desired.version, ctx.cluster.nodes.len())
            && db.set_shard_map(map.clone()).unwrap_or(false)
        {
            if db
                .pending_shard_map()
                .is_some_and(|pending| pending.version <= map.version)
            {
                // The adopted map supersedes (or beat the tiebreak against) ours.
                db.clear_pending_shard_map();
            }
            desired = map;
        }
    }

    let payload = match serde_json::to_vec(&desired) {
        Ok(payload) => payload,
        Err(_) => return,
    };
    let peers: Vec<String> = ctx
        .cluster
        .nodes
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != ctx.cluster.me)
        .map(|(_, peer)| peer.clone())
        .collect();
    let responses = join_all(peers.into_iter().map(|peer| {
        let payload = payload.clone();
        async move {
            let request = internal_request(
                ctx,
                reqwest::Method::PUT,
                &peer,
                &["_node", "_local", "_dbs", &db.name],
                None,
            )
            .header("content-type", "application/json")
            .body(payload);
            request
                .send()
                .await
                .map(|response| response.status().is_success())
                .unwrap_or(false)
        }
    }))
    .await;
    let acknowledgements = 1 + responses.into_iter().filter(|ok| *ok).count();
    if acknowledgements > ctx.cluster.nodes.len().max(1) / 2 {
        db.clear_pending_shard_map();
    }
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

/// Reconcile the versioned `_security` object with peers: adopt the newest
/// one any peer reports, then push it to the peers that hold something else.
/// This is what brings a restored, new or rollback-missed node up to date.
pub(crate) async fn reconcile_security(ctx: &ClusterCtx, db: &Arc<Db>) {
    if !ctx.cluster.is_clustered() {
        return;
    }
    let fetched = join_all(ctx.cluster.peers().map(|peer| async move {
        let request = internal_request(
            ctx,
            reqwest::Method::GET,
            peer,
            &["_node", "_local", "_dbs", &db.name, "security"],
            None,
        );
        let envelope = request
            .send()
            .await
            .ok()?
            .json::<serde_json::Value>()
            .await
            .ok()?;
        let version = envelope.get("version")?.as_u64()?;
        let security = envelope.get("security")?.clone();
        Some((peer.clone(), version, security))
    }))
    .await;
    let fetched: Vec<(String, u64, serde_json::Value)> = fetched.into_iter().flatten().collect();

    let (mut version, mut security) = db.security_versioned();
    for (_, peer_version, peer_security) in &fetched {
        if security_supersedes(*peer_version, peer_security, version, &security)
            && db
                .set_security_versioned(*peer_version, peer_security.clone())
                .unwrap_or(false)
        {
            version = *peer_version;
            security = peer_security.clone();
        }
    }

    let envelope = serde_json::json!({"version": version, "security": security});
    join_all(
        fetched
            .iter()
            .filter(|(_, peer_version, peer_security)| {
                *peer_version != version || *peer_security != security
            })
            .map(|(peer, _, _)| {
                let request = internal_request(
                    ctx,
                    reqwest::Method::PUT,
                    peer,
                    &["_node", "_local", "_dbs", &db.name, "security"],
                    None,
                )
                .json(&envelope);
                async move {
                    let _ = request.send().await;
                }
            }),
    )
    .await;
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

    let mut replicas = replicas_of_doc(db, docid);
    if let Some(index) = replicas.iter().position(|node| *node == ctx.cluster.me) {
        let local = replicas.remove(index);
        replicas.insert(0, local);
    }
    let responses = join_all(replicas.into_iter().map(|node| {
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
    db: &Db,
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
        let db_handle = st.db(dbname).await?;
        let pending = spawn_quorum_writes(st, &db_handle, dbname, groups, headers, &gate);
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
/// slow or unreachable replica keeps running after the request returns.
fn spawn_quorum_writes(
    st: &AppState,
    db: &Arc<Db>,
    dbname: &str,
    groups: HashMap<usize, SlotDocs>,
    headers: &HeaderMap,
    gate: &Arc<impl Send + Sync + 'static>,
) -> FuturesUnordered<tokio::task::JoinHandle<NodeReply>> {
    let ctx = &st.ctx;
    let pending = FuturesUnordered::new();
    for (node, group) in groups {
        let slots: Vec<usize> = group.iter().map(|(slot, _)| *slot).collect();
        if node == ctx.cluster.me {
            let subdocs: Vec<Value> = group.into_iter().map(|(_, doc)| (*doc).clone()).collect();
            let db = db.clone();
            let gate = gate.clone();
            pending.push(tokio::spawn(async move {
                let _gate = gate;
                (node, slots, db.put_docs(subdocs, true).await)
            }));
        } else {
            let subdocs: Vec<Arc<Value>> = group.into_iter().map(|(_, doc)| doc).collect();
            let client = ctx.client.clone();
            let secret = ctx.cluster.secret.clone();
            let peer = ctx.cluster.nodes[node].clone();
            let dbname = dbname.to_string();
            let headers = headers.clone();
            let gate = gate.clone();
            pending.push(tokio::spawn(async move {
                let _gate = gate;
                (
                    node,
                    slots,
                    forward_bulk_owned(client, secret, peer, dbname, subdocs, true, headers).await,
                )
            }));
        }
    }
    pending
}

/// Return as soon as the tally is settled. The remaining replica writes keep
/// running as detached tasks, so a slow or unreachable replica no longer sets
/// write tail latency.
async fn collect_quorum(
    mut pending: FuturesUnordered<tokio::task::JoinHandle<NodeReply>>,
    mut tally: QuorumTally<'_>,
) -> (Vec<Value>, bool) {
    while let Some(joined) = pending.next().await {
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
    let remote = futures::future::join_all(groups.into_iter().map(|(node, group)| {
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
    }))
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
                for (slot, result) in slots.into_iter().zip(results) {
                    let (Some(reached), Some(out)) = (reached.get_mut(slot), out.get_mut(slot))
                    else {
                        continue;
                    };
                    *reached = true;
                    if primaries.get(slot) == Some(&node) || out.is_null() {
                        *out = result;
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

/// Reassign the shards of failed nodes to live replicas. A shard with no live
/// copy is CouchDB's `{error, {nodedown, <<"progress not possible">>}}`
/// (fabric_streams, fabric_view:check_down_shards), which chttpd maps to 412.
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
                .ok_or_else(|| ApiError::new(412, "nodedown", "progress not possible"))?;
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
    let ClusterRead { ctx, db, .. } = read;
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
        let alive: Vec<usize> = (0..ctx.cluster.nodes.len())
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
    shards: Option<&[bool]>,
) -> DbResult<Value> {
    let ClusterRead {
        ctx,
        db,
        dbname,
        headers,
    } = read;
    if node == ctx.cluster.me {
        if let Some(shards) = shards {
            return db.all_docs_owned(opts, shards).await;
        }
        return db.all_docs(opts).await;
    }
    let peer = &ctx.cluster.nodes[node];
    let mut query: Vec<(&str, String)> = Vec::new();
    if let Some(shards) = shards {
        let selected: Vec<String> = shards
            .iter()
            .enumerate()
            .filter(|(_, owned)| **owned)
            .map(|(shard, _)| shard.to_string())
            .collect();
        query.push(("shards", selected.join(",")));
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
        &[dbname, "_all_docs"],
        Some(headers),
    )
    .query(&query)
    .json(&body);
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
        ctx,
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
    let request = internal_request(
        ctx,
        reqwest::Method::GET,
        peer,
        &[dbname, ddoc, "_view", view_name],
        Some(headers),
    )
    .query(&params);
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
    let request = internal_request(
        ctx,
        reqwest::Method::GET,
        peer,
        &[dbname, ddoc, "_view", view_name],
        Some(headers),
    )
    .query(&params);
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
    let mut js = crate::view::JsReducer::new(reducer, query.limits);
    crate::view::assemble_reduce(partials, query, &mut js)
        .map_err(|reason| crate::view::reduce_failure(&reason))
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
        rows.push(crate::view::Row {
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

pub(crate) fn row_key(row: &crate::view::Row) -> String {
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
    for node in replicas_of_doc(db, ddoc) {
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

fn mask_shards(mask: &[bool]) -> Vec<usize> {
    mask.iter()
        .enumerate()
        .filter(|(_, owned)| **owned)
        .map(|(shard, _)| shard)
        .collect()
}

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
    let (mut since, marks) = if raw_since == Some("now") {
        cluster_update_vector(read).await?
    } else if let Some(raw) = raw_since {
        let vector = match decode_since(raw, n) {
            Some(vector) => vector,
            None => vec![since_component(Some(raw), ctx.cluster.me, n); n],
        };
        (vector, decode_fallbacks(raw, n))
    } else {
        (vec![0; n], Vec::new())
    };
    since.resize(n, 0);
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
    let limit = num(q, "limit");
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
    let responses = scatter(
        read,
        &owned_by_node,
        "changes",
        |node, mask, round| async move {
            // Shards reassigned from a failed owner resume from the mark this
            // node left for exactly this set of shards, else from the start;
            // preferred masks are disjoint, so ids do not overlap.
            let shards = mask_shards(&mask);
            let since = match round {
                Round::Owner => since_ref[node],
                Round::Fallback => marks_ref
                    .iter()
                    .find(|mark| mark.node == node && mark.shards == shards)
                    .map_or(0, |mark| mark.seq),
            };
            let value = fetch_node_changes(read, changes, node, &mask, since, node_limit).await?;
            Ok((shards, since, value))
        },
    )
    .await?;

    let mut progress = since.clone();
    let mut fallbacks: Vec<FallbackMark> = Vec::new();
    let mut lanes: Vec<ChangeLane> = Vec::new();
    let mut pending = 0;
    for (node, round, (shards, lane_since, mut value)) in responses {
        pending += node_pending(&value);
        let mark = match round {
            Round::Owner => None,
            Round::Fallback => {
                fallbacks.push(FallbackMark {
                    node,
                    shards,
                    seq: lane_since,
                });
                Some(fallbacks.len() - 1)
            }
        };
        let mut rows: Vec<Value> = match value.get_mut("results").map(std::mem::take) {
            Some(Value::Array(rows)) => rows,
            _ => Vec::new(),
        };
        rows.sort_by_key(|row| row["seq"].as_u64().unwrap_or(0));
        lanes.push(ChangeLane {
            node,
            mark,
            rows: rows.into(),
            last: last_seq_for_node(&value, node, n),
        });
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

pub(crate) async fn fetch_node_changes(
    read: ClusterRead<'_>,
    changes: ChangesFetch<'_>,
    node: usize,
    mask: &[bool],
    since: u64,
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
                owned: Some(mask.to_vec()),
                until: None,
            })
            .await;
    }

    let peer = &ctx.cluster.nodes[node];
    let selected: Vec<String> = mask
        .iter()
        .enumerate()
        .filter(|(_, owned)| **owned)
        .map(|(shard, _)| shard.to_string())
        .collect();
    let mut params: Vec<(&str, String)> =
        vec![("since", since.to_string()), ("shards", selected.join(","))];
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

/// Current per-node update_seq vector, plus fallback marks for the shards of
/// any node that did not answer. Used for `since=now` and for the `update_seq`
/// reported in `GET /db`.
///
/// Like CouchDB's `fabric_db_info`, an unreachable node is fine while another
/// replica holds each of its shards; the feed then reads those shards from the
/// replica, so the mark starts at that replica's current sequence. When some
/// shard has no live copy the answer is 412 `nodedown` / `progress not
/// possible`. The unreachable node's own slot stays 0, so after it returns its
/// changes are replayed rather than skipped.
pub(crate) async fn cluster_update_vector(
    read: ClusterRead<'_>,
) -> DbResult<(Vec<u64>, Vec<FallbackMark>)> {
    let ClusterRead {
        ctx,
        db,
        dbname,
        headers,
    } = read;
    let seqs = futures::future::join_all((0..ctx.cluster.nodes.len()).map(|node| async move {
        if node == ctx.cluster.me {
            return Some(db.update_seq());
        }
        let peer = &ctx.cluster.nodes[node];
        let request = internal_request(ctx, reqwest::Method::GET, peer, &[dbname], Some(headers))
            .header("x-douchdb-update-seq", "1");
        let response = request.send().await.ok()?;
        if !response.status().is_success() {
            return None;
        }
        response.json::<Value>().await.ok()?["update_seq"].as_u64()
    }))
    .await;
    let failed: Vec<usize> = (0..seqs.len())
        .filter(|node| seqs[*node].is_none())
        .collect();
    let alive: Vec<usize> = (0..seqs.len())
        .filter(|node| seqs[*node].is_some())
        .collect();
    let vector: Vec<u64> = seqs.iter().map(|seq| seq.unwrap_or(0)).collect();
    if failed.is_empty() {
        return Ok((vector, Vec::new()));
    }
    let groups = fallback_shard_masks(db, &preferred_shard_masks(ctx, db), &failed, &alive)?;
    let marks = groups
        .into_iter()
        .map(|(node, mask)| FallbackMark {
            node,
            shards: mask_shards(&mask),
            seq: vector.get(node).copied().unwrap_or(0),
        })
        .collect();
    Ok((vector, marks))
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
    // `v1:` is still an opaque token to clients; `decode_since` accepts both.
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

/// Decodes a since token for a cluster of `nodes` nodes. A legacy token naming
/// a node index outside the cluster is invalid (`None`) rather than allocated.
pub(crate) fn decode_since(raw: &str, nodes: usize) -> Option<Vec<u64>> {
    let raw = raw.trim_matches('"');
    if let Some(rest) = raw.strip_prefix("v1:") {
        let mut out = Vec::new();
        let rest = rest.split('~').next().unwrap_or(rest);
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
    mask: &[bool],
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
    if !mask.iter().any(|owned| *owned) {
        return Ok(json!({"total_rows": 0, "rows": [], "bookmark": "search:0"}));
    }
    let mut local = request.clone();
    local.owner_shards = Some(mask.to_vec());
    local.bookmark = None;
    local.limit = window;
    if node == ctx.cluster.me {
        return crate::search::query(db, ddoc, index_name, &local, limits).await;
    }

    let peer = &ctx.cluster.nodes[node];
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

    let outgoing = internal_request(
        ctx,
        reqwest::Method::GET,
        peer,
        &[dbname, ddoc, "_search", index_name],
        Some(headers),
    )
    .query(&params);
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
        assert_eq!(decode_since(&token, 3), Some(vec![5, 0, 7]));
        assert_eq!(decode_fallbacks(&token, 3), marks);
        assert!(decode_fallbacks(&token, 1).is_empty());
        assert!(decode_fallbacks("v1:5,0,7~x:1:2", 3).is_empty());
        assert!(decode_fallbacks("7", 3).is_empty());
    }

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

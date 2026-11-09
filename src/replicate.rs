//! Pull replication: the same protocol PouchDB uses, spoken node to node.

use crate::auth::Auth;
use crate::cluster::Cluster;
use crate::cluster_http::{internal_request_raw, send_peer};
use crate::db::Db;
use crate::state::AppState;
use crate::store::PurgeInfo;
use anyhow::{anyhow, Result};
use futures::future::join_all;
use md5::{Digest, Md5};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Default)]
pub struct PullOutcome {
    pub docs: usize,
    pub checkpoint: Option<Value>,
}

/// Checkpoint namespace version. Bumped when the replication filter changes in
/// a way that must re-scan history: v2 always replicates design documents, so
/// old checkpoints that skipped them must not be reused.
const CHECKPOINT_VERSION: u8 = 2;

/// Copy everything the peer has that this node replicates, then checkpoint.
///
/// One call drains the peer: it keeps requesting `_changes` pages until the
/// peer has no more rows. Anti-entropy used to pull a single 200-document page
/// per sync interval, which capped repair at 200/interval even when both sides
/// were idle.
pub async fn pull(
    db: &Db,
    peer: &str,
    client: &reqwest::Client,
    cluster: &Cluster,
    auth: Option<&Auth>,
    shutdown: &CancellationToken,
) -> Result<PullOutcome> {
    let ctx = PullCtx {
        db,
        peer,
        client,
        cluster,
        auth,
    };
    ctx.sync_purges().await?;
    let checkpoint_id = checkpoint_id(db, peer);
    let mut since = db
        .checkpoint_get(&checkpoint_id)
        .await
        .ok()
        .flatten()
        .and_then(|v| v.get("seq").cloned())
        .unwrap_or_else(|| json!(0));

    let mut total = 0usize;
    loop {
        // Between batches, so a stop leaves the checkpoint at the last whole
        // batch and the next pass resumes from there.
        if shutdown.is_cancelled() {
            break;
        }
        let resp = ctx.fetch_changes_batch(&since).await?;
        let results = resp
            .get("results")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("_changes response has no results array"))?;
        if results.is_empty() {
            break;
        }

        let (mut requested, mut reqs) = ctx.revisions_to_request(results)?;
        ctx.drop_purged(&mut requested, &mut reqs).await?;
        let (received, unavailable) = ctx.fetch_missing_docs(&reqs).await?;
        total += write_batch(db, &received).await?;
        ensure_all_accounted(db, &requested, &received, &unavailable).await?;

        let last = resp
            .get("last_seq")
            .cloned()
            .ok_or_else(|| anyhow!("_changes response has no last_seq"))?;
        if last.is_null() {
            return Err(anyhow!("_changes response has null last_seq"));
        }
        if last == since {
            break;
        }
        db.checkpoint_put(&checkpoint_id, &json!({ "seq": last }))
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
        since = last;
    }
    Ok(PullOutcome {
        docs: total,
        checkpoint: Some(since),
    })
}

/// Everything one pull needs to talk to a peer about one database.
struct PullCtx<'a> {
    db: &'a Db,
    peer: &'a str,
    client: &'a reqwest::Client,
    cluster: &'a Cluster,
    auth: Option<&'a Auth>,
}

/// A set of `(doc id, rev)` pairs.
type RevPairs = BTreeSet<(String, String)>;

/// Revisions the peer returned, and revisions it explicitly no longer has.
type Fetched = (BTreeMap<(String, String), Value>, RevPairs);

/// Include the replica placement in the checkpoint id. When placement changes,
/// the new replica starts from zero instead of skipping documents that were
/// filtered out under the old map. Only the replica lists matter: the version
/// counter changes without changing what this node replicates, and keying on
/// it would re-pull every document after each metadata bump.
fn checkpoint_id(db: &Db, peer: &str) -> String {
    let map_tag = crate::util::hex(&Md5::digest(
        serde_json::to_vec(&db.replication_map().replicas).unwrap_or_default(),
    ));
    format!("ae{CHECKPOINT_VERSION}:{}:{}", peer, map_tag)
}

/// Where this node remembers how far it has read the peer's purge infos.
fn purge_checkpoint_id(db: &Db, peer: &str) -> String {
    format!("purge-{}", checkpoint_id(db, peer))
}

impl PullCtx<'_> {
    /// Learn of the purges the peer knows of (CouchDB's
    /// `mem3_rep:replicate_purges`): record and apply every purge info the
    /// peer holds past this node's checkpoint, for the shards this node
    /// replicates. Runs before any document is pulled, so a revision purged
    /// here is already known as purged when the peer offers it.
    async fn sync_purges(&self) -> Result<()> {
        let checkpoint = purge_checkpoint_id(self.db, self.peer);
        let since: Vec<u64> = self
            .db
            .checkpoint_get(&checkpoint)
            .await
            .ok()
            .flatten()
            .and_then(|doc| serde_json::from_value(doc.get("seqs")?.clone()).ok())
            .unwrap_or_default();
        let me = self.cluster.me.to_string();
        let list = since
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let mut request = internal_request_raw(
            self.client,
            &self.cluster.secret,
            reqwest::Method::GET,
            self.peer,
            &[&self.db.name, "_peer_purged_infos"],
        )
        .query(&[("since", list.as_str()), ("node", me.as_str())]);
        if let Some(auth) = self.auth {
            request = request.basic_auth(&auth.user, Some(&auth.pass));
        }
        let reply: Value = send_peer(request).await?.error_for_status()?.json().await?;
        let shards = reply
            .get("shards")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("_peer_purged_infos response has no shards array"))?;
        let mut seqs = Vec::with_capacity(shards.len());
        let mut removed = false;
        for (shard, entry) in shards.iter().enumerate() {
            seqs.push(
                entry
                    .get("purge_seq")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| anyhow!("_peer_purged_infos shard has no purge_seq"))?,
            );
            let infos: Vec<PurgeInfo> =
                serde_json::from_value(entry.get("infos").cloned().unwrap_or_else(|| json!([])))?;
            let infos: Vec<PurgeInfo> = infos
                .into_iter()
                .filter(|info| crate::fabric::is_replica_of(self.db, &info.id, self.cluster.me))
                .collect();
            removed |= self
                .db
                .apply_purge_infos(shard, infos, self.cluster.me)
                .await
                .map_err(|e| anyhow!(e.to_string()))?;
        }
        if removed {
            // A purge leaves no change row, so the indexes cannot follow it.
            crate::view::invalidate(self.db)
                .await
                .map_err(|e| anyhow!(e.to_string()))?;
            crate::search::cleanup(self.db)
                .await
                .map_err(|e| anyhow!(e.to_string()))?;
        }
        if seqs != since {
            self.db
                .checkpoint_put(&checkpoint, &json!({ "seqs": seqs }))
                .await
                .map_err(|e| anyhow!(e.to_string()))?;
        }
        Ok(())
    }

    /// Leave out the revisions a local purge info lists: internal replication
    /// must not bring a purged revision back. (Client writes with
    /// `new_edits=false` may, as in CouchDB.)
    async fn drop_purged(&self, requested: &mut RevPairs, reqs: &mut Vec<Value>) -> Result<()> {
        let purged = self
            .db
            .purged_among(requested)
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
        if purged.is_empty() {
            return Ok(());
        }
        requested.retain(|pair| !purged.contains(pair));
        reqs.retain(|req| {
            let field = |name: &str| req.get(name).and_then(Value::as_str).map(str::to_string);
            match (field("id"), field("rev")) {
                (Some(id), Some(rev)) => !purged.contains(&(id, rev)),
                _ => true,
            }
        });
        Ok(())
    }

    /// One `_changes` page from the peer.
    async fn fetch_changes_batch(&self, since: &Value) -> Result<Value> {
        const BATCH: usize = 1000;
        // A cluster `_changes` last_seq is an opaque base64 version vector, so
        // forward whatever the peer gave us rather than parsing it as an integer.
        let since_param = match since {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };

        let mut changes = internal_request_raw(
            self.client,
            &self.cluster.secret,
            reqwest::Method::GET,
            self.peer,
            &[&self.db.name, "_changes"],
        )
        .query(&[
            ("since", since_param.as_str()),
            ("style", "all_docs"),
            ("limit", &BATCH.to_string()),
        ]);
        if let Some(auth) = self.auth {
            changes = changes.basic_auth(&auth.user, Some(&auth.pass));
        }
        Ok(send_peer(changes).await?.error_for_status()?.json().await?)
    }

    /// A node only stores the shards it replicates. Filter before the
    /// _bulk_get request so every revision we ask for is one we need.
    fn revisions_to_request(&self, results: &[Value]) -> Result<(RevPairs, Vec<Value>)> {
        let mut requested: BTreeSet<(String, String)> = BTreeSet::new();
        let mut reqs = Vec::new();
        for row in results {
            let id = row
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("_changes row has no id"))?;
            let changes = row
                .get("changes")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("_changes row {id} has no changes array"))?;
            for change in changes {
                let rev = change
                    .get("rev")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("_changes row {id} has no rev"))?;
                if !crate::fabric::is_replica_of(self.db, id, self.cluster.me) {
                    continue;
                }
                if requested.insert((id.to_string(), rev.to_string())) {
                    // `revs` preserves the revision history; without it the
                    // target stores a leaf with no ancestor metadata.
                    reqs.push(json!({
                        "id": id,
                        "rev": rev,
                        "revs": true
                    }));
                }
            }
        }
        Ok((requested, reqs))
    }

    /// `_bulk_get` the requested revisions from the peer.
    async fn fetch_missing_docs(&self, reqs: &[Value]) -> Result<Fetched> {
        let mut received: BTreeMap<(String, String), Value> = BTreeMap::new();
        let mut unavailable: BTreeSet<(String, String)> = BTreeSet::new();
        if reqs.is_empty() {
            return Ok((received, unavailable));
        }
        let mut bulk = internal_request_raw(
            self.client,
            &self.cluster.secret,
            reqwest::Method::POST,
            self.peer,
            &[&self.db.name, "_bulk_get"],
        )
        .json(&json!({"docs": reqs}));
        if let Some(auth) = self.auth {
            bulk = bulk.basic_auth(&auth.user, Some(&auth.pass));
        }
        let got: Value = send_peer(bulk).await?.error_for_status()?.json().await?;
        let result_rows = got
            .get("results")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("_bulk_get response has no results array"))?;

        for row in result_rows {
            let docs = row
                .get("docs")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("_bulk_get result has no docs array"))?;
            for entry in docs {
                if let Some(ok) = entry.get("ok") {
                    let id = ok
                        .get("_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow!("_bulk_get ok entry has no _id"))?;
                    let rev = ok
                        .get("_rev")
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow!("_bulk_get ok entry has no _rev"))?;
                    received.insert((id.to_string(), rev.to_string()), ok.clone());
                } else if let Some(error) = entry.get("error") {
                    let id = error
                        .get("id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow!("_bulk_get error has no id"))?;
                    let rev = error
                        .get("rev")
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow!("_bulk_get error has no rev"))?;
                    let kind = error
                        .get("error")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown");
                    // A peer that explicitly no longer has the revision
                    // (purged/pruned) is a terminal answer. Transient or
                    // malformed failures are not.
                    if kind == "not_found" || kind == "deleted" {
                        tracing::warn!(
                            "peer {} could not supply {id}@{rev}: {kind}; treating as unavailable",
                            self.peer
                        );
                        unavailable.insert((id.to_string(), rev.to_string()));
                    } else {
                        return Err(anyhow!(
                            "_bulk_get failed for {id}@{rev}: {}",
                            error.get("reason").and_then(Value::as_str).unwrap_or(kind)
                        ));
                    }
                } else {
                    return Err(anyhow!("_bulk_get entry is neither ok nor error"));
                }
            }
        }
        Ok((received, unavailable))
    }
}

/// Apply the retrieved revisions and fail if any local write failed. Returns
/// how many documents were written.
async fn write_batch(db: &Db, received: &BTreeMap<(String, String), Value>) -> Result<usize> {
    let docs: Vec<Value> = received.values().cloned().collect();
    if docs.is_empty() {
        return Ok(0);
    }
    let count = docs.len();
    let applied = db
        .put_docs(docs, false)
        .await
        .map_err(|e| anyhow!(e.to_string()))?;
    if let Some(error) = applied.iter().find(|result| !result.is_ok()) {
        return Err(anyhow!("replicated document was not applied: {error}"));
    }
    Ok(count)
}

/// Revisions omitted from the response are acceptable only when they already
/// exist locally. This also covers idempotent duplicate pulls.
async fn ensure_all_accounted(
    db: &Db,
    requested: &BTreeSet<(String, String)>,
    received: &BTreeMap<(String, String), Value>,
    unavailable: &BTreeSet<(String, String)>,
) -> Result<()> {
    let mut unaccounted: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (id, rev) in requested {
        let key = (id.clone(), rev.clone());
        if received.contains_key(&key) || unavailable.contains(&key) {
            continue;
        }
        unaccounted.entry(id.clone()).or_default().push(rev.clone());
    }
    if unaccounted.is_empty() {
        return Ok(());
    }
    let body = Value::Object(
        unaccounted
            .iter()
            .map(|(id, revs)| (id.clone(), json!(revs)))
            .collect(),
    );
    let diff = db
        .revs_diff(&body)
        .await
        .map_err(|e| anyhow!(e.to_string()))?;
    let diff: BTreeMap<&str, &crate::db::RevsDiffEntry> = diff
        .0
        .iter()
        .map(|(id, entry)| (id.as_str(), entry))
        .collect();
    let mut missing = Vec::new();
    for (id, revs) in &unaccounted {
        let missing_set: BTreeSet<&str> = diff
            .get(id.as_str())
            .into_iter()
            .flat_map(|entry| entry.missing.iter().map(String::as_str))
            .collect();
        for rev in revs {
            if missing_set.contains(rev.as_str()) {
                missing.push(format!("{id}@{rev}"));
            }
        }
    }
    if !missing.is_empty() {
        return Err(anyhow!(
            "_bulk_get did not return {} requested revision(s): {}",
            missing.len(),
            missing.join(", ")
        ));
    }
    Ok(())
}

/// How many databases that are not open an anti-entropy pass opens: a quarter
/// of `--max-open-dbs`, at least one.
fn cold_budget(max_open_dbs: usize) -> usize {
    (max_open_dbs / 4).max(1)
}

/// The databases one anti-entropy pass works through, and where the next
/// pass resumes.
///
/// Every open database is included: it costs nothing to reach. Of the closed
/// ones, at most `budget` are taken in name order starting after `cursor`
/// and wrapping around, so successive passes visit every database and no
/// pass opens more than `budget` of them. `names` is sorted.
fn sweep_order(
    names: &[String],
    open: &std::collections::HashSet<String>,
    cursor: Option<&str>,
    budget: usize,
) -> (Vec<String>, Option<String>) {
    let (hot, cold): (Vec<&String>, Vec<&String>) =
        names.iter().partition(|name| open.contains(*name));
    let start = cursor.map_or(0, |after| {
        cold.partition_point(|name| name.as_str() <= after)
    });
    let taken: Vec<&String> = cold
        .iter()
        .skip(start)
        .chain(cold.iter().take(start))
        .take(budget)
        .copied()
        .collect();
    let next = taken
        .last()
        .map(|name| name.to_string())
        .or_else(|| cursor.map(str::to_string));
    let order = hot.into_iter().chain(taken).cloned().collect();
    (order, next)
}

/// Route label under which the peer calls of anti-entropy metadata sync are
/// counted in the metrics (`peer_calls` per route, one request per pass).
const ANTI_ENTROPY_ROUTE: &str = "_background/anti_entropy";

/// Run one anti-entropy pass over the databases and peers. Returns false when
/// a pass is already in flight.
///
/// A pass covers the open databases and a bounded share of the closed ones
/// (see `sweep_order`), going through the cache as least recently used so
/// that it neither opens nor closes the whole set when there are more
/// databases than `--max-open-dbs`. A named database is always processed. Returns false when a
/// pass is already in flight.
pub async fn sync_once(state: Arc<AppState>, only_db: Option<String>) -> bool {
    let Ok(_background) = state.admission.try_background() else {
        return false;
    };
    if !state.cluster_state.begin_replication() {
        return false;
    }
    let peers: Vec<String> = state.ctx.cluster.peers().cloned().collect();
    let mut names = state.db_names().await;
    names.sort();
    if let Some(only) = only_db {
        names.retain(|name| name == &only);
    } else {
        let open = state.open_db_names().await;
        let budget = cold_budget(state.config.max_open_dbs);
        let (order, cursor) = sweep_order(
            &names,
            &open,
            state.cluster_state.sweep_cursor().as_deref(),
            budget,
        );
        state.cluster_state.set_sweep_cursor(cursor);
        names = order;
    }

    // Anti-entropy runs outside any client request, so its metadata calls
    // are counted in a scope of their own, one per pass, under a fixed route
    // label (never derived from a path).
    let metadata_stats = crate::cluster_http::PeerStats::new(ANTI_ENTROPY_ROUTE.to_string(), false);
    crate::cluster_http::with_stats(metadata_stats, crate::fabric::sync_metadata(&state, &names))
        .await;
    let mut error: Option<String> = None;
    for name in names {
        if state.shutdown.is_cancelled() {
            break;
        }
        let Ok(db) = state.db_cold(&name).await else {
            continue;
        };
        let pulls = peers.iter().map(|peer| {
            let db = db.clone();
            let peer = peer.clone();
            let state = state.clone();
            let name = name.clone();
            async move {
                state.cluster_state.replication_started(&name, &peer);
                let result = pull(
                    &db,
                    &peer,
                    &state.ctx.client,
                    &state.ctx.cluster,
                    state.ctx.auth.as_ref(),
                    &state.shutdown,
                )
                .await;
                match result {
                    Ok(outcome) => {
                        state.cluster_state.replication_finished(
                            &name,
                            &peer,
                            outcome.docs as u64,
                            outcome.checkpoint,
                        );
                        Ok(outcome.docs)
                    }
                    Err(cause) => {
                        state
                            .cluster_state
                            .replication_failed(&name, &peer, cause.to_string());
                        Err((peer, cause.to_string()))
                    }
                }
            }
        });
        for result in join_all(pulls).await {
            match result {
                Ok(0) => {}
                Ok(n) => tracing::info!("replicated {n} docs of {name}"),
                Err((peer, cause)) => {
                    tracing::warn!("replication {name} from {peer} failed: {cause}");
                    if error.is_none() {
                        error = Some(cause);
                    }
                }
            }
        }
        drop(db);
        state.release_cold().await;
    }
    state.cluster_state.finish_replication(error);
    true
}

/// Pull from every peer on a timer. This is the non-client-induced replication
/// path: a node catches up from its peers even when no client reads or writes.
pub fn spawn_anti_entropy(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let shutdown = state.shutdown.clone();
        let interval = Duration::from_secs(state.config.sync_secs.max(1));
        let run = async {
            loop {
                if !state.cluster_state.replication_paused() {
                    sync_once(state.clone(), None).await;
                }
                tokio::time::sleep(interval).await;
            }
        };
        tokio::select! {
            () = run => {}
            () = shutdown.cancelled() => {}
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Clone)]
    struct Stub {
        partial_sent: Arc<AtomicBool>,
    }

    async fn changes() -> Json<Value> {
        Json(json!({
            "results": [
                {"seq": 1, "id": "a", "changes": [{"rev": "1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]},
                {"seq": 2, "id": "b", "changes": [{"rev": "1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}]}
            ],
            "last_seq": 2
        }))
    }

    async fn no_purges() -> Json<Value> {
        Json(json!({"shards": []}))
    }

    async fn bulk_get(State(stub): State<Stub>) -> Json<Value> {
        let a = json!({"id": "a", "docs": [{"ok": {
            "_id": "a",
            "_rev": "1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "_revisions": {"start": 1, "ids": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]},
            "v": 1
        }}]});
        if !stub.partial_sent.swap(true, Ordering::SeqCst) {
            return Json(json!({"results": [a]}));
        }
        let b = json!({"id": "b", "docs": [{"ok": {
            "_id": "b",
            "_rev": "1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "_revisions": {"start": 1, "ids": ["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]},
            "v": 2
        }}]});
        Json(json!({"results": [a, b]}))
    }

    /// A version bump that leaves every replica list alone must keep the
    /// checkpoint: the filter has not changed, so re-pulling from zero is waste.
    #[test]
    fn version_only_shard_map_bump_keeps_the_checkpoint_id() {
        let root = std::env::temp_dir().join(format!("douchdb-ckid-{}", crate::util::uuid()));
        let db = Db::open(
            &root,
            "ck",
            crate::db::DbConfig {
                q: 2,
                revs_limit: 100,
                shard_map: crate::cluster::ShardMap::balanced(2, 2, 1),
                partitioned: false,
            },
        )
        .expect("db");
        let before = checkpoint_id(&db, "http://peer");
        let mut bumped = db.shard_map();
        bumped.version += 1;
        assert!(db.set_shard_map(bumped).expect("set map"));
        assert_eq!(db.shard_map().version, 1);
        assert_eq!(before, checkpoint_id(&db, "http://peer"));

        let mut moved = db.shard_map();
        moved.version += 1;
        moved.replicas = vec![vec![1], vec![0]];
        assert!(db.set_shard_map(moved).expect("set map"));
        assert_ne!(before, checkpoint_id(&db, "http://peer"));
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A pull stops between batches once shutdown starts, instead of draining
    /// the peer: with the token already cancelled it asks for no changes.
    #[tokio::test]
    async fn pull_stops_between_batches_on_shutdown() {
        async fn asked(State(asked): State<Arc<AtomicBool>>) -> Json<Value> {
            asked.store(true, Ordering::SeqCst);
            Json(json!({"results": [], "last_seq": 0}))
        }
        let root = std::env::temp_dir().join(format!("douchdb-stop-{}", crate::util::uuid()));
        let db = Db::open(
            &root,
            "ae",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 100,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .expect("db");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let addr = listener.local_addr().expect("addr");
        let changes_asked = Arc::new(AtomicBool::new(false));
        let app = Router::new()
            .route("/ae/_peer_purged_infos", get(no_purges))
            .route("/ae/_changes", get(asked))
            .with_state(changes_asked.clone());
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let peer = format!("http://{addr}");
        let cluster = Cluster {
            nodes: vec![peer.clone()],
            me: 0,
            n: 1,
            secret: "test-secret".to_string(),
        };
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let outcome = pull(
            &db,
            &peer,
            &reqwest::Client::new(),
            &cluster,
            None,
            &shutdown,
        )
        .await
        .expect("a stopped pull is not an error");
        assert_eq!(outcome.docs, 0);
        assert!(
            !changes_asked.load(Ordering::SeqCst),
            "a cancelled pull still asked for changes"
        );
        server.abort();
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn partial_bulk_get_does_not_advance_the_checkpoint() {
        let root = std::env::temp_dir().join(format!("douchdb-ae-{}", crate::util::uuid()));
        let db = Db::open(
            &root,
            "ae",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 100,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .expect("db");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let addr = listener.local_addr().expect("addr");
        let stub = Stub {
            partial_sent: Arc::new(AtomicBool::new(false)),
        };
        let app = Router::new()
            .route("/ae/_peer_purged_infos", get(no_purges))
            .route("/ae/_changes", get(changes))
            .route("/ae/_bulk_get", post(bulk_get))
            .with_state(stub);
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let peer = format!("http://{addr}");
        let cluster = Cluster {
            nodes: vec![peer.clone()],
            me: 0,
            n: 1,
            secret: "test-secret".to_string(),
        };
        let client = reqwest::Client::new();

        let first = pull(
            &db,
            &peer,
            &client,
            &cluster,
            None,
            &CancellationToken::new(),
        )
        .await;
        assert!(first.is_err(), "partial _bulk_get must fail the pass");
        let checkpoint_id = checkpoint_id(&db, &peer);
        assert!(
            db.checkpoint_get(&checkpoint_id).await.unwrap().is_none(),
            "checkpoint advanced past a missing revision"
        );

        let second = pull(
            &db,
            &peer,
            &client,
            &cluster,
            None,
            &CancellationToken::new(),
        )
        .await
        .expect("second pass");
        assert_eq!(second.checkpoint, Some(json!(2)));
        assert!(db
            .get_doc("b", &crate::db::DocOpts::default())
            .await
            .is_ok());
        let checkpoint = db
            .checkpoint_get(&checkpoint_id)
            .await
            .unwrap()
            .expect("checkpoint");
        assert_eq!(checkpoint["seq"], 2);

        server.abort();
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn pull_sends_node_credentials_and_the_cluster_secret() {
        // Peers accept the cluster secret alone today; the node's own
        // credentials are still sent so a peer that checks them would work.
        type Seen = Arc<std::sync::Mutex<Vec<(String, Option<String>, Option<String>)>>>;
        async fn record(
            State(seen): State<Seen>,
            uri: axum::http::Uri,
            headers: axum::http::HeaderMap,
        ) -> Json<Value> {
            let header = |name: &str| {
                headers
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string)
            };
            seen.lock().unwrap().push((
                uri.path().to_string(),
                header("authorization"),
                header("x-douchdb-secret"),
            ));
            if uri.path().ends_with("_peer_purged_infos") {
                return Json(json!({"shards": []}));
            }
            if uri.path().ends_with("_changes") {
                return Json(json!({
                    "results": [{"seq": 1, "id": "a",
                                 "changes": [{"rev": "1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]}],
                    "last_seq": 1
                }));
            }
            Json(json!({"results": [{"id": "a", "docs": [{"ok": {
                "_id": "a",
                "_rev": "1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "_revisions": {"start": 1, "ids": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]}
            }}]}]}))
        }

        let root = std::env::temp_dir().join(format!("douchdb-auth-{}", crate::util::uuid()));
        let db = Db::open(
            &root,
            "ae",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 100,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .expect("db");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let addr = listener.local_addr().expect("addr");
        let seen: Seen = Arc::default();
        let app = Router::new()
            .route("/ae/_peer_purged_infos", get(record))
            .route("/ae/_changes", get(record))
            .route("/ae/_bulk_get", post(record))
            .with_state(seen.clone());
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let peer = format!("http://{addr}");
        let cluster = Cluster {
            nodes: vec![peer.clone()],
            me: 0,
            n: 1,
            secret: "test-secret".to_string(),
        };
        let auth = Auth::new("node".to_string(), "pw".to_string(), b"k".to_vec());
        pull(
            &db,
            &peer,
            &reqwest::Client::new(),
            &cluster,
            Some(&auth),
            &CancellationToken::new(),
        )
        .await
        .expect("pull");

        use base64::Engine;
        let basic = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("node:pw")
        );
        let seen = seen.lock().unwrap().clone();
        let paths: Vec<&str> = seen.iter().map(|(path, _, _)| path.as_str()).collect();
        assert!(paths.contains(&"/ae/_changes"), "{paths:?}");
        assert!(paths.contains(&"/ae/_bulk_get"), "{paths:?}");
        for (path, authorization, secret) in &seen {
            assert_eq!(authorization.as_deref(), Some(basic.as_str()), "{path}");
            assert_eq!(secret.as_deref(), Some("test-secret"), "{path}");
        }

        server.abort();
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Successive passes reach every closed database, each pass takes at
    /// most the budget of them, and the open ones are in every pass.
    #[test]
    fn sweep_order_visits_every_database_eventually() {
        let names: Vec<String> = (0..10).map(|n| format!("db{n}")).collect();
        let open: std::collections::HashSet<String> =
            ["db2", "db7"].iter().map(|n| n.to_string()).collect();
        let mut cursor: Option<String> = None;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..4 {
            let (order, next) = sweep_order(&names, &open, cursor.as_deref(), 2);
            let cold = order.iter().filter(|n| !open.contains(*n)).count();
            assert!(cold <= 2, "{cold} closed databases in one pass");
            assert!(order.contains(&"db2".to_string()) && order.contains(&"db7".to_string()));
            seen.extend(order);
            cursor = next;
        }
        assert_eq!(seen.len(), 10, "{seen:?}");
        // With nothing closed there is nothing to resume from.
        let all: std::collections::HashSet<String> = names.iter().cloned().collect();
        let (order, next) = sweep_order(&names, &all, Some("db3"), 2);
        assert_eq!(order.len(), 10);
        assert_eq!(next.as_deref(), Some("db3"));
    }

    /// A pass used to open every database through the cache, so with more
    /// databases than `--max-open-dbs` each pass closed and reopened them all.
    #[tokio::test]
    async fn anti_entropy_pass_opens_a_bounded_number_of_databases() {
        let root = std::env::temp_dir().join(format!("douchdb-ae-{}", crate::util::uuid()));
        let st = crate::state::test_state(root.clone(), 4);
        for n in 0..20 {
            st.create_db(&format!("db{n:02}"), false, None, None)
                .await
                .unwrap();
        }
        let before = st.db_opens.load(Ordering::Relaxed);
        assert!(sync_once(st.clone(), None).await);
        let opened = st.db_opens.load(Ordering::Relaxed) - before;
        let budget = cold_budget(4) as u64;
        assert!(
            opened <= budget,
            "a pass opened {opened} databases, budget is {budget}"
        );
        st.dbs.write().await.clear();
        let _ = std::fs::remove_dir_all(root);
    }
}

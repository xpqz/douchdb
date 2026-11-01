//! Pull replication: the same protocol PouchDB uses, spoken node to node.

use crate::api::AppState;
use crate::auth::Auth;
use crate::cluster::Cluster;
use crate::db::Db;
use anyhow::{anyhow, Result};
use futures::future::join_all;
use md5::{Digest, Md5};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Default)]
pub struct PullOutcome {
    pub docs: usize,
    pub checkpoint: Option<Value>,
}

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
) -> Result<PullOutcome> {
    const BATCH: usize = 1000;

    // Include the shard map in the checkpoint id. When placement changes, the
    // new replica starts from zero instead of skipping documents that were
    // filtered out under the old map.
    let map_tag = crate::db::hex(&Md5::digest(
        serde_json::to_vec(&db.shard_map()).unwrap_or_default(),
    ));
    let checkpoint_id = format!("ae:{}:{}", peer, map_tag);
    let mut since = db
        .local_get(&checkpoint_id)
        .await
        .ok()
        .flatten()
        .and_then(|v| v.get("seq").cloned())
        .unwrap_or_else(|| json!(0));

    let mut total = 0usize;
    loop {
        // A cluster `_changes` last_seq is an opaque base64 version vector, so
        // forward whatever the peer gave us rather than parsing it as an integer.
        let since_param = match &since {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };

        let mut changes = client
            .get(format!("{peer}/{}/_changes", db.name))
            .header("x-douchdb-forwarded", "1")
            .header("x-douchdb-secret", cluster.secret.as_str())
            .query(&[
                ("since", since_param.as_str()),
                ("style", "all_docs"),
                ("limit", &BATCH.to_string()),
            ]);
        if let Some(auth) = auth {
            changes = changes.basic_auth(&auth.user, Some(&auth.pass));
        }
        let resp: Value = changes.send().await?.error_for_status()?.json().await?;

        let results = resp
            .get("results")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("_changes response has no results array"))?;
        if results.is_empty() {
            break;
        }

        // A node only stores the shards it replicates. Filter before the
        // _bulk_get request so every revision we ask for is one we need.
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
                if !crate::fabric::is_replica_of(db, id, cluster.me) {
                    continue;
                }
                if requested.insert((id.to_string(), rev.to_string())) {
                    reqs.push(json!({"id": id, "rev": rev, "attachments": true}));
                }
            }
        }

        let mut received: BTreeMap<(String, String), Value> = BTreeMap::new();
        let mut unavailable: BTreeSet<(String, String)> = BTreeSet::new();
        if !reqs.is_empty() {
            let mut bulk = client
                .post(format!("{peer}/{}/_bulk_get", db.name))
                .header("x-douchdb-forwarded", "1")
                .header("x-douchdb-secret", cluster.secret.as_str())
                .json(&json!({"docs": reqs}));
            if let Some(auth) = auth {
                bulk = bulk.basic_auth(&auth.user, Some(&auth.pass));
            }
            let got: Value = bulk.send().await?.error_for_status()?.json().await?;
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
                                "peer {peer} could not supply {id}@{rev}: {kind}; treating as unavailable"
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
        }

        // Apply the retrieved revisions and fail if any local write failed.
        let docs: Vec<Value> = received.values().cloned().collect();
        if !docs.is_empty() {
            total += docs.len();
            let applied = db
                .put_docs(docs, false)
                .await
                .map_err(|e| anyhow!(e.to_string()))?;
            if let Some(error) = applied.iter().find(|result| result.get("ok").is_none()) {
                return Err(anyhow!("replicated document was not applied: {error}"));
            }
        }

        // Revisions omitted from the response are acceptable only when they
        // already exist locally. This also covers idempotent duplicate pulls.
        let mut unaccounted: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (id, rev) in &requested {
            let key = (id.clone(), rev.clone());
            if received.contains_key(&key) || unavailable.contains(&key) {
                continue;
            }
            unaccounted.entry(id.clone()).or_default().push(rev.clone());
        }
        if !unaccounted.is_empty() {
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
            let mut missing = Vec::new();
            for (id, revs) in &unaccounted {
                let missing_set: BTreeSet<&str> = diff
                    .get(id)
                    .and_then(|entry| entry.get("missing"))
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
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
        }

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
        db.local_put(&checkpoint_id, json!({ "seq": last }))
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
        since = last;
    }
    Ok(PullOutcome {
        docs: total,
        checkpoint: Some(since),
    })
}

/// Run one anti-entropy pass over every database and peer. Returns false when a
/// pass is already in flight.
pub async fn sync_once(state: Arc<AppState>, only_db: Option<String>) -> bool {
    if !state.cluster_state.begin_replication() {
        return false;
    }
    let peers: Vec<String> = state.cluster.peers().cloned().collect();
    let mut names = state.db_names().await;
    names.sort();
    if let Some(only) = only_db {
        names.retain(|name| name == &only);
    }

    let mut error: Option<String> = None;
    for name in names {
        let Ok(db) = state.db(&name).await else {
            continue;
        };
        crate::fabric::reconcile_shard_map(&state, &db).await;
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
                    &state.client,
                    &state.cluster,
                    state.auth.as_ref(),
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
    }
    state.cluster_state.finish_replication(error);
    true
}

/// Pull from every peer on a timer. This is the non-client-induced replication
/// path: a node catches up from its peers even when no client reads or writes.
pub fn spawn_anti_entropy(state: Arc<AppState>) {
    tokio::spawn(async move {
        let interval = Duration::from_secs(state.config.sync_secs.max(1));
        loop {
            if !state.cluster_state.replication_paused() {
                sync_once(state.clone(), None).await;
            }
            tokio::time::sleep(interval).await;
        }
    });
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

    #[tokio::test]
    async fn partial_bulk_get_does_not_advance_the_checkpoint() {
        let root = std::env::temp_dir().join(format!("douchdb-ae-{}", crate::db::uuid()));
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

        let first = pull(&db, &peer, &client, &cluster, None).await;
        assert!(first.is_err(), "partial _bulk_get must fail the pass");
        let map_tag = crate::db::hex(&Md5::digest(
            serde_json::to_vec(&db.shard_map()).unwrap_or_default(),
        ));
        let checkpoint_id = format!("ae:{peer}:{map_tag}");
        assert!(
            db.local_get(&checkpoint_id).await.unwrap().is_none(),
            "checkpoint advanced past a missing revision"
        );

        let second = pull(&db, &peer, &client, &cluster, None)
            .await
            .expect("second pass");
        assert_eq!(second.checkpoint, Some(json!(2)));
        assert!(db
            .get_doc("b", &crate::db::DocOpts::default())
            .await
            .is_ok());
        let checkpoint = db
            .local_get(&checkpoint_id)
            .await
            .unwrap()
            .expect("checkpoint");
        assert_eq!(checkpoint["seq"], 2);

        server.abort();
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }
}

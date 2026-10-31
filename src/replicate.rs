//! Pull replication: the same protocol PouchDB uses, spoken node to node.

use crate::api::AppState;
use crate::auth::Auth;
use crate::cluster::Cluster;
use crate::db::Db;
use anyhow::{anyhow, Result};
use futures::future::join_all;
use md5::{Digest, Md5};
use serde_json::{json, Value};
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

        let results = resp["results"].as_array().cloned().unwrap_or_default();
        if results.is_empty() {
            break;
        }

        let mut reqs = Vec::new();
        for row in &results {
            let id = row["id"].as_str().unwrap_or_default();
            for change in row["changes"].as_array().cloned().unwrap_or_default() {
                if let Some(rev) = change["rev"].as_str() {
                    reqs.push(json!({"id": id, "rev": rev, "attachments": true}));
                }
            }
        }

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

            let mut docs = Vec::new();
            if let Some(results) = got["results"].as_array() {
                for row in results {
                    for entry in row["docs"].as_array().cloned().unwrap_or_default() {
                        if let Some(ok) = entry.get("ok") {
                            docs.push(ok.clone());
                        }
                    }
                }
            }

            // A node only stores the shards it replicates. Without this filter
            // a pull from any peer would turn every node into a full copy.
            docs.retain(|doc| {
                doc.get("_id")
                    .and_then(Value::as_str)
                    .map(|id| crate::fabric::is_replica_of(db, id, cluster.me))
                    .unwrap_or(false)
            });

            total += docs.len();
            if !docs.is_empty() {
                db.put_docs(docs, false)
                    .await
                    .map_err(|e| anyhow!(e.to_string()))?;
            }
        }

        let last = resp["last_seq"].clone();
        if last.is_null() || last == since {
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

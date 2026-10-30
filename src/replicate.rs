//! Pull replication: the same protocol PouchDB uses, spoken node to node.

use crate::auth::Auth;
use crate::cluster::Cluster;
use crate::db::Db;
use anyhow::{anyhow, Result};
use md5::{Digest, Md5};
use serde_json::{json, Value};

/// Copy everything the peer has that this node replicates, then checkpoint.
pub async fn pull(
    db: &Db,
    peer: &str,
    client: &reqwest::Client,
    cluster: &Cluster,
    auth: Option<&Auth>,
) -> Result<usize> {
    // Include the shard map in the checkpoint id. When placement changes, the
    // new replica starts from zero instead of skipping documents that were
    // filtered out under the old map.
    let map_tag = crate::db::hex(&Md5::digest(
        serde_json::to_vec(&db.shard_map()).unwrap_or_default(),
    ));
    let checkpoint_id = format!("ae:{}:{}", peer, map_tag);
    let since = db
        .local_get(&checkpoint_id)
        .await
        .ok()
        .flatten()
        .and_then(|v| v.get("seq").cloned())
        .unwrap_or_else(|| json!(0));
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
            ("limit", "200"),
        ]);
    if let Some(auth) = auth {
        changes = changes.basic_auth(&auth.user, Some(&auth.pass));
    }
    let resp: Value = changes.send().await?.error_for_status()?.json().await?;

    let results = resp["results"].as_array().cloned().unwrap_or_default();
    if results.is_empty() {
        return Ok(0);
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

    // A node only stores the shards it replicates. Without this filter a pull
    // from any peer would turn every node into a full copy of the database.
    docs.retain(|doc| {
        doc.get("_id")
            .and_then(Value::as_str)
            .map(|id| crate::fabric::is_replica_of(db, id, cluster.me))
            .unwrap_or(false)
    });

    let count = docs.len();
    if !docs.is_empty() {
        db.put_docs(docs, false)
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
    }

    let last = resp["last_seq"].clone();
    if !last.is_null() && last != since {
        db.local_put(&checkpoint_id, json!({ "seq": last }))
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
    }
    Ok(count)
}

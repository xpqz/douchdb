//! Pull replication: the same protocol PouchDB uses, spoken node to node.

use crate::db::Db;
use anyhow::{anyhow, Result};
use serde_json::{json, Value};

/// Copy everything the peer has that we lack, then checkpoint.
pub async fn pull(db: &Db, peer: &str, client: &reqwest::Client) -> Result<usize> {
    let checkpoint_id = format!("ae:{}", peer);
    let since = db
        .local_get(&checkpoint_id)
        .await
        .ok()
        .flatten()
        .and_then(|v| v.get("seq").and_then(Value::as_u64))
        .unwrap_or(0);

    let url = format!(
        "{peer}/{}/_changes?since={since}&style=all_docs&limit=200",
        db.name
    );
    let resp: Value = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

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

    let got: Value = client
        .post(format!("{peer}/{}/_bulk_get", db.name))
        .json(&json!({"docs": reqs}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

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

    let count = docs.len();
    if !docs.is_empty() {
        db.put_docs(docs, false)
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
    }

    let last = resp["last_seq"].as_u64().unwrap_or(since);
    if last > since {
        db.local_put(&checkpoint_id, json!({ "seq": last }))
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
    }
    Ok(count)
}

//! CouchDB `validate_doc_update` functions.
//!
//! Design-document validators are JavaScript predicates
//! `function(newDoc, oldDoc, userCtx, secObj)`. A throw of
//! `{forbidden: reason}` or `{unauthorized: reason}` rejects the write. The
//! validator is wrapped in a map function so it runs through the same native,
//! process and WASM sandbox paths (and timeouts) as view code.

use crate::auth::Principal;
use crate::cluster_http::ClusterCtx;
use crate::db::{AllDocsOpts, ApiError, Db, DbResult, DocOpts};
use crate::rev::Rev;
use crate::view::{run_map_json, ViewLimits};
use axum::http::HeaderMap;
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// How long a coordinator reuses validators merged from every node. A
/// design-document write on this node drops them at once; the limit bounds how
/// long a node that missed such a write keeps using the old set.
const CLUSTER_VALIDATORS_TTL: std::time::Duration = std::time::Duration::from_secs(1);

/// Load validators for this write. A single node uses its cached local design
/// documents. Design documents are replicated to every node, but one that
/// missed a design-document write would skip that validator until
/// anti-entropy repaired it, so a cluster coordinator reads the `_design/`
/// range from every reachable node and keeps the newest revision of each.
pub(crate) async fn validation_functions(
    ctx: &ClusterCtx,
    db: &Db,
    dbname: &str,
    headers: &HeaderMap,
) -> DbResult<Vec<String>> {
    if !ctx.cluster.is_clustered() || ctx.cluster.is_internal(headers) {
        return db.validation_functions().await;
    }
    if let Some(functions) = db.cached_cluster_validators(CLUSTER_VALIDATORS_TTL) {
        return Ok(functions);
    }
    let generation = db.design_generation();
    let opts = AllDocsOpts {
        include_docs: true,
        startkey: Some("_design/".to_string()),
        endkey: Some("_design0".to_string()),
        limit: usize::MAX,
        ..Default::default()
    };
    let read = crate::fabric::ClusterRead {
        ctx,
        db,
        dbname,
        headers,
    };
    // The local read is the floor: this node's own design documents must
    // load, or the write fails rather than run unvalidated. It also fails
    // closed on a corrupt local design document.
    db.validation_functions().await?;
    let responses = futures::future::join_all(
        (0..ctx.cluster.nodes.len())
            .map(|node| crate::fabric::fetch_all_docs(read, node, &opts, None)),
    )
    .await;
    let (functions, complete) = merge_validators(responses, ctx.cluster.me)?;
    // A set built while a peer was unreachable is used for this write only.
    // Caching it would keep skipping that peer's newer validators.
    if complete {
        db.cache_cluster_validators(generation, functions.clone());
    }
    Ok(functions)
}

/// Merge the `_design/` ranges read from every node (indexed by node) into
/// validator sources, keeping the newest revision of each design document.
/// The result says whether every node answered.
///
/// Design documents are replicated to every node, so this node's own copy
/// already covers them all unless it missed a write. A peer that fails to
/// answer therefore does not fail the write: the local validators plus those
/// of the reachable peers apply, which only misses a design-document write
/// that reached just the unreachable peer. Failing instead would make every
/// write depend on every node being up. A failed local read is different, and
/// is an error: nothing is known to be validated.
fn merge_validators(responses: Vec<DbResult<Value>>, me: usize) -> DbResult<(Vec<String>, bool)> {
    let mut complete = true;
    let mut newest: BTreeMap<String, ((u64, Vec<u8>), Value)> = BTreeMap::new();
    for (node, response) in responses.into_iter().enumerate() {
        let value = match response {
            Ok(value) => value,
            Err(error) if node == me => return Err(error),
            Err(_) => {
                complete = false;
                continue;
            }
        };
        let Some(rows) = value.get("rows").and_then(Value::as_array) else {
            if node == me {
                return Err(ApiError::new(
                    500,
                    "unknown_error",
                    "local design documents could not be read",
                ));
            }
            complete = false;
            continue;
        };
        for row in rows {
            let (Some(id), Some(rev), Some(doc)) = (
                row.get("id").and_then(Value::as_str),
                row.pointer("/value/rev")
                    .and_then(Value::as_str)
                    .and_then(Rev::parse),
                row.get("doc"),
            ) else {
                continue;
            };
            let order = (rev.pos, rev.id);
            if newest.get(id).is_none_or(|(current, _)| order > *current) {
                newest.insert(id.to_string(), (order, doc.clone()));
            }
        }
    }
    let functions = newest
        .into_values()
        .filter_map(|(_, doc)| {
            doc.get("validate_doc_update")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect();
    Ok((functions, complete))
}

pub(crate) async fn validate_docs_with(
    functions: &[String],
    db: &Db,
    docs: &mut [Value],
    principal: &Principal,
) -> DbResult<Vec<Option<ApiError>>> {
    let mut errors: Vec<Option<ApiError>> =
        std::iter::repeat_with(|| None).take(docs.len()).collect();
    if functions.is_empty() {
        return Ok(errors);
    }

    let user_ctx = json!({
        "db": db.name,
        "name": principal.name,
        "roles": principal.roles,
    });
    let sec_obj = db.security();

    let mut slots = Vec::new();
    let mut ids = Vec::new();
    for (slot, doc) in docs.iter_mut().enumerate() {
        let id = match doc.get("_id").and_then(Value::as_str) {
            Some(id) => id.to_string(),
            None => {
                let id = crate::util::uuid();
                doc["_id"] = json!(id);
                id
            }
        };
        // Design and local documents are not user data. CouchDB does not run
        // validation functions for them.
        if id.starts_with("_design/") || id.starts_with("_local/") {
            continue;
        }
        slots.push(slot);
        ids.push(id);
    }
    if slots.is_empty() {
        return Ok(errors);
    }

    // One round trip per shard for every old document.
    let old_docs = db
        .get_docs(
            &ids,
            &DocOpts {
                include_deleted: true,
                ..Default::default()
            },
        )
        .await?;
    let mut inputs = Vec::with_capacity(slots.len());
    for ((slot, id), old) in slots.iter().zip(ids).zip(old_docs) {
        let old = match old {
            Ok(doc) => Some(doc),
            Err(error) if error.status == 404 => None,
            Err(error) => return Err(error),
        };
        inputs.push(Input {
            id,
            pair: pair_json(&docs[*slot], old.as_ref())?,
        });
    }

    // The validators run user JavaScript, which blocks its thread (and may
    // wait on a worker process), so they run off the async workers.
    let functions = functions.to_vec();
    let outcomes =
        crate::view::blocking(move || Ok(run_validators(&functions, &user_ctx, &sec_obj, inputs)))
            .await?;
    for (slot, outcome) in slots.into_iter().zip(outcomes) {
        errors[slot] = outcome;
    }
    Ok(errors)
}

/// One document to validate: its id and `[newDoc, oldDoc]` as JSON text.
struct Input {
    id: String,
    pair: String,
}

fn pair_json(new_doc: &Value, old_doc: Option<&Value>) -> DbResult<String> {
    let unknown = |error: serde_json::Error| ApiError::new(500, "unknown_error", error.to_string());
    let new = serde_json::to_string(new_doc).map_err(unknown)?;
    let old = serde_json::to_string(old_doc.unwrap_or(&Value::Null)).map_err(unknown)?;
    Ok(format!("[{new},{old}]"))
}

/// Run every validator over the documents, one JavaScript execution per
/// validator for all the documents that have not failed yet. Each document
/// stops at the first validator that rejects it. The result lines up with
/// `inputs`.
fn run_validators(
    functions: &[String],
    user_ctx: &Value,
    sec_obj: &Value,
    inputs: Vec<Input>,
) -> Vec<Option<ApiError>> {
    let mut errors: Vec<Option<ApiError>> =
        std::iter::repeat_with(|| None).take(inputs.len()).collect();
    let docs: Vec<(String, String)> = inputs
        .into_iter()
        .map(|input| (input.id, input.pair))
        .collect();
    for source in functions {
        let pending: Vec<usize> = (0..docs.len())
            .filter(|&index| errors[index].is_none())
            .collect();
        if pending.is_empty() {
            break;
        }
        let batch: Vec<(String, String)> =
            pending.iter().map(|&index| docs[index].clone()).collect();
        for (index, outcome) in pending
            .into_iter()
            .zip(run_validator(source, user_ctx, sec_obj, &batch))
        {
            errors[index] = outcome.err();
        }
    }
    errors
}

/// The outcome of one validator for each document. The documents share one
/// execution; should it fail as a whole (a timeout, say) or not answer once
/// per document, each is run on its own, so one document's failure is its own.
fn run_validator(
    source: &str,
    user_ctx: &Value,
    sec_obj: &Value,
    docs: &[(String, String)],
) -> Vec<Result<(), ApiError>> {
    let wrapped = wrap(source, user_ctx, sec_obj);
    match run_map_json(&wrapped, docs, ViewLimits::default()) {
        Ok(rows) if rows.len() == docs.len() => rows.iter().map(outcome).collect(),
        _ => docs
            .iter()
            .map(|doc| {
                let rows = run_map_json(&wrapped, std::slice::from_ref(doc), ViewLimits::default())
                    .map_err(ApiError::from)?;
                rows.first().map_or_else(
                    || {
                        Err(ApiError::new(
                            500,
                            "unknown_error",
                            "validate_doc_update produced no result",
                        ))
                    },
                    outcome,
                )
            })
            .collect(),
    }
}

/// The validator as a map function over `[newDoc, oldDoc]`. The user context
/// and security object are parsed afresh for every call, so a validator that
/// changes them cannot affect the next document.
fn wrap(source: &str, user_ctx: &Value, sec_obj: &Value) -> String {
    // A JSON string is also a JavaScript string literal.
    let literal = |value: &Value| {
        serde_json::to_string(&value.to_string()).unwrap_or_else(|_| "\"null\"".to_string())
    };
    let user = literal(user_ctx);
    let sec = literal(sec_obj);
    format!(
        r#"(function () {{
  var __user = {user};
  var __sec = {sec};
  return function (pair) {{
    try {{
      ({source})(pair[0], pair[1], JSON.parse(__user), JSON.parse(__sec));
      emit("ok", null);
    }} catch (error) {{
      if (error && typeof error === "object" && error.forbidden !== undefined) {{
        emit("forbidden", String(error.forbidden));
      }} else if (error && typeof error === "object" && error.unauthorized !== undefined) {{
        emit("unauthorized", String(error.unauthorized));
      }} else {{
        emit("error", String((error && error.message) || error));
      }}
    }}
  }};
}})()"#
    )
}

fn outcome(row: &crate::view::Row) -> Result<(), ApiError> {
    let reason = row
        .value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| row.value.to_string());
    match row.key.as_str() {
        Some("ok") => Ok(()),
        Some("forbidden") => Err(ApiError::new(403, "forbidden", reason)),
        Some("unauthorized") => Err(ApiError::new(401, "unauthorized", reason)),
        _ => Err(ApiError::new(500, "unknown_error", reason)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn validate_docs(
        db: &Db,
        docs: &mut [Value],
        principal: &Principal,
    ) -> DbResult<Vec<Option<ApiError>>> {
        let functions = db.validation_functions().await?;
        validate_docs_with(&functions, db, docs, principal).await
    }

    fn design_rows(rows: &[(&str, &str, &str)]) -> DbResult<Value> {
        let rows: Vec<Value> = rows
            .iter()
            .map(|(id, rev, source)| {
                json!({
                    "id": id,
                    "value": {"rev": rev},
                    "doc": {"_id": id, "validate_doc_update": source},
                })
            })
            .collect();
        Ok(json!({"total_rows": rows.len(), "offset": 0, "rows": rows}))
    }

    fn unreachable() -> DbResult<Value> {
        Err(ApiError::new(500, "unknown_error", "node unreachable"))
    }

    #[test]
    fn a_failed_local_read_fails_the_merge() {
        let peer = design_rows(&[("_design/a", "1-aa", "peer")]);
        let error = merge_validators(vec![unreachable(), peer], 0).unwrap_err();
        assert_eq!(error.status, 500);

        let malformed = Ok(json!({"error": "nope"}));
        let peer = design_rows(&[("_design/a", "1-aa", "peer")]);
        assert!(merge_validators(vec![peer, malformed], 1).is_err());
    }

    #[test]
    fn an_unreachable_peer_leaves_the_local_floor_and_is_not_complete() {
        let local = design_rows(&[("_design/a", "1-aa", "local")]);
        let (functions, complete) = merge_validators(vec![local, unreachable()], 0).unwrap();
        assert_eq!(functions, vec!["local".to_string()]);
        assert!(!complete);

        let local = design_rows(&[("_design/a", "1-aa", "local")]);
        let peer = design_rows(&[("_design/b", "1-bb", "peer")]);
        let (functions, complete) = merge_validators(vec![local, peer], 0).unwrap();
        assert_eq!(functions, vec!["local".to_string(), "peer".to_string()]);
        assert!(complete);
    }

    #[test]
    fn the_newest_revision_of_a_design_doc_wins() {
        let local = design_rows(&[("_design/a", "1-aa", "old")]);
        let peer = design_rows(&[("_design/a", "2-bb", "new")]);
        let (functions, _) = merge_validators(vec![local, peer], 0).unwrap();
        assert_eq!(functions, vec!["new".to_string()]);
    }

    fn principal() -> Principal {
        Principal {
            name: Some("alice".into()),
            roles: vec!["analyst".into()],
            admin: false,
        }
    }

    fn open(tag: &str, q: u32) -> (Db, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "douchdb-validate-{tag}-{}-{}",
            std::process::id(),
            crate::util::uuid()
        ));
        let db = Db::open(
            &root,
            "validate",
            crate::db::DbConfig {
                q,
                revs_limit: 1000,
                shard_map: crate::cluster::ShardMap::single(q),
                partitioned: false,
            },
        )
        .unwrap();
        (db, root)
    }

    /// A batch is validated together, yet each document gets its own verdict,
    /// from the first validator that rejects it, with its own old document.
    #[tokio::test]
    async fn a_batch_gets_one_verdict_per_document() {
        let (db, root) = open("batch", 4);
        db.put_docs(
            vec![
                json!({
                    "_id": "_design/a",
                    "validate_doc_update": "function (newDoc, oldDoc) { \
                        if (newDoc.n === 1) { throw {forbidden: 'a-one'}; } \
                        if (oldDoc && oldDoc.locked) { throw {forbidden: 'locked'}; } }"
                }),
                json!({
                    "_id": "_design/b",
                    "validate_doc_update": "function (newDoc, oldDoc, userCtx, secObj) { \
                        if (newDoc.n === 1) { throw {forbidden: 'b-one'}; } \
                        if (newDoc.n === 2) { throw {unauthorized: userCtx.name + secObj.x}; } \
                        if (newDoc.n === 3) { throw new Error('boom'); } }"
                }),
                json!({"_id": "held", "locked": true}),
            ],
            true,
        )
        .await
        .unwrap();
        let mut docs: Vec<Value> = (0..6)
            .map(|n| json!({"_id": format!("d{n}"), "n": n}))
            .collect();
        docs.push(json!({"_id": "held", "n": 9}));
        docs.push(json!({"_id": "_design/skipped", "n": 1}));
        docs.push(json!({"n": 4}));
        let functions = db.validation_functions().await.unwrap();
        assert_eq!(functions.len(), 2);
        let errors = validate_docs_with(&functions, &db, &mut docs, &principal())
            .await
            .unwrap();
        let verdicts: Vec<Option<(u16, String)>> = errors
            .into_iter()
            .map(|error| error.map(|error| (error.status, error.reason)))
            .collect();
        let verdict = |status, reason: &str| Some((status, reason.to_string()));
        assert_eq!(verdicts[0], None);
        // Both validators reject n = 1: the first one named wins.
        assert_eq!(verdicts[1], verdict(403, "a-one"));
        assert_eq!(verdicts[2].as_ref().map(|v| v.0), Some(401));
        assert_eq!(verdicts[3], verdict(500, "boom"));
        assert_eq!(verdicts[4], None);
        assert_eq!(verdicts[5], None);
        assert_eq!(verdicts[6], verdict(403, "locked"));
        assert_eq!(verdicts[7], None, "design documents are not validated");
        assert_eq!(verdicts[8], None);
        assert!(docs[8]["_id"].is_string(), "a missing id is assigned");
        let _ = std::fs::remove_dir_all(root);
    }

    /// A validator that calls `emit` itself breaks the one-row-per-document
    /// contract; each document is then run alone, as it always was.
    #[test]
    fn a_validator_that_emits_is_run_per_document() {
        let source =
            "function (newDoc) { emit('x', 1); if (newDoc.n === 2) { throw {forbidden: 'two'}; } }";
        let docs: Vec<(String, String)> = (0..3)
            .map(|n| (format!("d{n}"), format!("[{{\"n\":{n}}},null]")))
            .collect();
        let outcomes = run_validator(source, &json!({}), &json!({}), &docs);
        let reasons: Vec<Option<(u16, String)>> = outcomes
            .into_iter()
            .map(|outcome| outcome.err().map(|error| (error.status, error.reason)))
            .collect();
        // The first row of each document is the validator's own emit.
        assert_eq!(reasons.len(), 3);
        for reason in reasons {
            assert_eq!(reason.map(|r| r.0), Some(500));
        }
    }

    #[tokio::test]
    async fn design_validators_reject_and_accept_documents() {
        let root = std::env::temp_dir().join(format!(
            "douchdb-validate-{}-{}",
            std::process::id(),
            crate::util::uuid()
        ));
        let db = Db::open(
            &root,
            "validate",
            crate::db::DbConfig {
                q: 1,
                revs_limit: 1000,
                shard_map: crate::cluster::ShardMap::single(1),
                partitioned: false,
            },
        )
        .unwrap();
        db.put_docs(
            vec![json!({
                "_id": "_design/validate",
                "validate_doc_update": "function (newDoc, oldDoc, userCtx, secObj) { \
                    if (userCtx.name !== 'alice') { throw {unauthorized: 'wrong user'}; } \
                    if (newDoc.foo === undefined) { throw {unauthorized: 'missing foo'}; } \
                    if (newDoc.foo === 'bad') { throw {forbidden: 'bad foo'}; } \
                    if (oldDoc && oldDoc.foo === 'locked') { throw {forbidden: 'locked'}; } \
                }"
            })],
            true,
        )
        .await
        .unwrap();

        let mut ok = vec![json!({"foo": "good"})];
        assert!(validate_docs(&db, &mut ok, &principal()).await.unwrap()[0].is_none());

        let mut missing = vec![json!({"bar": 1})];
        let mut result = validate_docs(&db, &mut missing, &principal())
            .await
            .unwrap();
        let error = result[0].take().unwrap();
        assert_eq!(error.status, 401);
        assert_eq!(error.reason, "missing foo");

        let mut bad = vec![json!({"foo": "bad"})];
        let mut result = validate_docs(&db, &mut bad, &principal()).await.unwrap();
        let error = result[0].take().unwrap();
        assert_eq!(error.status, 403);
        assert_eq!(error.reason, "bad foo");

        db.put_docs(vec![json!({"_id": "locked", "foo": "locked"})], true)
            .await
            .unwrap();
        let mut update = vec![json!({"_id": "locked", "_rev": "2-x", "foo": "good"})];
        // The validator sees the old winner even when the submitted revision
        // is stale; the write will then fail on the conflict path.
        let mut result = validate_docs(&db, &mut update, &principal()).await.unwrap();
        let error = result[0].take().unwrap();
        assert_eq!(error.status, 403);
        assert_eq!(error.reason, "locked");

        let _ = std::fs::remove_dir_all(root);
    }
}

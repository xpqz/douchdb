//! CouchDB `validate_doc_update` functions.
//!
//! Design-document validators are JavaScript predicates
//! `function(newDoc, oldDoc, userCtx, secObj)`. A throw of
//! `{forbidden: reason}` or `{unauthorized: reason}` rejects the write. The
//! validator is wrapped in a map function so it runs through the same native,
//! process and WASM sandbox paths (and timeouts) as view code.

use crate::auth::Principal;
use crate::db::{AllDocsOpts, ApiError, Db, DbResult, DocOpts};
use crate::view::{run_map_json, ViewLimits};
use axum::http::HeaderMap;
use serde_json::{json, Value};

/// Load validators for this write. A single node can use the cached local
/// design documents; a cluster coordinator fetches the `_design/` range from
/// every shard owner so a design document on another shard is still applied.
pub(crate) async fn validation_functions(
    st: &crate::api::AppState,
    db: &Db,
    dbname: &str,
    headers: &HeaderMap,
) -> DbResult<Vec<String>> {
    if !st.cluster.is_clustered() || crate::api::is_forwarded(st, headers) {
        return db.validation_functions().await;
    }
    let opts = AllDocsOpts {
        include_docs: true,
        startkey: Some("_design/".to_string()),
        endkey: Some("_design0".to_string()),
        limit: usize::MAX,
        ..Default::default()
    };
    let value = crate::fabric::cluster_all_docs(st, db, dbname, &opts, headers).await?;
    let mut functions = Vec::new();
    if let Some(rows) = value.get("rows").and_then(Value::as_array) {
        for row in rows {
            if let Some(source) = row
                .get("doc")
                .and_then(|doc| doc.get("validate_doc_update"))
                .and_then(Value::as_str)
            {
                functions.push(source.to_string());
            }
        }
    }
    Ok(functions)
}

pub(crate) async fn validate_docs(
    db: &Db,
    docs: &mut [Value],
    principal: &Principal,
) -> DbResult<Vec<Option<ApiError>>> {
    let functions = db.validation_functions().await?;
    validate_docs_with(&functions, db, docs, principal).await
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

    for (slot, doc) in docs.iter_mut().enumerate() {
        let id = match doc.get("_id").and_then(Value::as_str) {
            Some(id) => id.to_string(),
            None => {
                let id = crate::db::uuid();
                doc["_id"] = json!(id);
                id
            }
        };
        // Design and local documents are not user data. CouchDB does not run
        // validation functions for them.
        if id.starts_with("_design/") || id.starts_with("_local/") {
            continue;
        }

        let old_doc = match db
            .get_doc(
                &id,
                &DocOpts {
                    include_deleted: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(doc) => Some(doc),
            Err(error) if error.status == 404 => None,
            Err(error) => return Err(error),
        };

        for source in functions {
            match run_validator(source, doc, old_doc.as_ref(), &user_ctx, &sec_obj) {
                Ok(()) => {}
                Err(error) => {
                    errors[slot] = Some(error);
                    break;
                }
            }
        }
    }
    Ok(errors)
}

fn run_validator(
    source: &str,
    new_doc: &Value,
    old_doc: Option<&Value>,
    user_ctx: &Value,
    sec_obj: &Value,
) -> Result<(), ApiError> {
    let old = serde_json::to_string(old_doc.unwrap_or(&Value::Null))
        .map_err(|error| ApiError::new(500, "unknown_error", error.to_string()))?;
    let user = serde_json::to_string(user_ctx)
        .map_err(|error| ApiError::new(500, "unknown_error", error.to_string()))?;
    let sec = serde_json::to_string(sec_obj)
        .map_err(|error| ApiError::new(500, "unknown_error", error.to_string()))?;
    let wrapped = format!(
        r#"(function () {{
  var __old = {old};
  var __user = {user};
  var __sec = {sec};
  return function (doc) {{
    try {{
      ({source})(doc, __old, __user, __sec);
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
    );

    let id = new_doc
        .get("_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let body = serde_json::to_string(new_doc)
        .map_err(|error| ApiError::new(500, "unknown_error", error.to_string()))?;
    let rows = run_map_json(&wrapped, &[(id, body)], ViewLimits::default())
        .map_err(|error| ApiError::new(500, "unknown_error", error))?;
    let Some(row) = rows.first() else {
        return Err(ApiError::new(
            500,
            "unknown_error",
            "validate_doc_update produced no result",
        ));
    };
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

    fn principal() -> Principal {
        Principal {
            name: Some("alice".into()),
            roles: vec!["analyst".into()],
            admin: false,
        }
    }

    #[tokio::test]
    async fn design_validators_reject_and_accept_documents() {
        let root = std::env::temp_dir().join(format!(
            "douchdb-validate-{}-{}",
            std::process::id(),
            crate::db::uuid()
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

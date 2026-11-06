//! Multi-document endpoints: `_bulk_docs`, `_bulk_get`, `_revs_diff` and
//! `_all_docs`.

use super::*;

pub(super) async fn bulk_docs(
    st: &AppState,
    db: &Db,
    dbname: &str,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
) -> Response {
    let Ok(value) = crate::json::from_slice(body) else {
        return fail(ApiError::bad_request("invalid JSON"));
    };
    let new_edits = match value.get("new_edits") {
        Some(Value::Bool(new_edits)) => *new_edits,
        Some(_) => {
            return fail(ApiError::bad_request(
                "`new_edits` parameter must be a boolean.",
            ))
        }
        None => true,
    };
    let Some(mut docs) = value.get("docs").and_then(Value::as_array).cloned() else {
        return fail(ApiError::bad_request("Missing JSON list of 'docs'"));
    };
    if !new_edits
        && docs
            .iter()
            .any(|doc| doc.get("_rev").is_none() && doc.get("_revisions").is_none())
    {
        return fail(ApiError::bad_request(
            "When `new_edits: false`, the document needs `_rev` or `_revisions` specified",
        ));
    }
    if let Some(response) = require_design_admin(st, db, headers, &docs) {
        return response;
    }
    for doc in &docs {
        if let Err(e) = validate_doc(doc) {
            return fail(e);
        }
        if !db.partitioned {
            match doc.get("_id") {
                Some(Value::String(id))
                    if id.starts_with('_')
                        && !id.starts_with("_design/")
                        && !id.starts_with("_local/") =>
                {
                    return fail(ApiError::new(
                        400,
                        "illegal_docid",
                        "Only reserved document ids may start with underscore.",
                    ))
                }
                Some(Value::String(_)) | None => {}
                Some(_) => {
                    return fail(ApiError::new(
                        400,
                        "illegal_docid",
                        "Document id must be a string",
                    ))
                }
            }
        }
    }
    if value
        .get("all_or_nothing")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        let results: Vec<Value> = docs
            .iter()
            .map(|_| {
                json!({
                    "error": "not_implemented",
                    "reason": "all_or_nothing is not supported"
                })
            })
            .collect();
        return json(StatusCode::EXPECTATION_FAILED, Value::Array(results));
    }

    let principal = st.principal(headers);
    let validation = if is_forwarded(st, headers) {
        std::iter::repeat_with(|| None).take(docs.len()).collect()
    } else {
        match crate::validate::validate_docs(db, &mut docs, &principal).await {
            Ok(errors) => errors,
            Err(error) => return fail(error),
        }
    };

    // `_local` documents live outside the shard/replica machinery. Split them
    // out so a mixed bulk request still writes ordinary documents through the
    // normal cluster path and returns results in request order.
    let mut results = vec![Value::Null; docs.len()];
    let mut local_slots: Vec<(usize, Value)> = Vec::new();
    let mut ordinary_slots: Vec<usize> = Vec::new();
    let mut ordinary_docs: Vec<Value> = Vec::new();
    for (slot, doc) in docs.into_iter().enumerate() {
        if let Some(error) = &validation[slot] {
            results[slot] = json!({
                "id": doc.get("_id").cloned().unwrap_or(Value::Null),
                "error": error.error,
                "reason": error.reason,
            });
        } else if doc
            .get("_id")
            .and_then(Value::as_str)
            .map(|id| id.starts_with("_local/"))
            .unwrap_or(false)
        {
            local_slots.push((slot, doc));
        } else {
            ordinary_slots.push(slot);
            ordinary_docs.push(doc);
        }
    }

    let (ordinary_results, accepted) = if ordinary_docs.is_empty() {
        (Vec::new(), false)
    } else if !st.cluster.is_clustered() || is_forwarded(st, headers) {
        // A forwarded batch has already been placed by its coordinator; write
        // it to this node's shards and let the coordinator count the ack.
        match db.put_docs(ordinary_docs, new_edits).await {
            Ok(results) => (results, false),
            Err(e) => return fail(e),
        }
    } else {
        let w = num(q, "w").unwrap_or(0);
        match commit_docs(st, db, dbname, ordinary_docs, new_edits, w, headers).await {
            Ok((results, quorum)) => (results, !quorum),
            Err(e) => return fail(e),
        }
    };

    for (slot, result) in ordinary_slots.into_iter().zip(ordinary_results) {
        results[slot] = result;
    }
    for (slot, doc) in local_slots {
        results[slot] = put_local_bulk(db, doc).await;
    }
    let mut response = bulk_response(new_edits, results);
    if accepted {
        // CouchDB answers 202 when the write reached fewer than the requested
        // number of replicas; anti-entropy repairs the rest.
        *response.status_mut() = StatusCode::ACCEPTED;
    }
    response
}

/// Design documents are database-admin data even on member-writable
/// endpoints (`_bulk_docs`, `POST /{db}`). Check the whole batch.
pub(super) fn require_design_admin(
    st: &AppState,
    db: &Db,
    headers: &HeaderMap,
    docs: &[Value],
) -> Option<Response> {
    let writes_design = docs.iter().any(|doc| {
        doc.get("_id")
            .and_then(Value::as_str)
            .map(|id| id.starts_with("_design/"))
            .unwrap_or(false)
    });
    if !writes_design {
        return None;
    }
    let principal = st.principal(headers);
    require_db_access(st, &principal, db, true)
}

pub(super) async fn validate_write(
    st: &AppState,
    db: &Db,
    dbname: &str,
    doc: &mut Value,
    headers: &HeaderMap,
) -> Option<Response> {
    let principal = st.principal(headers);
    let functions = match crate::validate::validation_functions(st, db, dbname, headers).await {
        Ok(functions) => functions,
        Err(error) => return Some(fail(error)),
    };
    match crate::validate::validate_docs_with(&functions, db, std::slice::from_mut(doc), &principal)
        .await
    {
        Ok(errors) => errors.into_iter().next().flatten().map(fail),
        Err(error) => Some(fail(error)),
    }
}

async fn put_local_bulk(db: &Db, doc: Value) -> Value {
    let id = doc
        .get("_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let name = id.strip_prefix("_local/").unwrap_or(&id).to_string();
    let deleted = doc
        .get("_deleted")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let result = if deleted {
        db.local_delete(&name).await
    } else {
        db.local_put(&name, doc).await
    };
    match result {
        Ok(value) => value,
        Err(error) => json!({"id": id, "error": error.error, "reason": error.reason}),
    }
}

fn bulk_response(new_edits: bool, results: Vec<Value>) -> Response {
    if new_edits {
        json(StatusCode::CREATED, Value::Array(results))
    } else {
        let errors: Vec<Value> = results
            .into_iter()
            .filter(|result| result.get("error").is_some())
            .collect();
        json(StatusCode::CREATED, Value::Array(errors))
    }
}

/// Place documents on their replica sets.
///
/// Replica candidates for a bulk read, with the coordinator's local replica
/// first when it holds one. The remaining replicas are ordered by the shard
/// map so a failed request can retry elsewhere.
fn bulk_read_candidates(db: &Db, id: &str, me: usize) -> Vec<usize> {
    let (owner, mut replicas) = db.placement_of_doc(id);
    if replicas.is_empty() {
        return vec![owner];
    }
    if replicas.contains(&me) {
        replicas.retain(|node| *node != me);
        replicas.insert(0, me);
    }
    replicas
}

pub(super) async fn bulk_get(
    st: &AppState,
    db: &Db,
    dbname: &str,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
) -> Response {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return fail(ApiError::bad_request("invalid JSON"));
    };
    let attachments = flag(q, "attachments");
    let revs = flag(q, "revs") || value.get("revs").and_then(Value::as_bool).unwrap_or(false);
    let latest = flag(q, "latest");
    if !st.cluster.is_clustered() || is_forwarded(st, headers) {
        return match db.bulk_get(&value, attachments, revs, latest).await {
            Ok(v) => json(StatusCode::OK, v),
            Err(e) => fail(e),
        };
    }
    let Some(reqs) = value.get("docs").and_then(Value::as_array).cloned() else {
        return fail(ApiError::bad_request("Missing JSON list of 'docs'"));
    };
    let mut candidates: Vec<Vec<usize>> = reqs
        .iter()
        .map(|req| {
            let id = req.get("id").and_then(Value::as_str).unwrap_or_default();
            bulk_read_candidates(db, id, st.cluster.me)
        })
        .collect();
    let mut out = vec![Value::Null; reqs.len()];
    let mut pending: Vec<usize> = (0..reqs.len()).collect();
    while !pending.is_empty() {
        let mut groups: HashMap<usize, Vec<usize>> = HashMap::new();
        for slot in pending.drain(..) {
            groups.entry(candidates[slot][0]).or_default().push(slot);
        }
        let requests = groups.into_iter().map(|(node, slots)| {
            let subdocs: Vec<Value> = slots.iter().map(|slot| reqs[*slot].clone()).collect();
            async move {
                let sub = json!({"docs": subdocs});
                let result = if node == st.cluster.me {
                    db.bulk_get(&sub, attachments, revs, latest).await
                } else {
                    let peer = st.cluster.nodes[node].clone();
                    let mut query = format!("revs={revs}");
                    if attachments {
                        query.push_str("&attachments=true");
                    }
                    if latest {
                        query.push_str("&latest=true");
                    }
                    forward_json(st, &peer, dbname, "_bulk_get", &query, &sub, headers).await
                };
                (slots, result)
            }
        });
        for (slots, result) in futures::future::join_all(requests).await {
            match result {
                Ok(value) => {
                    let rows = value["results"].as_array().cloned().unwrap_or_default();
                    for (slot, row) in slots.into_iter().zip(rows) {
                        out[slot] = row;
                    }
                }
                Err(error) => {
                    for slot in slots {
                        if candidates[slot].len() > 1 {
                            candidates[slot].remove(0);
                            pending.push(slot);
                        } else {
                            return fail(error);
                        }
                    }
                }
            }
        }
    }
    json(StatusCode::OK, json!({"results": out}))
}

pub(super) async fn revs_diff(
    st: &AppState,
    db: &Db,
    dbname: &str,
    body: &[u8],
    headers: &HeaderMap,
) -> Response {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return fail(ApiError::bad_request("invalid JSON"));
    };
    if !st.cluster.is_clustered() || is_forwarded(st, headers) {
        return match db.revs_diff(&value).await {
            Ok(v) => json(StatusCode::OK, v),
            Err(e) => fail(e),
        };
    }
    let Some(map) = value.as_object() else {
        return fail(ApiError::bad_request("Request body must be a JSON object"));
    };
    let entries: Vec<(String, Value)> = map
        .iter()
        .map(|(id, revs)| (id.clone(), revs.clone()))
        .collect();
    let mut candidates: Vec<Vec<usize>> = entries
        .iter()
        .map(|(id, _)| bulk_read_candidates(db, id, st.cluster.me))
        .collect();
    let mut out = Map::new();
    let mut pending: Vec<usize> = (0..entries.len()).collect();
    while !pending.is_empty() {
        let mut groups: HashMap<usize, Vec<usize>> = HashMap::new();
        for slot in pending.drain(..) {
            groups.entry(candidates[slot][0]).or_default().push(slot);
        }
        let requests = groups.into_iter().map(|(node, slots)| {
            let sub = Value::Object(
                slots
                    .iter()
                    .map(|slot| (entries[*slot].0.clone(), entries[*slot].1.clone()))
                    .collect(),
            );
            async move {
                let result = if node == st.cluster.me {
                    db.revs_diff(&sub).await
                } else {
                    let peer = st.cluster.nodes[node].clone();
                    forward_json(st, &peer, dbname, "_revs_diff", "", &sub, headers).await
                };
                (slots, result)
            }
        });
        for (slots, result) in futures::future::join_all(requests).await {
            match result {
                Ok(value) => {
                    if let Some(obj) = value.as_object() {
                        for (id, entry) in obj {
                            out.insert(id.clone(), entry.clone());
                        }
                    }
                }
                Err(error) => {
                    for slot in slots {
                        if candidates[slot].len() > 1 {
                            candidates[slot].remove(0);
                            pending.push(slot);
                        } else {
                            return fail(error);
                        }
                    }
                }
            }
        }
    }
    json(StatusCode::OK, Value::Object(out))
}

pub(super) async fn all_docs(req: DbRequest<'_>, partition: Option<String>) -> Response {
    let DbRequest {
        st,
        db,
        name: dbname,
        method,
        q,
        headers,
        body,
        ..
    } = req;
    let body: &[u8] = body;
    let partition = partition.or_else(|| q.get("partition").cloned());
    if let Some(partition) = &partition {
        if !db.partitioned {
            return fail(ApiError::bad_request(
                "`partition` parameter is not supported on this db",
            ));
        }
        if let Err(error) = crate::db::validate_partition(partition) {
            return fail(error);
        }
    }
    let query_limit = q.contains_key("limit");
    let query_skip = q.contains_key("skip");
    let mut opts = AllDocsOpts {
        partition: partition.clone(),
        include_docs: flag(q, "include_docs"),
        conflicts: flag(q, "conflicts"),
        attachments: flag(q, "attachments"),
        startkey: key_param(q, "startkey").or_else(|| key_param(q, "start_key")),
        endkey: key_param(q, "endkey").or_else(|| key_param(q, "end_key")),
        descending: flag(q, "descending"),
        inclusive_end: q
            .get("inclusive_end")
            .map(|value| value != "false")
            .unwrap_or(true),
        limit: num(q, "limit").unwrap_or(usize::MAX),
        skip: num(q, "skip").unwrap_or(0),
        ..Default::default()
    };
    if let Some(key) = key_param(q, "key") {
        if q.contains_key("keys") {
            return fail(ApiError::bad_request("`key` is incompatible with `keys`"));
        }
        // CouchDB treats `key` as a range query over live rows, not as a
        // `keys` lookup: a deleted document must not appear.
        opts.startkey = opts.startkey.or(Some(key.clone()));
        opts.endkey = opts.endkey.or(Some(key));
    } else if let Some(raw) = q.get("keys") {
        if let Ok(Value::Array(keys)) = serde_json::from_str::<Value>(raw) {
            opts.keys = Some(
                keys.into_iter()
                    .filter_map(|key| key.as_str().map(str::to_string))
                    .collect(),
            );
        }
    }
    if *method == Method::POST {
        if let Ok(value) = serde_json::from_slice::<Value>(body) {
            if opts.keys.is_none() {
                if let Some(keys_value) = value.get("keys") {
                    let Some(keys) = keys_value.as_array() else {
                        return fail(ApiError::bad_request("`keys` must be an array."));
                    };
                    opts.keys = Some(
                        keys.iter()
                            .filter_map(|k| k.as_str().map(str::to_string))
                            .collect(),
                    );
                }
            }
            if value
                .get("include_docs")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                opts.include_docs = true;
            }
            if value
                .get("descending")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                opts.descending = true;
            }
            if !query_limit {
                if let Some(limit) = value.get("limit").and_then(Value::as_u64) {
                    opts.limit = limit as usize;
                }
            }
            if !query_skip {
                if let Some(skip) = value.get("skip").and_then(Value::as_u64) {
                    opts.skip = skip as usize;
                }
            }
        }
    }
    if opts.keys.is_some() && (opts.startkey.is_some() || opts.endkey.is_some()) {
        return fail(ApiError::bad_request(
            "`keys` is incompatible with `startkey` and `endkey`",
        ));
    }
    let shard_mask = if is_forwarded(st, headers) {
        q.get("shards").and_then(|raw| parse_shard_mask(raw, db.q))
    } else {
        None
    };
    let result = if st.cluster.is_clustered() && !is_forwarded(st, headers) {
        cluster_all_docs(
            ClusterRead {
                st,
                db,
                dbname,
                headers,
            },
            &opts,
        )
        .await
    } else if let Some(mask) = shard_mask {
        db.all_docs_owned(&opts, &mask).await
    } else if flag(q, "owner_only") && is_forwarded(st, headers) {
        let owned: Vec<bool> = (0..db.q)
            .map(|shard| owner_of_shard(db, shard) == st.cluster.me)
            .collect();
        db.all_docs_owned(&opts, &owned).await
    } else {
        db.all_docs(&opts).await
    };
    if !still_allowed(st, db, headers) {
        return access_denied(&st.principal(headers));
    }
    match result {
        Ok(mut value) => {
            if flag(q, "update_seq") {
                value["update_seq"] = json!(db.update_seq());
            }
            json(StatusCode::OK, value)
        }
        Err(e) => fail(e),
    }
}

//! Multi-document endpoints: `_bulk_docs`, `_bulk_get`, `_revs_diff` and
//! `_all_docs`.

use super::*;
use crate::db::{BulkGetDoc, BulkGetError, BulkGetResult, BulkGetResults, DocList};

/// The parsed `_bulk_docs` body.
struct BulkBody {
    new_edits: bool,
    all_or_nothing: bool,
    docs: Vec<Value>,
}

/// Parse and check the shape of a `_bulk_docs` body.
fn parse_bulk_body(body: &[u8]) -> DbResult<BulkBody> {
    let Ok(mut value) = crate::json::from_slice(body) else {
        return Err(ApiError::bad_request("invalid UTF-8 JSON"));
    };
    let new_edits = match value.get("new_edits") {
        Some(Value::Bool(new_edits)) => *new_edits,
        Some(_) => {
            return Err(ApiError::bad_request(
                "`new_edits` parameter must be a boolean.",
            ))
        }
        None => true,
    };
    let Some(docs) = crate::json::take_array(&mut value, "docs") else {
        return Err(ApiError::bad_request("Missing JSON list of 'docs'"));
    };
    if !new_edits
        && docs
            .iter()
            .any(|doc| doc.get("_rev").is_none() && doc.get("_revisions").is_none())
    {
        return Err(ApiError::bad_request(
            "When `new_edits: false`, the document needs `_rev` or `_revisions` specified",
        ));
    }
    let all_or_nothing = value
        .get("all_or_nothing")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok(BulkBody {
        new_edits,
        all_or_nothing,
        docs,
    })
}

/// Reject malformed documents and ids that only reserved documents may use.
fn check_bulk_docs(db: &Db, docs: &[Value]) -> DbResult<()> {
    for doc in docs {
        validate_doc(doc)?;
        if !db.partitioned {
            match doc.get("_id") {
                Some(Value::String(id))
                    if id.starts_with('_')
                        && !id.starts_with("_design/")
                        && !id.starts_with("_local/") =>
                {
                    return Err(ApiError::new(
                        400,
                        "illegal_docid",
                        "Only reserved document ids may start with underscore.",
                    ))
                }
                Some(Value::String(_)) | None => {}
                Some(_) => {
                    return Err(ApiError::new(
                        400,
                        "illegal_docid",
                        "Document id must be a string",
                    ))
                }
            }
        }
    }
    Ok(())
}

/// Run the validate_doc_update functions over the batch, one verdict per
/// document. A forwarded batch was validated by its coordinator.
async fn validate_bulk_batch(
    st: &AppState,
    db: &Arc<Db>,
    headers: &HeaderMap,
    docs: &mut [Value],
) -> DbResult<Vec<Option<ApiError>>> {
    if st.ctx.cluster.is_internal(headers) {
        return Ok(std::iter::repeat_with(|| None).take(docs.len()).collect());
    }
    // The same validators as a single-document write.
    let principal = st.principal(headers);
    let functions = db.validation_functions().await?;
    crate::validate::validate_docs_with(&functions, db, docs, &principal).await
}

/// Write the documents that passed validation: on this node for a standalone
/// server or a forwarded batch, otherwise through the cluster coordinator.
/// The flag is true when the write reached fewer replicas than requested.
async fn write_bulk_batch(
    req: DbRequest<'_>,
    writable: Vec<Value>,
    new_edits: bool,
) -> DbResult<(Vec<Option<WriteResult>>, bool)> {
    let DbRequest {
        st,
        db,
        name: dbname,
        q,
        headers,
        ..
    } = req;
    if writable.is_empty() {
        return Ok((Vec::new(), false));
    }
    if !st.ctx.cluster.is_clustered() || st.ctx.cluster.is_internal(headers) {
        // A forwarded batch has already been placed by its coordinator; write
        // it to this node's shards and let the coordinator count the ack.
        let results =
            put_on_this_node(db, writable, new_edits, st.ctx.cluster.is_internal(headers)).await?;
        return Ok((results, false));
    }
    let w = num(q, "w").unwrap_or(0);
    let (results, quorum) = commit_docs(st, db, dbname, writable, new_edits, w, headers).await?;
    Ok((results, !quorum))
}

pub(super) async fn bulk_docs(req: DbRequest<'_>) -> Response {
    let DbRequest {
        st,
        db,
        body,
        headers,
        ..
    } = req;
    let BulkBody {
        new_edits,
        all_or_nothing,
        mut docs,
    } = match parse_bulk_body(body) {
        Ok(parsed) => parsed,
        Err(error) => return fail(error),
    };
    if let Some(response) = require_design_admin(st, db, headers, &docs) {
        return response;
    }
    if let Err(error) = check_bulk_docs(db, &docs) {
        return fail(error);
    }
    if all_or_nothing {
        let results: Vec<WriteResult> = docs
            .iter()
            .map(|_| {
                WriteResult::Error(WriteError {
                    id: None,
                    error: "not_implemented".to_string(),
                    reason: "all_or_nothing is not supported".to_string(),
                })
            })
            .collect();
        return json_of(StatusCode::EXPECTATION_FAILED, &results);
    }

    let validation = match validate_bulk_batch(st, db, headers, &mut docs).await {
        Ok(validation) => validation,
        Err(error) => return fail(error),
    };

    let mut results: Vec<Option<WriteResult>> = (0..docs.len()).map(|_| None).collect();
    let mut slots: Vec<usize> = Vec::new();
    let mut writable: Vec<Value> = Vec::new();
    for (slot, doc) in docs.into_iter().enumerate() {
        if let Some(error) = &validation[slot] {
            results[slot] = Some(WriteResult::Error(WriteError {
                id: Some(doc.get("_id").cloned().unwrap_or(Value::Null)),
                error: error.error.to_string(),
                reason: error.reason.clone(),
            }));
        } else {
            slots.push(slot);
            writable.push(doc);
        }
    }

    let (written, accepted) = match write_bulk_batch(req, writable, new_edits).await {
        Ok(written) => written,
        Err(error) => return fail(error),
    };
    for (slot, result) in slots.into_iter().zip(written) {
        results[slot] = result;
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
    doc: &mut Value,
    headers: &HeaderMap,
) -> Option<Response> {
    let principal = st.principal(headers);
    let functions = match db.validation_functions().await {
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

/// Write a batch to this node's shards. Replicated edits from a peer
/// (`internal` with `new_edits=false`) install `_local` documents exactly as
/// sent: that is how a rebalance hands a shard's local documents to a new
/// replica without numbering the copy one higher. Every other `_local` write
/// is an ordinary one.
async fn put_on_this_node(
    db: &Db,
    docs: Vec<Value>,
    new_edits: bool,
    internal: bool,
) -> DbResult<Vec<Option<WriteResult>>> {
    if new_edits || !internal {
        return Ok(db
            .put_docs(docs, new_edits)
            .await?
            .into_iter()
            .map(Some)
            .collect());
    }
    let is_local = |doc: &Value| {
        doc.get("_id")
            .and_then(Value::as_str)
            .is_some_and(|id| id.starts_with("_local/"))
    };
    let mut results: Vec<Option<WriteResult>> = (0..docs.len()).map(|_| None).collect();
    let mut local_slots = Vec::new();
    let mut locals = Vec::new();
    let mut other_slots = Vec::new();
    let mut others = Vec::new();
    for (slot, doc) in docs.into_iter().enumerate() {
        if is_local(&doc) {
            local_slots.push(slot);
            locals.push(doc);
        } else {
            other_slots.push(slot);
            others.push(doc);
        }
    }
    let installed = if locals.is_empty() {
        Vec::new()
    } else {
        db.install_local_docs(locals).await
    };
    let written = if others.is_empty() {
        Vec::new()
    } else {
        db.put_docs(others, false).await?
    };
    for (slot, result) in local_slots
        .into_iter()
        .zip(installed)
        .chain(other_slots.into_iter().zip(written))
    {
        results[slot] = Some(result);
    }
    Ok(results)
}

/// `None` is a document no replica reported on: JSON `null`, and for a
/// replicated write nothing to report at all.
fn bulk_response(new_edits: bool, results: Vec<Option<WriteResult>>) -> Response {
    if new_edits {
        json_of(StatusCode::CREATED, &results)
    } else {
        let errors: Vec<WriteResult> = results
            .into_iter()
            .flatten()
            .filter(|result| !result.is_ok())
            .collect();
        json_of(StatusCode::CREATED, &errors)
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

pub(super) async fn bulk_get(req: DbRequest<'_>) -> Response {
    let DbRequest {
        st,
        db,
        name: dbname,
        q,
        body,
        headers,
        ..
    } = req;
    let mut value = match json_body(body, "invalid JSON") {
        Ok(value) => value,
        Err(error) => return fail(error),
    };
    let attachments = flag(q, "attachments");
    let revs = flag(q, "revs") || value.get("revs").and_then(Value::as_bool).unwrap_or(false);
    let latest = flag(q, "latest");
    if !st.ctx.cluster.is_clustered() || st.ctx.cluster.is_internal(headers) {
        return match db.bulk_get(&value, attachments, revs, latest).await {
            Ok(results) => json_of(StatusCode::OK, &results),
            Err(e) => fail(e),
        };
    }
    let Some(reqs) = crate::json::take_array(&mut value, "docs") else {
        return fail(ApiError::bad_request("Missing JSON list of 'docs'"));
    };
    let mut candidates: Vec<Vec<usize>> = reqs
        .iter()
        .map(|req| {
            let id = req.get("id").and_then(Value::as_str).unwrap_or_default();
            bulk_read_candidates(db, id, st.ctx.cluster.me)
        })
        .collect();
    let mut out: Vec<Option<BulkGetResult>> = (0..reqs.len()).map(|_| None).collect();
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
                let result = if node == st.ctx.cluster.me {
                    db.bulk_get(&sub, attachments, revs, latest).await
                } else {
                    let peer = st.ctx.cluster.nodes[node].clone();
                    let mut query = format!("revs={revs}");
                    if attachments {
                        query.push_str("&attachments=true");
                    }
                    if latest {
                        query.push_str("&latest=true");
                    }
                    forward_json(&st.ctx, &peer, dbname, "_bulk_get", &query, &sub, headers)
                        .await
                        .and_then(|value| {
                            serde_json::from_value::<BulkGetResults>(value)
                                .map_err(|e| ApiError::new(502, "bad_gateway", e.to_string()))
                        })
                };
                (slots, result)
            }
        });
        for (slots, result) in
            crate::cluster_http::in_one_round(futures::future::join_all(requests)).await
        {
            match result {
                Ok(value) => {
                    for (slot, row) in slots.into_iter().zip(value.results) {
                        out[slot] = row;
                    }
                }
                Err(error) => {
                    for slot in slots {
                        if candidates[slot].len() > 1 {
                            candidates[slot].remove(0);
                            pending.push(slot);
                        } else if peer_unreachable(&error) {
                            // chttpd_db:bulk_get_docs_batched turns a failed
                            // fabric:open_revs into a per-doc error.
                            let id = reqs[slot].get("id").and_then(Value::as_str).unwrap_or("");
                            let rev = reqs[slot]
                                .get("rev")
                                .and_then(Value::as_str)
                                .unwrap_or("undefined");
                            out[slot] = Some(BulkGetResult {
                                id: id.to_string(),
                                docs: vec![BulkGetDoc::Error(BulkGetError {
                                    id: id.to_string(),
                                    rev: rev.to_string(),
                                    error: "internal_fabric_error".to_string(),
                                    reason: "all_workers_died".to_string(),
                                })],
                            });
                        } else {
                            return fail(error);
                        }
                    }
                }
            }
        }
    }
    json_of(StatusCode::OK, &BulkGetResults { results: out })
}

pub(super) async fn revs_diff(req: DbRequest<'_>) -> Response {
    let DbRequest {
        st,
        db,
        name: dbname,
        body,
        headers,
        ..
    } = req;
    let value = match json_body(body, "invalid JSON") {
        Ok(value) => value,
        Err(error) => return fail(error),
    };
    if !st.ctx.cluster.is_clustered() || st.ctx.cluster.is_internal(headers) {
        return match db.revs_diff(&value).await {
            Ok(diff) => json_of(StatusCode::OK, &diff),
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
        .map(|(id, _)| bulk_read_candidates(db, id, st.ctx.cluster.me))
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
                let result = if node == st.ctx.cluster.me {
                    match db.revs_diff(&sub).await {
                        Ok(diff) => serde_json::to_value(&diff).map_err(ApiError::internal),
                        Err(error) => Err(error),
                    }
                } else {
                    let peer = st.ctx.cluster.nodes[node].clone();
                    forward_json(&st.ctx, &peer, dbname, "_revs_diff", "", &sub, headers).await
                };
                (slots, result)
            }
        });
        for (slots, result) in
            crate::cluster_http::in_one_round(futures::future::join_all(requests)).await
        {
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
                        } else if peer_unreachable(&error) {
                            // fabric_doc_missing_revs ignores workers that
                            // went down: what nobody answered for is missing.
                            let (id, revs) = &entries[slot];
                            let missing = if revs.is_array() {
                                revs.clone()
                            } else {
                                json!([])
                            };
                            out.insert(id.clone(), json!({ "missing": missing }));
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

const ALL_DOCS_BOOL_PARAMS: &[&str] = &[
    "descending",
    "include_docs",
    "inclusive_end",
    "update_seq",
    "conflicts",
    "attachments",
    "reduce",
];

/// The options of an `_all_docs`-shaped query, from the query string and (for
/// POST) the body. `_design_docs` and `_local_docs` take the same ones.
fn all_docs_opts(
    db: &Db,
    q: &HashMap<String, String>,
    method: &Method,
    body: &[u8],
    partition: Option<String>,
) -> DbResult<AllDocsOpts> {
    let partition = partition.or_else(|| q.get("partition").cloned());
    if let Some(partition) = &partition {
        if !db.partitioned {
            return Err(ApiError::bad_request(
                "`partition` parameter is not supported on this db",
            ));
        }
        crate::db::validate_partition(partition)?;
    }
    validate_bool_params(q, ALL_DOCS_BOOL_PARAMS)?;
    let query_limit = uint_param(q, "limit")?;
    let query_skip = uint_param(q, "skip")?;
    let key_range = |name: &str, alias: &str| -> DbResult<Option<String>> {
        match json_key_param(q, name)? {
            Some(key) => Ok(Some(key)),
            None => json_key_param(q, alias),
        }
    };
    let startkey = key_range("startkey", "start_key")?;
    let endkey = key_range("endkey", "end_key")?;
    let single_key = json_key_param(q, "key")?;
    let mut opts = AllDocsOpts {
        partition,
        include_docs: flag(q, "include_docs"),
        conflicts: flag(q, "conflicts"),
        attachments: flag(q, "attachments"),
        startkey,
        endkey,
        descending: flag(q, "descending"),
        inclusive_end: !q
            .get("inclusive_end")
            .is_some_and(|value| value.eq_ignore_ascii_case("false")),
        limit: query_limit.unwrap_or(usize::MAX),
        skip: query_skip.unwrap_or(0),
        ..Default::default()
    };
    if let Some(key) = single_key {
        if q.contains_key("keys") {
            return Err(ApiError::bad_request("`key` is incompatible with `keys`"));
        }
        // CouchDB treats `key` as a range query over live rows, not as a
        // `keys` lookup: a deleted document must not appear.
        opts.startkey = opts.startkey.or(Some(key.clone()));
        opts.endkey = opts.endkey.or(Some(key));
    } else if let Some(keys) = json_param(q, "keys")? {
        match keys {
            Value::Array(keys) => opts.keys = Some(keys),
            _ => return Err(ApiError::bad_request("`keys` parameter must be an array.")),
        }
    }
    if *method == Method::POST {
        let value = body_object(body)?;
        if opts.keys.is_none() {
            if let Some(keys_value) = value.get("keys") {
                let Some(keys) = keys_value.as_array() else {
                    return Err(ApiError::bad_request(
                        "`keys` body member must be an array.",
                    ));
                };
                opts.keys = Some(keys.clone());
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
        let body_limit = uint_member(&value, "limit")?;
        let body_skip = uint_member(&value, "skip")?;
        if query_limit.is_none() {
            if let Some(limit) = body_limit {
                opts.limit = limit;
            }
        }
        if query_skip.is_none() {
            if let Some(skip) = body_skip {
                opts.skip = skip;
            }
        }
    }
    if opts.keys.is_some() && (opts.startkey.is_some() || opts.endkey.is_some()) {
        return Err(ApiError::bad_request(
            "`keys` is incompatible with `startkey` and `endkey`",
        ));
    }
    Ok(opts)
}

/// Run an `_all_docs` query on this node's shards or, for a cluster
/// coordinator, on the whole cluster.
async fn run_all_docs(req: &DbRequest<'_>, opts: &AllDocsOpts) -> DbResult<DocList> {
    let (st, db, headers) = (req.st, req.db, req.headers);
    let shard_mask = if st.ctx.cluster.is_internal(headers) {
        req.q
            .get("shards")
            .and_then(|raw| parse_shard_mask(raw, db.q))
    } else {
        None
    };
    if st.ctx.cluster.is_clustered() && !st.ctx.cluster.is_internal(headers) {
        cluster_all_docs(
            ClusterRead {
                ctx: &st.ctx,
                db,
                dbname: req.name,
                headers,
            },
            opts,
        )
        .await
    } else if let Some(mask) = shard_mask {
        db.all_docs_owned(opts, &mask).await
    } else if flag(req.q, "owner_only") && st.ctx.cluster.is_internal(headers) {
        let owned: ShardMask = (0..db.q)
            .map(|shard| db.owner_of_shard(shard) == st.ctx.cluster.me)
            .collect();
        db.all_docs_owned(opts, &owned).await
    } else {
        db.all_docs(opts).await
    }
}

/// The `update_seq` to report, when the request asks for one. It must be
/// taken before the rows are read: a client that follows `_changes` from it
/// then sees every write the rows missed, while a token taken after could
/// include a write the rows do not show, which the client would never see.
async fn requested_update_seq(req: &DbRequest<'_>) -> DbResult<Option<Value>> {
    if flag(req.q, "update_seq") {
        query_update_seq(req.st, req.db, req.name, req.headers)
            .await
            .map(Some)
    } else {
        Ok(None)
    }
}

/// The reply once the rows are known: re-check access (a security change may
/// have landed while the query ran) and add the `update_seq` taken before the
/// rows were read.
fn all_docs_reply(
    req: &DbRequest<'_>,
    result: DbResult<DocList>,
    update_seq: Option<Value>,
) -> Response {
    if !still_allowed(req.st, req.db, req.headers) {
        return access_denied(&req.st.principal(req.headers));
    }
    match result {
        Ok(mut list) => {
            if let Some(update_seq) = update_seq {
                list.update_seq = Some(update_seq);
            }
            json_of(StatusCode::OK, &list)
        }
        Err(e) => fail(e),
    }
}

pub(super) async fn all_docs(req: DbRequest<'_>, partition: Option<String>) -> Response {
    if let Some(response) = require_method(req.method, &["GET", "HEAD", "POST"]) {
        return response;
    }
    let opts = match all_docs_opts(req.db, req.q, req.method, req.body, partition) {
        Ok(opts) => opts,
        Err(error) => return fail(error),
    };
    let update_seq = match requested_update_seq(&req).await {
        Ok(update_seq) => update_seq,
        Err(error) => return fail(error),
    };
    if opts.keys.is_none()
        && !req.st.ctx.cluster.is_clustered()
        && !req.st.ctx.cluster.is_internal(req.headers)
    {
        return stream_all_docs(&req, &opts, update_seq).await;
    }
    let result = run_all_docs(&req, &opts).await;
    all_docs_reply(&req, result, update_seq)
}

/// Rows are encoded into chunks of about this size as they are read.
const STREAM_CHUNK: usize = 64 * 1024;

/// `_all_docs` without `keys` on this node: the body is written while the
/// rows are read, so a listing of any size takes constant memory and the
/// first bytes leave before the last row is read. The bytes are those of the
/// buffered reply. An error on the first page still gets its status; one
/// after that can only end the connection early. A HEAD reads no rows.
async fn stream_all_docs(
    req: &DbRequest<'_>,
    opts: &AllDocsOpts,
    update_seq: Option<Value>,
) -> Response {
    let (head, rows) = match Db::all_docs_stream(req.db.clone(), opts).await {
        Ok(stream) => stream,
        Err(error) => return fail(error),
    };
    if !still_allowed(req.st, req.db, req.headers) {
        return access_denied(&req.st.principal(req.headers));
    }
    let body = if *req.method == Method::HEAD {
        Body::from_stream(futures::stream::empty::<Result<Bytes, std::io::Error>>())
    } else {
        // Reading the first row reads the first page of every shard, so an
        // error there still gets a status.
        let mut rows = Box::pin(rows);
        let first_row = rows.next().await;
        if let Some(Err(error)) = first_row {
            return fail(error);
        }
        let rows = futures::stream::iter(first_row).chain(rows);
        let stream = async_stream::stream! {
            let mut rows = Box::pin(rows);
            let mut buffer = format!(
                "{{\"total_rows\":{},\"offset\":{},\"rows\":[",
                head.total_rows, head.offset
            )
            .into_bytes();
            let mut first = true;
            while let Some(item) = rows.next().await {
                let encoded = item.and_then(|row| {
                    serde_json::to_vec(&row).map_err(ApiError::internal)
                });
                match encoded {
                    Ok(encoded) => {
                        if !first {
                            buffer.push(b',');
                        }
                        first = false;
                        buffer.extend_from_slice(&encoded);
                    }
                    Err(error) => {
                        yield Err(std::io::Error::other(format!(
                            "{}: {}",
                            error.error, error.reason
                        )));
                        return;
                    }
                }
                if buffer.len() >= STREAM_CHUNK {
                    yield Ok::<Bytes, std::io::Error>(Bytes::from(std::mem::take(&mut buffer)));
                }
            }
            buffer.push(b']');
            if let Some(update_seq) = update_seq {
                buffer.extend_from_slice(format!(",\"update_seq\":{update_seq}").as_bytes());
            }
            buffer.push(b'}');
            yield Ok(Bytes::from(buffer));
        };
        Body::from_stream(stream)
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// First and one-past-last id of the design documents.
const DESIGN_FIRST: &str = "_design/";
const DESIGN_END: &str = "_design0";

/// `GET|POST /{db}/_design_docs`: `_all_docs` over the design documents.
/// Like CouchDB, `total_rows` counts only design documents while `offset`
/// still counts every document that sorts before the range.
pub(super) async fn design_docs(req: DbRequest<'_>) -> Response {
    if let Some(response) = require_method(req.method, &["GET", "HEAD", "POST"]) {
        return response;
    }
    let mut opts = match all_docs_opts(req.db, req.q, req.method, req.body, None) {
        Ok(opts) => opts,
        Err(error) => return fail(error),
    };
    match &mut opts.keys {
        // A key outside the range is not looked up, not even to report it
        // missing.
        Some(keys) => keys.retain(|key| {
            key.as_str()
                .is_some_and(|key| key.starts_with(DESIGN_FIRST))
        }),
        None => {
            let (low, high) = if opts.descending {
                (&mut opts.endkey, &mut opts.startkey)
            } else {
                (&mut opts.startkey, &mut opts.endkey)
            };
            if low.as_deref().is_none_or(|key| key < DESIGN_FIRST) {
                *low = Some(DESIGN_FIRST.to_string());
            }
            if high.as_deref().is_none_or(|key| key > DESIGN_END) {
                *high = Some(DESIGN_END.to_string());
            }
        }
    }
    let update_seq = match requested_update_seq(&req).await {
        Ok(update_seq) => update_seq,
        Err(error) => return fail(error),
    };
    let mut result = run_all_docs(&req, &opts).await;
    if let Ok(list) = &mut result {
        let everything = AllDocsOpts {
            startkey: Some(DESIGN_FIRST.to_string()),
            endkey: Some(DESIGN_END.to_string()),
            ..Default::default()
        };
        match run_all_docs(&req, &everything).await {
            Ok(all) => {
                let total = all.rows.len() as u64;
                list.total_rows = Some(total);
                list.offset = list.offset.map(|offset| offset.min(total));
            }
            Err(error) => result = Err(error),
        }
    }
    all_docs_reply(&req, result, update_seq)
}

/// `GET|POST /{db}/_local_docs`: the `_local/` documents. Like CouchDB,
/// `total_rows` and `offset` are null. In a cluster the coordinator lists one
/// copy of every shard; a request a peer already routed answers for the
/// shards it names, or all of its own.
pub(super) async fn local_docs(req: DbRequest<'_>) -> Response {
    if let Some(response) = require_method(req.method, &["GET", "HEAD", "POST"]) {
        return response;
    }
    let opts = match all_docs_opts(req.db, req.q, req.method, req.body, None) {
        Ok(opts) => opts,
        Err(error) => return fail(error),
    };
    let (st, db, headers) = (req.st, req.db, req.headers);
    let result = if st.ctx.cluster.is_clustered() && !st.ctx.cluster.is_internal(headers) {
        cluster_local_docs(
            ClusterRead {
                ctx: &st.ctx,
                db,
                dbname: req.name,
                headers,
            },
            &opts,
        )
        .await
    } else if let Some(mask) = st
        .ctx
        .cluster
        .is_internal(headers)
        .then(|| {
            req.q
                .get("shards")
                .and_then(|raw| parse_shard_mask(raw, db.q))
        })
        .flatten()
    {
        db.local_docs_owned(&opts, &mask).await
    } else {
        db.local_docs(&opts).await
    };
    all_docs_reply(
        &req,
        result,
        flag(req.q, "update_seq").then_some(Value::Null),
    )
}

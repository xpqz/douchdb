//! Database-level endpoints. `db_rest` dispatches every `/{db}/{*rest}` path
//! to the handlers in the sibling modules.

use super::*;

pub async fn db_root(
    State(st): State<Arc<AppState>>,
    DbName(name): DbName,
    Caller(principal): Caller,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // CouchDB validates a name only when creating it; for every other method an
    // illegal name is simply a database that does not exist.
    if method == Method::PUT && !valid_db_name(&name) {
        return fail(illegal_db_name(&name));
    }
    let q = query(&uri);
    let local_only = st.ctx.cluster.is_internal_db(&headers);
    if matches!(method, Method::PUT | Method::DELETE) && !principal.admin {
        return access_denied(&principal);
    }
    match method {
        Method::PUT => create_db(&st, &name, &q, &headers, local_only).await,
        Method::DELETE => delete_db(&st, &name, &headers, local_only).await,
        Method::GET | Method::HEAD | Method::POST => {
            let db = match st.db(&name).await {
                Ok(db) => db,
                Err(e) => return fail(e),
            };
            let req = DbRequest {
                st: &st,
                db: &db,
                name: &name,
                method: &method,
                uri: &uri,
                q: &q,
                headers: &headers,
                principal: &principal,
                body: &body,
            };
            if method == Method::POST {
                create_doc(req, &principal).await
            } else {
                db_info(req, &principal).await
            }
        }
        _ => fail(ApiError::new(
            405,
            "method_not_allowed",
            "method not allowed",
        )),
    }
}

/// `PUT /{db}`.
async fn create_db(
    st: &AppState,
    name: &str,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
    local_only: bool,
) -> Response {
    let partitioned = flag(q, "partitioned");
    let positive = |param: &str| -> Result<Option<usize>, ApiError> {
        match q.get(param) {
            None => Ok(None),
            Some(raw) => raw
                .parse::<usize>()
                .ok()
                .filter(|value| *value > 0)
                .map(Some)
                .ok_or_else(|| {
                    ApiError::bad_request(format!("`{param}` must be a positive integer"))
                }),
        }
    };
    let (shards, copies) = match (positive("q"), positive("n")) {
        (Ok(shards), Ok(copies)) => (shards.map(|q| q as u32), copies),
        (Err(e), _) | (_, Err(e)) => return fail(e),
    };
    // A database that already exists here is not the end of a clustered
    // create: the peers may have missed it, so they still get the call
    // (CouchDB heals that by replicating the shard map) and the client
    // is told 412 as before.
    let existing = match st.create_db(name, partitioned, shards, copies).await {
        Ok(_) => None,
        Err(e) if e.status == 412 && st.ctx.cluster.is_clustered() && !local_only => Some(e),
        Err(e) => return fail(e),
    };
    let mut status = StatusCode::CREATED;
    if st.ctx.cluster.is_clustered() && !local_only {
        // Peers build the same shard map from the same layout.
        let mut layout = vec![("partitioned", partitioned.to_string())];
        if let Some(shards) = shards {
            layout.push(("q", shards.to_string()));
        }
        if let Some(copies) = copies {
            layout.push(("n", copies.to_string()));
        }
        let answers = fan_out_db(st, headers, Method::PUT, name, &layout).await;
        if let Some(e) = existing {
            return fail(e);
        }
        // As fabric_db_create: 201 once a majority of the nodes hold
        // the database (a peer that already had it counts), otherwise
        // 202. The local node is one of them.
        let held = 1 + answers
            .iter()
            .flatten()
            .filter(|status| status.is_success() || **status == StatusCode::PRECONDITION_FAILED)
            .count();
        if held < st.ctx.cluster.nodes.len() / 2 + 1 {
            status = StatusCode::ACCEPTED;
        }
    }
    let mut response = json(status, json!({"ok": true}));
    if let Ok(value) = format!("/{name}").parse() {
        response.headers_mut().insert(header::LOCATION, value);
    }
    response
}

/// `DELETE /{db}`.
async fn delete_db(st: &AppState, name: &str, headers: &HeaderMap, local_only: bool) -> Response {
    let local = st.drop_db(name).await;
    if !st.ctx.cluster.is_clustered() || local_only {
        return match local {
            Ok(()) => ok(),
            Err(e) => fail(e),
        };
    }
    let answers = fan_out_db(st, headers, Method::DELETE, name, &[]).await;
    // As fabric_db_delete: 200 when every node answered (deleted or
    // not found) and at least one deleted it, 404 when every node
    // answered not found, 202 when some deleted it but a node did not
    // answer, 500 when none did.
    let mut deleted = usize::from(local.is_ok());
    let mut missing = usize::from(matches!(&local, Err(e) if e.status == 404));
    for status in answers.iter().flatten() {
        if status.is_success() {
            deleted += 1;
        } else if *status == StatusCode::NOT_FOUND {
            missing += 1;
        }
    }
    if deleted + missing == st.ctx.cluster.nodes.len() {
        if deleted == 0 {
            return fail(local.err().unwrap_or_else(ApiError::db_not_found));
        }
        ok()
    } else if deleted > 0 {
        json(StatusCode::ACCEPTED, json!({"ok": true}))
    } else {
        fail(ApiError::new(
            500,
            "internal_server_error",
            "no node confirmed the delete",
        ))
    }
}

/// `GET /{db}`.
async fn db_info(req: DbRequest<'_>, principal: &Principal) -> Response {
    let DbRequest {
        st,
        db,
        name,
        headers,
        ..
    } = req;
    if let Some(response) = require_db_access(st, principal, db, false) {
        return response;
    }
    if st.ctx.cluster.is_internal(headers)
        && headers
            .get("x-douchdb-update-seq")
            .and_then(|value| value.to_str().ok())
            == Some("1")
    {
        // Every local copy's uuid, committed position, purge sequence and
        // sizes, for a coordinator building the cluster's tokens and sums.
        return match crate::fabric::local_copies(db).await {
            Ok(shards) => json(StatusCode::OK, json!({ "shards": shards })),
            Err(e) => fail(e),
        };
    }
    // Read first: a compaction that leaves the queue is claimed already.
    let queued = st.compaction_queued(name);
    match db.info().await {
        Ok(mut info) => {
            info.compact_running |= queued;
            if st.ctx.cluster.is_internal(headers) {
                return json_of(StatusCode::OK, &info);
            }
            // The sequences are opaque tokens, as in CouchDB 3: one position
            // per shard range, on a named copy, the same form the changes
            // feed returns, so they work as `since`.
            let (seq, purge_seq) = if st.ctx.cluster.is_clustered() {
                match cluster_seq_tokens(ClusterRead {
                    ctx: &st.ctx,
                    db,
                    dbname: name,
                    headers,
                })
                .await
                {
                    Ok(tokens) => {
                        info.set_sizes(tokens.sizes);
                        (tokens.update_seq, tokens.purge_seq)
                    }
                    Err(e) => return fail(e),
                }
            } else {
                info.local_tokens(st.ctx.cluster.me)
            };
            match info_json(&info, &seq, &purge_seq) {
                Ok(info) => json(StatusCode::OK, info),
                Err(e) => fail(e),
            }
        }
        Err(e) => fail(e),
    }
}

/// `POST /{db}`: create a document with a server-generated id.
async fn create_doc(req: DbRequest<'_>, principal: &Principal) -> Response {
    let DbRequest {
        st,
        db,
        name,
        q,
        headers,
        body,
        ..
    } = req;
    if let Some(response) = require_db_access(st, principal, db, false) {
        return response;
    }
    if let Some(response) = reject_during_rebalance(st, db, &Method::POST, "", headers) {
        return response;
    }
    // CouchDB reads the body as JSON first; an empty one is as
    // invalid as garbage.
    let Ok(mut doc) = crate::json::from_slice(body) else {
        return fail(ApiError::bad_request("invalid UTF-8 JSON"));
    };
    if let Err(e) = validate_doc(&doc) {
        return fail(e);
    }
    if doc.get("_id").is_none() {
        doc["_id"] = json!(crate::util::uuid());
    }
    let id = match doc.get("_id") {
        Some(Value::String(id)) if !id.is_empty() => id.clone(),
        Some(Value::String(_)) => {
            return fail(ApiError::new(
                400,
                "illegal_docid",
                "Document id must not be empty",
            ))
        }
        Some(_) => {
            return fail(ApiError::new(
                400,
                "illegal_docid",
                "Document id must be a string",
            ))
        }
        None => unreachable!("id generated above"),
    };
    if let Err(e) = db.validate_doc_id(&id) {
        return fail(e);
    }
    if let Some(response) = require_design_admin(st, db, principal, std::slice::from_ref(&doc)) {
        return response;
    }
    if let Some(response) = validate_write(db, &mut doc, principal).await {
        return response;
    }
    // `commit_docs` routes the generated id to every replica in its
    // shard set, so a dead primary does not lose the document.
    match commit_docs(
        st,
        db,
        name,
        vec![doc],
        true,
        num(q, "w").unwrap_or(0),
        headers,
    )
    .await
    {
        Ok((results, quorum)) => {
            let result = match single_write_result(results) {
                Ok(result) => result,
                Err(error) => return write_failed(&error),
            };
            if q.get("batch").map(String::as_str) == Some("ok") {
                return json(StatusCode::ACCEPTED, json!({"ok": true, "id": id}));
            }
            json_of(
                if quorum {
                    StatusCode::CREATED
                } else {
                    StatusCode::ACCEPTED
                },
                &result,
            )
        }
        Err(e) => fail(e),
    }
}

async fn partition_info(req: DbRequest<'_>, partition: &str) -> Response {
    let DbRequest {
        st,
        db,
        name,
        method,
        headers,
        ..
    } = req;
    if *method != Method::GET && *method != Method::HEAD {
        return fail(ApiError::new(
            405,
            "method_not_allowed",
            "method not allowed",
        ));
    }
    if st.ctx.cluster.is_clustered() && !st.ctx.cluster.is_internal(headers) {
        let replicas = db.replicas_of_shard(db.partition_shard(partition) as u32);
        if !replicas.contains(&st.ctx.cluster.me) && !replicas.is_empty() {
            for node in &replicas {
                let request = internal_request(
                    &st.ctx,
                    reqwest::Method::GET,
                    &st.ctx.cluster.nodes[*node],
                    &[name, "_partition", partition],
                    Some(headers),
                );
                if let Ok(response) = send_peer(request).await {
                    let status = response.status();
                    let value = response.json::<Value>().await.unwrap_or_else(
                        |error| json!({"error": "bad_gateway", "reason": error.to_string()}),
                    );
                    return json(status, value);
                }
            }
            // No replica answered: fabric_db_partition_info answers
            // {error, {nodedown, <<"progress not possible">>}} (412).
            return fail(nodedown());
        }
    }
    match db.partition_counts(partition).await {
        // `external` comes with the counts, from the same walk of the
        // partition's records. Its share of the other tables is not known,
        // so `active` stays 0.
        Ok(counts) => json(
            StatusCode::OK,
            json!({
                "db_name": name,
                "doc_count": counts.docs,
                "doc_del_count": counts.deleted,
                "partition": partition,
                "sizes": { "active": 0, "external": counts.external },
            }),
        ),
        Err(error) => fail(error),
    }
}

/// `/{db}/_partition/{partition}[/_endpoint...]`; `tail` is everything after
/// `_partition/`.
async fn partition_request(req: DbRequest<'_>, tail: &str) -> Response {
    let (partition, suffix) = match tail.find("/_") {
        Some(index) => (&tail[..index], &tail[index + 1..]),
        None => (tail, ""),
    };
    if !req.db.partitioned {
        return fail(ApiError::bad_request("database is not partitioned"));
    }
    if let Err(error) = crate::db::validate_partition(partition) {
        return fail(error);
    }
    if let Some(requested) = req.q.get("partition") {
        if requested != partition {
            return fail(ApiError::bad_request(
                "Conflicting value for `partition` in query string",
            ));
        }
    }
    match suffix {
        "" => partition_info(req, partition).await,
        "_all_docs" => all_docs(req, Some(partition.to_string())).await,
        _ => match search_path(suffix) {
            Some((ddoc, index, info)) => {
                search_request(
                    req,
                    IndexRef {
                        ddoc: &ddoc,
                        name: &index,
                    },
                    Some(partition.to_string()),
                    info,
                )
                .await
            }
            None => match view_path(suffix) {
                Some((ddoc, view)) => {
                    view_request(
                        req,
                        IndexRef {
                            ddoc: &ddoc,
                            name: &view,
                        },
                        Some(partition.to_string()),
                    )
                    .await
                }
                None => fail(ApiError::not_found("missing partition endpoint")),
            },
        },
    }
}

pub async fn db_rest(
    State(st): State<Arc<AppState>>,
    DbName(name): DbName,
    Path((_, rest)): Path<(String, String)>,
    parts: axum::http::request::Parts,
    body: Bytes,
) -> Response {
    let principal = caller(&st, &parts);
    let axum::http::request::Parts {
        method,
        uri,
        headers,
        ..
    } = parts;
    if rest.is_empty() {
        // CouchDB accepts `GET /db/` and PouchDB uses it during setup.
        return db_root(
            State(st),
            DbName(name),
            Caller(principal),
            method,
            uri,
            headers,
            body,
        )
        .await;
    }
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(e) => return fail(e),
    };
    if let Some(response) =
        require_db_access(&st, &principal, &db, requires_db_admin(&rest, &method))
    {
        return response;
    }
    if let Some(response) = reject_during_rebalance(&st, &db, &method, &rest, &headers) {
        return response;
    }
    let q = query(&uri);
    let req = DbRequest {
        st: &st,
        db: &db,
        name: &name,
        method: &method,
        uri: &uri,
        q: &q,
        headers: &headers,
        principal: &principal,
        body: &body,
    };

    let allowed: Option<&[&str]> = match rest.as_str() {
        "_bulk_docs"
        | "_bulk_get"
        | "_revs_diff"
        | "_purge"
        | "_compact"
        | "_ensure_full_commit"
        | "_view_cleanup"
        | "_search_cleanup" => Some(&["POST"]),
        "_changes" => Some(&["GET", "POST", "HEAD"]),
        "_index_info" => Some(&["GET", "HEAD"]),
        "_revs_limit" | "_purged_infos_limit" | "_security" => Some(&["PUT", "GET"]),
        "_peer_purged_infos" => Some(&["GET"]),
        _ => None,
    };
    // CouchDB (mochiweb) serves a HEAD as a GET, but still advertises only
    // `PUT,GET` in `Allow`, so a HEAD of these endpoints is checked as a GET.
    let checked = if method == Method::HEAD
        && matches!(
            rest.as_str(),
            "_security" | "_revs_limit" | "_purged_infos_limit"
        ) {
        &Method::GET
    } else {
        &method
    };
    if let Some(response) = allowed.and_then(|allowed| require_method(checked, allowed)) {
        return response;
    }

    match rest.as_str() {
        "_bulk_docs" => return bulk_docs(req).await,
        "_bulk_get" => return bulk_get(req).await,
        "_all_docs" => return all_docs(req, None).await,
        "_design_docs" => return design_docs(req).await,
        "_local_docs" => return local_docs(req).await,
        "_changes" => return changes(st.clone(), db.clone(), req).await,
        "_revs_diff" => return revs_diff(req).await,
        "_purge" => return purge(req).await,
        "_peer_purged_infos" => return purged_infos(req).await,
        "_compact" => return compact(req).await,
        "_ensure_full_commit" => return ensure_full_commit(req).await,
        "_revs_limit" => return revs_limit(req).await,
        "_purged_infos_limit" => return purged_infos_limit(req).await,
        "_security" => return security(req).await,
        "_view_cleanup" => return view_cleanup(req).await,
        "_search_cleanup" => return search_cleanup(req).await,
        "_index_info" => return index_info(req),
        "_local" => return fail(ApiError::bad_request("missing local doc id")),
        _ => {}
    }

    // `Path` percent-decodes the wildcard, which would turn an encoded slash
    // in a document id (`a%2Fb`) into a path separator (`a`, attachment `b`).
    // Keep the raw remainder for document routing and decode each segment only
    // after splitting.
    // The database name is one raw segment too: `a%2Fb` arrives decoded as
    // `a/b`, so strip the first raw segment rather than the decoded name.
    let raw_rest = uri
        .path()
        .strip_prefix('/')
        .and_then(|path| path.split_once('/'))
        .map_or(rest.as_str(), |(_, raw)| raw)
        .to_string();

    if let Some(id) = raw_rest.strip_prefix("_local/") {
        return local_doc(req, &percent_decode(id)).await;
    }

    if let Some((ddoc, index, info)) = search_path(&rest) {
        return search_request(
            req,
            IndexRef {
                ddoc: &ddoc,
                name: &index,
            },
            None,
            info,
        )
        .await;
    }

    if let Some(tail) = rest.strip_prefix("_partition/") {
        return partition_request(req, tail).await;
    }

    if let Some((ddoc, view)) = view_path(&rest) {
        return view_request(
            req,
            IndexRef {
                ddoc: &ddoc,
                name: &view,
            },
            None,
        )
        .await;
    }

    doc_request(req, &raw_rest).await
}

/// `POST /{db}/_purge`. A fully purged document is removed from `_changes`
/// entirely, so the view and search indexes cannot be updated incrementally:
/// each sees the purge sequence of a shard it read move, and is rebuilt on
/// its next query.
///
/// In a cluster the coordinator sends each document's purge to every copy of
/// its shard and answers per `purge_results_to_json` (chttpd_db.erl); a
/// forwarded request purges this node's copy only.
async fn purge(req: DbRequest<'_>) -> Response {
    let Ok(request) = serde_json::from_slice::<Value>(req.body) else {
        return fail(ApiError::bad_request("invalid JSON"));
    };
    let Value::Object(map) = request else {
        return fail(ApiError::bad_request("Request body must be a JSON object"));
    };
    let st = Arc::clone(req.st);
    let name = req.name.to_string();
    let headers = req.headers.clone();
    let w = num(req.q, "w").unwrap_or(0);
    let cluster = &req.st.ctx.cluster;
    let coordinating = cluster.is_clustered() && !cluster.is_internal(req.headers);
    // A coordinated purge's forwarded calls outlive a client that goes
    // away, so they hold a detached-write slot, as replica writes do: a
    // slow peer pushes back on new purges rather than piling them up. A
    // peer's request (node-to-node traffic) takes none.
    let slot = if coordinating {
        st.admission.acquire_detached().await
    } else {
        None
    };
    // The purge runs in a task of its own, carrying the request's peer
    // accounting, so a coordinated purge's forwarded calls outlive a client
    // that goes away. The view and search indexes need no telling: each
    // records every shard's purge sequence and rebuilds once one moves.
    let db = Arc::clone(req.db);
    let purge = tokio::spawn(crate::cluster_http::in_request_scope(async move {
        let _slot = slot;
        let cluster = &st.ctx.cluster;
        if !coordinating {
            let request = Value::Object(map);
            // A peer's request carries the uuid the coordinator chose.
            let (request, uuids) = if cluster.is_internal(&headers) {
                crate::db::split_purge_uuids(&request)
            } else {
                (request, Default::default())
            };
            let purged = db.purge(&request, cluster.me, &uuids).await?;
            Ok::<_, ApiError>(json(StatusCode::CREATED, purged))
        } else {
            let results = purge_docs(&st, &db, &name, &map, w, &headers).await?;
            Ok(purge_results_to_json(results))
        }
    }));
    match purge.await {
        Ok(Ok(response)) => response,
        Ok(Err(e)) => fail(e),
        Err(e) => fail(ApiError::internal(format!("purging: {e}"))),
    }
}

/// `GET /{db}/_peer_purged_infos?since=<seq>,<seq>,...`: this node's purge infos
/// per shard after the given purge sequences, for a peer's anti-entropy.
/// Internal only: CouchDB has no such endpoint, its shard copies exchange
/// purge infos inside mem3_rep.
async fn purged_infos(req: DbRequest<'_>) -> Response {
    if !req.st.ctx.cluster.is_internal(req.headers) {
        return fail(ApiError::not_found("missing"));
    }
    let since: Vec<u64> = req
        .q
        .get("since")
        .map(|list| list.split(',').filter_map(|seq| seq.parse().ok()).collect())
        .unwrap_or_default();
    let reader = req.q.get("node").and_then(|node| node.parse().ok());
    match req.db.purged_infos(&since, reader).await {
        Ok(shards) => {
            let shards: Vec<Value> = shards
                .into_iter()
                .map(|(purge_seq, infos)| json!({"purge_seq": purge_seq, "infos": infos}))
                .collect();
            json(StatusCode::OK, json!({"shards": shards}))
        }
        Err(e) => fail(e),
    }
}

/// `purge_results_to_json` (chttpd_db.erl): 201, raised to 202 when an entry
/// was only accepted and to an entry's own status when it failed.
fn purge_results_to_json(results: Vec<(String, PurgeOutcome)>) -> Response {
    let mut code = 201;
    let mut purged = Map::new();
    for (id, outcome) in results {
        let entry = match outcome {
            PurgeOutcome::Ok(revs) => json!(revs),
            PurgeOutcome::Accepted(revs) => {
                code = code.max(202);
                json!(revs)
            }
            PurgeOutcome::Error(e) => {
                code = code.max(e.status);
                json!({"error": e.error, "reason": e.reason})
            }
        };
        purged.insert(id, entry);
    }
    json(
        StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        json!({"purge_seq": Value::Null, "purged": purged}),
    )
}

/// `POST /{db}/_compact`.
async fn compact(req: DbRequest<'_>) -> Response {
    if let Some(response) = require_json_content_type(req.headers) {
        return response;
    }
    // CouchDB answers at once and compacts in the background; `GET /db`
    // reports `compact_running` (queued included) and `_active_tasks` the
    // progress meanwhile. `--compact-concurrency` compactions run at once on
    // this node; the rest wait their turn. One already running or queued on
    // this database is left to finish.
    // Under the queue's lock: a queued compaction claims the database
    // before it leaves the queue, so it is seen as one or the other.
    let mut queue = req
        .st
        .compactions_queued
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let queued = !req.db.compacting() && queue.insert(req.name.to_string());
    drop(queue);
    if queued {
        tokio::spawn(run_queued_compaction(
            Arc::clone(req.st),
            req.name.to_string(),
        ));
    }
    // In a cluster, every other node holding a copy compacts it too
    // (fabric:compact), each asked on its own, so neither a slow peer nor
    // this node's compaction holds up another. A peer already being asked
    // about this database is not asked again meanwhile: it de-duplicates
    // anyway. Counted before the answer.
    for node in crate::cluster_ops::compaction_peers(&req.st.ctx, req.db, req.headers) {
        let key = (req.name.to_string(), node);
        let new = req
            .st
            .compaction_casts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key.clone());
        if !new {
            continue;
        }
        crate::metrics::compaction_cast_started();
        let st = Arc::clone(req.st);
        let headers = req.headers.clone();
        tokio::spawn(async move {
            let delivered =
                crate::cluster_ops::compact_on_peer(&st.ctx, &key.0, node, &headers).await;
            st.compaction_casts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&key);
            crate::metrics::compaction_cast_finished(delivered);
        });
    }
    json(StatusCode::ACCEPTED, json!({"ok": true}))
}

/// Wait for a compaction slot, then compact `name`. Only the name waits,
/// so a queued database can be closed meanwhile.
async fn run_queued_compaction(st: Arc<AppState>, name: String) {
    // The semaphore is never closed; were it, the compaction runs.
    let slot = st.compaction.acquire().await.ok();
    let db = st.db(&name).await;
    // Claimed before the name leaves the queue, so `GET /db` never finds
    // the compaction in neither.
    let claim = db.as_ref().ok().and_then(|db| db.claim_compaction());
    st.compactions_queued
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&name);
    let mut compacted = false;
    match (&db, claim) {
        (Ok(db), Some(claim)) => {
            if let Err(error) = db.run_compaction(claim).await {
                // A database deleted under its compaction is no fault.
                if db.check_live().is_err() {
                    tracing::warn!(db = %name, error = %error.reason, "compaction stopped: database deleted");
                } else {
                    tracing::error!(db = %name, error = %error.reason, "compaction failed");
                }
            } else {
                compacted = true;
            }
        }
        (Ok(_), None) => {}
        // Deleted while it waited.
        (Err(error), _) if error.status == 404 => {
            tracing::warn!(db = %name, error = %error.reason, "compaction not started");
        }
        (Err(error), _) => {
            tracing::error!(db = %name, error = %error.reason, "compaction not started");
        }
    }
    // The new files' sizes, measured once the compaction is reported done
    // and its slot is free for the next.
    drop(slot);
    if let (true, Ok(db)) = (compacted, &db) {
        db.measure_sizes().await;
    }
}

/// `POST /{db}/_ensure_full_commit`.
async fn ensure_full_commit(req: DbRequest<'_>) -> Response {
    match req.db.ensure_full_commit().await {
        Ok(v) => json(StatusCode::CREATED, v),
        Err(e) => fail(e),
    }
}

/// `GET`, `HEAD` or `PUT /{db}/_revs_limit`.
async fn revs_limit(req: DbRequest<'_>) -> Response {
    if *req.method == Method::GET || *req.method == Method::HEAD {
        return match req.db.get_revs_limit().await {
            Ok(n) => json(StatusCode::OK, json!(n)),
            Err(e) => fail(e),
        };
    }
    let limit = match serde_json::from_slice::<Value>(req.body) {
        Ok(value) => match value.as_u64().filter(|limit| *limit > 0) {
            Some(limit) => limit,
            None => {
                return fail(ApiError::bad_request(
                    "`_revs_limit` must be a positive integer",
                ))
            }
        },
        Err(_) => return fail(ApiError::bad_request("invalid UTF-8 JSON")),
    };
    if let Err(e) = req.db.set_revs_limit(limit).await {
        return fail(e);
    }
    match crate::cluster_ops::set_limit_on_peers(
        &req.st.ctx,
        req.name,
        "_revs_limit",
        limit,
        req.headers,
    )
    .await
    {
        Ok(()) => ok(),
        Err(e) => fail(e),
    }
}

/// `GET`, `HEAD` or `PUT /{db}/_purged_infos_limit`: how many purge infos
/// each shard keeps (chttpd_db.erl, default 1000).
async fn purged_infos_limit(req: DbRequest<'_>) -> Response {
    if *req.method == Method::GET || *req.method == Method::HEAD {
        return match req.db.get_purged_infos_limit().await {
            Ok(n) => json(StatusCode::OK, json!(n)),
            Err(e) => fail(e),
        };
    }
    let limit = match serde_json::from_slice::<Value>(req.body) {
        Ok(value) => match value.as_u64().filter(|limit| *limit > 0) {
            Some(limit) => limit,
            None => {
                return fail(ApiError::bad_request(
                    "`_purged_infos_limit` must be a positive integer",
                ))
            }
        },
        Err(_) => return fail(ApiError::bad_request("invalid UTF-8 JSON")),
    };
    if let Err(e) = req.db.set_purged_infos_limit(limit).await {
        return fail(e);
    }
    match crate::cluster_ops::set_limit_on_peers(
        &req.st.ctx,
        req.name,
        "_purged_infos_limit",
        limit,
        req.headers,
    )
    .await
    {
        Ok(()) => ok(),
        Err(e) => fail(e),
    }
}

/// `GET`, `HEAD` or `PUT /{db}/_security`.
async fn security(req: DbRequest<'_>) -> Response {
    if *req.method == Method::GET || *req.method == Method::HEAD {
        return json(StatusCode::OK, req.db.security());
    }
    let value: Value = match serde_json::from_slice(req.body) {
        Ok(value) => value,
        Err(_) => return fail(ApiError::bad_request("invalid security object")),
    };
    if let Err(error) = validate_security(&value) {
        return fail(error);
    }
    match crate::cluster_ops::set_security_everywhere(
        &req.st.ctx,
        req.db,
        req.name,
        value,
        req.headers,
    )
    .await
    {
        Ok(true) => ok(),
        // Stored here and queued for the peers that did not acknowledge it;
        // anti-entropy delivers it.
        Ok(false) => json(StatusCode::ACCEPTED, json!({"ok": true})),
        Err(error) => fail(error),
    }
}

/// `POST /{db}/_view_cleanup`.
async fn view_cleanup(req: DbRequest<'_>) -> Response {
    match crate::view::cleanup(req.db).await {
        Ok(()) => json(StatusCode::ACCEPTED, json!({"ok": true})),
        Err(error) => fail(error),
    }
}

/// `POST /{db}/_search_cleanup`.
async fn search_cleanup(req: DbRequest<'_>) -> Response {
    match crate::search::cleanup(req.db).await {
        Ok(()) => json(StatusCode::ACCEPTED, json!({"ok": true})),
        Err(error) => fail(error),
    }
}

/// `GET /{db}/_index_info`.
fn index_info(req: DbRequest<'_>) -> Response {
    let search = crate::search::all_indexes(req.db);
    let total_rows = search.len();
    json(
        StatusCode::OK,
        json!({
            "total_rows": total_rows,
            "bookmark": Value::Null,
            "view_indexes": [],
            "search_indexes": search,
            "nouveau_indexes": [],
        }),
    )
}

/// Send a create or delete to every other node at once. Each answer is the
/// peer's status, or `None` when it could not be reached.
async fn fan_out_db(
    st: &AppState,
    headers: &HeaderMap,
    method: Method,
    name: &str,
    layout: &[(&str, String)],
) -> Vec<Option<StatusCode>> {
    crate::cluster_http::in_one_round(futures::future::join_all(st.ctx.cluster.peers().map(
        |peer| {
            let request = st
                .ctx
                .client
                .request(method.clone(), peer_url(peer, &[name]))
                .query(layout)
                .header("x-douchdb-local-db", "1")
                .header("x-douchdb-secret", st.ctx.cluster.secret.as_str());
            let request = forward_client_auth(request, headers);
            async move {
                send_peer(request)
                    .await
                    .ok()
                    .map(|response| response.status())
            }
        },
    )))
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `method path` as an admin, through the router's database handlers.
    async fn request(st: &Arc<AppState>, method: Method, path: &str) -> (StatusCode, Value) {
        request_with(st, method, path, Bytes::new()).await
    }

    /// `method path` with `body`, as `request`.
    async fn request_with(
        st: &Arc<AppState>,
        method: Method,
        path: &str,
        body: Bytes,
    ) -> (StatusCode, Value) {
        let (mut parts, ()) = axum::http::Request::builder()
            .method(method.clone())
            .uri(path)
            .header(header::CONTENT_TYPE, "application/json")
            .body(())
            .unwrap()
            .into_parts();
        parts.extensions.insert(Principal::service_admin());
        let (name, rest) = path[1..].split_once('/').unwrap_or((&path[1..], ""));
        let response = db_rest(
            State(Arc::clone(st)),
            DbName(name.to_string()),
            Path((name.to_string(), rest.to_string())),
            parts,
            body,
        )
        .await;
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    /// Compactions waiting for `--compact-concurrency` wait by name: each
    /// queued database reports `compact_running` but is not held open, so
    /// `--max-open-dbs` still bounds the open databases.
    #[tokio::test]
    async fn queued_compactions_hold_no_database_open() {
        let root =
            std::env::temp_dir().join(format!("douchdb-compact-queue-{}", crate::util::uuid()));
        let st = crate::state::test_state(root.clone(), 2);
        let names: Vec<String> = (0..5).map(|n| format!("queued{n}")).collect();
        for name in &names {
            st.create_db(name, false, None, None).await.unwrap();
        }
        // The only compaction slot is taken, so every compaction queues.
        let slot = Arc::clone(&st.compaction).acquire_owned().await.unwrap();
        for name in &names {
            let (status, body) = request(&st, Method::POST, &format!("/{name}/_compact")).await;
            assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        }
        assert!(st.dbs.read().await.len() <= 2, "queued databases stay open");
        for name in &names {
            let (status, info) = request(&st, Method::GET, &format!("/{name}")).await;
            assert_eq!(status, StatusCode::OK, "{info}");
            assert_eq!(info["compact_running"], true, "{name}: {info}");
            assert!(st.dbs.read().await.len() <= 2, "queued databases stay open");
        }

        drop(slot);
        for name in &names {
            while request(&st, Method::GET, &format!("/{name}")).await.1["compact_running"] != false
            {
                tokio::task::yield_now().await;
            }
        }
        st.dbs.write().await.clear();
        let _ = std::fs::remove_dir_all(root);
    }

    /// A `_purge` whose client goes away once the purge is queued on the
    /// shard still lands, and the view no longer serves the purged document.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dropped_purge_leaves_no_purged_document_in_the_view() {
        let root = std::env::temp_dir().join(format!("douchdb-purge-drop-{}", crate::util::uuid()));
        let st = crate::state::test_state(root.clone(), 2);
        let db = st.create_db("purged", false, None, None).await.unwrap();
        let written = db
            .put_docs(
                vec![
                    json!({"_id": "_design/d",
                           "views": {"v": {"map": "function(doc) { emit(doc._id, null); }"}}}),
                    json!({"_id": "a"}),
                ],
                true,
            )
            .await
            .unwrap();
        let rev = written[1].json()["rev"].as_str().unwrap().to_string();
        let view = "/purged/_design/d/_view/v";
        let (status, indexed) = request(&st, Method::GET, view).await;
        assert_eq!(status, StatusCode::OK, "{indexed}");
        assert_eq!(indexed["rows"][0]["id"], "a", "{indexed}");

        // Hold the shard's actor, so the purge waits in its queue.
        let shard = db.shard_handle(0);
        let (started_tx, started) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let blocker = tokio::spawn(shard.call(move |_| {
            let _ = started_tx.send(());
            held.recv().map_err(ApiError::internal)
        }));
        started.await.unwrap();
        let holders = Arc::strong_count(&db);
        let purge = tokio::spawn({
            let st = Arc::clone(&st);
            let body = Bytes::from(json!({ "a": [rev] }).to_string());
            async move { request_with(&st, Method::POST, "/purged/_purge", body).await }
        });
        while shard.queued() < 1 {
            tokio::task::yield_now().await;
        }
        purge.abort();
        assert!(purge.await.is_err_and(|error| error.is_cancelled()));
        release.send(()).unwrap();
        blocker.await.unwrap().unwrap();
        shard.barrier().await.unwrap().unwrap();
        // Whatever the dropped request left running is done with the database.
        while Arc::strong_count(&db) > holders {
            tokio::task::yield_now().await;
        }

        let gone = db.get_doc("a", &crate::db::DocOpts::default()).await;
        assert_eq!(gone.unwrap_err().status, 404, "the purge did not land");
        let (status, after) = request(&st, Method::GET, view).await;
        assert_eq!(status, StatusCode::OK, "{after}");
        assert_eq!(
            after["rows"],
            json!([]),
            "the view still serves the purged document"
        );
        drop(db);
        st.dbs.write().await.clear();
        let _ = std::fs::remove_dir_all(root);
    }

    /// A `_purge` that lands on one shard and fails on another answers with
    /// the failure, and the landed purge leaves no document in the view.
    /// A refused body purges nothing and leaves the view index in place.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_partly_failed_purge_leaves_no_purged_document_in_the_view() {
        let root =
            std::env::temp_dir().join(format!("douchdb-purge-partial-{}", crate::util::uuid()));
        let st = crate::state::test_state(root.clone(), 2);
        let db = st.create_db("partial", false, Some(2), None).await.unwrap();
        let on = |shard: usize, tag: &str| {
            (0..)
                .map(|i| format!("{tag}{i}"))
                .find(|id| db.shard_index(id) == shard)
                .unwrap()
        };
        let (doc, damaged) = (on(0, "doc"), on(1, "damaged"));
        let written = db
            .put_docs(
                vec![
                    json!({"_id": doc}),
                    json!({"_id": "_design/d",
                           "views": {"v": {"map": "function(doc) { emit(doc._id, null); }"}}}),
                ],
                true,
            )
            .await
            .unwrap();
        let rev = written[0].json()["rev"].as_str().unwrap().to_string();
        let (status, indexed) = request(&st, Method::GET, "/partial/_design/d/_view/v").await;
        assert_eq!(status, StatusCode::OK, "{indexed}");
        assert!(indexed["rows"].to_string().contains(&doc), "{indexed}");
        let indexes = || {
            std::fs::read_dir(db.path.join("_view_indexes"))
                .unwrap()
                .count()
        };
        assert_ne!(indexes(), 0, "the view built no index file");
        // Shard 1's purge cannot read the damaged document's metadata.
        let key = damaged.clone();
        db.shard_handle(1)
            .call(move |s| {
                s.put_unsealed(crate::kv::META, key.as_str(), br#"{"tree":{},"seq":9}"#);
                // A job that wrote nothing through the shard commits nothing.
                Ok(s.put_local("setup", b"1")?)
            })
            .await
            .unwrap();

        // A body that is not an object is refused before anything runs.
        let (status, refused) =
            request_with(&st, Method::POST, "/partial/_purge", Bytes::from("[]")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{refused}");
        assert_ne!(indexes(), 0, "a refused purge dropped the view index");

        let body =
            json!({ doc.clone(): [rev], damaged.clone(): ["1-ffffffffffffffffffffffffffffffff"] });
        let (status, failed) = request_with(
            &st,
            Method::POST,
            "/partial/_purge",
            Bytes::from(body.to_string()),
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{failed}");
        assert!(
            failed["reason"].to_string().contains("checksum"),
            "{failed}"
        );
        let gone = db.get_doc(&doc, &crate::db::DocOpts::default()).await;
        assert_eq!(
            gone.unwrap_err().status,
            404,
            "shard 0's purge did not land"
        );
        // The damaged record would fail the rebuild a query starts, so it
        // goes first; removing it moves neither the shard's uuid nor its
        // purge sequence.
        db.shard_handle(1)
            .call(move |s| Ok(s.remove_meta(&damaged, None)?))
            .await
            .unwrap();
        let (status, after) = request(&st, Method::GET, "/partial/_design/d/_view/v").await;
        assert_eq!(status, StatusCode::OK, "{after}");
        assert_eq!(
            after["rows"],
            json!([]),
            "the view still serves the purged document"
        );
        drop(db);
        st.dbs.write().await.clear();
        let _ = std::fs::remove_dir_all(root);
    }
}

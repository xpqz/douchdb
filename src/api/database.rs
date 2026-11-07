//! Database-level endpoints. `db_rest` dispatches every `/{db}/{*rest}` path
//! to the handlers in the sibling modules.

use super::*;

pub async fn db_root(
    State(st): State<Arc<AppState>>,
    DbName(name): DbName,
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
    let principal = st.principal(&headers);

    let local_only = st.ctx.cluster.is_internal_db(&headers);
    match method {
        Method::PUT => {
            if !principal.admin {
                return access_denied(&principal);
            }
            let partitioned = flag(&q, "partitioned");
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
            let existing = match st.create_db(&name, partitioned, shards, copies).await {
                Ok(_) => None,
                Err(e) if e.status == 412 && st.ctx.cluster.is_clustered() && !local_only => {
                    Some(e)
                }
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
                let answers = fan_out_db(&st, &headers, Method::PUT, &name, &layout).await;
                if let Some(e) = existing {
                    return fail(e);
                }
                // As fabric_db_create: 201 once a majority of the nodes hold
                // the database (a peer that already had it counts), otherwise
                // 202. The local node is one of them.
                let held = 1 + answers
                    .iter()
                    .flatten()
                    .filter(|status| {
                        status.is_success() || **status == StatusCode::PRECONDITION_FAILED
                    })
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
        Method::DELETE => {
            if !principal.admin {
                return access_denied(&principal);
            }
            let local = st.drop_db(&name).await;
            if !st.ctx.cluster.is_clustered() || local_only {
                return match local {
                    Ok(()) => ok(),
                    Err(e) => fail(e),
                };
            }
            let answers = fan_out_db(&st, &headers, Method::DELETE, &name, &[]).await;
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
                    return fail(local.err().unwrap_or_else(|| {
                        ApiError::not_found(format!("Database does not exist: {name}"))
                    }));
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
        Method::GET | Method::HEAD => match st.db(&name).await {
            Ok(db) => {
                if let Some(response) = require_db_access(&st, &principal, &db, false) {
                    return response;
                }
                if is_forwarded(&st, &headers)
                    && headers
                        .get("x-douchdb-update-seq")
                        .and_then(|value| value.to_str().ok())
                        == Some("1")
                {
                    return json(StatusCode::OK, json!({"update_seq": db.update_seq()}));
                }
                match db.info().await {
                    Ok(mut info) => {
                        // In a cluster the update_seq is the same version vector
                        // the changes feed returns, so clients that use it as
                        // `since` do not skip another node's changes.
                        if st.ctx.cluster.is_clustered() && !is_forwarded(&st, &headers) {
                            let (vector, marks) = match cluster_update_vector(ClusterRead {
                                ctx: &st.ctx,
                                db: &db,
                                dbname: &name,
                                headers: &headers,
                            })
                            .await
                            {
                                Ok(token) => token,
                                Err(e) => return fail(e),
                            };
                            let seq = encode_token(&vector, &marks);
                            info["update_seq"] = json!(seq);
                            info["committed_update_seq"] = json!(seq);
                        }
                        json(StatusCode::OK, info)
                    }
                    Err(e) => fail(e),
                }
            }
            Err(e) => fail(e),
        },
        Method::POST => {
            // Create a document with a server-generated id.
            let db = match st.db(&name).await {
                Ok(db) => db,
                Err(e) => return fail(e),
            };
            if let Some(response) = require_db_access(&st, &principal, &db, false) {
                return response;
            }
            if let Some(response) = reject_during_rebalance(&st, &db, &Method::POST, "", &headers) {
                return response;
            }
            // CouchDB reads the body as JSON first; an empty one is as
            // invalid as garbage.
            let Ok(mut doc) = crate::json::from_slice(&body) else {
                return fail(ApiError::bad_request("invalid UTF-8 JSON"));
            };
            if let Err(e) = validate_doc(&doc) {
                return fail(e);
            }
            if let Err(e) = validate_design_doc(&doc) {
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
            if let Some(response) =
                require_design_admin(&st, &db, &headers, std::slice::from_ref(&doc))
            {
                return response;
            }
            if id.starts_with("_local/") {
                // Local documents bypass the shard path and validation, as
                // with `PUT /{db}/_local/{id}`.
                let local = id.strip_prefix("_local/").unwrap_or(&id);
                return match db.local_put(local, doc).await {
                    Ok(v) => json(StatusCode::CREATED, v),
                    Err(e) => fail(e),
                };
            }
            if let Some(response) = validate_write(&st, &db, &name, &mut doc, &headers).await {
                return response;
            }
            // `commit_docs` routes the generated id to every replica in its
            // shard set, so a dead primary does not lose the document.
            match commit_docs(
                &st,
                &db,
                &name,
                vec![doc],
                true,
                num(&q, "w").unwrap_or(0),
                &headers,
            )
            .await
            {
                Ok((mut results, quorum)) => {
                    let mut result = results.pop().unwrap_or(Value::Null);
                    if result.get("error").is_some() {
                        if let Some(object) = result.as_object_mut() {
                            object.remove("id");
                        }
                        return json(doc_error_status(&result), result);
                    }
                    if q.get("batch").map(String::as_str) == Some("ok") {
                        return json(StatusCode::ACCEPTED, json!({"ok": true, "id": id}));
                    }
                    json(
                        if quorum {
                            StatusCode::CREATED
                        } else {
                            StatusCode::ACCEPTED
                        },
                        result,
                    )
                }
                Err(e) => fail(e),
            }
        }
        _ => fail(ApiError::new(
            405,
            "method_not_allowed",
            "method not allowed",
        )),
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
    if st.ctx.cluster.is_clustered() && !is_forwarded(st, headers) {
        let replicas = db.replicas_of_shard(db.partition_shard(partition) as u32);
        if !replicas.contains(&st.ctx.cluster.me) {
            if let Some(node) = replicas.first() {
                let request = internal_request(
                    &st.ctx,
                    reqwest::Method::GET,
                    &st.ctx.cluster.nodes[*node],
                    &[name, "_partition", partition],
                    Some(headers),
                );
                return match request.send().await {
                    Ok(response) => {
                        let status = response.status();
                        let value = response.json::<Value>().await.unwrap_or_else(
                            |error| json!({"error": "bad_gateway", "reason": error.to_string()}),
                        );
                        json(status, value)
                    }
                    Err(error) => fail(ApiError::new(
                        502,
                        "bad_gateway",
                        format!("{}: {error}", st.ctx.cluster.nodes[*node]),
                    )),
                };
            }
        }
    }
    match db.partition_counts(partition).await {
        Ok((docs, deleted)) => json(
            StatusCode::OK,
            json!({
                "db_name": name,
                "doc_count": docs,
                "doc_del_count": deleted,
                "partition": partition,
                "sizes": { "active": 0, "external": 0 },
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
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if rest.is_empty() {
        // CouchDB accepts `GET /db/` and PouchDB uses it during setup.
        return db_root(State(st), DbName(name), method, uri, headers, body).await;
    }
    let db = match st.db(&name).await {
        Ok(db) => db,
        Err(e) => return fail(e),
    };
    let principal = st.principal(&headers);
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
        "_revs_limit" | "_security" => Some(&["PUT", "GET"]),
        _ => None,
    };
    if let Some(response) = allowed.and_then(|allowed| require_method(&method, allowed)) {
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
        "_compact" => return compact(req).await,
        "_ensure_full_commit" => return ensure_full_commit(req).await,
        "_revs_limit" => return revs_limit(req).await,
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
        return local_doc(&db, &percent_decode(id), &method, &body).await;
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
/// entirely, so the view and search indexes cannot be updated incrementally;
/// they are rebuilt on the next query.
async fn purge(req: DbRequest<'_>) -> Response {
    let Ok(request) = serde_json::from_slice::<Value>(req.body) else {
        return fail(ApiError::bad_request("invalid JSON"));
    };
    let purged = match req.db.purge(&request).await {
        Ok(purged) => purged,
        Err(e) => return fail(e),
    };
    if let Err(e) = invalidate_indexes(req.db).await {
        return fail(e);
    }
    json(StatusCode::OK, purged)
}

async fn invalidate_indexes(db: &Db) -> DbResult<()> {
    crate::view::invalidate(db).await?;
    crate::search::cleanup(db).await
}

/// `POST /{db}/_compact`.
async fn compact(req: DbRequest<'_>) -> Response {
    let Ok(_permit) = req.st.compaction.try_acquire() else {
        return fail(ApiError::new(
            409,
            "compaction_in_progress",
            "another compaction is already running on this node",
        ));
    };
    if let Err(e) = req.db.compact().await {
        return fail(e);
    }
    json(StatusCode::ACCEPTED, json!({"ok": true}))
}

/// `POST /{db}/_ensure_full_commit`.
async fn ensure_full_commit(req: DbRequest<'_>) -> Response {
    match req.db.ensure_full_commit().await {
        Ok(v) => json(StatusCode::CREATED, v),
        Err(e) => fail(e),
    }
}

/// `GET` or `PUT /{db}/_revs_limit`.
async fn revs_limit(req: DbRequest<'_>) -> Response {
    if *req.method == Method::GET {
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
    match req.db.set_revs_limit(limit).await {
        Ok(()) => ok(),
        Err(e) => fail(e),
    }
}

/// `GET` or `PUT /{db}/_security`.
async fn security(req: DbRequest<'_>) -> Response {
    if *req.method == Method::GET {
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
    futures::future::join_all(st.ctx.cluster.peers().map(|peer| {
        let request = st
            .ctx
            .client
            .request(method.clone(), peer_url(peer, &[name]))
            .query(layout)
            .header("x-douchdb-local-db", "1")
            .header("x-douchdb-secret", st.ctx.cluster.secret.as_str());
        let request = forward_client_auth(request, headers);
        async move { request.send().await.ok().map(|response| response.status()) }
    }))
    .await
}

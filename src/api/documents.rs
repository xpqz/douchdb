//! Single-document endpoints, including `_local` documents.

use super::*;

pub(super) async fn local_doc(db: &Db, id: &str, method: &Method, body: &[u8]) -> Response {
    match method.as_str() {
        "GET" => match db.local_get(id).await {
            Ok(Some(doc)) => json(StatusCode::OK, doc),
            Ok(None) => fail(ApiError::not_found("missing")),
            Err(e) => fail(e),
        },
        "PUT" => match crate::json::from_slice(body) {
            Ok(doc) => {
                if let Err(e) = validate_doc(&doc) {
                    return fail(e);
                }
                match db.local_put(id, doc).await {
                    Ok(v) => json(StatusCode::CREATED, v),
                    Err(e) => fail(e),
                }
            }
            Err(_) => fail(ApiError::bad_request("invalid JSON")),
        },
        "DELETE" => match db.local_delete(id).await {
            Ok(v) => json(StatusCode::OK, v),
            Err(e) => fail(e),
        },
        _ => fail(ApiError::new(
            405,
            "method_not_allowed",
            "method not allowed",
        )),
    }
}

pub(super) async fn doc_request(req: DbRequest<'_>, rest: &str) -> Response {
    let DbRequest {
        st,
        db,
        name: dbname,
        q,
        headers,
        ..
    } = req;
    let method = req.method.clone();
    let body = req.body.clone();
    let (docid, attachment) = split_doc_path(rest);
    if docid.is_empty() {
        return fail(ApiError::bad_request("invalid document id"));
    }

    let default_read_quorum = if st.ctx.cluster.is_clustered() {
        db.copies() / 2 + 1
    } else {
        1
    };
    let read_quorum = num(q, "r").unwrap_or(default_read_quorum);
    // `revs_info=false` must not downgrade the requested consistency level.
    let revision_shaped = q.contains_key("open_revs") || flag(q, "revs_info");
    if attachment.is_none()
        && (method == Method::GET || method == Method::HEAD)
        && st.ctx.cluster.is_clustered()
        && read_quorum > 1
        && !is_forwarded(st, headers)
    {
        if revision_shaped {
            // Revision-shaped responses are served locally, but the requested
            // quorum still has to be reachable.
            if let Some(response) =
                quorum_reachable(&st.ctx, db, dbname, &docid, q, headers, read_quorum).await
            {
                return response;
            }
        } else {
            return quorum_read(&st.ctx, db, dbname, &docid, q, headers, read_quorum).await;
        }
    }

    // Serve locally if this node holds a replica; otherwise try every replica
    // in preference order so a dead primary does not break reads or writes.
    let replicas = replicas_of_doc(db, &docid);
    if st.ctx.cluster.is_clustered() && !replicas.contains(&st.ctx.cluster.me) {
        let mut forward_q = q.clone();
        // The peer is a replica; answer locally instead of starting another
        // quorum read and bouncing the request back.
        forward_q.insert("r".to_string(), "1".to_string());
        let request = ForwardRequest {
            method: &method,
            dbname,
            rest,
            q: &forward_q,
            headers,
            body: &body,
        };
        let mut last = None;
        for node in replicas {
            let peer = st.ctx.cluster.nodes[node].clone();
            let response = proxy(&st.ctx, &request, &peer).await;
            if !unavailable(&response) {
                return response;
            }
            last = Some(response);
        }
        return last.unwrap_or_else(|| {
            fail(ApiError::new(
                502,
                "bad_gateway",
                "no replica in the shard set is reachable",
            ))
        });
    }

    if attachment.is_some() {
        // No document has attachments, so a read finds none: 404 for the
        // attachment (or for the document, as CouchDB reports a missing one).
        if method == Method::GET || method == Method::HEAD {
            return match db.get_doc(&docid, &DocOpts::default()).await {
                Ok(_) => fail(ApiError::not_found("Document is missing attachment")),
                Err(error) => fail(error),
            };
        }
        return fail(attachments_unsupported());
    }

    match method {
        Method::GET | Method::HEAD => {
            let include_deleted =
                is_forwarded(st, headers) && headers.contains_key("x-douchdb-quorum-read");
            get_doc(db, &docid, q, headers, include_deleted).await
        }
        Method::PUT => put_doc(st, db, dbname, &docid, body, q, headers).await,
        Method::DELETE => delete_doc(st, db, dbname, &docid, q, headers).await,
        _ if method.as_str() == "COPY" => copy_doc(st, db, dbname, &docid, q, headers).await,
        _ => fail(ApiError::new(
            405,
            "method_not_allowed",
            "method not allowed",
        )),
    }
}

fn split_doc_path(rest: &str) -> (String, Option<String>) {
    // Split on literal slashes first, then decode each segment. That keeps
    // `a%2Fb` as one document id while `doc/att` is still an attachment path.
    let segments: Vec<String> = rest.split('/').map(percent_decode).collect();
    if segments.is_empty() {
        return (String::new(), None);
    }
    if segments[0] == "_design" && segments.len() > 1 {
        let attachment = segments
            .get(2..)
            .map(|parts| parts.join("/"))
            .filter(|name| !name.is_empty());
        return (format!("_design/{}", segments[1]), attachment);
    }
    let attachment = segments
        .get(1..)
        .map(|parts| parts.join("/"))
        .filter(|name| !name.is_empty());
    (segments[0].clone(), attachment)
}

pub(crate) fn doc_opts(q: &HashMap<String, String>) -> DocOpts {
    DocOpts {
        rev: q.get("rev").cloned(),
        revs: flag(q, "revs"),
        revs_info: flag(q, "revs_info"),
        local_seq: flag(q, "local_seq"),
        conflicts: flag(q, "conflicts"),
        deleted_conflicts: flag(q, "deleted_conflicts"),
        attachments: flag(q, "attachments"),
        open_revs: q
            .get("open_revs")
            .map(|v| serde_json::from_str::<Vec<String>>(v).unwrap_or_else(|_| vec![v.clone()])),
        latest: flag(q, "latest"),
        ..Default::default()
    }
}

pub(crate) fn doc_response(doc: Value) -> Response {
    let etag = doc
        .get("_rev")
        .and_then(Value::as_str)
        .map(|r| format!("\"{r}\""))
        .unwrap_or_default();
    let mut response = json(StatusCode::OK, doc);
    if !etag.is_empty() {
        if let Ok(value) = etag.parse() {
            response.headers_mut().insert(header::ETAG, value);
        }
    }
    response
}

async fn get_doc(
    db: &Db,
    docid: &str,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
    include_deleted: bool,
) -> Response {
    let mut opts = doc_opts(q);
    opts.include_deleted = include_deleted;
    match db.get_doc(docid, &opts).await {
        Ok(doc) => {
            let etag = doc
                .get("_rev")
                .and_then(Value::as_str)
                .map(|rev| format!("\"{rev}\""))
                .unwrap_or_default();
            if !etag.is_empty() && etag_matches(headers, header::IF_NONE_MATCH, &etag) {
                let mut response = StatusCode::NOT_MODIFIED.into_response();
                if let Ok(value) = etag.parse() {
                    response.headers_mut().insert(header::ETAG, value);
                }
                return response;
            }
            doc_response(doc)
        }
        Err(e) => fail(e),
    }
}

pub(crate) fn etag_matches(headers: &HeaderMap, name: header::HeaderName, etag: &str) -> bool {
    headers.get_all(name).iter().any(|value| {
        value
            .to_str()
            .ok()
            .unwrap_or_default()
            .split(',')
            .any(|candidate| {
                let candidate = candidate.trim();
                candidate == "*" || candidate == etag || candidate.strip_prefix("W/") == Some(etag)
            })
    })
}

async fn check_if_match(db: &Db, docid: &str, headers: &HeaderMap) -> Option<Response> {
    headers.get(header::IF_MATCH)?;
    let current = db
        .get_doc(docid, &DocOpts::default())
        .await
        .ok()
        .and_then(|doc| {
            doc.get("_rev")
                .and_then(Value::as_str)
                .map(|rev| format!("\"{rev}\""))
        });
    match current {
        Some(etag) if etag_matches(headers, header::IF_MATCH, &etag) => None,
        _ => Some(fail(ApiError::new(
            412,
            "precondition_failed",
            "If-Match does not match the current revision",
        ))),
    }
}

async fn copy_doc(
    st: &AppState,
    db: &Db,
    dbname: &str,
    docid: &str,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Response {
    let Some(raw) = headers
        .get("destination")
        .and_then(|value| value.to_str().ok())
    else {
        return fail(ApiError::bad_request("Destination header is required"));
    };
    let (raw, destination_rev) = match raw.split_once('?') {
        Some((destination, query)) => {
            let rev = query
                .split('&')
                .find_map(|pair| pair.strip_prefix("rev=").map(percent_decode));
            (destination, rev)
        }
        None => (raw, None),
    };
    let destination = percent_decode(raw);
    if destination.is_empty() {
        return fail(ApiError::bad_request("Destination must not be empty"));
    }
    if destination.starts_with("_design/") {
        let principal = st.principal(headers);
        if let Some(response) = require_db_access(st, &principal, db, true) {
            return response;
        }
    }

    let source = match db
        .get_doc(
            docid,
            &DocOpts {
                rev: q.get("rev").cloned(),
                ..Default::default()
            },
        )
        .await
    {
        Ok(doc) => doc,
        Err(error) => return fail(error),
    };
    if source.get("_attachments").is_some() {
        return fail(ApiError::bad_request(
            "Attachments are not supported by douchdb",
        ));
    }
    let mut copy = source;
    if let Some(object) = copy.as_object_mut() {
        object.remove("_id");
        object.remove("_rev");
        object.remove("_revisions");
        object.remove("_conflicts");
        object.remove("_deleted_conflicts");
        object.remove("_deleted");
    }
    copy["_id"] = json!(destination);
    if let Some(rev) = destination_rev {
        copy["_rev"] = json!(rev);
    }
    if let Some(response) = validate_write(st, db, dbname, &mut copy, headers).await {
        return response;
    }
    match commit_docs(
        st,
        db,
        dbname,
        vec![copy],
        true,
        num(q, "w").unwrap_or(0),
        headers,
    )
    .await
    {
        Ok((mut results, _)) => {
            let result = results.pop().unwrap_or(Value::Null);
            if result.get("error").is_some() {
                json(doc_error_status(&result), result)
            } else {
                json(StatusCode::CREATED, result)
            }
        }
        Err(error) => fail(error),
    }
}

async fn put_doc(
    st: &AppState,
    db: &Db,
    dbname: &str,
    docid: &str,
    body: Bytes,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Response {
    let mut doc = match crate::json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return fail(ApiError::bad_request("invalid UTF-8 JSON")),
    };
    if let Err(e) = validate_doc(&doc) {
        return fail(e);
    }
    // The URL owns the id: CouchDB ignores a conflicting `_id` in the body.
    doc["_id"] = json!(docid);
    if let Err(e) = validate_design_doc(&doc) {
        return fail(e);
    }
    if let Err(error) = db.validate_doc_id(docid) {
        return fail(error);
    }
    let body_rev = doc.get("_rev").and_then(Value::as_str).map(str::to_string);
    if let Some(query_rev) = q.get("rev") {
        match &body_rev {
            Some(body_rev) if body_rev != query_rev => {
                return fail(ApiError::bad_request(
                    "Document rev from request body and query string have different values",
                ))
            }
            None => doc["_rev"] = json!(query_rev),
            _ => {}
        }
    }
    if headers.get(header::IF_MATCH).is_some() {
        if let Some(body_rev) = &body_rev {
            let etag = format!("\"{body_rev}\"");
            if !etag_matches(headers, header::IF_MATCH, &etag) {
                return fail(ApiError::bad_request(
                    "Document rev and etag have different values",
                ));
            }
        }
    }
    let new_edits = !q.contains_key("new_edits") || flag(q, "new_edits");
    if !new_edits && doc.get("_rev").is_none() && doc.get("_revisions").is_none() {
        return fail(ApiError::bad_request(
            "When `new_edits: false`, the document needs `_rev` or `_revisions` specified",
        ));
    }
    if let Some(response) = check_if_match(db, docid, headers).await {
        return response;
    }
    let tombstone = doc
        .get("_deleted")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if let Some(response) = validate_write(st, db, dbname, &mut doc, headers).await {
        return response;
    }
    match commit_docs(
        st,
        db,
        dbname,
        vec![doc],
        new_edits,
        num(q, "w").unwrap_or(0),
        headers,
    )
    .await
    {
        Ok((mut results, quorum)) => {
            let mut result = results.pop().unwrap_or(Value::Null);
            if result.get("error").is_some() {
                // Single-document endpoints, unlike `_bulk_docs`, return no id.
                if let Some(obj) = result.as_object_mut() {
                    obj.remove("id");
                }
                return json(doc_error_status(&result), result);
            }
            // `batch=ok` answers 202 without a rev, as for `POST /{db}`.
            if q.get("batch").map(String::as_str) == Some("ok") {
                return json(StatusCode::ACCEPTED, json!({"ok": true, "id": docid}));
            }
            // CouchDB answers 200, not 201, when a PUT lands as a tombstone.
            let status = if tombstone {
                StatusCode::OK
            } else if quorum {
                StatusCode::CREATED
            } else {
                StatusCode::ACCEPTED
            };
            let etag = result
                .get("rev")
                .or_else(|| result.get("_rev"))
                .and_then(Value::as_str)
                .map(|rev| format!("\"{rev}\""));
            let mut response = json(status, result);
            if let Some(etag) = etag {
                if let Ok(value) = etag.parse() {
                    response.headers_mut().insert(header::ETAG, value);
                }
            }
            response
        }
        Err(e) => fail(e),
    }
}

async fn delete_doc(
    st: &AppState,
    db: &Db,
    dbname: &str,
    docid: &str,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Response {
    if let Err(error) = db.validate_doc_id(docid) {
        return fail(error);
    }
    // A missing document is 404 even without a `rev`; only a live document
    // without a rev is a conflict. A tombstone reports reason "deleted".
    if let Err(e) = db.get_doc(docid, &DocOpts::default()).await {
        return fail(e);
    }
    if let Some(response) = check_if_match(db, docid, headers).await {
        return response;
    }
    let Some(rev) = q.get("rev") else {
        return fail(ApiError::conflict());
    };
    let mut doc = json!({"_id": docid, "_rev": rev, "_deleted": true});
    if let Some(response) = validate_write(st, db, dbname, &mut doc, headers).await {
        return response;
    }
    match commit_docs(
        st,
        db,
        dbname,
        vec![doc],
        true,
        num(q, "w").unwrap_or(0),
        headers,
    )
    .await
    {
        Ok((mut results, quorum)) => {
            let mut result = results.pop().unwrap_or(Value::Null);
            if result.get("error").is_some() {
                if let Some(obj) = result.as_object_mut() {
                    obj.remove("id");
                }
                return json(doc_error_status(&result), result);
            }
            json(
                if quorum {
                    StatusCode::OK
                } else {
                    StatusCode::ACCEPTED
                },
                result,
            )
        }
        Err(e) => fail(e),
    }
}

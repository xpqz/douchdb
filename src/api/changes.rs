//! The `_changes` feed and its filters.

use super::*;

/// `limit` for `_changes`, with CouchDB's own messages for a bad value.
fn changes_limit(q: &HashMap<String, String>) -> DbResult<Option<usize>> {
    let Some(raw) = q.get("limit") else {
        return Ok(None);
    };
    match raw.parse::<i64>() {
        Ok(n) if n >= 0 => Ok(Some(usize::try_from(n).unwrap_or(usize::MAX))),
        Ok(_) => Err(ApiError::bad_request(
            "The limit value should be a non-negative integer",
        )),
        Err(_) => Err(ApiError::bad_request(
            "The limit parameter should be a non-negative integer",
        )),
    }
}

/// CouchDB's `_changes` rejects a malformed `since` rather than reading it as
/// 0: it must be `now`, a non-negative integer or an opaque sequence token
/// (`N-<base64>`). douchdb's own cluster tokens decode through `decode_since`.
fn validate_since(raw: &str, nodes: usize) -> DbResult<()> {
    let opaque = raw.split_once('-').is_some_and(|(seq, token)| {
        !seq.is_empty()
            && seq.bytes().all(|b| b.is_ascii_digit())
            && !token.is_empty()
            && token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    });
    if raw == "now" || raw.parse::<u64>().is_ok() || opaque || decode_since(raw, nodes).is_some() {
        return Ok(());
    }
    Err(ApiError::bad_request(
        "Malformed sequence supplied in 'since' parameter.",
    ))
}

/// The `heartbeat` parameter in milliseconds; `true` asks for CouchDB's
/// default of one minute.
fn changes_heartbeat(q: &HashMap<String, String>) -> DbResult<Option<std::time::Duration>> {
    let Some(raw) = q.get("heartbeat") else {
        return Ok(None);
    };
    if raw == "true" {
        return Ok(Some(std::time::Duration::from_millis(60_000)));
    }
    match raw.parse::<i64>() {
        Ok(n) if n > 0 => Ok(Some(std::time::Duration::from_millis(n.unsigned_abs()))),
        Ok(_) => Err(ApiError::bad_request(
            "The heartbeat value should be a positive integer (in milliseconds).",
        )),
        Err(_) => Err(ApiError::bad_request(
            "Invalid heartbeat value. Expecting a positive integer value (in milliseconds).",
        )),
    }
}

fn changes_timeout(q: &HashMap<String, String>) -> DbResult<Option<usize>> {
    let Some(raw) = q.get("timeout") else {
        return Ok(None);
    };
    match raw.parse::<i64>() {
        Ok(n) if n >= 0 => Ok(Some(usize::try_from(n).unwrap_or(usize::MAX))),
        Ok(_) => Err(ApiError::bad_request(
            "The timeout value should be a non-negative integer",
        )),
        Err(_) => Err(ApiError::bad_request(
            "The timeout parameter should be a non-negative integer",
        )),
    }
}

/// `st` and `db` are the owned handles behind `req`: a streaming feed outlives
/// the handler, so it needs `Arc`s rather than the borrows in the request.
pub(super) async fn changes(st: Arc<AppState>, db: Arc<Db>, req: DbRequest<'_>) -> Response {
    let DbRequest {
        name: dbname,
        q,
        body,
        headers,
        ..
    } = req;
    let limit = match changes_limit(q) {
        Ok(limit) => limit,
        Err(error) => return fail(error),
    };
    let local_only = is_forwarded(&st, headers);
    let feed = q.get("feed").map(String::as_str).unwrap_or("normal");
    if !["normal", "continuous", "live", "longpoll", "eventsource"].contains(&feed) {
        return fail(ApiError::bad_request(
            "Supported `feed` types: normal, continuous, live, longpoll, eventsource",
        ));
    }
    if let Some(raw) = q.get("since") {
        if let Err(error) = validate_since(raw, st.ctx.cluster.nodes.len().max(1)) {
            return fail(error);
        }
    }
    let timeout = match changes_timeout(q) {
        Ok(timeout) => timeout,
        Err(error) => return fail(error),
    };
    let heartbeat = match changes_heartbeat(q) {
        Ok(heartbeat) => heartbeat,
        Err(error) => return fail(error),
    };
    // Resolve filters before a longpoll/continuous stream starts. A stream
    // must not turn a bad filter into a 200 response that the client retries
    // forever.
    if let Err(error) = resolve_changes_filter(&db, q).await {
        return fail(error);
    }
    // Cap longpoll/continuous connections so a client cannot pin a worker for
    // an arbitrary amount of time.
    let deadline = timeout.unwrap_or(60_000).min(300_000) as u64;

    let eventsource = feed == "eventsource";
    if feed == "continuous" || eventsource {
        let dbname = dbname.to_string();
        let mut query = q.clone();
        let body = Bytes::copy_from_slice(body);
        let headers = headers.clone();
        let stream = async_stream::stream! {
            let started = std::time::Instant::now();
            let mut sent = 0usize;
            let mut heartbeat = Heartbeat::new(heartbeat);
            // Where the feed stopped, for the closing line CouchDB sends when
            // a continuous feed times out or reaches its limit.
            let mut trailer: Option<(Value, u64)> = None;
            loop {
                if limit.map(|limit| sent >= limit).unwrap_or(false) {
                    break;
                }
                if !still_allowed(&st, &db, &headers) {
                    trailer = None;
                    break;
                }
                match changes_once(&st, &db, &dbname, &query, &body, &headers, local_only).await {
                    Ok(mut value) => {
                        if !still_allowed(&st, &db, &headers) {
                            trailer = None;
                            break;
                        }
                        let rows = crate::json::take_array(&mut value, "results").unwrap_or_default();
                        let mut sent_here = 0usize;
                        let mut last_sent = None;
                        for row in &rows {
                            if limit.map(|limit| sent >= limit).unwrap_or(false) {
                                break;
                            }
                            last_sent = row.get("seq").cloned();
                            sent_here += 1;
                            let json = serde_json::to_vec(&row).unwrap_or_default();
                            let line = feed_line(&json, eventsource);
                            yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                            sent += 1;
                        }
                        let unsent = (rows.len() - sent_here) as u64;
                        let reported = value.get("last_seq").cloned();
                        let end_seq = if unsent > 0 { last_sent.or(reported) } else { reported };
                        let behind = value.get("pending").and_then(Value::as_u64).unwrap_or(0);
                        trailer = end_seq.map(|seq| (seq, behind + unsent));
                        if let Some(seq) = value.get("last_seq") {
                            let since = seq
                                .as_str()
                                .map(str::to_string)
                                .unwrap_or_else(|| seq.to_string());
                            query.insert("since".to_string(), since);
                        }
                    }
                    Err(error) => {
                        trailer = None;
                        let json = serde_json::to_vec(&json!({
                            "error": error.error,
                            "reason": error.reason,
                        }))
                        .unwrap_or_default();
                        let line = feed_line(&json, eventsource);
                        yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                        break;
                    }
                }
                if started.elapsed().as_millis() as u64 >= deadline {
                    break;
                }
                if heartbeat.due() {
                    yield Ok::<Bytes, std::io::Error>(Bytes::from_static(if eventsource {
                        b": heartbeat\n\n"
                    } else {
                        b"\n"
                    }));
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            // CouchDB's eventsource feed ends without this line.
            if let (Some((last_seq, pending)), false) = (trailer, eventsource) {
                let mut line = serde_json::to_vec(&json!({
                    "last_seq": last_seq,
                    "pending": pending,
                }))
                .unwrap_or_default();
                line.push(b'\n');
                yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
            }
        };
        return feed_response(
            if eventsource {
                "text/event-stream"
            } else {
                "application/json"
            },
            stream,
        );
    }

    if feed == "longpoll" {
        if heartbeat.is_some() {
            let mut heartbeat = Heartbeat::new(heartbeat);
            let dbname = dbname.to_string();
            let query = q.clone();
            let body = Bytes::copy_from_slice(body);
            let headers = headers.clone();
            let stream = async_stream::stream! {
                let started = std::time::Instant::now();
                loop {
                    if !still_allowed(&st, &db, &headers) {
                        break;
                    }
                    match changes_once(&st, &db, &dbname, &query, &body, &headers, local_only).await {
                        Ok(value)
                            if (value["results"]
                                .as_array()
                                .map(|rows| !rows.is_empty())
                                .unwrap_or(false)
                                || started.elapsed().as_millis() as u64 >= deadline)
                                && still_allowed(&st, &db, &headers) =>
                        {
                            let mut line = serde_json::to_vec(&value).unwrap_or_default();
                            line.push(b'\n');
                            yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                            break;
                        }
                        Ok(_) => {}
                        Err(error) => {
                            let mut line = serde_json::to_vec(&json!({
                                "error": error.error,
                                "reason": error.reason,
                            }))
                            .unwrap_or_default();
                            line.push(b'\n');
                            yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                            break;
                        }
                    }
                    if heartbeat.due() {
                        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"\n"));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            };
            return feed_response("application/json", stream);
        }
        let started = std::time::Instant::now();
        loop {
            if !still_allowed(&st, &db, headers) {
                return access_denied(&st.principal(headers));
            }
            match changes_once(&st, &db, dbname, q, body, headers, local_only).await {
                Ok(v)
                    if v["results"]
                        .as_array()
                        .map(|a| !a.is_empty())
                        .unwrap_or(false) =>
                {
                    if !still_allowed(&st, &db, headers) {
                        return access_denied(&st.principal(headers));
                    }
                    return json(StatusCode::OK, v);
                }
                Ok(v) if started.elapsed().as_millis() as u64 >= deadline => {
                    if !still_allowed(&st, &db, headers) {
                        return access_denied(&st.principal(headers));
                    }
                    return json(StatusCode::OK, v);
                }
                Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
                Err(e) => return fail(e),
            }
        }
    }

    if feed == "normal" {
        if let Some(response) = changes_normal_raw(&st, &db, q, body, headers, local_only).await {
            return response;
        }
    }

    match changes_once(&st, &db, dbname, q, body, headers, local_only).await {
        Ok(v) => json(StatusCode::OK, v),
        Err(e) => fail(e),
    }
}

/// One feed entry: an SSE `data:` event for `eventsource`, else a JSON line.
fn feed_line(json: &[u8], eventsource: bool) -> Vec<u8> {
    if eventsource {
        [b"data: ".as_slice(), json, b"\n\n"].concat()
    } else {
        [json, b"\n"].concat()
    }
}

async fn changes_normal_raw(
    st: &AppState,
    db: &Arc<Db>,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
    local_only: bool,
) -> Option<Response> {
    if flag(q, "descending") {
        return None;
    }
    let mut local_mask = None;
    if st.ctx.cluster.is_clustered() && !local_only {
        let masks = crate::fabric::preferred_shard_masks(&st.ctx, db);
        let active: Vec<usize> = masks
            .iter()
            .enumerate()
            .filter(|(_, mask)| !mask.is_empty())
            .map(|(node, _)| node)
            .collect();
        if active.len() != 1 || active[0] != st.ctx.cluster.me {
            return None;
        }
        local_mask = Some(masks[st.ctx.cluster.me].clone());
    }
    let (mut opts, mut progress) = changes_options(st, db, q, body, headers, local_only)
        .await
        .ok()?;
    let rewrite = local_mask.is_some();
    if let Some(mask) = local_mask {
        opts.owned = Some(mask);
    }
    let me = st.ctx.cluster.me;
    let initial_seq = progress.get(me).copied().unwrap_or(0);
    let pending_descending = opts.descending;
    let pending_owned = opts.owned.clone();
    let pending_db = db.clone();
    let mut rows = Box::pin(Db::changes_stream_arc(db.clone(), opts));
    let stream = async_stream::stream! {
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"{\"results\":["));
        let mut buffer = String::with_capacity(64 * 1024);
        let mut first = true;
        let mut last_seq = initial_seq;
        while let Some(item) = rows.next().await {
            let row = match item {
                Ok(row) => row,
                Err(error) => {
                    yield Err(std::io::Error::other(format!("{}: {}", error.error, error.reason)));
                    return;
                }
            };
            last_seq = row.seq;
            if rewrite {
                progress[me] = row.seq;
            }
            if !first {
                buffer.push(',');
            }
            first = false;
            buffer.push_str(&Db::change_row_json(&row));
            if buffer.len() >= 64 * 1024 {
                yield Ok(Bytes::from(std::mem::take(&mut buffer)));
            }
        }
        let last_seq_json = if rewrite {
            crate::fabric::encode_since(&progress)
        } else {
            last_seq.to_string()
        };
        let pending = pending_db
            .changes_pending(last_seq, pending_descending, pending_owned.as_ref())
            .await
            .unwrap_or(0);
        buffer.push_str("],\"last_seq\":");
        if rewrite {
            buffer.push('"');
            buffer.push_str(&last_seq_json);
            buffer.push('"');
        } else {
            buffer.push_str(&last_seq_json);
        }
        buffer.push_str(&format!(",\"pending\":{pending}}}"));
        if !buffer.is_empty() {
            yield Ok(Bytes::from(buffer));
        }
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from_stream(stream))
        .ok()
}

async fn changes_options(
    st: &AppState,
    db: &Db,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
    local_only: bool,
) -> DbResult<(ChangesOpts, Vec<u64>)> {
    let n = st.ctx.cluster.nodes.len().max(1);
    let raw_since = q.get("since").map(String::as_str);
    let mut progress = if raw_since == Some("now") {
        if st.ctx.cluster.is_clustered() && !local_only {
            cluster_update_vector(ClusterRead {
                ctx: &st.ctx,
                db,
                dbname: &db.name,
                headers,
            })
            .await?
            .0
        } else {
            let mut vector = vec![0; n];
            vector[st.ctx.cluster.me] = db.update_seq();
            vector
        }
    } else if let Some(raw) = raw_since {
        match decode_since(raw, n) {
            Some(mut vector) => {
                vector.resize(n.max(vector.len()), 0);
                vector
            }
            None => vec![since_component(Some(raw), st.ctx.cluster.me, n); n],
        }
    } else {
        vec![0; n]
    };
    progress.resize(n, 0);
    let since = progress.get(st.ctx.cluster.me).copied().unwrap_or(0);
    let owned = if is_forwarded(st, headers) {
        q.get("shards").and_then(|raw| parse_shard_mask(raw, db.q))
    } else {
        None
    };
    let opts = ChangesOpts {
        since,
        limit: changes_limit(q)?,
        include_docs: flag(q, "include_docs"),
        style_all_docs: q.get("style").map(String::as_str) == Some("all_docs"),
        descending: flag(q, "descending"),
        doc_ids: doc_ids_from(q, body),
        filter: resolve_changes_filter(db, q).await?,
        conflicts: flag(q, "conflicts"),
        owned,
        until: None,
    };
    Ok((opts, progress))
}

async fn changes_once(
    st: &AppState,
    db: &Db,
    dbname: &str,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
    local_only: bool,
) -> DbResult<Value> {
    if st.ctx.cluster.is_clustered() && !local_only && !flag(q, "descending") {
        return cluster_changes(
            ClusterRead {
                ctx: &st.ctx,
                db,
                dbname,
                headers,
            },
            q,
            body,
        )
        .await;
    }

    let (opts, _) = changes_options(st, db, q, body, headers, local_only).await?;
    db.changes(&opts).await
}

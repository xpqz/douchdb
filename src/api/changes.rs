//! The `_changes` feed and its filters.

use super::*;

pub(super) async fn changes(
    st: Arc<AppState>,
    db: Arc<Db>,
    dbname: &str,
    q: &HashMap<String, String>,
    method: &Method,
    body: &[u8],
    headers: &HeaderMap,
) -> Response {
    let _ = method;
    let local_only = is_forwarded(&st, headers);
    let feed = q.get("feed").map(String::as_str).unwrap_or("normal");
    // Resolve filters before a longpoll/continuous stream starts. A stream
    // must not turn a bad filter into a 200 response that the client retries
    // forever.
    if let Err(error) = resolve_changes_filter(&db, q).await {
        return fail(error);
    }
    // Cap longpoll/continuous connections so a client cannot pin a worker for
    // an arbitrary amount of time.
    let deadline = num(q, "timeout").unwrap_or(60_000).min(300_000) as u64;
    let heartbeat = num(q, "heartbeat")
        .filter(|value| *value > 0)
        .map(|value| std::time::Duration::from_millis(value as u64));

    let eventsource = feed == "eventsource";
    if feed == "continuous" || eventsource {
        let dbname = dbname.to_string();
        let mut query = q.clone();
        let body = Bytes::copy_from_slice(body);
        let headers = headers.clone();
        let limit = num(q, "limit");
        let stream = async_stream::stream! {
            let started = std::time::Instant::now();
            let mut sent = 0usize;
            let mut last_heartbeat = std::time::Instant::now();
            loop {
                if limit.map(|limit| sent >= limit).unwrap_or(false)
                    || !still_allowed(&st, &db, &headers)
                {
                    break;
                }
                match changes_once(&st, &db, &dbname, &query, &body, &headers, local_only).await {
                    Ok(value) => {
                        if !still_allowed(&st, &db, &headers) {
                            break;
                        }
                        let rows = value["results"].as_array().cloned().unwrap_or_default();
                        for row in rows {
                            if limit.map(|limit| sent >= limit).unwrap_or(false) {
                                break;
                            }
                            let json = serde_json::to_vec(&row).unwrap_or_default();
                            let line = if eventsource {
                                [b"data: ".as_slice(), json.as_slice(), b"\n\n"].concat()
                            } else {
                                [json.as_slice(), b"\n"].concat()
                            };
                            yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                            sent += 1;
                        }
                        if let Some(seq) = value.get("last_seq") {
                            let since = seq
                                .as_str()
                                .map(str::to_string)
                                .unwrap_or_else(|| seq.to_string());
                            query.insert("since".to_string(), since);
                        }
                    }
                    Err(error) => {
                        let json = serde_json::to_vec(&json!({
                            "error": error.error,
                            "reason": error.reason,
                        }))
                        .unwrap_or_default();
                        let line = if eventsource {
                            [b"data: ".as_slice(), json.as_slice(), b"\n\n"].concat()
                        } else {
                            [json.as_slice(), b"\n"].concat()
                        };
                        yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                        break;
                    }
                }
                if started.elapsed().as_millis() as u64 >= deadline {
                    break;
                }
                if let Some(heartbeat) = heartbeat {
                    if last_heartbeat.elapsed() >= heartbeat {
                        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(if eventsource {
                            b": heartbeat\n\n"
                        } else {
                            b"\n"
                        }));
                        last_heartbeat = std::time::Instant::now();
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        };
        return Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                if eventsource {
                    "text/event-stream"
                } else {
                    "application/json"
                },
            )
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from_stream(stream))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    if feed == "longpoll" {
        if let Some(heartbeat) = heartbeat {
            let dbname = dbname.to_string();
            let query = q.clone();
            let body = Bytes::copy_from_slice(body);
            let headers = headers.clone();
            let stream = async_stream::stream! {
                let started = std::time::Instant::now();
                let mut last_heartbeat = std::time::Instant::now();
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
                    if last_heartbeat.elapsed() >= heartbeat {
                        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"\n"));
                        last_heartbeat = std::time::Instant::now();
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            };
            return Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::CACHE_CONTROL, "no-cache")
                .body(Body::from_stream(stream))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
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
    if st.cluster.is_clustered() && !local_only {
        let masks = crate::fabric::preferred_shard_masks(st, db);
        let active: Vec<usize> = masks
            .iter()
            .enumerate()
            .filter(|(_, mask)| mask.iter().any(|owned| *owned))
            .map(|(node, _)| node)
            .collect();
        if active.len() != 1 || active[0] != st.cluster.me {
            return None;
        }
        local_mask = Some(masks[st.cluster.me].clone());
    }
    let (mut opts, mut progress) = changes_options(st, db, q, body, headers, local_only)
        .await
        .ok()?;
    let rewrite = local_mask.is_some();
    if let Some(mask) = local_mask {
        opts.owned = Some(mask);
    }
    let me = st.cluster.me;
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
        let pending = if rewrite {
            0
        } else {
            pending_db
                .changes_pending(last_seq, pending_descending, pending_owned.as_deref())
                .await
                .unwrap_or(0)
        };
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

async fn design_filter(
    db: &Db,
    spec: &str,
    is_view: bool,
    q: &HashMap<String, String>,
) -> DbResult<ChangeFilter> {
    let (ddoc, name) = spec
        .split_once('/')
        .ok_or_else(|| ApiError::bad_request("filter must be `design/name`"))?;
    if ddoc.is_empty() || name.is_empty() || name.contains('/') {
        return Err(ApiError::not_found("missing"));
    }
    let id = format!("_design/{ddoc}");
    let doc = db
        .get_doc(&id, &DocOpts::default())
        .await
        .map_err(|_| ApiError::not_found("missing"))?;
    let source = if is_view {
        doc.get("views")
            .and_then(|views| views.get(name))
            .and_then(|view| view.get("map"))
            .and_then(Value::as_str)
    } else {
        doc.get("filters")
            .and_then(|filters| filters.get(name))
            .and_then(Value::as_str)
    }
    .ok_or_else(|| ApiError::not_found("missing"))?;
    let query = q
        .iter()
        .map(|(key, value)| (key.clone(), Value::String(value.clone())))
        .collect::<Map<String, Value>>();
    Ok(ChangeFilter::Js {
        source: source.to_string(),
        query: Value::Object(query).to_string(),
    })
}

pub(crate) async fn resolve_changes_filter(
    db: &Db,
    q: &HashMap<String, String>,
) -> DbResult<Option<ChangeFilter>> {
    let explicit = q.get("filter").map(String::as_str);
    let view = q
        .get("view")
        .map(String::as_str)
        .filter(|view| !view.is_empty());
    match explicit {
        // Id and selector filters are handled without loading bodies. PouchDB
        // also applies selector filtering client-side.
        Some("_doc_ids") | Some("_selector") => Ok(None),
        Some("_design") => Ok(Some(ChangeFilter::Design)),
        Some("_view") => {
            let Some(view) = view else {
                return Err(ApiError::bad_request("`view` parameter is required"));
            };
            design_filter(db, view, true, q).await.map(Some)
        }
        // A bare name means `_design/<name>` with the same filter name.
        Some(name) if !name.contains('/') => {
            let spec = format!("{name}/{name}");
            design_filter(db, &spec, false, q).await.map(Some)
        }
        Some(name) => {
            // CouchDB silently ignores malformed filters with more than one
            // slash rather than failing the feed.
            if name.matches('/').count() != 1 {
                return Ok(None);
            }
            design_filter(db, name, false, q).await.map(Some)
        }
        None => match view {
            Some(view) => design_filter(db, view, true, q).await.map(Some),
            None => Ok(None),
        },
    }
}

async fn changes_options(
    st: &AppState,
    db: &Db,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
    local_only: bool,
) -> DbResult<(ChangesOpts, Vec<u64>)> {
    let n = st.cluster.nodes.len().max(1);
    let raw_since = q.get("since").map(String::as_str);
    let mut progress = if raw_since == Some("now") {
        if st.cluster.is_clustered() && !local_only {
            cluster_update_vector(ClusterRead {
                st,
                db,
                dbname: &db.name,
                headers,
            })
            .await?
        } else {
            let mut vector = vec![0; n];
            vector[st.cluster.me] = db.update_seq();
            vector
        }
    } else if let Some(raw) = raw_since {
        match decode_since(raw) {
            Some(mut vector) => {
                vector.resize(n.max(vector.len()), 0);
                vector
            }
            None => vec![since_component(Some(raw), st.cluster.me); n],
        }
    } else {
        vec![0; n]
    };
    progress.resize(n, 0);
    let since = progress.get(st.cluster.me).copied().unwrap_or(0);
    let owned = if is_forwarded(st, headers) {
        q.get("shards").and_then(|raw| parse_shard_mask(raw, db.q))
    } else {
        None
    };
    let opts = ChangesOpts {
        since,
        limit: num(q, "limit"),
        include_docs: flag(q, "include_docs"),
        style_all_docs: q.get("style").map(String::as_str) == Some("all_docs"),
        descending: flag(q, "descending"),
        doc_ids: doc_ids_from(q, body),
        filter: resolve_changes_filter(db, q).await?,
        conflicts: flag(q, "conflicts"),
        owned,
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
    if st.cluster.is_clustered() && !local_only && !flag(q, "descending") {
        return cluster_changes(
            ClusterRead {
                st,
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

pub(crate) fn doc_ids_from(q: &HashMap<String, String>, body: &[u8]) -> Option<Vec<String>> {
    if q.get("filter").map(String::as_str) != Some("_doc_ids") {
        return None;
    }
    if let Ok(value) = serde_json::from_slice::<Value>(body) {
        if let Some(ids) = value.get("doc_ids").and_then(Value::as_array) {
            return Some(
                ids.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect(),
            );
        }
    }
    q.get("doc_ids")
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
}

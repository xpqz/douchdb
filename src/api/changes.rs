//! The `_changes` feed and its filters.

use super::*;
use crate::db::{ChangesItem, ChangesList, Seq};

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
/// 0 (`validate_start_seq`, fabric_view_changes.erl): it must be `now`, the
/// unquoted `0` or an opaque sequence token (`N-<base64>`) that decodes.
/// Any other plain integer is malformed too. A peer node's own requests keep
/// passing the plain per-node sequences the shards use.
fn validate_since(raw: &str, local_only: bool) -> DbResult<()> {
    if raw == "now"
        || raw == "0"
        || decode_since(raw).is_some()
        || (local_only && raw.parse::<u64>().is_ok())
    {
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

/// The feed settings that the streaming and long-polling modes share, once
/// the request's parameters are validated.
struct FeedOpts {
    limit: Option<usize>,
    local_only: bool,
    heartbeat: Option<std::time::Duration>,
    /// Milliseconds a longpoll/continuous feed stays open.
    deadline: u64,
    /// How often an idle feed re-checks when no local commit wakes it.
    recheck: std::time::Duration,
}

/// Validate every `_changes` parameter up front and return the feed type
/// with its settings. A stream must not turn a bad parameter or filter into
/// a 200 response that the client retries forever.
async fn parse_feed(
    st: &AppState,
    db: &Arc<Db>,
    q: &HashMap<String, String>,
    headers: &HeaderMap,
) -> DbResult<(&'static str, FeedOpts)> {
    let limit = changes_limit(q)?;
    let local_only = st.ctx.cluster.is_internal(headers);
    // `live` is sugar for `continuous` (`chttpd_db:parse_changes_query`).
    let feed = match q.get("feed").map(String::as_str).unwrap_or("normal") {
        "live" | "continuous" => "continuous",
        "normal" => "normal",
        "longpoll" => "longpoll",
        "eventsource" => "eventsource",
        _ => {
            return Err(ApiError::bad_request(
                "Supported `feed` types: normal, continuous, live, longpoll, eventsource",
            ))
        }
    };
    if let Some(raw) = q.get("since") {
        validate_since(raw, local_only)?;
    }
    let timeout = changes_timeout(q)?;
    let heartbeat = changes_heartbeat(q)?;
    resolve_changes_filter(db, q).await?;
    // Cap longpoll/continuous connections so a client cannot pin a worker for
    // an arbitrary amount of time.
    let deadline = timeout.unwrap_or(60_000).min(300_000) as u64;
    let recheck = if st.ctx.cluster.is_clustered() && !local_only {
        CLUSTER_FEED_RECHECK
    } else {
        st.config.local_feed_recheck
    };
    Ok((
        feed,
        FeedOpts {
            limit,
            local_only,
            heartbeat,
            deadline,
            recheck,
        },
    ))
}

/// `st` and `db` are the owned handles behind `req`: a streaming feed outlives
/// the handler, so it needs `Arc`s rather than the borrows in the request.
pub(super) async fn changes(st: Arc<AppState>, db: Arc<Db>, req: DbRequest<'_>) -> Response {
    // `last-event-id` is another name for `since`, and an eventsource
    // client's `Last-Event-ID` header (a browser reconnecting) wins over both
    // (`parse_changes_query` in chttpd_db.erl).
    let mut req = req;
    let from_header = match req.headers.get("last-event-id") {
        Some(value) if req.q.get("feed").map(String::as_str) == Some("eventsource") => {
            value.to_str().ok().map(str::to_string)
        }
        _ => None,
    };
    let resume = from_header.or_else(|| req.q.get("last-event-id").cloned());
    let resumed_query;
    if let Some(since) = resume {
        let mut query = req.q.clone();
        query.insert("since".to_string(), since);
        resumed_query = query;
        req.q = &resumed_query;
    }
    let (feed, opts) = match parse_feed(&st, &db, req.q, req.headers).await {
        Ok(parsed) => parsed,
        Err(error) => return fail(error),
    };
    match feed {
        "continuous" => changes_continuous(st, db, req, opts, false),
        "eventsource" => changes_continuous(st, db, req, opts, true),
        "longpoll" if opts.heartbeat.is_some() => changes_longpoll_stream(st, db, req, opts),
        "longpoll" => changes_longpoll(&st, &db, req, &opts).await,
        _ => changes_normal(&st, &db, req, &opts).await,
    }
}

/// `feed=normal`: the raw fast path when it applies, else one pass.
async fn changes_normal(
    st: &AppState,
    db: &Arc<Db>,
    req: DbRequest<'_>,
    opts: &FeedOpts,
) -> Response {
    let DbRequest {
        name: dbname,
        q,
        body,
        headers,
        ..
    } = req;
    if let Some(response) = changes_normal_raw(st, db, q, body, headers, opts.local_only).await {
        return response;
    }
    match changes_once(st, db, dbname, q, body, headers, opts.local_only).await {
        Ok(v) => json_of(StatusCode::OK, &v),
        Err(e) => fail(e),
    }
}

/// `feed=continuous` and `feed=eventsource`: one JSON line (or SSE event) per
/// change until the deadline, the limit or shutdown.
fn changes_continuous(
    st: Arc<AppState>,
    db: Arc<Db>,
    req: DbRequest<'_>,
    opts: FeedOpts,
    eventsource: bool,
) -> Response {
    let FeedOpts {
        limit,
        local_only,
        heartbeat,
        deadline,
        recheck,
    } = opts;
    let dbname = req.name.to_string();
    let mut query = req.q.clone();
    let body = Bytes::copy_from_slice(req.body);
    let headers = req.headers.clone();
    let stream = async_stream::stream! {
        let started = std::time::Instant::now();
        let mut sent = 0usize;
        let mut heartbeat = Heartbeat::new(heartbeat);
        // Where the feed stopped, for the closing line CouchDB sends when
        // a continuous feed times out or reaches its limit.
        let mut trailer: Option<(Seq, u64)> = None;
        let mut wake = db.subscribe_commits();
        loop {
            if limit.map(|limit| sent >= limit).unwrap_or(false) {
                break;
            }
            if !still_allowed(&st, &db, &headers) {
                trailer = None;
                break;
            }
            wake.borrow_and_update();
            match changes_once(&st, &db, &dbname, &query, &body, &headers, local_only).await {
                Ok(value) => {
                    if !still_allowed(&st, &db, &headers) {
                        trailer = None;
                        break;
                    }
                    let ChangesList { results: rows, last_seq: reported, pending: behind } = value;
                    let mut sent_here = 0usize;
                    let mut last_sent = None;
                    for row in &rows {
                        if limit.map(|limit| sent >= limit).unwrap_or(false) {
                            break;
                        }
                        last_sent = Some(row.seq.clone());
                        sent_here += 1;
                        let json = serde_json::to_vec(&row).unwrap_or_default();
                        let line = feed_line(&json, eventsource, Some(&row.seq));
                        yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                        sent += 1;
                    }
                    let unsent = (rows.len() - sent_here) as u64;
                    query.insert("since".to_string(), reported.to_string());
                    let end_seq = if unsent > 0 { last_sent.unwrap_or(reported) } else { reported };
                    trailer = Some((end_seq, behind + unsent));
                }
                Err(error) => {
                    trailer = None;
                    let json = serde_json::to_vec(&json!({
                        "error": error.error,
                        "reason": error.reason,
                    }))
                    .unwrap_or_default();
                    let line = feed_line(&json, eventsource, None);
                    yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                    break;
                }
            }
            if started.elapsed().as_millis() as u64 >= deadline
                || limit.is_some_and(|limit| sent >= limit)
                || st.shutdown.is_cancelled()
            {
                break;
            }
            if heartbeat.due() {
                yield Ok::<Bytes, std::io::Error>(Bytes::from_static(if eventsource {
                    b": heartbeat\n\n"
                } else {
                    b"\n"
                }));
            }
            wait_for_commit(&mut wake, recheck, &heartbeat, started, deadline, &st.shutdown).await;
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
    feed_response(
        if eventsource {
            "text/event-stream"
        } else {
            "application/json"
        },
        stream,
    )
}

/// `feed=longpoll` with a heartbeat: the response streams, sending a blank
/// line while it waits and the whole result as one line at the end.
fn changes_longpoll_stream(
    st: Arc<AppState>,
    db: Arc<Db>,
    req: DbRequest<'_>,
    opts: FeedOpts,
) -> Response {
    let FeedOpts {
        local_only,
        heartbeat,
        deadline,
        recheck,
        ..
    } = opts;
    let mut heartbeat = Heartbeat::new(heartbeat);
    let dbname = req.name.to_string();
    let query = req.q.clone();
    let body = Bytes::copy_from_slice(req.body);
    let headers = req.headers.clone();
    let stream = async_stream::stream! {
        let started = std::time::Instant::now();
        let mut wake = db.subscribe_commits();
        loop {
            if !still_allowed(&st, &db, &headers) {
                break;
            }
            wake.borrow_and_update();
            match changes_once(&st, &db, &dbname, &query, &body, &headers, local_only).await {
                Ok(value)
                    if (!value.results.is_empty()
                        || started.elapsed().as_millis() as u64 >= deadline
                        || st.shutdown.is_cancelled())
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
            wait_for_commit(&mut wake, recheck, &heartbeat, started, deadline, &st.shutdown).await;
        }
    };
    feed_response("application/json", stream)
}

/// `feed=longpoll` without a heartbeat: hold the request until a change
/// arrives or the deadline passes, then answer once.
async fn changes_longpoll(
    st: &AppState,
    db: &Arc<Db>,
    req: DbRequest<'_>,
    opts: &FeedOpts,
) -> Response {
    let DbRequest {
        name: dbname,
        q,
        body,
        headers,
        ..
    } = req;
    let started = std::time::Instant::now();
    let mut wake = db.subscribe_commits();
    let no_heartbeat = Heartbeat::new(None);
    loop {
        if !still_allowed(st, db, headers) {
            return access_denied(&st.principal(headers));
        }
        wake.borrow_and_update();
        match changes_once(st, db, dbname, q, body, headers, opts.local_only).await {
            Ok(v) if !v.results.is_empty() => {
                if !still_allowed(st, db, headers) {
                    return access_denied(&st.principal(headers));
                }
                return json_of(StatusCode::OK, &v);
            }
            Ok(v)
                if started.elapsed().as_millis() as u64 >= opts.deadline
                    || st.shutdown.is_cancelled() =>
            {
                if !still_allowed(st, db, headers) {
                    return access_denied(&st.principal(headers));
                }
                return json_of(StatusCode::OK, &v);
            }
            Ok(_) => {
                wait_for_commit(
                    &mut wake,
                    opts.recheck,
                    &no_heartbeat,
                    started,
                    opts.deadline,
                    &st.shutdown,
                )
                .await
            }
            Err(e) => return fail(e),
        }
    }
}

/// How often an idle clustered feed re-checks when no local commit wakes it.
/// It also reads peers, whose commits this node cannot observe, so it polls
/// more often than a standalone feed (`Config::local_feed_recheck`).
const CLUSTER_FEED_RECHECK: std::time::Duration = std::time::Duration::from_secs(1);

/// Sleep an idle feed until a commit lands, the recheck interval passes, the
/// next heartbeat is due, the feed's deadline arrives or the server starts
/// shutting down, whichever is first.
async fn wait_for_commit(
    wake: &mut tokio::sync::watch::Receiver<()>,
    recheck: std::time::Duration,
    heartbeat: &Heartbeat,
    started: std::time::Instant,
    deadline_ms: u64,
    shutdown: &tokio_util::sync::CancellationToken,
) {
    let left = std::time::Duration::from_millis(deadline_ms).saturating_sub(started.elapsed());
    let wait = recheck
        .min(left)
        .min(heartbeat.next_in().unwrap_or(recheck));
    tokio::select! {
        _ = wake.changed() => {}
        _ = tokio::time::sleep(wait) => {}
        _ = shutdown.cancelled() => {}
    }
}

/// One feed entry: an SSE event for `eventsource`, else a JSON line. A change's
/// event ends with `id: <seq>` as in CouchDB, so a reconnecting client can send
/// it back as `Last-Event-ID`.
fn feed_line(json: &[u8], eventsource: bool, seq: Option<&Seq>) -> Vec<u8> {
    if !eventsource {
        return [json, b"\n"].concat();
    }
    let mut line = [b"data: ".as_slice(), json, b"\n"].concat();
    if let Some(seq) = seq {
        line.extend_from_slice(b"id: ");
        line.extend_from_slice(&serde_json::to_vec(seq).unwrap_or_default());
        line.push(b'\n');
    }
    line.push(b'\n');
    line
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
    // A client sees opaque tokens; a peer's own reads stay plain shard seqs.
    let rewrite = !local_only;
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
                Ok(ChangesItem::Row(row)) => row,
                Ok(ChangesItem::Scanned(seq)) => {
                    last_seq = seq;
                    if let Some(slot) = progress.get_mut(me) {
                        *slot = seq;
                    }
                    continue;
                }
                Err(error) => {
                    yield Err(std::io::Error::other(format!("{}: {}", error.error, error.reason)));
                    return;
                }
            };
            last_seq = row.seq;
            let seq = if rewrite {
                if let Some(slot) = progress.get_mut(me) {
                    *slot = row.seq;
                }
                format!("\"{}\"", crate::fabric::encode_since(&progress))
            } else {
                row.seq.to_string()
            };
            if !first {
                buffer.push(',');
            }
            first = false;
            buffer.push_str(&Db::change_row_json(&row, &seq));
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
        match decode_since(raw) {
            Some(mut vector) => {
                vector.resize(n.max(vector.len()), 0);
                vector
            }
            None => vec![since_component(Some(raw), st.ctx.cluster.me); n],
        }
    } else {
        vec![0; n]
    };
    progress.resize(n, 0);
    // This node always knows its own sequence; a token made elsewhere while it
    // was down has no position for it, and then it is read from the start.
    if let Some(slot) = progress.get_mut(st.ctx.cluster.me) {
        if *slot == crate::fabric::UNKNOWN_SEQ {
            *slot = 0;
        }
    }
    let since = progress.get(st.ctx.cluster.me).copied().unwrap_or(0);
    let owned = if st.ctx.cluster.is_internal(headers) {
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
) -> DbResult<ChangesList> {
    #[cfg(test)]
    record_changes_once(dbname);
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

    let (opts, progress) = changes_options(st, db, q, body, headers, local_only).await?;
    let mut list = db.changes(&opts).await?;
    if !local_only {
        // The sequences a client gets are opaque, as in CouchDB 3, where even
        // one node is a cluster; a peer's own reads stay plain shard seqs.
        let me = st.ctx.cluster.me;
        let token = |seq: &mut Seq| {
            if let Seq::Num(num) = *seq {
                let mut vector = progress.clone();
                if let Some(slot) = vector.get_mut(me) {
                    *slot = num;
                }
                *seq = Seq::Token(crate::fabric::encode_since(&vector));
            }
        };
        token(&mut list.last_seq);
        list.results.iter_mut().for_each(|row| token(&mut row.seq));
    }
    Ok(list)
}

/// `changes_once` calls per database name, so a test can tell whether an idle
/// feed polls.
#[cfg(test)]
static CHANGES_ONCE_CALLS: std::sync::Mutex<Vec<(String, usize)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn record_changes_once(dbname: &str) {
    let mut calls = CHANGES_ONCE_CALLS
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    match calls.iter_mut().find(|(name, _)| name == dbname) {
        Some((_, count)) => *count += 1,
        None => calls.push((dbname.to_string(), 1)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::time::Duration;

    fn changes_once_calls(dbname: &str) -> usize {
        CHANGES_ONCE_CALLS
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .find(|(name, _)| name == dbname)
            .map_or(0, |(_, count)| *count)
    }

    /// A continuous feed used to re-run `changes_once` every 50 ms for as long
    /// as it was open. An idle one must wait for a commit instead, and a
    /// commit must still wake it well before any fallback poll.
    #[tokio::test]
    async fn an_idle_continuous_feed_waits_for_a_commit_instead_of_polling() {
        let root = std::env::temp_dir().join(format!("douchdb-idle-feed-{}", crate::util::uuid()));
        let st = crate::state::test_state(root.clone(), 500);
        let db = st.create_db("idlefeed", false, None, None).await.unwrap();

        let uri: Uri = "/idlefeed/_changes?feed=continuous&since=now&timeout=20000"
            .parse()
            .unwrap();
        let q = query(&uri);
        let headers = HeaderMap::new();
        let body = Bytes::new();
        let response = changes(
            st.clone(),
            db.clone(),
            DbRequest {
                st: &st,
                db: &db,
                name: "idlefeed",
                method: &Method::GET,
                uri: &uri,
                q: &q,
                headers: &headers,
                body: &body,
            },
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let mut feed = response.into_body().into_data_stream();

        // Nothing is written, so nothing arrives.
        assert!(
            tokio::time::timeout(Duration::from_millis(600), feed.next())
                .await
                .is_err()
        );
        let calls = changes_once_calls("idlefeed");
        assert!(calls <= 2, "an idle feed ran changes_once {calls} times");

        // A commit wakes it long before a fallback poll could.
        db.put_docs(vec![json!({"_id": "a"})], true).await.unwrap();
        let line = tokio::time::timeout(Duration::from_secs(60), feed.next())
            .await
            .expect("the feed did not wake for a commit")
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&line).contains("\"id\":\"a\""));

        drop(feed);
        st.dbs.write().await.clear();
        let _ = std::fs::remove_dir_all(root);
    }
}

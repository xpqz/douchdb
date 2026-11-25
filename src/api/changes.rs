//! The `_changes` feed and its filters.

use super::*;
use crate::db::{ChangesItem, ChangesList};
use crate::params::parse_since;
use crate::since::Since;

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
    recheck: Recheck,
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
    // A descending feed starts from the current update_seq whatever `since`
    // says, so CouchDB never validates it there.
    if !flag(q, "descending") {
        parse_since(q.get("since").map(String::as_str))?;
    }
    let timeout = changes_timeout(q)?;
    let heartbeat = changes_heartbeat(q)?;
    resolve_changes_filter(db, q).await?;
    // Cap longpoll/continuous connections so a client cannot pin a worker for
    // an arbitrary amount of time.
    let deadline = timeout.unwrap_or(60_000).min(300_000) as u64;
    let recheck = if st.ctx.cluster.is_clustered() && !local_only {
        Recheck::Peers(CLUSTER_FEED_RECHECK)
    } else {
        Recheck::Security(st.config.local_feed_recheck)
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
    st: &Arc<AppState>,
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
    if st.ctx.cluster.wants_stream(headers) {
        return changes_peer_stream(st, db, q, body, headers).await;
    }
    if let Some(response) = changes_normal_raw(st, db, q, body, headers, opts.local_only).await {
        return response;
    }
    if st.ctx.cluster.is_clustered() && !opts.local_only && !flag(q, "descending") {
        let read = ClusterRead {
            ctx: &st.ctx,
            db,
            dbname,
            headers,
        };
        return match crate::fabric::cluster_changes_stream(read, db.clone(), q, body).await {
            Ok(stream) => Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from_stream(stream))
                .unwrap_or_else(|_| fail(ApiError::internal("building the changes response"))),
            Err(error) => fail(error),
        };
    }
    match changes_once(st, db, dbname, q, body, headers, opts.local_only).await {
        Ok((page, _)) => json_of(StatusCode::OK, &page),
        Err(e) => fail(e),
    }
}

/// This node's part of a coordinator's streamed `_changes` read, as
/// newline-delimited JSON (`Db::changes_lines`). An error after the status
/// is sent ends the body before its end line.
async fn changes_peer_stream(
    st: &AppState,
    db: &Arc<Db>,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
) -> Response {
    let since = match parse_since(q.get("since").map(String::as_str)) {
        Ok(since) => since,
        Err(error) => return fail(error),
    };
    let opts = match changes_options(st, db, q, body, headers, &since).await {
        Ok(opts) => opts,
        Err(error) => return fail(error),
    };
    let lines = Db::changes_lines(db.clone(), st.ctx.cluster.me, opts);
    let stream = async_stream::stream! {
        let mut lines = Box::pin(lines);
        let mut buffer = Vec::with_capacity(64 * 1024);
        while let Some(line) = lines.next().await {
            match line {
                Ok(line) => {
                    buffer.extend_from_slice(line.as_bytes());
                    buffer.push(b'\n');
                }
                Err(error) => {
                    yield Err(std::io::Error::other(format!("{}: {}", error.error, error.reason)));
                    return;
                }
            }
            if buffer.len() >= 64 * 1024 {
                yield Ok::<Bytes, std::io::Error>(Bytes::from(std::mem::take(&mut buffer)));
            }
        }
        yield Ok(Bytes::from(buffer));
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| fail(ApiError::internal("building the changes response")))
}

/// A clustered `feed=longpoll` read too large for one page: the first page
/// is read already (so an error still gets its own status); the rest stream.
fn changes_normal_paged(
    st: Arc<AppState>,
    db: Arc<Db>,
    dbname: &str,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
    first: ChangesList,
) -> Response {
    let stream = paged_changes_body(st, db, dbname, q, body, headers, first);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| fail(ApiError::internal("building the changes response")))
}

/// The JSON of a clustered changes read that did not fit one page: `first`'s
/// rows, then each further page's, each read from where the previous one
/// ended, until no node has more or the request's `limit` is reached; then
/// the last page's `last_seq` and `pending`. A paged read is not one
/// snapshot: a document written while it runs may be listed again, with its
/// new sequence, as any later read would list it.
fn paged_changes_body(
    st: Arc<AppState>,
    db: Arc<Db>,
    dbname: &str,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
    first: ChangesList,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
    let dbname = dbname.to_string();
    let mut query = q.clone();
    let limit = num(q, "limit");
    let body = Bytes::copy_from_slice(body);
    let headers = headers.clone();
    async_stream::stream! {
        let mut buffer = String::from("{\"results\":[");
        let (mut page, mut more) = (first, true);
        let mut sent = 0usize;
        loop {
            for row in &page.results {
                if sent > 0 {
                    buffer.push(',');
                }
                buffer.push_str(&serde_json::to_string(row).unwrap_or_default());
                sent += 1;
            }
            let left = limit.map(|limit| limit.saturating_sub(sent));
            if !more || left == Some(0) || page.results.is_empty() {
                break;
            }
            yield Ok::<Bytes, std::io::Error>(Bytes::from(std::mem::take(&mut buffer)));
            query.insert("since".to_string(), page.last_seq.clone());
            if let Some(left) = left {
                query.insert("limit".to_string(), left.to_string());
            }
            match changes_once(&st, &db, &dbname, &query, &body, &headers, false).await {
                Ok(next) => (page, more) = next,
                Err(error) => {
                    yield Err(std::io::Error::other(format!("{}: {}", error.error, error.reason)));
                    return;
                }
            }
        }
        buffer.push_str("],\"last_seq\":");
        buffer.push_str(&serde_json::to_string(&page.last_seq).unwrap_or_default());
        buffer.push_str(&format!(",\"pending\":{}}}", page.pending));
        yield Ok(Bytes::from(buffer));
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
    let principal = req.principal.clone();
    let stream = async_stream::stream! {
        let started = std::time::Instant::now();
        let mut sent = 0usize;
        let mut heartbeat = Heartbeat::new(heartbeat);
        // Where the feed stopped, for the closing line CouchDB sends when
        // a continuous feed times out or reaches its limit.
        let mut trailer: Option<(String, u64)> = None;
        let mut wake = db.subscribe_commits();
        let mut poll = true;
        loop {
            if limit.map(|limit| sent >= limit).unwrap_or(false) {
                break;
            }
            if !still_allowed(&st, &db, &principal) {
                trailer = None;
                break;
            }
            wake.borrow_and_update();
            // Whether a node has rows past the page just read: then the feed
            // reads on at once instead of waiting for a commit.
            let mut more = false;
            if poll {
            match changes_once(&st, &db, &dbname, &query, &body, &headers, local_only).await {
                Ok((value, rest)) => {
                    more = rest;
                    if !still_allowed(&st, &db, &principal) {
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
                        let line = feed_line(&json, eventsource, Some(row.seq.as_str()));
                        yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                        sent += 1;
                    }
                    let unsent = (rows.len() - sent_here) as u64;
                    query.insert("since".to_string(), reported.clone());
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
            }
            // The timeout ends an idle feed: the changes already there are
            // all sent first, as one unpaged read would have sent them.
            if (!more && started.elapsed().as_millis() as u64 >= deadline)
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
            poll = more
                || wait_for_commit(&mut wake, recheck, &heartbeat, started, deadline, &st.shutdown).await;
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
    let mut query = req.q.clone();
    let body = Bytes::copy_from_slice(req.body);
    let headers = req.headers.clone();
    let principal = req.principal.clone();
    let stream = async_stream::stream! {
        let started = std::time::Instant::now();
        let mut wake = db.subscribe_commits();
        let mut poll = true;
        loop {
            if !still_allowed(&st, &db, &principal) {
                break;
            }
            wake.borrow_and_update();
            if poll {
            match changes_once(&st, &db, &dbname, &query, &body, &headers, local_only).await {
                Ok((value, more))
                    if (!value.results.is_empty()
                        || started.elapsed().as_millis() as u64 >= deadline
                        || st.shutdown.is_cancelled())
                        && still_allowed(&st, &db, &principal) =>
                {
                    if more {
                        let mut rest = Box::pin(paged_changes_body(
                            st.clone(), db.clone(), &dbname, &query, &body, &headers, value,
                        ));
                        while let Some(chunk) = futures::StreamExt::next(&mut rest).await {
                            yield chunk;
                        }
                        yield Ok(Bytes::from_static(b"\n"));
                        break;
                    }
                    let mut line = serde_json::to_vec(&value).unwrap_or_default();
                    line.push(b'\n');
                    yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                    break;
                }
                // Read on from where this poll ended, so `since=now` is not
                // resolved again past a write that wakes the feed.
                Ok((value, _)) => {
                    query.insert("since".to_string(), value.last_seq);
                }
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
            }
            if heartbeat.due() {
                yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"\n"));
            }
            poll = wait_for_commit(&mut wake, recheck, &heartbeat, started, deadline, &st.shutdown).await;
        }
    };
    feed_response("application/json", stream)
}

/// `feed=longpoll` without a heartbeat: hold the request until a change
/// arrives or the deadline passes, then answer once.
async fn changes_longpoll(
    st: &Arc<AppState>,
    db: &Arc<Db>,
    req: DbRequest<'_>,
    opts: &FeedOpts,
) -> Response {
    let DbRequest {
        name: dbname,
        q,
        body,
        headers,
        principal,
        ..
    } = req;
    let started = std::time::Instant::now();
    let mut wake = db.subscribe_commits();
    let no_heartbeat = Heartbeat::new(None);
    let mut q = q.clone();
    loop {
        if !still_allowed(st, db, principal) {
            return access_denied(principal);
        }
        wake.borrow_and_update();
        match changes_once(st, db, dbname, &q, body, headers, opts.local_only).await {
            Ok((v, more)) if !v.results.is_empty() => {
                if !still_allowed(st, db, principal) {
                    return access_denied(principal);
                }
                if more {
                    return changes_normal_paged(
                        st.clone(),
                        db.clone(),
                        dbname,
                        &q,
                        body,
                        headers,
                        v,
                    );
                }
                return json_of(StatusCode::OK, &v);
            }
            Ok((v, _))
                if started.elapsed().as_millis() as u64 >= opts.deadline
                    || st.shutdown.is_cancelled() =>
            {
                if !still_allowed(st, db, principal) {
                    return access_denied(principal);
                }
                return json_of(StatusCode::OK, &v);
            }
            // Read on from where this poll ended, so `since=now` is not
            // resolved again past a write that wakes the feed.
            Ok((v, _)) => {
                q.insert("since".to_string(), v.last_seq);
            }
            Err(e) => return fail(e),
        }
        // A tick that only had to notice a security change needs no new read.
        while !wait_for_commit(
            &mut wake,
            opts.recheck,
            &no_heartbeat,
            started,
            opts.deadline,
            &st.shutdown,
        )
        .await
        {
            if !still_allowed(st, db, principal) {
                return access_denied(principal);
            }
        }
    }
}

/// How often an idle clustered feed re-checks when no local commit wakes it.
/// It also reads peers, whose commits this node cannot observe, so it polls
/// more often than a standalone feed (`Config::local_feed_recheck`).
const CLUSTER_FEED_RECHECK: std::time::Duration = std::time::Duration::from_secs(1);

/// What an idle feed does when no commit wakes it.
#[derive(Clone, Copy)]
enum Recheck {
    /// Standalone: this node sees every commit, so the only out-of-band change
    /// is the security object, which `still_allowed` re-reads. The feed does
    /// not repeat its `changes_once` (filter resolution included).
    Security(std::time::Duration),
    /// Clustered: peers commit where this node cannot see them, so the feed
    /// reads again.
    Peers(std::time::Duration),
}

impl Recheck {
    fn interval(self) -> std::time::Duration {
        match self {
            Recheck::Security(every) | Recheck::Peers(every) => every,
        }
    }
}

/// Sleep an idle feed until a commit lands, the recheck interval passes, the
/// next heartbeat is due, the feed's deadline arrives or the server starts
/// shutting down, whichever is first. Returns whether the feed should read
/// `changes_once` now: always for a commit, the deadline, shutdown or a
/// clustered recheck; for a standalone tick or heartbeat only the security
/// check (done by the caller) is due.
async fn wait_for_commit(
    wake: &mut tokio::sync::watch::Receiver<()>,
    recheck: Recheck,
    heartbeat: &Heartbeat,
    started: std::time::Instant,
    deadline_ms: u64,
    shutdown: &tokio_util::sync::CancellationToken,
) -> bool {
    let left = std::time::Duration::from_millis(deadline_ms).saturating_sub(started.elapsed());
    let every = recheck.interval();
    let wait = every.min(left).min(heartbeat.next_in().unwrap_or(every));
    let woken = tokio::select! {
        _ = wake.changed() => true,
        _ = tokio::time::sleep(wait) => false,
        _ = shutdown.cancelled() => true,
    };
    woken || matches!(recheck, Recheck::Peers(_)) || wait >= left
}

/// One feed entry: an SSE event for `eventsource`, else a JSON line. A change's
/// event ends with `id: <seq>` as in CouchDB, so a reconnecting client can send
/// it back as `Last-Event-ID`.
fn feed_line(json: &[u8], eventsource: bool, seq: Option<&str>) -> Vec<u8> {
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
    let me = st.ctx.cluster.me;
    let since = parse_since(q.get("since").map(String::as_str)).ok()?;
    let mut local_mask = None;
    if st.ctx.cluster.is_clustered() && !local_only {
        // Only when this node serves every shard itself, and the token (if
        // any) names no other node's copy: a token resumes on the copies it
        // names, which takes the cluster path.
        if let Since::Token(copies) = &since {
            if copies.iter().any(|copy| copy.node != me) {
                return None;
            }
        }
        let masks = crate::fabric::preferred_shard_masks(&st.ctx, db);
        let active: Vec<usize> = masks
            .iter()
            .enumerate()
            .filter(|(_, mask)| !mask.is_empty())
            .map(|(node, _)| node)
            .collect();
        if active.len() != 1 || active[0] != me {
            return None;
        }
        local_mask = Some(masks[me].clone());
    }
    let mut opts = changes_options(st, db, q, body, headers, &since)
        .await
        .ok()?;
    if let Some(mask) = local_mask {
        opts.owned = Some(mask);
    }
    let selected: Vec<usize> = (0..db.q as usize)
        .filter(|shard| opts.owned.as_ref().is_none_or(|mask| mask.covers(*shard)))
        .collect();
    // A client gets every selected shard's position in each row's seq; a
    // coordinator merging nodes only that row's shard.
    let full_rows = !local_only;
    let mut progress = opts.since.clone();
    let pending_descending = opts.descending;
    let pending_owned = opts.owned.clone();
    let pending_db = db.clone();
    let mut rows = Box::pin(Db::changes_stream_arc(db.clone(), opts));
    let stream = async_stream::stream! {
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"{\"results\":["));
        let mut buffer = String::with_capacity(64 * 1024);
        let mut first = true;
        while let Some(item) = rows.next().await {
            let row = match item {
                Ok(ChangesItem::Row(row)) => row,
                Ok(ChangesItem::Scanned(shard, seq)) => {
                    progress.0[shard] = seq;
                    continue;
                }
                Err(error) => {
                    yield Err(std::io::Error::other(format!("{}: {}", error.error, error.reason)));
                    return;
                }
            };
            progress.0[row.shard] = row.seq;
            let token = if full_rows {
                pending_db.local_token(me, &selected, &progress)
            } else {
                pending_db.local_token(me, &[row.shard], &progress)
            };
            if !first {
                buffer.push(',');
            }
            first = false;
            buffer.push_str(&Db::change_row_json(&row, &format!("\"{token}\"")));
            if buffer.len() >= 64 * 1024 {
                yield Ok(Bytes::from(std::mem::take(&mut buffer)));
            }
        }
        let last_seq = pending_db.local_token(me, &selected, &progress);
        let pending = pending_db
            .changes_pending(&progress, pending_descending, pending_owned.as_ref())
            .await
            .unwrap_or(0);
        buffer.push_str("],\"last_seq\":\"");
        buffer.push_str(&last_seq);
        buffer.push_str(&format!("\",\"pending\":{pending}}}"));
        yield Ok(Bytes::from(buffer));
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from_stream(stream))
        .ok()
}

/// The options of a read of this node's own shards. `since` positions count
/// only for copies on this node with a matching uuid (`Db::shard_since`).
async fn changes_options(
    st: &AppState,
    db: &Db,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
    since: &Since,
) -> DbResult<ChangesOpts> {
    let owned = if st.ctx.cluster.is_internal(headers) {
        q.get("shards").and_then(|raw| parse_shard_mask(raw, db.q))
    } else {
        None
    };
    Ok(ChangesOpts {
        since: db.shard_since(st.ctx.cluster.me, since),
        limit: changes_limit(q)?,
        include_docs: flag(q, "include_docs"),
        style_all_docs: q.get("style").map(String::as_str) == Some("all_docs"),
        descending: flag(q, "descending"),
        doc_ids: doc_ids_from(q, body),
        filter: resolve_changes_filter(db, q).await?,
        conflicts: flag(q, "conflicts"),
        owned,
        until: None,
    })
}

async fn changes_once(
    st: &AppState,
    db: &Db,
    dbname: &str,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
    local_only: bool,
) -> DbResult<(ChangesList, bool)> {
    st.feed_stats
        .changes_once
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
            Some(CLUSTER_PAGE),
        )
        .await;
    }
    let since = if flag(q, "descending") {
        crate::since::Since::Zero
    } else {
        parse_since(q.get("since").map(String::as_str))?
    };
    let opts = changes_options(st, db, q, body, headers, &since).await?;
    // The sequences are opaque, as in CouchDB 3, where even one node is a
    // cluster. A peer's own reads get one shard per row token, which is all a
    // coordinator merging nodes needs.
    Ok((
        db.changes(st.ctx.cluster.me, &opts, !local_only).await?,
        false,
    ))
}

/// Rows a clustered read asks each node for at a time. A coordinator holds at
/// most this many per node, and reads again for the rest.
const CLUSTER_PAGE: usize = 1000;

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

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
                principal: &Principal::service_admin(),
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
        let calls = st.feed_stats.changes_once.load(Ordering::Relaxed);
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

    /// Standalone, every commit wakes a feed, so its fallback tick only has to
    /// notice a security change. It must not re-run `changes_once` (filter
    /// resolution included) to do that.
    #[tokio::test]
    async fn an_idle_feed_does_not_rerun_changes_once_on_its_fallback_ticks() {
        let root = std::env::temp_dir().join(format!("douchdb-idle-tick-{}", crate::util::uuid()));
        let mut st = crate::state::test_state(root.clone(), 500);
        Arc::get_mut(&mut st).unwrap().config.local_feed_recheck = Duration::from_millis(10);
        let db = st.create_db("idletick", false, None, None).await.unwrap();

        let uri: Uri = "/idletick/_changes?feed=continuous&since=now&heartbeat=20&timeout=20000"
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
                name: "idletick",
                method: &Method::GET,
                uri: &uri,
                q: &q,
                headers: &headers,
                principal: &Principal::service_admin(),
                body: &body,
            },
        )
        .await;
        let mut feed = response.into_body().into_data_stream();

        // Let many fallback ticks (and heartbeats) pass with nothing written.
        for _ in 0..5 {
            let line = tokio::time::timeout(Duration::from_secs(60), feed.next())
                .await
                .expect("the feed sent no heartbeat")
                .unwrap()
                .unwrap();
            assert_eq!(&line[..], b"\n");
        }
        let calls = st.feed_stats.changes_once.load(Ordering::Relaxed);
        assert_eq!(
            calls, 1,
            "an idle feed re-ran changes_once on {calls} ticks"
        );

        // A commit still arrives.
        db.put_docs(vec![json!({"_id": "a"})], true).await.unwrap();
        loop {
            let line = tokio::time::timeout(Duration::from_secs(60), feed.next())
                .await
                .expect("the feed did not wake for a commit")
                .unwrap()
                .unwrap();
            if line.len() > 1 {
                assert!(String::from_utf8_lossy(&line).contains("\"id\":\"a\""));
                break;
            }
        }

        drop(feed);
        st.dbs.write().await.clear();
        let _ = std::fs::remove_dir_all(root);
    }
}

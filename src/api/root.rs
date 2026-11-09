//! Server-level endpoints: `/`, `/_up`, `/_all_dbs`, `/_uuids`, `/_db_updates`
//! and `/_dbs_info`.

use super::*;

pub async fn welcome() -> Response {
    json(
        StatusCode::OK,
        json!({
            "couchdb": "Welcome",
            "version": "3.3.3",
            "vendor": { "name": "douchdb" },
            "features": ["access-ready", "partitioned", "pluggable-storage-engines", "reshard", "scheduler", "search"],
        }),
    )
}

pub async fn up() -> Response {
    json(StatusCode::OK, json!({"status": "ok"}))
}

pub async fn all_dbs(
    State(st): State<Arc<AppState>>,
    Caller(principal): Caller,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if !principal.is_authenticated() {
        return unauthorized();
    }
    let q = query(&uri);
    let (skip, limit) = match (uint_param(&q, "skip"), uint_param(&q, "limit")) {
        (Ok(skip), Ok(limit)) => (skip.unwrap_or(0), limit.unwrap_or(usize::MAX)),
        (Err(error), _) | (_, Err(error)) => return fail(error),
    };
    let mut names = st.db_names().await;
    if st.ctx.cluster.is_clustered() && !st.ctx.cluster.is_internal_db(&headers) {
        for (i, peer) in st.ctx.cluster.nodes.iter().enumerate() {
            if i == st.ctx.cluster.me {
                continue;
            }
            let request = st
                .ctx
                .client
                .get(peer_url(peer, &["_all_dbs"]))
                .header("x-douchdb-local-db", "1")
                .header("x-douchdb-secret", st.ctx.cluster.secret.as_str());
            let request = forward_client_auth(request, &headers);
            if let Ok(response) = send_peer(request).await {
                if let Ok(Value::Array(remote)) = response.json::<Value>().await {
                    for name in remote {
                        if let Some(name) = name.as_str() {
                            if !names.iter().any(|n| n == name) {
                                names.push(name.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    names.sort();
    names.dedup();

    if !principal.admin {
        names = visible_db_names(&st, &principal, names).await;
    }

    let descending = flag(&q, "descending");
    if descending {
        names.reverse();
    }
    if let Some(start) = key_param(&q, "startkey").or_else(|| key_param(&q, "start_key")) {
        names.retain(|name| {
            if descending {
                name <= &start
            } else {
                name >= &start
            }
        });
    }
    if let Some(end) = key_param(&q, "endkey").or_else(|| key_param(&q, "end_key")) {
        names.retain(|name| {
            if descending {
                name >= &end
            } else {
                name <= &end
            }
        });
    }
    names = names.into_iter().skip(skip).take(limit).collect();
    json(StatusCode::OK, json!(names))
}

pub async fn uuids(State(_st): State<Arc<AppState>>, uri: Uri) -> Response {
    let q = query(&uri);
    // CouchDB's `uuids.max_count` defaults to 1000.
    let count = match q.get("count") {
        None => 1,
        Some(raw) => match raw.parse::<usize>() {
            Ok(count) if count > 1000 => {
                return fail(ApiError::bad_request("count parameter too large"))
            }
            Ok(count) => count,
            Err(_) => return fail(ApiError::bad_request("count must be a positive integer")),
        },
    };
    let ids: Vec<String> = (0..count).map(|_| crate::util::uuid()).collect();
    json(StatusCode::OK, json!({ "uuids": ids }))
}

/// `_db_updates` is an in-memory global feed of database lifecycle and write
/// events. It is intentionally best-effort: a restart starts a new sequence.
pub async fn db_updates(State(st): State<Arc<AppState>>, uri: Uri, _: AdminOnly) -> Response {
    let q = query(&uri);
    let feed = q.get("feed").map(String::as_str).unwrap_or("normal");
    let timeout = num(&q, "timeout").unwrap_or(60_000).min(300_000) as u64;
    let heartbeat = num(&q, "heartbeat")
        .filter(|value| *value > 0)
        .map(|value| std::time::Duration::from_millis(value as u64));
    let limit = match q.get("limit").map(|raw| raw.parse::<usize>()) {
        None => None,
        Some(Ok(limit)) => Some(limit),
        Some(Err(_)) => return fail(ApiError::bad_request("invalid_integer")),
    };
    let mut since = match q.get("since").map(String::as_str) {
        Some("now") => st.db_updates.current(),
        Some(raw) => raw.trim_matches('"').parse::<u64>().unwrap_or(0),
        None => 0,
    };

    if feed == "continuous" {
        let stream = async_stream::stream! {
            let started = std::time::Instant::now();
            let mut sent = 0usize;
            let mut heartbeat = Heartbeat::new(heartbeat);
            let mut wake = st.db_updates.subscribe();
            loop {
                if limit.map(|limit| sent >= limit).unwrap_or(false) {
                    break;
                }
                // Mark the signal seen before reading, so an event recorded
                // after the read wakes the wait below.
                wake.borrow_and_update();
                let (events, _) = st.db_updates.since(since);
                st.feed_stats.db_updates_polls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                for event in events {
                    if limit.map(|limit| sent >= limit).unwrap_or(false) {
                        break;
                    }
                    if let Some(seq) = event["seq"].as_str().and_then(|seq| seq.parse().ok()) {
                        since = seq;
                    }
                    let mut line = serde_json::to_vec(&event).unwrap_or_default();
                    line.push(b'\n');
                    yield Ok::<Bytes, std::io::Error>(Bytes::from(line));
                    sent += 1;
                }
                if started.elapsed().as_millis() as u64 >= timeout || st.shutdown.is_cancelled() {
                    break;
                }
                if heartbeat.due() {
                    yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"\n"));
                }
                wait_for_event(&mut wake, &heartbeat, started, timeout, &st.shutdown).await;
            }
        };
        return feed_response("application/json", stream);
    }

    let started = std::time::Instant::now();
    let mut wake = st.db_updates.subscribe();
    let no_heartbeat = Heartbeat::new(None);
    loop {
        wake.borrow_and_update();
        let (events, current) = st.db_updates.since(since);
        st.feed_stats
            .db_updates_polls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if !events.is_empty()
            || feed == "normal"
            || started.elapsed().as_millis() as u64 >= timeout
            || st.shutdown.is_cancelled()
        {
            let selected: Vec<Value> = events
                .into_iter()
                .take(limit.unwrap_or(usize::MAX))
                .collect();
            let last_seq = selected
                .last()
                .and_then(|event| event["seq"].as_str())
                .map(str::to_string)
                .unwrap_or_else(|| current.to_string());
            return json(
                StatusCode::OK,
                json!({"results": selected, "last_seq": last_seq}),
            );
        }
        wait_for_event(&mut wake, &no_heartbeat, started, timeout, &st.shutdown).await;
    }
}

/// Sleep an idle `_db_updates` feed until an event is recorded, the next
/// heartbeat is due, its timeout arrives or the server starts shutting down.
async fn wait_for_event(
    wake: &mut tokio::sync::watch::Receiver<()>,
    heartbeat: &Heartbeat,
    started: std::time::Instant,
    timeout_ms: u64,
    shutdown: &tokio_util::sync::CancellationToken,
) {
    let left = std::time::Duration::from_millis(timeout_ms).saturating_sub(started.elapsed());
    let wait = heartbeat.next_in().map_or(left, |next| next.min(left));
    tokio::select! {
        _ = wake.changed() => {}
        _ = tokio::time::sleep(wait) => {}
        _ = shutdown.cancelled() => {}
    }
}

/// CouchDB's `[chttpd] max_db_number_for_dbs_info_req` default: the most keys
/// one `_dbs_info` request may name.
const MAX_DBS_INFO_KEYS: usize = 100;

pub async fn dbs_info(
    State(st): State<Arc<AppState>>,
    method: Method,
    uri: Uri,
    _: AdminOnly,
    body: Bytes,
) -> Response {
    let q = query(&uri);
    let keys = if method == Method::POST {
        serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|mut value| crate::json::take_array(&mut value, "keys"))
    } else {
        q.get("keys")
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            .and_then(|value| match value {
                Value::Array(keys) => Some(keys),
                _ => None,
            })
    };
    let Some(keys) = keys else {
        return fail(ApiError::bad_request("`keys` must be a JSON array"));
    };
    if keys.len() > MAX_DBS_INFO_KEYS {
        // CouchDB 3.5 answers {"error":"bad_request","reason":"too_many_keys"}.
        return fail(ApiError::bad_request("too_many_keys"));
    }
    let mut results = Vec::new();
    for key in keys {
        let Some(name) = key.as_str() else {
            results.push(json!({"key": key, "error": "bad_request"}));
            continue;
        };
        match st.db(name).await {
            Ok(db) => match db.info().await {
                Ok(info) => {
                    let cluster = &st.ctx.cluster;
                    let seq = encode_local_seq(cluster, info.update_seq);
                    let purge_seq = encode_local_seq(cluster, info.purge_seq);
                    match info_json(&info, &seq, &purge_seq) {
                        Ok(info) => results.push(json!({"key": name, "info": info})),
                        Err(error) => results.push(json!({
                            "key": name,
                            "error": error.error,
                            "reason": error.reason,
                        })),
                    }
                }
                Err(error) => results.push(json!({
                    "key": name,
                    "error": error.error,
                    "reason": error.reason,
                })),
            },
            Err(error) => results.push(json!({
                "key": name,
                "error": error.error,
                "reason": error.reason,
            })),
        }
    }
    json(StatusCode::OK, Value::Array(results))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    /// A longpoll `_db_updates` used to re-read the event log every 100 ms. It
    /// must wait for an event, read the log again only when one is recorded,
    /// and then answer.
    #[tokio::test]
    async fn an_idle_longpoll_db_updates_waits_for_an_event_instead_of_polling() {
        let root = std::env::temp_dir().join(format!("douchdb-idle-dbu-{}", crate::util::uuid()));
        let st = crate::state::test_state(root.clone(), 500);
        let uri: Uri = "/_db_updates?feed=longpoll&timeout=300000".parse().unwrap();
        let feed = tokio::spawn(db_updates(State(st.clone()), uri, AdminOnly));
        while st.feed_stats.db_updates_polls.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Several old poll intervals pass with nothing recorded.
        tokio::time::sleep(Duration::from_millis(700)).await;
        let polls = st.feed_stats.db_updates_polls.load(Ordering::Relaxed);
        assert_eq!(polls, 1, "an idle feed read the log {polls} times");

        st.create_db("woken", false, None, None).await.unwrap();
        let response = tokio::time::timeout(Duration::from_secs(60), feed)
            .await
            .expect("the feed did not wake for an event")
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["results"][0]["db_name"], "woken");
        assert_eq!(body["results"][0]["type"], "created");

        st.dbs.write().await.clear();
        let _ = std::fs::remove_dir_all(root);
    }
}

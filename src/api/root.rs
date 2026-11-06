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

pub async fn all_dbs(State(st): State<Arc<AppState>>, headers: HeaderMap, uri: Uri) -> Response {
    if !st.authorized(&headers) {
        return unauthorized();
    }
    let q = query(&uri);
    let mut names = st.db_names().await;
    if st.cluster.is_clustered() && !st.cluster.is_internal_db(&headers) {
        for (i, peer) in st.cluster.nodes.iter().enumerate() {
            if i == st.cluster.me {
                continue;
            }
            let request = st
                .client
                .get(peer_url(peer, &["_all_dbs"]))
                .header("x-douchdb-local-db", "1")
                .header("x-douchdb-secret", st.cluster.secret.as_str());
            let request = forward_client_auth(request, &headers);
            if let Ok(response) = request.send().await {
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

    let principal = st.principal(&headers);
    if !principal.admin {
        let mut visible = Vec::new();
        for name in names {
            if let Ok(db) = st.db(&name).await {
                if db_access(&st, &principal, &db).is_some() {
                    visible.push(name);
                }
            }
        }
        names = visible;
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
    let skip = num(&q, "skip").unwrap_or(0);
    let limit = num(&q, "limit").unwrap_or(usize::MAX);
    names = names.into_iter().skip(skip).take(limit).collect();
    json(StatusCode::OK, json!(names))
}

pub async fn uuids(State(_st): State<Arc<AppState>>, uri: Uri) -> Response {
    let count = num(&query(&uri), "count").unwrap_or(1).clamp(1, 1000);
    let ids: Vec<String> = (0..count).map(|_| crate::db::uuid()).collect();
    json(StatusCode::OK, json!({ "uuids": ids }))
}

/// `_db_updates` is an in-memory global feed of database lifecycle and write
/// events. It is intentionally best-effort: a restart starts a new sequence.
pub async fn db_updates(State(st): State<Arc<AppState>>, headers: HeaderMap, uri: Uri) -> Response {
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
    }
    let q = query(&uri);
    let feed = q.get("feed").map(String::as_str).unwrap_or("normal");
    let timeout = num(&q, "timeout").unwrap_or(60_000).min(300_000) as u64;
    let heartbeat = num(&q, "heartbeat")
        .filter(|value| *value > 0)
        .map(|value| std::time::Duration::from_millis(value as u64));
    let limit = num(&q, "limit");
    let mut since = match q.get("since").map(String::as_str) {
        Some("now") => st.db_updates.current(),
        Some(raw) => raw.trim_matches('"').parse::<u64>().unwrap_or(0),
        None => 0,
    };

    if feed == "continuous" {
        let stream = async_stream::stream! {
            let started = std::time::Instant::now();
            let mut sent = 0usize;
            let mut last_heartbeat = std::time::Instant::now();
            loop {
                if limit.map(|limit| sent >= limit).unwrap_or(false)
                    || !st.principal(&headers).admin
                {
                    break;
                }
                let (events, _) = st.db_updates.since(since);
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
                if started.elapsed().as_millis() as u64 >= timeout {
                    break;
                }
                if let Some(heartbeat) = heartbeat {
                    if last_heartbeat.elapsed() >= heartbeat {
                        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"\n"));
                        last_heartbeat = std::time::Instant::now();
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
        let (events, current) = st.db_updates.since(since);
        if !events.is_empty() || feed == "normal" || started.elapsed().as_millis() as u64 >= timeout
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
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

pub async fn dbs_info(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Response {
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
    }
    let q = query(&uri);
    let keys = if method == Method::POST {
        serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|value| value.get("keys").and_then(Value::as_array).cloned())
    } else {
        q.get("keys")
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            .and_then(|value| value.as_array().cloned())
    };
    let Some(keys) = keys else {
        return fail(ApiError::bad_request("`keys` must be a JSON array"));
    };
    let mut results = Vec::new();
    for key in keys {
        let Some(name) = key.as_str() else {
            results.push(json!({"key": key, "error": "bad_request"}));
            continue;
        };
        match st.db(name).await {
            Ok(db) => match db.info().await {
                Ok(info) => results.push(json!({"key": name, "info": info})),
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

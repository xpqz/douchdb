//! `_view` and `_search` query endpoints.

use super::*;

/// `_design/{ddoc}/_view/{view}` (the rest has already been percent-decoded).
pub(super) fn search_path(rest: &str) -> Option<(String, String, bool)> {
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() == 4
        && parts[0] == "_design"
        && (parts[2] == "_search" || parts[2] == "_search_info")
        && !parts[1].is_empty()
        && !parts[3].is_empty()
    {
        Some((
            format!("_design/{}", parts[1]),
            parts[3].to_string(),
            parts[2] == "_search_info",
        ))
    } else {
        None
    }
}

pub(super) fn view_path(rest: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() == 4
        && parts[0] == "_design"
        && parts[2] == "_view"
        && !parts[1].is_empty()
        && !parts[3].is_empty()
    {
        Some((format!("_design/{}", parts[1]), parts[3].to_string()))
    } else {
        None
    }
}

fn view_json_param(q: &HashMap<String, String>, name: &str) -> DbResult<Option<Value>> {
    match q.get(name) {
        Some(raw) => match serde_json::from_str::<Value>(raw) {
            Ok(value) => Ok(Some(value)),
            // CouchDB wants JSON-encoded keys, but tolerating a bare string is
            // friendlier to hand-written URLs.
            Err(_) => Ok(Some(Value::String(raw.clone()))),
        },
        None => Ok(None),
    }
}

fn query_values(uri: &Uri, name: &str) -> Vec<String> {
    uri.query()
        .unwrap_or_default()
        .split('&')
        .filter_map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            if percent_decode(key) == name {
                Some(percent_decode(value))
            } else {
                None
            }
        })
        .collect()
}

fn drilldown_clause(value: &Value) -> Option<(String, Vec<Value>)> {
    let values = value.as_array()?;
    let field = values.first()?.as_str()?.to_string();
    Some((field, values.get(1..).unwrap_or_default().to_vec()))
}

fn string_list(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) => text
            .split(',')
            .map(str::trim)
            .filter(|field| !field.is_empty())
            .map(str::to_string)
            .collect(),
        Value::Array(values) => values
            .iter()
            .filter_map(|value| value.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    }
}

fn parse_drilldown_value(value: &Value) -> Vec<Vec<(String, Vec<Value>)>> {
    let Some(values) = value.as_array() else {
        return Vec::new();
    };
    if values.first().map(Value::is_array).unwrap_or(false) {
        values
            .iter()
            .filter_map(|value| drilldown_clause(value).map(|clause| vec![clause]))
            .collect()
    } else {
        drilldown_clause(value)
            .map(|clause| vec![clause])
            .into_iter()
            .collect()
    }
}

pub(super) async fn search_request(
    st: &AppState,
    db: &Db,
    dbname: &str,
    ddoc: &str,
    index_name: &str,
    method: &Method,
    q: &HashMap<String, String>,
    uri: &Uri,
    body: &[u8],
    headers: &HeaderMap,
    partition: Option<String>,
    info: bool,
) -> Response {
    let _ = (dbname, headers);
    if *method != Method::GET && *method != Method::POST && *method != Method::HEAD {
        return fail(ApiError::new(
            405,
            "method_not_allowed",
            "method not allowed",
        ));
    }
    let limits = crate::view::ViewLimits {
        timeout: std::time::Duration::from_millis(st.config.view_timeout_ms),
        memory: st.config.view_memory_mb.saturating_mul(1024 * 1024),
    };
    if info {
        return match crate::search::info(db, ddoc, index_name, limits).await {
            Ok(value) => json(StatusCode::OK, value),
            Err(error) => fail(error),
        };
    }

    let mut request = crate::search::SearchRequest {
        partition,
        ..Default::default()
    };
    let mut supplied = false;
    if *method == Method::POST {
        if let Ok(value) = serde_json::from_slice::<Value>(body) {
            let value = match value {
                Value::String(text) => serde_json::from_str::<Value>(&text).unwrap_or(Value::Null),
                other => other,
            };
            if let Some(object) = value.as_object() {
                supplied = true;
                if let Some(query) = object.get("q").or_else(|| object.get("query")) {
                    request.query = query.as_str().unwrap_or("*:*").to_string();
                }
                if let Some(limit) = object.get("limit").and_then(Value::as_u64) {
                    request.limit = limit as usize;
                }
                if let Some(bookmark) = object.get("bookmark").and_then(Value::as_str) {
                    request.bookmark = Some(bookmark.to_string());
                }
                if let Some(include_docs) = object.get("include_docs").and_then(Value::as_bool) {
                    request.include_docs = include_docs;
                }
                if let Some(counts) = object.get("counts").and_then(Value::as_array) {
                    request.counts = counts
                        .iter()
                        .filter_map(|field| field.as_str().map(str::to_string))
                        .collect();
                }
                if let Some(ranges) = object.get("ranges").and_then(Value::as_object) {
                    request.ranges = ranges.clone();
                }
                if let Some(field) = object.get("group_field").and_then(Value::as_str) {
                    request.group_field = Some(field.to_string());
                }
                if let Some(drilldown) = object.get("drilldown") {
                    request.drilldown = parse_drilldown_value(drilldown);
                }
                if let Some(sort) = object.get("sort") {
                    request.sort = crate::search::parse_sort(sort);
                }
                if let Some(fields) = object.get("highlight_fields") {
                    request.highlight_fields = string_list(fields);
                }
                if let Some(value) = object.get("highlight_pre_tag").and_then(Value::as_str) {
                    request.highlight_pre = value.to_string();
                }
                if let Some(value) = object.get("highlight_post_tag").and_then(Value::as_str) {
                    request.highlight_post = value.to_string();
                }
                if let Some(value) = object.get("highlight_size").and_then(Value::as_u64) {
                    request.highlight_size = value as usize;
                }
                if let Some(value) = object.get("highlight_number").and_then(Value::as_u64) {
                    request.highlight_number = value as usize;
                }
                if let Some(value) = object.get("timeout").and_then(Value::as_u64) {
                    request.timeout_ms = value;
                }
            }
        }
    }
    if !supplied {
        request.query = q
            .get("q")
            .or_else(|| q.get("query"))
            .cloned()
            .unwrap_or_else(|| "*:*".to_string());
        if let Some(limit) = num(q, "limit") {
            request.limit = limit;
        }
        request.bookmark = q.get("bookmark").cloned();
        request.include_docs = flag(q, "include_docs");
        if let Some(raw) = q.get("counts") {
            if let Ok(Value::Array(counts)) = serde_json::from_str::<Value>(raw) {
                request.counts = counts
                    .into_iter()
                    .filter_map(|field| field.as_str().map(str::to_string))
                    .collect();
            }
        }
        if let Some(raw) = q.get("ranges") {
            if let Ok(Value::Object(ranges)) = serde_json::from_str::<Value>(raw) {
                request.ranges = ranges;
            }
        }
        request.group_field = q.get("group_field").cloned();
        for raw in query_values(uri, "drilldown") {
            if let Ok(value) = serde_json::from_str::<Value>(&raw) {
                request.drilldown.extend(parse_drilldown_value(&value));
            }
        }
        if let Some(raw) = q.get("sort") {
            let value = serde_json::from_str::<Value>(raw).unwrap_or(Value::String(raw.clone()));
            request.sort = crate::search::parse_sort(&value);
        }
        if let Some(raw) = q.get("highlight_fields") {
            let value =
                serde_json::from_str::<Value>(raw).unwrap_or_else(|_| Value::String(raw.clone()));
            request.highlight_fields = string_list(&value);
        }
        if let Some(value) = q
            .get("highlight_pre_tag")
            .or_else(|| q.get("highlight_pre"))
        {
            request.highlight_pre = value.clone();
        }
        if let Some(value) = q
            .get("highlight_post_tag")
            .or_else(|| q.get("highlight_post"))
        {
            request.highlight_post = value.clone();
        }
        if let Some(value) = num(q, "highlight_size") {
            request.highlight_size = value;
        }
        if let Some(value) = num(q, "highlight_number") {
            request.highlight_number = value;
        }
        if let Some(value) = num(q, "timeout") {
            request.timeout_ms = value as u64;
        }
    }

    let started = std::time::Instant::now();
    let result = if is_forwarded(st, headers) {
        if let Some(mask) = q.get("shards").and_then(|raw| parse_shard_mask(raw, db.q)) {
            request.owner_shards = Some(mask);
        }
        crate::search::query(db, ddoc, index_name, &request, limits).await
    } else if st.cluster.is_clustered() {
        cluster_search(st, db, dbname, ddoc, index_name, &request, limits, headers).await
    } else {
        crate::search::query(db, ddoc, index_name, &request, limits).await
    };
    if !is_forwarded(st, headers) {
        crate::metrics::record_engine(
            "search",
            started.elapsed().as_secs_f64() * 1000.0,
            result.is_ok(),
        );
    }
    if !still_allowed(st, db, headers) {
        return access_denied(&st.principal(headers));
    }
    match result {
        Ok(value) => json(StatusCode::OK, value),
        Err(error) => fail(error),
    }
}

pub(super) async fn view_request(
    st: &AppState,
    db: &Db,
    dbname: &str,
    ddoc: &str,
    view_name: &str,
    method: &Method,
    q: &HashMap<String, String>,
    body: &[u8],
    headers: &HeaderMap,
    partition: Option<String>,
) -> Response {
    if *method != Method::GET && *method != Method::POST && *method != Method::HEAD {
        return fail(ApiError::new(
            405,
            "method_not_allowed",
            "method not allowed",
        ));
    }

    let build = || -> DbResult<crate::view::ViewQuery> {
        let mut keys = match view_json_param(q, "keys")? {
            Some(Value::Array(keys)) => Some(keys),
            Some(_) => return Err(ApiError::bad_request("`keys` must be a JSON array")),
            None => None,
        };
        if keys.is_none() && *method == Method::POST {
            if let Ok(value) = serde_json::from_slice::<Value>(body) {
                if let Some(array) = value.get("keys").and_then(Value::as_array) {
                    keys = Some(array.clone());
                }
            }
        }
        let group_level = num(q, "group_level");
        if group_level.is_some_and(|level| level > 0)
            && q.get("group").is_some_and(|_| !flag(q, "group"))
        {
            return Err(ApiError::new(
                400,
                "query_parse_error",
                "Can't specify group=false and group_level>0 at the same time",
            ));
        }
        Ok(crate::view::ViewQuery {
            key: view_json_param(q, "key")?,
            keys,
            startkey: view_json_param(q, "startkey")?.or(view_json_param(q, "start_key")?),
            endkey: view_json_param(q, "endkey")?.or(view_json_param(q, "end_key")?),
            startkey_docid: q
                .get("startkey_docid")
                .or_else(|| q.get("start_key_docid"))
                .cloned(),
            endkey_docid: q
                .get("endkey_docid")
                .or_else(|| q.get("end_key_doc_id"))
                .cloned(),
            descending: flag(q, "descending"),
            inclusive_end: q
                .get("inclusive_end")
                .map(|value| value != "false")
                .unwrap_or(true),
            skip: num(q, "skip").unwrap_or(0),
            limit: num(q, "limit").unwrap_or(usize::MAX),
            reduce: q.get("reduce").map(|_| flag(q, "reduce")),
            // CouchDB: an explicit group_level wins over group. With
            // group_level=0 the result is ungrouped even if group=true.
            group: group_level.is_none() && flag(q, "group"),
            group_level: group_level.unwrap_or(0),
            include_docs: flag(q, "include_docs"),
            attachments: flag(q, "attachments"),
            update_seq: flag(q, "update_seq"),
            limits: crate::view::ViewLimits {
                timeout: std::time::Duration::from_millis(st.config.view_timeout_ms),
                memory: st.config.view_memory_mb.saturating_mul(1024 * 1024),
            },
            owner_shards: None,
            shard_count: db.q,
            partition: partition.clone().or_else(|| q.get("partition").cloned()),
            partitioned: db.partitioned,
            total_rows_override: None,
        })
    };

    let mut query = match build() {
        Ok(query) => query,
        Err(error) => return fail(error),
    };
    if is_forwarded(st, headers) {
        if let Some(mask) = q.get("shards").and_then(|raw| parse_shard_mask(raw, db.q)) {
            query.owner_shards = Some(mask);
        } else if flag(q, "owner_only") {
            query.owner_shards = Some(
                (0..db.q)
                    .map(|shard| owner_of_shard(db, shard) == st.cluster.me)
                    .collect(),
            );
        }
    }

    // A global `_count`/`_sum`/`_stats` with no grouping can be reduced
    // independently on each owner and combined, instead of shipping every map
    // row to the coordinator.
    let combine_reduce = if st.cluster.is_clustered() && !is_forwarded(st, headers) {
        match crate::view::load_spec(db, ddoc, view_name).await {
            Ok(spec) => {
                let reduce = query.reduce.unwrap_or(spec.reduce.is_some());
                if reduce
                    && query.key.is_none()
                    && query.keys.is_none()
                    && query.startkey.is_none()
                    && query.endkey.is_none()
                    && !query.descending
                    && spec
                        .reduce
                        .as_deref()
                        .map(|reducer| reducer != "_approx_count_distinct")
                        .unwrap_or(false)
                {
                    spec.reduce
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    } else {
        None
    };

    let started = std::time::Instant::now();
    let result = if let Some(reducer) = combine_reduce.as_deref() {
        cluster_reduce(st, db, dbname, ddoc, view_name, &query, reducer, headers).await
    } else if st.cluster.is_clustered() && !is_forwarded(st, headers) {
        match cluster_view_rows(st, db, dbname, ddoc, view_name, &query, headers).await {
            Ok((spec, rows, docs, total_rows)) => {
                if let Some(total_rows) = total_rows {
                    query.total_rows_override = Some(total_rows);
                }
                crate::view::finish(&spec, rows, docs, &query)
            }
            Err(error) => Err(error),
        }
    } else {
        crate::view::query(db, ddoc, view_name, &query).await
    };
    if !is_forwarded(st, headers) {
        crate::metrics::record_engine(
            "views",
            started.elapsed().as_secs_f64() * 1000.0,
            result.is_ok(),
        );
    }
    if !still_allowed(st, db, headers) {
        return access_denied(&st.principal(headers));
    }

    match result {
        Ok(mut value) => {
            if query.update_seq {
                if let Ok(info) = db.info().await {
                    value["update_seq"] = info["update_seq"].clone();
                }
            }
            json(StatusCode::OK, value)
        }
        Err(error) => fail(error),
    }
}

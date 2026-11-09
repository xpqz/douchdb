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

/// Read the search parameters out of a POSTed JSON body. Returns false when
/// the body carries no object, so the caller falls back to the query string.
fn search_params_from_body(
    request: &mut crate::search::SearchRequest,
    body: &[u8],
) -> DbResult<bool> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(false);
    }
    let value = json_body(body, "invalid UTF-8 JSON")?;
    let value = match value {
        Value::String(text) => serde_json::from_str::<Value>(&text).unwrap_or(Value::Null),
        other => other,
    };
    let Some(object) = value.as_object() else {
        return Ok(false);
    };
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
    Ok(true)
}

/// Read the search parameters out of the query string.
fn search_params_from_query(
    request: &mut crate::search::SearchRequest,
    q: &HashMap<String, String>,
    uri: &Uri,
) {
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
    if let Some(Value::Array(counts)) = lenient_json_param(q, "counts") {
        request.counts = counts
            .into_iter()
            .filter_map(|field| field.as_str().map(str::to_string))
            .collect();
    }
    if let Some(Value::Object(ranges)) = lenient_json_param(q, "ranges") {
        request.ranges = ranges;
    }
    request.group_field = q.get("group_field").cloned();
    for raw in query_values(uri, "drilldown") {
        if let Ok(value) = serde_json::from_str::<Value>(&raw) {
            request.drilldown.extend(parse_drilldown_value(&value));
        }
    }
    if let Some(value) = json_or_string_param(q, "sort") {
        request.sort = crate::search::parse_sort(&value);
    }
    if let Some(value) = json_or_string_param(q, "highlight_fields") {
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

/// Run the search on this node, or across the cluster for a coordinator.
async fn run_search(
    req: DbRequest<'_>,
    index: IndexRef<'_>,
    request: &mut crate::search::SearchRequest,
    limits: crate::view::ViewLimits,
) -> DbResult<Value> {
    let DbRequest {
        st,
        db,
        name: dbname,
        q,
        headers,
        ..
    } = req;
    let IndexRef {
        ddoc,
        name: index_name,
    } = index;
    if st.ctx.cluster.is_internal(headers) {
        if let Some(mask) = q.get("shards").and_then(|raw| parse_shard_mask(raw, db.q)) {
            request.owner_shards = Some(mask);
        }
        crate::search::query(db, ddoc, index_name, request, limits).await
    } else if st.ctx.cluster.is_clustered() {
        cluster_search(
            ClusterRead {
                ctx: &st.ctx,
                db,
                dbname,
                headers,
            },
            IndexRef {
                ddoc,
                name: index_name,
            },
            request,
            limits,
        )
        .await
    } else {
        crate::search::query(db, ddoc, index_name, request, limits).await
    }
}

pub(super) async fn search_request(
    req: DbRequest<'_>,
    index: IndexRef<'_>,
    partition: Option<String>,
    info: bool,
) -> Response {
    let DbRequest {
        st,
        db,
        method,
        uri,
        q,
        headers,
        body,
        ..
    } = req;
    let body: &[u8] = body;
    if let Some(response) = require_method(method, &["GET", "POST", "HEAD"]) {
        return response;
    }
    let limits = crate::view::ViewLimits {
        timeout: std::time::Duration::from_millis(st.config.view_timeout_ms),
        memory: st.config.view_memory_mb.saturating_mul(1024 * 1024),
    };
    if info {
        return match crate::search::info(db, index.ddoc, index.name, limits).await {
            Ok(value) => json(StatusCode::OK, value),
            Err(error) => fail(error),
        };
    }

    let mut request = crate::search::SearchRequest {
        partition,
        ..Default::default()
    };
    let supplied = if *method == Method::POST {
        match search_params_from_body(&mut request, body) {
            Ok(supplied) => supplied,
            Err(error) => return fail(error),
        }
    } else {
        false
    };
    if !supplied {
        search_params_from_query(&mut request, q, uri);
    }

    let started = std::time::Instant::now();
    let result = run_search(req, index, &mut request, limits).await;
    if !st.ctx.cluster.is_internal(headers) {
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

/// Parse the `_view` query parameters (and a POSTed `keys`, `limit` and
/// `skip`) into a view query.
fn parse_view_query(
    req: DbRequest<'_>,
    partition: Option<String>,
) -> DbResult<crate::view::ViewQuery> {
    let DbRequest {
        st,
        method,
        q,
        body,
        ..
    } = req;
    let body: &[u8] = body;
    validate_bool_params(
        q,
        &[
            "descending",
            "include_docs",
            "inclusive_end",
            "update_seq",
            "conflicts",
            "attachments",
            "reduce",
            "group",
        ],
    )?;
    let mut keys = match json_param(q, "keys")? {
        Some(Value::Array(keys)) => Some(keys),
        Some(_) => return Err(ApiError::bad_request("`keys` must be a JSON array")),
        None => None,
    };
    let mut limit = uint_param(q, "limit")?;
    let mut skip = uint_param(q, "skip")?;
    if *method == Method::POST {
        let value = body_object(body)?;
        if keys.is_none() {
            match value.get("keys") {
                Some(Value::Array(array)) => keys = Some(array.clone()),
                Some(_) => return Err(ApiError::bad_request("`keys` member must be an array.")),
                None => {}
            }
        }
        let (body_limit, body_skip) = (uint_member(&value, "limit")?, uint_member(&value, "skip")?);
        limit = limit.or(body_limit);
        skip = skip.or(body_skip);
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
        key: json_param(q, "key")?,
        keys,
        startkey: json_param(q, "startkey")?.or(json_param(q, "start_key")?),
        endkey: json_param(q, "endkey")?.or(json_param(q, "end_key")?),
        startkey_docid: q
            .get("startkey_docid")
            .or_else(|| q.get("start_key_docid"))
            .cloned(),
        endkey_docid: q
            .get("endkey_docid")
            .or_else(|| q.get("end_key_doc_id"))
            .cloned(),
        descending: flag(q, "descending"),
        inclusive_end: !q
            .get("inclusive_end")
            .is_some_and(|value| value.eq_ignore_ascii_case("false")),
        skip: skip.unwrap_or(0),
        limit: limit.unwrap_or(usize::MAX),
        reduce: q.get("reduce").map(|_| flag(q, "reduce")),
        // CouchDB: an explicit group_level wins over group. With
        // group_level=0 the result is ungrouped even if group=true.
        group: group_level.is_none() && flag(q, "group"),
        group_level: group_level.unwrap_or(0),
        include_docs: flag(q, "include_docs"),
        update_seq: flag(q, "update_seq"),
        limits: crate::view::ViewLimits {
            timeout: std::time::Duration::from_millis(st.config.view_timeout_ms),
            memory: st.config.view_memory_mb.saturating_mul(1024 * 1024),
        },
        owner_shards: None,
        partition: partition.or_else(|| q.get("partition").cloned()),
        total_rows_override: None,
        offset_override: None,
    })
}

/// A forwarded view read covers only the shards its coordinator names (or the
/// ones this node owns).
fn restrict_to_owned_shards(req: DbRequest<'_>, query: &mut crate::view::ViewQuery) {
    let DbRequest { st, db, q, .. } = req;
    if let Some(mask) = q.get("shards").and_then(|raw| parse_shard_mask(raw, db.q)) {
        query.owner_shards = Some(mask);
    } else if flag(q, "owner_only") {
        query.owner_shards = Some(
            (0..db.q)
                .map(|shard| db.owner_of_shard(shard) == st.ctx.cluster.me)
                .collect::<ShardMask>(),
        );
    }
}

/// The reduce function a coordinator can ask the owners to run. Owners reduce
/// their own shards and the coordinator combines the partials, instead of
/// shipping every map row.
async fn combinable_reducer(
    req: DbRequest<'_>,
    view: IndexRef<'_>,
    query: &crate::view::ViewQuery,
) -> Option<String> {
    let DbRequest {
        st, db, headers, ..
    } = req;
    if !st.ctx.cluster.is_clustered() || st.ctx.cluster.is_internal(headers) {
        return None;
    }
    let spec = crate::view::load_spec(db, view.ddoc, view.name)
        .await
        .ok()?;
    let reduce = query.reduce.unwrap_or(spec.reduce.is_some());
    if reduce {
        spec.reduce
    } else {
        None
    }
}

/// Read the view on this node, or across the cluster for a coordinator.
async fn run_view(
    req: DbRequest<'_>,
    view: IndexRef<'_>,
    query: &mut crate::view::ViewQuery,
    reducer: Option<&str>,
) -> DbResult<Value> {
    let DbRequest {
        st,
        db,
        name: dbname,
        headers,
        ..
    } = req;
    let IndexRef {
        ddoc,
        name: view_name,
    } = view;
    let read = ClusterRead {
        ctx: &st.ctx,
        db,
        dbname,
        headers,
    };
    if let Some(reducer) = reducer {
        cluster_reduce(read, view, query, reducer).await
    } else if st.ctx.cluster.is_clustered() && !st.ctx.cluster.is_internal(headers) {
        let merged = cluster_view_rows(read, view, query).await?;
        query.total_rows_override = Some(merged.total_rows);
        query.offset_override = Some(merged.offset);
        crate::view::finish(&merged.spec, merged.rows, merged.docs, query).await
    } else {
        crate::view::query(db, ddoc, view_name, query).await
    }
}

pub(super) async fn view_request(
    req: DbRequest<'_>,
    view: IndexRef<'_>,
    partition: Option<String>,
) -> Response {
    let DbRequest {
        st,
        db,
        name: dbname,
        method,
        headers,
        ..
    } = req;
    if let Some(response) = require_method(method, &["GET", "POST", "HEAD"]) {
        return response;
    }

    let mut query = match parse_view_query(req, partition) {
        Ok(query) => query,
        Err(error) => return fail(error),
    };
    if st.ctx.cluster.is_internal(headers) {
        restrict_to_owned_shards(req, &mut query);
    }
    let combine_reduce = combinable_reducer(req, view, &query).await;

    // Taken before any index catch-up or row read, on this node and on the
    // owners it asks: a client that follows `_changes` from this token then
    // sees every write the rows missed. A token taken after could include a
    // write the rows do not show, which the client would never see.
    let update_seq = if query.update_seq {
        match query_update_seq(st, db, dbname, headers).await {
            Ok(update_seq) => Some(update_seq),
            Err(error) => return fail(error),
        }
    } else {
        None
    };
    let started = std::time::Instant::now();
    let result = run_view(req, view, &mut query, combine_reduce.as_deref()).await;
    if !st.ctx.cluster.is_internal(headers) {
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
            if let Some(update_seq) = update_seq {
                value["update_seq"] = update_seq;
            }
            json(StatusCode::OK, value)
        }
        Err(error) => fail(error),
    }
}

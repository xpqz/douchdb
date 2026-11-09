//! Request-parameter parsing shared by the HTTP layer and the cluster code
//! that replays a client's request against its peers.

use crate::db::{ApiError, ChangeFilter, Db, DbResult, DocOpts};
use serde_json::{Map, Value};
use std::collections::HashMap;

pub(crate) fn flag(q: &HashMap<String, String>, key: &str) -> bool {
    match q.get(key) {
        Some(raw) => raw == "1" || raw.eq_ignore_ascii_case("true"),
        None => false,
    }
}

pub(crate) fn num(q: &HashMap<String, String>, key: &str) -> Option<usize> {
    q.get(key).and_then(|v| v.parse().ok())
}

pub(crate) fn doc_opts(q: &HashMap<String, String>) -> DocOpts {
    DocOpts {
        rev: q.get("rev").cloned(),
        revs: flag(q, "revs"),
        revs_info: flag(q, "revs_info"),
        local_seq: flag(q, "local_seq"),
        conflicts: flag(q, "conflicts"),
        deleted_conflicts: flag(q, "deleted_conflicts"),
        open_revs: q
            .get("open_revs")
            .map(|v| serde_json::from_str::<Vec<String>>(v).unwrap_or_else(|_| vec![v.clone()])),
        latest: flag(q, "latest"),
        ..Default::default()
    }
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

/// A query parameter that must hold JSON, as CouchDB's `key`, `startkey` and
/// `keys` do: absent is `None`, and anything that is not JSON is a 400
/// (`key=abc` must be written `key="abc"`).
pub(crate) fn json_param(q: &HashMap<String, String>, name: &str) -> DbResult<Option<Value>> {
    match q.get(name) {
        Some(raw) => serde_json::from_str::<Value>(raw)
            .map(Some)
            .map_err(|_| ApiError::bad_request("invalid UTF-8 JSON")),
        None => Ok(None),
    }
}

/// A query parameter that may hold JSON but is ignored when it does not.
pub(crate) fn lenient_json_param(q: &HashMap<String, String>, name: &str) -> Option<Value> {
    q.get(name)
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
}

/// A query parameter read as JSON when it parses, else as the plain string.
pub(crate) fn json_or_string_param(q: &HashMap<String, String>, name: &str) -> Option<Value> {
    q.get(name).map(|raw| {
        serde_json::from_str::<Value>(raw).unwrap_or_else(|_| Value::String(raw.clone()))
    })
}

/// A request body that must be JSON; `reason` is the 400 when it is not.
pub(crate) fn json_body(body: &[u8], reason: &str) -> DbResult<Value> {
    serde_json::from_slice::<Value>(body).map_err(|_| ApiError::bad_request(reason))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn query(name: &str, raw: &str) -> HashMap<String, String> {
        HashMap::from([(name.to_string(), raw.to_string())])
    }

    #[test]
    fn json_param_distinguishes_absent_valid_and_malformed() {
        assert_eq!(json_param(&HashMap::new(), "key").unwrap(), None);
        assert_eq!(
            json_param(&query("key", "\"abc\""), "key").unwrap(),
            Some(json!("abc"))
        );
        let error = json_param(&query("key", "abc"), "key").unwrap_err();
        assert_eq!(
            (error.status, error.reason.as_str()),
            (400, "invalid UTF-8 JSON")
        );
    }

    #[test]
    fn lenient_readers_ignore_or_keep_malformed_values() {
        assert_eq!(
            lenient_json_param(&query("counts", "[1]"), "counts"),
            Some(json!([1]))
        );
        assert_eq!(lenient_json_param(&query("counts", "x"), "counts"), None);
        assert_eq!(
            json_or_string_param(&query("sort", "x"), "sort"),
            Some(json!("x"))
        );
        assert_eq!(
            json_or_string_param(&query("sort", "[\"a\"]"), "sort"),
            Some(json!(["a"]))
        );
        assert_eq!(json_or_string_param(&HashMap::new(), "sort"), None);
    }

    #[test]
    fn json_body_reports_the_given_reason() {
        assert_eq!(json_body(b"{}", "bad").unwrap(), json!({}));
        assert_eq!(json_body(b"{", "bad").unwrap_err().reason, "bad");
    }
}

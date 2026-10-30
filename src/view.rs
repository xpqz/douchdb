//! CouchDB map/reduce views, evaluated with an embedded QuickJS runtime.
//!
//! Views are computed on demand from the winner of every document. There is no
//! incremental index yet; the JS engine sees the same `doc`/`emit` environment
//! as CouchDB's JavaScript view server.

use crate::db::{AllDocsOpts, ApiError, Db, DbResult, DocOpts};
use serde_json::{json, Map, Value};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Default)]
pub struct ViewQuery {
    pub key: Option<Value>,
    pub keys: Option<Vec<Value>>,
    pub startkey: Option<Value>,
    pub endkey: Option<Value>,
    pub startkey_docid: Option<String>,
    pub endkey_docid: Option<String>,
    pub descending: bool,
    pub inclusive_end: bool,
    pub skip: usize,
    pub limit: usize,
    pub reduce: Option<bool>,
    pub group: bool,
    pub group_level: usize,
    pub include_docs: bool,
    pub attachments: bool,
    pub update_seq: bool,
}

#[derive(Clone, Debug)]
pub struct ViewSpec {
    pub map: String,
    pub reduce: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Row {
    pub id: String,
    pub key: Value,
    pub value: Value,
}

/// Run one view query against the local shards.
pub async fn query(db: &Db, ddoc_id: &str, view_name: &str, query: &ViewQuery) -> DbResult<Value> {
    let (spec, rows, docs) = local_map(db, ddoc_id, view_name, query.attachments).await?;
    finish(&spec, rows, docs, query)
}

/// Local map rows plus the documents they came from, for cluster merging.
pub async fn local_map(
    db: &Db,
    ddoc_id: &str,
    view_name: &str,
    attachments: bool,
) -> DbResult<(ViewSpec, Vec<Row>, HashMap<String, Value>)> {
    let spec = load_spec(db, ddoc_id, view_name).await?;
    let (rows, docs) = map_local(db, &spec.map, attachments).await?;
    Ok((spec, rows, docs))
}

pub fn finish(
    spec: &ViewSpec,
    rows: Vec<Row>,
    docs: HashMap<String, Value>,
    query: &ViewQuery,
) -> DbResult<Value> {
    render(spec, rows, docs, query)
}

async fn load_spec(db: &Db, ddoc_id: &str, view_name: &str) -> DbResult<ViewSpec> {
    let ddoc = db.get_doc(ddoc_id, &DocOpts::default()).await?;
    spec_from_doc(&ddoc, view_name)
}

pub fn spec_from_doc(ddoc: &Value, view_name: &str) -> DbResult<ViewSpec> {
    let language = ddoc
        .get("language")
        .and_then(Value::as_str)
        .unwrap_or("javascript");
    if language != "javascript" {
        return Err(ApiError::bad_request(format!(
            "View language must be javascript, not {language}"
        )));
    }
    let spec = ddoc
        .get("views")
        .and_then(Value::as_object)
        .and_then(|views| views.get(view_name))
        .ok_or_else(|| ApiError::not_found(format!("missing view {view_name}")))?;
    let map = spec
        .get("map")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::bad_request("View is missing a map function"))?
        .to_string();
    let reduce = spec
        .get("reduce")
        .and_then(Value::as_str)
        .map(str::to_string);
    Ok(ViewSpec { map, reduce })
}

/// Build the map index from every live document's winning revision.
async fn map_local(
    db: &Db,
    map_source: &str,
    attachments: bool,
) -> DbResult<(Vec<Row>, HashMap<String, Value>)> {
    let all = db
        .all_docs(&AllDocsOpts {
            include_docs: true,
            attachments,
            limit: usize::MAX,
            ..Default::default()
        })
        .await?;

    let mut docs = Vec::new();
    let mut by_id = HashMap::new();
    for row in all["rows"].as_array().cloned().unwrap_or_default() {
        let Some(id) = row.get("id").and_then(Value::as_str).map(str::to_string) else {
            continue;
        };
        if id.starts_with("_design/") || id.starts_with("_local/") {
            continue;
        }
        let Some(doc) = row.get("doc").cloned() else {
            continue;
        };
        by_id.insert(id.clone(), doc.clone());
        docs.push((id, doc));
    }

    let rows = run_map(map_source, &docs).map_err(|reason| {
        ApiError::new(500, "view_error", format!("map function failed: {reason}"))
    })?;
    Ok((rows, by_id))
}

fn engine() -> Result<rquickjs::Runtime, String> {
    let runtime = rquickjs::Runtime::new().map_err(|error| error.to_string())?;
    runtime.set_memory_limit(64 * 1024 * 1024);
    runtime.set_max_stack_size(512 * 1024);
    // A view is arbitrary user code. Stop runaway loops instead of hanging the
    // request thread forever.
    let deadline = Instant::now() + Duration::from_secs(5);
    runtime.set_interrupt_handler(Some(Box::new(move || Instant::now() > deadline)));
    Ok(runtime)
}

fn run_map(source: &str, docs: &[(String, Value)]) -> Result<Vec<Row>, String> {
    let runtime = engine()?;
    let context = rquickjs::Context::full(&runtime).map_err(|error| error.to_string())?;

    context.with(|ctx| {
        let wrapped = format!(
            r#"
            (function () {{
              function sum(values) {{
                var total = 0;
                for (var i = 0; i < values.length; i++) total += values[i];
                return total;
              }}
              function log(message) {{ }}
              function isArray(value) {{ return Array.isArray(value); }}
              function toJSON(value) {{ return JSON.stringify(value); }}
              var __out = [];
              var emit = function (key, value) {{
                __out.push([
                  key === undefined ? null : key,
                  value === undefined ? null : value
                ]);
              }};
              var __map = ({source});
              if (typeof __map !== "function") {{
                throw new Error("map must be a function");
              }}
              return function (json) {{
                var doc = JSON.parse(json);
                __out = [];
                __map(doc);
                return JSON.stringify(__out);
              }};
            }})()
            "#
        );
        let func: rquickjs::Function = ctx
            .eval(wrapped)
            .map_err(|error| format!("compile: {error}"))?;
        let mut rows = Vec::new();
        for (id, doc) in docs {
            let input = serde_json::to_string(doc).map_err(|error| error.to_string())?;
            let output: String = func
                .call((input,))
                .map_err(|error| format!("{id}: {error}"))?;
            let emitted: Value =
                serde_json::from_str(&output).map_err(|error| format!("{id}: {error}"))?;
            for pair in emitted.as_array().cloned().unwrap_or_default() {
                let mut pair = pair.as_array().cloned().unwrap_or_default();
                if pair.len() != 2 {
                    continue;
                }
                let value = pair.pop().unwrap_or(Value::Null);
                let key = pair.pop().unwrap_or(Value::Null);
                rows.push(Row {
                    id: id.clone(),
                    key,
                    value,
                });
            }
        }
        Ok(rows)
    })
}

fn render(
    spec: &ViewSpec,
    mut rows: Vec<Row>,
    docs: HashMap<String, Value>,
    query: &ViewQuery,
) -> DbResult<Value> {
    let total_rows = rows.len();
    rows.sort_by(|a, b| cmp_json(&a.key, &b.key).then_with(|| a.id.cmp(&b.id)));
    if query.descending {
        rows.reverse();
    }

    if let Some(keys) = &query.keys {
        let mut selected = Vec::new();
        for key in keys {
            for row in rows
                .iter()
                .filter(|row| cmp_json(&row.key, key) == Ordering::Equal)
            {
                selected.push(row.clone());
            }
        }
        rows = selected;
    } else if let Some(key) = &query.key {
        rows.retain(|row| cmp_json(&row.key, key) == Ordering::Equal);
    }

    let before_start = count_before_start(&rows, query);
    rows.retain(|row| in_range(row, query));
    let reduce = query.reduce.unwrap_or(spec.reduce.is_some());

    if reduce {
        let Some(reducer) = &spec.reduce else {
            return Err(ApiError::new(
                400,
                "query_parse_error",
                "Reduce is invalid for map-only views",
            ));
        };
        let groups = group_rows(rows, query);
        let mut out = Vec::new();
        for (key, members) in groups {
            let keys: Vec<Value> = members.iter().map(|row| json!([row.id, row.key])).collect();
            let values: Vec<Value> = members.iter().map(|row| row.value.clone()).collect();
            let value = match reduce_value(reducer, &keys, &values) {
                Ok(value) => value,
                Err(reason) => {
                    return Err(ApiError::new(
                        500,
                        "view_error",
                        format!("reduce function failed: {reason}"),
                    ))
                }
            };
            out.push(json!({"key": key, "value": value}));
        }
        return Ok(json!({"rows": out}));
    }

    let skip = query.skip.min(rows.len());
    let take = query.limit;
    let selected: Vec<Row> = rows.into_iter().skip(skip).take(take).collect();
    let rows: Vec<Value> = selected
        .into_iter()
        .map(|row| {
            let mut entry = json!({
                "id": row.id,
                "key": row.key,
                "value": row.value,
            });
            if query.include_docs {
                if let Some(doc) = docs.get(&row.id) {
                    entry["doc"] = doc.clone();
                }
            }
            entry
        })
        .collect();
    Ok(json!({
        "total_rows": total_rows,
        "offset": before_start + skip,
        "rows": rows,
    }))
}

fn count_before_start(rows: &[Row], query: &ViewQuery) -> usize {
    let Some(start) = &query.startkey else {
        return 0;
    };
    rows.iter()
        .filter(|row| match cmp_json(&row.key, start) {
            Ordering::Less => !query.descending,
            Ordering::Greater => query.descending,
            Ordering::Equal => match &query.startkey_docid {
                Some(id) if query.descending => row.id > *id,
                Some(id) => row.id < *id,
                None => false,
            },
        })
        .count()
}

fn in_range(row: &Row, query: &ViewQuery) -> bool {
    if let Some(start) = &query.startkey {
        let order = cmp_json(&row.key, start);
        let after = match order {
            Ordering::Greater => true,
            Ordering::Less => false,
            Ordering::Equal => match &query.startkey_docid {
                Some(id) if query.descending => row.id <= *id,
                Some(id) => row.id >= *id,
                None => true,
            },
        };
        if after == query.descending {
            return false;
        }
    }
    if let Some(end) = &query.endkey {
        let order = cmp_json(&row.key, end);
        let before = match order {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => {
                if !query.inclusive_end {
                    false
                } else {
                    match &query.endkey_docid {
                        Some(id) if query.descending => row.id >= *id,
                        Some(id) => row.id <= *id,
                        None => true,
                    }
                }
            }
        };
        if before == query.descending {
            return false;
        }
    }
    true
}

fn group_rows(rows: Vec<Row>, query: &ViewQuery) -> Vec<(Value, Vec<Row>)> {
    if !query.group && query.group_level == 0 {
        return vec![(Value::Null, rows)];
    }
    let mut groups: Vec<(Value, Vec<Row>)> = Vec::new();
    for row in rows {
        let key = group_key(&row.key, query.group_level);
        match groups.last_mut() {
            Some((existing, members)) if cmp_json(existing, &key) == Ordering::Equal => {
                members.push(row);
            }
            _ => groups.push((key, vec![row])),
        }
    }
    groups
}

fn group_key(key: &Value, level: usize) -> Value {
    if level == 0 {
        return key.clone();
    }
    match key {
        Value::Array(items) => Value::Array(items.iter().take(level).cloned().collect()),
        other => other.clone(),
    }
}

fn reduce_value(source: &str, keys: &[Value], values: &[Value]) -> Result<Value, String> {
    match source {
        "_count" => return Ok(json!(values.len())),
        "_sum" => {
            let mut sum = 0.0;
            for value in values {
                let number = value
                    .as_f64()
                    .ok_or_else(|| format!("_sum requires numbers, got {value}"))?;
                sum += number;
            }
            return Ok(number_value(sum));
        }
        "_stats" => return stats(values),
        "_approx_count_distinct" => {
            let mut distinct = std::collections::HashSet::new();
            for value in values {
                distinct.insert(serde_json::to_string(value).map_err(|error| error.to_string())?);
            }
            return Ok(json!(distinct.len()));
        }
        _ => {}
    }

    let runtime = engine()?;
    let context = rquickjs::Context::full(&runtime).map_err(|error| error.to_string())?;
    context.with(|ctx| {
        let wrapped = format!(
            r#"
            (function () {{
              function sum(values) {{
                var total = 0;
                for (var i = 0; i < values.length; i++) total += values[i];
                return total;
              }}
              function log(message) {{ }}
              function isArray(value) {{ return Array.isArray(value); }}
              function toJSON(value) {{ return JSON.stringify(value); }}
              var __reduce = ({source});
              if (typeof __reduce !== "function") {{
                throw new Error("reduce must be a function");
              }}
              return function (keys, values) {{
                var result = __reduce(JSON.parse(keys), JSON.parse(values), false);
                return result === undefined ? "null" : JSON.stringify(result);
              }};
            }})()
            "#
        );
        let func: rquickjs::Function = ctx
            .eval(wrapped)
            .map_err(|error| format!("compile: {error}"))?;
        let keys = serde_json::to_string(keys).map_err(|error| error.to_string())?;
        let values = serde_json::to_string(values).map_err(|error| error.to_string())?;
        let output: String = func
            .call((keys, values))
            .map_err(|error| error.to_string())?;
        serde_json::from_str(&output).map_err(|error| error.to_string())
    })
}

fn stats(values: &[Value]) -> Result<Value, String> {
    if values.is_empty() {
        return Ok(json!({
            "sum": 0, "count": 0, "min": null, "max": null, "sumsqr": 0
        }));
    }
    let mut sum = 0.0;
    let mut sumsqr = 0.0;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for value in values {
        let number = value
            .as_f64()
            .ok_or_else(|| format!("_stats requires numbers, got {value}"))?;
        sum += number;
        sumsqr += number * number;
        min = min.min(number);
        max = max.max(number);
    }
    Ok(json!({
        "sum": number_value(sum),
        "count": values.len(),
        "min": number_value(min),
        "max": number_value(max),
        "sumsqr": number_value(sumsqr),
    }))
}

fn number_value(value: f64) -> Value {
    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        json!(value as i64)
    } else {
        json!(value)
    }
}

/// CouchDB JSON collation: null < false < true < number < string < array <
/// object. Strings compare by Unicode scalar value rather than ICU collation,
/// which matches ASCII exactly and is documented as a limitation.
pub fn cmp_json(a: &Value, b: &Value) -> Ordering {
    fn rank(value: &Value) -> u8 {
        match value {
            Value::Null => 0,
            Value::Bool(false) => 1,
            Value::Bool(true) => 2,
            Value::Number(_) => 3,
            Value::String(_) => 4,
            Value::Array(_) => 5,
            Value::Object(_) => 6,
        }
    }

    let (ra, rb) = (rank(a), rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Number(a), Value::Number(b)) => {
            let (a, b) = (a.as_f64().unwrap_or(0.0), b.as_f64().unwrap_or(0.0));
            a.partial_cmp(&b).unwrap_or(Ordering::Equal)
        }
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Array(a), Value::Array(b)) => {
            for (left, right) in a.iter().zip(b.iter()) {
                let order = cmp_json(left, right);
                if order != Ordering::Equal {
                    return order;
                }
            }
            a.len().cmp(&b.len())
        }
        (Value::Object(a), Value::Object(b)) => {
            let a = sorted_entries(a);
            let b = sorted_entries(b);
            for (left, right) in a.iter().zip(b.iter()) {
                let order = left.0.cmp(right.0).then_with(|| cmp_json(left.1, right.1));
                if order != Ordering::Equal {
                    return order;
                }
            }
            a.len().cmp(&b.len())
        }
        _ => Ordering::Equal,
    }
}

fn sorted_entries(map: &Map<String, Value>) -> Vec<(&String, &Value)> {
    let mut entries: Vec<_> = map.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn couchdb_collation_orders_every_json_type() {
        let ordered = vec![
            Value::Null,
            json!(false),
            json!(true),
            json!(-1),
            json!(2.5),
            json!("a"),
            json!(["a"]),
            json!({"a": 1}),
        ];
        for pair in ordered.windows(2) {
            assert_eq!(cmp_json(&pair[0], &pair[1]), Ordering::Less);
        }
    }

    #[test]
    fn map_emits_keys_and_values() {
        let docs = vec![
            (
                "a".to_string(),
                json!({"_id": "a", "city": "Berlin", "n": 2}),
            ),
            (
                "b".to_string(),
                json!({"_id": "b", "city": "Berlin", "n": 3}),
            ),
        ];
        let rows = run_map("function(doc){ emit(doc.city, doc.n); }", &docs).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key, json!("Berlin"));
        assert_eq!(rows[1].value, json!(3));
    }

    #[test]
    fn builtin_reduces_work() {
        assert_eq!(
            reduce_value("_count", &[], &[json!(1), json!(2)]).unwrap(),
            json!(2)
        );
        assert_eq!(
            reduce_value("_sum", &[], &[json!(1), json!(2.5)]).unwrap(),
            json!(3.5)
        );
        let stats = reduce_value("_stats", &[], &[json!(1), json!(3)]).unwrap();
        assert_eq!(stats["count"], 2);
        assert_eq!(stats["min"], 1);
        assert_eq!(stats["max"], 3);
    }
}

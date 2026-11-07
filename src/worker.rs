//! Request dispatch for the sandboxed `--js-worker` process. This is the one
//! place that knows the view and search engines, so `js` itself need not.

use crate::js::ViewLimits;
use serde_json::{json, Value};
use std::time::Instant;

/// Serve the worker protocol on stdin/stdout until EOF.
pub fn run_stdio(landlock: &str) -> Result<(), crate::js::JsError> {
    crate::js::run_worker_stdio(landlock, handle_request)
}

pub fn handle_request(request: Value) -> Value {
    let limits = ViewLimits {
        timeout: std::time::Duration::from_millis(
            request
                .get("timeout_ms")
                .and_then(Value::as_u64)
                .unwrap_or(5_000),
        ),
        memory: request
            .get("memory")
            .and_then(Value::as_u64)
            .unwrap_or(64 * 1024 * 1024) as usize,
    };
    let source = match request.get("source").and_then(Value::as_str) {
        Some(source) => source,
        None => return json!({"ok": false, "error": "missing source"}),
    };
    let started = Instant::now();
    let result = match request.get("kind").and_then(Value::as_str) {
        Some("map") => {
            let docs: Vec<(String, String)> =
                match serde_json::from_value(request.get("docs").cloned().unwrap_or_default()) {
                    Ok(docs) => docs,
                    Err(error) => return json!({"ok": false, "error": error.to_string()}),
                };
            crate::view::run_map_json(source, &docs, limits).map(|rows| json!(rows))
        }
        Some("reduce") => match reduce_call(&request) {
            Ok((keys, values, rereduce)) => {
                crate::view::run_reduce(source, &keys, &values, rereduce, limits)
            }
            Err(error) => return json!({"ok": false, "error": error}),
        },
        Some("reduce_many") => return reduce_many(source, &request, limits),
        Some("index") => {
            let docs: Vec<(String, String)> =
                match serde_json::from_value(request.get("docs").cloned().unwrap_or_default()) {
                    Ok(docs) => docs,
                    Err(error) => return json!({"ok": false, "error": error.to_string()}),
                };
            crate::search::run_index_json(source, &docs, limits).map(|docs| json!(docs))
        }
        _ => return json!({"ok": false, "error": "unknown request kind"}),
    };
    respond(result, started, limits)
}

fn respond(
    result: Result<Value, crate::js::JsError>,
    started: Instant,
    limits: ViewLimits,
) -> Value {
    let result = match result {
        Err(error) if started.elapsed() >= limits.timeout => Err(format!(
            "timed out after {} ms: {error}",
            limits.timeout.as_millis()
        )),
        other => other.map_err(|error| error.to_string()),
    };
    match result {
        Ok(value) => json!({"ok": true, "value": value}),
        Err(error) => json!({"ok": false, "error": error}),
    }
}

/// The keys, values and rereduce flag of one reduce call. A rereduce carries
/// null keys (CouchDB passes null, not []).
fn reduce_call(call: &Value) -> Result<(Vec<Value>, Vec<Value>, bool), String> {
    let keys: Vec<Value> = match call.get("keys") {
        None | Some(Value::Null) => Vec::new(),
        Some(keys) => {
            serde_json::from_value(keys.clone()).map_err(|error| format!("reduce keys: {error}"))?
        }
    };
    let values: Vec<Value> = match call.get("values") {
        Some(values) => serde_json::from_value(values.clone())
            .map_err(|error| format!("reduce values: {error}"))?,
        None => return Err("reduce values are missing".to_string()),
    };
    let rereduce = call
        .get("rereduce")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok((keys, values, rereduce))
}

/// Run the calls of a batched reduce in order against one compiled reducer.
/// Each call has the full timeout; the first failure ends the batch.
fn reduce_many(source: &str, request: &Value, limits: ViewLimits) -> Value {
    let Some(calls) = request.get("calls").and_then(Value::as_array) else {
        return json!({"ok": false, "error": "reduce calls are missing"});
    };
    let mut reducer = crate::view::JsReducer::new(source, limits);
    let mut results = Vec::with_capacity(calls.len());
    for call in calls {
        let (keys, values, rereduce) = match reduce_call(call) {
            Ok(call) => call,
            Err(error) => return json!({"ok": false, "error": error}),
        };
        let started = Instant::now();
        match reducer.call(&keys, &values, rereduce) {
            Ok(value) => results.push(value),
            Err(error) => return respond(Err(error), started, limits),
        }
    }
    json!({"ok": true, "value": results})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_reduce_rejects_malformed_keys_and_values() {
        let reduce = |keys: Value, values: Value, rereduce: bool| {
            handle_request(json!({
                "kind": "reduce",
                "source": "function(keys, values, rereduce){ return sum(values); }",
                "keys": keys,
                "values": values,
                "rereduce": rereduce,
            }))
        };
        let ok = reduce(json!([[1, "a"]]), json!([3]), false);
        assert_eq!(ok["ok"], true, "{ok}");
        assert_eq!(ok["value"], 3);
        // A rereduce carries null keys.
        let ok = reduce(Value::Null, json!([1, 2]), true);
        assert_eq!(ok["ok"], true, "{ok}");
        assert_eq!(ok["value"], 3);

        for (keys, values) in [
            (json!("nope"), json!([1])),
            (json!({"a": 1}), json!([1])),
            (json!([]), json!("nope")),
            (json!([]), Value::Null),
            (json!([[1, "a"]]), json!({"a": 1})),
        ] {
            let bad = reduce(keys.clone(), values.clone(), false);
            assert_eq!(bad["ok"], false, "keys {keys} values {values}: {bad}");
            assert!(bad["error"].is_string(), "{bad}");
        }
        let missing =
            handle_request(json!({"kind": "reduce", "source": "function(k, v, r){ return 1; }"}));
        assert_eq!(missing["ok"], false, "{missing}");
    }

    #[test]
    fn worker_reduce_many_runs_every_call_in_one_request() {
        let many = |calls: Value| {
            handle_request(json!({
                "kind": "reduce_many",
                "source": "function(keys, values, rereduce){ return rereduce ? sum(values) * 10 : sum(values); }",
                "calls": calls,
            }))
        };
        let ok = many(json!([
            {"keys": [["a", 1]], "values": [3], "rereduce": false},
            {"keys": [["b", 2], ["c", 3]], "values": [4, 5], "rereduce": false},
            {"keys": null, "values": [1, 2], "rereduce": true},
        ]));
        assert_eq!(ok["ok"], true, "{ok}");
        assert_eq!(ok["value"], json!([3, 9, 30]));

        // The first failing call fails the request, as a sequence of single
        // calls would.
        let bad = many(json!([
            {"keys": [], "values": [1], "rereduce": false},
            {"keys": "nope", "values": [1], "rereduce": false},
        ]));
        assert_eq!(bad["ok"], false, "{bad}");
        assert!(bad["error"].is_string(), "{bad}");
        let empty = many(json!([]));
        assert_eq!(empty["value"], json!([]), "{empty}");
    }
}

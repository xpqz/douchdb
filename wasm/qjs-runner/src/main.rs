use rquickjs::{Context, Function, Runtime};
use serde_json::{json, Value};
use std::cell::Cell;
use std::io::Read;
use std::rc::Rc;
use std::time::{Duration, Instant};

fn main() {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input).unwrap();
    let response = match run(&input) {
        Ok(value) => json!({"ok": true, "value": value}),
        Err(error) => json!({"ok": false, "error": error}),
    };
    println!("{}", response);
}

fn run(input: &str) -> Result<Value, String> {
    let request: Value = serde_json::from_str(input).map_err(|e| e.to_string())?;
    let source = request.get("source").and_then(Value::as_str).ok_or("missing source")?;
    let kind = request.get("kind").and_then(Value::as_str).ok_or("missing kind")?;
    // The wrappers are shared with the native engine (wasm/js); the user's
    // function replaces the placeholder.
    let wrap = |template: &str| template.replace("__DOUCHDB_SOURCE__", source);
    if kind == "reduce_many" {
        let timeout = request.get("timeout_ms").and_then(Value::as_u64).map(Duration::from_millis);
        return reduce_many(&request, wrap(include_str!("../../js/reduce.js")), timeout);
    }
    let mut ids: Vec<String> = Vec::new();
    let (script, argument) = match kind {
        "map" | "index" => {
            let docs = request.get("docs").and_then(Value::as_array).ok_or("missing docs")?;
            let mut raw = String::from("[");
            for (index, doc) in docs.iter().enumerate() {
                if index > 0 { raw.push(','); }
                // Row ids come from the host, not from the document object the
                // user's function was handed (it may change or delete _id).
                ids.push(doc.get(0).and_then(Value::as_str).ok_or("bad doc")?.to_string());
                raw.push_str(doc.get(1).and_then(Value::as_str).ok_or("bad doc")?);
            }
            raw.push(']');
            let template = if kind == "map" { include_str!("../../js/map.js") } else { include_str!("../../js/index.js") };
            (wrap(template), raw)
        }
        "reduce" => {
            // Same checks and messages as the process worker (src/js.rs): a
            // rereduce carries null keys, everything else must be an array.
            if let Some(keys) = request.get("keys").filter(|keys| !keys.is_null()) {
                serde_json::from_value::<Vec<Value>>(keys.clone())
                    .map_err(|error| format!("reduce keys: {error}"))?;
            }
            match request.get("values") {
                Some(values) => {
                    serde_json::from_value::<Vec<Value>>(values.clone())
                        .map_err(|error| format!("reduce values: {error}"))?;
                }
                None => return Err("reduce values are missing".to_string()),
            }
            let keys = serde_json::to_string(&request.get("keys").cloned().unwrap_or(Value::Null)).unwrap();
            let values = serde_json::to_string(&request.get("values").cloned().unwrap_or(Value::Null)).unwrap();
            let rereduce = request.get("rereduce").and_then(Value::as_bool).unwrap_or(false);
            let script = wrap(include_str!("../../js/reduce.js"));
            (script, format!("[{keys},{values},{rereduce}]"))
        }
        other => return Err(format!("unknown kind {other}")),
    };
    let runtime = Runtime::new().map_err(|e| e.to_string())?;
    let context = Context::full(&runtime).map_err(|e| e.to_string())?;
    let output: String = context.with(|ctx| {
        let func: Function = ctx.eval(script).map_err(|e| e.to_string())?;
        if kind == "reduce" {
            let parsed: Vec<Value> = serde_json::from_str(&argument).map_err(|e: serde_json::Error| e.to_string())?;
            let keys = serde_json::to_string(&parsed[0]).unwrap();
            let values = serde_json::to_string(&parsed[1]).unwrap();
            let rereduce = parsed[2].as_bool().unwrap_or(false);
            func.call((keys, values, rereduce)).map_err(|e| e.to_string())
        } else {
            func.call((argument,)).map_err(|e| e.to_string())
        }
    })?;
    let value: Value = serde_json::from_str(&output).map_err(|e| e.to_string())?;
    match kind {
        "map" => shape(value, &ids, |id, pairs| {
            pairs
                .into_iter()
                .map(|pair| json!({"id": id, "key": pair.get(0), "value": pair.get(1)}))
                .collect()
        }),
        "index" => shape(value, &ids, |id, pairs| {
            let mut fields = serde_json::Map::new();
            for pair in pairs {
                if let Some(name) = pair.get(0).and_then(Value::as_str) {
                    fields.insert(name.to_string(), pair.get(1).cloned().unwrap_or(Value::Null));
                }
            }
            vec![json!({"id": id, "fields": fields})]
        }),
        _ => Ok(value),
    }
}

/// Run the calls of a batched reduce in order against one compiled reducer.
/// Each call is checked as a lone reduce is; the first failure ends the batch.
/// A call that outlives `timeout` is interrupted, so one runaway call does not
/// spend the budget of the whole batch; the host's own deadline still applies.
fn reduce_many(request: &Value, script: String, timeout: Option<Duration>) -> Result<Value, String> {
    let calls = request.get("calls").and_then(Value::as_array).ok_or("reduce calls are missing")?;
    let mut arguments = Vec::with_capacity(calls.len());
    for call in calls {
        if let Some(keys) = call.get("keys").filter(|keys| !keys.is_null()) {
            serde_json::from_value::<Vec<Value>>(keys.clone())
                .map_err(|error| format!("reduce keys: {error}"))?;
        }
        match call.get("values") {
            Some(values) => {
                serde_json::from_value::<Vec<Value>>(values.clone())
                    .map_err(|error| format!("reduce values: {error}"))?;
            }
            None => return Err("reduce values are missing".to_string()),
        }
        let keys = call.get("keys").cloned().unwrap_or(Value::Null).to_string();
        let values = call.get("values").cloned().unwrap_or(Value::Null).to_string();
        let rereduce = call.get("rereduce").and_then(Value::as_bool).unwrap_or(false);
        arguments.push((keys, values, rereduce));
    }
    let runtime = Runtime::new().map_err(|e| e.to_string())?;
    let deadline = Rc::new(Cell::new(None::<Instant>));
    let shared = Rc::clone(&deadline);
    runtime.set_interrupt_handler(Some(Box::new(move || {
        shared.get().is_some_and(|deadline| Instant::now() > deadline)
    })));
    let context = Context::full(&runtime).map_err(|e| e.to_string())?;
    let outputs: Vec<String> = context.with(|ctx| {
        let func: Function = ctx.eval(script).map_err(|e| e.to_string())?;
        arguments
            .into_iter()
            .map(|call| {
                deadline.set(timeout.map(|timeout| Instant::now() + timeout));
                func.call(call).map_err(|e| e.to_string())
            })
            .collect::<Result<_, String>>()
    })?;
    outputs
        .iter()
        .map(|output| serde_json::from_str(output).map_err(|e| e.to_string()))
        .collect::<Result<Vec<Value>, String>>()
        .map(Value::Array)
}

/// Pair the wrapper's per-document output with the host-supplied ids.
fn shape(
    value: Value,
    ids: &[String],
    rows: impl Fn(&str, Vec<Value>) -> Vec<Value>,
) -> Result<Value, String> {
    let Value::Array(per_doc) = value else {
        return Err("wrapper returned a non-array".to_string());
    };
    if per_doc.len() != ids.len() {
        return Err("wrapper returned the wrong number of results".to_string());
    }
    let mut out = Vec::new();
    for (id, emitted) in ids.iter().zip(per_doc) {
        let Value::Array(pairs) = emitted else {
            return Err("wrapper returned a malformed result".to_string());
        };
        out.extend(rows(id, pairs));
    }
    Ok(Value::Array(out))
}

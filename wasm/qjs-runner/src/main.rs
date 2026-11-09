use rquickjs::{Context, Function, Runtime};
use serde_json::{json, Value};
use std::cell::Cell;
use std::io::Read;
use std::rc::Rc;
use std::time::{Duration, Instant};

// Shared with the host (src/js.rs): the output limit, its error text and the
// batch sizes.
#[path = "../../limits.rs"]
mod limits;

thread_local! {
    /// What the running request calls itself in a panic message, as native's
    /// `js::guard` labels it.
    static LABEL: Cell<&'static str> = const { Cell::new("wasm worker") };
}

fn main() {
    // This target cannot unwind, so a panic aborts the guest. The host sees a
    // trap; the hook first writes the error native's `js::guard` would have
    // reported ("map function panicked: ..."), which the host prefers to the
    // bare trap.
    std::panic::set_hook(Box::new(|info| {
        let message = info.payload_as_str().unwrap_or("unknown panic");
        let error = format!("{} panicked: {message}", LABEL.with(Cell::get));
        println!("{}", json!({"ok": false, "error": error}));
    }));
    let mut input = String::new();
    let read = std::io::stdin().read_to_string(&mut input).map_err(|e| e.to_string());
    let response = match read.and_then(|_| run(&input)) {
        Ok(value) => json!({"ok": true, "value": value}),
        Err(error) => json!({"ok": false, "error": error}),
    };
    println!("{}", response);
}

fn run(input: &str) -> Result<Value, String> {
    let request: Value = serde_json::from_str(input).map_err(|e| e.to_string())?;
    let source = request.get("source").and_then(Value::as_str).ok_or("missing source")?;
    let kind = request.get("kind").and_then(Value::as_str).ok_or("missing kind")?;
    LABEL.with(|label| {
        label.set(match kind {
            "map" => "map function",
            "index" => "index function",
            _ => "reduce function",
        })
    });
    // The wrappers are shared with the native engine (wasm/js); the user's
    // function replaces the placeholder.
    let wrap = |template: &str| template.replace("__DOUCHDB_SOURCE__", source);
    if kind == "reduce_many" {
        let timeout = request.get("timeout_ms").and_then(Value::as_u64).map(Duration::from_millis);
        return reduce_many(&request, wrap(include_str!("../../js/reduce.js")), timeout);
    }
    if kind == "reduce" {
        // Same checks and messages as the process worker (src/js.rs): a
        // rereduce carries null keys, everything else must be an array.
        let call = reduce_arguments(&request)?;
        let runtime = Runtime::new().map_err(|e| e.to_string())?;
        let context = Context::full(&runtime).map_err(|e| e.to_string())?;
        // As native: `compile:` for the wrapper, nothing before a call's error.
        let output: String = context.with(|ctx| {
            let func: Function = ctx
                .eval(wrap(include_str!("../../js/reduce.js")))
                .map_err(|e| format!("compile: {e}"))?;
            func.call(call).map_err(|e| e.to_string())
        })?;
        check_output("reduce function", &output)?;
        return serde_json::from_str(&output).map_err(|e| e.to_string());
    }
    let (template, label) = match kind {
        "map" => (include_str!("../../js/map.js"), "map function"),
        "index" => (include_str!("../../js/index.js"), "index function"),
        other => return Err(format!("unknown kind {other}")),
    };
    let docs = request.get("docs").and_then(Value::as_array).ok_or("missing docs")?;
    // Row ids come from the host, not from the document object the user's
    // function was handed (it may change or delete _id).
    let mut batch_input: Vec<(&str, &str)> = Vec::with_capacity(docs.len());
    for doc in docs {
        batch_input.push((
            doc.get(0).and_then(Value::as_str).ok_or("bad doc")?,
            doc.get(1).and_then(Value::as_str).ok_or("bad doc")?,
        ));
    }
    let (max_docs, max_bytes) = if kind == "map" {
        (limits::MAP_BATCH_DOCS, limits::MAP_BATCH_BYTES)
    } else {
        (limits::INDEX_BATCH_DOCS, usize::MAX)
    };
    let runtime = Runtime::new().map_err(|e| e.to_string())?;
    let context = Context::full(&runtime).map_err(|e| e.to_string())?;
    context.with(|ctx| {
        let func: Function = ctx
            .eval(wrap(template))
            .map_err(|e| format!("compile: {e}"))?;
        let mut out = Vec::new();
        let mut start = 0;
        while start < batch_input.len() {
            // The same batches as the native engine runs (src/viewfn.rs,
            // src/search.rs), so that the output limit and the document named
            // in an error are the same in every sandbox.
            let mut end = start;
            let mut bytes = 2;
            while end < batch_input.len()
                && end - start < max_docs
                && (end == start || bytes + batch_input[end].1.len() < max_bytes)
            {
                bytes += batch_input[end].1.len() + 1;
                end += 1;
            }
            let batch = &batch_input[start..end];
            let first = batch.first().map(|(id, _)| *id).unwrap_or_default();
            let mut json = String::from("[");
            for (index, (_, raw)) in batch.iter().enumerate() {
                if index > 0 { json.push(','); }
                json.push_str(raw);
            }
            json.push(']');
            let output: String = func.call((json,)).map_err(|e| format!("{first}: {e}"))?;
            check_output(label, &output)?;
            let value: Value = serde_json::from_str(&output)
                .map_err(|e| if kind == "map" { format!("{first}: {e}") } else { e.to_string() })?;
            let ids: Vec<&str> = batch.iter().map(|(id, _)| *id).collect();
            if kind == "map" {
                shape("map", first, value, &ids, &mut out, |id, pairs| {
                    pairs
                        .into_iter()
                        .map(|pair| json!({"id": id, "key": pair.get(0), "value": pair.get(1)}))
                        .collect()
                })?;
            } else {
                shape("index", first, value, &ids, &mut out, |id, pairs| {
                    let mut fields = serde_json::Map::new();
                    for pair in pairs {
                        if let Some(name) = pair.get(0).and_then(Value::as_str) {
                            fields.insert(name.to_string(), pair.get(1).cloned().unwrap_or(Value::Null));
                        }
                    }
                    vec![json!({"id": id, "fields": fields})]
                })?;
            }
            start = end;
        }
        Ok(Value::Array(out))
    })
}

/// Check one reduce call as the process worker does and return its arguments:
/// the JSON keys and values and the rereduce flag.
fn reduce_arguments(call: &Value) -> Result<(String, String, bool), String> {
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
    Ok((keys, values, rereduce))
}

/// Run the calls of a batched reduce in order against one compiled reducer.
/// Each call is checked as a lone reduce is; the first failure ends the batch.
/// A call that outlives `timeout` is interrupted, so one runaway call does not
/// spend the budget of the whole batch; the host's own deadline still applies.
fn reduce_many(request: &Value, script: String, timeout: Option<Duration>) -> Result<Value, String> {
    let calls = request.get("calls").and_then(Value::as_array).ok_or("reduce calls are missing")?;
    let arguments = calls
        .iter()
        .map(reduce_arguments)
        .collect::<Result<Vec<_>, String>>()?;
    let runtime = Runtime::new().map_err(|e| e.to_string())?;
    let deadline = Rc::new(Cell::new(None::<Instant>));
    let shared = Rc::clone(&deadline);
    runtime.set_interrupt_handler(Some(Box::new(move || {
        shared.get().is_some_and(|deadline| Instant::now() > deadline)
    })));
    let context = Context::full(&runtime).map_err(|e| e.to_string())?;
    let outputs: Vec<String> = context.with(|ctx| {
        let func: Function = ctx.eval(script).map_err(|e| format!("compile: {e}"))?;
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
        .map(|output| {
            check_output("reduce function", output)?;
            serde_json::from_str(output).map_err(|e| e.to_string())
        })
        .collect::<Result<Vec<Value>, String>>()
        .map(Value::Array)
}

/// The native engine's output size limit (`js::check_output`), with its text.
fn check_output(label: &str, output: &str) -> Result<(), String> {
    if output.len() > limits::MAX_OUTPUT_BYTES {
        Err(limits::output_too_large(label, output.len()))
    } else {
        Ok(())
    }
}

/// Pair the wrapper's per-document output with the host-supplied ids and add
/// the rows to `out`. `what` and `first` word the error for a wrong number of
/// results as native does.
fn shape(
    what: &str,
    first: &str,
    value: Value,
    ids: &[&str],
    out: &mut Vec<Value>,
    rows: impl Fn(&str, Vec<Value>) -> Vec<Value>,
) -> Result<(), String> {
    let Value::Array(per_doc) = value else {
        return Err("wrapper returned a non-array".to_string());
    };
    if per_doc.len() != ids.len() {
        return Err(format!(
            "{first}: {what} returned {} results for {} documents",
            per_doc.len(),
            ids.len()
        ));
    }
    for (id, emitted) in ids.iter().zip(per_doc) {
        let Value::Array(pairs) = emitted else {
            return Err("wrapper returned a malformed result".to_string());
        };
        out.extend(rows(id, pairs));
    }
    Ok(())
}

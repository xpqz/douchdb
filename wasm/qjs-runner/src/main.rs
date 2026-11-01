use rquickjs::{Context, Function, Runtime};
use serde_json::{json, Value};
use std::io::Read;

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
    let (script, argument) = match kind {
        "map" => {
            let docs = request.get("docs").and_then(Value::as_array).ok_or("missing docs")?;
            let mut raw = String::from("[");
            for (index, doc) in docs.iter().enumerate() {
                if index > 0 { raw.push(','); }
                raw.push_str(doc.get(1).and_then(Value::as_str).ok_or("bad doc")?);
            }
            raw.push(']');
            let script = format!(r#"
                (function () {{
                  var __out = [];
                  function sum(values) {{ var total = 0; for (var i=0;i<values.length;i++) total += values[i]; return total; }}
                  function log(message) {{ }}
                  function isArray(value) {{ return Array.isArray(value); }}
                  function toJSON(value) {{ return JSON.stringify(value); }}
                  var emit = function (key, value) {{
                    __out.push([key === undefined ? null : key, value === undefined ? null : value]);
                  }};
                  var __map = ({source});
                  if (typeof __map !== "function") throw new Error("map must be a function");
                  return function (json) {{
                    var docs = JSON.parse(json);
                    var all = [];
                    for (var i = 0; i < docs.length; i++) {{
                      __out = [];
                      __map(docs[i]);
                      for (var j = 0; j < __out.length; j++) all.push({{id: docs[i]._id, key: __out[j][0], value: __out[j][1]}});
                    }}
                    return JSON.stringify(all);
                  }};
                }})()
            "#);
            (script, raw)
        }
        "index" => {
            let docs = request.get("docs").and_then(Value::as_array).ok_or("missing docs")?;
            let mut raw = String::from("[");
            for (index, doc) in docs.iter().enumerate() {
                if index > 0 { raw.push(','); }
                raw.push_str(doc.get(1).and_then(Value::as_str).ok_or("bad doc")?);
            }
            raw.push(']');
            let script = format!(r#"
                (function () {{
                  var __out = [];
                  function log(message) {{ }}
                  var index = function (name, value, options) {{
                    if (value === undefined || value === null) return;
                    __out.push([String(name), value]);
                  }};
                  var __index = ({source});
                  if (typeof __index !== "function") throw new Error("index must be a function");
                  return function (json) {{
                    var docs = JSON.parse(json);
                    var all = [];
                    for (var i = 0; i < docs.length; i++) {{
                      __out = [];
                      __index(docs[i]);
                      var fields = {{}};
                      for (var j = 0; j < __out.length; j++) fields[__out[j][0]] = __out[j][1];
                      all.push({{id: docs[i]._id, fields: fields}});
                    }}
                    return JSON.stringify(all);
                  }};
                }})()
            "#);
            (script, raw)
        }
        "reduce" => {
            let keys = serde_json::to_string(&request.get("keys").cloned().unwrap_or(Value::Null)).unwrap();
            let values = serde_json::to_string(&request.get("values").cloned().unwrap_or(Value::Null)).unwrap();
            let rereduce = request.get("rereduce").and_then(Value::as_bool).unwrap_or(false);
            let script = format!(r#"
                (function () {{
                  function sum(values) {{ var total = 0; for (var i=0;i<values.length;i++) total += values[i]; return total; }}
                  function log(message) {{ }}
                  function isArray(value) {{ return Array.isArray(value); }}
                  function toJSON(value) {{ return JSON.stringify(value); }}
                  var __reduce = ({source});
                  if (typeof __reduce !== "function") throw new Error("reduce must be a function");
                  return function (keys, values, rereduce) {{
                    var result = __reduce(JSON.parse(keys), JSON.parse(values), rereduce);
                    return result === undefined ? "null" : JSON.stringify(result);
                  }};
                }})()
            "#);
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
    serde_json::from_str(&output).map_err(|e| e.to_string())
}

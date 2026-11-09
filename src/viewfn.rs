//! Running and checking the user functions of a design document that sit
//! below the view engine: map rows, `_changes` filters, the save-time compile
//! check and the built-in reduce names. `db` validates design documents with
//! these, so this module must not depend on `db` or `view`.

use crate::js::{JsError, ViewLimits};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The JS wrappers around user functions live once, in `wasm/js`, and are
/// shared with the WASM runner. The placeholder is replaced by the user's
/// function source.
const MAP_WRAPPER: &str = include_str!("../wasm/js/map.js");
pub(crate) const WRAPPER_SOURCE: &str = "__DOUCHDB_SOURCE__";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row {
    pub id: String,
    pub key: Value,
    pub value: Value,
}

/// The built-in reduces, which CouchDB selects by name prefix:
/// `_sum_anything` is `_sum`. `_top_N` and `_bottom_N` carry their rank.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Builtin {
    Count,
    Sum,
    Stats,
    Distinct,
    Top(usize),
    Bottom(usize),
    First,
    Last,
}

/// The largest `N` of `_top_N` and `_bottom_N`.
const MAX_RANK: i64 = 100;

impl Builtin {
    /// The built-in a reduce source names, in the order of CouchDB's
    /// `couch_query_servers:builtin_reduce/4`.
    pub(crate) fn parse(source: &str) -> Option<Builtin> {
        if source.starts_with("_sum") {
            Some(Builtin::Sum)
        } else if source.starts_with("_count") {
            Some(Builtin::Count)
        } else if source.starts_with("_stats") {
            Some(Builtin::Stats)
        } else if source.starts_with("_approx_count_distinct") {
            Some(Builtin::Distinct)
        } else if let Some(rank) = source.strip_prefix("_top_") {
            parse_rank(rank).ok().map(Builtin::Top)
        } else if let Some(rank) = source.strip_prefix("_bottom_") {
            parse_rank(rank).ok().map(Builtin::Bottom)
        } else if source.starts_with("_first") {
            Some(Builtin::First)
        } else if source.starts_with("_last") {
            Some(Builtin::Last)
        } else {
            None
        }
    }
}

/// CouchDB's `check_rank/1`: Erlang's `binary_to_integer` (optional sign,
/// then digits) in 1..=100.
fn parse_rank(rank: &str) -> Result<usize, String> {
    let digits = rank.strip_prefix(['+', '-']).unwrap_or(rank);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("invalid rank reducer".to_string());
    }
    match rank.parse::<i64>() {
        Ok(value) if (1..=MAX_RANK).contains(&value) => Ok(value as usize),
        _ => Err(format!("rank value must be between 1 and {MAX_RANK}")),
    }
}

/// CouchDB's save-time check of a view's reduce (`couch_mrview:validate/2`):
/// a name starting with `_` must be a builtin. The error is the reason of
/// `invalid_design_doc`.
pub(crate) fn check_reduce_name(reduce: &str) -> Result<(), String> {
    if let Some(rank) = reduce
        .strip_prefix("_top_")
        .or_else(|| reduce.strip_prefix("_bottom_"))
    {
        return parse_rank(rank).map(|_| ());
    }
    if reduce.starts_with('_') && Builtin::parse(reduce).is_none() {
        return Err(format!("`{reduce}` is not a supported reduce function."));
    }
    Ok(())
}

fn engine(limits: ViewLimits) -> Result<rquickjs::Runtime, JsError> {
    crate::js::runtime(limits.timeout, limits.memory)
}

/// Evaluate a `_changes` filter function for each document. CouchDB filters are
/// ordinary JavaScript predicates `function(doc, req) { ... }`. Wrapping the
/// predicate in a map function lets filters use the same process and WASM
/// sandbox paths (and timeouts) as view maps. The wrapper is never a native
/// map, so it always takes the JavaScript path.
pub(crate) fn run_filter_json(
    source: &str,
    query: &str,
    docs: &[(String, String)],
    limits: ViewLimits,
) -> Result<std::collections::HashSet<String>, JsError> {
    let wrapped = format!(
        "function (doc) {{ if (({source})(doc, {{\"query\":{query}}})) {{ emit(doc._id, null); }} }}"
    );
    let rows = run_map_js(&wrapped, docs, limits)?;
    Ok(rows.into_iter().map(|row| row.id).collect())
}

/// Which of a view's functions [`check_compiles`] is looking at.
#[derive(Clone, Copy)]
pub(crate) enum ViewFunction {
    Map,
    Reduce,
}

/// Compile a view function without running it, as CouchDB's `add_fun` does
/// when a design document is saved. The source is evaluated in the same
/// position as in the real wrappers, so it compiles here exactly when it would
/// at query time; the stand-in it is wrapped in does nothing with its input.
/// Returns the engine's message, to be put in CouchDB's `compilation_error`.
pub(crate) fn check_compiles(
    function: ViewFunction,
    source: &str,
    limits: ViewLimits,
) -> Result<(), String> {
    const NOT_A_FUNCTION: &str = "Expression does not eval to a function.";
    let probe = format!(
        "(function(){{ if (typeof ({source}) !== \"function\") throw new Error(\"{NOT_A_FUNCTION}\"); return function(){{}}; }})()"
    );
    let result = if crate::js::sandboxed() {
        // The sandbox has no compile-only request; the probe's stand-in
        // function runs once on an empty input.
        match function {
            ViewFunction::Map => crate::js::sandbox_request(
                "map",
                json!({"source": probe, "docs": [("_", "{}")]}),
                limits,
            )
            .map(|_| ()),
            ViewFunction::Reduce => crate::js::sandbox_request(
                "reduce",
                json!({"source": probe, "keys": [], "values": [], "rereduce": false}),
                limits,
            )
            .map(|_| ()),
        }
    } else {
        probe_native(&probe, limits).map_err(JsError::Runtime)
    };
    result.map_err(|error| {
        let text = error.to_string();
        let text = text.trim_start_matches("compile: ");
        if text.contains(NOT_A_FUNCTION) {
            NOT_A_FUNCTION.to_string()
        } else {
            text.to_string()
        }
    })
}

/// Evaluate the probe in-process, keeping the exception's own name and
/// message, which a plain `rquickjs::Error` does not carry.
fn probe_native(probe: &str, limits: ViewLimits) -> Result<(), String> {
    let runtime = engine(limits).map_err(|error| error.to_string())?;
    let context = rquickjs::Context::full(&runtime).map_err(|error| error.to_string())?;
    crate::js::guard("view function", || {
        Ok(
            context.with(|ctx| match ctx.eval::<rquickjs::Value, _>(probe) {
                Ok(_) => Ok(()),
                Err(rquickjs::Error::Exception) => {
                    let thrown = ctx.catch();
                    let text = |member: &str| {
                        thrown
                            .as_object()
                            .and_then(|object| object.get::<_, String>(member).ok())
                    };
                    Err(match (text("name"), text("message")) {
                        (Some(name), Some(message)) => format!("{name}: {message}"),
                        (None, Some(message)) => message,
                        _ => "Exception generated by QuickJS".to_string(),
                    })
                }
                Err(other) => Err(other.to_string()),
            }),
        )
    })
    .map_err(|error| error.to_string())?
}

/// Run a map function in QuickJS, in batches, without the native fast path.
pub(crate) fn run_map_js(
    source: &str,
    docs: &[(String, String)],
    limits: ViewLimits,
) -> Result<Vec<Row>, JsError> {
    use crate::js::limits::{MAP_BATCH_BYTES as MAX_BYTES, MAP_BATCH_DOCS as MAX_DOCS};
    crate::view::debug_assert_off_runtime_workers();
    // Sandbox calls pay a process/wasm boundary per batch. Use a larger batch
    // than the native path while keeping the input and output bounded.
    let (max_docs, max_bytes) = if crate::js::sandboxed() {
        (2048usize, 8usize * 1024 * 1024)
    } else {
        (MAX_DOCS, MAX_BYTES)
    };

    if crate::js::sandboxed() {
        let mut rows = Vec::new();
        let mut start = 0;
        while start < docs.len() {
            let mut end = start;
            let mut bytes = 2;
            while end < docs.len()
                && end - start < max_docs
                && (end == start || bytes + docs[end].1.len() < max_bytes)
            {
                bytes += docs[end].1.len() + 1;
                end += 1;
            }
            let value = crate::js::sandbox_request(
                "map",
                json!({"source": source, "docs": &docs[start..end]}),
                limits,
            )?;
            let chunk: Vec<Row> = serde_json::from_value(value)
                .map_err(|error| JsError::Runtime(error.to_string()))?;
            rows.extend(chunk);
            start = end;
        }
        return Ok(rows);
    }

    let runtime = engine(limits)?;
    let context =
        rquickjs::Context::full(&runtime).map_err(|error| JsError::Runtime(error.to_string()))?;

    crate::js::guard("map function", || {
        context.with(|ctx| {
            let wrapped = MAP_WRAPPER.replace(WRAPPER_SOURCE, source);
            let func: rquickjs::Function = ctx
                .eval(wrapped)
                .map_err(|error| JsError::Runtime(format!("compile: {error}")))?;

            let mut rows = Vec::new();
            let mut start = 0;
            while start < docs.len() {
                let mut end = start;
                let mut bytes = 2;
                while end < docs.len()
                    && end - start < MAX_DOCS
                    && (end == start || bytes + docs[end].1.len() < MAX_BYTES)
                {
                    bytes += docs[end].1.len() + 1;
                    end += 1;
                }

                let mut batch = String::with_capacity(bytes);
                batch.push('[');
                for (index, (_, doc)) in docs[start..end].iter().enumerate() {
                    if index > 0 {
                        batch.push(',');
                    }
                    batch.push_str(doc);
                }
                batch.push(']');

                let first_id = &docs[start].0;
                let output: String = func
                    .call((batch,))
                    .map_err(|error| JsError::Runtime(format!("{first_id}: {error}")))?;
                crate::js::check_output("map function", &output)?;
                let emitted: Vec<Vec<(Value, Value)>> = serde_json::from_str(&output)
                    .map_err(|error| JsError::Runtime(format!("{first_id}: {error}")))?;
                if emitted.len() != end - start {
                    return Err(JsError::Runtime(format!(
                        "{first_id}: map returned {} results for {} documents",
                        emitted.len(),
                        end - start
                    )));
                }
                for (offset, pairs) in emitted.into_iter().enumerate() {
                    let id = &docs[start + offset].0;
                    for (key, value) in pairs {
                        rows.push(Row {
                            id: id.clone(),
                            key,
                            value,
                        });
                    }
                }
                start = end;
            }
            Ok(rows)
        })
    })
}

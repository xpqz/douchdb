//! Shared QuickJS execution controls.
//!
//! These limits protect availability and keep one query from corrupting
//! another. They are **not** a security sandbox: QuickJS is C and a
//! memory-safety bug or segfault still takes down this process. See
//! `docs/quickjs-sandbox.md` for the threat model and the worker/WASM plans.

use serde_json::{json, Value};
use std::io::{Read, Write};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// A single map/reduce/index call may not return more JSON than this. The
/// QuickJS heap limit bounds allocations inside the engine; this bounds the
/// Rust-side string that comes back before it is parsed into values.
pub const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;

/// Build a runtime with the standard view/search limits.
pub fn runtime(timeout: Duration, memory: usize) -> Result<rquickjs::Runtime, String> {
    let runtime = rquickjs::Runtime::new().map_err(|error| error.to_string())?;
    runtime.set_memory_limit(memory);
    runtime.set_max_stack_size(512 * 1024);
    // A view is arbitrary user code. Stop runaway loops instead of hanging the
    // request thread forever.
    let deadline = Instant::now() + timeout;
    runtime.set_interrupt_handler(Some(Box::new(move || Instant::now() > deadline)));
    Ok(runtime)
}

/// Contain a Rust panic from the bindings (a C `abort` or segfault is not
/// unwindable; that is what the worker-process plan is for).
pub fn guard<T>(label: &str, f: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => {
            let message = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("unknown panic");
            Err(format!("{label} panicked: {message}"))
        }
    }
}

pub fn check_output(label: &str, output: &str) -> Result<(), String> {
    if output.len() > MAX_OUTPUT_BYTES {
        Err(format!(
            "{label} produced {} bytes of output (limit {MAX_OUTPUT_BYTES})",
            output.len()
        ))
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Optional worker-process sandbox

static PROCESS_SANDBOX: AtomicBool = AtomicBool::new(false);

pub fn configure(process: bool) {
    PROCESS_SANDBOX.store(process, Ordering::SeqCst);
}

pub fn process_enabled() -> bool {
    PROCESS_SANDBOX.load(Ordering::SeqCst)
}

/// Run a map/reduce/index request in a short-lived worker process.
pub fn process_request(
    kind: &str,
    payload: Value,
    limits: crate::view::ViewLimits,
) -> Result<Value, String> {
    let mut request = match payload {
        Value::Object(map) => map,
        _ => return Err("worker request must be a JSON object".to_string()),
    };
    request.insert("kind".to_string(), Value::String(kind.to_string()));
    request.insert(
        "timeout_ms".to_string(),
        json!(limits.timeout.as_millis() as u64),
    );
    request.insert("memory".to_string(), json!(limits.memory));
    let body = serde_json::to_vec(&Value::Object(request)).map_err(|error| error.to_string())?;

    let exe = std::env::current_exe().map_err(|error| error.to_string())?;
    let mut command = Command::new(exe);
    command
        .arg("--js-worker")
        .env_clear()
        .current_dir("/")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // The debug binary maps ~1 GB of virtual address space before QuickJS
        // allocates anything, so the address-space cap needs generous headroom.
        let memory = limits.memory.saturating_add(3 * 1024 * 1024 * 1024) as libc::rlim_t;
        let cpu = limits.timeout.as_secs().saturating_add(5).max(5) as libc::rlim_t;
        unsafe {
            command.pre_exec(move || {
                let mut limit = libc::rlimit {
                    rlim_cur: memory,
                    rlim_max: memory,
                };
                if libc::setrlimit(libc::RLIMIT_AS, &limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                limit = libc::rlimit {
                    rlim_cur: cpu,
                    rlim_max: cpu,
                };
                if libc::setrlimit(libc::RLIMIT_CPU, &limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                let zero = libc::rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                let _ = libc::setrlimit(libc::RLIMIT_CORE, &zero);
                let _ = libc::setrlimit(libc::RLIMIT_FSIZE, &zero);
                let files = libc::rlimit {
                    rlim_cur: 64,
                    rlim_max: 64,
                };
                let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &files);
                let _ = libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);
                // Do not leave a worker behind if the server is killed while a
                // query is in flight.
                let _ = libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
                Ok(())
            });
        }
    }
    let mut child = command.spawn().map_err(|error| error.to_string())?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| "worker stdin unavailable".to_string())?;
    stdin
        .write_all(&body)
        .map_err(|error| format!("worker write: {error}"))?;
    drop(stdin);
    let output = child
        .wait_with_output()
        .map_err(|error| format!("worker wait: {error}"))?;
    if !output.status.success() {
        return Err(format!("worker exited with {}", output.status));
    }
    let response: Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("worker response: {error}"))?;
    if response.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(response.get("value").cloned().unwrap_or(Value::Null))
    } else {
        Err(response
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("worker failed")
            .to_string())
    }
}

/// Entry point for the hidden `--js-worker` mode. One request per process.
pub fn run_worker_stdio() -> Result<(), String> {
    let mut input = String::new();
    std::io::stdin()
        .read_to_string(&mut input)
        .map_err(|error| error.to_string())?;
    let request: Value = serde_json::from_str(&input).map_err(|error| error.to_string())?;
    let response = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        handle_worker_request(request)
    })) {
        Ok(response) => response,
        Err(_) => json!({"ok": false, "error": "worker panicked"}),
    };
    let body = serde_json::to_vec(&response).map_err(|error| error.to_string())?;
    let mut stdout = std::io::stdout();
    stdout
        .write_all(&body)
        .and_then(|_| stdout.flush())
        .map_err(|error| error.to_string())
}

fn handle_worker_request(request: Value) -> Value {
    let limits = crate::view::ViewLimits {
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
    let result = match request.get("kind").and_then(Value::as_str) {
        Some("map") => {
            let docs: Vec<(String, String)> =
                match serde_json::from_value(request.get("docs").cloned().unwrap_or_default()) {
                    Ok(docs) => docs,
                    Err(error) => return json!({"ok": false, "error": error.to_string()}),
                };
            crate::view::run_map_json(source, &docs, limits).map(|rows| json!(rows))
        }
        Some("reduce") => {
            let keys: Vec<Value> =
                serde_json::from_value(request.get("keys").cloned().unwrap_or_default())
                    .unwrap_or_default();
            let values: Vec<Value> =
                serde_json::from_value(request.get("values").cloned().unwrap_or_default())
                    .unwrap_or_default();
            let rereduce = request
                .get("rereduce")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            crate::view::run_reduce(source, &keys, &values, rereduce, limits)
        }
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
    match result {
        Ok(value) => json!({"ok": true, "value": value}),
        Err(error) => json!({"ok": false, "error": error}),
    }
}

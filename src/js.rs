//! Shared QuickJS execution controls.
//!
//! These limits protect availability and keep one query from corrupting
//! another. They are **not** a security sandbox: QuickJS is C and a
//! memory-safety bug or segfault still takes down this process. See
//! `docs/quickjs-sandbox.md` for the threat model and the worker/WASM plans.

use serde_json::{json, Value};
use std::io::{BufReader, Read, Write};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sandbox {
    Native,
    Process,
    Wasm,
}

static SANDBOX: AtomicU8 = AtomicU8::new(0);
static MAX_WORKERS: AtomicUsize = AtomicUsize::new(2);
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

pub fn configure(sandbox: Sandbox, workers: usize) {
    SANDBOX.store(
        match sandbox {
            Sandbox::Native => 0,
            Sandbox::Process => 1,
            Sandbox::Wasm => 2,
        },
        Ordering::SeqCst,
    );
    MAX_WORKERS.store(workers.clamp(1, 64), Ordering::SeqCst);
}

pub fn sandboxed() -> bool {
    SANDBOX.load(Ordering::SeqCst) != 0
}

/// Dispatch a map/reduce/index call to the selected sandbox.
pub fn sandbox_request(
    kind: &str,
    payload: Value,
    limits: crate::view::ViewLimits,
) -> Result<Value, String> {
    match SANDBOX.load(Ordering::SeqCst) {
        1 => process_request(kind, payload, limits),
        2 => wasm_request(kind, payload, limits),
        _ => Err("sandbox request without a sandbox".to_string()),
    }
}

/// Install a Linux seccomp denylist in the worker. The worker runs after this
/// point, so file/network/exec syscalls are no longer needed.
#[cfg(target_os = "linux")]
pub fn install_seccomp() -> Result<(), String> {
    use seccompiler::{
        apply_filter_all_threads, BpfProgram, SeccompAction, SeccompFilter, SeccompRule,
    };
    use std::collections::BTreeMap;

    let denied = [
        libc::SYS_open,
        libc::SYS_openat,
        libc::SYS_creat,
        libc::SYS_unlink,
        libc::SYS_unlinkat,
        libc::SYS_rename,
        libc::SYS_renameat,
        libc::SYS_mkdir,
        libc::SYS_mkdirat,
        libc::SYS_rmdir,
        libc::SYS_openat2,
        libc::SYS_socket,
        libc::SYS_socketpair,
        libc::SYS_sendmsg,
        libc::SYS_recvmsg,
        libc::SYS_connect,
        libc::SYS_bind,
        libc::SYS_listen,
        libc::SYS_accept,
        libc::SYS_accept4,
        libc::SYS_sendto,
        libc::SYS_recvfrom,
        libc::SYS_execve,
        libc::SYS_execveat,
        libc::SYS_fork,
        libc::SYS_vfork,
        libc::SYS_ptrace,
        libc::SYS_mount,
        libc::SYS_umount2,
        libc::SYS_chroot,
        libc::SYS_kill,
        libc::SYS_bpf,
        libc::SYS_perf_event_open,
        libc::SYS_userfaultfd,
        libc::SYS_memfd_create,
        libc::SYS_process_vm_readv,
        libc::SYS_process_vm_writev,
        libc::SYS_unshare,
        libc::SYS_setns,
        libc::SYS_pivot_root,
        libc::SYS_io_uring_setup,
        libc::SYS_io_uring_enter,
        libc::SYS_io_uring_register,
    ];
    let rules: BTreeMap<i64, Vec<SeccompRule>> = denied
        .iter()
        .map(|syscall| (*syscall as i64, Vec::new()))
        .collect();
    let arch = std::env::consts::ARCH
        .try_into()
        .map_err(|error| format!("seccomp arch: {error:?}"))?;
    let filter: BpfProgram = SeccompFilter::new(
        rules,
        SeccompAction::Allow,
        SeccompAction::Errno(libc::EPERM as u32),
        arch,
    )
    .map_err(|error| format!("seccomp filter: {error}"))?
    .try_into()
    .map_err(|error| format!("seccomp compile: {error}"))?;
    apply_filter_all_threads(&filter).map_err(|error| format!("seccomp apply: {error}"))
}

#[cfg(not(target_os = "linux"))]
pub fn install_seccomp() -> Result<(), String> {
    Ok(())
}

// ---------------------------------------------------------------------------
// WASM engine

const WASM_RUNNER: &[u8] = include_bytes!("../wasm/qjs-runner.wasm");

struct WasmEngineState {
    engine: wasmi::Engine,
    module: wasmi::Module,
}

struct WasmData {
    wasi: wasmi_wasi::WasiCtx,
    limits: wasmi::StoreLimits,
}

fn wasm_engine() -> Result<&'static WasmEngineState, String> {
    static ENGINE: OnceLock<Result<WasmEngineState, String>> = OnceLock::new();
    let state = ENGINE.get_or_init(|| {
        let mut config = wasmi::Config::default();
        config.consume_fuel(true);
        let engine = wasmi::Engine::new(&config);
        let module = wasmi::Module::new(&engine, WASM_RUNNER)
            .map_err(|error| format!("wasm module: {error}"))?;
        Ok(WasmEngineState { engine, module })
    });
    match state {
        Ok(state) => Ok(state),
        Err(error) => Err(error.clone()),
    }
}

fn wasm_request(
    kind: &str,
    payload: Value,
    limits: crate::view::ViewLimits,
) -> Result<Value, String> {
    let mut request = match payload {
        Value::Object(map) => map,
        _ => return Err("wasm request must be a JSON object".to_string()),
    };
    request.insert("kind".to_string(), Value::String(kind.to_string()));
    request.insert(
        "timeout_ms".to_string(),
        json!(limits.timeout.as_millis() as u64),
    );
    request.insert("memory".to_string(), json!(limits.memory));
    let body = serde_json::to_vec(&Value::Object(request)).map_err(|error| error.to_string())?;

    let state = wasm_engine()?;
    let mut linker = wasmi::Linker::new(&state.engine);
    wasmi_wasi::add_to_linker(&mut linker, |data: &mut WasmData| &mut data.wasi)
        .map_err(|error| format!("wasm wasi: {error}"))?;
    let stdout = wasmi_wasi::wasi_common::pipe::WritePipe::new_in_memory();
    let wasi = wasmi_wasi::WasiCtxBuilder::new()
        .stdin(Box::new(wasmi_wasi::wasi_common::pipe::ReadPipe::from(
            body,
        )))
        .stdout(Box::new(stdout.clone()))
        .build();
    let data = WasmData {
        wasi,
        limits: wasmi::StoreLimitsBuilder::new()
            .memory_size(128 * 1024 * 1024)
            .trap_on_grow_failure(true)
            .build(),
    };
    let mut store = wasmi::Store::new(&state.engine, data);
    store.limiter(|data| &mut data.limits);
    let fuel = (limits.timeout.as_millis() as u64)
        .saturating_mul(2_000_000)
        .max(500_000_000);
    store
        .set_fuel(fuel)
        .map_err(|error| format!("wasm fuel: {error}"))?;
    let instance = linker
        .instantiate_and_start(&mut store, &state.module)
        .map_err(|error| format!("wasm instantiate: {error}"))?;
    let start = instance
        .get_typed_func::<(), ()>(&store, "_start")
        .map_err(|error| format!("wasm start: {error}"))?;
    start
        .call(&mut store, ())
        .map_err(|error| format!("wasm run: {error}"))?;
    drop(store);
    let output = stdout
        .try_into_inner()
        .map_err(|_| "wasm stdout is still borrowed".to_string())?
        .into_inner();
    check_output("wasm worker", &String::from_utf8_lossy(&output))?;
    let response: Value =
        serde_json::from_slice(&output).map_err(|error| format!("wasm response: {error}"))?;
    if response.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(response.get("value").cloned().unwrap_or(Value::Null))
    } else {
        Err(response
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("wasm worker failed")
            .to_string())
    }
}

/// One request/response exchange with a worker process.
fn write_frame<W: Write>(writer: &mut W, body: &[u8]) -> Result<(), String> {
    if body.len() > MAX_FRAME_BYTES {
        return Err(format!("worker frame is too large: {} bytes", body.len()));
    }
    writer
        .write_all(&(body.len() as u32).to_be_bytes())
        .and_then(|_| writer.write_all(body))
        .and_then(|_| writer.flush())
        .map_err(|error| format!("worker write: {error}"))
}

fn read_frame<R: Read>(reader: &mut R) -> Result<Option<Vec<u8>>, String> {
    let mut length = [0u8; 4];
    match reader.read_exact(&mut length) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(format!("worker read: {error}")),
    }
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(format!("worker frame is too large: {length} bytes"));
    }
    let mut body = vec![0u8; length];
    reader
        .read_exact(&mut body)
        .map_err(|error| format!("worker read body: {error}"))?;
    Ok(Some(body))
}

struct Worker {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    pid: i32,
}

enum WorkerOutcome {
    Value(Value),
    AppError(String),
}

struct WorkerError {
    message: String,
    timed_out: bool,
}

impl Worker {
    fn spawn() -> Result<Worker, String> {
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
            // The debug binary maps ~1 GB of virtual address space before
            // QuickJS allocates anything, so leave generous headroom. QuickJS
            // itself enforces the per-request heap limit.
            let memory = 4 * 1024 * 1024 * 1024u64 as libc::rlim_t;
            unsafe {
                command.pre_exec(move || {
                    let limit = libc::rlimit {
                        rlim_cur: memory,
                        rlim_max: memory,
                    };
                    if libc::setrlimit(libc::RLIMIT_AS, &limit) != 0 {
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
                    let _ = libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
                    Ok(())
                });
            }
        }
        let mut child = command.spawn().map_err(|error| error.to_string())?;
        let pid = child.id() as i32;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "worker stdin unavailable".to_string())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "worker stdout unavailable".to_string())?;
        Ok(Worker {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            pid,
        })
    }

    fn request(&mut self, body: &[u8], timeout: Duration) -> Result<WorkerOutcome, WorkerError> {
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let pid = self.pid;
        let deadline = timeout + Duration::from_secs(2);
        let timed_out = Arc::new(AtomicBool::new(false));
        let flag = timed_out.clone();
        let watchdog = std::thread::spawn(move || match done_rx.recv_timeout(deadline) {
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                flag.store(true, Ordering::SeqCst);
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                }
            }
        });
        let result = write_frame(&mut self.stdin, body).and_then(|_| {
            read_frame(&mut self.stdout)?.ok_or_else(|| "worker closed the connection".to_string())
        });
        let _ = done_tx.send(());
        let _ = watchdog.join();
        let response = result.map_err(|message| WorkerError {
            message,
            timed_out: timed_out.load(Ordering::SeqCst),
        })?;
        let response: Value = serde_json::from_slice(&response).map_err(|error| WorkerError {
            message: error.to_string(),
            timed_out: false,
        })?;
        if response.get("ok").and_then(Value::as_bool) == Some(true) {
            Ok(WorkerOutcome::Value(
                response.get("value").cloned().unwrap_or(Value::Null),
            ))
        } else {
            Ok(WorkerOutcome::AppError(
                response
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("worker failed")
                    .to_string(),
            ))
        }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct PoolState {
    idle: Vec<Worker>,
    total: usize,
}

struct WorkerPool {
    state: Mutex<PoolState>,
    ready: Condvar,
    max: usize,
}

impl WorkerPool {
    fn new(max: usize) -> WorkerPool {
        WorkerPool {
            state: Mutex::new(PoolState {
                idle: Vec::new(),
                total: 0,
            }),
            ready: Condvar::new(),
            max: max.max(1),
        }
    }

    fn acquire(&self) -> Result<Worker, String> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        loop {
            if let Some(mut worker) = state.idle.pop() {
                if worker.child.try_wait().ok().flatten().is_none() {
                    return Ok(worker);
                }
                state.total = state.total.saturating_sub(1);
                continue;
            }
            if state.total < self.max {
                state.total += 1;
                drop(state);
                return match Worker::spawn() {
                    Ok(worker) => Ok(worker),
                    Err(error) => {
                        let mut state = self
                            .state
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner());
                        state.total = state.total.saturating_sub(1);
                        self.ready.notify_one();
                        Err(error)
                    }
                };
            }
            state = self
                .ready
                .wait(state)
                .unwrap_or_else(|poison| poison.into_inner());
        }
    }

    fn release(&self, worker: Worker) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.idle.push(worker);
        self.ready.notify_one();
    }

    fn discard(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.total = state.total.saturating_sub(1);
        self.ready.notify_one();
    }
}

fn pool() -> &'static WorkerPool {
    static POOL: OnceLock<WorkerPool> = OnceLock::new();
    POOL.get_or_init(|| WorkerPool::new(MAX_WORKERS.load(Ordering::SeqCst)))
}

/// Run a map/reduce/index request in a pooled worker process.
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

    let pool = pool();
    let mut worker = pool.acquire()?;
    let mut last_error = String::new();
    for attempt in 0..2 {
        match worker.request(&body, limits.timeout) {
            Ok(WorkerOutcome::Value(value)) => {
                pool.release(worker);
                return Ok(value);
            }
            Ok(WorkerOutcome::AppError(error)) => {
                pool.release(worker);
                return Err(error);
            }
            Err(error) => {
                worker.kill();
                pool.discard();
                last_error = error.message;
                // A deadline kill is not retried: the request already consumed
                // its budget. One retry covers a worker that died between
                // requests.
                if error.timed_out || attempt > 0 {
                    return Err(last_error);
                }
                worker = pool.acquire()?;
            }
        }
    }
    Err(last_error)
}

/// Entry point for the hidden `--js-worker` mode. Frames on stdin/stdout.
pub fn run_worker_stdio() -> Result<(), String> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();
    loop {
        let Some(body) = read_frame(&mut input)? else {
            return Ok(());
        };
        let response = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            match serde_json::from_slice::<Value>(&body) {
                Ok(request) => handle_worker_request(request),
                Err(error) => json!({"ok": false, "error": error.to_string()}),
            }
        })) {
            Ok(response) => response,
            Err(_) => json!({"ok": false, "error": "worker panicked"}),
        };
        let body = serde_json::to_vec(&response).map_err(|error| error.to_string())?;
        write_frame(&mut output, &body)?;
    }
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

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
    let (runtime, deadline) = reusable_runtime(memory)?;
    *deadline.lock().unwrap_or_else(|poison| poison.into_inner()) = Instant::now() + timeout;
    Ok(runtime)
}

/// A runtime for several calls: the caller moves the deadline before each.
pub fn reusable_runtime(memory: usize) -> Result<(rquickjs::Runtime, Arc<Mutex<Instant>>), String> {
    let runtime = rquickjs::Runtime::new().map_err(|error| error.to_string())?;
    runtime.set_memory_limit(memory);
    runtime.set_max_stack_size(512 * 1024);
    // A view is arbitrary user code. Stop runaway loops instead of hanging the
    // request thread forever.
    let deadline = Arc::new(Mutex::new(Instant::now()));
    let shared = Arc::clone(&deadline);
    runtime.set_interrupt_handler(Some(Box::new(move || {
        Instant::now() > *shared.lock().unwrap_or_else(|poison| poison.into_inner())
    })));
    Ok((runtime, deadline))
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

/// Which WebAssembly runtime executes `qjs-runner.wasm`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WasmRuntime {
    /// Pure-Rust interpreter; small dependency, slower.
    Wasmi,
    /// Cranelift JIT; much faster, larger optional dependency.
    Wasmtime,
}

static SANDBOX: AtomicU8 = AtomicU8::new(0);
#[cfg(feature = "wasm")]
static WASM_RUNTIME: AtomicU8 = AtomicU8::new(0);
static MAX_WORKERS: AtomicUsize = AtomicUsize::new(2);
/// A debug-build worker needs a moment to start; a hung one is killed after this.
const WORKER_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
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

/// Select the wasm runtime. Returns an error when Wasmtime was requested but
/// this binary was built without the `wasmtime` feature.
#[cfg(feature = "wasm")]
pub fn set_wasm_runtime(runtime: WasmRuntime) -> Result<(), String> {
    match runtime {
        WasmRuntime::Wasmi => {
            WASM_RUNTIME.store(0, Ordering::SeqCst);
            Ok(())
        }
        WasmRuntime::Wasmtime => {
            #[cfg(feature = "wasmtime")]
            {
                crate::js_wasmtime::warm()?;
                WASM_RUNTIME.store(1, Ordering::SeqCst);
                Ok(())
            }
            #[cfg(not(feature = "wasmtime"))]
            {
                Err(
                    "the wasmtime backend is not compiled; rebuild with `cargo build --features wasmtime`"
                        .to_string(),
                )
            }
        }
    }
}

/// Without the `wasm` feature there is no wasm sandbox to select.
#[cfg(not(feature = "wasm"))]
pub fn set_wasm_runtime(_runtime: WasmRuntime) -> Result<(), String> {
    Err(
        "the wasm sandbox is not compiled; rebuild with default features (the `wasm` feature)"
            .to_string(),
    )
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
        #[cfg(feature = "wasm")]
        2 => wasm_request(kind, payload, limits),
        _ => Err("sandbox request without a sandbox".to_string()),
    }
}

/// Syscalls the worker may make once it is serving requests: reading and
/// writing the request pipes, memory management for QuickJS, futexes, clocks
/// and exiting. Derived by tracing a worker through map, reduce, index,
/// timeout, out-of-memory and stack-overflow requests, plus the calls a
/// panic or abort needs. Everything else (files, sockets, exec, signals to
/// other processes, namespaces, links, renames, chmod/chown, mknod, ...) fails
/// with EPERM. Only syscalls present on both x86_64 and aarch64 are listed.
#[cfg(target_os = "linux")]
const WORKER_SYSCALLS: &[i64] = &[
    libc::SYS_read,
    libc::SYS_write,
    libc::SYS_readv,
    libc::SYS_writev,
    libc::SYS_pread64,
    libc::SYS_close,
    libc::SYS_lseek,
    libc::SYS_fstat,
    libc::SYS_fcntl,
    libc::SYS_mmap,
    libc::SYS_munmap,
    libc::SYS_mremap,
    libc::SYS_mprotect,
    libc::SYS_madvise,
    libc::SYS_brk,
    libc::SYS_futex,
    libc::SYS_sched_yield,
    libc::SYS_clock_gettime,
    libc::SYS_clock_nanosleep,
    libc::SYS_nanosleep,
    libc::SYS_gettimeofday,
    libc::SYS_getrandom,
    libc::SYS_getpid,
    libc::SYS_gettid,
    libc::SYS_rt_sigaction,
    libc::SYS_rt_sigprocmask,
    libc::SYS_rt_sigreturn,
    libc::SYS_sigaltstack,
    libc::SYS_restart_syscall,
    libc::SYS_exit,
    libc::SYS_exit_group,
];

/// Install a Linux seccomp allowlist in the worker: only the syscalls in
/// [`WORKER_SYSCALLS`] succeed, everything else returns EPERM. `tgkill` is
/// allowed only against the worker's own pid (that is how `abort` raises
/// SIGABRT); `kill`, `tkill` and `pidfd_send_signal` are not allowed, so a
/// compromised worker cannot signal the server that spawned it.
///
/// Install Landlock first: its syscalls are not on the list.
#[cfg(target_os = "linux")]
pub fn install_seccomp() -> Result<(), String> {
    use seccompiler::{
        apply_filter_all_threads, BpfProgram, SeccompAction, SeccompCmpArgLen, SeccompCmpOp,
        SeccompCondition, SeccompFilter, SeccompRule,
    };
    use std::collections::BTreeMap;

    let mut rules: BTreeMap<i64, Vec<SeccompRule>> = WORKER_SYSCALLS
        .iter()
        .map(|syscall| (*syscall, Vec::new()))
        .collect();
    let own_pid = std::process::id() as u64;
    let own_process = SeccompCondition::new(0, SeccompCmpArgLen::Dword, SeccompCmpOp::Eq, own_pid)
        .and_then(|condition| SeccompRule::new(vec![condition]))
        .map_err(|error| format!("seccomp tgkill rule: {error}"))?;
    rules.insert(libc::SYS_tgkill, vec![own_process]);
    let arch = std::env::consts::ARCH
        .try_into()
        .map_err(|error| format!("seccomp arch: {error:?}"))?;
    let filter: BpfProgram = SeccompFilter::new(
        rules,
        SeccompAction::Errno(libc::EPERM as u32),
        SeccompAction::Allow,
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

/// Add a Landlock layer that denies filesystem access. Best effort: kernels
/// without Landlock (or with an older ABI) still retain the seccomp
/// allowlist. Returns how much was enforced so the server can log it.
#[cfg(target_os = "linux")]
pub fn install_landlock() -> Result<&'static str, String> {
    use landlock::{
        Access, AccessFs, CompatLevel, Compatible, RestrictSelfAttr, Ruleset, RulesetAttr,
        RulesetStatus, ABI,
    };
    let status = Ruleset::default()
        .set_compatibility(CompatLevel::BestEffort)
        .handle_access(AccessFs::from_all(ABI::V9))
        .map_err(|error| format!("landlock access: {error}"))?
        .create()
        .map_err(|error| format!("landlock create: {error}"))?
        .all_threads(true)
        .map_err(|error| format!("landlock threads: {error}"))?
        .restrict_self()
        .map_err(|error| format!("landlock restrict: {error}"))?;
    Ok(match status.ruleset {
        RulesetStatus::FullyEnforced => "fully enforced",
        RulesetStatus::PartiallyEnforced => "partially enforced",
        RulesetStatus::NotEnforced => "not enforced",
    })
}

#[cfg(not(target_os = "linux"))]
pub fn install_landlock() -> Result<&'static str, String> {
    Ok("unavailable on this platform")
}

// ---------------------------------------------------------------------------
// WASM engine

#[cfg(feature = "wasm")]
const WASM_RUNNER: &[u8] = include_bytes!("../wasm/qjs-runner.wasm");

/// Linear memory the module declares up front (runtime, data segments and
/// stack), in bytes. The view memory limit applies on top of it.
#[cfg(feature = "wasm")]
fn wasmi_baseline_memory(module: &wasmi::Module) -> usize {
    module
        .exports()
        .find_map(|export| match export.ty() {
            wasmi::ExternType::Memory(memory) if export.name() == "memory" => {
                Some(memory.minimum().saturating_mul(65536) as usize)
            }
            _ => None,
        })
        .unwrap_or(0)
}

/// Cap on the wasm linear memory for one request: what the module needs to
/// start, the configured view memory limit for the QuickJS heap, and room for
/// the request and its parsed copy. The runner ignores the `memory` field in
/// the request, so the host enforces the limit by refusing to grow past this.
#[cfg(feature = "wasm")]
pub(crate) fn wasm_memory_cap(
    baseline: usize,
    limits: crate::view::ViewLimits,
    body_len: usize,
) -> usize {
    baseline
        .saturating_add(limits.memory)
        .saturating_add(body_len.saturating_mul(2))
}

#[cfg(feature = "wasm")]
struct WasmEngineState {
    engine: wasmi::Engine,
    module: wasmi::Module,
    baseline_memory: usize,
    linker: OnceLock<Result<wasmi::Linker<WasmData>, String>>,
}

#[cfg(feature = "wasm")]
struct WasmData {
    wasi: wasmi_wasi::WasiCtx,
    limits: wasmi::StoreLimits,
}

#[cfg(feature = "wasm")]
fn wasm_engine() -> Result<&'static WasmEngineState, String> {
    static ENGINE: OnceLock<Result<WasmEngineState, String>> = OnceLock::new();
    let state = ENGINE.get_or_init(|| {
        let mut config = wasmi::Config::default();
        config.consume_fuel(true);
        // Fuel is only a way to regain control (see `run_with_deadline`). A
        // lazily translated function or a bulk memory operation that needs
        // more fuel than is left fails the call instead of pausing it, so
        // translate everything up front and make copies effectively free.
        config.compilation_mode(wasmi::CompilationMode::Eager);
        config.fuel_cost(wasmi::CustomFuelCosts {
            bytes_copied_per_fuel: u32::MAX,
            fuel_per_bytes_translated: 0,
            fuel_per_bytes_validated: 0,
        });
        let engine = wasmi::Engine::new(&config);
        let module = wasmi::Module::new(&engine, WASM_RUNNER)
            .map_err(|error| format!("wasm module: {error}"))?;
        Ok(WasmEngineState {
            engine,
            baseline_memory: wasmi_baseline_memory(&module),
            module,
            linker: OnceLock::new(),
        })
    });
    match state {
        Ok(state) => Ok(state),
        Err(error) => Err(error.clone()),
    }
}

#[cfg(feature = "wasm")]
fn wasmi_linker(
    state: &'static WasmEngineState,
) -> Result<&'static wasmi::Linker<WasmData>, String> {
    match state.linker.get_or_init(|| {
        let mut linker = wasmi::Linker::new(&state.engine);
        wasmi_wasi::add_to_linker(&mut linker, |data: &mut WasmData| &mut data.wasi)
            .map_err(|error| format!("wasm wasi: {error}"))?;
        Ok(linker)
    }) {
        Ok(linker) => Ok(linker),
        Err(error) => Err(error.clone()),
    }
}

#[cfg(feature = "wasm")]
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
    wasm_run(&body, limits)
}

#[cfg(feature = "wasm")]
fn wasm_run(body: &[u8], limits: crate::view::ViewLimits) -> Result<Value, String> {
    if WASM_RUNTIME.load(Ordering::SeqCst) == 1 {
        #[cfg(feature = "wasmtime")]
        {
            return crate::js_wasmtime::request(body, limits);
        }
        #[cfg(not(feature = "wasmtime"))]
        {
            return Err(
                "the wasmtime backend is not compiled; rebuild with `cargo build --features wasmtime`"
                    .to_string(),
            );
        }
    }
    wasmi_request(body, limits)
}

#[cfg(feature = "wasm")]
fn wasmi_request(body: &[u8], limits: crate::view::ViewLimits) -> Result<Value, String> {
    let state = wasm_engine()?;
    // The linker is immutable after setup; build it once per process instead of
    // resolving every WASI import on each request.
    let linker = wasmi_linker(state)?;
    let stdout = wasmi_wasi::wasi_common::pipe::WritePipe::new_in_memory();
    let wasi = wasmi_wasi::WasiCtxBuilder::new()
        .stdin(Box::new(wasmi_wasi::wasi_common::pipe::ReadPipe::from(
            body.to_vec(),
        )))
        .stdout(Box::new(stdout.clone()))
        .build();
    let data = WasmData {
        wasi,
        limits: wasmi::StoreLimitsBuilder::new()
            .memory_size(wasm_memory_cap(state.baseline_memory, limits, body.len()))
            .trap_on_grow_failure(true)
            .build(),
    };
    let mut store = wasmi::Store::new(&state.engine, data);
    store.limiter(|data| &mut data.limits);
    let instance = linker
        .instantiate_and_start(&mut store, &state.module)
        .map_err(|error| format!("wasm instantiate: {error}"))?;
    let start = instance
        .get_typed_func::<(), ()>(&store, "_start")
        .map_err(|error| format!("wasm start: {error}"))?;
    run_with_deadline(&mut store, &start, limits.timeout)?;
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

/// Run `start` in slices of fuel and check the wall clock between slices.
/// wasmi has no interrupt, so fuel is the only way to regain control; the
/// slice adapts so a check happens roughly every `SLICE_TARGET` whatever the
/// interpreter's speed (debug builds are orders of magnitude slower).
#[cfg(feature = "wasm")]
fn run_with_deadline(
    store: &mut wasmi::Store<WasmData>,
    start: &wasmi::TypedFunc<(), ()>,
    timeout: Duration,
) -> Result<(), String> {
    const SLICE_TARGET: Duration = Duration::from_millis(20);
    let started = Instant::now();
    let deadline = started + timeout;
    const MIN_SLICE: u64 = 10_000;
    let mut slice: u64 = 1_000_000;
    let mut slice_started = started;
    store
        .set_fuel(slice)
        .map_err(|error| format!("wasm fuel: {error}"))?;
    let mut call = start
        .call_resumable(&mut *store, ())
        .map_err(|error| format!("wasm run: {error}"))?;
    loop {
        match call {
            wasmi::TypedResumableCall::Finished(()) => return Ok(()),
            wasmi::TypedResumableCall::HostTrap(trap) => {
                return Err(format!("wasm run: {}", trap.host_error()))
            }
            wasmi::TypedResumableCall::OutOfFuel(pending) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(format!(
                        "timed out after {} ms: wasm run exceeded its deadline",
                        timeout.as_millis()
                    ));
                }
                let took = now
                    .saturating_duration_since(slice_started)
                    .as_nanos()
                    .max(1);
                let scaled = u128::from(slice) * SLICE_TARGET.as_nanos() / took;
                slice = (scaled.clamp(u128::from(slice / 4), u128::from(slice) * 4) as u64)
                    .clamp(MIN_SLICE, 4_000_000_000);
                store
                    .set_fuel(slice.max(pending.required_fuel()))
                    .map_err(|error| format!("wasm fuel: {error}"))?;
                slice_started = Instant::now();
                call = pending
                    .resume(&mut *store)
                    .map_err(|error| format!("wasm run: {error}"))?;
            }
        }
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
        command.arg("--js-worker");
        let mut worker = Worker::spawn_command(command)?;
        // The worker announces its confinement before it serves requests.
        let hello = worker.await_hello(WORKER_STARTUP_TIMEOUT)?;
        static LOGGED: std::sync::Once = std::sync::Once::new();
        LOGGED.call_once(|| {
            let landlock = hello
                .get("landlock")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            if landlock == "fully enforced" || cfg!(not(target_os = "linux")) {
                tracing::info!("js worker confinement: landlock {landlock}");
            } else {
                tracing::warn!(
                    "js worker confinement: landlock {landlock}; relying on seccomp and rlimits"
                );
            }
        });
        Ok(worker)
    }

    /// Start `command` as a worker: cleared environment, `/` cwd, piped
    /// stdin/stdout and the resource limits.
    fn spawn_command(mut command: Command) -> Result<Worker, String> {
        command
            .env_clear()
            .current_dir("/")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            unsafe {
                command.pre_exec(move || {
                    // The debug binary maps ~1 GB of virtual address space
                    // before QuickJS allocates anything, so leave generous
                    // headroom. QuickJS itself enforces the per-request heap
                    // limit. macOS rejects RLIMIT_AS with EINVAL, so it is
                    // skipped there; the worker still gets the other limits
                    // and, on macOS, has no seccomp/Landlock confinement (WASM
                    // is the portable containment option).
                    #[cfg(not(target_os = "macos"))]
                    {
                        let memory = 4 * 1024 * 1024 * 1024u64 as libc::rlim_t;
                        let limit = libc::rlimit {
                            rlim_cur: memory,
                            rlim_max: memory,
                        };
                        if libc::setrlimit(libc::RLIMIT_AS, &limit) != 0 {
                            return Err(std::io::Error::last_os_error());
                        }
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
                    #[cfg(target_os = "linux")]
                    {
                        // No PR_SET_PDEATHSIG: it fires when the *thread*
                        // that forked exits, and pool workers are forked from
                        // blocking-pool threads that exit when idle. The
                        // parent holds the only write end of the worker's
                        // stdin, so its death closes the pipe and the worker
                        // exits on EOF (a request in flight ends at its own
                        // deadline, then hits EOF or a closed stdout).
                        let _ = libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);
                    }
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

    /// Read the hello frame, killing the worker if it does not arrive within
    /// `timeout`.
    fn await_hello(&mut self, timeout: Duration) -> Result<Value, String> {
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let pid = self.pid;
        let watchdog = std::thread::spawn(move || {
            if let Err(std::sync::mpsc::RecvTimeoutError::Timeout) = done_rx.recv_timeout(timeout) {
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                }
            }
        });
        let hello = read_frame(&mut self.stdout);
        let _ = done_tx.send(());
        let _ = watchdog.join();
        match hello {
            Ok(Some(body)) => serde_json::from_slice(&body)
                .map_err(|error| format!("worker hello: {error}"))
                .inspect_err(|_| self.kill()),
            _ => {
                self.kill();
                Err("worker did not start".to_string())
            }
        }
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
                last_error = if error.timed_out {
                    format!("worker timed out: {}", error.message)
                } else {
                    error.message
                };
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
pub fn run_worker_stdio(landlock: &str) -> Result<(), String> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();
    let hello =
        serde_json::to_vec(&json!({"landlock": landlock})).map_err(|error| error.to_string())?;
    write_frame(&mut output, &hello)?;
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
        Some("reduce") => {
            // A rereduce carries null keys (CouchDB passes null, not []).
            let keys: Vec<Value> = match request.get("keys") {
                None | Some(Value::Null) => Vec::new(),
                Some(keys) => match serde_json::from_value(keys.clone()) {
                    Ok(keys) => keys,
                    Err(error) => {
                        return json!({"ok": false, "error": format!("reduce keys: {error}")})
                    }
                },
            };
            let values: Vec<Value> = match request.get("values") {
                Some(values) => match serde_json::from_value(values.clone()) {
                    Ok(values) => values,
                    Err(error) => {
                        return json!({"ok": false, "error": format!("reduce values: {error}")})
                    }
                },
                None => return json!({"ok": false, "error": "reduce values are missing"}),
            };
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
    let result = match result {
        Err(error) if started.elapsed() >= limits.timeout => Err(format!(
            "timed out after {} ms: {error}",
            limits.timeout.as_millis()
        )),
        other => other,
    };
    match result {
        Ok(value) => json!({"ok": true, "value": value}),
        Err(error) => json!({"ok": false, "error": error}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[cfg(feature = "wasm")]
    fn wasmi_map(source: &str, limits: crate::view::ViewLimits) -> Result<Value, String> {
        let body = serde_json::to_vec(&json!({
            "kind": "map",
            "source": source,
            "docs": [["d0", "{\"_id\":\"d0\"}"]],
            "timeout_ms": limits.timeout.as_millis() as u64,
            "memory": limits.memory,
        }))
        .expect("body");
        wasmi_request(&body, limits)
    }

    #[test]
    #[cfg(feature = "wasm")]
    fn wasmi_enforces_the_view_memory_limit() {
        let limits = crate::view::ViewLimits {
            timeout: Duration::from_secs(60),
            memory: 4 * 1024 * 1024,
        };
        // Within the limit. Sizes stay small: building strings in a debug
        // build of the interpreter costs seconds per megabyte.
        let small = "function(doc){ emit('x'.repeat(256 * 1024).length, null); }";
        wasmi_map(small, limits).expect("a small allocation fits");
        // Far over the limit, but under the 128 MiB cap the host used to apply
        // whatever the configured limit was.
        let large = "function(doc){ emit('x'.repeat(12 * 1024 * 1024).length, null); }";
        let error = wasmi_map(large, limits).expect_err("12 MiB must exceed a 4 MiB limit");
        assert!(!error.is_empty());
    }

    #[test]
    #[cfg(feature = "wasm")]
    fn wasmi_timeout_is_wall_clock() {
        let limits = crate::view::ViewLimits {
            timeout: Duration::from_millis(150),
            memory: 64 * 1024 * 1024,
        };
        let started = Instant::now();
        let error = wasmi_map("function(doc){ while (true) {} }", limits)
            .expect_err("an endless loop must be stopped");
        assert!(error.contains("timed out"), "{error}");
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "deadline overshoot: {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn a_worker_outlives_the_thread_that_spawned_it() {
        // Pool workers are spawned from whichever thread needs one, which may
        // be a blocking-pool thread that exits when idle. `cat` stands in for
        // the worker: it only ends when its stdin closes.
        let mut worker = std::thread::spawn(|| Worker::spawn_command(Command::new("cat")))
            .join()
            .expect("spawning thread")
            .expect("spawn");
        std::thread::sleep(Duration::from_millis(300));
        let exited = worker.child.try_wait().expect("try_wait");
        worker.kill();
        assert!(
            exited.is_none(),
            "the worker died with its spawning thread: {exited:?}"
        );
    }

    #[test]
    fn a_worker_that_never_announces_itself_is_killed() {
        let mut command = Command::new("sleep");
        command.arg("30");
        let mut worker = Worker::spawn_command(command).expect("spawn");
        let started = Instant::now();
        let error = worker
            .await_hello(Duration::from_millis(300))
            .expect_err("no hello was sent");
        assert!(error.contains("did not start"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn worker_reduce_rejects_malformed_keys_and_values() {
        let reduce = |keys: Value, values: Value, rereduce: bool| {
            handle_worker_request(json!({
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
        let missing = handle_worker_request(
            json!({"kind": "reduce", "source": "function(k, v, r){ return 1; }"}),
        );
        assert_eq!(missing["ok"], false, "{missing}");
    }

    #[test]
    #[cfg(feature = "wasm")]
    fn wasm_reduce_rejects_malformed_keys_and_values() {
        let limits = crate::view::ViewLimits {
            timeout: Duration::from_secs(60),
            memory: 64 * 1024 * 1024,
        };
        let reduce = |keys: Option<Value>, values: Option<Value>, rereduce: bool| {
            let mut request = serde_json::Map::new();
            request.insert(
                "source".into(),
                json!("function(k, v, r){ return sum(v); }"),
            );
            request.insert("rereduce".into(), json!(rereduce));
            if let Some(keys) = keys {
                request.insert("keys".into(), keys);
            }
            if let Some(values) = values {
                request.insert("values".into(), values);
            }
            wasm_request("reduce", Value::Object(request), limits)
        };
        assert_eq!(
            reduce(Some(json!([[1, "a"]])), Some(json!([3])), false).expect("valid reduce"),
            json!(3)
        );
        // A rereduce carries null keys.
        assert_eq!(
            reduce(Some(Value::Null), Some(json!([1, 2])), true).expect("rereduce"),
            json!(3)
        );
        for (keys, values) in [
            (json!("nope"), json!([1])),
            (json!({"a": 1}), json!([1])),
            (json!([]), json!("nope")),
            (json!([]), Value::Null),
            (json!([[1, "a"]]), json!({"a": 1})),
        ] {
            let error = reduce(Some(keys.clone()), Some(values.clone()), false)
                .expect_err(&format!("keys {keys} values {values} must be rejected"));
            assert!(
                error.contains("reduce keys") || error.contains("reduce values"),
                "{error}"
            );
        }
        let error = reduce(None, None, false).expect_err("missing values");
        assert!(error.contains("reduce values are missing"), "{error}");
    }

    #[cfg(target_os = "linux")]
    fn errno_after(result: libc::c_long) -> i32 {
        if result == -1 {
            std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
        } else {
            0
        }
    }

    /// Runs in a child process (see the test below): installs the filter and
    /// prints the errno of each probe. Every probe is harmless when allowed:
    /// signal 0, nonexistent paths, an invalid fd or a zero-sized argument.
    #[cfg(target_os = "linux")]
    #[test]
    fn seccomp_probe_child() {
        if std::env::var_os("DOUCHDB_SECCOMP_PROBE").is_none() {
            return;
        }
        install_seccomp().expect("install seccomp");
        let parent = unsafe { libc::getppid() } as libc::c_long;
        let me = unsafe { libc::getpid() } as libc::c_long;
        let tid = unsafe { libc::syscall(libc::SYS_gettid) };
        let path = c"/nonexistent-douchdb/a".as_ptr();
        let other = c"/nonexistent-douchdb/b".as_ptr();
        let at = libc::AT_FDCWD as libc::c_long;
        let probes: Vec<(&str, i32)> = unsafe {
            vec![
                (
                    "tgkill_parent",
                    errno_after(libc::syscall(libc::SYS_tgkill, parent, parent, 0)),
                ),
                (
                    "tkill_parent",
                    errno_after(libc::syscall(libc::SYS_tkill, parent, 0)),
                ),
                (
                    "pidfd_send_signal",
                    errno_after(libc::syscall(libc::SYS_pidfd_send_signal, -1, 0, 0, 0)),
                ),
                ("clone3", errno_after(libc::syscall(libc::SYS_clone3, 0, 0))),
                (
                    "linkat",
                    errno_after(libc::syscall(libc::SYS_linkat, at, path, at, other, 0)),
                ),
                (
                    "renameat2",
                    errno_after(libc::syscall(libc::SYS_renameat2, at, path, at, other, 0)),
                ),
                (
                    "symlinkat",
                    errno_after(libc::syscall(libc::SYS_symlinkat, path, at, other)),
                ),
                (
                    "fchmodat",
                    errno_after(libc::syscall(libc::SYS_fchmodat, at, path, 0o600)),
                ),
                (
                    "fchownat",
                    errno_after(libc::syscall(libc::SYS_fchownat, at, path, 0, 0, 0)),
                ),
                (
                    "mknodat",
                    errno_after(libc::syscall(libc::SYS_mknodat, at, path, 0o600, 0)),
                ),
                (
                    "openat",
                    errno_after(libc::syscall(libc::SYS_openat, at, path, 0, 0)),
                ),
                // Control: the worker may signal itself (abort).
                (
                    "tgkill_self",
                    errno_after(libc::syscall(libc::SYS_tgkill, me, tid, 0)),
                ),
            ]
        };
        for (name, errno) in probes {
            println!("\nprobe:{name}={errno}");
        }
        std::process::exit(0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn seccomp_denies_signals_namespaces_and_filesystem_mutation() {
        let output = Command::new(std::env::current_exe().expect("exe"))
            .args([
                "--exact",
                "js::tests::seccomp_probe_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("DOUCHDB_SECCOMP_PROBE", "1")
            .output()
            .expect("run probe");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let probes: std::collections::BTreeMap<&str, i32> = stdout
            .lines()
            .filter_map(|line| line.strip_prefix("probe:")?.split_once('='))
            .filter_map(|(name, errno)| Some((name, errno.parse().ok()?)))
            .collect();
        assert!(!probes.is_empty(), "probe child failed: {output:?}");
        for (name, errno) in &probes {
            if *name == "tgkill_self" {
                assert_eq!(*errno, 0, "the worker must be able to signal itself");
            } else {
                assert_eq!(*errno, libc::EPERM, "{name} is not denied: {probes:?}");
            }
        }
    }

    #[test]
    fn wasm_runner_matches_the_checked_in_checksum() {
        let bytes = include_bytes!("../wasm/qjs-runner.wasm");
        let digest = Sha256::digest(bytes);
        let actual: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        let expected = include_str!("../wasm/qjs-runner.sha256")
            .split_whitespace()
            .next()
            .unwrap_or_default();
        assert_eq!(
            actual, expected,
            "rebuild wasm/qjs-runner.wasm or update wasm/qjs-runner.sha256"
        );
    }
}

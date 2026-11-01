# Sandboxing QuickJS

douchdb executes design-document JavaScript in an embedded QuickJS runtime.
That code comes from database documents, so in a multi-tenant deployment it is
untrusted input. This document describes the implemented boundary, what it does
and does not protect, and the measured trade-offs. The short version:

- **The default in-process QuickJS is a resource boundary, not a security
  boundary.**
- **`--js-sandbox process` contains crashes and memory-safety bugs in a pooled
  worker process with rlimits and seccomp.**
- **`--js-sandbox wasm` is the portable, memory-safe option: QuickJS runs as
  WASI under `wasmi` with fuel and linear-memory limits.**
- The same JavaScript API and conformance tests run behind a small engine
  boundary so modes can change without changing view/search semantics.

## Capability guarantee

No engine mode gives design-document JavaScript network or filesystem access:

- The wrappers expose only CouchDB helpers (`emit`, `sum`, `log`, `isArray`,
  `toJSON` or `index`). There is no `require`, `process`, `fetch`,
  `XMLHttpRequest`, `WebSocket`, `fs`, `os`, `net`, `console` or module loader.
  `quickjs_sandbox_exposes_no_host_globals` checks this in native, process and
  wasm modes.
- **Native:** there are no host callbacks at all, so JS has no file or socket
  objects to call. This is still an in-process engine and not crash-safe.
- **Process:** the worker starts with a cleared environment, `/` cwd and no
  database/HTTP handles. A seccomp denylist rejects `open`, `openat`,
  `socket`, `connect`, `execve` and related syscalls; a best-effort Landlock
  layer denies filesystem access across all threads. `process_worker_installs_the_seccomp_filter`
  checks the filter is active.
- **WASM:** the WASI context has no preopens, environment or args. The module's
  only host pipes are the request stdin and response stdout, plus WASI
  clock/random; the store has fuel and a 128 MiB linear-memory cap.

The OS-level layers are defense in depth: even a memory-corruption bug in
QuickJS should not turn into host file or network access. They are not a claim
that native mode is safe for hostile design documents.

## Threat model

The attacker can create or modify a design document and make the server run it:

- `views.*.map` and `views.*.reduce` on `_view` queries and index builds;
- `indexes.*.index` on search index builds;
- filter functions if JavaScript changes filters are added later.

The same attacker may be a tenant in a shared database, or may have replicated a
malicious design document into the database. The attacker does not have shell
access to the host and cannot normally execute native code.

What must be protected:

1. **Host files and network.** JavaScript must not read `data/`, open sockets,
   read environment variables or spawn processes.
2. **Process integrity.** A malformed script must not be able to crash the whole
   server, corrupt other requests' memory, or escape into the Rust process.
3. **Availability.** Infinite loops, allocation bombs, deep recursion and
   pathological regexps must be bounded.
4. **Cross-request isolation.** One query's globals, mutations or prototypes
   must not affect another query.

## Current implementation

In native mode, both `src/view.rs` and `src/search.rs` create one
`rquickjs::Runtime` and one `Context::full` per query, then evaluate a
wrapper that defines the CouchDB helpers (`emit`, `sum`, `log`, `isArray`, `toJSON` or `index`). The runtime is
dropped when the query finishes.

Current controls:

| Control | Where | What it limits |
|---|---|---|
| Fresh runtime/context per query | `view::engine`, `search::engine` | Cross-query JS state |
| `set_memory_limit` | both | QuickJS heap allocations |
| `set_max_stack_size(512 KiB)` | both | JS call stack / C stack growth |
| `set_interrupt_handler` | both | Wall-clock runaway bytecode loops |
| Batch size / byte caps | `run_map_json` | Input to one QuickJS call |
| 16 MiB output cap | `js::check_output` | Rust-side result parsing |
| `catch_unwind` around calls | `js::guard` | Rust panics from the bindings |
| No host callbacks | wrapper | No Rust functions are exposed to JS |
| No module loader | default rquickjs runtime | `import()` has nowhere to load from |
| Native fast paths | `NativeMap` | Simple maps never enter QuickJS |
| `--js-sandbox process` | `src/js.rs` | Runs QuickJS in a pooled separate process with cleared environment, `/` cwd, rlimits, seccomp and no database handles |
| `--js-sandbox wasm` | `src/js.rs`, `wasm/` | Runs QuickJS as WASI under `wasmi` with fuel, linear-memory and output limits |

Observed behaviour:

- A map function can mutate the `doc` object it receives, but only the emitted
  value sees the mutation; `include_docs` returns the stored document.
- `emit.constructor('return this')()` returns the QuickJS global object. It does
  not reach the Rust host because there are no host functions on that object.
  This matches CouchDB's QuickJS expectation in
  `test/elixir/test/view_sandboxing_test.exs`.
- `gc()` and `map_funs.push()` are undefined and fail the query.
- A map exception is returned as `500 view_error`; a missing global does not
  silently produce zero rows.

What the default in-process controls do **not** cover (the process and wasm
modes address the crash, memory-safety and capability points):

- **Memory safety in QuickJS/rquickjs.** QuickJS is C; rquickjs is an unsafe FFI
  wrapper. A memory-safety bug in the interpreter can read or write host memory.
- **Crash containment.** A segfault or abort in QuickJS kills the server
  process, taking every database and request with it.
- **Host allocations outside the QuickJS heap.** `serde_json` input/output,
  batch strings and result vectors are allocated by Rust and are not counted by
  `set_memory_limit`.
- **Native CPU paths.** The interrupt handler is polled by the interpreter. A
  pathological regexp or C-level operation may run for a while before the
  handler gets a chance to stop it.
- **`Context::full` semantics.** `full` installs all standard intrinsics,
  including `eval`. `Context::custom` can omit `Eval`, but the base objects
  always include `Function`, and `({}).constructor.constructor` still reaches
  it. Removing `eval` alone is not a security boundary; it only reduces one
  route to dynamic compilation.

The conclusion is that the default in-process setup is appropriate for a
trusted single-operator deployment, but it should not be sold as a sandbox for
untrusted tenants. Use `--js-sandbox process` on Linux or `--js-sandbox wasm`
for a portable memory-safe boundary. The process path also installs a
best-effort Landlock filesystem layer on top of its seccomp denylist.

## Option A: harden the in-process runtime

Lowest effort, no new process or wasm build:

- use `Context::custom` with only the intrinsics the wrappers need (JSON,
  RegExp, Date, Map/Set, TypedArrays) and omit `Eval`;
- install a loader that always rejects imports;
- cap emitted rows, output bytes and document counts;
- run each query inside `std::panic::catch_unwind` on a dedicated thread;
- lower memory/stack defaults and add a hard output cap;
- add the CouchDB sandboxing cases to the conformance suite.

Status: the output cap and `catch_unwind` containment are implemented;
trimming intrinsics and installing a rejecting loader are not. This reduces
accidental damage but does not fix memory-unsafety or crash containment.
`catch_unwind` cannot catch a segfault or `abort()`. Treat this as hardening,
not a sandbox.

## Option B: worker process (implemented as `--js-sandbox process`)

Start douchdb with:

```sh
douchdb --js-sandbox process ...
```

Each map/reduce/search-index evaluation is sent to a small pool of child
processes (``douchdb --js-worker``) that have no database handles, no cluster
secret and no inherited environment. This is the same containment model CouchDB
uses with `couchjs`. The pool defaults to two workers and is controlled by
`--js-workers N`.

What is implemented:

- length-prefixed JSON frames over stdin/stdout to a pooled child, with the
  source, documents/keys/values, timeout and memory limit;
- a small worker pool (`--js-workers`, default 2) so processes are reused;
- a watchdog that kills a worker after the request deadline + 2 s;
- `env_clear()` and `current_dir("/")`, so the worker does not inherit the
  cluster secret, data paths or CWD;
- `RLIMIT_AS`, `RLIMIT_CORE`, `RLIMIT_FSIZE` and `RLIMIT_NOFILE`; the
  per-request watchdog is the CPU backstop for pooled workers;
- `PR_SET_NO_NEW_PRIVS`, a Linux seccomp denylist for file, network, exec,
  process-inspection and io_uring syscalls, and a best-effort Landlock layer
  that denies filesystem access across all threads when the kernel supports it;
- a hidden `--js-worker` mode that runs the normal map/reduce/index code in the
  child; the parent's sandbox flag is false in the child, so there is no
  recursive spawning;
- the existing 16 MiB output cap and QuickJS memory/stack/interrupt limits;
- native `NativeMap` fast paths remain in the server process because they are
  Rust, not JavaScript.

The default remains `--js-sandbox native`. Use `process` for deployments where
design documents are untrusted.

Platform note:

- Process-mode syscall/filesystem confinement (seccomp and Landlock) is
  Linux-specific. On other platforms, process mode still gets a cleared
  environment, process isolation and rlimits where available, but a hard
  portable boundary should use `--js-sandbox wasm`. A kernel without Landlock
  falls back to the seccomp denylist.

Run the mode tests:

```sh
cargo test --test conformance quickjs_process_sandbox
cargo test --test conformance quickjs_wasm_sandbox
```

### Shape

```text
douchdb (HTTP)  -- framed request -->  douchdb-js worker
                <-- framed result  --
```

A request carries the source, the operation (`map`, `reduce`, `search_index`),
the documents/keys/values and the limits. The response carries rows/values or a
structured error. Framing can be length-prefixed JSON or MessagePack over
stdin/stdout, a Unix socket or a socketpair. In the implementation the child is
the same binary in a hidden `--js-worker` mode: it runs only the map/reduce/
index path and does not open databases or construct the HTTP state.

### Isolation

- **Process boundary:** a crash or QuickJS memory bug kills only the worker.
  The supervisor restarts it and fails the in-flight request.
- **Resource limits:** `setrlimit` for address space, CPU, file size and open
  files; the supervisor enforces a wall-clock deadline and kills a stuck
  worker.
- **Linux:** `PR_SET_NO_NEW_PRIVS`, a seccomp filter that denies `socket`,
  `connect`, `open`, `execve`, `ptrace` and similar calls, or a Landlock ruleset
  that grants no filesystem access. If the deployment already runs containers,
  run the worker in a minimal container/namespace.
- **Other platforms:** process plus rlimits is still useful; OS-specific
  sandboxing can be added behind a feature flag.
- **No secrets:** the worker gets only the source and document JSON; it never
  receives the cluster secret, database handles or environment variables.

### Cost

- IPC serialization and a process hop per batch. A small pool of warmed workers
  amortizes startup; the existing view code already batches documents.
- Linux-specific sandbox code is maintenance. A portable fallback is still
  required.
- The worker must be supervised, restarted and drained on shutdown.

This is the best risk reduction per unit of work for a Linux server and keeps
the current QuickJS semantics and performance. It does not make QuickJS
memory-safe; it makes a QuickJS compromise unable to reach the host.

## Option C: QuickJS in WebAssembly (implemented as `--js-sandbox wasm`)

Compile QuickJS to `wasm32-wasip1` and run the module inside `wasmi`. The host
never exposes a native function to the JS engine; the only interface is linear
memory plus WASI stdio. Start douchdb with:

```sh
douchdb --js-sandbox wasm ...
```

The checked-in `wasm/qjs-runner.wasm` is built from
`wasm/qjs-runner/` with the maintained `rquickjs` crate and a WASI SDK. The host
compiles the module once,
then creates a fresh `Store`/instance per request with:

- a WASI context containing only the request on stdin and an in-memory stdout,
  with no preopens, environment or args;
- a 128 MiB linear-memory cap (`StoreLimits`);
- a fuel budget derived from the request timeout (2 million fuel per
  millisecond, minimum 500 million);
- a JSON request/response envelope matching the process worker.

The runner has no filesystem or network imports that can reach the host; a wasm
trap (including fuel exhaustion or memory growth failure) fails only that
request. `NativeMap` fast paths still run in the server process.

### Shape

- Build a small QuickJS runner once, pinned to a QuickJS version and WASI SDK:
  - `qjs_run(source, operation, input_json) -> output_json`
  - an allocator/free pair for the host to copy strings.
- Host runtime: `wasmi` (pure Rust, simpler dependency, interpreter speed);
  `wasmtime` remains an option for higher throughput.
- Compile the module once at startup or embed a precompiled `.wasm`; instantiate
  a fresh `Store`/instance per request (or use a small pool with a fresh runtime
  inside each instance).
- Memory: `StoreLimits` caps linear memory, tables and instances. The QuickJS
  heap lives inside that linear memory; the current runner does not also call
  `set_memory_limit`, so the linear-memory ceiling is the heap bound.
- CPU: the implementation uses fuel (deterministic instruction budgets).
  Epoch interruption remains an option for a wall-clock-style deadline.
- Capability surface: no preopens, no WASI sockets, no environment. Allow only
  the WASI calls QuickJS actually needs (clock/random/stdio); ideally replace
  them with deterministic host stubs.
- Crash containment: a wasm trap or out-of-bounds access aborts the instance,
  not the server.

### Build and supply chain

The runner uses the maintained `rquickjs` crate and a pinned WASI SDK. The
checked-in artifact is reproducible with the commands in `wasm/README.md`; the
source is in `wasm/qjs-runner/`. The checked-in artifact checksum is enforced
by `js::tests::wasm_runner_matches_the_checked_in_checksum`.

### Performance

WASM adds a module boundary, linear-memory copies and a slower interpreter
(QuickJS compiled to wasm is not the same as native QuickJS). The JSON bridge is
the same shape as today, but every input/output crosses linear memory. This
is measured in the table below; it is not a default candidate for
throughput-sensitive deployments yet. Keep the native `NativeMap` fast path
regardless.

WASM is the right long-term default if the deployment needs a portable,
memory-safe boundary or runs untrusted tenants on shared infrastructure. It is
not a drop-in replacement for the current `rquickjs` calls.

### Measured cost

`scripts/js_sandbox_bench.py` builds 2,000 documents, forces the same
non-native map/reduce/search functions through each engine and records cold
index builds plus 10 warm queries. Release build, Linux x86_64, 16 CPUs:

| Metric | native | process | wasm |
|---|---:|---:|---:|
| cold map (2,000 docs) | 14.6 ms | 29.8 ms | 281 ms |
| cold custom reduce | 13.2 ms | 27.3 ms | 307 ms |
| cold search index | 55.9 ms | 79.8 ms | 380 ms |
| warm map p50 | 0.30 ms | 0.29 ms | 0.29 ms |
| warm custom reduce p50 | 2.78 ms | 5.44 ms | 51.3 ms |
| warm search query p50 | 1.78 ms | 1.91 ms | 1.94 ms |

Warm map and search queries use the persisted index and do not enter the
engine, so they are effectively identical. The differences are in cold index
builds and custom reduces: process mode adds about 1.5–2× over native, while
wasm adds roughly 7× for search and 20–25× for map/reduce because `wasmi`
interprets QuickJS compiled to wasm.
That is the expected trade-off for a memory-safe portable boundary, not a
reason to make wasm the default.

```sh
cargo build --release
python3 scripts/js_sandbox_bench.py --docs 2000
```

The JSON is written to `target/js-sandbox-bench.json`.

## Option D: a memory-safe Rust JavaScript engine

Boa or another pure-Rust engine would remove the C memory-safety class
entirely, but currently has different language semantics, performance and
compatibility from QuickJS. It is worth a spike for a future engine option, not
the first move for CouchDB compatibility.

## Recommendation and status

Use a `JsEngine` boundary and migrate in stages:

1. **Done: harden and measure.**
   - Added CouchDB sandbox conformance cases and a deadline test.
   - Added a 16 MiB output cap and Rust panic containment.
   - Documented that in-process QuickJS is not a security boundary.
2. **Done: worker process.**
   - Implemented `--js-sandbox process` with env clearing, cwd isolation,
     rlimits, no database handles, a seccomp denylist, a best-effort Landlock
     layer, a parent-side kill watchdog and a warmed worker pool.
   - Recommended for Linux deployments that execute untrusted design docs.
3. **Done: WASM engine.**
   - Checked in a QuickJS WASI runner and run it under `wasmi`.
   - Enforced linear memory, fuel and output limits.
   - Exposed it as `--js-sandbox wasm` and covered it with the same view,
     reduce, search and deadline tests.
   - Built with the maintained `rquickjs` crate; benchmark recorded below.
     Choose a default per deployment.

This gets crash containment and a portable memory-safe engine without pretending
that removing `eval` or setting a memory limit is a security sandbox. The
default remains `native`; `process` is the Linux containment boundary and
`wasm` is the portable option.

## Validation matrix

Every engine must pass the same cases:

- map/reduce/search results and CouchDB revision/view semantics;
- mutating `doc` does not persist;
- `emit.constructor('return this')()` cannot reach a host object;
- `gc`, `map_funs`, `map_results` and host globals are unavailable;
- infinite loop and deep recursion hit the deadline without hanging the server;
- allocation bomb hits the memory limit;
- pathological regexp is bounded by the deadline;
- oversized output is rejected;
- a worker crash/trap fails one request and leaves the server serving;
- one request cannot observe another request's globals or prototypes;
- malformed source and thrown exceptions return stable `view_error` /
  `search_index_error` shapes.

## Open decisions

- Is the deployment single-tenant/trusted, or must design docs be treated as
  hostile? That decides whether the worker process is mandatory.
- Resolved: process mode is Linux-hardened; wasm is the cross-platform
  memory-safe option.
- Resolved: the runner source and the built `.wasm` are checked in, it uses
  the maintained `rquickjs` crate, and `cargo test` enforces the artifact
  checksum.
- Cost is measured in the table above; choose the mode per deployment and
  threat model.
- Resolved for the worker path: a warmed pool with a fresh QuickJS runtime per
  request; `--js-workers` controls its size. One process per request remains a
  stronger-isolation option if needed.

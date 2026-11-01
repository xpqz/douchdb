# Sandboxing QuickJS

douchdb executes design-document JavaScript in an embedded QuickJS runtime.
That code comes from database documents, so in a multi-tenant deployment it is
untrusted input. This document describes the current boundary, what it does and
does not protect, and the options for a real sandbox. The short version:

- **In-process QuickJS is a resource boundary, not a security boundary.**
- **A separate worker process is the smallest change that contains crashes and
  memory-safety bugs.**
- **A WASM build of QuickJS is the strongest portable boundary, but it needs a
  pinned toolchain and a new runtime dependency.**
- Keep the same JavaScript API and conformance tests behind a `JsEngine`
  abstraction so the engine can change without changing view/search semantics.

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

Both `src/view.rs` and `src/search.rs` create one `rquickjs::Runtime` and one
`Context::full` per query, then evaluate a wrapper that defines the CouchDB
helpers (`emit`, `sum`, `log`, `isArray`, `toJSON` or `index`). The runtime is
dropped when the query finishes.

Current controls:

| Control | Where | What it limits |
|---|---|---|
| Fresh runtime/context per query | `view::engine`, `search::engine` | Cross-query JS state |
| `set_memory_limit` | both | QuickJS heap allocations |
| `set_max_stack_size(512 KiB)` | both | JS call stack / C stack growth |
| `set_interrupt_handler` | both | Wall-clock runaway bytecode loops |
| Batch size / byte caps | `run_map_json` | Input to one QuickJS call |
| No host callbacks | wrapper | No Rust functions are exposed to JS |
| No module loader | default rquickjs runtime | `import()` has nowhere to load from |
| Native fast paths | `NativeMap` | Simple maps never enter QuickJS |

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

What the current controls do **not** cover:

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

The conclusion is that the current setup is appropriate for a trusted
single-operator deployment, but it should not be sold as a sandbox for
untrusted tenants.

## Option A: harden the in-process runtime

Lowest effort, no new process or wasm build:

- use `Context::custom` with only the intrinsics the wrappers need (JSON,
  RegExp, Date, Map/Set, TypedArrays) and omit `Eval`;
- install a loader that always rejects imports;
- cap emitted rows, output bytes and document counts;
- run each query inside `std::panic::catch_unwind` on a dedicated thread;
- lower memory/stack defaults and add a hard output cap;
- add the CouchDB sandboxing cases to the conformance suite.

This reduces accidental damage but does not fix memory-unsafety or crash
containment. `catch_unwind` cannot catch a segfault or `abort()`. Treat this as
hardening, not a sandbox.

## Option B: worker process (recommended first step)

Run QuickJS in a separate `douchdb-js` process and keep the HTTP server free of
untrusted native code. This is the model CouchDB itself uses with `couchjs`.

### Shape

```text
douchdb (HTTP)  -- framed request -->  douchdb-js worker
                <-- framed result  --
```

A request carries the source, the operation (`map`, `reduce`, `search_index`),
the documents/keys/values and the limits. The response carries rows/values or a
structured error. Framing can be length-prefixed JSON or MessagePack over
stdin/stdout, a Unix socket or a socketpair. The worker is single-purpose and
has no CouchDB code, database paths or network client.

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

## Option C: QuickJS in WebAssembly

Compile QuickJS to `wasm32-wasip1` (or a freestanding WASM target) and run the
module inside a WebAssembly runtime. The host never exposes a native function to
the JS engine; the only interface is linear memory plus a small set of imports.

### Shape

- Build a small QuickJS runner once, pinned to a QuickJS version and WASI SDK:
  - `qjs_run(source, operation, input_json) -> output_json`
  - an allocator/free pair for the host to copy strings.
- Host runtime: `wasmtime` (fast, mature, epoch/fuel interruption) or `wasmi`
  (pure Rust, simpler dependency, interpreter speed).
- Compile the module once at startup or embed a precompiled `.wasm`; instantiate
  a fresh `Store`/instance per request (or use a small pool with a fresh runtime
  inside each instance).
- Memory: `StoreLimits` caps linear memory, tables and instances. The QuickJS
  heap lives inside that linear memory, so `set_memory_limit` and the wasm
  memory ceiling overlap.
- CPU: epoch interruption (`Config::epoch_interruption`, a ticker thread and
  `Store::set_epoch_deadline`) or fuel. Epoch gives a wall-clock-style deadline;
  fuel gives deterministic instruction budgets.
- Capability surface: no preopens, no WASI sockets, no environment. Allow only
  the WASI calls QuickJS actually needs (clock/random/stdio); ideally replace
  them with deterministic host stubs.
- Crash containment: a wasm trap or out-of-bounds access aborts the instance,
  not the server.

### Build and supply chain

This is the main cost. `quickjs-wasm-sys`/`quickjs-wasm-rs` from Javy are
deprecated. A production build needs a maintained path, for example:

- QuickJS-ng cross-compiled with a pinned WASI SDK, plus a tiny C shim;
- a hermetic build script with a checksummed `.wasm` artifact checked into the
  repository or produced by CI;
- a reproducible build and a policy for updating QuickJS/WASI SDK/wasmtime.

Without that, "wasm" becomes an unpinned binary blob or a network-dependent
build step, which is worse than the current in-process engine.

### Performance

WASM adds a module boundary, linear-memory copies and a slower interpreter
(QuickJS compiled to wasm is not the same as native QuickJS). The JSON bridge is
the same shape as today, but every input/output crosses linear memory. This must
be benchmarked on cold index builds and warm map queries before making it the
default. Keep the native `NativeMap` fast path regardless.

WASM is the right long-term default if the deployment needs a portable,
memory-safe boundary or runs untrusted tenants on shared infrastructure. It is
not a drop-in replacement for the current `rquickjs` calls.

## Option D: a memory-safe Rust JavaScript engine

Boa or another pure-Rust engine would remove the C memory-safety class
entirely, but currently has different language semantics, performance and
compatibility from QuickJS. It is worth a spike for a future engine option, not
the first move for CouchDB compatibility.

## Recommendation

Use a `JsEngine` boundary and migrate in two stages:

1. **Now: harden and measure.**
   - Add sandbox conformance tests from CouchDB's view suite.
   - Cap input/output rows and bytes; reject oversized results.
   - Add a dedicated execution thread and panic containment.
   - Document that in-process QuickJS is not a security boundary.
2. **Next: worker process.**
   - Extract the map/reduce/index wrappers behind a `JsEngine` trait.
   - Add a supervised worker with rlimits and Linux seccomp/Landlock.
   - Pool workers; kill and restart on timeout or crash.
   - Make it the default for deployments that execute untrusted design docs.
3. **Then: WASM engine.**
   - Build a pinned QuickJS WASM runner and run it under wasmtime/wasmi.
   - Enforce memory, epoch/fuel and output limits.
   - Run the same `JsEngine` conformance suite and benchmarks.
   - Offer `--js-engine=native|process|wasm`; choose the default based on the
     threat model and measured cost.

This order gets crash containment quickly, keeps the door open for WASM, and
avoids pretending that removing `eval` or setting a memory limit is a security
sandbox.

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
- Linux-only sandboxing or cross-platform process isolation?
- Is a checked-in wasm artifact acceptable, or must the build be fully
  reproducible from source?
- What performance regression is acceptable for untrusted tenants?
- Should the worker run one request per process (strongest isolation, highest
  cost) or a warmed pool with a fresh QuickJS runtime per request?

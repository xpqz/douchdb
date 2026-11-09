# WASM QuickJS runner

`qjs-runner.wasm` is a WASI command that evaluates the same map/reduce/index
wrappers as the native view/search code: `wasm/js/map.js`, `index.js` and
`reduce.js` are the single copy of those wrappers, `include_str!`'d by both the
host (`src/view.rs`, `src/search.rs`) and the runner. The placeholder
`__DOUCHDB_SOURCE__` is replaced by the user's function. The wrappers return
per-document results only; the runner pairs them with the document ids the host
sent. Changing a `.js` file changes the runner: rebuild it. The host compiles it once, gives it a
request on stdin and reads a JSON response from stdout. It has no filesystem or
network capabilities: the WASI context has no preopens, environment or args,
and the host grants only the request/stdout pipes plus WASI clock/random,
memory, fuel and a 128 MiB linear-memory cap.

The runner ignores the `memory` and `timeout_ms` request fields on purpose:
the host enforces both from outside (a cap on linear-memory growth and a
wall-clock deadline in wasmi and Wasmtime), which holds even if the guest is
compromised or stuck in a tight loop. The one use the runner makes of
`timeout_ms` is in a batched `reduce_many` request, whose host deadline is the
timeout times the number of calls: it interrupts a call that outlives
`timeout_ms`, so one runaway reducer does not spend the whole batch's budget.

Rebuild it with Docker (nothing else is needed):

```sh
wasm/build.sh
```

The script pins the whole toolchain:

| Component | Version |
| --- | --- |
| Docker image | `rust:1.96.1-bookworm`, digest-pinned in `build.sh` (rustc 1.96.1) |
| Rust target | `wasm32-wasip1` (`rustup target add`) |
| WASI SDK | 24.0 (`wasi-sdk-24`, linux tarball, sha256 checked by the script) |
| Dependencies | `wasm/qjs-runner/Cargo.lock` (`cargo build --locked`) |

It runs `cargo build --release --target wasm32-wasip1` with
`WASI_SDK=/opt/wasi-sdk`, which is what `rquickjs-sys` uses for its C
compiler and sysroot, and copies the result to `wasm/qjs-runner.wasm`.

The script builds for `linux/amd64` on every host (under emulation on arm64):
the arm64 and x86_64 WASI SDKs give different wasm bytes, so the checksum is
only reproducible per platform. Two consecutive builds gave identical bytes. CI
rebuilds the runner on x86_64 and compares the checksum (the `WASM runner`
workflow, which runs only when `wasm/` changes).

The runner uses the maintained `rquickjs` crate (MIT) with QuickJS (MIT),
built through the WASI SDK. Keep the generated `.wasm` in sync with the source;
`cargo test` exercises the checked-in artifact through `wasmi`; with
`--features wasmtime` the same artifact also runs under the Wasmtime JIT. The
artifact is unchanged by the host runtime choice. The host side is behind the
default-on `wasm` cargo feature; `--no-default-features` builds without wasmi.

Expected artifact checksum for the checked-in binary:

```text
f3961f44893d65cf42fb6c981c972474a802838d75fc880a03b2c853f7b0cd3d  qjs-runner.wasm
```

`wasm/qjs-runner.sha256` is the manifest checked by the unit test:

```sh
cargo test --lib wasm_runner_matches_the_checked_in_checksum
```

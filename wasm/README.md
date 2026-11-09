# WASM QuickJS runner

`qjs-runner.wasm` is a WASI command that evaluates the same map/reduce/index
wrappers as the native view/search code: `wasm/js/map.js`, `index.js` and
`reduce.js` are the single copy of those wrappers, `include_str!`'d by both the
host (`src/view.rs`, `src/search.rs`) and the runner. The placeholder
`__DOUCHDB_SOURCE__` is replaced by the user's function. The wrappers return
per-document results only; the runner pairs them with the document ids the host
sent. `wasm/limits.rs` is shared the same way (a `#[path]` module in `src/js.rs`
and in the runner): the 16 MiB output limit with its error text, and the
batch sizes. The runner re-batches the documents of a request at the native
engine's batch sizes and applies the output limit per batch, and it words its
errors as the native engine does (`compile: ...` for the wrapper, the first
document id of the batch for a map or index call), so a function behaves alike
in every sandbox. The runner is built with `panic = "abort"` (this target
cannot unwind): a panic prints the native `... panicked: ...` error to stdout
from a panic hook and then traps, and the host prefers that text to the bare
trap. Changing a `.js` file or `limits.rs` changes the runner: rebuild it.
The host compiles it once, gives it a
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

The script builds for `linux/arm64` on every host: native on Apple silicon and
on GitHub's arm64 runners (`ubuntu-24.04-arm`). The arm64 and x86_64 WASI SDKs
give different wasm bytes, so the checksum is only reproducible on the
reference platform, arm64; an x86_64 host gets different bytes (the build runs
under emulation there, and its result will not match the checksum). Two
consecutive builds gave identical bytes. CI rebuilds the runner on arm64 and
compares the checksum (the `WASM runner` workflow, which runs only when `wasm/`
changes).

The runner uses the maintained `rquickjs` crate (MIT) with QuickJS (MIT),
built through the WASI SDK. Keep the generated `.wasm` in sync with the source;
`cargo test` exercises the checked-in artifact through `wasmi`; with
`--features wasmtime` the same artifact also runs under the Wasmtime JIT. The
artifact is unchanged by the host runtime choice. The host side is behind the
default-on `wasm` cargo feature; `--no-default-features` builds without wasmi.

Expected artifact checksum for the checked-in binary:

```text
1786b747d8e470872ee24c604176c903818bc827512444e62b99cddb5f951e58  qjs-runner.wasm
```

`wasm/qjs-runner.sha256` is the manifest checked by the unit test:

```sh
cargo test --lib wasm_runner_matches_the_checked_in_checksum
```

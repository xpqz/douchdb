# WASM QuickJS runner

`qjs-runner.wasm` is a WASI command that evaluates the same map/reduce/index
wrappers as the native view/search code. The host compiles it once, gives it a
request on stdin and reads a JSON response from stdout. It has no filesystem or
network capabilities: the WASI context has no preopens, environment or args,
and the host grants only the request/stdout pipes plus WASI clock/random,
memory, fuel and a 128 MiB linear-memory cap.

The runner ignores the `memory` and `timeout_ms` request fields on purpose:
the host enforces both from outside (a cap on linear-memory growth and a
wall-clock deadline in wasmi and Wasmtime), which holds even if the guest is
compromised or stuck in a tight loop.

Rebuild it with Docker (nothing else is needed):

```sh
wasm/build.sh
```

The script pins the whole toolchain:

| Component | Version |
| --- | --- |
| Docker image | `rust:1.96.1-bookworm` (rustc 1.96.1) |
| Rust target | `wasm32-wasip1` (`rustup target add`) |
| WASI SDK | 24.0 (`wasi-sdk-24`, linux tarball, sha256 checked by the script) |
| Dependencies | `wasm/qjs-runner/Cargo.lock` (`cargo build --locked`) |

It runs `cargo build --release --target wasm32-wasip1` with
`WASI_SDK=/opt/wasi-sdk`, which is what `rquickjs-sys` uses for its C
compiler and sysroot, and copies the result to `wasm/qjs-runner.wasm`. Two
consecutive builds on an arm64 host gave identical bytes; the script also runs
on x86_64.

The runner uses the maintained `rquickjs` crate (MIT) with QuickJS (MIT),
built through the WASI SDK. Keep the generated `.wasm` in sync with the source;
`cargo test` exercises the checked-in artifact through `wasmi`; with
`--features wasmtime` the same artifact also runs under the Wasmtime JIT. The
artifact is unchanged by the host runtime choice. The host side is behind the
default-on `wasm` cargo feature; `--no-default-features` builds without wasmi.

Expected artifact checksum for the checked-in binary:

```text
9b067ea4c0b117dcc35d81b5a98230acd14b86bc2c0c050a982855a2a4d7874e  qjs-runner.wasm
```

`wasm/qjs-runner.sha256` is the manifest checked by the unit test:

```sh
cargo test --lib wasm_runner_matches_the_checked_in_checksum
```

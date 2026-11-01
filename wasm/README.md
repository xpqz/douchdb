# WASM QuickJS runner

`qjs-runner.wasm` is a WASI command that evaluates the same map/reduce/index
wrappers as the native view/search code. The host compiles it once, gives it a
request on stdin and reads a JSON response from stdout. It has no filesystem or
network capabilities: the WASI context has no preopens, environment or args,
and the host grants only the request/stdout pipes plus WASI clock/random,
memory, fuel and a 128 MiB linear-memory cap.

Rebuild it with the pinned toolchain:

```sh
rustup target add wasm32-wasip1
cd wasm/qjs-runner
cargo build --release --target wasm32-wasip1
cp target/wasm32-wasip1/release/qjs-runner.wasm ../
```

The runner uses the maintained `rquickjs` crate (MIT) with QuickJS (MIT),
built through the WASI SDK. Keep the generated `.wasm` in sync with the source;
`cargo test` exercises the checked-in artifact through `wasmi`.

Expected artifact checksum for the checked-in binary:

```text
88dd6a707c7d4ee99c1e92ab35dc172308fe8d79cc6b0cb4e39905bda06f71f2  qjs-runner.wasm
```

`wasm/qjs-runner.sha256` is the manifest checked by the unit test:

```sh
cargo test --bin douchdb wasm_runner_matches_the_checked_in_checksum
```

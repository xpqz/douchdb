# WASM QuickJS runner

`qjs-runner.wasm` is a WASI command that evaluates the same map/reduce/index
wrappers as the native view/search code. The host compiles it once, gives it a
request on stdin and reads a JSON response from stdout. It has no filesystem or
network capabilities: the WASI context has no preopens and only memory, fuel
and a 128 MiB linear-memory cap are granted.

Rebuild it with the pinned toolchain:

```sh
rustup target add wasm32-wasip1
cd wasm/qjs-runner
cargo build --release --target wasm32-wasip1
cp target/wasm32-wasip1/release/qjs-runner.wasm ../
```

The runner uses `quickjs-wasm-rs` (Apache-2.0 WITH LLVM-exception), which builds
QuickJS through the WASI SDK. Keep the generated `.wasm` in sync with the
source; `cargo test` exercises the checked-in artifact through `wasmi`.

Expected artifact checksum for the checked-in binary:

```text
b01ccc8d63dd1953e6b4a4d488dddc7c223452315d5c75dc4b8e46087309f424  qjs-runner.wasm
```

`wasm/qjs-runner.sha256` is the manifest checked by the unit test:

```sh
cargo test --bin douchdb wasm_runner_matches_the_checked_in_checksum
```

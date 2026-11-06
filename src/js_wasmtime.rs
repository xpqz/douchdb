//! Optional Wasmtime (Cranelift JIT) backend for the QuickJS WASI runner.
//!
//! Same module, same JSON envelope and the same linear-memory/output limits as
//! the `wasmi` backend, but execution is compiled to native code. Enabled at
//! build time with `--features wasmtime` and selected at runtime with
//! `--wasm-runtime wasmtime`.

use crate::js::{check_output, wasm_memory_cap, MAX_OUTPUT_BYTES};
use crate::view::ViewLimits;
use serde_json::Value;
use std::sync::OnceLock;
use std::time::Duration;
use wasmtime::{Config, Engine, InstancePre, Linker, Module, Store, StoreLimits};
use wasmtime_wasi::p1::{self, WasiP1Ctx};
use wasmtime_wasi::p2::pipe::{MemoryInputPipe, MemoryOutputPipe};
use wasmtime_wasi::WasiCtxBuilder;

const WASM_RUNNER: &[u8] = include_bytes!("../wasm/qjs-runner.wasm");
const EPOCH_TICK: Duration = Duration::from_millis(10);

struct WasmtimeData {
    wasi: WasiP1Ctx,
    limits: StoreLimits,
}

struct WasmtimeState {
    engine: Engine,
    baseline_memory: usize,
    pre: InstancePre<WasmtimeData>,
}

/// Compile and cache the wasm module at startup instead of paying the
/// Cranelift compilation cost on the first query.
pub fn warm() -> Result<(), String> {
    engine().map(|_| ())
}

fn engine() -> Result<&'static WasmtimeState, String> {
    static ENGINE: OnceLock<Result<WasmtimeState, String>> = OnceLock::new();
    let state = ENGINE.get_or_init(|| {
        let mut config = Config::new();
        // Epoch interruption is a cheap wall-clock deadline check, unlike
        // fuel accounting on every instruction.
        config.epoch_interruption(true);
        // Debug builds are started constantly by the test suite; compile
        // quickly there. Release builds compile for execution speed.
        config.cranelift_opt_level(if cfg!(debug_assertions) {
            wasmtime::OptLevel::None
        } else {
            wasmtime::OptLevel::Speed
        });
        let engine = Engine::new(&config).map_err(|error| format!("wasmtime engine: {error}"))?;
        let module =
            Module::new(&engine, WASM_RUNNER).map_err(|error| format!("wasm module: {error}"))?;
        let mut linker: Linker<WasmtimeData> = Linker::new(&engine);
        p1::add_to_linker_sync(&mut linker, |data: &mut WasmtimeData| &mut data.wasi)
            .map_err(|error| format!("wasm wasi: {error}"))?;
        // Linear memory the module declares up front; the view memory limit
        // applies on top of it.
        let baseline_memory = module
            .exports()
            .find_map(|export| match export.ty() {
                wasmtime::ExternType::Memory(memory) if export.name() == "memory" => {
                    Some(memory.minimum().saturating_mul(65536) as usize)
                }
                _ => None,
            })
            .unwrap_or(0);
        let pre = linker
            .instantiate_pre(&module)
            .map_err(|error| format!("wasm instantiate: {error}"))?;
        // A single ticker advances every store's epoch. Each request sets its
        // deadline in ticks, so the timeout remains a wall-clock budget.
        let ticker = engine.clone();
        std::thread::Builder::new()
            .name("wasmtime-epoch".to_string())
            .spawn(move || loop {
                std::thread::sleep(EPOCH_TICK);
                ticker.increment_epoch();
            })
            .map_err(|error| format!("wasmtime epoch thread: {error}"))?;
        Ok(WasmtimeState {
            engine,
            baseline_memory,
            pre,
        })
    });
    match state {
        Ok(state) => Ok(state),
        Err(error) => Err(error.clone()),
    }
}

pub fn request(body: &[u8], limits: ViewLimits) -> Result<Value, String> {
    // The synchronous WASI shim calls `Handle::current().block_on()`, which
    // panics when invoked from inside the server's Tokio runtime. The wasm call
    // is CPU-bound anyway, so hop to a plain thread and block the caller.
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|scope| {
            scope
                .spawn(|| request_inner(body, limits))
                .join()
                .unwrap_or_else(|payload| Err(format!("wasmtime worker panicked: {payload:?}")))
        })
    } else {
        request_inner(body, limits)
    }
}

fn request_inner(body: &[u8], limits: ViewLimits) -> Result<Value, String> {
    let state = engine()?;
    let stdout = MemoryOutputPipe::new(MAX_OUTPUT_BYTES + 1);
    let wasi = WasiCtxBuilder::new()
        .stdin(MemoryInputPipe::new(body.to_vec()))
        .stdout(stdout.clone())
        .build_p1();
    let mut store = Store::new(
        &state.engine,
        WasmtimeData {
            wasi,
            limits: wasmtime::StoreLimitsBuilder::new()
                .memory_size(wasm_memory_cap(state.baseline_memory, limits, body.len()))
                .trap_on_grow_failure(true)
                .build(),
        },
    );
    store.limiter(|data| &mut data.limits);
    let tick_ms = EPOCH_TICK.as_millis() as u64;
    let ticks = (limits.timeout.as_millis() as u64)
        .div_ceil(tick_ms.max(1))
        .max(1);
    store.set_epoch_deadline(ticks);

    let instance = state
        .pre
        .instantiate(&mut store)
        .map_err(|error| format!("wasm instantiate: {error}"))?;
    let start = instance
        .get_typed_func::<(), ()>(&mut store, "_start")
        .map_err(|error| format!("wasm start: {error}"))?;
    start
        .call(&mut store, ())
        .map_err(|error| format!("wasm run: {error:?}"))?;
    drop(store);

    let output = stdout.contents();
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Instant;

    fn limits(timeout: Duration) -> ViewLimits {
        ViewLimits {
            timeout,
            memory: 64 * 1024 * 1024,
        }
    }

    #[test]
    fn maps_documents_and_enforces_the_epoch_deadline() {
        let docs = json!([
            ["d0", "{\"_id\":\"d0\",\"n\":1}"],
            ["d1", "{\"_id\":\"d1\",\"n\":2}"]
        ]);
        let body = serde_json::to_vec(&json!({
            "kind": "map",
            "source": "function(doc){ var n = doc.n + 1; emit(n, null); }",
            "docs": docs,
            "timeout_ms": 5000,
            "memory": 64 * 1024 * 1024
        }))
        .unwrap();
        let value = request(&body, limits(Duration::from_secs(5))).unwrap();
        assert_eq!(value.as_array().unwrap().len(), 2);
        assert_eq!(value[0]["key"], 2);

        let body = serde_json::to_vec(&json!({
            "kind": "map",
            "source": "function(doc){ while (true) {} }",
            "docs": [["d0", "{\"_id\":\"d0\"}"]],
            "timeout_ms": 150,
            "memory": 64 * 1024 * 1024
        }))
        .unwrap();
        let started = Instant::now();
        let error = request(&body, limits(Duration::from_millis(150))).unwrap_err();
        assert!(
            error.contains("interrupt") || error.contains("epoch"),
            "{error}"
        );
        assert!(started.elapsed() < Duration::from_secs(3));
    }
}

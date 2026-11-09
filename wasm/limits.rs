// Limits shared by the host (src/js.rs, src/viewfn.rs, src/search.rs) and the
// WASM runner (wasm/qjs-runner), so that a call behaves alike in every
// sandbox: the output limit and its error text, and the size of the batches a
// map or index function runs on. The host sends the runner larger requests
// than the native engine runs; the runner re-batches them with these.

/// A single map/reduce/index call may not return more JSON than this. The
/// QuickJS heap limit bounds allocations inside the engine; this bounds the
/// Rust-side string that comes back before it is parsed into values.
pub const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;

/// The error text for a call of `label` that returned `len` bytes.
pub fn output_too_large(label: &str, len: usize) -> String {
    format!("{label} produced {len} bytes of output (limit {MAX_OUTPUT_BYTES})")
}

/// A map call takes at most this many documents ...
pub const MAP_BATCH_DOCS: usize = 256;
/// ... and about this many bytes of document JSON.
pub const MAP_BATCH_BYTES: usize = 4 * 1024 * 1024;
/// An index call takes at most this many documents.
pub const INDEX_BATCH_DOCS: usize = 128;

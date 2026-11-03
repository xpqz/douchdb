#![no_main]

use libfuzzer_sys::fuzz_target;

#[path = "../../src/json.rs"]
mod json;

fuzz_target!(|data: &[u8]| {
    let _ = json::from_slice(data);
});

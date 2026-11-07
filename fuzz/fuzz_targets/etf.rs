#![no_main]

use libfuzzer_sys::fuzz_target;
use serde_json::Value;

#[path = "../../src/etf.rs"]
mod etf;
// etf.rs uses crate::json::is_integer.
#[path = "../../src/json.rs"]
mod json;

fuzz_target!(|data: &[u8]| {
    if let Ok(value) = serde_json::from_slice::<Value>(data) {
        let _ = etf::from_json(&value).encode();
    }
});

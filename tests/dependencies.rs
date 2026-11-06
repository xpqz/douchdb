//! Guards on the dependency graph (issue #30).

use std::fs;

fn read(path: &str) -> String {
    fs::read_to_string(format!("{}/{path}", env!("CARGO_MANIFEST_DIR")))
        .unwrap_or_else(|error| panic!("{path}: {error}"))
}

/// Versions of `name` recorded in Cargo.lock.
fn locked_versions(lock: &str, name: &str) -> Vec<String> {
    let mut versions = Vec::new();
    let mut current: Option<&str> = None;
    for line in lock.lines() {
        if let Some(value) = line.strip_prefix("name = ") {
            current = Some(value.trim_matches('"'));
        } else if let Some(value) = line.strip_prefix("version = ") {
            if current == Some(name) {
                versions.push(value.trim_matches('"').to_string());
            }
        }
    }
    versions
}

#[test]
fn one_http_client_stack() {
    let lock = read("Cargo.lock");
    let versions = locked_versions(&lock, "reqwest");
    assert_eq!(
        versions.len(),
        1,
        "reqwest must resolve to a single version (ours and opentelemetry-otlp's): {versions:?}"
    );
}

#[test]
fn wasm_sandbox_dependencies_are_optional() {
    let manifest = read("Cargo.toml");
    for name in ["wasmi", "wasmi_wasi"] {
        let line = manifest
            .lines()
            .find(|line| line.starts_with(&format!("{name} = ")))
            .unwrap_or_else(|| panic!("{name} missing from Cargo.toml"));
        assert!(
            line.contains("optional = true"),
            "{name} must be optional so a build without the `wasm` feature skips it: {line}"
        );
    }
    assert!(
        manifest
            .lines()
            .any(|line| line.starts_with("default = [\"wasm\"]")),
        "the `wasm` feature must be on by default"
    );
}

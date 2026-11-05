//! Command-line arguments for the `douchdb` server.

use std::path::PathBuf;

pub struct Args {
    pub listen: String,
    pub data: PathBuf,
    pub q: u32,
    pub n: usize,
    pub revs_limit: u64,
    pub admin: Option<(String, String)>,
    pub users_file: Option<String>,
    pub session_secret: String,
    pub default_security: String,
    pub max_in_flight: usize,
    pub per_user_in_flight: usize,
    pub per_user_rate: usize,
    pub heavy_in_flight: usize,
    pub heavy_per_user: usize,
    pub background_in_flight: usize,
    pub compact_concurrency: usize,
    pub nodes: Vec<String>,
    pub me: usize,
    pub advertise: String,
    pub seed_nodes: Vec<String>,
    pub sync_secs: u64,
    pub js_sandbox: String,
    pub wasm_runtime: String,
    pub js_workers: usize,
    pub worker: bool,
    pub cors_origins: Vec<String>,
    pub view_timeout_ms: u64,
    pub view_memory_mb: usize,
    pub max_request_mb: usize,
    pub cluster_secret: String,
    pub otel_endpoint: Option<String>,
}

pub fn parse_args() -> Args {
    let mut args = Args {
        listen: "127.0.0.1:5984".into(),
        data: PathBuf::from("data"),
        q: 2,
        n: 3,
        revs_limit: 1000,
        admin: None,
        users_file: None,
        session_secret: String::new(),
        default_security: String::new(),
        max_in_flight: 64,
        per_user_in_flight: 16,
        per_user_rate: 0,
        heavy_in_flight: 8,
        heavy_per_user: 1,
        background_in_flight: 2,
        compact_concurrency: 1,
        nodes: Vec::new(),
        me: 0,
        advertise: String::new(),
        seed_nodes: Vec::new(),
        sync_secs: 5,
        js_sandbox: "native".to_string(),
        wasm_runtime: "wasmi".to_string(),
        js_workers: 2,
        worker: false,
        cors_origins: Vec::new(),
        view_timeout_ms: 5000,
        view_memory_mb: 64,
        max_request_mb: 64,
        cluster_secret: std::env::var("DOUCHDB_CLUSTER_SECRET").unwrap_or_default(),
        otel_endpoint: std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--listen" => args.listen = it.next().unwrap_or_default(),
            "--data" => args.data = PathBuf::from(it.next().unwrap_or_default()),
            "--q" => args.q = it.next().and_then(|v| v.parse().ok()).unwrap_or(2),
            "--n" => args.n = it.next().and_then(|v| v.parse().ok()).unwrap_or(3),
            "--revs-limit" => {
                args.revs_limit = it.next().and_then(|v| v.parse().ok()).unwrap_or(1000)
            }
            "--admin" => {
                if let Some(value) = it.next() {
                    if let Some((user, pass)) = value.split_once(':') {
                        args.admin = Some((user.to_string(), pass.to_string()));
                    }
                }
            }
            "--users-file" => args.users_file = it.next(),
            "--session-secret" => args.session_secret = it.next().unwrap_or_default(),
            "--default-security" => args.default_security = it.next().unwrap_or_default(),
            "--max-in-flight" => {
                args.max_in_flight = it.next().and_then(|v| v.parse().ok()).unwrap_or(64)
            }
            "--per-user-in-flight" => {
                args.per_user_in_flight = it.next().and_then(|v| v.parse().ok()).unwrap_or(16)
            }
            "--per-user-rate" => {
                args.per_user_rate = it.next().and_then(|v| v.parse().ok()).unwrap_or(0)
            }
            "--heavy-in-flight" => {
                args.heavy_in_flight = it.next().and_then(|v| v.parse().ok()).unwrap_or(8)
            }
            "--heavy-per-user" => {
                args.heavy_per_user = it.next().and_then(|v| v.parse().ok()).unwrap_or(1)
            }
            "--background-in-flight" => {
                args.background_in_flight = it.next().and_then(|v| v.parse().ok()).unwrap_or(2)
            }
            "--compact-concurrency" => {
                args.compact_concurrency = it.next().and_then(|v| v.parse().ok()).unwrap_or(1)
            }
            "--nodes" => {
                args.nodes = it
                    .next()
                    .unwrap_or_default()
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect()
            }
            "--me" => args.me = it.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            "--advertise" => args.advertise = it.next().unwrap_or_default(),
            "--seed-nodes" => {
                args.seed_nodes = it
                    .next()
                    .unwrap_or_default()
                    .split(',')
                    .map(str::trim)
                    .filter(|node| !node.is_empty())
                    .map(|node| node.trim_end_matches('/').to_string())
                    .collect()
            }
            "--sync-secs" => args.sync_secs = it.next().and_then(|v| v.parse().ok()).unwrap_or(5),
            "--js-sandbox" => args.js_sandbox = it.next().unwrap_or_else(|| "native".to_string()),
            "--wasm-runtime" => {
                args.wasm_runtime = it.next().unwrap_or_else(|| "wasmi".to_string())
            }
            "--js-worker" => args.worker = true,
            "--js-workers" => {
                args.js_workers = it.next().and_then(|value| value.parse().ok()).unwrap_or(2)
            }
            "--view-timeout-ms" => {
                args.view_timeout_ms = it.next().and_then(|v| v.parse().ok()).unwrap_or(5000)
            }
            "--view-memory-mb" => {
                args.view_memory_mb = it.next().and_then(|v| v.parse().ok()).unwrap_or(64)
            }
            "--max-request-mb" => {
                args.max_request_mb = it.next().and_then(|v| v.parse().ok()).unwrap_or(64)
            }
            "--cluster-secret" => args.cluster_secret = it.next().unwrap_or_default(),
            "--otel-endpoint" => args.otel_endpoint = it.next(),
            "--cors-origins" => {
                args.cors_origins = it
                    .next()
                    .unwrap_or_default()
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect()
            }
            "--help" | "-h" => {
                println!(
                    "douchdb [--listen ADDR] [--data DIR] [--q N] [--n N] [--revs-limit N]\n\
                     \x20       [--admin user:pass] [--users-file FILE] [--session-secret SECRET]\n\
                     \x20       [--default-security open|admin-only]\n\
                     \x20       [--max-in-flight N] [--per-user-in-flight N] [--per-user-rate N]\n\
                     \x20       [--heavy-in-flight N] [--heavy-per-user N] [--background-in-flight N]\n\
                     \x20       [--compact-concurrency N]\n\
                     \x20       [--nodes URL,URL] [--me INDEX] [--sync-secs N]\n\
                     \x20       [--advertise URL] [--seed-nodes URL,URL]\n\
                     \x20       [--js-sandbox native|process|wasm] [--wasm-runtime wasmi|wasmtime]\n\
                     \x20       [--js-workers N]\n\
                     \x20       [--cors-origins ORIGIN,ORIGIN] [--view-timeout-ms N]\n\
                     \x20       [--view-memory-mb N] [--max-request-mb N]\n\
                     \x20       [--cluster-secret SECRET]   (or DOUCHDB_CLUSTER_SECRET)\n\
                     \x20       [--otel-endpoint URL]       (or OTEL_EXPORTER_OTLP_ENDPOINT)"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
    }
    args
}

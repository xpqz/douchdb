//! douchdb: a CouchDB wire-compatible database on LevelDB.

mod api;
mod auth;
mod cluster;
mod cluster_state;
mod db;
mod discovery;
mod error;
mod etf;
mod fabric;
mod js;
mod replicate;
mod rev;
mod revtree;
mod search;
mod store;
mod telemetry;
mod ui;
mod view;

use api::{AppState, Config};
use auth::Auth;
use axum::routing::{any, get};
use axum::Router;
use cluster::Cluster;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

struct Args {
    listen: String,
    data: PathBuf,
    q: u32,
    n: usize,
    revs_limit: u64,
    admin: Option<(String, String)>,
    nodes: Vec<String>,
    me: usize,
    advertise: String,
    seed_nodes: Vec<String>,
    sync_secs: u64,
    js_sandbox: String,
    js_workers: usize,
    worker: bool,
    cors_origins: Vec<String>,
    view_timeout_ms: u64,
    view_memory_mb: usize,
    max_request_mb: usize,
    cluster_secret: String,
    otel_endpoint: Option<String>,
}

fn parse_args() -> Args {
    let mut args = Args {
        listen: "127.0.0.1:5984".into(),
        data: PathBuf::from("data"),
        q: 1,
        n: 3,
        revs_limit: 1000,
        admin: None,
        nodes: Vec::new(),
        me: 0,
        advertise: String::new(),
        seed_nodes: Vec::new(),
        sync_secs: 5,
        js_sandbox: "native".to_string(),
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
            "--q" => args.q = it.next().and_then(|v| v.parse().ok()).unwrap_or(1),
            "--n" => args.n = it.next().and_then(|v| v.parse().ok()).unwrap_or(2),
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
                     \x20       [--admin user:pass] [--nodes URL,URL] [--me INDEX] [--sync-secs N]\n\
                     \x20       [--advertise URL] [--seed-nodes URL,URL]\n\
                     \x20       [--js-sandbox native|process|wasm] [--js-workers N]\n\
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = parse_args();
    if args.worker {
        if let Err(error) = js::install_seccomp()
            .and_then(|_| js::install_landlock())
            .and_then(|_| js::run_worker_stdio())
        {
            eprintln!("js worker: {error}");
            std::process::exit(1);
        }
        return Ok(());
    }
    match args.js_sandbox.as_str() {
        "native" => js::configure(js::Sandbox::Native, args.js_workers),
        "process" => js::configure(js::Sandbox::Process, args.js_workers),
        "wasm" => js::configure(js::Sandbox::Wasm, args.js_workers),
        other => {
            eprintln!("unknown --js-sandbox value: {other} (expected native, process or wasm)");
            std::process::exit(2);
        }
    }
    let telemetry = telemetry::init(args.otel_endpoint.clone(), &args.me.to_string())?;
    if args.nodes.len() > 1 {
        if args.me >= args.nodes.len() {
            eprintln!("--me must be smaller than the number of --nodes");
            std::process::exit(2);
        }
        if args.cluster_secret.trim().len() < 16 {
            eprintln!(
                "--cluster-secret of at least 16 characters is required with more than one --nodes entry"
            );
            std::process::exit(2);
        }
    }
    if args.cors_origins.iter().any(|origin| origin == "*") {
        tracing::warn!(
            "CORS allows any origin and sends credentials; any website can act as a logged-in user"
        );
    }
    std::fs::create_dir_all(&args.data)?;

    let advertise = if !args.advertise.trim().is_empty() {
        args.advertise.trim().trim_end_matches('/').to_string()
    } else if let Some(url) = args.nodes.get(args.me) {
        url.trim_end_matches('/').to_string()
    } else {
        format!("http://{}", args.listen)
    };
    let config = Config {
        q: args.q.max(1),
        n: args.n.max(1),
        revs_limit: args.revs_limit.max(1),
        admin: args.admin.clone(),
        nodes: args.nodes.clone(),
        me: args.me,
        sync_secs: args.sync_secs,
        seeds: args.seed_nodes.clone(),
        advertise: advertise.clone(),
        cors_origins: args.cors_origins.clone(),
        view_timeout_ms: args.view_timeout_ms.max(1),
        view_memory_mb: args.view_memory_mb.max(1),
    };
    let state = Arc::new(AppState {
        root: args.data.clone(),
        dbs: RwLock::new(HashMap::new()),
        config,
        auth: args
            .admin
            .as_ref()
            .map(|(u, p)| Auth::new(u.clone(), p.clone())),
        cluster: Cluster {
            nodes: args.nodes.clone(),
            me: args.me,
            n: args.n.max(1),
            secret: args.cluster_secret.clone(),
        },
        client: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(60))
            .build()?,
        db_updates: api::DbUpdates::default(),
        cluster_state: std::sync::Arc::new(cluster_state::ClusterState::new()),
    });

    if state.cluster.is_clustered() {
        replicate::spawn_anti_entropy(state.clone());
        tracing::info!("cluster mode: node {} of {}", args.me, args.nodes.len());
    }
    if state.cluster.is_clustered() || !state.config.seeds.is_empty() {
        discovery::spawn(state.clone());
    }

    let app = Router::new()
        .route("/", get(api::welcome))
        .route("/_up", get(api::up))
        .route("/_all_dbs", get(api::all_dbs))
        .route("/_db_updates", get(api::db_updates))
        .route("/_dbs_info", get(api::dbs_info).post(api::dbs_info))
        .route("/_uuids", get(api::uuids))
        .route("/_membership", get(api::membership))
        .route("/_node/_local/_dbs", get(api::dbs_metadata))
        .route("/_node/_local/_cluster", get(api::cluster_status))
        .route(
            "/_node/_local/_cluster/discover",
            axum::routing::post(api::cluster_discover),
        )
        .route("/_node/_local/_replication", get(api::replication_status))
        .route(
            "/_node/_local/_replication/run",
            axum::routing::post(api::replication_run),
        )
        .route(
            "/_node/_local/_replication/pause",
            axum::routing::post(api::replication_pause),
        )
        .route(
            "/_node/_local/_replication/resume",
            axum::routing::post(api::replication_resume),
        )
        .route(
            "/_node/_local/_dbs/{db}",
            get(api::db_metadata).put(api::db_metadata_put),
        )
        .route("/_node/_local/_dbs/{db}/shards", get(api::db_shard_counts))
        .route(
            "/_node/_local/_dbs/{db}/rebalance",
            axum::routing::post(api::db_rebalance),
        )
        .route(
            "/_node/_local/_dbs/{db}/cleanup",
            axum::routing::post(api::db_cleanup),
        )
        .route(
            "/_session",
            get(api::session_get)
                .post(api::session_post)
                .delete(api::session_delete),
        )
        .route("/_utils", get(ui::redirect))
        .route("/_utils/", get(ui::index))
        .route("/_utils/{*path}", get(ui::asset))
        .route("/favicon.ico", get(ui::favicon))
        .route("/{db}", any(api::db_root))
        .route("/{db}/", any(api::db_root))
        .route("/{db}/{*rest}", any(api::db_rest))
        .fallback(api::not_found)
        // Axum's 2 MiB default would reject a single 1 MiB attachment plus a
        // multi-document `_bulk_docs`. CouchDB's limit is far higher.
        .layer(axum::extract::DefaultBodyLimit::max(
            args.max_request_mb.max(1).saturating_mul(1024 * 1024),
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            api::cors,
        ))
        .layer(axum::middleware::from_fn(api::log_requests))
        .layer(axum::middleware::from_fn(api::security_headers))
        .layer(axum::middleware::from_fn(api::request_id))
        .layer(axum::middleware::from_fn(api::head_only))
        .layer(tower_http::compression::CompressionLayer::new())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    tracing::info!("douchdb listening on {}", args.listen);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    if let Some(telemetry) = telemetry {
        telemetry.shutdown();
    }
    Ok(())
}

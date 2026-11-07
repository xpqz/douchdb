//! Server startup: JS sandbox selection, application state, background tasks
//! and the HTTP router.

use crate::api::{self, AppState, Config};
use crate::auth::Auth;
use crate::cli::Args;
use crate::cluster::Cluster;
use crate::cluster_http::ClusterCtx;
use crate::{admission, cluster_state, discovery, js, replicate, telemetry, ui};
use axum::routing::{any, get};
use axum::serve::ListenerExt;
use axum::Router;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Exit when stdin reaches EOF. Test harnesses set `DOUCHDB_EXIT_ON_STDIN_EOF`
/// and hold the write end of a stdin pipe, so a server dies with its parent even
/// when the parent is killed and cannot clean up. Off unless the variable is set.
fn exit_on_stdin_eof() {
    std::thread::spawn(|| {
        use std::io::Read;
        let mut sink = [0u8; 256];
        let mut stdin = std::io::stdin().lock();
        while matches!(stdin.read(&mut sink), Ok(n) if n > 0) {}
        std::process::exit(0);
    });
}

/// Run a sandboxed JS worker (`--js-worker`) on stdin/stdout until EOF.
///
/// This must run without an async runtime. A runtime's worker threads start
/// asynchronously; if they are still initialising when the seccomp filter is
/// installed, their syscalls are refused and the worker dies before it
/// announces itself (seen under CPU load as "worker did not start").
pub fn run_worker() -> ! {
    // Landlock first: its syscalls are not on the seccomp allowlist.
    let result = js::install_landlock().and_then(|landlock| {
        js::install_seccomp()?;
        crate::worker::run_stdio(landlock)
    });
    match result {
        Ok(()) => std::process::exit(0),
        Err(error) => {
            eprintln!("js worker: {error}");
            std::process::exit(1);
        }
    }
}

/// Choose the JS sandbox from `--js-sandbox` and `--wasm-runtime`, exiting with
/// status 2 on a value the CLI does not know.
fn configure_js_sandbox(args: &Args) {
    match args.js_sandbox.as_str() {
        "native" => js::configure(js::Sandbox::Native, args.js_workers),
        "process" => js::configure(js::Sandbox::Process, args.js_workers),
        "wasm" => {
            js::configure(js::Sandbox::Wasm, args.js_workers);
            let runtime = match args.wasm_runtime.as_str() {
                "wasmi" => js::WasmRuntime::Wasmi,
                "wasmtime" => js::WasmRuntime::Wasmtime,
                other => {
                    eprintln!("unknown --wasm-runtime value: {other} (expected wasmi or wasmtime)");
                    std::process::exit(2);
                }
            };
            if let Err(error) = js::set_wasm_runtime(runtime) {
                eprintln!("{error}");
                std::process::exit(2);
            }
        }
        other => {
            eprintln!("unknown --js-sandbox value: {other} (expected native, process or wasm)");
            std::process::exit(2);
        }
    }
}

/// Refuse a cluster configuration that cannot work or is not safe, exiting with
/// status 2.
fn check_cluster_args(args: &Args) {
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
}

/// The URL peers and clients are told to reach this node on.
fn advertise_url(args: &Args) -> String {
    if !args.advertise.trim().is_empty() {
        args.advertise.trim().trim_end_matches('/').to_string()
    } else if let Some(url) = args.nodes.get(args.me) {
        url.trim_end_matches('/').to_string()
    } else {
        format!("http://{}", args.listen)
    }
}

/// Credentials from `--users-file` or `--admin`, if either is configured.
fn build_auth(args: &Args) -> anyhow::Result<Option<Auth>> {
    Ok(if args.users_file.is_some() || args.admin.is_some() {
        if args.nodes.len() > 1 && args.session_secret.trim().is_empty() {
            tracing::warn!(
                "no --session-secret: each node signs session cookies with its own key, so a cookie only works on the node that issued it"
            );
        }
        let secret = crate::auth::session_secret(&args.data, &args.session_secret)
            .map_err(anyhow::Error::msg)?;
        if let Some(path) = &args.users_file {
            Some(
                Auth::from_users_file(args.admin.clone(), std::path::Path::new(path), secret)
                    .map_err(anyhow::Error::msg)?,
            )
        } else {
            args.admin
                .as_ref()
                .map(|(user, pass)| Auth::new(user.clone(), pass.clone(), secret))
        }
    } else {
        None
    })
}

/// Whether new databases start admin-only, from `--default-security`.
fn default_admin_only(args: &Args, auth: &Option<Auth>) -> bool {
    match args.default_security.as_str() {
        "open" => false,
        "admin-only" => true,
        "" => auth.is_some(),
        other => {
            eprintln!("unknown --default-security value: {other} (expected open or admin-only)");
            std::process::exit(2);
        }
    }
}

/// The shared application state for this node.
fn build_state(
    args: &Args,
    auth: Option<Auth>,
    advertise: String,
) -> anyhow::Result<Arc<AppState>> {
    let default_admin_only = default_admin_only(args, &auth);
    let config = Config {
        q: args.q.max(1),
        revs_limit: args.revs_limit.max(1),
        sync_secs: args.sync_secs,
        seeds: args.seed_nodes.clone(),
        advertise,
        cors_origins: args.cors_origins.clone(),
        view_timeout_ms: args.view_timeout_ms.max(1),
        view_memory_mb: args.view_memory_mb.max(1),
        default_admin_only,
    };
    Ok(Arc::new(AppState {
        root: args.data.clone(),
        dbs: RwLock::new(HashMap::new()),
        db_locks: Default::default(),
        fs_health: Default::default(),
        config,
        compaction: std::sync::Arc::new(tokio::sync::Semaphore::new(
            args.compact_concurrency.max(1),
        )),
        admission: std::sync::Arc::new(admission::Admission::new(admission::Limits {
            max_in_flight: args.max_in_flight,
            per_user_in_flight: args.per_user_in_flight,
            per_user_rate: args.per_user_rate,
            heavy_in_flight: args.heavy_in_flight,
            heavy_per_user: args.heavy_per_user,
            background_in_flight: args.background_in_flight,
        })),
        ctx: ClusterCtx {
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
            auth,
        },
        db_updates: api::DbUpdates::default(),
        cluster_state: std::sync::Arc::new(cluster_state::ClusterState::new()),
    }))
}

/// The HTTP routes and middleware.
fn build_router(args: &Args, state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(api::welcome))
        .route("/_up", get(api::up))
        .route("/_health", get(api::health_public))
        .route("/_all_dbs", get(api::all_dbs))
        .route("/_db_updates", get(api::db_updates))
        .route("/_dbs_info", get(api::dbs_info).post(api::dbs_info))
        .route("/_uuids", get(api::uuids))
        .route("/_membership", get(api::membership))
        .route("/_node/_local/_dbs", get(api::dbs_metadata))
        .route("/_node/_local/_cluster", get(api::cluster_status))
        .route("/_node/_local/_metrics", get(api::metrics_status))
        .route("/_node/_local/_health", get(api::health_status))
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
        .route(
            "/_node/_local/_dbs/{db}/security",
            axum::routing::get(api::db_security_get).put(api::db_security_put),
        )
        .route(
            "/_node/_local/_dbs/{db}/migration",
            axum::routing::put(api::db_migration_put).delete(api::db_migration_delete),
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
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            api::admit,
        ))
        .layer(axum::middleware::from_fn(api::log_requests))
        .layer(axum::middleware::from_fn(api::security_headers))
        .layer(axum::middleware::from_fn(api::request_id))
        .layer(axum::middleware::from_fn(api::head_only))
        .layer(tower_http::compression::CompressionLayer::new())
        .with_state(state)
}

/// Run the server until shutdown. `--js-worker` is dispatched by the binary to
/// [`run_worker`] before any runtime exists.
pub async fn run(args: Args) -> anyhow::Result<()> {
    if args.worker {
        anyhow::bail!("--js-worker must be started through run_worker, outside an async runtime");
    }
    if std::env::var_os("DOUCHDB_EXIT_ON_STDIN_EOF").is_some() {
        exit_on_stdin_eof();
    }
    configure_js_sandbox(&args);
    crate::index_cache::set_max_open(args.max_open_indexes);
    let telemetry = telemetry::init(args.otel_endpoint.clone(), &args.me.to_string())?;
    check_cluster_args(&args);
    if args.cors_origins.iter().any(|origin| origin == "*") {
        tracing::warn!(
            "CORS allows any origin and sends credentials; any website can act as a logged-in user"
        );
    }
    std::fs::create_dir_all(&args.data)?;
    crate::db::migrate_nested_dbs(&args.data).map_err(anyhow::Error::msg)?;

    let advertise = advertise_url(&args);
    let auth = build_auth(&args)?;
    let state = build_state(&args, auth, advertise)?;
    if state.ctx.cluster.is_clustered() {
        replicate::spawn_anti_entropy(state.clone());
        tracing::info!("cluster mode: node {} of {}", args.me, args.nodes.len());
    }
    if state.ctx.cluster.is_clustered() || !state.config.seeds.is_empty() {
        discovery::spawn(state.clone());
    }

    let app = build_router(&args, state);

    // Streamed responses are written in several pieces. With Nagle's algorithm
    // a later piece waits for the client's delayed ACK of the first, adding
    // about 40 ms to every such response.
    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await?
        .tap_io(|stream| {
            if let Err(error) = stream.set_nodelay(true) {
                tracing::debug!("could not set TCP_NODELAY: {error}");
            }
        });
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

//! Server startup: JS sandbox selection, application state, background tasks
//! and the HTTP router.

use crate::api::{self, AppState, Config};
use crate::auth::Auth;
use crate::cli::Args;
use crate::cluster::Cluster;
use crate::cluster_http::ClusterCtx;
use crate::{admission, cluster_state, discovery, js, replicate, telemetry, ui};
use axum::routing::{any, get};
use axum::Router;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// Shut down gracefully when stdin reaches EOF. Test harnesses set
/// `DOUCHDB_EXIT_ON_STDIN_EOF` and hold the write end of a stdin pipe, so a
/// server stops with its parent even when the parent is killed and cannot
/// clean up. Off unless the variable is set.
fn shutdown_on_stdin_eof(shutdown: CancellationToken) {
    std::thread::spawn(move || {
        use std::io::Read;
        let mut sink = [0u8; 256];
        let mut stdin = std::io::stdin().lock();
        while matches!(stdin.read(&mut sink), Ok(n) if n > 0) {}
        tracing::info!("stdin closed; shutting down");
        shutdown.cancel();
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

/// How long a shutdown may take, from the first SIGTERM/SIGINT, before the
/// server gives up on a clean stop and exits anyway.
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(20);

/// How long the final telemetry flush may take.
const TELEMETRY_FLUSH_DEADLINE: Duration = Duration::from_secs(5);

/// A configuration the server refuses to start with. The binary exits with
/// status 2 after printing it.
#[derive(Debug)]
pub struct ConfigError(String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

fn config_error(message: impl Into<String>) -> ConfigError {
    ConfigError(message.into())
}

/// Check the `--wasm-runtime` choice against the `--js-sandbox` it applies to.
fn configure_js_sandbox(args: &Args) -> Result<(), ConfigError> {
    if args.js_sandbox == js::Sandbox::Wasm {
        #[cfg(feature = "wasmtime")]
        if args.wasm_runtime == js::WasmRuntime::Wasmtime {
            crate::js_wasmtime::install().map_err(|error| config_error(error.to_string()))?;
        }
        js::set_wasm_runtime(args.wasm_runtime).map_err(|error| config_error(error.to_string()))?;
    }
    Ok(())
}

/// Refuse a cluster configuration that cannot work or is not safe.
fn check_cluster_args(args: &Args) -> Result<(), ConfigError> {
    if args.nodes.len() > 1 {
        if args.me >= args.nodes.len() {
            return Err(config_error(
                "--me must be smaller than the number of --nodes",
            ));
        }
        if args.cluster_secret.trim().len() < 16 {
            return Err(config_error(
                "--cluster-secret of at least 16 characters is required with more than one --nodes entry",
            ));
        }
    }
    Ok(())
}

/// Check every option whose value the CLI parser cannot. Runs before anything
/// else starts, so a bad configuration leaves nothing behind.
fn validate_config(args: &Args) -> Result<(), ConfigError> {
    configure_js_sandbox(args)?;
    check_cluster_args(args)
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
fn build_auth(args: &Args) -> Result<Option<Auth>, ConfigError> {
    Ok(if args.users_file.is_some() || args.admin.is_some() {
        if args.nodes.len() > 1 && args.session_secret.trim().is_empty() {
            tracing::warn!(
                "no --session-secret: each node signs session cookies with its own key, so a cookie only works on the node that issued it"
            );
        }
        let secret =
            crate::auth::session_secret(&args.data, &args.session_secret).map_err(config_error)?;
        if let Some(path) = &args.users_file {
            Some(
                Auth::from_users_file(args.admin.clone(), std::path::Path::new(path), secret)
                    .map_err(config_error)?,
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
    args.default_admin_only.unwrap_or_else(|| auth.is_some())
}

/// The shared application state for this node.
fn build_state(
    args: &Args,
    auth: Option<Auth>,
    advertise: String,
    shutdown: CancellationToken,
) -> anyhow::Result<Arc<AppState>> {
    let default_admin_only = default_admin_only(args, &auth);
    let config = Config {
        q: args.q,
        revs_limit: args.revs_limit,
        sync_secs: args.sync_secs,
        seeds: args.seed_nodes.clone(),
        advertise,
        cors_origins: args.cors_origins.clone(),
        cors_credentials: args.cors_credentials,
        cors_headers: args
            .cors_headers
            .iter()
            .map(|header| header.to_ascii_lowercase())
            .collect(),
        cors_methods: args.cors_methods.clone(),
        cors_exposed_headers: args
            .cors_exposed_headers
            .iter()
            .map(|header| header.to_ascii_lowercase())
            .collect(),
        cors_max_age: args.cors_max_age,
        view_timeout_ms: args.view_timeout_ms,
        view_memory_mb: args.view_memory_mb,
        default_admin_only,
        max_open_dbs: args.max_open_dbs,
        local_feed_recheck: crate::state::LOCAL_FEED_RECHECK,
    };
    Ok(Arc::new(AppState {
        root: args.data.clone(),
        dbs: RwLock::new(HashMap::new()),
        db_locks: Default::default(),
        db_opens: Default::default(),
        last_sweep: Default::default(),
        principal_resolutions: Default::default(),
        fs_health: Default::default(),
        config,
        compaction: std::sync::Arc::new(tokio::sync::Semaphore::new(args.compact_concurrency)),
        admission: std::sync::Arc::new(admission::Admission::new(admission::Limits {
            max_in_flight: args.max_in_flight,
            per_user_in_flight: args.per_user_in_flight,
            per_user_rate: args.per_user_rate,
            heavy_in_flight: args.heavy_in_flight,
            heavy_per_user: args.heavy_per_user,
            background_in_flight: args.background_in_flight,
            ..Default::default()
        })),
        ctx: ClusterCtx {
            cluster: Cluster {
                nodes: args.nodes.clone(),
                me: args.me,
                n: args.n,
                secret: args.cluster_secret.clone(),
            },
            client: crate::cluster_http::peer_client(
                args.nodes
                    .iter()
                    .chain(&args.seed_nodes)
                    .any(|node| node.starts_with("https://")),
            )?,
            auth,
            health: Default::default(),
        },
        db_updates: api::DbUpdates::default(),
        feed_stats: Default::default(),
        cluster_state: std::sync::Arc::new(cluster_state::ClusterState::new()),
        shutdown,
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
            "/_node/_local/_metadata_sync",
            axum::routing::post(api::metadata_sync),
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
        // Axum's 2 MiB default would reject a large
        // multi-document `_bulk_docs`. CouchDB's limit is far higher.
        .layer(axum::extract::DefaultBodyLimit::max(
            args.max_request_mb.saturating_mul(1024 * 1024),
        ))
        .layer(axum::middleware::from_fn(api::json_cache_control))
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
        .layer(axum::middleware::from_fn(api::couch_reason_phrase))
        .layer(tower_http::compression::CompressionLayer::new())
        .with_state(state)
}

/// Start a graceful shutdown on the first SIGTERM (Docker, systemd) or SIGINT
/// (Ctrl-C). The handlers are installed here, before the caller binds the
/// listener, so a signal sent as soon as the server answers is never lost.
/// Later signals are logged and ignored: the shutdown deadline bounds the stop.
fn handle_signals(shutdown: CancellationToken) -> std::io::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    tokio::spawn(async move {
        loop {
            let name = tokio::select! {
                _ = term.recv() => "SIGTERM",
                _ = int.recv() => "SIGINT",
            };
            if shutdown.is_cancelled() {
                tracing::info!("{name} received; already shutting down");
            } else {
                tracing::info!("{name} received; shutting down");
                shutdown.cancel();
            }
        }
    });
    Ok(())
}

/// Run the server until shutdown. `--js-worker` is dispatched by the binary to
/// [`run_worker`] before any runtime exists.
///
/// A configuration error is returned as a [`ConfigError`]. Telemetry is
/// flushed on every path that got as far as starting it.
pub async fn run(args: Args) -> anyhow::Result<()> {
    if args.worker {
        anyhow::bail!("--js-worker must be started through run_worker, outside an async runtime");
    }
    validate_config(&args)?;
    js::configure(args.js_sandbox, args.js_workers);
    crate::index_cache::set_max_open(args.max_open_indexes);
    crate::cluster_ops::set_write_drain_ms(args.rebalance_write_drain_ms);
    crate::cluster_ops::set_stable_wait_ms(args.rebalance_stable_wait_ms);
    crate::db::set_rebalance_freeze_ms(args.rebalance_freeze_ms);
    crate::js::set_worker_startup_ms(args.js_worker_startup_ms);
    let telemetry = telemetry::init(args.otel_endpoint.clone(), &args.me.to_string())
        .map_err(|error| config_error(format!("telemetry: {error}")))?;
    let shutdown = CancellationToken::new();
    if std::env::var_os("DOUCHDB_EXIT_ON_STDIN_EOF").is_some() {
        shutdown_on_stdin_eof(shutdown.clone());
    }
    crate::index_cache::stop_updates_on(shutdown.clone());
    let result = serve(&args, shutdown).await;
    if let Some(telemetry) = telemetry {
        let flush = tokio::task::spawn_blocking(move || telemetry.shutdown());
        if tokio::time::timeout(TELEMETRY_FLUSH_DEADLINE, flush)
            .await
            .is_err()
        {
            tracing::warn!("telemetry flush did not finish in time");
        }
    }
    result
}

/// Start the node and serve until a shutdown signal, then stop the background
/// tasks and close the databases, all within [`SHUTDOWN_DEADLINE`] of the signal.
async fn serve(args: &Args, shutdown: CancellationToken) -> anyhow::Result<()> {
    if args.cors_credentials && args.cors_origins.iter().any(|origin| origin == "*") {
        tracing::warn!(
            "CORS allows any origin and sends credentials; any website can act as a logged-in user"
        );
    }
    std::fs::create_dir_all(&args.data)?;

    let advertise = advertise_url(args);
    let auth = build_auth(args)?;
    let state = build_state(args, auth, advertise, shutdown.clone())?;
    handle_signals(shutdown.clone())?;
    let mut tasks = Vec::new();
    if state.ctx.cluster.is_clustered() {
        tasks.push(replicate::spawn_anti_entropy(state.clone()));
        tracing::info!("cluster mode: node {} of {}", args.me, args.nodes.len());
    }
    if state.ctx.cluster.is_clustered() || !state.config.seeds.is_empty() {
        tasks.push(discovery::spawn(state.clone()));
    }

    let app = build_router(args, state.clone());

    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    tracing::info!("douchdb listening on {}", args.listen);
    let graceful = async {
        crate::listener::serve(listener, app, shutdown.clone()).await;
        // Also stops the tasks and feeds when the server failed on its own.
        shutdown.cancel();
        for task in tasks {
            let _ = task.await;
        }
        close_databases(&state).await;
        Ok(())
    };
    tokio::select! {
        result = graceful => result,
        () = async {
            shutdown.cancelled().await;
            tokio::time::sleep(SHUTDOWN_DEADLINE).await;
        } => Err(anyhow::anyhow!(
            "shutdown did not finish within {} s; exiting anyway",
            SHUTDOWN_DEADLINE.as_secs()
        )),
    }
}

/// Drop every open database off the async threads. Each one joins its shard
/// actors, which finish their queued writes first, so a restart finds every
/// acknowledged write.
async fn close_databases(state: &AppState) {
    let open = std::mem::take(&mut *state.dbs.write().await);
    if open.is_empty() {
        return;
    }
    let _ = tokio::task::spawn_blocking(move || drop(open)).await;
}

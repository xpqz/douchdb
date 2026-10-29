//! douchdb: a CouchDB wire-compatible database on LevelDB.

mod api;
mod auth;
mod cluster;
mod db;
mod error;
mod etf;
mod replicate;
mod rev;
mod revtree;
mod store;
mod ui;

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
    sync_secs: u64,
}

fn parse_args() -> Args {
    let mut args = Args {
        listen: "127.0.0.1:5984".into(),
        data: PathBuf::from("data"),
        q: 1,
        n: 2,
        revs_limit: 1000,
        admin: None,
        nodes: Vec::new(),
        me: 0,
        sync_secs: 5,
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
            "--sync-secs" => args.sync_secs = it.next().and_then(|v| v.parse().ok()).unwrap_or(5),
            "--help" | "-h" => {
                println!(
                    "douchdb [--listen ADDR] [--data DIR] [--q N] [--n N] [--revs-limit N]\n\
                     \x20       [--admin user:pass] [--nodes URL,URL] [--me INDEX] [--sync-secs N]"
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
    tracing_subscriber::fmt().with_target(false).init();
    let args = parse_args();
    std::fs::create_dir_all(&args.data)?;

    let config = Config {
        q: args.q.max(1),
        n: args.n.max(1),
        revs_limit: args.revs_limit.max(1),
        admin: args.admin.clone(),
        nodes: args.nodes.clone(),
        me: args.me,
        sync_secs: args.sync_secs,
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
        },
        client: reqwest::Client::new(),
    });

    if state.cluster.is_clustered() {
        spawn_anti_entropy(state.clone());
        tracing::info!("cluster mode: node {} of {}", args.me, args.nodes.len());
    }

    let app = Router::new()
        .route("/", get(api::welcome))
        .route("/_up", get(api::up))
        .route("/_all_dbs", get(api::all_dbs))
        .route("/_uuids", get(api::uuids))
        .route("/_membership", get(api::membership))
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
        .layer(axum::extract::DefaultBodyLimit::max(256 * 1024 * 1024))
        .layer(axum::middleware::from_fn(api::log_requests))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    tracing::info!("douchdb listening on {}", args.listen);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

/// Pull from every peer on a timer. The same `_changes`/`_bulk_get` protocol
/// clients use, so the replication path is exercised constantly.
fn spawn_anti_entropy(state: Arc<AppState>) {
    tokio::spawn(async move {
        let interval = Duration::from_secs(state.config.sync_secs.max(1));
        loop {
            tokio::time::sleep(interval).await;
            let peers: Vec<String> = state.cluster.peers().cloned().collect();
            if peers.is_empty() {
                continue;
            }
            for name in state.db_names().await {
                let Ok(db) = state.db(&name).await else {
                    continue;
                };
                for peer in &peers {
                    match replicate::pull(&db, peer, &state.client, &state.cluster).await {
                        Ok(0) => {}
                        Ok(n) => tracing::info!("replicated {n} docs of {name} from {peer}"),
                        Err(e) => tracing::warn!("replication {name} from {peer} failed: {e}"),
                    }
                }
            }
        }
    });
}

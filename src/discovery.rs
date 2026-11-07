//! Peer discovery for the cluster management view.
//!
//! Placement remains the static `--nodes` list. Discovery probes the configured
//! nodes plus any `--seed-nodes`, follows the membership lists they report, and
//! records nodes that are visible on the network but absent from the placement
//! list. That lets the console show drift without silently changing routing.

use crate::cluster_state::{now_ms, NodeProbe};
use crate::state::AppState;
use serde_json::Value;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn normalize(url: &str) -> String {
    url.trim().trim_end_matches('/').to_string()
}

async fn probe(state: &AppState, url: &str, seed: bool, trusted: bool) -> NodeProbe {
    let started = Instant::now();
    let mut probe = NodeProbe {
        url: url.to_string(),
        seed,
        ..Default::default()
    };
    let up = state
        .ctx
        .client
        .get(format!("{url}/_up"))
        .timeout(Duration::from_secs(2))
        .send()
        .await;
    match up {
        Ok(response) if response.status().is_success() => {
            probe.reachable = true;
            probe.last_seen_ms = now_ms();
            probe.latency_ms = started.elapsed().as_millis() as u64;
        }
        Ok(response) => {
            probe.error = Some(format!("_up returned {}", response.status()));
            return probe;
        }
        Err(error) => {
            probe.error = Some(error.to_string());
            return probe;
        }
    }

    // The secret and admin credentials only go to configured cluster members.
    // Seeds and advertised URLs are unvetted; they are probed anonymously, so
    // a node that requires admin for _membership reports an error here rather
    // than receiving credentials it could replay.
    let mut membership = state
        .ctx
        .client
        .get(format!("{url}/_membership"))
        .timeout(Duration::from_secs(2));
    if trusted {
        membership = membership
            .header("x-douchdb-forwarded", "1")
            .header("x-douchdb-secret", state.ctx.cluster.secret.as_str());
        if let Some(auth) = &state.ctx.auth {
            membership = membership.basic_auth(&auth.user, Some(&auth.pass));
        }
    }
    let membership = membership.send().await;
    match membership {
        Ok(response) if response.status().is_success() => {
            probe.membership = response.json::<Value>().await.ok();
        }
        Ok(response) => {
            probe.error = Some(format!("_membership returned {}", response.status()));
        }
        Err(error) => {
            probe.error = Some(error.to_string());
        }
    }
    probe
}

fn membership_nodes(probe: &NodeProbe) -> Vec<String> {
    let Some(membership) = &probe.membership else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for key in ["all_nodes", "cluster_nodes"] {
        if let Some(nodes) = membership.get(key).and_then(Value::as_array) {
            for node in nodes {
                if let Some(node) = node.as_str().filter(|node| node.contains("://")) {
                    out.push(normalize(node));
                }
            }
        }
    }
    out
}

/// One discovery pass. Safe to call concurrently; an in-flight pass wins.
pub async fn discover_once(state: Arc<AppState>) {
    let Ok(_background) = state.admission.try_background() else {
        return;
    };
    if !state.cluster_state.begin_discovery() {
        return;
    }

    let configured: Vec<String> = state
        .ctx
        .cluster
        .nodes
        .iter()
        .map(|url| normalize(url))
        .collect();
    let seeds: Vec<String> = state
        .config
        .seeds
        .iter()
        .map(|url| normalize(url))
        .collect();

    let mut candidates: Vec<(String, bool, bool)> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for url in configured.iter().chain(seeds.iter()) {
        if seen.insert(url.clone()) {
            candidates.push((url.clone(), seeds.contains(url), false));
        }
    }

    let mut probes = Vec::new();
    for (url, seed, discovered) in &candidates {
        let mut probe = probe(&state, url, *seed, configured.contains(url)).await;
        probe.discovered = *discovered;
        probes.push(probe);
    }

    // Follow membership lists one hop so a seed can reveal nodes that were not
    // configured locally. Newly found nodes are probed once, not recursively.
    let mut extra: Vec<(String, bool, bool)> = Vec::new();
    for probe in &probes {
        for url in membership_nodes(probe) {
            if seen.insert(url.clone()) {
                extra.push((url.clone(), seeds.contains(&url), true));
            }
        }
    }
    for (url, seed, discovered) in &extra {
        // `seen` already holds every configured URL, so these are never members.
        let mut probe = probe(&state, url, *seed, false).await;
        probe.discovered = *discovered;
        probes.push(probe);
    }

    let error = if !candidates.is_empty() && probes.iter().all(|probe| !probe.reachable) {
        Some("no configured node is reachable".to_string())
    } else {
        None
    };
    state.cluster_state.finish_discovery(probes, error);
}

/// Probe immediately, then retry quickly while the rest of the cluster boots,
/// and settle into the configured sync interval.
pub fn spawn(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let shutdown = state.shutdown.clone();
        let interval = Duration::from_secs(state.config.sync_secs.max(1));
        let run = async {
            let quick = [
                Duration::from_millis(200),
                Duration::from_millis(400),
                Duration::from_millis(800),
                Duration::from_millis(1600),
                Duration::from_millis(3200),
            ];
            discover_once(state.clone()).await;
            for delay in quick {
                tokio::time::sleep(delay).await;
                discover_once(state.clone()).await;
            }
            loop {
                tokio::time::sleep(interval).await;
                discover_once(state.clone()).await;
            }
        };
        tokio::select! {
            () = run => {}
            () = shutdown.cancelled() => {}
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Auth;
    use crate::cluster::Cluster;
    use crate::cluster_http::ClusterCtx;
    use crate::state::Config;
    use axum::extract::State;
    use axum::http::HeaderMap;
    use axum::routing::get;
    use axum::{Json, Router};
    use serde_json::json;

    /// Per-request record: (path, x-douchdb-secret, authorization).
    type Seen = Arc<std::sync::Mutex<Vec<(String, Option<String>, Option<String>)>>>;

    #[derive(Clone)]
    struct Stub {
        seen: Seen,
        advertised: Vec<String>,
    }

    fn record(stub: &Stub, path: &str, headers: &HeaderMap) {
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        };
        if let Ok(mut seen) = stub.seen.lock() {
            seen.push((
                path.to_string(),
                header("x-douchdb-secret"),
                header("authorization"),
            ));
        }
    }

    async fn up(State(stub): State<Stub>, headers: HeaderMap) -> Json<Value> {
        record(&stub, "/_up", &headers);
        Json(json!({"status": "ok"}))
    }

    async fn membership(State(stub): State<Stub>, headers: HeaderMap) -> Json<Value> {
        record(&stub, "/_membership", &headers);
        Json(json!({"all_nodes": stub.advertised, "cluster_nodes": stub.advertised}))
    }

    async fn serve(advertised: Vec<String>) -> (String, Seen, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let addr = listener.local_addr().expect("addr");
        let seen: Seen = Arc::default();
        let app = Router::new()
            .route("/_up", get(up))
            .route("/_membership", get(membership))
            .with_state(Stub {
                seen: seen.clone(),
                advertised,
            });
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), seen, task)
    }

    fn state(nodes: Vec<String>, seeds: Vec<String>) -> Arc<AppState> {
        Arc::new(AppState {
            root: std::env::temp_dir(),
            dbs: Default::default(),
            db_locks: Default::default(),
            fs_health: Default::default(),
            shutdown: Default::default(),
            config: Config {
                q: 1,
                revs_limit: 1000,
                sync_secs: 60,
                seeds,
                advertise: String::new(),
                cors_origins: Vec::new(),
                cors_credentials: false,
                cors_headers: Vec::new(),
                view_timeout_ms: 1000,
                view_memory_mb: 1,
                default_admin_only: true,
                max_open_dbs: 256,
            },
            ctx: ClusterCtx {
                auth: Some(Auth::new("admin".into(), "hunter2".into(), b"k".to_vec())),
                cluster: Cluster {
                    nodes,
                    me: 0,
                    n: 1,
                    secret: "cluster-secret".to_string(),
                },
                client: reqwest::Client::new(),
            },
            db_updates: Default::default(),
            cluster_state: Arc::new(crate::cluster_state::ClusterState::new()),
            admission: Arc::new(crate::admission::Admission::new(Default::default())),
            compaction: Arc::new(tokio::sync::Semaphore::new(1)),
        })
    }

    #[tokio::test]
    async fn secret_and_admin_credentials_go_only_to_configured_nodes() {
        let (hostile, hostile_seen, hostile_task) = serve(Vec::new()).await;
        let (stranger, stranger_seen, stranger_task) = serve(Vec::new()).await;
        // The configured node advertises the stranger.
        let (member, member_seen, member_task) = serve(vec![stranger.clone()]).await;
        let st = state(vec![member.clone()], vec![hostile.clone()]);

        discover_once(st.clone()).await;

        let member_seen = member_seen.lock().expect("lock").clone();
        assert!(
            member_seen
                .iter()
                .any(|(path, secret, auth)| path == "/_membership"
                    && secret.as_deref() == Some("cluster-secret")
                    && auth.is_some()),
            "configured node must still get the secret: {member_seen:?}"
        );
        for (name, seen) in [("seed", &hostile_seen), ("discovered", &stranger_seen)] {
            let seen = seen.lock().expect("lock").clone();
            assert!(!seen.is_empty(), "{name} node was not probed");
            for (path, secret, auth) in seen {
                assert!(
                    secret.is_none() && auth.is_none(),
                    "{name} node received credentials on {path}"
                );
            }
        }

        // The stranger still shows up, reachable, as a discovered node.
        let snapshot = st
            .cluster_state
            .discovery_json(&st.ctx.cluster.nodes, &st.config.seeds);
        let nodes = snapshot["nodes"].as_array().cloned().unwrap_or_default();
        assert!(nodes
            .iter()
            .any(|node| node["url"] == stranger.as_str() && node["discovered"] == true));

        hostile_task.abort();
        stranger_task.abort();
        member_task.abort();
    }
}

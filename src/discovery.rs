//! Peer discovery for the cluster management view.
//!
//! Placement remains the static `--nodes` list. Discovery probes the configured
//! nodes plus any `--seed-nodes`, follows the membership lists they report, and
//! records nodes that are visible on the network but absent from the placement
//! list. That lets the console show drift without silently changing routing.

use crate::api::AppState;
use crate::cluster_state::{now_ms, NodeProbe};
use serde_json::Value;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn normalize(url: &str) -> String {
    url.trim().trim_end_matches('/').to_string()
}

async fn probe(state: &AppState, url: &str, seed: bool) -> NodeProbe {
    let started = Instant::now();
    let mut probe = NodeProbe {
        url: url.to_string(),
        seed,
        ..Default::default()
    };
    let up = state
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

    let mut membership = state
        .client
        .get(format!("{url}/_membership"))
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", state.cluster.secret.as_str())
        .timeout(Duration::from_secs(2));
    if let Some(auth) = &state.auth {
        membership = membership.basic_auth(&auth.user, Some(&auth.pass));
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
    if !state.cluster_state.begin_discovery() {
        return;
    }

    let configured: Vec<String> = state
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
        let mut probe = probe(&state, url, *seed).await;
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
        let mut probe = probe(&state, url, *seed).await;
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
pub fn spawn(state: Arc<AppState>) {
    tokio::spawn(async move {
        let interval = Duration::from_secs(state.config.sync_secs.max(1));
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
    });
}

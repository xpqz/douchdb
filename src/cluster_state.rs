//! Observable state for cluster discovery and background replication.
//!
//! This is deliberately separate from placement: `Cluster` stays the static
//! source of truth for routing, while these snapshots describe what the node
//! has observed and what the replicator has done.

use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Clone, Debug, Default)]
pub struct NodeProbe {
    pub url: String,
    pub reachable: bool,
    pub latency_ms: u64,
    pub last_seen_ms: u64,
    pub error: Option<String>,
    pub membership: Option<Value>,
    /// True when a peer reported this node but it is not in the configured
    /// placement list.
    pub discovered: bool,
    pub seed: bool,
}

#[derive(Clone, Debug, Default)]
struct DiscoverySnapshot {
    last_run_ms: u64,
    last_finished_ms: u64,
    error: Option<String>,
    nodes: Vec<NodeProbe>,
}

#[derive(Clone, Debug, Default)]
pub struct ReplicationEntry {
    pub db: String,
    pub peer: String,
    pub state: String,
    pub started_ms: u64,
    pub finished_ms: u64,
    pub docs: u64,
    pub error: Option<String>,
    pub checkpoint: Option<Value>,
}

#[derive(Clone, Debug, Default)]
struct ReplicationSnapshot {
    last_run_ms: u64,
    last_finished_ms: u64,
    runs: u64,
    docs: u64,
    error: Option<String>,
    entries: BTreeMap<String, ReplicationEntry>,
}

#[derive(Default)]
pub struct ClusterState {
    discovery: Mutex<DiscoverySnapshot>,
    replication: Mutex<ReplicationSnapshot>,
    discovery_running: AtomicBool,
    replication_running: AtomicBool,
    replication_paused: AtomicBool,
}

impl ClusterState {
    pub fn new() -> ClusterState {
        ClusterState::default()
    }

    pub fn discovery_running(&self) -> bool {
        self.discovery_running.load(Ordering::SeqCst)
    }

    /// Claim the discovery slot. Returns false when another pass is running.
    pub fn begin_discovery(&self) -> bool {
        if self
            .discovery_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return false;
        }
        let mut snapshot = self.lock_discovery();
        snapshot.last_run_ms = now_ms();
        snapshot.error = None;
        true
    }

    pub fn finish_discovery(&self, nodes: Vec<NodeProbe>, error: Option<String>) {
        let mut snapshot = self.lock_discovery();
        snapshot.last_finished_ms = now_ms();
        snapshot.error = error;
        snapshot.nodes = nodes;
        self.discovery_running.store(false, Ordering::SeqCst);
    }

    pub fn discovery_json(&self, configured: &[String], seeds: &[String]) -> Value {
        let snapshot = self.lock_discovery();
        let nodes: Vec<Value> = snapshot
            .nodes
            .iter()
            .map(|node| {
                json!({
                    "url": node.url,
                    "in_membership": configured.iter().any(|url| url == &node.url),
                    "seed": node.seed,
                    "discovered": node.discovered,
                    "reachable": node.reachable,
                    "latency_ms": node.latency_ms,
                    "last_seen": node.last_seen_ms,
                    "membership": node.membership,
                    "error": node.error,
                })
            })
            .collect();
        json!({
            "running": self.discovery_running(),
            "last_run": snapshot.last_run_ms,
            "last_finished": snapshot.last_finished_ms,
            "error": snapshot.error,
            "configured": configured,
            "seeds": seeds,
            "nodes": nodes,
        })
    }

    pub fn replication_running(&self) -> bool {
        self.replication_running.load(Ordering::SeqCst)
    }

    pub fn replication_paused(&self) -> bool {
        self.replication_paused.load(Ordering::SeqCst)
    }

    pub fn set_replication_paused(&self, paused: bool) {
        self.replication_paused.store(paused, Ordering::SeqCst);
    }

    /// Claim the replicator slot. Returns false when a pass is already running.
    pub fn begin_replication(&self) -> bool {
        if self
            .replication_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return false;
        }
        let mut snapshot = self.lock_replication();
        snapshot.last_run_ms = now_ms();
        snapshot.error = None;
        snapshot.runs += 1;
        true
    }

    pub fn finish_replication(&self, error: Option<String>) {
        let mut snapshot = self.lock_replication();
        snapshot.last_finished_ms = now_ms();
        snapshot.error = error;
        self.replication_running.store(false, Ordering::SeqCst);
    }

    pub fn replication_started(&self, db: &str, peer: &str) {
        let mut snapshot = self.lock_replication();
        let key = entry_key(db, peer);
        let entry = snapshot
            .entries
            .entry(key)
            .or_insert_with(|| ReplicationEntry {
                db: db.to_string(),
                peer: peer.to_string(),
                state: "idle".to_string(),
                ..Default::default()
            });
        entry.state = "running".to_string();
        entry.started_ms = now_ms();
        entry.error = None;
    }

    pub fn replication_finished(&self, db: &str, peer: &str, docs: u64, checkpoint: Option<Value>) {
        let mut snapshot = self.lock_replication();
        snapshot.docs += docs;
        let entry = snapshot
            .entries
            .entry(entry_key(db, peer))
            .or_insert_with(|| ReplicationEntry {
                db: db.to_string(),
                peer: peer.to_string(),
                ..Default::default()
            });
        entry.state = "ok".to_string();
        entry.finished_ms = now_ms();
        entry.docs = docs;
        entry.error = None;
        entry.checkpoint = checkpoint;
    }

    pub fn replication_failed(&self, db: &str, peer: &str, error: String) {
        let mut snapshot = self.lock_replication();
        if snapshot.error.is_none() {
            snapshot.error = Some(error.clone());
        }
        let entry = snapshot
            .entries
            .entry(entry_key(db, peer))
            .or_insert_with(|| ReplicationEntry {
                db: db.to_string(),
                peer: peer.to_string(),
                ..Default::default()
            });
        entry.state = "error".to_string();
        entry.finished_ms = now_ms();
        entry.error = Some(error);
    }

    pub fn replication_json(&self) -> Value {
        let snapshot = self.lock_replication();
        let entries: Vec<Value> = snapshot
            .entries
            .values()
            .map(|entry| {
                json!({
                    "db": entry.db,
                    "peer": entry.peer,
                    "state": entry.state,
                    "started": entry.started_ms,
                    "finished": entry.finished_ms,
                    "docs": entry.docs,
                    "error": entry.error,
                    "checkpoint": entry.checkpoint,
                })
            })
            .collect();
        json!({
            "running": self.replication_running(),
            "paused": self.replication_paused(),
            "last_run": snapshot.last_run_ms,
            "last_finished": snapshot.last_finished_ms,
            "runs": snapshot.runs,
            "docs": snapshot.docs,
            "error": snapshot.error,
            "entries": entries,
        })
    }

    fn lock_discovery(&self) -> std::sync::MutexGuard<'_, DiscoverySnapshot> {
        self.discovery
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn lock_replication(&self) -> std::sync::MutexGuard<'_, ReplicationSnapshot> {
        self.replication
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

fn entry_key(db: &str, peer: &str) -> String {
    format!("{db}\0{peer}")
}

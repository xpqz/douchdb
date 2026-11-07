//! Local and cluster-wide metrics and health views.
//!
//! This module owns the observability handlers; the HTTP router in `api`
//! re-exports them so route registration stays in one place.

use super::{access_denied, flag, internal_request, query, AppState};
use crate::state::FsHealth;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::Response;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// observability

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn dir_size(path: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    let mut total = 0u64;
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            total += dir_size(&entry.path());
        } else if meta.is_file() {
            total += meta.len();
        }
    }
    total
}

/// Probe the data root: list it, write and remove a scratch file, and check
/// that every database directory still carries a parseable `db.json`. Opening
/// every database would be far more expensive. Blocking; call it from
/// `spawn_blocking`.
fn probe_fs(root: &std::path::Path) -> FsHealth {
    let readable = std::fs::read_dir(root).is_ok();
    let probe = root.join(format!(".health-{}", crate::util::uuid()));
    let writable = std::fs::write(&probe, b"ok").is_ok();
    let _ = std::fs::remove_file(&probe);
    let mut unreadable = 0u64;
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let db_file = entry.path().join("db.json");
            if db_file.is_file()
                && std::fs::read(&db_file)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                    .is_none()
            {
                unreadable += 1;
            }
        }
    }
    FsHealth {
        readable,
        writable,
        unreadable,
    }
}

async fn fs_health(st: &AppState) -> FsHealth {
    let now = std::time::Instant::now();
    if let Some(cached) = st.fs_health.fresh(now) {
        return cached;
    }
    let root = st.root.clone();
    match tokio::task::spawn_blocking(move || probe_fs(&root)).await {
        Ok(value) => {
            st.fs_health.store(now, value);
            value
        }
        // The probe itself failed; report storage as unusable but retry next time.
        Err(_) => FsHealth {
            readable: false,
            writable: false,
            unreadable: 0,
        },
    }
}

fn node_json(st: &AppState) -> Value {
    json!({
        "index": st.ctx.cluster.me,
        "url": st.config.advertise,
        "clustered": st.ctx.cluster.is_clustered(),
        "q": st.config.q,
        "n": st.ctx.cluster.replication_factor(),
        "sync_secs": st.config.sync_secs,
    })
}

/// Process-local metrics plus the storage, database, replication, discovery and
/// health detail that needs database access.
async fn local_metrics(st: &AppState) -> Value {
    let mut metrics = crate::metrics::snapshot();
    let names = st.db_names().await;
    let mut databases = Vec::new();
    let mut total_bytes = 0u64;
    let mut total_docs = 0u64;
    let mut total_deleted = 0u64;
    let mut failed = Vec::new();
    for name in &names {
        match st.db(name).await {
            Ok(db) => {
                let info = db.info().await.unwrap_or(Value::Null);
                let counts = db.shard_counts().await.unwrap_or_default();
                let map = db.shard_map();
                let local_shards = (0..map.q)
                    .filter(|shard| map.replicas_of(*shard).contains(&st.ctx.cluster.me))
                    .count();
                let docs: u64 = counts.iter().map(|(docs, _)| *docs).sum();
                let deleted: u64 = counts.iter().map(|(_, deleted)| *deleted).sum();
                let db_path = crate::db::db_path(&st.root, name);
                let bytes = tokio::task::spawn_blocking(move || dir_size(&db_path))
                    .await
                    .unwrap_or(0);
                total_bytes += bytes;
                total_docs += docs;
                total_deleted += deleted;
                let shards: Vec<Value> = counts
                    .iter()
                    .enumerate()
                    .map(|(shard, (docs, deleted))| {
                        json!({
                            "shard": shard,
                            "docs": docs,
                            "deleted": deleted,
                            "replicas": map.replicas_of(shard as u32),
                            "owner": map.owner_of(shard as u32),
                            "local": map.replicas_of(shard as u32).contains(&st.ctx.cluster.me),
                        })
                    })
                    .collect();
                databases.push(json!({
                    "name": name,
                    "partitioned": db.partitioned,
                    "q": db.q,
                    "doc_count": docs,
                    "doc_del_count": deleted,
                    "update_seq": info["update_seq"],
                    "shard_map_version": map.version,
                    "pending_shard_map": db.pending_shard_map().is_some(),
                    "local_shards": local_shards,
                    "disk_bytes": bytes,
                    "shards": shards,
                }));
            }
            Err(error) => failed.push(json!({"name": name, "error": error.reason})),
        }
    }
    metrics["node"] = node_json(st);
    metrics["generated_at"] = json!(now_ms());
    metrics["storage"] = json!({
        "root": st.root,
        "databases": names.len(),
        "disk_bytes": total_bytes,
        "disk_mb": total_bytes as f64 / (1024.0 * 1024.0),
        "doc_count": total_docs,
        "doc_del_count": total_deleted,
        "failed_databases": failed,
    });
    metrics["databases"] = json!(databases);
    metrics["replication"] = st.cluster_state.replication_json();
    metrics["discovery"] = st
        .cluster_state
        .discovery_json(&st.ctx.cluster.nodes, &st.config.seeds);
    metrics["health"] = local_health(st, true).await;
    metrics
}

fn health_rank(status: &str) -> u8 {
    match status {
        "critical" => 2,
        "degraded" => 1,
        _ => 0,
    }
}

fn health_status_name(rank: u8) -> &'static str {
    match rank {
        2 => "critical",
        1 => "degraded",
        _ => "ok",
    }
}

/// The process check: pid, uptime, threads and resident memory.
fn check_process(process: &Value) -> Value {
    let threads = process["threads"]
        .as_u64()
        .map(|threads| threads.to_string())
        .unwrap_or_else(|| "unavailable".to_string());
    let rss = process["rss_mb"]
        .as_f64()
        .map(|rss| format!("{rss:.1} MB"))
        .unwrap_or_else(|| "unavailable".to_string());
    json!({
        "name": "process",
        "status": "ok",
        "detail": format!(
            "pid {} · uptime {}s · {} threads · {} rss",
            process["pid"], process["uptime_secs"], threads, rss
        ),
        "uptime_secs": process["uptime_secs"],
        "rss_mb": process["rss_mb"],
    })
}

/// Whether the data directory is readable and writable.
async fn check_storage(st: &AppState, detailed: bool) -> (Value, usize, u64) {
    let root = &st.root;
    let FsHealth {
        readable,
        writable,
        unreadable,
    } = fs_health(st).await;
    let databases = st.db_names().await.len();
    let check = json!({
        "name": "storage",
        "status": if readable && writable { "ok" } else { "critical" },
        "detail": if detailed {
            format!(
                "{} · {} databases · readable={readable} writable={writable}",
                root.display(),
                databases
            )
        } else {
            format!("{databases} databases · readable={readable} writable={writable}")
        },
        "databases": databases,
        "readable": readable,
        "writable": writable,
    });
    (check, databases, unreadable)
}

/// Databases whose shard map a peer has not acknowledged.
async fn check_placement(st: &AppState) -> Value {
    let pending = {
        let dbs = st.dbs.read().await;
        dbs.values()
            .filter(|db| db.pending_shard_map().is_some())
            .count()
    };
    json!({
        "name": "placement",
        "status": if pending == 0 { "ok" } else { "degraded" },
        "detail": format!("{pending} database(s) with an unacknowledged shard map"),
        "pending_databases": pending,
    })
}

/// Databases whose files could not be read.
fn check_databases(databases: usize, unreadable: u64) -> Value {
    json!({
        "name": "databases",
        "status": if unreadable == 0 { "ok" } else { "critical" },
        "detail": format!("{databases} databases · {unreadable} unreadable"),
        "unreadable": unreadable,
    })
}

/// Whether the placement nodes answer the discovery probes.
fn check_cluster(st: &AppState) -> Value {
    if st.ctx.cluster.is_clustered() {
        let mut discovery = st
            .cluster_state
            .discovery_json(&st.ctx.cluster.nodes, &st.config.seeds);
        let nodes = crate::json::take_array(&mut discovery, "nodes").unwrap_or_default();
        let placement: Vec<&Value> = nodes
            .iter()
            .filter(|node| node["in_membership"] == true)
            .collect();
        let total = placement.len();
        let reachable = placement
            .iter()
            .filter(|node| node["reachable"] == true)
            .count();
        let probed = placement
            .iter()
            .any(|node| node["last_seen"].as_u64().unwrap_or(0) > 0);
        let (status, detail) = if total == 0 || !probed {
            ("degraded", "no peer probes yet".to_string())
        } else if reachable < total {
            (
                "degraded",
                format!("{reachable}/{total} placement nodes reachable"),
            )
        } else {
            (
                "ok",
                format!("{reachable}/{total} placement nodes reachable"),
            )
        };
        json!({
            "name": "cluster",
            "status": status,
            "detail": detail,
            "reachable": reachable,
            "nodes": total,
        })
    } else {
        json!({"name": "cluster", "status": "ok", "detail": "single node"})
    }
}

/// Whether background replication is running.
fn check_replication(st: &AppState) -> Value {
    let replication = st.cluster_state.replication_json();
    let (status, detail) = if replication["paused"] == true {
        ("degraded", "background replication paused".to_string())
    } else if let Some(error) = replication["error"].as_str() {
        ("degraded", format!("last pass: {error}"))
    } else if st.ctx.cluster.is_clustered()
        && replication["last_finished"].as_u64().unwrap_or(0) == 0
    {
        ("degraded", "no replication pass yet".to_string())
    } else {
        (
            "ok",
            format!(
                "{} runs · {} docs",
                replication["runs"], replication["docs"]
            ),
        )
    };
    json!({
        "name": "replication",
        "status": status,
        "detail": detail,
        "running": replication["running"],
        "paused": replication["paused"],
        "last_finished": replication["last_finished"],
        "runs": replication["runs"],
        "docs": replication["docs"],
    })
}

/// The recent 5xx rate.
fn check_http() -> Value {
    // Authorization denials and other 4xx responses are expected client
    // behaviour, not evidence that the server is unhealthy. Only 5xx responses
    // contribute to the readiness signal. The ratio covers a recent window and
    // leaves out probe requests, so a node that has recovered reports healthy
    // again even while a load balancer keeps polling it.
    let (total, errors) = crate::metrics::recent_http();
    let window = crate::metrics::HEALTH_WINDOW_SECS;
    let rate = if total == 0 {
        0.0
    } else {
        errors as f64 / total as f64 * 100.0
    };
    let (status, detail) = if total < 20 {
        (
            "ok",
            format!("{total} requests sampled in the last {window}s"),
        )
    } else if rate >= 25.0 {
        (
            "critical",
            format!("{rate:.1}% of {total} requests in the last {window}s failed with 5xx"),
        )
    } else if rate >= 10.0 {
        (
            "degraded",
            format!("{rate:.1}% of {total} requests in the last {window}s failed with 5xx"),
        )
    } else {
        (
            "ok",
            format!("{rate:.1}% of {total} requests in the last {window}s failed with 5xx"),
        )
    };
    json!({
        "name": "http",
        "status": status,
        "detail": detail,
        "errors": errors,
        "total": total,
        "error_rate_pct": rate,
    })
}

/// Health is deliberately explicit about what is checked. It says nothing
/// about power-loss durability: rusty-leveldb does not fsync.
async fn local_health(st: &AppState, detailed: bool) -> Value {
    let snapshot = crate::metrics::snapshot();
    let process = &snapshot["process"];
    let (storage, databases, unreadable) = check_storage(st, detailed).await;
    let checks = vec![
        check_process(process),
        storage,
        check_placement(st).await,
        check_databases(databases, unreadable),
        check_cluster(st),
        check_replication(st),
        check_http(),
    ];

    let worst = checks
        .iter()
        .map(|check| health_rank(check["status"].as_str().unwrap_or("ok")))
        .max()
        .unwrap_or(0);
    json!({
        "status": health_status_name(worst),
        "ok": worst == 0,
        "checks": checks,
        "generated_at": now_ms(),
        "uptime_secs": process["uptime_secs"],
    })
}

#[derive(Default)]
struct RouteAgg {
    requests: u64,
    errors: u64,
    total_ms: f64,
    max_ms: f64,
    buckets: Vec<u64>,
}

#[derive(Default)]
struct EngineAgg {
    queries: u64,
    errors: u64,
    total_ms: f64,
    max_ms: f64,
}

#[derive(Default)]
struct DbAgg {
    q: u64,
    partitioned: bool,
    update_seq: u64,
    local_shards: u64,
    disk_bytes: u64,
    nodes: u64,
    /// Per-shard live/deleted counts. Replicas hold the same shard, so the
    /// cluster-wide count is the sum of per-shard maxima: not the sum of node
    /// totals (double counts replicas) and not the maximum node total (misses
    /// shards that only another node owns).
    shards: BTreeMap<u64, (u64, u64)>,
}

fn merge_u64_map(into: &mut BTreeMap<String, u64>, value: &Value) {
    if let Some(map) = value.as_object() {
        for (key, count) in map {
            *into.entry(key.clone()).or_default() += count.as_u64().unwrap_or(0);
        }
    }
}

fn u64_map_json(map: BTreeMap<String, u64>) -> Value {
    Value::Object(
        map.into_iter()
            .map(|(key, value)| (key, json!(value)))
            .collect(),
    )
}

fn percentile_from(buckets: &[u64], requests: u64, pct: f64, max_ms: f64) -> f64 {
    if requests == 0 {
        return 0.0;
    }
    let target = (requests as f64 * pct / 100.0).ceil() as u64;
    let mut seen = 0u64;
    for (index, count) in buckets.iter().enumerate() {
        seen += count;
        if seen >= target {
            return crate::metrics::LATENCY_BOUNDS
                .get(index)
                .copied()
                .unwrap_or(max_ms);
        }
    }
    max_ms
}

/// Merge node snapshots into one cluster-wide view. Counters are summed, route
/// latency buckets are merged before percentiles are recomputed, and per-database
/// document counts take the per-node maximum (replicas hold the same documents)
/// while disk usage is summed.
fn aggregate_metrics(snapshots: &[Value]) -> Map<String, Value> {
    let mut http_total = 0u64;
    let mut in_flight = 0u64;
    let mut http_errors = 0u64;
    let mut total_ms = 0f64;
    let mut max_ms = 0f64;
    let mut buckets = vec![0u64; crate::metrics::LATENCY_BOUNDS.len()];
    let mut status_classes = BTreeMap::new();
    let mut status_codes = BTreeMap::new();
    let mut methods = BTreeMap::new();
    let mut routes: BTreeMap<String, RouteAgg> = BTreeMap::new();
    let mut seconds: BTreeMap<u64, (u64, u64, f64)> = BTreeMap::new();
    let mut views = EngineAgg::default();
    let mut search = EngineAgg::default();
    let mut local_reads = 0u64;
    let mut remote_reads = 0u64;
    let mut quorum_failures = 0u64;
    let mut degraded_reads = 0u64;
    let mut fanout_count = 0u64;
    let mut fanout_sum = 0u64;
    let mut fanout_max = 0u64;
    let mut shard_map_updates = 0u64;
    let mut shard_map_pending = 0u64;
    let mut rebalance_started = 0u64;
    let mut rebalance_completed = 0u64;
    let mut rebalance_timeout = 0u64;
    let mut cleaned_shards = 0u64;
    let mut corrupt_records_skipped = 0u64;
    let mut disk_bytes = 0u64;
    let mut failed_databases = 0u64;
    let mut databases: BTreeMap<String, DbAgg> = BTreeMap::new();
    let mut replication_runs = 0u64;
    let mut replication_docs = 0u64;
    let mut replication_error: Option<String> = None;
    let mut replication_running = false;
    let mut replication_paused = false;
    let mut replication_entries: BTreeMap<String, Value> = BTreeMap::new();
    let mut worst_health = 0u8;
    let mut health_nodes = Vec::new();
    let mut health_failing = Vec::new();

    for snapshot in snapshots {
        let http = &snapshot["http"];
        http_total += http["total"].as_u64().unwrap_or(0);
        in_flight += http["in_flight"].as_u64().unwrap_or(0);
        http_errors += http["errors"].as_u64().unwrap_or(0);
        total_ms += http["latency_ms"]["avg"].as_f64().unwrap_or(0.0)
            * http["total"].as_u64().unwrap_or(0) as f64;
        max_ms = max_ms.max(http["latency_ms"]["max"].as_f64().unwrap_or(0.0));
        if let Some(counts) = http["latency_buckets"]["counts"].as_array() {
            for (index, count) in counts.iter().enumerate() {
                if let Some(slot) = buckets.get_mut(index) {
                    *slot += count.as_u64().unwrap_or(0);
                }
            }
        }
        merge_u64_map(&mut status_classes, &http["status_classes"]);
        merge_u64_map(&mut status_codes, &http["status_codes"]);
        merge_u64_map(&mut methods, &http["methods"]);
        if let Some(items) = http["routes"].as_array() {
            for item in items {
                let route = item["route"].as_str().unwrap_or("unknown").to_string();
                let entry = routes.entry(route).or_insert_with(|| RouteAgg {
                    buckets: vec![0u64; crate::metrics::LATENCY_BOUNDS.len()],
                    ..Default::default()
                });
                entry.requests += item["requests"].as_u64().unwrap_or(0);
                entry.errors += item["errors"].as_u64().unwrap_or(0);
                entry.total_ms += item["total_ms"].as_f64().unwrap_or(0.0);
                entry.max_ms = entry.max_ms.max(item["max_ms"].as_f64().unwrap_or(0.0));
                if let Some(counts) = item["latency_buckets"].as_array() {
                    for (index, count) in counts.iter().enumerate() {
                        if let Some(slot) = entry.buckets.get_mut(index) {
                            *slot += count.as_u64().unwrap_or(0);
                        }
                    }
                }
            }
        }
        if let Some(items) = http["per_second"].as_array() {
            for item in items {
                let second = item["t"].as_u64().unwrap_or(0);
                let entry = seconds.entry(second).or_insert((0, 0, 0.0));
                entry.0 += item["requests"].as_u64().unwrap_or(0);
                entry.1 += item["errors"].as_u64().unwrap_or(0);
                entry.2 += item["avg_ms"].as_f64().unwrap_or(0.0)
                    * item["requests"].as_u64().unwrap_or(0) as f64;
            }
        }

        for (target, value) in [
            (&mut views, &snapshot["views"]),
            (&mut search, &snapshot["search"]),
        ] {
            target.queries += value["queries"].as_u64().unwrap_or(0);
            target.errors += value["errors"].as_u64().unwrap_or(0);
            target.total_ms += value["total_ms"].as_f64().unwrap_or(0.0);
            target.max_ms = target.max_ms.max(value["max_ms"].as_f64().unwrap_or(0.0));
        }

        let fabric = &snapshot["fabric"];
        local_reads += fabric["local_reads"].as_u64().unwrap_or(0);
        remote_reads += fabric["remote_reads"].as_u64().unwrap_or(0);
        quorum_failures += fabric["quorum_failures"].as_u64().unwrap_or(0);
        degraded_reads += fabric["degraded_reads"].as_u64().unwrap_or(0);
        fanout_count += fabric["fanout_count"].as_u64().unwrap_or(0);
        fanout_sum += fabric["fanout_sum"].as_u64().unwrap_or(0);
        fanout_max = fanout_max.max(fabric["fanout_max"].as_u64().unwrap_or(0));
        shard_map_updates += fabric["shard_map_updates"].as_u64().unwrap_or(0);
        shard_map_pending += fabric["shard_map_pending"].as_u64().unwrap_or(0);
        rebalance_started += fabric["rebalance_started"].as_u64().unwrap_or(0);
        rebalance_completed += fabric["rebalance_completed"].as_u64().unwrap_or(0);
        rebalance_timeout += fabric["rebalance_timeout"].as_u64().unwrap_or(0);
        cleaned_shards += fabric["cleaned_shards"].as_u64().unwrap_or(0);
        corrupt_records_skipped += snapshot["integrity"]["corrupt_records_skipped"]
            .as_u64()
            .unwrap_or(0);

        let storage = &snapshot["storage"];
        disk_bytes += storage["disk_bytes"].as_u64().unwrap_or(0);
        failed_databases += storage["failed_databases"]
            .as_array()
            .map(|failed| failed.len() as u64)
            .unwrap_or(0);
        if let Some(items) = snapshot["databases"].as_array() {
            for item in items {
                let name = item["name"].as_str().unwrap_or("unknown").to_string();
                let entry = databases.entry(name).or_default();
                entry.q = item["q"].as_u64().unwrap_or(0);
                entry.partitioned |= item["partitioned"] == true;
                entry.update_seq = entry
                    .update_seq
                    .max(item["update_seq"].as_u64().unwrap_or(0));
                entry.local_shards += item["local_shards"].as_u64().unwrap_or(0);
                entry.disk_bytes += item["disk_bytes"].as_u64().unwrap_or(0);
                entry.nodes += 1;
                if let Some(shards) = item["shards"].as_array() {
                    for shard in shards {
                        let id = shard["shard"].as_u64().unwrap_or(0);
                        let entry = entry.shards.entry(id).or_insert((0, 0));
                        entry.0 = entry.0.max(shard["docs"].as_u64().unwrap_or(0));
                        entry.1 = entry.1.max(shard["deleted"].as_u64().unwrap_or(0));
                    }
                }
            }
        }
        let replication = &snapshot["replication"];
        replication_runs += replication["runs"].as_u64().unwrap_or(0);
        replication_docs += replication["docs"].as_u64().unwrap_or(0);
        replication_running |= replication["running"] == true;
        replication_paused |= replication["paused"] == true;
        if replication_error.is_none() {
            if let Some(error) = replication["error"].as_str() {
                replication_error = Some(error.to_string());
            }
        }
        if let Some(items) = replication["entries"].as_array() {
            for item in items {
                let key = format!(
                    "{}|{}",
                    item["db"].as_str().unwrap_or("?"),
                    item["peer"].as_str().unwrap_or("?")
                );
                let newer = replication_entries
                    .get(&key)
                    .and_then(|old| old["finished"].as_u64())
                    .unwrap_or(0)
                    <= item["finished"].as_u64().unwrap_or(0);
                if newer {
                    replication_entries.insert(key, item.clone());
                }
            }
        }

        let health = &snapshot["health"];
        let status = health["status"].as_str().unwrap_or("ok");
        worst_health = worst_health.max(health_rank(status));
        health_nodes.push(json!({
            "index": snapshot["node"]["index"],
            "url": snapshot["node"]["url"],
            "status": status,
            "checks": health["checks"],
            "uptime_secs": health["uptime_secs"],
        }));
        if status != "ok" {
            health_failing.push(json!({
                "index": snapshot["node"]["index"],
                "url": snapshot["node"]["url"],
                "status": status,
                "checks": health["checks"],
            }));
        }
    }

    let doc_count: u64 = databases
        .values()
        .map(|entry| entry.shards.values().map(|(docs, _)| *docs).sum::<u64>())
        .sum();
    let doc_del_count: u64 = databases
        .values()
        .map(|entry| {
            entry
                .shards
                .values()
                .map(|(_, deleted)| *deleted)
                .sum::<u64>()
        })
        .sum();

    let mut route_rows: Vec<Value> = routes
        .iter()
        .map(|(route, stat)| {
            json!({
                "route": route,
                "requests": stat.requests,
                "errors": stat.errors,
                "error_rate_pct": if stat.requests == 0 { 0.0 } else { stat.errors as f64 / stat.requests as f64 * 100.0 },
                "total_ms": stat.total_ms,
                "avg_ms": if stat.requests == 0 { 0.0 } else { stat.total_ms / stat.requests as f64 },
                "p50_ms": percentile_from(&stat.buckets, stat.requests, 50.0, stat.max_ms),
                "p95_ms": percentile_from(&stat.buckets, stat.requests, 95.0, stat.max_ms),
                "p99_ms": percentile_from(&stat.buckets, stat.requests, 99.0, stat.max_ms),
                "max_ms": stat.max_ms,
                "latency_buckets": stat.buckets,
            })
        })
        .collect();
    route_rows.sort_by(|left, right| right["requests"].as_u64().cmp(&left["requests"].as_u64()));

    let per_second: Vec<Value> = seconds
        .into_iter()
        .map(|(second, (requests, errors, total))| {
            json!({
                "t": second,
                "requests": requests,
                "errors": errors,
                "avg_ms": if requests == 0 { 0.0 } else { total / requests as f64 },
            })
        })
        .collect();

    let database_rows: Vec<Value> = databases
        .into_iter()
        .map(|(name, db)| {
            let docs: u64 = db.shards.values().map(|(docs, _)| *docs).sum();
            let deleted: u64 = db.shards.values().map(|(_, deleted)| *deleted).sum();
            json!({
                "name": name,
                "q": db.q,
                "partitioned": db.partitioned,
                "doc_count": docs,
                "doc_del_count": deleted,
                "update_seq": db.update_seq,
                "local_shards": db.local_shards,
                "disk_bytes": db.disk_bytes,
                "nodes_reporting": db.nodes,
            })
        })
        .collect();

    let mut out = Map::new();
    out.insert(
        "http".to_string(),
        json!({
            "total": http_total,
            "in_flight": in_flight,
            "errors": http_errors,
            "error_rate_pct": if http_total == 0 { 0.0 } else { http_errors as f64 / http_total as f64 * 100.0 },
            "status_classes": u64_map_json(status_classes),
            "status_codes": u64_map_json(status_codes),
            "methods": u64_map_json(methods),
            "latency_ms": {
                "avg": if http_total == 0 { 0.0 } else { total_ms / http_total as f64 },
                "p50": percentile_from(&buckets, http_total, 50.0, max_ms),
                "p95": percentile_from(&buckets, http_total, 95.0, max_ms),
                "p99": percentile_from(&buckets, http_total, 99.0, max_ms),
                "max": max_ms,
            },
            "latency_buckets": {
                "bounds": crate::metrics::LATENCY_BOUNDS,
                "counts": buckets,
            },
            "routes": route_rows,
            "per_second": per_second,
        }),
    );
    out.insert(
        "views".to_string(),
        json!({
            "queries": views.queries,
            "errors": views.errors,
            "error_rate_pct": if views.queries == 0 { 0.0 } else { views.errors as f64 / views.queries as f64 * 100.0 },
            "total_ms": views.total_ms,
            "avg_ms": if views.queries == 0 { 0.0 } else { views.total_ms / views.queries as f64 },
            "max_ms": views.max_ms,
        }),
    );
    out.insert(
        "search".to_string(),
        json!({
            "queries": search.queries,
            "errors": search.errors,
            "error_rate_pct": if search.queries == 0 { 0.0 } else { search.errors as f64 / search.queries as f64 * 100.0 },
            "total_ms": search.total_ms,
            "avg_ms": if search.queries == 0 { 0.0 } else { search.total_ms / search.queries as f64 },
            "max_ms": search.max_ms,
        }),
    );
    out.insert(
        "fabric".to_string(),
        json!({
            "local_reads": local_reads,
            "remote_reads": remote_reads,
            "quorum_failures": quorum_failures,
            "degraded_reads": degraded_reads,
            "fanout_count": fanout_count,
            "fanout_sum": fanout_sum,
            "fanout_avg": if fanout_count == 0 { 0.0 } else { fanout_sum as f64 / fanout_count as f64 },
            "fanout_max": fanout_max,
            "shard_map_updates": shard_map_updates,
            "shard_map_pending": shard_map_pending,
            "rebalance_started": rebalance_started,
            "rebalance_completed": rebalance_completed,
            "rebalance_timeout": rebalance_timeout,
            "cleaned_shards": cleaned_shards,
        }),
    );
    out.insert(
        "integrity".to_string(),
        json!({ "corrupt_records_skipped": corrupt_records_skipped }),
    );
    out.insert(
        "storage".to_string(),
        json!({
            "databases": database_rows.len(),
            "disk_bytes": disk_bytes,
            "disk_mb": disk_bytes as f64 / (1024.0 * 1024.0),
            "doc_count": doc_count,
            "doc_del_count": doc_del_count,
            "failed_databases": failed_databases,
        }),
    );
    out.insert("databases".to_string(), json!(database_rows));
    out.insert(
        "replication".to_string(),
        json!({
            "running": replication_running,
            "paused": replication_paused,
            "runs": replication_runs,
            "docs": replication_docs,
            "error": replication_error,
            "entries": replication_entries.into_values().collect::<Vec<_>>(),
        }),
    );
    out.insert(
        "health".to_string(),
        json!({
            "status": health_status_name(worst_health),
            "ok": worst_health == 0,
            "nodes": health_nodes,
            "failing": health_failing,
            "generated_at": now_ms(),
        }),
    );
    out
}

/// Local or cluster-wide metrics. `?cluster=true` fans out to every configured
/// peer and merges counters, latency buckets and health.
pub async fn metrics_status(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
    }
    let q = query(&uri);
    let local = local_metrics(&st).await;
    let cluster_requested = flag(&q, "cluster")
        && st.ctx.cluster.is_clustered()
        && !st.ctx.cluster.is_internal(&headers);
    if !cluster_requested {
        let mut value = local.clone();
        value["scope"] = json!("local");
        value["nodes"] = json!([{
            "index": local["node"]["index"],
            "url": local["node"]["url"],
            "reachable": true,
            "latency_ms": 0,
            "health": local["health"],
            "process": local["process"],
            "http": {"total": local["http"]["total"], "errors": local["http"]["errors"]},
            "storage": local["storage"],
        }]);
        return super::json(StatusCode::OK, value);
    }

    let peers: Vec<(usize, String)> = st
        .ctx
        .cluster
        .nodes
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != st.ctx.cluster.me)
        .map(|(index, url)| (index, url.clone()))
        .collect();
    let responses = futures::future::join_all(peers.into_iter().map(|(index, url)| {
        let started = std::time::Instant::now();
        let request = internal_request(
            &st.ctx,
            reqwest::Method::GET,
            &url,
            &["_node", "_local", "_metrics"],
            Some(&headers),
        )
        .query(&[("local", "1")]);
        async move {
            let result = match request.send().await {
                Ok(response) if response.status().is_success() => {
                    match response.json::<Value>().await {
                        Ok(value) => Ok(value),
                        Err(error) => Err(format!("invalid metrics JSON: {error}")),
                    }
                }
                Ok(response) => Err(format!("HTTP {}", response.status())),
                Err(error) => Err(error.to_string()),
            };
            (index, url, started.elapsed().as_millis() as u64, result)
        }
    }))
    .await;

    let mut snapshots = vec![local.clone()];
    let mut nodes = vec![json!({
        "index": local["node"]["index"],
        "url": local["node"]["url"],
        "reachable": true,
        "latency_ms": 0,
        "health": local["health"],
        "process": local["process"],
        "http": {"total": local["http"]["total"], "errors": local["http"]["errors"]},
        "storage": local["storage"],
    })];
    let mut reachable = 1usize;
    for (index, url, latency_ms, result) in responses {
        match result {
            Ok(value) => {
                reachable += 1;
                nodes.push(json!({
                    "index": index,
                    "url": url,
                    "reachable": true,
                    "latency_ms": latency_ms,
                    "health": value["health"],
                    "process": value["process"],
                    "http": {"total": value["http"]["total"], "errors": value["http"]["errors"]},
                    "storage": value["storage"],
                }));
                snapshots.push(value);
            }
            Err(error) => nodes.push(json!({
                "index": index,
                "url": url,
                "reachable": false,
                "latency_ms": latency_ms,
                "error": error,
            })),
        }
    }

    let mut value = Value::Object(aggregate_metrics(&snapshots));
    value["scope"] = json!("cluster");
    value["node"] = local["node"].clone();
    value["generated_at"] = json!(now_ms());
    // The Process card is explicitly local; expose it at the top level so the
    // cluster response has the same shape as a local response.
    value["process"] = local["process"].clone();
    value["local"] = local;
    let node_count = nodes.len();
    value["nodes"] = json!(nodes);
    value["summary"] = json!({"nodes": node_count, "reachable": reachable});

    // A peer that cannot answer the metrics fan-out is a cluster health
    // problem even though it contributes no snapshot to merge.
    let mut worst = health_rank(value["health"]["status"].as_str().unwrap_or("ok"));
    if reachable < node_count {
        worst = worst.max(1);
        let mut failing =
            crate::json::take_array(&mut value["health"], "failing").unwrap_or_default();
        for node in &nodes {
            if node["reachable"] == false {
                failing.push(json!({
                    "index": node["index"],
                    "url": node["url"],
                    "status": "degraded",
                    "checks": [{
                        "name": "reachability",
                        "status": "degraded",
                        "detail": node["error"],
                    }],
                }));
            }
        }
        value["health"]["failing"] = json!(failing);
    }
    value["health"]["status"] = json!(health_status_name(worst));
    value["health"]["ok"] = json!(worst == 0);
    super::json(StatusCode::OK, value)
}

pub async fn health_status(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let principal = st.principal(&headers);
    if !principal.admin {
        return access_denied(&principal);
    }
    let local = local_health(&st, true).await;
    let q = query(&uri);
    let cluster_requested = flag(&q, "cluster")
        && st.ctx.cluster.is_clustered()
        && !st.ctx.cluster.is_internal(&headers);
    if !cluster_requested {
        return super::json(StatusCode::OK, local);
    }
    let peers: Vec<(usize, String)> = st
        .ctx
        .cluster
        .nodes
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != st.ctx.cluster.me)
        .map(|(index, url)| (index, url.clone()))
        .collect();
    let responses = futures::future::join_all(peers.into_iter().map(|(index, url)| {
        let request = internal_request(
            &st.ctx,
            reqwest::Method::GET,
            &url,
            &["_node", "_local", "_health"],
            Some(&headers),
        )
        .query(&[("local", "1")]);
        async move {
            let result = match request.send().await {
                Ok(response) if response.status().is_success() => response
                    .json::<Value>()
                    .await
                    .map_err(|error| error.to_string()),
                Ok(response) => Err(format!("HTTP {}", response.status())),
                Err(error) => Err(error.to_string()),
            };
            (index, url, result)
        }
    }))
    .await;
    let mut worst = health_rank(local["status"].as_str().unwrap_or("ok"));
    let mut nodes = vec![json!({
        "index": st.ctx.cluster.me,
        "url": st.config.advertise,
        "reachable": true,
        "status": local["status"],
        "checks": local["checks"],
    })];
    for (index, url, result) in responses {
        match result {
            Ok(value) => {
                worst = worst.max(health_rank(value["status"].as_str().unwrap_or("ok")));
                nodes.push(json!({
                    "index": index,
                    "url": url,
                    "reachable": true,
                    "status": value["status"],
                    "checks": value["checks"],
                }));
            }
            Err(error) => {
                // Same severity as the metrics view: a peer that cannot answer
                // degrades the cluster, it does not take it down.
                worst = worst.max(1);
                nodes.push(json!({
                    "index": index,
                    "url": url,
                    "reachable": false,
                    "status": "degraded",
                    "error": error,
                }));
            }
        }
    }
    super::json(
        StatusCode::OK,
        json!({
            "status": health_status_name(worst),
            "ok": worst == 0,
            "scope": "cluster",
            "generated_at": now_ms(),
            "nodes": nodes,
        }),
    )
}

/// Unauthenticated liveness probe for load balancers. It reports only the
/// aggregate status and non-sensitive checks. Detailed health stays behind the
/// authenticated `/_node/_local/_health` endpoint.
pub async fn health_public(State(st): State<Arc<AppState>>) -> Response {
    let health = local_health(&st, false).await;
    let status = if health["status"] == "critical" {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    let checks: Vec<Value> = health["checks"]
        .as_array()
        .map(|checks| {
            checks
                .iter()
                .map(|check| {
                    json!({
                        "name": check["name"],
                        "status": check["status"],
                        "detail": check["detail"],
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    super::json(
        status,
        json!({
            "status": health["status"],
            "ok": health["ok"],
            "checks": checks,
            "generated_at": health["generated_at"],
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_fs_counts_unreadable_db_json() {
        let dir = std::env::temp_dir().join(format!("douchdb-fsprobe-{}", crate::util::uuid()));
        let good = dir.join("good");
        let bad = dir.join("bad");
        assert!(std::fs::create_dir_all(&good).is_ok());
        assert!(std::fs::create_dir_all(&bad).is_ok());
        assert!(std::fs::write(good.join("db.json"), b"{}").is_ok());
        assert!(std::fs::write(bad.join("db.json"), b"not json").is_ok());
        let health = probe_fs(&dir);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(health.readable && health.writable);
        assert_eq!(health.unreadable, 1);
    }
}

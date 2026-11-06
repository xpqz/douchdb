//! In-process metrics used by the health/metrics endpoints and console.
//!
//! These are deliberately independent of OpenTelemetry: they work without a
//! collector, and the OTLP path remains an optional export.

use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub(crate) const LATENCY_BOUNDS: [f64; 12] = [
    1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0,
];
const HISTORY_SECS: u64 = 120;

#[derive(Clone, Debug, Default)]
struct RouteStat {
    requests: u64,
    errors: u64,
    total_ms: f64,
    max_ms: f64,
    buckets: [u64; LATENCY_BOUNDS.len()],
}

impl RouteStat {
    fn record(&mut self, duration_ms: f64, error: bool) {
        self.requests += 1;
        self.errors += u64::from(error);
        self.total_ms += duration_ms;
        self.max_ms = self.max_ms.max(duration_ms);
        let bucket = LATENCY_BOUNDS
            .iter()
            .position(|bound| duration_ms <= *bound)
            .unwrap_or(LATENCY_BOUNDS.len() - 1);
        self.buckets[bucket] += 1;
    }

    fn percentile(&self, pct: f64) -> f64 {
        if self.requests == 0 {
            return 0.0;
        }
        let target = (self.requests as f64 * pct / 100.0).ceil() as u64;
        let mut seen = 0u64;
        for (index, count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen >= target {
                return LATENCY_BOUNDS[index];
            }
        }
        self.max_ms
    }

    fn json(&self, route: &str) -> Value {
        json!({
            "route": route,
            "requests": self.requests,
            "errors": self.errors,
            "error_rate_pct": pct(self.errors, self.requests),
            "total_ms": self.total_ms,
            "avg_ms": if self.requests == 0 { 0.0 } else { self.total_ms / self.requests as f64 },
            "p50_ms": self.percentile(50.0),
            "p95_ms": self.percentile(95.0),
            "p99_ms": self.percentile(99.0),
            "max_ms": self.max_ms,
            // Raw buckets let a cluster coordinator merge percentiles exactly.
            "latency_buckets": self.buckets.to_vec(),
        })
    }
}

#[derive(Clone, Debug, Default)]
struct SecondBucket {
    second: u64,
    requests: u64,
    errors: u64,
    total_ms: f64,
}

#[derive(Clone, Debug, Default)]
struct EngineStat {
    queries: u64,
    errors: u64,
    total_ms: f64,
    max_ms: f64,
}

impl EngineStat {
    fn record(&mut self, duration_ms: f64, success: bool) {
        self.queries += 1;
        self.errors += u64::from(!success);
        self.total_ms += duration_ms;
        self.max_ms = self.max_ms.max(duration_ms);
    }

    fn json(&self) -> Value {
        json!({
            "queries": self.queries,
            "errors": self.errors,
            "error_rate_pct": pct(self.errors, self.queries),
            "total_ms": self.total_ms,
            "avg_ms": if self.queries == 0 { 0.0 } else { self.total_ms / self.queries as f64 },
            "max_ms": self.max_ms,
        })
    }
}

#[derive(Clone, Debug, Default)]
struct FabricStat {
    local_reads: u64,
    remote_reads: u64,
    quorum_failures: u64,
    degraded_reads: u64,
    fanout_count: u64,
    fanout_sum: u64,
    fanout_max: u64,
    shard_map_updates: u64,
    shard_map_pending: u64,
    rebalance_started: u64,
    rebalance_completed: u64,
    rebalance_timeout: u64,
    cleaned_shards: u64,
}

#[derive(Default)]
struct State {
    requests: u64,
    in_flight: u64,
    errors: u64,
    total_ms: f64,
    max_ms: f64,
    buckets: [u64; LATENCY_BOUNDS.len()],
    status_codes: BTreeMap<u16, u64>,
    methods: BTreeMap<String, u64>,
    routes: BTreeMap<String, RouteStat>,
    history: VecDeque<SecondBucket>,
    fabric: FabricStat,
    views: EngineStat,
    search: EngineStat,
    corrupt_records_skipped: u64,
}

struct Registry {
    started: Instant,
    started_unix: u64,
    state: Mutex<State>,
}

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(|| Registry {
        started: Instant::now(),
        started_unix: now_secs(),
        state: Mutex::new(State::default()),
    })
}

fn state() -> std::sync::MutexGuard<'static, State> {
    registry()
        .state
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn pct(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        (part as f64 / total as f64) * 100.0
    }
}

fn status_class(status: u16) -> u16 {
    status / 100
}

fn bucket_of(duration_ms: f64) -> usize {
    LATENCY_BOUNDS
        .iter()
        .position(|bound| duration_ms <= *bound)
        .unwrap_or(LATENCY_BOUNDS.len() - 1)
}

/// Percentile from cumulative buckets. Returns the bucket's upper bound, so a
/// p95 of 12 ms is reported as 25 ms; fine for dashboards and consistent across
/// a merged cluster.
fn percentile(buckets: &[u64], requests: u64, pct: f64, max_ms: f64) -> f64 {
    if requests == 0 {
        return 0.0;
    }
    let target = (requests as f64 * pct / 100.0).ceil() as u64;
    let mut seen = 0u64;
    for (index, count) in buckets.iter().enumerate() {
        seen += count;
        if seen >= target {
            return LATENCY_BOUNDS.get(index).copied().unwrap_or(max_ms);
        }
    }
    max_ms
}

/// Record the beginning of an HTTP request so `in_flight` is live.
pub fn request_started() {
    state().in_flight += 1;
}

/// Record a completed HTTP request. `path` is only used for a low-cardinality
/// route label, never as an unbounded label value.
pub fn record_http(method: &str, status: u16, duration_ms: f64, path: &str) {
    let route = route_label(path);
    let error = status >= 400;
    let mut state = state();
    state.in_flight = state.in_flight.saturating_sub(1);
    state.requests += 1;
    state.errors += u64::from(error);
    state.total_ms += duration_ms;
    state.max_ms = state.max_ms.max(duration_ms);
    state.buckets[bucket_of(duration_ms)] += 1;
    *state.status_codes.entry(status).or_default() += 1;
    *state.methods.entry(method.to_string()).or_default() += 1;
    state
        .routes
        .entry(route)
        .or_default()
        .record(duration_ms, error);

    let now = now_secs();
    if state.history.back().map(|bucket| bucket.second) != Some(now) {
        state.history.push_back(SecondBucket {
            second: now,
            ..Default::default()
        });
    }
    if let Some(bucket) = state.history.back_mut() {
        bucket.requests += 1;
        bucket.errors += u64::from(error);
        bucket.total_ms += duration_ms;
    }
    while state
        .history
        .front()
        .map(|bucket| now.saturating_sub(bucket.second) > HISTORY_SECS)
        .unwrap_or(false)
    {
        state.history.pop_front();
    }
}

pub fn record_engine(engine: &str, duration_ms: f64, success: bool) {
    let mut state = state();
    match engine {
        "views" => state.views.record(duration_ms, success),
        "search" => state.search.record(duration_ms, success),
        _ => {}
    }
}

pub fn record_fabric(kind: &str, value: u64) {
    let mut state = state();
    let fabric = &mut state.fabric;
    match kind {
        "local_read" => fabric.local_reads += value,
        "remote_read" => fabric.remote_reads += value,
        "quorum_failure" => fabric.quorum_failures += value,
        "degraded_read" => fabric.degraded_reads += value,
        "fanout" => {
            fabric.fanout_count += 1;
            fabric.fanout_sum += value;
            fabric.fanout_max = fabric.fanout_max.max(value);
        }
        "shard_map_update" => fabric.shard_map_updates += value,
        "shard_map_pending" => fabric.shard_map_pending += value,
        "rebalance_started" => fabric.rebalance_started += value,
        "rebalance_completed" => fabric.rebalance_completed += value,
        "rebalance_timeout" => fabric.rebalance_timeout += value,
        "cleaned_shards" => fabric.cleaned_shards += value,
        _ => {}
    }
}

/// Count one corrupt stored record that a listing or scan skipped.
pub fn record_corrupt_record_skipped() {
    state().corrupt_records_skipped += 1;
}

/// Corrupt stored records skipped so far by listings and scans.
pub fn corrupt_records_skipped() -> u64 {
    state().corrupt_records_skipped
}

fn route_label(path: &str) -> String {
    let segments: Vec<&str> = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    if segments.is_empty() {
        return "/".to_string();
    }
    if segments[0] == "_utils" {
        return "/_utils/*".to_string();
    }
    if segments[0] == "_node" {
        return match segments.get(2).copied().unwrap_or("") {
            "" => "/_node".to_string(),
            endpoint => format!("/_node/*/{endpoint}"),
        };
    }
    if segments[0].starts_with('_') {
        return format!("/{}", segments[0]);
    }
    if segments.len() == 1 {
        return "/{db}".to_string();
    }
    let db = segments[0];
    let endpoint = segments[1];
    if endpoint == "_design" {
        return match segments.get(2..).unwrap_or_default() {
            [_, "_view", _] => "/{db}/_design/*/_view/*".to_string(),
            [_, "_search", _] | [_, "_search_info", _] => "/{db}/_design/*/_search/*".to_string(),
            _ => "/{db}/_design/*".to_string(),
        };
    }
    if endpoint == "_partition" {
        return "/{db}/_partition/*".to_string();
    }
    if endpoint == "_local" {
        return "/{db}/_local/*".to_string();
    }
    if endpoint.starts_with('_') {
        return format!("/{{db}}/{endpoint}");
    }
    let _ = db;
    "/{db}/doc".to_string()
}

fn process_json() -> Value {
    #[cfg(target_os = "linux")]
    {
        let mut rss = 0u64;
        let mut threads = 0u64;
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if let Some(value) = line.strip_prefix("VmRSS:") {
                    rss = value
                        .split_whitespace()
                        .next()
                        .and_then(|value| value.parse::<u64>().ok())
                        .unwrap_or(0)
                        * 1024;
                }
                if let Some(value) = line.strip_prefix("Threads:") {
                    threads = value.trim().parse().unwrap_or(0);
                }
            }
        }
        json!({
            "rss_bytes": rss,
            "rss_mb": rss as f64 / (1024.0 * 1024.0),
            "threads": threads,
            "pid": std::process::id(),
            "uptime_secs": uptime_secs(),
            "started_unix": registry().started_unix,
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Do not present an unsupported measurement as a valid zero.
        json!({
            "rss_bytes": Value::Null,
            "rss_mb": Value::Null,
            "threads": Value::Null,
            "pid": std::process::id(),
            "uptime_secs": uptime_secs(),
            "started_unix": registry().started_unix,
        })
    }
}

fn history_json(state: &State) -> Vec<Value> {
    let now = now_secs();
    let first = now.saturating_sub(59);
    let mut by_second: BTreeMap<u64, (u64, u64, f64)> = BTreeMap::new();
    for bucket in &state.history {
        if bucket.second >= first {
            by_second.insert(
                bucket.second,
                (bucket.requests, bucket.errors, bucket.total_ms),
            );
        }
    }
    (first..=now)
        .map(|second| {
            let (requests, errors, total_ms) = by_second.get(&second).copied().unwrap_or_default();
            json!({
                "t": second,
                "requests": requests,
                "errors": errors,
                "avg_ms": if requests == 0 { 0.0 } else { total_ms / requests as f64 },
            })
        })
        .collect()
}

/// Snapshot of process-local metrics. Storage, health and replication are
/// attached by the HTTP handler because they require database access.
pub fn snapshot() -> Value {
    let state = state();
    let status_classes: Map<String, Value> = state
        .status_codes
        .iter()
        .fold(
            BTreeMap::<u16, u64>::new(),
            |mut classes, (status, count)| {
                *classes.entry(status_class(*status)).or_default() += count;
                classes
            },
        )
        .into_iter()
        .map(|(class, count)| (format!("{class}xx"), json!(count)))
        .collect();
    let status_codes: Map<String, Value> = state
        .status_codes
        .iter()
        .map(|(status, count)| (status.to_string(), json!(count)))
        .collect();
    let methods: Map<String, Value> = state
        .methods
        .iter()
        .map(|(method, count)| (method.clone(), json!(count)))
        .collect();
    let mut routes: Vec<Value> = state
        .routes
        .iter()
        .map(|(route, stat)| stat.json(route))
        .collect();
    routes.sort_by(|left, right| right["requests"].as_u64().cmp(&left["requests"].as_u64()));
    let max_ms = state.max_ms;
    let avg_ms = if state.requests == 0 {
        0.0
    } else {
        state.total_ms / state.requests as f64
    };
    let p50 = percentile(&state.buckets, state.requests, 50.0, max_ms);
    let p95 = percentile(&state.buckets, state.requests, 95.0, max_ms);
    let p99 = percentile(&state.buckets, state.requests, 99.0, max_ms);
    let fanout_avg = if state.fabric.fanout_count == 0 {
        0.0
    } else {
        state.fabric.fanout_sum as f64 / state.fabric.fanout_count as f64
    };
    json!({
        "process": process_json(),
        "http": {
            "total": state.requests,
            "in_flight": state.in_flight,
            "errors": state.errors,
            "error_rate_pct": pct(state.errors, state.requests),
            "status_classes": status_classes,
            "status_codes": status_codes,
            "methods": methods,
            "latency_ms": {
                "avg": avg_ms,
                "p50": p50,
                "p95": p95,
                "p99": p99,
                "max": max_ms,
            },
            "latency_buckets": {
                "bounds": LATENCY_BOUNDS,
                "counts": state.buckets.to_vec(),
            },
            "routes": routes,
            "per_second": history_json(&state),
        },
        "views": state.views.json(),
        "search": state.search.json(),
        "integrity": {
            "corrupt_records_skipped": state.corrupt_records_skipped,
        },
        "fabric": {
            "local_reads": state.fabric.local_reads,
            "remote_reads": state.fabric.remote_reads,
            "quorum_failures": state.fabric.quorum_failures,
            "degraded_reads": state.fabric.degraded_reads,
            "fanout_count": state.fabric.fanout_count,
            "fanout_sum": state.fabric.fanout_sum,
            "fanout_avg": fanout_avg,
            "fanout_max": state.fabric.fanout_max,
            "shard_map_updates": state.fabric.shard_map_updates,
            "shard_map_pending": state.fabric.shard_map_pending,
            "rebalance_started": state.fabric.rebalance_started,
            "rebalance_completed": state.fabric.rebalance_completed,
            "rebalance_timeout": state.fabric.rebalance_timeout,
            "cleaned_shards": state.fabric.cleaned_shards,
        },
    })
}

pub fn uptime_secs() -> u64 {
    registry().started.elapsed().as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels() -> (Vec<String>, Vec<String>) {
        let snap = snapshot();
        let routes = snap["http"]["routes"]
            .as_array()
            .map(|routes| {
                routes
                    .iter()
                    .filter_map(|route| route["route"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let methods = snap["http"]["methods"]
            .as_object()
            .map(|methods| methods.keys().cloned().collect())
            .unwrap_or_default();
        (routes, methods)
    }

    #[test]
    fn user_supplied_paths_and_methods_never_become_labels() {
        record_http("BREW-7f3a9c", 404, 1.0, "/_zz9-unique-top");
        record_http("GET", 404, 1.0, "/_node/_local/_zz9-unique-node");
        record_http("GET", 404, 1.0, "/somedb/_zz9-unique-endpoint");
        let (routes, methods) = labels();
        assert!(
            routes.iter().all(|route| !route.contains("zz9")),
            "{routes:?}"
        );
        assert!(
            methods.iter().all(|method| !method.contains("BREW")),
            "{methods:?}"
        );
        assert!(routes.iter().any(|route| route == "other"), "{routes:?}");
        assert!(methods.iter().any(|method| method == "other"), "{methods:?}");
    }
}

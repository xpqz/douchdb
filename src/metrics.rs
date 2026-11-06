//! In-process metrics used by the health/metrics endpoints and console.
//!
//! These are deliberately independent of OpenTelemetry: they work without a
//! collector, and the OTLP path remains an optional export.

use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub(crate) const LATENCY_BOUNDS: [f64; 12] = [
    1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0,
];
const HISTORY_SECS: u64 = 120;
/// Window over which `/_health` judges the 5xx ratio. A node that recovers
/// leaves the unhealthy state once the failures age out of this window.
pub const HEALTH_WINDOW_SECS: u64 = 60;

static IN_FLIGHT: AtomicU64 = AtomicU64::new(0);

/// Counts one in-flight HTTP request for as long as it is alive. Dropping it,
/// including when a handler is cancelled by a client disconnect, decrements.
pub struct InFlightGuard(&'static AtomicU64);

impl InFlightGuard {
    fn new(counter: &'static AtomicU64) -> InFlightGuard {
        counter.fetch_add(1, Ordering::Relaxed);
        InFlightGuard(counter)
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        // Every guard incremented the counter in `new`, so this cannot wrap.
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Per-second request and 5xx counts over a bounded recent window.
#[derive(Default)]
struct RecentWindow {
    /// `(second, requests, server_errors)`, oldest first, strictly increasing
    /// in `second`, never longer than `HEALTH_WINDOW_SECS` entries.
    buckets: VecDeque<(u64, u64, u64)>,
}

impl RecentWindow {
    fn prune(&mut self, now: u64) {
        while self
            .buckets
            .front()
            .is_some_and(|(second, _, _)| now.saturating_sub(*second) >= HEALTH_WINDOW_SECS)
        {
            self.buckets.pop_front();
        }
    }

    fn record(&mut self, now: u64, server_error: bool) {
        self.prune(now);
        if self.buckets.back().map(|bucket| bucket.0) != Some(now) {
            self.buckets.push_back((now, 0, 0));
        }
        if let Some(bucket) = self.buckets.back_mut() {
            bucket.1 += 1;
            bucket.2 += u64::from(server_error);
        }
    }

    /// `(requests, server_errors)` over the window ending at `now`.
    fn totals(&self, now: u64) -> (u64, u64) {
        self.buckets
            .iter()
            .filter(|(second, _, _)| now.saturating_sub(*second) < HEALTH_WINDOW_SECS)
            .fold((0, 0), |(requests, errors), (_, r, e)| {
                (requests + r, errors + e)
            })
    }
}

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
    errors: u64,
    total_ms: f64,
    max_ms: f64,
    buckets: [u64; LATENCY_BOUNDS.len()],
    status_codes: BTreeMap<u16, u64>,
    methods: BTreeMap<String, u64>,
    routes: BTreeMap<String, RouteStat>,
    history: VecDeque<SecondBucket>,
    recent: RecentWindow,
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

/// Record the beginning of an HTTP request so `in_flight` is live. Keep the
/// guard until the request ends; dropping it decrements.
pub fn request_started() -> InFlightGuard {
    InFlightGuard::new(&IN_FLIGHT)
}

/// Requests and 5xx responses over the last `HEALTH_WINDOW_SECS`, excluding
/// health and liveness probes so probing never feeds the ratio it reads.
pub fn recent_http() -> (u64, u64) {
    state().recent.totals(now_secs())
}

/// Record a completed HTTP request. `path` is only used for a low-cardinality
/// route label, never as an unbounded label value.
pub fn record_http(method: &str, status: u16, duration_ms: f64, path: &str) {
    let route = route_label(path);
    let error = status >= 400;
    let mut state = state();
    if !is_probe(&route) {
        state.recent.record(now_secs(), status >= 500);
    }
    state.requests += 1;
    state.errors += u64::from(error);
    state.total_ms += duration_ms;
    state.max_ms = state.max_ms.max(duration_ms);
    state.buckets[bucket_of(duration_ms)] += 1;
    *state.status_codes.entry(status).or_default() += 1;
    *state
        .methods
        .entry(method_label(method).to_string())
        .or_default() += 1;
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

const OTHER: &str = "other";

/// Methods that may appear as a label; anything else is `other`.
pub fn method_label(method: &str) -> &'static str {
    match method {
        "GET" => "GET",
        "HEAD" => "HEAD",
        "POST" => "POST",
        "PUT" => "PUT",
        "DELETE" => "DELETE",
        "COPY" => "COPY",
        "OPTIONS" => "OPTIONS",
        "PATCH" => "PATCH",
        _ => OTHER,
    }
}

/// Routes that load balancers and peers poll. They say nothing about how
/// clients are being served, so health excludes them from its error ratio.
fn is_probe(route: &str) -> bool {
    matches!(route, "/_up" | "/_health" | "/_node/*/_health")
}

/// Low-cardinality route template. Every label comes from a fixed set; a path
/// segment chosen by the client is never copied into it.
fn route_label(path: &str) -> String {
    const TOP: [&str; 8] = [
        "_up",
        "_health",
        "_all_dbs",
        "_db_updates",
        "_dbs_info",
        "_uuids",
        "_membership",
        "_session",
    ];
    const NODE: [&str; 5] = ["_dbs", "_cluster", "_metrics", "_health", "_replication"];
    const DB: [&str; 15] = [
        "_all_docs",
        "_design_docs",
        "_local_docs",
        "_bulk_docs",
        "_bulk_get",
        "_changes",
        "_compact",
        "_ensure_full_commit",
        "_index_info",
        "_purge",
        "_revs_diff",
        "_revs_limit",
        "_search_cleanup",
        "_security",
        "_view_cleanup",
    ];
    let segments: Vec<&str> = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    let Some(first) = segments.first().copied() else {
        return "/".to_string();
    };
    if first == "favicon.ico" {
        return "/favicon.ico".to_string();
    }
    if first == "_utils" {
        return "/_utils/*".to_string();
    }
    if first == "_node" {
        return match segments.get(2).copied() {
            None => "/_node".to_string(),
            Some(endpoint) if NODE.contains(&endpoint) => format!("/_node/*/{endpoint}"),
            Some(_) => OTHER.to_string(),
        };
    }
    if first.starts_with('_') {
        return if TOP.contains(&first) {
            format!("/{first}")
        } else {
            OTHER.to_string()
        };
    }
    let Some(endpoint) = segments.get(1).copied() else {
        return "/{db}".to_string();
    };
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
        return if DB.contains(&endpoint) {
            format!("/{{db}}/{endpoint}")
        } else {
            OTHER.to_string()
        };
    }
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
            "in_flight": IN_FLIGHT.load(Ordering::Relaxed),
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
        assert!(
            methods.iter().any(|method| method == "other"),
            "{methods:?}"
        );
    }

    #[test]
    fn known_routes_keep_their_templates() {
        assert_eq!(route_label("/"), "/");
        assert_eq!(route_label("/_up"), "/_up");
        assert_eq!(route_label("/db"), "/{db}");
        assert_eq!(route_label("/db/doc1"), "/{db}/doc");
        assert_eq!(route_label("/db/_changes"), "/{db}/_changes");
        assert_eq!(
            route_label("/db/_design/d/_view/v"),
            "/{db}/_design/*/_view/*"
        );
        assert_eq!(route_label("/_node/_local/_metrics"), "/_node/*/_metrics");
        assert_eq!(route_label("/_node/_local/_nope"), "other");
        assert_eq!(method_label("GET"), "GET");
        assert_eq!(method_label("get"), "other");
    }

    #[test]
    fn probes_are_recognised_by_route() {
        for path in ["/_health", "/_up", "/_node/_local/_health"] {
            assert!(is_probe(&route_label(path)), "{path}");
        }
        for path in ["/", "/db", "/_all_dbs", "/_node/_local/_metrics"] {
            assert!(!is_probe(&route_label(path)), "{path}");
        }
    }

    #[test]
    fn recent_window_forgets_old_failures() {
        let mut window = RecentWindow::default();
        for _ in 0..30 {
            window.record(1_000, true);
        }
        for _ in 0..10 {
            window.record(1_001, false);
        }
        assert_eq!(window.totals(1_001), (40, 30));
        // Just inside the window the failures still count.
        assert_eq!(window.totals(1_000 + HEALTH_WINDOW_SECS - 1), (40, 30));
        // Once the failing second ages out only the later traffic remains.
        assert_eq!(window.totals(1_000 + HEALTH_WINDOW_SECS), (10, 0));
        assert_eq!(window.totals(1_001 + HEALTH_WINDOW_SECS), (0, 0));
    }

    #[test]
    fn recent_window_stays_bounded() {
        let mut window = RecentWindow::default();
        for second in 0..10 * HEALTH_WINDOW_SECS {
            window.record(second, false);
        }
        assert!(window.buckets.len() as u64 <= HEALTH_WINDOW_SECS);
        assert_eq!(
            window.totals(10 * HEALTH_WINDOW_SECS - 1),
            (HEALTH_WINDOW_SECS, 0)
        );
    }

    #[test]
    fn in_flight_guard_decrements_when_the_request_future_is_dropped() {
        use futures::FutureExt;
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let mut request = Box::pin(async {
            let _guard = InFlightGuard::new(&COUNTER);
            std::future::pending::<()>().await;
        });
        assert!(request.as_mut().now_or_never().is_none());
        assert_eq!(COUNTER.load(Ordering::Relaxed), 1);
        // The client went away: the handler future is dropped mid-request.
        drop(request);
        assert_eq!(COUNTER.load(Ordering::Relaxed), 0);
    }
}

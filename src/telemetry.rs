//! Optional OpenTelemetry tracing and metrics.
//!
//! With no endpoint configured this is a no-op over the normal fmt subscriber.
//! Set `--otel-endpoint` or `OTEL_EXPORTER_OTLP_ENDPOINT` to export OTLP
//! traces and metrics to a collector.

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::trace::TracerProvider;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use std::sync::OnceLock;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

pub struct Telemetry {
    tracer: SdkTracerProvider,
    meter: SdkMeterProvider,
}

pub fn init(endpoint: Option<String>, node: &str) -> anyhow::Result<Option<Telemetry>> {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_filter(tracing_subscriber::filter::LevelFilter::INFO);
    let Some(endpoint) = endpoint.filter(|endpoint| !endpoint.trim().is_empty()) else {
        tracing_subscriber::registry().with(fmt_layer).init();
        return Ok(None);
    };

    let resource = Resource::builder()
        .with_service_name("douchdb")
        .with_attribute(KeyValue::new("service.instance.id", node.to_string()))
        .build();
    let span_exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(signal_endpoint(&endpoint, "traces"))
        .build()?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();
    let tracer = tracer_provider.tracer("douchdb");

    let metric_exporter = MetricExporter::builder()
        .with_http()
        .with_endpoint(signal_endpoint(&endpoint, "metrics"))
        .build()?;
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource)
        .build();
    global::set_tracer_provider(tracer_provider.clone());
    global::set_meter_provider(meter_provider.clone());

    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let metrics_layer = tracing_opentelemetry::MetricsLayer::new(meter_provider.clone());
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(otel_layer)
        .with(metrics_layer)
        .init();

    Ok(Some(Telemetry {
        tracer: tracer_provider,
        meter: meter_provider,
    }))
}

fn signal_endpoint(base: &str, signal: &str) -> String {
    let base = base.trim_end_matches('/');
    if base.ends_with(&format!("/v1/{signal}")) {
        base.to_string()
    } else {
        format!("{base}/v1/{signal}")
    }
}

impl Telemetry {
    pub fn shutdown(&self) {
        let _ = self.tracer.shutdown();
        let _ = self.meter.shutdown();
    }
}

struct HttpMetrics {
    requests: Counter<u64>,
    duration: Histogram<f64>,
}

fn metrics() -> &'static HttpMetrics {
    static METRICS: OnceLock<HttpMetrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let meter = global::meter("douchdb");
        HttpMetrics {
            requests: meter.u64_counter("http.server.requests").build(),
            duration: meter.f64_histogram("http.server.duration.ms").build(),
        }
    })
}

pub fn record_http(method: &str, status: u16, duration_ms: f64) {
    let metrics = metrics();
    let attributes = [
        KeyValue::new("http.method", method.to_string()),
        KeyValue::new("http.status_code", status as i64),
    ];
    metrics.requests.add(1, &attributes);
    metrics.duration.record(duration_ms, &attributes);
}

struct FabricMetrics {
    local_reads: Counter<u64>,
    remote_reads: Counter<u64>,
    quorum_failures: Counter<u64>,
    degraded_reads: Counter<u64>,
    fanout: Histogram<u64>,
    shard_map_updates: Counter<u64>,
    shard_map_pending: Counter<u64>,
    rebalance_started: Counter<u64>,
    rebalance_completed: Counter<u64>,
    rebalance_timeout: Counter<u64>,
    cleaned_shards: Counter<u64>,
}

fn fabric_metrics() -> &'static FabricMetrics {
    static METRICS: OnceLock<FabricMetrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let meter = global::meter("douchdb");
        FabricMetrics {
            local_reads: meter.u64_counter("fabric.local_reads").build(),
            remote_reads: meter.u64_counter("fabric.remote_reads").build(),
            quorum_failures: meter.u64_counter("fabric.quorum_failures").build(),
            degraded_reads: meter.u64_counter("fabric.degraded_reads").build(),
            fanout: meter.u64_histogram("fabric.fanout").build(),
            shard_map_updates: meter.u64_counter("shard_map.updates").build(),
            shard_map_pending: meter.u64_counter("shard_map.pending").build(),
            rebalance_started: meter.u64_counter("rebalance.started").build(),
            rebalance_completed: meter.u64_counter("rebalance.completed").build(),
            rebalance_timeout: meter.u64_counter("rebalance.timeout").build(),
            cleaned_shards: meter.u64_counter("rebalance.cleaned_shards").build(),
        }
    })
}

pub fn record_local_read() {
    fabric_metrics().local_reads.add(1, &[]);
}

pub fn record_remote_read() {
    fabric_metrics().remote_reads.add(1, &[]);
}

pub fn record_quorum_failure() {
    fabric_metrics().quorum_failures.add(1, &[]);
}

pub fn record_degraded_read() {
    fabric_metrics().degraded_reads.add(1, &[]);
}

pub fn record_fanout(nodes: u64) {
    fabric_metrics().fanout.record(nodes, &[]);
}

pub fn record_shard_map_update(pending: bool) {
    fabric_metrics().shard_map_updates.add(1, &[]);
    if pending {
        fabric_metrics().shard_map_pending.add(1, &[]);
    }
}

pub fn record_rebalance_started() {
    fabric_metrics().rebalance_started.add(1, &[]);
}

pub fn record_rebalance_completed(cleaned: u64) {
    fabric_metrics().rebalance_completed.add(1, &[]);
    fabric_metrics().cleaned_shards.add(cleaned, &[]);
}

pub fn record_rebalance_timeout() {
    fabric_metrics().rebalance_timeout.add(1, &[]);
}

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

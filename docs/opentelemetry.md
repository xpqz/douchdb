# OpenTelemetry

douchdb can export OTLP/HTTP traces and metrics. Export is optional and off by
default, so a single binary has no collector dependency.

## Configuration

```sh
./target/release/douchdb --otel-endpoint http://127.0.0.1:4318
# or
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 ./target/release/douchdb
```

The endpoint is the collector **base** URL. douchdb appends
`/v1/traces` and `/v1/metrics` (standard OTLP/HTTP paths). `--otel-endpoint`
overrides the environment variable.

Only OTLP/HTTP with protobuf is configured; use a collector that exposes the
OTLP HTTP receiver, not a gRPC-only listener.

## Signals

### Traces

Every HTTP request creates a span named `http.request` with:

- `http.method`
- `http.url` (path and query; no request body)
- resource `service.name=douchdb`
- resource `service.instance.id=<--me>` (useful when several nodes export)

The normal `tracing` log line for the request remains available on stderr.

### Metrics

Two metrics are recorded for every HTTP response:

| Metric | Type | Attributes |
|---|---|---|
| `http.server.requests` | counter | `http.method`, `http.status_code` |
| `http.server.duration.ms` | histogram | `http.method`, `http.status_code` |

They describe HTTP only. Database, view, replication and shard-level metrics are
not exported yet.

## Verify locally without a collector

`scripts/otel_capture.py` is a tiny OTLP/HTTP capture endpoint. It does not
decode protobuf; it writes each payload so you can inspect it with `strings` or
decode it with your own tooling.

```sh
cargo build --release
python3 scripts/otel_capture.py --port 4318 --out target/otel-capture &
./target/release/douchdb --otel-endpoint http://127.0.0.1:4318 &

curl -X PUT localhost:5984/demo
curl -X PUT localhost:5984/demo/doc -H 'Content-Type: application/json' -d '{"hello":"otel"}'
curl localhost:5984/demo/doc
curl localhost:5984/demo/_all_docs

# Stop douchdb with Ctrl-C (or SIGINT) so the batch/periodic exporters flush.
kill -INT %2
```

Expected artifacts:

```text
target/otel-capture/<timestamp>-v1-traces.bin
target/otel-capture/<timestamp>-v1-metrics.bin
```

The trace payload contains `http.request`, `http.method` and the request paths.
The metrics payload contains `http.server.requests` and
`http.server.duration.ms`. A captured run from this repository showed:

```text
$ strings target/otel-capture/*v1-traces.bin | grep http
http.request
http.method
http.url
/demo
/demo/doc
/demo/_all_docs

$ strings target/otel-capture/*v1-metrics.bin | grep http.server
http.server.requests
http.server.duration.ms
```

## Real collector

Minimal collector config:

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [debug]
    metrics:
      receivers: [otlp]
      exporters: [debug]
```

```sh
docker run --rm -p 4318:4318 \
  -v "$PWD/otel-collector.yaml:/etc/otelcol-contrib/config.yaml" \
  otel/opentelemetry-collector-contrib:latest \
  --config=/etc/otelcol-contrib/config.yaml

./target/release/douchdb --otel-endpoint http://127.0.0.1:4318
```

The collector's debug exporter should print `http.request` spans and the two
HTTP metrics. Replace `debug` with your normal OTLP backend exporter.

## Cluster and operations

- Each node exports independently with `service.instance.id` set to its
  `--me` index. Aggregate traces/metrics in the collector/backend.
- Exports are batched; metrics are periodic. On SIGINT the providers are shut
  down and flushed. A SIGKILL does not flush.
- Spans contain request paths and query strings. Document IDs and view keys can
  appear there. Request bodies and credentials do not.
- Add authentication/TLS at the collector or network layer; douchdb's OTLP
  exporter itself is plain HTTP.

## Limitations

- HTTP server spans and metrics only; no database/view/replication/spans yet.
- OTLP/HTTP only; no OTLP/gRPC exporter.
- No per-request sampling configuration; the SDK default sampler is used.
- No `Last-Event-ID`-style replay for telemetry; it is observability, not
  durable auditing.

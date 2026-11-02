# Observability

douchdb collects health, performance and operational metrics in-process, with
no collector required. OpenTelemetry export is an optional additional path.

## Local metrics and health

Everything below is available on every node:

| Endpoint | Auth | Purpose |
|---|---|---|
| `GET /_health` | none | Liveness/readiness probe with overall status. |
| `GET /_node/_local/_health` | yes | Detailed checks for this node. |
| `GET /_node/_local/_health?cluster=true` | yes | Worst status across configured peers. |
| `GET /_node/_local/_metrics` | yes | Live and cumulative process-local metrics. |
| `GET /_node/_local/_metrics?cluster=true` | yes | Same shape, merged across configured peers. |

`/_health` returns `200` unless a check is `critical`, then `503`. It exposes
only status, check names and short details. The authenticated endpoint adds
data-root paths and the full check detail.

A health response looks like:

```json
{
  "status": "ok",
  "ok": true,
  "checks": [
    {"name": "process", "status": "ok", "detail": "pid 1234 · uptime 90s · 17 threads"},
    {"name": "storage", "status": "ok", "detail": "/var/lib/douchdb · 4 databases · readable=true writable=true"},
    {"name": "placement", "status": "ok", "detail": "0 database(s) with an unacknowledged shard map"},
    {"name": "databases", "status": "ok", "detail": "4 databases · 0 unreadable"},
    {"name": "cluster", "status": "ok", "detail": "3/3 placement nodes reachable"},
    {"name": "replication", "status": "ok", "detail": "12 runs · 340 docs"},
    {"name": "http", "status": "ok", "detail": "0.1% of 1200 requests failed with 5xx"}
  ]
}
```

Checks are graded `ok`, `degraded` (warning) or `critical`. The overall status is
the worst check. Power-loss durability is explicitly not claimed:
`rusty-leveldb` does not fsync, so health checks confirm process-crash safety and
writable storage, not fsync semantics.

### Metrics collected

The local registry records:

- **HTTP**: total requests, in-flight requests, errors, 5xx-based health error rate, status
  classes and exact codes, methods, average/p50/p95/p99/max latency, a
  12-bucket latency histogram, and a 120-second per-second rate/error/latency
  history for live sparklines.
- **Routes**: the same counters per low-cardinality route template, such as
  `/{db}/doc`, `/{db}/_all_docs`, `/{db}/_changes`,
  `/{db}/_design/*/_view/*`, `/{db}/_design/*/_search/*` and
  `/_node/*/_metrics`. Document ids, view names and partition names never become
  label values.
- **Views and search**: client-facing query count, errors, total/average/max
  latency. Internal shard requests are not counted again, so a cluster merge
  reflects one measurement per user query.
- **Fabric**: local and remote reads, quorum failures, degraded reads, fan-out
  count/average/max, shard-map updates and pending maps, rebalance
  started/completed/timed out, and cleaned shards.
- **Storage and databases**: data-root disk bytes, per-database disk bytes,
  live/deleted document counts, update sequence, shard map version, pending map
  flag, and per-shard owner/replicas/local flag/document counts.
- **Process**: RSS, thread count, PID, start time and uptime.
- **Replication and discovery**: the same snapshots served by
  `/_node/_local/_replication` and the `cluster` endpoint.

`in_flight` includes the metrics request itself, so it is at least `1` while a
scrape is being served. Counters reset on process restart.

### Cluster aggregation

`?cluster=true` fans out to every configured peer using the internal cluster
credentials and the caller's forwarded credentials. It returns:

```json
{
  "scope": "cluster",
  "summary": {"nodes": 3, "reachable": 3},
  "http": {"total": 1234, "latency_ms": {"p95": 12.5}},
  "storage": {"databases": 4, "disk_bytes": 123456},
  "databases": [{"name": "demo", "doc_count": 100, "nodes_reporting": 3}],
  "health": {"status": "ok", "nodes": [], "failing": []},
  "nodes": [
    {"index": 0, "url": "http://node0:5984", "reachable": true,
     "health": {"status": "ok"}, "process": {"rss_mb": 42.0}}
  ],
  "local": {"scope": null, "http": {}}
}
```

Counters and latency buckets are summed and percentiles are recomputed after the
merge. Per-database document counts are the sum of per-shard maxima: replicas
hold the same shard, so summing node totals would double count and taking the
largest node total would miss shards owned elsewhere. Disk usage is summed
because it is real storage on each node. `local` is the coordinator's own
snapshot, unchanged.

A peer that cannot answer the fan-out is listed with `reachable: false` and
forces cluster health to `degraded`. If every peer succeeds, the worst node
health wins.

The console page at `/_utils/#/metrics` refreshes every five seconds and shows
health, request/latency/error sparklines, route and database tables, replication
state, fabric counters and per-node health/process data.

## OpenTelemetry

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

The fabric counters and histograms (`fabric.local_reads`, `fabric.remote_reads`,
`fabric.quorum_failures`, `fabric.degraded_reads`, `fabric.fanout`,
`shard_map.updates`, `shard_map.pending`, `rebalance.*`) are also exported.
View/search latency, storage and process metrics remain local-only; read them
from `/_node/_local/_metrics`.

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

- OTLP exports HTTP spans, HTTP metrics and fabric counters; the richer
  view/search/storage/process data is local-only through
  `/_node/_local/_metrics`.
- OTLP/HTTP only; no OTLP/gRPC exporter.
- No per-request sampling configuration; the SDK default sampler is used.
- No `Last-Event-ID`-style replay for telemetry; it is observability, not
  durable auditing.
- In-process counters reset on restart and are not persisted.
- Power-loss durability is not claimed; `rusty-leveldb` does not fsync.

# Docker Compose demo

`docker-compose.yml` starts a three-node douchdb cluster, an nginx
load-balanced edge that also serves the embedded console, and Grafana's
OpenTelemetry LGTM stack.

```sh
docker compose up --build -d
```

| URL | What |
|---|---|
| http://localhost:8080/ | Cluster API through nginx, round-robin across all nodes |
| http://localhost:8080/_utils/ | Console (Fauxton-style) |
| http://localhost:5984/ | node0 API (direct, for debugging) |
| http://localhost:5985/ | node1 API (direct) |
| http://localhost:5986/ | node2 API (direct) |
| http://localhost:3000/ | Grafana (traces and metrics) |
| http://localhost:4318/ | OTLP/HTTP receiver |

Console login: `admin` / `secret`.

## Cluster

All three nodes use:

```text
--q 4 --n 3
--nodes http://node0:5984,http://node1:5984,http://node2:5984
--me 0|1|2
--sync-secs 3
--admin admin:secret
--cluster-secret <DOUCHDB_CLUSTER_SECRET>
--otel-endpoint http://otel-lgtm:4318
```

With `n=3` on three nodes every document lives on every node, so any
coordinator can serve point reads locally. That is the CouchDB-recommended
cluster shape for a three-node deployment; the trade-off is 3× storage and
three replica writes per document.

Data lives in the `node0-data`, `node1-data` and `node2-data` volumes. The
default cluster secret is `dev-cluster-secret-please-change`; override it for
anything beyond a local demo:

```sh
DOUCHDB_CLUSTER_SECRET='replace-with-a-long-random-secret' docker compose up --build -d
```

The admin password is set in the compose commands. Change it in
`docker-compose.yml` before exposing the stack.

## Load balancing

Every douchdb node is an equal cluster coordinator: a request can arrive at any
node and is routed to the document owner or fanned out across the replica set.
The `proxy` nginx service puts all three nodes in a `least_conn` upstream with
keep-alive and passive failure handling, so clients use `http://localhost:8080`
as a single cluster endpoint:

```text
browser/curl -> nginx :8080 -> node0 / node1 / node2 :5984
```

The direct node ports remain exposed for debugging. If a node is down, nginx
retries idempotent requests against the other upstreams; the cluster itself
also tries every replica of a document before giving up.

## Console

The console is compiled into douchdb. Opening `http://localhost:8080/_utils/`
makes the browser talk to the same nginx origin, which keeps the `AuthSession`
cookie first-party and lets the live changes feed stream without CORS. The
console's own API calls are load-balanced like any other client.

Use the **Server** field if you point the console at another origin. For this
compose stack, the default `http://localhost:8080` is correct.

## Grafana / OpenTelemetry

The `otel-lgtm` service is Grafana's all-in-one OpenTelemetry backend (pinned to
0.39.0). Every douchdb node exports OTLP/HTTP traces and metrics to it. The demo
sets `OTEL_METRIC_EXPORT_INTERVAL=5000` and `OTEL_BSP_SCHEDULE_DELAY=1000` on
the nodes so telemetry appears promptly; production defaults are slower.

- **Traces:** Grafana → Explore → Tempo → service name `douchdb`.
- **Metrics:** Grafana → Explore → Prometheus → query
  `http_server_requests_total` or `http_server_duration_ms_bucket`.
- Anonymous Grafana access is enabled with the Admin role for this local demo;
  the admin password is `admin`.

Generate traffic through the console or directly:

```sh
curl -X PUT localhost:8080/demo
curl -X PUT localhost:8080/demo/doc \
  -H 'Content-Type: application/json' -d '{"hello":"compose"}'
curl localhost:8080/demo/_all_docs
```

Each request produces an `http.request` span and the two HTTP metrics
documented in [opentelemetry.md](opentelemetry.md).

Direct checks (anonymous Grafana has the datasources provisioned):

```sh
curl -s -u admin:admin \
  'http://localhost:3000/api/datasources/proxy/uid/prometheus/api/v1/query?query=http_server_requests_total'

curl -s -u admin:admin \
  'http://localhost:3000/api/datasources/proxy/uid/tempo/api/search?tags=service.name%3Ddouchdb&limit=3'
```

The LGTM container is ephemeral in this compose file: `docker compose down`
removes it. Add a volume for Grafana/Tempo/Prometheus persistence if needed.

## Operations

```sh
docker compose ps
docker compose logs -f proxy
docker compose logs -f node0
docker compose down          # keep node data volumes
docker compose down -v       # remove cluster data too
```

The stack has no TLS and uses demo credentials; it is for local development and
testing, not an internet-facing deployment.

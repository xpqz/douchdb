# Docker Compose demo

`docker-compose.yml` starts a three-node douchdb cluster, an nginx
load-balanced edge that also serves the embedded console, and Grafana's
OpenTelemetry LGTM stack.

The stack needs four secrets before it will start; there are no defaults.
Generate a `.env` file next to `docker-compose.yml` (compose reads it
automatically, and it is git-ignored):

```sh
umask 077
cat > .env <<EOF
DOUCHDB_CLUSTER_SECRET=$(openssl rand -hex 24)
DOUCHDB_SESSION_SECRET=$(openssl rand -hex 24)
DOUCHDB_ADMIN=admin:$(openssl rand -hex 12)
GRAFANA_ADMIN_PASSWORD=$(openssl rand -hex 12)
EOF
docker compose up --build -d
```

`docker compose up` without them fails with a message naming the missing
variable. Read the generated admin password from `.env`.

Every published port is bound to `127.0.0.1`, so the stack is reachable only
from the machine running it. To expose it to a network, change the port
mappings deliberately and put TLS in front; the compose file does not provide
it.

| URL | What |
|---|---|
| http://localhost:8080/ | Cluster API through nginx, round-robin across all nodes |
| http://localhost:8080/_utils/ | Console (Fauxton-style) |
| http://localhost:5984/ | node0 API (direct, for debugging) |
| http://localhost:5985/ | node1 API (direct) |
| http://localhost:5986/ | node2 API (direct) |
| http://localhost:3000/ | Grafana (traces and metrics) |
| http://localhost:4318/ | OTLP/HTTP receiver |

Console login: the `DOUCHDB_ADMIN` user and password from `.env`. Grafana login: `admin` and `GRAFANA_ADMIN_PASSWORD`; anonymous access is disabled.

## Cluster

All three nodes use:

```text
--q 4 --n 3
--nodes http://node0:5984,http://node1:5984,http://node2:5984
--me 0|1|2
--sync-secs 3
--admin <DOUCHDB_ADMIN>
--cluster-secret <DOUCHDB_CLUSTER_SECRET>
--otel-endpoint http://otel-lgtm:4318
```

With `n=3` on three nodes every document lives on every node, so any
coordinator can serve point reads locally. That is the CouchDB-recommended
cluster shape for a three-node deployment; the trade-off is 3× storage and
three replica writes per document. Point reads default to a quorum (`r=2`);
use `?r=1` for the local-replica fast path.

Data lives in the `node0-data`, `node1-data` and `node2-data` volumes.

The secrets reach the nodes as the `DOUCHDB_CLUSTER_SECRET`,
`DOUCHDB_SESSION_SECRET` and `DOUCHDB_ADMIN` environment variables rather than
command-line flags, so they do not appear in the process list. The session
secret signs cookies and is shared by all three nodes so a login works behind
the proxy.

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

- **Dashboards:** two provisioned dashboards are created automatically in the
  **DouchDB** folder: **DouchDB Overview** (traffic, errors, latency,
  per-method/status/node rates) and **DouchDB Fabric & Rebalance** (local vs
  remote reads, quorum failures, fan-out, shard-map and rebalance events).
  Grafana opens **DouchDB Overview** as its home dashboard.
- **Traces:** Grafana → Explore → Tempo → service name `douchdb`.
- **Metrics:** Grafana → Explore → Prometheus → query
  `http_server_requests_total` or `http_server_duration_ms_bucket`.
- Anonymous Grafana access is enabled with the Admin role for this local demo;
  the admin password is `admin`.

Dashboard JSON lives in `docker/grafana/dashboards/` and is mounted read-only;
the provider is `docker/grafana/provisioning/dashboards/douchdb.yaml`. Edit the
JSON and restart the `otel-lgtm` service to update the provisioned dashboards.

Generate traffic through the console or directly:

```sh
curl -X PUT localhost:8080/demo
curl -X PUT localhost:8080/demo/doc \
  -H 'Content-Type: application/json' -d '{"hello":"compose"}'
curl localhost:8080/demo/_all_docs
```

Each request produces an `http.request` span and the two HTTP metrics
documented in [opentelemetry.md](opentelemetry.md).

The compose stack also serves the local console observability page at
`http://localhost:8080/_utils/#/metrics`. It needs no collector and shows live
sparklines, route/database/replication/fabric tables, health checks and
per-node state. The JSON is available directly (admin credentials required):

```sh
curl -u "$DOUCHDB_ADMIN" 'http://localhost:8080/_node/_local/_metrics?cluster=true'
curl -u "$DOUCHDB_ADMIN" 'http://localhost:8080/_node/_local/_health?cluster=true'
curl 'http://localhost:8080/_health'
```

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

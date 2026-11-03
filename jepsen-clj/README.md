# Real Jepsen tests for douchdb

This directory runs the actual [Jepsen](https://jepsen.io) framework (not the
small Python `jepsen-couchdb` harness in `../jepsen/`) against a disposable
douchdb cluster. It uses Jepsen's generator/interpreter, nemesis protocol and
Knossos linearizability checker.

The test is a set of independent registers. Each key is a document:

```json
{"_id": "k-0", "value": 3}
```

Each key gets one writer and one reader. Writes are conditional updates from
the revision the writer just read; reads use `?r=2`. A node can be killed and
restarted, or disconnected from the Docker network, while the workload runs.
The checker asks whether every completed read is explainable by a linearizable
register, given that failed and indeterminate operations may not have happened.

## Quick start

Only Docker is required. Java, Clojure and Leiningen are not installed on the
host; they run in the runner container.

```sh
# Build images, start three nodes, run 30s of writes/reads with node kills
./jepsen-clj/run.sh --fault kill --time-limit 30

# Network partition instead of process death
./jepsen-clj/run.sh --fault partition --time-limit 30

# No faults: useful as a smoke test
./jepsen-clj/run.sh --no-nemesis --time-limit 10
```

Useful options:

```text
-t, --time-limit SECONDS       test duration (default 30)
-c, --concurrency N            concurrent clients, even (default 8)
-i, --nemesis-interval SECONDS fault/recovery period (default 10)
-f, --fault kill|partition     fault type (default kill)
-n, --no-nemesis               run without faults
    --nodes NODES              comma-separated container names
    --db NAME                  database name (default jepsen)
    --network NAME             Docker network (default douchdb-jepsen-net)
-s, --seed SEED                random seed
```

`run.sh` builds `douchdb:jepsen` from the repository `Dockerfile`, builds the
runner image, creates `douchdb-jepsen-net`, and starts three node containers on
ports 15984-15986. Set `DOUCHDB_SKIP_BUILD=1` to reuse existing images.

CI compiles the harness in the `clojure:tools-deps-bookworm` image with
`clojure -M -e "(require 'douchdb.core)"`. It does not run a fault-injection
test, because that needs a Docker socket and several minutes.

Set `DOUCHDB_KEEP_CLUSTER=1` to leave the node containers running after the
run. Node stdout is always copied to `jepsen-clj/logs/`, and Jepsen histories
and checker results are written under `jepsen-clj/store/`.

## What runs

- **Database:** three douchdb containers, `q=4`, `n=3`, one-second
  anti-entropy, shared cluster secret.
- **Control:** Jepsen's SSH transport is configured with `:dummy? true`.
  Database setup and teardown use HTTP directly; no SSH server is needed.
- **Client:** `douchdb.client`, using `r=2` reads and `w=2` writes. A write is
  a read of the current revision followed by a conditional `PUT` with `_rev`.
  A lost revision race is a `:fail`; a `202` or transport error is `:info`.
  CAS is implemented (`If-Match` plus `_rev`) but is not in the default
  generator.
- **Workload:** `jepsen.independent/concurrent-generator`, two clients per key,
  one reserved for writes and one for reads. With concurrency 8 that is four
  independent registers active at once.
- **Faults:** a custom Docker nemesis. `kill` uses `docker kill`/`docker
  start`; `partition` uses `docker network disconnect`/`connect`. The nemesis
  starts and reconnects every node during teardown.
- **Checker:** `jepsen.checker/linearizable` with
  `knossos.model/cas-register`, wrapped by `jepsen.independent/checker`.

## Validation

On 2026-11-03 against the current tree, with concurrency 8 and a
10-second fault interval. Counts are operations in the top-level history:

| Scenario | Duration | Completed ops | Indeterminate | Failed | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| no faults | 10s | 6,065 | 0 | 0 | valid |
| kill/restart | 30s | 12,142 | 104 | 0 | valid |
| network partition | 30s | 11,336 | 18 | 0 | valid |

`Indeterminate` operations are expected while a node is down: the request may
have reached some replicas but not a write quorum, or the client may have lost
its connection. Jepsen's checker treats those as possibly having happened.

## Limits

- This is a single-host Docker setup. A "partition" is a container network
  disconnect, not an iptables partition, clock skew or disk fault.
- The custom nemesis does not use Jepsen's standard SSH/iptables nemeses. It
  mounts `/var/run/docker.sock` into the runner, which is root-equivalent on
  the host; run it only on a development machine.
- Node data volumes are removed at the start of each run. This is a test
  harness, not a deployment tool.
- Douchdb's default quorum and anti-entropy semantics still apply. The test
  uses `r=2`/`w=2` explicitly so a single failed node leaves a working
  majority.
- The checker verifies linearizability, not durability across power loss.
  rusty-leveldb does not fsync; see `../docs/storage-dependency.md`.

## Files

- `src/douchdb/core.clj` — CLI, workload, nemesis generator, test map.
- `src/douchdb/client.clj` — register client over the CouchDB HTTP API.
- `src/douchdb/db.clj` — database setup/teardown and readiness waits.
- `src/douchdb/nemesis.clj` — Docker kill/start and network partition.
- `src/douchdb/http.clj` — small clj-http wrapper.
- `Dockerfile` — Clojure + Docker CLI runner image.
- `run.sh` — build, cluster startup, Jepsen invocation and log capture.

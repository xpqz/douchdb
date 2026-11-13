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

Without faults and under kills, the checker asks whether every completed read
is explainable by a linearizable register, given that failed and indeterminate
operations may not have happened. Under network partitions it checks what
CouchDB promises instead; see [Partitions](#partitions).

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
-c, --concurrency N            concurrent clients (default 8)
-w, --workload WORKLOAD        register, contention or cas
-i, --nemesis-interval SECONDS fault/recovery period (default 10)
-f, --fault kill|partition     fault type (default kill)
-n, --no-nemesis               run without faults
    --read-quorum N            read quorum r, 1-3 (default 2)
    --write-quorum N           write quorum w, 1-3 (default 2)
    --no-final-reads           skip the final read of every key (not partition runs)
    --liveness-interval-ms MS  container/health sample interval (default 500)
    --max-indeterminate-pct N  fail above this indeterminate percentage (default 20)
    --no-liveness              disable runtime liveness sampling
    --nodes NODES              comma-separated container names
    --db NAME                  database name (default jepsen)
    --network NAME             Docker network (default douchdb-jepsen-net)
-s, --seed SEED                random seed
```

`register` uses one writer and one reader per key. `contention` uses two
writers and one reader, so writers race on the same revision; concurrency must
be a multiple of 3. `cas` uses an If-Match writer and a reader; concurrency
must be even.

Run the quorum matrix with:

```sh
./jepsen-clj/run.sh --no-nemesis --read-quorum 1 --write-quorum 1
./jepsen-clj/run.sh --no-nemesis --read-quorum 2 --write-quorum 2
./jepsen-clj/run.sh --no-nemesis --read-quorum 3 --write-quorum 3
```

A single-node profile uses the same harness with one node and one replica:

```sh
DOUCHDB_NODES=douchdb-jepsen-0 DOUCHDB_N=1 DOUCHDB_Q=1 \
  ./jepsen-clj/run.sh --no-nemesis --read-quorum 1 --write-quorum 1
```

After the timed workload, the final generator reads every key once, so a write
that was acknowledged but disappeared by the end of the test is still checked.
Pass `--no-final-reads` to omit it.

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
- **Client:** `douchdb.client`, using configurable `r`/`w` quorums (default
  `r=2`, `w=2`). A write is a read of the current revision followed by a
  conditional `PUT` with `_rev`. A lost revision race is a `:fail`; a `202` or
  transport error is `:info`. `cas` uses `If-Match` plus `_rev`.
- **Workloads:** `register` runs one reserved writer and one reader per key.
  `contention` runs two writers and one reader, exercising lost revision races.
  `cas` runs an If-Match writer and a reader.
- **Final reads:** after the time limit, one read of every generated key runs
  outside the timed section, so an acknowledged write that vanished by the end
  of the test still fails the checker.
- **Faults:** a custom Docker nemesis. `kill` uses `docker kill`/`docker
  start`; `partition` uses `docker network disconnect`/`connect`. The nemesis
  starts and reconnects every node during teardown.
- **Checker:** without faults and under kills, `jepsen.checker/linearizable`
  with a CAS-register model wrapped by `jepsen.independent/checker`. The model
  accepts the CAS workload's `:cas-write` operation as a successful write.
  Under partitions, `douchdb.convergence/checker`; see below.

## Partitions

CouchDB is not a single copy of the data while the network is split, and
douchdb mimics it. A write that reaches fewer than `w` copies is still stored
and answered with 202, a writer can read a stale revision and start a conflict
branch, and copies answer reads differently until anti-entropy catches up. A
read can therefore return an older value than an earlier read did. The
linearizability checker rejects that, correctly, so partition runs check
CouchDB's actual promises instead:

- **No invented data:** every read returns nil or a value that some write,
  invoked before the read returned and not known to have failed, tried to
  store.
- **Acknowledged writes survive:** the client records the revision of every
  201 write. After the run, that revision must be in the document's revision
  tree, as the winner, an ancestor or a conflict.
- **Convergence:** after the workload, the nemesis heals the network and a
  final `:final-state` operation per key asks every node for its own copy
  (`open_revs=all&revs=true&r=1`) until all nodes hold the same leaves. A key
  that has not converged within 120 seconds fails the run.

The final state replaces the final reads, so `--no-final-reads` has no effect
on partition runs.

## Liveness

A background monitor samples each node's Docker state and HTTP health every
500 ms. The liveness checker compares outages with the nemesis history:

- a node that stops or loses health inside an expected kill/partition window is
  allowed;
- a node that stops outside one fails the run;
- restart-count increases outside an expected window fail the run;
- indeterminate operations above `--max-indeterminate-pct` fail the run;
- throughput and the longest gap between successful operations are recorded.

Samples are written to `liveness.edn` under the run's `store` directory.

`.github/workflows/jepsen-nightly.yml` runs a 30-second no-fault smoke plus
10-minute kill and partition soaks with fixed seeds and uploads `store/` and
`logs/`.

## Validation

On 2026-11-03 against the current tree. Counts are operations in the top-level
history; the first three rows are the original register runs.

| Scenario | Workload | r/w | Duration | Completed | Indeterminate | Failed | Result |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| no faults | register | 2/2 | 10s | 6,065 | 0 | 0 | valid |
| kill/restart | register | 2/2 | 30s | 12,142 | 104 | 0 | valid |
| network partition | register | 2/2 | 30s | 11,336 | 18 | 0 | valid |
| no faults | register | 1/1 | 8s | 14,132 | 0 | 0 | valid |
| no faults | register | 3/3 | 8s | 10,576 | 0 | 0 | valid |
| no faults | contention (c=9) | 2/2 | 10s | 12,510 | 0 | 0 | valid |
| kill/restart | contention (c=9) | 2/2 | 20s | 15,726 | — | — | valid |
| no faults | cas | 2/2 | 10s | 12,960 | 0 | 0 | valid |
| single node | register | 1/1 | 8s | 9,292 | 0 | 0 | valid |

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
- `src/douchdb/convergence.clj` — final-state collection and partition checker.
- `src/douchdb/db.clj` — database setup/teardown and readiness waits.
- `src/douchdb/nemesis.clj` — Docker kill/start and network partition.
- `src/douchdb/http.clj` — small clj-http wrapper.
- `Dockerfile` — Clojure + Docker CLI runner image.
- `run.sh` — build, cluster startup, Jepsen invocation and log capture.

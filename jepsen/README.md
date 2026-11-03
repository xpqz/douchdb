# jepsen-couchdb scenarios for douchdb

This directory is a Python 3 adaptation of
[garbados/jepsen-couchdb](https://github.com/garbados/jepsen-couchdb) for
douchdb. The original is a small CouchDB consistency stress harness (not the
Clojure Jepsen framework), with workloads for random/direct/sequential writes,
updates, sleeps, quorum reads, and replication partitions. The scenarios are
reimplemented here against douchdb's HTTP API and built-in sharded cluster.

## What changed

- Python 2 → Python 3 and current `requests`.
- Five externally installed CouchDB copies and `_replicator` documents are
  replaced by douchdb's native shard/replica cluster and anti-entropy.
- `run.py` starts a disposable three-node cluster with four shards per
  database, replication factor three, and a one-second repair interval.
- Per-node checks use douchdb's trusted forwarding headers to read the local
  replica; quorum checks use `?r=`.
- `node_loss` kills a node mid-workload, continues writes through the
  survivors, restarts it, and verifies convergence.
- The original `part_network` replication-topology trick has no douchdb
  equivalent because douchdb has no per-peer replication switch. For a real
  network partition, run the nodes in Docker and disconnect containers with
  `docker network disconnect`, or apply host firewall rules; keep the test
  client outside that partition. This is not automated here.

This is a consistency/stress harness, not a linearizability checker. It will
find missing documents, divergence and non-convergence; it will not prove
strict serializability.

## Quick start

From the repository root, build the release binary, then:

```sh
python3 -m pip install -r jepsen/requirements.txt

# disposable cluster
python3 jepsen/run.py simple sequential --n 100
python3 jepsen/run.py sleep random_work --n 100
python3 jepsen/run.py quorum sequential --n 100
python3 jepsen/run.py trainwreck trainwreck --n 100
python3 jepsen/run.py node_loss random_work --n 100
```

`run.py` starts nodes on `127.0.0.1:15984-15986`, creates the `jepsen` database,
runs the scenario, deletes it, and stops the nodes. Set
`DOUCHDB_KEEP_DATA=1` to retain the data and logs under the temporary cluster
directory.

To use an existing cluster instead:

```sh
DOUCHDB_PORTS=5984,5985,5986 \
DOUCHDB_CLUSTER_SECRET=your-secret \
python3 jepsen/test.py simple sequential --n 100
```

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `DOUCHDB_BIN` | `target/release/douchdb` | binary used by `run.py` |
| `DOUCHDB_HOST` | `127.0.0.1` | node host |
| `DOUCHDB_PORTS` | `15984,15985,15986` | node ports |
| `DOUCHDB_DB` | `jepsen` | test database |
| `DOUCHDB_Q` | `4` | shards per database |
| `DOUCHDB_SYNC_SECS` | `1` | anti-entropy interval |
| `DOUCHDB_CLUSTER_SECRET` | `jepsen-cluster-secret-please-change` | internal cluster secret |
| `DOUCHDB_SETTLE_SECS` | `3` | wait before consistency checks |
| `DOUCHDB_TIMEOUT` | `30` | HTTP timeout |

## Checks

- `simple`: write, then report each node's local view immediately.
- `sleep`: write, wait `DOUCHDB_SETTLE_SECS`, then compare every local view.
- `quorum`: write, then read a document through a douchdb quorum read and
  report acknowledged documents missing from local views.
- `trainwreck`: create then update documents; report missing ids and differing
  winning revisions across nodes.
- `node_loss`: write half the workload, kill one node, write the rest, restart
  it, and compare local views after convergence.

## Notes and limits

- The local feed/query headers use `x-douchdb-secret`, so the harness must know
  the cluster secret.
- The `sleep` and `node_loss` checks rely on anti-entropy; use a short
  `DOUCHDB_SYNC_SECS` for the disposable cluster.
- Real network partition testing needs external network control as described
  above.
- The original project has no license file; this adaptation is a fresh Python 3
  implementation of the same scenarios with attribution.

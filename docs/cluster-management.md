# Cluster management and replication

douchdb exposes a small operational surface for a running cluster: a cluster
overview API, peer discovery, background replication status, and controls for
forcing or pausing replication. The embedded console at `/_utils/#/cluster`
uses the same endpoints.

The placement model is still static. This layer observes the cluster and
manages the replicator; it does not rewrite `--nodes` at runtime.

## Membership and discovery

Routing and shard placement come from the static `--nodes` / `--me` list. To
make a cluster easier to inspect on a network, a node can also probe peers and
follow their membership lists:

```sh
douchdb \
  --listen 127.0.0.1:5984 \
  --data /tmp/n1 \
  --nodes http://127.0.0.1:5984,http://127.0.0.1:5985,http://127.0.0.1:5986 \
  --me 0 \
  --sync-secs 5 \
  --advertise http://127.0.0.1:5984 \
  --seed-nodes http://127.0.0.1:5984,http://127.0.0.1:5985 \
  --cluster-secret "$SECRET"
```

- `--advertise` is the URL shown for this node in the management view. It
  defaults to the matching `--nodes` entry, or `http://{listen}`.
- `--seed-nodes` is an additional comma-separated list probed during
  discovery. It can contain peers that are not in the local `--nodes` list.
- Discovery first probes every configured node and seed, then follows the
  `all_nodes` / `cluster_nodes` lists those peers report and probes any newly
  seen node once. This is how a node can reveal peers it was not configured
  with.
- A node that is reachable but absent from the local `--nodes` list is marked
  `discovered` in the API and console. It participates in replication only if
  it is in the placement list; discovery does not add it automatically.

Membership changes are still an operator action. Restart every node with the
same `--nodes` list. The discovery view exists so drift is visible before a
restart, not to perform an online join.

## Non-client-induced replication

Every clustered node runs a background replicator. It is the same CouchDB
replication protocol clients use, but it runs on a timer without a client
request:

1. Every `--sync-secs` seconds, pull `_changes` from each peer.
2. Fetch the referenced revisions with `_bulk_get?revs=true`, preserving the
   revision history metadata.
3. Keep only documents whose shard this node replicates.
4. Write them with `_bulk_docs?new_edits=false`.
5. Store a checkpoint in a `_local` document keyed by peer and shard-map hash.

Checkpoints are advanced only after every requested revision has reached a
terminal state: applied locally, already present locally, or explicitly
reported unavailable by the peer (purged/pruned). A partial or malformed
`_bulk_get` response leaves the checkpoint behind and fails the pass, so the
next tick retries those revisions. `partial_bulk_get_does_not_advance_the_checkpoint`
covers this path.

The map-hash key means a placement change starts a fresh backfill instead of
resuming a checkpoint that filtered out newly owned documents. This path
repairs replicas after failed writes, a restart, or a period offline. It also
keeps the search and view indexes warm because their documents arrive without a
client read.

The replicator is pull-based. There is no separate push scheduler: normal
writes already fan out to replicas, and the timer repairs what they missed.

### Changes after an owner loss

When an owner is unreachable, the coordinator reassigns its shards to live
replicas. Those reassigned masks start at `since=0`: the surviving node's
checkpoint describes its own shards, not the newly borrowed ones. Preferred
masks stay disjoint, so replaying from zero fills gaps. Reassigned shard masks
are at-least-once: while an owner is down, rows already delivered before the
checkpoint may be replayed, so clients should deduplicate by revision id.

## Replication controls

Background replication can be paused per node without stopping the process:

```text
POST /_node/_local/_replication/pause
POST /_node/_local/_replication/resume
```

The pause is in memory and resets when the node restarts. It affects only the
background timer. An explicit `POST /_node/_local/_replication/run` still runs
one pass while paused, which makes it safe to stop the timer, run a controlled
sync, and resume.

Force one pass now, optionally for one database:

```text
POST /_node/_local/_replication/run
POST /_node/_local/_replication/run?db=mydb
```

The pass runs in the background and returns `202 Accepted`. Poll the status
endpoint for completion. Only one pass runs at a time; additional triggers are
ignored while one is in flight.

## API

All endpoints require admin authentication when the server has `--admin`.
Membership probes use the shared cluster secret for internal access.

### `GET /_node/_local/_cluster`

```json
{
  "local": {
    "url": "http://127.0.0.1:5984",
    "index": 0,
    "clustered": true,
    "q": 4,
    "n": 3,
    "sync_secs": 5
  },
  "membership": {
    "all_nodes": ["http://127.0.0.1:5984", "..."],
    "cluster_nodes": ["http://127.0.0.1:5984", "..."]
  },
  "discovery": {
    "running": false,
    "last_run": 1700000000000,
    "last_finished": 1700000000123,
    "error": null,
    "configured": ["http://127.0.0.1:5984", "..."],
    "seeds": ["http://127.0.0.1:5984"],
    "nodes": [
      {
        "url": "http://127.0.0.1:5984",
        "in_membership": true,
        "seed": false,
        "discovered": false,
        "reachable": true,
        "latency_ms": 1,
        "last_seen": 1700000000100,
        "membership": {"all_nodes": ["..."], "cluster_nodes": ["..."]},
        "error": null
      }
    ]
  },
  "replication": {
    "running": false,
    "paused": false,
    "last_run": 1700000000000,
    "last_finished": 1700000000200,
    "runs": 12,
    "docs": 430,
    "error": null,
    "entries": [
      {
        "db": "mydb",
        "peer": "http://127.0.0.1:5985",
        "state": "ok",
        "started": 1700000000000,
        "finished": 1700000000200,
        "docs": 4,
        "error": null,
        "checkpoint": "12-g1AAAA..."
      }
    ]
  },
  "databases": [
    {
      "name": "mydb",
      "q": 4,
      "node_count": 3,
      "version": 2,
      "shards": [
        {"shard": 0, "replicas": [0, 1, 2], "owner": 0, "local": true, "docs": 123, "deleted": 2}
      ]
    }
  ]
}
```

The cluster endpoint reports the receiving node's view. Node URLs, discovery
results and replication checkpoints are not aggregated across peers.

### `POST /_node/_local/_cluster/discover`

Starts a discovery pass and returns `202 Accepted` with the previous snapshot.
The pass probes `_up` and `_membership` on configured nodes, seeds and
membership-discovered nodes. It retries quickly during startup and then follows
`--sync-secs`.

### `GET /_node/_local/_replication`

Returns the replicator snapshot: whether a pass is running, whether the timer
is paused, total runs and documents, the last error, and one entry per
database/peer pair with its checkpoint and last result.

### `POST /_node/_local/_replication/run`

Starts one replicator pass and returns `202 Accepted`. Pass `?db=name` to
restrict the pass to one database.

### `POST /_node/_local/_replication/pause` / `resume`

Toggle the background timer. Returns the current replication snapshot.

### Existing shard controls

The cluster page also exposes the existing shard-map controls:

```text
GET  /_node/_local/_dbs
GET  /_node/_local/_dbs/{db}
GET  /_node/_local/_dbs/{db}/shards
POST /_node/_local/_dbs/{db}/rebalance
POST /_node/_local/_dbs/{db}/cleanup
```

`rebalance` computes a balanced map, broadcasts it, waits for each shard to
reach the old replica count and removes copies on nodes that no longer own the
shard. `cleanup` removes local shard data that the current map says this node
does not replicate.

## Console

Open `/_utils/#/cluster` (the **Cluster** button in the header) to see:

- local node identity and cluster settings;
- discovery state, seeds and a node table with reachability and latency;
- replication state with per-database/peer rows, checkpoints and errors;
- buttons to **Discover now**, **Sync now**, pause/resume background
  replication, and rebalance or clean up each database;
- expandable shard maps showing replicas, primary owner and local ownership.

The page refreshes every five seconds and records every request in the usual
request log.

## Limits

- **Static placement.** Discovery reports peers but does not change routing,
  shard maps or `--nodes`. A discovered node is visible, not joined.
- **No membership consensus.** There is no gossip, Raft or automatic join.
  Restart all nodes with an identical `--nodes` list to change membership.
- **Pull-only repair.** Background replication pulls from peers; there is no
  independent push scheduler.
- **Per-node view.** `_cluster` and `_replication` describe the receiving node.
  There is no cluster-wide aggregation endpoint.
- **Pause is in memory.** Restarting a node resumes background replication.
- **No pause of client writes.** Pausing replication does not stop normal
  replica fan-out.
- **No inter-node TLS or service accounts.** Discovery and replication trust
  the shared cluster secret; run nodes on a trusted network.

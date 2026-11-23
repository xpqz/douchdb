# Aviation explorer

A single-page explorer for the four Kaggle aviation datasets (see
`docs/kaggle-import.md`), served by a three-node douchdb cluster and read
entirely through the CouchDB HTTP API from the browser.

- `views.py` installs `_design/explore` in each database: `_stats` views by
  date (`[year, month]`), make, flight phase, weather, country/state and more,
  each with a `[value, year]` twin for drill-down, plus lookups for the
  deadliest events, registrations and (for `airline_accidents`) one-degree
  map cells. One `_stats` row gives both the event count and the fatalities.
- `index.html` has no dependencies. It shows totals, a year/month timeline, a
  breakdown per dimension, a map, the deadliest events, a registration search
  across all datasets and the cluster's three nodes; every panel prints the
  query it sent. Click a bar, a dot or a row to drill down.

## Running it

```sh
# Three native nodes, q=4 n=3, admin/secret, CORS for the page's origin.
# --heavy-per-user 4 lets the page run a few view queries at once (default 1).
for i in 0 1 2; do
  DOUCHDB_ADMIN=admin:secret DOUCHDB_CLUSTER_SECRET=demo-cluster-secret \
  DOUCHDB_SESSION_SECRET=demo-session-secret \
  douchdb --listen 127.0.0.1:598$((4+i)) --data demo/node$i --q 4 --n 3 \
    --nodes http://127.0.0.1:5984,http://127.0.0.1:5985,http://127.0.0.1:5986 \
    --me $i --sync-secs 3 --heavy-per-user 4 \
    --cors-origins http://127.0.0.1:8090,http://localhost:8090 &
done

# Import (see docs/kaggle-import.md), then install the views.
DOUCHDB_PASSWORD=secret DOUCHDB_CLUSTER_SECRET=demo-cluster-secret \
  python3 scripts/import_kaggle.py --import --url http://127.0.0.1:5984 \
  --pause-replication --verify-nodes http://127.0.0.1:5984,http://127.0.0.1:5985,http://127.0.0.1:5986
DOUCHDB_PASSWORD=secret python3 examples/aviation-explorer/views.py

# Serve the page and open http://127.0.0.1:8090/?connect
cd examples/aviation-explorer && python3 -m http.server 8090 --bind 127.0.0.1
```

`?connect` signs in with the defaults (admin/secret on 127.0.0.1:5984);
`&db=<database>` opens a dataset directly.

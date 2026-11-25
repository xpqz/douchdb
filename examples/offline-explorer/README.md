# Offline-first explorer: a step-by-step tutorial

The [aviation explorer](../aviation-explorer) asks the cluster every question
over HTTP. That is how you would use most databases, and with douchdb it works
well. But douchdb, like CouchDB, is built for another way of working too:
the application keeps **its own copy** of the data (here: PouchDB, inside the
browser) and reads and writes that copy, while **replication** keeps the copy
and the server in step in the background, whenever there is a network.

Unless you have seen both side by side, the difference is easy to miss. This
example puts them side by side, on one page, over the same data:

- **Remote** queries the cluster directly, through node 0.
- **Device A** is a PouchDB copy that syncs with node 0.
- **Device B** is a PouchDB copy that syncs with node 2. Think of it as a
  second user's laptop, connected to a different server.

Each pane has an online switch. Below the panes, a **propagation timeline**
follows the last edit from copy to copy, a **conflicts** panel shows what
happens when two copies are edited apart, and an **event log** records every
sync step.

The data is a slice of the `airline_accidents` dataset: the events of 2006 and
2007, 3,867 of them, copied into a database of its own, `offline_demo`, so
the demo's edits never touch the imported data.

## 1. What you will see, and why it matters

The local copies keep working with no network at all, so the application
stays usable on a train, in a hangar, or when the server is down, and it never
waits on a round trip. The price is that copies agree **eventually**, not
instantly: an edit made on one device reaches the others a moment later, or
much later if a device was offline, and two devices that edit the same record
while apart produce a **conflict** that someone has to resolve. Querying the
server directly has none of those problems and none of those benefits. The
steps below show each of them.

## 2. Start the cluster and create the demo database

You need the three-node cluster from the aviation explorer, with
`airline_accidents` imported (see `docs/kaggle-import.md`). Two flags differ:
the page's origin (port 8091) must be allowed, and PouchDB sends its requests
with credentials, so `--cors-credentials` is needed.

```sh
for i in 0 1 2; do
  DOUCHDB_ADMIN=admin:secret DOUCHDB_CLUSTER_SECRET=demo-cluster-secret \
  DOUCHDB_SESSION_SECRET=demo-session-secret \
  douchdb --listen 127.0.0.1:598$((4+i)) --data demo/node$i --q 4 --n 3 \
    --nodes http://127.0.0.1:5984,http://127.0.0.1:5985,http://127.0.0.1:5986 \
    --me $i --sync-secs 3 --heavy-per-user 4 \
    --cors-origins http://127.0.0.1:8090,http://localhost:8090,http://127.0.0.1:8091,http://localhost:8091 \
    --cors-credentials &
done
```

Then create `offline_demo`. The script deletes and recreates it, copies the
2006–2007 events and installs `_design/offline`. Its views are the aviation
explorer's own (`views.py`), so the same map functions run on the cluster and,
later, inside the browser.

```sh
DOUCHDB_PASSWORD=secret python3 examples/offline-explorer/seed.py
# offline_demo: 3867 documents
```

Run it again at any time to reset the cluster side of the demo.

## 3. Open the page and watch the devices fill up

```sh
cd examples/offline-explorer && python3 -m http.server 8091 --bind 127.0.0.1
```

Open <http://127.0.0.1:8091/?connect>. `?connect` signs in as admin/secret,
which is fine for a local demo and nothing else; without it, fill in the form
and press Connect.

The Remote pane shows its totals at once. Each device first **pulls** the
whole database (watch `pulling 2000/3868` in its state line: the extra
document is the design document), then builds its local index
(`building index…`), then shows the same totals. With the cluster on the same
machine this takes about ten seconds. From then on each device runs a **live
two-way sync** with its node, and its state moves between `active` and `idle`.
The log shows `pulled 3868 documents from …`.

This first pull is the main cost of working offline first: every device stores
and downloads its copy before it can answer anything.

## 4. Compare: same answers, different costs

All three panes now show the same number of events (3,867), the same number
of fatalities (2,824) and the same breakdown by flight phase. Press **Run
query again** in each pane a few times and compare the **last query** times.

The result may surprise you. With the cluster on the same machine, the Remote
pane answers in a few milliseconds and the devices take a few hundred. The
summary is an aggregate (`_stats` over every event). The cluster keeps the
aggregate precomputed in its view index and sends back one row. PouchDB
recomputes it in JavaScript, from every row it reads out of the browser's
IndexedDB. The device's time never depends on the network, though. Over a
mobile connection, a round trip alone can take longer than the whole local
query, and with no connection the Remote pane has no answer at all (next
step). Opening a single record (step 6) is local and immediate on a device,
whatever the network.

So offline first is not automatically faster. Its gains are availability and
predictability. Aggregating over a lot of data is work the server does better
(see step 9).

## 5. Go offline

Switch **Remote** off and press **Run query again**: the pane says `offline`
and the log says `request refused`. A page that queries the server has
nothing to show without a network.

Now switch **Device A** off. Its sync stops (`offline`), but its pane keeps
answering, as fast as before. Nothing about reading the data needed the
network.

The switches are simulated: the device cancels its sync and the remote pane
refuses to send requests. To try it for real, stop the douchdb processes, or
use your browser's devtools to go offline: the devices keep working and their
sync retries until the cluster is back.

Switch Remote back on before the next step.

## 6. Edit on Device A while it is offline

With Device A still offline, pick a record in Device A's **Edit a record**
list (the deadliest events of the slice are listed; or type a registration and
press Enter), raise its fatality count by one, add a note, and press **Save on
this device**.

- Device A's fatality total goes up by one at once, and its **unsynced
  changes** counter shows `1`: the edit exists only in this browser.
- Device B still shows the old total, and so does the Remote pane when you
  press Run query again. Nothing has left Device A.
- The propagation timeline now follows the new revision. Only Device A holds
  it.

## 7. Back online: watch the edit travel

Switch Device A back on and watch the timeline:

1. **Device A** already had it.
2. **Cluster (node 0)** gets it when A's sync pushes it (the log says
   `Device A: pushed 1 document`).
3. **Device B** gets it next. B syncs with **node 2**, so the edit travels
   A → node 0 → the shard copies on node 2 → B. B's sync picks the change up
   from node 2's changes feed (`Device B: pulled 1 document`).

Each row says how long after the save that copy first held the revision. The
page reads every copy once a second, so the cluster and Device B often show
up in the same second. On one machine the whole trip takes a couple of
seconds. Nothing guarantees a bound. This is
**eventual consistency**: every copy ends up with the same data, but for a
while, different copies give different answers. The Remote pane's total
changes when you press Run query again.

## 8. Make a conflict, then resolve it

Switch **both devices** off. Open the same record on both: the deadliest event of the slice, at the
top of both lists, `20070723X00977:00000956` (São Paulo, 2007-07-17, 199
fatalities; it has no registration, so the list shows its id). On Device A set the note to `from A` and save; on Device B set
it to `from B` and save. Each device now holds a different version of the
same record, made from the same parent revision.

Switch both back on. Within a few seconds the **Conflicts** panel lists the
record **on both devices**, with the **same winner** on both. Nobody chose
it: every copy applies CouchDB's rule (the longer revision history wins, then
the higher revision id), so all copies agree without talking to each other.
The losing version is not thrown away; it stays stored as a conflict, on both
devices and on the cluster.

Press **Keep this** next to the version you want (try the one that lost).
The page writes that version's content on top of the winner and deletes the
other version, in one batch. That resolution is an ordinary edit: it syncs to
the cluster and to the other device, and the conflict disappears from both.
Applications that work offline first need a policy like this, whether that is
asking the user, as here, or merging the versions in code.

## 9. When to query the server instead

Offline first suits data a user works on, that fits on their device, and that
must stay usable without a network: field work, inspection checklists, notes,
a personal or team slice of a larger database.

Query the server directly when:

- the question is about the whole dataset (the aviation explorer's
  aggregates over 337,455 records: no device should download all of it to
  count it);
- the data is too large to copy, or changes too fast for copies to be useful;
- the user must not hold the data, or may only see answers computed from it;
- an answer must reflect every write the moment it was made (with copies, a
  reader may see an older version for a while).

And account for what offline first costs: storage on every device, the first
sync before anything works, and conflicts that your application has to
resolve.

## Resetting

In this order:

1. Switch **both devices off**. A device that is still syncing while the
   database is recreated would push its old copy, edits and conflicts
   included, straight back into the new one.
2. Run `seed.py` again to reset the cluster side.
3. Press **Reset devices**: both PouchDB copies are deleted.
4. Switch both devices back on: each pulls the fresh database.

## Running the automated check

`clients/pouchdb/run-offline-explorer.sh` drives this page in Chromium (the
pinned Playwright image) through steps 3–8 and reset, against synthetic data:

```sh
# Cluster as above, but with --cors-origins http://127.0.0.1:8099 --cors-credentials
DOUCHDB_PASSWORD=secret python3 examples/offline-explorer/seed.py --fixture 300
clients/pouchdb/run-offline-explorer.sh http://host.docker.internal:5984 http://host.docker.internal:5986 admin:secret
```

`python3 -m unittest` in this directory tests `seed.py` without a cluster.

## Files

- `index.html`: the page (no build step; PouchDB is loaded from `vendor/`).
- `seed.py`: creates `offline_demo` and installs `_design/offline`.
- `vendor/pouchdb.min.js`: PouchDB 9.0.0, from the `pouchdb` npm package
  (Apache-2.0, `vendor/LICENSE`).

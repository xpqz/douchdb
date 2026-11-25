# Offline-first aviation explorer: design

Status: for approval, 2026-11-25. Reviewed by a builder check; its questions are answered below.

## Intent

douchdb is a CouchDB-alike, and CouchDB-alikes are best used offline first:
the application reads and writes a local copy of its data (PouchDB in a
browser) and replication keeps that copy and the server in step in the
background. The existing example, `examples/aviation-explorer`, does the
opposite: every panel queries the cluster over HTTP. Someone reading the two
would not see why the local-copy approach is different, let alone better for
some uses.

This example makes the difference visible, and makes its price visible too:
copies agree eventually, not instantly, and two copies edited apart can
conflict. It is one piece of a broader set of examples and documentation, so
its write-up is a step-by-step tutorial that a reader can follow, not a
reference.

## Goals

- One page, three panes, over the same slice of the aviation data:
  - **Remote**: queries the cluster directly, as the existing explorer does.
  - **Device A**: a PouchDB copy that syncs with cluster node 0.
  - **Device B**: a PouchDB copy that syncs with cluster node 2.
- Each pane shows the same summary (totals, a breakdown) and how long its last
  query took, so local speed and remote round trips can be compared.
- Each pane has an online/offline switch. Offline, the remote pane fails and
  the devices keep working; a device's edits wait as unsynced changes.
- Edits: a reader can correct a record (fatalities, a note) on any pane.
- A propagation timeline shows, for the record last edited, which revision
  each copy holds (Device A, cluster, Device B) and when each copy got it.
- A conflict can be produced on purpose (both devices offline, same record
  edited differently, both back online). Both devices then show the same
  winner and the losing revision, and the reader can resolve it.
- A tutorial (`README.md`) that walks through the demo step by step and says
  what each step shows, ending with when to use which approach.

## Non-goals

- Not a production app: no user accounts beyond the demo admin, no form
  validation beyond what the demo needs, no styling ambitions beyond clarity.
- Not a full replica of the datasets: one slice of one database.
- Not a view of the cluster's own internal replication between nodes. The
  cluster is shown as one copy; node-to-node anti-entropy is a separate topic.

## Anti-goals

- Hiding how "offline" is done. The switches are simulated (replication is
  stopped and the remote pane refuses to send requests); the page says so. A
  reader who pulls the network cable should see the same thing, and the
  tutorial says how to try that.
- Pretending conflicts do not happen, or resolving them silently.
- A demo that needs more than a minute of waiting before it is usable.

## Constraints

- Data: the `airline_accidents` events dated 2006–2007 (about 3,900
  documents; the dataset ends in 2007), copied into a database of their own,
  `offline_demo`, by a seed script in the example (my call). The script
  deletes and recreates `offline_demo`, copies the documents whose
  `event_date` (stored as ISO `YYYY-MM-DD`) satisfies `$gte "2006-01-01"` and
  `$lt "2008-01-01"` (read by paging `_all_docs?include_docs=true` of
  `airline_accidents`, since douchdb has no Mango `_find`; written without
  revisions), and installs `_design/offline`. The demo's edits then
  never touch the imported datasets, re-running the script resets the
  cluster side in seconds, and every copy holds exactly the same documents,
  so the "same totals" step cannot drift. (douchdb has no client-facing
  `_replicate`, so the seeding is a script.)
- Views: `_design/offline` holds `by_date`, `by_phase`, `by_fatalities` and
  `by_registration`, generated with `views.py`'s helpers so their map
  functions are the explorer's own. The devices replicate the whole
  `offline_demo` database, design document included, and query those same
  views locally: one definition, run by QuickJS on the cluster and by the
  browser in PouchDB (the functions are ES5, as QuickJS needs). PouchDB's
  map/reduce has `_stats` built in. No filtered replication is needed.
- PouchDB 9 in the browser (IndexedDB), which `clients/pouchdb` already tests
  against douchdb with CORS and credentials. The cluster must run with
  `--cors-origins <page origin> --cors-credentials`.
- PouchDB is vendored next to the page: `pouchdb.min.js` from the `pouchdb`
  9.x npm package (the same major version `clients/pouchdb` tests), with its
  licence. It includes map/reduce; `pouchdb-find` is not needed. The page
  needs no network beyond the cluster and stays dependency-free to serve.
- Auth as the existing explorer: admin/secret over Basic auth, `?connect`
  signs in with the defaults; PouchDB gets the same credentials via its
  `auth` option. Fine for a local demo, and the tutorial says so.
- Same cluster setup as the existing explorer: three nodes, q=4 n=3,
  admin/secret, `--sync-secs 3`.

## Approaches considered

1. **Remote and local side by side.** One remote pane and one PouchDB pane,
   one network switch. Clear contrast, but no disagreement between copies.
2. **Two devices.** Two PouchDB copies on different nodes, each with a
   switch. Shows convergence and conflicts, but never shows querying the
   server directly.
3. **Both: three panes plus a propagation timeline.** Chosen (2026-11-25):
   the only one that shows the contrast the reader is missing and the
   eventual consistency that comes with it. It is the largest page.

## Design

### Layout

```
┌────────────── Offline-first explorer · airline accidents 2006–2007 ─────────────┐
│ Remote (cluster)        │ Device A (PouchDB ↔ node 0) │ Device B (PouchDB ↔ node 2)│
│ [online ●]              │ [online ●]  sync: idle      │ [online ●]  sync: idle     │
│ events 3,867 · fatal …  │ events 3,867 · fatal …      │ events 3,867 · fatal …     │
│ query 38 ms             │ query 4 ms                  │ query 4 ms                 │
│ by phase ▇▇▅▃▂          │ by phase ▇▇▅▃▂              │ by phase ▇▇▅▃▂             │
│ record: N123AB  [edit]  │ record … [edit] unsynced: 1 │ record … [edit]            │
├──────────────────────────────────────────────────────────────────────────────────┤
│ Propagation of <record id>:  A 3-a1… 12:00:01 → cluster 3-a1… 12:00:02 → B …      │
│ Conflicts: <id> winner 4-c… · other 4-b… [keep winner] [keep other]               │
│ Event log: A pushed 1 · B pulled 1 · Remote: request refused (offline) …          │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Behaviour

- **First load.** Each device creates its PouchDB (`offline-a`, `offline-b`)
  and pulls `offline_demo` (one-off replication), showing progress. Then
  each starts a live two-way sync with its node, with retry.
- **Summaries.** Every pane shows: number of events and total fatalities for
  the slice, a breakdown by flight phase, and the time its last query took.
  Remote queries `_design/offline` on the cluster through node 0, the
  same node Device A syncs with; devices query their local copy of it. Device panes refresh on every local change
  (PouchDB `changes`, live) and every replication `change` event.
- **Switches.** A device going offline cancels its sync; coming back starts
  it again. The remote pane going offline makes the page's own request
  function for that pane fail at once with "offline", as a browser without
  a network would; it does not wrap the global `fetch`, so the devices are
  unaffected. Each pane shows its
  state and, for devices, the count of local changes not yet pushed.
- **Edits.** Any pane can open a record (pick from the deadliest events of
  the slice or search by registration) and change its fatality count
  (`total_fatal_injuries`, so the summaries visibly move) or add a note. The
  remote pane writes to the cluster directly (HTTP PUT); devices write
  locally. The timeline then follows that record. The tutorial names one
  record (the deadliest of the slice) for the conflict step, so readers do
  not have to hunt for one.
- **Propagation timeline.** For the record last edited, the page polls each
  copy's revision (device: local `get`; cluster: GET through node 0) once a
  second and records the time each one first showed the new revision.
  Device B syncs through node 2: a write that reached the cluster through
  node 0 is already on node 2's copies (every node holds every shard, n=3,
  and writes go to the shard copies), and the cluster's anti-entropy
  (`--sync-secs 3`) closes any gap within seconds. So "cluster" means "as
  read through node 0", the expected delays are a few seconds, and nothing
  in the page or a test asserts a timing, only that copies converge.
- **Conflicts.** Each device lists its documents with `_conflicts`
  (`conflicts: true` on reads of edited records, plus a check after each
  sync). Replication carries every leaf revision, so the cluster and both
  devices hold both branches and pick the same winner (CouchDB's rule). A
  conflicted record shows both revisions side by side. "Keep" writes the
  chosen revision's content on top of the winning revision and deletes the
  other leaf, in one `bulkDocs`; that resolution then syncs like any edit.
- **Reset.** Re-running the seed script resets the cluster side; the page's
  Reset button destroys both PouchDBs and pulls again. The tutorial does the
  two together before a second run.

### Files

- `examples/offline-explorer/index.html`: the page (HTML, CSS and script in
  one file, as the existing explorer).
- `examples/offline-explorer/seed.py`: creates and fills `offline_demo` and
  installs `_design/offline` (Python standard library, as `views.py`).
- `examples/offline-explorer/vendor/pouchdb.min.js` and its licence.
- `examples/offline-explorer/README.md`: the tutorial.
- Optionally `examples/README.md` (or the existing explorer's README) gains a
  line pointing from one example to the other.

### Tutorial outline

1. What you will see, and why it matters (one paragraph).
2. Start the cluster (same commands as the existing explorer, plus
   `--cors-credentials` and the new page's origin), import, run `seed.py`.
3. Serve and open the page; watch the devices pull the slice.
4. Compare: same totals in all three panes, local query times vs remote.
5. Go offline: remote fails, devices keep working.
6. Edit on Device A while offline: unsynced change, nothing elsewhere moves.
7. Back online: watch the timeline (A → cluster → B) and note the delay.
8. Make a conflict: both devices offline, edit the same record differently,
   both online; same winner on both, the other revision kept; resolve it.
9. When to query the server directly instead (aggregates over the whole
   dataset, data too big to copy, data the user must not hold), and what
   offline first costs (storage, first sync, conflicts to handle).

## Left to the builder

- Exact visual layout, colours and wording, within the sketch above.
- How records are picked for editing (a short list plus search is enough).
- Poll intervals and log length.
- Whether the remote pane's edit form shares code with the devices'.

## Testing

A manual run of the tutorial against a local three-node cluster, recorded as
done in the PR (each step's result). Optionally a Playwright script in
`clients/pouchdb` that loads the page, toggles a device offline, edits, comes
back online and waits for the other device to show the edit; the builder
decides whether it pays for itself.

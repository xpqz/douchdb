# Offline-first explorer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A one-page demo, `examples/offline-explorer`, with a Remote pane that queries the douchdb cluster directly and two PouchDB "devices" that sync with different nodes, plus a propagation timeline and conflict resolution, and a step-by-step tutorial.

**Architecture:** `seed.py` builds a database `offline_demo` (a 2006–2007 slice of `airline_accidents`, or a small synthetic fixture for tests) and installs `_design/offline`, whose views are taken verbatim from `examples/aviation-explorer/views.py`. `index.html` (one file, no build) loads a vendored PouchDB 9.0.0; every pane runs the same summary queries against the same design document, remotely or locally. A Playwright script in `clients/pouchdb` drives the page against a local three-node cluster and is the automated check.

**Tech Stack:** Python 3 standard library; plain browser JavaScript; PouchDB 9.0.0 (`pouchdb.min.js`); Playwright (`playwright-core` 1.49.1, image `mcr.microsoft.com/playwright:v1.49.1-noble`) as `clients/pouchdb` already pins.

**Spec:** `docs/superpowers/specs/2026-11-25-offline-first-explorer-design.md`

## Global Constraints

- Branch `offline-explorer`, worktree `/Volumes/Space2/douchdb-scratch/wt-offline`. All build output, cluster data and temp files under `/Volumes/Space2/douchdb-scratch/` (e.g. `tmp/offline-test/`); delete them when done.
- The machine is in use: `nice -n 15`, `cargo build --release -j 4`. Every run has a hard bound (`timeout`, ≤ 15 min) and kills its children.
- Tests wait on conditions (Playwright `waitForFunction`, polling with an overall cap), never on fixed sleeps or asserted timings.
- No mention of AI/assistants in commits, PRs or files.
- Database `offline_demo`; design document `_design/offline` with views `by_date`, `by_phase`, `by_fatalities`, `by_registration`, byte-identical to `views.design("airline_accidents")["views"][...]`.
- Slice: `event_date` `$gte "2006-01-01"` and `$lt "2008-01-01"`, copied from `airline_accidents` without `_rev`.
- Device databases `offline-a` (syncs with node 0) and `offline-b` (syncs with node 2). Remote pane and the timeline's "cluster" read go through node 0.
- Defaults: node 0 `http://127.0.0.1:5984`, node 2 `http://127.0.0.1:5986`, user `admin`, password `secret`; `?connect` signs in with them. Page served on port 8091 (the existing explorer uses 8090). Cluster needs `--cors-origins <page origin> --cors-credentials`.
- Offline is simulated and the page says so: devices cancel their sync; the remote pane's own request function throws `offline`; the global `fetch` is never wrapped.
- The edited fields are `total_fatal_injuries` (number) and `demo_note` (string).
- Map functions stay ES5 (QuickJS); the page script may use modern browser JS.

## Review Focus

1. **Per-user request limits.** Two live syncs, their initial pulls and the remote pane's queries all run as `admin`. If the cluster answers 429, sync must retry and the event log must show it; the page must not hang. Covered by the Playwright run in Task 3, which starts the cluster with `--heavy-per-user 4` as the tutorial does.
2. **A device's first query builds its local index** (seconds for about 3,900 docs). The pane must show "building index…" rather than a blank or stale pane. Task 3's test waits for the device totals to equal the remote totals.
3. **Going offline during the initial pull.** The switch cancels the pull. Coming back online restarts it, then starts the live sync. Task 3 test step: switch Device B off during its pull, back on, and wait for equal totals.
4. **A remote edit on a stale revision (409).** It shows "changed elsewhere, reloaded" and reloads the record, and nothing is lost silently. Task 4 test step: edit on Device A, let it sync, then save the remote pane's form that was opened before the sync.
5. **A conflict with more than one losing leaf.** "Keep" deletes every losing leaf, not just the first. Task 4's Keep helper is written over `_conflicts` as a list, and the test checks that `_conflicts` is empty on both devices afterwards.

---

### Task 1: `seed.py`

**Files:**
- Create: `examples/offline-explorer/seed.py`
- Create: `examples/offline-explorer/test_seed.py`

**Interfaces:**
- Consumes: `examples/aviation-explorer/views.py` (`design(db) -> dict`), imported by putting `../aviation-explorer` on `sys.path`.
- Produces:
  - `design_doc() -> dict`
  - `slice_selector() -> dict`
  - `prepare(doc: dict) -> dict` (drops `_rev`)
  - `fixture_docs(n: int) -> list[dict]`
  - CLI `python3 seed.py [--url U] [--username admin] [--fixture N]`. The password comes from `DOUCHDB_PASSWORD`. The command prints `offline_demo: <count> documents`.

- [ ] **Step 1: Write the failing tests** (`python3 -m unittest`, no cluster)

```python
import sys, os, unittest
sys.path.insert(0, os.path.dirname(__file__))
import seed, views

class SeedTest(unittest.TestCase):
    def test_design_doc_reuses_the_explorer_views(self):
        d = seed.design_doc()
        self.assertEqual(d["_id"], "_design/offline")
        src = views.design("airline_accidents")["views"]
        self.assertEqual(sorted(d["views"]), ["by_date", "by_fatalities", "by_phase", "by_registration"])
        for k, v in d["views"].items():
            self.assertEqual(v, src[k])

    def test_slice_selector(self):
        self.assertEqual(seed.slice_selector(),
            {"event_date": {"$gte": "2006-01-01", "$lt": "2008-01-01"}})

    def test_prepare_drops_rev_only(self):
        self.assertEqual(seed.prepare({"_id": "x", "_rev": "1-a", "n": 1}), {"_id": "x", "n": 1})

    def test_fixture_docs_are_in_the_slice_and_deterministic(self):
        a, b = seed.fixture_docs(300), seed.fixture_docs(300)
        self.assertEqual(a, b)
        self.assertEqual(len({d["_id"] for d in a}), 300)
        for d in a:
            self.assertTrue("2006-01-01" <= d["event_date"] < "2008-01-01")
            self.assertIsInstance(d["total_fatal_injuries"], int)
            self.assertIn("broad_phase_of_flight", d)
            self.assertIn("aircraft_registration", d)
        self.assertEqual(max(d["total_fatal_injuries"] for d in a),
                         a[0]["total_fatal_injuries"])  # fixture-0 is the deadliest
```

- [ ] **Step 2: Run** `cd examples/offline-explorer && python3 -m unittest test_seed -v`. Expected: FAIL, `No module named 'seed'`.

- [ ] **Step 3: Implement `seed.py`**
  - `design_doc()` takes the four views from `views.design("airline_accidents")` and adds `"language": "javascript"`.
  - `fixture_docs(n)` builds ids `fixture-0000`… and spreads the dates over 2006–2007. It uses phases from a fixed list (`TAKEOFF`, `CRUISE`, `APPROACH`, `LANDING`, `TAXI`) and registrations `N<number>F`. `fixture-0000` has the strictly highest `total_fatal_injuries`.
  - `main()` does the following in order:
    1. DELETE `offline_demo`, ignoring 404, then PUT it.
    2. Without `--fixture`: `POST /airline_accidents/_find` with `slice_selector()`, `limit` 2000 and `skip` paging until a short page comes back. With `--fixture N`: use `fixture_docs(N)`.
    3. `POST /offline_demo/_bulk_docs` in batches of 500 with `prepare`d docs. Fail with a non-zero exit if any row has an `error`.
    4. PUT `design_doc()`.
    5. Print the count from `GET /offline_demo` `doc_count` minus 1 (the design doc).
  - Same HTTP helper shape as `views.py`, with a 120 s timeout per request.

- [ ] **Step 4: Run the tests again.** Expected: 4 tests OK.

- [ ] **Step 5: Commit** `git commit -m "offline-explorer: seed script for offline_demo"`

### Task 2: Test cluster and runner

**Files:**
- Create: `clients/pouchdb/offline-explorer.js` (skeleton: serves the page and logs in; no checks yet)
- Create: `clients/pouchdb/run-offline-explorer.sh`

**Interfaces:**
- Produces:
  - `run-offline-explorer.sh <node0 url> <node2 url> [user:password]`. It mounts the repo's `examples/offline-explorer` read-only at `/page`, runs `node offline-explorer.js "$node0" "$node2" "$auth"` in the pinned Playwright image under `timeout 600`, and removes the container on exit, as `run-browser.sh` does.
  - `offline-explorer.js` serves `/page` on `127.0.0.1:8099`. It opens `/index.html` in Chromium, fills `#node0`, `#node2`, `#user` and `#password`, then clicks `#connect`. Checks are added in Tasks 3–4. Exit code 0 means every check passed; any failure prints the check name and the page's event log (`#log` text) and exits 1.
- The test cluster (the executor's recipe, also used in Tasks 3–5):
  1. `nice -n 15 cargo build --release -j 4` with `CARGO_TARGET_DIR=/Volumes/Space2/douchdb-scratch/tmp/target`.
  2. Start three nodes on `127.0.0.1:25984-25986` with data under `/Volumes/Space2/douchdb-scratch/tmp/offline-test/node$i`, using the README's flags plus `--cors-origins http://127.0.0.1:8099 --cors-credentials --heavy-per-user 4`. Use admin `admin:secret`.
  3. Seed with `DOUCHDB_PASSWORD=secret python3 examples/offline-explorer/seed.py --url http://127.0.0.1:25984 --fixture 300`.
  4. Run the script with node URLs `http://host.docker.internal:25984` and `:25986`.
  5. Kill the nodes by PID when done.

- [ ] **Step 1:** Write both files. The page does not exist yet.
- [ ] **Step 2:** Run against the test cluster. Expected: FAIL, because `#connect` is not found.
- [ ] **Step 3: Commit** `git commit -m "offline-explorer: Playwright runner"`

### Task 3: The page: connect, panes, sync, switches

**Files:**
- Create: `examples/offline-explorer/index.html`
- Create: `examples/offline-explorer/vendor/pouchdb.min.js` and `vendor/LICENSE`. Both are copied from the `pouchdb@9.0.0` npm tarball (`npm pack pouchdb@9.0.0` in the scratch dir; `package/dist/pouchdb.min.js` and `package/LICENSE`).
- Modify: `clients/pouchdb/offline-explorer.js`

**Interfaces (DOM contract the test relies on):**
- Sign-in: `#node0`, `#node2`, `#user`, `#password`, `#connect`. `?connect` submits the defaults.
- Each pane is `section[data-pane="remote"|"a"|"b"]` and holds:
  - `[data-role=online]` (a checkbox, checked = online)
  - `[data-role=state]` (text: `pulling 1200/3867`, `building index…`, `idle`, `active`, `offline`, `error: …`)
  - `[data-role=events]` and `[data-role=fatal]` (integers, no separators)
  - `[data-role=ms]` (the last query's time)
  - `[data-role=phases]` (the breakdown)
  - `[data-role=reload]` (a button that reruns the summary)
  - devices only: `[data-role=unsynced]` (an integer)
- `#log`: the event log, newest first, capped at 200 lines.
- Page functions used by later tasks:
  - `remoteFetch(path, init) -> Promise<Response>`. It adds Basic auth and throws `Error("offline")` while the remote switch is off.
  - `observerFetch(path, init)`: the same request, but it ignores the switch (the timeline's view of the cluster).
  - `devices.a` / `devices.b`: `{db: PouchDB, remote: PouchDB, sync, online, unsynced: Set<string>}`. `unsynced` holds `id@rev` pairs.
  - `refresh(paneKey)`.
  - `log(text)`.

- [ ] **Step 1: Add checks to `offline-explorer.js`**
  - `remote_totals`: the remote pane's events equal the fixture count (300). The script reads this count from the cluster (`doc_count - 1`) at start.
  - `devices_converge`: Device B's switch goes off while its state starts with `pulling`, then back on. Then both device panes' events and fatal values equal the remote's (Review Focus 2, 3).
  - `remote_offline_fails`: with the remote switch off, clicking `[data-role=reload]` sets the remote state to `offline`. Both devices' events still equal the remote's earlier value.
- [ ] **Step 2: Run.** Expected: FAIL at `remote_totals`.
- [ ] **Step 3: Implement**
  - Summaries are the same on every pane:
    - `by_date` with `reduce=true` and no group: one `_stats` row (`count` gives events, `sum` gives fatal).
    - `by_phase` with `group=true`, drawn as bars.
    - Time both queries together with `performance.now()`.
  - Remote calls `/offline_demo/_design/offline/_view/...` through `remoteFetch`. Devices call `db.query('offline/by_date', {reduce: true})`.
  - Device startup:
    1. `new PouchDB('offline-a')` and a remote `new PouchDB(node0 + '/offline_demo', {auth})`.
    2. `PouchDB.replicate(remote, db, {batch_size: 500})`, showing progress from `change` events (`docs_written` against the remote `doc_count`).
    3. Then `db.sync(remote, {live: true, retry: true})`. Log every `change` (direction and doc count), `paused`, `active`, `denied` and `error`.
  - The device's first local query runs while the state shows `building index…`.
  - `db.changes({since: 'now', live: true})` triggers `refresh`, debounced to 250 ms.
  - A device switch going off cancels the pull or sync in progress. Going on restarts from the step it was cancelled in.
  - Every pane, and the header, says in one line that offline is simulated.
- [ ] **Step 4: Run.** Expected: all three checks pass.
- [ ] **Step 5: Commit** `git commit -m "offline-explorer: three panes with local and remote queries"`

### Task 4: Edits, timeline, conflicts, reset

**Files:**
- Modify: `examples/offline-explorer/index.html`
- Modify: `clients/pouchdb/offline-explorer.js`

**Interfaces:**
- Per pane:
  - `[data-role=pick]`: the deadliest 10 from `by_fatalities` (`descending=true`, `limit=10`), plus a registration search box `[data-role=search]` over `by_registration` (exact key after upper-casing). "No match" shows as text.
  - The edit form: `[data-role=fatal-input]`, `[data-role=note-input]`, `[data-role=save]`, and `[data-role=record-rev]` (the rev of the record open in that pane).
- `#timeline`: one row per copy, `[data-copy="a"|"cluster"|"b"]`, with `[data-role=rev]` (the current winning rev) and `[data-role=seen]` (time the target rev was first held, or blank).
- `#conflicts`: per device, a row `[data-device=a|b][data-conflict-id=<id>]` with `[data-role=winner]` and `[data-role=others]` (the revs), and buttons `[data-keep=winner]` and `[data-keep=<rev>]`.
- `#reset`.

- [ ] **Step 1: Add checks** (the fixture's deadliest record is `fixture-0000`)
  - `offline_edit_stays_local`:
    1. Switch A off and edit `fixture-0000` on A, with fatal +1.
    2. A's `unsynced` becomes 1 and A's fatal total rises by 1.
    3. B's and the remote's totals are unchanged (checked after A's refresh has shown the change).
  - `edit_propagates`: switch A on. Wait until `#timeline` shows the target rev on `a`, `cluster` and `b` with `seen` filled, B's fatal total equals A's, and A's `unsynced` is 0.
  - `stale_remote_edit` (Review Focus 4):
    1. Open `fixture-0000` in the remote pane.
    2. Edit it on A and wait for the cluster row to hold the new rev.
    3. Save the remote form. The remote state shows `changed elsewhere, reloaded`, and the form's rev equals the cluster's.
  - `conflict_same_winner`:
    1. Switch A and B off and edit `fixture-0000` differently on each (notes `from A`, `from B`), then switch both on.
    2. Wait until both devices list a conflict row for `fixture-0000` with the same winner and the same others.
  - `keep_resolves` (Review Focus 5):
    1. Click `[data-keep=<the other rev>]` on A.
    2. Wait until neither device lists `fixture-0000`, and `get(..., {conflicts: true})` on both shows no `_conflicts` (read via `page.evaluate` on `devices.x.db`).
    3. The winning `demo_note` on both equals the kept revision's note.
  - `reset`: click `#reset` and wait for both devices' totals to equal the remote's (the cluster side is not reseeded here).
- [ ] **Step 2: Run.** Expected: FAIL at `offline_edit_stays_local`.
- [ ] **Step 3: Implement**
  - **Remote save:** GET, then PUT with the open rev. A 409 reloads and shows `changed elsewhere, reloaded`.
  - **Device save:** `db.put`, then add `id@rev` to `unsynced`. Remove it when a push `change` event's `docs` includes that id at that rev or a later one.
  - **Timeline:**
    - The target is `{id, rev}` from the last save on any pane.
    - Every 1 s, read each copy: device copies with `db.get(id, {conflicts: true})`, the cluster with `observerFetch` `GET /offline_demo/<id>?conflicts=true`.
    - A copy holds the target when its `_rev` or one of its `_conflicts` equals the target rev. Record `seen` the first time it does.
  - **Conflicts:**
    - Each device tracks a `watched` set: ids saved on this page, plus ids in sync `change` docs.
    - After every change, re-read the watched ids with `{conflicts: true}` and list those with `_conflicts`.
    - Keep `r`: read the content of `r` (`db.get(id, {rev: r})`) and `bulkDocs` it as one batch: that content, minus `_rev`/`_conflicts`, with `_rev` set to the winner, plus `{_id, _rev: loser, _deleted: true}` for every rev in `_conflicts`.
  - **Reset:**
    1. Cancel both syncs and `destroy()` both databases.
    2. Clear the timeline, the conflicts, `unsynced` and `watched`.
    3. Run device startup again.
- [ ] **Step 4: Run.** Expected: all nine checks pass.
- [ ] **Step 5: Commit** `git commit -m "offline-explorer: edits, propagation timeline, conflicts, reset"`

### Task 5: Tutorial, manual run, cross-link

**Files:**
- Create: `examples/offline-explorer/README.md`
- Modify: `examples/aviation-explorer/README.md` (one line pointing to the new example)
- Modify: `clients/pouchdb/offline-explorer.js` header comment (how to run it)

- [ ] **Step 1: Write `README.md`**
  - Follow the spec's nine-step outline, with the commands for the README cluster: node flags plus `--cors-origins http://127.0.0.1:8090,http://localhost:8090,http://127.0.0.1:8091,http://localhost:8091 --cors-credentials`.
  - Then `seed.py` without `--fixture`, and serve on 8091.
  - Each step says what to do, what you should see, and why.
  - Name the deadliest record of the real slice for the conflict step. Get it from the manual run (`by_fatalities?descending=true&limit=1`).
  - Add a "try it for real" note: stop the douchdb processes, or use the browser's devtools offline mode, instead of the switches.
  - State that admin/secret is for a local demo only.
  - Add a "running the check" section for `run-offline-explorer.sh`.
- [ ] **Step 2: Manual run** against a local three-node cluster with `airline_accidents` imported:
  - Use `scripts/import_kaggle.py` from the main checkout (it is untracked there; run it by absolute path and do not copy it into the branch), with data under Space2.
  - Then seed. Record the printed count and the deadliest record, and put them in the README.
  - Follow the tutorial step by step and note each step's result for the PR description.
- [ ] **Step 3:** Re-run `run-offline-explorer.sh` once, then `python3 -m unittest` in `examples/offline-explorer`. Expected: all pass.
- [ ] **Step 4: Commit** `git commit -m "offline-explorer: tutorial"`
- [ ] **Step 5:** Stop the clusters, and delete `tmp/offline-test`, the import data dirs and the npm pack output.

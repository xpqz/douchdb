#!/usr/bin/env node
"use strict";

/**
 * Drives examples/offline-explorer in Chromium against a three-node douchdb
 * cluster: the devices converge with the cluster, an offline edit stays local
 * until its device comes back, a conflict shows the same winner on both
 * devices and resolving it clears it everywhere.
 *
 *   node offline-explorer.js <node 0 url> <node 2 url> <user:password>
 *
 * Serves the page from /page (or PAGE_DIR) on 127.0.0.1:8099. Use
 * run-offline-explorer.sh to run it in the pinned Playwright image. Every wait
 * is on a condition in the page, capped by WAIT_MS; nothing asserts timing.
 */

const http = require("http");
const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright-core");

const [node0, node2] = [process.argv[2], process.argv[3]];
const [user, pass] = (process.argv[4] || "admin:secret").split(":");
const pageDir = process.env.PAGE_DIR || "/page";
const pagePort = 8099;
const WAIT_MS = Number(process.env.WAIT_MS || 120000);
const TYPES = { ".html": "text/html", ".js": "text/javascript" };

function startPage() {
  return new Promise((resolve) => {
    const server = http.createServer((req, res) => {
      const rel = decodeURIComponent(new URL(req.url, "http://x").pathname).replace(/^\/+/, "");
      const file = path.join(pageDir, rel || "index.html");
      if (!file.startsWith(pageDir) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
        res.writeHead(404);
        res.end();
        return;
      }
      res.setHeader("content-type", TYPES[path.extname(file)] || "application/octet-stream");
      res.end(fs.readFileSync(file));
    });
    server.listen(pagePort, "127.0.0.1", () => resolve(server));
  });
}

/** Admin request to node 0 from the test itself (not through the page). */
async function cluster(pathname) {
  const r = await fetch(node0 + pathname, {
    headers: { authorization: "Basic " + Buffer.from(`${user}:${pass}`).toString("base64") },
  });
  if (!r.ok) throw new Error(`${pathname}: ${r.status}`);
  return r.json();
}

const sel = (pane, role) => `section[data-pane="${pane}"] [data-role="${role}"]`;

/** Text of a pane's element, trimmed. */
const text = (tab, pane, role) => tab.textContent(sel(pane, role)).then((t) => (t || "").trim());

/** Waits until a pane's element reads exactly `want`. */
function waitText(tab, pane, role, want) {
  return tab.waitForFunction(
    ([s, w]) => (document.querySelector(s)?.textContent || "").trim() === w,
    [sel(pane, role), String(want)]
  );
}

/**
 * Before connecting: switch Device B off the moment it starts its first pull
 * (a MutationObserver fires before any replication response can arrive).
 */
async function cutDeviceBDuringPull(tab) {
  await tab.evaluate((s) => {
    const obs = new MutationObserver(() => {
      const state = document.querySelector(s.state);
      if (state && state.textContent.startsWith("pulling")) {
        obs.disconnect();
        document.querySelector(s.online).click();
        window.__cutDuringPull = true;
      }
    });
    obs.observe(document.body, { subtree: true, childList: true, characterData: true });
  }, { state: sel("b", "state"), online: sel("b", "online") });
}


/** Polls `fn` (an async predicate) until it is true; fails after WAIT_MS. */
async function until(what, fn) {
  const end = Date.now() + WAIT_MS;
  for (;;) {
    if (await fn()) return;
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 250));
  }
}

const ID = "fixture-0000";

/** Opens the fixture's deadliest record in a pane and saves it with `change` applied. */
async function edit(tab, pane, change) {
  await tab.click(`${sel(pane, "pick")} button[data-id="${ID}"]`);
  await tab.waitForFunction((s) => document.querySelector(s).textContent.trim() !== "", sel(pane, "record-rev"));
  if (change.fatalDelta) {
    const now = Number(await tab.inputValue(sel(pane, "fatal-input")));
    await tab.fill(sel(pane, "fatal-input"), String(now + change.fatalDelta));
  }
  if (change.note !== undefined) await tab.fill(sel(pane, "note-input"), change.note);
  const before = await text(tab, pane, "record-rev");
  await tab.click(sel(pane, "save"));
  return before;
}

/** The device's copy of the record, read in the page: {rev, conflicts, note}. */
const localCopy = (tab, device, rev) =>
  tab.evaluate(async ([d, id, r]) => {
    const doc = await devices[d].db.get(id, r ? { rev: r } : { conflicts: true });
    return { rev: doc._rev, conflicts: doc._conflicts || [], note: doc.demo_note };
  }, [device, ID, rev]);

const conflictRow = (d) => `#conflicts [data-device="${d}"][data-conflict-id="${ID}"]`;
const timelineSeen = (tab, copy) => tab.textContent(`#timeline [data-copy="${copy}"] [data-role="seen"]`).then((t) => t.trim());
const timelineRev = (tab, copy) => tab.textContent(`#timeline [data-copy="${copy}"] [data-role="rev"]`).then((t) => t.trim());

/** Turns a pane's online switch to `on` (no-op if it already is). */
async function setOnline(tab, pane, on) {
  if ((await tab.isChecked(sel(pane, "online"))) !== on) await tab.click(sel(pane, "online"));
}

/** Fills the sign-in form and connects; `offline` lists device panes to start switched off. */
async function signIn(tab, offline = []) {
  await tab.fill("#node0", node0);
  await tab.fill("#node2", node2);
  await tab.fill("#user", user);
  await tab.fill("#password", pass);
  for (const d of offline) await setOnline(tab, d, false);
}

/** Waits until neither pane has a refresh in flight. */
const settled = (tab, key) => until(`${key} refresh to finish`, () => tab.evaluate((k) => !refreshing[k], key));

/** Text of the event log. */
const logText = (tab) => tab.textContent("#log");

/**
 * Edits the record apart on each listed pane (devices offline, Remote online),
 * then brings the devices back and waits until both list the same conflict
 * with `losers` losing revisions. Returns the losing revisions.
 */
async function makeConflict(tab, notes, losers) {
  await until("B to hold the last edit", async () => (await timelineSeen(tab, "b")) !== "");
  await setOnline(tab, "a", false);
  await setOnline(tab, "b", false);
  for (const [pane, note] of Object.entries(notes)) await edit(tab, pane, { note });
  await setOnline(tab, "a", true);
  await setOnline(tab, "b", true);
  for (const d of ["a", "b"]) await tab.waitForSelector(conflictRow(d));
  const row = async (d) => [
    await tab.textContent(`${conflictRow(d)} [data-role="winner"]`),
    await tab.textContent(`${conflictRow(d)} [data-role="others"]`),
  ].map((t) => t.trim());
  await until("both devices to agree on the conflict", async () => {
    const [a, b] = [await row("a"), await row("b")];
    return a[0] === b[0] && a[1] === b[1] && a[1].split(" ").length === losers;
  });
  return (await row("a"))[1].split(" ");
}

/** Waits until neither device lists a conflict and both hold `note` with no _conflicts. */
async function resolvedEverywhere(tab, note) {
  for (const d of ["a", "b"]) {
    await until(`${d} to drop the conflict`, async () => (await tab.$(conflictRow(d))) === null);
    await until(`${d} to hold the resolution`, async () => {
      const c = await localCopy(tab, d);
      return c.conflicts.length === 0 && c.note === note;
    });
  }
}

const CHECKS = [
  ["remote_totals", async ({ tab, cluster, state }) => {
    state.count = (await cluster("/offline_demo")).doc_count - 1;
    await waitText(tab, "remote", "events", state.count);
    state.fatal = Number(await text(tab, "remote", "fatal"));
    if (!(state.fatal > 0)) throw new Error(`remote fatal ${state.fatal}`);
  }],
  ["devices_converge", async ({ tab, state }) => {
    await waitText(tab, "b", "state", "offline");
    if (!(await tab.evaluate(() => window.__cutDuringPull))) throw new Error("B was not cut during its pull");
    await tab.click(sel("b", "online"));
    for (const d of ["a", "b"]) {
      await waitText(tab, d, "events", state.count);
      await waitText(tab, d, "fatal", state.fatal);
    }
  }],
  ["remote_offline_fails", async ({ tab, state }) => {
    await tab.click(sel("remote", "online"));
    await tab.click(sel("remote", "reload"));
    await waitText(tab, "remote", "state", "offline");
    await tab.click(sel("a", "reload"));
    await tab.click(sel("b", "reload"));
    for (const d of ["a", "b"]) await waitText(tab, d, "events", state.count);
    await tab.click(sel("remote", "online"));
    await tab.click(sel("remote", "reload"));
    await waitText(tab, "remote", "state", "idle");
  }],
  ["remote_switched_off_mid_query_says_offline", async ({ tab }) => {
    // #159: hold the remote pane's next request until the switch is off.
    await tab.evaluate(() => {
      const real = window.observerFetch;
      window.__release = null;
      // The last request of a refresh (its pick list); everything else passes.
      window.observerFetch = (path, init) => {
        if (!path.includes("/_view/by_fatalities")) return real(path, init);
        window.observerFetch = real;
        return new Promise((r) => (window.__release = r)).then(() => real(path, init));
      };
    });
    await tab.click(sel("remote", "reload"));
    await until("the held request", () => tab.evaluate(() => window.__release !== null));
    await tab.click(sel("remote", "online"));
    await tab.evaluate(() => window.__release());
    await settled(tab, "remote");
    if ((await text(tab, "remote", "state")) !== "offline") throw new Error(`state ${await text(tab, "remote", "state")}`);
    await tab.click(sel("remote", "online"));
    await waitText(tab, "remote", "state", "idle");
  }],
  ["failed_refresh_recovers", async ({ tab }) => {
    // #155: one failing query, with a rerun queued behind it.
    await tab.evaluate(() => {
      const db = devices.a.db;
      const real = db.query;
      window.__queries = 0;
      db.query = (...args) => {
        window.__queries++;
        if (window.__queries === 1) return Promise.reject(new Error("injected"));
        return real.apply(db, args);
      };
      window.__restoreQuery = () => (db.query = real);
      refresh("a");
      refresh("a");
    });
    await until("the queued rerun", () => tab.evaluate(() => window.__queries >= 3));
    await settled(tab, "a");
    await tab.evaluate(() => window.__restoreQuery());
    const state = await text(tab, "a", "state");
    if (state.startsWith("error")) throw new Error(`state stuck at ${state}`);
    if (!(await logText(tab)).includes("query failed: injected")) throw new Error("failure not logged");
  }],
  ["offline_edit_stays_local", async ({ tab, state }) => {
    await setOnline(tab, "a", false);
    await edit(tab, "a", { fatalDelta: 1 });
    await waitText(tab, "a", "unsynced", 1);
    await waitText(tab, "a", "fatal", state.fatal + 1);
    state.rev = await text(tab, "a", "record-rev");
    if (Number(await text(tab, "b", "fatal")) !== state.fatal) throw new Error("B moved while A was offline");
    await tab.click(sel("remote", "reload"));
    await waitText(tab, "remote", "state", "idle");
    if (Number(await text(tab, "remote", "fatal")) !== state.fatal) throw new Error("remote moved while A was offline");
  }],
  ["edit_propagates", async ({ tab, state }) => {
    await setOnline(tab, "a", true);
    for (const copy of ["a", "cluster", "b"]) {
      await until(`${copy} to hold ${state.rev}`, async () => (await timelineSeen(tab, copy)) !== "");
    }
    await waitText(tab, "b", "fatal", state.fatal + 1);
    await waitText(tab, "a", "unsynced", 0);
  }],
  ["timeline_says_when_every_copy_has_it", async ({ tab }) => {
    // #160: polling stops once all three copies hold the revision.
    await tab.waitForFunction(() => document.querySelector('#timeline [data-role="target"]').textContent.includes("Every copy has it"));
  }],
  ["timeline_counts_a_later_revision", async ({ tab }) => {
    // #160: a copy that only ever sees a later revision still holds the followed one.
    await setOnline(tab, "a", false);
    await edit(tab, "a", { note: "followed" });
    await tab.evaluate(async (id) => {
      const doc = await devices.a.db.get(id);
      await devices.a.db.put({ ...doc, demo_note: "written on top, outside the page" });
    }, ID);
    await setOnline(tab, "a", true);
    for (const copy of ["cluster", "b"]) {
      await until(`${copy} to count the later revision`, async () => (await timelineSeen(tab, copy)) !== "");
    }
  }],
  ["stale_device_save", async ({ tab }) => {
    // #157: B's form is opened, then A's edit arrives on B before B saves.
    await tab.click(`${sel("b", "pick")} button[data-id="${ID}"]`);
    await tab.waitForFunction((s) => document.querySelector(s).textContent.trim() !== "", sel("b", "record-rev"));
    const opened = await text(tab, "b", "record-rev");
    await edit(tab, "a", { note: "edited on A again" });
    await until("A's edit on B", async () => (await localCopy(tab, "b")).rev !== opened);
    await tab.fill(sel("b", "note-input"), "stale device edit");
    await tab.click(sel("b", "save"));
    await waitText(tab, "b", "record-msg", "changed elsewhere, reloaded");
    if ((await text(tab, "b", "record-rev")) !== (await localCopy(tab, "b")).rev) throw new Error("B's form not reloaded");
  }],
  ["stale_remote_edit", async ({ tab, cluster }) => {
    await tab.click(`${sel("remote", "pick")} button[data-id="${ID}"]`);
    await tab.waitForFunction((s) => document.querySelector(s).textContent.trim() !== "", sel("remote", "record-rev"));
    const opened = await text(tab, "remote", "record-rev");
    await edit(tab, "a", { note: "edited on A" });
    await until("A's edit to reach the cluster", async () =>
      (await timelineRev(tab, "cluster")) === (await text(tab, "a", "record-rev")) &&
      (await text(tab, "a", "record-rev")) !== opened);
    await tab.fill(sel("remote", "note-input"), "stale remote edit");
    await tab.click(sel("remote", "save"));
    await waitText(tab, "remote", "state", "changed elsewhere, reloaded");
    const current = (await cluster(`/offline_demo/${ID}`))._rev;
    if ((await text(tab, "remote", "record-rev")) !== current) throw new Error("remote form not reloaded");
  }],
  ["stale_remote_edit_when_reload_fails", async ({ tab }) => {
    // #156: the 409 is real, but re-reading the record fails.
    await tab.click(`${sel("remote", "pick")} button[data-id="${ID}"]`);
    await tab.waitForFunction((s) => document.querySelector(s).textContent.trim() !== "", sel("remote", "record-rev"));
    const opened = await text(tab, "remote", "record-rev");
    await edit(tab, "a", { note: "edited on A once more" });
    await until("A's edit to reach the cluster", async () =>
      (await timelineRev(tab, "cluster")) === (await text(tab, "a", "record-rev")) &&
      (await text(tab, "a", "record-rev")) !== opened);
    await tab.evaluate(() => {
      const real = window.observerFetch;
      // Fail only the pane's re-read of the record (a GET without query; the timeline adds one).
      window.observerFetch = (path, init) => {
        if ((init && init.method === "PUT") || path.includes("?")) return real(path, init);
        window.observerFetch = real;
        return Promise.reject(new Error("injected"));
      };
    });
    await tab.fill(sel("remote", "note-input"), "stale remote edit");
    await tab.click(sel("remote", "save"));
    await waitText(tab, "remote", "state", "changed elsewhere; reload failed: injected");
    if (!(await tab.isHidden(sel("remote", "editor")))) throw new Error("stale form still shown");
  }],
  ["conflict_same_winner", async ({ tab, state }) => {
    state.other = (await makeConflict(tab, { a: "from A", b: "from B" }, 1))[0];
  }],
  ["state_survives_a_page_reload", async ({ tab, state }) => {
    // #158: an offline edit and an open conflict, then the page is reloaded with A offline.
    await setOnline(tab, "a", false);
    await edit(tab, "a", { note: "from A, before the reload" });
    await waitText(tab, "a", "unsynced", 1);
    await tab.reload();
    await signIn(tab, ["a"]);
    await tab.click("#connect");
    await waitText(tab, "a", "events", state.count);
    await waitText(tab, "a", "state", "offline");
    await waitText(tab, "a", "unsynced", 1);
    for (const d of ["a", "b"]) await tab.waitForSelector(conflictRow(d));
    await setOnline(tab, "a", true);
    await waitText(tab, "a", "unsynced", 0);
    await until("B to hold A's edit", async () => (await localCopy(tab, "b")).note === "from A, before the reload");
  }],
  ["keep_resolves", async ({ tab, state }) => {
    const kept = (await localCopy(tab, "a", state.other)).note;
    await tab.click(`${conflictRow("a")} [data-keep="${state.other}"]`);
    await resolvedEverywhere(tab, kept);
  }],
  ["keep_resolves_two_losers", async ({ tab }) => {
    // #163: three versions made apart (Remote online, both devices offline).
    const losers = await makeConflict(tab, { a: "three: A", b: "three: B", remote: "three: Remote" }, 2);
    const kept = (await localCopy(tab, "a", losers[1])).note;
    await tab.click(`${conflictRow("a")} [data-keep="${losers[1]}"]`);
    await resolvedEverywhere(tab, kept);
  }],
  ["partial_keep_failure_is_reported", async ({ tab }) => {
    // #161: the content write lands, the deletion of the losing version fails.
    const [loser] = await makeConflict(tab, { a: "partial: A", b: "partial: B" }, 1);
    await tab.evaluate(() => {
      const db = devices.a.db;
      const real = db.bulkDocs;
      db.bulkDocs = async (docs) => {
        db.bulkDocs = real;
        const done = await real.call(db, docs.slice(0, 1));
        return [...done, ...docs.slice(1).map((d) => ({ id: d._id, rev: d._rev, error: true, message: "injected" }))];
      };
    });
    await tab.click(`${conflictRow("a")} [data-keep="winner"]`);
    await until("the partial resolution in the log", async () => (await logText(tab)).includes("partly resolved"));
    await until("the new revision to be pushed", async () => (await text(tab, "a", "unsynced")) === "0");
    await tab.waitForSelector(`${conflictRow("a")} [data-keep="${loser}"]`);
    await tab.click(`${conflictRow("a")} [data-keep="winner"]`);
    await resolvedEverywhere(tab, (await localCopy(tab, "a")).note);
  }],
  ["reset", async ({ tab }) => {
    await tab.evaluate(() => {
      window.__pulledAgain = new Set();
      const obs = new MutationObserver(() => {
        for (const d of ["a", "b"]) {
          const s = document.querySelector(`section[data-pane="${d}"] [data-role="state"]`);
          if (s.textContent.startsWith("pulling")) window.__pulledAgain.add(d);
        }
      });
      obs.observe(document.body, { subtree: true, childList: true, characterData: true });
    });
    // #154: the device switches are locked while the copies are deleted.
    const locked = await tab.evaluate(async () => {
      const done = resetDevices();
      const during = [el("a", "online").disabled, el("b", "online").disabled];
      await done;
      return { during, after: [el("a", "online").disabled, el("b", "online").disabled] };
    });
    if (locked.during.join() !== "true,true" || locked.after.join() !== "false,false") {
      throw new Error(`switch lock ${JSON.stringify(locked)}`);
    }
    await tab.click(sel("remote", "reload"));
    await waitText(tab, "remote", "state", "idle");
    const events = await text(tab, "remote", "events");
    const fatal = await text(tab, "remote", "fatal");
    await until("both devices to pull again", () => tab.evaluate(() => window.__pulledAgain.size === 2));
    for (const d of ["a", "b"]) {
      await waitText(tab, d, "events", events);
      await waitText(tab, d, "fatal", fatal);
      await waitText(tab, d, "unsynced", 0);
    }
    if ((await timelineSeen(tab, "a")) !== "") throw new Error("timeline not cleared");
  }],
];

async function main() {
  const server = await startPage();
  const browser = await chromium.launch();
  const failures = [];
  let tab;
  try {
    tab = await browser.newPage();
    tab.setDefaultTimeout(WAIT_MS);
    tab.on("console", (m) => console.log(`[page] ${m.text()}`));
    tab.on("pageerror", (e) => console.log(`[page error] ${e.message}`));
    await tab.goto(`http://127.0.0.1:${pagePort}/index.html`);
    await signIn(tab);
    await cutDeviceBDuringPull(tab);
    await tab.click("#connect");
    const ctx = { tab, sel, cluster, state: {} };
    // Later checks build on earlier ones, but a failure is reported and the
    // run goes on, so one run shows every check that fails.
    for (const [name, run] of CHECKS) {
      try {
        await run(ctx);
        console.log(`ok - ${name}`);
      } catch (e) {
        failures.push(`${name}: ${e.message.split("\n")[0]}`);
        console.log(`not ok - ${name}: ${e.message.split("\n")[0]}`);
      }
    }
  } catch (e) {
    failures.push(`setup: ${e.message}`);
  }
  if (failures.length) {
    const log = tab
      ? await tab.$$eval("#log li", (items) => items.map((li) => li.textContent).join("\n")).catch(() => "(no #log)")
      : "";
    console.error(`FAILED (${failures.length}):\n  ${failures.join("\n  ")}\npage log:\n${log.slice(0, 4000)}`);
  }
  await browser.close();
  server.close();
  if (failures.length) process.exit(1);
  console.log("offline explorer checks passed");
  process.exit(0);
}

main();

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
const WAIT_MS = 120000;
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
  ["conflict_same_winner", async ({ tab, state }) => {
    await until("B to hold A's last edit", async () => (await timelineSeen(tab, "b")) !== "");
    await setOnline(tab, "a", false);
    await setOnline(tab, "b", false);
    await edit(tab, "a", { note: "from A" });
    await edit(tab, "b", { note: "from B" });
    await setOnline(tab, "a", true);
    await setOnline(tab, "b", true);
    for (const d of ["a", "b"]) await tab.waitForSelector(conflictRow(d));
    const row = async (d) => [
      await tab.textContent(`${conflictRow(d)} [data-role="winner"]`),
      await tab.textContent(`${conflictRow(d)} [data-role="others"]`),
    ].map((t) => t.trim());
    await until("both devices to agree on the conflict", async () => {
      const [a, b] = [await row("a"), await row("b")];
      return a[0] === b[0] && a[1] === b[1] && a[1] !== "";
    });
    state.other = (await row("a"))[1];
  }],
  ["keep_resolves", async ({ tab, state }) => {
    if (state.other.includes(" ")) throw new Error(`expected one losing revision, got ${state.other}`);
    const kept = (await localCopy(tab, "a", state.other)).note;
    await tab.click(`${conflictRow("a")} [data-keep="${state.other}"]`);
    for (const d of ["a", "b"]) {
      await until(`${d} to drop the conflict`, async () => (await tab.$(conflictRow(d))) === null);
      await until(`${d} to hold the resolution`, async () => {
        const c = await localCopy(tab, d);
        return c.conflicts.length === 0 && c.note === kept;
      });
    }
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
    await tab.click("#reset");
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
  let failure = null;
  let tab;
  try {
    tab = await browser.newPage();
    tab.setDefaultTimeout(WAIT_MS);
    tab.on("console", (m) => console.log(`[page] ${m.text()}`));
    tab.on("pageerror", (e) => console.log(`[page error] ${e.message}`));
    await tab.goto(`http://127.0.0.1:${pagePort}/index.html`);
    await tab.fill("#node0", node0);
    await tab.fill("#node2", node2);
    await tab.fill("#user", user);
    await tab.fill("#password", pass);
    await cutDeviceBDuringPull(tab);
    await tab.click("#connect");
    const ctx = { tab, sel, cluster, state: {} };
    for (const [name, run] of CHECKS) {
      try {
        await run(ctx);
      } catch (e) {
        throw new Error(`${name}: ${e.message}`);
      }
      console.log(`ok - ${name}`);
    }
  } catch (e) {
    failure = e;
  }
  if (failure) {
    const log = tab ? await tab.textContent("#log").catch(() => "(no #log)") : "";
    console.error(`FAILED: ${failure.message}\npage log:\n${log}`);
  }
  await browser.close();
  server.close();
  if (failure) process.exit(1);
  console.log("offline explorer checks passed");
  process.exit(0);
}

main();

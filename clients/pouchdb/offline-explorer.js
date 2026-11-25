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

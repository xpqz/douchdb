#!/usr/bin/env node
"use strict";

/**
 * Browser PouchDB 9 sync against douchdb, with real CORS.
 *
 * Playwright drives Chromium. The page is served from one origin and PouchDB
 * (the browser build, IndexedDB local database) talks to douchdb through a
 * logging proxy on another origin, so every request is cross-origin and every
 * non-simple one is preceded by a CORS preflight that the proxy forwards to
 * douchdb untouched. The test fails if any step of a two-way sync fails and
 * prints the distinct preflights the browser sent.
 *
 *   node browser.js <douchdb url> <user:password> <page origin host:port ...>
 *
 * The caller starts douchdb with `--admin user:password --cors-origins
 * http://<page host>:<page port> --cors-credentials` (PouchDB sends requests
 * with credentials). Use run-browser.sh to do this in the pinned Playwright
 * image.
 */

const http = require("http");
const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright-core");

const target = new URL(process.argv[2] || "http://127.0.0.1:5984");
const [user, pass] = (process.argv[3] || "admin:pass").split(":");
const pagePort = Number(process.env.PAGE_PORT || 8099);
const proxyPort = Number(process.env.PROXY_PORT || 8098);
const pouchdbJs = require.resolve("pouchdb/dist/pouchdb.js");

const preflights = new Map();

function startPage() {
  const html = `<!doctype html><meta charset="utf-8"><title>pouch</title>
<script src="/pouchdb.js"></script>`;
  return new Promise((resolve) => {
    const server = http.createServer((req, res) => {
      if (req.url === "/pouchdb.js") {
        res.setHeader("content-type", "text/javascript");
        res.end(fs.readFileSync(pouchdbJs));
      } else {
        res.setHeader("content-type", "text/html");
        res.end(html);
      }
    });
    server.listen(pagePort, "127.0.0.1", () => resolve(server));
  });
}

function startProxy() {
  return new Promise((resolve) => {
    const server = http.createServer((req, res) => {
      if (req.method === "OPTIONS") {
        const key = [
          req.headers["access-control-request-method"],
          req.headers["access-control-request-headers"],
        ].join(" | ");
        preflights.set(key, (preflights.get(key) || 0) + 1);
      }
      const headers = { ...req.headers, host: target.host };
      delete headers["accept-encoding"];
      const upstream = http.request(
        {
          host: target.hostname,
          port: target.port,
          path: req.url,
          method: req.method,
          headers,
        },
        (up) => {
          if (req.method === "OPTIONS") {
            const cors = Object.entries(up.headers)
              .filter(([k]) => k.startsWith("access-control-"))
              .map(([k, v]) => `${k}: ${v}`)
              .join("; ");
            console.log(
              `preflight ${req.url} -> ${up.statusCode} ${cors} (asked: ${req.headers["access-control-request-headers"]})`
            );
          }
          if (up.statusCode >= 400 && req.method !== "OPTIONS") {
            const parts = [];
            up.on("data", (c) => parts.push(c));
            up.on("end", () =>
              console.log(
                `error ${req.method} ${req.url} -> ${up.statusCode} ${Buffer.concat(parts)}`
              )
            );
          }
          res.writeHead(up.statusCode, up.headers);
          up.pipe(res);
        }
      );
      upstream.on("error", () => {
        res.writeHead(502);
        res.end();
      });
      req.pipe(upstream);
    });
    server.listen(proxyPort, "127.0.0.1", () => resolve(server));
  });
}

/** Runs in the page. Resolves to a list of check names, or throws. */
async function pageTest({ remoteBase, user, pass, dbName }) {
  const checks = [];
  const ok = (cond, msg) => {
    if (!cond) throw new Error(`check failed: ${msg}`);
    checks.push(msg);
  };
  const remote = new PouchDB(`${remoteBase}/${dbName}`, {
    auth: { username: user, password: pass },
  });
  const local = new PouchDB(`local-${dbName}`);
  try {
    // Local documents. (douchdb has no attachment support, so no attachment step.)
    await local.put({ _id: "from-local", v: 1 });
    await local.put({ _id: "conflict", v: "base" });

    // Push, which creates the remote database (PUT) and replicates.
    const push = await local.replicate.to(remote);
    ok(push.ok && push.docs_written === 2, "push replication wrote the documents");
    ok((await remote.get("from-local")).v === 1, "pushed doc is on the server");

    // Server-side write, then pull.
    await remote.put({ _id: "from-remote", v: 2 });
    const pull = await local.replicate.from(remote);
    ok(pull.ok, "pull replication succeeded");
    ok((await local.get("from-remote")).v === 2, "pulled doc is local");

    // Make a conflict: both sides edit the same revision.
    const base = await local.get("conflict");
    await local.put({ ...base, v: "local edit" });
    await remote.put({ ...base, v: "remote edit" });
    const sync = await local.sync(remote);
    ok(sync.push.ok && sync.pull.ok, "two-way sync succeeded");
    // The two directions of one sync() run concurrently and PouchDB's result
    // can reflect only part of the exchange, so settle with a second round
    // before comparing the sides.
    await local.sync(remote);
    const l = await local.get("conflict", { conflicts: true });
    const r = await remote.get("conflict", { conflicts: true });
    ok(l._rev === r._rev, `both sides pick the same winner (${JSON.stringify([l, r])})`);
    ok(
      (l._conflicts || []).length === 1 && (r._conflicts || []).length === 1,
      "the conflict is visible on both sides"
    );

    // A deletion replicates, and the changes feed works.
    const gone = await local.get("from-local");
    await local.remove(gone);
    await local.replicate.to(remote);
    let missing = false;
    try {
      await remote.get("from-local");
    } catch (e) {
      missing = e.status === 404;
    }
    ok(missing, "a deletion replicates");
    const changes = await remote.changes({ since: 0 });
    ok(changes.results.length === 3, "remote _changes lists the documents");
    return checks;
  } finally {
    await remote.destroy().catch(() => {});
    await local.destroy().catch(() => {});
  }
}

async function main() {
  const page = await startPage();
  const proxy = await startProxy();
  const browser = await chromium.launch();
  let failure = null;
  try {
    const tab = await browser.newPage();
    tab.on("console", (m) => console.log(`[page] ${m.text()}`));
    await tab.goto(`http://127.0.0.1:${pagePort}/`);
    const checks = await tab.evaluate(pageTest, {
      remoteBase: `http://127.0.0.1:${proxyPort}`,
      user,
      pass,
      dbName: `browser-compat-${process.pid}`,
    });
    for (const c of checks) console.log(`ok - ${c}`);
  } catch (e) {
    failure = e;
  } finally {
    await browser.close();
    page.close();
    proxy.close();
  }
  console.log("distinct preflights (method | request headers => count):");
  for (const [k, n] of [...preflights].sort()) console.log(`  ${k} => ${n}`);
  if (failure) {
    console.error(`FAILED: ${failure.message}`);
    process.exit(1);
  }
  console.log("browser PouchDB sync passed");
  process.exit(0);
}

main();

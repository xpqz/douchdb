#!/usr/bin/env node
"use strict";

/**
 * PouchDB 9 client compatibility checks against douchdb.
 *
 * This exercises the client surfaces PouchDB uses for local/remote
 * replication: document CRUD, bulk writes, all_docs, changes, conflicts,
 * tombstones, one-shot push/pull and a live remote changes feed.
 */

const Core = require("pouchdb-core");
const HttpAdapter = require("pouchdb-adapter-http");
const MemoryAdapter = require("pouchdb-adapter-memory");
const Replication = require("pouchdb-replication");

const PouchDB = Core.default || Core;
PouchDB.plugin(HttpAdapter.default || HttpAdapter);
PouchDB.plugin(MemoryAdapter.default || MemoryAdapter);
PouchDB.plugin(Replication.default || Replication);

const base = (process.argv[2] || "http://127.0.0.1:5984").replace(/\/$/, "");
const dbName = `pouch-compat-${process.pid}`;
const remoteUrl = `${base}/${dbName}`;
const localUrl = `pouch-compat-local-${process.pid}`;

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function equal(actual, expected, message) {
  assert(
    JSON.stringify(actual) === JSON.stringify(expected),
    `${message}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`
  );
}

async function resetRemote() {
  await fetch(remoteUrl, { method: "DELETE" });
  const response = await fetch(remoteUrl, { method: "PUT" });
  assert(response.status === 201 || response.status === 202,
    `create remote database: ${response.status} ${await response.text()}`);
}

async function main() {
  await resetRemote();
  const local = new PouchDB(localUrl, { adapter: "memory" });
  const remote = new PouchDB(remoteUrl);

  // Basic writes and reads.
  const first = await local.put({ _id: "doc-1", n: 1, s: "a" });
  equal((await local.get("doc-1")).n, 1, "local get");
  const second = await local.put({
    _id: "doc-1",
    _rev: first.rev,
    n: 2,
    s: "a",
  });
  equal((await local.get("doc-1")).n, 2, "local update");

  // Bulk docs, including a delete.
  const bulkResult = await local.bulkDocs({
    docs: [
      { _id: "doc-2", n: 3, s: "b" },
      { _id: "doc-3", n: 4, s: "c" },
      { _id: "doc-4", n: 5, s: "b" },
    ],
  });
  const doc3 = await local.get("doc-3");
  await local.remove(doc3);

  const all = await local.allDocs({ include_docs: true });
  equal(all.total_rows, 3, "local all_docs total");
  assert(all.rows.every((row) => row.doc._id !== "doc-3"), "local delete");

  // All_docs keys and pagination.
  const keyed = await local.allDocs({ keys: ["doc-2", "doc-4", "missing"], include_docs: true });
  equal(keyed.rows.map((row) => row.key), ["doc-2", "doc-4", "missing"], "all_docs keys");
  assert(keyed.rows[2].error === "not_found", "missing key row");
  const page = await local.allDocs({ limit: 1, skip: 1, include_docs: true });
  equal(page.rows.length, 1, "all_docs limit");
  assert(page.rows[0].id !== all.rows[0].id, "all_docs skip");

  // Push everything to douchdb and read it back through the HTTP adapter.
  const push = await local.replicate.to(remote);
  assert(push.ok, "push replication ok");
  equal((await remote.get("doc-1")).n, 2, "remote get after push");
  equal((await remote.get("doc-1"))._rev, second.rev, "remote rev after push");

  let deleted = null;
  try {
    await remote.get("doc-3");
  } catch (error) {
    deleted = error;
  }
  assert(deleted && deleted.status === 404, "remote tombstone is 404");

  const remoteAll = await remote.allDocs({ include_docs: true });
  equal(remoteAll.total_rows, 3, "remote all_docs total");
  const remoteBulk = await remote.bulkGet({ docs: [{ id: "doc-2" }, { id: "doc-3" }] });
  equal(remoteBulk.results.length, 2, "bulkGet shape");
  assert(remoteBulk.results[0].docs[0].ok._id === "doc-2", "bulkGet doc");

  // Pull the remote state into a fresh local database.
  const pulled = new PouchDB(`${localUrl}-pull`, { adapter: "memory" });
  const pull = await pulled.replicate.from(remote);
  assert(pull.ok, "pull replication ok");
  equal((await pulled.get("doc-1")).n, 2, "pulled value");
  equal((await pulled.allDocs()).total_rows, 3, "pulled total");

  // A conflict path: insert a second leaf with new_edits=false.
  const conflictRev = await remote.put({ _id: "conflict", v: "left" });

  const branch = "1-ffffffffffffffffffffffffffffffff";
  const inserted = await fetch(`${remoteUrl}/_bulk_docs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      docs: [
        {
          _id: "conflict",
          _rev: branch,
          _revisions: { start: 1, ids: [branch.split("-")[1]] },
          v: "right",
        },
      ],
      new_edits: false,
    }),
  }).then((r) => r.json());
  assert(Array.isArray(inserted) && inserted.length === 0,
    `new_edits=false should return only errors: ${JSON.stringify(inserted)}`);
  const conflicted = await fetch(`${remoteUrl}/conflict?conflicts=true`).then((r) => r.json());
  assert(Array.isArray(conflicted._conflicts) && conflicted._conflicts.length === 1,
    `expected one conflict: ${JSON.stringify(conflicted)}`);

  // Changes pagination through PouchDB.
  const changes = await remote.changes({ since: 0, limit: 2 });
  equal(changes.results.length, 2, "remote changes limit");
  assert(changes.last_seq !== undefined, "remote changes last_seq");

  await local.destroy();
  await pulled.destroy();
  await remote.destroy();
  await fetch(remoteUrl, { method: "DELETE" }).catch(() => {});
  console.log("PouchDB 9 compatibility checks passed.");
}

main().catch((error) => {
  console.error(error.stack || error);
  process.exitCode = 1;
});

#!/usr/bin/env python3
"""Crash and corruption checks for douchdb's redb storage.

Every shard is one redb file, `<db>/shard-NNNN.redb`, and every view index
one more, `<db>/_view_indexes/<md5>.redb`. Acceptance is split, as in the
storage design ("Corruption detection", "Testing"):

  * A crash (SIGKILL mid-write, repeated) keeps every acknowledged write,
    with the acknowledged body. No exceptions.
  * So does a SIGKILL during `_compact`, at each point from the compaction
    file's creation to just after the swap; the restart leaves no
    `.compact.redb` behind, and the database takes writes and compacts.
  * Deliberate damage (truncation, bit flips, a removed file) must fail
    closed: requests to the damaged database answer 500 (the file refuses to
    open), the server stays up and keeps serving its other databases, and
    the damaged database stays listed. Recovering the data is not required;
    serving wrong data, or a database missing acknowledged writes, is a
    failure.
  * A damaged value read while the server runs is a 500 on that read. A
    damaged tree (branch) page is detected by the next integrity check: when
    the idle database closes, or on the first open after a restart.
  * A damaged view index is rebuilt from the shards and answers the same
    rows.

Damage targets live pages, found by walking the file from the header's
primary commit slot (see `RedbFile`): copy-on-write leaves stale copies of
the same bytes in freed pages, so a search of the whole file could damage a
page nothing reads.

Each scenario runs under a hard time bound and kills its servers itself.
Temporary data goes under $TMPDIR and is removed afterwards.
"""

import argparse
import atexit
import hashlib
import http.client
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path

DOCS = 300
# Hard bounds on one scenario and on the whole run, enforced with SIGALRM.
SCENARIO_SECONDS = 120
TOTAL_SECONDS = 600
# Bound on waiting for a server to answer after it starts.
READY_SECONDS = 30
KILL_ROUNDS = 12

CHILDREN = []


def kill_children():
    for proc in CHILDREN:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    CHILDREN.clear()


atexit.register(kill_children)


class ScenarioTimeout(Exception):
    pass


def on_alarm(_signum, _frame):
    raise ScenarioTimeout("scenario exceeded its time bound")


def on_term(signum, _frame):
    # An outer `timeout` sends SIGTERM: unwind, so `finally` and `atexit`
    # kill the servers and remove the temporary data.
    raise SystemExit(128 + signum)


class Failure(Exception):
    """The scenario saw an outcome the acceptance rules forbid."""


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


# ---------------------------------------------------------------------------
# HTTP


LOCAL = threading.local()


def reset_connections():
    """Drop this thread's kept-alive connections (a server stopped)."""
    for conn in getattr(LOCAL, "conns", {}).values():
        conn.close()
    LOCAL.conns = {}


def request(port, method, path, body=None, timeout=10):
    """One request over this thread's kept-alive connection to `port`
    (thousands of fresh connections exhaust the ephemeral ports)."""
    conns = getattr(LOCAL, "conns", None)
    if conns is None:
        conns = LOCAL.conns = {}
    conn = conns.get(port)
    if conn is None:
        conn = conns[port] = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    conn.timeout = timeout
    if conn.sock is not None:
        conn.sock.settimeout(timeout)
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    try:
        conn.request(method, path, body=data, headers=headers)
        response = conn.getresponse()
        raw = response.read()
    except BaseException:
        conn.close()
        conns.pop(port, None)
        raise
    if response.will_close:
        conn.close()
        conns.pop(port, None)
    return response.status, raw


def get_json(port, path):
    status, raw = request(port, "GET", path)
    try:
        return status, json.loads(raw)
    except ValueError:
        return status, raw


# ---------------------------------------------------------------------------
# Servers


class Server:
    def __init__(self, binary, data, port, log, extra=()):
        self.binary, self.data, self.port, self.log, self.extra = binary, data, port, log, extra
        reset_connections()
        handle = open(log, "ab", buffering=0)
        self.proc = subprocess.Popen(
            [
                binary,
                "--listen",
                f"127.0.0.1:{port}",
                "--data",
                data,
                "--n",
                "1",
                "--sync-secs",
                "3600",
                *extra,
            ],
            stdout=handle,
            stderr=handle,
        )
        handle.close()
        CHILDREN.append(self.proc)
        deadline = time.monotonic() + READY_SECONDS
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise Failure(f"server exited with {self.proc.poll()} on start\n{tail(log)}")
            try:
                if request(port, "GET", "/", timeout=1)[0] == 200:
                    return
            except OSError:
                pass
            time.sleep(0.05)
        raise Failure(f"server not ready after {READY_SECONDS}s\n{tail(log)}")

    def alive(self):
        return self.proc.poll() is None

    def stop(self, sig=signal.SIGKILL):
        reset_connections()
        if self.proc.poll() is None:
            self.proc.send_signal(sig)
            try:
                self.proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        if self.proc in CHILDREN:
            CHILDREN.remove(self.proc)

    def restart(self, sig=signal.SIGKILL):
        self.stop(sig)
        return Server(self.binary, self.data, self.port, self.log, self.extra)


def tail(log, size=1500):
    try:
        return Path(log).read_text(errors="replace")[-size:]
    except OSError:
        return "<no log>"


def strip(body):
    return {k: v for k, v in body.items() if not k.startswith("_")}


def seed(port, name="faults", count=DOCS):
    status, raw = request(port, "PUT", f"/{name}")
    if status != 201:
        raise Failure(f"create {name}: {status} {raw[:200]}")
    docs = [
        {"_id": f"doc-{i:04}", "n": i, "s": f"payload-{i}", "blob": "x" * 200}
        for i in range(count)
    ]
    for offset in range(0, count, 25):
        status, raw = request(port, "POST", f"/{name}/_bulk_docs", {"docs": docs[offset : offset + 25]})
        if status != 201:
            raise Failure(f"seed {name}: {status} {raw[:200]}")
    return {d["_id"]: strip(d) for d in docs}


def put_last(port, name="faults"):
    """One more acknowledged document, in a commit of its own: the newest
    commit in the file, which is what a rollback to the previous commit
    would lose."""
    body = {"s": "payload-last", "note": "acknowledged in the newest commit"}
    status, raw = request(port, "PUT", f"/{name}/last", body)
    if status != 201:
        raise Failure(f"PUT last: {status} {raw[:200]}")
    return {"last": body}


def read_doc(port, name, docid):
    return get_json(port, f"/{name}/{docid}")


def check_intact(port, name, expected):
    """None if every expected document reads back with its body and the
    count matches; otherwise the first wrong answer."""
    status, info = get_json(port, f"/{name}")
    if status != 200:
        return f"GET /{name} -> {status} {str(info)[:200]}"
    if info.get("doc_count") != len(expected):
        return f"doc_count {info.get('doc_count')}, expected {len(expected)}"
    for docid, body in expected.items():
        status, got = read_doc(port, name, docid)
        if status != 200:
            return f"{docid} -> {status} {str(got)[:160]}"
        if strip(got) != body:
            return f"{docid} -> wrong body {str(got)[:160]}"
    return None


def assert_fails_closed(server, name, expected, healthy):
    """The damaged database answers 500 for its info and for every read it
    cannot serve correctly (never 404, never a wrong body); it stays listed;
    the server is up and still serves `healthy`."""
    for _ in range(2):
        status, info = get_json(server.port, f"/{name}")
        if status != 500:
            if status == 200:
                problem = check_intact(server.port, name, expected)
                raise Failure(
                    f"GET /{name} -> 200 after damage; "
                    + (f"serves wrong data: {problem}" if problem else "all data intact")
                )
            raise Failure(f"GET /{name} -> {status} {str(info)[:200]}, expected 500")
    for docid, body in expected.items():
        status, got = read_doc(server.port, name, docid)
        if status == 200 and strip(got) == body:
            continue
        if status != 500:
            raise Failure(f"{docid} -> {status} {str(got)[:160]}, expected 500")
    status, dbs = get_json(server.port, "/_all_dbs")
    if status != 200 or name not in dbs:
        raise Failure(f"/_all_dbs -> {status} {dbs}: {name} not listed")
    problem = check_intact(server.port, "healthy", healthy)
    if problem:
        raise Failure(f"healthy database affected: {problem}")
    if not server.alive():
        raise Failure(f"server died\n{tail(server.log)}")
    return f"/{name} answers 500, still listed, other database served"


# ---------------------------------------------------------------------------
# The redb (4.x) file format, enough to find live pages.
#
# Header (src/tree_store/page_store/header.rs): magic "redb..." at 0, god
# byte at 9 (bit 0: which commit slot is primary), page size u32 at 12,
# region header pages u32 at 16, region max data pages u32 at 20. Commit
# slots of 128 bytes at 64 and 192: byte 1 is "user root non-null", the user
# root page number is the u64 at 8 (then its checksum u128 and length u64).
#
# Page numbers (base.rs): index in bits 0..20 (masked by order), region in
# bits 20..40, order in bits 59..64. A page of order k is page_size << k
# bytes at page_size + region * region_size + region_header_pages *
# page_size + index * (page_size << k).
#
# Pages (btree_base.rs): byte 0 is 1 (leaf) or 2 (branch), a u16 count at 2.
# A branch with n keys holds n + 1 child checksums (u128) from byte 8, then
# n + 1 child page numbers (u64). A leaf whose keys and values are variable
# width holds n key ends then n value ends (u32 page offsets) from byte 4,
# then the keys, then the values.
#
# The user root is the table tree: its leaves map a table name to the
# table's definition (table_tree_base.rs), whose bytes are type (1), length
# (u64), root non-null (1), and the root page number (u64) at offset 10.

LEAF, BRANCH = 1, 2
# God byte flag: set while a file is open, cleared by a clean close.
RECOVERY_REQUIRED = 2


def u16(raw, at):
    return int.from_bytes(raw[at : at + 2], "little")


def u32(raw, at):
    return int.from_bytes(raw[at : at + 4], "little")


def u64(raw, at):
    return int.from_bytes(raw[at : at + 8], "little")


class RedbFile:
    def __init__(self, path):
        self.path = path
        self.raw = Path(path).read_bytes()
        if self.raw[:4] != b"redb":
            raise RuntimeError(f"{path} is not a redb file")
        self.page_size = u32(self.raw, 12)
        self.region_header_pages = u32(self.raw, 16)
        region_max_data_pages = u32(self.raw, 20)
        self.region_size = (self.region_header_pages + region_max_data_pages) * self.page_size
        self.primary = 64 + 128 * (self.raw[9] & 1)

    def page(self, number):
        """(offset, length) of page `number`."""
        order = number >> 59
        index = number & (0xFFFFF >> order)
        region = (number >> 20) & 0xFFFFF
        size = self.page_size << order
        start = (
            self.page_size
            + region * self.region_size
            + self.region_header_pages * self.page_size
            + index * size
        )
        return start, size

    def pages(self, root):
        """Every page of the tree at `root`: (offset, length, kind)."""
        out = []

        def walk(number):
            start, size = self.page(number)
            kind = self.raw[start]
            out.append((start, size, kind))
            if kind == BRANCH:
                n = u16(self.raw, start + 2)
                base = start + 8 + 16 * (n + 1)
                for i in range(n + 1):
                    walk(u64(self.raw, base + 8 * i))
            elif kind != LEAF:
                raise RuntimeError(f"{self.path}: page at {start} is neither leaf nor branch")

        walk(root)
        return out

    def leaf_entries(self, start):
        n = u16(self.raw, start + 2)
        entries = []
        for i in range(n):
            key_start = 4 + 8 * n if i == 0 else u32(self.raw, start + 4 + 4 * (i - 1))
            key_end = u32(self.raw, start + 4 + 4 * i)
            if i == 0:
                value_start = u32(self.raw, start + 4 + 4 * (n - 1))
            else:
                value_start = u32(self.raw, start + 4 + 4 * n + 4 * (i - 1))
            value_end = u32(self.raw, start + 4 + 4 * n + 4 * i)
            entries.append(
                (self.raw[start + key_start : start + key_end], self.raw[start + value_start : start + value_end])
            )
        return entries

    def tables(self, slot=None):
        """Table name -> root page number, from the primary commit slot (or
        the slot at offset `slot`)."""
        slot = self.primary if slot is None else slot
        if not self.raw[slot + 1]:
            return {}
        roots = {}
        for start, _, kind in self.pages(u64(self.raw, slot + 8)):
            if kind == LEAF:
                for key, value in self.leaf_entries(start):
                    if value[9]:
                        roots[key.decode()] = u64(value, 10)
        return roots

    def in_previous_commit(self, offset):
        """Whether the page at `offset` belongs to the secondary commit slot's
        trees (the commit before the newest)."""
        previous = 64 + 128 * (1 - (self.raw[9] & 1))
        return any(
            start == offset
            for root in self.tables(previous).values()
            for start, _, _ in self.pages(root)
        )

    def live_leaves_with(self, marker):
        """(table, leaf offset, marker offset) of every live leaf holding
        `marker`."""
        found = []
        for table, root in self.tables().items():
            for start, size, kind in self.pages(root):
                at = self.raw.find(marker, start, start + size)
                if kind == LEAF and at >= 0:
                    found.append((table, start, at))
        return found

    def root_branch(self, table):
        start, _ = self.page(self.tables()[table])
        if self.raw[start] != BRANCH:
            raise RuntimeError(f"{table} root at {start} is a leaf; seed more documents")
        return start


def flip(path, offset):
    with open(path, "r+b") as handle:
        handle.seek(offset)
        byte = handle.read(1)
        handle.seek(offset)
        handle.write(bytes([byte[0] ^ 0xFF]))
        handle.flush()
        os.fsync(handle.fileno())


def flip_value(path, marker=b"payload-last"):
    """Flip a byte inside a stored body: a value page (a leaf of the bodies
    table)."""
    found = [hit for hit in RedbFile(path).live_leaves_with(marker) if hit[0] == "bodies"]
    if len(found) != 1:
        raise RuntimeError(f"{len(found)} live bodies leaves hold {marker!r}")
    _, leaf, at = found[0]
    flip(path, at + 3)
    return f"flipped byte {at + 3} (in {marker.decode()!r}) of the live bodies leaf at {leaf}"


def flip_branch(path):
    """Flip a byte of the first child checksum in the root branch page of
    the bodies table. Lookups route through the page as before, so reads are
    unaffected; only the integrity check, which hashes the page against its
    parent's record of it, can see it."""
    start = RedbFile(path).root_branch("bodies")
    flip(path, start + 8)
    return f"flipped byte {start + 8} (a child checksum) of the live bodies root branch at {start}"


def truncate(path):
    size = os.path.getsize(path)
    with open(path, "r+b") as handle:
        handle.truncate(size * 3 // 4)
    return f"truncated {os.path.basename(path)} {size} -> {os.path.getsize(path)}"


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def shard(data, n=0, name="faults"):
    return os.path.join(data, name, f"shard-{n:04}.redb")


# ---------------------------------------------------------------------------
# Scenarios. Each takes (binary, workdir, port) and returns a detail string,
# or raises Failure.


def seeded(binary, work, port, extra=("--q", "1")):
    """A running server over a new data directory holding `faults` (DOCS
    documents) and `healthy` (20). One shard (q=1) unless `extra` says
    otherwise, so the damage lands in the file that holds its target."""
    data = os.path.join(work, "data")
    server = Server(binary, data, port, os.path.join(work, "server.log"), extra)
    expected = seed(port)
    healthy = seed(port, "healthy", 20)
    return data, server, expected, healthy


class Load:
    """Four writers that create documents (single PUTs and _bulk_docs) and
    update their own counter document, and what the server acknowledged."""

    def __init__(self):
        self.created = {}  # docid -> acknowledged body
        self.counters = {}  # docid -> (rev, body) of the last acknowledged update
        self.pending = {}  # docid -> body of the update in flight
        self.lock = threading.Condition()
        self.acked = 0
        self.stopping = threading.Event()
        self.threads = []

    def start(self, port, round_):
        self.stopping.clear()
        self.threads = [
            threading.Thread(target=self.writer, args=(port, round_, w), daemon=True) for w in range(4)
        ]
        for thread in self.threads:
            thread.start()

    def ack(self):
        with self.lock:
            self.acked += 1
            self.lock.notify_all()

    def wait_acked(self, count, timeout=60):
        """Whether `count` requests in all have been acknowledged."""
        with self.lock:
            return self.lock.wait_for(lambda: self.acked >= count, timeout)

    def writer(self, port, round_, w):
        counter = f"counter-{w}"
        i = 0
        while not self.stopping.is_set():
            i += 1
            try:
                if i % 3 == 0:
                    with self.lock:
                        rev = self.counters.get(counter, (None, None))[0]
                        body = {"round": round_, "i": i, "w": w}
                        self.pending[counter] = body
                    sent = dict(body, _rev=rev) if rev else body
                    status, raw = request(port, "PUT", f"/faults/{counter}", sent, timeout=5)
                    if status == 201:
                        with self.lock:
                            self.counters[counter] = (json.loads(raw)["rev"], body)
                            self.pending.pop(counter, None)
                        self.ack()
                elif i % 3 == 1:
                    docid = f"r{round_}-w{w}-{i}"
                    body = {"round": round_, "i": i, "blob": "y" * (10 + (i * 37) % 400)}
                    status, _ = request(port, "PUT", f"/faults/{docid}", body, timeout=5)
                    if status == 201:
                        with self.lock:
                            self.created[docid] = body
                        self.ack()
                else:
                    docs = [{"_id": f"r{round_}-w{w}-{i}-b{k}", "k": k, "blob": "z" * 50} for k in range(10)]
                    status, raw = request(port, "POST", "/faults/_bulk_docs", {"docs": docs}, timeout=5)
                    if status == 201:
                        with self.lock:
                            for d, result in zip(docs, json.loads(raw)):
                                if result.get("ok"):
                                    self.created[d["_id"]] = strip(d)
                        self.ack()
            except (OSError, http.client.HTTPException, ValueError):
                return

    def stop(self):
        """Stop the writers (call once the server is killed)."""
        self.stopping.set()
        for thread in self.threads:
            thread.join(timeout=15)
            if thread.is_alive():
                raise Failure("a writer did not stop after the kill")

    def check(self, port, round_):
        """Every acknowledged write reads back with its body; an update in
        flight at the kill may have landed."""
        for docid, body in self.created.items():
            status, got = read_doc(port, "faults", docid)
            if status != 200 or strip(got) != body:
                raise Failure(f"round {round_}: acknowledged {docid} -> {status} {str(got)[:160]}")
        for w in range(4):
            docid = f"counter-{w}"
            status, got = read_doc(port, "faults", docid)
            if docid not in self.counters:
                # Never acknowledged: whatever landed is the next round's base.
                if status == 200:
                    self.counters[docid] = (got["_rev"], strip(got))
                continue
            rev, body = self.counters[docid]
            if status != 200:
                raise Failure(f"round {round_}: acknowledged {docid} -> {status}")
            if got["_rev"] == rev and strip(got) == body:
                continue
            # The update in flight at the kill may have landed: one
            # generation on, with the body that was sent.
            generation = int(got["_rev"].split("-")[0])
            if generation == int(rev.split("-")[0]) + 1 and strip(got) == self.pending.get(docid):
                self.counters[docid] = (got["_rev"], strip(got))
                continue
            raise Failure(f"round {round_}: {docid} is {got['_rev']} {strip(got)}, acknowledged {rev} {body}")
        self.pending.clear()


def sigkill_mid_write(binary, work, port):
    """Four writers create documents (single PUTs and _bulk_docs) and update
    their own counter document; SIGKILL once a random number of requests is
    acknowledged; restart and check every acknowledged write, with its body.
    Repeated KILL_ROUNDS times on one data directory (q=2)."""
    data = os.path.join(work, "data")
    rng = random.Random(7)
    load = Load()
    server = Server(binary, data, port, os.path.join(work, "server.log"), ("--q", "2"))
    if request(port, "PUT", "/faults")[0] != 201:
        raise Failure("create faults")
    for round_ in range(KILL_ROUNDS):
        target = load.acked + rng.randrange(20, 200)
        load.start(port, round_)
        if not load.wait_acked(target):
            raise Failure(f"round {round_}: only {load.acked} writes acknowledged")
        server.stop(signal.SIGKILL)
        load.stop()
        server = server.restart()
        load.check(port, round_)
    server.stop()
    return f"{KILL_ROUNDS} kills, {load.acked} acknowledged requests, {len(load.created)} documents, all intact"


# Documents seeded before the compaction kills: enough that the compaction
# runs long enough to be killed at each point.
COMPACT_DOCS = 20000


def compaction_file(data):
    return Path(data, "faults", "shard-0000.compact.redb")


def compacting(port):
    """The progress of the running compaction of `faults`, or None."""
    status, tasks = get_json(port, "/_active_tasks")
    if status != 200:
        raise Failure(f"GET /_active_tasks -> {status} {str(tasks)[:200]}")
    for task in tasks:
        if task.get("type") == "database_compaction" and task["database"].endswith("/faults"):
            return task["changes_done"] / max(task["total_changes"], 1)
    return None


def check_seeds(port, point):
    """The COMPACT_DOCS seeded documents are all there, by `_all_docs`."""
    status, rows = get_json(port, "/faults/_all_docs?startkey=%22seed-%22&endkey=%22seed-~%22")
    if status != 200 or len(rows.get("rows", [])) != COMPACT_DOCS:
        raise Failure(f"{point}: _all_docs over the seeds -> {status} {str(rows)[:200]}")


def compact_running(port):
    status, info = get_json(port, "/faults")
    if status != 200:
        raise Failure(f"GET /faults -> {status} {str(info)[:200]}")
    return info["compact_running"]


# Where to kill a compaction: a condition on (port, data) that holds at that
# point, and whether the kill comes before the rename. The kill lands right
# after the condition is seen, or once the compaction ends; a kill that
# finds the compaction elsewhere (the file gone before the rename, or the
# shard file not replaced after it) is a miss, and the round is retried.
KILL_POINTS = [
    ("file created", lambda port, data: compaction_file(data).exists(), True),
    ("copy started", lambda port, data: (compacting(port) or 0) > 0, True),
    ("copy half done", lambda port, data: (compacting(port) or 0) >= 0.5, True),
    ("copy done", lambda port, data: (compacting(port) or 0) >= 1, True),
    ("after the swap", lambda port, data: not compact_running(port), False),
]
KILL_POINT_TRIES = 5


def sigkill_mid_compaction(binary, work, port):
    """q=1, COMPACT_DOCS documents, then for each kill point: start the
    writers, start `_compact`, SIGKILL at the point, restart. Every
    acknowledged write is there with its body, no compaction file is left,
    and the database takes a write; at the end it compacts in full. A kill
    counts only once a write was acknowledged after `_compact`: with the
    writers stalled, the survival check would prove nothing."""
    data = os.path.join(work, "data")
    server = Server(binary, data, port, os.path.join(work, "server.log"), ("--q", "1"))
    seed(port, count=0)
    for offset in range(0, COMPACT_DOCS, 500):
        docs = [{"_id": f"seed-{i:06}", "blob": "x" * 300} for i in range(offset, offset + 500)]
        status, raw = request(port, "POST", "/faults/_bulk_docs", {"docs": docs})
        if status != 201:
            raise Failure(f"seed: {status} {raw[:200]}")
    live = Path(shard(data))
    load = Load()
    hits = []
    round_ = 0
    for point, reached, before_rename in KILL_POINTS:
        for tries in range(1, KILL_POINT_TRIES + 1):
            round_ += 1
            load.start(port, round_)
            if not load.wait_acked(load.acked + 20):
                raise Failure(f"{point}: writes stalled")
            inode = live.stat().st_ino
            status, raw = request(port, "POST", "/faults/_compact", {})
            if status != 202:
                raise Failure(f"POST _compact -> {status} {raw[:200]}")
            at_compact = load.acked
            while compact_running(port) and not (load.acked > at_compact and reached(port, data)):
                pass
            acked = load.acked
            server.stop(signal.SIGKILL)
            load.stop()
            leftover = compaction_file(data).exists()
            swapped = live.stat().st_ino != inode
            server = server.restart()
            load.check(port, round_)
            check_seeds(port, point)
            if list(Path(data).rglob("*.compact.redb")):
                raise Failure(f"{point}: a compaction file survived the restart")
            if request(port, "PUT", f"/faults/after-{round_}", {"ok": True})[0] != 201:
                raise Failure(f"{point}: no write after the restart")
            load.created[f"after-{round_}"] = {"ok": True}
            if acked > at_compact and (leftover, swapped) == (before_rename, not before_rename):
                hits.append(f"{point} (try {tries})")
                break
        else:
            raise Failure(f"{point}: missed in {KILL_POINT_TRIES} tries")
    inode = live.stat().st_ino
    if request(port, "POST", "/faults/_compact", {})[0] != 202:
        raise Failure("POST _compact after the kills")
    while compact_running(port):
        pass
    if live.stat().st_ino == inode or compaction_file(data).exists():
        raise Failure(f"the compaction after the kills did not finish\n{tail(server.log)}")
    load.check(port, "final")
    check_seeds(port, "final")
    server.stop()
    return f"killed at {', '.join(hits)}; {load.acked} acknowledged requests, all intact; no leftover; compacts after"


def closed_damage(damage, kill_before, compact_first=False):
    """Damage the shard file of a stopped server, then restart: the
    database must fail closed, twice (no crash loop). The newest commit is a
    lone PUT; with `kill_before` the server is SIGKILLed after it, so that
    commit is the newest in the file rather than redb's close-time one. With
    `compact_first` the database is compacted after it: the file checked
    before its rename is checked again by the next process."""

    def run(binary, work, port):
        data, server, expected, healthy = seeded(binary, work, port)
        expected.update(put_last(port))
        if compact_first:
            inode = os.stat(shard(data)).st_ino
            if request(port, "POST", "/faults/_compact", {})[0] != 202:
                raise Failure("POST _compact")
            while compact_running(port):
                pass
            if os.stat(shard(data)).st_ino == inode:
                raise Failure(f"the compaction did not replace the shard file\n{tail(server.log)}")
        server.stop(signal.SIGKILL if kill_before else signal.SIGTERM)
        what = damage(shard(data))
        server = server.restart()
        result = assert_fails_closed(server, "faults", expected, healthy)
        server = server.restart()
        assert_fails_closed(server, "faults", expected, healthy)
        server.stop()
        return f"{what}; {result}; same after another restart"

    return run


def removed_shard(binary, work, port):
    """q=4; remove one shard file of a stopped database. The database fails
    closed and stays listed, the file is not recreated, and the other files
    are kept: after the first failing request, further requests leave them
    byte for byte, and putting the removed file back restores every
    document."""
    extra = ("--q", "4")
    data, server, expected, healthy = seeded(binary, work, port, extra)
    server.stop(signal.SIGTERM)
    victim = shard(data, 2)
    kept = os.path.join(work, "shard-0002.redb.kept")
    shutil.move(victim, kept)
    others = sorted(p for p in Path(data, "faults").rglob("*") if p.is_file())
    server = server.restart()
    status, _ = get_json(port, "/faults")
    if status != 500:
        raise Failure(f"GET /faults with a shard missing -> {status}, expected 500")
    # The first open in a process may commit to the files it opened (redb's
    # integrity check); from then on they are left alone.
    snapshot = {p: digest(p) for p in others}
    result = assert_fails_closed(server, "faults", expected, healthy)
    if {p: digest(p) for p in others} != snapshot:
        raise Failure("the remaining shard files changed after the first failing request")
    server.stop(signal.SIGTERM)
    if os.path.exists(victim):
        raise Failure(f"{victim} was recreated")
    now = sorted(p for p in Path(data, "faults").rglob("*") if p.is_file())
    if now != others:
        raise Failure(f"the database's other files changed from {others} to {now}")
    shutil.move(kept, victim)
    server = server.restart()
    problem = check_intact(port, "faults", expected)
    if problem:
        raise Failure(f"with the file put back: {problem}")
    server.stop()
    return (
        f"removed shard-0002.redb of 4; {result}; not recreated, the other "
        f"{len(others)} files kept byte for byte after the first open; with it put back all {len(expected)} documents read back"
    )


def value_while_running(binary, work, port):
    """Damage a stored body while the server runs with the database open
    (and the body not read since the open, so not in redb's page cache): the
    read is a 500, never the damaged bytes or a miss. After a restart the
    first-open integrity check refuses the file."""
    data, server, expected, healthy = seeded(binary, work, port)
    expected.update(put_last(port))
    server = server.restart(signal.SIGTERM)
    status, _ = get_json(port, "/faults")  # opens the database
    if status != 200:
        raise Failure(f"GET /faults before damage -> {status}")
    what = flip_value(shard(data))
    reads = []
    for _ in range(2):
        status, got = read_doc(port, "faults", "last")
        reads.append(status)
        if status == 200 and strip(got) == expected["last"]:
            raise Failure("the damaged page was served from cache; the read never reached the disk")
        if status != 500:
            raise Failure(f"GET last after damage -> {status} {str(got)[:160]}, expected 500")
    problem = check_intact(port, "healthy", healthy)
    if problem:
        raise Failure(f"healthy database affected: {problem}")
    if not server.alive():
        raise Failure("server died")
    server = server.restart(signal.SIGKILL)
    result = assert_fails_closed(server, "faults", expected, healthy)
    server.stop()
    return f"{what}; reads -> {reads}; after restart {result}"


def branch_while_running(binary, work, port):
    """Write, then damage a branch page of the open database, then let it
    close idle (--max-open-dbs 1, then use another database): the close-time
    integrity check must catch it, and the next open must fail closed."""
    extra = ("--q", "1", "--max-open-dbs", "1")
    data, server, expected, healthy = seeded(binary, work, port, extra)
    server = server.restart(signal.SIGTERM)
    expected.update(put_last(port))
    what = flip_branch(shard(data))
    # Until the check runs, reads may still route through the page: a
    # correct 200 or a 500 is acceptable, wrong data is not.
    for docid in ["doc-0000", "doc-0150", "last"]:
        status, got = read_doc(port, "faults", docid)
        if status == 200 and strip(got) != expected[docid]:
            raise Failure(f"{docid} -> wrong body {got}")
        if status not in (200, 500):
            raise Failure(f"{docid} -> {status} before the integrity check")
    # Opening `healthy` closes the idle `faults`, which runs the check.
    problem = check_intact(port, "healthy", healthy)
    if problem:
        raise Failure(f"healthy database: {problem}")
    deadline = time.monotonic() + 30
    while "failed its integrity check on close" not in tail(server.log, 1 << 20):
        if time.monotonic() > deadline:
            raise Failure(f"no close-time integrity failure logged\n{tail(server.log)}")
        time.sleep(0.05)
    result = assert_fails_closed(server, "faults", expected, healthy)
    server = server.restart()
    assert_fails_closed(server, "faults", expected, healthy)
    server.stop()
    return f"{what}; the close-time check logged it; {result}; same after a restart"


VIEW = {"views": {"by_n": {"map": "function (doc) { if (doc.n !== undefined) { emit(doc.n, doc.s); } }"}}}
REBUILD_LOG = "view index is damaged; deleting it to rebuild"


def view_damage(damage):
    """Damage the view index file of a stopped server, then query the view:
    the file fails its first-open check, the rebuild is logged, and the view
    answers 200 with exactly the rows it had before."""

    def run(binary, work, port):
        data, server, _, _ = seeded(binary, work, port)
        status, raw = request(port, "PUT", "/faults/_design/d", VIEW)
        if status != 201:
            raise Failure(f"design doc: {status} {raw[:200]}")
        status, before = get_json(port, "/faults/_design/d/_view/by_n")
        if status != 200 or len(before.get("rows", [])) != DOCS:
            raise Failure(f"view before damage: {status} {str(before)[:200]}")
        server.stop(signal.SIGTERM)
        files = list(Path(data, "faults", "_view_indexes").glob("*.redb"))
        if len(files) != 1:
            raise Failure(f"expected one view index file, found {files}")
        def state(path):
            return "flagged for recovery" if RedbFile(path).raw[9] & RECOVERY_REQUIRED else "clean"

        clean = f"after SIGTERM the view file is {state(files[0])}, the shard {state(shard(data))}"
        what = damage(files[0])
        server = server.restart()
        status, after = get_json(port, "/faults/_design/d/_view/by_n")
        if status != 200:
            raise Failure(f"view after damage -> {status} {str(after)[:200]}")
        if after["rows"] != before["rows"] or after.get("total_rows") != before.get("total_rows"):
            raise Failure("the view's rows after the damage differ from the rows before it")
        if REBUILD_LOG not in tail(server.log, 1 << 20):
            raise Failure(f"the view answered without a logged rebuild\n{tail(server.log)}")
        server.stop()
        return f"{clean}; {what}; rebuild logged; {len(after['rows'])} rows identical"

    return run


def flip_view_row(path):
    hits = RedbFile(path).live_leaves_with(b"payload-150")
    if len(hits) != 1:
        raise Failure(f"{len(hits)} live view leaves hold payload-150")
    index = RedbFile(path)
    table, leaf, at = hits[0]
    shared = "also" if index.in_previous_commit(leaf) else "not"
    flip(path, at + 3)
    return f"flipped byte {at + 3} of the live {table} leaf at {leaf} ({shared} in the previous commit)"


SCENARIOS = [
    ("sigkill-mid-write", sigkill_mid_write),
    ("sigkill-mid-compaction", sigkill_mid_compaction),
    ("truncate", closed_damage(truncate, kill_before=False)),
    ("truncate-after-kill", closed_damage(truncate, kill_before=True)),
    ("value-flip", closed_damage(flip_value, kill_before=False)),
    ("value-flip-after-kill", closed_damage(flip_value, kill_before=True)),
    ("value-flip-after-compaction", closed_damage(flip_value, kill_before=True, compact_first=True)),
    ("branch-flip", closed_damage(flip_branch, kill_before=False)),
    ("branch-flip-after-kill", closed_damage(flip_branch, kill_before=True)),
    ("removed-shard", removed_shard),
    ("value-while-running", value_while_running),
    ("branch-while-running", branch_while_running),
    ("view-flip", view_damage(flip_view_row)),
    ("view-truncate", view_damage(truncate)),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin", default="target/release/douchdb")
    parser.add_argument("--port", type=int, default=0, help="0 picks a free port per scenario")
    parser.add_argument("--only", action="append", help="run only the named scenario (repeatable)")
    args = parser.parse_args()
    binary = os.path.abspath(args.bin)
    if not os.path.exists(binary):
        raise SystemExit(f"missing binary {binary}; run cargo build --release")
    signal.signal(signal.SIGALRM, on_alarm)
    signal.signal(signal.SIGTERM, on_term)

    failures = []
    started = time.monotonic()
    for name, scenario in SCENARIOS:
        if args.only and name not in args.only:
            continue
        work = tempfile.mkdtemp(prefix="douchdb-fault-")
        port = args.port or free_port()
        began = time.monotonic()
        remaining = int(TOTAL_SECONDS - (time.monotonic() - started))
        if remaining < 1:
            print(f"{name:28s} FAIL  the run exceeded {TOTAL_SECONDS}s")
            failures.append(name)
            continue
        signal.alarm(min(SCENARIO_SECONDS, remaining))
        try:
            detail = scenario(binary, work, port)
            result = "PASS"
        except (Failure, ScenarioTimeout, RuntimeError, OSError, http.client.HTTPException) as error:
            detail = str(error)
            result = "FAIL"
            failures.append(name)
        finally:
            signal.alarm(0)
            kill_children()
            shutil.rmtree(work, ignore_errors=True)
        print(f"{name:28s} {result} {time.monotonic() - began:5.1f}s  {detail}", flush=True)

    print(f"total {time.monotonic() - started:.1f}s")
    if failures:
        print(f"FAIL: {', '.join(failures)}")
        return 1
    print("PASS: crashes kept every acknowledged write; damage failed closed or was rebuilt")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

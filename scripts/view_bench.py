#!/usr/bin/env python3
"""View query benchmark for one douchdb binary.

Builds nothing. Starts a fresh single-node douchdb, loads generated
documents, and measures views: index build, memory once ten views have been
queried, warm map queries (limit 10, 1,000 and every row), warm reduce
(ungrouped and grouped), an incremental update, and the first queries after
a restart. A partitioned database with one partition per document is
measured too. Prints one JSON object.

    cargo build --release
    python3 scripts/view_bench.py --binary $CARGO_TARGET_DIR/release/douchdb \\
        --scratch /path/to/scratch --docs 250000

The data is generated, so runs of two binaries compare like for like. Each
document emits four rows into each of the ten views, so a view holds four
rows per document (1M rows at the default 250,000 documents).
"""

import argparse
import http.client
import json
import random
import shutil
import signal
import socket
import statistics
import subprocess
import threading
import time
import urllib.parse
from pathlib import Path

DB = "views"
PDB = "pviews"
VIEWS = 10
STEP_TIMEOUT = 600  # seconds for any one request
GROUPS = 100


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def call(port, method, path, body=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=STEP_TIMEOUT)
    try:
        headers = {"Content-Type": "application/json"} if body is not None else {}
        conn.request(method, path, body=body, headers=headers)
        response = conn.getresponse()
        raw = response.read()
        if response.status >= 400:
            raise RuntimeError(f"{method} {path}: {response.status} {raw[:300]!r}")
        return raw
    finally:
        conn.close()


class Server:
    """A single-node douchdb on `data`, stopped on exit."""

    def __init__(self, binary, data, q):
        self.port = free_port()
        self.log = open(data.parent / f"{data.name}.log", "ab")
        self.proc = subprocess.Popen(
            [binary, "--listen", f"127.0.0.1:{self.port}", "--data", str(data),
             "--n", "1", "--q", str(q)],
            stdout=self.log, stderr=self.log,
        )

    def __enter__(self):
        try:
            deadline = time.monotonic() + 60
            while True:
                if self.proc.poll() is not None:
                    raise RuntimeError(f"douchdb exited with {self.proc.returncode}")
                try:
                    call(self.port, "GET", "/_up")
                    return self
                except OSError:
                    if time.monotonic() > deadline:
                        raise RuntimeError("douchdb did not start")
                    time.sleep(0.1)
        except BaseException:
            self.__exit__()
            raise

    def __exit__(self, *_):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.log.close()

    def rss_mb(self):
        out = subprocess.run(["ps", "-o", "rss=", "-p", str(self.proc.pid)],
                             capture_output=True, text=True, check=True)
        return round(int(out.stdout.strip()) / 1024, 1)


def doc(i, partitioned):
    rng = random.Random(i)
    body = {
        "_id": f"p{i}:d" if partitioned else f"d{i:07d}",
        "n": rng.randrange(1_000_000),
        "g": i % GROUPS,
        "s": "".join(rng.choice("abcdefghij") for _ in range(8)),
    }
    return json.dumps(body).encode()


def load(port, db, count, partitioned, clients=4, batch=1000):
    bodies = [
        b'{"docs":[' + b",".join(doc(i, partitioned) for i in range(start, min(start + batch, count))) + b"]}"
        for start in range(0, count, batch)
    ]
    queue = list(reversed(bodies))
    lock = threading.Lock()
    errors = []

    def client():
        try:
            while True:
                with lock:
                    if not queue:
                        return
                    body = queue.pop()
                call(port, "POST", f"/{db}/_bulk_docs", body)
        except Exception as error:  # noqa: BLE001 - reported below
            errors.append(error)

    started = time.monotonic()
    threads = [threading.Thread(target=client) for _ in range(clients)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise errors[0]
    return time.monotonic() - started


def design(partitioned):
    if partitioned:
        return {"_id": "_design/p", "options": {"partitioned": True}, "views": {
            "v": {"map": "function(doc) { emit(doc.n, doc.g); }", "reduce": "_count"}}}
    views = {}
    for v in range(VIEWS):
        views[f"v{v}"] = {
            # Four rows per document, keys spread over groups and numbers.
            "map": ("function(doc) { for (var k = 0; k < 4; k++) "
                    f"emit([doc.g, doc.n + k + {v}], doc.n); }}"),
            "reduce": "_sum",
        }
    return {"_id": "_design/d", "views": views}


def timed(port, path):
    started = time.monotonic()
    raw = call(port, "GET", path)
    return (time.monotonic() - started) * 1000, raw


def p50(port, path, reps):
    return round(statistics.median(timed(port, path)[0] for _ in range(reps)), 3)


def view(name, query=""):
    return f"/{DB}/_design/d/_view/{name}{'?' + query if query else ''}"


def bench(binary, scratch, docs, pdocs):
    data = scratch / "view-bench-data"
    shutil.rmtree(data, ignore_errors=True)
    data.mkdir(parents=True)
    out = {"binary": str(binary), "docs": docs, "rows_per_view": docs * 4}
    with Server(binary, data, q=4) as server:
        port = server.port
        call(port, "PUT", f"/{DB}")
        out["load_s"] = round(load(port, DB, docs, False), 2)
        call(port, "PUT", f"/{DB}/_design/d", json.dumps(design(False)).encode())
        builds = [timed(port, view(f"v{v}", "limit=0&reduce=false"))[0] for v in range(VIEWS)]
        out["build_ms_each"] = round(statistics.median(builds), 1)
        for v in range(VIEWS):
            call(port, "GET", view(f"v{v}", "limit=10&reduce=false"))
            call(port, "GET", view(f"v{v}"))
        out["rss_mb_after_10_views"] = server.rss_mb()

        rng = random.Random(7)
        starts = [
        urllib.parse.quote(json.dumps([rng.randrange(GROUPS), rng.randrange(1_000_000)]))
        for _ in range(50)
    ]
        lat = [timed(port, view("v0", f"limit=10&reduce=false&startkey={s}"))[0] for s in starts]
        out["map_limit10_p50_ms"] = round(statistics.median(lat), 3)
        lat = [timed(port, view("v0", f"limit=1000&reduce=false&startkey={s}"))[0] for s in starts[:20]]
        out["map_limit1000_p50_ms"] = round(statistics.median(lat), 3)
        out["map_all_rows_ms"] = round(min(timed(port, view("v0", "reduce=false"))[0] for _ in range(3)), 1)
        out["reduce_p50_ms"] = p50(port, view("v0"), 50)
        out["reduce_range_p50_ms"] = p50(port, view("v0", "startkey=%5B10,0%5D&endkey=%5B60,500000%5D"), 50)
        out["reduce_group_level1_p50_ms"] = p50(port, view("v0", "group_level=1"), 20)
        out["reduce_group_p50_ms"] = p50(port, view("v0", "group=true&limit=1000"), 20)

        # An incremental update: 1,000 changed documents, then one query.
        updates = []
        for round_ in range(5):
            ids = [f"d{i:07d}" for i in range(round_ * 1000, round_ * 1000 + 1000)]
            raw = call(port, "POST", f"/{DB}/_all_docs", json.dumps({"keys": ids}).encode())
            revs = {row["id"]: row["value"]["rev"] for row in json.loads(raw)["rows"]}
            changed = [dict(json.loads(doc(int(i[1:]), False)), _rev=revs[i], n=round_) for i in ids]
            call(port, "POST", f"/{DB}/_bulk_docs", json.dumps({"docs": changed}).encode())
            updates.append(timed(port, view("v0", "limit=10&reduce=false"))[0])
        out["update_1000_docs_p50_ms"] = round(statistics.median(updates), 1)
        out["rss_mb_end"] = server.rss_mb()

    with Server(binary, data, q=4) as server:
        out["restart_first_map_ms"] = round(timed(server.port, view("v0", "limit=10&reduce=false"))[0], 1)
        out["restart_first_reduce_ms"] = round(timed(server.port, view("v1"))[0], 1)
        out["restart_rss_mb"] = server.rss_mb()

    pdata = scratch / "view-bench-pdata"
    shutil.rmtree(pdata, ignore_errors=True)
    pdata.mkdir(parents=True)
    with Server(binary, pdata, q=4) as server:
        port = server.port
        call(port, "PUT", f"/{PDB}?partitioned=true")
        load(port, PDB, pdocs, True)
        call(port, "PUT", f"/{PDB}/_design/p", json.dumps(design(True)).encode())
        out["partitioned_docs"] = pdocs
        out["partitioned_build_ms"] = round(timed(port, f"/{PDB}/_partition/p0/_design/p/_view/v?reduce=false")[0], 1)
        rng = random.Random(9)
        paths = [f"/{PDB}/_partition/p{rng.randrange(pdocs)}/_design/p/_view/v" for _ in range(50)]
        out["partition_map_p50_ms"] = round(statistics.median(timed(port, p + "?reduce=false")[0] for p in paths), 3)
        out["partition_reduce_p50_ms"] = round(statistics.median(timed(port, p)[0] for p in paths), 3)
        out["partitioned_rss_mb"] = server.rss_mb()

    shutil.rmtree(data, ignore_errors=True)
    shutil.rmtree(pdata, ignore_errors=True)
    return out


def main():
    parser = argparse.ArgumentParser(description="View query benchmark for one douchdb binary.")
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--scratch", required=True, type=Path)
    parser.add_argument("--docs", type=int, default=250_000)
    parser.add_argument("--partitioned-docs", type=int, default=100_000)
    args = parser.parse_args()
    print(json.dumps(bench(args.binary, args.scratch, args.docs, args.partitioned_docs), indent=2))


if __name__ == "__main__":
    main()

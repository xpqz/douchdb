#!/usr/bin/env python3
"""`_bulk_docs` against the bare redb floor (examples/redb_floor.rs).

Builds nothing. Imports the airline fixture into a fresh single-node douchdb
for every configuration and round, reads the shard counters from
`/_node/_local/_metrics` before and after, runs the floor on the same
documents and batches in the same round, and prints a markdown table.

    cargo build --release --examples --bins
    timeout 900 python3 scripts/bulk_bench.py \\
        --binary $CARGO_TARGET_DIR/release/douchdb --fixture <dir> --rounds 1

`<dir>` holds `airline_accidents.jsonl` (see docs/kaggle-import.md). The
floor binary is `examples/redb_floor` next to `--binary`. Scratch data goes
under $TMPDIR and is removed on exit.

Columns, all medians over the rounds:
- floor, douchdb: documents per second (wall time of the whole import).
- jobs us/doc: the shard actors' time in their jobs (`shard.jobs_us`), and
  the floor's insert time (begin to last insert), each summed over the
  shards and divided by the documents. Syncs are excluded from both.
- syncs/commit and KB/commit: douchdb per shard group, floor per shard
  transaction. KB counts every byte redb writes to the shard files.
- requests/group: requests a douchdb shard group served (4 clients group
  several); the floor always commits one request's batch.
"""

import argparse
import http.client
import json
import os
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

FIXTURE = "airline_accidents.jsonl"
DB = "bench"
CACHE_MB = "16"  # the floor's page cache, douchdb's default
STEP_TIMEOUT = 600  # seconds for any one child process


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


def stop(proc):
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


class Server:
    """A single-node douchdb on a fresh data directory, stopped on exit."""

    def __init__(self, binary, q, data):
        self.port = free_port()
        self.data = data
        self.log = open(data.parent / f"{data.name}.log", "wb")
        self.proc = subprocess.Popen(
            [binary, "--listen", f"127.0.0.1:{self.port}", "--data", str(data),
             "--n", "1", "--q", str(q), "--cache-mb", CACHE_MB],
            stdout=self.log, stderr=self.log,
        )

    def __enter__(self):
        # `with` calls __exit__ only once __enter__ has returned.
        try:
            return self._wait_ready()
        except BaseException:
            self.__exit__()
            raise

    def _wait_ready(self):
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

    def __exit__(self, *_):
        stop(self.proc)
        self.log.close()


def batches(lines, size):
    """Request bodies of `size` documents each, joined from the raw lines."""
    return [
        b'{"docs":[' + b",".join(lines[i:i + size]) + b"]}"
        for i in range(0, len(lines), size)
    ]


def shard_metrics(port):
    return json.loads(call(port, "GET", "/_node/_local/_metrics"))["shard"]


def import_docs(port, bodies, clients):
    """Send `bodies` from `clients` connections; the wall time in seconds."""
    queue = list(reversed(bodies))
    lock = threading.Lock()
    errors = []

    def client():
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=STEP_TIMEOUT)
        try:
            while True:
                with lock:
                    if not queue or errors:
                        return
                    body = queue.pop()
                conn.request("POST", f"/{DB}/_bulk_docs", body=body,
                             headers={"Content-Type": "application/json"})
                response = conn.getresponse()
                raw = response.read()
                if response.status != 201 or b'"error"' in raw:
                    raise RuntimeError(f"_bulk_docs: {response.status} {raw[:300]!r}")
        except Exception as error:  # noqa: BLE001
            with lock:
                errors.append(error)
        finally:
            conn.close()

    threads = [threading.Thread(target=client) for _ in range(clients)]
    started = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise errors[0]
    return time.perf_counter() - started


def douchdb_run(binary, q, bodies, docs, clients, scratch, tag):
    data = scratch / tag
    try:
        with Server(binary, q, data) as server:
            call(server.port, "PUT", f"/{DB}")
            before = shard_metrics(server.port)
            wall = import_docs(server.port, bodies, clients)
            after = shard_metrics(server.port)
    finally:
        shutil.rmtree(data, ignore_errors=True)
    delta = {name: after[name] - before[name] for name in after}
    groups = max(delta["groups"], 1)
    return {
        "docs_per_s": docs / wall,
        "jobs_us_per_doc": delta["jobs_us"] / docs,
        "syncs_per_commit": delta["syncs"] / groups,
        "kb_per_commit": delta["bytes_written"] / groups / 1024,
        # Each request is one job on every shard it touches (all of them).
        "requests_per_group": len(bodies) * q / groups,
    }


def floor_run(floor, fixture, shard, batch, q):
    out = subprocess.run(
        [floor, "--docs", str(fixture), "--shard", str(shard),
         "--batch", str(batch), "--q", str(q)],
        capture_output=True, check=True, timeout=STEP_TIMEOUT,
    )
    run = json.loads(out.stdout)
    batches_run = -(-run["docs"] // batch)
    return {
        "docs_per_s": run["docs_per_s"],
        "insert_us_per_doc": run["insert_us_per_batch"] * batches_run / run["docs"],
        "syncs_per_commit": run["syncs_per_commit"],
        "kb_per_commit": run["bytes_per_commit"] / 1024,
    }


def source_shard(binary, lines, scratch):
    """Import the fixture once at q=1 and keep its shard file for the floor."""
    data = scratch / "source"
    with Server(binary, 1, data) as server:
        call(server.port, "PUT", f"/{DB}")
        import_docs(server.port, batches(lines, 1000), 1)
    files = sorted(data.rglob("shard-0000.redb"))
    files = [f for f in files if "_" not in f.parent.name]
    if len(files) != 1:
        raise RuntimeError(f"expected one shard file of {DB}, found {files}")
    return files[0]


def median(runs, key):
    return statistics.median(run[key] for run in runs)


def main():
    parser = argparse.ArgumentParser(
        description="`_bulk_docs` against the bare redb floor (examples/redb_floor.rs).")
    parser.add_argument("--binary", required=True)
    parser.add_argument("--fixture", required=True, type=Path)
    parser.add_argument("--batches", default="250,1000,10000")
    parser.add_argument("--q", default="1,2")
    parser.add_argument("--clients", default="1,4")
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    binary = os.path.abspath(args.binary)
    floor = str(Path(binary).parent / "examples" / "redb_floor")
    fixture = args.fixture / FIXTURE
    sizes = [int(n) for n in args.batches.split(",")]
    qs = [int(n) for n in args.q.split(",")]
    client_counts = [int(n) for n in args.clients.split(",")]

    lines = [line for line in fixture.read_bytes().split(b"\n") if line]
    scratch = Path(tempfile.mkdtemp(prefix="bulk-bench-"))
    rows = []
    try:
        shard = source_shard(binary, lines, scratch)
        for q in qs:
            for size in sizes:
                bodies = batches(lines, size)
                floors = []
                servers = {clients: [] for clients in client_counts}
                for round_ in range(args.rounds):
                    # Interleaved, so a slow spell hits both sides alike.
                    floors.append(floor_run(floor, fixture, shard, size, q))
                    for clients in client_counts:
                        tag = f"q{q}-b{size}-c{clients}-r{round_}"
                        servers[clients].append(
                            douchdb_run(binary, q, bodies, len(lines), clients, scratch, tag))
                floor_rate = median(floors, "docs_per_s")
                insert = median(floors, "insert_us_per_doc")
                for clients in client_counts:
                    runs = servers[clients]
                    rate = median(runs, "docs_per_s")
                    jobs = median(runs, "jobs_us_per_doc")
                    rows.append(
                        f"| q{q}/{size}/{clients}c | {floor_rate:,.0f} | {rate:,.0f} | "
                        f"{rate / floor_rate:.2f} | {jobs:.1f} | {insert:.1f} | "
                        f"{jobs / insert:.2f} | {median(runs, 'syncs_per_commit'):.2f} / "
                        f"{median(floors, 'syncs_per_commit'):.2f} | "
                        f"{median(runs, 'kb_per_commit'):,.0f} / "
                        f"{median(floors, 'kb_per_commit'):,.0f} | "
                        f"{median(runs, 'requests_per_group'):.2f} |")
                    print(rows[-1], file=sys.stderr, flush=True)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)

    print(f"{len(lines):,} documents, {args.rounds} round(s), medians.\n")
    print("| q/batch/clients | floor docs/s | douchdb docs/s | ratio | "
          "jobs us/doc | floor insert us/doc | jobs/insert | "
          "syncs/commit (douchdb / floor) | KB/commit (douchdb / floor) | "
          "requests/group |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""10 GB storage spike for douchdb.

Measures the thing the plan is unsure about: what a single shard actor costs
when LevelDB is large and compacts. The workload is deliberately unfriendly:

  * incompressible payloads (base64 of random bytes), so Snappy cannot turn
    10 GB of logical data into a few MB on disk;
  * several concurrent writers, so the shard actor's group commit is exercised;
  * a latency probe running before and during `_compact`, which measures the
    p99 a client sees while compaction owns the `&mut self` shard handle;
  * a SIGKILL/restart at the end to confirm the synced WriteBatches were real.

Run it against a release build:

    cargo build --release
    python3 scripts/storage_spike.py --target-gb 10

It starts its own server, so nothing else may be listening on --port.
"""

import argparse
import base64
import http.client
import json
import os
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time

WORDS = None


def percentile(values, pct):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * pct / 100.0) - 1))
    return ordered[index]


def request(conn, method, path, body=None, timeout=3600):
    headers = {"Content-Type": "application/json"}
    data = None
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        headers["Content-Length"] = str(len(data))
    started = time.perf_counter()
    conn.request(method, path, body=data, headers=headers)
    response = conn.getresponse()
    raw = response.read()
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return response.status, raw, elapsed_ms


def connect(port, timeout=3600):
    return http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)


def wait_ready(port, proc, seconds=30):
    deadline = time.time() + seconds
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited with {proc.returncode}")
        try:
            conn = connect(port, timeout=2)
            status, _, _ = request(conn, "GET", "/")
            conn.close()
            if status == 200:
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError("server did not become ready")


def start_server(args):
    log = open(os.path.join(args.data + ".log"), "ab", buffering=0)
    proc = subprocess.Popen(
        [
            args.bin,
            "--listen",
            f"127.0.0.1:{args.port}",
            "--data",
            args.data,
            "--q",
            str(args.q),
            "--n",
            "1",
            "--sync-secs",
            "3600",
        ],
        stdout=log,
        stderr=log,
    )
    wait_ready(args.port, proc)
    return proc


def stop_server(proc, sig=signal.SIGKILL):
    if proc is None or proc.poll() is not None:
        return
    proc.send_signal(sig)
    try:
        proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def du_bytes(path):
    out = subprocess.check_output(["du", "-sb", path], text=True)
    return int(out.split()[0])


def make_payload(size):
    # Random bytes, base64'd, are incompressible to Snappy. Reusing a small
    # pool is still incompressible because Snappy works per block.
    return base64.b64encode(os.urandom(max(3, size * 3 // 4))).decode()


def load_worker(args, payloads, counter, lock, latencies, errors, stop):
    conn = connect(args.port)
    batch_docs = args.batch_docs
    while not stop.is_set():
        with lock:
            if counter[0] >= args.total_docs:
                break
            start = counter[0]
            end = min(start + batch_docs, args.total_docs)
            counter[0] = end
        docs = []
        for i in range(start, end):
            docs.append(
                {
                    "_id": f"spike-{i:08d}",
                    "n": i,
                    "blob": payloads[i % len(payloads)],
                }
            )
        try:
            status, raw, elapsed = request(
                conn, "POST", f"/{args.db}/_bulk_docs", {"docs": docs}
            )
            with lock:
                latencies.append(elapsed)
            if status not in (201, 202):
                raise RuntimeError(f"bulk_docs {status}: {raw[:300]!r}")
            for row in json.loads(raw):
                if "error" in row:
                    raise RuntimeError(f"bulk error: {row}")
        except Exception as exc:  # noqa: BLE001 - report in the summary
            with lock:
                errors.append(str(exc))
            break
    conn.close()


def probe_loop(args, latencies, stop, errors):
    conn = connect(args.port, timeout=3600)
    while not stop.is_set():
        try:
            status, _, elapsed = request(conn, "GET", f"/{args.db}/probe")
            if status == 200:
                latencies.append(elapsed)
        except Exception as exc:  # noqa: BLE001
            # A request queued behind compaction will report a long latency,
            # not an error; capture anything else for the summary.
            if not stop.is_set():
                errors.append(str(exc))
        stop.wait(args.probe_interval)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin", default="target/release/douchdb")
    parser.add_argument("--data", default="target/spike-data")
    parser.add_argument("--db", default="spike")
    parser.add_argument("--port", type=int, default=5999)
    parser.add_argument("--q", type=int, default=1, help="shards; 1 is worst-case serialisation")
    parser.add_argument("--target-gb", type=float, default=10.0)
    parser.add_argument("--doc-bytes", type=int, default=1_000_000)
    parser.add_argument("--batch-docs", type=int, default=4)
    parser.add_argument("--loaders", type=int, default=4)
    parser.add_argument("--probe-interval", type=float, default=0.01)
    parser.add_argument("--baseline-secs", type=float, default=5.0)
    parser.add_argument("--json-out", default="target/storage-spike.json")
    args = parser.parse_args()

    target_bytes = int(args.target_gb * 1_000_000_000)
    # Document JSON is slightly larger than the blob; aim the doc count at the
    # blob size and report the real logical total afterwards.
    args.total_docs = max(1, target_bytes // args.doc_bytes)

    shutil.rmtree(args.data, ignore_errors=True)
    os.makedirs(args.data, exist_ok=True)

    pool_size = min(args.total_docs, 16)
    print(
        f"generating {pool_size} incompressible {args.doc_bytes}-byte payloads...",
        file=sys.stderr,
    )
    payloads = [make_payload(args.doc_bytes) for _ in range(pool_size)]

    proc = start_server(args)
    conn = connect(args.port)
    request(conn, "DELETE", f"/{args.db}")
    status, raw, _ = request(conn, "PUT", f"/{args.db}", {})
    if status != 201:
        raise RuntimeError(f"create db {status}: {raw!r}")
    status, raw, _ = request(conn, "PUT", f"/{args.db}/probe", {"v": 1})
    if status != 201:
        raise RuntimeError(f"create probe {status}: {raw!r}")

    print(
        f"loading {args.total_docs} docs (~{args.target_gb:.1f} GB logical) "
        f"with {args.loaders} writers, q={args.q}...",
        file=sys.stderr,
    )
    counter = [0]
    lock = threading.Lock()
    latencies = []
    errors = []
    stop = threading.Event()
    loaders = [
        threading.Thread(
            target=load_worker,
            args=(args, payloads, counter, lock, latencies, errors, stop),
        )
        for _ in range(args.loaders)
    ]
    load_started = time.time()
    for thread in loaders:
        thread.start()
    for thread in loaders:
        thread.join()
    load_seconds = time.time() - load_started

    status, raw, _ = request(conn, "GET", f"/{args.db}")
    if status != 200:
        raise RuntimeError(f"db info {status}: {raw!r}")
    info = json.loads(raw)
    doc_count = info["doc_count"]

    # Logical size: read the payload back from one document and multiply.
    status, raw, _ = request(conn, "GET", f"/{args.db}/spike-00000000")
    sample = json.loads(raw)
    doc_json_bytes = len(json.dumps(sample))
    logical_bytes = doc_json_bytes * doc_count
    disk_after_load = du_bytes(args.data)

    print("baseline latency probe...", file=sys.stderr)
    baseline = []
    baseline_errors = []
    stop.clear()
    probe = threading.Thread(
        target=probe_loop, args=(args, baseline, stop, baseline_errors)
    )
    probe.start()
    time.sleep(args.baseline_secs)
    stop.set()
    probe.join()

    print("starting compaction with the probe running...", file=sys.stderr)
    during = []
    during_errors = []
    stop = threading.Event()
    probe = threading.Thread(target=probe_loop, args=(args, during, stop, during_errors))
    probe.start()
    compact_started = time.time()
    status, raw, _ = request(conn, "POST", f"/{args.db}/_compact", {}, timeout=7200)
    compact_seconds = time.time() - compact_started
    stop.set()
    probe.join()
    if status not in (200, 202):
        raise RuntimeError(f"compact {status}: {raw[:300]!r}")

    disk_after_compact = du_bytes(args.data)

    print("kill -9 and restart, checking durability...", file=sys.stderr)
    stop_server(proc)
    proc = start_server(args)
    conn.close()
    conn = connect(args.port)
    status, raw, _ = request(conn, "GET", f"/{args.db}")
    restarted = json.loads(raw) if status == 200 else {}
    stop_server(proc)

    result = {
        "config": {
            "q": args.q,
            "doc_bytes": args.doc_bytes,
            "batch_docs": args.batch_docs,
            "loaders": args.loaders,
            "target_gb": args.target_gb,
        },
        "docs": doc_count,
        "logical_bytes": logical_bytes,
        "logical_gb": round(logical_bytes / 1e9, 3),
        "load_seconds": round(load_seconds, 2),
        "write_mb_per_sec": round(logical_bytes / 1e6 / load_seconds, 2),
        "write_latency_ms": {
            "p50": round(percentile(latencies, 50), 2),
            "p99": round(percentile(latencies, 99), 2),
            "max": round(max(latencies), 2) if latencies else 0,
        },
        "disk_after_load_bytes": disk_after_load,
        "disk_after_compact_bytes": disk_after_compact,
        "write_amplification_after_compact": round(disk_after_compact / logical_bytes, 3),
        "baseline_probe_ms": {
            "n": len(baseline),
            "p50": round(percentile(baseline, 50), 2),
            "p99": round(percentile(baseline, 99), 2),
            "max": round(max(baseline), 2) if baseline else 0,
        },
        "compaction_seconds": round(compact_seconds, 2),
        "during_compaction_probe_ms": {
            "n": len(during),
            "p50": round(percentile(during, 50), 2),
            "p99": round(percentile(during, 99), 2),
            "max": round(max(during), 2) if during else 0,
        },
        "doc_count_after_restart": restarted.get("doc_count"),
        "errors": errors[:10] + baseline_errors[:10] + during_errors[:10],
    }

    os.makedirs(os.path.dirname(args.json_out) or ".", exist_ok=True)
    with open(args.json_out, "w") as handle:
        json.dump(result, handle, indent=2)
        handle.write("\n")

    print(json.dumps(result, indent=2))
    if errors or result["doc_count_after_restart"] != doc_count:
        sys.exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Single-node douchdb HTTP performance baseline.

Runs against a release binary, creates its own data directory and reports
latency/throughput for the common HTTP operations. The server uses q=1 and no
authentication so the numbers describe storage/routing rather than auth.

    cargo build --release
    python3 scripts/perf_sweep.py --docs 6000

Result is written to target/perf-baseline.json.
"""

import argparse
import http.client
import json
import os
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time


def percentile(values, pct):
    if not values:
        return 0.0
    values = sorted(values)
    index = max(0, min(len(values) - 1, int(len(values) * pct / 100.0) - 1))
    return values[index]


def summary(latencies, wall_s, units=None):
    units = len(latencies) if units is None else units
    return {
        "n": len(latencies),
        "units": units,
        "wall_s": round(wall_s, 3),
        "per_s": round(units / wall_s, 1) if wall_s else 0,
        "latency_ms": {
            "p50": round(percentile(latencies, 50), 3),
            "p95": round(percentile(latencies, 95), 3),
            "p99": round(percentile(latencies, 99), 3),
            "max": round(max(latencies), 3) if latencies else 0,
        },
    }


def request(conn, method, path, body=None, timeout=60):
    data = None
    headers = {}
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    started = time.perf_counter()
    conn.request(method, path, body=data, headers=headers)
    response = conn.getresponse()
    raw = response.read()
    elapsed = (time.perf_counter() - started) * 1000.0
    if response.status >= 400:
        raise RuntimeError(f"{method} {path}: {response.status} {raw[:300]!r}")
    return elapsed, raw


def connect(port):
    return http.client.HTTPConnection("127.0.0.1", port, timeout=120)


def free_port():
    import socket

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def wait_ready(port, proc):
    deadline = time.time() + 30
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited with {proc.returncode}")
        try:
            conn = connect(port)
            request(conn, "GET", "/_up")
            conn.close()
            return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError("server did not start")


def run_workers(workers, total, fn):
    workers = max(1, min(workers, total))
    bounds = []
    base = total // workers
    extra = total % workers
    start = 0
    for i in range(workers):
        size = base + (1 if i < extra else 0)
        bounds.append((start, start + size))
        start += size

    barrier = threading.Barrier(workers)
    lock = threading.Lock()
    latencies = []
    errors = []

    def worker(lo, hi):
        conn = connect(PORT)
        barrier.wait()
        local = []
        try:
            for i in range(lo, hi):
                elapsed, _ = fn(conn, i)
                local.append(elapsed)
        except Exception as error:  # noqa: BLE001
            with lock:
                errors.append(str(error))
        finally:
            conn.close()
        with lock:
            latencies.extend(local)

    threads = [threading.Thread(target=worker, args=bound) for bound in bounds]
    started = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    wall = time.perf_counter() - started
    if errors:
        raise RuntimeError(errors[0])
    return latencies, wall


def put_doc(conn, doc_id, value, text):
    return request(conn, "PUT", f"/perf/{doc_id}", {"value": value, "text": text})


def get_doc(conn, i):
    return request(conn, "GET", f"/perf/doc-{i:06d}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin", default="target/release/douchdb")
    parser.add_argument("--data", default="target/perf-data")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--docs", type=int, default=6000)
    parser.add_argument("--levels", default="1,8,32,64")
    parser.add_argument("--per-level", type=int, default=1000)
    parser.add_argument("--json-out", default="target/perf-baseline.json")
    args = parser.parse_args()

    global PORT
    PORT = args.port or free_port()
    shutil.rmtree(args.data, ignore_errors=True)
    os.makedirs(args.data, exist_ok=True)
    log = open(args.data + ".log", "wb")
    binary = os.path.abspath(args.bin)
    proc = subprocess.Popen(
        [
            binary,
            "--listen",
            f"127.0.0.1:{PORT}",
            "--data",
            args.data,
            "--q",
            "1",
            "--n",
            "1",
            "--sync-secs",
            "3600",
        ],
        stdout=log,
        stderr=log,
    )
    result = {"environment": {}, "config": {}, "operations": {}}
    try:
        wait_ready(PORT, proc)
        conn = connect(PORT)
        request(conn, "PUT", "/perf", {})
        print(f"server ready on {PORT}; loading {args.docs} docs...", file=sys.stderr)

        # Setup: load documents with bulk batches (not part of the benchmark).
        batch = 500
        for start in range(0, args.docs, batch):
            docs = [
                {"_id": f"doc-{i:06d}", "value": i, "text": "x" * 100}
                for i in range(start, min(start + batch, args.docs))
            ]
            request(conn, "POST", "/perf/_bulk_docs", {"docs": docs})
        result["config"] = {"q": 1, "n": 1, "docs": args.docs, "port": PORT}

        # Sequential single-document writes.
        seq_lat = []
        started = time.perf_counter()
        for i in range(500):
            elapsed, _ = put_doc(conn, f"seq-{i:06d}", i, "x" * 100)
            seq_lat.append(elapsed)
        result["operations"]["put_single_sequential"] = summary(
            seq_lat, time.perf_counter() - started
        )
        conn.close()

        # Concurrent single-document writes at several concurrency levels.
        for workers in [int(level) for level in args.levels.split(",") if level]:
            def write(conn, i, workers=workers):
                return put_doc(conn, f"w{workers}-{i:06d}", i, "x" * 100)

            latency, wall = run_workers(workers, args.per_level, write)
            result["operations"][f"put_single_concurrent_w{workers}"] = summary(
                latency, wall
            )
            print(f"  put w={workers}: {summary(latency, wall)['per_s']} req/s", file=sys.stderr)

        # Sequential and concurrent reads.
        def read(conn, i):
            return get_doc(conn, i)

        latency, wall = run_workers(1, 1000, read)
        result["operations"]["get_single_sequential"] = summary(latency, wall)
        for workers in [int(level) for level in args.levels.split(",") if level]:
            latency, wall = run_workers(workers, args.per_level, read)
            result["operations"][f"get_single_concurrent_w{workers}"] = summary(
                latency, wall
            )
            print(f"  get w={workers}: {summary(latency, wall)['per_s']} req/s", file=sys.stderr)

        # Bulk operations.
        bulk_lat = []
        started = time.perf_counter()
        for batch_no in range(10):
            docs = [
                {"_id": f"bulk-{batch_no}-{i:04d}", "value": i, "text": "x" * 100}
                for i in range(250)
            ]
            elapsed, _ = request(conn, "POST", "/perf/_bulk_docs", {"docs": docs})
            bulk_lat.append(elapsed)
        bulk_docs_wall = time.perf_counter() - started
        result["operations"]["bulk_docs_250"] = summary(
            bulk_lat, bulk_docs_wall, units=10 * 250
        )
        result["operations"]["bulk_docs_250"]["requests"] = summary(
            bulk_lat, bulk_docs_wall
        )

        bulk_get_lat = []
        started = time.perf_counter()
        for batch_no in range(10):
            docs = [
                {"id": f"doc-{i:06d}"}
                for i in range(batch_no * 250, (batch_no + 1) * 250)
            ]
            elapsed, _ = request(conn, "POST", "/perf/_bulk_get", {"docs": docs})
            bulk_get_lat.append(elapsed)
        bulk_get_wall = time.perf_counter() - started
        result["operations"]["bulk_get_250"] = summary(
            bulk_get_lat, bulk_get_wall, units=10 * 250
        )
        result["operations"]["bulk_get_250"]["requests"] = summary(
            bulk_get_lat, bulk_get_wall
        )

        # Metadata / range endpoints.
        all_docs_lat = []
        started = time.perf_counter()
        for _ in range(50):
            elapsed, _ = request(conn, "GET", "/perf/_all_docs?limit=100")
            all_docs_lat.append(elapsed)
        result["operations"]["all_docs_limit_100"] = summary(
            all_docs_lat, time.perf_counter() - started
        )

        changes_lat = []
        started = time.perf_counter()
        for _ in range(20):
            elapsed, _ = request(conn, "GET", "/perf/_changes?since=0&limit=1000")
            changes_lat.append(elapsed)
        result["operations"]["changes_since_0_limit_1000"] = summary(
            changes_lat, time.perf_counter() - started
        )

        info_lat = []
        started = time.perf_counter()
        for _ in range(200):
            elapsed, _ = request(conn, "GET", "/perf")
            info_lat.append(elapsed)
        result["operations"]["db_info"] = summary(info_lat, time.perf_counter() - started)

        # View: first query builds/persists the index, subsequent queries reuse it.
        ddoc = {
            "views": {
                "by_value": {
                    "map": "function(doc){ if (typeof doc.value === 'number') emit(doc.value, 1); }",
                    "reduce": "_count",
                }
            }
        }
        request(conn, "PUT", "/perf/_design/perf", ddoc)
        cold_elapsed, _ = request(conn, "GET", "/perf/_design/perf/_view/by_value?limit=100")
        warm_lat = []
        started = time.perf_counter()
        for _ in range(20):
            elapsed, _ = request(conn, "GET", "/perf/_design/perf/_view/by_value?limit=100")
            warm_lat.append(elapsed)
        result["operations"]["view_map_cold"] = {"n": 1, "latency_ms": {"p50": round(cold_elapsed, 3)}}
        result["operations"]["view_map_warm"] = summary(
            warm_lat, time.perf_counter() - started
        )

        map_only_lat = []
        started = time.perf_counter()
        for _ in range(20):
            elapsed, _ = request(
                conn, "GET", "/perf/_design/perf/_view/by_value?reduce=false&limit=100"
            )
            map_only_lat.append(elapsed)
        result["operations"]["view_map_only_warm"] = summary(
            map_only_lat, time.perf_counter() - started
        )

        reduce_lat = []
        started = time.perf_counter()
        for _ in range(20):
            elapsed, _ = request(conn, "GET", "/perf/_design/perf/_view/by_value?reduce=true")
            reduce_lat.append(elapsed)
        result["operations"]["view_reduce_warm"] = summary(
            reduce_lat, time.perf_counter() - started
        )

        # Large documents (1 MiB string).
        payload = "x" * (1024 * 1024)
        large_put = []
        for i in range(10):
            elapsed, _ = request(conn, "PUT", f"/perf/large-{i}", {"blob": payload})
            large_put.append(elapsed)
        large_get = []
        for i in range(10):
            elapsed, _ = request(conn, "GET", f"/perf/large-{i}")
            large_get.append(elapsed)
        result["operations"]["large_doc_1m_put"] = {
            "n": 10,
            "latency_ms": {
                "p50": round(percentile(large_put, 50), 3),
                "p99": round(percentile(large_put, 99), 3),
            },
        }
        result["operations"]["large_doc_1m_get"] = {
            "n": 10,
            "latency_ms": {
                "p50": round(percentile(large_get, 50), 3),
                "p99": round(percentile(large_get, 99), 3),
            },
        }

        # Storage and compaction.
        size_before = subprocess.check_output(["du", "-sb", args.data], text=True).split()[0]
        compact_elapsed, _ = request(conn, "POST", "/perf/_compact", {})
        size_after = subprocess.check_output(["du", "-sb", args.data], text=True).split()[0]
        result["storage"] = {
            "data_dir_bytes_before_compact": int(size_before),
            "data_dir_bytes_after_compact": int(size_after),
            "compact_seconds": round(compact_elapsed / 1000.0, 3),
        }

        result["environment"] = {
            "cpu_count": os.cpu_count(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        }
        conn.close()
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        log.close()

    os.makedirs(os.path.dirname(args.json_out) or ".", exist_ok=True)
    with open(args.json_out, "w") as handle:
        json.dump(result, handle, indent=2)
        handle.write("\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Local multi-node douchdb HTTP benchmark.

Starts a real cluster (default: 3 nodes, q=4, n=3), loads documents through a
coordinator, and measures the paths that fan out across nodes: document CRUD,
bulk operations, `_all_docs` (range and keys), `_changes`, database info and
map/reduce views.

    cargo build --release
    python3 scripts/perf_cluster.py --docs 6000

The result is written to target/perf-cluster.json.
"""

import argparse
import http.client
import json
import os
import platform
import shutil
import signal
import socket
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


def request(conn, method, path, body=None):
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
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def wait_ready(port, proc):
    deadline = time.time() + 30
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server on {port} exited with {proc.returncode}")
        try:
            conn = connect(port)
            request(conn, "GET", "/_up")
            conn.close()
            return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"server on {port} did not start")


def run_workers(ports, workers, total, fn):
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

    def worker(lo, hi, port):
        conn = connect(port)
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

    ports_for_workers = [ports[i % len(ports)] for i in range(workers)]
    threads = [
        threading.Thread(target=worker, args=(*bound, ports_for_workers[i]))
        for i, bound in enumerate(bounds)
    ]
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
    parser.add_argument("--data", default="target/perf-cluster-data")
    parser.add_argument("--docs", type=int, default=6000)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--q", type=int, default=4)
    parser.add_argument("--n", type=int, default=3)
    parser.add_argument("--sync-secs", default="3600")
    parser.add_argument(
        "--spread",
        action="store_true",
        help="round-robin benchmark clients across every coordinator port",
    )
    parser.add_argument(
        "--native",
        action="store_true",
        help="also run the native Rust loadgen against the seeded documents",
    )
    parser.add_argument("--loadgen-bin", default="target/release/loadgen")
    parser.add_argument("--native-count", type=int, default=5000)
    parser.add_argument("--native-path", default="/perf/doc-{id}")
    parser.add_argument("--workers", default="8")
    parser.add_argument("--per-level", type=int, default=500)
    parser.add_argument("--json-out", default="target/perf-cluster.json")
    args = parser.parse_args()

    nodes = args.nodes
    ports = [free_port() for _ in range(nodes)]
    urls = [f"http://127.0.0.1:{port}" for port in ports]
    node_list = ",".join(urls)
    binary = os.path.abspath(args.bin)

    shutil.rmtree(args.data, ignore_errors=True)
    os.makedirs(args.data, exist_ok=True)
    processes = []
    logs = []
    result = {
        "environment": {
            "cpu_count": os.cpu_count(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
        "config": {
            "nodes": nodes,
            "q": args.q,
            "n": args.n,
            "docs": args.docs,
            "sync_secs": args.sync_secs,
            "spread": args.spread,
            "native": args.native,
            "coordinator_port": ports[0],
        },
        "operations": {},
    }

    try:
        for index, port in enumerate(ports):
            log_path = os.path.join(args.data, f"node{index}.log")
            log = open(log_path, "wb")
            logs.append(log)
            proc = subprocess.Popen(
                [
                    binary,
                    "--listen",
                    f"127.0.0.1:{port}",
                    "--data",
                    os.path.join(args.data, f"node{index}"),
                    "--q",
                    str(args.q),
                    "--n",
                    str(args.n),
                    "--nodes",
                    node_list,
                    "--me",
                    str(index),
                    "--sync-secs",
                    args.sync_secs,
                    "--cluster-secret",
                    "perf-cluster-secret",
                ],
                stdout=log,
                stderr=log,
            )
            processes.append(proc)
            wait_ready(port, proc)

        bench_ports = ports if args.spread else [ports[0]]
        conns = [connect(port) for port in bench_ports]
        conn = conns[0]
        request(conn, "PUT", "/perf", {})
        print(
            f"{nodes}-node cluster ready (q={args.q}, n={args.n}, spread={args.spread}); "
            f"loading {args.docs} docs...",
            file=sys.stderr,
        )

        # Setup is deliberately not part of the benchmark.
        batch = 500
        for start in range(0, args.docs, batch):
            docs = [
                {
                    "_id": f"doc-{i:06d}",
                    "value": i,
                    "text": "x" * 100,
                }
                for i in range(start, min(start + batch, args.docs))
            ]
            request(conn, "POST", "/perf/_bulk_docs", {"docs": docs})

        if args.native:
            loadgen = os.path.abspath(args.loadgen_bin)
            completed = subprocess.run(
                [
                    loadgen,
                    "--base",
                    ",".join(urls),
                    "--path",
                    args.native_path,
                    "--start",
                    "0",
                    "--count",
                    str(args.native_count),
                    "--concurrency",
                    str(max(int(w) for w in args.workers.split(",") if w)),
                ],
                capture_output=True,
                text=True,
            )
            if completed.returncode != 0:
                raise RuntimeError(f"loadgen failed: {completed.stderr}")
            native = json.loads(completed.stdout.strip().splitlines()[-1])
            result["operations"]["get_native"] = native
            print(
                f"  native get: {native['per_s']} req/s p50={native['latency_ms']['p50']} ms",
                file=sys.stderr,
            )

        # Single-document writes, spread across coordinators when requested.
        seq_lat = []
        started = time.perf_counter()
        for i in range(500):
            elapsed, _ = put_doc(conns[i % len(conns)], f"seq-{i:06d}", i, "x" * 100)
            seq_lat.append(elapsed)
        result["operations"]["put_single_sequential"] = summary(
            seq_lat, time.perf_counter() - started
        )

        for workers in [int(w) for w in args.workers.split(",") if w]:
            def write(conn, i, workers=workers):
                return put_doc(conn, f"w{workers}-{i:06d}", i, "x" * 100)

            latency, wall = run_workers(bench_ports, workers, args.per_level, write)
            result["operations"][f"put_single_concurrent_w{workers}"] = summary(
                latency, wall
            )
            print(f"  put w={workers}: {summary(latency, wall)['per_s']} req/s", file=sys.stderr)

        def read(conn, i):
            return get_doc(conn, i)

        latency, wall = run_workers(bench_ports, 1, 1000, read)
        result["operations"]["get_single_sequential"] = summary(latency, wall)
        for workers in [int(w) for w in args.workers.split(",") if w]:
            latency, wall = run_workers(bench_ports, workers, args.per_level, read)
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
            elapsed, _ = request(
                conns[batch_no % len(conns)], "POST", "/perf/_bulk_docs", {"docs": docs}
            )
            bulk_lat.append(elapsed)
        bulk_wall = time.perf_counter() - started
        result["operations"]["bulk_docs_250"] = summary(
            bulk_lat, bulk_wall, units=10 * 250
        )
        result["operations"]["bulk_docs_250"]["requests"] = summary(bulk_lat, bulk_wall)

        bulk_get_lat = []
        started = time.perf_counter()
        for batch_no in range(10):
            docs = [
                {"id": f"doc-{i:06d}"}
                for i in range(batch_no * 250, (batch_no + 1) * 250)
            ]
            elapsed, _ = request(
                conns[batch_no % len(conns)], "POST", "/perf/_bulk_get", {"docs": docs}
            )
            bulk_get_lat.append(elapsed)
        bulk_get_wall = time.perf_counter() - started
        result["operations"]["bulk_get_250"] = summary(
            bulk_get_lat, bulk_get_wall, units=10 * 250
        )
        result["operations"]["bulk_get_250"]["requests"] = summary(
            bulk_get_lat, bulk_get_wall
        )

        # Cluster metadata endpoints.
        all_docs_lat = []
        started = time.perf_counter()
        for i in range(50):
            elapsed, _ = request(
                conns[i % len(conns)], "GET", "/perf/_all_docs?limit=100"
            )
            all_docs_lat.append(elapsed)
        result["operations"]["all_docs_limit_100"] = summary(
            all_docs_lat, time.perf_counter() - started
        )

        startkey_lat = []
        started = time.perf_counter()
        for i in range(50):
            elapsed, _ = request(
                conns[i % len(conns)],
                "GET",
                "/perf/_all_docs?startkey=%22doc-005000%22&limit=100",
            )
            startkey_lat.append(elapsed)
        result["operations"]["all_docs_startkey_limit_100"] = summary(
            startkey_lat, time.perf_counter() - started
        )

        keys = [f"doc-{i:06d}" for i in range(0, 100)]
        keys_lat = []
        started = time.perf_counter()
        for i in range(20):
            elapsed, _ = request(
                conns[i % len(conns)], "POST", "/perf/_all_docs", {"keys": keys}
            )
            keys_lat.append(elapsed)
        result["operations"]["all_docs_keys_100"] = summary(
            keys_lat, time.perf_counter() - started
        )

        # Local-first feeds maintain one sequence index per coordinator; warm
        # each node and each full-feed mode so the measurement is steady-state
        # rather than index/first-request overhead.
        for conn in conns:
            request(conn, "GET", "/perf/_changes?since=0&limit=1")
            for query in [
                "",
                "&style=all_docs",
                "&include_docs=true",
                "&style=all_docs&include_docs=true",
            ]:
                request(conn, "GET", f"/perf/_changes?since=0{query}")

        changes_lat = []
        started = time.perf_counter()
        for i in range(20):
            elapsed, _ = request(
                conns[i % len(conns)], "GET", "/perf/_changes?since=0&limit=1000"
            )
            changes_lat.append(elapsed)
        result["operations"]["changes_since_0_limit_1000"] = summary(
            changes_lat, time.perf_counter() - started
        )

        changes_all_lat = []
        started = time.perf_counter()
        for i in range(5):
            elapsed, _ = request(conns[i % len(conns)], "GET", "/perf/_changes?since=0")
            changes_all_lat.append(elapsed)
        result["operations"]["changes_since_0_all"] = summary(
            changes_all_lat, time.perf_counter() - started
        )

        for name, query in [
            ("changes_style_all_docs", "style=all_docs"),
            ("changes_include_docs", "include_docs=true"),
            ("changes_style_all_docs_include_docs", "style=all_docs&include_docs=true"),
        ]:
            latencies = []
            started = time.perf_counter()
            for i in range(5):
                elapsed, _ = request(
                    conns[i % len(conns)],
                    "GET",
                    f"/perf/_changes?since=0&{query}",
                )
                latencies.append(elapsed)
            result["operations"][name] = summary(
                latencies, time.perf_counter() - started
            )

        info_lat = []
        started = time.perf_counter()
        for i in range(100):
            elapsed, _ = request(conns[i % len(conns)], "GET", "/perf")
            info_lat.append(elapsed)
        result["operations"]["db_info_cluster"] = summary(
            info_lat, time.perf_counter() - started
        )

        # Views. Cold build happens on every node; warm queries are merged here.
        ddoc = {
            "views": {
                "by_value": {
                    "map": "function(doc){ if (typeof doc.value === 'number') emit(doc.value, 1); }",
                    "reduce": "_count",
                }
            }
        }
        request(conns[0], "PUT", "/perf/_design/perf", ddoc)
        cold_elapsed, _ = request(
            conns[0], "GET", "/perf/_design/perf/_view/by_value?limit=100"
        )
        # Shard-masked reads prefer the local replica, so every coordinator
        # needs its own warm view index before measuring steady-state latency.
        for conn in conns:
            request(conn, "GET", "/perf/_design/perf/_view/by_value?limit=100")
        warm_lat = []
        started = time.perf_counter()
        for i in range(20):
            elapsed, _ = request(
                conns[i % len(conns)],
                "GET",
                "/perf/_design/perf/_view/by_value?limit=100",
            )
            warm_lat.append(elapsed)
        result["operations"]["view_map_cold"] = {
            "n": 1,
            "latency_ms": {"p50": round(cold_elapsed, 3)},
        }
        result["operations"]["view_map_warm"] = summary(
            warm_lat, time.perf_counter() - started
        )

        map_only_lat = []
        started = time.perf_counter()
        for i in range(20):
            elapsed, _ = request(
                conns[i % len(conns)],
                "GET",
                "/perf/_design/perf/_view/by_value?reduce=false&limit=100",
            )
            map_only_lat.append(elapsed)
        result["operations"]["view_map_only_warm"] = summary(
            map_only_lat, time.perf_counter() - started
        )

        reduce_lat = []
        started = time.perf_counter()
        for i in range(20):
            elapsed, _ = request(
                conns[i % len(conns)],
                "GET",
                "/perf/_design/perf/_view/by_value?reduce=true",
            )
            reduce_lat.append(elapsed)
        result["operations"]["view_reduce_warm"] = summary(
            reduce_lat, time.perf_counter() - started
        )

        # A deliberately simple map that takes the native field-extraction
        # path instead of compiling and running QuickJS.
        native_ddoc = {
            "views": {
                "by_value": {
                    "map": "function(doc){ emit(doc.value, 1); }",
                    "reduce": "_count",
                }
            }
        }
        request(conns[0], "PUT", "/perf/_design/native", native_ddoc)
        native_cold, _ = request(
            conns[0], "GET", "/perf/_design/native/_view/by_value?limit=100"
        )
        for conn in conns:
            request(conn, "GET", "/perf/_design/native/_view/by_value?limit=100")
        native_lat = []
        started = time.perf_counter()
        for i in range(20):
            elapsed, _ = request(
                conns[i % len(conns)],
                "GET",
                "/perf/_design/native/_view/by_value?limit=100",
            )
            native_lat.append(elapsed)
        result["operations"]["view_native_map_cold"] = {
            "n": 1,
            "latency_ms": {"p50": round(native_cold, 3)},
        }
        result["operations"]["view_native_map_warm"] = summary(
            native_lat, time.perf_counter() - started
        )
        native_reduce_lat = []
        started = time.perf_counter()
        for i in range(20):
            elapsed, _ = request(
                conns[i % len(conns)],
                "GET",
                "/perf/_design/native/_view/by_value?reduce=true",
            )
            native_reduce_lat.append(elapsed)
        result["operations"]["view_native_reduce_warm"] = summary(
            native_reduce_lat, time.perf_counter() - started
        )

        # Semantically identical to the native map above, but the extra local
        # variable forces the QuickJS path.
        js_ddoc = {
            "views": {
                "by_value": {
                    "map": "function(doc){ var v = doc.value; emit(v, 1); }",
                    "reduce": "_count",
                }
            }
        }
        request(conns[0], "PUT", "/perf/_design/jsfx", js_ddoc)
        js_cold, _ = request(
            conns[0], "GET", "/perf/_design/jsfx/_view/by_value?limit=100"
        )
        for conn in conns:
            request(conn, "GET", "/perf/_design/jsfx/_view/by_value?limit=100")
        js_lat = []
        started = time.perf_counter()
        for i in range(20):
            elapsed, _ = request(
                conns[i % len(conns)],
                "GET",
                "/perf/_design/jsfx/_view/by_value?limit=100",
            )
            js_lat.append(elapsed)
        result["operations"]["view_js_map_cold"] = {
            "n": 1,
            "latency_ms": {"p50": round(js_cold, 3)},
        }
        result["operations"]["view_js_map_warm"] = summary(
            js_lat, time.perf_counter() - started
        )

        include_lat = []
        started = time.perf_counter()
        for i in range(20):
            elapsed, _ = request(
                conns[i % len(conns)],
                "GET",
                "/perf/_design/native/_view/by_value?reduce=false&include_docs=true&limit=100",
            )
            include_lat.append(elapsed)
        result["operations"]["view_include_docs_limit_100"] = summary(
            include_lat, time.perf_counter() - started
        )

        request(conns[0], "POST", "/perf/_ensure_full_commit", {})
        for conn in conns:
            conn.close()
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
        for proc in processes:
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for log in logs:
            log.close()

    os.makedirs(os.path.dirname(args.json_out) or ".", exist_ok=True)
    with open(args.json_out, "w") as handle:
        json.dump(result, handle, indent=2)
        handle.write("\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

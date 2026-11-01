#!/usr/bin/env python3
"""Compare native, process and WASM QuickJS engines for view/search work.

    cargo build --release
    python3 scripts/js_sandbox_bench.py --docs 2000

The map function uses a local variable so it cannot take the native Rust fast
path; every mode therefore executes the same JavaScript.
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
import time


def free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def request(conn, method, path, body=None):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    started = time.perf_counter()
    conn.request(method, path, body=data, headers=headers)
    response = conn.getresponse()
    raw = response.read()
    elapsed = (time.perf_counter() - started) * 1000.0
    if response.status >= 400:
        raise RuntimeError(f"{method} {path}: {response.status} {raw[:300]!r}")
    return elapsed, raw


def wait_ready(port, proc):
    deadline = time.time() + 60
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited with {proc.returncode}")
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            request(conn, "GET", "/_up")
            conn.close()
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("server did not start")


def percentile(values, pct):
    values = sorted(values)
    index = max(0, min(len(values) - 1, int(len(values) * pct / 100.0) - 1))
    return values[index]


def bench_mode(binary, mode, docs, queries):
    port = free_port()
    data = f"target/js-sandbox-{mode}"
    shutil.rmtree(data, ignore_errors=True)
    log = open(data + ".log", "wb")
    args = [
        os.path.abspath(binary),
        "--listen", f"127.0.0.1:{port}",
        "--data", data,
        "--q", "1",
        "--n", "1",
        "--sync-secs", "3600",
        "--js-sandbox", mode,
    ]
    proc = subprocess.Popen(args, stdout=log, stderr=log)
    try:
        wait_ready(port, proc)
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
        request(conn, "PUT", "/bench", {})
        batch = 500
        for start in range(0, docs, batch):
            request(conn, "POST", "/bench/_bulk_docs", {"docs": [
                {"_id": f"d{i:06d}", "value": i, "group": i % 5}
                for i in range(start, min(start + batch, docs))
            ]})
        request(conn, "PUT", "/bench/_design/v", {
            "views": {
                "computed": {
                    "map": "function(doc){ var n = doc.value + 1; emit(n, null); }",
                },
                "total": {
                    "map": "function(doc){ var one = 1; emit(doc.group, one); }",
                    "reduce": "function(keys, values, rereduce){ return sum(values); }",
                },
            },
            "indexes": {
                "values": {"index": "function(doc){ index('value', doc.value); }"},
            },
        })

        cold, _ = request(conn, "GET", "/bench/_design/v/_view/computed?reduce=false&limit=10")
        warm = []
        for _ in range(queries):
            elapsed, _ = request(conn, "GET", "/bench/_design/v/_view/computed?reduce=false&limit=10")
            warm.append(elapsed)

        reduce_cold, _ = request(conn, "GET", "/bench/_design/v/_view/total?group=true")
        reduce_warm = []
        for _ in range(queries):
            elapsed, _ = request(conn, "GET", "/bench/_design/v/_view/total?group=true")
            reduce_warm.append(elapsed)

        search_cold, _ = request(conn, "GET", "/bench/_design/v/_search/values?q=value:42&limit=1")
        search_warm = []
        for _ in range(queries):
            elapsed, _ = request(conn, "GET", "/bench/_design/v/_search/values?q=value:42&limit=1")
            search_warm.append(elapsed)
        conn.close()
        return {
            "mode": mode,
            "cold_map_ms": round(cold, 2),
            "warm_map_p50_ms": round(percentile(warm, 50), 2),
            "warm_map_p95_ms": round(percentile(warm, 95), 2),
            "cold_custom_reduce_ms": round(reduce_cold, 2),
            "warm_custom_reduce_p50_ms": round(percentile(reduce_warm, 50), 2),
            "cold_search_ms": round(search_cold, 2),
            "warm_search_p50_ms": round(percentile(search_warm, 50), 2),
        }
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()
        shutil.rmtree(data, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin", default="target/release/douchdb")
    parser.add_argument("--modes", default="native,process,wasm")
    parser.add_argument("--docs", type=int, default=2000)
    parser.add_argument("--queries", type=int, default=10)
    parser.add_argument("--json-out", default="target/js-sandbox-bench.json")
    args = parser.parse_args()

    results = []
    for mode in [mode for mode in args.modes.split(",") if mode]:
        print(f"benchmarking {mode}...", flush=True)
        results.append(bench_mode(args.bin, mode, args.docs, args.queries))
    payload = {"docs": args.docs, "queries": args.queries, "results": results}
    with open(args.json_out, "w") as output:
        json.dump(payload, output, indent=2)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Peer round trips per client request on a local three-node cluster.

Starts three douchdb processes (no Docker), drives node 0 with
`target/release/loadgen` one request at a time, and reads the coordinator's
peer-call accounting (`/_node/_local/_metrics`, see `cluster_http.rs`) before
and after each operation. For every operation it reports:

  calls    peer calls per request made by the coordinator
  rounds   sequential rounds per request (a round is a period with at least
           one peer call in flight, so a concurrent fan-out is one round)
  nested   peer calls per request made by the nodes the coordinator reached
  p50/p99  client-side latency in ms (loadgen, concurrency 1)
  wait     ms per request the coordinator spent waiting on peers
  other    mean client latency minus wait: local work, (de)serialization of
           bodies and the client round trip. The three are not separated.

Peer-call and round counts are exact and repeatable; latencies depend on the
machine's load and are indicative only.

    cargo build --release --bins
    timeout 600 python3 scripts/bench_peer_trips.py

Every run is bounded (`--max-seconds`, default 540) and every node process is
killed on exit, including on SIGINT/SIGTERM and on the time limit.
"""

import argparse
import http.client
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

PROCESSES = []


def cleanup():
    for proc in PROCESSES:
        if proc.poll() is None:
            proc.kill()
    for proc in PROCESSES:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
    PROCESSES.clear()


def die(signum, _frame):
    raise SystemExit(f"stopped by signal {signum}")


def free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def call(port, method, path, body=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    conn.request(method, path, body=data, headers=headers)
    response = conn.getresponse()
    raw = response.read()
    conn.close()
    if response.status >= 400:
        raise RuntimeError(f"{method} {path}: {response.status} {raw[:200]!r}")
    return json.loads(raw) if raw else None


def start_cluster(args, replication):
    ports = [free_port() for _ in range(3)]
    nodes = ",".join(f"http://127.0.0.1:{port}" for port in ports)
    root = os.path.join(args.data, f"n{replication}")
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root)
    for index, port in enumerate(ports):
        log = open(os.path.join(root, f"node{index}.log"), "wb")
        proc = subprocess.Popen(
            [
                args.bin,
                "--listen", f"127.0.0.1:{port}",
                "--data", os.path.join(root, f"node{index}"),
                "--q", "4",
                "--n", str(replication),
                "--nodes", nodes,
                "--me", str(index),
                "--sync-secs", "3600",
                "--cluster-secret", "bench-peer-trips",
            ] + args.node_arg,
            stdout=log,
            stderr=log,
        )
        PROCESSES.append(proc)
    deadline = time.time() + 30
    for port in ports:
        while True:
            try:
                call(port, "GET", "/_up")
                break
            except OSError:
                if time.time() > deadline:
                    raise RuntimeError(f"node on {port} did not start")
                time.sleep(0.1)
    return ports, PROCESSES[-3:]


def peer_connections(ports, procs):
    """Established TCP connections each node holds open to each other node.

    Counts the connections a node opened (its local port is an ephemeral one,
    the remote port is a node's listening port), per ordered pair, from lsof.
    Returns {"0->1": count, ...}. Nodes that are gone are skipped.
    """
    live = [(i, p.pid) for i, p in enumerate(procs) if p.poll() is None]
    counts = {}
    if not live:
        return counts
    out = subprocess.run(
        ["lsof", "-nP", "-a", "-p", ",".join(str(pid) for _, pid in live),
         "-iTCP", "-sTCP:ESTABLISHED", "-Fpn"],
        capture_output=True, text=True, timeout=30,
    ).stdout
    owner = None
    pids = {pid: i for i, pid in live}
    for line in out.splitlines():
        if line.startswith("p"):
            owner = pids.get(int(line[1:]))
        elif line.startswith("n") and owner is not None and "->" in line:
            local, remote = line[1:].split("->")
            lport = int(local.rsplit(":", 1)[1])
            rport = int(remote.rsplit(":", 1)[1])
            if rport in ports and lport not in ports:
                key = f"{owner}->{ports.index(rport)}"
                counts[key] = counts.get(key, 0) + 1
    return counts


def metrics(port):
    return call(port, "GET", "/_node/_local/_metrics")


def idle_snapshot(ports):
    """Per-node route rows once no request has peer accounting open."""
    out = []
    for port in ports:
        deadline = time.time() + 60
        while True:
            snap = metrics(port)
            if snap["http"]["peer_open"] == 0:
                break
            if time.time() > deadline:
                raise RuntimeError("peer accounting never closed")
            time.sleep(0.02)
        out.append({row["route"]: row for row in snap["http"]["routes"]})
    return out


def total(rows, field):
    return sum(row.get(field, 0) for row in rows.values())


def loadgen(args, ports, loadgen_args, count, concurrency=1):
    """Drive node 0 with the native load generator, one request at a time."""
    command = [
        args.loadgen, "--base", f"http://127.0.0.1:{ports[0]}",
        "--concurrency", str(concurrency), "--count", str(count),
    ] + loadgen_args
    done = subprocess.run(command, capture_output=True, text=True, timeout=120)
    if done.returncode != 0:
        raise RuntimeError(f"loadgen failed: {done.stderr.strip()}")
    return json.loads(done.stdout)


def paced_puts(port, prefix, count, gap):
    """`count` writes `gap` seconds apart, in loadgen's result shape."""
    latencies = []
    started = time.perf_counter()
    for i in range(count):
        time.sleep(gap)
        t = time.perf_counter()
        call(port, "PUT", f"/bench/{prefix}-{i}", {"n": i})
        latencies.append((time.perf_counter() - t) * 1000.0)
    latencies.sort()
    pick = lambda pct: latencies[max(0, int(len(latencies) * pct / 100) - 1)]
    mean = sum(latencies) / len(latencies)
    return {"wall_s": mean * count / 1000.0,
            "latency_ms": {"p50": round(pick(50), 3), "p99": round(pick(99), 3)}}


def measure(args, ports, label, route, drive, count):
    """Counters around `drive()`, which sends `count` requests to node 0."""
    before = idle_snapshot(ports)
    result = drive()
    after = idle_snapshot(ports)

    def delta(node, field):
        old = before[node].get(route, {}).get(field, 0)
        return after[node].get(route, {}).get(field, 0) - old

    calls = delta(0, "peer_calls") / count
    rounds = delta(0, "peer_rounds") / count
    wait = delta(0, "peer_wait_ms") / count
    nested = sum(
        total(after[i], "nested_calls") - total(before[i], "nested_calls")
        for i in range(len(ports))
    ) / count
    mean = result["wall_s"] * 1000.0 / count
    return {
        "operation": label,
        "requests": count,
        "calls": round(calls, 2),
        "rounds": round(rounds, 2),
        "nested": round(nested, 2),
        "p50_ms": result["latency_ms"]["p50"],
        "p99_ms": result["latency_ms"]["p99"],
        "mean_ms": round(mean, 3),
        "wait_ms": round(wait, 3),
        "other_ms": round(max(mean - wait, 0.0), 3),
    }


def seed(ports):
    call(ports[0], "PUT", "/bench", {})
    for start in range(0, 1000, 250):
        docs = [{"_id": f"seed-{i:05d}", "n": i} for i in range(start, start + 250)]
        call(ports[0], "POST", "/bench/_bulk_docs", {"docs": docs})
    call(ports[0], "PUT", "/bench/_design/v", {
        "views": {
            "by_n": {"map": "function (doc) { emit(doc.n, 1); }"},
            "total": {"map": "function (doc) { emit(doc.n, 1); }", "reduce": "_count"},
        }
    })
    # The first query of a view builds its index; keep that out of the numbers.
    for path in ("/bench/_design/v/_view/by_n?limit=1", "/bench/_design/v/_view/total"):
        call(ports[0], "GET", path)


def run_cluster(args, replication):
    ports, procs = start_cluster(args, replication)
    seed(ports)
    doc = "/{db}/doc"
    view = "/{db}/_design/*/_view/*"
    bulk_body = json.dumps({"docs": [{"_id": f"b{{id}}-{k}", "n": k} for k in range(100)]})
    n = args.requests
    ops = [
        ("PUT doc", doc, ["--method", "PUT", "--path", "/bench/put-{id}",
                          "--body", '{"n": "{id}"}'], n),
        ("_bulk_docs (100 docs)", "/{db}/_bulk_docs",
         ["--method", "POST", "--path", "/bench/_bulk_docs", "--body", bulk_body],
         max(n // 10, 10)),
        ("GET doc", doc, ["--path", "/bench/put-{id}"], n),
        ("_all_docs?limit=100", "/{db}/_all_docs",
         ["--path", "/bench/_all_docs?limit=100"], n),
        ("map view", view, ["--path", "/bench/_design/v/_view/by_n?limit=100"], n),
        ("reduce view", view, ["--path", "/bench/_design/v/_view/total"], n),
        ("_changes?since=now", "/{db}/_changes",
         ["--path", "/bench/_changes?since=now"], n),
        ("_changes?limit=100 (page)", "/{db}/_changes",
         ["--path", "/bench/_changes?limit=100"], n),
    ]
    rows = []
    # The most connections any node held to any one peer after an operation.
    peak = {}

    def sample():
        for key, value in peer_connections(ports, procs).items():
            peak[key] = max(peak.get(key, 0), value)

    for label, route, extra, count in ops:
        rows.append(measure(args, ports, label, route,
                            lambda: loadgen(args, ports, extra, count), count))
        sample()
        print(f"  {label}: {rows[-1]['calls']} calls, {rows[-1]['rounds']} rounds",
              file=sys.stderr)
    # Many requests at once: HTTP/1.1 needs a connection per request in
    # flight, HTTP/2 multiplexes them over one. Its mean is wall time per
    # request, so only the percentiles and the connection count mean much.
    extra = ["--method", "PUT", "--path", "/bench/conc-{id}", "--body", '{"n": "{id}"}']
    rows.append(measure(args, ports, "PUT doc, 16 at once", doc,
                        lambda: loadgen(args, ports, extra, n * 3, 16), n * 3))
    sample()
    # Cluster writes cache their validator set for a second (validate.rs), so
    # a burst of writes pays the validator fan-out only about once a second,
    # while a write that comes later than that pays it every time.
    rows.append(measure(args, ports, "PUT doc, validators expired", doc,
                        lambda: paced_puts(ports[0], "sparse", 5, 1.1), 5))
    sample()
    # Take node 2 down (by PID) and write through node 0. With fewer than
    # three copies some writes would simply fail, so only n=3 does this. The
    # dead node's counters are unreadable, so only the live nodes are compared.
    if replication >= 3:
        procs[2].kill()
        procs[2].wait(timeout=10)
        time.sleep(1.5)  # let the validator cache from before the kill expire
        extra = ["--method", "PUT", "--path", "/bench/down-{id}", "--body", '{"n": "{id}"}']
        rows.append(measure(args, ports[:2], "PUT doc, one node down", doc,
                            lambda: loadgen(args, ports, extra, n), n))
    cleanup()
    return rows, peak


def table(rows):
    head = ("operation", "calls", "rounds", "nested", "p50 ms", "p99 ms",
            "wait ms", "other ms")
    lines = ["| " + " | ".join(head) + " |",
             "|---|" + "---:|" * (len(head) - 1)]
    for row in rows:
        lines.append("| {operation} | {calls} | {rounds} | {nested} | {p50_ms} | "
                     "{p99_ms} | {wait_ms} | {other_ms} |".format(**row))
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--bin", default="target/release/douchdb")
    parser.add_argument("--loadgen", default="target/release/loadgen")
    parser.add_argument("--data", default="target/bench-peer-trips")
    parser.add_argument("--requests", type=int, default=300,
                        help="requests per operation (a tenth for _bulk_docs)")
    parser.add_argument("--replication", default="3,1",
                        help="comma-separated n values, one cluster each")
    parser.add_argument("--max-seconds", type=int, default=540)
    parser.add_argument("--node-arg", action="append", default=[],
                        help="extra argument for every node (repeatable)")
    parser.add_argument("--json-out", default=None)
    args = parser.parse_args()
    args.bin = os.path.abspath(args.bin)
    args.loadgen = os.path.abspath(args.loadgen)

    for name in (signal.SIGINT, signal.SIGTERM, signal.SIGALRM):
        signal.signal(name, die)
    signal.alarm(args.max_seconds)
    results = {}
    connections = {}
    try:
        for replication in [int(v) for v in args.replication.split(",")]:
            print(f"n={replication}", file=sys.stderr)
            results[replication], connections[replication] = run_cluster(args, replication)
    finally:
        signal.alarm(0)
        cleanup()
    for replication, rows in results.items():
        print(f"\n3 nodes, q=4, n={replication}; counts per request\n")
        print(table(rows))
        peak = connections.get(replication, {})
        pairs = ", ".join(f"{k}: {v}" for k, v in sorted(peak.items())) or "none"
        print(f"\nestablished TCP connections per ordered node pair (peak after "
              f"an operation): {pairs}")
    if args.json_out:
        with open(args.json_out, "w") as out:
            json.dump({"rows": results, "connections": connections}, out, indent=2)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Isolated native cluster scaling benchmark.

Starts 1-3 douchdb nodes with server CPU sets and runs the native Rust
`loadgen` on a disjoint client CPU set. This avoids the single-host
client/server CPU contention that caps the uninstrumented measurements.

    cargo build --release --bin douchdb --bin loadgen
    python3 scripts/perf_isolated.py --nodes 1,2,3

Results are printed and written to target/perf-isolated.json.
"""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from perf_cluster import connect, free_port, request, summary, wait_ready  # noqa: E402


def run_cluster(nodes, args):
    ports = [free_port() for _ in range(nodes)]
    urls = [f"http://127.0.0.1:{port}" for port in ports]
    node_list = ",".join(urls)
    data = Path(args.data) / f"n{nodes}"
    shutil.rmtree(data, ignore_errors=True)
    data.mkdir(parents=True, exist_ok=True)
    processes = []
    logs = []
    try:
        for index, port in enumerate(ports):
            log = open(data / f"node{index}.log", "wb")
            logs.append(log)
            server_cpus = f"{index * args.cpus_per_node}-{(index + 1) * args.cpus_per_node - 1}"
            proc = subprocess.Popen(
                [
                    "taskset",
                    "-c",
                    server_cpus,
                    os.path.abspath(args.bin),
                    "--listen",
                    f"127.0.0.1:{port}",
                    "--data",
                    str(data / f"node{index}"),
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
                    "isolated-scaling-secret-123456",
                ],
                stdout=log,
                stderr=log,
            )
            processes.append(proc)
            wait_ready(port, proc)

        conn = connect(ports[0])
        request(conn, "PUT", f"/{args.db}", {})
        batch = 500
        for start in range(0, args.docs, batch):
            docs = [
                {"_id": f"doc-{i:06d}", "value": i, "text": "x" * 100}
                for i in range(start, min(start + batch, args.docs))
            ]
            request(conn, "POST", f"/{args.db}/_bulk_docs", {"docs": docs})
        conn.close()

        # Let the synchronous replica writes settle before measuring.
        time.sleep(1.0)
        results = {}
        for mode, path in [
            ("r1", f"/{args.db}/doc-{{id}}?r=1"),
            ("default_r", f"/{args.db}/doc-{{id}}"),
        ]:
            samples = []
            for _ in range(args.rounds):
                command = [
                    "taskset",
                    "-c",
                    args.client_cpus,
                    os.path.abspath(args.loadgen),
                    "--base",
                    ",".join(urls),
                    "--path",
                    path,
                    "--start",
                    "0",
                    "--count",
                    str(args.docs),
                    "--concurrency",
                    str(args.concurrency),
                ]
                completed = subprocess.run(command, capture_output=True, text=True)
                if completed.returncode != 0:
                    raise RuntimeError(f"loadgen failed: {completed.stderr}")
                samples.append(json.loads(completed.stdout.strip().splitlines()[-1]))
            results[mode] = {
                "per_s": round(
                    sorted(sample["per_s"] for sample in samples)[len(samples) // 2]
                ),
                "p50_ms": round(
                    sorted(sample["latency_ms"]["p50"] for sample in samples)[
                        len(samples) // 2
                    ],
                    3,
                ),
                "samples": samples,
            }
        return {"nodes": nodes, "urls": urls, "operations": results}
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin", default="target/release/douchdb")
    parser.add_argument("--loadgen", default="target/release/loadgen")
    parser.add_argument("--data", default="target/perf-isolated-data")
    parser.add_argument("--nodes", default="1,2,3")
    parser.add_argument("--q", type=int, default=4)
    parser.add_argument("--n", type=int, default=3)
    parser.add_argument("--docs", type=int, default=12000)
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--cpus-per-node", type=int, default=2)
    parser.add_argument("--client-cpus", default="8-15")
    parser.add_argument("--sync-secs", default="3600")
    parser.add_argument("--db", default="scale")
    parser.add_argument("--json-out", default="target/perf-isolated.json")
    args = parser.parse_args()

    node_counts = [int(value) for value in args.nodes.split(",") if value]
    result = {
        "config": vars(args),
        "environment": {
            "cpus": os.cpu_count(),
        },
        "runs": [],
    }
    for nodes in node_counts:
        print(f"=== isolated {nodes}-node cluster ===", file=sys.stderr)
        run = run_cluster(nodes, args)
        result["runs"].append(run)
        for mode, metrics in run["operations"].items():
            print(
                f"  {mode}: {metrics['per_s']} req/s p50={metrics['p50_ms']} ms",
                file=sys.stderr,
            )

    os.makedirs(os.path.dirname(args.json_out) or ".", exist_ok=True)
    with open(args.json_out, "w") as handle:
        json.dump(result, handle, indent=2)
        handle.write("\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Benchmark a running 3-node CouchDB or DouchDB cluster.

The matching compose files live in docker/bench/. Bring one cluster up at a
time, then run:

    python3 scripts/docker_compare.py --engine couchdb --json-out couch.json
    python3 scripts/docker_compare.py --engine douchdb --json-out douch.json
"""
import argparse
import base64
import http.client
import json
import threading
import time
import urllib.parse

COUCH = {
    "name": "couchdb",
    "hosts": [("127.0.0.1", 15984), ("127.0.0.1", 15985), ("127.0.0.1", 15986)],
    "auth": "Basic " + base64.b64encode(b"admin:password").decode(),
    "create": "/bench?q=8&n=3",
}
DOUCH = {
    "name": "douchdb",
    "hosts": [("127.0.0.1", 25984), ("127.0.0.1", 25985), ("127.0.0.1", 25986)],
    "auth": "Basic " + base64.b64encode(b"admin:secret").decode(),
    "create": "/bench",
}


class Conn:
    def __init__(self, engine):
        self.engine = engine
        self.host, self.port = engine["hosts"][0]
        self.conn = http.client.HTTPConnection(self.host, self.port, timeout=180)
        self.headers = {"Authorization": engine["auth"], "Connection": "keep-alive"}
        self.index = 0

    def rotate(self):
        self.index = (self.index + 1) % len(self.engine["hosts"])
        self.host, self.port = self.engine["hosts"][self.index]
        self.conn.close()
        self.conn = http.client.HTTPConnection(self.host, self.port, timeout=180)

    def request(self, method, path, body=None, headers=None):
        data = None
        h = dict(self.headers)
        if body is not None:
            data = json.dumps(body).encode()
            h["Content-Type"] = "application/json"
        if headers:
            h.update(headers)
        started = time.perf_counter()
        self.conn.request(method, path, body=data, headers=h)
        response = self.conn.getresponse()
        raw = response.read()
        elapsed = (time.perf_counter() - started) * 1000.0
        if response.status >= 400:
            raise RuntimeError(f"{method} {path}: {response.status} {raw[:500]!r}")
        return elapsed, raw

    def close(self):
        self.conn.close()


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


def wait_ready(engine):
    for _ in range(120):
        try:
            conn = Conn(engine)
            conn.request("GET", "/")
            conn.close()
            return
        except Exception:
            time.sleep(0.5)
    raise SystemExit(f"{engine['name']} did not become ready")


def reset_db(engine):
    conn = Conn(engine)
    try:
        conn.request("DELETE", "/bench")
    except Exception:
        pass
    for _ in range(50):
        try:
            conn.request("GET", "/bench")
        except Exception:
            break
        time.sleep(0.1)
    conn.request("PUT", engine["create"])
    for _ in range(50):
        try:
            conn.request("GET", "/bench")
            break
        except Exception:
            time.sleep(0.1)
    conn.close()
    time.sleep(1.0)


def parallel(engine, count, workers, fn):
    latencies = []
    errors = []
    lock = threading.Lock()

    def worker(worker_id):
        local = []
        conn = Conn(engine)
        for _ in range(worker_id % len(engine["hosts"])):
            conn.rotate()
        try:
            for slot in range(worker_id, count, workers):
                elapsed, raw = fn(conn, slot)
                local.append(elapsed)
                with lock:
                    latencies.append(elapsed)
        except Exception as error:
            with lock:
                errors.append(str(error))
        finally:
            conn.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise RuntimeError(errors[0])
    return latencies


def bench_engine(engine, docs):
    wait_ready(engine)
    reset_db(engine)
    conns = [Conn(engine) for _ in engine["hosts"]]
    rr = [0]

    def seq(method, path, body=None):
        conn = conns[rr[0] % len(conns)]
        rr[0] += 1
        return conn.request(method, path, body)

    result = {"engine": engine["name"], "docs": docs, "operations": {}}

    # Seed.
    batch = 500
    seed_started = time.perf_counter()
    for start in range(0, docs, batch):
        payload = [
            {
                "_id": f"doc-{i:06d}",
                "value": i,
                "text": "x" * 100,
            }
            for i in range(start, min(start + batch, docs))
        ]
        seq("POST", "/bench/_bulk_docs", {"docs": payload})
    result["seed"] = summary([], time.perf_counter() - seed_started, docs)
    time.sleep(1.0)

    # Sequential writes.
    lat = []
    started = time.perf_counter()
    for i in range(500):
        elapsed, _ = seq("PUT", f"/bench/write-{i:05d}", {"value": i, "text": "x" * 100})
        lat.append(elapsed)
    result["operations"]["put_single_sequential"] = summary(lat, time.perf_counter() - started)

    # Concurrent writes.
    started = time.perf_counter()
    lat = parallel(
        engine,
        500,
        8,
        lambda c, i: c.request("PUT", f"/bench/cwrite-{i:05d}", {"value": i, "text": "x" * 100}),
    )
    result["operations"]["put_single_concurrent_w8"] = summary(
        lat, time.perf_counter() - started
    )

    # Bulk writes.
    lat = []
    started = time.perf_counter()
    for b in range(10):
        payload = [
            {"_id": f"bulk-{b:02d}-{i:03d}", "value": i, "text": "x" * 100}
            for i in range(250)
        ]
        elapsed, _ = seq("POST", "/bench/_bulk_docs", {"docs": payload})
        lat.append(elapsed)
    result["operations"]["bulk_docs_250"] = summary(lat, time.perf_counter() - started, 2500)
    result["operations"]["bulk_docs_250"]["requests"] = summary(
        lat, time.perf_counter() - started
    )

    # Sequential point reads.
    lat = []
    started = time.perf_counter()
    for i in range(1000):
        elapsed, _ = seq("GET", f"/bench/doc-{i:06d}")
        lat.append(elapsed)
    result["operations"]["get_single_sequential"] = summary(
        lat, time.perf_counter() - started
    )

    # Concurrent point reads.
    started = time.perf_counter()
    lat = parallel(
        engine,
        500,
        8,
        lambda c, i: c.request("GET", f"/bench/doc-{i:06d}"),
    )
    result["operations"]["get_single_concurrent_w8"] = summary(
        lat, time.perf_counter() - started
    )

    # Bulk get.
    lat = []
    started = time.perf_counter()
    for b in range(10):
        reqs = [{"id": f"doc-{b * 250 + i:06d}"} for i in range(250)]
        elapsed, _ = seq("POST", "/bench/_bulk_get", {"docs": reqs})
        lat.append(elapsed)
    result["operations"]["bulk_get_250"] = summary(lat, time.perf_counter() - started, 2500)
    result["operations"]["bulk_get_250"]["requests"] = summary(
        lat, time.perf_counter() - started
    )

    # _all_docs.
    lat = []
    started = time.perf_counter()
    for _ in range(50):
        elapsed, _ = seq("GET", "/bench/_all_docs?limit=100")
        lat.append(elapsed)
    result["operations"]["all_docs_limit_100"] = summary(lat, time.perf_counter() - started)

    startkey = urllib.parse.quote(json.dumps("doc-003000"))
    lat = []
    started = time.perf_counter()
    for _ in range(50):
        elapsed, _ = seq("GET", f"/bench/_all_docs?startkey={startkey}&limit=100")
        lat.append(elapsed)
    result["operations"]["all_docs_startkey_limit_100"] = summary(
        lat, time.perf_counter() - started
    )

    keys = [f"doc-{i:06d}" for i in range(100)]
    lat = []
    started = time.perf_counter()
    for _ in range(20):
        elapsed, _ = seq("POST", "/bench/_all_docs", {"keys": keys})
        lat.append(elapsed)
    result["operations"]["all_docs_keys_100"] = summary(lat, time.perf_counter() - started)

    # Warm changes caches on every coordinator.
    for conn in conns:
        conn.request("GET", "/bench/_changes?since=0&limit=1")

    lat = []
    started = time.perf_counter()
    for _ in range(20):
        elapsed, _ = seq("GET", "/bench/_changes?since=0&limit=1000")
        lat.append(elapsed)
    result["operations"]["changes_since_0_limit_1000"] = summary(
        lat, time.perf_counter() - started
    )

    for name, query in [
        ("changes_since_0_all", ""),
        ("changes_style_all_docs", "&style=all_docs"),
        ("changes_include_docs", "&include_docs=true"),
        ("changes_style_all_docs_include_docs", "&style=all_docs&include_docs=true"),
    ]:
        lat = []
        started = time.perf_counter()
        for _ in range(5):
            elapsed, _ = seq("GET", f"/bench/_changes?since=0{query}")
            lat.append(elapsed)
        result["operations"][name] = summary(lat, time.perf_counter() - started)

    # DB info.
    lat = []
    started = time.perf_counter()
    for _ in range(100):
        elapsed, _ = seq("GET", "/bench")
        lat.append(elapsed)
    result["operations"]["db_info"] = summary(lat, time.perf_counter() - started)

    # Views.
    ddoc = {
        "views": {
            "by_value": {
                "map": "function(doc){ if (typeof doc.value === 'number') emit(doc.value, 1); }",
                "reduce": "_count",
            }
        }
    }
    try:
        seq("DELETE", "/bench/_design/perf")
    except Exception:
        pass
    seq("PUT", "/bench/_design/perf", ddoc)
    cold, _ = seq("GET", "/bench/_design/perf/_view/by_value?limit=100")
    # Warm every coordinator's view index.
    for conn in conns:
        conn.request("GET", "/bench/_design/perf/_view/by_value?limit=100")
    result["operations"]["view_map_cold"] = {"n": 1, "latency_ms": {"p50": round(cold, 3)}}

    lat = []
    started = time.perf_counter()
    for _ in range(20):
        elapsed, _ = seq("GET", "/bench/_design/perf/_view/by_value?limit=100")
        lat.append(elapsed)
    result["operations"]["view_map_warm"] = summary(lat, time.perf_counter() - started)

    lat = []
    started = time.perf_counter()
    for _ in range(20):
        elapsed, _ = seq("GET", "/bench/_design/perf/_view/by_value?reduce=false&limit=100")
        lat.append(elapsed)
    result["operations"]["view_map_only_warm"] = summary(
        lat, time.perf_counter() - started
    )

    lat = []
    started = time.perf_counter()
    for _ in range(20):
        elapsed, _ = seq("GET", "/bench/_design/perf/_view/by_value?reduce=true")
        lat.append(elapsed)
    result["operations"]["view_reduce_warm"] = summary(
        lat, time.perf_counter() - started
    )

    for conn in conns:
        conn.close()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["couchdb", "douchdb"], required=True)
    parser.add_argument("--docs", type=int, default=6000)
    parser.add_argument("--json-out", default=None)
    args = parser.parse_args()
    engine = COUCH if args.engine == "couchdb" else DOUCH
    result = bench_engine(engine, args.docs)
    text = json.dumps(result, indent=2)
    if args.json_out:
        with open(args.json_out, "w") as handle:
            handle.write(text + "\n")
    print(text)


if __name__ == "__main__":
    main()

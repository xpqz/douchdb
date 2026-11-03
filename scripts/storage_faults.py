#!/usr/bin/env python3
"""Storage corruption and fail-closed checks for douchdb.

Each scenario writes a known document set, stops the server, damages one
LevelDB file (or kills during compaction), and restarts. The rule is simple:

  * all acknowledged documents are still readable, or
  * the server refuses to start.

Starting with a missing or partial document set is a failure. This complements
the 10 GB storage spike, which only covers a clean SIGKILL.
"""

import argparse
import http.client
import json
import os
import random
import shutil
import signal
import subprocess
import tempfile
import time
from pathlib import Path

DOCS = 200


def connect(port, timeout=10):
    return http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)


def request(port, method, path, body=None, timeout=10):
    conn = connect(port, timeout)
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    conn.request(method, path, body=data, headers=headers)
    response = conn.getresponse()
    raw = response.read()
    conn.close()
    return response.status, raw


def wait_ready(port, proc, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            return False
        try:
            status, _ = request(port, "GET", "/", timeout=1)
            if status == 200:
                return True
        except OSError:
            pass
        time.sleep(0.05)
    return False


def start(binary, data, port, log):
    handle = open(log, "ab", buffering=0)
    proc = subprocess.Popen(
        [
            binary,
            "--listen",
            f"127.0.0.1:{port}",
            "--data",
            data,
            "--q",
            "1",
            "--n",
            "1",
            "--sync-secs",
            "3600",
        ],
        stdout=handle,
        stderr=handle,
    )
    return proc, handle


def stop(proc, sig=signal.SIGKILL):
    if proc is None or proc.poll() is not None:
        return
    proc.send_signal(sig)
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def seed(binary, data, port, log):
    proc, handle = start(binary, data, port, log)
    try:
        if not wait_ready(port, proc):
            raise RuntimeError(f"server did not start: {log}")
        request(port, "PUT", "/faults")
        docs = [
            {"_id": f"doc-{i:04}", "n": i, "s": f"payload-{i}", "blob": "x" * 200}
            for i in range(DOCS)
        ]
        for offset in range(0, len(docs), 25):
            status, raw = request(
                port, "POST", "/faults/_bulk_docs", {"docs": docs[offset : offset + 25]}
            )
            if status != 201:
                raise RuntimeError(f"seed failed: {status} {raw[:200]}")
        status, raw = request(port, "POST", "/faults/_compact")
        if status not in (200, 202):
            raise RuntimeError(f"compact failed: {status} {raw[:200]}")
        deadline = time.time() + 20
        while time.time() < deadline:
            _, raw = request(port, "GET", "/faults")
            if not json.loads(raw).get("compact_running"):
                break
            time.sleep(0.05)
        else:
            raise RuntimeError("compaction did not finish")
    finally:
        stop(proc)
        handle.close()


def tail(log):
    try:
        text = Path(log).read_text(errors="replace")
    except OSError:
        return "<no log>"
    return text[-2000:]


def verify(binary, data, port, log):
    """Returns ('refused'|'ok'|'corrupt', detail)."""
    proc, handle = start(binary, data, port, log)
    try:
        if not wait_ready(port, proc):
            return "refused", f"exit={proc.poll()} (see {log})"
        try:
            status, raw = request(port, "GET", "/faults")
        except (OSError, http.client.HTTPException) as error:
            if proc.poll() is not None:
                return "refused", f"exit={proc.poll()} opening database ({error})\n{tail(log)}"
            return "corrupt", f"connection failed while serving /faults: {error}\n{tail(log)}"
        if status != 200:
            if status >= 500:
                return "refused", f"GET /faults -> {status} {raw[:200]}"
            return "corrupt", f"GET /faults -> {status} {raw[:200]}"
        info = json.loads(raw)
        if info.get("doc_count") != DOCS:
            return "corrupt", f"doc_count={info.get('doc_count')} expected {DOCS}"
        for i in range(DOCS):
            status, raw = request(port, "GET", f"/faults/doc-{i:04}")
            if status != 200:
                if status >= 500:
                    return "refused", f"doc-{i:04} -> {status} {raw[:120]}"
                return "corrupt", f"doc-{i:04} -> {status} {raw[:120]}"
        return "ok", f"all {DOCS} documents readable"
    finally:
        stop(proc)
        handle.close()


def leveldb_files(data):
    return sorted(
        [p for p in Path(data).rglob("*") if p.suffix in (".ldb", ".sst")],
        key=lambda p: p.stat().st_size,
        reverse=True,
    )


def damage(data, kind, rng):
    files = leveldb_files(data)
    if not files:
        raise RuntimeError("no LevelDB files found")
    if kind == "truncate":
        target = files[0]
        size = target.stat().st_size
        if size < 64:
            raise RuntimeError(f"{target} too small to truncate")
        with target.open("r+b") as handle:
            handle.truncate(size - max(1, size // 4))
        return f"truncated {target.name} {size} -> {target.stat().st_size}"
    if kind == "bitflip":
        target = files[0]
        raw = target.read_bytes()
        marker = b"payload-60"
        at = raw.find(marker)
        if at >= 0:
            offset = at + len(marker) // 2
        else:
            offset = rng.randrange(32, max(33, len(raw) - 64))
        with target.open("r+b") as handle:
            handle.seek(offset)
            byte = handle.read(1)
            handle.seek(offset)
            handle.write(bytes([byte[0] ^ 0xFF]))
        return f"flipped byte {offset} in {target.name}"
    if kind == "remove":
        target = files[0]
        target.unlink()
        return f"removed {target.name}"
    if kind == "manifest":
        manifests = [p for p in Path(data).rglob("MANIFEST-*")]
        if not manifests:
            raise RuntimeError("no MANIFEST found")
        target = manifests[0]
        size = target.stat().st_size
        with target.open("r+b") as handle:
            handle.truncate(max(0, size - 8))
        return f"truncated {target.name} {size} -> {target.stat().st_size}"
    raise ValueError(kind)


def scenario(binary, kind, port, rng):
    directory = tempfile.mkdtemp(prefix="douchdb-fault-")
    data = os.path.join(directory, "data")
    log = os.path.join(directory, "server.log")
    try:
        seed(binary, data, port, log)
        if kind == "clean":
            detail = "clean SIGKILL"
        elif kind == "kill-compaction":
            # Seed already compacts; kill again while the next compact starts.
            proc, handle = start(binary, data, port, log)
            wait_ready(port, proc)
            try:
                request(port, "POST", "/faults/_compact")
            except (OSError, http.client.HTTPException):
                pass
            time.sleep(0.01)
            stop(proc)
            handle.close()
            detail = "killed during compact"
        else:
            detail = damage(data, kind, rng)
        result, note = verify(binary, data, port, log)
        return result, f"{kind}: {detail}; {note}"
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin", default="target/release/douchdb")
    parser.add_argument("--port", type=int, default=25999)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()
    rng = random.Random(args.seed)
    binary = os.path.abspath(args.bin)
    if not os.path.exists(binary):
        raise SystemExit(f"missing binary {binary}; run cargo build --release")

    failures = 0
    for kind in ["clean", "kill-compaction", "truncate", "bitflip", "remove", "manifest"]:
        result, note = scenario(binary, kind, args.port, rng)
        print(f"{kind:16s} {result:8s} {note}")
        if result == "corrupt":
            failures += 1

    if failures:
        print(f"FAIL: {failures} scenario(s) silently lost or partially served data")
        return 1
    print("PASS: every scenario either preserved all documents or refused to start")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

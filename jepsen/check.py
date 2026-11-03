"""Checks for the adapted jepsen-couchdb scenarios."""

import time
from collections import defaultdict

import requests

import config
import sync


def _local_docs(port):
    try:
        response = requests.get(
            config.make_url(port, f"{config.DB}/_all_docs"),
            params={"include_docs": "true"},
            headers=config.local_headers(),
            timeout=config.TIMEOUT,
        )
        response.raise_for_status()
        return response.json()["rows"]
    except requests.RequestException:
        return None


def _local_sets(ports=None):
    ports = ports or config.PORTS
    out = {}
    for port in ports:
        rows = _local_docs(port)
        if rows is None:
            print(f"{port} down :(")
            continue
        out[port] = {
            row["id"]: row["doc"]
            for row in rows
            if not row["id"].startswith("_design/")
        }
    return out


def _expected(results):
    return {
        result["id"]: result["_value"]
        for result in results
        if "ok" in result and "id" in result
    }


def _report(results, settle=None):
    expected = _expected(results)
    print(f"{len(results)} writes total.")
    print(f"{len(expected)} writes acknowledged.")
    if settle:
        time.sleep(settle)
    sets = _local_sets()
    for port, docs in sets.items():
        missing = sorted(set(expected) - set(docs))
        print(f"{len(docs)} writes written to node {port}")
        if not missing and all(
            docs[doc_id].get("value") == value
            for doc_id, value in expected.items()
        ):
            print("...and checksum passes!")
        else:
            print(f"...{len(missing)} missing; checksum differs")
    return expected, sets


def simple(work, n=100):
    results = work(n)
    _report(results)


def sleep(work, n=100):
    results = work(n)
    _report(results, settle=config.SETTLE_SECS)


def quorum(work, n=100):
    results = work(n)
    expected = _expected(results)
    print(f"{len(results)} writes total.")
    print(f"{len(expected)} writes acknowledged.")

    doc_id = next(iter(expected), None)
    if not doc_id:
        print("no document to read by quorum")
        return
    r = max(1, len(config.PORTS) // 2 + 1)
    for port in config.PORTS:
        try:
            response = requests.get(
                config.make_url(port, f"{config.DB}/{doc_id}"),
                params={"r": r},
                timeout=config.TIMEOUT,
            )
        except requests.RequestException:
            continue
        if response.status_code == 200:
            print(f"Got {doc_id} by quorum r={r} on node {port}!")
            break
    else:
        print(f"Failed to get {doc_id} by quorum r={r}")
        return

    docs = _local_sets()
    print(f"{len(expected)} docs in result set.")
    for port, local in docs.items():
        missing = set(expected) - set(local)
        if missing:
            print(f"node {port} missing {len(missing)} acknowledged docs")


def trainwreck(work, n=100):
    results = work(n)
    expected = _expected(results)
    print(f"{len(results)} writes total.")
    print(f"{len(expected)} writes acknowledged.")
    docs = _local_sets()
    by_id = defaultdict(set)
    for local in docs.values():
        for doc_id, doc in local.items():
            by_id[doc_id].add(doc.get("_rev"))
    conflicts = {doc_id: revs for doc_id, revs in by_id.items() if len(revs) > 1}
    print(f"{len(by_id)} docs in result set.")
    if conflicts:
        print(f"{len(conflicts)} ids have different winning revisions across nodes")
    for port, local in docs.items():
        missing = set(expected) - set(local)
        if missing:
            print(f"node {port} missing {len(missing)} acknowledged docs")


def node_loss(work, n, cluster):
    """Write through a node loss and verify convergence after restart."""
    first = work(n // 2)
    lost = min(1, len(cluster.ports) - 1)
    cluster.kill_node(lost)
    print(f"Killed node {cluster.ports[lost]}; continuing writes.")
    second = work(n - n // 2)
    cluster.restart_node(lost)
    print(f"Restarted node {cluster.ports[lost]}.")
    _report(first + second, settle=config.SETTLE_SECS)

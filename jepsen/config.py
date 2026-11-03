"""Configuration for the douchdb adaptation of jepsen-couchdb."""

import os

HOST = os.environ.get("DOUCHDB_HOST", "127.0.0.1")
PORTS = [
    int(port)
    for port in os.environ.get(
        "DOUCHDB_PORTS", "15984,15985,15986"
    ).split(",")
    if port.strip()
]
DB = os.environ.get("DOUCHDB_DB", "jepsen")
SECRET = os.environ.get(
    "DOUCHDB_CLUSTER_SECRET", "jepsen-cluster-secret-please-change"
)
BIN = os.environ.get("DOUCHDB_BIN", "target/release/douchdb")
Q = int(os.environ.get("DOUCHDB_Q", "4"))
SYNC_SECS = os.environ.get("DOUCHDB_SYNC_SECS", "1")
SETTLE_SECS = float(os.environ.get("DOUCHDB_SETTLE_SECS", "3"))
TIMEOUT = float(os.environ.get("DOUCHDB_TIMEOUT", "30"))


def make_url(port, path=""):
    base = f"http://{HOST}:{port}"
    if not path:
        return base
    return f"{base}/{path.lstrip('/')}"


def local_headers():
    """Headers that make a request read the receiving node's own shards."""
    if not SECRET:
        return {}
    return {
        "x-douchdb-forwarded": "1",
        "x-douchdb-secret": SECRET,
    }

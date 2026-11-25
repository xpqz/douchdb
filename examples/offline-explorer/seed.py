#!/usr/bin/env python3
"""Create the offline explorer's database, offline_demo.

Deletes and recreates offline_demo, fills it with the airline_accidents events
dated 2006-2007 (read through _all_docs) (or, with --fixture N, N synthetic events for tests) and
installs _design/offline, whose views are the aviation explorer's own. Re-run
it to reset the cluster side of the demo.

Usage: DOUCHDB_PASSWORD=secret python3 seed.py [--url http://127.0.0.1:5984] [--fixture N]
"""
import argparse, base64, json, os, sys, urllib.error, urllib.parse, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "aviation-explorer"))
import views  # noqa: E402  (the explorer's view definitions)

DB = "offline_demo"
SOURCE = "airline_accidents"
VIEWS = ["by_date", "by_phase", "by_fatalities", "by_registration"]
PAGE = 5000
BATCH = 500
PHASES = ["TAKEOFF", "CRUISE", "APPROACH", "LANDING", "TAXI"]


def design_doc():
    """_design/offline: four of the explorer's airline_accidents views, unchanged."""
    source = views.design(SOURCE)["views"]
    return {"_id": "_design/offline", "language": "javascript",
            "views": {name: source[name] for name in VIEWS}}


def in_slice(doc):
    """True for the 2006-2007 events (event_date is an ISO YYYY-MM-DD string)."""
    date = doc.get("event_date")
    return (not doc["_id"].startswith("_design/") and isinstance(date, str)
            and "2006-01-01" <= date < "2008-01-01")


def prepare(doc):
    """A copy to write as a new document: everything but the source revision."""
    return {k: v for k, v in doc.items() if k != "_rev"}


def fixture_docs(n):
    """n synthetic events inside the slice; fixture-0000 is the single deadliest."""
    docs = []
    for i in range(n):
        day = i % 730
        year, rest = (2006, day) if day < 365 else (2007, day - 365)
        month, mday = 1 + rest // 31 % 12, 1 + rest % 28
        docs.append({
            "_id": f"fixture-{i:04d}",
            "event_date": f"{year}-{month:02d}-{mday:02d}",
            "total_fatal_injuries": 50 if i == 0 else i % 7,
            "broad_phase_of_flight": PHASES[i % len(PHASES)],
            "aircraft_registration": f"N{1000 + i}F",
        })
    return docs


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://127.0.0.1:5984")
    p.add_argument("--username", default="admin")
    p.add_argument("--fixture", type=int, metavar="N",
                   help="write N synthetic events instead of copying airline_accidents")
    a = p.parse_args()
    auth = "Basic " + base64.b64encode(f"{a.username}:{os.environ['DOUCHDB_PASSWORD']}".encode()).decode()

    def req(method, path, body=None):
        r = urllib.request.Request(a.url + path, method=method,
                                   headers={"Authorization": auth, "Content-Type": "application/json"},
                                   data=None if body is None else json.dumps(body).encode())
        try:
            with urllib.request.urlopen(r, timeout=120) as resp:
                return resp.status, json.load(resp)
        except urllib.error.HTTPError as e:
            return e.code, json.load(e)

    def check(status, body, what):
        if status >= 300:
            sys.exit(f"{what}: {status} {body}")
        return body

    status, body = req("DELETE", f"/{DB}")
    if status != 404:
        check(status, body, f"delete {DB}")
    check(*req("PUT", f"/{DB}"), f"create {DB}")

    if a.fixture is not None:
        docs = fixture_docs(a.fixture)
    else:
        # douchdb has no Mango (_find): page through _all_docs and keep the slice.
        docs, start = [], None
        while True:
            qs = f"include_docs=true&limit={PAGE + 1}"
            if start is not None:
                qs += "&startkey=" + urllib.parse.quote(json.dumps(start))
            rows = check(*req("GET", f"/{SOURCE}/_all_docs?{qs}"), f"read {SOURCE}")["rows"]
            page, start = rows[:PAGE], (rows[PAGE]["id"] if len(rows) > PAGE else None)
            docs.extend(prepare(r["doc"]) for r in page if r.get("doc") and in_slice(r["doc"]))
            if start is None:
                break

    for i in range(0, len(docs), BATCH):
        rows = check(*req("POST", f"/{DB}/_bulk_docs", {"docs": docs[i:i + BATCH]}), "write")
        failed = [r for r in rows if "error" in r]
        if failed:
            sys.exit(f"write: {len(failed)} documents failed, first {failed[0]}")

    check(*req("PUT", f"/{DB}/{urllib.parse.quote(design_doc()['_id'], safe='/')}", design_doc()),
          "install _design/offline")
    info = check(*req("GET", f"/{DB}"), f"read {DB}")
    print(f"{DB}: {info['doc_count'] - 1} documents")


if __name__ == "__main__":
    main()

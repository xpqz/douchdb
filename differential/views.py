#!/usr/bin/env python3
"""Differential map/reduce fuzzer for douchdb against Apache CouchDB.

The corpus and design document are fixed, so a failing query is reproducible
from the query parameters alone. Query parameters are generated from a seed.

CouchDB's stock Docker image has no `_search` endpoint (that needs Clouseau),
so search is not part of this differential suite.
"""

from __future__ import annotations

import json
import random
import urllib.parse
from typing import Any

from run import HERE, Http

DB = "diff_views"
DDOC_ID = "_design/fuzz"

MAP_NUMBERS = "function(doc) { if (doc.n !== undefined) { emit(doc.n, doc.n); } }"
MAP_STRINGS = "function(doc) { if (doc.s !== undefined) { emit(doc.s, doc._id); } }"
MAP_TAGS = (
    "function(doc) { if (doc.tag !== undefined && typeof doc.n === 'number') "
    "{ emit(doc.tag, doc.n); } }"
)
MAP_TAG_COUNT = "function(doc) { if (doc.tag !== undefined) { emit(doc.tag, 1); } }"
MAP_PAIRS = (
    "function(doc) { if (doc.tag !== undefined && typeof doc.n === 'number') "
    "{ emit([doc.tag, doc.n], {tag: doc.tag, n: doc.n}); } }"
)
MAP_PAIRS_SUM = (
    "function(doc) { if (doc.tag !== undefined && typeof doc.n === 'number') "
    "{ emit([doc.tag, doc.n], doc.n); } }"
)
MAP_UNICODE = "function(doc) { if (doc.u !== undefined) { emit(doc.u, 1); } }"
MAP_IDS = "function(doc) { emit(doc._id, doc._id); }"
MAP_MULTI = (
    "function(doc) { if (doc.tag !== undefined) { emit(doc.tag, doc.n); } "
    "if (doc.s !== undefined) { emit(doc.s, doc.n); } }"
)
CUSTOM_SUM = (
    "function(keys, values, rereduce) { var total = 0; "
    "for (var i = 0; i < values.length; i++) { total += values[i]; } return total; }"
)

VIEWS = {
    "numbers": {"map": MAP_NUMBERS},
    "strings": {"map": MAP_STRINGS},
    "tags": {"map": MAP_TAGS, "reduce": "_sum"},
    "tag_count": {"map": MAP_TAG_COUNT, "reduce": "_count"},
    "tag_stats": {"map": MAP_TAGS, "reduce": "_stats"},
    "tag_custom": {"map": MAP_TAGS, "reduce": CUSTOM_SUM},
    "pairs": {"map": MAP_PAIRS},
    "pairs_sum": {"map": MAP_PAIRS_SUM, "reduce": "_sum"},
    "unicode": {"map": MAP_UNICODE, "reduce": "_count"},
    "ids": {"map": MAP_IDS},
    "multi": {"map": MAP_MULTI},
}
REDUCE_VIEWS = {name for name, view in VIEWS.items() if "reduce" in view}
NON_REDUCE_VIEWS = set(VIEWS) - REDUCE_VIEWS


def corpus() -> list[dict[str, Any]]:
    tags = ["a", "b", "c", "d"]
    strings = ["alpha", "Beta", "ä", "z", "", "10", "2", "a b", "a/b", "😀"]
    numbers = [-5, -1, 0, 1, 2, 2.5, 10, 100, None]
    unicode = ["éclair", "eclair", "Zebra", "apple", "😀😀"]
    docs: list[dict[str, Any]] = []
    for index in range(40):
        doc: dict[str, Any] = {
            "_id": f"doc-{index:02d}",
            "tag": tags[index % len(tags)],
        }
        if index % 7 != 3:
            doc["n"] = numbers[index % len(numbers)]
        if index % 5 != 0:
            doc["s"] = strings[index % len(strings)]
        if index % 6 == 0:
            doc["u"] = unicode[index % len(unicode)]
        if index % 4 == 0:
            doc["b"] = index % 8 == 0
        if index % 9 == 0:
            doc["arr"] = [index, strings[index % len(strings)], None]
        if index % 10 == 0:
            doc["obj"] = {"i": index, "tag": tags[index % len(tags)]}
        docs.append(doc)
    docs.append(
        {
            "_id": "doc-big",
            "tag": "a",
            "n": 123456789012345678901234567890,
            "s": "big",
        }
    )
    return docs


def design_doc() -> dict[str, Any]:
    return {"_id": DDOC_ID, "views": VIEWS}


def view_path(view: str, params: dict[str, str]) -> str:
    path = f"/{DB}/{DDOC_ID}/_view/{view}"
    if params:
        path += "?" + urllib.parse.urlencode(params)
    return path


def json_param(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def systematic_queries() -> list[tuple[str, dict[str, str]]]:
    queries: list[tuple[str, dict[str, str]]] = []

    for view in sorted(VIEWS):
        queries.append((view, {}))
        queries.append((view, {"limit": "5"}))
        queries.append((view, {"skip": "2", "limit": "5"}))
        queries.append((view, {"descending": "true", "limit": "4"}))
        if view in NON_REDUCE_VIEWS:
            queries.append((view, {"include_docs": "true"}))
            queries.append((view, {"include_docs": "true", "limit": "3"}))
        if view in REDUCE_VIEWS:
            queries.append((view, {"reduce": "false"}))
            queries.append((view, {"group": "true"}))
            queries.append((view, {"group": "true", "limit": "4"}))
            queries.append((view, {"group": "true", "group_level": "0"}))
            queries.append((view, {"group": "false", "group_level": "2"}))
            queries.append((view, {"include_docs": "true"}))
            queries.append((view, {"keys": json_param([])}))
        else:
            queries.append((view, {"group": "true"}))
            queries.append((view, {"reduce": "true"}))

    for view in ["strings", "unicode", "multi"]:
        key = "a" if view == "strings" else ("éclair" if view == "unicode" else "a")
        queries.append((view, {"key": json_param(key)}))
        queries.append((view, {"keys": json_param([key, "b"])}))
        queries.append((view, {"keys": json_param([])}))
        queries.append(
            (
                view,
                {
                    "keys": json_param([key, key]),
                    "startkey": json_param("b"),
                },
            )
        )
        queries.append(
            (
                view,
                {
                    "keys": json_param([key, key]),
                    "endkey": json_param("b"),
                },
            )
        )
        queries.append((view, {"startkey": json_param(key), "endkey": json_param("d")}))
        queries.append(
            (
                view,
                {
                    "startkey": json_param(key),
                    "endkey": json_param("d"),
                    "inclusive_end": "false",
                },
            )
        )
        queries.append(
            (
                view,
                {
                    "startkey": json_param("d"),
                    "endkey": json_param(key),
                    "descending": "true",
                },
            )
        )

    for key in [1, 2, 2, 2.5, None]:
        queries.append(("numbers", {"keys": json_param([key])}))
    queries.append(("numbers", {"keys": json_param([1, 2, 2])}))
    queries.append(("numbers", {"keys": json_param([])}))
    queries.append(("numbers", {"startkey": "1", "endkey": "10"}))
    queries.append(
        ("numbers", {"startkey": "10", "endkey": "1", "descending": "true"})
    )
    queries.append(
        (
            "numbers",
            {"startkey": "1", "endkey": "10", "inclusive_end": "false"},
        )
    )

    queries.append(("pairs", {"keys": json_param([["a", 1], ["b", 2]])}))
    queries.append(
        ("pairs", {"startkey": json_param(["a"]), "endkey": json_param(["a", {}])})
    )
    queries.append(
        ("pairs", {"startkey": json_param(["a"]), "endkey": json_param(["c", {}])})
    )
    queries.append(("pairs_sum", {"group_level": "1"}))
    queries.append(("pairs_sum", {"group_level": "2"}))
    queries.append(("pairs_sum", {"group": "true"}))
    queries.append(("pairs_sum", {"reduce": "false"}))
    queries.append(("tag_count", {"group_level": "1"}))
    queries.append(("ids", {"key": json_param("doc-01")}))
    return queries


def random_query(rng: random.Random) -> tuple[str, dict[str, str]]:
    view = rng.choice(sorted(VIEWS))
    params: dict[str, str] = {}

    if view in NON_REDUCE_VIEWS or rng.random() < 0.3:
        if rng.random() < 0.45:
            params["limit"] = str(rng.randint(0, 8))
        if rng.random() < 0.2:
            params["skip"] = str(rng.randint(0, 5))
    if view in NON_REDUCE_VIEWS and rng.random() < 0.35:
        params["include_docs"] = "true"
    if rng.random() < 0.25:
        params["descending"] = "true"
    if view in REDUCE_VIEWS:
        if rng.random() < 0.35:
            params["reduce"] = rng.choice(["true", "false"])
        if rng.random() < 0.4:
            params["group"] = "true"
        if view == "pairs_sum" and rng.random() < 0.35:
            params["group_level"] = str(rng.randint(0, 2))
    elif rng.random() < 0.25:
        params["keys"] = json_param(rng.choice([[], ["a"], ["a", "b"], [1, 2]]))
    if "keys" not in params and rng.random() < 0.2:
        params["startkey"] = json_param(rng.choice(["a", "b", 1, ["a"]]))
        params["endkey"] = json_param(rng.choice(["d", "z", 10, ["d", {}]]))
    return view, params


def _summary(body: Any) -> Any:
    if not isinstance(body, dict):
        return body
    if "rows" in body:
        return {
            "total_rows": body.get("total_rows"),
            "offset": body.get("offset"),
            "rows": body["rows"][:5],
            "row_count": len(body["rows"]),
        }
    return body


def run_views(
    left: Http,
    right: Http,
    *,
    seed: int,
    queries: int,
    verbose: bool,
) -> int:
    docs = corpus()
    design = design_doc()

    for http in (left, right):
        http.request("DELETE", f"/{DB}")
        response = http.request("PUT", f"/{DB}")
        if response.status not in (201, 202):
            raise RuntimeError(f"could not create {DB}: {response.status} {response.raw}")
        response = http.request("POST", f"/{DB}/_bulk_docs", body={"docs": docs})
        if response.status != 201:
            raise RuntimeError(f"could not seed {DB}: {response.status} {response.raw}")
        response = http.request("PUT", f"/{DB}/{DDOC_ID}", body=design)
        if response.status != 201:
            raise RuntimeError(
                f"could not create design document: {response.status} {response.raw}"
            )

    planned = systematic_queries()
    rng = random.Random(seed)
    planned.extend(random_query(rng) for _ in range(queries))
    print(
        f"==> Fuzzing {len(planned)} view queries over {len(docs)} documents "
        f"(seed {seed})",
        flush=True,
    )

    for index, (view, params) in enumerate(planned):
        path = view_path(view, params)
        left_response = left.get(path)
        right_response = right.get(path)
        problem = compare(left_response, right_response, params)
        if verbose:
            print(f"    {index:03d} {view} {params} -> {left_response.status}")
        if problem is None:
            continue

        print()
        print(f"VIEW DIVERGENCE at query {index} ({problem})")
        print(f"  view:    {view}")
        print(f"  params:  {json.dumps(params, ensure_ascii=False, sort_keys=True)}")
        print(f"  path:    {path}")
        print(f"  douchdb: {describe(left_response)}")
        print(f"  couchdb: {describe(right_response)}")
        print()
        directory = HERE / "artifacts" / f"views-{seed}"
        directory.mkdir(parents=True, exist_ok=True)
        payload = {
            "kind": "views",
            "seed": seed,
            "view": view,
            "params": params,
            "path": path,
            "corpus": docs,
            "design": design,
            "douchdb": {"status": left_response.status, "body": left_response.body},
            "couchdb": {"status": right_response.status, "body": right_response.body},
        }
        (directory / "repro.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False)
        )
        print(f"==> Wrote {directory / 'repro.json'}")
        return 1

    print("No divergence.")
    return 0


def compare(left: Any, right: Any, params: dict[str, str]) -> str | None:
    if left.status != right.status:
        return "status_mismatch"
    if left.status >= 400:
        left_error = left.body.get("error") if isinstance(left.body, dict) else None
        right_error = right.body.get("error") if isinstance(right.body, dict) else None
        if left_error != right_error:
            return "error_code_mismatch"
        return None
    if comparable_body(left.body, params) != comparable_body(right.body, params):
        return "body_mismatch"
    return None


def comparable_body(body: Any, params: dict[str, str]) -> Any:
    """Drop fields CouchDB does not define for this query shape.

    CouchDB's `offset` and row order for multi-key queries follow internal
    legacy rules that are not part of the API contract. Multi-key rows are
    compared as a multiset; single-key and range queries are compared exactly.
    """
    if not isinstance(body, dict) or "keys" not in params:
        return body
    try:
        keys = json.loads(params["keys"])
    except json.JSONDecodeError:
        return body
    if not isinstance(keys, list) or len(keys) <= 1:
        return body
    body = dict(body)
    body.pop("offset", None)
    rows = body.get("rows")
    if isinstance(rows, list):
        body["rows"] = sorted(
            rows, key=lambda row: json.dumps(row, ensure_ascii=False, sort_keys=True)
        )
    return body


def describe(response: Any) -> str:
    body = json.dumps(_summary(response.body), ensure_ascii=False, sort_keys=True)
    if len(body) > 1200:
        body = body[:1197] + "..."
    return f"{response.status} {body}"

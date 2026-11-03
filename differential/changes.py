#!/usr/bin/env python3
"""Differential _changes pagination test against CouchDB.

Documents are written one at a time so CouchDB and douchdb allocate sequence
numbers in the same logical order. The test then compares full feeds, limits,
descending order and cursor pagination. Sequence values themselves are opaque
and are removed before comparison; row order, revisions, deleted flags, docs
and pending counts are compared.
"""

from __future__ import annotations

import json
import urllib.parse
from typing import Any

from run import HERE, Http

DB = "diff_changes"


def path(params: dict[str, Any]) -> str:
    return f"/{DB}/_changes?" + urllib.parse.urlencode(params)


def normalize(body: Any) -> Any:
    if not isinstance(body, dict):
        return body
    rows = []
    for row in body.get("results", []):
        row = dict(row)
        row.pop("seq", None)
        rows.append(row)
    return {"rows": rows, "pending": body.get("pending")}


def compare(left: Any, right: Any) -> str | None:
    if left.status != right.status:
        return "status_mismatch"
    if left.status >= 400:
        left_error = left.body.get("error") if isinstance(left.body, dict) else None
        right_error = right.body.get("error") if isinstance(right.body, dict) else None
        return None if left_error == right_error else "error_code_mismatch"
    if left.body.get("results") is None:
        return "missing_results"
    return None if normalize(left.body) == normalize(right.body) else "body_mismatch"


def write_artifact(label: str, query: dict[str, Any], left: Any, right: Any) -> str:
    directory = HERE / "artifacts" / "changes"
    directory.mkdir(parents=True, exist_ok=True)
    payload = {
        "kind": "changes",
        "check": label,
        "query": query,
        "douchdb": {"status": left.status, "body": left.body},
        "couchdb": {"status": right.status, "body": right.body},
    }
    (directory / "repro.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    return str(directory / "repro.json")


def query(http: Http, params: dict[str, Any]) -> Any:
    return http.get(path(params))


def run_changes(left: Http, right: Http, *, verbose: bool) -> int:
    for http in (left, right):
        http.request("DELETE", f"/{DB}")
        response = http.request(
            "PUT", f"/{DB}" + ("?q=1" if http is right else ""), body={}
        )
        if response.status not in (201, 202):
            raise RuntimeError(f"could not create {DB}: {response.status} {response.raw}")

    # One write at a time, in the same order on both servers. Bulk requests let
    # CouchDB assign sequences in a different order even with q=1.
    for index in range(12):
        body = {"v": 0, "group": index % 3}
        status = left.request("PUT", f"/{DB}/doc-{index:02}", body=body).status
        assert status == 201, status
        status = right.request("PUT", f"/{DB}/doc-{index:02}", body=body).status
        assert status == 201, status

    for index in [0, 1, 2, 4]:
        doc = left.get(f"/{DB}/doc-{index:02}").body
        rev = doc["_rev"]
        body = {"v": index + 1, "group": index % 3}
        for http in (left, right):
            response = http.request(
                "PUT", f"/{DB}/doc-{index:02}", body={**body, "_rev": rev}
            )
            assert response.status == 201, response.status

    doc = left.get(f"/{DB}/doc-05").body
    rev = doc["_rev"]
    for http in (left, right):
        response = http.request("DELETE", f"/{DB}/doc-05?rev={rev}")
        assert response.status == 200, response.status

    for http in (left, right):
        response = http.request("PUT", f"/{DB}/doc-05", body={"v": 99})
        assert response.status == 201, response.status

    checks: list[tuple[str, dict[str, Any]]] = [
        ("full", {"since": "0"}),
        ("full-all-docs", {"since": "0", "style": "all_docs"}),
        ("full-include-docs", {"since": "0", "include_docs": "true"}),
    ]
    for limit in (1, 2, 3, 5):
        checks.append((f"limit-{limit}", {"since": "0", "limit": str(limit)}))
        checks.append(
            (
                f"descending-{limit}",
                {"since": "0", "descending": "true", "limit": str(limit)},
            )
        )
    checks.append(("descending-full", {"since": "0", "descending": "true"}))
    checks.append(("limit-zero", {"since": "0", "limit": "0"}))

    for label, params in checks:
        left_response = query(left, params)
        right_response = query(right, params)
        problem = compare(left_response, right_response)
        if verbose:
            print(f"    {label}: {left_response.status}")
        if problem:
            print(f"\nCHANGES DIVERGENCE {label} ({problem})")
            print(f"  query:   {params}")
            print(f"  douchdb: {json.dumps(normalize(left_response.body), ensure_ascii=False)[:900]}")
            print(f"  couchdb: {json.dumps(normalize(right_response.body), ensure_ascii=False)[:900]}")
            print(f"==> Wrote {write_artifact(label, params, left_response, right_response)}")
            return 1

    # Ascending pagination: each server uses its own opaque last_seq.
    left_since = "0"
    right_since = "0"
    page = 0
    while True:
        params = {"since": left_since, "limit": "3"}
        left_response = query(left, params)
        params_right = {"since": right_since, "limit": "3"}
        right_response = query(right, params_right)
        problem = compare(left_response, right_response)
        if verbose:
            print(f"    page {page}: {len(left_response.body.get('results', []))} rows")
        if problem:
            print(f"\nCHANGES DIVERGENCE page-{page} ({problem})")
            print(f"  douchdb: {json.dumps(normalize(left_response.body), ensure_ascii=False)[:900]}")
            print(f"  couchdb: {json.dumps(normalize(right_response.body), ensure_ascii=False)[:900]}")
            print(f"==> Wrote {write_artifact(f'page-{page}', params, left_response, right_response)}")
            return 1
        left_rows = left_response.body.get("results", [])
        right_rows = right_response.body.get("results", [])
        if not left_rows and not right_rows:
            break
        left_since = str(left_response.body.get("last_seq", left_since))
        right_since = str(right_response.body.get("last_seq", right_since))
        page += 1
        if page > 20:
            print("CHANGES DIVERGENCE pagination did not terminate")
            return 1

    print("No divergence.")
    return 0

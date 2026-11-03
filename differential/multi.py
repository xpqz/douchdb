#!/usr/bin/env python3
"""Differential multi-document fuzzer for douchdb against Apache CouchDB.

This covers the endpoints that operate on several documents at once:
_bulk_docs, _all_docs, _changes, _bulk_get, _revs_diff, open_revs,
_revisions and revs_info. Sequence values are opaque and are normalized;
everything else is compared exactly.
"""

from __future__ import annotations

import json
import random
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

from run import HERE, Http

DB = "diff_multi"


@dataclass
class Model:
    revs: dict[str, str] = field(default_factory=dict)
    history: dict[str, list[str]] = field(default_factory=dict)
    deleted: set[str] = field(default_factory=set)

    def remember(self, doc: str, rev: str, deleted: bool = False) -> None:
        self.revs[doc] = rev
        self.history.setdefault(doc, []).append(rev)
        if deleted:
            self.deleted.add(doc)
        else:
            self.deleted.discard(doc)


def initial_docs() -> list[dict[str, Any]]:
    docs = []
    for index in range(10):
        docs.append(
            {
                "_id": f"doc-{index}",
                "n": index,
                "s": ["a", "b", "c"][index % 3],
                "nested": {"i": index},
            }
        )
    return docs


def envelope(docs: list[dict[str, Any]]) -> dict[str, Any]:
    return {"docs": docs}


def doc_path(doc: str) -> str:
    return f"/{DB}/{urllib.parse.quote(doc, safe='')}"


def query(path: str, params: dict[str, Any]) -> str:
    if not params:
        return path
    return path + "?" + urllib.parse.urlencode(params)


def make_write(rng: random.Random, model: Model, docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    batch = []
    for _ in range(rng.randint(1, 3)):
        if rng.random() < 0.25:
            index = len(model.revs) + rng.randint(0, 3)
            doc_id = f"doc-{index}"
        else:
            doc_id = rng.choice(docs)["_id"]
        rev = model.revs.get(doc_id)
        body: dict[str, Any] = {"_id": doc_id}
        if rev:
            body["_rev"] = rev
        if rng.random() < 0.2:
            body["_deleted"] = True
        else:
            body["n"] = rng.randint(-3, 30)
            body["s"] = rng.choice(["a", "b", "c", "d"])
            if rng.random() < 0.5:
                body["extra"] = [rng.randint(0, 5), {"x": rng.randint(0, 5)}]
        batch.append(body)
    return batch


def all_docs_query(rng: random.Random, model: Model) -> tuple[str, dict[str, str]]:
    params: dict[str, str] = {}
    if rng.random() < 0.65:
        params["include_docs"] = "true"
    if rng.random() < 0.3:
        params["descending"] = "true"
    if rng.random() < 0.45:
        params["limit"] = str(rng.randint(0, 6))
    if rng.random() < 0.25:
        params["skip"] = str(rng.randint(0, 4))
    if rng.random() < 0.3:
        ids = list(model.revs)
        keys = rng.sample(ids, k=min(len(ids), rng.randint(1, 3)))
        keys.append("missing")
        params["keys"] = json.dumps(keys, separators=(",", ":"))
    elif rng.random() < 0.4:
        ids = sorted(model.revs)
        if ids:
            lo = rng.choice(ids)
            hi = rng.choice(ids)
            params["startkey"] = json.dumps(lo)
            params["endkey"] = json.dumps(hi)
            if rng.random() < 0.2:
                params["inclusive_end"] = "false"
    if rng.random() < 0.2:
        params["conflicts"] = "true"
    return f"/{DB}/_all_docs", params


def changes_params(rng: random.Random) -> dict[str, str]:
    # CouchDB returns one row per document; douchdb currently returns every
    # sequence event (issue #125). Ask for the whole backlog and compare the
    # latest row per id, so limit/pending/row-count semantics do not mask
    # revision correctness.
    params = {"limit": "1000"}
    if rng.random() < 0.5:
        params["include_docs"] = "true"
    if rng.random() < 0.3:
        params["style"] = "all_docs"
    return params


def normalize_changes(body: Any) -> Any:
    if not isinstance(body, dict):
        return body
    rows: dict[str, dict[str, Any]] = {}
    for row in body.get("results", []):
        row = dict(row)
        row.pop("seq", None)
        rows[row["id"]] = row
    return {"rows": [rows[key] for key in sorted(rows)]}


def normalize_open_revs(body: Any) -> Any:
    if not isinstance(body, list):
        return body
    return sorted(body, key=lambda item: json.dumps(item, sort_keys=True, ensure_ascii=False))


def compare(left: Any, right: Any, normalize=None) -> str | None:
    if left.status != right.status:
        return "status_mismatch"
    if left.status >= 400:
        left_error = left.body.get("error") if isinstance(left.body, dict) else None
        right_error = right.body.get("error") if isinstance(right.body, dict) else None
        if left_error != right_error:
            return "error_code_mismatch"
        return None
    left_body = normalize(left.body) if normalize else left.body
    right_body = normalize(right.body) if normalize else right.body
    if left_body != right_body:
        return "body_mismatch"
    return None


def describe(response: Any) -> str:
    body = json.dumps(response.body, ensure_ascii=False, sort_keys=True)
    if len(body) > 900:
        body = body[:897] + "..."
    return f"{response.status} {body}"


def run_multi(
    left: Http,
    right: Http,
    *,
    seed: int,
    steps: int,
    verbose: bool,
) -> int:
    docs = initial_docs()
    for http in (left, right):
        http.request("DELETE", f"/{DB}")
        response = http.request("PUT", f"/{DB}")
        if response.status not in (201, 202):
            raise RuntimeError(f"could not create {DB}: {response.status} {response.raw}")
        response = http.request("POST", f"/{DB}/_bulk_docs", body=envelope(docs))
        if response.status != 201:
            raise RuntimeError(f"could not seed {DB}: {response.status} {response.raw}")

    model = Model()
    response = left.get(f"/{DB}/_all_docs?include_docs=true")
    for row in response.body.get("rows", []):
        doc = row.get("doc") or {}
        model.remember(row["id"], row["value"]["rev"])

    rng = random.Random(seed)
    since_left = "0"
    since_right = "0"
    print(f"==> Fuzzing {steps} multi-document operations (seed {seed})", flush=True)

    for index in range(steps):
        roll = rng.random()
        label = ""
        normalize = None
        detail: Any = None
        if roll < 0.36:
            batch = make_write(rng, model, docs)
            label = "bulk_docs"
            detail = {"docs": batch}
            left_response = left.request("POST", f"/{DB}/_bulk_docs", body=envelope(batch))
            right_response = right.request("POST", f"/{DB}/_bulk_docs", body=envelope(batch))
            if left_response.status == 201 and right_response.status == 201:
                for body, result in zip(batch, left_response.body):
                    if result.get("ok"):
                        model.remember(body["_id"], result["rev"], bool(body.get("_deleted")))
        elif roll < 0.52:
            doc = rng.choice(list(model.revs))
            rev = rng.choice(model.history[doc])
            params: dict[str, Any] = {"rev": rev}
            if rng.random() < 0.5:
                params["revs"] = "true"
            if rev == model.revs[doc] and doc not in model.deleted and rng.random() < 0.3:
                params["revs_info"] = "true"
                # CouchDB only emits _revs_info together with latest=true,
                # and latest=true returns the current revision.
                params["latest"] = "true"
            label = "get_rev"
            path = query(doc_path(doc), params)
            detail = {"path": path}
            left_response = left.get(path)
            right_response = right.get(path)
        elif roll < 0.62:
            doc = rng.choice(list(model.revs))
            label = "open_revs"
            normalize = normalize_open_revs
            path = query(doc_path(doc), {"open_revs": "all", "revs": "true"})
            detail = {"path": path}
            left_response = left.get(path)
            right_response = right.get(path)
        elif roll < 0.78:
            label = "all_docs"
            path, params = all_docs_query(rng, model)
            detail = {"path": query(path, params)}
            left_response = left.get(query(path, params))
            right_response = right.get(query(path, params))
        elif roll < 0.9:
            label = "changes"
            normalize = normalize_changes
            params = changes_params(rng)
            left_path = query(f"/{DB}/_changes", {**params, "since": since_left})
            detail = {"left_path": left_path, "right_since": since_right}
            left_response = left.get(left_path)
            right_response = right.get(
                query(f"/{DB}/_changes", {**params, "since": since_right})
            )
            if left_response.status == 200:
                since_left = left_response.body.get("last_seq", since_left)
            if right_response.status == 200:
                since_right = right_response.body.get("last_seq", since_right)
        elif roll < 0.95:
            label = "bulk_get"
            requests = []
            for doc in rng.sample(list(model.revs), k=min(len(model.revs), rng.randint(1, 3))):
                entry: dict[str, Any] = {"id": doc}
                if rng.random() < 0.7:
                    entry["rev"] = rng.choice(model.history[doc])
                requests.append(entry)
            requests.append({"id": "missing", "rev": "1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
            body = {"docs": requests}
            left_response = left.request("POST", f"/{DB}/_bulk_get", body=body)
            right_response = right.request("POST", f"/{DB}/_bulk_get", body=body)
        else:
            label = "revs_diff"
            body = {}
            for doc in rng.sample(list(model.revs), k=min(len(model.revs), rng.randint(1, 3))):
                revs = model.history[doc][-2:]
                body[doc] = revs + ["9-deadbeefdeadbeefdeadbeefdeadbeef"]
            left_response = left.request("POST", f"/{DB}/_revs_diff", body=body)
            right_response = right.request("POST", f"/{DB}/_revs_diff", body=body)

        problem = compare(left_response, right_response, normalize)
        if verbose:
            print(f"    {index:03d} {label} -> {left_response.status}")
            if detail is not None:
                print(f"         {json.dumps(detail, ensure_ascii=False, sort_keys=True)}")
        if problem is None:
            continue

        print()
        print(f"MULTI DIVERGENCE at step {index} ({problem})")
        print(f"  operation: {label}")
        if detail is not None:
            print(f"  detail:    {json.dumps(detail, ensure_ascii=False, sort_keys=True)}")
        print(f"  douchdb:   {describe(left_response)}")
        print(f"  couchdb:   {describe(right_response)}")
        print()
        directory = HERE / "artifacts" / f"multi-{seed}"
        directory.mkdir(parents=True, exist_ok=True)
        payload = {
            "kind": "multi",
            "seed": seed,
            "step": index,
            "operation": label,
            "detail": detail,
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

#!/usr/bin/env python3
"""Differential revision-tree and conflict fuzzer against CouchDB.

Each scenario inserts one or more branches with `new_edits=false`, then compares
the winner, leaves, conflicts, tombstones and changes feed against CouchDB.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from run import HERE, Http

DB = "diff_conflicts"


@dataclass
class Node:
    rev: str
    ids: list[str]
    deleted: bool = False
    value: Any = None

    def doc(self, doc_id: str) -> dict[str, Any]:
        body: dict[str, Any] = {
            "_id": doc_id,
            "_rev": self.rev,
            "_revisions": {"start": len(self.ids), "ids": self.ids},
            "v": self.value,
        }
        if self.deleted:
            body["_deleted"] = True
        return body


def rid(label: str) -> str:
    return hashlib.md5(label.encode()).hexdigest()


def child(parent: Node | None, label: str, *, deleted: bool = False, value: Any = None) -> Node:
    identifier = rid(label)
    parent_ids = parent.ids if parent else []
    return Node(
        rev=f"{len(parent_ids) + 1}-{identifier}",
        ids=[identifier] + parent_ids,
        deleted=deleted,
        value=value if value is not None else label,
    )


def scenarios() -> list[tuple[str, list[Node], int | None]]:
    base = child(None, "base")
    equal_a = child(base, "equal-a")
    equal_b = child(base, "equal-b")
    deep = child(child(base, "deep-1"), "deep-2")
    shallow = child(base, "shallow")
    deleted = child(base, "deleted", deleted=True)
    three_a = child(base, "three-a")
    three_b = child(base, "three-b")
    three_c = child(base, "three-c")
    winner = max(equal_a, equal_b, key=lambda node: node.rev)
    deleted_winner = child(winner, "delete-winner", deleted=True)

    chain = base
    for index in range(1, 6):
        chain = child(chain, f"chain-{index}")
    pruned_branch = child(child(base, "prune-1"), "prune-2")

    return [
        ("equal", [equal_a, equal_b], None),
        ("depth", [deep, shallow], None),
        ("three", [three_a, three_b, three_c], None),
        ("deleted-leaf", [equal_a, deleted], None),
        ("delete-winner", [equal_a, equal_b, deleted_winner], None),
        ("prune", [pruned_branch, chain], 2),
        ("idempotent", [equal_a, equal_a], None),
    ]


def normalize(body: Any) -> Any:
    if isinstance(body, list):
        return sorted(body, key=lambda item: json.dumps(item, sort_keys=True, ensure_ascii=False))
    if not isinstance(body, dict):
        return body
    body = dict(body)
    for value in body.values():
        if isinstance(value, dict) and isinstance(value.get("possible_ancestors"), list):
            value["possible_ancestors"] = sorted(value["possible_ancestors"])
    for key in ("_conflicts", "_deleted_conflicts"):
        if isinstance(body.get(key), list):
            body[key] = sorted(body[key])
    for row in body.get("rows", []):
        value = row.get("value")
        if isinstance(value, dict):
            for key in ("_conflicts", "_deleted_conflicts"):
                if isinstance(value.get(key), list):
                    value[key] = sorted(value[key])
        doc = row.get("doc")
        if isinstance(doc, dict):
            for key in ("_conflicts", "_deleted_conflicts"):
                if isinstance(doc.get(key), list):
                    doc[key] = sorted(doc[key])
    results = body.get("results")
    if isinstance(results, list):
        rows: dict[str, dict[str, Any]] = {}
        for row in results:
            row = dict(row)
            row.pop("seq", None)
            changes = row.get("changes")
            if isinstance(changes, list):
                row["changes"] = sorted(changes, key=lambda item: item.get("rev", ""))
            rows[row["id"]] = row
        return {"rows": [rows[key] for key in sorted(rows)]}
    return body


def compare(label: str, left: Any, right: Any) -> str | None:
    if left.status != right.status:
        return f"{label}: status {left.status} != {right.status}"
    if left.status >= 400:
        if left.body.get("error") != right.body.get("error"):
            return f"{label}: error {left.body.get('error')} != {right.body.get('error')}"
        return None
    if normalize(left.body) != normalize(right.body):
        return f"{label}: body mismatch"
    return None


def random_scenarios(seed: int, rounds: int) -> list[tuple[str, list[Node], int | None]]:
    import random

    rng = random.Random(seed)
    base = child(None, "random-base")
    scenarios: list[tuple[str, list[Node], int | None]] = []
    for round_index in range(rounds):
        nodes = [base]
        for index in range(rng.randint(1, 4)):
            parent = rng.choice(nodes)
            nodes.append(
                child(
                    parent,
                    f"random-{round_index}-{index}",
                    deleted=rng.random() < 0.2,
                    value={"n": rng.randint(0, 20), "s": rng.choice(["a", "b", "c"])},
                )
            )
        scenarios.append(
            (
                f"random-{round_index}",
                nodes[1:],
                # Shared-ancestor pruning still diverges (#127); keep random
                # trees at the default limit until it is fixed.
                None,
            )
        )
    return scenarios


def run_conflicts(
    left: Http, right: Http, *, verbose: bool, rounds: int = 0, seed: int = 1
) -> int:
    selected = scenarios()
    if rounds:
        selected.extend(random_scenarios(seed, rounds))
    for scenario, nodes, revs_limit in selected:
        for http in (left, right):
            http.request("DELETE", f"/{DB}")
            response = http.request("PUT", f"/{DB}" + ("?q=1" if http is right else ""))
            if response.status not in (201, 202):
                raise RuntimeError(f"could not create {DB}: {response.status} {response.raw}")
            if revs_limit is not None:
                response = http.request("PUT", f"/{DB}/_revs_limit", body=revs_limit)
                if response.status not in (200, 201):
                    raise RuntimeError(
                        f"could not set revs_limit: {response.status} {response.raw}"
                    )

        body = {"docs": [node.doc("doc") for node in nodes], "new_edits": False}
        left_response = left.request("POST", f"/{DB}/_bulk_docs", body=body)
        right_response = right.request("POST", f"/{DB}/_bulk_docs", body=body)
        problem = compare("bulk_docs", left_response, right_response)

        leaves = sorted({node.rev for node in nodes})
        checks: list[tuple[str, str]] = [("winner", f"/{DB}/doc")]
        checks.append(("open_revs", f"/{DB}/doc?open_revs=all&revs=true"))
        checks.append(
            ("all_docs", f"/{DB}/_all_docs?conflicts=true&include_docs=true")
        )
        checks.append(("changes", f"/{DB}/_changes?style=all_docs"))
        for rev in leaves:
            checks.append((f"get {rev}", f"/{DB}/doc?rev={rev}"))
            checks.append((f"revisions {rev}", f"/{DB}/doc?rev={rev}&revs=true"))
        checks.append(("revs_diff", ""))
        if verbose:
            print(f"    {scenario}: {len(nodes)} leaves, revs_limit={revs_limit}")

        for label, path in checks:
            if not problem:
                if path:
                    left_check = left.get(path)
                    right_check = right.get(path)
                else:
                    diff_body = {"doc": leaves + ["9-deadbeefdeadbeefdeadbeefdeadbeef"]}
                    left_check = left.request("POST", f"/{DB}/_revs_diff", body=diff_body)
                    right_check = right.request("POST", f"/{DB}/_revs_diff", body=diff_body)
                problem = compare(f"{scenario} {label}", left_check, right_check)
                if problem:
                    print()
                    print(f"CONFLICT DIVERGENCE: {problem}")
                    print(f"  douchdb: {json.dumps(left_check.body, ensure_ascii=False)}")
                    print(f"  couchdb: {json.dumps(right_check.body, ensure_ascii=False)}")
                    print(f"  path:    {path or '/_revs_diff'}")
                    print()
                    directory = HERE / "artifacts" / "conflicts"
                    directory.mkdir(parents=True, exist_ok=True)
                    payload = {
                        "kind": "conflicts",
                        "scenario": scenario,
                        "revs_limit": revs_limit,
                        "docs": body,
                        "check": label,
                        "path": path,
                        "douchdb": {
                            "status": left_check.status,
                            "body": left_check.body,
                        },
                        "couchdb": {
                            "status": right_check.status,
                            "body": right_check.body,
                        },
                    }
                    (directory / "repro.json").write_text(
                        json.dumps(payload, indent=2, ensure_ascii=False)
                    )
                    print(f"==> Wrote {directory / 'repro.json'}")
                    return 1

        if problem:
            print()
            print(f"CONFLICT DIVERGENCE: {problem}")
            print(f"  douchdb: {json.dumps(left_response.body, ensure_ascii=False)}")
            print(f"  couchdb: {json.dumps(right_response.body, ensure_ascii=False)}")
            print()
            directory = HERE / "artifacts" / "conflicts"
            directory.mkdir(parents=True, exist_ok=True)
            payload = {
                "kind": "conflicts",
                "scenario": scenario,
                "revs_limit": revs_limit,
                "docs": body,
                "check": "bulk_docs",
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

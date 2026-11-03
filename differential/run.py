#!/usr/bin/env python3
"""Differential fuzzer for douchdb against Apache CouchDB.

The fuzzer sends the same random document operations to a douchdb instance and
a CouchDB instance, compares status codes, error codes, revision ids and
subsequent document state, and minimizes the first divergence to a small,
replayable operation list.

This is a single-node API test. It complements, but does not replace, the
cluster and fault-injection tests in ``jepsen-clj/``.
"""

from __future__ import annotations

import argparse
import base64
import copy
import json
import os
import random
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
HERE = Path(__file__).resolve().parent
DEFAULT_DOUCHDB_IMAGE = "douchdb:differential"
DEFAULT_COUCHDB_IMAGE = "couchdb:3.5"
DB = "diff"
LARGE_INTEGERS = [2**64, 2**63, -(2**63) - 1, 10**30, 10**40]

# Raw bodies bypass json.dumps so duplicate keys and insignificant whitespace
# reach both servers exactly as written. CouchDB decodes duplicate keys
# last-wins, and the revision hash covers the decoded document, not the bytes.
RAW_BODIES = [
    '{"a":1,"a":2}',
    '{"a":1,"b":2,"a":3}',
    '{"nested":{"x":1,"x":2}}',
    '{"a":1}   ',
    '{"a":1}\n',
    '{"\\u00e9":1,"\\u00e9":2}',
    '{"a":1,"b":[1,2,{"c":null}]}',
]


@dataclass
class Response:
    status: int
    body: Any
    raw: str


class Http:
    """Minimal JSON-over-HTTP client with optional basic authentication."""

    def __init__(self, base: str, auth: str | None = None, timeout: float = 10.0):
        parsed = urllib.parse.urlsplit(base)
        if parsed.username is not None:
            user = urllib.parse.unquote(parsed.username)
            password = urllib.parse.unquote(parsed.password or "")
            auth = f"{user}:{password}"
            netloc = parsed.hostname or ""
            if parsed.port is not None:
                netloc = f"{netloc}:{parsed.port}"
            parsed = parsed._replace(netloc=netloc)
            base = urllib.parse.urlunsplit(parsed)
        self.base = base.rstrip("/")
        self.auth = auth
        self.timeout = timeout

    def request(
        self,
        method: str,
        path: str,
        body: Any = None,
        headers: dict[str, str] | None = None,
        raw: str | None = None,
    ) -> Response:
        url = self.base + path
        data = None
        request_headers = {"Accept": "application/json"}
        if headers:
            request_headers.update(headers)
        if raw is not None:
            data = raw.encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        elif body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        if self.auth:
            token = base64.b64encode(self.auth.encode()).decode()
            request_headers["Authorization"] = f"Basic {token}"

        request = urllib.request.Request(
            url, data=data, method=method, headers=request_headers
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
                return Response(response.status, _parse_json(raw), raw)
        except urllib.error.HTTPError as error:
            raw = error.read().decode("utf-8")
            return Response(error.code, _parse_json(raw), raw)
        except OSError as error:
            raise RuntimeError(f"{method} {url} failed: {error}") from error

    def get(self, path: str) -> Response:
        return self.request("GET", path)


def _parse_json(raw: str) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def doc_path(doc_id: str) -> str:
    return f"/{DB}/{urllib.parse.quote(doc_id, safe='')}"


def with_query(path: str, **params: Any) -> str:
    if not params:
        return path
    return path + "?" + urllib.parse.urlencode(params)


def contains_large_integer(value: Any) -> bool:
    """True if value contains an integer outside serde_json's i64/u64 range."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value > 2**64 - 1 or value < -(2**63)
    if isinstance(value, float):
        return False
    if isinstance(value, str):
        return False
    if isinstance(value, list):
        return any(contains_large_integer(item) for item in value)
    if isinstance(value, dict):
        return any(contains_large_integer(item) for item in value.values())
    return False


def make_doc_body(value: Any, doc_id: str, rev: str | None) -> dict[str, Any]:
    body = copy.deepcopy(value) if isinstance(value, dict) else {"value": copy.deepcopy(value)}
    if rev:
        body["_id"] = doc_id
        body["_rev"] = rev
    return body


def value_pool(exclude_large_integers: bool) -> list[Any]:
    values = [
        None,
        True,
        False,
        0,
        1,
        -1,
        2**53,
        2**63 - 1,
        2**64 - 1,
        2**64,
        2**63,
        -(2**63) - 1,
        10**30,
        0.1,
        -0.0,
        1.5e300,
        5e-324,
        "",
        "hello",
        "héllo",
        "emoji 😀",
        "line\nbreak",
        'quote"\\',
        [1, "two", None, {"three": 3}],
        {"a": 1, "b": [True, False]},
        {"b": 2, "a": 1},
        {"unicode-ключ": "значение"},
    ]
    if exclude_large_integers:
        values = [value for value in values if not contains_large_integer(value)]
    return values


def choose_value(rng: random.Random, exclude_large_integers: bool) -> Any:
    if not exclude_large_integers and rng.random() < 0.15:
        return copy.deepcopy(rng.choice(LARGE_INTEGERS))
    return copy.deepcopy(rng.choice(value_pool(exclude_large_integers)))


def generate_ops(
    seed: int,
    steps: int,
    exclude_large_integers: bool,
    exclude_duplicate_keys: bool,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    docs = [
        "doc-0",
        "doc-1",
        "doc-2",
        "doc-3",
        "doc-4",
        "a/b",
        "a b",
        "a?b",
        "a#b",
        "a%b",
        "héllo",
        "emoji-😀",
    ]
    ops: list[dict[str, Any]] = []
    for _ in range(steps):
        doc = rng.choice(docs)
        roll = rng.random()
        if roll < 0.45:
            if not exclude_duplicate_keys and rng.random() < 0.08:
                ops.append(
                    {
                        "op": "put",
                        "doc": doc,
                        "raw": rng.choice(RAW_BODIES),
                        "rev": "none",
                        "stale_offset": 1,
                    }
                )
            else:
                ops.append(
                    {
                        "op": "put",
                        "doc": doc,
                        "value": choose_value(rng, exclude_large_integers),
                        "rev": rng.choice(
                            ["current", "current", "current", "none", "stale"]
                        ),
                        "stale_offset": rng.randint(1, 3),
                    }
                )
        elif roll < 0.75:
            ops.append({"op": "get", "doc": doc})
        elif roll < 0.9:
            ops.append(
                {
                    "op": "delete",
                    "doc": doc,
                    "rev": rng.choice(["current", "current", "none", "stale"]),
                    "stale_offset": rng.randint(1, 3),
                }
            )
        else:
            ops.append(
                {
                    "op": "get",
                    "doc": doc,
                    "rev": rng.choice(["current", "stale"]),
                    "stale_offset": rng.randint(1, 3),
                }
            )
    return ops


def resolve_rev(op: dict[str, Any], state: dict[str, Any]) -> str | None:
    strategy = op.get("rev")
    doc = op["doc"]
    current = state["revs"].get(doc)
    if strategy == "current":
        return current
    if strategy == "none":
        return None
    if strategy == "stale":
        history = state["history"].get(doc, [])
        index = len(history) - 1 - op.get("stale_offset", 1)
        if 0 <= index < len(history):
            return history[index]
        return current
    return None


def divergence_kind(
    op: dict[str, Any], left: Response, right: Response
) -> str | None:
    """Classifies the first difference, or returns None if the pair agrees."""
    large_integer = contains_large_integer(op.get("value"))

    if left.status != right.status:
        if large_integer:
            return "large_integer_precision"
        return "status_mismatch"

    if left.status >= 400:
        left_error = (left.body or {}).get("error") if isinstance(left.body, dict) else None
        right_error = (right.body or {}).get("error") if isinstance(right.body, dict) else None
        if left_error != right_error:
            return "error_code_mismatch"
        return None

    if left.body == right.body:
        return None

    if large_integer or contains_large_integer(left.body) or contains_large_integer(right.body):
        return "large_integer_precision"

    left_rev = left.body.get("rev") if isinstance(left.body, dict) else None
    right_rev = right.body.get("rev") if isinstance(right.body, dict) else None
    if op["op"] in ("put", "delete") and left_rev != right_rev:
        if "raw" in op:
            return "duplicate_key_order"
        return "rev_mismatch"

    return "body_mismatch"


@dataclass
class OpResult:
    left: Response
    right: Response
    kind: str | None

    @property
    def diverged(self) -> bool:
        return self.kind is not None


@dataclass
class Divergence:
    step: int
    op: dict[str, Any]
    left: Response
    right: Response
    kind: str
    ops: list[dict[str, Any]]


def execute_op(
    left: Http,
    right: Http,
    op: dict[str, Any],
    state: dict[str, Any],
) -> OpResult:
    rev = resolve_rev(op, state)
    path = doc_path(op["doc"])

    if op["op"] == "put":
        if "raw" in op:
            left_response = left.request("PUT", path, raw=op["raw"])
            right_response = right.request("PUT", path, raw=op["raw"])
        else:
            body = make_doc_body(op["value"], op["doc"], rev)
            left_response = left.request("PUT", path, body=body)
            right_response = right.request("PUT", path, body=body)
    elif op["op"] == "get":
        query_path = with_query(path, rev=rev) if rev else path
        left_response = left.get(query_path)
        right_response = right.get(query_path)
    elif op["op"] == "delete":
        query_path = with_query(path, rev=rev) if rev else path
        left_response = left.request("DELETE", query_path)
        right_response = right.request("DELETE", query_path)
    else:
        raise ValueError(f"unknown operation {op['op']!r}")

    kind = divergence_kind(op, left_response, right_response)
    if kind is None:
        update_state(state, op, left_response)
    return OpResult(left_response, right_response, kind)


def update_state(state: dict[str, Any], op: dict[str, Any], response: Response) -> None:
    if response.status >= 400 or not isinstance(response.body, dict):
        return
    doc = op["doc"]
    rev = response.body.get("rev")
    if not rev:
        return
    state["history"].setdefault(doc, []).append(rev)
    if op["op"] == "delete":
        state["revs"].pop(doc, None)
    else:
        state["revs"][doc] = rev


def fresh_state() -> dict[str, Any]:
    return {"revs": {}, "history": {}}


def reset(http: Http) -> None:
    http.request("DELETE", f"/{DB}")
    response = http.request("PUT", f"/{DB}")
    if response.status not in (201, 202):
        raise RuntimeError(f"could not create database {DB!r}: {response.status} {response.raw}")


def replay(
    left: Http,
    right: Http,
    ops: list[dict[str, Any]],
) -> Divergence | None:
    reset(left)
    reset(right)
    state = fresh_state()
    for index, op in enumerate(ops):
        result = execute_op(left, right, op, state)
        if result.diverged:
            return Divergence(
                step=index,
                op=op,
                left=result.left,
                right=result.right,
                kind=result.kind,
                ops=ops[: index + 1],
            )
    return None


def minimize(
    left: Http,
    right: Http,
    divergence: Divergence,
    max_replays: int,
) -> list[dict[str, Any]]:
    """Greedily removes operations while preserving the same divergence kind."""
    current = list(divergence.ops)
    signature = (divergence.kind, divergence.op["op"])
    replays = 0
    changed = True
    while changed and replays < max_replays:
        changed = False
        for index in range(len(current)):
            if replays >= max_replays:
                break
            candidate = current[:index] + current[index + 1 :]
            if not candidate:
                continue
            replays += 1
            found = replay(left, right, candidate)
            if found and (found.kind, found.op["op"]) == signature:
                current = candidate
                changed = True
                break
    return current


def describe_response(response: Response) -> str:
    body = json.dumps(response.body, ensure_ascii=False, sort_keys=True)
    if len(body) > 400:
        body = body[:397] + "..."
    return f"{response.status} {body}"


def load_known_divergences(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    with path.open() as handle:
        return json.load(handle)


def print_divergence(divergence: Divergence, known: dict[str, Any] | None) -> None:
    print()
    print(f"DIVERGENCE at step {divergence.step} ({divergence.kind})")
    print(f"  op:      {json.dumps(divergence.op, ensure_ascii=False, sort_keys=True)}")
    print(f"  douchdb: {describe_response(divergence.left)}")
    print(f"  couchdb: {describe_response(divergence.right)}")
    if known:
        print(
            f"  known:   issue #{known['issue']} - {known.get('description', '')}"
        )
        print(f"  remove when: {known.get('remove_when', 'the issue is fixed')}")
    print()


def write_artifacts(
    seed: int,
    ops: list[dict[str, Any]],
    divergence: Divergence,
    minimized: list[dict[str, Any]],
    known: dict[str, Any] | None,
    logs: dict[str, str],
) -> Path:
    directory = HERE / "artifacts" / str(seed)
    directory.mkdir(parents=True, exist_ok=True)
    payload = {
        "seed": seed,
        "ops": ops,
        "minimized_ops": minimized,
        "divergence": {
            "step": divergence.step,
            "kind": divergence.kind,
            "op": divergence.op,
            "douchdb": {"status": divergence.left.status, "body": divergence.left.body},
            "couchdb": {"status": divergence.right.status, "body": divergence.right.body},
            "known_issue": known["issue"] if known else None,
        },
    }
    (directory / "repro.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    for name, text in logs.items():
        (directory / f"{name}.log").write_text(text)
    return directory


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class DockerCluster:
    def __init__(
        self,
        douchdb_image: str,
        couchdb_image: str,
        build: bool,
        keep: bool,
    ):
        self.douchdb_image = douchdb_image
        self.couchdb_image = couchdb_image
        self.build = build
        self.keep = keep
        self.prefix = f"differential-{os.getpid()}"
        self.douchdb_name = f"{self.prefix}-douchdb"
        self.couchdb_name = f"{self.prefix}-couchdb"
        self.douchdb_port = free_port()
        self.couchdb_port = free_port()
        self.douchdb_url = f"http://127.0.0.1:{self.douchdb_port}"
        self.couchdb_url = f"http://127.0.0.1:{self.couchdb_port}"

    def start(self) -> None:
        self._run(["docker", "info"], check=True)
        if self.build:
            print(f"==> Building {self.douchdb_image}", flush=True)
            self._run(["docker", "build", "-t", self.douchdb_image, str(ROOT)], check=True)

        self._remove(self.douchdb_name)
        self._remove(self.couchdb_name)

        print(f"==> Starting douchdb on {self.douchdb_url}", flush=True)
        self._run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                self.douchdb_name,
                "-p",
                f"127.0.0.1:{self.douchdb_port}:5984",
                self.douchdb_image,
                "--listen",
                "0.0.0.0:5984",
                "--data",
                "/data",
                "--q",
                "1",
                "--n",
                "1",
            ],
            check=True,
        )

        print(f"==> Starting CouchDB on {self.couchdb_url}", flush=True)
        self._run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                self.couchdb_name,
                "-p",
                f"127.0.0.1:{self.couchdb_port}:5984",
                "-e",
                "COUCHDB_USER=admin",
                "-e",
                "COUCHDB_PASSWORD=pass",
                self.couchdb_image,
            ],
            check=True,
        )

        wait_ready(Http(self.douchdb_url), "douchdb")
        wait_ready(Http(self.couchdb_url, auth="admin:pass"), "couchdb")

    def stop(self) -> None:
        if self.keep:
            print(f"==> Keeping containers {self.douchdb_name} and {self.couchdb_name}")
            return
        self._remove(self.douchdb_name)
        self._remove(self.couchdb_name)

    def logs(self) -> dict[str, str]:
        logs = {}
        for name, container in (
            ("douchdb", self.douchdb_name),
            ("couchdb", self.couchdb_name),
        ):
            result = self._run(["docker", "logs", container], check=False)
            logs[name] = (result.stdout or "") + (result.stderr or "")
        return logs

    def _remove(self, name: str) -> None:
        self._run(["docker", "rm", "-f", name], check=False)

    def _run(
        self, command: list[str], check: bool
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(command, capture_output=True, text=True, check=check)


def wait_ready(http: Http, name: str, timeout: float = 120.0) -> None:
    deadline = time.time() + timeout
    last_error = ""
    while time.time() < deadline:
        try:
            response = http.get("/")
            if response.status == 200:
                return
            last_error = f"{response.status} {response.raw}"
        except RuntimeError as error:
            last_error = str(error)
        time.sleep(0.25)
    raise RuntimeError(f"{name} did not become ready: {last_error}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Differential fuzz douchdb against Apache CouchDB.",
    )
    parser.add_argument("--seed", type=int, default=1, help="random seed (default: 1)")
    parser.add_argument("--steps", type=int, default=60, help="operations per run (default: 60)")
    parser.add_argument(
        "--exclude-large-integers",
        action="store_true",
        help="omit integers outside i64/u64, so known issue #110 does not stop the run",
    )
    parser.add_argument(
        "--exclude-duplicate-keys",
        action="store_true",
        help="omit raw bodies with duplicate keys, so known issue #123 does not stop the run",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit nonzero even for known divergences",
    )
    parser.add_argument(
        "--no-minimize",
        action="store_true",
        help="do not shrink a divergence before reporting it",
    )
    parser.add_argument(
        "--max-minimize-replays",
        type=int,
        default=200,
        help="replay budget for minimization (default: 200)",
    )
    parser.add_argument(
        "--replay",
        type=Path,
        help="replay a saved repro.json instead of generating operations",
    )
    parser.add_argument(
        "--douchdb-url",
        help="use an existing douchdb at this URL instead of starting Docker",
    )
    parser.add_argument(
        "--couchdb-url",
        help="use an existing CouchDB at this URL instead of starting Docker",
    )
    parser.add_argument(
        "--douchdb-image",
        default=DEFAULT_DOUCHDB_IMAGE,
        help=f"douchdb image (default: {DEFAULT_DOUCHDB_IMAGE})",
    )
    parser.add_argument(
        "--couchdb-image",
        default=DEFAULT_COUCHDB_IMAGE,
        help=f"CouchDB image (default: {DEFAULT_COUCHDB_IMAGE})",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="reuse the douchdb image instead of building it",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="keep Docker containers after the run",
    )
    parser.add_argument(
        "--known-divergences",
        type=Path,
        default=HERE / "known_divergences.json",
        help="path to the known-divergence allowlist",
    )
    parser.add_argument("--verbose", action="store_true", help="print every operation")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    if bool(args.douchdb_url) != bool(args.couchdb_url):
        print("--douchdb-url and --couchdb-url must be supplied together", file=sys.stderr)
        return 2

    known_divergences = load_known_divergences(args.known_divergences)

    cluster: DockerCluster | None = None
    try:
        if args.douchdb_url:
            douchdb_url = args.douchdb_url
            couchdb_url = args.couchdb_url
        else:
            cluster = DockerCluster(
                douchdb_image=args.douchdb_image,
                couchdb_image=args.couchdb_image,
                build=not args.skip_build,
                keep=args.keep,
            )
            cluster.start()
            douchdb_url = cluster.douchdb_url
            couchdb_url = cluster.couchdb_url

        left = Http(douchdb_url)
        right = Http(couchdb_url, auth="admin:pass")

        if args.replay:
            payload = json.loads(args.replay.read_text())
            ops = payload.get("minimized_ops") or payload["ops"]
            print(f"==> Replaying {len(ops)} operations from {args.replay}")
        else:
            ops = generate_ops(
                args.seed,
                args.steps,
                args.exclude_large_integers,
                args.exclude_duplicate_keys,
            )
            print(f"==> Fuzzing seed {args.seed} with {len(ops)} operations")

        divergence = replay(left, right, ops)
        if divergence is None:
            print("No divergence.")
            return 0

        known = known_divergences.get(divergence.kind)
        print_divergence(divergence, known)

        minimized = divergence.ops
        if not args.no_minimize and len(divergence.ops) > 1:
            print(f"==> Minimizing ({args.max_minimize_replays} replay budget)")
            minimized = minimize(
                left,
                right,
                divergence,
                max_replays=args.max_minimize_replays,
            )
            if minimized != divergence.ops:
                print(f"==> Minimized to {len(minimized)} operations:")
                for op in minimized:
                    print("   ", json.dumps(op, ensure_ascii=False, sort_keys=True))
                minimized_divergence = replay(left, right, minimized)
                if minimized_divergence:
                    print_divergence(minimized_divergence, known)
            else:
                print("==> Could not reduce the operation list further")

        logs = cluster.logs() if cluster else {}
        if not args.replay:
            directory = write_artifacts(
                args.seed, ops, divergence, minimized, known, logs
            )
            print(f"==> Wrote {directory / 'repro.json'}")

        if known and not args.strict:
            print(f"Known divergence for issue #{known['issue']}; not failing the run.")
            return 0
        return 1
    finally:
        if cluster:
            cluster.stop()


if __name__ == "__main__":
    sys.exit(main())

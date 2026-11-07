#!/usr/bin/env python3
"""Manual mutation smoke tests for critical correctness paths.

Each mutation is applied to a throwaway copy of the source tree (the real
checkout is never modified, so an interrupted run cannot leave it mutated).
The copy is first built with `--no-run`: a mutation that does not compile is
an ERROR, not a kill. Then the named test is run. A mutation is KILLED when the
test fails; SURVIVED is a test gap.

Cargo's target directory is shared with the checkout (`CARGO_TARGET_DIR`, or
`<repo>/target`) so registry dependencies are not rebuilt. An optional argument
selects the mutations whose name contains it.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

MUTATIONS = [
    (
        "default write quorum",
        "src/fabric.rs",
        "        copies / 2 + 1",
        "        copies",
        ["cargo", "test", "--locked", "--test", "conformance", "cluster_default_write_quorum_is_a_majority"],
    ),
    (
        "changes watermark cap",
        "src/db.rs",
        "            safe = safe.min(committed[index].max(since));",
        "            safe = safe.max(committed[index].max(since));",
        ["cargo", "test", "--locked", "--lib", "changes_cursor_stops_at_allocated_but_uncommitted_shard_sequence"],
    ),
    (
        "winner prefers deleted revisions",
        "src/revtree.rs",
        "                Some(((!self.is_deleted(rev), parsed.pos, parsed.id), rev))",
        "                Some(((self.is_deleted(rev), parsed.pos, parsed.id), rev))",
        ["cargo", "test", "--locked", "--lib", "conflicting_branches_both_remain"],
    ),
    (
        "role check grants every role",
        "src/auth.rs",
        "        self.admin || self.roles.iter().any(|candidate| candidate == role)",
        "        self.admin || true",
        ["cargo", "test", "--locked", "--test", "conformance", "security_object_enforces_members_admins_roles_and_all_dbs_filtering"],
    ),
    (
        "reduce query ignores skip",
        "src/view.rs",
        "    for (key, mut values) in groups.into_iter().skip(query.skip).take(query.limit) {",
        "    for (key, mut values) in groups.into_iter().take(query.limit) {",
        ["cargo", "test", "--locked", "--test", "conformance", "views_map_reduce_collation_and_query_options"],
    ),
    (
        "changes includes the since row",
        "src/store.rs",
        "        let start = since.checked_add(1).map(|seq| (seq, String::new()));",
        "        let start = Some((since, String::new()));",
        ["cargo", "test", "--locked", "--test", "conformance", "changes_returns_latest_per_document_and_pending"],
    ),
]


BUILD_TIMEOUT = 900
TEST_TIMEOUT = 600
IGNORED = shutil.ignore_patterns("target", ".git", ".skis", ".claude", "node_modules", "data", "store")


def run(command: list[str], cwd: Path, env: dict[str, str], timeout: int) -> subprocess.CompletedProcess | None:
    try:
        return subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return None


def main() -> int:
    selected = sys.argv[1] if len(sys.argv) > 1 else ""
    env = dict(os.environ)
    env.setdefault("CARGO_TARGET_DIR", str(ROOT / "target"))
    failures = 0
    with tempfile.TemporaryDirectory(prefix="mutation-smoke-") as scratch:
        work = Path(scratch) / "tree"
        shutil.copytree(ROOT, work, ignore=IGNORED)
        for name, relative, old, new, test in MUTATIONS:
            if selected not in name:
                continue
            path = work / relative
            original = (ROOT / relative).read_text()
            if original.count(old) != 1:
                print(f"ERROR    {name}: expected one match in {relative}, found {original.count(old)}")
                failures += 1
                continue
            path.write_text(original.replace(old, new))
            try:
                build = run([*test, "--no-run"], work, env, BUILD_TIMEOUT)
                if build is None or build.returncode != 0:
                    reason = "timed out" if build is None else "does not compile"
                    print(f"ERROR    {name} ({relative}): mutant {reason}")
                    if build is not None:
                        print(build.stderr[-2000:])
                    failures += 1
                    continue
                result = run(test, work, env, TEST_TIMEOUT)
            finally:
                path.write_text(original)
            if result is None:
                print(f"ERROR    {name} ({relative}): test timed out")
                failures += 1
            elif result.returncode == 0:
                print(f"SURVIVED {name} ({relative})")
                failures += 1
            else:
                print(f"KILLED   {name} ({relative})")
    if failures:
        print(f"FAIL: {failures} mutation(s) survived or could not be checked")
        return 1
    print("PASS: every critical mutation was caught")
    return 0


if __name__ == "__main__":
    sys.exit(main())

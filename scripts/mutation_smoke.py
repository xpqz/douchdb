#!/usr/bin/env python3
"""Manual mutation smoke tests for critical correctness paths.

Each mutation is applied in place, the named test is run, and the file is
restored. A mutation is KILLED when the test fails; SURVIVED is a test gap.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

MUTATIONS = [
    (
        "default write quorum",
        "src/fabric.rs",
        "        replication / 2 + 1",
        "        replication",
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


def main() -> int:
    failures = 0
    for name, relative, old, new, test in MUTATIONS:
        path = ROOT / relative
        original = path.read_text()
        if original.count(old) != 1:
            print(f"ERROR {name}: expected one match in {relative}, found {original.count(old)}")
            failures += 1
            continue
        path.write_text(original.replace(old, new))
        try:
            result = subprocess.run(test, cwd=ROOT, capture_output=True, text=True)
        finally:
            path.write_text(original)
        if result.returncode == 0:
            print(f"SURVIVED {name} ({relative})")
            failures += 1
        else:
            print(f"KILLED   {name} ({relative})")
    if failures:
        print(f"FAIL: {failures} mutation(s) survived or could not be applied")
        return 1
    print("PASS: every critical mutation was caught")
    return 0


if __name__ == "__main__":
    sys.exit(main())

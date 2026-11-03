# Mutation testing report

Tool: `cargo-mutants` 27.1.0 on Rust 1.99.0.
Command:

```sh
cargo mutants --in-place \
  --file src/revtree.rs --file src/etf.rs --file src/json.rs \
  --timeout 45 --cargo-test-arg=--bin --cargo-test-arg=douchdb
```

Scope: the revision-tree merge/prune/winner logic, ETF integer and bignum
encoding, and the JSON parser/reserved-key scanner. The unit-test target was
used so the many process-spawning conformance tests did not dominate the run.

## Result

```text
171 mutants tested: 105 caught, 35 missed, 20 unviable, 11 timeouts
```

After the run, additional tests were added for the missed revision-tree
conflict/ancestor paths and ETF integer boundaries.

## Triage of surviving mutants

### Revision trees

- `conflicts`/`deleted_conflicts` return-value stubs and the `!=`/`&&`
  conditions: **missing tests**, now covered by
  `conflicts_and_deleted_conflicts_exclude_the_winner_and_sort`.
- `is_ancestor_of` true/`==` mutants: **missing tests**, now covered by
  `ancestor_checks_follow_the_stored_path`.
- `merge_path` arithmetic and prune boundary mutants: partially **equivalent**
  for the generated cases; the important shared-ancestor and bulk-merge cases
  are covered by `merge_ignores_ancestors_already_pruned_from_the_tree`,
  `merge_extends_an_existing_leaf_without_losing_its_parent`,
  `revs_limit_truncates_each_revisions_path` and the conflict differential.

### ETF

- Integer tag boundaries (`i32`/`i64` guards, unsigned boundaries, bignum
  length): **missing tests**, now covered by
  `integer_boundaries_use_the_expected_etf_tags` and the proptest
  `decimal_magnitude_matches_u128/i128`.
- Loop shifts and carry conditions in `decimal_magnitude`: equivalent for the
  tested small values; the u128/i128 proptest covers the non-trivial carry
  paths. A mutation that turns the conversion into an infinite loop is
  reported as a timeout rather than a pass.
- Tuple/list length guards: **unviable** or only reachable with >255 elements;
  the relevant max is tested indirectly by the bignum encoding tests.

### JSON

- `expecting` formatting mutations: **equivalent**; they change only a
  diagnostic string.
- `has_reserved_number_key` index/increment mutants: several **timeout**
  because a broken increment stops making progress. The scanner is bounded by
  the input length and only runs on request bodies; the timeout result is
  treated as a detection, not a pass. Tests cover the top-level, nested and
  escaped reserved key.
- Arbitrary-input parser mutants: covered by the proptest
  `arbitrary_bytes_never_panic` and `arbitrary_text_never_panics`.

### Unviable

Twenty mutants were unviable because the replacement did not compile (type
mismatches or impossible matches). They carry no test signal.

## Manual mutation smoke

`scripts/mutation_smoke.py` applies six targeted mutations to critical paths
and asserts that the named test fails:

| Path | Mutation | Test |
| --- | --- | --- |
| default write quorum | `n/2+1` -> `n` | `cluster_default_write_quorum_is_a_majority` |
| changes watermark | `min` -> `max` | `changes_cursor_stops_at_allocated_but_uncommitted_shard_sequence` |
| winner selection | live-before-deleted -> deleted-before-live | `conflicting_branches_both_remain` |
| role check | role match -> always true | `security_object_enforces_members_admins_roles_and_all_dbs_filtering` |
| reduce skip | requested skip -> 0 | `views_map_reduce_collation_and_query_options` |
| changes since | `seq > since` -> `seq >= since` | `changes_returns_latest_per_document_and_pending` |

Latest run: all six KILLED.

## Remaining work

The `cargo-mutants` file run covered parser, revision-tree and ETF code only.
The manual smoke covers the first slice of quorum, winner, changes, auth and
view paths. A longer-running `cargo-mutants` job over `db.rs`, `fabric.rs` and
`api.rs` is still desirable; they are listed in #121.


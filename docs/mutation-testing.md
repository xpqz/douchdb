# Mutation testing report

Tool: `cargo-mutants` 27.1.0 on Rust 1.99.0.
Command:

```sh
cargo mutants --in-place \
  --file src/revtree.rs --file src/etf.rs --file src/json.rs \
  --timeout 45 --cargo-test-arg=--lib
```

(The run below used `--bin douchdb`; unit tests moved to the library target
when the crate gained `src/lib.rs`.)

Scope: the revision-tree merge/prune/winner logic, ETF integer and bignum
encoding, and the JSON parser/reserved-key scanner. The unit-test target was
used so the many process-spawning conformance tests did not dominate the run.

## Result

```text
171 mutants tested: 105 caught, 35 missed, 20 unviable, 11 timeouts
```

After the run, tests closed the actionable survivors: revision-tree
conflict/ancestor assertions, full sign-and-magnitude ETF assertions at the
`i64` boundaries, tuple 255/256 boundaries, signed zero, negative bignums with
a zero low byte, and JSON reserved-key scanning through escaped quotes and
whitespace. A follow-up `cargo mutants --iterate` rerun caught 18 more
mutants; the three remaining survivors below are equivalent.

Run with `TMPDIR` on disk-backed storage. A first run left two build trees in
`/tmp` (a 14 GB tmpfs); `rust-lld` then died with SIGBUS and a batch of
innocent mutants was recorded as unviable after `Disk quota exceeded`. That
run was discarded and rerun in place against the on-disk `target/`.

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
  `integer_boundaries_use_the_expected_etf_tags`,
  `big_integers_encode_the_full_sign_and_magnitude` and the proptest
  `decimal_magnitude_matches_u128/i128`. The tag-only assertions were not
  enough: they leave broken shifts, masks and sign tests alive.
- Tuple length guards: **missing tests**, now covered by
  `tuple_length_selects_the_small_or_large_form` (255 versus 256 elements).
- Carry shifts and trim conditions that cannot change an emitted magnitude
  are **equivalent**: the trim loop only runs for a leading zero byte the
  converter cannot produce, and the sign comparison never sees zero because
  zero takes the small-integer arm. A mutation that turns the conversion into
  an infinite loop is reported as a timeout rather than a pass.

### JSON

- `expecting` formatting mutations: **equivalent**; they change only a
  diagnostic string.
- `has_reserved_number_key` index/increment mutants: several **timeout**
  because a broken increment stops making progress. The scanner is bounded by
  the input length and only runs on request bodies; the timeout result is
  treated as a detection, not a pass. `reserved_number_key_is_found_through_escapes_and_whitespace`
  covers the top-level, nested, whitespace-separated and escaped-quote cases
  that previously let the scanner desynchronise and miss the reserved key.
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


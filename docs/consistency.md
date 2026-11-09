# Consistency model

This document defines what DouchDB guarantees, what it does not, and which test
backs each claim. It is intentionally narrower than CouchDB's documentation:
where behaviour is best-effort, it says so.

## Document writes

- A document is `{_id, _rev, body}`. Revision ids are derived with CouchDB's
  ETF body hash, so the same body produces the same `_rev` on both systems.
- A write without `_rev` creates a document or creates a new revision after a
  tombstone. A write with `_rev` is conditional on that revision being a live
  leaf; a stale or deleted leaf returns `409 conflict`.
- `_bulk_docs` applies each document independently. A failed document does not
  roll back successful documents in the same request.
- `validate_doc_update` functions from live design documents run on every
  document write, including deletes, `_bulk_docs` batches and replicated writes
  received through the HTTP API. A throw of `{unauthorized: reason}` is `401`;
  `{forbidden: reason}` is `403`. Design and `_local` documents are exempt.
  Validators run through the same QuickJS/process/WASM sandbox as view code.
- `new_edits=false` inserts the supplied revision path as-is. It is the
  replication path and may create conflict leaves. Duplicate paths are
  idempotent.
- A write is sent to every replica in the document's shard set. It returns
  `201` once `w` replicas acknowledge (`w` defaults to `n/2+1`), `202` when the
  requested quorum was not met, and a transport/HTTP error otherwise.
- `202` means the write may exist on some replicas and anti-entropy will repair
  the rest. It is not a rollback and it is not a durability claim.

## Document reads

- A read asks `r` replicas (`r` defaults to `n/2+1`, `?r=1` is the local fast
  path). If fewer than `r` replicas answer, the response is
  `503 quorum_not_met`. This is a deliberate departure from CouchDB, whose
  `fabric_doc_open` serves the best document it received when `r` is not met;
  douchdb stays strict because linearizability checks depend on it. When no
  replica answered at all, the errors are CouchDB's: `500 error`/`read_failure`
  for a plain or `revs_info` read, `503 service unavailable`
  (`all_workers_died`) for `open_revs` or an explicit `rev`.
- Replica responses are reconciled by revision ancestry before winner
  selection. A longer ancestry on one replica is not discarded.
- The winner is the live leaf with the highest position, then the greatest
  revision id. Tombstones participate in read reconciliation: a deleted winner
  is returned as `404 deleted` for a normal GET, not hidden behind a stale live
  replica.
- Non-winning live leaves are exposed as `_conflicts` where requested.
- `open_revs`, `revs_info=true` and `_revisions` are revision-shaped reads.
  They still require the requested read quorum, but the response shape is
  served from the winning/local replica.
- `latest=true` resolves an explicitly requested revision to the tip of its
  branch, including a deleted tip, and deduplicates `open_revs` that resolve to
  the same tip.
- `_revisions` and `_conflicts` render revision ids the way they arrived:
  32-character hex ids as lowercase hex, other ids verbatim.
- `_bulk_get` and `_revs_diff` use the same document state; `_bulk_get` returns
  tombstones when the requested revision is deleted.

## `_changes`

- The normal feed returns one row per document at that document's latest
  revision. It does not emit one row for every historical sequence entry.
- `since` accepts `0`, `now` or an opaque `<sum>-<base64url>` token, as
  CouchDB does; another plain integer is a 400. Every sequence douchdb reports
  is such a token. In a cluster it holds a version vector so a partial fan-out
  resumes without skipping a document; standalone it holds the node's one
  sequence.
- `limit` counts documents. `descending=true` returns newest documents first
  and, as in CouchDB 3.5, ignores `since`.
- `pending` is the number of documents after `last_seq` in the requested
  direction.
- `style=all_docs` includes all leaf revisions; the winner is first and the
  remaining leaves follow in descending position order. The default includes
  only the winner.
- `filter=_design`, `filter=_doc_ids`, named design-document filters and
  `filter=_view` with `view=<ddoc>/<view>` are supported. `conflicts=true`
  adds `_conflicts` to the embedded winner document.
- Continuous, longpoll and SSE feeds are at-least-once. Clients must dedupe by
  document id and revision, and must not assume a row is delivered exactly
  once.
- A numeric cursor is watermarked across shards: it never advances past a shard
  with an allocated-but-uncommitted sequence. A resumed feed may re-read a row
  but does not skip one.

## `_all_docs`

- Rows are live documents only; deleted documents are omitted unless requested
  through `keys`.
- `include_docs` embeds the winning document. `conflicts=true` puts
  `_conflicts` only inside the embedded `doc`, matching CouchDB; it is not put
  in `row.value`.
- `startkey`/`endkey`, `descending`, `inclusive_end`, `keys`, `skip` and
  `limit` follow CouchDB. `offset` is the insertion point of the start key plus
  skip, capped at `total_rows`.
- `key` is a range query over live rows, so it omits deleted documents;
  `keys` is the lookup form that reports deleted rows. `update_seq=true` adds
  the local update sequence to the response.
- `_all_docs` is not a snapshot. Concurrent writes may appear between pages.

## Views and search

- Map/reduce views are updated when queried. A query reflects all writes that
  completed before its index update.
- View keys use CouchDB JSON collation: `null < false < true < number <
  string < array < object`; strings use ICU collation; object keys use stored
  member order; integers beyond `f64` precision compare exactly.
- Reduce views support `_count`, `_sum`, `_stats`,
  `_approx_count_distinct` and custom JavaScript reducers. Rows honour
  `group`, `group_level`, `keys`, `startkey`/`endkey`, `descending`, `skip` and
  `limit` with CouchDB's validation rules.
- The native `emit(doc.field, ...)` extractor must produce the same rows as the
  JavaScript path. If it does not recognise a function, it falls back to
  JavaScript.
- View builds use QuickJS in-process, a worker process, wasmi, or Wasmtime.
  The sandbox changes containment and performance, not view semantics.
- Search is an extension, not a CouchDB endpoint. It is eventually updated from
  the same document log and may lag a concurrent write.
- There is no cross-view or cross-document snapshot.

## Replication, anti-entropy and rebalance

- Anti-entropy periodically pulls missing document revisions from peers for
  the shards a node owns. It is not client-triggered and does not require a
  read.
- Replication converges eventually when every replica is reachable and
  anti-entropy has run. `202` writes, missed writes and restarted nodes are
  repaired this way.
- Conflict leaves, tombstones and revision history are replicated.
- `_revs_limit` pruning is applied after a bulk request's paths are merged.
  `_revisions` and `_revs_info` are truncated per returned leaf to the current
  limit.
- Rebalance is operator-triggered. It migrates to a target map while the old
  map serves reads, freezes writes for a bounded window, seeds the target, and
  activates all peers atomically. Cleanup of old replicas is explicit with
  `?cleanup=true`; a timeout leaves the old map in place.
- Validators come from the coordinator's local design documents. A node that
  missed a design-document write validates without it until anti-entropy pulls
  the document (startup, then every `--sync-secs`).
- Static membership: nodes and placement are configuration, not a consensus
  protocol. There is no online membership change.

## Durability

- Acknowledged writes survive a process crash (`kill -9`) because they reach
  the operating-system page cache.
- They do **not** survive power loss: the storage layer does not fsync. See
  [durability.md](durability.md).
- DouchDB is not a primary power-loss-durable store.

## Explicit non-guarantees

- No multi-document transactions or atomic cross-document writes.
- No Mango, lists, shows, or attachments.
- No cross-database atomicity.
- No linearizable `_changes` cursors across independent clients; cursors are
  per-feed opaque values.
- No guarantee that a view or search result is a point-in-time snapshot.
- No online rebalancing without a write freeze.
- No protection against a malicious administrator with the cluster secret.

## Claims to tests

| Claim | Evidence |
| --- | --- |
| CouchDB CRUD, revision ids, ETags, error shapes | `document_crud_matches_couchdb_rev_ids`, `couchdb_basics_ports`, `basic`, `differential/run.py` |
| Big integers, duplicate keys, float spellings | `large_integers_round_trip_and_match_couchdb_rev_ids`, `duplicate_object_keys_use_last_position_like_couchdb`, `float_spellings_are_canonicalised_like_couchdb` |
| `_bulk_docs`, `_all_docs`, `_bulk_get`, `_revs_diff` | `bulk_docs_and_all_docs`, `changes_revs_diff_and_bulk_get`, `differential/run.py --multi` |
| `validate_doc_update` rejects and accepts writes | `validate_doc_update_rejects_writes_like_couchdb`, `cluster_validation_functions_follow_design_docs_across_shards`, `cluster_node_that_missed_a_design_doc_enforces_it_after_anti_entropy`, `validate::tests::design_validators_reject_and_accept_documents` |
| `_changes` latest row, limit, pending, pagination | `changes_returns_latest_per_document_and_pending`, `all_docs_ranges_and_changes_limits_after_deletes`, `differential/run.py --changes` |
| Cluster write/read quorums and `503`/`202` | `cluster_honours_w_and_r_quorum_parameters`, `quorum_read_prefers_tombstone_over_stale_live_replica`, `quorum_read_preserves_conditional_and_explicit_revision_semantics` |
| Conflict winner, leaf retention and revision ancestry | `quorum_read_prefers_conflict_winner_across_replicas`, `quorum_reconciliation_is_coordinator_independent`, `quorum_merges_ancestry_across_replicas_with_different_history_lengths`, `differential/run.py --conflicts` |
| Tombstones and conflict replication | `deleted_leaf_cannot_be_updated_by_rev`, `anti_entropy_replicates_conflicts_and_tombstones` |
| `_revs_limit` pruning and returned history | `revs_limit_truncates_each_revisions_path`, `differential/run.py --conflicts` |
| Map/reduce, grouping, collation, reductions | `views_map_reduce_collation_and_query_options`, `couchdb_view_*`, `differential/run.py --views` |
| Native/JS/sandbox view equivalence | `native_map_matches_javascript_for_generated_corpus`, `js_sandboxes_agree_on_view_rows` |
| View freshness after writes and design changes | `differential/run.py --views --view-mutations N` |
| Search API and cluster fan-out | `couchdb_search_ports`, `search_fans_out_across_cluster_shards`, `cluster_partition_search_ports` |
| Anti-entropy convergence and node loss | `cluster_anti_entropy_converges_without_client_reads`, `cluster_survives_killing_a_node_mid_replication`, `replication_source_and_target_agree_after_convergence` |
| Rebalance freeze, activation and cleanup | `cluster_rebalance_converges_and_cleans_old_replicas`, `cluster_rebalance_timeout_does_not_clean_old_replicas`, `rebalance_verifies_conflict_leaves_before_cleanup` |
| Cross-endpoint consistency under concurrent writes/faults | `internal_consistency_after_concurrent_writes`, `internal_consistency_survives_node_loss_and_restart` |
| Storage corruption fails closed | `scripts/storage_faults.py` |
| Linearizability under faults | `jepsen-clj/README.md` validation table, `differential`/Jepsen nightly workflows |
| Liveness and indeterminate bounds | `jepsen-clj/src/douchdb/liveness.clj`, `internal_consistency_survives_node_loss_and_restart` |

## Compatibility matrix

| Surface | Tested |
| --- | --- |
| CouchDB HTTP API: documents, bulk, all_docs, changes, revisions, views, reduce, validation | Yes, conformance + differential |
| CouchDB 3.5 `couchdb:3.5` server | Yes |
| PouchDB-style document/replication protocol | CouchDB ports + `jepsen` Python harness; upstream PouchDB suite tracked in #122 |
| CouchDB replicator as a client | Partial; `_changes`/`_revs_diff`/`_bulk_get`/`new_edits=false` |
| Search | DouchDB extension only; no CouchDB differential |
| Attachments | No |
| Mango/lists/shows | No |

## Open gaps

- Exact CouchDB client-suite coverage is tracked in #122.
- Mutation testing of critical paths is tracked in #121.
- Cluster-version-vector `pending` is reported as zero; only the single-node
  feed has an exact pending count.
- Corrupt filter/footer handling is tested; arbitrary metadata corruption that
  leaves a readable but incomplete index remains a storage-engine risk.

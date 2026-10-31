# CouchDB test-suite ports

douchdb ports a focused subset of Apache CouchDB's Elixir integration suite
(`test/elixir/test/`). The full suite assumes attachments, Mango, search,
rewrite/show/list/update handlers, `validate_doc_update`, advanced auth and
resharding, so tests are selected by supported feature area rather than copied
one-for-one.

## Ported cases

| CouchDB source | douchdb test |
|---|---|
| `basics_test.exs` | `couchdb_basics_ports` |
| `all_docs_test.exs` | `couchdb_all_docs_ports` |
| `invalid_docids_test.exs`, bulk error cases | `couchdb_invalid_docid_and_bulk_error_ports` |
| `bulk_docs_test.exs` | `couchdb_bulk_docs_ports` |
| `changes_test.exs` (`_doc_ids`, limit, include_docs) | `couchdb_changes_ports` |
| `view_pagination_test.exs` | `couchdb_view_pagination_and_alias_ports` |
| `reduce_builtin_test.exs` | `couchdb_builtin_reduce_ports` |
| `view_include_docs_test.exs`, `utf8_test.exs` | `couchdb_view_include_docs_and_utf8_ports` |
| `view_collation_test.exs` | `couchdb_view_collation_ports` |
| `search_test.exs` | `couchdb_search_ports` |
| `partition_search_test.exs` | `couchdb_partition_search_ports` |
| PouchDB `test.local_docs.js`, `test.revs_diff.js`, `test.bulk_get.js` | `pouchdb_local_docs_revs_diff_and_bulk_get_ports` |
| PouchDB `test.changes.js`, `test.all_docs.js` | `pouchdb_changes_and_all_docs_ports` |
| PouchDB `test.mapreduce.js`, `test.conflicts.js`, `test.uuids.js` | `pouchdb_mapreduce_conflicts_and_uuids_ports` |
| PouchDB `test.reserved.js` | `pouchdb_reserved_ids_ports` |
| PouchDB `test.basics.js`, `test.get.js` error cases | `pouchdb_get_and_error_ports` |
| PouchDB `test.all_docs.js` keys/descending/deleted-doc cases | `pouchdb_all_docs_keys_ports` |
| CouchDB `conflicts_test.exs` header/rev cases | `couchdb_conflict_request_ports` |

The set covers:

- welcome, database lifecycle, `_all_dbs`, database info, document create/get
- ETags on document creation, retrieval, `HEAD` and `If-None-Match`
- `revs_info`, `local_seq`
- `_all_docs` keys, `key`, start/end aliases, body/window limits
- illegal document ids, `_bulk_docs` validation, `all_or_nothing` rejection
- bulk create/update/delete and conflict reporting
- `_changes` doc-id filtering, limits and `include_docs`
- view pagination, offset, descending ranges and parameter aliases
- built-in `_count`, `_sum`, `_stats`, `_approx_count_distinct`
- view `include_docs`, UTF-8 keys, full CouchDB collation order
- object collation by stored member order
- Tantivy search indexes, field queries, drilldown, counts, ranges, groups,
  bookmarks and partitioned search
- PouchDB local-document, revs-diff, bulk-get, changes and all-docs semantics

## Gaps found and fixed

These ports exposed and fixed:

- `_all_docs?key=` and `_all_docs?keys=` were missing.
- `start_key`/`end_key`/`start_key_docid`/`end_key_doc_id` aliases were missing
  for views and `_all_docs`.
- Database creation did not return a `Location` header.
- Single-document PUT did not return an ETag on creation.
- `?local_seq=true` did not return `_local_seq`.
- Reserved-underscore document IDs returned `bad_request` rather than
  `illegal_docid`.
- `_bulk_docs` accepted invalid `new_edits`, reserved IDs and `all_or_nothing`.
- Descending view ranges dropped rows whose start key equalled a supplied
  `startkey_docid`.
- `?rev=` was not applied to the document body, and a body/`If-Match`
  revision mismatch was not rejected.
- Object view keys were collated by sorted member names rather than CouchDB's
  stored member order.

## Deliberately not ported

- Attachments and multipart requests.
- Mango, Search, Nouveau.
- Show/list/rewrite/update handlers and `validate_doc_update`.
- JWT, proxy auth, users DB and security-object administration.
- Resharding, cluster setup/config, compaction/disk-monitor internals.
- JavaScript changes filters and view sandboxing limits.
- Search highlighting, custom sorting, `_index_info`, search timeouts and
  cluster-wide search fan-out.
- CouchDB's attachment, local-doc and replication suites; douchdb has separate
  conformance coverage for replication and local documents.

Run the ports with:

```sh
cargo test --test conformance couchdb_
```

# Search indexes

douchdb implements a Tantivy-backed search index compatible with the common
CouchDB `_search` API shape. Index definitions live in design documents and
are evaluated by the embedded QuickJS runtime. This document covers both the
HTTP API and the mechanics behind it.

## Defining an index

```json
{
  "_id": "_design/inventory",
  "indexes": {
    "fruits": {
      "analyzer": {"name": "standard"},
      "index": "function (doc) { index(\"item\", doc.item, {facet: true}); index(\"place\", doc.place, {facet: true}); index(\"price\", doc.price, {facet: true}); }"
    }
  }
}
```

`index(name, value)` adds a field. Null and undefined values are skipped.
Strings become Tantivy text fields; numbers and booleans become typed fields.
The field set and types are inferred from the documents during the build.
CouchDB's `{facet: true}` and `{store: true}` options are accepted but ignored:
douchdb keeps the emitted values in a sidecar file, so faceting and the
`fields` response do not depend on Tantivy storing them.

## Querying

```sh
curl 'localhost:5984/inventory/_design/inventory/_search/fruits?q=*:*&include_docs=true'
curl 'localhost:5984/inventory/_design/inventory/_search/fruits?q=item:apple'
curl -X POST localhost:5984/inventory/_design/inventory/_search/fruits \
  -H 'Content-Type: application/json' \
  -d '{"q":"*:*","drilldown":["place","kitchen"],"include_docs":true}'
```

Supported parameters:

- `q` / `query` — Tantivy query syntax; `*:*` matches everything. An empty
  query is treated as `*:*`.
- `limit`, `bookmark` — paging; bookmarks are opaque strings returned by the
  previous response.
- `include_docs` — attach the current stored document to each row.
- `drilldown` — repeated definitions are ANDed; values in one definition are
  ORed. Nested definitions such as `[["state","old"],["item","apple"]]` are
  supported.
- `counts` — JSON array of fields; returns per-value counts.
- `ranges` — JSON object of field name to label/range pairs. Numeric ranges use
  `[lower TO upper]` with `Infinity` available.
- `group_field` — returns `groups` grouped by a field value.
- `sort` — a field name, `-field` for descending, or a JSON array such as
  `["-price", "item"]`. CouchDB's optional `<type>` suffix is accepted and
  ignored because field types are inferred when the index is built.
- `highlight_fields` — JSON array (or comma-separated list) of fields to
  highlight. Matching query terms are wrapped in `highlight_pre_tag` /
  `highlight_post_tag`, with `highlight_size` and `highlight_number`
  controlling fragment length and count.
- `timeout` — request timeout in milliseconds (`30000` by default). `0` makes
  the request fail immediately with CouchDB's `timeout` error shape.

Partitioned form:

```text
GET /{db}/_partition/{partition}/_design/{ddoc}/_search/{index}
```

Search responses contain `total_rows`, `bookmark`, and `rows` entries with
`id`, `fields`, `order`, and optionally `doc` and `highlights`. Facet counts,
ranges and groups are added when requested. `order` contains the values used
for each requested sort field.

`GET /{db}/_design/{ddoc}/_search/{index}` and
`POST /{db}/_design/{ddoc}/_search/{index}` accept the same options; the POST
body is a JSON object. `drilldown` may be repeated in both forms.

## How search works

Search is a derived, rebuildable index. The database documents remain the
source of truth; `_search_indexes/` can be deleted at any time and the next
query recreates what it needs.

### On-disk layout

Each design-document index gets one directory:

```text
{db}/_search_indexes/{md5(ddoc_id + NUL + index_name)}/
    meta.json                  # Tantivy's index metadata
    ...                        # Tantivy segment and term files
    douchdb-search-meta.json   # douchdb sidecar
```

The sidecar contains:

- the search-index format version,
- the database update sequence and design signature the build used,
- the design-document id and index name,
- the raw `id` and emitted field values for every indexed document.

Tantivy owns query matching. The sidecar owns everything that must preserve
CouchDB's JSON values exactly: the `fields` response, counts, ranges, grouping
and custom sorting. Keeping both means a rebuild does not need to decode
Tantivy's stored representation back into JSON.

### Index identity and freshness

The directory name is derived only from the design-document id and index name.
The contents are considered current when all three match:

1. the sidecar format version (`SEARCH_VERSION`),
2. the database's local update sequence,
3. a design signature over `SEARCH_VERSION`, the analyzer name and the index
   function source.

Any document write advances the database update sequence, so the next search
rebuilds the index. Editing the design document changes the signature and also
forces a rebuild. Bumping `SEARCH_VERSION` invalidates every existing index
after an upgrade. There is no incremental update: the index is rebuilt from
scratch rather than patching Tantivy segments.

### Building an index

A build runs synchronously inside a `_search` or `_search_info` request that
first observes a stale or missing index. `_index_info` is read-only: it lists
existing index directories without building them.

1. Read the current live documents from the node's local shards. Design
   documents (`_design/...`) and local documents (`_local/...`) are excluded.
2. Execute the design document's `index` function in QuickJS with CouchDB's
   helper:

   ```js
   index(name, value, options)
   ```

   The function must be a function; otherwise the request fails with
   `500 search_index_error`. `null` and `undefined` values are skipped.
   Documents are processed in batches of 128. The same `view_timeout_ms` and
   `view_memory_mb` limits used by views bound the runtime.
3. Infer one type per field across all emitted documents:
   - numbers become `f64` fields,
   - booleans become boolean fields,
   - strings become text fields,
   - anything else (arrays, objects) is treated as text and serialized as
     JSON,
   - a field seen with both numeric and boolean values (or any mix involving
     text) falls back to text.
4. Create a fresh Tantivy schema:
   - `_id`: string, indexed, stored and fast,
   - text fields: Tantivy `TEXT` (tokenized and indexed),
   - numeric fields: `f64`, indexed and stored,
   - boolean fields: indexed and stored.
5. Add every document and commit the Tantivy writer.
6. Write the sidecar with the snapshot's update sequence and all raw values.

The build replaces the directory, so a failed build leaves no usable index;
the next request sees the missing or stale metadata and rebuilds again.

### Query execution

A query follows this path:

1. Load the design document and locate `indexes.<name>`.
2. Ensure the index is current (possibly rebuilding it).
3. Open the Tantivy index and run the query.
   - `*:*` or an empty query becomes Tantivy's `AllQuery`.
   - Everything else is parsed by Tantivy's query parser. All indexed fields
     except `_id` are default fields, so `apple` searches every field while
     `item:apple` restricts the match to `item`.
   - Results are collected as an unordered document set. douchdb does not use
     Tantivy relevance scores, so there is no `_score` ordering by default.
4. Map each Tantivy hit back to its `_id`, then to the sidecar entry. This is
   where the original JSON values come from.
5. Apply partition, cluster owner-shard and `drilldown` filters.
6. Sort, count, page and decorate the rows.

### Sorting and paging

Without `sort`, matches are ordered by `_id` ascending. With `sort`, each field
is compared using CouchDB's JSON collation (`null` < `false` < `true` <
numbers < strings < arrays < objects). Missing fields sort before present
fields. Ties are broken by `_id` ascending. Fields are read from the sidecar,
not from Tantivy's fast fields, so sorting works on any emitted field.

`total_rows` is the number of matches after filtering, before paging.

A bookmark is an offset into the sorted result set, encoded as `search:N`
(`N` alone is also accepted). The response bookmark points at the first row
after the returned page. `limit=0` returns no rows but still computes
`total_rows` and facets. At most 2,000 rows are returned per request, including
partitioned searches; a larger partition limit is rejected.

### Counts, ranges and groups

These are computed in Rust over the filtered match set, not by Tantivy facet
collectors:

- `counts` counts exact raw values. String values are keyed as themselves;
  other values are keyed by their JSON encoding. A field absent from a
  document is not counted.
- `ranges` accepts CouchDB-style strings such as `[0 TO 0.99]`. Square
  brackets are inclusive and braces are exclusive. `*`, `Infinity` and
  `-Infinity` are supported. Comparisons use numeric ordering when both the
  value and the bound parse as numbers, otherwise lexicographic string
  ordering. Other value types do not match a range.
- `group_field` groups by the raw value of the field (or `null` when absent).
  Group keys are sorted as strings. Groups contain every matching document;
  group member rows follow the top-level sort order but carry an empty `order`
  array. The top-level `limit` does not paginate within a group, and
  `group_limit` / `group_sort` are not implemented.

`drilldown` is exact equality on the raw emitted value, not a tokenized
search. A definition may contain several values (OR); separate definitions
are ANDed.

### Highlighting

Highlighting is deliberately simple:

1. Extract candidate terms from the query by splitting on whitespace and
   parentheses, removing field prefixes and operators, and lowercasing.
2. Look for the first case-insensitive substring occurrence of each term in
   each requested string field.
3. Return up to `highlight_number` fragments, each roughly `highlight_size`
   characters wide, wrapping the matched text in the pre/post tags.

It does not use the analyzer, stemming, token positions or phrase matching, so
it can highlight a substring that Tantivy did not tokenize as a match. Only
string-valued fields produce `highlights`.

### `include_docs`

`include_docs` fetches the current stored document by id after paging, using the
same read path as views. It does not return the historical snapshot that was
indexed if the document has changed since the build.

### Cluster fan-out

A clustered database keeps one search index per node, built from the documents
that node stores. The coordinator does not need every shard locally:

1. Assign each shard to exactly one preferred node. The coordinator prefers
   itself when it holds a replica; otherwise it chooses the primary owner.
   This produces disjoint owner masks.
2. Query the local node and every peer concurrently. Forwarded requests carry
   a `shards=` mask and only return documents whose shard is marked. Each
   node's index may contain extra replicas; the mask filters them out.
3. Merge the responses: add `total_rows`, deduplicate rows by `_id`, sum
   `counts` and `ranges`, concatenate groups with the same `by` value, then
   apply the global sort, bookmark offset and limit.
4. If an owner is unreachable, reassign its shards to live replicas from the
   shard map and re-query. If no replica is reachable, the request fails with
   `502`.

In the common three-node, full-replication case the coordinator holds every
shard, so the whole search is answered locally without HTTP fan-out.

`_search_info` and `_index_info` read the local node's indexes; they are not
fanned out. Design documents are replicated to every node, so every node can
build the same definition.

### Timeouts and cancellation

The request `timeout` is checked before and after the index build. The
JavaScript index function is additionally bounded by `view_timeout_ms` and
`view_memory_mb`, which abort runaway scripts. Tantivy query execution itself
is not interruptible at an arbitrary point; the timeout is a boundary check,
not a hard cancellation of an in-flight segment scan.

## Index information and cleanup

```text
GET  /{db}/_design/{ddoc}/_search_info/{index}
GET  /{db}/_index_info?type=search
POST /{db}/_search_cleanup
```

`_search_info` ensures the index is current, then reports the indexed document
count, the definition signature and the expected `updates_pending` shape.
`_index_info?type=search` returns the same object under `search_indexes`, with
empty `view_indexes` and `nouveau_indexes` lists. It scans the local
`_search_indexes/` directories and reads their sidecars.

`_search_cleanup` removes every search index directory for the database. It
does not need valid design documents and does not remove views.

## Limits and unsupported behavior

- **Analyzer tuning is fixed.** The analyzer name is part of the design
  signature but all text fields use Tantivy's default tokenizer.
- **No relevance ranking.** Results are ordered by `_id` or by `sort`; there is
  no BM25 score ordering or `_score` sort.
- **No incremental indexing.** Every document write invalidates the index and
  the next query performs a full rebuild.
- **No background indexer.** Builds happen in the request path, limited by the
  view timeout and memory settings.
- **Concurrent rebuilds are not serialized.** Two requests that both observe a
  stale index may rebuild it redundantly; if one fails, the next request
  rebuilds from the missing/stale metadata.
- **Indexed snapshot vs. current document.** `fields`, `counts`, ranges and
  groups come from the snapshot; `include_docs` fetches the current document.
- **Text fields are not stored in Tantivy.** Response fields and facets come
  from the sidecar, so arbitrary emitted JSON values round-trip.
- **Arrays and objects are not deeply indexed.** They are stringified as JSON
  and treated as text; querying inside them is token-based, not structured.
- **Unsupported CouchDB search options.** `include_fields`, `group_limit`,
  `group_sort` and `stale` are not implemented. `facet`/`store` index options
  are accepted but ignored.
- **Cluster info is local.** `_search_info` and `_index_info` do not aggregate
  across nodes.
- **Bookmarks are offsets.** A malformed bookmark restarts at offset 0, and
  cluster paging is bounded by the 2,000-row page limit.

## Tests

`tests/conformance.rs` includes ports from CouchDB's search suite covering:

- GET/POST `*:*` and field queries
- `include_docs`
- drilldown (single, multiple, nested, repeated GET)
- counts and numeric ranges
- grouping
- custom sorting and highlighting
- request timeouts and `_index_info?type=search`
- update/rebuild after a document change
- search info and cleanup
- partitioned search with bookmarks and the partition limit
- cluster-wide fan-out across shards with merged counts and sorting

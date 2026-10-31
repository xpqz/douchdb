# Search indexes

douchdb implements a Tantivy-backed search index compatible with the common
CouchDB `_search` API shape. Index definitions live in design documents and
are evaluated by the embedded QuickJS runtime.

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

## Querying

```sh
curl 'localhost:5984/inventory/_design/inventory/_search/fruits?q=*:*&include_docs=true'
curl 'localhost:5984/inventory/_design/inventory/_search/fruits?q=item:apple'
curl -X POST localhost:5984/inventory/_design/inventory/_search/fruits \
  -H 'Content-Type: application/json' \
  -d '{"q":"*:*","drilldown":["place","kitchen"],"include_docs":true}'
```

Supported parameters:

- `q` / `query` — Tantivy query syntax; `*:*` matches everything.
- `limit`, `bookmark` — paging; bookmarks are opaque strings returned by the
  previous response.
- `include_docs`
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
- `timeout` — request timeout in milliseconds. `0` makes the request fail
  immediately with CouchDB's `timeout` error shape.

Partitioned form:

```text
GET /{db}/_partition/{partition}/_design/{ddoc}/_search/{index}
```

Search responses contain `total_rows`, `bookmark`, and `rows` entries with
`id`, `fields`, `order`, and optionally `doc` and `highlights`. Facet counts,
ranges and groups are added when requested. `order` contains the values used
for each requested sort field.

## Index information and cleanup

```text
GET  /{db}/_design/{ddoc}/_search_info/{index}
GET  /{db}/_index_info?type=search
POST /{db}/_search_cleanup
```

The info endpoint reports the indexed document count, the definition
signature and the expected `updates_pending` shape. `_index_info?type=search`
returns the same object under `search_indexes`, with empty `view_indexes` and
`nouveau_indexes` lists. Cleanup removes all search index directories for the
database.

## Implementation and limits

- The index is stored under `_search_indexes/` and rebuilt when the database
  update sequence or the design-document index definition changes.
- Index values used for counts/ranges/groups are kept in a sidecar JSON file;
  Tantivy handles query matching.
- The `analyzer` name is accepted but currently all string fields use Tantivy's
  default tokenizer. Analyzer tuning is intentionally out of scope.
- Cluster search assigns each shard to one preferred owner, queries those
  owners concurrently and merges/deduplicates rows, counts, ranges and groups.
  If an owner is unreachable the coordinator falls back to another live
  replica. In the common three-node, full-replication case the coordinator can
  answer locally without any fan-out.
- Search builds against the documents stored on the local node. In a cluster
  with fewer replicas than nodes, a non-owner does not index the documents it
  does not hold; the coordinator fan-out supplies those rows from the owners.
- A search bookmark is an offset into the merged result window. Cluster
  pagination beyond the 2,000-row page limit is therefore not supported, the
  same as a single-node limit of 2,000 rows per request.

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

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

Partitioned form:

```text
GET /{db}/_partition/{partition}/_design/{ddoc}/_search/{index}
```

Search responses contain `total_rows`, `bookmark`, and `rows` entries with
`id`, `fields`, `order`, and optionally `doc`. Facet counts, ranges and groups
are added when requested.

## Index information and cleanup

```text
GET  /{db}/_design/{ddoc}/_search_info/{index}
POST /{db}/_search_cleanup
```

The info endpoint reports the indexed document count, the definition
signature and the expected `updates_pending` shape. Cleanup removes all search
index directories for the database.

## Implementation and limits

- The index is stored under `_search_indexes/` and rebuilt when the database
  update sequence or the design-document index definition changes.
- Index values used for counts/ranges/groups are kept in a sidecar JSON file;
  Tantivy handles query matching.
- The `analyzer` name is accepted but currently all string fields use Tantivy's
  default tokenizer.
- Highlighting, custom sorting, `_index_info`, search timeouts and cluster-wide
  search fan-out are not implemented yet.
- Search currently indexes local documents only; for n<3, a coordinator without
  every shard does not have a complete search index.

## Tests

`tests/conformance.rs` includes ports from CouchDB's search suite covering:

- GET/POST `*:*` and field queries
- `include_docs`
- drilldown (single, multiple, nested, repeated GET)
- counts and numeric ranges
- grouping
- update/rebuild after a document change
- search info and cleanup
- partitioned search with bookmarks and the partition limit

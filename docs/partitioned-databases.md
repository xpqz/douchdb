# Partitioned databases

Partitioned databases group related documents by a partition key. douchdb
implements the CouchDB 3.x partitioned-database API: `partition:docid`
document ids, partition-based shard placement, `_partition` endpoints, and
partitioned or global design documents.

## Creating a partitioned database

```sh
curl -X PUT 'http://localhost:5984/sensors?partitioned=true'
```

The flag is stored in the database's `db.json`. A normal `GET /sensors` then
reports it:

```json
{
  "db_name": "sensors",
  "doc_count": 3,
  "doc_del_count": 0,
  "props": {
    "partitioned": true
  }
}
```

Non-partitioned databases do not include `props`.

In a cluster the flag is propagated to every peer as part of database
creation.

## Document ids

Every document except design and local documents must have the form:

```text
partition:docid
```

The partition is everything before the **first** colon. The document id is
everything after it and may contain more colons.

Valid:

```text
field-1:reading-1
field-1:2026:10:30
```

Invalid:

```text
reading-1          missing colon
:reading-1         empty partition
_bad:reading-1     underscore-leading partition
field-1:           empty document id
field-1:_private   underscore-leading document id
```

`PUT`, `POST`, `_bulk_docs` and replication writes all validate ids.
`_bulk_docs` reports failures per document; the rest of the batch still
applies.

`POST /{db}` cannot generate a server-side id in a partitioned database
because a generated UUID has no partition. Supply `_id` explicitly.

Design documents (`_design/...`) and local documents (`_local/...`) are
global and exempt from the rule.

## Writing documents

```sh
curl -X POST http://localhost:5984/sensors/_bulk_docs \
  -H 'Content-Type: application/json' \
  -d '{
    "docs": [
      {"_id": "field-1:reading-1", "value": 0.15},
      {"_id": "field-1:reading-2", "value": 0.14},
      {"_id": "field-2:reading-1", "value": 0.22}
    ]
  }'
```

## Partition information

```sh
curl http://localhost:5984/sensors/_partition/field-1
```

```json
{
  "db_name": "sensors",
  "doc_count": 2,
  "doc_del_count": 0,
  "partition": "field-1",
  "sizes": {
    "active": 0,
    "external": 0
  }
}
```

`doc_count` is the number of live documents in the partition.
`doc_del_count` counts documents whose winning revision is a tombstone.

In a cluster, the coordinator forwards this request to a replica of the
partition's shard when it does not hold one itself.

## Partition `_all_docs`

```sh
curl http://localhost:5984/sensors/_partition/field-1/_all_docs
```

```json
{
  "total_rows": 2,
  "offset": 0,
  "rows": [
    {"id": "field-1:reading-1", "key": "field-1:reading-1", "value": {"rev": "1-..."}},
    {"id": "field-1:reading-2", "key": "field-1:reading-2", "value": {"rev": "1-..."}}
  ]
}
```

The usual `_all_docs` options are supported:

```sh
# Range. Keys are full document ids, clamped to the partition.
curl 'http://localhost:5984/sensors/_partition/field-1/_all_docs?startkey=%22field-1:reading-2%22'

# Newest first.
curl 'http://localhost:5984/sensors/_partition/field-1/_all_docs?descending=true&limit=1'

# Include document bodies.
curl 'http://localhost:5984/sensors/_partition/field-1/_all_docs?include_docs=true'

# Key lookup.
curl -X POST http://localhost:5984/sensors/_partition/field-1/_all_docs \
  -H 'Content-Type: application/json' \
  -d '{"keys": ["field-1:reading-1", "field-2:reading-1", "missing"]}'
```

`total_rows` is the number of live documents in the partition. Keys outside
the partition return an `{"error": "not_found"}` row. `offset`, `skip`,
`limit` and descending order all operate within the partition range.

Global `_all_docs` still works on a partitioned database and spans all
partitions:

```sh
curl http://localhost:5984/sensors/_all_docs
```

## Partitioned views

A design document in a partitioned database is partitioned by default:

```sh
curl -X PUT http://localhost:5984/sensors/_design/readings \
  -H 'Content-Type: application/json' \
  -d '{
    "views": {
      "by_time": {
        "map": "function(doc) { emit(doc.time, doc.value); }"
      },
      "count": {
        "map": "function(doc) { emit(doc.time, 1); }",
        "reduce": "_count"
      }
    }
  }'
```

Query it through the partition path:

```sh
curl http://localhost:5984/sensors/_partition/field-1/_design/readings/_view/by_time
curl http://localhost:5984/sensors/_partition/field-1/_design/readings/_view/count
```

- Map and reduce queries are scoped to the partition.
- `total_rows` is the number of map rows in the partition.
- `startkey`, `endkey`, `key`, `keys`, `descending`, `skip`, `limit`,
  `include_docs` and grouping work as usual inside the partition.
- Querying a partitioned view without `_partition` returns `400`.

### Global views

A design document that needs to query across partitions opts out explicitly:

```sh
curl -X PUT http://localhost:5984/sensors/_design/all_readings \
  -H 'Content-Type: application/json' \
  -d '{
    "options": {"partitioned": false},
    "views": {
      "by_field": {
        "map": "function(doc) { emit(doc.field, doc.value); }"
      }
    }
  }'
```

Query it through the normal path:

```sh
curl http://localhost:5984/sensors/_design/all_readings/_view/by_field
```

A global design document ignores partition placement and behaves like a view
on a non-partitioned database. Querying a global view through `_partition`
returns `400`. A design document is either partitioned or global; views in one
design document cannot mix the two.

## Placement and the cluster

Partitioned databases hash the partition key, not the full document id. Every
document in a partition therefore lives on one shard and its replica set.

Consequences:

- Partition data lives on one shard's replica set. Cluster `_all_docs` and
  view fan-out still contacts the other nodes with owner masks, but only the
  partition's shard returns rows.
- Rebalancing moves whole shards, so a partition moves with its shard.
- n=3 on three nodes still keeps every partition on every node.
- Any node can coordinate; nginx can round-robin across nodes.
- Partition metadata is forwarded to a replica when the coordinator does not
  hold one. Partition reads use normal owner-mask fan-out and shard-mask
  replica fallback.

## Errors

| Condition | Status | Error |
|---|---:|---|
| Document id has no colon | 400 | `illegal_docid` |
| Empty partition | 400 | `illegal_partition` |
| Partition starts with `_` | 400 | `illegal_partition` |
| Empty document id after the colon | 400 | `illegal_docid` |
| Document id after the colon starts with `_` | 400 | `illegal_docid` |
| Partition endpoint on a non-partitioned database | 400 | bad request |
| Partitioned view queried globally | 400 | bad request |
| Global view queried through `_partition` | 400 | bad request |
| Conflicting `partition` query value | 400 | bad request |

## Limits and differences from CouchDB

- Mango is not implemented, so partition-scoped `_find`/`_explain` are not
  available. `_all_docs` and views are.
- Partition `sizes` are zero placeholders, matching the existing database-info
  behavior. `doc_count` and `doc_del_count` are real.
- View indexes are built from all local documents and filtered to the partition
  at query time. Shard placement still guarantees a partition's rows come from
  one shard, but the on-disk index is not physically split per partition.
- `max_partition_size` is not enforced.
- There is no partition-scoped `_changes` endpoint. Use `_changes` and filter
  document ids client-side.

## Tests

The conformance suite covers:

- `partitioned_database_documents_info_and_views`
- `partitioned_all_docs_ranges_keys_and_nested_ids`
- `cluster_partitioned_database_propagates_and_isolates_partitions`
- `cluster_partitioned_reads_route_to_the_owner_with_one_replica`

They verify validation, bulk errors, counts and tombstones, ranges and keys,
nested ids, partitioned and global views, reduce views, single-shard placement,
cluster propagation and owner forwarding.

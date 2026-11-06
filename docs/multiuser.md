# Multiuser access and fair admission

DouchDB keeps multiuser support deliberately small. There is no replicated
`_users` database and no IAM system. The model is:

1. **Identity** from a deployment-owned users file (`--users-file`) or the
   existing server admin (`--admin`).
2. **Authorization** from a CouchDB-shaped per-database `_security` object with
   `admins`/`members` names and roles.
3. **Admission** at the HTTP boundary: global and per-principal in-flight
   limits, a request-rate token bucket, and separate heavy/background lanes.

The goal is not multi-tenant billing or global scheduling. It is to ensure that
one of five users cannot starve the other four, and that view/search work does
not consume all interactive capacity.

## Users file

```json
{
  "users": [
    {"name": "alice", "token": "alice-token", "roles": ["analyst"]},
    {"name": "bob", "token_sha256": "<64 hex characters>", "roles": []},
    {"name": "root", "token": "root-token", "roles": ["_admin"]}
  ]
}
```

- `name` is required and must be unique.
- Provide exactly one of `token` or `token_sha256`. The hash is SHA-256 of the
  bearer token. Prefer `token_sha256` in production; keep the file `0600`.
- `roles` is an array of strings. A user with the `_admin` role, or
  `"admin": true`, is a server admin.
- The file is read at startup. Restart the process to apply user, token or role
  changes; `_security` changes take effect on the next request without a
  restart.
- `--admin user:pass` can be used alongside a users file. The config admin is
  always a server admin.

Start with:

```sh
douchdb --users-file /etc/douchdb/users.json ...
```

Authentication methods:

- `Authorization: Bearer <token>`
- Basic auth with the user name and token as the password
- `POST /_session` with `name` and the token as `password`, then the signed
  `AuthSession` cookie

Passwords are not stored or hashed by DouchDB; the token is the credential. This
avoids PBKDF2 work on every request and keeps the implementation small. Use
TLS in front of DouchDB because tokens are bearer secrets.

Session cookies are signed with, in order:

1. `--session-secret`, if set;
2. the cluster secret plus a `:session` suffix, if clustered;
3. the admin credentials, if only `--admin` is used;
4. a random per-process secret.

Set `--session-secret` explicitly when running multiple nodes or restarting a
node and expecting existing cookies to remain valid. Logout is client-side only;
the cookie is stateless, as in CouchDB.

## `_security`

`GET`/`PUT /{db}/_security` uses the CouchDB shape:

```json
{
  "admins":  {"names": ["alice"], "roles": []},
  "members": {"names": ["alice", "bob"], "roles": ["analyst"]}
}
```

- **Server admin** (`_admin` role) can access everything.
- **Database admin** can read/write all documents, write design documents,
  change `_security`, compact and clean up views/search, and change the revision
  limit.
- **Database member** can read/write ordinary documents and query views/search.
- **Anonymous** is a member only when the database is public.
- If `members.names` and `members.roles` are both empty **and** the admins lists
  are also empty, the database is public only when `--default-security open`.
  This is deliberately stricter than CouchDB: setting admins without members
  makes a database admin-only, not public. A database is never exposed by
  accidentally omitting the member list.
- The default for an entirely empty security object is:
  - `admin-only` (default when `--admin` or `--users-file` is configured)
  - `open` (explicitly public, including anonymous requests)
- With no authentication configured at all, every request is a service admin.
  This preserves the single-user default.

`GET /_all_dbs` is filtered for non-admin principals; a user sees only databases
where they are a member or admin. `GET /_dbs_info`, `_db_updates`, `_membership`,
`/_node/...`, cluster/replication management, detailed health and metrics remain
server-admin operations.

The security object is stored in `security.json` beside the database. A security
change is applied locally and to **every configured peer** before the coordinator
reports success. If any peer fails, the coordinator rolls back its local copy and
the reachable peers, and returns `502 security_propagation_failed`. This prevents
a healthy peer from keeping a newer security object than the coordinator. If a
peer is offline, change security when the cluster is healthy; concurrent admin
changes should go through one coordinator. Cluster-wide security consensus is
outside this version.

Design documents are database-admin data even when written through
`_bulk_docs` or `COPY`; a member cannot create or overwrite one through those
routes either. Long-lived `_changes` feeds re-check database access on every
iteration and close when the principal is revoked.

## Admission and fairness

The admission layer runs before the request reaches a database. It is not a
queue: excess work is rejected immediately with:

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 1

{"error":"too_many_requests","reason":"too many concurrent requests for this user"}
```

Limits:

| Flag | Default | Meaning |
|---|---:|---|
| `--max-in-flight` | `64` | Global concurrent HTTP requests |
| `--per-user-in-flight` | `16` | Concurrent requests per principal |
| `--per-user-rate` | `0` | Requests per second per principal; `0` is unlimited |
| `--heavy-in-flight` | `8` | Concurrent view/search requests globally |
| `--heavy-per-user` | `1` | Concurrent view/search requests per principal |
| `--background-in-flight` | `2` | Concurrent anti-entropy/discovery passes |

Request classes:

- **Normal**: point reads/writes, `_all_docs`, `_changes`, bulk operations.
- **Heavy**: paths containing `/_view/` or `/_search/`.
- **Background**: anti-entropy and discovery. These bypass per-user accounting
  and use the background permit.

A heavy request consumes both a heavy permit and a normal permit, so the global
interactive bound still applies. A named user can have one heavy request while
other users read and write. A no-auth or internal service principal has no
per-user identity to share fairly; it is bounded by the global and heavy-global
limits only.

`/_up`, `/_health`, `_utils` assets and the favicon are exempt. `_session` is
admitted like any other request; a login storm is bounded by the anonymous
principal's limits. The console request log redacts `_session` request bodies and
is cleared on both login and logout so one user's documents are not left visible
in a shared browser.

### What this guarantees

- A single authenticated user cannot fill the global in-flight pool.
- A user cannot run multiple heavy view/search builds at once.
- Another user can still perform point reads while a heavy build runs.
- Background replication/discovery does not run concurrently with itself and
  does not consume user permits.
- Excess load produces an explicit 429 instead of unbounded latency and memory.

### What this does not guarantee

- Strict equal throughput under heterogeneous costs. A point read and a
  100k-document view build both consume one admission slot. Cost-weighted
  deficit round-robin is a future extension, not part of this version.
- A cross-coordinator global share. Admission is per node; a user hitting N
  coordinators can receive up to N times the per-node share. For a small
  cluster, route clients through one coordinator if this matters.
- CPU isolation. Heavy work still runs on Tokio worker threads; the heavy
  permit bounds concurrency but does not move the work to a separate CPU pool.
- A bounded wait queue. Requests are admitted or rejected; there is no
  per-user FIFO.
- Per-user metrics labels. The admission counters are not yet broken down by
  principal in `/_node/_local/_metrics`.

## Console access management

The console has an **Access** page behind the header button:

- lists every database visible to the signed-in principal;
- shows `_security` members and admins as names and `role:` entries;
- labels empty objects as `default`, members as `members`, and admins-with-no-
  members as `admin-only`;
- edits member/admin names and roles with `PUT /{db}/_security`;
- shows whether the signed-in principal is a server admin.

The page is deliberately **database security only**. Creating users, generating
or rotating tokens, and editing the users file remain file/CLI operations; there
is no credential-management API or UI. This keeps bearer secrets out of the HTTP
surface and avoids pretending that a local file edit propagates across a cluster.

## Cluster behavior

- User credentials are forwarded on cluster fan-out; peers re-check
  `_security` for the forwarded principal.
- Internal requests without user credentials are service admins, trusted by the
  shared cluster secret. Do not expose the cluster secret to clients.
- `_security` objects are versioned (beside the object, never in the
  `GET /db/_security` body). The coordinator stores the change and pushes it to
  every peer; peers reject older versions, and concurrent updates with the same
  version settle on one winner everywhere. The anti-entropy pass reconciles the
  object, so a peer that was offline, restored or added later converges. The
  write returns 200 once a majority holds it and 202 when it does not yet, in
  which case it is still kept and delivered when the peers return.
- Set the same `--session-secret` (or cluster secret) on every node if session
  cookies should work across nodes.
- Admission is local to each node.

## Contract tests

The behavior is pinned by:

```text
users_file_authenticates_tokens_sessions_and_roles
security_object_enforces_members_admins_roles_and_all_dbs_filtering
default_security_open_allows_any_authenticated_user
per_user_admission_caps_concurrency_and_preserves_other_users
global_admission_limit_protects_capacity_across_users
cross_user_isolation_is_watertight
longpoll_changes_stops_after_security_revocation
cluster_security_reconciles_to_peer_that_missed_the_update
cluster_security_without_quorum_is_accepted_and_converges
```

Unit tests in `src/admission.rs` cover per-user/global limits, the heavy lane,
the background lane, path classification and token-bucket refill. Unit tests in
`src/auth.rs` cover tokens, hashes, roles, sessions and admin roles.

## Deliberate non-goals

- No `_users` database, user documents, password hashing, auth cache or user
  replication. Use the users file or an external identity proxy.
- No per-document or row-level authorization.
- No OAuth/OIDC/JWT verification inside DouchDB. Put it in a trusted reverse
  proxy and pass a token that the users file recognizes, or extend the auth
  layer later.
- No quotas, billing, weighted shares or cluster-wide scheduler.
- No self-service user or database creation for non-admin users.

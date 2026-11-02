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
- If `members.names` and `members.roles` are both empty, the database is public.
  If the entire security object is empty, the default depends on
  `--default-security`:
  - `admin-only` (default when `--admin` or `--users-file` is configured)
  - `open` (explicitly public, including anonymous requests)
- With no authentication configured at all, every request is a service admin.
  This preserves the single-user default.

`GET /_all_dbs` is filtered for non-admin principals; a user sees only databases
where they are a member or admin. `GET /_dbs_info`, `_db_updates`, `_membership`,
`/_node/...`, cluster/replication management, detailed health and metrics remain
server-admin operations.

The security object is stored in `security.json` beside the database. Changes
are propagated to reachable peers on a best-effort basis. A peer that was
offline during a change will keep its old copy until the change is applied
again; cluster-wide security consensus is deliberately out of scope.

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
| `--background-in-flight` | `1` | Concurrent anti-entropy/discovery passes |

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
principal's limits.

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

## Cluster behavior

- User credentials are forwarded on cluster fan-out; peers re-check
  `_security` for the forwarded principal.
- Internal requests without user credentials are service admins, trusted by the
  shared cluster secret. Do not expose the cluster secret to clients.
- `_security` changes are fanned out to reachable peers. There is no consensus;
  reapply after a peer returns if it missed a change.
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

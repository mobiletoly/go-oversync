---
layout: default
title: HTTP API
permalink: /documentation/api/
---

# HTTP API

All `/sync/*` endpoints are expected to be mounted behind authentication. The handlers require an
authenticated `oversync.Actor{UserID, SourceID}` in request context. `GET /health` and
`GET /status` do not require an authenticated actor.

All visible sync keys are structured JSON objects whose values are strings on the wire. UUID-valued
keys use canonical dashed lowercase UUID strings. Hidden server ownership columns such as
`_sync_scope_id` never appear in client-visible payloads, conflicts, committed bundle rows, or
snapshot rows.

## POST `/sync/connect`

Resolve the first-connect lifecycle for the authenticated `(user_id, source_id)`.

Request:

```json
{
  "source_id": "device-1",
  "has_local_pending_rows": true
}
```

Response:

```json
{
  "resolution": "initialize_local",
  "initialization_id": "6df0d8dd-a84b-43b6-bbca-8de70432922a",
  "lease_expires_at": "2026-03-22T12:00:00Z"
}
```

Possible `resolution` values:

- `remote_authoritative`
- `initialize_local`
- `initialize_empty`
- `retry_later`

Failure contract:

- `400 connect_invalid`

Notes:

- `retry_later` is a normal lifecycle outcome, not an auth failure
- the caller must carry `initialization_id` into the first seed push only when the response is
  `initialize_local`

## POST `/sync/push-sessions`

Create one staging push session for a logical dirty-set bundle.

Request:

```json
{
  "source_id": "device-1",
  "source_bundle_id": 7,
  "planned_row_count": 1
}
```

Response:

```json
{
  "push_id": "6df0d8dd-a84b-43b6-bbca-8de70432922a",
  "status": "staging",
  "planned_row_count": 1,
  "next_expected_row_ordinal": 0
}
```

Notes:

- creating a fresh session for the same `(user_id, source_id, source_bundle_id)` hard-replaces any
  older uncommitted staging session for that tuple
- repeating the same tuple after the server has already committed returns
  `status = "already_committed"` plus the committed bundle metadata
- session creation is serialized by `(user_id, source_id, source_bundle_id)`

Create failure contract:

- `400 push_session_invalid`
- `409 scope_uninitialized`
- `409 scope_initializing`
- `409 initialization_stale`
- `410 initialization_expired`

## POST `/sync/push-sessions/{push_id}/chunks`

Upload one contiguous chunk into a staging push session.

Request:

```json
{
  "start_row_ordinal": 0,
  "rows": [
    {
      "schema": "business",
      "table": "users",
      "key": {"id": "550e8400-e29b-41d4-a716-446655440000"},
      "op": "INSERT",
      "base_row_version": 0,
      "payload": {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "name": "John Doe",
        "email": "john@example.com"
      }
    }
  ]
}
```

Response:

```json
{
  "push_id": "6df0d8dd-a84b-43b6-bbca-8de70432922a",
  "next_expected_row_ordinal": 1
}
```

Failure contract:

- `400 push_chunk_invalid`
- `403 push_session_forbidden`
- `404 push_session_not_found`
- `409 push_chunk_out_of_order`
- `409 initialization_stale`
- `410 push_session_expired`
- `410 initialization_expired`

## POST `/sync/push-sessions/{push_id}/commit`

Commit the fully staged push session atomically.

Response:

```json
{
  "bundle_seq": 143,
  "source_id": "device-1",
  "source_bundle_id": 7,
  "row_count": 1,
  "bundle_hash": "4c8d2d5f5d2c5a41d9aa6f4d2f3ac5d0d1d5d8bbf1d7a8c39f3b3a970f6af21a"
}
```

Failure contract:

- `400 push_commit_invalid`
- `403 push_session_forbidden`
- `404 push_session_not_found`
- `409 push_conflict`
- `409 initialization_stale`
- `410 push_session_expired`
- `410 initialization_expired`

Push conflict response:

```json
{
  "error": "push_conflict",
  "message": "update conflict on business.users 550e8400-e29b-41d4-a716-446655440000: expected version 7, got 3",
  "conflict": {
    "schema": "business",
    "table": "users",
    "key": {"id": "550e8400-e29b-41d4-a716-446655440000"},
    "op": "UPDATE",
    "base_row_version": 3,
    "server_row_version": 7,
    "server_row_deleted": false,
    "server_row": {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "Jane Doe",
      "email": "jane@example.com"
    }
  }
}
```

## GET `/sync/committed-bundles/{bundle_seq}/rows`

Fetch one deterministic page of authoritative committed rows for accepted-push replay.

Query:

- `after_row_ordinal`
- `max_rows` (optional, defaults to the server capability and is capped by the server capability)

Response:

```json
{
  "bundle_seq": 143,
  "source_id": "device-1",
  "source_bundle_id": 7,
  "row_count": 1,
  "bundle_hash": "4c8d2d5f5d2c5a41d9aa6f4d2f3ac5d0d1d5d8bbf1d7a8c39f3b3a970f6af21a",
  "rows": [
    {
      "schema": "business",
      "table": "users",
      "key": {"id": "550e8400-e29b-41d4-a716-446655440000"},
      "op": "INSERT",
      "row_version": 143,
      "payload": {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "name": "John Doe",
        "email": "john@example.com"
      }
    }
  ],
  "next_row_ordinal": 0,
  "has_more": false
}
```

## DELETE `/sync/push-sessions/{push_id}`

Best-effort explicit cleanup for an abandoned uncommitted push session.

Response:

- HTTP `204` with no body on success

## GET `/sync/pull`

Pull complete committed bundles after the client checkpoint.

Query:

- `after_bundle_seq`
- `max_bundles` (optional, defaults to `1000`, capped at `5000`)
- `target_bundle_seq`

Response:

```json
{
  "stable_bundle_seq": 156,
  "bundles": [
    {
      "bundle_seq": 143,
      "source_id": "device-2",
      "source_bundle_id": 18,
      "rows": [
        {
          "schema": "business",
          "table": "users",
          "key": {"id": "550e8400-e29b-41d4-a716-446655440001"},
          "op": "INSERT",
          "row_version": 143,
          "payload": {
            "id": "550e8400-e29b-41d4-a716-446655440001",
            "name": "Jane Doe",
            "email": "jane@example.com"
          }
        }
      ]
    }
  ],
  "has_more": true
}
```

Notes:

- the first response freezes `stable_bundle_seq`
- follow-up requests in the same pull session must pass that value back as `target_bundle_seq`
- if the provided checkpoint is older than the retained bundle floor, the server returns HTTP `409`
  with `error=history_pruned`

Failure contract:

- `400 pull_invalid`
- `409 history_pruned`
- `409 scope_uninitialized`
- `409 scope_initializing`

## POST `/sync/snapshot-sessions`

Create one frozen snapshot session for hydration or destructive recovery.

Response:

```json
{
  "snapshot_id": "2f2d3f7a-cd1f-42b9-8d08-c4d2b45d9f88",
  "snapshot_bundle_seq": 156,
  "row_count": 124,
  "byte_count": 18240,
  "expires_at": "2026-03-22T12:00:00Z"
}
```

Failure contract:

- `409 scope_uninitialized`
- `409 scope_initializing`

## GET `/sync/snapshot-sessions/{snapshot_id}`

Fetch one deterministic chunk from a frozen snapshot session.

Query:

- `after_row_ordinal`
- `max_rows` (optional, defaults to the server capability and is capped by the server capability)

Response:

```json
{
  "snapshot_id": "2f2d3f7a-cd1f-42b9-8d08-c4d2b45d9f88",
  "snapshot_bundle_seq": 156,
  "rows": [
    {
      "schema": "business",
      "table": "users",
      "key": {"id": "550e8400-e29b-41d4-a716-446655440001"},
      "row_version": 143,
      "payload": {
        "id": "550e8400-e29b-41d4-a716-446655440001",
        "name": "Jane Doe",
        "email": "jane@example.com"
      }
    }
  ],
  "next_row_ordinal": 1,
  "has_more": true
}
```

## DELETE `/sync/snapshot-sessions/{snapshot_id}`

Best-effort explicit cleanup for a completed or abandoned snapshot session.

Response:

- HTTP `204` with no body on success

## GET `/sync/capabilities`

Returns the protocol version, schema version, app name, registered tables, registered table specs,
feature flags, and bundle limits.

Important feature flags:

- `bundle_push`
- `bundle_pull`
- `connect_lifecycle`
- `push_session_chunking`
- `committed_bundle_row_fetch`
- `snapshot_chunking`
- `history_pruned_errors`

Important bundle limit fields:

- `max_bundles_per_pull`
- `default_rows_per_push_chunk`
- `max_rows_per_push_chunk`
- `default_rows_per_committed_bundle_chunk`
- `max_rows_per_committed_bundle_chunk`
- `default_rows_per_snapshot_chunk`
- `max_rows_per_snapshot_chunk`
- `push_session_ttl_seconds`
- `snapshot_session_ttl_seconds`
- `max_rows_per_snapshot_session`
- `max_bytes_per_snapshot_session`

## GET `/health`

Readiness-oriented health response. Returns HTTP `200` when the service is `healthy`, and HTTP
`503` when the service is `unhealthy`.

## GET `/status`

Returns a lifecycle and operability snapshot including lifecycle, registered tables, feature flags,
bundle visibility, retained-floor visibility, and error counters.

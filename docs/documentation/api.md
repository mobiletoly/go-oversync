---
layout: default
title: HTTP API
---

# HTTP API

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

- Creating a fresh session for the same `(user_id, source_id, source_bundle_id)` hard-replaces any
  older uncommitted staging session for that tuple; the previous `push_id` becomes unknown.
- Repeating the same `(user_id, source_id, source_bundle_id)` after the server has already
  committed returns `status = "already_committed"` plus the committed bundle metadata.
- Session creation is serialized by `(user_id, source_id, source_bundle_id)`.

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

Notes:

- `start_row_ordinal` must match the server-reported `next_expected_row_ordinal`.
- Duplicate or out-of-order uploads fail closed; clients must not infer partial acceptance beyond
  the returned `next_expected_row_ordinal`.
- Duplicate logical target rows are rejected both within a chunk and across the full staged set.

## POST `/sync/push-sessions/{push_id}/commit`

Commit the fully staged push session atomically.

Response:
```json
{
  "bundle_seq": 143,
  "source_id": "device-1",
  "source_bundle_id": 7,
  "row_count": 1,
  "bundle_hash": "sha256:4c8d2d5f..."
}
```

Notes:

- Commit is idempotent by `(user_id, source_id, source_bundle_id)`.
- The server rejects incomplete, duplicated, or non-contiguous staged uploads.

Failure contract:

- `POST /sync/push-sessions`
  - `400 push_session_invalid`
- `POST /sync/push-sessions/{push_id}/chunks`
  - `400 push_chunk_invalid`
  - `403 push_session_forbidden`
  - `404 push_session_not_found`
  - `409 push_chunk_out_of_order`
  - `410 push_session_expired`
- `POST /sync/push-sessions/{push_id}/commit`
  - `400 push_commit_invalid`
  - `403 push_session_forbidden`
  - `404 push_session_not_found`
  - `409 push_conflict`
  - `410 push_session_expired`

## GET `/sync/committed-bundles/{bundle_seq}/rows`

Fetch one deterministic page of authoritative committed rows for accepted-push replay.

Query:

- `after_row_ordinal`
- `max_rows` (optional, defaults to server capability, capped by server capability)

Response:
```json
{
  "bundle_seq": 143,
  "source_id": "device-1",
  "source_bundle_id": 7,
  "row_count": 1,
  "bundle_hash": "sha256:4c8d2d5f...",
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

Notes:

- Push is still all-or-nothing at bundle level.
- `payload` is a full-row after-image for `INSERT` and `UPDATE`.
- Canonical binary wire contract:
  - non-key binary payload fields use standard base64
  - UUID-valued keys and UUID-valued key columns use dashed UUID text

## DELETE `/sync/push-sessions/{push_id}`

Best-effort explicit cleanup for an abandoned uncommitted push session.

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

- The first response freezes `stable_bundle_seq`.
- Follow-up requests in the same pull session must pass that value back as `target_bundle_seq`.
- Clients apply whole bundles and only then advance their durable checkpoint.
- If the provided checkpoint is older than the retained bundle floor, the server returns HTTP `409`
  with `error=history_pruned`.
- The same canonical binary wire contract applies here:
  - non-key binary payload fields use standard base64
  - UUID-valued keys and UUID-valued key columns use dashed UUID text

## POST `/sync/snapshot-sessions`

Creates one frozen snapshot session for hydration or destructive recovery.

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

## GET `/sync/snapshot-sessions/{snapshot_id}`

Fetches one deterministic chunk from a frozen snapshot session.

Query:

- `after_row_ordinal`
- `max_rows` (optional, defaults to `1000`, capped at `5000`)

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

Use snapshot sessions for:

- initial hydration
- destructive recovery after `history_pruned`
- rebuilding a client to a stable current after-image

The same canonical binary wire contract applies to snapshot chunk payloads:

- non-key binary payload fields use standard base64
- UUID-valued keys and UUID-valued key columns use dashed UUID text

## DELETE `/sync/snapshot-sessions/{snapshot_id}`

Best-effort explicit cleanup for a completed or abandoned snapshot session.

## GET `/sync/capabilities`

Returns the protocol version, schema version, registered tables, feature flags, and bundle limits.

Important feature flags:

- `bundle_push`
- `bundle_pull`
- `push_session_chunking`
- `committed_bundle_row_fetch`
- `snapshot_chunking`
- `history_pruned_errors`

## GET `/health`

Readiness-oriented health response. Returns HTTP `200` when the service is `healthy`,
and HTTP `503` when the service is `unhealthy`.

## GET `/status`

Returns a lifecycle and operability snapshot including:

- lifecycle (`running`, `shutting_down`, `closed`)
- whether new operations are accepted
- registered tables and runtime feature flags
- latest committed bundle visibility across tracked users
- retained bundle floor min/max visibility
- retained history window min/max visibility
- retention-floor invariant violations
- `history_pruned` response count
- accepted push replay count
- rejected registered write count
- committed bundle count and committed bundle bytes

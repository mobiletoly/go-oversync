---
layout: default
title: HTTP API
---

# HTTP API

## POST `/sync/upload`

Request:
```json
{
  "last_server_seq_seen": 42,
  "changes": [
    {
      "source_change_id": 1,
      "schema": "business",
      "table": "users",
      "op": "INSERT",
      "pk": "550e8400-e29b-41d4-a716-446655440000",
      "server_version": 0,
      "payload": {"id":"550e...","name":"John","email":"john@example.com"}
    }
  ]
}
```

Response:
```json
{
  "accepted": true,
  "highest_server_seq": 156,
  "statuses": [
    {"source_change_id":1, "status":"applied", "new_server_version":1}
  ]
}
```

Notes:

- `accepted=false` means the server rejected the whole batch (e.g. `batch_too_large`); clients should keep all pending changes and retry with a smaller batch.
- `status=invalid` includes an `invalid.reason` and a free-form `invalid.details` object. Some invalid reasons are retryable (`fk_missing`, `precheck_error`, `internal_error`, `batch_too_large`); others are not (`bad_payload`, `unregistered_table`).
- Materialization (optional server-side projection) is best-effort: upload still returns `status=applied` and failures are recorded for admin retry.

## GET `/sync/download`

Query: `after`, `limit`, `schema`, `include_self`, `until`

Response:
```json
{
  "changes": [
    {
      "server_id": 43,
      "schema": "business",
      "table": "users",
      "op": "INSERT",
      "pk": "550e8400-e29b-41d4-a716-446655440001",
      "payload": {"id":"550e...","name":"Jane","email":"jane@example.com"},
      "server_version": 1,
      "deleted": false,
      "source_id": "device-abc",
      "source_change_id": 456,
      "ts": "2025-08-08T07:01:00Z"
    }
  ],
  "has_more": false,
  "next_after": 43,
  "window_until": 43
}
```

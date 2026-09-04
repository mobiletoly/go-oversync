---
layout: default
title: HTTP API
permalink: /documentation/api/
---

# HTTP API

All `/sync/*` endpoints are expected to be mounted behind authentication. The handlers require an
authenticated `oversync.Actor{UserID, SourceID}` in request context. Auxiliary `GET /syncx/health`
and `GET /syncx/status` do not require an authenticated actor.

All visible sync keys are structured JSON objects whose values are strings on the wire. UUID-valued
keys use canonical dashed lowercase UUID strings. Hidden server ownership columns such as
`_sync_scope_id` never appear in client-visible payloads, conflicts, committed bundle rows, or
snapshot rows.

Wire-facing `row_version` and `base_row_version` fields are bundle sequence values. The server
stores the current version internally as `sync.row_state.bundle_seq` and returns it through the
existing wire field names.

Oversync uses RFC 8785 JSON Canonicalization Scheme (JCS) for canonical bytes. Generation-bound
integers are range-checked canonical strings. Exact decimals are bounded strings without exponent
notation. `float64` uses the shortest finite spelling and normalizes negative zero to `"0"`;
PostgreSQL `float4` values may reflect storage normalization under that same token. Booleans are
JSON literals only; string or numeric bridges are invalid.
Bytes require canonical padded standard base64, UUIDs require lowercase dashed text, and a `json`
column is inner canonical JCS bytes carried as an outer JSON string. Text and inner-JSON strings
exclude U+0000 because the required PostgreSQL mappings cannot represent it. Outer JSON `null`
means SQL NULL, while outer string `"null"` is the non-null JSON document `null`.
`bundle_hash` authenticates the committed logical row stream, while `canonical_request_hash`
associates that commit with the immutable original upload.
Hash-only protocol counters are decimal strings inside the logical hash model. `byte_count` is the
length of those canonical logical bytes. This is standard JCS plus a typed wire schema, not an
arbitrary-precision extension to JCS.

Canonical history is stored once. Direct-push success and exact replay return the same complete
compact authoritative row projection and `bundle_hash`. Pull returns indivisible compact bundles;
there is no committed-row follow-up endpoint. A projection with no visible rows is still emitted by
pull and advances the canonical bundle cursor.

Push commit returns HTTP 422 `push_effect_coverage_incomplete` when any uploaded key lacks a final
visible captured effect; the complete transaction is rolled back. Projection/history failures use
the documented `generation_history_unavailable`, `generation_projection_integrity_error`,
`bundle_projection_too_large`, and `history_projection_capacity_exceeded` families. Generation
discovery publishes finite logical and decoded-body limits for push, pull, snapshots, and rows.
Row-bearing responses use compact JSON without HTML or U+2028/U+2029 escaping.

Oversync does not migrate incompatible stored hashes, server history, staged sessions, or durable
client state. To adopt this contract, deploy compatible server and client versions together and
recreate the affected server and client databases. Mixed-version operation is unsupported.

Authenticated discovery requests to `GET /sync/generations/{generation_id}` send authorization and
`Oversync-Source-ID`, and must omit both generation-proof headers. After discovery and confirmation,
all other authenticated `/sync/*` requests must send:

- `Authorization: Bearer <token>`
- `Oversync-Source-ID: <current-source-id>`
- `Oversync-Schema-Generation-ID: <canonical-positive-decimal-id>`
- `Oversync-Schema-Generation-Fingerprint: <complete-64-lowercase-hex-fingerprint>`

The two schema-generation headers are required again on every post-discovery retry and watch
reconnect; a prior discovery response or `Last-Event-ID` never supplies them implicitly.
Discovery, `/syncx/health`, and `/syncx/status` are proof-exempt.

## POST `/sync/connect`

Resolve the first-connect lifecycle for the authenticated `(user_id, source_id)`.

Request:

```json
{
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

## POST `/sync/push`

Commit one complete compact dirty-set bundle atomically. The request carries a source bundle ID,
canonical request hash, optional initialization ID, and compact row groups. The response contains
the complete authoritative compact effect with its committed bundle sequence and hash.

The server serializes the exact `(user_id, source_id, source_bundle_id)` tuple. Repeating the same
complete request returns the same authoritative response; changing its hash is a sequence error.
Clients retain the frozen request after a transport failure and replay it verbatim. There is no
upload locator, chunk ordinal, commit call, or authoritative-row fetch.

Failure contract includes `push_conflict`, `history_pruned`, source-sequence errors, source
retirement, initialization errors, and HTTP 422 application rejections. A conflict response has
the same structured conflict object below.

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

## GET `/sync/pull`

Pull complete committed bundles after the client checkpoint.

Query:

- `after_bundle_seq` (optional, defaults to `0`)
- `max_bundles` (optional, defaults to `1000`, capped at `5000`)
- `target_bundle_seq`

Response:

```json
{
  "stable_bundle_seq": 156,
  "bundles": [
    {
      "bundle_seq": 143,
      "source_id": "source-2",
      "source_bundle_id": 18,
      "bundle_hash": "<sha256>",
      "rows": [
        [0, 0, [["i", ["550e8400-e29b-41d4-a716-446655440001"], 143,
          {"id":"550e8400-e29b-41d4-a716-446655440001","name":"Jane Doe","email":"jane@example.com"}]]]
      ]
    }
  ],
  "has_more": true
}
```

Notes:

- the first response freezes `stable_bundle_seq`
- follow-up requests in the same pull session must pass that value back as `target_bundle_seq`
- `retained_bundle_floor` is the highest discarded bundle; a checkpoint equal to the floor is
  valid and returns bundles strictly above it
- when the floor is positive, lower checkpoints (including zero) return HTTP `409` with
  `error=history_pruned`
- a checkpoint or positive target above current committed history returns HTTP `409` with
  `error=checkpoint_ahead`; the server does not clamp it into a durable stable checkpoint
- target zero captures current committed history; a positive target below `after_bundle_seq`
  returns HTTP `400` with `error=invalid_request`

Failure contract:

- `400 invalid_request`
- `409 history_pruned`
- `409 checkpoint_ahead`
- `409 scope_uninitialized`
- `409 scope_initializing`

## GET `/sync/watch`

Open an optional Server-Sent Events stream for metadata-only bundle wakeups.

Query:

- `after_bundle_seq` (optional, defaults to `0`)

Event:

```text
event: bundle
data: {"bundle_seq":157}
```

Notes:

- `/sync/watch` is only a wake-up hint; clients must still call `/sync/pull` or existing sync
  operations to receive authoritative data
- heartbeat comments such as `: heartbeat` keep idle connections observable and clean up stale
  subscribers
- events can coalesce; clients must not expect one event per committed bundle

Failure contract:

- `400 invalid_request`
- `401 authentication_failed`
- `409 scope_uninitialized`
- `409 scope_initializing`
- `503 bundle_change_watch_disabled`

## POST `/sync/snapshot-sessions`

Create one frozen snapshot session for hydration or destructive recovery.

The request body is optional.

Keep-source rebuild request:

```json
{}
```

Rotated rebuild request:

```json
{
  "source_replacement": {
    "previous_source_id": "device-old",
    "new_source_id": "device-new",
    "reason": "history_pruned"
  }
}
```

Notes:

- omitting `source_replacement` means keep-source hydrate/rebuild
- including `source_replacement` means rotated rebuild; the server reserves `new_source_id` and
  retires `previous_source_id` atomically with snapshot-session creation
- `previous_source_id` must match the authenticated `Oversync-Source-ID`
- supported `reason` values are:
  - `history_pruned`
  - `source_sequence_out_of_order`
  - `source_sequence_changed`
  - `source_retired`

Response:

```json
{
  "snapshot_id": "2f2d3f7a-cd1f-42b9-8d08-c4d2b45d9f88",
  "snapshot_bundle_seq": 156,
  "row_count": 124,
  "logical_byte_count": 18240,
  "expires_at": "2026-03-22T12:00:00Z"
}
```

Failure contract:

- `400 snapshot_session_invalid`
- `409 source_replacement_invalid`
- `409 source_retired`
- `409 scope_uninitialized`
- `409 scope_initializing`
- `409 snapshot_session_limit_exceeded` with required `dimension`, `actual`, and `limit` fields
- `429 snapshot_build_capacity` with required positive delta-seconds `Retry-After`

`snapshot_session_limit_exceeded` applies to `row_count`, `logical_byte_count`, and
`row_decoded_body_bytes`. `logical_byte_count` is the checked sum of canonical
`["i", table_index, key_values, row_version, payload]` values and is independent of pagination.
`row_decoded_body_bytes` is the canonical expanded key-plus-payload size governed by
`max_row_decoded_bytes`; compact group framing and response-envelope bytes are excluded.

Structured `source_retired` response:

```json
{
  "error": "source_retired",
  "message": "source device-old was retired for user user-123 and replaced by device-new",
  "source_id": "device-old",
  "replaced_by_source_id": "device-new"
}
```

## GET `/sync/snapshot-sessions/{snapshot_id}`

Fetch one deterministic chunk from a frozen snapshot session.

Query:

- `after_row_ordinal` (optional, defaults to `0`)
- `max_rows` (optional, capped by the admitted generation-discovery limit)
- `max_bytes` (optional complete decoded compact-response byte budget, capped by the admitted limit)

Response:

```json
{
  "rows": [
    [0, 0, [["i", ["550e8400-e29b-41d4-a716-446655440001"], 143,
      {"id":"550e8400-e29b-41d4-a716-446655440001","name":"Jane Doe","email":"jane@example.com"}]]]
  ],
  "next_row_ordinal": 1,
  "logical_byte_count": 143,
  "has_more": true
}
```

If the first eligible row does not fit `max_bytes`, the server returns
`400 snapshot_chunk_too_small` with its exact `required_decoded_body_bytes`. Excess concurrent chunk reads
return `429 snapshot_chunk_capacity` with required positive delta-seconds `Retry-After`.

The Go client handles these two exact capacity codes with a dedicated elapsed-time
policy (enabled by default, 30-second budget, one-second missing/invalid-header
fallback) rather than consuming the generic transport retry-attempt budget.

## DELETE `/sync/snapshot-sessions/{snapshot_id}`

Logically retire a completed or abandoned snapshot session. The bounded cleanup worker removes its
rows and then its empty parent asynchronously.

Response:

- HTTP `204` with no body on success

## GET `/sync/generations/{generation_id}`

Returns the selected immutable generation descriptor and its bounded data-plane limits. A client
obtains and validates this descriptor at attachment/opening time, retains it for its ready lifetime,
and does not perform a per-operation discovery or capability preflight. Every subsequent
authenticated data-plane request carries the retained generation proof.

The breaking-development protocol version remains exactly `v1`; clients reject an empty, unknown,
or mismatched value before connected synchronization work.

## GET `/syncx/health`

Readiness-oriented health response. Returns HTTP `200` when the service is `healthy`, and HTTP
`503` when the service is `unhealthy`. A newly constructed runtime remains unhealthy and reports
`accepting_operations=false` until `Bootstrap()` has committed schema validation, trigger
installation, and any populated-table adoption.

## GET `/syncx/status`

Returns a lifecycle and operability snapshot including lifecycle, registered tables, feature flags,
bundle visibility, retained-floor visibility, and error counters. Unlike sync operations, status is
available while bootstrap is incomplete so operators can observe the readiness gate.

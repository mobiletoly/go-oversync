---
layout: default
title: HTTP API
permalink: /documentation/api/
---

# HTTP API

All `/sync/*` endpoints are expected to be mounted behind authentication. The handlers require an
authenticated `oversync.Actor{UserID, SourceID}` in request context. Auxiliary `GET /syncx/health`
and `GET /syncx/status` do not require an authenticated actor.

Compact push, pull, and snapshot rows carry ordered key arrays. Conflict details carry a JSON key
object. UUID keys use canonical dashed lowercase UUID strings. Hidden server ownership columns such as
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

Canonical history is stored once. The server retains a terminal upload outcome separately
until acknowledgment. Pull returns indivisible compact bundles. Even an empty projection
advances the canonical bundle cursor.

Push commit returns HTTP 422 `push_effect_coverage_incomplete` when any uploaded key lacks a final
visible captured effect; the complete transaction is rolled back. Projection/history failures use
the documented `generation_history_unavailable`, `generation_projection_integrity_error`,
`bundle_projection_too_large`, and `history_projection_capacity_exceeded` families. Generation
discovery publishes finite logical and decoded-body limits for push, pull, snapshots, and rows.
Row-bearing responses use compact JSON without HTML or U+2028/U+2029 escaping.

v0.5.0 replaces capabilities with generation discovery and replaces staged upload and committed-row
paging with compact one-shot push and durable outcomes. See the
[v0.5.0 release notes]({{ site.baseurl }}/releases/v0.5.0/) and
[OpenAPI schemas and examples](https://github.com/mobiletoly/go-oversync/blob/main/swagger/two_way_sync.yaml).

## Authentication and headers

Send these headers on every authenticated `/sync/*` request, including discovery:

- `Authorization: Bearer <token>` when your host uses bearer authentication
- `Oversync-Source-ID: <source-id>`
- `Oversync-Protocol-Version: v1` as one exact header value. Missing, duplicate, or different
  values return HTTP 426 `protocol_version_mismatch`.

Omit both proof headers on `GET /sync/generations/{generation_id}`. On later requests, including
retries and watch reconnects, send exactly one of each:

- `Oversync-Schema-Generation-ID: <canonical-positive-decimal-id>` in the signed 32-bit range
- `Oversync-Schema-Generation-Fingerprint: <64-lowercase-hex-characters>`

Resolve, retained lookup, and acknowledgment use the upload's original source and generation proof.
Use the newer target proof when requesting a `schema_generation_upgrade` snapshot under the old
source. The `/syncx/health` and `/syncx/status` endpoints are outside these requirements.

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

Send one bounded bundle with `source_bundle_id`, `canonical_request_hash`, compact `rows`,
and `initialization_id` when Connect requires it. Persist the source, generation proof, sequence,
hash, and token together with the frozen request. Exact replay returns the retained outcome while
it awaits acknowledgment.

HTTP 200 carries a terminal outcome. Validate the immutable binding and `outcome_hash` before
recording local disposition:

| Status | Local disposition |
| --- | --- |
| `committed` | Validate and apply the complete authoritative effect in `committed`, including its bundle sequence, hash, row count, and compact rows. Preserve edits made after the upload froze. |
| `rejected` | Durably record `rejection.code` and preserve local intent for the application's decision. When the code is `push_conflict`, `rejection.conflict` includes the affected key, base and server versions, deletion flag, and current server row or explicit JSON null. The operation code is `i`, `u`, or `d`. |
| `cancelled` | Record the cancellation and requeue remaining intent. |

All three statuses consume the source operation sequence. Acknowledge the outcome after local
disposition commits. Retry surviving intent under a successor sequence. A snapshot cannot settle
a pending outcome.

HTTP 422 `push_effect_coverage_incomplete` rolls back the entire business transaction and is
nonterminal. Other nonterminal failures include stale bindings, a pending outcome, and capacity
refusals. If the request might have reached the server, keep the frozen request and use resolution
before collecting a replacement.

## POST `/sync/push-outcomes/resolve`

Send `source_bundle_id`, `canonical_request_hash`, and the original `initialization_id` or
JSON null. The server serializes this operation with push. It returns the matching retained
outcome or durably cancels the next operation, fencing any delayed push of that operation.

## POST `/sync/push-outcomes/retained`

Send `source_bundle_id` only. Use this lookup after `push_outcome_mismatch` when the client has
lost its original local freeze. It reads the immutable outcome owned by the same source and
proof. A `409 push_outcome_absent` cannot establish whether an upload committed. Lookup neither
cancels nor acknowledges the operation.

## POST `/sync/push-outcomes/ack`

After durable local disposition, send `source_bundle_id`, `canonical_request_hash`, and
`outcome_hash`. HTTP 204 confirms acknowledgment. Keep the obligation across restart until
confirmed. An exact retry of the latest ack can return 204 even after retirement or rebinding.
Older acknowledged operations return `410 push_outcome_acknowledged`.

Push, resolve, retained lookup, and pull can return HTTP 503
`history_projection_capacity_exceeded` with `Retry-After: 1`. See
[Durable upload outcomes]({{ site.baseurl }}/documentation/durable-upload-outcomes/) for the
settlement order and recovery details.

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

Create one frozen snapshot session. Send an empty body or `{}` for ordinary keep-source hydration.
For clean recovery, supply the minimum snapshot checkpoint:

```json
{
  "recovery": {"minimum_bundle_seq": 42}
}
```

The server echoes the recovery request and seals the snapshot at a sequence at or above the floor.
The client verifies the seal, stages the full snapshot, and atomically publishes the checkpoint.
This empty recovery snapshot is the OpenAPI fixture example:

```json
{
  "snapshot_id": "snapshot-fixture",
  "snapshot_bundle_seq": 42,
  "row_count": 0,
  "logical_byte_count": 0,
  "expires_at": "2026-10-23T13:00:00Z",
  "snapshot_hash": "d293c0d8c2d8bd27d98d74ec351e0610add37cb8850111c4a1e4d0ca3d481942",
  "recovery": {"minimum_bundle_seq": 42}
}
```

For source replacement, send `source_replacement` in place of `recovery`:

```json
{
  "source_replacement": {
    "previous_source_id": "device-old",
    "new_source_id": "device-new",
    "reason": "schema_generation_upgrade"
  }
}
```

- Send the previous source in `Oversync-Source-ID`. For `schema_generation_upgrade`, send the
  newer target proof. The other supported reasons use the current proof: `history_pruned`,
  `source_sequence_out_of_order`, `source_sequence_changed`, and `source_retired`.
- The server retires the previous source and reserves the replacement in the transaction that
  creates the snapshot. Use the replacement source and snapshot's proof for chunk reads and deletion.
- Session creation returns `snapshot_hash` for both modes. The `recovery` field appears only
  for a request that supplied it.

Failure contract:

- `400 snapshot_session_invalid`
- `409 source_replacement_invalid`
- `409 push_outcome_pending` when an outstanding outcome blocks clean recovery or replacement
- `409 recovery_floor_ahead` when the recovery floor exceeds current committed history
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

Fetch one deterministic chunk from a frozen snapshot session under its owning source and proof.
The first nonempty group starts at `after_row_ordinal + 1`.

Query:

- `after_row_ordinal` (optional, defaults to `0`)
- `max_rows` (optional, capped by the admitted generation-discovery limit)
- `max_bytes` (optional complete decoded compact-response byte budget, capped by the admitted limit)

Response:

```json
{
  "rows": [
    [0, 1, [["i", ["550e8400-e29b-41d4-a716-446655440001"], 143,
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

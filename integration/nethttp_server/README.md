# Net/HTTP Integration Test Server

This repository-owned server drives the mobile-flow, load, fault, and recovery suites through the
standard Go `net/http` stack. It is runnable for integration work, but it is not a consumer-facing
starter application. Use `examples/samplesync_server` for the maintained application example.

## Endpoints

- `POST /dummy-signin`
- `POST /test/retention-floor`
- `POST /sync/connect`
- `POST /sync/push`
- `GET /sync/pull`
- `GET /sync/watch`
- `POST /sync/snapshot-sessions`
- `GET /sync/snapshot-sessions/{snapshot_id}`
- `DELETE /sync/snapshot-sessions/{snapshot_id}`
- `GET /sync/generations/{generation_id}`
- `GET /syncx/status`
- `GET /syncx/health`

## Architecture

- Business tables are authoritative.
- Sync history is captured from committed business-table effects.
- `/sync/watch` is enabled as an optional wake-up hint; `/sync/pull` remains authoritative.
- For server-originated writes, prefer `oversync.ScopeManager.ExecWrite(...)` as the convenience
  API; `WithinSyncBundle(...)` remains available for lower-level callers.
- The supported push transport is one bounded, atomic request; its success response contains the
  complete authoritative effects for that source bundle.
- The server declares one complete schema generation. Discovery admits its exact descriptor; every
  subsequent authenticated `/sync` call requires `Oversync-Schema-Generation-ID` and
  `Oversync-Schema-Generation-Fingerprint`.
- Clients still push one logical dirty-set bundle and pull complete committed bundles.
- Hydration and destructive recovery rebuild through chunked snapshot sessions.
- The runtime stores one canonical effect history and projects push replay, pull, conflicts, and
  snapshots to the request's fixed generation. Invisible-only commits remain empty pull envelopes.
- Trigger normalization and extra effects are supported, but every uploaded key must have a final
  visible effect or the commit rolls back with HTTP 422 `push_effect_coverage_incomplete`.
- Projection uses finite row, byte, response-body, and concurrency limits and no projection cache.
- Registered tables in this harness use hidden server ownership via `_sync_scope_id TEXT` and
  scope-inclusive keys/FKs on PostgreSQL.
- The integration schema exercises cascades, self-references, and a two-table cycle inside the
  supported FK envelope.

## Server-originated write pattern

The integration test suite includes real client-visible coverage for admin/backoffice-style writes in
`server/scope_manager_sync_test.go`.

Typical usage:

```go
scopeMgr := oversync.NewScopeManager(syncService, oversync.ScopeManagerConfig{})

// Persist these with the logical command and reuse them after restart or an
// unknown commit result. operationHash covers every value used by callback SQL.
operationHash := sha256.Sum256(canonicalCommandBytes)

_, err := scopeMgr.ExecWrite(ctx, scopeID, oversync.ScopeWriteOptions{
	WriterID: "admin-panel",
	RetryableWriteOptions: oversync.RetryableWriteOptions{
		OperationID:         commandID,
		OperationHash:       operationHash,
		OperationValidUntil: commandValidUntil,
	},
}, func(callbackCtx context.Context, tx oversync.DatabaseWriteTx) error {
	_, err := tx.Exec(callbackCtx, `
		UPDATE business.users
		SET name = $3
		WHERE _sync_scope_id = $1
		  AND id = $2
	`, scopeID, userID, "Updated By Admin")
	return err
})
```

That write is captured into the normal committed-bundle history and later reaches clients through
ordinary pull or snapshot flows. There is no separate admin-only delivery path.

The callback can run up to three times after definite rollback. Keep it database-only, use its
supplied context and transaction, and do not publish external effects from it. Preserve the exact
operation identity, hash, and deadline to resolve `oversync.CommitOutcomeUnknownError` by replay.
Persist the deadline before the first attempt even when no receipt exists. The integration server uses
the unregistered `business.server_operation_deadlines` table for that application-owned record;
rolled-back Oversync attempts therefore cannot cause a restart to generate a different deadline.
`ScopeWriteResult.Bundle` is a durable summary and does not contain row payloads.

If the server reserves a trusted writer ID, pass the same exact set to both
`server.ServerConfig.ReservedServerSourceIDs` and the underlying Sync service/actor boundary. Client
requests using a reserved source are rejected without exposing the configured set.

## Run

```bash
createdb clisync_example
DATABASE_URL="postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable" \
JWT_SECRET="your-secret-key-change-in-production" \
go run ./integration/nethttp_server
```

Snapshot admission defaults to eight active builds and four active chunk
requests. Override them with positive integers in
`OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS` and
`OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS`; excess requests receive an
immediate retryable 429 rather than entering an unbounded server queue.

History projection admission defaults to four response-held permits. A positive
`OVERSYNC_MAX_CONCURRENT_HISTORY_PROJECTIONS` override is available for measured
load tests. Size it together with `OVERSYNC_DB_POOL_MAX_CONNS`, request duration,
and server memory; excess work receives retryable HTTP 503 with `Retry-After: 1`.

## Demo auth

```bash
curl -X POST http://localhost:8080/dummy-signin \
  -H "Content-Type: application/json" \
  -d '{"user":"demo-user","password":"anything"}'
```

## Test connect lifecycle

```bash
curl -X POST http://localhost:8080/sync/connect \
  -H "Authorization: Bearer <jwt-token>" \
  -H "Oversync-Source-ID: source-123" \
  -H "Oversync-Schema-Generation-ID: 1" \
  -H "Oversync-Schema-Generation-Fingerprint: <64-lowercase-hex-fingerprint>" \
  -H "Content-Type: application/json" \
  -d '{
    "has_local_pending_rows": false
  }'
```

Possible `resolution` values:

- `remote_authoritative`
- `initialize_local`
- `initialize_empty`
- `retry_later`

## Reset the local integration database

The running server has no reset endpoint. `TRUNCATE` is rejected on every registered table with
SQLSTATE `55000`, including inside bundle context and when reached through `CASCADE`.

To reset the harness, stop every server and client process, delete and recreate PostgreSQL, create
the permanent business tables, recreate every client database, and then start compatible server
and client versions together. All rows, bundles, checkpoints, outboxes, and offline work
are discarded. Do not disable the managed triggers or use schema drops while the server is running.

The server's `server/baseline-readiness.json` is its canonical static deployment identity. Changing
an application or qualification revision in that manifest intentionally requires this full database
recreation; Go source comments, formatting, and file location do not change the identity.

## Force prune recovery for one user

```bash
curl -X POST http://localhost:8080/test/retention-floor \
  -H "Authorization: Bearer <jwt-token>" \
  -H "Content-Type: application/json" \
  -d '{"retained_bundle_floor": 999999}'
```

This is a test helper for forcing the `history_pruned` path and snapshot recovery flows.

## Test compact push

If `POST /sync/connect` returned `initialize_local`, include the returned `initialization_id`; omit it
for an initialized scope. Obtain the table index and key order from generation discovery. This example
uses table index `0` and one UUID key.

```bash
curl -X POST http://localhost:8080/sync/push \
  -H "Authorization: Bearer <jwt-token>" \
  -H "Oversync-Source-ID: source-123" \
  -H "Oversync-Schema-Generation-ID: 1" \
  -H "Oversync-Schema-Generation-Fingerprint: <64-lowercase-hex-fingerprint>" \
  -H "Content-Type: application/json" \
  -d '{
    "source_bundle_id": 1,
    "canonical_request_hash": "<generation-bound-64-lowercase-hex-sha256>",
    "rows": [[0, 0, [["i", ["550e8400-e29b-41d4-a716-446655440000"], 0, {"id":"550e8400-e29b-41d4-a716-446655440000","name":"John Doe","email":"john@example.com"}]]]
  }'
```

## Test pull

```bash
curl -H "Authorization: Bearer <jwt-token>" \
  -H "Oversync-Source-ID: source-123" \
  -H "Oversync-Schema-Generation-ID: 1" \
  -H "Oversync-Schema-Generation-Fingerprint: <64-lowercase-hex-fingerprint>" \
  "http://localhost:8080/sync/pull?after_bundle_seq=0&max_bundles=100"
```

## Recovery

If incremental pull falls behind the retained bundle floor, the server returns `history_pruned`.
Clients then create a frozen snapshot session with `POST /sync/snapshot-sessions` and fetch
chunks from `GET /sync/snapshot-sessions/{snapshot_id}` until the rebuild is complete.

`POST /sync/connect` is the only ordinary operation that creates a permanent `(user, source)`
generation binding. New sources are admitted under the configured per-user bound (default 64,
maximum 4096); active, reserved, and retired bindings all count. Generation-changing replacement
uses a new source and the `schema_generation_upgrade` reason. Snapshot sessions persist their
effective source and exact proof. An ambiguous push is retried by replaying the same complete source
tuple and request hash; no push-session locator, chunk cursor, or committed-row cursor exists.

Lifecycle-specific failure codes to expect on the sync endpoints:

- `scope_uninitialized`
- `scope_initializing`
- `initialization_stale`
- `initialization_expired`

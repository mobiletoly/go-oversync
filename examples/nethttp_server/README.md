# Net/HTTP Server Example

This example shows a bundle-based go-oversync server built with the standard Go `net/http` stack.

## Endpoints

- `POST /dummy-signin`
- `POST /test/reset`
- `POST /test/retention-floor`
- `POST /sync/push-sessions`
- `POST /sync/push-sessions/{push_id}/chunks`
- `POST /sync/push-sessions/{push_id}/commit`
- `DELETE /sync/push-sessions/{push_id}`
- `GET /sync/committed-bundles/{bundle_seq}/rows`
- `GET /sync/pull`
- `POST /sync/snapshot-sessions`
- `GET /sync/snapshot-sessions/{snapshot_id}`
- `DELETE /sync/snapshot-sessions/{snapshot_id}`
- `GET /sync/capabilities`
- `GET /status`
- `GET /health`

## Architecture

- Business tables are authoritative.
- Sync history is captured from committed business-table effects.
- The supported push transport is staged push sessions plus authoritative committed-bundle fetch.
- Clients still push one logical dirty-set bundle and pull complete committed bundles.
- Hydration and destructive recovery rebuild through chunked snapshot sessions.
- Registered tables in this example use hidden server ownership via `_sync_scope_id TEXT` and
  scope-inclusive keys/FKs on PostgreSQL.
- The example schema exercises cascades, self-references, and a two-table cycle inside the
  supported FK envelope.

## Run

```bash
createdb clisync_example
DATABASE_URL="postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable" \
JWT_SECRET="your-secret-key-change-in-production" \
go run ./examples/nethttp_server
```

## Demo auth

```bash
curl -X POST http://localhost:8080/dummy-signin \
  -H "Content-Type: application/json" \
  -d '{"user":"demo-user","password":"anything","device":"device-123"}'
```

## Reset local example database

```bash
curl -X POST http://localhost:8080/test/reset
```

This drops and recreates the example business schema plus the local `sync` schema used by
`nethttp_server`.

## Force prune recovery for one user

```bash
curl -X POST http://localhost:8080/test/retention-floor \
  -H "Authorization: Bearer <jwt-token>" \
  -H "Content-Type: application/json" \
  -d '{"retained_bundle_floor": 999999}'
```

This is a test helper for forcing the `history_pruned` path and snapshot recovery flows.

## Example push session flow

```bash
PUSH_ID=$(curl -s -X POST http://localhost:8080/sync/push-sessions \
  -H "Authorization: Bearer <jwt-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "source_id": "device-123",
    "source_bundle_id": 1,
    "planned_row_count": 1
  }' | jq -r '.push_id')

curl -X POST "http://localhost:8080/sync/push-sessions/${PUSH_ID}/chunks" \
  -H "Authorization: Bearer <jwt-token>" \
  -H "Content-Type: application/json" \
  -d '{
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
  }'

curl -X POST "http://localhost:8080/sync/push-sessions/${PUSH_ID}/commit" \
  -H "Authorization: Bearer <jwt-token>"
```

## Example pull

```bash
curl -H "Authorization: Bearer <jwt-token>" \
  "http://localhost:8080/sync/pull?after_bundle_seq=0&max_bundles=100"
```

## Recovery

If incremental pull falls behind the retained bundle floor, the server returns `history_pruned`.
Clients then create a frozen snapshot session with `POST /sync/snapshot-sessions` and fetch
chunks from `GET /sync/snapshot-sessions/{snapshot_id}` until the rebuild is complete.

Samplesync Server (PostgreSQL)

This example runs a minimal bundle-based go-oversync server for the `samplesync-kmp` app.

The server uses business tables as authoritative state and exposes the supported lifecycle and
bundle-based sync endpoints.

Endpoints

- `POST /dummy-signin`
- `POST /sync/connect`
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
- `GET /syncx/health`
- `GET /syncx/status`

Database

- Schema: `business`
- Tables:
  - `business.person`
  - `business.person_address`
  - `business.comment`

Registered tables use scope-bound PostgreSQL identity with `_sync_scope_id TEXT NOT NULL`.
Each registered table keeps one visible UUID sync key column, scope-plus-key uniqueness, and
scope-inclusive foreign keys. The synced table set is FK-closed.

Run locally

1. Start PostgreSQL:
   `docker run --rm -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=samplesync -p 5432:5432 postgres:16`
2. Set environment variables and start the server:
   `DATABASE_URL="postgres://postgres:postgres@localhost:5432/samplesync?sslmode=disable" JWT_SECRET="dev-secret" go run ./examples/samplesync_server`

Client settings

- Base URL: `http://localhost:8080` (desktop/iOS) or `http://10.0.2.2:8080` (Android emulator)
- Schema: `business`
- Send `Oversync-Source-ID: <current-source-id>` on authenticated `/sync/*` requests
- Call `POST /sync/connect` after local `Open()` to resolve
  `remote_authoritative`, `initialize_local`, `initialize_empty`, or `retry_later`
- Expect lifecycle-related sync failures:
  - `scope_uninitialized`
  - `scope_initializing`
  - `initialization_stale`
  - `initialization_expired`

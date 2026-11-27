Samplesync Server (PostgreSQL)

This example runs a minimal bundle-based go-oversync server for the `samplesync-kmp` app.

The server uses business tables as authoritative state and exposes the supported lifecycle and
bundle-based sync endpoints.

It stores one immutable canonical effect history and computes bounded generation-visible push,
pull, conflict, and snapshot results on demand. Empty visible bundles still advance checkpoints.
Trigger-normalized and additional effects are valid, but an uploaded key without a final visible
effect rejects and rolls back the whole commit with HTTP 422 `push_effect_coverage_incomplete`.

Endpoints

- `POST /dummy-signin`
- `POST /sync/connect`
- `POST /sync/push`
- `POST /sync/push-outcomes/resolve`
- `POST /sync/push-outcomes/retained`
- `POST /sync/push-outcomes/ack`
- `GET /sync/pull`
- `GET /sync/watch`
- `POST /sync/snapshot-sessions`
- `GET /sync/snapshot-sessions/{snapshot_id}`
- `DELETE /sync/snapshot-sessions/{snapshot_id}`
- `GET /sync/generations/{generation_id}`
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
   `docker run --rm -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=samplesync -p 5432:5432 postgres:17.11`
2. Set environment variables and start the server:
   `DATABASE_URL="postgres://postgres:postgres@localhost:5432/samplesync?sslmode=disable" JWT_SECRET="dev-secret" go run ./examples/samplesync_server`

The tracked `server/baseline-readiness.json` is this sample's canonical static deployment identity.
Changing one of its semantic revisions requires recreating the non-production PostgreSQL database;
source comments, formatting, and file location do not rotate the identity.

Concurrency

- Snapshot admission defaults to four active builds and four active chunk requests.
- Set `OVERSYNC_MACHINE=small|medium|large|auto` (or `OVERSYNC_CPUS` with `OVERSYNC_MEMORY`) to
  size the permits, pool, and Go memory limit for the machine. Positive integers in
  `OVERSYNC_MAX_CONCURRENT_HISTORY_PROJECTIONS`, `OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS` and
  `OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS` override single permits. The full list is in
  the "Sizing and Deployment" documentation page.
- Requests above the active snapshot limit receive a retryable HTTP 429 response instead of
  entering an unbounded server queue. Supported clients honor `Retry-After` and retry within
  their configured capacity-wait budget.
- The PostgreSQL pool defaults to the service's connection budget (18 connections at the default
  permits); `OVERSYNC_DB_POOL_MAX_CONNS` or `pool_max_conns` in `DATABASE_URL` override it.
  Snapshot concurrency limits bound the memory-heavy work; they do not limit connected users or
  devices.
- `OVERSYNC_LOG_LEVEL` selects the log level (default `info`); `PORT` the listen port. The server
  drains requests for up to 30 seconds on SIGTERM.

Client settings

- Base URL: `http://localhost:8080` (desktop/iOS) or `http://10.0.2.2:8080` (Android emulator)
- Schema: `business`
- Send `Oversync-Protocol-Version: v1` and `Oversync-Source-ID` on authenticated `/sync/*` requests.
- Omit proof headers when reading the descriptor at `/sync/generations/{generation_id}`.
- Send the admitted `Oversync-Schema-Generation-ID` and `Oversync-Schema-Generation-Fingerprint`
  on subsequent requests. Acknowledge uploads under the original binding.
- `/sync/watch` is available as an optional wake-up stream; clients still pull data through
  `/sync/pull`
- Call `POST /sync/connect` after the local database is ready to resolve
  `remote_authoritative`, `initialize_local`, `initialize_empty`, or `retry_later`
- Expect lifecycle-related sync failures:
  - `scope_uninitialized`
  - `scope_initializing`
  - `initialization_stale`
  - `initialization_expired`

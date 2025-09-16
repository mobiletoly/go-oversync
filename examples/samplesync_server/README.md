Samplesync Server (PostgreSQL)

This example runs a minimal HTTP server wired to the Oversync sidecar that serves the KMP sample app (samplesync-kmp) using a PostgreSQL database schema named `samplesync`.

Endpoints
- POST `/dummy-signin` → issues a short-lived JWT (demo only)
- POST `/sync/upload` → upload per-item changes
- GET `/sync/download` → download ordered stream (requires `schema=business`)
- GET `/sync/schema-version` → current schema version
- GET `/health` → readiness probe

Database
- Schema: `business`
- Tables (business):
  - `business.person(id uuid pk, first_name, last_name, email unique, phone, birth_date, created_at, notes)`
  - `business.person_address(id uuid pk, person_id uuid fk -> person, address_type, street, city, state, postal_code, country, is_primary, created_at)`
  - `business.comment(id uuid pk, person_id uuid fk -> person, comment, created_at, tags)`

Note: Primary keys are UUIDs, as required by the Oversync sidecar. The KMP sample app should use UUID `id` values for proper sync.

Run locally
1) Start PostgreSQL (e.g. Docker):
   docker run --rm -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=samplesync -p 5432:5432 postgres:16

2) Set env and run:
   export DATABASE_URL="postgres://postgres:postgres@localhost:5432/samplesync?sslmode=disable"
   export JWT_SECRET="dev-secret"
   go run ./examples/samplesync_server

The server listens on :8080 by default.

KMP client settings
- Base URL: http://localhost:8080 (desktop/iOS) or http://10.0.2.2:8080 (Android emulator)
- Schema: `business`


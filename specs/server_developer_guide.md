<!-- moved to specs -->
# Building a Server with go-oversync (Developer Guide)

This guide shows how to integrate the `oversync` server SDK into your Go service to provide robust two‑way synchronization with PostgreSQL, using the sidecar schema. It covers setup, handlers, authentication, table registration, and production tips.

## Overview

- Sidecar schema (`sync.*`) stores sync metadata; your business tables stay clean.
- Single‑user multi‑device model: JWT `sub` = user, JWT `did` = device.
- Upload: Optimistic concurrency via `server_version`, idempotency via `(user_id, source_id, source_change_id)`.
- Download: Snapshot‑consistent paging with optional `until` ceiling (`?until=`); use windowed downloads for hydrations.

## Prerequisites

- Go 1.21+
- PostgreSQL 13+
- go mod: `go get github.com/mobiletoly/go-oversync`

## Step 1 — Define your business schema

Keep business tables clean. Example:

```sql
CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE business.users (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE business.posts (
  id UUID PRIMARY KEY,
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  author_id UUID NOT NULL REFERENCES business.users(id) DEFERRABLE INITIALLY DEFERRED,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);
```

Using DEFERRABLE FKs enables safe batch processing; oversync can auto‑migrate non‑deferrable FKs if desired.

## Step 2 — Initialize pgx pool

```go
cfg, _ := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
cfg.MaxConns = 50
cfg.MinConns = 5
cfg.MaxConnLifetime = time.Hour
cfg.MaxConnIdleTime = 30 * time.Minute
pool, _ := pgxpool.NewWithConfig(context.Background(), cfg)
```

## Step 3 — Create SyncService

Register the schema.table pairs you want to sync. Optionally attach table handlers to materialize sidecar state into your business tables.

```go
serviceCfg := &oversync.ServiceConfig{
  MaxSupportedSchemaVersion: 1,
  AppName: "my-app",
  RegisteredTables: []oversync.RegisteredTable{
    {Schema: "business", Table: "users", Handler: &UsersHandler{logger}},
    {Schema: "business", Table: "posts", Handler: &PostsHandler{logger}},
  },
  DisableAutoMigrateFKs: false, // set true to skip deferrable FK migration
}
svc, err := oversync.NewSyncService(pool, serviceCfg, logger)
```

The service will:
- Create/upgrade the sidecar schema and indexes
- Discover FK topology (for ordering & prechecks)
- Optionally migrate FKs to DEFERRABLE INITIALLY DEFERRED

## Step 4 — JWT authentication

Use the provided JWT helper or plug in your own authenticator.

```go
jwtAuth := auth.NewJWTAuth(os.Getenv("JWT_SECRET")) // HS256; expects sub (user) and did (device)
```

## Step 5 — HTTP handlers (net/http)

```go
handlers := oversync.NewSyncHandlers(svc, jwtAuth, logger)
mux := http.NewServeMux()

mux.Handle("POST /sync/upload", jwtAuth.Middleware(http.HandlerFunc(handlers.HandleUpload)))
mux.Handle("GET /sync/download", jwtAuth.Middleware(http.HandlerFunc(handlers.HandleDownload)))
mux.Handle("GET /sync/schema-version", jwtAuth.Middleware(http.HandlerFunc(handlers.HandleSchemaVersion)))

http.ListenAndServe(":8080", mux)
```

- Upload: POST `/sync/upload` with `UploadRequest` JSON
- Download: GET `/sync/download?after=<id>&limit=<n>&include_self=<bool>&schema=<name>&until=<id>`
- Schema version: GET `/sync/schema-version`

See `swagger/two_way_sync.yaml` for the OpenAPI spec.

## Step 6 — (Optional) Table handlers (materialization)

Implement `oversync.TableHandler` to mirror sidecar changes to your business tables idempotently.

```go
type UsersHandler struct{ logger *slog.Logger }

func (h *UsersHandler) ApplyInsertOrUpdate(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
  var u struct{ ID, Name, Email string }
  if err := json.Unmarshal(payload, &u); err != nil { return err }
  _, err := tx.Exec(ctx,
    `INSERT INTO business.users (id,name,email) VALUES ($1,$2,$3)
     ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name,email=EXCLUDED.email`,
    pk, u.Name, u.Email,
  )
  return err
}

func (h *UsersHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
  _, err := tx.Exec(ctx, `DELETE FROM business.users WHERE id=$1`, pk)
  return err
}
```

Handlers run inside the main upload transaction; any error returns `materialize_error` for that change.

## Step 7 — Client expectations (quick notes)

- JWT must include `sub` (user) and `did` (device)
- Upload changes with incrementing `source_change_id` per device
- Use windowed downloads for hydrations:
  1) First page without `until` (server returns `window_until`)
  2) Subsequent pages include `until=window_until`
- Use `include_self=true` only for recovery/hydration

## Step 8 — Quick test (curl)

Upload:

```bash
curl -X POST http://localhost:8080/sync/upload \
  -H "Authorization: Bearer $JWT" -H "Content-Type: application/json" \
  -d '{
    "last_server_seq_seen": 0,
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
  }'
```

Download (windowed):

```bash
# First page (server returns window_until)
curl -s "http://localhost:8080/sync/download?after=0&limit=100" \
  -H "Authorization: Bearer $JWT" | jq .

# Next page (reuse window_until)
curl -s "http://localhost:8080/sync/download?after=LAST&limit=100&until=WINDOW_UNTIL" \
  -H "Authorization: Bearer $JWT" | jq .
```

## Production tips

- Set PostgreSQL timeouts: `statement_timeout`, `idle_in_transaction_session_timeout`; keep `lock_timeout` local
- Limit upload batch size and per-change payload size
- Monitor metrics: applied/conflict/invalid/materialize_error counts; download latency; page sizes
- Add index `(user_id, schema_name, server_id)` for schema‑filtered paging (created automatically by service)
- For huge tables, plan FK migration to deferrable in maintenance windows

## Troubleshooting

- `conflict`: client must refresh with current server_row and retry
- `invalid` with `fk_missing`: ensure parent changes are present earlier in the same batch or in DB
- `materialize_error`: business projection failed; sidecar not advanced — investigate handler and retry

## References

- OpenAPI spec: `swagger/two_way_sync.yaml`
- Engineering flow: `specs/oversync_flow.md`
- Production readiness checklist: `specs/prod_readiness.md`

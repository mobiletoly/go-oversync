# go-oversync Net/HTTP Server Example (Single-User Multi-Device Sync)

This example demonstrates how to build a two-way sync server using go-oversync with the **single-user multi-device sync architecture** and the standard Go net/http package.

## Features

- **Single-user multi-device sync** - Multiple devices for the same user can sync data
- **User isolation** - Different users cannot see each other's data
- **Sidecar architecture** - Clean separation of business data and sync metadata
- **Two-way synchronization** with conflict resolution
- **JWT authentication** with user ID and device ID claims
- **Optimistic concurrency control** via sidecar tables
- **Idempotent operations** using `(user_id, source_id, source_change_id)` uniqueness
- **Optional materialization** to business tables via handlers
- **Enhanced status types** - applied, conflict, invalid (materialize failures are recorded for admin retry)
- **Graceful shutdown** with proper cleanup

## Single-User Multi-Device Sync Architecture

The example uses the **sidecar architecture** with **user isolation** where business tables are clean and sync metadata is stored separately with user scoping:

### Business Tables (Clean - No Sync Metadata)

#### Users Table
```sql
CREATE TABLE users (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
    -- No server_version - handled by sidecar tables
);
```

#### Posts Table
```sql
CREATE TABLE posts (
    id UUID PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    author_id UUID NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
    -- No server_version - handled by sidecar tables
);
```

### Sidecar Tables (Automatically Created)

The sync service automatically creates these sidecar tables:

#### sync_row_meta - Per-row versioning and lifecycle
```sql
CREATE TABLE sync_row_meta (
    table_name     TEXT      NOT NULL,
    pk_uuid        UUID      NOT NULL,
    server_version BIGINT    NOT NULL DEFAULT 0,
    deleted        BOOLEAN   NOT NULL DEFAULT FALSE,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, pk_uuid)
);
```

#### sync_state - Current after-image for snapshots
```sql
CREATE TABLE sync_state (
    table_name  TEXT   NOT NULL,
    pk_uuid     UUID   NOT NULL,
    payload     JSONB  NOT NULL,
    PRIMARY KEY (table_name, pk_uuid)
);
```

#### server_change_log - Distribution log for idempotency and download
```sql
CREATE TABLE server_change_log (
    server_id          BIGSERIAL PRIMARY KEY,
    table_name         TEXT      NOT NULL,
    op                 TEXT      NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
    pk_uuid            UUID      NOT NULL,
    payload            JSONB,
    source_id          TEXT      NOT NULL,
    source_change_id   BIGINT    NOT NULL,
    ts                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_id, source_change_id)
);
```

## API Endpoints

### Two-Way Sync Endpoints (JWT Required)

- **POST /sync/upload** - Upload changes with conflict resolution
- **GET /sync/download** - Download changes from server
- **GET /sync/schema-version** - Get current schema version

### Utility Endpoints

- **GET /health** - Health check (no auth required)
- **GET /** - API documentation page
- **POST /dummy-signin** - Obtain a short‑lived JWT for a given user/device (demo only)

## Running the Example

### Prerequisites

1. **PostgreSQL database** running locally or accessible via connection string
2. **Go 1.21+** installed

### Setup

1. **Create database:**
   ```bash
   createdb clisync_example
   ```

2. **Set environment variables (optional):**
   ```bash
   export DATABASE_URL="postgres://postgres:password@localhost:5432/clisync_example?sslmode=disable"
   export JWT_SECRET="your-secret-key-change-in-production"
   ```

3. **Run the server:**
   ```bash
   cd examples/nethttp_server
   go run .
   ```

4. **Visit the server:**
   - Open http://localhost:8080 for API documentation
   - Check http://localhost:8080/health for health status

## Authentication

All sync endpoints require JWT authentication. The JWT token must contain a `source_id` claim that identifies the client.

### Getting a JWT (demo)

Use the demo endpoint to get a token (any password is accepted):

```bash
curl -X POST http://localhost:8080/dummy-signin \
  -H "Content-Type: application/json" \
  -d '{"user":"demo-user","password":"anything","device":"device-123"}'
# => {"token":"<jwt>","expires_in":300,"user":"demo-user","device":"device-123"}
```

### Making Authenticated Requests

```bash
curl -H "Authorization: Bearer <jwt-token>" \
     -H "Content-Type: application/json" \
     -X POST http://localhost:8080/sync/upload \
     -d '{
       "source_id": "client-device-123",
       "last_server_seq_seen": 0,
       "changes": [
         {
           "source_change_id": 1,
           "table": "users",
           "op": "INSERT",
           "pk": "550e8400-e29b-41d4-a716-446655440000",
           "server_version": 0,
           "payload": {
             "id": "550e8400-e29b-41d4-a716-446655440000",
             "name": "John Doe",
             "email": "john@example.com",
             "updated_at": 1640995200
           }
         }
       ]
     }'
```

## Two-Way Sync Flow

### 1. Upload Changes (Client → Server)
```json
POST /sync/upload
{
  "source_id": "mobile-app-v1",
  "last_server_seq_seen": 42,
  "changes": [
    {
      "source_change_id": 123,
      "table": "users",
      "op": "UPDATE",
      "pk": "550e8400-e29b-41d4-a716-446655440000",
      "server_version": 5,
      "payload": {
        "name": "Updated Name",
        "email": "updated@example.com",
        "updated_at": 1640995300
      }
    }
  ]
}
```

### 2. Download Changes (Server → Client)
```bash
GET /sync/download?after=42&limit=100
```

Response:
```json
{
  "changes": [
    {
      "server_id": 43,
      "table": "users",
      "op": "INSERT",
      "pk": "550e8400-e29b-41d4-a716-446655440001",
      "payload": {
        "id": "550e8400-e29b-41d4-a716-446655440001",
        "name": "Jane Doe",
        "email": "jane@example.com",
        "updated_at": "2025-08-08T07:00:00Z"
      },
      "server_version": 1,
      "deleted": false,
      "source_id": "web-app-v2",
      "source_change_id": 456,
      "ts": "2025-08-08T07:01:00Z"
    }
  ],
  "has_more": false,
  "next_after": 43
}
```

## Conflict Resolution

When a conflict occurs (server_version mismatch), the server returns the current sidecar state:

```json
{
  "accepted": true,
  "highest_server_seq": 156,
  "statuses": [
    {
      "source_change_id": 123,
      "status": "conflict",
      "server_row": {
        "table_name": "users",
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "server_version": 7,
        "deleted": false,
        "payload": {
          "id": "550e8400-e29b-41d4-a716-446655440000",
          "name": "Different Name",
          "email": "different@example.com",
          "updated_at": "2025-08-08T07:05:00Z"
        }
      }
    }
  ]
}
```

### Status Types

- **applied**: Change successfully applied to sidecar (materialization is best-effort)
- **conflict**: Version mismatch, see server_row for current state
- **invalid**: Validation failed (bad table name, UUID, JSON, etc.)
- Materialization failures are recorded in `sync.materialize_failures` for admin retry; upload still returns `applied`.

## Sidecar Architecture

The example demonstrates the new sidecar architecture:

1. **Service Layer**: `clisync.SyncService` handles all sync logic via sidecar tables
2. **Sidecar Tables**: Automatic sync metadata management (versioning, lifecycle, distribution)
3. **Table Handlers**: Optional materialization to clean business tables
4. **HTTP Layer**: Standard net/http with JWT middleware
5. **Database Layer**: PostgreSQL with pgx for optimal performance

### Data Flow

1. **Upload**: Client → Sidecar tables → Optional business table materialization
2. **Download**: Sidecar tables → Enhanced response with server_version and deleted status
3. **Conflict Resolution**: Sidecar metadata provides current state for conflicts

## Production Considerations

- Change the JWT secret in production
- Use proper database connection pooling
- Add rate limiting and request validation
- Implement proper logging and monitoring
- Consider using HTTPS in production
- Add database migrations for schema changes

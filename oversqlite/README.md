# oversqlite — SQLite Client SDK for go-oversync

The `oversqlite` package is a SQLite-based client for go-oversync’s single-user, multi-device sync.
It tracks local changes with triggers, performs idempotent uploads, and applies ordered downloads
with snapshot consistency and conflict handling.

## Features

- SQLite integration with minimal assumptions about your schema
- Automatic change tracking via triggers (INSERT/UPDATE/DELETE)
- Single-user multi-device sync (user_id + source_id)
- Optimistic concurrency (server_version) and conflict handling hooks
- Idempotent uploads with device-local `change_id`
- Windowed, snapshot-consistent downloads (frozen `until`) with include-self for hydration
- Clean separation: business tables remain clean; client-side sync tables are separate

## Client-side Tables (created automatically)

```
Business Tables           Client Sync Tables (local)
┌────────────────────┐    ┌──────────────────────────────┐
│ users, posts, ...  │    │ _sync_client_info            │
|                    |    |                              |
│  (your schema)     │    │ • user_id, source_id         │
└────────────────────┘    │ • next_change_id             │
                          │ • last_server_seq_seen       │
                          │ • apply_mode (trigger guard) │
                          │ • current_window_until       │
                          └──────────────────────────────┘
                          ┌──────────────────────────────┐
                          │ _sync_row_meta               │
                          |                              |
                          │ • table_name, pk_uuid        │
                          │ • server_version, deleted    │
                          │ • updated_at                 │
                          └──────────────────────────────┘
                          ┌──────────────────────────────┐
                          │ _sync_pending (one row/PK)   │
                          |                              |
                          │ • table_name, pk_uuid, op    │
                          │ • base_version, payload      │
                          │ • change_id, queued_at       │
                          └──────────────────────────────┘
```

Business tables stay clean. Triggers coalesce pending changes per PK and capture payloads. BLOB
columns are hex-encoded in triggers and converted to base64 on upload.

Notes on cursors and watermarks:

- `last_server_seq_seen` is the **download cursor** (last applied `server_id`). It must advance only
  when a download page is successfully applied; fast-forwarding it from upload responses can skip
  peer changes when `server_id` gaps are large under high parallelism.
- `current_window_until` tracks the latest known server watermark (e.g., from upload/download
  responses) and can be used as the `until` ceiling for windowed paging.

## Quick Start

```go
package main

import (
    "context"
    "database/sql"
    "log"
    _ "github.com/mattn/go-sqlite3"

    "github.com/mobiletoly/go-oversync/oversqlite"
)

func main() {
    db, err := sql.Open("sqlite3", "app.db")
    if err != nil { log.Fatal(err) }
    defer db.Close()

    // Your business tables
    _, _ = db.Exec(`CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT NOT NULL
    )`)
    _, _ = db.Exec(`CREATE TABLE IF NOT EXISTS posts (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        content TEXT,
        author_id TEXT NOT NULL,
        FOREIGN KEY (author_id) REFERENCES users(id)
    )`)

    // JWT provider (refresh as needed)
    token := func(ctx context.Context) (string, error) { return "<jwt>", nil }

    // Configure tracked tables; triggers are created automatically
    cfg := oversqlite.DefaultConfig("business", []oversqlite.SyncTable{
        {TableName: "users"},
        {TableName: "posts"},
    })

    // Use a stable, per-device source_id (persist it); helper available:
    // sourceID, _ := oversqlite.EnsureSourceID(db, userID)
    userID := "user-123"
    sourceID := "device-abc"

    client, err := oversqlite.NewClient(db, "http://localhost:8080", userID, sourceID, token, cfg)
    if err != nil { log.Fatal(err) }

    // One-shot syncs
    if err := client.UploadOnce(context.Background()); err != nil {
        log.Printf("upload error: %v", err)
    }
    if applied, _, err := client.DownloadOnce(context.Background(), 1000); err == nil {
        log.Printf("applied %d changes", applied)
    }
}
```

## API Overview

- Construction
  - `DefaultConfig(schema string, tables []SyncTable) *Config`
  - `NewClient(db *sql.DB, baseURL, userID, sourceID string, tok func(context.Context) (string, error), config *Config) (*Client, error)`
  - Optional helper: `EnsureSourceID(db *sql.DB, userID string) (string, error)`

- One-shot operations
  - `UploadOnce(ctx context.Context) error`
  - `DownloadOnce(ctx context.Context, limit int) (applied int, nextAfter int64, err error)`
  - `DownloadOnceUntil(ctx context.Context, limit int, until int64) (applied int, nextAfter int64, err error)`
  - `DownloadWithSelf(ctx context.Context, limit int) (applied int, nextAfter int64, err error)`
  - `Hydrate(ctx context.Context) error` (fresh install/recovery; includes own history with a frozen window)

- Background loops
  - `Start(ctx context.Context) error` (spawns uploader and downloader loops with backoff)
  - `PauseUploads()/ResumeUploads()` and `PauseDownloads()/ResumeDownloads()`

- Introspection helpers
  - `GetTableRowCount(table string) (int64, error)`
  - `GetTableRows(table string) ([]map[string]interface{}, error)`
  - `SerializeRow(ctx, table, pk) (json.RawMessage, error)`

## Configuration

```go
type SyncTable struct {
    TableName         string // e.g., "users"
    SyncKeyColumnName string // primary key column (defaults to auto-detected or "id")
}

type Config struct {
    Schema        string        // schema name for server-side filtering
    Tables        []SyncTable   // tracked tables
    UploadLimit   int           // batch size
    DownloadLimit int           // page size
    BackoffMin    time.Duration // background retry min
    BackoffMax    time.Duration // background retry max
}
```

For tables that don’t use `id` as the PK, set `SyncKeyColumnName`. If omitted, the client
auto-detects the primary key from the table schema, falling back to `id` if absent.

## Triggers and Payloads

- Triggers coalesce pending operations per PK in `_sync_pending` and record a JSON payload.
- BLOB columns are hex-encoded in payloads by triggers; the client converts them to base64
  before upload so the server receives standard base64.
- For BLOB primary keys, PK values are hex-encoded; the client converts for queries accordingly.

## Conflict Handling

- Server enforces optimistic concurrency. On `conflict`, the client consults a `Resolver`:
  - DefaultResolver is “server wins”.
  - Implement `Resolver.Merge(table, pk, server, local)` to keep local changes or merge.
- FK invalids (retryable) are kept in pending for future retries; permanently invalid changes are dropped.

## Download Windows

- The server supports a frozen `until` upper bound for snapshot-consistent paging.
- `Hydrate` downloads a consistent snapshot including your own history (include_self=true) across the frozen window.
- Normal paging excludes own `source_id` to avoid echo; lookback after upload prevents missed peer changes.

## Examples

- End-to-end mobile simulator: `examples/mobile_flow/`
- HTTP server example: `examples/nethttp_server/`

## Testing

```bash
go test ./oversqlite
```

Tests cover trigger creation, BLOB handling, pagination, hydration, conflict flows, and multi-device scenarios.

## Notes

- Keep a stable `source_id` per install and per user. Use `EnsureSourceID` to generate/persist.
- Ensure your HTTP server exposes `/sync/upload` and `/sync/download` per the go-oversync spec and returns per-change statuses.

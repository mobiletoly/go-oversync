---
layout: default
title: Getting Started
---

# Getting Started

This guide shows the current bundle-based go-oversync contract: business tables are authoritative
on the server, clients push one logical dirty set at a time, and clients pull complete committed
bundles or rebuild from a stable snapshot.

## Supported Envelope

The current production-ready contract is intentionally narrow:

- single-column UUID sync keys
- single-column foreign keys between registered tables
- FK-closed client/server table sets
- fail-closed bootstrap if schema or config falls outside that envelope

If your schema is outside that envelope, startup should fail instead of degrading into best-effort
runtime behavior.

## Schema Requirements For Registered Tables

If you register a table for sync, the table creator must define it according to the supported
contract. go-oversync does not rewrite table definitions during bootstrap.

Required in the current first cut:

- each registered table must have exactly one sync key column, and it must be a `UUID` primary key
- every registered foreign key must point to another registered table or to the same registered
  table for self-references
- every supported foreign key on a registered table must be `DEFERRABLE`
- `INITIALLY DEFERRED` is recommended
- `DEFERRABLE INITIALLY IMMEDIATE` is accepted because the runtime defers constraints inside sync
  transactions
- composite primary keys and composite foreign keys are unsupported and cause bootstrap failure

If a table violates these rules, `Bootstrap()` fails with an `UnsupportedSchemaError` that includes
the offending table or constraint name and a hint about what to fix.

Recommended pattern:

```sql
CREATE TABLE business.users (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE business.posts (
    id UUID PRIMARY KEY,
    author_id UUID NOT NULL,
    title TEXT NOT NULL,
    CONSTRAINT posts_author_id_fkey
        FOREIGN KEY (author_id) REFERENCES business.users(id)
        ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED
);
```

Accepted but less preferred:

```sql
CREATE TABLE business.categories (
    id UUID PRIMARY KEY,
    parent_id UUID,
    name TEXT NOT NULL,
    CONSTRAINT categories_parent_id_fkey
        FOREIGN KEY (parent_id) REFERENCES business.categories(id)
        DEFERRABLE INITIALLY IMMEDIATE
);
```

Unsupported in the current first cut:

```sql
CREATE TABLE business.memberships (
    user_id UUID NOT NULL,
    group_id UUID NOT NULL,
    PRIMARY KEY (user_id, group_id)
);
```

## Core Concepts

### User ID

- Identifies one isolated sync stream.
- Comes from your authentication system.
- Every pushed or pulled bundle is scoped to exactly one user.

### Device ID / Source ID

- Identifies one app installation.
- Used for idempotent push replay.
- Must stay stable across normal app restarts.

### Bundle

- The smallest durable sync unit.
- Represents the net committed effects of one accepted server transaction.
- Is pulled and applied as a whole, never row-by-row as a public contract.

### Row Version

- Monotonic version of one synced row.
- Used for optimistic concurrency on push.

### Snapshot

- Full current after-image at one exact `snapshot_bundle_seq`.
- Used by `Hydrate()` and `Recover()` when a client is new, reset, or falls behind retained
  history.

## Server Metadata

go-oversync derives sync metadata from committed business-table effects. The main bundle-era tables
are:

- `sync.user_state`
- `sync.row_state`
- `sync.bundle_log`
- `sync.bundle_rows`
- `sync.applied_pushes`

Your business tables remain the source of truth.

## Step 1: Start PostgreSQL

```bash
docker run --name oversync-pg \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -d postgres:16
createdb my_sync_app
```

## Step 2: Create Your Business Tables

```sql
CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.users (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS business.posts (
    id UUID PRIMARY KEY,
    author_id UUID NOT NULL,
    title TEXT NOT NULL,
    content TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT posts_author_id_fkey
        FOREIGN KEY (author_id) REFERENCES business.users(id)
        ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED
);
```

## Step 3: Create The Server

```go
package main

import (
    "context"
    "log"
    "log/slog"
    "net/http"
    "os"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/mobiletoly/go-oversync/oversync"
)

func main() {
    ctx := context.Background()
    logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

    pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/my_sync_app?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    cfg := &oversync.ServiceConfig{
        MaxSupportedSchemaVersion: 1,
        AppName: "my-sync-app",
        RegisteredTables: []oversync.RegisteredTable{
            {Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}},
            {Schema: "business", Table: "posts", SyncKeyColumns: []string{"id"}},
        },
    }

    svc, err := oversync.NewRuntimeService(pool, cfg, logger)
    if err != nil {
        log.Fatal(err)
    }
    if err := svc.Bootstrap(ctx); err != nil {
        log.Fatal(err)
    }

    handlers := oversync.NewHTTPSyncHandlers(svc, logger)

    mux := http.NewServeMux()
    mux.Handle("POST /sync/push-sessions", yourAuthMiddleware(http.HandlerFunc(handlers.HandleCreatePushSession)))
    mux.Handle("POST /sync/push-sessions/{push_id}/chunks", yourAuthMiddleware(http.HandlerFunc(handlers.HandlePushSessionChunk)))
    mux.Handle("POST /sync/push-sessions/{push_id}/commit", yourAuthMiddleware(http.HandlerFunc(handlers.HandleCommitPushSession)))
    mux.Handle("DELETE /sync/push-sessions/{push_id}", yourAuthMiddleware(http.HandlerFunc(handlers.HandleDeletePushSession)))
    mux.Handle("GET /sync/committed-bundles/{bundle_seq}/rows", yourAuthMiddleware(http.HandlerFunc(handlers.HandleGetCommittedBundleRows)))
    mux.Handle("GET /sync/pull", yourAuthMiddleware(http.HandlerFunc(handlers.HandlePull)))
    mux.Handle("POST /sync/snapshot-sessions", yourAuthMiddleware(http.HandlerFunc(handlers.HandleCreateSnapshotSession)))
    mux.Handle("GET /sync/snapshot-sessions/{snapshot_id}", yourAuthMiddleware(http.HandlerFunc(handlers.HandleGetSnapshotChunk)))
    mux.Handle("DELETE /sync/snapshot-sessions/{snapshot_id}", yourAuthMiddleware(http.HandlerFunc(handlers.HandleDeleteSnapshotSession)))
    mux.Handle("GET /sync/capabilities", yourAuthMiddleware(http.HandlerFunc(handlers.HandleCapabilities)))
    mux.HandleFunc("GET /health", handlers.HandleHealth)
    mux.HandleFunc("GET /status", handlers.HandleStatus)

    log.Fatal(http.ListenAndServe(":8080", mux))
}
```

`yourAuthMiddleware` must authenticate the request and inject
`oversync.Actor{UserID, SourceID}` into the request context.

## Step 4: Create The SQLite Client

```go
package main

import (
    "context"
    "database/sql"
    "log"

    "github.com/mobiletoly/go-oversync/oversqlite"
    _ "github.com/mattn/go-sqlite3"
)

func main() {
    db, err := sql.Open("sqlite3", "app.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    _, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            author_id TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT,
            FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE
        );
    `)
    if err != nil {
        log.Fatal(err)
    }

    cfg := oversqlite.DefaultConfig("business", []oversqlite.SyncTable{
        {TableName: "users", SyncKeyColumnName: "id"},
        {TableName: "posts", SyncKeyColumnName: "id"},
    })

    tokenProvider := func(ctx context.Context) (string, error) {
        return "<jwt>", nil
    }

    client, err := oversqlite.NewClient(
        db,
        "http://localhost:8080",
        "user-123",
        "device-abc",
        tokenProvider,
        cfg,
    )
    if err != nil {
        log.Fatal(err)
    }

    if err := client.Bootstrap(context.Background(), false); err != nil {
        log.Fatal(err)
    }
}
```

## Step 5: Sync

Once the client exists, the supported high-level operations are:

```go
ctx := context.Background()

if err := client.PushPending(ctx); err != nil {
    log.Fatal(err)
}

if err := client.PullToStable(ctx); err != nil {
    log.Fatal(err)
}

if err := client.Sync(ctx); err != nil {
    log.Fatal(err)
}
```

Recovery operations:

```go
if err := client.Hydrate(ctx); err != nil {
    log.Fatal(err)
}

if err := client.Recover(ctx); err != nil {
    log.Fatal(err)
}
```

Behavior to expect:

- `PushPending()` freezes one outbound snapshot, uploads it through push sessions, fetches the
  committed authoritative rows, and replays them locally.
- `PullToStable()` drains complete bundles until the frozen `stable_bundle_seq` is reached.
- `Hydrate()` rebuilds through chunked snapshot sessions.
- `Recover()` rebuilds through chunked snapshot sessions and rotates `source_id`.

## Important Client Rules

- Every managed table must declare its sync key explicitly.
- Managed tables must be FK-closed.
- `PullToStable()`, `Hydrate()`, and `Recover()` fail closed while `_sync_push_outbound` exists.
- `Sync()` fails closed while `_sync_client_state.rebuild_required = 1`.
- The durable read checkpoint is `_sync_client_state.last_bundle_seq_seen`.
- The next outgoing client bundle id is `_sync_client_state.next_source_bundle_id`.

## Endpoints

The supported HTTP endpoints are:

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
- `GET /health`
- `GET /status`

## Binary Payload Contract

- non-key binary payload fields use standard base64 on the wire
- UUID-valued keys and UUID-valued key columns use dashed UUID text on the wire
- local trigger capture may use different internal encodings; those are not the HTTP contract

## Next Steps

- Run the reference server in `examples/nethttp_server/`.
- Run the simulator in `examples/mobile_flow/`.
- Inspect the public API contract in `docs/documentation/api.md`.

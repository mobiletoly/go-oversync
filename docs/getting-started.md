---
layout: default
title: Getting Started
---

# Getting Started

This guide reflects the current contract implemented in this repository:

- PostgreSQL business tables are authoritative
- clients push one logical dirty set through staged push sessions
- clients replay the authoritative committed bundle after push commit
- clients pull complete committed bundles only
- fresh installs and prune recovery rebuild through snapshot sessions

## Supported Envelope

The supported envelope is intentionally narrow:

- exactly one visible sync key column per registered PostgreSQL table
- visible sync key type `uuid` or `text` on registered PostgreSQL tables
- scope-bound registered PostgreSQL identity through `_sync_scope_id TEXT NOT NULL`
- scope-inclusive foreign keys between registered PostgreSQL tables
- FK-closed server and client table sets
- fail-closed bootstrap if schema or config falls outside that envelope

## PostgreSQL Table Requirements

Registered PostgreSQL tables must satisfy these rules:

- each registered table must have exactly one visible sync key column
- visible sync key type must be `uuid` or `text`
- each registered table must define `_sync_scope_id TEXT NOT NULL`
- `(_sync_scope_id, sync_key)` must be unique
- every unique constraint and unique index on a registered table must include `_sync_scope_id`
- every registered foreign key must point to another registered table or to the same registered
  table for self-references
- every registered foreign key must include `_sync_scope_id`
- every supported foreign key on a registered table must be `DEFERRABLE`
- supported `ON DELETE` / `ON UPDATE` actions are `NO ACTION`, `RESTRICT`, `CASCADE`, `SET NULL`,
  or `SET DEFAULT`
- supported `MATCH` options are empty, `NONE`, or `SIMPLE`
- `DEFERRABLE INITIALLY DEFERRED` is recommended
- `DEFERRABLE INITIALLY IMMEDIATE` is accepted because the runtime defers constraints inside sync
  transactions
- partial, predicate, and expression unique indexes are unsupported on registered tables

If a table violates these rules, `Bootstrap()` fails with an `UnsupportedSchemaError`.

Recommended pattern:

```sql
CREATE TABLE business.users (
    _sync_scope_id TEXT NOT NULL,
    id UUID NOT NULL,
    name TEXT NOT NULL
    PRIMARY KEY (_sync_scope_id, id)
);

CREATE TABLE business.posts (
    _sync_scope_id TEXT NOT NULL,
    id UUID NOT NULL,
    author_id UUID NOT NULL,
    title TEXT NOT NULL,
    PRIMARY KEY (_sync_scope_id, id),
    CONSTRAINT posts_author_id_fkey
        FOREIGN KEY (_sync_scope_id, author_id) REFERENCES business.users(_sync_scope_id, id)
        ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED
);
```

## Core Terms

- `user_id`: one isolated sync stream
- `source_id`: one app installation or device identity
- `source_bundle_id`: per-device monotonically increasing push id
- `bundle_seq`: server-side committed bundle sequence
- `row_version`: authoritative row version used for optimistic concurrency
- `snapshot_bundle_seq`: the frozen bundle ceiling attached to a snapshot rebuild

## Server Metadata

The server keeps sync metadata in the `sync` schema. The main runtime tables are:

- `sync.user_state`
- `sync.row_state`
- `sync.bundle_log`
- `sync.bundle_rows`
- `sync.applied_pushes`
- `sync.push_sessions`
- `sync.push_session_rows`
- `sync.snapshot_sessions`
- `sync.snapshot_session_rows`

## Step 1: Start PostgreSQL

```bash
docker run --name oversync-pg \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -d postgres:16

docker exec oversync-pg createdb -U postgres my_sync_app
```

## Step 2: Create Your Business Tables

```sql
CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.users (
    _sync_scope_id TEXT NOT NULL,
    id UUID NOT NULL,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (_sync_scope_id, id),
    UNIQUE (_sync_scope_id, email)
);

CREATE TABLE IF NOT EXISTS business.posts (
    _sync_scope_id TEXT NOT NULL,
    id UUID NOT NULL,
    author_id UUID NOT NULL,
    title TEXT NOT NULL,
    content TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (_sync_scope_id, id),
    CONSTRAINT posts_author_id_fkey
        FOREIGN KEY (_sync_scope_id, author_id) REFERENCES business.users(_sync_scope_id, id)
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
        AppName:                   "my-sync-app",
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
`oversync.Actor{UserID, SourceID}` into request context. The server derives `_sync_scope_id` from
`Actor.UserID`, so clients must not send `_sync_scope_id` in push payloads.

## Step 4: Create The SQLite Client

Local SQLite managed tables must declare exactly one visible sync key column, and that column
must also be the local SQLite `PRIMARY KEY` in the current runtime. Supported local key shapes are
`TEXT PRIMARY KEY` and UUID-backed `BLOB PRIMARY KEY`.

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
    ctx := context.Background()

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
    defer client.Close()

    if err := client.Bootstrap(ctx, true); err != nil {
        log.Fatal(err)
    }
}
```

## Step 5: Sync

The supported high-level operations are:

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
- `PullToStable()` rebuilds automatically through snapshot sessions if the server returns
  `history_pruned`.
- `Hydrate()` rebuilds through chunked snapshot sessions.
- `Recover()` rebuilds through chunked snapshot sessions and rotates `source_id`.

## Important Client Rules

- every managed table must declare its sync key explicitly
- managed tables must be FK-closed
- `PullToStable()`, `Hydrate()`, and `Recover()` fail closed while `_sync_push_outbound` exists
- `Sync()` fails closed while `_sync_client_state.rebuild_required = 1`
- the durable read checkpoint is `_sync_client_state.last_bundle_seq_seen`
- the next outgoing client bundle id is `_sync_client_state.next_source_bundle_id`

## Endpoints

The server exposes:

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
- Inspect the API contract in `docs/documentation/api.md`.

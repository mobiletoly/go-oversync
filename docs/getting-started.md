---
layout: default
title: Getting Started
---

# Getting Started

This guide reflects the current contract implemented in this repository:

- PostgreSQL business tables are authoritative
- clients push one logical dirty set through one bounded `POST /sync/push` request
- clients apply the authoritative response bundle atomically
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
- visible sync key type must be `uuid` or `text` and must declare `NOT NULL`
- each registered relation must be a permanent logged PostgreSQL table
  (`pg_class.relpersistence = 'p'`); `UNLOGGED` and temporary relations are unsupported
- each registered root and every current partition/inheritance descendant must define
  `_sync_scope_id TEXT NOT NULL` and the visible sync key as `NOT NULL`
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

Those bullets describe the legacy registered-table envelope. A v1 generation-visible foreign key
is narrower: `set_null` is allowed only for `OnDelete`, the visible child column must be nullable,
and PostgreSQL must use `ON DELETE SET NULL (visible_child_column)` so `_sync_scope_id` remains
unchanged. `OnUpdate: set_null` and generation-visible `set_default` are rejected.

If a table violates these rules, `Bootstrap()` fails with an `UnsupportedSchemaError`.
Persistence validation runs before Oversync creates or accepts `sync.*`, including when an existing
layout is already marked ready.

The `sync` schema is reserved. Bootstrap also validates every managed table, column, constraint,
index, sequence, function, and reserved registered-table trigger before readiness. A coherent
marked layout is not rewritten. Drift fails with a typed `managed sync layout mismatch`; stop all
server instances, investigate the reported definition delta, and restore a reviewed compatible
backup or follow a separately reviewed operator procedure. Do not run mixed server versions.

Nullable identities fail before sync metadata or registered-trigger mutation even when the table is
empty. Stop writers, resolve existing NULL values through application policy, add the reported
`NOT NULL` constraints, and retry. Coordinate the migration before starting corrected servers;
older binaries do not enforce it. HTTP/wire, checkpoint, pull, and snapshot semantics are unchanged.

For a fresh managed layout, Bootstrap locks registered tables and atomically adopts existing
business rows before readiness. Once that transaction has committed, ordinary restarts use a
trusted-database attachment path. Attachment revalidates registered declarations and exact managed
definitions but reads no business rows or managed operational state, takes no explicit data locks,
and performs no DML, DDL, adoption, hashing, or repair.

Normal direct registered-table `INSERT`, `UPDATE`, and `DELETE` without bundle context remain
rejected, as does every registered-table `TRUNCATE`. Privileged guard bypass and direct edits to
managed rows are unsupported. Bootstrap does not detect or repair those data mutations; restore a
known coherent backup or use a separately reviewed operator procedure.

Recommended pattern:

```sql
CREATE TABLE business.users (
    _sync_scope_id TEXT NOT NULL,
    id UUID NOT NULL,
    name TEXT NOT NULL,
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
- `current_source_id`: the internally managed current sync source identity
- `source_bundle_id`: per-source monotonically increasing push id
- `bundle_seq`: server-side committed bundle sequence
- `row_version`: authoritative row version used for optimistic concurrency
- `snapshot_bundle_seq`: the frozen bundle ceiling attached to a snapshot rebuild

## Server Metadata

The server keeps sync metadata in the `sync` schema. The main runtime tables are:

- `sync.meta`
- `sync.table_catalog`
- `sync.user_state`
- `sync.scope_state`
- `sync.source_state`
- `sync.row_state`
- `sync.bundle_log`
- `sync.bundle_rows`
- `sync.snapshot_sessions`
- `sync.snapshot_session_rows`

The large server-side sync tables use compact internal identifiers such as `user_pk`, `table_id`,
`key_bytes`, and `op_code`. The HTTP API still exposes readable `schema`, `table`, structured
`key`, `op`, and `row_version` fields.

## Step 1: Start PostgreSQL

Oversync supports PostgreSQL 16.x and 17.x. This example uses the maintained PostgreSQL 16
compatibility lane.

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
    created_at TEXT NOT NULL,
    PRIMARY KEY (_sync_scope_id, id),
    UNIQUE (_sync_scope_id, email)
);

CREATE TABLE IF NOT EXISTS business.posts (
    _sync_scope_id TEXT NOT NULL,
    id UUID NOT NULL,
    author_id UUID NOT NULL,
    title TEXT NOT NULL,
    content TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (_sync_scope_id, id),
    CONSTRAINT posts_author_id_fkey
        FOREIGN KEY (_sync_scope_id, author_id) REFERENCES business.users(_sync_scope_id, id)
        ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED
);
```

## Step 3: Create The Server

{% raw %}
```go
package main

import (
    "context"
    "errors"
    "log"
    "log/slog"
    "net/http"
    "os"
    "strings"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/mobiletoly/go-oversync/oversync"
)

var registeredTables = []oversync.RegisteredTable{
    {Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}},
    {Schema: "business", Table: "posts", SyncKeyColumns: []string{"id"}},
}

var schemaGenerations = []oversync.SchemaGeneration{{
    ID: 1,
    Tables: []oversync.GenerationTable{
        {
            Schema: "business",
            Table:  "users",
            Columns: []oversync.GenerationColumn{
                {Name: "created_at", WireType: oversync.GenerationWireTypeText},
                {Name: "email", WireType: oversync.GenerationWireTypeText},
                {Name: "id", WireType: oversync.GenerationWireTypeUUID},
                {Name: "name", WireType: oversync.GenerationWireTypeText},
            },
        },
        {
            Schema: "business",
            Table:  "posts",
            Columns: []oversync.GenerationColumn{
                {Name: "author_id", WireType: oversync.GenerationWireTypeUUID},
                {Name: "content", WireType: oversync.GenerationWireTypeText, Nullable: true},
                {Name: "created_at", WireType: oversync.GenerationWireTypeText},
                {Name: "id", WireType: oversync.GenerationWireTypeUUID},
                {Name: "title", WireType: oversync.GenerationWireTypeText},
            },
            ForeignKeys: []oversync.GenerationForeignKey{{
                Columns:           []string{"author_id"},
                ReferencedSchema:  "business",
                ReferencedTable:   "users",
                ReferencedColumns: []string{"id"},
                OnDelete:          oversync.GenerationForeignKeyCascade,
                OnUpdate:          oversync.GenerationForeignKeyNoAction,
            }},
        },
    },
}}

func main() {
    ctx := context.Background()
    logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

    pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/my_sync_app?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    cfg := &oversync.ServiceConfig{
        Readiness:                     yourApplicationReadiness, // exact deployment contract and database adapter
        SchemaGenerations:              schemaGenerations, // one or two complete declarations
        AppName:                        "my-sync-app",
        RegisteredTables:               registeredTables,
        RegisteredTableCatalogRevision: 1,
        BundleChangeWatch:              oversync.BundleChangeWatchConfig{Enabled: true},
    }

    svc, err := oversync.NewRuntimeService(pool, cfg, logger)
    if err != nil {
        log.Fatal(err)
    }
    if err := svc.Bootstrap(ctx); err != nil {
        log.Fatal(err)
    }

    registry, err := oversync.CompileSchemaGenerationRegistry(registeredTables, schemaGenerations)
    if err != nil {
        log.Fatal(err)
    }
    contract, ok := registry.Generation(1)
    if !ok {
        log.Fatal("generation 1 was not compiled")
    }
    logger.Info("registered schema generation",
        "generation_id", contract.ID(),
        "generation_fingerprint", contract.Fingerprint())
    listenerCtx, stopListener := context.WithCancel(ctx)
    defer stopListener()
    go func() {
        if err := svc.RunBundleChangeListener(listenerCtx); err != nil && !errors.Is(err, context.Canceled) {
            logger.Error("bundle change listener stopped", "error", err)
        }
    }()

    handlers := oversync.NewHTTPSyncHandlers(svc, logger)

    syncActorMiddleware := oversync.ActorMiddleware(oversync.ActorMiddlewareConfig{
        UserIDFromContext: func(ctx context.Context) (string, error) {
            return yourUserIDFromContext(ctx)
        },
    })

    withSyncAdmission := func(next http.Handler) http.Handler {
        return yourSyncClientPolicy(yourAuthMiddleware(syncActorMiddleware(next)))
    }

    mux := http.NewServeMux()
    // Register raw sync handlers. The outer dispatcher below owns admission so
    // ServeMux cannot clean malformed discovery paths before authentication.
    mux.HandleFunc("POST /sync/connect", handlers.HandleConnect)
    mux.HandleFunc("POST /sync/push", handlers.HandlePush)
    mux.HandleFunc("GET /sync/pull", handlers.HandlePull)
    mux.HandleFunc("GET /sync/watch", handlers.HandleWatch)
    mux.HandleFunc("POST /sync/snapshot-sessions", handlers.HandleCreateSnapshotSession)
    mux.HandleFunc("GET /sync/snapshot-sessions/{snapshot_id}", handlers.HandleGetSnapshotChunk)
    mux.HandleFunc("DELETE /sync/snapshot-sessions/{snapshot_id}", handlers.HandleDeleteSnapshotSession)
    generationDiscovery := http.HandlerFunc(handlers.HandleGeneration)
    mux.Handle("GET /sync/generations/{generation_id}", generationDiscovery)
    mux.Handle("GET /sync/generations/", generationDiscovery)
    mux.HandleFunc("GET /syncx/health", handlers.HandleHealth)
    mux.HandleFunc("GET /syncx/status", handlers.HandleStatus)

    isGenerationDiscovery := func(r *http.Request) bool {
        return r.Method == http.MethodGet &&
            (r.URL.Path == "/sync/generations" || strings.HasPrefix(r.URL.Path, "/sync/generations/"))
    }
    admissionTarget := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if isGenerationDiscovery(r) {
            generationDiscovery.ServeHTTP(w, r)
            return
        }
        mux.ServeHTTP(w, r)
    })
    admittedSync := withSyncAdmission(admissionTarget)
    operationalSync := svc.ReadinessGuard(admittedSync)
    rootHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if isGenerationDiscovery(r) {
            admittedSync.ServeHTTP(w, r)
            return
        }
        if strings.HasPrefix(r.URL.Path, "/sync/") {
            operationalSync.ServeHTTP(w, r)
            return
        }
        mux.ServeHTTP(w, r)
    })

    log.Fatal(http.ListenAndServe(":8080", rootHandler))
}
```
{% endraw %}

`yourApplicationReadiness` is an application-owned `*oversync.ReadinessConfig` that pins
the deployed migration manifest, registry and generation enablement. Its adapter reads durable
application evidence; Bootstrap rejects missing or inconsistent evidence. Prepare that evidence
through your deployment controller before starting the service. The repository’s
[shared baseline readiness helper](../internal/baselinereadiness/baseline.go) shows the baseline
contract. Static applications provide a canonical semantic manifest, so only an intentional manifest
revision changes their durable deployment identity. The repository's
[schema-evolution harness](../integration/schema_evolution/README.md) supplies the maintenance,
SQL conversion, census and certificate implementation for migrations.

`yourAuthMiddleware` must authenticate the request and expose trusted `user_id` in request context.
`yourSyncClientPolicy` applies deployment client policy and shutdown admission. Generation discovery
bypasses only operational readiness; canonical and malformed generation paths remain authenticated,
source-validated, policy-admitted, cancellation-aware, and operation-tracked. The outer dispatcher
reaches the authenticated handler before `http.ServeMux` can clean repeated slashes or dot segments,
so all malformed discovery paths receive the canonical HTTP 400 error envelope. Every other `/sync/`
request, including non-GET discovery requests, uses operational readiness admission.
For accepted discovery responses, the server derives the canonical `SchemaGenerationProof`; callers do not
author or override that proof.
`oversync.ActorMiddleware(...)` reads `Oversync-Source-ID` and combines it with that trusted user
identity into `oversync.Actor{UserID, SourceID}`. The server derives `_sync_scope_id` from
`Actor.UserID`, so clients must not send `_sync_scope_id` in push payloads.

`schemaGenerations` is required. The client selects one generation ID and discovers its descriptor
from the server. Discovery includes authentication and `Oversync-Source-ID`, with neither generation
proof header. The SDK verifies and retains the descriptor, then includes both
`Oversync-Schema-Generation-ID` and `Oversync-Schema-Generation-Fingerprint` on all subsequent
`/sync` operations. `/syncx/health` and `/syncx/status` remain exempt.

The v1 generation contract deliberately excludes PostgreSQL date/time types. Store a timestamp as
application-chosen canonical text, such as RFC3339 with an explicit zone, and send that exact text
in `created_at`; the server does not invent a default or normalize it. The declarations above cover
every generation-visible physical column, including nullable `posts.content`, and match the SQL
surface exactly.

## Step 4: Create The SQLite Client

Local SQLite managed tables must declare exactly one visible sync key column, and that column
must also be the local SQLite `PRIMARY KEY` in the current runtime, with `NOT NULL` declared
explicitly. Supported local key shapes are `TEXT NOT NULL PRIMARY KEY` and UUID-backed
`BLOB NOT NULL PRIMARY KEY`.

```go
package main

import (
    "context"
    "log"

    "github.com/mobiletoly/go-oversync/oversqlite"
)

type initialSchemaMigrator struct{}

func (initialSchemaMigrator) InstalledVersion(ctx context.Context, reader oversqlite.LocalMigrationReader) (int64, error) {
    var version int64
    err := reader.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version)
    return version, err
}

func (initialSchemaMigrator) Migrate(ctx context.Context, tx oversqlite.LocalMigrationTx, _ oversqlite.LocalMigrationStep) error {
    _, err := tx.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS users (
            id TEXT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS posts (
            id TEXT NOT NULL PRIMARY KEY,
            author_id TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
        );
        PRAGMA user_version=1;
    `)
    return err
}

func main() {
    ctx := context.Background()

    cfg := &oversqlite.Config{
        GenerationID: 1,
        DatabaseID: "app-db",
        ServiceID: "my-service",
        LocalVersion: 1,
        Tables: []oversqlite.SyncTable{
            {TableName: "users", SyncKeyColumnName: "id"},
            {TableName: "posts", SyncKeyColumnName: "id"},
        },
        UploadLimit: 1000,
        DownloadLimit: 1000,
        BundleChangeWatchMode: oversqlite.BundleChangeWatchAuto,
    }

    tokenProvider := func(ctx context.Context) (string, error) {
        return "<jwt>", nil
    }

    opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{
        DatabasePath: "app.db",
        Plan: &oversqlite.UpgradePlan{
            PlanID: "fresh-v1", Mode: oversqlite.UpgradeModeFresh,
            DatabaseID: "app-db", ServiceID: "my-service",
            TargetGenerationID: 1, TargetVersion: 1,
            Migrator: initialSchemaMigrator{},
        },
        BaseURL: "http://localhost:8080", Token: tokenProvider,
        TargetClientConfig: cfg,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer opening.Close()

    opened, err := opening.Advance(ctx)
    if err != nil {
        log.Fatal(err)
    }
    client := opened.Ready
    if client == nil {
        log.Fatalf("database is not ready: %s", opened.Status.Outcome)
    }
    defer client.Close()
    db := client.Database()
    _ = db // borrowed for application SQL after ready

    connectResult, err := client.Attach(ctx, "user-123")
    if err != nil {
        log.Fatal(err)
    }
    if connectResult.Status == oversqlite.AttachStatusRetryLater {
        log.Printf("connect pending, retry after %s", connectResult.RetryAfter)
        return
    }
}
```

## Step 5: Sync

The supported high-level operations are:

```go
ctx := context.Background()

pushReport, err := client.PushPending(ctx)
if err != nil {
    log.Fatal(err)
}
log.Printf("push outcome: %s", pushReport.Outcome)

pullReport, err := client.PullToStable(ctx)
if err != nil {
    log.Fatal(err)
}
log.Printf("pull outcome: %s", pullReport.Outcome)

syncReport, err := client.Sync(ctx)
if err != nil {
    log.Fatal(err)
}
log.Printf("sync outcomes: push=%s remote=%s", syncReport.PushOutcome, syncReport.RemoteOutcome)

detachResult, err := client.Detach(ctx)
if err != nil {
    log.Fatal(err)
}
if detachResult.Outcome == oversqlite.DetachOutcomeBlockedUnsyncedData {
    log.Printf("detach blocked by %d pending rows", detachResult.PendingRowCount)
}
```

Rebuild operation:

```go
rebuildReport, err := client.Rebuild(ctx)
if err != nil {
    log.Fatal(err)
}
log.Printf("rebuild outcome: %s", rebuildReport.Outcome)
```

Behavior to expect:

- `Attach()` resolves first-account lifecycle through `POST /sync/connect`.
- `Open()`, `PushPending()`, `PullToStable()`, `Sync()`, `Detach()`, and `Rebuild()` return
  structured results in addition to `error`.
- `PushPending()` freezes one outbound snapshot, sends it once to `/sync/push`, and applies the
  authoritative response locally.
- after an ambiguous push reply, `PushPending()` replays the exact same complete request with its
  frozen source bundle ID and canonical request hash; an already-committed replay converges without
  recollecting or changing rows.
- `Attach()` may return `retry_later` as a normal retriable lifecycle outcome before the client
  becomes attached.
- `PullToStable()` drains complete bundles until the frozen `stable_bundle_seq` is reached.
- `PullToStable()` rebuilds automatically through snapshot sessions if the server returns
  `history_pruned`.
- an ambiguous push replay that falls below the retained floor rebuilds from a snapshot while
  preserving the frozen source tuple until recovery establishes its outcome.
- `Rebuild(ctx)` rebuilds through chunked snapshot sessions.
- if stale/pruned, out-of-order, changed, or retired source recovery is active, `Rebuild(ctx)`
  internally chooses the rebuild-plus-rotate path and preserves frozen local intent.
- `Detach()` clears synced local cache/state after a successful attached detach, restores dirty-row
  capture before returning, and reports blocked detach as a normal `DetachResult`.
- local writes made after `Detach()` are captured immediately as anonymous pending rows and can be synced after a later `Attach()`.
- Go intentionally does not expose reactive progress streams; inspect result structs for final
  lifecycle and sync outcomes instead.

Each data-plane path uses the client's fixed generation proof. The server retains one canonical
effect history and projects it to that proof for push replay, pull, conflicts, and snapshots.
Projected empty pull envelopes still advance the checkpoint. Trigger-normalized results are
accepted, while a push whose uploaded key is suppressed or otherwise uncovered fails atomically
with `push_effect_coverage_incomplete`. This setup does not switch generations or perform local DDL;
those remain upgrade responsibilities.

## Important Client Rules

- every managed table must declare its sync key explicitly
- managed tables must be FK-closed
- `PullToStable()` and `Rebuild(ctx)` fail closed while `_sync_outbox_*` exists
- `Sync()` automatically resumes checkpoint recovery while
  `_sync_attachment_state.rebuild_required = 1`; source-identity recovery remains explicit
- the durable read checkpoint is `_sync_attachment_state.last_bundle_seq_seen`
- the next outgoing client bundle id is `_sync_source_state.next_source_bundle_id`
- sync-visible absolute timestamps should use RFC3339 or RFC3339Nano text with an explicit zone, for example `2026-03-24T18:02:00Z`

## Endpoints

The server exposes:

- `POST /sync/connect`
- `POST /sync/push`
- `GET /sync/pull`
- `POST /sync/snapshot-sessions`
- `GET /sync/snapshot-sessions/{snapshot_id}`
- `DELETE /sync/snapshot-sessions/{snapshot_id}`
- `GET /sync/generations/{generation_id}`
- `GET /syncx/health`
- `GET /syncx/status`

All authenticated `/sync/*` requests must send `Oversync-Source-ID: <current-source-id>`.

## Echo Integration

If your server uses Echo, you can still use the standard `net/http` middleware:

```go
func WrapHTTPMiddleware(mw func(http.Handler) http.Handler) echo.MiddlewareFunc {
    return func(next echo.HandlerFunc) echo.HandlerFunc {
        return func(c echo.Context) error {
            var handlerErr error
            h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                c.SetRequest(r)
                handlerErr = next(c)
            }))
            h.ServeHTTP(c.Response(), c.Request())
            return handlerErr
        }
    }
}
```

Lifecycle-specific sync failures to expect on the HTTP surface:

- `scope_uninitialized`
- `scope_initializing`
- `initialization_stale`
- `initialization_expired`

## Binary Payload Contract

- non-key binary payload fields use standard base64 on the wire
- UUID-valued keys and UUID-valued key columns use dashed UUID text on the wire
- local trigger capture may use different internal encodings; those are not the HTTP contract

## Next Steps

- Run the maintained sample server in `examples/samplesync_server/`.
- Use `integration/` only when developing or qualifying this repository.
- Inspect the API contract in `docs/documentation/api.md`.

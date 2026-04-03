---
layout: page
title: Architecture
permalink: /architecture/
---

# Multi-Device Synchronization Architecture

go-oversync implements a bundle-based replication model for SQLite clients and PostgreSQL servers.
This page describes the current architecture in this repository, not earlier protocol iterations.

## Overview

The current architecture has four main pieces:

- your application HTTP server
- `oversync.SyncService` on PostgreSQL
- `oversqlite.Client` on SQLite
- the bundle/snapshot HTTP protocol between them

## How It Works

1. Your server registers the PostgreSQL tables that participate in sync.
2. `Bootstrap()` validates the server schema and prepares the sync metadata/runtime topology.
3. The SQLite client runs `Open()` on launch, then `Attach(userID)` after sign-in.
4. The SQLite client tracks local writes in `_sync_dirty_rows` with triggers.
   After `Detach()`, those triggers stay active so offline anonymous writes continue to enqueue
   local dirty state immediately.
5. `PushPending()` freezes those writes into `_sync_outbox_*`, uploads them through
   `/sync/push-sessions`, then replays the authoritative committed bundle returned by the server.
   Process restart keeps that singleton outbox durable and resumes from it instead of recollecting
   a new local bundle.
6. `PullToStable()` reads complete committed bundles from `/sync/pull` and advances the local
   checkpoint only after durable local apply.
7. `Rebuild(ctx)` rebuilds managed tables from `/sync/snapshot-sessions` when a client is new or
   falls behind retained history. For stale/out-of-order source recovery, `oversqlite` chooses the
   rebuild-plus-rotate path internally. The server materializes those frozen snapshots inside
   PostgreSQL session tables and serves chunk reads statelessly by `snapshot_id`. During rotated
   recovery the client persists one replacement source id durably, the server reserves that
   replacement source explicitly, and the old source is retired fail-closed until rebuild finishes.

```mermaid
flowchart LR
    subgraph Clients["SQLite Clients"]
        DeviceA["Device A<br/>(oversqlite)"]
        DeviceB["Device B<br/>(compatible client)"]
    end

    subgraph Server["Server"]
        HTTPServer["Your HTTP Server"]
        SyncService["oversync.SyncService"]
        PostgreSQL["PostgreSQL"]
    end

    DeviceA <-->|"push / pull / snapshot"| HTTPServer
    DeviceB <-->|"push / pull / snapshot"| HTTPServer
    HTTPServer --> SyncService
    SyncService --> PostgreSQL
```

## Authoritative State

PostgreSQL business tables are authoritative. Sync metadata is derived from committed
business-table effects and stored under the `sync` schema.

Important runtime tables include:

- `sync.user_state`
- `sync.row_state`
- `sync.bundle_log`
- `sync.bundle_rows`
- `sync.applied_pushes`
- `sync.push_sessions`
- `sync.push_session_rows`
- `sync.snapshot_sessions`
- `sync.snapshot_session_rows`

## Server-Originated Writes

If your application writes registered PostgreSQL tables outside client push handling, it should do
so through `ScopeManager.ExecWrite(...)` in the common case, or `WithinSyncBundle(...)` if your
application needs the lower-level primitive directly. Both paths ensure the committed business
transaction is captured as one sync bundle visible to other clients.

## Core Concepts

### [Core Concepts →]({{ site.baseurl }}/documentation/core-concepts/)

Read the core concepts page for the exact vocabulary used by the current contract.

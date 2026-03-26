---
layout: default
title: Server
permalink: /documentation/server/
---

# Server

The server treats registered PostgreSQL business tables as authoritative state.

## Runtime Tables

Authoritative replication state:

- `sync.user_state`: per-user bundle sequencing and retained-floor tracking
- `sync.row_state`: authoritative per-row row-version and tombstone state
- `sync.bundle_log`: one row per committed sync bundle
- `sync.bundle_rows`: normalized row effects for each committed bundle
- `sync.applied_pushes`: accepted-push replay table keyed by `(user_id, source_id, source_bundle_id)`

Transport/session state:

- `sync.push_sessions`: active staged push sessions
- `sync.push_session_rows`: staged rows for each push session
- `sync.snapshot_sessions`: active frozen snapshot session metadata
- `sync.snapshot_session_rows`: materialized rows for each snapshot session

## Write Path

- clients create staged push sessions with `POST /sync/push-sessions`
- clients upload ordered chunks with `POST /sync/push-sessions/{push_id}/chunks`
- clients commit the staged push with `POST /sync/push-sessions/{push_id}/commit`
- accepted-push recovery fetches authoritative rows through
  `GET /sync/committed-bundles/{bundle_seq}/rows`
- server-originated registered-table writes should run inside `WithinSyncBundle(...)`
- registered-table writes are captured by bundle triggers and finalized atomically with the
  business-table transaction

One push session, one committed bundle, and one `WithinSyncBundle(...)` transaction belong to
exactly one `user_id`. If application logic needs to affect multiple users, split that work into
separate per-user transactions or bundles.

## Registered Table DDL Requirements

Registered PostgreSQL tables must satisfy these rules before bootstrap:

- exactly one visible sync key column per registered table
- visible sync key type must be `uuid` or `text`
- every registered table must define `_sync_scope_id TEXT NOT NULL`
- `(_sync_scope_id, sync_key)` must be unique
- every unique constraint or unique index on a registered table must include `_sync_scope_id`
- registered table sets must be FK-closed
- registered-to-registered foreign keys must be scope-inclusive and `DEFERRABLE`
- supported `ON DELETE` / `ON UPDATE` actions are `NO ACTION`, `RESTRICT`, `CASCADE`, `SET NULL`,
  and `SET DEFAULT`
- supported `MATCH` options are empty, `NONE`, or `SIMPLE`
- `DEFERRABLE INITIALLY DEFERRED` is recommended
- `DEFERRABLE INITIALLY IMMEDIATE` is accepted
- partial, predicate, and expression unique indexes are unsupported on registered tables

Bootstrap validates these requirements and fails closed with an `UnsupportedSchemaError` if the
registered schema is outside the supported envelope.

## Read Path

- `GET /sync/pull` returns complete committed bundles only
- `POST /sync/snapshot-sessions` materializes one frozen current after-image inside PostgreSQL and
  returns a snapshot session id
- `GET /sync/snapshot-sessions/{snapshot_id}` returns deterministic chunks from that frozen
  snapshot without holding one long-lived database transaction across client round trips
- snapshot session creation may be bounded by optional row and byte caps in `ServiceConfig`
- if a client checkpoint falls behind the retained bundle floor, the server returns
  `history_pruned`

## Auth Contract

The handlers expect the caller to authenticate first and place
`oversync.Actor{UserID, SourceID}` into request context. The example servers do this with JWT
middleware, but the runtime does not require any specific auth stack. `_sync_scope_id` is derived
from `Actor.UserID`, enforced only on the authoritative PostgreSQL side, and excluded from
client-visible payloads, conflicts, pulls, and snapshots.

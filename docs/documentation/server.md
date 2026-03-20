---
layout: default
title: Server
---

# Server

The server treats registered business tables as authoritative state.

## Core metadata

- `sync.user_state`: per-user bundle sequencing and retained floor tracking.
- `sync.row_state`: authoritative per-row row-version/deleted state keyed by structured key.
- `sync.bundle_log`: one row per committed sync bundle.
- `sync.bundle_rows`: normalized row effects captured from committed business-table writes.
- `sync.applied_pushes`: accepted-push replay table keyed by `(user_id, source_id, source_bundle_id)`.

## Write path

- Client writes arrive through staged push sessions:
  - `POST /sync/push-sessions`
  - `POST /sync/push-sessions/{push_id}/chunks`
  - `POST /sync/push-sessions/{push_id}/commit`
- Accepted-push recovery fetches authoritative rows through
  `GET /sync/committed-bundles/{bundle_seq}/rows`.
- Server-originated writes must execute inside `WithinSyncBundle(...)`.
- Registered-table writes are captured by bundle triggers and finalized atomically with the
  business-table transaction.
- First cut is single-user only: one push session, one committed bundle, and one
  `WithinSyncBundle(...)` transaction belong to exactly one `user_id`. If application logic needs
  to affect multiple users, it must split that work into separate per-user transactions / bundles
  before calling oversync.

## Registered Table DDL Requirements

Table creators are responsible for defining registered business tables so they satisfy the sync
contract before bootstrap.

Current first-cut requirements:

- one `UUID` primary key per registered table
- one sync key column, matching that primary key
- registered table sets must be FK-closed
- supported foreign keys on registered tables must be `DEFERRABLE`
- `DEFERRABLE INITIALLY DEFERRED` is recommended
- `DEFERRABLE INITIALLY IMMEDIATE` is accepted
- composite primary keys and composite foreign keys are unsupported

Bootstrap validates these requirements and fails closed with an `UnsupportedSchemaError` if a
registered schema falls outside the supported envelope. The error includes the offending table or
constraint name and, for non-deferrable FKs, a hint to make the constraint `DEFERRABLE` before
bootstrap.

## Read path

- `GET /sync/pull` returns complete committed bundles only.
- `POST /sync/snapshot-sessions` freezes one current after-image and returns a snapshot session id.
- `GET /sync/snapshot-sessions/{snapshot_id}` returns deterministic chunks from that frozen snapshot.
- If a client checkpoint falls behind the retained bundle floor, the server returns
  `history_pruned` and the client rebuilds through chunked snapshot sessions.

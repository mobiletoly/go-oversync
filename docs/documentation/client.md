---
layout: default
title: Client
---

# Client

Clients track local edits via triggers and sync with the server.

Key concepts
- `_sync_pending`: coalesced queue of local changes (one row per PK).
- `_sync_row_meta`: per‑row `server_version` and `deleted` state.
- Upload: sends changes with `source_change_id`; server enforces idempotency.
- Download: ordered stream by `server_id`; echo suppression excludes your `source_id`.
- Hydration: windowed paging for snapshot‑consistent multi‑page downloads.

KMP client (Android/iOS/JVM)
- Call `bootstrap(userId, sourceId)` after sign‑in (stable user id and persisted device id).
- Run `uploadOnce()` then loop `downloadOnce()` until applied < limit.
- For recovery/first‑run, call `hydrate(windowed=true)`.

Go client (SQLite)
- Mirrors the same model; see the Go package README for examples.


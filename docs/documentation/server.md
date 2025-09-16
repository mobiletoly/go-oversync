---
layout: default
title: Server
---

# Server

The server SDK adds a sidecar `sync` schema and HTTP handlers for upload/download.

Highlights
- Per‑user isolation via keys that include `user_id`.
- Idempotent uploads with `(user_id, source_id, source_change_id)`.
- Optimistic concurrency with `server_version`.
- FK‑safe batches: parent‑first ordering + deferrable constraints.
- Snapshot‑consistent paging via `until`.

See the deep‑dive spec in `/specs/oversync_flow.md` for SQL and invariants.


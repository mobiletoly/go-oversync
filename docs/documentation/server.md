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
- Optional FK auto-migration: can be disabled via `DisableAutoMigrateFKs`.
- FK precheck controls: `FKPrecheckMode` (`enabled`, `disabled`, `ref_column_aware`).
- Manual ordering: `DependencyOverrides` (adds parent-first edges for ordering only).
- Upload guardrails: `MaxUploadBatchSize` (batch rejection) and `MaxPayloadBytes` (per-change invalid).

See `docs/documentation/advanced-concepts.md` for SQL, invariants, and edge cases.

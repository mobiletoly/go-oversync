# Complex Multi-Batch Scenario

## Purpose

`complex-multi-batch` is the large offline-dirty-set scenario for the current bundle-based client contract.
It checks that a client can:

- accumulate a large local dirty set while offline
- push that dirty set successfully with `PushPending()` when one logical push spans multiple
  upload chunks
- preserve FK correctness for the supported example schema, including:
  - `users -> posts`
  - self-referential `categories`
  - cyclic `teams <-> team_members`
  - `files -> file_reviews` BLOB-bearing rows
- hydrate or pull back to the same final state after reinstall-style rebuilds

## What It Stresses

### Large local dirty set

The scenario creates and mutates enough rows to force a substantial bundle-shaped push, then sets a
low per-chunk `UploadLimit` so the client must use multiple `/sync/push-sessions/{push_id}/chunks`
requests instead of one tiny happy-path upload.

### Local coalescing

Repeated edits to the same row should collapse into one dirty-row intent in `_sync_dirty_rows`
before push.

### FK-safe push ordering

The client should order upserts parent-first and deletes child-first for the supported local FK
graph.

### Stable rebuild

After the server has accepted the data, a rebuilt client should reach the same final state through
`Hydrate()` and one consistent chunked snapshot rebuild.

### Stale follower recovery

The scenario also leaves the phone behind while the laptop performs a large multi-chunk upload, then
forces retained-history pruning before the phone catches up. That proves a stale follower can still
rebuild to the same final state without any total-dirty-set ceiling on the writer.

### Restart recovery

The scenario also forces both supported restart boundaries:

- crash after upload but before commit, which must cause a full re-upload from row ordinal `0`
- crash after commit but before local authoritative replay, which must recover through
  `already_committed` plus committed-bundle row fetch

### Canonical binary wire contract

The scenario now also checks that the supported binary HTTP contract survives a large multi-batch,
multi-device run:

- non-key binary payload fields use standard base64 on the wire
- UUID-valued keys and UUID-valued key columns use dashed UUID text on the wire
- local SQLite dirty-row storage may still use internal encodings, but that is not the protocol

## Success Criteria

- `PushPending()` clears the local dirty set after authoritative bundle replay.
- a pre-commit restart re-uploads the same frozen outbound snapshot from zero
- a post-commit restart resumes from committed-bundle fetch and finishes authoritative replay
- a stale follower catches up through prune-triggered rebuild after the writer used a low per-chunk
  `UploadLimit`
- The final business-table state is correct on the server.
- A rebuilt client reaches the same final state with `Hydrate()`.
- `files` and `file_reviews` converge with the same guarantees as the scalar tables.

## Signals To Inspect

### On the client

- `_sync_dirty_rows` drops to zero after successful authoritative replay
- `_sync_client_state.next_source_bundle_id` advances
- `_sync_client_state.last_bundle_seq_seen` reaches the expected stable bundle sequence

### On the server

- `sync.bundle_log` contains committed bundles for the scenario user
- `sync.bundle_rows` contains the normalized row effects
- `sync.row_state` reflects final row versions and delete state
- `business.files` and `business.file_reviews` contain the expected final BLOB-backed rows

## Common Failure Shapes

### Dirty rows stay queued after push

Look at:

- push-session create / chunk / commit responses
- committed-bundle row fetch responses
- client authoritative bundle replay
- local `_sync_dirty_rows` and `_sync_row_state`

### Rebuilt client does not match server

Look at:

- snapshot-session chunk payloads
- local snapshot apply transaction
- bundle checkpoint after hydrate

### FK violation on rebuild

Look at:

- local schema closure
- deferred-FK snapshot apply
- whether the example schema was changed outside the supported envelope

## Why This Scenario Still Matters

This scenario is no longer about old upload-status-array behavior. It is now a stress test for the
bundle-based guarantees:

- one logical dirty-set push
- multi-chunk push-session upload for that one logical push
- authoritative committed bundle replay
- stable snapshot rebuild
- supported FK correctness under load
- binary payload correctness under load, not only scalar-table correctness

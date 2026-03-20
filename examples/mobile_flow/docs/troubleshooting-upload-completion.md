# Troubleshooting Bundle Push Completion

This note covers the current bundle-era failure mode: a client finishes `PushPending()` only after
the server accepts the push and the client durably replays the authoritative committed bundle
locally.

## Expected Success Path

1. Local writes accumulate in `_sync_dirty_rows`.
2. `PushPending()` freezes those rows into `_sync_push_outbound`.
3. The client creates `POST /sync/push-sessions`.
4. The client uploads one or more `POST /sync/push-sessions/{push_id}/chunks` requests.
5. The client commits with `POST /sync/push-sessions/{push_id}/commit`.
6. The client fetches authoritative rows from `GET /sync/committed-bundles/{bundle_seq}/rows`.
7. The client replays that committed bundle locally.
8. `_sync_dirty_rows` is cleared for the committed keys and `_sync_push_outbound` is removed.
9. `_sync_client_state.next_source_bundle_id` advances.

If any of those steps fails, the push should fail closed rather than partially mutating durable
client state.

## What To Check First

### Local dirty rows

```sql
SELECT COUNT(*) FROM _sync_dirty_rows;
```

If this stays non-zero after a failed push, that is usually correct. Dirty rows are supposed to
remain until authoritative replay succeeds.

### Client bundle state

```sql
SELECT source_id, next_source_bundle_id, last_bundle_seq_seen
FROM _sync_client_state
WHERE user_id = ?;
```

Key rule:

- `next_source_bundle_id` must not advance before the accepted bundle is durably replayed.

### Server bundle metadata

```sql
SELECT bundle_seq, source_id, source_bundle_id, row_count
FROM sync.bundle_log
WHERE user_id = $1
ORDER BY bundle_seq DESC
LIMIT 5;
```

If the server accepted the push, there should be a committed bundle for that `(user_id, source_id,
source_bundle_id)`.

## Common Failure Shapes

### Transport failure before acceptance

Symptoms:

- HTTP request fails
- no new row in `sync.bundle_log`
- `_sync_dirty_rows` and/or `_sync_push_outbound` stay intact

Expected behavior:

- retry the same logical push later
- reuse the same `source_bundle_id` until authoritative replay succeeds

### Accepted push but failed local replay

Symptoms:

- server has a new committed bundle
- client still has `_sync_push_outbound` or staged replay state
- `next_source_bundle_id` did not advance

Expected behavior:

- retry `PushPending()`
- the server should return `already_committed` for the same source identifiers
- the client should finish by fetching committed-bundle rows and replaying them locally

### Whole-bundle conflict

Symptoms:

- server returns HTTP `409`
- `_sync_dirty_rows` stays intact

Expected behavior:

- no partial success
- resolve the conflict locally, then push again

## Useful Logs

Look for:

- client push request/response
- local authoritative bundle replay errors
- server `sync.bundle_log` and `sync.bundle_rows` writes

## Summary

In the bundle-era client:

- success means accepted by the server and durably replayed locally
- failure means dirty rows remain and bundle ids do not advance prematurely
- retries are safe because push replay is idempotent by `(user_id, source_id, source_bundle_id)`

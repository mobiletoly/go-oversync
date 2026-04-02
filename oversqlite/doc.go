// Package oversqlite provides the SQLite client runtime for go-oversync's
// bundle-based sync contract.
//
// Applications call Open on startup, then Attach after sign-in. oversqlite
// manages source identity internally while still sending Oversync-Source-ID on
// authenticated sync requests. Local writes are captured into _sync_dirty_rows,
// frozen into a singleton durable outbox during PushPending, replayed from
// authoritative committed bundles, and rebuilt from chunked snapshot sessions
// when history is pruned or when the client needs a full refresh.
package oversqlite

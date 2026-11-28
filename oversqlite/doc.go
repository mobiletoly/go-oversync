// Package oversqlite provides the SQLite client runtime for go-oversync's
// bundle-based sync contract.
//
// Applications use NewOpening and Opening.Advance to obtain a ready Client,
// then call Client.Attach after sign-in. The client owns the SQLite pool and
// manages source identity. It freezes local writes during Client.PushPending,
// settles and acknowledges terminal upload outcomes, and applies complete
// committed bundles or sealed snapshots to advance its pull checkpoint.
package oversqlite
